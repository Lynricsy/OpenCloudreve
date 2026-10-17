package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeDrive 实现本包用到的 Google OAuth / Drive v3 接口子集。
type fakeDrive struct {
	mu       sync.Mutex
	nextID   int
	files    map[string]*fakeFile
	sessions map[string]*fakeSession

	// failChunkPuts 接下来若干次分块 PUT 返回 503
	failChunkPuts int
	// partialAcks 接下来若干次分块 PUT 只确认前 256 KiB
	partialAcks int
	chunkPuts   int
}

type fakeFile struct {
	meta DriveFile
	data []byte
}

type fakeSession struct {
	meta DriveFile
	data []byte
}

// redirectTransport 把所有请求转发到测试服务器，保留路径与查询参数。
type redirectTransport struct{ target *url.URL }

func (t redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	r.URL.Scheme, r.URL.Host, r.Host = t.target.Scheme, t.target.Host, t.target.Host
	return http.DefaultTransport.RoundTrip(r)
}

func newFakeDrive(t *testing.T) (*fakeDrive, *DriveClient) {
	t.Helper()
	fd := &fakeDrive{files: make(map[string]*fakeFile), sessions: make(map[string]*fakeSession)}
	srv := httptest.NewServer(fd)
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)

	oldChunk, oldDelay := uploadChunkSize, retryBaseDelay
	uploadChunkSize, retryBaseDelay = 512<<10, time.Millisecond
	t.Cleanup(func() { uploadChunkSize, retryBaseDelay = oldChunk, oldDelay })

	hc := &http.Client{Transport: redirectTransport{target: target}}
	return fd, NewDriveClient(hc, tokenFunc(func() string { return "access" }))
}

type tokenFunc func() string

func (f tokenFunc) Token(context.Context) (string, error) { return f(), nil }

func (fd *fakeDrive) id() string {
	fd.nextID++
	return "f" + strconv.Itoa(fd.nextID)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (fd *fakeDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	switch {
	case r.URL.Path == "/token":
		writeJSON(w, 200, map[string]any{"access_token": "access", "expires_in": 3600, "refresh_token": "refresh"})
	case r.URL.Path == "/drive/v3/about":
		writeJSON(w, 200, map[string]any{"user": map[string]string{"emailAddress": "a@b.c"}, "storageQuota": map[string]string{"limit": "100"}})
	case r.URL.Path == "/drive/v3/files" && r.Method == http.MethodGet:
		fd.list(w, r.URL.Query().Get("q"))
	case r.URL.Path == "/drive/v3/files" && r.Method == http.MethodPost:
		meta, ok := decodeCreateMeta(w, r)
		if !ok {
			return
		}
		meta.ID, meta.CreatedTime = fd.id(), time.Now()
		fd.files[meta.ID] = &fakeFile{meta: meta}
		writeJSON(w, 200, meta)
	case strings.HasPrefix(r.URL.Path, "/drive/v3/files/"):
		id := strings.TrimPrefix(r.URL.Path, "/drive/v3/files/")
		f, ok := fd.files[id]
		if !ok {
			writeJSON(w, 404, map[string]any{"error": map[string]any{"message": "not found"}})
			return
		}
		switch {
		case r.Method == http.MethodDelete:
			delete(fd.files, id)
			w.WriteHeader(204)
		case r.URL.Query().Get("alt") == "media":
			_, _ = w.Write(f.data)
		default:
			writeJSON(w, 200, f.meta)
		}
	case r.URL.Path == "/upload/drive/v3/files" && r.Method == http.MethodPost:
		meta, ok := decodeCreateMeta(w, r)
		if !ok {
			return
		}
		sid := fd.id()
		fd.sessions[sid] = &fakeSession{meta: meta}
		w.Header().Set("Location", "https://www.googleapis.com/upload/drive/v3/files?upload_id="+sid)
		w.WriteHeader(200)
	case r.URL.Path == "/upload/drive/v3/files" && r.Method == http.MethodPut:
		fd.putChunk(w, r)
	default:
		http.Error(w, "unexpected "+r.Method+" "+r.URL.String(), 400)
	}
}

// decodeCreateMeta 按真实 Drive 的行为校验新建文件的元数据：请求体带 id（哪怕是空串）会返回
// 400 fileIdNotUsable，带 createdTime 会改写文件创建时间，因此只接受客户端应当发送的字段。
func decodeCreateMeta(w http.ResponseWriter, r *http.Request) (DriveFile, bool) {
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeJSON(w, 400, map[string]any{"error": map[string]any{"message": err.Error()}})
		return DriveFile{}, false
	}
	for k := range raw {
		switch k {
		case "name", "mimeType", "parents", "appProperties":
		default:
			writeJSON(w, 400, map[string]any{"error": map[string]any{
				"message": "unexpected field " + k + " in create request",
				"errors":  []map[string]string{{"reason": "fileIdNotUsable"}},
			}})
			return DriveFile{}, false
		}
	}
	var meta DriveFile
	b, _ := json.Marshal(raw)
	_ = json.Unmarshal(b, &meta)
	return meta, true
}

var rangeRe = regexp.MustCompile(`^bytes (\*|(\d+)-(\d+))/(\*|\d+)$`)

func (fd *fakeDrive) putChunk(w http.ResponseWriter, r *http.Request) {
	s, ok := fd.sessions[r.URL.Query().Get("upload_id")]
	if !ok {
		w.WriteHeader(404)
		return
	}
	body, _ := io.ReadAll(r.Body)
	m := rangeRe.FindStringSubmatch(r.Header.Get("Content-Range"))
	if m == nil {
		http.Error(w, "bad range", 400)
		return
	}
	ack := func() {
		if len(s.data) > 0 {
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(s.data)-1))
		}
		w.WriteHeader(308)
	}

	if m[1] != "*" {
		fd.chunkPuts++
		if fd.failChunkPuts > 0 {
			fd.failChunkPuts--
			w.WriteHeader(503)
			return
		}
		start, _ := strconv.Atoi(m[2])
		if start != len(s.data) {
			http.Error(w, fmt.Sprintf("offset %d, have %d", start, len(s.data)), 400)
			return
		}
		if fd.partialAcks > 0 && len(body) > 256<<10 {
			fd.partialAcks--
			s.data = append(s.data, body[:256<<10]...)
			ack()
			return
		}
		s.data = append(s.data, body...)
	}
	if m[4] == "*" || strconv.Itoa(len(s.data)) != m[4] {
		ack()
		return
	}
	meta := s.meta
	meta.ID, meta.CreatedTime, meta.Size = fd.id(), time.Now(), strconv.Itoa(len(s.data))
	fd.files[meta.ID] = &fakeFile{meta: meta, data: s.data}
	writeJSON(w, 200, meta)
}

var (
	propRe   = regexp.MustCompile(`^appProperties has \{ key='(.*)' and value='(.*)' \}$`)
	parentRe = regexp.MustCompile(`^'(.*)' in parents$`)
	mimeRe   = regexp.MustCompile(`^mimeType='(.*)'$`)
	nameRe   = regexp.MustCompile(`^name = '(.*)'$`)
)

// splitAnd 按顶层的 " and " 切分查询（忽略花括号内的 and）。
func splitAnd(q string) []string {
	var res []string
	depth, last := 0, 0
	for i := range len(q) {
		switch q[i] {
		case '{':
			depth++
		case '}':
			depth--
		}
		if depth == 0 && strings.HasPrefix(q[i:], " and ") {
			res = append(res, q[last:i])
			last = i + len(" and ")
		}
	}
	return append(res, q[last:])
}

func (fd *fakeDrive) list(w http.ResponseWriter, q string) {
	var res []DriveFile
	for _, f := range fd.files {
		if fd.match(f, q) {
			res = append(res, f.meta)
		}
	}
	writeJSON(w, 200, map[string]any{"files": res})
}

func (fd *fakeDrive) match(f *fakeFile, q string) bool {
	for _, c := range splitAnd(q) {
		switch {
		case c == "trashed=false":
		case propRe.MatchString(c):
			m := propRe.FindStringSubmatch(c)
			if f.meta.AppProperties[m[1]] != m[2] {
				return false
			}
		case parentRe.MatchString(c):
			m := parentRe.FindStringSubmatch(c)
			if len(f.meta.Parents) == 0 || f.meta.Parents[0] != m[1] {
				return false
			}
		case mimeRe.MatchString(c):
			if f.meta.MimeType != mimeRe.FindStringSubmatch(c)[1] {
				return false
			}
		case nameRe.MatchString(c):
			if f.meta.Name != nameRe.FindStringSubmatch(c)[1] {
				return false
			}
		default:
			panic("unsupported query clause: " + c)
		}
	}
	return true
}
