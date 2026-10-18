package manager

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
)

type seekableBody struct {
	*bytes.Reader
}

func (seekableBody) Close() error { return nil }

func newStreamSession(id string, size int64) *fs.UploadSession {
	return &fs.UploadSession{Props: &fs.UploadProps{UploadSessionID: id, Size: size, ExpireAt: time.Now().Add(time.Hour)}}
}

// writeChunk 模拟驱动按 offset 写入一段数据：经由 beginStreamHash 包装读取，ok 表示写入是否成功。
func writeChunk(t *testing.T, m *manager, s *fs.UploadSession, policy *ent.StoragePolicy, offset int64, data []byte, ok bool) {
	t.Helper()
	req := &fs.UploadRequest{File: io.NopCloser(bytes.NewReader(data)), Offset: offset, Props: s.Props.Copy()}
	a := m.beginStreamHash(s, req, policy)
	if a == nil {
		return
	}
	if _, err := io.Copy(io.Discard, a); err != nil {
		t.Fatal(err)
	}
	a.finish(ok)
}

func sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func TestStreamHash(t *testing.T) {
	local := &ent.StoragePolicy{Type: types.PolicyTypeLocal}
	data := []byte(strings.Repeat("0123456789", 30))
	parts := [][]byte{data[:100], data[100:200], data[200:]}
	size := int64(len(data))

	cases := []struct {
		name   string
		policy *ent.StoragePolicy
		run    func(m *manager, s *fs.UploadSession)
		want   string
	}{
		{"按顺序写满得到哈希", local, func(m *manager, s *fs.UploadSession) {
			writeChunk(t, m, s, local, 0, parts[0], true)
			writeChunk(t, m, s, local, 100, parts[1], true)
			writeChunk(t, m, s, local, 200, parts[2], true)
		}, sum(data)},
		{"乱序到达放弃流式结果", local, func(m *manager, s *fs.UploadSession) {
			writeChunk(t, m, s, local, 0, parts[0], true)
			writeChunk(t, m, s, local, 200, parts[2], true)
			writeChunk(t, m, s, local, 100, parts[1], true)
		}, ""},
		{"某段写入失败后即使重传也放弃", local, func(m *manager, s *fs.UploadSession) {
			writeChunk(t, m, s, local, 0, parts[0], true)
			writeChunk(t, m, s, local, 100, parts[1], false)
			writeChunk(t, m, s, local, 100, parts[1], true)
			writeChunk(t, m, s, local, 200, parts[2], true)
		}, ""},
		{"已计入的分片被重传（内容可能不同）时放弃", local, func(m *manager, s *fs.UploadSession) {
			writeChunk(t, m, s, local, 0, parts[0], true)
			writeChunk(t, m, s, local, 100, parts[1], true)
			writeChunk(t, m, s, local, 100, bytes.ToUpper(parts[1]), true)
			writeChunk(t, m, s, local, 200, parts[2], true)
		}, ""},
		{"未写满声明的大小不返回结果", local, func(m *manager, s *fs.UploadSession) {
			writeChunk(t, m, s, local, 0, parts[0], true)
			writeChunk(t, m, s, local, 100, parts[1], true)
		}, ""},
		{"其他存储策略不计算", &ent.StoragePolicy{Type: types.PolicyTypeS3}, func(m *manager, s *fs.UploadSession) {
			s3 := &ent.StoragePolicy{Type: types.PolicyTypeS3}
			writeChunk(t, m, s, s3, 0, data, true)
		}, ""},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := &manager{}
			s := newStreamSession(fmt.Sprintf("case-%d", i), size)
			c.run(m, s)
			if got := takeStreamHash(s.Props.UploadSessionID, size); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestStreamHashConcurrentChunk(t *testing.T) {
	local := &ent.StoragePolicy{Type: types.PolicyTypeLocal}
	m := &manager{}
	s := newStreamSession("concurrent", 20)

	first := m.beginStreamHash(s, &fs.UploadRequest{File: io.NopCloser(bytes.NewReader(make([]byte, 10))), Props: s.Props.Copy()}, local)
	if first == nil {
		t.Fatal("first chunk should be hashed")
	}
	// 第一段尚未写完时到达的另一段（即使偏移看似连续）不能参与哈希，并使整个会话失效
	if second := m.beginStreamHash(s, &fs.UploadRequest{File: io.NopCloser(bytes.NewReader(make([]byte, 10))), Offset: 0, Props: s.Props.Copy()}, local); second != nil {
		t.Fatal("concurrent chunk must not be hashed")
	}
	_, _ = io.Copy(io.Discard, first)
	first.finish(true)

	if got := takeStreamHash("concurrent", 10); got != "" {
		t.Fatalf("session should be broken, got %q", got)
	}
}

func TestStreamHashSeekBreaks(t *testing.T) {
	local := &ent.StoragePolicy{Type: types.PolicyTypeLocal}
	m := &manager{}
	data := []byte("hello world")
	s := newStreamSession("seek", int64(len(data)))

	body := seekableBody{bytes.NewReader(data)}
	req := &fs.UploadRequest{File: body, Seeker: body, Props: s.Props.Copy()}
	a := m.beginStreamHash(s, req, local)
	if a == nil {
		t.Fatal("chunk should be hashed")
	}
	// 驱动读了一部分后回退重读：前面的数据会被计入两次
	buf := make([]byte, 5)
	_, _ = a.Read(buf)
	if _, err := a.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, a)
	a.finish(true)

	if got := takeStreamHash("seek", int64(len(data))); got != "" {
		t.Fatalf("seek should break stream hash, got %q", got)
	}
}

func TestStreamHashSkipsClientSideEncrypted(t *testing.T) {
	local := &ent.StoragePolicy{Type: types.PolicyTypeLocal}
	m := &manager{}
	s := newStreamSession("cse", 3)
	props := s.Props.Copy()
	props.ClientSideEncrypted = true
	// 客户端加密时请求体是密文，不能当作明文哈希
	if a := m.beginStreamHash(s, &fs.UploadRequest{File: io.NopCloser(bytes.NewReader([]byte("abc"))), Props: props}, local); a != nil {
		t.Fatal("client-side encrypted upload must not be hashed")
	}
}
