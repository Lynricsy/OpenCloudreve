package backup

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

// Drive 上的目录结构（全部由本应用创建，drive.file 权限可见）：
//
//	OpenCloudreve Backup/        ocr_kind=root
//	├── blobs/                   ocr_kind=blobs
//	│   ├── 00/ … ff/            按 blob 名称前两位分片，避免单目录超过 Drive 的条目上限
//	│   │   └── <blob 名称>      ocr_kind=blob；内容为文件明文（加密时为 age 密文）
//	└── manifests/               ocr_kind=manifests
//	    └── <时间>.jsonl.gz[.age] ocr_kind=manifest；每次备份的完整文件清单
//
// 所有文件都带 ocr_root=<根目录 ID>，用于区分同一账号下多个站点的备份。
const (
	propKind        = "ocr_kind"
	propRoot        = "ocr_root"
	propKey         = "ocr_k"
	propFiles       = "ocr_files"
	propSize        = "ocr_size"
	propTime        = "ocr_time"
	kindRoot        = "root"
	kindBlobs       = "blobs"
	kindManifest    = "manifest"
	kindManifests   = "manifests"
	kindBlob        = "blob"
	rootFolderName  = "OpenCloudreve Backup"
	manifestVersion = 1
)

type (
	// Store 一个站点在 Drive 上的备份仓库。可并发使用。
	Store struct {
		dc        *DriveClient
		root      string
		blobs     string
		manifests string

		mu     sync.Mutex
		shards map[string]string
	}

	// BlobInfo Drive 上已有的 blob。
	BlobInfo struct {
		ID   string
		Size int64
	}

	// ManifestHeader 清单首行。
	ManifestHeader struct {
		Version   int       `json:"version"`
		CreatedAt time.Time `json:"created_at"`
		KeyID     string    `json:"key_id"`
		Policies  []int     `json:"policies"`
		// SiteID 生成快照的站点 ID（setting.SiteBasic(ctx).ID）。
		SiteID string `json:"site_id,omitempty"`
	}

	// ManifestEntry 清单中的一个文件。Path 为所有者“我的文件”下以 / 开头的完整路径。
	ManifestEntry struct {
		FileID     int       `json:"fid"`
		OwnerID    int       `json:"uid"`
		Owner      string    `json:"owner"`
		Path       string    `json:"path"`
		Size       int64     `json:"size"`
		ModifiedAt time.Time `json:"mtime"`
		Sha256     string    `json:"sha256"`
		Blob       string    `json:"blob"`
		// EntityID 主版本实体 ID。实体内容不可变，下次备份可直接沿用这里的 sha256，无需重新读取。
		EntityID int `json:"eid"`
		// EntityCreated 主版本实体的创建时间（UnixMicro）。
		EntityCreated int64 `json:"ectime,omitempty"`
	}

	// Manifest 解析后的清单。
	Manifest struct {
		Header  ManifestHeader
		Entries []ManifestEntry
	}

	// Snapshot Drive 上的一份清单（快照）。
	Snapshot struct {
		ID        string
		Name      string
		CreatedAt time.Time
		Files     int64
		Size      int64
		KeyID     string
	}
)

func kindQuery(kind string) string {
	return fmt.Sprintf("appProperties has { key=%s and value=%s }", quoteQuery(propKind), quoteQuery(kind))
}

func (s *Store) rootQuery(kind string) string {
	return fmt.Sprintf("%s and appProperties has { key=%s and value=%s } and trashed=false",
		kindQuery(kind), quoteQuery(propRoot), quoteQuery(s.root))
}

// EnsureRoot 返回可用的根目录 ID：优先使用已记录的 rootID；它不存在或已进回收站时，
// 采用账号中最新的已有备份根目录（重装站点后可接上原备份），都没有则新建。
func EnsureRoot(ctx context.Context, dc *DriveClient, rootID string) (string, error) {
	if rootID != "" {
		f, err := dc.Get(ctx, rootID)
		if err == nil && !f.Trashed {
			return rootID, nil
		}
		var de *DriveError
		if err != nil && !(errors.As(err, &de) && de.Status == 404) {
			return "", fmt.Errorf("failed to check backup root folder: %w", err)
		}
	}

	var newest *DriveFile
	if err := dc.List(ctx, kindQuery(kindRoot)+" and trashed=false", func(f *DriveFile) error {
		if newest == nil || f.CreatedTime.After(newest.CreatedTime) {
			newest = f
		}
		return nil
	}); err != nil {
		return "", fmt.Errorf("failed to search backup root folder: %w", err)
	}
	if newest != nil {
		return newest.ID, nil
	}

	f, err := dc.CreateFolder(ctx, rootFolderName, "", map[string]string{propKind: kindRoot})
	if err != nil {
		return "", fmt.Errorf("failed to create backup root folder: %w", err)
	}
	return f.ID, nil
}

// OpenStore 打开根目录下的备份仓库，缺失的子目录会被创建。
func OpenStore(ctx context.Context, dc *DriveClient, root string) (*Store, error) {
	s := &Store{dc: dc, root: root, shards: make(map[string]string)}
	var err error
	if s.blobs, err = s.ensureChild(ctx, root, "blobs", kindBlobs); err != nil {
		return nil, err
	}
	if s.manifests, err = s.ensureChild(ctx, root, "manifests", kindManifests); err != nil {
		return nil, err
	}
	if err := dc.List(ctx, fmt.Sprintf("%s in parents and mimeType=%s and trashed=false", quoteQuery(s.blobs), quoteQuery(folderMime)),
		func(f *DriveFile) error {
			s.shards[f.Name] = f.ID
			return nil
		}); err != nil {
		return nil, fmt.Errorf("failed to list blob shards: %w", err)
	}
	return s, nil
}

func (s *Store) ensureChild(ctx context.Context, parent, name, kind string) (string, error) {
	id := ""
	if err := s.dc.List(ctx, fmt.Sprintf("%s in parents and %s and trashed=false", quoteQuery(parent), kindQuery(kind)),
		func(f *DriveFile) error {
			if id == "" {
				id = f.ID
			}
			return nil
		}); err != nil {
		return "", fmt.Errorf("failed to find %s folder: %w", name, err)
	}
	if id != "" {
		return id, nil
	}
	f, err := s.dc.CreateFolder(ctx, name, parent, map[string]string{propKind: kind, propRoot: s.root})
	if err != nil {
		return "", fmt.Errorf("failed to create %s folder: %w", name, err)
	}
	return f.ID, nil
}

// Root 根目录 ID。
func (s *Store) Root() string {
	return s.root
}

func (s *Store) shard(ctx context.Context, name string) (string, error) {
	key := name[:2]
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.shards[key]; ok {
		return id, nil
	}
	f, err := s.dc.CreateFolder(ctx, key, s.blobs, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create blob shard %q: %w", key, err)
	}
	s.shards[key] = f.ID
	return f.ID, nil
}

// ListBlobs 列出本仓库全部 blob（名称 → 文件列表；正常情况下每个名称只有一个文件）。
func (s *Store) ListBlobs(ctx context.Context) (map[string][]BlobInfo, error) {
	res := make(map[string][]BlobInfo)
	err := s.dc.List(ctx, s.rootQuery(kindBlob), func(f *DriveFile) error {
		size, _ := strconv.ParseInt(f.Size, 10, 64)
		res[f.Name] = append(res[f.Name], BlobInfo{ID: f.ID, Size: size})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list blobs: %w", err)
	}
	return res, nil
}

// FindBlob 按名称查找 blob，不存在返回 nil。
func (s *Store) FindBlob(ctx context.Context, name string) (*DriveFile, error) {
	var found *DriveFile
	err := s.dc.List(ctx, s.rootQuery(kindBlob)+" and name = "+quoteQuery(name), func(f *DriveFile) error {
		if found == nil {
			found = f
		}
		return nil
	})
	return found, err
}

// DeleteFile 删除 Drive 文件。
func (s *Store) DeleteFile(ctx context.Context, id string) error {
	return s.dc.Delete(ctx, id)
}

// uploadStream 把 produce 写出的数据流式上传。produce 返回错误时上传会被中止，不会留下残缺文件。
func (s *Store) uploadStream(ctx context.Context, name, parent string, props map[string]string, produce func(w io.Writer) error) (*DriveFile, error) {
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := produce(pw)
		_ = pw.CloseWithError(err)
		done <- err
	}()
	f, upErr := s.dc.Upload(ctx, name, parent, props, pr)
	// 上传提前结束时让生产者退出
	_ = pr.CloseWithError(errors.New("upload aborted"))
	prodErr := <-done
	if upErr != nil {
		// 只有上传因读到生产者的错误而失败时，才报告源错误；否则生产者的错误
		// 只是上面主动中止管道的结果，应返回上传侧的原始错误（保留 DriveError）。
		if prodErr != nil && errors.Is(upErr, prodErr) {
			return nil, prodErr
		}
		return nil, upErr
	}
	return f, nil
}

// UploadBlob 上传一个 blob。上传过程中重新计算明文 sha256，与 sum 不一致时中止上传。
func (s *Store) UploadBlob(ctx context.Context, kr *Keyring, sum []byte, src io.Reader) (*DriveFile, error) {
	name := kr.BlobName(sum)
	parent, err := s.shard(ctx, name)
	if err != nil {
		return nil, err
	}
	return s.uploadStream(ctx, name, parent, map[string]string{propKind: kindBlob, propRoot: s.root, propKey: kr.ID()},
		func(w io.Writer) error {
			ew, err := kr.Encrypt(w)
			if err != nil {
				return err
			}
			h := sha256.New()
			if _, err := io.Copy(io.MultiWriter(ew, h), src); err != nil {
				return fmt.Errorf("failed to read source: %w", err)
			}
			if err := ew.Close(); err != nil {
				return err
			}
			if got := h.Sum(nil); string(got) != string(sum) {
				return errors.New("source content changed during upload (sha256 mismatch)")
			}
			return nil
		})
}

// OpenBlob 打开 blob 的明文内容。
func (s *Store) OpenBlob(ctx context.Context, kr *Keyring, id string) (io.ReadCloser, error) {
	rc, err := s.dc.Download(ctx, id)
	if err != nil {
		return nil, err
	}
	r, err := kr.Decrypt(rc)
	if err != nil {
		rc.Close()
		return nil, fmt.Errorf("failed to decrypt blob: %w", err)
	}
	return readCloser{Reader: r, Closer: rc}, nil
}

type readCloser struct {
	io.Reader
	io.Closer
}

// ListSnapshots 列出全部快照，按时间从新到旧排序。
func (s *Store) ListSnapshots(ctx context.Context) ([]Snapshot, error) {
	var res []Snapshot
	err := s.dc.List(ctx, s.rootQuery(kindManifest), func(f *DriveFile) error {
		snap := Snapshot{ID: f.ID, Name: f.Name, CreatedAt: f.CreatedTime, KeyID: f.AppProperties[propKey]}
		snap.Files, _ = strconv.ParseInt(f.AppProperties[propFiles], 10, 64)
		snap.Size, _ = strconv.ParseInt(f.AppProperties[propSize], 10, 64)
		if t, err := strconv.ParseInt(f.AppProperties[propTime], 10, 64); err == nil {
			snap.CreatedAt = time.Unix(t, 0)
		}
		res = append(res, snap)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	slices.SortFunc(res, func(a, b Snapshot) int {
		return b.CreatedAt.Compare(a.CreatedAt)
	})
	return res, nil
}

// UploadManifest 压缩、（可选）加密并上传本地 JSONL 清单。
func (s *Store) UploadManifest(ctx context.Context, kr *Keyring, localPath string, createdAt time.Time, files, size int64) (*DriveFile, error) {
	name := createdAt.UTC().Format("20060102T150405Z") + ".jsonl.gz"
	if kr != nil {
		name += ".age"
	}
	return s.uploadStream(ctx, name, s.manifests, map[string]string{
		propKind:  kindManifest,
		propRoot:  s.root,
		propKey:   kr.ID(),
		propFiles: strconv.FormatInt(files, 10),
		propSize:  strconv.FormatInt(size, 10),
		propTime:  strconv.FormatInt(createdAt.Unix(), 10),
	}, func(w io.Writer) error {
		f, err := os.Open(localPath)
		if err != nil {
			return err
		}
		defer f.Close()
		ew, err := kr.Encrypt(w)
		if err != nil {
			return err
		}
		gz := gzip.NewWriter(ew)
		if _, err := io.Copy(gz, f); err != nil {
			return err
		}
		if err := gz.Close(); err != nil {
			return err
		}
		return ew.Close()
	})
}

// ScanManifest 流式下载并解析快照清单，依次回调首行与每个条目；fn 返回错误时中止。
func (s *Store) ScanManifest(ctx context.Context, kr *Keyring, id string, onHeader func(*ManifestHeader) error, fn func(*ManifestEntry) error) error {
	rc, err := s.dc.Download(ctx, id)
	if err != nil {
		return err
	}
	defer rc.Close()
	r, err := kr.Decrypt(rc)
	if err != nil {
		return fmt.Errorf("failed to decrypt manifest: %w", err)
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("failed to decompress manifest: %w", err)
	}
	defer gz.Close()

	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	first := true
	for sc.Scan() {
		if first {
			first = false
			var h ManifestHeader
			if err := json.Unmarshal(sc.Bytes(), &h); err != nil {
				return fmt.Errorf("invalid manifest header: %w", err)
			}
			if h.Version != manifestVersion {
				return fmt.Errorf("unsupported manifest version %d", h.Version)
			}
			if onHeader != nil {
				if err := onHeader(&h); err != nil {
					return err
				}
			}
			continue
		}
		var e ManifestEntry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return fmt.Errorf("invalid manifest entry: %w", err)
		}
		if err := fn(&e); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("failed to read manifest: %w", err)
	}
	if first {
		return errors.New("empty manifest")
	}
	return nil
}

// ReadManifest 下载并解析整份快照清单。
func (s *Store) ReadManifest(ctx context.Context, kr *Keyring, id string) (*Manifest, error) {
	m := &Manifest{}
	err := s.ScanManifest(ctx, kr, id, func(h *ManifestHeader) error {
		m.Header = *h
		return nil
	}, func(e *ManifestEntry) error {
		m.Entries = append(m.Entries, *e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// NewManifestHeader 构造清单首行。siteID 为生成快照的站点 ID。
func NewManifestHeader(createdAt time.Time, kr *Keyring, policies []int, siteID string) *ManifestHeader {
	return &ManifestHeader{Version: manifestVersion, CreatedAt: createdAt, KeyID: kr.ID(), Policies: policies, SiteID: siteID}
}

// EncodeLine 把 v 编码为一行 JSON（含换行符）。
func EncodeLine(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
