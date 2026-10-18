package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/util"
)

// 上传时流式计算 SHA-256。
//
// 只有当一个上传会话的全部数据都在本进程内、从偏移 0 起按顺序、每段都成功写入时，结果才可信：
// 任何乱序、并发、重传、失败或回退读取都会把该会话标记为失效，完成上传后改由计算任务重新读取。
// 这依赖“主机只有一个进程”的前提（与上传分片计数 uploadSessionLocks 相同）。
type (
	streamHash struct {
		mu       sync.Mutex
		h        hash.Hash
		next     int64 // 已按顺序写入并计入哈希的字节数
		busy     bool  // 有一段数据正在写入
		broken   bool
		expireAt time.Time
	}

	// streamHashAttempt 包装一段上传数据，在驱动读取时同步计入哈希。
	streamHashAttempt struct {
		st     *streamHash
		offset int64
		n      int64
		seeked bool
		file   io.ReadCloser
		seeker io.Seeker
	}
)

// streamHashes 上传会话 ID → *streamHash
var streamHashes sync.Map

const streamHashDefaultTTL = 24 * time.Hour

func (a *streamHashAttempt) Read(p []byte) (int, error) {
	n, err := a.file.Read(p)
	if n > 0 {
		a.st.h.Write(p[:n])
		a.n += int64(n)
	}
	return n, err
}

func (a *streamHashAttempt) Close() error {
	return a.file.Close()
}

// Seek 驱动重试时会回退读取位置，已计入哈希的数据会被再读一遍，此时放弃流式结果。
func (a *streamHashAttempt) Seek(offset int64, whence int) (int64, error) {
	pos, err := a.seeker.Seek(offset, whence)
	if err != nil || pos != a.n {
		a.seeked = true
	}
	return pos, err
}

// finish 结束这一段写入；ok 为驱动是否成功写入全部数据。
func (a *streamHashAttempt) finish(ok bool) {
	a.st.mu.Lock()
	defer a.st.mu.Unlock()

	a.st.busy = false
	if !ok || a.seeked || a.st.broken {
		a.st.broken = true
		return
	}
	a.st.next = a.offset + a.n
}

// beginStreamHash 为本段上传数据开始流式哈希；不满足条件时返回 nil。
// 只用于本机策略：完成时要核对落盘文件的大小（见 hashForNewEntity），从机策略的中转上传无法在主机核对，交给任务计算。
func (m *manager) beginStreamHash(session *fs.UploadSession, req *fs.UploadRequest, policy *ent.StoragePolicy) *streamHashAttempt {
	if m.stateless || session == nil || session.Props == nil || session.Importing || req.File == nil ||
		req.ImportFrom != nil || req.Props == nil || req.Props.ClientSideEncrypted ||
		policy == nil || policy.Type != types.PolicyTypeLocal ||
		(session.Props.EntityType != nil && *session.Props.EntityType != types.EntityTypeVersion) {
		return nil
	}

	id := session.Props.UploadSessionID
	var st *streamHash
	if req.Offset == 0 {
		expireAt := session.Props.ExpireAt
		if expireAt.IsZero() {
			expireAt = time.Now().Add(streamHashDefaultTTL)
		}
		v, loaded := streamHashes.LoadOrStore(id, &streamHash{h: sha256.New(), expireAt: expireAt})
		st = v.(*streamHash)
		if !loaded {
			sweepStreamHashes()
		}
	} else {
		v, ok := streamHashes.Load(id)
		if !ok {
			return nil
		}
		st = v.(*streamHash)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.broken || st.busy || req.Offset != st.next {
		st.broken = true
		return nil
	}

	st.busy = true
	return &streamHashAttempt{st: st, offset: req.Offset, file: req.File, seeker: req.Seeker}
}

// takeStreamHash 取出并移除会话的流式哈希；只有恰好按顺序写满 size 字节时才返回结果。
func takeStreamHash(sessionID string, size int64) string {
	v, ok := streamHashes.LoadAndDelete(sessionID)
	if !ok {
		return ""
	}

	st := v.(*streamHash)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.broken || st.busy || st.next != size {
		return ""
	}
	return hex.EncodeToString(st.h.Sum(nil))
}

func dropStreamHash(sessionID string) {
	streamHashes.Delete(sessionID)
}

// sweepStreamHashes 清理已过期、未完成的会话，避免放弃的上传占用内存。
func sweepStreamHashes() {
	now := time.Now()
	streamHashes.Range(func(key, value any) bool {
		st := value.(*streamHash)
		st.mu.Lock()
		expired := now.After(st.expireAt)
		st.mu.Unlock()
		if expired {
			streamHashes.Delete(key)
		}
		return true
	})
}

// hashForNewEntity 在上传完成后为新版本实体写入流式哈希；没有可用结果时创建计算任务。
func (m *manager) hashForNewEntity(ctx context.Context, session *fs.UploadSession) {
	sum := takeStreamHash(session.Props.UploadSessionID, session.Props.Size)
	if session.Importing || session.EntityID <= 0 || !HashComputablePolicy(session.Policy) ||
		(session.Props.EntityType != nil && *session.Props.EntityType != types.EntityTypeVersion) {
		return
	}

	fc := m.dep.FileClient()
	if sum != "" && localFileSizeIs(session.Props.SavePath, session.Props.Size) {
		err := fc.SetEntitySha256(ctx, session.EntityID, sum)
		if err == nil {
			return
		}
		m.l.Warning("Failed to save streamed sha256 of entity %d: %s", session.EntityID, err)
	}

	e, err := fc.GetEntityByID(ctx, session.EntityID)
	if err != nil {
		m.l.Warning("Failed to load entity %d for hashing: %s", session.EntityID, err)
		return
	}
	if _, err := m.QueueEntityHash(ctx, []*ent.Entity{e}); err != nil {
		m.l.Warning("Failed to queue hash task for entity %d: %s", session.EntityID, err)
	}
}

// localFileSizeIs 核对本机策略落盘文件的大小。本机驱动写入时不截断已有文件（分片可能乱序到达），
// 覆盖同一物理路径上更长的旧文件时会留下旧内容的尾部，这时流式结果与实际内容不符，必须放弃。
// 路径解析与本机驱动 Put 保持一致。
func localFileSizeIs(savePath string, size int64) bool {
	info, err := os.Stat(util.RelativePath(filepath.FromSlash(savePath)))
	return err == nil && info.Mode().IsRegular() && info.Size() == size
}
