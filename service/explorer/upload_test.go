package explorer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/pkg/cache"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/stretchr/testify/require"
)

type uploadTestDep struct {
	dependency.Dep
	kv cache.Driver
}

func (d uploadTestDep) KV() cache.Driver { return d.kv }

func uploadTestContext(kv cache.Driver, uid int) context.Context {
	ctx := context.WithValue(context.Background(), dependency.DepCtx{}, uploadTestDep{kv: kv})
	return context.WithValue(ctx, inventory.UserCtx{}, &ent.User{ID: uid})
}

func newUploadTestKV() cache.Driver {
	return cache.NewMemoStore("", logging.NewConsoleLogger(logging.LevelError))
}

func requireAppErrCode(t *testing.T, err error, code int) {
	t.Helper()
	var appErr serializer.AppError
	require.True(t, errors.As(err, &appErr), "期望错误码 %d，实际 %v", code, err)
	require.Equal(t, code, appErr.ErrCode())
}

// 会话仍在 → uploading；完成回执 → completed（含 URI）；其他用户的会话/回执、未知会话 → 40011
func TestUploadSessionStatus(t *testing.T) {
	kv := newUploadTestKV()
	uri, err := fs.NewUriFromString("cloudreve://my/a.bin")
	require.NoError(t, err)
	session := fs.UploadSession{UID: 1, Props: &fs.UploadProps{UploadSessionID: "s-up", Uri: uri}}
	require.NoError(t, kv.Set(manager.UploadSessionCachePrefix+"s-up", session, 60))
	require.NoError(t, kv.Set(manager.UploadReceiptCachePrefix+"s-done", `{"uid":1,"uri":"cloudreve://my/b.bin"}`, 60))

	get := func(uid int, id string) (*UploadSessionStatusResponse, error) {
		return (&UploadSessionStatusService{ID: id}).Get(uploadTestContext(kv, uid))
	}

	res, err := get(1, "s-up")
	require.NoError(t, err)
	require.Equal(t, &UploadSessionStatusResponse{Status: UploadSessionStatusUploading, Uri: "cloudreve://my/a.bin"}, res)

	res, err = get(1, "s-done")
	require.NoError(t, err)
	require.Equal(t, &UploadSessionStatusResponse{Status: UploadSessionStatusCompleted, Uri: "cloudreve://my/b.bin"}, res)

	_, err = get(2, "s-up")
	requireAppErrCode(t, err, serializer.CodeUploadSessionExpired)
	_, err = get(2, "s-done")
	requireAppErrCode(t, err, serializer.CodeUploadSessionExpired)
	_, err = get(1, "s-unknown")
	requireAppErrCode(t, err, serializer.CodeUploadSessionExpired)
}

// 同一用户同一 request_id 的重放返回第一次创建的会话，不再创建；并发重放也只创建一次
func TestCreateUploadSessionOnceReplaysSameRequest(t *testing.T) {
	kv := newUploadTestKV()
	var created atomic.Int32
	create := func() (*UploadSessionResponse, error) {
		n := created.Add(1)
		time.Sleep(20 * time.Millisecond)
		return &UploadSessionResponse{SessionID: "s" + string(rune('0'+n)), Expires: time.Now().Add(time.Hour).Unix(), CallbackSecret: "sec"}, nil
	}

	var wg sync.WaitGroup
	results := make([]*UploadSessionResponse, 4)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := createUploadSessionOnce(kv, 1, "req-1", create)
			require.NoError(t, err)
			results[i] = res
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), created.Load())
	for _, r := range results {
		require.Equal(t, "s1", r.SessionID)
		require.Equal(t, "sec", r.CallbackSecret)
	}

	// 其他用户的同名 request_id、空 request_id 都各自创建
	res, err := createUploadSessionOnce(kv, 2, "req-1", create)
	require.NoError(t, err)
	require.Equal(t, "s2", res.SessionID)
	res, err = createUploadSessionOnce(kv, 1, "", create)
	require.NoError(t, err)
	require.Equal(t, "s3", res.SessionID)
}

// 创建失败不留下记录，同一 request_id 再次请求时重新创建
func TestCreateUploadSessionOnceDoesNotRememberFailure(t *testing.T) {
	kv := newUploadTestKV()
	calls := 0
	_, err := createUploadSessionOnce(kv, 1, "req-1", func() (*UploadSessionResponse, error) {
		calls++
		return nil, serializer.NewError(serializer.CodeObjectExist, "", nil)
	})
	requireAppErrCode(t, err, serializer.CodeObjectExist)
	res, err := createUploadSessionOnce(kv, 1, "req-1", func() (*UploadSessionResponse, error) {
		calls++
		return &UploadSessionResponse{SessionID: "s-ok", Expires: time.Now().Add(time.Hour).Unix()}, nil
	})
	require.NoError(t, err)
	require.Equal(t, "s-ok", res.SessionID)
	require.Equal(t, 2, calls)
}
