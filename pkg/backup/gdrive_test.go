package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestDriveRefreshesTokenOnceAfter401(t *testing.T) {
	fd, base := newFakeDrive(t)
	fd.validToken = "new"
	ts := &fakeTokens{token: "old", refreshTo: "new"}
	dc := NewDriveClient(base.http, ts)

	if _, err := dc.About(context.Background()); err != nil {
		t.Fatalf("About() error = %v", err)
	}
	if fd.authRejects != 1 || len(ts.stale) != 1 || ts.stale[0] != "old" {
		t.Fatalf("authRejects = %d, refreshed stale tokens = %v; want 1 rejection and one refresh of \"old\"", fd.authRejects, ts.stale)
	}
	// 刷新后的令牌继续使用，不再重复刷新
	if _, err := dc.About(context.Background()); err != nil {
		t.Fatalf("second About() error = %v", err)
	}
	if fd.authRejects != 1 || len(ts.stale) != 1 {
		t.Fatalf("after second call authRejects = %d, refreshes = %d; want 1, 1", fd.authRejects, len(ts.stale))
	}
}

func TestDriveStopsAfterOneRefreshWhen401Persists(t *testing.T) {
	fd, base := newFakeDrive(t)
	fd.validToken = "never-issued"
	ts := &fakeTokens{token: "old", refreshTo: "still-rejected"}
	dc := NewDriveClient(base.http, ts)

	_, err := dc.About(context.Background())
	var de *DriveError
	if !errors.As(err, &de) || de.Status != http.StatusUnauthorized {
		t.Fatalf("About() error = %v, want DriveError 401", err)
	}
	if fd.authRejects != 2 || len(ts.stale) != 1 {
		t.Fatalf("authRejects = %d, refreshes = %d; want exactly 2 requests and 1 refresh", fd.authRejects, len(ts.stale))
	}
}

func TestDriveKeepsAuthErrorWhenRefreshFails(t *testing.T) {
	fd, base := newFakeDrive(t)
	fd.validToken = "new"
	ts := &fakeTokens{token: "old", refreshErr: fmt.Errorf("failed to refresh google drive token: %w", ErrDriveUnauthorized)}
	dc := NewDriveClient(base.http, ts)

	_, err := dc.About(context.Background())
	if !errors.Is(err, ErrDriveUnauthorized) {
		t.Fatalf("About() error = %v, want ErrDriveUnauthorized", err)
	}
	var de *DriveError
	if !errors.As(err, &de) || de.Status != http.StatusUnauthorized || de.Reason != "authError" {
		t.Fatalf("About() error = %v, want the original DriveError 401 authError", err)
	}
	if fd.authRejects != 1 {
		t.Fatalf("authRejects = %d, want 1 (no replay without a new token)", fd.authRejects)
	}
}

func TestUploadBlobKeepsDriveErrorWhenSessionInitFails(t *testing.T) {
	key, _, _ := GenerateKey()
	kr, _ := ParseKey(key)
	for name, keyring := range map[string]*Keyring{"plain": nil, "encrypted": kr} {
		t.Run(name, func(t *testing.T) {
			fd, s := openTestStore(t)
			fd.initErr = &DriveError{Status: http.StatusForbidden, Reason: "storageQuotaExceeded", Message: "quota"}
			data := randomBytes(t, 3*uploadChunkSize)
			sum := sha256.Sum256(data)

			_, err := s.UploadBlob(context.Background(), keyring, sum[:], bytes.NewReader(data))
			var de *DriveError
			if !errors.As(err, &de) || de.Status != http.StatusForbidden || de.Reason != "storageQuotaExceeded" {
				t.Fatalf("UploadBlob() error = %v, want DriveError 403 storageQuotaExceeded", err)
			}
		})
	}
}

// failPutTransport 让会话分块 PUT 在网络层失败，其余请求照常转发。
type failPutTransport struct{ next http.RoundTripper }

func (t failPutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPut {
		return nil, errors.New("connection reset by peer")
	}
	return t.next.RoundTrip(req)
}

func TestUploadErrorHidesSessionURIAndCancelsSession(t *testing.T) {
	fd, base := newFakeDrive(t)
	dc := NewDriveClient(&http.Client{Transport: failPutTransport{next: base.http.Transport}}, base.tokens)

	_, err := dc.Upload(context.Background(), "x", "p", nil, bytes.NewReader(randomBytes(t, 1000)))
	if err == nil {
		t.Fatal("Upload() succeeded although every chunk PUT failed")
	}
	if strings.Contains(err.Error(), "upload_id") {
		t.Fatalf("Upload() error leaks the session URI: %v", err)
	}
	if !strings.Contains(err.Error(), "connection reset by peer") {
		t.Fatalf("Upload() error = %v, want the network cause", err)
	}
	if len(fd.canceled) != 1 || len(fd.sessions) != 0 {
		t.Fatalf("canceled sessions = %v, remaining = %d; want the failed session canceled", fd.canceled, len(fd.sessions))
	}
}
