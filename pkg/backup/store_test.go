package backup

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func openTestStore(t *testing.T) (*fakeDrive, *Store) {
	t.Helper()
	fd, dc := newFakeDrive(t)
	ctx := context.Background()
	root, err := EnsureRoot(ctx, dc, "")
	if err != nil {
		t.Fatalf("EnsureRoot: %v", err)
	}
	s, err := OpenStore(ctx, dc, root)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return fd, s
}

func TestUploadResumesAfterServerErrorsAndPartialAcks(t *testing.T) {
	fd, dc := newFakeDrive(t)
	// 跨 3 个分块，且最后一块不满
	data := randomBytes(t, 2*uploadChunkSize+12345)
	fd.failChunkPuts = 2
	fd.partialAcks = 1

	f, err := dc.Upload(context.Background(), "x", "p", nil, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	got := fd.files[f.ID].data
	if !bytes.Equal(got, data) {
		t.Fatalf("Upload() stored %d bytes, want %d identical bytes", len(got), len(data))
	}
	if fd.failChunkPuts != 0 || fd.partialAcks != 0 {
		t.Fatalf("injected faults not consumed: fail=%d partial=%d", fd.failChunkPuts, fd.partialAcks)
	}
}

func TestUploadExactChunkMultiple(t *testing.T) {
	fd, dc := newFakeDrive(t)
	// 长度恰好是分块整数倍时，最后需要一个空的结束请求
	data := randomBytes(t, 2*uploadChunkSize)
	f, err := dc.Upload(context.Background(), "x", "p", nil, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if !bytes.Equal(fd.files[f.ID].data, data) {
		t.Fatalf("Upload() stored content differs")
	}
}

func TestBlobRoundTrip(t *testing.T) {
	key, _, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	kr, err := ParseKey(key)
	if err != nil {
		t.Fatal(err)
	}

	for name, keyring := range map[string]*Keyring{"plain": nil, "encrypted": kr} {
		t.Run(name, func(t *testing.T) {
			fd, s := openTestStore(t)
			ctx := context.Background()
			data := randomBytes(t, uploadChunkSize+777)
			sum := sha256.Sum256(data)

			f, err := s.UploadBlob(ctx, keyring, sum[:], bytes.NewReader(data))
			if err != nil {
				t.Fatalf("UploadBlob() error = %v", err)
			}
			if f.Name != keyring.BlobName(sum[:]) {
				t.Fatalf("blob name = %q, want %q", f.Name, keyring.BlobName(sum[:]))
			}
			stored := fd.files[f.ID].data
			if keyring != nil && bytes.Contains(stored, data[:64]) {
				t.Fatalf("encrypted blob contains plaintext")
			}

			found, err := s.FindBlob(ctx, f.Name)
			if err != nil || found == nil || found.ID != f.ID {
				t.Fatalf("FindBlob() = %v, %v; want %s", found, err, f.ID)
			}

			rc, err := s.OpenBlob(ctx, keyring, f.ID)
			if err != nil {
				t.Fatalf("OpenBlob() error = %v", err)
			}
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("OpenBlob() content differs (err %v)", err)
			}
		})
	}
}

func TestUploadBlobAbortsWhenContentDoesNotMatchHash(t *testing.T) {
	fd, s := openTestStore(t)
	data := randomBytes(t, 1000)
	wrong := sha256.Sum256([]byte("other"))

	_, err := s.UploadBlob(context.Background(), nil, wrong[:], bytes.NewReader(data))
	if err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
		t.Fatalf("UploadBlob() error = %v, want the source sha256 mismatch", err)
	}
	if len(fd.canceled) != 1 {
		t.Fatalf("canceled sessions = %v, want the aborted session canceled", fd.canceled)
	}
	blobs, err := s.ListBlobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 0 {
		t.Fatalf("aborted upload left %d blobs", len(blobs))
	}
}

func TestBlobNameIsKeySpecific(t *testing.T) {
	sum := sha256.Sum256([]byte("hello"))
	k1, _, _ := GenerateKey()
	k2, _, _ := GenerateKey()
	kr1, _ := ParseKey(k1)
	kr1Again, _ := ParseKey(k1)
	kr2, _ := ParseKey(k2)

	if got := (*Keyring)(nil).BlobName(sum[:]); got != hex.EncodeToString(sum[:]) {
		t.Fatalf("plain blob name = %q, want sha256", got)
	}
	if kr1.BlobName(sum[:]) == hex.EncodeToString(sum[:]) {
		t.Fatal("encrypted blob name leaks plaintext sha256")
	}
	if kr1.BlobName(sum[:]) != kr1Again.BlobName(sum[:]) {
		t.Fatal("blob name is not deterministic for the same key")
	}
	if kr1.BlobName(sum[:]) == kr2.BlobName(sum[:]) {
		t.Fatal("different keys produce the same blob name")
	}
}

func TestManifestRoundTripAndWrongKey(t *testing.T) {
	_, s := openTestStore(t)
	ctx := context.Background()
	key, _, _ := GenerateKey()
	kr, _ := ParseKey(key)
	other, _, _ := GenerateKey()
	krOther, _ := ParseKey(other)

	created := time.Unix(1700000000, 0)
	entries := []ManifestEntry{
		{FileID: 1, OwnerID: 2, Owner: "a@b.c", Path: "/a.txt", Size: 3, Sha256: "x", Blob: "y", EntityID: 9, EntityCreated: 1700000000123456},
		{FileID: 5, OwnerID: 2, Path: "/dir/b.txt"},
	}
	var buf bytes.Buffer
	header, _ := EncodeLine(NewManifestHeader(created, kr, []int{1}, "site-a"))
	buf.Write(header)
	for i := range entries {
		line, _ := EncodeLine(&entries[i])
		buf.Write(line)
	}
	p := filepath.Join(t.TempDir(), "m.jsonl")
	if err := os.WriteFile(p, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := s.UploadManifest(ctx, kr, p, created, 2, 3); err != nil {
		t.Fatalf("UploadManifest() error = %v", err)
	}
	snaps, err := s.ListSnapshots(ctx)
	if err != nil || len(snaps) != 1 {
		t.Fatalf("ListSnapshots() = %v, %v", snaps, err)
	}
	snap := snaps[0]
	if snap.KeyID != kr.ID() || snap.Files != 2 || snap.Size != 3 || !snap.CreatedAt.Equal(created) {
		t.Fatalf("snapshot metadata = %+v", snap)
	}

	m, err := s.ReadManifest(ctx, kr, snap.ID)
	if err != nil {
		t.Fatalf("ReadManifest() error = %v", err)
	}
	if len(m.Entries) != 2 || m.Entries[0] != entries[0] || m.Entries[1] != entries[1] || m.Header.KeyID != kr.ID() || m.Header.SiteID != "site-a" {
		t.Fatalf("ReadManifest() = %+v", m)
	}
	if _, err := s.ReadManifest(ctx, krOther, snap.ID); err == nil {
		t.Fatal("ReadManifest() with another key succeeded")
	}
}
