package backup

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestSlaveKeyRoundTripEncryptsWithoutPrivateKey(t *testing.T) {
	key, _, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	kr, err := ParseKey(key)
	if err != nil {
		t.Fatal(err)
	}

	sk := kr.SlaveKey()
	if strings.Contains(sk, key) || strings.Contains(strings.ToUpper(sk), "AGE-SECRET-KEY") {
		t.Fatalf("SlaveKey() contains the private key: %q", sk)
	}
	slave, err := ParseSlaveKey(sk)
	if err != nil {
		t.Fatalf("ParseSlaveKey() error = %v", err)
	}
	if slave.SlaveKey() != sk {
		t.Fatalf("SlaveKey() is not stable across round trip")
	}
	if slave.ID() != kr.ID() {
		t.Fatalf("ID() = %q, want %q", slave.ID(), kr.ID())
	}
	sum := sha256.Sum256([]byte("hello"))
	if slave.BlobName(sum[:]) != kr.BlobName(sum[:]) {
		t.Fatal("BlobName() differs from the original keyring")
	}

	// 从机加密的内容能被主机的完整密钥解密
	var buf bytes.Buffer
	w, err := slave.Encrypt(&buf)
	if err != nil {
		t.Fatalf("Encrypt() error = %v", err)
	}
	_, _ = w.Write([]byte("secret content"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := kr.Decrypt(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("master Decrypt() error = %v", err)
	}
	if got, _ := io.ReadAll(r); string(got) != "secret content" {
		t.Fatalf("decrypted = %q", got)
	}

	if _, err := slave.Decrypt(bytes.NewReader(buf.Bytes())); err == nil {
		t.Fatal("Decrypt() with a slave key succeeded")
	}
}

func TestParseSlaveKeyPlainAndInvalid(t *testing.T) {
	if (*Keyring)(nil).SlaveKey() != "" {
		t.Fatal("nil keyring SlaveKey() should be empty")
	}
	if kr, err := ParseSlaveKey(""); kr != nil || err != nil {
		t.Fatalf("ParseSlaveKey(\"\") = %v, %v; want nil, nil", kr, err)
	}
	key, _, _ := GenerateKey()
	for _, s := range []string{key, "ocr-slave-key-v1:age1abc", "ocr-slave-key-v1:age1abc:AAAA"} {
		if _, err := ParseSlaveKey(s); err == nil {
			t.Fatalf("ParseSlaveKey(%q) succeeded", s)
		}
	}
}

func TestKeyringForSnapshot(t *testing.T) {
	k1, _, _ := GenerateKey()
	k2, _, _ := GenerateKey()
	kr1, _ := ParseKey(k1)
	kr2, _ := ParseKey(k2)

	if got, err := KeyringForSnapshot(KeyIDPlain, kr1); got != nil || err != nil {
		t.Fatalf("plain snapshot = %v, %v; want nil, nil", got, err)
	}
	if got, err := KeyringForSnapshot(kr1.ID(), kr1); got != kr1 || err != nil {
		t.Fatalf("same key snapshot = %v, %v; want current keyring", got, err)
	}
	if _, err := KeyringForSnapshot(kr2.ID(), kr1); !errors.Is(err, ErrSnapshotForeignKey) {
		t.Fatalf("foreign key snapshot error = %v, want ErrSnapshotForeignKey", err)
	}
	if _, err := KeyringForSnapshot(kr2.ID(), nil); !errors.Is(err, ErrSnapshotForeignKey) {
		t.Fatalf("encrypted snapshot without key error = %v, want ErrSnapshotForeignKey", err)
	}
}
