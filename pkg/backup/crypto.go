package backup

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"filippo.io/age"
)

// KeyIDPlain 未加密备份的密钥标识。
const KeyIDPlain = "plain"

// slaveKeyPrefix SlaveKey 序列化格式的前缀：<前缀><公钥>:<base64url(nameKey)>。
const slaveKeyPrefix = "ocr-slave-key-v1:"

var (
	// ErrSnapshotForeignKey 快照用当前之外的密钥加密，无法读取。
	ErrSnapshotForeignKey = errors.New("snapshot was encrypted with another key")
	// errNoIdentity 只含公钥的 Keyring（从机）不能解密。
	errNoIdentity = errors.New("backup key has no private key, cannot decrypt")
)

// Keyring 备份加密密钥。nil 表示不加密。
//
// 密钥是一个 age X25519 私钥字符串（AGE-SECRET-KEY-1...），可直接保存到密码管理器；
// 备份文件是标准 age 格式，脱离 Cloudreve 也能用 `age -d -i key.txt` 解密。
type Keyring struct {
	identity  *age.X25519Identity
	recipient *age.X25519Recipient
	nameKey   []byte
}

// GenerateKey 生成新的备份密钥，返回私钥字符串与公钥。
func GenerateKey() (string, string, error) {
	id, err := age.GenerateX25519Identity()
	if err != nil {
		return "", "", err
	}
	return id.String(), id.Recipient().String(), nil
}

// ParseKey 解析备份密钥；空字符串返回错误。
func ParseKey(s string) (*Keyring, error) {
	id, err := age.ParseX25519Identity(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("invalid backup key: %w", err)
	}
	// 文件名密钥从私钥派生：加密备份的 blob 名称不暴露明文的 sha256。
	mac := hmac.New(sha256.New, []byte("opencloudreve-backup-blob-name-v1"))
	mac.Write([]byte(id.String()))
	return &Keyring{identity: id, recipient: id.Recipient(), nameKey: mac.Sum(nil)}, nil
}

// SlaveKey 序列化下发给从机的密钥：只含加密用的公钥和 blob 命名用的 nameKey，不含私钥。
// k 为 nil（不加密）时返回空串。
func (k *Keyring) SlaveKey() string {
	if k == nil {
		return ""
	}
	return slaveKeyPrefix + k.recipient.String() + ":" + base64.RawURLEncoding.EncodeToString(k.nameKey)
}

// ParseSlaveKey 解析 SlaveKey 的输出；空串表示不加密，返回 (nil, nil)。
// 得到的 Keyring 可以 ID/Encrypt/BlobName，Decrypt 会返回错误。
func ParseSlaveKey(s string) (*Keyring, error) {
	if s == "" {
		return nil, nil
	}
	rest, ok := strings.CutPrefix(s, slaveKeyPrefix)
	if !ok {
		return nil, errors.New("invalid slave backup key: unknown format")
	}
	pub, nk, ok := strings.Cut(rest, ":")
	if !ok {
		return nil, errors.New("invalid slave backup key: missing name key")
	}
	recipient, err := age.ParseX25519Recipient(pub)
	if err != nil {
		return nil, fmt.Errorf("invalid slave backup key: %w", err)
	}
	nameKey, err := base64.RawURLEncoding.DecodeString(nk)
	if err != nil || len(nameKey) != sha256.Size {
		return nil, errors.New("invalid slave backup key: bad name key")
	}
	return &Keyring{recipient: recipient, nameKey: nameKey}, nil
}

// KeyringForSnapshot 返回读取某份快照（清单和它引用的 blob）应使用的 Keyring：
// 明文快照返回 nil，与当前密钥一致时返回 kr，否则返回 ErrSnapshotForeignKey。
func KeyringForSnapshot(keyID string, kr *Keyring) (*Keyring, error) {
	if keyID == KeyIDPlain {
		return nil, nil
	}
	if kr != nil && kr.ID() == keyID {
		return kr, nil
	}
	return nil, ErrSnapshotForeignKey
}

// ID 密钥标识（公钥），未加密为 "plain"。写入 Drive 文件属性，用于判断快照能否用当前密钥解密。
func (k *Keyring) ID() string {
	if k == nil {
		return KeyIDPlain
	}
	return k.recipient.String()
}

// BlobName 由明文 sha256 计算 Drive 上的 blob 名称。
func (k *Keyring) BlobName(sum []byte) string {
	if k == nil {
		return hex.EncodeToString(sum)
	}
	mac := hmac.New(sha256.New, k.nameKey)
	mac.Write(sum)
	return hex.EncodeToString(mac.Sum(nil))
}

// Encrypt 返回加密写入 w 的 WriteCloser；未加密时原样写入。必须 Close 才会写出最后一块。
func (k *Keyring) Encrypt(w io.Writer) (io.WriteCloser, error) {
	if k == nil {
		return nopWriteCloser{w}, nil
	}
	return age.Encrypt(w, k.recipient)
}

// Decrypt 返回解密后的 Reader；未加密时原样返回。只含公钥的 Keyring（ParseSlaveKey）返回错误。
func (k *Keyring) Decrypt(r io.Reader) (io.Reader, error) {
	if k == nil {
		return r, nil
	}
	if k.identity == nil {
		return nil, errNoIdentity
	}
	return age.Decrypt(r, k.identity)
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
