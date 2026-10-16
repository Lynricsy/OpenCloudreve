package backup

import (
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/pkg/credmanager"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
)

// CredentialKey Drive access token 在 CredManager 中的键。从机通过主机的
// /slave/credential/:id 接口用同一个键拉取令牌，因此从机不需要刷新令牌。
const CredentialKey = "cred_backup_gdrive"

// Credential Drive access token。刷新令牌保存在设置 gdrive_backup_refresh_token 中，Refresh 时读取。
type Credential struct {
	AccessToken     string `json:"access_token"`
	ExpiresAtUnix   int64  `json:"expires_at"`
	RefreshedAtUnix int64  `json:"refreshed_at"`
}

func init() {
	gob.Register(Credential{})
}

// Refresh 用刷新令牌换取新的 access token（仅主机调用）。
func (c Credential) Refresh(ctx context.Context) (credmanager.Credential, error) {
	dep := dependency.FromContext(ctx)
	cfg := dep.SettingProvider().GDriveBackup(ctx)
	hc, err := NewHTTPClient(cfg.Proxy)
	if err != nil {
		return nil, err
	}
	token, expiry, err := appFromSetting(cfg).Refresh(ctx, hc, cfg.RefreshToken)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh google drive token: %w", err)
	}
	return Credential{AccessToken: token, ExpiresAtUnix: expiry.Unix(), RefreshedAtUnix: time.Now().Unix()}, nil
}

func (c Credential) Key() string {
	return CredentialKey
}

func (c Credential) Expiry() time.Time {
	return time.Unix(c.ExpiresAtUnix, 0)
}

func (c Credential) String() string {
	return c.AccessToken
}

func (c Credential) RefreshedAt() *time.Time {
	if c.RefreshedAtUnix == 0 {
		return nil
	}
	t := time.Unix(c.RefreshedAtUnix, 0)
	return &t
}

// ResetCredential 让 CredManager 丢弃缓存的 access token，下次使用时按当前设置重新刷新。
// 主机启动、授权完成和断开账号时调用。
func ResetCredential(ctx context.Context, cm credmanager.CredManager) error {
	return cm.Upsert(ctx, Credential{})
}

func appFromSetting(cfg *setting.GDriveBackup) OAuthApp {
	return OAuthApp{ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret}
}

// credTokenSource 从 CredManager 取 access token：主机按需刷新，从机向主机拉取。
type credTokenSource struct {
	cm credmanager.CredManager
}

func (s credTokenSource) Token(ctx context.Context) (string, error) {
	cred, err := s.cm.Obtain(ctx, CredentialKey)
	if err != nil {
		return "", err
	}
	return cred.String(), nil
}

// NewDriveClientFromDep 用当前节点的 CredManager 创建 Drive 客户端。
func NewDriveClientFromDep(dep dependency.Dep, proxy string) (*DriveClient, error) {
	hc, err := NewHTTPClient(proxy)
	if err != nil {
		return nil, err
	}
	return NewDriveClient(hc, credTokenSource{cm: dep.CredManager()}), nil
}
