package backup

import (
	"context"
	"errors"
	"fmt"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
)

const settingRootID = "gdrive_backup_root_id"

// ErrNotAuthorized 尚未登录 Google 账号。
var ErrNotAuthorized = errors.New("google drive account is not connected")

// Env 主机侧一次备份/恢复操作所需的全部对象。
type Env struct {
	Config  *setting.GDriveBackup
	Keyring *Keyring
	Drive   *DriveClient
	Store   *Store
}

// KeyringFromSetting 按设置返回加密密钥；未开启加密返回 nil。
func KeyringFromSetting(cfg *setting.GDriveBackup) (*Keyring, error) {
	if !cfg.Encrypt {
		return nil, nil
	}
	if cfg.Key == "" {
		return nil, errors.New("backup encryption is enabled but no key is configured")
	}
	return ParseKey(cfg.Key)
}

// Open 按当前设置连接 Drive 并打开备份仓库；根目录变化时写回设置。
func Open(ctx context.Context, dep dependency.Dep) (*Env, error) {
	cfg := dep.SettingProvider().GDriveBackup(ctx)
	if cfg.RefreshToken == "" {
		return nil, ErrNotAuthorized
	}
	kr, err := KeyringFromSetting(cfg)
	if err != nil {
		return nil, err
	}
	dc, err := NewDriveClientFromDep(dep, cfg.Proxy)
	if err != nil {
		return nil, err
	}
	root, err := EnsureRoot(ctx, dc, cfg.RootID)
	if err != nil {
		return nil, err
	}
	if root != cfg.RootID {
		if err := dep.SettingClient().Set(ctx, map[string]string{settingRootID: root}); err != nil {
			return nil, fmt.Errorf("failed to save backup root folder: %w", err)
		}
		_ = dep.KV().Delete(setting.KvSettingPrefix, settingRootID)
		cfg.RootID = root
	}
	store, err := OpenStore(ctx, dc, root)
	if err != nil {
		return nil, err
	}
	return &Env{Config: cfg, Keyring: kr, Drive: dc, Store: store}, nil
}
