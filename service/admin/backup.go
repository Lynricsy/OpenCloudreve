package admin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	enttask "github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster/routes"
	"github.com/cloudreve/Cloudreve/v4/pkg/crontab"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/workflows"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/gin-gonic/gin"
	"github.com/robfig/cron/v3"
)

const (
	backupOAuthStateKey  = "gdrive_backup_oauth_state"
	backupOAuthStateTTL  = 600
	settingRefreshToken  = "gdrive_backup_refresh_token"
	backupAccountTimeout = 15 * time.Second
)

type (
	BackupAccount struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}

	BackupQuota struct {
		Limit int64 `json:"limit"`
		Usage int64 `json:"usage"`
	}

	BackupTaskInfo struct {
		Status    string         `json:"status"`
		CreatedAt time.Time      `json:"created_at"`
		UpdatedAt time.Time      `json:"updated_at"`
		Error     string         `json:"error,omitempty"`
		Summary   map[string]any `json:"summary"`
	}

	BackupStatus struct {
		RedirectURI  string          `json:"redirect_uri"`
		Authorized   bool            `json:"authorized"`
		Account      *BackupAccount  `json:"account,omitempty"`
		Quota        *BackupQuota    `json:"quota,omitempty"`
		AccountError string          `json:"account_error,omitempty"`
		KeyID        string          `json:"key_id"`
		Running      bool            `json:"running"`
		LastTask     *BackupTaskInfo `json:"last_task,omitempty"`
	}

	BackupSnapshot struct {
		ID        string    `json:"id"`
		Name      string    `json:"name"`
		CreatedAt time.Time `json:"created_at"`
		Files     int64     `json:"files"`
		Size      int64     `json:"size"`
		KeyID     string    `json:"key_id"`
		Readable  bool      `json:"readable"`
	}

	BackupKey struct {
		Key   string `json:"key"`
		KeyID string `json:"key_id"`
	}
)

func backupRedirectURI(c *gin.Context) string {
	dep := dependency.FromContext(c)
	return routes.MasterBackupOAuthCallback(dep.SettingProvider().SiteURL(c)).String()
}

// GetBackupStatus 返回授权状态、账号空间与最近一次备份任务。
func GetBackupStatus(c *gin.Context) (*BackupStatus, error) {
	dep := dependency.FromContext(c)
	cfg := dep.SettingProvider().GDriveBackup(c)
	res := &BackupStatus{
		RedirectURI: backupRedirectURI(c),
		Authorized:  cfg.RefreshToken != "",
		KeyID:       backup.KeyIDPlain,
	}
	if kr, err := backup.KeyringFromSetting(cfg); err == nil {
		res.KeyID = kr.ID()
	}

	if res.Authorized {
		ctx, cancel := context.WithTimeout(c, backupAccountTimeout)
		defer cancel()
		about, err := func() (*backup.DriveAbout, error) {
			dc, err := backup.NewDriveClientFromDep(dep, cfg.Proxy)
			if err != nil {
				return nil, err
			}
			return dc.About(ctx)
		}()
		if err != nil {
			res.AccountError = err.Error()
		} else {
			res.Account = &BackupAccount{Email: about.User.EmailAddress, Name: about.User.DisplayName}
			res.Quota = &BackupQuota{}
			res.Quota.Limit, _ = strconv.ParseInt(about.StorageQuota.Limit, 10, 64)
			res.Quota.Usage, _ = strconv.ParseInt(about.StorageQuota.Usage, 10, 64)
		}
	}

	running, err := workflows.GDriveTaskPending(c, dep, queue.GDriveBackupTaskType)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to query backup tasks", err)
	}
	res.Running = running

	last, err := dep.DBClient().Task.Query().
		Where(enttask.Type(queue.GDriveBackupTaskType)).
		Order(ent.Desc(enttask.FieldID)).
		First(c)
	if err != nil && !ent.IsNotFound(err) {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to query backup tasks", err)
	}
	if last != nil {
		info := &BackupTaskInfo{Status: string(last.Status), CreatedAt: last.CreatedAt, UpdatedAt: last.UpdatedAt}
		if last.PublicState != nil {
			info.Error = last.PublicState.Error
		}
		if t, err := queue.NewTaskFromModel(last); err == nil {
			if s := t.Summarize(dep.HashIDEncoder()); s != nil {
				info.Summary = s.Props
			}
		}
		res.LastTask = info
	}
	return res, nil
}

// GetBackupOAuthURL 生成 Google 授权地址，state 存入 KV 防止 CSRF。
func GetBackupOAuthURL(c *gin.Context) (string, error) {
	dep := dependency.FromContext(c)
	cfg := dep.SettingProvider().GDriveBackup(c)
	if cfg.ClientID == "" || cfg.ClientSecret == "" {
		return "", serializer.NewError(serializer.CodeParamErr, "Please save the OAuth client ID and secret first", nil)
	}

	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", serializer.NewError(serializer.CodeInternalSetting, "Failed to generate state", err)
	}
	state := hex.EncodeToString(buf)
	if err := dep.KV().Set(backupOAuthStateKey, state, backupOAuthStateTTL); err != nil {
		return "", serializer.NewError(serializer.CodeCacheOperation, "Failed to save state", err)
	}

	app := backup.OAuthApp{ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret}
	return app.AuthURL(backupRedirectURI(c), state), nil
}

type (
	FinishBackupOAuthService struct {
		Code  string `json:"code" binding:"required"`
		State string `json:"state" binding:"required"`
	}
	FinishBackupOAuthParamCtx struct{}
)

// Finish 用授权码换取刷新令牌并保存，然后定位/创建 Drive 上的备份根目录。
func (s *FinishBackupOAuthService) Finish(c *gin.Context) error {
	dep := dependency.FromContext(c)
	kv := dep.KV()
	expected, ok := kv.Get(backupOAuthStateKey)
	if !ok || expected.(string) != s.State {
		return serializer.NewError(serializer.CodeParamErr, "Invalid or expired OAuth state, please sign in again", nil)
	}
	_ = kv.Delete("", backupOAuthStateKey)

	cfg := dep.SettingProvider().GDriveBackup(c)
	hc, err := backup.NewHTTPClient(cfg.Proxy)
	if err != nil {
		return serializer.NewError(serializer.CodeParamErr, "Invalid proxy", err)
	}
	app := backup.OAuthApp{ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret}
	refreshToken, err := app.ExchangeCode(c, hc, backupRedirectURI(c), s.Code)
	if err != nil {
		return serializer.NewError(serializer.CodeParamErr, "Failed to obtain token: "+err.Error(), err)
	}

	if err := saveBackupSettings(c, dep, map[string]string{settingRefreshToken: refreshToken}); err != nil {
		return err
	}
	if _, err := backup.Open(c, dep); err != nil {
		return serializer.NewError(serializer.CodeInternalSetting, "Signed in, but failed to prepare the backup folder: "+err.Error(), err)
	}
	return nil
}

// DisconnectBackup 清除刷新令牌。
func DisconnectBackup(c *gin.Context) error {
	return saveBackupSettings(c, dependency.FromContext(c), map[string]string{settingRefreshToken: ""})
}

// saveBackupSettings 写入设置、清理缓存并让 CredManager 按新设置刷新令牌。
func saveBackupSettings(ctx context.Context, dep dependency.Dep, settings map[string]string) error {
	if err := dep.SettingClient().Set(ctx, settings); err != nil {
		return serializer.NewError(serializer.CodeDBError, "Failed to save settings", err)
	}
	keys := make([]string, 0, len(settings))
	for k := range settings {
		keys = append(keys, k)
	}
	if err := dep.KV().Delete(setting.KvSettingPrefix, keys...); err != nil {
		return serializer.NewError(serializer.CodeCacheOperation, "Failed to clear cache", err)
	}
	if err := backup.ResetCredential(ctx, dep.CredManager()); err != nil {
		return serializer.NewError(serializer.CodeInternalSetting, "Failed to reset credential", err)
	}
	return nil
}

// GenerateBackupKey 生成新的 age 密钥（不保存）。
func GenerateBackupKey() (*BackupKey, error) {
	key, id, err := backup.GenerateKey()
	if err != nil {
		return nil, serializer.NewError(serializer.CodeInternalSetting, "Failed to generate key", err)
	}
	return &BackupKey{Key: key, KeyID: id}, nil
}

// RunBackup 立即执行一次备份。
func RunBackup(c *gin.Context) error {
	dep := dependency.FromContext(c)
	if dep.SettingProvider().GDriveBackup(c).RefreshToken == "" {
		return serializer.NewError(serializer.CodeParamErr, "Google Drive account is not connected", nil)
	}
	t, err := workflows.NewGDriveBackupTask(c, dep, workflows.GDriveBackupTriggerManual)
	if err != nil {
		if errors.Is(err, workflows.ErrGDriveBackupRunning) {
			return serializer.NewError(serializer.CodeConflict, "A backup task is already running", err)
		}
		return serializer.NewError(serializer.CodeDBError, "Failed to create backup task", err)
	}
	if err := dep.IoIntenseQueue(c).QueueTask(c, t); err != nil {
		return serializer.NewError(serializer.CodeInternalSetting, "Failed to queue backup task", err)
	}
	return nil
}

func openBackup(c *gin.Context) (*backup.Env, error) {
	env, err := backup.Open(c, dependency.FromContext(c))
	if err != nil {
		if errors.Is(err, backup.ErrNotAuthorized) {
			return nil, serializer.NewError(serializer.CodeParamErr, "Google Drive account is not connected", err)
		}
		return nil, serializer.NewError(serializer.CodeInternalSetting, "Failed to connect to Google Drive: "+err.Error(), err)
	}
	return env, nil
}

func snapshotReadable(s *backup.Snapshot, kr *backup.Keyring) bool {
	return s.KeyID == backup.KeyIDPlain || s.KeyID == kr.ID()
}

// ListBackupSnapshots 列出快照（新→旧）。
func ListBackupSnapshots(c *gin.Context) ([]BackupSnapshot, error) {
	env, err := openBackup(c)
	if err != nil {
		return nil, err
	}
	snaps, err := env.Store.ListSnapshots(c)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeInternalSetting, "Failed to list snapshots: "+err.Error(), err)
	}
	res := make([]BackupSnapshot, 0, len(snaps))
	for i := range snaps {
		s := &snaps[i]
		res = append(res, BackupSnapshot{
			ID: s.ID, Name: s.Name, CreatedAt: s.CreatedAt, Files: s.Files, Size: s.Size,
			KeyID: s.KeyID, Readable: snapshotReadable(s, env.Keyring),
		})
	}
	return res, nil
}

// manifestCache 缓存最近浏览的一份快照，逐层浏览时不必反复下载。
var manifestCache struct {
	sync.Mutex
	id string
	m  *backup.Manifest
}

func loadSnapshotManifest(c *gin.Context, env *backup.Env, id string) (*backup.Manifest, error) {
	manifestCache.Lock()
	defer manifestCache.Unlock()
	if manifestCache.id == id && manifestCache.m != nil {
		return manifestCache.m, nil
	}

	snaps, err := env.Store.ListSnapshots(c)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeInternalSetting, "Failed to list snapshots: "+err.Error(), err)
	}
	idx := slices.IndexFunc(snaps, func(s backup.Snapshot) bool { return s.ID == id })
	if idx < 0 {
		return nil, serializer.NewError(serializer.CodeNotFound, "Snapshot not found", nil)
	}
	if !snapshotReadable(&snaps[idx], env.Keyring) {
		return nil, serializer.NewError(serializer.CodeParamErr, "Snapshot was encrypted with another key: "+snaps[idx].KeyID, nil)
	}
	m, err := env.Store.ReadManifest(c, env.Keyring, id)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeInternalSetting, "Failed to read snapshot: "+err.Error(), err)
	}
	manifestCache.id, manifestCache.m = id, m
	return m, nil
}

type (
	BrowseBackupService struct {
		SnapshotID string `json:"snapshot_id" binding:"required"`
		UID        *int   `json:"uid"`
		Path       string `json:"path"`
	}
	BrowseBackupParamCtx struct{}

	BackupUser struct {
		UID   int    `json:"uid"`
		Owner string `json:"owner"`
		Files int64  `json:"files"`
		Size  int64  `json:"size"`
	}

	BackupEntry struct {
		Name       string     `json:"name"`
		Path       string     `json:"path"`
		IsDir      bool       `json:"is_dir"`
		Size       int64      `json:"size"`
		Files      int64      `json:"files"`
		ModifiedAt *time.Time `json:"modified_at,omitempty"`
	}

	BrowseBackupResult struct {
		Users   []BackupUser  `json:"users,omitempty"`
		Entries []BackupEntry `json:"entries,omitempty"`
	}
)

// Browse 不带 uid 时按用户汇总；带 uid 时列出该用户 path 下一层的文件与目录。
func (s *BrowseBackupService) Browse(c *gin.Context) (*BrowseBackupResult, error) {
	env, err := openBackup(c)
	if err != nil {
		return nil, err
	}
	m, err := loadSnapshotManifest(c, env, s.SnapshotID)
	if err != nil {
		return nil, err
	}

	if s.UID == nil {
		users := make(map[int]*BackupUser)
		for _, e := range m.Entries {
			u, ok := users[e.OwnerID]
			if !ok {
				u = &BackupUser{UID: e.OwnerID, Owner: e.Owner}
				users[e.OwnerID] = u
			}
			u.Files++
			u.Size += e.Size
		}
		res := &BrowseBackupResult{Users: make([]BackupUser, 0, len(users))}
		for _, u := range users {
			res.Users = append(res.Users, *u)
		}
		slices.SortFunc(res.Users, func(a, b BackupUser) int { return a.UID - b.UID })
		return res, nil
	}

	dir := path.Clean("/" + s.Path)
	prefix := dir
	if prefix != "/" {
		prefix += "/"
	}
	children := make(map[string]*BackupEntry)
	for _, e := range m.Entries {
		if e.OwnerID != *s.UID || !strings.HasPrefix(e.Path, prefix) {
			continue
		}
		rest := strings.TrimPrefix(e.Path, prefix)
		name, _, isDir := strings.Cut(rest, "/")
		child, ok := children[name]
		if !ok {
			child = &BackupEntry{Name: name, Path: prefix + name, IsDir: isDir}
			children[name] = child
		}
		child.Files++
		child.Size += e.Size
		if !isDir {
			t := e.ModifiedAt
			child.ModifiedAt = &t
		}
	}
	res := &BrowseBackupResult{Entries: make([]BackupEntry, 0, len(children))}
	for _, e := range children {
		res.Entries = append(res.Entries, *e)
	}
	slices.SortFunc(res.Entries, func(a, b BackupEntry) int {
		if a.IsDir != b.IsDir {
			if a.IsDir {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	return res, nil
}

type (
	RestoreBackupService struct {
		SnapshotID string                        `json:"snapshot_id" binding:"required"`
		Items      []workflows.GDriveRestoreItem `json:"items" binding:"required,min=1"`
		TargetUID  int                           `json:"target_uid"`
		TargetDir  string                        `json:"target_dir" binding:"required"`
	}
	RestoreBackupParamCtx struct{}
)

// Restore 创建恢复任务。
func (s *RestoreBackupService) Restore(c *gin.Context) error {
	dep := dependency.FromContext(c)
	if !strings.HasPrefix(s.TargetDir, "/") {
		return serializer.NewError(serializer.CodeParamErr, "Target folder must start with /", nil)
	}
	for i := range s.Items {
		s.Items[i].Path = path.Clean("/" + s.Items[i].Path)
	}
	if s.TargetUID > 0 {
		if _, err := dep.UserClient().GetByID(c, s.TargetUID); err != nil {
			return serializer.NewError(serializer.CodeNotFound, "Target user not found", err)
		}
	}

	t, err := workflows.NewGDriveRestoreTask(c, s.SnapshotID, s.Items, s.TargetUID, path.Clean(s.TargetDir))
	if err != nil {
		return serializer.NewError(serializer.CodeInternalSetting, "Failed to create restore task", err)
	}
	if err := dep.IoIntenseQueue(c).QueueTask(c, t); err != nil {
		return serializer.NewError(serializer.CodeInternalSetting, "Failed to queue restore task", err)
	}
	return nil
}

// gdriveBackupPreProcessor 校验备份设置。只提交了部分设置时，与已保存的值合并后再校验。
func gdriveBackupPreProcessor(ctx context.Context, settings map[string]string) error {
	cfg := dependency.FromContext(ctx).SettingProvider().GDriveBackup(ctx)
	value := func(key, current string) string {
		if v, ok := settings[key]; ok {
			return v
		}
		return current
	}

	encrypt := value("gdrive_backup_encrypt", map[bool]string{true: "1", false: "0"}[cfg.Encrypt])
	key := strings.TrimSpace(value("gdrive_backup_key", cfg.Key))
	if _, ok := settings["gdrive_backup_key"]; ok {
		settings["gdrive_backup_key"] = key
	}
	if key != "" {
		if _, err := backup.ParseKey(key); err != nil {
			return err
		}
	} else if encrypt == "1" {
		return errors.New("an encryption key is required when backup encryption is enabled")
	}

	if v, ok := settings["gdrive_backup_policies"]; ok {
		var ids []int
		if err := json.Unmarshal([]byte(v), &ids); err != nil {
			return fmt.Errorf("invalid storage policy list: %w", err)
		}
	}
	if v, ok := settings["gdrive_backup_retention_days"]; ok {
		if n, err := strconv.Atoi(v); err != nil || n < 1 {
			return fmt.Errorf("retention days must be a positive integer")
		}
	}
	if v, ok := settings["gdrive_backup_daily_limit"]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err != nil || n < 0 {
			return fmt.Errorf("daily upload limit must be a non-negative integer")
		}
	}
	if v, ok := settings["cron_gdrive_backup"]; ok {
		if _, err := cron.ParseStandard(v); err != nil {
			return fmt.Errorf("invalid cron expression %q: %w", v, err)
		}
	}
	if v, ok := settings["gdrive_backup_proxy"]; ok && v != "" {
		if u, err := url.Parse(v); err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("invalid proxy URL %q", v)
		}
	}
	return nil
}

// gdriveBackupPostProcessor 让新的执行时间、代理和 OAuth 客户端立即生效。
func gdriveBackupPostProcessor(ctx context.Context, settings map[string]string) error {
	dep := dependency.FromContext(ctx)
	if _, ok := settings["cron_gdrive_backup"]; ok {
		if err := crontab.Start(ctx, dep); err != nil {
			return err
		}
	}
	return backup.ResetCredential(ctx, dep.CredManager())
}
