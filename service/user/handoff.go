package user

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"sync"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/user"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/gin-gonic/gin"
)

const (
	// handoffKVPrefix 会话交接码的 KV 键前缀，值为用户 ID
	handoffKVPrefix = "user_handoff_"
	// handoffTTL 交接码有效期（秒）
	handoffTTL = 60
)

// handoffExchangeLock 保证同一交接码的“读取 + 删除”在进程内是原子的，交接码只能兑换一次。
// 与 uploadSessionLocks 相同，前提是主机只有一个进程。
var handoffExchangeLock sync.Mutex

// HandoffResponse 创建会话交接码的响应
type HandoffResponse struct {
	Code string `json:"code"`
}

// CreateHandoff 为当前登录用户生成一次性会话交接码，供原生客户端在网页视图中免登录打开站点。
func CreateHandoff(c *gin.Context) (*HandoffResponse, error) {
	dep := dependency.FromContext(c)
	u := inventory.UserFromContext(c)

	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return nil, serializer.NewError(serializer.CodeInternalSetting, "Failed to generate handoff code", err)
	}

	code := base64.RawURLEncoding.EncodeToString(buf)
	if err := dep.KV().Set(handoffKVPrefix+code, u.ID, handoffTTL); err != nil {
		return nil, serializer.NewError(serializer.CodeInternalSetting, "Failed to create handoff session", err)
	}

	return &HandoffResponse{Code: code}, nil
}

type (
	ExchangeHandoffParamCtx struct{}
	// ExchangeHandoffService 用交接码换取完整的登录会话
	ExchangeHandoffService struct {
		Code string `json:"code" binding:"required"`
	}
)

// Exchange 兑换交接码，返回对应的用户（含用户组），由 UserIssueToken 签发令牌。
func (s *ExchangeHandoffService) Exchange(c *gin.Context) (*ent.User, error) {
	dep := dependency.FromContext(c)
	kv := dep.KV()

	// binding:"required" 已保证 Code 非空；kv.Delete 不带 key 会删掉整个前缀，这里再防一次
	if s.Code == "" {
		return nil, serializer.NewError(serializer.CodeNotFound, "Session not found", nil)
	}

	handoffExchangeLock.Lock()
	sessionRaw, ok := kv.Get(handoffKVPrefix + s.Code)
	if ok {
		// 一次性：先删除再使用
		if err := kv.Delete(handoffKVPrefix, s.Code); err != nil {
			handoffExchangeLock.Unlock()
			return nil, serializer.NewError(serializer.CodeInternalSetting, "Failed to delete handoff session", err)
		}
	}
	handoffExchangeLock.Unlock()

	if !ok {
		return nil, serializer.NewError(serializer.CodeNotFound, "Session not found", nil)
	}

	uid, ok := sessionRaw.(int)
	if !ok {
		return nil, serializer.NewError(serializer.CodeNotFound, "Session not found", nil)
	}

	ctx := context.WithValue(c, inventory.LoadUserGroup{}, true)
	expectedUser, err := dep.UserClient().GetByID(ctx, uid)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeUserNotFound, "User not found", err)
	}

	// 与密码登录相同的状态校验
	if expectedUser.Status == user.StatusManualBanned || expectedUser.Status == user.StatusSysBanned {
		return nil, serializer.NewError(serializer.CodeUserBaned, "This account has been blocked", nil)
	}
	if expectedUser.Status == user.StatusInactive {
		return nil, serializer.NewError(serializer.CodeUserNotActivated, "This account is not activated", nil)
	}

	return expectedUser, nil
}
