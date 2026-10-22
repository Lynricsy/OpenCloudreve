package routers

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/auth"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/stretchr/testify/require"
)

// 移动端 client 申请的全部写 scope（不含 offline_access）
var mobileWriteScopes = []string{
	types.ScopeOpenID,
	types.ScopeUserInfoWrite, types.ScopeUserSecurityInfoWrite,
	types.ScopeWorkflowWrite, types.ScopeFilesWrite, types.ScopeSharesWrite,
	types.ScopeDavAccountWrite, types.ScopeAdminWrite,
}

func checkHandoffScopes(tokenScopes []string) error {
	ctx := context.WithValue(context.Background(), auth.ScopeContextKey{}, tokenScopes)
	return auth.CheckScope(ctx, handoffCreateScopes...)
}

// 交接兑换得到可刷新的完整会话：没有 offline_access 的 OAuth 令牌不能创建交接码
func TestHandoffCreateRequiresOfflineAccess(t *testing.T) {
	err := checkHandoffScopes(mobileWriteScopes)
	var appErr serializer.AppError
	require.True(t, errors.As(err, &appErr), "缺 offline_access 必须被拒绝，实际 %v", err)
	require.Equal(t, serializer.CodeInsufficientScope, appErr.ErrCode())

	withOffline := append(append([]string{}, mobileWriteScopes...), types.ScopeOfflineAccess)
	require.NoError(t, checkHandoffScopes(withOffline), "移动端完整 scope 仍可创建")

	// 未受 scope 限制的会话（密码登录/网页会话）不经 scope 检查
	require.NoError(t, auth.CheckScope(context.Background(), handoffCreateScopes...))
}
