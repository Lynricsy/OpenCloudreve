package inventory

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent/enttest"
	"github.com/cloudreve/Cloudreve/v4/ent/oauthclient"
	"github.com/cloudreve/Cloudreve/v4/ent/oauthgrant"
	"github.com/cloudreve/Cloudreve/v4/ent/schema"
	"github.com/cloudreve/Cloudreve/v4/pkg/boolset"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/stretchr/testify/require"
)

// 撤销授权后重新授权，刷新令牌依赖的授权记录必须可见（issue #1）。
func TestOAuthGrantRevokeThenReconsent(t *testing.T) {
	ctx := context.Background()
	client := enttest.Open(t, "sqlite3", filepath.Join(t.TempDir(), "grant.db")+"?_fk=1")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NoError(t, migrateOAuthClient(logging.NewConsoleLogger(logging.LevelError), client, ctx))

	group := client.Group.Create().SetName("g").SetPermissions(&boolset.BooleanSet{}).SaveX(ctx)
	u := client.User.Create().SetEmail("a@example.com").SetNick("a").SetGroupUsers(group.ID).SaveX(ctx)
	app := client.OAuthClient.Query().Where(oauthclient.GUID(OAuthClientDesktopGUID)).OnlyX(ctx)
	c := NewOAuthClientClient(client, "sqlite")
	scopes := []string{"openid", "offline_access"}

	grantVisible := func() bool {
		got, err := c.GetByGUIDWithGrants(ctx, OAuthClientDesktopGUID, u.ID)
		require.NoError(t, err)
		return len(got.Edges.Grants) == 1
	}

	require.NoError(t, c.UpsertGrant(ctx, u.ID, app.ID, scopes))
	require.True(t, grantVisible())

	deleted, err := c.DeleteGrantByUserAndClientGUID(ctx, u.ID, OAuthClientDesktopGUID)
	require.NoError(t, err)
	require.True(t, deleted)
	require.False(t, grantVisible())
	// 撤销是物理删除，不留占用唯一索引的行
	require.Zero(t, client.OAuthGrant.Query().CountX(schema.SkipSoftDelete(ctx)))

	require.NoError(t, c.UpsertGrant(ctx, u.ID, app.ID, scopes))
	require.True(t, grantVisible())

	// 旧版本留下的软删除行：重新授权后授权必须重新可见
	client.OAuthGrant.Update().Where(oauthgrant.UserID(u.ID)).SetDeletedAt(app.CreatedAt).ExecX(ctx)
	require.False(t, grantVisible())
	require.NoError(t, c.UpsertGrant(ctx, u.ID, app.ID, scopes))
	require.True(t, grantVisible())

	// 旧版本留下的软删除行：再次撤销能清理掉它，而不是返回 404
	client.OAuthGrant.Update().Where(oauthgrant.UserID(u.ID)).SetDeletedAt(app.CreatedAt).ExecX(ctx)
	deleted, err = c.DeleteGrantByUserAndClientGUID(ctx, u.ID, OAuthClientDesktopGUID)
	require.NoError(t, err)
	require.True(t, deleted)
	require.Zero(t, client.OAuthGrant.Query().CountX(schema.SkipSoftDelete(ctx)))
}
