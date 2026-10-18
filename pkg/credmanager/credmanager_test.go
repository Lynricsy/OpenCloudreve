package credmanager

import (
	"context"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/pkg/cache"
)

// testCred 刷新后的令牌为原令牌追加 "+"，并记录刷新次数。
type testCred struct {
	Token     string
	ExpiresAt time.Time
	refreshes *int
}

func (c testCred) String() string          { return c.Token }
func (c testCred) Key() string             { return "k" }
func (c testCred) Expiry() time.Time       { return c.ExpiresAt }
func (c testCred) RefreshedAt() *time.Time { return nil }
func (c testCred) Refresh(context.Context) (Credential, error) {
	*c.refreshes++
	return testCred{Token: c.Token + "+", ExpiresAt: time.Now().Add(time.Hour), refreshes: c.refreshes}, nil
}

func TestForceRefreshReplacesRejectedTokenOnce(t *testing.T) {
	ctx := context.Background()
	refreshes := 0
	m := New(cache.NewMemoStore("", nil))
	if err := m.Upsert(ctx, testCred{Token: "old", ExpiresAt: time.Now().Add(time.Hour), refreshes: &refreshes}); err != nil {
		t.Fatal(err)
	}

	// 未到本地过期时间，Obtain 仍返回旧令牌
	if c, _ := m.Obtain(ctx, "k"); c.String() != "old" || refreshes != 0 {
		t.Fatalf("Obtain() = %v (refreshes %d), want cached old token", c, refreshes)
	}
	c, err := m.ForceRefresh(ctx, "k", TokenDigest("old"))
	if err != nil || c.String() != "old+" || refreshes != 1 {
		t.Fatalf("ForceRefresh() = %v, %v (refreshes %d); want old+ after one refresh", c, err, refreshes)
	}
	if c, _ := m.Obtain(ctx, "k"); c.String() != "old+" {
		t.Fatalf("Obtain() after ForceRefresh = %v, want old+", c)
	}
	// 另一调用方也因同一旧令牌失败：已刷新过，不重复刷新
	if c, err := m.ForceRefresh(ctx, "k", TokenDigest("old")); err != nil || c.String() != "old+" || refreshes != 1 {
		t.Fatalf("second ForceRefresh() = %v, %v (refreshes %d); want cached old+ without refresh", c, err, refreshes)
	}
}

func TestForceRefreshMissingCredential(t *testing.T) {
	m := New(cache.NewMemoStore("", nil))
	if _, err := m.ForceRefresh(context.Background(), "k", TokenDigest("x")); err == nil {
		t.Fatal("ForceRefresh() on missing credential succeeded")
	}
}
