package manager

import (
	"context"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
)

func (m *manager) MountPolicy(ctx context.Context, uri *fs.URI, policyID int) (*ent.StoragePolicy, error) {
	return m.fs.MountPolicy(ctx, uri, policyID)
}
