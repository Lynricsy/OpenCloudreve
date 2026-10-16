package manager

import (
	"context"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
)

func (m *manager) PrepareRelocate(ctx context.Context, uri *fs.URI, dst *ent.StoragePolicy) (*fs.PrepareRelocateRes, error) {
	return m.fs.PrepareRelocate(ctx, uri, dst)
}

func (m *manager) RelocateEntity(ctx context.Context, e *fs.RelocateEntity, dstPolicyID int) error {
	return m.fs.RelocateEntity(ctx, e, dstPolicyID)
}
