package inventory

import (
	"context"
	"fmt"

	"entgo.io/ent/dialect/sql"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/entity"
	"github.com/cloudreve/Cloudreve/v4/ent/file"
	"github.com/cloudreve/Cloudreve/v4/ent/predicate"
	"github.com/cloudreve/Cloudreve/v4/ent/storagepolicy"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
)

// HashCoverage 统计用户“我的文件”中当前版本的哈希覆盖情况。
type HashCoverage struct {
	// Hashed 已有哈希的文件数
	Hashed int `json:"hashed"`
	// Computable 位于可计算策略上、尚无哈希的文件数
	Computable int `json:"computable"`
	// Unsupported 其余尚无哈希的文件数（其他策略或上传未完成）
	Unsupported int `json:"unsupported"`
}

// ErrEntityHashConflict 实体已有不同的哈希值。实体内容不可变，出现时说明存储内容已损坏。
var ErrEntityHashConflict = fmt.Errorf("entity already has a different sha256")

func (f *fileClient) SetEntitySha256(ctx context.Context, id int, sum string) error {
	affected, err := f.client.Entity.Update().
		Where(entity.ID(id), entity.Sha256IsNil()).
		SetSha256(sum).
		Save(ctx)
	if err != nil {
		return fmt.Errorf("failed to update entity sha256: %w", err)
	}
	if affected > 0 {
		return nil
	}

	e, err := f.client.Entity.Query().Where(entity.ID(id)).Only(ctx)
	if err != nil {
		return fmt.Errorf("failed to load entity %d: %w", id, err)
	}
	if e.Sha256 != nil && *e.Sha256 != sum {
		return fmt.Errorf("%w: stored %s, computed %s", ErrEntityHashConflict, *e.Sha256, sum)
	}
	return nil
}

// ownFilePredicates 限定用户“我的文件”下的普通文件：回收站中的文件没有父目录，分享给我的是符号链接。
func ownFilePredicates(ownerID int) []predicate.File {
	return []predicate.File{
		file.OwnerID(ownerID),
		file.TypeEQ(int(types.FileTypeFile)),
		file.FileChildrenNotNil(),
		file.IsSymbolic(false),
	}
}

// primaryEntityWhere 限定主实体满足给定条件的文件。
func primaryEntityWhere(cond func(t *sql.SelectTable) *sql.Predicate) predicate.File {
	return func(s *sql.Selector) {
		t := sql.Table(entity.Table)
		sub := sql.Select(t.C(entity.FieldID)).From(t).Where(sql.And(
			sql.IsNull(t.C(entity.FieldDeletedAt)),
			sql.EQ(t.C(entity.FieldType), int(types.EntityTypeVersion)),
			cond(t),
		))
		s.Where(sql.In(s.C(file.FieldPrimaryEntity), sub))
	}
}

func (f *fileClient) FindOwnFilesBySha256(ctx context.Context, ownerID int, sums []string, limit int) ([]*ent.File, map[int]string, error) {
	if len(sums) == 0 {
		return nil, map[int]string{}, nil
	}

	args := make([]any, len(sums))
	for i, s := range sums {
		args[i] = s
	}

	files, err := f.client.File.Query().
		Where(ownFilePredicates(ownerID)...).
		Where(primaryEntityWhere(func(t *sql.SelectTable) *sql.Predicate {
			return sql.In(t.C(entity.FieldSha256), args...)
		})).
		Order(file.ByID()).
		Limit(limit).
		All(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query files by sha256: %w", err)
	}

	entityIDs := make([]int, 0, len(files))
	for _, fi := range files {
		entityIDs = append(entityIDs, fi.PrimaryEntity)
	}

	entities, err := f.client.Entity.Query().
		Where(entity.IDIn(entityIDs...)).
		Select(entity.FieldID, entity.FieldSha256).
		All(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query entity sha256: %w", err)
	}

	hashes := make(map[int]string, len(entities))
	for _, e := range entities {
		if e.Sha256 != nil {
			hashes[e.ID] = *e.Sha256
		}
	}

	return files, hashes, nil
}

func (f *fileClient) CountOwnFileHashCoverage(ctx context.Context, ownerID int, computablePolicyTypes []string) (*HashCoverage, error) {
	base := ownFilePredicates(ownerID)
	total, err := f.client.File.Query().Where(base...).Count(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to count files: %w", err)
	}

	hashed, err := f.client.File.Query().Where(base...).
		Where(primaryEntityWhere(func(t *sql.SelectTable) *sql.Predicate {
			return sql.NotNull(t.C(entity.FieldSha256))
		})).
		Count(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to count hashed files: %w", err)
	}

	policyTypes := make([]any, len(computablePolicyTypes))
	for i, s := range computablePolicyTypes {
		policyTypes[i] = s
	}
	computable, err := f.client.File.Query().Where(base...).
		Where(primaryEntityWhere(func(t *sql.SelectTable) *sql.Predicate {
			p := sql.Table(storagepolicy.Table)
			return sql.And(
				sql.IsNull(t.C(entity.FieldSha256)),
				sql.IsNull(t.C(entity.FieldUploadSessionID)),
				sql.In(t.C(entity.FieldStoragePolicyEntities), sql.Select(p.C(storagepolicy.FieldID)).From(p).Where(sql.And(
					sql.IsNull(p.C(storagepolicy.FieldDeletedAt)),
					sql.In(p.C(storagepolicy.FieldType), policyTypes...),
				))),
			)
		})).
		Count(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to count computable files: %w", err)
	}

	return &HashCoverage{
		Hashed:      hashed,
		Computable:  computable,
		Unsupported: max(0, total-hashed-computable),
	}, nil
}
