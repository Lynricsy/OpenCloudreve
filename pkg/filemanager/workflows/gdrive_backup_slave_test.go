package workflows

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/enttest"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/backup"
	"github.com/cloudreve/Cloudreve/v4/pkg/cluster"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/encrypt"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/stretchr/testify/require"
)

const testSlaveNodeID = 7

type (
	// slaveTestDep 只实现派发和轮询从机批次用到的依赖
	slaveTestDep struct {
		dependency.Dep
		db   *ent.Client
		pool cluster.NodePool
	}

	staticMasterKey []byte

	slaveTestPool struct{ node cluster.Node }

	// slaveTestNode 记录派发的状态，并按 respond 返回轮询结果
	slaveTestNode struct {
		cluster.Node
		payloads []string
		respond  func(payload string) *cluster.SlaveTaskSummary
	}
)

func (d *slaveTestDep) DBClient() *ent.Client { return d.db }
func (d *slaveTestDep) NodePool(ctx context.Context) (cluster.NodePool, error) {
	return d.pool, nil
}
func (d *slaveTestDep) MasterEncryptKeyVault(ctx context.Context) encrypt.MasterEncryptKeyVault {
	return staticMasterKey(bytes.Repeat([]byte{1}, 32))
}

func (k staticMasterKey) GetMasterKey(ctx context.Context) ([]byte, error) { return k, nil }

func (p *slaveTestPool) Upsert(ctx context.Context, node *ent.Node) {}
func (p *slaveTestPool) Get(ctx context.Context, capability types.NodeCapability, preferred int, allowed []int) (cluster.Node, error) {
	return p.node, nil
}

func (n *slaveTestNode) ID() int        { return testSlaveNodeID }
func (n *slaveTestNode) Name() string   { return "slave" }
func (n *slaveTestNode) IsMaster() bool { return false }
func (n *slaveTestNode) CreateTask(ctx context.Context, taskType string, state string) (int, error) {
	n.payloads = append(n.payloads, state)
	// 每次都返回 1：模拟从机重启后任务 ID 被复用
	return 1, nil
}
func (n *slaveTestNode) GetTask(ctx context.Context, id int, clearOnComplete bool) (*cluster.SlaveTaskSummary, error) {
	return n.respond(n.payloads[len(n.payloads)-1]), nil
}

// GetOwnedTask 模拟忽略 nonce 参数的旧版从机，由主机自己核对 nonce
func (n *slaveTestNode) GetOwnedTask(ctx context.Context, id int, nonce string, clearOnComplete bool) (*cluster.SlaveTaskSummary, error) {
	return n.GetTask(ctx, id, clearOnComplete)
}

func (n *slaveTestNode) lastState(t *testing.T) *SlaveGDriveBackupState {
	t.Helper()
	require.NotEmpty(t, n.payloads)
	s := &SlaveGDriveBackupState{}
	require.NoError(t, json.Unmarshal([]byte(n.payloads[len(n.payloads)-1]), s))
	return s
}

// completedWith 返回一个按派发内容完成的从机任务：fn 为每个实体生成结果，返回 false 表示结果中缺失该实体
func completedWith(fn func(e *ent.Entity) (SlaveGDriveBackupResult, bool)) func(string) *cluster.SlaveTaskSummary {
	return func(payload string) *cluster.SlaveTaskSummary {
		s := &SlaveGDriveBackupState{}
		_ = json.Unmarshal([]byte(payload), s)
		res := &SlaveGDriveBackupState{Nonce: s.Nonce}
		for _, item := range s.Items {
			if r, ok := fn(item.Entity); ok {
				res.Results = append(res.Results, r)
			}
		}
		out, _ := json.Marshal(res)
		return &cluster.SlaveTaskSummary{Status: task.StatusCompleted, PrivateState: string(out)}
	}
}

func uploadedResult(e *ent.Entity) (SlaveGDriveBackupResult, bool) {
	return SlaveGDriveBackupResult{EntityID: e.ID, Sha256: "sha", Blob: "new-blob", BlobID: "drive-id", Uploaded: true}, true
}

type slaveBatchFixture struct {
	m        *GDriveBackupTask
	dep      *slaveTestDep
	node     *slaveTestNode
	db       *ent.Client
	policyID int
}

func newSlaveBatchFixture(t *testing.T, kr *backup.Keyring) *slaveBatchFixture {
	t.Helper()
	db := enttest.Open(t, "sqlite3", filepath.Join(t.TempDir(), "backup.db"))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	policy := db.StoragePolicy.Create().SetName("slave").SetType(types.PolicyTypeRemote).SaveX(context.Background())
	policy.NodeID = testSlaveNodeID
	node := &slaveTestNode{}
	dep := &slaveTestDep{db: db, pool: &slaveTestPool{node: node}}
	m := &GDriveBackupTask{
		DBTask: &queue.DBTask{Task: &ent.Task{}},
		l:      logging.NewConsoleLogger(logging.LevelError),
		state: &GDriveBackupTaskState{
			TempDir: t.TempDir(),
			Remote:  map[int]*GDriveRemoteNode{},
		},
		rt: &gdriveBackupRuntime{
			env:      &backup.Env{Config: &setting.GDriveBackup{}, Keyring: kr, Store: &backup.Store{}},
			policies: map[int]*ent.StoragePolicy{policy.ID: policy},
			prev:     map[int]backup.ManifestEntry{},
			blobs:    map[string]string{},
		},
	}
	return &slaveBatchFixture{m: m, dep: dep, node: node, db: db, policyID: policy.ID}
}

// entity 在数据库中创建从机策略上的实体
func (f *slaveBatchFixture) entity(t *testing.T, props *types.EntityProps) int {
	t.Helper()
	q := f.db.Entity.Create().SetType(0).SetSource("src").SetSize(5).SetStoragePolicyEntities(f.policyID)
	if props != nil {
		q.SetProps(props)
	}
	return q.SaveX(context.Background()).ID
}

// enqueue 把文件 fileID（主实体 entityID）排入从机队列；withPrev 时给它一份旧快照条目，失败时应被沿用
func (f *slaveBatchFixture) enqueue(t *testing.T, fileID, entityID int, withPrev bool) {
	t.Helper()
	entry := backup.ManifestEntry{FileID: fileID, EntityID: entityID, Path: "/f" + string(rune('0'+fileID)), Size: 5}
	require.NoError(t, f.m.appendPending(testSlaveNodeID, &gdrivePendingLine{Entry: entry, PolicyID: f.policyID}))
	if withPrev {
		prev := entry
		prev.Sha256, prev.Blob = "old-sha", "old-blob"
		f.m.rt.prev[fileID] = prev
		f.m.rt.blobs["old-blob"] = "old-id"
	}
}

func (f *slaveBatchFixture) pump(t *testing.T) {
	t.Helper()
	require.NoError(t, f.m.pumpRemote(context.Background(), f.dep))
}

func (f *slaveBatchFixture) remote() *GDriveRemoteNode {
	return f.m.state.Remote[testSlaveNodeID]
}

// manifest 返回已写入清单的条目：文件 ID → 条目
func (f *slaveBatchFixture) manifest(t *testing.T) map[int]backup.ManifestEntry {
	t.Helper()
	require.NoError(t, f.m.closeWriters())
	res := make(map[int]backup.ManifestEntry)
	file, err := os.Open(filepath.Join(f.m.state.TempDir, gdriveManifestFile))
	if os.IsNotExist(err) {
		return res
	}
	require.NoError(t, err)
	defer file.Close()
	s := bufio.NewScanner(file)
	for s.Scan() {
		var e backup.ManifestEntry
		require.NoError(t, json.Unmarshal(s.Bytes(), &e))
		_, dup := res[e.FileID]
		require.False(t, dup, "file %d written twice", e.FileID)
		res[e.FileID] = e
	}
	require.NoError(t, s.Err())
	return res
}

// 实体密钥解不开只让引用它的文件失败并沿用旧条目，同批其他文件照常派发，任务不报错。
func TestDispatchBatchUndecryptableEntityFailsOnlyThatFile(t *testing.T) {
	f := newSlaveBatchFixture(t, nil)
	good := f.entity(t, nil)
	// 主密钥加密的实体密钥至少要有 16 字节 IV，这里模拟损坏的数据
	bad := f.entity(t, &types.EntityProps{EncryptMetadata: &types.EncryptMetadata{Algorithm: types.CipherAES256CTR, Key: []byte("broken")}})
	f.enqueue(t, 1, good, false)
	f.enqueue(t, 2, bad, true)
	f.node.respond = completedWith(uploadedResult)

	f.pump(t)
	sent := f.node.lastState(t)
	require.Len(t, sent.Items, 1)
	require.Equal(t, good, sent.Items[0].Entity.ID)

	f.pump(t)
	n := f.remote()
	require.Nil(t, n.Inflight)
	require.Equal(t, n.Written, n.Done)
	require.EqualValues(t, 1, f.m.state.Stats.Failed)
	require.EqualValues(t, 1, f.m.state.Stats.Uploaded)
	entries := f.manifest(t)
	require.Equal(t, "new-blob", entries[1].Blob)
	require.Equal(t, "old-blob", entries[2].Blob, "failed file must carry over the previous entry")
}

// 批次里只有密钥解不开的实体时不派发，直接结算为失败。
func TestDispatchBatchOnlyUndecryptableEntitiesSettlesWithoutSlave(t *testing.T) {
	f := newSlaveBatchFixture(t, nil)
	bad := f.entity(t, &types.EntityProps{EncryptMetadata: &types.EncryptMetadata{Algorithm: types.CipherAES256CTR, Key: []byte("broken")}})
	f.enqueue(t, 1, bad, true)

	f.pump(t)
	require.Empty(t, f.node.payloads)
	n := f.remote()
	require.Equal(t, n.Written, n.Done)
	require.EqualValues(t, 1, f.m.state.Stats.Failed)
	require.Equal(t, "old-blob", f.manifest(t)[1].Blob)
}

// 从机重启后任务 ID 被另一个任务复用：nonce 对不上时按任务丢失重派，3 次后整批记为失败，不能把别的任务的完成当作本批完成。
func TestPollBatchNonceMismatchTreatedAsLost(t *testing.T) {
	f := newSlaveBatchFixture(t, nil)
	e := f.entity(t, nil)
	f.enqueue(t, 1, e, true)
	foreign := []string{
		`{"results":[]}`, // 没有 nonce 的其他任务（例如 slave_upload）
		`{"nonce":"other"}`,
		`{"nonce":""}`,
	}
	f.node.respond = func(string) *cluster.SlaveTaskSummary {
		return &cluster.SlaveTaskSummary{Status: task.StatusCompleted, PrivateState: foreign[len(f.node.payloads)-1]}
	}

	f.pump(t) // 首次派发
	for i := 0; i < gdriveRemoteMaxAttempts && f.remote().Done < f.remote().Written; i++ {
		f.pump(t)
	}
	n := f.remote()
	require.Len(t, f.node.payloads, gdriveRemoteMaxAttempts)
	require.Nil(t, n.Inflight)
	require.Equal(t, n.Written, n.Done)
	require.EqualValues(t, 1, f.m.state.Stats.Failed)
	require.Zero(t, f.m.state.Stats.Reused)
	require.Equal(t, "old-blob", f.manifest(t)[1].Blob)

	nonces := map[string]bool{}
	for _, p := range f.node.payloads {
		s := &SlaveGDriveBackupState{}
		require.NoError(t, json.Unmarshal([]byte(p), s))
		require.NotEmpty(t, s.Nonce)
		nonces[s.Nonce] = true
	}
	require.Len(t, nonces, gdriveRemoteMaxAttempts, "every dispatch must use a fresh nonce")
}

// nonce 一致的结果正常结算。
func TestPollBatchMatchingNonceApplied(t *testing.T) {
	f := newSlaveBatchFixture(t, nil)
	e := f.entity(t, nil)
	f.enqueue(t, 1, e, false)
	f.node.respond = completedWith(uploadedResult)

	f.pump(t)
	f.pump(t)
	require.Len(t, f.node.payloads, 1)
	require.EqualValues(t, 1, f.m.state.Stats.Uploaded)
	require.Equal(t, "new-blob", f.manifest(t)[1].Blob)
}

// 已派发但结果里缺失的实体计为失败并沿用旧条目；只有派发时就已删除的实体才跳过。
func TestSettleBatchMissingResultCountsAsFailure(t *testing.T) {
	f := newSlaveBatchFixture(t, nil)
	ok := f.entity(t, nil)
	missing := f.entity(t, nil)
	f.enqueue(t, 1, ok, false)
	f.enqueue(t, 2, missing, true)
	f.enqueue(t, 3, missing+100, true) // 派发前已被删除的实体
	f.node.respond = completedWith(func(e *ent.Entity) (SlaveGDriveBackupResult, bool) {
		if e.ID == missing {
			return SlaveGDriveBackupResult{}, false
		}
		return uploadedResult(e)
	})

	f.pump(t)
	require.ElementsMatch(t, []int{ok, missing}, f.remote().Inflight.Dispatched)
	f.pump(t)
	require.EqualValues(t, 1, f.m.state.Stats.Failed)
	require.Len(t, f.m.state.Errors, 1)
	require.Contains(t, f.m.state.Errors[0], "no result")
	entries := f.manifest(t)
	require.Equal(t, "new-blob", entries[1].Blob)
	require.Equal(t, "old-blob", entries[2].Blob)
	_, written := entries[3]
	require.False(t, written, "entity deleted before dispatch is skipped")
}

// 加密模式下下发给从机的只有公钥和 blob 命名密钥：从机能加密并算出与主机相同的 blob 名称，但拿不到私钥。
func TestDispatchBatchSendsSlaveKeyWithoutPrivateKey(t *testing.T) {
	secret, _, err := backup.GenerateKey()
	require.NoError(t, err)
	kr, err := backup.ParseKey(secret)
	require.NoError(t, err)
	f := newSlaveBatchFixture(t, kr)
	f.m.rt.env.Config.Key = secret
	e := f.entity(t, nil)
	f.enqueue(t, 1, e, false)
	f.node.respond = completedWith(uploadedResult)

	f.pump(t)
	payload := f.node.payloads[0]
	require.NotContains(t, payload, secret)
	require.NotContains(t, strings.ToUpper(payload), "AGE-SECRET-KEY")

	sent := f.node.lastState(t)
	slaveKr, err := backup.ParseSlaveKey(sent.EncryptKey)
	require.NoError(t, err)
	require.NotNil(t, slaveKr)
	require.Equal(t, kr.ID(), slaveKr.ID())
	sum := bytes.Repeat([]byte{7}, 32)
	require.Equal(t, kr.BlobName(sum), slaveKr.BlobName(sum))

	// 从机加密的内容主机能解开，从机自己解不开
	var buf bytes.Buffer
	w, err := slaveKr.Encrypt(&buf)
	require.NoError(t, err)
	_, err = w.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	_, err = slaveKr.Decrypt(bytes.NewReader(buf.Bytes()))
	require.Error(t, err)
	r, err := kr.Decrypt(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	plain, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "hello", string(plain))
}

// 不加密时不下发任何密钥。
func TestDispatchBatchPlainSendsNoKey(t *testing.T) {
	f := newSlaveBatchFixture(t, nil)
	e := f.entity(t, nil)
	f.enqueue(t, 1, e, false)
	f.node.respond = completedWith(uploadedResult)

	f.pump(t)
	require.Empty(t, f.node.lastState(t).EncryptKey)
}
