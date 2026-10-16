# AGENTS.md — OpenCloudreve

Cloudreve 社区版 v4 的个人分支。原则：**界面上显示的功能都必须能用**。不要加回 Pro 标识、升级推销或无后端实现的功能壳。

## 1. 仓库布局

| 位置 | origin（可推送） | upstream（只读） | 分支 |
|---|---|---|---|
| 根仓库（Go 后端） | `Lynricsy/OpenCloudreve` | `cloudreve/Cloudreve` | `master` |
| `assets/` 子模块（React 前端） | `Lynricsy/OpenCloudreve-frontend` | `cloudreve/frontend` | `main` |

- 分叉基线：后端 `9f1e0b8`，前端 `19da0fe`。查看本分支全部改动：`git diff 9f1e0b8..HEAD -- . ':!assets'` 和 `git -C assets diff 19da0fe..HEAD`。
- `.gitmodules` 已指向前端 fork（`branch = main`），不要改回上游。
- 提交顺序：先在 `assets/` 提交并 `git push origin main`，再在根仓库 `git add assets <其他文件>` 提交并 `git push origin master`。
- 工作日志在 `.agent-logs/`（已 gitignore），通过 `record-agent-log` 写，`search-logs` 查。本分支的设计决策记录在编号 0003–0030 的日志里。

## 2. 构建、运行与验证

- **后端编译**依赖 `application/statics/assets.zip`（`go:embed`，已 gitignore）。只需要编译时，放一个占位 zip 即可，内含 `assets/build/index.html`。本机没有 `zip` 命令，用 Python 的 `zipfile` 生成。
- **正式打包**：`.build/build-assets.sh <版本号>`。这个脚本会把前端 `package.json` 的版本改成传入的版本号，需要系统有 `zip` 命令。如果直接 `yarn build`，启动时会报 `Static resource version mismatch`，页面仍能用。版本号必须等于 `constants.BackendVersion`。
- **前端**：用 `yarn`（`npm i -g yarn@1`）。`assets/` 有 husky pre-commit，会对暂存文件跑 prettier（lint-staged）。
- **类型检查**：`npx tsc --noEmit -p .`。上游本身就带一批 TS 错误（分叉时 127 条，现在 115 条）。`vite build` 不做类型检查，所以 `yarn build` 通过不代表类型正确。验收标准是“没有新增错误”：在改动前的提交上建一个临时 worktree 跑一次 tsc 作为基线，去掉行列号、`LC_ALL=C sort` 之后用 `comm` 对比。注意：含 `GroupPermission` 类型字面量的报错，文本会随权限位的增减而变化，属于误报。
- **后端检查**：`go build ./... && go test ./service/oauth/... ./inventory/... ./pkg/filemanager/... ./pkg/cluster/... ./pkg/queue/... ./pkg/auth/...`。`go vet ./...` 有上游既有告警（aria2 rpc 复制锁、HMACAuth 无键字面量、几个过时的 `_test.go`），不是本分支引入的。
- **运行冒烟**：
  - 环境：在仓库根目录执行 `mkdir -p /tmp/x && go build -o /tmp/x/cloudreve .`，然后 `cd /tmp/x`，写 `conf.ini`（`[System]\nMode = master\nListen = :<端口>`），再 `./cloudreve -c conf.ini` 启动。数据库和数据都在 `/tmp/x/data/`。不要在仓库目录里运行二进制，否则会误用残留的旧二进制和旧数据。
  - 管理员：新库里第一个注册的用户（`POST /api/v4/user`）就是管理员，登录接口是 `POST /api/v4/session/token`。
  - 文件链接：新库的站点 URL 默认是 `localhost:5212`，下载链接需要自己改主机名。
  - 本地存储策略：后台新建时，`dir_name_rule` 会被重置为默认值（上游行为），建好后要再 `PUT /admin/policy/:id` 一次才能改。

## 3. 本分支的功能与不变量（修改前必读）

### 3.1 已删除（不要恢复）
支付/商品/积分/礼品码、收据模板、事件审计（Events）、举报、第三方登录（OIDC/Logto/QQ）、公告、App 反馈与论坛、子地址邮箱禁用/邮箱域过滤、原始用户组与到期时间、提升匿名用户权限，以及 `ProChip`/`ProDialog`/`pro` prop 机制。后端的 `IsPro`、`--license-key`、`-pro` 版本后缀也都删了。

### 3.2 权限位（`inventory/types/types.go` ↔ `assets/src/api/user.ts`）
| 位 | 后端常量 | 前端键 | 用途 |
|---|---|---|---|
| 8 | `GroupPermissionRelocate` | `relocate` | 存储策略转移 |
| 10 | `GroupPermissionSelectNode` | `select_node` | 用户创建任务时选择节点 |
| 13 | `_CommunityPlaceholder3` | — | 未用 |
| 14 | `GroupPermissionFolderDirectLink` | `folder_direct_link` | 目录直链（还需开启重定向直链 `redirected_source`） |
| 15 | `SetExplicitUser_placeholder` | — | 原 Pro 的“提升匿名权限”，已放弃，不要启用 |

新增权限位要两端同步，并且先确认上游没有占用同一位。

### 3.3 数据模型（为避免改 ent schema，新字段都放在 JSON 设置里）
- `GroupSetting.available_policies []int`：默认策略之外的可选策略。默认策略仍是 `groups.storage_policy_id`。有效集合是 `{默认} ∪ available_policies`，默认排在第一个（`StoragePolicyClient.ListAvailableByGroup`）。
- `GroupSetting.allowed_nodes []int`：节点白名单，为空表示全部节点。`NodePool.Get(ctx, cap, preferred, allowed)` 的自动分配也受它限制。
- `GroupSetting.default_pinned [{share_id,name}]`：会被前置到 `/user/me` 的 `pined` 里，并带 `locked: true`；对这些项调用 `UnpinFile` 会返回 403。
- `PolicySetting.lb_children [{id,weight}]`：负载均衡策略的子策略。
- `files.storage_policy_files` 的含义：在**目录**上，`>0` 表示显式挂载了策略，NULL 表示继承；在**文件**上，记录主实体所在的策略。迁移补丁 `clear_folder_storage_policy`（EndVersion `4.14.1`）会**无条件**清空所有目录上的这一列，用来清理旧版本写入的值；它只能在从 4.14.1 之前的版本升级时跑一次。
- 设置项 `mail_exceed_quota_template`；KV 键 `quota_exceeded_notified_<uid>`（24 小时内只发一次）。

### 3.4 关键不变量
- **负载均衡（LB）**：任何文件或实体都不引用 LB 策略。所有写入路径都先经过 `dbfs.resolvePolicy` / `inventory.ResolveLoadBalance` 解析成具体子策略；`manager.GetStorageDriver` 遇到 LB 直接报错。`PUT /file/upload` 返回的 `storage_policy` 永远不是 LB，桌面端靠这一点选择上传方式。同一文件的新版本、缩略图优先放在该文件已有实体所在的子策略。LB 与普通类型之间不能互相切换；导入和转移都拒绝 LB。
- **策略解析**：`getPreferredPolicy` 沿目录祖先链往上找，取第一个已挂载、且仍在用户组可用集合里的策略，找不到就用组默认策略。管理员代他人操作时，按**目标所有者**的组解析。
- **前端上传器**（`Uploader/core`）：先按当前目录的策略派发上传器，会话创建后改用 `session.storage_policy`；如果类型不同，转交给对应的上传器（`uploaderClassFor`）。加密能力按浏览器实际能力申报，由后端根据最终策略决定是否加密。断点续传的缓存键用 `originPolicy`。
- **转移任务**（`workflows/relocate.go` + `dbfs/relocate.go`）：
  - 每个实体的顺序固定：读取源 → 上传到目标（路径带 16 位随机前缀，在所有路径模板展开之后加）→ 校验字节数 → 事务提交 → 删除旧对象。
  - 删除旧对象前检查有没有其他实体引用同一个 `(policy, source)`。
  - 以下实体跳过并计入 skipped：加密实体、仍在上传中的实体、被其他用户的文件引用的实体。
  - 任务可在重启后恢复。部分失败时仍标记为 Completed（与导入任务一致），全部失败才标记为失败。
- **管理员访问他人文件**：通过 `cloudreve://<uid>@my/` 访问。
  - 后端只能经由 `dbfs.CanAccessOthersFiles(ctx, u, write)` / `isOwnerOrBypass(ctx, ownerID, write)` 放行。如果令牌受 OAuth scope 限制，读需要 `Admin.Read`，写需要 `Admin.Write`；在 `service/explorer/workflows.go` 的 `checkTaskUrisAccess` 创建任务时也做同样检查。
  - 容量按目标所有者计算（`Capacity(ctx, u)` 用 `u.Storage`）；保存路径里的 `{uid}` 用文件所有者；禁止跨所有者移动。
  - 前端用 `canManageFile`（`useActionDisplayOpt.ts`）判断管理员能否操作，**不要**把后端的 `Owned` 字段改成对管理员恒为 true，因为它还决定分享设置等信息是否可见。
- **目录直链**：只支持重定向型直链。路由是 `/f/:id/:name/*path` 和 `/f/d/:id/:name/*path`。子路径中的空段、`.`、`..` 一律返回 404；每次访问都会重新检查所有者所在组的权限。

### 3.5 新增与改动的 API
`PATCH /file/policy {uri, policy_id}`（`policy_id` 传 `""` 表示改回继承）、`GET /user/setting/policies`（LB 策略带 `children`/`weight`）、`POST /workflow/relocate {src[], dst_policy_id}`、`GET /workflow/nodes?capability=`；压缩、解压、离线下载接口新增可选参数 `node_id`；`extended_info.storage_policy_inherited`；`/site/ping` 只返回版本号。

### 3.6 官方桌面客户端契约（`cloudreve/desktop`，没有 Pro 门控）
- **版本**：`/site/ping` 返回的版本必须 ≥ 4.12.0，并且只能是纯数字点分格式（客户端按 `.` 切分后逐段比较数字），**不要**加 `-xxx` 之类的后缀。
- **OAuth**：内置 client `393a1839-f52e-498e-9972-e77cc2241eee`，在 `inventory/migration.go` 里写入；回调地址 `/callback/desktop`，PKCE S256；刷新令牌走 `POST /session/token/refresh`。
- **其他契约**：SSE `GET /file/events`（要求 `X-Cr-Client-Id` 为 UUID）；`ExtendedInfo.storage_policy_inherited` 是必填字段；上传凭证不得是 LB 策略。
- **已知不一致**：客户端期望 `GET /user/setting` 返回 `credit` 字段（Pro 积分残留），我们没有补，客户端目前也不调用这个接口。

## 4. 同步上游

```bash
# 1) 前端
cd assets && git fetch upstream && git checkout main
git merge upstream/master            # 用 merge，不要 rebase（历史已推送）
# 解决冲突 → 第 5 节检查 → git push origin main
# 2) 后端
cd .. && git fetch upstream && git checkout -b sync/upstream-<版本> master
git merge upstream/master
# 冲突：assets gitlink 取我方合并后的前端 HEAD（git add assets）；.gitmodules 保留我方
# 解决冲突 → 第 5 节检查 → 合并回 master 并 push
```

注意事项：
- **版本号和迁移补丁（最容易出错）**：
  - 机制：只有当库里没有 `db_version_<BackendVersion>` 这一行时才会跑迁移；补丁只在“已应用的最高版本 < 补丁 EndVersion”时执行。
  - 风险：本分支已经用了 `4.14.1`。如果上游之后发布的版本 ≤ 本分支已部署的版本，上游补丁的 EndVersion 也会 ≤ 本分支库里记录的版本，**这些补丁在本分支已升级过的库上会被跳过**。
  - 规则：合并上游版本 X 后，把 `BackendVersion` 设为大于 X、也大于本分支任何已发布版本的纯数字版本，比如上游发布 4.14.1 后，本分支改用 4.14.2。逐个审计上游新增的补丁：EndVersion ≤ 本分支已部署版本的，在本分支已升级的库上不会执行，要先确认该补丁在这些库上是否必要、能否安全重复执行，再决定把 EndVersion 提到新版本，或另写一个适配本分支的新补丁。
  - **禁止上调本分支已发布补丁的 EndVersion**，尤其是 `clear_folder_storage_policy`。`applyPatches` 只比较“库中记录的最高版本”和 EndVersion，不记录哪些补丁已经跑过。把它提到新版本，会让已部署的库重新执行 `ClearStoragePolicies`，清掉用户真实的目录挂载。已发布补丁保持原来的边界；需要新的数据调整时另写新补丁。
  - 前端 `package.json` 的版本由打包脚本写入，要和后端版本一致。
- **Pro 残留**：上游一直在社区版里留 Pro 占位（`pro: true`、`ProDialog`、`checked={false}` 的死开关、新的 `_CommunityPlaceholder` 位、指向不存在 Pro 组件的 import）。每次同步后都要检查：上游新出现的 Pro 入口，要么在本分支实现，要么删掉，不能只留着显示。
- **冲突热点**：
  - 后端：`pkg/filemanager/fs/dbfs/{dbfs,upload,manage,my_navigator,props,lock}.go`、`pkg/filemanager/manager/{entity,operation,manager}.go`、`pkg/filemanager/workflows/*`、`pkg/cluster/pool.go`、`routers/router.go`、`service/explorer/{file,workflows,response,upload}.go`、`service/admin/{group,policy}.go`、`inventory/{types/types.go,migration.go,policy.go,file.go,setting.go}`。
  - 前端：`src/api/*.ts`、`redux/globalStateSlice.ts`、`redux/thunks/file.ts`、`FileManager/ContextMenu/{useActionDisplayOpt.ts,MoreMenuItems.tsx}`、`Uploader/core/{index.ts,uploader/base.ts}`、`Admin/Group/EditGroup/*`、`Admin/StoragePolicy/*`、`public/locales/*/{application,dashboard}.json`。
- **上游新增的所有者判断**（`ByPassOwnerCheckCtxKey`、`OwnerID() != user.ID` 等）要改用 `isOwnerOrBypass`。上游新增的写入路径要接入 LB 解析。上游新增的 `NodePool.Get` 调用要传 `allowed` 参数（不需要限制时传 nil）。
- **i18n**：Pro 相关的键已从 13 种语言里删掉。上游合并时如果把这些键加回来，引用它们的代码也已删除，冲突时保留我方的删除即可。删除任何键之前，先 grep 确认没有引用（包括 `t(\`...${}\`)` 这种动态拼接的引用）。
- **待同步**：上游前端 `c7b6fa5`（Cloudreve CLI 的 OAuth 登录前端）还没有合并到 `main`；对应的后端提交 `9f1e0b8` 已经在本分支里。

## 5. 同步或改动后的检查清单
1. `go build ./...`，并跑第 2 节列出的测试包。
2. `cd assets && yarn build`，tsc 与基线对比没有新增错误。
3. `grep -rnw "pro\|ProChip\|ProDialog" assets/src --include=*.ts --include=*.tsx` 结果为空；`grep -rn 'IsPro\|licenseKey\|"-pro"' --include=*.go .` 结果为空。
4. 用新库启动：`/api/v4/site/ping` 返回纯数字版本号；后台全部页面和设置页里没有 Pro、升级、支付、积分、举报等字样；能正常上传网页文件。
5. 用旧版本的库启动新代码，确认新的迁移补丁都执行了（日志里有 `Applying schema patch ...`）。再用一个已部署过本分支上一版本、并且有目录挂载策略的库启动，确认挂载仍然保留：`select count(*) from files where type=1 and storage_policy_files is not null` 升级前后一致。
6. 改动涉及存储策略、上传、转移或权限时，回归第 3.4 节对应的不变量；改动涉及 OAuth、上传凭证或 SSE 时，用 curl 按第 3.6 节模拟一遍桌面端流程。

## 6. 协作规范
- 提交信息格式为 `<type>(<scope>): <gitmoji> <subject>`，正文末尾加 `Co-authored-by: Wine Fox <fox@ling.plus>`，提交要原子化。
- 只 `git add <明确路径>`。多个 agent 同时在一个工作区工作时，曾经出现过 `git apply --cached --recount` 把 hunk 应用到错误函数的事故（`api.ts` 的 `bypassSnackbar`）。拆分 hunk 提交后，要用 `git show HEAD -- <file>` 复核落点。
- 新代码的注释用中文。复用现有模式，不要建立第二套约定。
- 已知遗留：WebDAV 上传超额不会发配额邮件；分享失效后，用户组默认固定的那一项仍会显示；管理员在他人空间打开挂载策略对话框时，列出的是管理员自己组的可用策略（后端会校验，越权会被拒绝）。
