# AGENTS.md — OpenCloudreve

Cloudreve 社区版 v4 的个人分支。原则：**界面上显示的功能都必须能用**。不要加回 Pro 标识、升级推销或无后端实现的功能壳。

## 1. 仓库布局

| 位置 | origin（可推送） | upstream（只读） | 分支 |
|---|---|---|---|
| 根仓库（Go 后端） | `Lynricsy/OpenCloudreve` | `cloudreve/Cloudreve` | `master` |
| `assets/` 子模块（React 前端） | `Lynricsy/OpenCloudreve-frontend` | `cloudreve/frontend` | `main` |

- 分叉基线：后端 `9f1e0b8`（上游发布 tag `4.19.1` 之后的 1 个提交），前端 `19da0fe`。查看本分支全部改动：`git diff 9f1e0b8..HEAD -- . ':!assets'` 和 `git -C assets diff 19da0fe..HEAD`。
- `.gitmodules` 已指向前端 fork（`branch = main`），不要改回上游。
- 提交顺序：先在 `assets/` 提交并 `git push origin main`，再在根仓库 `git add assets <其他文件>` 提交并 `git push origin master`。
- 工作日志在 `.agent-logs/`（已 gitignore），通过 `record-agent-log` 写，`search-logs` 查。本分支的设计决策记录在编号 0003–0030 的日志里。

## 2. 构建、运行与验证

- **后端编译**依赖 `application/statics/assets.zip`（`go:embed`，已 gitignore）。只需要编译时，放一个占位 zip 即可，内含 `assets/build/index.html`。本机没有 `zip` 命令，用 Python 的 `zipfile` 生成。
- **版本号**：上游源码里的 `constants.BackendVersion` 是过期的默认值（上游 4.19.1 的源码里仍写着 4.14.0），正式版本由 goreleaser 按 git tag 用 ldflags 注入。判断上游版本要用 `git describe --tags upstream/master`，不要看 `constants.go`。本分支的源码常量就是真实版本（当前 `4.19.6`）；CI 发布 tag 时会强制要求 tag 与常量一致。新增 `DefaultSettings` 键时必须同时提升版本号：`migrateDefaultSettings` 只在库里没有当前版本标记时运行，而 `SettingClient.Set` 只更新已存在的行，缺行时设置接口会静默不生效。
- **正式打包**：`.build/build-assets.sh <版本号>`。这个脚本会把前端 `package.json` 的版本改成传入的版本号，需要系统有 `zip` 命令。如果直接 `yarn build`，启动时会报 `Static resource version mismatch`，页面仍能用。版本号必须等于 `constants.BackendVersion`。
- **前端**：用 `yarn`（`npm i -g yarn@1`）。`assets/` 有 husky pre-commit，会对暂存文件跑 prettier（lint-staged）。
- **类型检查**：`.github/scripts/tsc-check.sh`（需先在 `assets/` 里 `yarn install`）。上游本身就带一批 TS 错误（分叉时 127 条，现在 115 条），基线存在 `.github/tsc-baseline.txt`，脚本会去掉行列号、按字节序排序后对比，出现基线之外的错误就失败。`vite build` 不做类型检查，所以 `yarn build` 通过不代表类型正确。错误减少，或者因为权限位增减导致 `GroupPermission` 类型字面量的报错文本变化时，确认没有真正新增错误后，运行 `.github/scripts/tsc-check.sh --update` 更新基线。
- **后端检查**：与 CI 一致，`go build ./...`；`go vet -composites=false -copylocks=false -stdmethods=false` 和 `go test`，范围是除 `pkg/conf`、`pkg/cache`、`pkg/request` 以外的全部包（这三个包的测试文件在上游已无法编译；关掉的三类 vet 告警都是上游既有问题）。
- **Pro 残留检查**：`.github/scripts/pro-check.sh`。
- **CI**（`.github/workflows/`）：
  - `ci.yml`：代码检查。后端对改动的 Go 文件跑 gofmt，再跑 build、vet、test；前端跑 Pro 残留检查、tsc 基线对比和 `yarn build`。PR 和手动触发时单独运行；master 推送和版本 tag 不单独触发，而是由 `docker.yml` 通过 `workflow_call` 调用。只推送前端仓库不会触发，要等根仓库更新子模块指针后才会跑。
  - `docker.yml`：先调用 `ci.yml`，检查通过才发布到 `ghcr.io/lynricsy/opencloudreve`。push 到 master 时发布 `edge` 和 `sha-<短哈希>`；推送纯数字版本 tag（如 `4.19.2`）时发布 `4.19.2`、`4.19`、`latest`。流程是前端只构建一次，amd64 和 arm64 分别在原生 runner 上编译二进制、构建镜像，最后合并成多架构清单。镜像沿用上游 `Dockerfile`，不要为 CI 改它。
  - 发版步骤：先把 `constants.BackendVersion` 改成新版本并推送 master，再执行 `git tag <版本> && git push origin <版本>`。
- **运行冒烟**：
  - 环境：在仓库根目录执行 `mkdir -p /tmp/x && go build -o /tmp/x/cloudreve .`，然后 `cd /tmp/x`，写 `conf.ini`（`[System]\nMode = master\nListen = :<端口>`），再 `./cloudreve -c conf.ini` 启动。数据库和数据都在 `/tmp/x/data/`。不要在仓库目录里运行二进制，否则会误用残留的旧二进制和旧数据。
  - 管理员：新库里第一个注册的用户（`POST /api/v4/user`）就是管理员，登录接口是 `POST /api/v4/session/token`。
  - 文件链接：新库的站点 URL 默认是 `localhost:5212`，下载链接需要自己改主机名。
  - 本地存储策略：后台新建时，`dir_name_rule` 会被重置为默认值（上游行为），建好后要再 `PUT /admin/policy/:id` 一次才能改。
  - 从机节点：`PUT /admin/node` 的 `node.capabilities` 必须传（例如 `"AA=="`），否则会在事务中 panic，SQLite 写锁一直不释放，只能重启进程。从机的 `[Slave] Secret` 至少 64 个字符。从机上传完成后会回调主机，所以要先把 `siteURL` 改成主机的实际地址。
  - Google Drive 备份：没有真实的 Google 账号时，可以写一个 HTTPS MITM 代理来伪造 `oauth2.googleapis.com` 和 `www.googleapis.com`（用自签 CA 处理 CONNECT），启动主机和从机时带上 `SSL_CERT_FILE=<CA>`，再把 `gdrive_backup_proxy` 设为这个代理。授权流程是先调 `POST /admin/backup/oauth/signin`，从返回的 URL 里取出 state，然后调 callback。

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
- `files.storage_policy_files` 的含义：在**目录**上，`>0` 表示显式挂载了策略，NULL 表示继承；在**文件**上，记录主实体所在的策略。迁移补丁 `clear_folder_storage_policy`（EndVersion `4.19.2`）会**无条件**清空所有目录上的这一列，用来清理旧版本写入的值；它只在从上游版本（≤ 4.19.1）升级时跑一次。这个补丁已经随 4.19.2 发布，EndVersion 不能再改。
- 设置项 `mail_exceed_quota_template`；KV 键 `quota_exceeded_notified_<uid>`（24 小时内只发一次）。
- Google Drive 备份设置项 `gdrive_backup_*` 与 `cron_gdrive_backup`（`setting.Provider.GDriveBackup`）。`gdrive_backup_refresh_token` 在 `RedactedSettings` 里，只由 OAuth 回调写入；`gdrive_backup_root_id` 由 `backup.Open` 自动维护。备份不写数据库：sha256 缓存和文件清单都在 Drive 上的快照里。

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
- **Google Drive 备份**（`pkg/backup` 负责 Drive 客户端、仓库布局、age 加密、凭证；任务在 `workflows/gdrive_{backup,backup_slave,restore}.go`）：
  - Drive 布局：根目录（`ocr_kind=root`）下有 `blobs/<前两位>/<blob名>` 和 `manifests/<时间>.jsonl.gz[.age]`，所有文件带 `ocr_root=<根目录ID>`。blob 名称在明文模式下是 sha256；加密模式下是由私钥派生的 HMAC，因此换密钥等于换一个命名空间。权限范围只有 `drive.file`。
  - 每次运行都完整遍历选定策略（LB 展开为子策略）中、位于“我的文件”下的文件，不使用时间水位。选中条件是主实体已完成上传（`upload_session_id` 为空）；仍在上传中的计入 pending，下次运行时自然会被选中。回收站中的文件不备份。
  - sha256 从上一份同密钥快照中按实体 ID 复用（实体内容不可变）。Drive 上已有同名 blob 时不上传。失败或 pending 的文件沿用上一份快照中的条目，前提是对应 blob 仍然存在，这样连续失败超过保留期也不会丢掉旧备份。
  - 从机策略（`remote`）的实体写入本地队列文件，按批派发 `slave_gdrive_backup` 从机任务。从机通过主机的 `/slave/credential/cred_backup_gdrive` 拿 access token，直接读取本地文件、上传到 Drive，主机不经手文件内容。age 私钥和实体解密密钥随任务状态下发给从机，所以主从之间应使用 HTTPS。从机任务丢失（比如从机重启）后会重派，最多 3 次。
  - 清理顺序：先上传新快照，再删除早于保留期的快照（最新一份始终保留），最后删除不被任何保留快照引用的 blob。如果有恢复任务正在执行，或者任一保留快照无法读取（例如用其他密钥加密），就跳过 blob 清理。
  - 恢复使用目标用户自己的 FileManager 写入（容量和策略按目标用户计算）。同名文件跳过不覆盖。读到流末尾时校验 sha256，不一致则让上传失败。
  - 同一时间只允许一个备份任务（`GDriveTaskPending`）。`crontab.Start` 可以重复调用；修改 `cron_gdrive_backup` 后由设置后处理器重建 cron，立即生效。
  - 运行记录：两种任务的私有状态都带有 `events`（`{t,l,c,p}`，最多 500 条，超出后丢弃最旧的并累计 `events_dropped`），备份任务还记录 `trigger`（`cron`/`manual`，同时写入 summary）。前端“备份 → 运行记录”页签通过现有的 `/admin/queue` 接口读取，按 `backup.events.<code>` 翻译，并用 `started`/`scan_done`/`manifest_started`/`snapshot_uploaded`/`finished` 推算阶段时间线。新增或改名事件 code 时，前端三种语言的文案要同步改。`task_failed` 只在任务最终失败时由 `OnError` 写入，可重试的错误不记录。
  - 前端页面（`Admin/Backup/Backup.tsx`）分为六个横向页签：运行状态、Google 账号（含代理）、备份设置、加密、快照与恢复、运行记录，URL 参数为 `?tab=`，OAuth 回调结束后回到 `?tab=account`。备份状态由页面统一获取，任务运行中每 5 秒轮询一次，结果分发给各页签。账号和加密页签通过 `SettingsWrapper` 的 `onSaved` 在保存后刷新状态。设置类页签各自用一个 `SettingsWrapper` 读取和保存。传给它的 `settings` 数组必须是模块级常量：`SettingsWrapper` 以这个数组的引用作为 effect 依赖，如果写成内联数组，状态轮询一刷新就会重新拉取设置，并丢掉还没保存的修改。
  - 页签内容统一使用 `Admin/Backup/BackupUi.tsx` 中的组件：`BackupCard`（描边卡片）、`BackupCardHeader`（色调图标、标题、副标题、操作区）、`StatTile`（统计块）、`ToneIcon`。颜色只取主题语义色（`primary`/`success`/`warning`/`error`），并用 `alpha` 生成浅色底，这样深色模式下也能正常显示，不要写死色值。页签枚举 `BackupPageTab` 也定义在这个文件里，避免与 `Backup.tsx` 循环引用。

### 3.5 新增与改动的 API
`PATCH /file/policy {uri, policy_id}`（`policy_id` 传 `""` 表示改回继承）、`GET /user/setting/policies`（LB 策略带 `children`/`weight`）、`POST /workflow/relocate {src[], dst_policy_id}`、`GET /workflow/nodes?capability=`；压缩、解压、离线下载接口新增可选参数 `node_id`；`extended_info.storage_policy_inherited`；`/site/ping` 只返回版本号。管理员备份接口位于 `/admin/backup/*`：`GET status`、`POST oauth/signin`、`POST oauth/callback {code,state}`、`DELETE oauth`、`POST key`、`POST run`、`GET snapshots`、`POST snapshots/browse {snapshot_id, uid?, path?}`、`POST restore {snapshot_id, items[{uid,path}], target_uid?, target_dir}`。前端页面是后台侧边栏的“备份”（`/admin/backup`，组件在 `Admin/Backup/`，设置项仍经 `SettingsWrapper` 读写），回调页是 `/admin/backup/oauth`；上游遗留的死路由 `/callback/googledrive/auth` 已删除。

### 3.6 官方桌面客户端契约（`cloudreve/desktop`，没有 Pro 门控）
- **版本**：`/site/ping` 返回的版本必须 ≥ 4.12.0，并且只能是纯数字点分格式（客户端按 `.` 切分后逐段比较数字），**不要**加 `-xxx` 之类的后缀。
- **官方 CLI（`cloudreve/cli`，有 macOS 版）**：要求服务端版本 ≥ 4.17.0 且 < 5。CLI 1.0.0 已在 4.19.2 上实测通过：密码登录、`ls`、上传、`cat`、`stat` 都正常。
- **OAuth**：内置 client `393a1839-f52e-498e-9972-e77cc2241eee`，在 `inventory/migration.go` 里写入；回调地址 `/callback/desktop`，PKCE S256；刷新令牌走 `POST /session/token/refresh`。
- **撤销授权**（`DELETE /session/oauth/grant/:app_id`）：`oauth_grants` 是物理删除（软删除行会占住 `(user_id, client_id)` 唯一索引，导致重新授权后刷新令牌永久失效，见 issue #1）；`UpsertGrant` 会先清掉旧版本留下的软删除行。重新授权总是新建授权记录，`tokenAuth.Refresh` 拒绝 `NotBefore` 早于当前授权 `created_at` 超过 2 秒的刷新令牌，所以撤销前签发的令牌不会因重新授权复活；例外是新授权创建前 2 秒内签发的令牌仍可刷新（容差用于抵消 `NotBefore` 截断到秒和 MySQL datetime 四舍五入）。同一 client 被多端共用时，对仍有效的授权重新同意不会新建记录，但会用本次请求的 scopes 覆盖授权的 scopes，其他端令牌的 scopes 若超出新范围，刷新时会被拒绝。
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
  - 风险：如果上游之后发布的版本 ≤ 本分支已部署的版本（当前 4.19.6），上游补丁的 EndVersion 也会 ≤ 本分支库里记录的版本，**这些补丁在本分支已升级过的库上会被跳过**。
  - 规则：合并上游版本 X 后，把 `BackendVersion` 设为大于 X、也大于本分支任何已发布版本的纯数字版本，比如上游发布 4.19.6 后，本分支改用 4.19.7。`constants.go` 冲突时保留我方的值，再按这条规则改。逐个审计上游新增的补丁：EndVersion ≤ 本分支已部署版本的，在本分支已升级的库上不会执行，要先确认该补丁在这些库上是否必要、能否安全重复执行，再决定把 EndVersion 提到新版本，或另写一个适配本分支的新补丁。
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
2. `.github/scripts/tsc-check.sh` 通过，`cd assets && yarn build` 成功。
3. `.github/scripts/pro-check.sh` 通过。
4. 用新库启动：`/api/v4/site/ping` 返回纯数字版本号；后台全部页面和设置页里没有 Pro、升级、支付、积分、举报等字样；能正常上传网页文件。
5. 用旧版本的库启动新代码，确认新的迁移补丁都执行了（日志里有 `Applying schema patch ...`）。再用一个已部署过本分支上一版本、并且有目录挂载策略的库启动，确认挂载仍然保留：`select count(*) from files where type=1 and storage_policy_files is not null` 升级前后一致。
6. 改动涉及存储策略、上传、转移或权限时，回归第 3.4 节对应的不变量；改动涉及 OAuth、上传凭证或 SSE 时，用 curl 按第 3.6 节模拟一遍桌面端流程。

## 6. 协作规范
- 提交信息格式为 `<type>(<scope>): <gitmoji> <subject>`，正文末尾加 `Co-authored-by: Wine Fox <fox@ling.plus>`，提交要原子化。
- 只 `git add <明确路径>`。多个 agent 同时在一个工作区工作时，曾经出现过 `git apply --cached --recount` 把 hunk 应用到错误函数的事故（`api.ts` 的 `bypassSnackbar`）。拆分 hunk 提交后，要用 `git show HEAD -- <file>` 复核落点。
- 新代码的注释用中文。复用现有模式，不要建立第二套约定。
- 已知遗留：WebDAV 上传超额不会发配额邮件；分享失效后，用户组默认固定的那一项仍会显示；管理员在他人空间打开挂载策略对话框时，列出的是管理员自己组的可用策略（后端会校验，越权会被拒绝）。
- 已知遗留（Google Drive 备份）：
  - 恢复时单个文件下载中断不会续传，整个文件记为失败，需要重新发起恢复。
  - “断开”（`DisconnectBackup`）只清空本地保存的刷新令牌，不会调用 Google 的撤销接口，旧令牌在 Google 侧仍然有效。令牌泄露时，要到 Google 账号的第三方应用授权管理里撤销授权，或者调用 Google 的令牌撤销端点。
  - 首次全量备份可能受 Google 的上传或 API 限额影响（具体额度因账号类型而异，以 Drive 返回的错误和 Cloud 控制台配额为准）；因限额失败的文件记为失败，下次运行时重试。
  - Drive 新建文件的请求体只能带 `name`/`mimeType`/`parents`/`appProperties`（`driveCreateMeta`）。不要复用响应结构 `DriveFile`：空 `id` 会被 Drive 以 400 `fileIdNotUsable` 拒绝。测试用的假 Drive（`fakedrive_test.go`）对此做了同样的校验。
  - 上游 `SetSetting` 按处理器的函数类型名去重，所有处理器的类型名都相同，所以一次提交里即使涉及多种前置/后置处理器，也只会执行其中一个。备份页面只提交自己的设置项，不受影响；其他设置页签混合提交时可能漏执行处理器。
- 语言包缓存：前端把语言包缓存在 localStorage，缓存版本是构建时 `public/locales` 的内容哈希（`vite.config.ts` 的 `__LOCALES_HASH__`）。不要去掉这个版本，否则升级后新增的文案会显示成键名，`returnObjects` 数组还会让页面崩溃。
