[English Version](https://github.com/cloudreve/cloudreve/blob/master/README.md)

<h1 align="center">
  <br>
  <a href="https://cloudreve.org/" alt="logo" ><img src="https://raw.githubusercontent.com/cloudreve/frontend/master/public/static/img/logo192.png" width="150"/></a>
  <br>
  Cloudreve
  <br>
</h1>

<h4 align="center">支持多家云存储驱动的公有云文件系统.</h4>

<p align="center">
  <a href="https://dev.azure.com/abslantliu/cloudreve/_build?definitionId=6">
    <img src="https://img.shields.io/github/check-runs/cloudreve/cloudreve/master"
         alt="Azure pipelines">
  </a>
  <a href="https://github.com/cloudreve/cloudreve/releases">
    <img src="https://img.shields.io/github/v/release/cloudreve/cloudreve?include_prereleases" />
  </a>
  <a href="https://github.com/cloudreve/cloudreve/releases">
     <img src="https://badgen.net/static/release%20size/34%20MB/blue"/>
  </a>
  <a href="https://hub.docker.com/r/cloudreve/cloudreve">
  <img alt="Docker Pulls" src="https://img.shields.io/docker/pulls/cloudreve/cloudreve" />
  </a>
</p>
<p align="center">
  <a href="https://cloudreve.org">主页</a> •
  <a href="https://demo.cloudreve.org">演示</a> •
  <a href="https://github.com/cloudreve/cloudreve/discussions">讨论</a> •
  <a href="https://docs.cloudreve.org">文档</a> •
  <a href="https://github.com/cloudreve/cloudreve/releases">下载</a> •
  <a href="https://t.me/cloudreve_official">Telegram</a> •
  <a href="https://discord.com/invite/WTpMFpZT76">Discord</a>
</p>

![Screenshot](https://raw.githubusercontent.com/cloudreve/docs/master/images/homepage.png)

## :sparkles: 特性

- :cloud: 支持本机、从机、七牛 Kodo、阿里云 OSS、腾讯云 COS、华为云 OBS、金山云 KS3、又拍云、OneDrive (包括世纪互联版) 、S3 兼容协议 作为存储端
- :outbox_tray: 上传/下载 支持客户端直传，支持下载限速
- 💾 可对接 Aria2/qBittorrent 离线下载，可使用多个从机节点分担下载任务
- 📚 在线 压缩/解压缩/压缩包预览、多文件打包下载
- 💻 覆盖全部存储策略的 WebDAV 协议支持
- :zap: 拖拽上传、目录上传、并行分片上传
- :card_file_box: 提取媒体元数据，通过元数据或标签搜索文件
- :family_woman_girl_boy: 多用户、用户组、多存储策略
- :link: 创建文件、目录的分享链接，可设定自动过期
- :eye_speech_bubble: 视频、图像、音频、 ePub 在线预览，文本、Office 文档在线编辑
- :art: 自定义配色、黑暗模式、PWA 应用、全站单页应用、国际化支持
- :rocket: All-in-One 打包，开箱即用
- 🌈 ... ...

## :fox_face: 关于本分支（OpenCloudreve）

本仓库是 Cloudreve 社区版 v4 的个人分支（后端 [Lynricsy/OpenCloudreve](https://github.com/Lynricsy/OpenCloudreve)，前端子模块 [Lynricsy/OpenCloudreve-frontend](https://github.com/Lynricsy/OpenCloudreve-frontend)），原则是“界面上显示的功能都能用”：

- 移除全部 Pro 标识、升级推销与无法使用的付费功能壳（支付/商品/积分、礼品码、事件审计、举报、第三方登录、公告、提升匿名用户权限等）。
- 在社区版基础上自研实现：
  - 用户组多存储策略（默认策略 + 可选策略），用户可为目录挂载存储策略（`PATCH /api/v4/file/policy`、`GET /api/v4/user/setting/policies`）；
  - 负载均衡存储策略（按权重在子策略间分配，同一文件的新版本保持在原子策略）；
  - 存储策略转移任务（`POST /api/v4/workflow/relocate`，可恢复，跳过加密实体）；
  - 目录直链（`/f/<id>/<目录名>/<相对路径>`，需开启重定向直链）；
  - 管理员在后台直接打开并读写用户文件（`cloudreve://<uid>@my/`）；
  - 用户组可用节点白名单，用户创建压缩/解压/离线下载任务时可选择节点（`GET /api/v4/workflow/nodes`）；
  - 用户组默认固定分享；自定义属性 `user` 类型；存储配额超出邮件通知；
  - Google Drive 备份（后台侧边栏“备份”）：每天把选定存储策略中的文件增量上传到管理员登录的 Google Drive，按内容 SHA-256 去重；快照默认保留 7 天，可浏览快照并把文件/目录恢复到任意用户；可选 age 加密（密钥是一串 `AGE-SECRET-KEY-1…`，可存入 1Password，离线可用 `age -d` 解密）；从机策略的文件由从机直接上传，不经过主机；可设置每日上传上限，限制每天（按服务器时区，主机和从机合计）上传的总量，超出当天额度的文件延后到下次运行。页面分为六个页签：运行状态（上次运行结果、Google 账号、Drive 空间用量、加密状态和上次备份统计）、Google 账号（OAuth 客户端、代理、登录授权）、备份设置、加密、快照与恢复、运行记录（每次备份/恢复的阶段时间线、事件日志、从机批次和失败文件）。需要自建 Google OAuth 客户端（`drive.file` 权限）并把同意屏幕发布为正式版，否则刷新令牌 7 天过期。
  - 文件内容 SHA-256（供同步客户端与 agent 校验完整性、在自己的文件里查重）：本机与从机存储策略上的文件由服务端计算（本机策略上分片按顺序到达、且落盘大小一致时边上传边算，否则上传后由后台任务计算；从机上的文件在从机本地读取）。`POST /api/v4/file/hash {uris, compute?}` 返回哈希及状态（`compute` 只对自己的文件生效，分享链接只读），`GET /api/v4/file?hash=true` 在列表中附带 `sha256`/`hash_status`，`POST /api/v4/file/hash/search {sha256: [...]}` 在自己的文件中按哈希查找，并给出尚未覆盖的文件数。文件详情面板会显示哈希。哈希不用于跨用户去重或秒传。
- 兼容官方桌面客户端 [cloudreve/desktop](https://github.com/cloudreve/desktop)（OAuth + PKCE、SSE 文件事件、上传凭证永不返回负载均衡策略）。从 `4.19.7` 起，在“已授权应用”里撤销后重新授权可以正常使用：之前重新授权后每次刷新令牌都返回 `40020`，客户端每到 access token 过期就会掉登录。已经处于这种状态的用户，升级后重新授权一次即可恢复。撤销前签发的刷新令牌在重新授权后仍然无效。
- 原生移动客户端 [OpenCloudreve Mobile](https://github.com/Lynricsy/CloudreveMobile)（iOS/Android）所需的服务端支持：内置 OAuth 客户端（`opencloudreve://oauth/callback`，PKCE）；通过 `/.well-known/assetlinks.json` 支持 Android 通行密钥登录（在后台“设置 → 用户会话”填写应用签名证书指纹，默认为空）；App 到网页的一次性会话交接（`POST /api/v4/session/handoff`，页面 `/session/handoff`）；供 App 内 WebView 使用的无外壳验证码页 `/captcha`。
- 基于上游发布版 `4.19.1`，本分支版本号为 `4.19.11`（上游源码里的版本号是过期的默认值，正式发布时按 git tag 注入）。从上游版本升级时会自动执行一次数据库补丁 `clear_folder_storage_policy`。
- Docker 镜像由 GitHub Actions 发布到 GHCR：`ghcr.io/lynricsy/opencloudreve:latest`（版本 tag）与 `:edge`（`master` 最新提交），支持 `linux/amd64` 与 `linux/arm64`。仓库自带的 `docker-compose.yml` 已改用该镜像。

## :hammer_and_wrench: 部署

你可以参考 [快速开始](https://docs.cloudreve.org/overview/quickstart) 启动一个本地实例进行体验、测试。

当你准备好将 Cloudreve 部署到生产环境时，可以参考 [部署](https://docs.cloudreve.org/overview/deploy/) 进行完整部署。

## :gear: 构建

你可以参考 [构建](https://docs.cloudreve.org/overview/build/) 从源代码构建 Cloudreve。

## :rocket: 贡献

如果你有兴趣为 Cloudreve 贡献代码，请参考 [贡献](https://docs.cloudreve.org/api/contributing/) 了解如何贡献。

## :alembic: 技术栈

- [Go](https://golang.org/) + [Gin](https://github.com/gin-gonic/gin) + [ent](https://github.com/ent/ent)
- [React](https://github.com/facebook/react) + [Redux](https://github.com/reduxjs/redux) + [Material-UI](https://github.com/mui-org/material-ui)

## :scroll: 许可证

GPL V3
