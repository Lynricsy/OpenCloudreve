[中文版本](https://github.com/cloudreve/cloudreve/blob/master/README_zh-CN.md)

<h1 align="center">
  <br>
  <a href="https://cloudreve.org/" alt="logo" ><img src="https://raw.githubusercontent.com/cloudreve/frontend/master/public/static/img/logo192.png" width="150"/></a>
  <br>
  Cloudreve
  <br>
</h1>
<h4 align="center">Self-hosted file management system with multi-cloud support.</h4>

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
  <a href="https://cloudreve.org">Homepage</a> •
  <a href="https://demo.cloudreve.org">Try it</a> •
  <a href="https://github.com/cloudreve/cloudreve/discussions">Discussion</a> •
  <a href="https://docs.cloudreve.org">Documents</a> •
  <a href="https://github.com/cloudreve/cloudreve/releases">Download</a> •
  <a href="https://t.me/cloudreve_official">Telegram</a> •
  <a href="https://discord.com/invite/WTpMFpZT76">Discord</a>
</p>

![Screenshot](https://raw.githubusercontent.com/cloudreve/docs/master/images/homepage.png)

## :sparkles: Features

- :cloud: Support storing files into Local, Remote node, OneDrive, S3 compatible API, Qiniu Kodo, Aliyun OSS, Tencent COS, Huawei Cloud OBS, Kingsoft Cloud KS3, Upyun.
- :outbox_tray: Upload/Download in directly transmission from client to storage providers.
- 💾 Integrate with Aria2/qBittorrent to download files in background, use multiple download nodes to share the load.
- 📚 Compress/Extract/Preview archived files, download files in batch.
- 💻 WebDAV support covering all storage providers.
- :zap:Drag&Drop to upload files or folders, with parallel resumable upload support.
- :card_file_box: Extract media metadata from files, search files by metadata or tags.
- :family_woman_girl_boy: Multi-users with multi-groups.
- :link: Create share links for files and folders with expiration date.
- :eye_speech_bubble: Preview videos, images, audios, ePub files online; edit texts, diagrams, Markdown, images, Office documents online.
- :art: Customize theme colors, dark mode, PWA application, SPA, i18n.
- :rocket: All-in-one packaging, with all features out of the box.
- 🌈 ... ...

## :fox_face: About this fork (OpenCloudreve)

This is a personal fork of Cloudreve Community Edition v4 (backend [Lynricsy/OpenCloudreve](https://github.com/Lynricsy/OpenCloudreve), frontend submodule [Lynricsy/OpenCloudreve-frontend](https://github.com/Lynricsy/OpenCloudreve-frontend)). Every feature shown in the UI works:

- All Pro badges, upgrade prompts and unusable paid-only shells are removed (payments/products/credits, gift codes, audit events, abuse reports, third-party sign-in, announcements, anonymous permission escalation, etc.).
- Implemented on top of the community edition:
  - Multiple storage policies per group (default + extra), and per-folder policy mounting (`PATCH /api/v4/file/policy`, `GET /api/v4/user/setting/policies`);
  - Weighted load-balance storage policy (new versions of a file stay on the same child policy);
  - Resumable storage policy relocation task (`POST /api/v4/workflow/relocate`, encrypted entities are skipped);
  - Folder direct links (`/f/<id>/<folder>/<relative path>`, redirected direct links required);
  - Admins can open and edit users' files from the dashboard (`cloudreve://<uid>@my/`);
  - Per-group allowed node whitelist and user-selected processing node for archive/extract/remote download tasks (`GET /api/v4/workflow/nodes`);
  - Group default pinned shares, `user` custom property type, storage quota exceeded email;
  - Google Drive backup (Dashboard sidebar → Backup): daily incremental upload of the files in selected storage policies to the admin's Google Drive, deduplicated by SHA-256; snapshots are kept for 7 days by default and files/folders can be restored to any user; optional age encryption (the key is a single `AGE-SECRET-KEY-1…` string you can keep in 1Password, decryptable offline with `age -d`); files on slave storage policies are uploaded by the slave node directly, not through the master; a "Run history" tab shows each backup/restore run with phase timeline, event log, slave batches and failed files. Requires your own Google OAuth client (`drive.file` scope) with the consent screen published to production, otherwise refresh tokens expire after 7 days.
- Compatible with the official desktop client [cloudreve/desktop](https://github.com/cloudreve/desktop) (OAuth + PKCE, SSE file events, upload credentials never expose a load-balance policy).
- Based on upstream release `4.19.1`; this fork reports version `4.19.3` (upstream source keeps a stale default version and injects the real one from the git tag at release). Upgrading from upstream runs the `clear_folder_storage_policy` database patch once.
- Docker images are published to GHCR by GitHub Actions: `ghcr.io/lynricsy/opencloudreve:latest` (release tags) and `:edge` (latest `master`), for `linux/amd64` and `linux/arm64`. The bundled `docker-compose.yml` uses this image.

## :hammer_and_wrench: Deploy

To deploy Cloudreve, you can refer to [Getting started](https://docs.cloudreve.org/overview/quickstart) for a quick local deployment to test.

When you're ready to deploy Cloudreve to a production environment, you can refer to [Deploy](https://docs.cloudreve.org/overview/deploy/) for a complete deployment.

## :gear: Build

Please refer to [Build](https://docs.cloudreve.org/overview/build/) for how to build Cloudreve from source code.

## :rocket: Contributing

If you're interested in contributing to Cloudreve, please refer to [Contributing](https://docs.cloudreve.org/api/contributing/) for how to contribute to Cloudreve.

## :alembic: Stacks

- [Go](https://golang.org/) + [Gin](https://github.com/gin-gonic/gin) + [ent](https://github.com/ent/ent)
- [React](https://github.com/facebook/react) + [Redux](https://github.com/reduxjs/redux) + [Material-UI](https://github.com/mui-org/material-ui)

## :scroll: License

GPL V3
