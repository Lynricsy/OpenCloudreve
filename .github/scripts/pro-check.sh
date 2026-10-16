#!/usr/bin/env bash
# Pro 残留检查：本分支不允许出现 Pro 标识、Pro 推销组件或 Pro 版本开关。
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
status=0

if hits="$(grep -rnw -E 'pro|ProChip|ProDialog' assets/src --include=*.ts --include=*.tsx)"; then
  echo "::error::前端出现 Pro 残留："
  echo "$hits"
  status=1
fi

if hits="$(grep -rn -E 'IsPro|isPro|licenseKey|"-pro"' --include=*.go --exclude-dir=assets .)"; then
  echo "::error::后端出现 Pro 残留："
  echo "$hits"
  status=1
fi

[[ $status -eq 0 ]] && echo "未发现 Pro 残留"
exit $status
