#!/usr/bin/env bash
# 前端类型检查：上游本身带有一批 TS 错误，这里只拒绝“基线之外新增的错误”。
# 用法：.github/scripts/tsc-check.sh          检查（需在已 yarn install 的仓库中运行）
#       .github/scripts/tsc-check.sh --update 用当前结果重写基线（仅在确认错误减少或消息文本变化后使用）
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
baseline="$root/.github/tsc-baseline.txt"
current="$(mktemp)"
trap 'rm -f "$current"' EXIT

set +e
output="$(cd "$root/assets" && npx tsc --noEmit -p . 2>&1)"
tsc_status=$?
set -e

errors="$(grep "error TS" <<<"$output" || true)"

# tsc 非零退出却没有任何类型诊断：说明编译器本身启动失败或崩溃，不能当作基线结果
if [[ $tsc_status -ne 0 && -z "$errors" ]]; then
  echo "::error::tsc 异常退出（退出码 $tsc_status），未产生类型诊断："
  tail -n 50 <<<"$output"
  exit 1
fi

# 去掉行列号与本机绝对路径并按字节序排序，避免行号漂移或检出目录不同造成误报
if [[ -n "$errors" ]]; then
  sed -E 's/\([0-9]+,[0-9]+\)//' <<<"$errors" |
    sed "s#${root}/assets/##g" |
    LC_ALL=C sort >"$current"
fi

if [[ "${1:-}" == "--update" ]]; then
  cp "$current" "$baseline"
  echo "基线已更新：$(wc -l <"$baseline") 条"
  exit 0
fi

new_errors="$(LC_ALL=C comm -13 "$baseline" "$current")"
echo "当前 TS 错误 $(wc -l <"$current") 条，基线 $(wc -l <"$baseline") 条"
if [[ -n "$new_errors" ]]; then
  echo "::error::出现基线之外的 TypeScript 错误："
  echo "$new_errors"
  exit 1
fi
