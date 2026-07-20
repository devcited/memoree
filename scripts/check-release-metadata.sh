#!/bin/sh
set -eu

repo_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$repo_dir"

cargo_version="$(sed -n 's/^version = "\([^"]*\)"/\1/p' Cargo.toml | head -1)"
lock_version="$(awk '
  $0 == "name = \"memoree\"" { found = 1; next }
  found && /^version = / { gsub(/^version = \"|\"$/, ""); print; exit }
' Cargo.lock)"
site_version="$(sed -n 's/^[[:space:]]*"version": "\([^"]*\)",/\1/p' site/package.json | head -1)"
feed_version="$(sed -n 's/^[[:space:]]*"version": "\([^"]*\)",/\1/p' site/public/releases/latest.json | head -1)"
feed_tag="$(sed -n 's/^[[:space:]]*"tag": "\([^"]*\)",/\1/p' site/public/releases/latest.json | head -1)"
latest_tag="$(tr -d '\r\n' < site/public/releases/latest.txt)"
schema_version="$(sed -n 's/^pub const SCHEMA_VERSION: i64 = \([0-9][0-9]*\);/\1/p' src/store.rs | head -1)"
feed_schema="$(sed -n 's/^[[:space:]]*"store_schema_version": \([0-9][0-9]*\),/\1/p' site/public/releases/latest.json | head -1)"

test -n "$cargo_version"
test "$cargo_version" = "$lock_version"
test "$cargo_version" = "$site_version"
test "$cargo_version" = "$feed_version"
test "v$cargo_version" = "$feed_tag"
test "$feed_tag" = "$latest_tag"
test "$schema_version" = "$feed_schema"
grep -F "## [$cargo_version]" CHANGELOG.md >/dev/null

if [ "${GITHUB_REF_TYPE:-}" = "tag" ]; then
  test "${GITHUB_REF_NAME:-}" = "$feed_tag"
fi
