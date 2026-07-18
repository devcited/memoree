#!/bin/sh
set -eu

repo_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
public_dir="$repo_dir/site/public"
schema_dir="$public_dir/schema"
agents_dir="$public_dir/agents"

command -v jq >/dev/null 2>&1 || {
  printf 'jq is required\n' >&2
  exit 1
}

mkdir -p "$schema_dir" "$agents_dir"
cargo run --quiet --locked --manifest-path "$repo_dir/Cargo.toml" --bin memoree -- schema |
  jq '.result' > "$schema_dir/v1.json"
cargo run --quiet --locked --manifest-path "$repo_dir/Cargo.toml" --bin memoree -- instructions --format markdown |
  jq -j '.result.content' > "$agents_dir/instructions.md"
