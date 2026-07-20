#!/bin/sh
set -eu

repo_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
public_dir="$repo_dir/site/public"
schema_dir="$public_dir/schema"
agents_dir="$public_dir/agents"
temp_dir=""

cleanup() {
  if [ -n "$temp_dir" ] && [ -d "$temp_dir" ]; then
    rm -rf -- "$temp_dir"
  fi
}
trap cleanup 0
trap 'exit 1' HUP INT TERM

for command_name in cargo jq mkdir mktemp mv rm; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command_name" >&2
    exit 1
  }
done

mkdir -p "$schema_dir" "$agents_dir"
temp_dir="$(mktemp -d "$public_dir/.memoree-contracts.XXXXXXXX")"

cargo run --quiet --locked --manifest-path "$repo_dir/Cargo.toml" --bin memoree -- schema \
  > "$temp_dir/schema-envelope.json"
jq -e 'select(.ok == true) | .result | select(type == "object")' \
  "$temp_dir/schema-envelope.json" > "$temp_dir/v1.json"

cargo run --quiet --locked --manifest-path "$repo_dir/Cargo.toml" --bin memoree -- \
  instructions --format markdown > "$temp_dir/instructions-envelope.json"
jq -e -j 'select(.ok == true) | .result.content | select(type == "string")' \
  "$temp_dir/instructions-envelope.json" > "$temp_dir/instructions.md"

mv -f "$temp_dir/v1.json" "$schema_dir/v1.json"
mv -f "$temp_dir/instructions.md" "$agents_dir/instructions.md"
