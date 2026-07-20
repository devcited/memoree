#!/bin/sh
set -eu

repo_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
dist_dir="${1:?usage: build-signed-release-manifest.sh DIST_DIR TAG}"
tag="${2:?usage: build-signed-release-manifest.sh DIST_DIR TAG}"
version="${tag#v}"
output="$dist_dir/memoree-release.json"
signature="$output.sig"
private_key=""

cleanup() {
  if [ -n "$private_key" ] && [ -f "$private_key" ]; then
    rm -f -- "$private_key"
  fi
}
trap cleanup 0 HUP INT TERM

[ "$tag" = "v$version" ] || {
  printf 'invalid release tag: %s\n' "$tag" >&2
  exit 1
}
[ -n "${MEMOREE_UPDATE_SIGNING_KEY_B64:-}" ] || {
  printf 'MEMOREE_UPDATE_SIGNING_KEY_B64 is required\n' >&2
  exit 1
}
for command_name in jq openssl sha256sum awk base64 mktemp tail sed; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command_name" >&2
    exit 1
  }
done

schema_version="$(sed -n 's/^pub const SCHEMA_VERSION: i64 = \([0-9][0-9]*\);/\1/p' "$repo_dir/src/store.rs" | head -1)"
installer_sha="$(sha256sum "$repo_dir/site/public/install.sh" | awk '{print $1}')"
published_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
targets='[]'

for triple in \
  aarch64-apple-darwin \
  x86_64-apple-darwin \
  aarch64-unknown-linux-musl \
  x86_64-unknown-linux-musl
do
  archive="memoree-$triple.tar.gz"
  archive_path="$dist_dir/$archive"
  checksum_path="$archive_path.sha256"
  [ -f "$archive_path" ] || {
    printf 'missing release archive: %s\n' "$archive_path" >&2
    exit 1
  }
  [ -f "$checksum_path" ] || {
    printf 'missing release checksum: %s\n' "$checksum_path" >&2
    exit 1
  }
  expected="$(awk 'NR == 1 {print $1}' "$checksum_path")"
  actual="$(sha256sum "$archive_path" | awk '{print $1}')"
  [ "$expected" = "$actual" ] || {
    printf 'checksum mismatch for %s\n' "$archive" >&2
    exit 1
  }
  targets="$(jq -cn \
    --argjson targets "$targets" \
    --arg triple "$triple" \
    --arg url "https://github.com/devcited/memoree/releases/download/$tag/$archive" \
    --arg sha256 "$actual" \
    '$targets + [{triple: $triple, archive_url: $url, sha256: $sha256}]')"
done

jq -n \
  --arg version "$version" \
  --arg tag "$tag" \
  --argjson store_schema_version "$schema_version" \
  --arg published_at "$published_at" \
  --arg installer_url "https://memoree.dev/install.sh" \
  --arg installer_sha "$installer_sha" \
  --argjson targets "$targets" \
  '{
    schema: 1,
    name: "memoree",
    version: $version,
    tag: $tag,
    store_schema_version: $store_schema_version,
    published_at: $published_at,
    installer: {url: $installer_url, sha256: $installer_sha},
    targets: $targets
  }' > "$output"

private_key="$(mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/memoree-update-key.XXXXXXXX")"
printf '%s' "$MEMOREE_UPDATE_SIGNING_KEY_B64" | base64 -d > "$private_key"
chmod 600 "$private_key"
embedded_public_key="$(
  sed -n 's/^const SIGNING_PUBLIC_KEY_B64: &str = "\([^"]*\)";/\1/p' \
    "$repo_dir/src/update.rs"
)"
derived_public_key="$(
  openssl pkey -in "$private_key" -pubout -outform DER 2>/dev/null |
    tail -c 32 |
    base64 -w 0
)"
[ -n "$embedded_public_key" ] || {
  printf 'could not read the embedded update public key\n' >&2
  exit 1
}
[ "$derived_public_key" = "$embedded_public_key" ] || {
  printf 'release signing key does not match the public key embedded in this binary\n' >&2
  exit 1
}
openssl pkeyutl -sign -rawin -inkey "$private_key" -in "$output" |
  base64 -w 0 > "$signature"
printf '\n' >> "$signature"

openssl pkey -in "$private_key" -pubout > "$dist_dir/memoree-update-public.pem"
base64 -d < "$signature" |
  openssl pkeyutl -verify -rawin \
    -pubin -inkey "$dist_dir/memoree-update-public.pem" \
    -in "$output" -sigfile /dev/stdin >/dev/null
rm -f -- "$dist_dir/memoree-update-public.pem"

printf 'created signed release manifest for %s\n' "$tag"
