#!/bin/sh
set -eu

release_root="${MEMOREE_RELEASE_ROOT:-https://memoree.dev/releases}"
version="${MEMOREE_VERSION:-}"
install_dir="${MEMOREE_INSTALL_DIR:-}"
temp_dir=""

fail() {
  printf 'memoree installer: %s\n' "$1" >&2
  exit 1
}

cleanup() {
  if [ -n "$temp_dir" ] && [ -d "$temp_dir" ]; then
    rm -rf -- "$temp_dir"
  fi
}
trap cleanup 0
trap 'exit 1' HUP INT TERM

for command_name in curl tar awk uname mktemp mkdir install mv cp rm grep tr; do
  command -v "$command_name" >/dev/null 2>&1 || fail "$command_name is required"
done

if [ -z "$version" ]; then
  version="$(curl --proto '=https' --tlsv1.2 -sfL "$release_root/latest.txt" | tr -d '\r\n')"
fi
printf '%s' "$version" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+([-.][0-9A-Za-z.-]+)?$' ||
  fail "invalid version: $version"

kernel="$(uname -s)"
machine="$(uname -m)"
case "$kernel:$machine" in
  Darwin:arm64 | Darwin:aarch64) target="aarch64-apple-darwin" ;;
  Darwin:x86_64 | Darwin:amd64) target="x86_64-apple-darwin" ;;
  Linux:aarch64 | Linux:arm64) target="aarch64-unknown-linux-musl" ;;
  Linux:x86_64 | Linux:amd64) target="x86_64-unknown-linux-musl" ;;
  *) fail "unsupported platform: $kernel $machine" ;;
esac

if [ -z "$install_dir" ]; then
  if [ -n "${CARGO_HOME:-}" ]; then
    install_dir="$CARGO_HOME/bin"
  else
    [ -n "${HOME:-}" ] || fail "HOME is required when MEMOREE_INSTALL_DIR is unset"
    install_dir="$HOME/.local/bin"
  fi
fi

archive="memoree-$target.tar.gz"
download_root="$release_root/download/$version"
archive_url="${MEMOREE_ARCHIVE_URL:-$download_root/$archive}"
signed_expected="${MEMOREE_EXPECTED_ARCHIVE_SHA256:-}"
temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/memoree-install.XXXXXXXX")"
archive_path="$temp_dir/$archive"
checksum_path="$archive_path.sha256"
previous_version=""
previous_daemon_running=0
rollback_memoree="$temp_dir/rollback-memoree"
rollback_eval="$temp_dir/rollback-memoree-eval"

if [ -x "$install_dir/memoree" ]; then
  previous_version="$("$install_dir/memoree" --version 2>/dev/null | awk 'NR == 1 { print $2 }')"
  cp -p "$install_dir/memoree" "$rollback_memoree"
  if [ -x "$install_dir/memoree-eval" ]; then
    cp -p "$install_dir/memoree-eval" "$rollback_eval"
  fi
  if (
    unset MEMOREE_ENDPOINT MEMOREE_NO_AUTOSTART
    "$install_dir/memoree" daemon status >/dev/null 2>&1
  ); then
    previous_daemon_running=1
  fi
fi

printf 'Downloading memoree %s for %s...\n' "$version" "$target"
curl --proto '=https' --tlsv1.2 -fsSL --retry 3 -o "$archive_path" "$archive_url"
if [ -n "$signed_expected" ]; then
  expected="$signed_expected"
else
  curl --proto '=https' --tlsv1.2 -fsSL --retry 3 -o "$checksum_path" "$download_root/$archive.sha256"
  expected="$(awk 'NR == 1 { print $1 }' "$checksum_path")"
fi
printf '%s' "$expected" | grep -Eq '^[0-9a-fA-F]{64}$' || fail "invalid SHA-256 checksum file"
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "$archive_path" | awk '{ print $1 }')"
elif command -v shasum >/dev/null 2>&1; then
  actual="$(shasum -a 256 "$archive_path" | awk '{ print $1 }')"
else
  fail "sha256sum or shasum is required"
fi
[ "$actual" = "$expected" ] || fail "SHA-256 verification failed"

mkdir -p "$temp_dir/unpack" "$install_dir"
tar -xzf "$archive_path" -C "$temp_dir/unpack"
package_dir="$temp_dir/unpack/memoree-$target"
[ -f "$package_dir/memoree" ] || fail "release archive does not contain memoree"
[ -f "$package_dir/memoree-eval" ] || fail "release archive does not contain memoree-eval"

install -m 755 "$package_dir/memoree" "$install_dir/.memoree.new.$$"
install -m 755 "$package_dir/memoree-eval" "$install_dir/.memoree-eval.new.$$"
mv -f "$install_dir/.memoree-eval.new.$$" "$install_dir/memoree-eval" ||
  fail "could not install memoree-eval"
if ! mv -f "$install_dir/.memoree.new.$$" "$install_dir/memoree"; then
  if [ -x "$rollback_eval" ]; then
    cp -p "$rollback_eval" "$install_dir/memoree-eval"
  else
    rm -f -- "$install_dir/memoree-eval"
  fi
  fail "could not install memoree"
fi

upgrade_status=0
if [ "$previous_daemon_running" -eq 1 ]; then
  MEMOREE_MANAGED_INSTALL=1 MEMOREE_INSTALL_PREFIX="$install_dir" \
    "$install_dir/memoree" upgrade apply \
    --previous-version "$previous_version" \
    --legacy-default-was-running || upgrade_status=$?
elif [ -n "$previous_version" ]; then
  MEMOREE_MANAGED_INSTALL=1 MEMOREE_INSTALL_PREFIX="$install_dir" \
    "$install_dir/memoree" upgrade apply \
    --previous-version "$previous_version" || upgrade_status=$?
else
  MEMOREE_MANAGED_INSTALL=1 MEMOREE_INSTALL_PREFIX="$install_dir" \
    "$install_dir/memoree" upgrade apply || upgrade_status=$?
fi

if [ "$upgrade_status" -ne 0 ] && [ "$upgrade_status" -ne 20 ]; then
  rollback_safe=0
  "$install_dir/memoree" upgrade rollback-safe >/dev/null 2>&1 || rollback_safe=$?
  if [ "$rollback_safe" -eq 0 ] && [ -x "$rollback_memoree" ]; then
    mv -f "$rollback_memoree" "$install_dir/memoree"
    if [ -x "$rollback_eval" ]; then
      mv -f "$rollback_eval" "$install_dir/memoree-eval"
    fi
    if [ "$previous_daemon_running" -eq 1 ]; then
      (
        unset MEMOREE_ENDPOINT MEMOREE_NO_AUTOSTART
        "$install_dir/memoree" daemon restart >/dev/null
      ) || fail "upgrade failed; previous binaries were restored but the previous daemon could not be restarted"
    fi
    fail "upgrade reconciliation failed; previous binaries were restored"
  fi
  if [ "$rollback_safe" -eq 0 ]; then
    fail "post-install reconciliation failed; no previous binary was available to restore"
  fi
  fail 'upgrade reconciliation failed after the store reached the new schema; the new binaries were retained and memoree upgrade apply can be retried'
fi

printf 'Installed and reconciled memoree %s in %s\n' "$version" "$install_dir"
if [ "$upgrade_status" -eq 20 ]; then
  fail "the binary is installed, but a supervised daemon or post-migration health check requires attention"
fi
