#!/bin/sh
set -eu

repo_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$repo_dir"

case "$(uname -s):$(uname -m)" in
  Darwin:arm64 | Darwin:aarch64) target="aarch64-apple-darwin" ;;
  Darwin:x86_64 | Darwin:amd64) target="x86_64-apple-darwin" ;;
  Linux:aarch64 | Linux:arm64) target="aarch64-unknown-linux-musl" ;;
  Linux:x86_64 | Linux:amd64) target="x86_64-unknown-linux-musl" ;;
  *) printf 'unsupported updater-test host\n' >&2; exit 1 ;;
esac

test_root="$(mktemp -d "/tmp/memoree-updater-test.XXXXXXXX")"
user_root="$test_root/user"
memoree_home="$user_root/memoree-home"
install_dir="$user_root/bin"
server_root="$test_root/server"
server_pid=""

cleanup() {
  if [ -x "$install_dir/memoree" ]; then
    HOME="$user_root" MEMOREE_HOME="$memoree_home" MEMOREE_AUTO_UPDATE=off \
      "$install_dir/memoree" daemon stop >/dev/null 2>&1 || true
  fi
  if [ -n "$server_pid" ]; then
    kill "$server_pid" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$test_root"
}
trap cleanup EXIT HUP INT TERM

for command_name in openssl python3 base64; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command_name" >&2
    exit 1
  }
done

cargo build --locked --bin memoree
mkdir -p "$install_dir" "$memoree_home" "$server_root"
install -m 755 target/debug/memoree "$install_dir/memoree"
install -m 755 target/debug/memoree "$test_root/replacement-memoree"

private_key="$test_root/private.pem"
public_key="$test_root/public.pem"
openssl genpkey -algorithm ED25519 -out "$private_key" >/dev/null 2>&1
openssl pkey -in "$private_key" -pubout -out "$public_key" 2>/dev/null
test_public_key="$(openssl pkey -in "$private_key" -pubout -outform DER 2>/dev/null | tail -c 32 | base64 | tr -d '\r\n')"

marker="$test_root/installer-ran"
cat > "$server_root/install.sh" <<'INSTALLER'
#!/bin/sh
set -eu
[ "${MEMOREE_VERSION:-}" = "v0.4.1" ]
[ -n "${MEMOREE_INSTALL_DIR:-}" ]
[ -n "${MEMOREE_EXPECTED_ARCHIVE_SHA256:-}" ]
replacement="${MEMOREE_TEST_REPLACEMENT_BINARY:?}"
temporary="${MEMOREE_INSTALL_DIR}/.memoree-replacement.$$"
cp "$replacement" "$temporary"
chmod 755 "$temporary"
mv -f "$temporary" "${MEMOREE_INSTALL_DIR}/memoree"
printf 'installed\n' > "${MEMOREE_TEST_UPDATE_MARKER:?}"
INSTALLER
chmod 755 "$server_root/install.sh"
if command -v sha256sum >/dev/null 2>&1; then
  installer_sha="$(sha256sum "$server_root/install.sh" | awk '{print $1}')"
else
  installer_sha="$(shasum -a 256 "$server_root/install.sh" | awk '{print $1}')"
fi

port=$((22000 + $$ % 20000))
base_url="http://127.0.0.1:$port"
cat > "$server_root/memoree-release.json" <<EOF
{
  "schema": 1,
  "name": "memoree",
  "version": "0.4.1",
  "tag": "v0.4.1",
  "store_schema_version": 5,
  "published_at": "2026-07-20T00:00:00Z",
  "installer": {"url": "$base_url/install.sh", "sha256": "$installer_sha"},
  "targets": [{"triple": "$target", "archive_url": "$base_url/memoree-$target.tar.gz", "sha256": "0000000000000000000000000000000000000000000000000000000000000000"}]
}
EOF
openssl pkeyutl -sign -rawin -inkey "$private_key" \
  -in "$server_root/memoree-release.json" |
  base64 | tr -d '\r\n' > "$server_root/memoree-release.json.sig"
printf '\n' >> "$server_root/memoree-release.json.sig"
cat > "$server_root/latest.json" <<EOF
{
  "schema": 2,
  "name": "memoree",
  "version": "0.4.1",
  "tag": "v0.4.1",
  "signed_manifest_url": "$base_url/memoree-release.json",
  "signature_url": "$base_url/memoree-release.json.sig"
}
EOF

python3 -m http.server "$port" --bind 127.0.0.1 --directory "$server_root" \
  >"$test_root/server.log" 2>&1 &
server_pid=$!
ready=0
for _attempt in 1 2 3 4 5 6 7 8 9 10; do
  if curl -sf "$base_url/latest.json" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
test "$ready" -eq 1

export HOME="$user_root"
export MEMOREE_HOME="$memoree_home"
export MEMOREE_MANAGED_INSTALL=1
export MEMOREE_INSTALL_PREFIX="$install_dir"
export MEMOREE_UPDATE_FEED_URL="$base_url/latest.json"
export MEMOREE_UPDATE_ALLOW_INSECURE=1
export MEMOREE_TEST_UPDATE_PUBLIC_KEY="$test_public_key"
export MEMOREE_TEST_UPDATE_MARKER="$marker"
export MEMOREE_TEST_REPLACEMENT_BINARY="$test_root/replacement-memoree"
unset CI

"$install_dir/memoree" upgrade apply >/dev/null
"$install_dir/memoree" update status | grep -F '"managed_install":true' >/dev/null

python3 - "$install_dir/memoree" <<'PY' > "$test_root/pty-output"
import os, pty, select, sys, time

binary = sys.argv[1]
pid, fd = pty.fork()
if pid == 0:
    os.execve(binary, [binary, "capabilities"], os.environ.copy())

output = bytearray()
answered = False
deadline = time.time() + 20
while time.time() < deadline:
    readable, _, _ = select.select([fd], [], [], 0.2)
    if readable:
        try:
            chunk = os.read(fd, 65536)
        except OSError:
            break
        if not chunk:
            break
        output.extend(chunk)
        if not answered and b"Update and reconcile memory now?" in output:
            os.write(fd, b"y\n")
            answered = True
    finished, status = os.waitpid(pid, os.WNOHANG)
    if finished:
        if status != 0:
            raise SystemExit(os.waitstatus_to_exitcode(status))
        break
else:
    os.kill(pid, 9)
    raise SystemExit("automatic update PTY test timed out")

sys.stdout.buffer.write(output)
PY

grep -F 'Update and reconcile memory now?' "$test_root/pty-output" >/dev/null
grep -F '"product":"memoree"' "$test_root/pty-output" >/dev/null
test -f "$marker"

cp "$server_root/memoree-release.json" "$test_root/valid-release.json"
printf '\n' >> "$server_root/memoree-release.json"
if "$install_dir/memoree" update check >"$test_root/tamper-output" 2>&1; then
  printf 'tampered signed manifest was accepted\n' >&2
  exit 1
fi
grep -F 'signature verification failed' "$test_root/tamper-output" >/dev/null
cp "$test_root/valid-release.json" "$server_root/memoree-release.json"

second_marker="$test_root/tampered-installer-ran"
export MEMOREE_TEST_UPDATE_MARKER="$second_marker"
printf '\n# tampered\n' >> "$server_root/install.sh"
if "$install_dir/memoree" update apply >"$test_root/installer-tamper-output" 2>&1; then
  printf 'tampered installer was accepted\n' >&2
  exit 1
fi
grep -F 'installer SHA-256 verification failed' "$test_root/installer-tamper-output" >/dev/null
test ! -e "$second_marker"

printf 'signed automatic update flow passed in an isolated home\n'
