#!/bin/sh
set -eu

repo_dir="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$repo_dir"

case "$(uname -s):$(uname -m)" in
  Darwin:arm64 | Darwin:aarch64) target="aarch64-apple-darwin" ;;
  Darwin:x86_64 | Darwin:amd64) target="x86_64-apple-darwin" ;;
  Linux:aarch64 | Linux:arm64) target="aarch64-unknown-linux-musl" ;;
  Linux:x86_64 | Linux:amd64) target="x86_64-unknown-linux-musl" ;;
  *) printf 'unsupported installer-test host\n' >&2; exit 1 ;;
esac

# Keep the default Unix socket path below macOS's SUN_LEN limit even when the
# host-provided TMPDIR is a long per-user path.
test_root="$(mktemp -d "/tmp/memoree-installer-test.XXXXXXXX")"
server_root="$test_root/server"
user_root="$test_root/user"
install_dir="$user_root/bin"
memoree_home="$user_root/memoree home"
project_dir="$user_root/project"
server_pid=""

cleanup() {
  if [ -x "$install_dir/memoree" ]; then
    HOME="$user_root" MEMOREE_HOME="$memoree_home" \
      "$install_dir/memoree" daemon stop >/dev/null 2>&1 || true
  fi
  if [ -n "$server_pid" ]; then
    kill "$server_pid" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$test_root"
}
trap cleanup EXIT HUP INT TERM

cargo build --locked --bins
package_dir="$test_root/memoree-$target"
mkdir -p "$package_dir" "$server_root/releases/download/v0.4.0" \
  "$install_dir" "$memoree_home" "$project_dir" \
  "$user_root/.codex" "$user_root/.claude"
install -m 755 target/debug/memoree "$package_dir/memoree"
install -m 755 target/debug/memoree-eval "$package_dir/memoree-eval"
archive="$server_root/releases/download/v0.4.0/memoree-$target.tar.gz"
tar -czf "$archive" -C "$test_root" "memoree-$target"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$archive" > "$archive.sha256"
else
  shasum -a 256 "$archive" > "$archive.sha256"
fi
install -m 644 site/public/releases/latest.txt "$server_root/releases/latest.txt"

openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
  -keyout "$test_root/key.pem" -out "$test_root/cert.pem" \
  -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost' >/dev/null 2>&1
port=$((20000 + $$ % 20000))
python3 -c 'import http.server, ssl, sys; root,key,cert,port=sys.argv[1:]; handler=lambda *a, **kw: http.server.SimpleHTTPRequestHandler(*a, directory=root, **kw); server=http.server.ThreadingHTTPServer(("127.0.0.1", int(port)), handler); context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); context.load_cert_chain(cert,key); server.socket=context.wrap_socket(server.socket,server_side=True); server.serve_forever()' \
  "$server_root" "$test_root/key.pem" "$test_root/cert.pem" "$port" \
  >"$test_root/server.log" 2>&1 &
server_pid=$!

ready=0
for _attempt in 1 2 3 4 5 6 7 8 9 10; do
  if CURL_CA_BUNDLE="$test_root/cert.pem" \
    curl --proto '=https' --tlsv1.2 -sfL \
      "https://localhost:$port/releases/latest.txt" >/dev/null; then
    ready=1
    break
  fi
  sleep 1
done
test "$ready" -eq 1

old_archive="$test_root/memoree-v0.2.0.tar.gz"
curl --proto '=https' --tlsv1.2 -fsSL --retry 3 \
  -o "$old_archive" \
  "https://memoree.dev/releases/download/v0.2.0/memoree-$target.tar.gz"
tar -xzf "$old_archive" -C "$test_root"
install -m 755 "$test_root/memoree-$target/memoree" "$install_dir/memoree"
install -m 755 "$test_root/memoree-$target/memoree-eval" "$install_dir/memoree-eval"

HOME="$user_root" MEMOREE_HOME="$memoree_home" CODEX_HOME="$user_root/.codex" \
  CLAUDE_CONFIG_DIR="$user_root/.claude" \
  "$install_dir/memoree" init --name installer-upgrade --directory "$project_dir" >/dev/null
HOME="$user_root" MEMOREE_HOME="$memoree_home" \
  "$install_dir/memoree" doctor >/dev/null
(
  cd "$project_dir"
  HOME="$user_root" MEMOREE_HOME="$memoree_home" \
    "$install_dir/memoree" remember --raw --apply \
      'Installer fixture survives the automatic v0.2 to v0.3 migration.' >/dev/null
)

CURL_CA_BUNDLE="$test_root/cert.pem" \
  MEMOREE_RELEASE_ROOT="https://localhost:$port/releases" \
  MEMOREE_INSTALL_DIR="$install_dir" HOME="$user_root" \
  MEMOREE_HOME="$memoree_home" CODEX_HOME="$user_root/.codex" \
  CLAUDE_CONFIG_DIR="$user_root/.claude" \
  sh site/public/install.sh >/dev/null

"$install_dir/memoree" --version | grep -F '0.4.0' >/dev/null
HOME="$user_root" MEMOREE_HOME="$memoree_home" \
  "$install_dir/memoree" update status | grep -F '"managed_install":true' >/dev/null
HOME="$user_root" MEMOREE_HOME="$memoree_home" \
  "$install_dir/memoree" doctor | grep -F '"binary_version":"0.4.0"' >/dev/null
(
  cd "$project_dir"
  HOME="$user_root" MEMOREE_HOME="$memoree_home" \
    "$install_dir/memoree" recall installer fixture migration \
      | grep -F '"presence":"artifacts_only"' >/dev/null
)
test -f "$user_root/.codex/skills/use-memoree/SKILL.md"
test -f "$user_root/.claude/skills/use-memoree/SKILL.md"
find "$memoree_home/data/migration-backups" -mindepth 1 -maxdepth 1 -type d \
  | grep . >/dev/null
