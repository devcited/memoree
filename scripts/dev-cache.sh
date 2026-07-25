#!/bin/sh
set -eu

repo_dir=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
target_dir="$repo_dir/target"
debug_dir="$target_dir/debug"
watch_dir="$target_dir/watch"
watch_pid_file="$watch_dir/.memoree-watch.pid"
watch_limit_gib=${MEMOREE_WATCH_CACHE_LIMIT_GIB:-12}

usage() {
    echo "usage: $0 {size|clean|watch [cargo-watch arguments...]}" >&2
    exit 2
}

size_kib() {
    if [ -d "$1" ]; then
        du -sk "$1" | awk '{print $1}'
    else
        echo 0
    fi
}

print_size() {
    label=$1
    path=$2
    if [ -d "$path" ]; then
        printf '%-18s %s\n' "$label" "$(du -sh "$path" | awk '{print $1}')"
    else
        printf '%-18s %s\n' "$label" "0B"
    fi
}

watch_is_running() {
    [ -f "$watch_pid_file" ] || return 1
    watch_pid=$(sed -n '1p' "$watch_pid_file")
    case "$watch_pid" in
        ''|*[!0-9]*) return 1 ;;
    esac
    kill -0 "$watch_pid" 2>/dev/null
}

debug_build_is_running() {
    lock_file="$debug_dir/.cargo-lock"
    [ -f "$lock_file" ] || return 1
    command -v lsof >/dev/null 2>&1 || {
        echo "refusing cleanup: lsof is required to verify that Cargo is idle" >&2
        exit 1
    }
    lsof -t "$lock_file" >/dev/null 2>&1
}

remove_cache_dir() {
    cache_path=$1
    case "$cache_path" in
        "$debug_dir"/.fingerprint|\
        "$debug_dir"/build|\
        "$debug_dir"/deps|\
        "$debug_dir"/examples|\
        "$debug_dir"/incremental|\
        "$watch_dir")
            ;;
        *)
            echo "refusing unexpected cache path: $cache_path" >&2
            exit 1
            ;;
    esac
    [ ! -e "$cache_path" ] || rm -rf -- "$cache_path"
}

show_sizes() {
    print_size "target total:" "$target_dir"
    print_size "debug profile:" "$debug_dir"
    print_size "watch cache:" "$watch_dir"
    print_size "release profile:" "$target_dir/release"
}

clean_dev() {
    if watch_is_running; then
        echo "refusing cleanup: cargo dev-watch is running as PID $watch_pid" >&2
        exit 1
    fi
    if debug_build_is_running; then
        echo "refusing cleanup: Cargo is writing to the debug profile" >&2
        exit 1
    fi

    before_kib=$(size_kib "$target_dir")
    remove_cache_dir "$debug_dir/.fingerprint"
    remove_cache_dir "$debug_dir/build"
    remove_cache_dir "$debug_dir/deps"
    remove_cache_dir "$debug_dir/examples"
    remove_cache_dir "$debug_dir/incremental"
    remove_cache_dir "$watch_dir"
    after_kib=$(size_kib "$target_dir")
    reclaimed_kib=$((before_kib - after_kib))

    printf 'reclaimed %.1f GiB of rebuildable development caches\n' \
        "$(awk -v kib="$reclaimed_kib" 'BEGIN { print kib / 1048576 }')"
    echo "retained top-level debug binaries and all release/dist artifacts"
    show_sizes
}

run_watch() {
    case "$watch_limit_gib" in
        ''|*[!0-9]*)
            echo "MEMOREE_WATCH_CACHE_LIMIT_GIB must be a whole number" >&2
            exit 2
            ;;
    esac

    if watch_is_running; then
        echo "cargo dev-watch is already running as PID $watch_pid" >&2
        exit 1
    fi

    watch_limit_kib=$((watch_limit_gib * 1048576))
    watch_kib=$(size_kib "$watch_dir")
    if [ "$watch_kib" -gt "$watch_limit_kib" ]; then
        printf 'watch cache exceeds %s GiB; cleaning it before startup\n' \
            "$watch_limit_gib"
        remove_cache_dir "$watch_dir"
    fi

    mkdir -p "$watch_dir"
    echo "$$" > "$watch_pid_file"
    cleanup_pid_file() {
        rm -f -- "$watch_pid_file"
    }
    trap cleanup_pid_file EXIT HUP INT TERM

    CARGO_TARGET_DIR="$watch_dir" cargo watch "$@"
}

case "${1:-}" in
    size)
        [ "$#" -eq 1 ] || usage
        show_sizes
        ;;
    clean)
        [ "$#" -eq 1 ] || usage
        clean_dev
        ;;
    watch)
        shift
        run_watch "$@"
        ;;
    *)
        usage
        ;;
esac
