#![cfg(unix)]

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::Duration,
};

use rusqlite::Connection;
use serde_json::Value;

fn current_binary() -> &'static Path {
    assert_cmd::cargo::cargo_bin!("memoree")
}

fn run(binary: &Path, cwd: &Path, memoree_home: &Path, user_home: &Path, args: &[&str]) -> Output {
    Command::new(binary)
        .current_dir(cwd)
        .env("MEMOREE_HOME", memoree_home)
        .env("HOME", user_home)
        .env("CODEX_HOME", user_home.join(".codex"))
        .env("CLAUDE_CONFIG_DIR", user_home.join(".claude"))
        .env_remove("MEMOREE_ENDPOINT")
        .env_remove("MEMOREE_NO_AUTOSTART")
        .args(args)
        .output()
        .expect("run Memoree binary")
}

fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout was not JSON: {error}; status={}; stdout={}; stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
#[ignore = "requires MEMOREE_V02_BINARY pointing at the verified v0.2.0 release binary"]
fn real_v02_running_store_upgrades_to_v04_automatically() {
    let old_binary =
        PathBuf::from(env::var_os("MEMOREE_V02_BINARY").expect("MEMOREE_V02_BINARY is required"));
    assert!(old_binary.is_file(), "{} is missing", old_binary.display());

    let temporary = tempfile::tempdir().unwrap();
    let cwd = temporary.path().join("project with spaces");
    let memoree_home = temporary.path().join("memoree home");
    let user_home = temporary.path().join("user home");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(user_home.join(".codex")).unwrap();
    fs::create_dir_all(user_home.join(".claude")).unwrap();

    let initialized = run(
        &old_binary,
        &cwd,
        &memoree_home,
        &user_home,
        &["init", "--name", "upgrade-fixture"],
    );
    assert!(initialized.status.success(), "{}", json(&initialized));
    let remembered = run(
        &old_binary,
        &cwd,
        &memoree_home,
        &user_home,
        &[
            "remember",
            "--raw",
            "--apply",
            "The v0.2 upgrade fixture preserves this exact authority record.",
        ],
    );
    assert!(remembered.status.success(), "{}", json(&remembered));
    let old_doctor = run(&old_binary, &cwd, &memoree_home, &user_home, &["doctor"]);
    assert!(old_doctor.status.success(), "{}", json(&old_doctor));
    assert!(json(&old_doctor)["result"].get("binary_version").is_none());

    let upgraded = run(
        current_binary(),
        &cwd,
        &memoree_home,
        &user_home,
        &[
            "upgrade",
            "apply",
            "--previous-version",
            "0.2.0",
            "--legacy-default-was-running",
        ],
    );
    let upgraded_json = json(&upgraded);
    assert!(upgraded.status.success(), "{upgraded_json}");
    assert_eq!(upgraded_json["result"]["authority"]["schema_version"], 5);
    assert_eq!(upgraded_json["result"]["daemon"]["state"], "restarted");
    assert_eq!(
        upgraded_json["result"]["daemon"]["doctor"]["binary_version"],
        env!("CARGO_PKG_VERSION")
    );
    let backup = upgraded_json["result"]["authority"]["migration"]["backup_destination"]
        .as_str()
        .expect("migration backup path");
    let backup_connection = Connection::open(Path::new(backup).join("memoree.sqlite3")).unwrap();
    let backup_schema: i64 = backup_connection
        .query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(backup_schema, 3);

    let recalled = run(
        current_binary(),
        &cwd,
        &memoree_home,
        &user_home,
        &["recall", "upgrade", "fixture", "authority"],
    );
    let recalled_json = json(&recalled);
    assert!(recalled.status.success(), "{recalled_json}");
    assert_ne!(recalled_json["result"]["presence"], "none");
    assert!(
        user_home
            .join(".codex/skills/use-memoree/SKILL.md")
            .is_file()
    );
    assert!(
        user_home
            .join(".claude/skills/use-memoree/SKILL.md")
            .is_file()
    );

    let stopped = run(
        current_binary(),
        &cwd,
        &memoree_home,
        &user_home,
        &["daemon", "stop"],
    );
    assert!(stopped.status.success(), "{}", json(&stopped));

    let live_data_dir = memoree_home.join("data");
    let database_path = live_data_dir.join("memoree.sqlite3");
    let database_before = fs::read(&database_path).unwrap();
    let old_socket = temporary.path().join("old-reopen.sock");
    let mut old_reopen = Command::new(&old_binary)
        .current_dir(&cwd)
        .env("MEMOREE_HOME", &memoree_home)
        .args([
            "serve",
            "--listen",
            &format!("unix://{}", old_socket.display()),
            "--data-dir",
            live_data_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut refused = None;
    for _ in 0..100 {
        if let Some(status) = old_reopen.try_wait().unwrap() {
            refused = Some(status);
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    if refused.is_none() {
        let _ = old_reopen.kill();
        let _ = old_reopen.wait();
        panic!("v0.2.0 unexpectedly opened and served a schema-4 store");
    }
    assert!(!refused.unwrap().success());
    assert_eq!(fs::read(database_path).unwrap(), database_before);
}

#[test]
#[ignore = "requires MEMOREE_V02_BINARY pointing at the verified v0.2.0 release binary"]
fn real_v02_stopped_store_upgrades_to_v04_without_starting_a_daemon() {
    let old_binary =
        PathBuf::from(env::var_os("MEMOREE_V02_BINARY").expect("MEMOREE_V02_BINARY is required"));
    let temporary = tempfile::tempdir().unwrap();
    let cwd = temporary.path().join("project");
    let memoree_home = temporary.path().join("memoree-home");
    let user_home = temporary.path().join("user-home");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(user_home.join(".codex")).unwrap();

    let initialized = run(
        &old_binary,
        &cwd,
        &memoree_home,
        &user_home,
        &["init", "--name", "stopped-upgrade-fixture"],
    );
    assert!(initialized.status.success(), "{}", json(&initialized));
    let remembered = run(
        &old_binary,
        &cwd,
        &memoree_home,
        &user_home,
        &[
            "remember",
            "--raw",
            "--apply",
            "A stopped v0.2 daemon must remain stopped after upgrade.",
        ],
    );
    assert!(remembered.status.success(), "{}", json(&remembered));
    let stopped = run(
        &old_binary,
        &cwd,
        &memoree_home,
        &user_home,
        &["daemon", "stop"],
    );
    assert!(stopped.status.success(), "{}", json(&stopped));

    let upgraded = run(
        current_binary(),
        &cwd,
        &memoree_home,
        &user_home,
        &["upgrade", "apply", "--previous-version", "0.2.0"],
    );
    let report = json(&upgraded);
    assert!(upgraded.status.success(), "{report}");
    assert_eq!(report["result"]["authority"]["schema_version"], 5);
    assert_eq!(report["result"]["daemon"]["state"], "remained_stopped");
    assert!(!memoree_home.join("run/memoree.sock").exists());
}
