#![cfg(unix)]

use std::{
    fs,
    io::Write as _,
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn binary() -> &'static Path {
    assert_cmd::cargo::cargo_bin!("memoree")
}

fn invoke(cwd: &Path, endpoint: &str, args: &[&str]) -> (Output, Value) {
    let output = Command::new(binary())
        .current_dir(cwd)
        .arg("--endpoint")
        .arg(endpoint)
        .args(args)
        .output()
        .expect("run memoree CLI");
    let envelope = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "CLI stdout was not a JSON envelope: {error}; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, envelope)
}

fn invoke_local(cwd: &Path, home: &Path, args: &[&str]) -> (Output, Value) {
    let output = Command::new(binary())
        .current_dir(cwd)
        .env("MEMOREE_HOME", home)
        .env_remove("MEMOREE_ENDPOINT")
        .env_remove("MEMOREE_NO_AUTOSTART")
        .args(args)
        .output()
        .expect("run local memoree CLI");
    let envelope = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "CLI stdout was not a JSON envelope: {error}; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, envelope)
}

fn invoke_upgrade(
    cwd: &Path,
    memoree_home: &Path,
    user_home: &Path,
    args: &[&str],
) -> (Output, Value) {
    let output = Command::new(binary())
        .current_dir(cwd)
        .env("MEMOREE_HOME", memoree_home)
        .env("HOME", user_home)
        .env("CODEX_HOME", user_home.join(".codex"))
        .env("CLAUDE_CONFIG_DIR", user_home.join(".claude"))
        .env_remove("MEMOREE_ENDPOINT")
        .env_remove("MEMOREE_NO_AUTOSTART")
        .args(args)
        .output()
        .expect("run local memoree upgrade");
    let envelope = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "upgrade stdout was not JSON: {error}; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, envelope)
}

fn invoke_call(cwd: &Path, home: &Path, request: &Value) -> (Output, Value) {
    let mut child = Command::new(binary())
        .current_dir(cwd)
        .env("MEMOREE_HOME", home)
        .env_remove("MEMOREE_ENDPOINT")
        .env_remove("MEMOREE_NO_AUTOSTART")
        .arg("call")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("run canonical memoree call");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(request).unwrap())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    let envelope = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "call stdout was not a JSON envelope: {error}; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output, envelope)
}

struct LocalDaemonGuard {
    cwd: PathBuf,
    home: PathBuf,
}

impl Drop for LocalDaemonGuard {
    fn drop(&mut self) {
        let _ = Command::new(binary())
            .current_dir(&self.cwd)
            .env("MEMOREE_HOME", &self.home)
            .env_remove("MEMOREE_ENDPOINT")
            .args(["daemon", "stop"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn wait_for_socket(path: &Path, server: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        if let Some(status) = server.try_wait().expect("poll memoree daemon") {
            panic!("memoree daemon exited before readiness: {status}");
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("memoree daemon did not create {}", path.display());
}

#[test]
fn raw_remember_previews_without_a_daemon_and_applies_idempotently() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("client");
    let home = root.path().join("home");
    fs::create_dir_all(&cwd).unwrap();
    let _guard = LocalDaemonGuard {
        cwd: cwd.clone(),
        home: home.clone(),
    };

    let (status, initialized) = invoke_local(&cwd, &home, &["init", "--name", "remember-e2e"]);
    assert!(status.status.success(), "{initialized}");

    let source = "Remember raw evidence without inference.";
    let (status, preview) = invoke_local(&cwd, &home, &["remember", "--raw", source]);
    assert!(status.status.success(), "{preview}");
    assert_eq!(preview["result"]["applied"], false);
    assert_eq!(preview["result"]["compiler"]["mode"], "raw");
    assert!(preview["result"]["artifact"].is_null());

    let (status, stopped) = invoke_local(&cwd, &home, &["daemon", "status"]);
    assert_eq!(status.status.code(), Some(1), "{stopped}");

    let (status, applied) = invoke_local(&cwd, &home, &["remember", "--raw", "--apply", source]);
    assert!(status.status.success(), "{applied}");
    assert_eq!(applied["result"]["applied"], true);
    assert_eq!(applied["result"]["artifact"]["created"], true);
    let artifact_id = applied["result"]["artifact"]["artifact_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let (status, replayed) = invoke_local(&cwd, &home, &["remember", "--raw", "--apply", source]);
    assert!(status.status.success(), "{replayed}");
    assert_eq!(replayed["result"]["artifact"]["created"], false);
    assert_eq!(replayed["result"]["artifact"]["artifact_id"], artifact_id);

    let (status, search) = invoke_local(&cwd, &home, &["search", "raw", "evidence"]);
    assert!(status.status.success(), "{search}");
    assert_eq!(search["result"]["hits"].as_array().unwrap().len(), 1);
    assert_eq!(search["result"]["hits"][0]["entity_id"], artifact_id);
}

#[test]
fn recall_returns_claims_with_exact_refs_and_honest_empty_states() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("client");
    let home = root.path().join("home");
    fs::create_dir_all(&cwd).unwrap();
    let _guard = LocalDaemonGuard {
        cwd: cwd.clone(),
        home: home.clone(),
    };
    let (status, initialized) = invoke_local(&cwd, &home, &["init", "--name", "recall-e2e"]);
    assert!(status.status.success(), "{initialized}");

    let source = "Primary source: SQLite is authoritative. artifact-only-token";
    let source_path = cwd.join("source.txt");
    fs::write(&source_path, source).unwrap();
    let (status, artifact) = invoke_local(
        &cwd,
        &home,
        &[
            "artifact",
            "put",
            source_path.to_str().unwrap(),
            "--kind",
            "decision",
            "--title",
            "Storage source",
        ],
    );
    assert!(status.status.success(), "{artifact}");
    let artifact_id = artifact["result"]["artifact_id"].as_str().unwrap();
    let revision_id = artifact["result"]["revision_id"].as_str().unwrap();
    let quote = "SQLite is authoritative";
    let start = source.find(quote).unwrap();
    let evidence = format!(
        "{artifact_id}@{revision_id}#{start}-{}",
        start + quote.len()
    );
    let (status, claim) = invoke_local(
        &cwd,
        &home,
        &[
            "claim",
            "assert",
            "decision",
            "SQLite is authoritative for durable memory.",
            "--evidence",
            &evidence,
        ],
    );
    assert!(status.status.success(), "{claim}");
    let claim_id = claim["result"]["claim_id"].as_str().unwrap();

    let (status, recall) = invoke_local(&cwd, &home, &["recall", "SQLite", "authoritative"]);
    assert!(status.status.success(), "{recall}");
    assert_eq!(recall["result"]["presence"], "claims");
    assert_eq!(recall["result"]["claims"][0]["claim_id"], claim_id);
    assert_eq!(
        recall["result"]["claims"][0]["evidence"][0]["excerpt"],
        quote
    );
    assert!(
        recall["result"]["claims"][0]["evidence"][0]["citation"]
            .as_str()
            .unwrap()
            .ends_with(&format!("#{start}-{}", start + quote.len()))
    );
    assert_eq!(
        recall["result"]["artifact_refs"][0]["artifact_id"],
        artifact_id
    );

    let (status, artifacts_only) = invoke_local(&cwd, &home, &["recall", "artifact-only-token"]);
    assert!(status.status.success(), "{artifacts_only}");
    assert_eq!(artifacts_only["result"]["presence"], "artifacts_only");
    assert!(
        artifacts_only["result"]["claims"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        artifacts_only["result"]["artifact_refs"][0]["artifact_id"],
        artifact_id
    );

    let (status, none) = invoke_local(&cwd, &home, &["recall", "zzzxqv987654"]);
    assert!(status.status.success(), "{none}");
    assert_eq!(none["result"]["presence"], "none");
    assert_eq!(none["result"]["searched_horizons"][0], "ambient");
    assert!(none["result"]["claims"].as_array().unwrap().is_empty());
    assert!(
        none["result"]["artifact_refs"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let (status, bundle) = invoke_local(
        &cwd,
        &home,
        &[
            "context",
            "build",
            "SQLite",
            "authoritative",
            "--max-bytes",
            "4096",
        ],
    );
    assert!(status.status.success(), "{bundle}");
    assert!(
        bundle["result"]["rendered_markdown"]
            .as_str()
            .unwrap()
            .contains(&format!(
                "memoree://artifact/{artifact_id}@{revision_id}#{start}-{}",
                start + quote.len()
            ))
    );
}

#[test]
fn checkpoints_are_private_last_write_wins_and_absent_from_recall() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("client");
    let home = root.path().join("home");
    fs::create_dir_all(&cwd).unwrap();
    let _guard = LocalDaemonGuard {
        cwd: cwd.clone(),
        home: home.clone(),
    };
    let (status, initialized) = invoke_local(&cwd, &home, &["init", "--name", "checkpoint-e2e"]);
    assert!(status.status.success(), "{initialized}");

    let long_note = format!("continuity ZZZCHECKPOINT7421 {}", "x".repeat(10_000));
    let (status, first) = invoke_local(
        &cwd,
        &home,
        &[
            "checkpoint",
            "--session",
            "session-1",
            "--task",
            "task-1",
            &long_note,
        ],
    );
    assert!(status.status.success(), "{first}");
    assert_eq!(first["result"]["pending"], true);
    assert_eq!(first["result"]["recallable"], false);
    assert_eq!(first["result"]["checkpoint"]["truncated"], true);
    assert!(
        first["result"]["checkpoint"]["stored_bytes"]
            .as_u64()
            .unwrap()
            <= 4096
    );
    let checkpoint_id = first["result"]["checkpoint"]["checkpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let (status, stopped) = invoke_local(&cwd, &home, &["daemon", "status"]);
    assert_eq!(status.status.code(), Some(1), "{stopped}");

    let (status, second) = invoke_local(
        &cwd,
        &home,
        &[
            "checkpoint",
            "--session",
            "session-1",
            "later deliberate note",
        ],
    );
    assert!(status.status.success(), "{second}");
    assert_eq!(
        second["result"]["checkpoint"]["checkpoint_id"],
        checkpoint_id
    );
    let (status, listed) = invoke_local(&cwd, &home, &["pending", "list"]);
    assert!(status.status.success(), "{listed}");
    assert_eq!(listed["result"]["checkpoints"].as_array().unwrap().len(), 1);
    let (status, shown) = invoke_local(&cwd, &home, &["pending", "show", &checkpoint_id]);
    assert!(status.status.success(), "{shown}");
    assert_eq!(
        shown["result"]["checkpoint"]["text"],
        "later deliberate note"
    );

    let (status, recall) = invoke_local(&cwd, &home, &["recall", "ZZZCHECKPOINT7421"]);
    assert!(status.status.success(), "{recall}");
    assert_eq!(recall["result"]["presence"], "none");

    let (status, flagged) = invoke_local(
        &cwd,
        &home,
        &[
            "checkpoint",
            "--session",
            "session-secret",
            "Authorization: Bearer eyJfake",
        ],
    );
    assert!(status.status.success(), "{flagged}");
    assert_eq!(
        flagged["result"]["checkpoint"]["sensitive_flags"][0],
        "bearer_token"
    );
    let flagged_id = flagged["result"]["checkpoint"]["checkpoint_id"]
        .as_str()
        .unwrap();
    let (status, rejected) = invoke_local(&cwd, &home, &["pending", "apply", flagged_id]);
    assert_eq!(status.status.code(), Some(2), "{rejected}");
    assert_eq!(rejected["error"]["code"], "INVALID_REQUEST");
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--allow-flagged")
    );

    let (status, dropped) = invoke_local(&cwd, &home, &["pending", "drop", &checkpoint_id]);
    assert!(status.status.success(), "{dropped}");
    assert_eq!(dropped["result"]["dropped"], true);
    assert_eq!(dropped["result"]["recoverable"], false);
}

#[test]
fn missing_compiler_logins_fail_loudly_without_writing_or_using_api_keys() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("client");
    let home = root.path().join("home");
    let fake_bin = root.path().join("bin");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(&fake_bin).unwrap();
    let _guard = LocalDaemonGuard {
        cwd: cwd.clone(),
        home: home.clone(),
    };

    let (status, initialized) = invoke_local(&cwd, &home, &["init", "--name", "auth-failure"]);
    assert!(status.status.success(), "{initialized}");
    let fake_codex = fake_bin.join("codex");
    fs::write(&fake_codex, "#!/bin/sh\nexit 42\n").unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();

    let output = Command::new(binary())
        .current_dir(&cwd)
        .env("MEMOREE_HOME", &home)
        .env("PATH", &fake_bin)
        .env("OPENAI_API_KEY", "must-not-be-used")
        .args([
            "remember",
            "--apply",
            "This source must not be written after compiler failure.",
        ])
        .output()
        .unwrap();
    let envelope: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(output.status.code(), Some(2), "{envelope}");
    assert_eq!(envelope["error"]["code"], "CONFIG_ERROR");
    assert!(
        envelope["error"]["message"]
            .as_str()
            .unwrap()
            .contains("codex login")
    );
    assert!(
        envelope["error"]["message"]
            .as_str()
            .unwrap()
            .contains("claude auth login")
    );
    assert!(
        envelope["error"]["message"]
            .as_str()
            .unwrap()
            .contains("memoree compiler configure")
    );

    let (status, stopped) = invoke_local(&cwd, &home, &["daemon", "status"]);
    assert_eq!(status.status.code(), Some(1), "{stopped}");
    assert_eq!(stopped["result"]["running"], false);
}

#[test]
fn cli_round_trip_context_isolation_and_explicit_broadening() {
    let root = tempfile::tempdir().unwrap();
    let data = root.path().join("data");
    let socket = root.path().join("memoree.sock");
    let endpoint = format!("unix://{}", socket.display());
    let first = root.path().join("first");
    let sibling = root.path().join("sibling");
    fs::create_dir_all(&first).unwrap();
    fs::create_dir_all(&sibling).unwrap();
    fs::write(
        first.join("decision.md"),
        "Ambient retrieval is the safe default.\n",
    )
    .unwrap();
    fs::write(
        sibling.join("private.md"),
        "Sibling-only codename QUARTZ-NEBULA-7319.\n",
    )
    .unwrap();

    let mut child = Command::new(binary())
        .args(["serve", "--listen"])
        .arg(&endpoint)
        .arg("--data-dir")
        .arg(&data)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start memoree daemon");
    wait_for_socket(&socket, &mut child);
    let _server = ServerGuard(child);

    let (status, initialized) = invoke(&first, &endpoint, &["init", "--name", "first"]);
    assert!(status.status.success(), "{initialized}");
    let workspace = initialized["result"]["context"]["workspace_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let project = initialized["result"]["context"]["project_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let (status, put) = invoke(
        &first,
        &endpoint,
        &[
            "artifact",
            "put",
            "decision.md",
            "--kind",
            "decision",
            "--title",
            "Retrieval decision",
        ],
    );
    assert!(status.status.success(), "{put}");
    assert_eq!(put["context"]["project_id"], project);
    let commit_seq = put["commit_seq"].as_i64().unwrap();
    let artifact_id = put["result"]["artifact_id"].as_str().unwrap();
    let revision_id = put["result"]["revision_id"].as_str().unwrap();

    let evidence = format!("{artifact_id}@{revision_id}");
    let (status, claim) = invoke(
        &first,
        &endpoint,
        &[
            "claim",
            "assert",
            "fact",
            "Graph linkage assertion",
            "--evidence",
            &evidence,
        ],
    );
    assert!(status.status.success(), "{claim}");
    let claim_id = claim["result"]["claim_id"].as_str().unwrap().to_owned();
    let claim_revision_id = claim["result"]["revision_id"].as_str().unwrap().to_owned();
    let artifact_ref = format!("artifact:{artifact_id}");
    let claim_ref = format!("claim:{claim_id}");
    let (status, link) = invoke(
        &first,
        &endpoint,
        &["link", &artifact_ref, "supports", &claim_ref],
    );
    assert!(status.status.success(), "{link}");
    let (status, relations) = invoke(
        &first,
        &endpoint,
        &[
            "relation",
            "list",
            &artifact_ref,
            "--direction",
            "outgoing",
            "--relation",
            "supports",
        ],
    );
    assert!(status.status.success(), "{relations}");
    assert!(relations["commit_seq"].is_null());
    assert_eq!(relations["context"]["project_id"], project);
    assert_eq!(relations["result"]["direction"], "outgoing");
    assert_eq!(
        relations["result"]["relations"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        relations["result"]["relations"][0]["source_id"],
        artifact_id
    );
    assert_eq!(relations["result"]["relations"][0]["target_id"], claim_id);

    let (status, revised_claim) = invoke(
        &first,
        &endpoint,
        &[
            "claim",
            "revise",
            &claim_id,
            "Graph linkage assertion revised",
            "--if-revision",
            &claim_revision_id,
            "--evidence",
            &evidence,
        ],
    );
    assert!(status.status.success(), "{revised_claim}");
    let revised_claim_revision_id = revised_claim["result"]["revision_id"].as_str().unwrap();
    let (status, claim_history) = invoke(
        &first,
        &endpoint,
        &["claim", "history", &claim_id, "--limit", "1"],
    );
    assert!(status.status.success(), "{claim_history}");
    assert!(claim_history["context"].is_null());
    assert!(claim_history["commit_seq"].is_null());
    assert_eq!(
        claim_history["result"]["revisions"][0]["revision_id"],
        revised_claim_revision_id
    );
    assert_eq!(
        claim_history["result"]["revisions"][0]["revision_number"],
        2
    );
    assert_eq!(claim_history["result"]["truncated"], true);
    assert_eq!(claim_history["result"]["next_before_revision_number"], 2);
    let (status, older_claim_history) = invoke(
        &first,
        &endpoint,
        &[
            "claim",
            "history",
            &claim_id,
            "--limit",
            "1",
            "--before-revision-number",
            "2",
        ],
    );
    assert!(status.status.success(), "{older_claim_history}");
    assert_eq!(
        older_claim_history["result"]["revisions"][0]["revision_id"],
        claim_revision_id
    );
    assert_eq!(
        older_claim_history["result"]["revisions"][0]["revision_number"],
        1
    );
    assert_eq!(older_claim_history["result"]["truncated"], false);

    let min_commit_seq = commit_seq.to_string();
    let (status, search) = invoke(
        &first,
        &endpoint,
        &[
            "search",
            "safe",
            "default",
            "--min-commit-seq",
            &min_commit_seq,
        ],
    );
    assert!(status.status.success(), "{search}");
    assert_eq!(search["result"]["hits"][0]["entity_id"], artifact_id);
    let citation = search["result"]["hits"][0]["citation"]
        .as_str()
        .expect("search citation");
    assert!(
        citation.starts_with(&format!("memoree://artifact/{artifact_id}@{revision_id}#")),
        "artifact body matches must expose an exact immutable byte span: {citation}"
    );
    let (status, bundle) = invoke(
        &first,
        &endpoint,
        &[
            "context",
            "build",
            "safe",
            "default",
            "--min-commit-seq",
            &min_commit_seq,
            "--max-bytes",
            "2048",
        ],
    );
    assert!(status.status.success(), "{bundle}");
    assert_eq!(bundle["result"]["content_is_untrusted"], true);

    let workspace_arg = workspace.as_str();
    let (status, sibling_init) = invoke(
        &sibling,
        &endpoint,
        &["init", "--name", "sibling", "--workspace", workspace_arg],
    );
    assert!(status.status.success(), "{sibling_init}");
    let (status, sibling_put) = invoke(
        &sibling,
        &endpoint,
        &["artifact", "put", "private.md", "--kind", "note"],
    );
    assert!(status.status.success(), "{sibling_put}");

    let (status, global_claim_history) = invoke(
        &sibling,
        &endpoint,
        &["claim", "history", &claim_id, "--limit", "2"],
    );
    assert!(status.status.success(), "{global_claim_history}");
    assert!(global_claim_history["context"].is_null());
    assert_eq!(
        global_claim_history["result"]["revisions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let (status, ambient) = invoke(&first, &endpoint, &["search", "QUARTZ", "NEBULA"]);
    assert!(status.status.success(), "{ambient}");
    assert_eq!(ambient["result"]["hits"].as_array().unwrap().len(), 0);
    assert!(ambient["result"]["broaden_hint"].is_string());

    let (status, denied) = invoke(
        &first,
        &endpoint,
        &["search", "QUARTZ", "NEBULA", "--horizon", "workspace"],
    );
    assert_eq!(status.status.code(), Some(2));
    assert_eq!(denied["error"]["code"], "INVALID_REQUEST");

    let (status, broadened) = invoke(
        &first,
        &endpoint,
        &[
            "search",
            "QUARTZ",
            "NEBULA",
            "--horizon",
            "workspace",
            "--reason",
            "explicit cross-project comparison",
        ],
    );
    assert!(status.status.success(), "{broadened}");
    assert_eq!(broadened["context"]["broadened"], true);
    assert_eq!(broadened["result"]["hits"].as_array().unwrap().len(), 1);

    let output_path: PathBuf = root.path().join("materialized.md");
    let (status, get) = invoke(
        &first,
        &endpoint,
        &[
            "artifact",
            "get",
            artifact_id,
            "--revision",
            revision_id,
            "--output",
            output_path.to_str().unwrap(),
        ],
    );
    assert!(status.status.success(), "{get}");
    assert!(get["result"].get("content").is_none());
    assert_eq!(
        fs::read(output_path).unwrap(),
        fs::read(first.join("decision.md")).unwrap()
    );

    // A revision without --media-type preserves the established media type;
    // the replacement path's extension must not silently alter indexing.
    let replacement = first.join("replacement.bin");
    fs::write(
        &replacement,
        "Ambient retrieval remains the safe default.\n",
    )
    .unwrap();
    let original_media_type = put["result"]["media_type"].as_str().unwrap();
    let (status, revised_artifact) = invoke(
        &first,
        &endpoint,
        &[
            "artifact",
            "revise",
            artifact_id,
            replacement.to_str().unwrap(),
            "--if-revision",
            revision_id,
        ],
    );
    assert!(status.status.success(), "{revised_artifact}");
    assert_eq!(
        revised_artifact["result"]["media_type"],
        original_media_type
    );

    let binary = binary().to_str().unwrap();
    let (status, child_doctor) = invoke(
        &first,
        &endpoint,
        &[
            "session",
            "exec",
            "--task",
            "endpoint-propagation",
            binary,
            "doctor",
        ],
    );
    assert!(status.status.success(), "{child_doctor}");
    assert_eq!(child_doctor["result"]["status"], "ok");

    // Text media can contain highly JSON-expanding control bytes. The CLI and
    // store must use base64 on the wire so every accepted artifact remains
    // exactly retrievable below the framed-transport limit.
    let hostile_text = first.join("control-heavy.txt");
    fs::write(&hostile_text, vec![0_u8; 8 * 1024 * 1024]).unwrap();
    let (status, large_put) = invoke(
        &first,
        &endpoint,
        &[
            "artifact",
            "put",
            hostile_text.to_str().unwrap(),
            "--media-type",
            "text/plain",
            "--title",
            "Control-heavy text",
        ],
    );
    assert!(status.status.success(), "{large_put}");
    let large_id = large_put["result"]["artifact_id"].as_str().unwrap();
    let restored = root.path().join("control-heavy-restored.txt");
    let (status, large_get) = invoke(
        &first,
        &endpoint,
        &[
            "artifact",
            "get",
            large_id,
            "--output",
            restored.to_str().unwrap(),
        ],
    );
    assert!(status.status.success(), "{large_get}");
    assert!(large_get["result"].get("content").is_none());
    let restored = fs::read(restored).unwrap();
    assert_eq!(restored.len(), 8 * 1024 * 1024);
    assert!(restored.iter().all(|byte| *byte == 0));
}

#[test]
fn auto_started_daemon_has_machine_readable_lifecycle_controls() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("client");
    let home = root.path().join("home");
    fs::create_dir_all(&cwd).unwrap();
    let _guard = LocalDaemonGuard {
        cwd: cwd.clone(),
        home: home.clone(),
    };

    let (status, stopped) = invoke_local(&cwd, &home, &["daemon", "status"]);
    assert_eq!(status.status.code(), Some(1), "{stopped}");
    assert_eq!(stopped["result"]["running"], false);

    let (status, started) = invoke_local(&cwd, &home, &["doctor"]);
    assert!(status.status.success(), "{started}");
    assert_eq!(started["result"]["running"], true);
    assert!(started["result"]["daemon_pid"].as_u64().unwrap() > 1);
    assert_eq!(
        started["result"]["binary_version"],
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(started["result"]["schema_version"], 4);
    assert_eq!(started["result"]["lifecycle_owner"], "memoree");

    let (status, running) = invoke_local(&cwd, &home, &["daemon", "status"]);
    assert!(status.status.success(), "{running}");
    assert_eq!(running["result"]["running"], true);

    let (status, restarted) = invoke_local(&cwd, &home, &["daemon", "restart"]);
    assert!(status.status.success(), "{restarted}");
    assert_eq!(restarted["result"]["running"], true);

    let (status, stopped) = invoke_local(&cwd, &home, &["daemon", "stop"]);
    assert!(status.status.success(), "{stopped}");
    assert_eq!(stopped["result"]["running"], false);
    assert!(!home.join("run/memoree.sock").exists());

    let (status, stopped) = invoke_local(&cwd, &home, &["daemon", "status"]);
    assert_eq!(status.status.code(), Some(1), "{stopped}");
    assert_eq!(stopped["result"]["running"], false);
}

#[test]
fn upgrade_apply_preserves_stopped_state_and_syncs_skills_idempotently() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("client");
    let memoree_home = root.path().join("memoree home");
    let user_home = root.path().join("user home");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(user_home.join(".codex")).unwrap();
    fs::create_dir_all(user_home.join(".claude")).unwrap();
    let _guard = LocalDaemonGuard {
        cwd: cwd.clone(),
        home: memoree_home.clone(),
    };

    let (status, first) = invoke_upgrade(
        &cwd,
        &memoree_home,
        &user_home,
        &["upgrade", "apply", "--previous-version", "0.2.0"],
    );
    assert!(status.status.success(), "{first}");
    assert_eq!(first["result"]["daemon"]["state"], "remained_stopped");
    assert_eq!(first["result"]["state"]["phase"], "complete");
    assert_eq!(first["result"]["skills"]["items"][0]["action"], "installed");
    assert_eq!(first["result"]["skills"]["items"][1]["action"], "installed");
    assert!(!memoree_home.join("run/memoree.sock").exists());

    let (status, second) = invoke_upgrade(
        &cwd,
        &memoree_home,
        &user_home,
        &["upgrade", "apply", "--previous-version", "0.2.0"],
    );
    assert!(status.status.success(), "{second}");
    assert_eq!(second["result"]["daemon"]["state"], "remained_stopped");
    assert!(
        second["result"]["skills"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["action"] == "unchanged")
    );
    assert!(!memoree_home.join("data/memoree.sqlite3").exists());
}

#[test]
fn upgrade_apply_never_touches_an_explicit_supervisor_endpoint() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("client");
    let memoree_home = root.path().join("memoree-home");
    let user_home = root.path().join("user-home");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(user_home.join(".codex")).unwrap();

    let endpoint = format!("unix://{}", root.path().join("supervisor.sock").display());
    let (status, report) = invoke_upgrade(
        &cwd,
        &memoree_home,
        &user_home,
        &[
            "--endpoint",
            &endpoint,
            "upgrade",
            "apply",
            "--previous-version",
            "0.2.0",
        ],
    );
    assert_eq!(status.status.code(), Some(20), "{report}");
    assert_eq!(
        report["result"]["daemon"]["state"],
        "external_action_required"
    );
    assert_eq!(report["result"]["authority"]["state"], "deferred");
    assert!(!memoree_home.join("data/memoree.sqlite3").exists());
}

#[test]
fn upgrade_apply_refuses_a_nondefault_daemon_holding_the_same_store() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("client");
    let memoree_home = root.path().join("memoree-home");
    let user_home = root.path().join("user-home");
    let data_dir = memoree_home.join("data");
    let external_socket = root.path().join("external.sock");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(user_home.join(".codex")).unwrap();
    let mut child = Command::new(binary())
        .current_dir(&cwd)
        .env("MEMOREE_HOME", &memoree_home)
        .args([
            "serve",
            "--listen",
            &format!("unix://{}", external_socket.display()),
            "--data-dir",
            data_dir.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_for_socket(&external_socket, &mut child);
    let mut guard = ServerGuard(child);

    let (status, report) = invoke_upgrade(
        &cwd,
        &memoree_home,
        &user_home,
        &["upgrade", "apply", "--previous-version", "0.2.0"],
    );
    assert_eq!(status.status.code(), Some(20), "{report}");
    assert_eq!(report["result"]["authority"]["state"], "deferred");
    assert_eq!(
        report["result"]["daemon"]["state"],
        "external_action_required"
    );
    assert!(guard.0.try_wait().unwrap().is_none());
}

#[test]
fn interrupted_upgrade_state_resumes_running_and_blocks_binary_downgrade() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("client");
    let memoree_home = root.path().join("memoree-home");
    let user_home = root.path().join("user-home");
    fs::create_dir_all(&cwd).unwrap();
    fs::create_dir_all(user_home.join(".codex")).unwrap();
    let _guard = LocalDaemonGuard {
        cwd: cwd.clone(),
        home: memoree_home.clone(),
    };

    let (status, initialized) = invoke_local(&cwd, &memoree_home, &["init", "--name", "resume"]);
    assert!(status.status.success(), "{initialized}");
    let (status, started) = invoke_local(&cwd, &memoree_home, &["doctor"]);
    assert!(status.status.success(), "{started}");
    let (status, stopped) = invoke_local(&cwd, &memoree_home, &["daemon", "stop"]);
    assert!(status.status.success(), "{stopped}");

    let state_path = memoree_home.join("data/upgrade-state.json");
    fs::write(
        &state_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema": 1,
            "target_version": env!("CARGO_PKG_VERSION"),
            "phase": "authority_committed",
            "prior_daemon_running": true,
            "previous_daemon_version": "0.2.0",
            "migration_backup": null,
            "store_schema_version": 4,
            "skill_digest": "0".repeat(64),
            "updated_at": "2026-07-20T00:00:00Z"
        }))
        .unwrap(),
    )
    .unwrap();

    let (status, resumed) = invoke_upgrade(
        &cwd,
        &memoree_home,
        &user_home,
        &["upgrade", "apply", "--previous-version", "0.2.0"],
    );
    assert!(status.status.success(), "{resumed}");
    assert_eq!(resumed["result"]["daemon"]["state"], "restarted");
    assert_eq!(resumed["result"]["state"]["phase"], "complete");

    let (status, guard) = invoke_upgrade(
        &cwd,
        &memoree_home,
        &user_home,
        &["upgrade", "rollback-safe"],
    );
    assert_eq!(status.status.code(), Some(20), "{guard}");
    assert_eq!(guard["result"]["rollback_safe"], false);
}

#[test]
fn canonical_call_preserves_request_id_on_cli_side_failure() {
    let root = tempfile::tempdir().unwrap();
    let cwd = root.path().join("uninitialized");
    let home = root.path().join("home");
    fs::create_dir_all(&cwd).unwrap();

    let request = serde_json::json!({
        "v": 1,
        "request_id": "req_correlation_probe",
        "op": "search",
        "input": {
            "query": "ambient context is intentionally missing",
            "horizon": "ambient",
            "limit": 10,
            "include_historical": false
        }
    });
    let (status, response) = invoke_call(&cwd, &home, &request);

    assert_eq!(status.status.code(), Some(5), "{response}");
    assert_eq!(response["request_id"], "req_correlation_probe");
    assert_eq!(response["error"]["code"], "NO_AMBIENT_CONTEXT");
}
