use std::{process::Command, time::Instant};

#[test]
fn hard_suite_timeout_stops_blocking_evaluation_without_a_report() {
    let temporary = tempfile::tempdir().unwrap();
    let timings = temporary.path().join("timings.json");
    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_memoree-eval"))
        .arg(temporary.path())
        .arg("--suite-timeout-ms")
        .arg("50")
        .arg("--timings-json")
        .arg(&timings)
        .env("MEMOREE_EVAL_TEST_BLOCK_MS", "5000")
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(124));
    assert!(started.elapsed().as_secs_f64() < 2.0);
    assert!(!timings.exists());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("hard timeout after 50 ms"), "{stderr}");
    assert!(stderr.contains("no report was written"), "{stderr}");
}
