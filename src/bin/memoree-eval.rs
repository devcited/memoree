use std::{
    io::Write,
    path::PathBuf,
    process::ExitCode,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "memoree-eval",
    about = "Run an isolated, versioned Memoree retrieval regression corpus"
)]
struct Cli {
    /// Directory containing seed.jsonl, cases.jsonl, and baseline.json.
    corpus_dir: PathBuf,
    /// Verified local semantic model directory; evaluation performs no downloads.
    #[arg(long, value_name = "DIRECTORY")]
    semantic_model: Option<PathBuf>,
    /// Verified local ordering-only reranker directory; evaluation performs no downloads.
    #[arg(long, value_name = "DIRECTORY")]
    reranker_model: Option<PathBuf>,
    /// Evaluate only cases covered by probe-recovery.json.
    #[arg(long)]
    recovery_only: bool,
    /// Evaluate one stable case identifier.
    #[arg(long, value_name = "CASE_ID")]
    case: Option<String>,
    /// Soft per-case budget, checked after each case finishes.
    #[arg(long, default_value_t = 60_000)]
    case_timeout_ms: u64,
    /// Hard wall-clock deadline for the entire selected suite.
    #[arg(long, default_value_t = 600_000)]
    suite_timeout_ms: u64,
    /// Deterministic worker count; v0.6 requires one.
    #[arg(long, default_value_t = 1)]
    jobs: usize,
    /// Load installed models once before timed cases.
    #[arg(long)]
    prewarm_models: bool,
    /// Write content-free stage timings to a separate JSON file.
    #[arg(long, value_name = "PATH")]
    timings_json: Option<PathBuf>,
    #[arg(long)]
    pretty: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.suite_timeout_ms == 0 {
        eprintln!("memoree-eval failed: evaluation timeout values must be greater than zero");
        return ExitCode::from(2);
    }

    let started = Instant::now();
    let deadline = Duration::from_millis(cli.suite_timeout_ms);
    let corpus_dir = cli.corpus_dir.clone();
    let semantic_model = cli.semantic_model.clone();
    let reranker_model = cli.reranker_model.clone();
    let options = memoree::eval::EvalOptions {
        recovery_only: cli.recovery_only,
        case_id: cli.case.clone(),
        case_timeout_ms: cli.case_timeout_ms,
        suite_timeout_ms: cli.suite_timeout_ms,
        jobs: cli.jobs,
        prewarm_models: cli.prewarm_models,
    };
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        #[cfg(debug_assertions)]
        if let Some(delay) = test_blocking_delay() {
            thread::sleep(delay);
        }
        let outcome = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("could not create evaluation runtime: {error}"))
            .and_then(|runtime| {
                runtime
                    .block_on(memoree::eval::run_retrieval_eval_with_options(
                        &corpus_dir,
                        semantic_model.as_deref(),
                        reranker_model.as_deref(),
                        options,
                    ))
                    .map_err(|error| error.to_string())
            });
        let _ = sender.send(outcome);
    });

    let remaining = deadline.saturating_sub(started.elapsed());
    let outcome = match receiver.recv_timeout(remaining) {
        Ok(outcome) => outcome,
        Err(mpsc::RecvTimeoutError::Timeout) => hard_timeout(cli.suite_timeout_ms),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            eprintln!("memoree-eval failed: evaluation worker stopped unexpectedly");
            return ExitCode::from(2);
        }
    };

    match outcome {
        Ok(report) => {
            if let Some(path) = &cli.timings_json {
                let timing_bytes = if cli.pretty {
                    serde_json::to_vec_pretty(&report.timings)
                } else {
                    serde_json::to_vec(&report.timings)
                };
                match timing_bytes
                    .and_then(|bytes| std::fs::write(path, bytes).map_err(serde_json::Error::io))
                {
                    Ok(()) => {}
                    Err(error) => {
                        eprintln!("could not write timing report {}: {error}", path.display());
                        return ExitCode::from(2);
                    }
                }
            }
            let rendered = if cli.pretty {
                serde_json::to_string_pretty(&report)
            } else {
                serde_json::to_string(&report)
            };
            match rendered {
                Ok(rendered) => println!("{rendered}"),
                Err(error) => {
                    eprintln!("could not serialize eval report: {error}");
                    return ExitCode::from(2);
                }
            }
            if report.passed {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        Err(error) => {
            eprintln!("memoree-eval failed: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(debug_assertions)]
fn test_blocking_delay() -> Option<Duration> {
    std::env::var("MEMOREE_EVAL_TEST_BLOCK_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn hard_timeout(limit_ms: u64) -> ! {
    eprintln!("memoree-eval hard timeout after {limit_ms} ms; no report was written");
    let _ = std::io::stderr().flush();
    let _ = std::io::stdout().flush();
    std::process::exit(124);
}
