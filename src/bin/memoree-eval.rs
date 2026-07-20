use std::{path::PathBuf, process::ExitCode};

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
    #[arg(long)]
    pretty: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match memoree::eval::run_retrieval_eval_with_models(
        &cli.corpus_dir,
        cli.semantic_model.as_deref(),
        cli.reranker_model.as_deref(),
    )
    .await
    {
        Ok(report) => {
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
