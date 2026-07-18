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
    #[arg(long)]
    pretty: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match memoree::eval::run_retrieval_eval(&cli.corpus_dir).await {
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
