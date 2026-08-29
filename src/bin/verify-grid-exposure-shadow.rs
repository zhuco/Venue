use std::{path::PathBuf, process::ExitCode};

use clap::Parser;

#[derive(Debug, Parser)]
#[command(about = "Read-only verification of normalized grid exposure Shadow evidence")]
struct Args {
    #[arg(long)]
    artifacts_root: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match venue::runtime::verify_stage7_exposure_shadow_evidence(&args.artifacts_root) {
        Ok(report) => match serde_json::to_string(&report) {
            Ok(encoded) => {
                println!("{encoded}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("exposure shadow report encoding failed: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("exposure shadow evidence rejected: {error}");
            ExitCode::FAILURE
        }
    }
}
