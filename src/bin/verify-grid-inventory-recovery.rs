use std::{path::PathBuf, process::ExitCode};

use clap::Parser;

#[derive(Debug, Parser)]
#[command(about = "Read-only verification of one durable grid inventory-recovery episode")]
struct Args {
    #[arg(long)]
    artifacts_root: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match venue::runtime::verify_stage7_inventory_recovery_evidence(&args.artifacts_root) {
        Ok(report) => match serde_json::to_string(&report) {
            Ok(encoded) => {
                println!("{encoded}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("inventory recovery report encoding failed: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("inventory recovery evidence rejected: {error}");
            ExitCode::FAILURE
        }
    }
}
