use std::process::ExitCode;

use clap::Parser;

fn main() -> ExitCode {
    let cli = venue::Cli::parse();

    match venue::start(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("venue: {err}");
            ExitCode::FAILURE
        }
    }
}
