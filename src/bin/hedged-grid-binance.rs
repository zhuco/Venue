use std::process::ExitCode;

use clap::Parser;

fn main() -> ExitCode {
    run(venue::Cli::parse())
}

fn run(cli: venue::Cli) -> ExitCode {
    match venue::start_hedged_grid_binance_deployment(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("hedged-grid-binance: {error}");
            ExitCode::FAILURE
        }
    }
}
