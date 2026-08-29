use std::process::ExitCode;

use clap::Parser;
use venue_gateway_api::{GatewayMode, VenueId};
use venue_gateway_binance::BinanceConfig;
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, reject_unintegrated_legacy_test_runtime, report_result,
};

const PROGRAM: &str = "venue-node-binance";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Binance)?;
    let arguments = launch.legacy_runtime_arguments(PROGRAM)?;
    let cli = venue::Cli::try_parse_from(arguments)?;
    let config =
        venue::config::Config::load(&cli.config).map_err(|error| NodeError::ExistingRuntime {
            venue: VenueId::Binance,
            message: error.to_string(),
        })?;
    launch.validate_runtime_scope(&config.trading_account_id, &config.symbol)?;
    let account_binding = config
        .binance_config()
        .map_err(|error| NodeError::ExistingRuntime {
            venue: VenueId::Binance,
            message: error.to_string(),
        })?;
    let adapter = BinanceConfig::for_binding(account_binding.account_binding, launch.binding())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Binance))?;
    AdapterIsolation {
        venue: VenueId::Binance,
        mode: adapter.mode(),
        endpoints: &[
            adapter.portfolio_rest_origin(),
            adapter.usd_m_public_rest_origin(),
            adapter.public_stream_origin(),
            adapter.private_stream_origin(),
        ],
        credential_environment: &["BINANCE_API_KEY", "BINANCE_API_SECRET"],
        credential_prefix: "BINANCE_",
        account_binding: adapter.account_binding().as_str(),
    }
    .validate(launch.binding())?;
    if launch.binding().mode == GatewayMode::Test {
        return reject_unintegrated_legacy_test_runtime(VenueId::Binance);
    }
    venue::start_hedged_grid_binance_deployment(cli).map_err(|error| NodeError::ExistingRuntime {
        venue: VenueId::Binance,
        message: error.to_string(),
    })
}
