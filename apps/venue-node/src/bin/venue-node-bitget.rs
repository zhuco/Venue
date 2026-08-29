use std::process::ExitCode;

use clap::Parser;
use venue_gateway_api::{GatewayMode, VenueId};
use venue_gateway_bitget::BitgetConfig;
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, reject_unintegrated_legacy_test_runtime, report_result,
};

const PROGRAM: &str = "venue-node-bitget";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Bitget)?;
    let arguments = launch.legacy_runtime_arguments(PROGRAM)?;
    let cli = venue::Cli::try_parse_from(arguments)?;
    let config =
        venue::config::Config::load(&cli.config).map_err(|error| NodeError::ExistingRuntime {
            venue: VenueId::Bitget,
            message: error.to_string(),
        })?;
    launch.validate_runtime_scope(&config.trading_account_id, &config.symbol)?;
    let account_binding = config
        .bitget
        .ok_or(NodeError::RuntimeScope)?
        .account_binding;
    account_binding
        .validate_gateway_binding(launch.binding())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Bitget))?;
    let adapter = BitgetConfig::for_mode(launch.binding().mode);
    AdapterIsolation {
        venue: VenueId::Bitget,
        mode: adapter.mode(),
        endpoints: &[
            adapter.rest_origin(),
            adapter.public_ws(),
            adapter.private_ws(),
        ],
        credential_environment: &[
            "BITGET_API_KEY",
            "BITGET_API_SECRET",
            "BITGET_API_PASSPHRASE",
            "BITGET_PASSPHRASE",
        ],
        credential_prefix: "BITGET_",
        account_binding: account_binding.as_str(),
    }
    .validate(launch.binding())?;
    if launch.binding().mode == GatewayMode::Test {
        return reject_unintegrated_legacy_test_runtime(VenueId::Bitget);
    }
    venue::start_hedged_grid_bitget_deployment(cli).map_err(|error| NodeError::ExistingRuntime {
        venue: VenueId::Bitget,
        message: error.to_string(),
    })
}
