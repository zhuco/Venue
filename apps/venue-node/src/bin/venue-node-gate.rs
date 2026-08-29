use std::process::ExitCode;

use clap::Parser;
use venue_gateway_api::{GatewayMode, VenueId};
use venue_gateway_gate::{GateConfig, GateGatewayBinding};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, reject_unintegrated_legacy_test_runtime, report_result,
};

const PROGRAM: &str = "venue-node-gate";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Gate)?;
    let arguments = launch.legacy_runtime_arguments(PROGRAM)?;
    let cli = venue::Cli::try_parse_from(arguments)?;
    let config =
        venue::config::Config::load(&cli.config).map_err(|error| NodeError::ExistingRuntime {
            venue: VenueId::Gate,
            message: error.to_string(),
        })?;
    launch.validate_runtime_scope(&config.trading_account_id, &config.symbol)?;
    let account_binding = config.gate.ok_or(NodeError::RuntimeScope)?.account_binding;
    let _binding = GateGatewayBinding::new(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Gate))?;
    let adapter = GateConfig::for_mode(launch.binding().mode);
    let account_binding = match account_binding {
        venue::config::GateAccountBinding::UsdtFuturesDual => "usdt_futures_dual",
    };
    AdapterIsolation {
        venue: VenueId::Gate,
        mode: adapter.mode(),
        endpoints: &[adapter.rest_origin(), adapter.usdt_futures_ws()],
        credential_environment: &["GATEIO_API_KEY", "GATEIO_API_SECRET"],
        credential_prefix: "GATEIO_",
        account_binding,
    }
    .validate(launch.binding())?;
    if launch.binding().mode == GatewayMode::Test {
        return reject_unintegrated_legacy_test_runtime(VenueId::Gate);
    }
    venue::start_hedged_grid_gate_deployment(cli).map_err(|error| NodeError::ExistingRuntime {
        venue: VenueId::Gate,
        message: error.to_string(),
    })
}
