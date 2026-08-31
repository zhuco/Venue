use std::{process::ExitCode, time::Duration};

use venue_gateway_api::VenueId;
use venue_gateway_gate::{GateAccountGateway, GateConfig, GateGatewayBinding, GateTransportLimits};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, error_chain, load_root_dotenv, report_result,
    run_live_gate_mvp,
};

const PROGRAM: &str = "venue-node-gate";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Gate)?;
    let command = launch.live_mvp_command()?;
    let _binding = GateGatewayBinding::new(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Gate))?;
    let adapter = GateConfig::for_mode(launch.binding().mode);
    AdapterIsolation {
        venue: VenueId::Gate,
        mode: adapter.mode(),
        endpoints: &[adapter.rest_origin(), adapter.usdt_futures_ws()],
        credential_environment: &["GATEIO_API_KEY", "GATEIO_API_SECRET"],
        credential_prefix: "GATEIO_",
        account_binding: "usdt_futures_dual",
    }
    .validate(launch.binding())?;
    load_root_dotenv()?;
    let limits =
        GateTransportLimits::new(Duration::from_secs(10), 2 * 1024 * 1024).map_err(|error| {
            NodeError::LiveGateway {
                venue: VenueId::Gate,
                message: error_chain(&error),
            }
        })?;
    let gateway = GateAccountGateway::connect_from_environment(launch.binding().clone(), limits)
        .map_err(|error| NodeError::LiveGateway {
            venue: VenueId::Gate,
            message: error_chain(&error),
        })?;
    run_live_gate_mvp(&launch, command, gateway)
}
