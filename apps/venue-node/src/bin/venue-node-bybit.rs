use std::{process::ExitCode, time::Duration};

use venue_gateway_api::VenueId;
use venue_gateway_bybit::{BybitAccountGateway, BybitGatewayBinding, BybitTransportLimits};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, error_chain, load_root_dotenv, report_result,
    run_live_bybit_mvp,
};

const PROGRAM: &str = "venue-node-bybit";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Bybit)?;
    let command = launch.live_mvp_command()?;
    let adapter = BybitGatewayBinding::new(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Bybit))?;
    AdapterIsolation {
        venue: VenueId::Bybit,
        mode: adapter.config().mode(),
        endpoints: &[
            adapter.config().rest_origin(),
            adapter.config().public_ws(),
            adapter.config().private_ws(),
        ],
        credential_environment: &["BYBIT_API_KEY", "BYBIT_API_SECRET"],
        credential_prefix: "BYBIT_",
        account_binding: "uta2_linear",
    }
    .validate(launch.binding())?;
    load_root_dotenv()?;
    let limits =
        BybitTransportLimits::new(Duration::from_secs(10), 2 * 1024 * 1024).map_err(|error| {
            NodeError::LiveGateway {
                venue: VenueId::Bybit,
                message: error_chain(&error),
            }
        })?;
    let gateway = BybitAccountGateway::connect_from_environment(launch.binding().clone(), limits)
        .map_err(|error| NodeError::LiveGateway {
        venue: VenueId::Bybit,
        message: error_chain(&error),
    })?;
    run_live_bybit_mvp(&launch, command, gateway)
}
