use std::process::ExitCode;

use venue_gateway_api::VenueId;
use venue_gateway_bybit::{BybitGatewayBinding, capabilities};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, reject_unintegrated_runtime, report_result,
};

const PROGRAM: &str = "venue-node-bybit";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Bybit)?;
    launch.require_no_runtime_arguments()?;
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
    let _isolated_artifacts_root = launch.artifacts_root();
    reject_unintegrated_runtime(VenueId::Bybit, launch.binding().mode, capabilities())
}
