use std::process::ExitCode;

use venue_gateway_api::VenueId;
use venue_gateway_okx::{OkxConfig, capabilities};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, reject_unintegrated_runtime, report_result,
};

const PROGRAM: &str = "venue-node-okx";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Okx)?;
    launch.require_no_runtime_arguments()?;
    let adapter = OkxConfig::for_binding(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Okx))?;
    AdapterIsolation {
        venue: VenueId::Okx,
        mode: adapter.mode(),
        endpoints: &[
            adapter.rest_origin(),
            adapter.public_ws(),
            adapter.private_ws(),
        ],
        credential_environment: &["OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"],
        credential_prefix: "OKX_",
        account_binding: "linear_swap",
    }
    .validate(launch.binding())?;
    let _isolated_artifacts_root = launch.artifacts_root();
    reject_unintegrated_runtime(VenueId::Okx, launch.binding().mode, capabilities())
}
