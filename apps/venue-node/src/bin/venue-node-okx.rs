use std::{process::ExitCode, time::Duration};

use venue_gateway_api::VenueId;
use venue_gateway_okx::{OkxAccountGateway, OkxConfig, OkxTradeMode};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, error_chain, load_root_dotenv, report_result,
    run_live_okx_mvp,
};

const PROGRAM: &str = "venue-node-okx";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Okx)?;
    let command = launch.live_mvp_command()?;
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
    load_root_dotenv()?;
    let gateway = OkxAccountGateway::connect_from_environment(
        launch.binding().clone(),
        OkxTradeMode::Cross,
        Duration::from_secs(10),
        2 * 1024 * 1024,
    )
    .map_err(|error| NodeError::LiveGateway {
        venue: VenueId::Okx,
        message: error_chain(&error),
    })?;
    run_live_okx_mvp(&launch, command, gateway)
}
