use std::{process::ExitCode, time::Duration};

use venue_gateway_api::VenueId;
use venue_gateway_bitget::{
    BitgetAccountBinding, BitgetAccountGateway, BitgetConfig, BitgetTransportLimits,
};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, error_chain, load_root_dotenv, report_result,
    run_live_bitget_mvp,
};

const PROGRAM: &str = "venue-node-bitget";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Bitget)?;
    let command = launch.live_mvp_command()?;
    BitgetAccountBinding::UtaUsdtFuturesHedge
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
        account_binding: BitgetAccountBinding::UtaUsdtFuturesHedge.as_str(),
    }
    .validate(launch.binding())?;
    load_root_dotenv()?;
    let limits =
        BitgetTransportLimits::new(Duration::from_secs(10), 2 * 1024 * 1024).map_err(|error| {
            NodeError::LiveGateway {
                venue: VenueId::Bitget,
                message: error_chain(&error),
            }
        })?;
    let gateway = BitgetAccountGateway::connect_from_environment(launch.binding().clone(), limits)
        .map_err(|error| NodeError::LiveGateway {
            venue: VenueId::Bitget,
            message: error_chain(&error),
        })?;
    run_live_bitget_mvp(&launch, command, gateway)
}
