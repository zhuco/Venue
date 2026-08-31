use std::{collections::BTreeSet, process::ExitCode, time::Duration};

use venue_gateway_api::VenueId;
use venue_gateway_binance::{
    BinanceAccountBinding, BinanceAccountGateway, BinanceConfig, BinanceTransportLimits,
};
use venue_node::{
    AdapterIsolation, LiveMvpCommand, NodeError, NodeLaunch, NodeRuntimeConfig, error_chain,
    load_root_dotenv, report_result, run_live_binance_mvp,
};

const PROGRAM: &str = "venue-node-binance";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Binance)?;
    let command = launch.live_mvp_command()?;
    let symbols = match &command {
        LiveMvpCommand::Run(path) => NodeRuntimeConfig::load(path, launch.binding())?
            .configured_symbols(launch.binding())?
            .iter()
            .cloned()
            .collect(),
        LiveMvpCommand::Preflight | LiveMvpCommand::Dispatch(_) => {
            BTreeSet::from([launch.binding().symbol.clone()])
        }
    };
    let adapter =
        BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, launch.binding())
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
    load_root_dotenv()?;
    let limits =
        BinanceTransportLimits::new(Duration::from_secs(10), 2 * 1024 * 1024).map_err(|error| {
            NodeError::LiveGateway {
                venue: VenueId::Binance,
                message: error_chain(&error),
            }
        })?;
    let gateway = BinanceAccountGateway::connect_from_environment_for_symbols(
        launch.binding().clone(),
        symbols,
        limits,
    )
    .map_err(|error| NodeError::LiveGateway {
        venue: VenueId::Binance,
        message: error_chain(&error),
    })?;
    run_live_binance_mvp(&launch, command, gateway)
}
