use std::{process::ExitCode, time::Duration};

use venue_gateway_api::VenueId;
use venue_gateway_hyperliquid::{
    HyperliquidAccountGateway, HyperliquidConfig, HyperliquidGatewayBinding,
};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, error_chain, load_root_dotenv, report_result,
    run_live_hyperliquid_mvp,
};

const PROGRAM: &str = "venue-node-hyperliquid";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Hyperliquid)?;
    let command = launch.live_mvp_command()?;
    let binding = HyperliquidGatewayBinding::new(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Hyperliquid))?;
    let adapter = HyperliquidConfig::for_binding(&binding);
    AdapterIsolation {
        venue: VenueId::Hyperliquid,
        mode: adapter.mode(),
        endpoints: &[adapter.rest_origin(), adapter.websocket()],
        credential_environment: &[
            "HYPERLIQUID_ACCOUNT_ADDRESS",
            "HYPERLIQUID_API_WALLET_ADDRESS",
            "HYPERLIQUID_API_WALLET_PRIVATE_KEY",
            "HYPERLIQUID_VAULT_ADDRESS",
        ],
        credential_prefix: "HYPERLIQUID_",
        account_binding: "usdc_perpetual_api_wallet",
    }
    .validate(launch.binding())?;
    load_root_dotenv()?;
    let gateway = HyperliquidAccountGateway::connect_from_environment(
        launch.binding().clone(),
        launch.artifacts_root().join("nonce.json"),
        Duration::from_secs(10),
        2 * 1024 * 1024,
    )
    .map_err(|error| NodeError::LiveGateway {
        venue: VenueId::Hyperliquid,
        message: error_chain(&error),
    })?;
    run_live_hyperliquid_mvp(&launch, command, gateway)
}
