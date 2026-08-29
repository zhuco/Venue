use std::process::ExitCode;

use venue_gateway_api::VenueId;
use venue_gateway_hyperliquid::{HyperliquidConfig, HyperliquidGatewayBinding, capabilities};
use venue_node::{
    AdapterIsolation, NodeError, NodeLaunch, reject_unintegrated_runtime, report_result,
};

const PROGRAM: &str = "venue-node-hyperliquid";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Hyperliquid)?;
    launch.require_no_runtime_arguments()?;
    let binding = HyperliquidGatewayBinding::new(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Hyperliquid))?;
    let adapter = HyperliquidConfig::for_binding(&binding);
    AdapterIsolation {
        venue: VenueId::Hyperliquid,
        mode: adapter.mode(),
        endpoints: &[adapter.rest_origin(), adapter.websocket()],
        credential_environment: &[
            "HYPERLIQUID_MASTER_ADDRESS",
            "HYPERLIQUID_USER_ADDRESS",
            "HYPERLIQUID_VAULT_ADDRESS",
            "HYPERLIQUID_AGENT_NAME",
            "HYPERLIQUID_AGENT_ADDRESS",
            "HYPERLIQUID_AGENT_PRIVATE_KEY",
        ],
        credential_prefix: "HYPERLIQUID_",
        account_binding: "usdc_perpetual_agent",
    }
    .validate(launch.binding())?;
    let _isolated_artifacts_root = launch.artifacts_root();
    reject_unintegrated_runtime(VenueId::Hyperliquid, launch.binding().mode, capabilities())
}
