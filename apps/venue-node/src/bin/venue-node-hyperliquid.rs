use std::process::ExitCode;

use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding, VenueId};
use venue_gateway_hyperliquid::{
    HyperliquidConfig, HyperliquidGatewayBinding, HyperliquidNodeCandidate, capabilities,
};
use venue_node::{
    AdapterIsolation, DispatchPermit, GatewayDispatchResult, GatewayRecoveryPermit, NodeError,
    NodeLaunch, PhysicalGateway, SignedReadbackReceipt, SignedReadbackRequest,
    reject_unintegrated_runtime, report_result,
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
    let candidate_bridge = HyperliquidPhysicalGatewayCandidate::new(launch.binding().clone(), None)
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Hyperliquid))?;
    let advertised = candidate_bridge.capability_snapshot();
    if !advertised.flags.is_empty() || candidate_bridge.persisted_probe_candidate().is_some() {
        return Err(NodeError::UnexpectedAdapterCapability(VenueId::Hyperliquid));
    }
    reject_unintegrated_runtime(VenueId::Hyperliquid, launch.binding().mode, capabilities())
}

/// Fixed-binary adapter boundary. A persisted probe can be attached for validation and action
/// preparation, but the `PhysicalGateway` view remains closed until the shared host can explicitly
/// promote candidate evidence and drive the async readback/action surfaces without bypassing its
/// Control, Owner, WAL, writer and Canary sequence.
struct HyperliquidPhysicalGatewayCandidate {
    binding: GatewayBinding,
    persisted_probe: Option<HyperliquidNodeCandidate>,
}

impl HyperliquidPhysicalGatewayCandidate {
    fn new(
        binding: GatewayBinding,
        persisted_probe: Option<HyperliquidNodeCandidate>,
    ) -> Result<Self, HyperliquidBridgeError> {
        if binding.venue != VenueId::Hyperliquid
            || persisted_probe.as_ref().is_some_and(|candidate| {
                candidate.candidate_capability_snapshot().binding != binding
            })
        {
            return Err(HyperliquidBridgeError::Binding);
        }
        Ok(Self {
            binding,
            persisted_probe,
        })
    }

    fn persisted_probe_candidate(&self) -> Option<&CapabilitySnapshot> {
        self.persisted_probe
            .as_ref()
            .map(HyperliquidNodeCandidate::candidate_capability_snapshot)
    }

    fn closed_capability_snapshot(&self) -> CapabilitySnapshot {
        CapabilitySnapshot {
            binding: self.binding.clone(),
            version: 0,
            observed_ms: 0,
            expires_ms: 0,
            flags: CapabilityFlags::empty(),
        }
    }
}

impl PhysicalGateway for HyperliquidPhysicalGatewayCandidate {
    type Error = HyperliquidBridgeError;

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.closed_capability_snapshot()
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        if permit.binding() != &self.binding {
            return Err(HyperliquidBridgeError::Binding);
        }
        Err(HyperliquidBridgeError::SharedIntegration)
    }

    fn signed_readback(
        &mut self,
        request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        if request.binding() != &self.binding {
            return Err(HyperliquidBridgeError::Binding);
        }
        Err(HyperliquidBridgeError::SharedIntegration)
    }

    fn verify_signed_readback(&self, receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        if receipt.binding() != &self.binding {
            return Err(HyperliquidBridgeError::Binding);
        }
        Err(HyperliquidBridgeError::SharedIntegration)
    }

    fn dispatch(&mut self, permit: DispatchPermit) -> GatewayDispatchResult {
        if permit.binding() != &self.binding {
            return GatewayDispatchResult::Unknown;
        }
        GatewayDispatchResult::Unknown
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum HyperliquidBridgeError {
    #[error("Hyperliquid fixed-node bridge binding does not match the launch scope")]
    Binding,
    #[error(
        "Hyperliquid candidate bridge lacks shared async runtime, readback promotion and command mapping"
    )]
    SharedIntegration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::GatewayMode;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    fn binding(mode: GatewayMode) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Hyperliquid,
            mode,
            ACCOUNT,
            "BTC/USDC".parse()?,
        )?)
    }

    fn assert_physical_gateway<T: PhysicalGateway>() {}

    #[test]
    fn fixed_candidate_bridge_is_physical_but_never_auto_authorizes()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_physical_gateway::<HyperliquidPhysicalGatewayCandidate>();
        for mode in [GatewayMode::Test, GatewayMode::Live] {
            let selected = binding(mode)?;
            let bridge = HyperliquidPhysicalGatewayCandidate::new(selected.clone(), None)?;
            assert_eq!(bridge.binding(), &selected);
            assert!(bridge.capability_snapshot().flags.is_empty());
            assert_eq!(bridge.capability_snapshot().version, 0);
            assert!(bridge.persisted_probe_candidate().is_none());
        }
        Ok(())
    }

    #[test]
    fn fixed_candidate_bridge_rejects_other_venue() -> Result<(), Box<dyn std::error::Error>> {
        let wrong = GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Test,
            ACCOUNT,
            "BTC/USDC".parse()?,
        )?;
        assert!(matches!(
            HyperliquidPhysicalGatewayCandidate::new(wrong, None),
            Err(HyperliquidBridgeError::Binding)
        ));
        Ok(())
    }
}
