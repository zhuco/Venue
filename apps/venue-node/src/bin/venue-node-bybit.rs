use std::{
    path::{Path, PathBuf},
    process::ExitCode,
};

use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding, VenueId};
use venue_gateway_bybit::{BybitGatewayBinding, BybitSynchronousPhysicalSession, capabilities};
use venue_node::{
    AdapterIsolation, DispatchPermit, GatewayDispatchResult, GatewayRecoveryPermit, NodeError,
    NodeLaunch, PhysicalGateway, SignedReadbackReceipt, SignedReadbackRequest,
    reject_unintegrated_runtime, report_result,
};

const PROGRAM: &str = "venue-node-bybit";
const PROBE_FILE: &str = "bybit_capability_probe.json";
const INSTRUMENT_FILE: &str = "bybit_linear_instrument.json";

/// Exact shared delta that prevents this candidate from loading credentials or creating transport.
/// The adapter probe remains evidence, never account-node capability authority.
pub const BYBIT_PHYSICAL_GATEWAY_SHARED_DELTA: &str = "GatewayRecoveryPermit lacks opaque Actor-applied Control and Canary receipts plus recovered Owner/WAL and installed writer-session authority; PhysicalGateway has no host-admitted capability separate from an adapter probe; DispatchPermit lacks the fresh same-generation BBO required by Bybit market and reduce-only IOC preparation";

/// Fixed-binary bridge candidate. Persisted paths are secret-free and inert; the synchronous
/// physical session is deliberately absent until the shared host can prove the complete authority
/// listed above before credential access.
pub struct BybitPhysicalGatewayCandidate {
    binding: BybitGatewayBinding,
    probe_path: PathBuf,
    instrument_path: PathBuf,
    session: Option<BybitSynchronousPhysicalSession>,
}

impl BybitPhysicalGatewayCandidate {
    #[must_use]
    pub fn new(binding: BybitGatewayBinding, artifacts_root: &Path) -> Self {
        let account_root = artifacts_root.join("account");
        Self {
            binding,
            probe_path: account_root.join(PROBE_FILE),
            instrument_path: account_root.join(INSTRUMENT_FILE),
            session: None,
        }
    }

    #[must_use]
    pub fn probe_path(&self) -> &Path {
        &self.probe_path
    }

    #[must_use]
    pub fn instrument_path(&self) -> &Path {
        &self.instrument_path
    }

    #[must_use]
    pub const fn has_loaded_session(&self) -> bool {
        self.session.is_some()
    }

    fn fail_before_credentials(&self) -> BybitPhysicalGatewayBridgeError {
        BybitPhysicalGatewayBridgeError::SharedAuthority(BYBIT_PHYSICAL_GATEWAY_SHARED_DELTA)
    }
}

impl PhysicalGateway for BybitPhysicalGatewayCandidate {
    type Error = BybitPhysicalGatewayBridgeError;

    fn binding(&self) -> &GatewayBinding {
        self.binding.gateway_binding()
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        CapabilitySnapshot {
            binding: self.binding.gateway_binding().clone(),
            version: 0,
            observed_ms: 0,
            expires_ms: 0,
            flags: CapabilityFlags::empty(),
        }
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        if permit.binding() != self.binding.gateway_binding() {
            return Err(BybitPhysicalGatewayBridgeError::Binding);
        }
        Err(self.fail_before_credentials())
    }

    fn signed_readback(
        &mut self,
        _request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        Err(self.fail_before_credentials())
    }

    fn verify_signed_readback(&self, _receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        Err(self.fail_before_credentials())
    }

    fn dispatch(&mut self, _permit: DispatchPermit) -> GatewayDispatchResult {
        GatewayDispatchResult::Rejected {
            reason_code: "bybit_shared_authority_missing".to_owned(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitPhysicalGatewayBridgeError {
    #[error("Bybit physical gateway binding does not match the fixed node")]
    Binding,
    #[error("Bybit physical gateway remains pre-credential fail-closed: {0}")]
    SharedAuthority(&'static str),
}

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
    let candidate = BybitPhysicalGatewayCandidate::new(adapter, &launch.artifacts_root());
    debug_assert!(!candidate.has_loaded_session());
    let admitted_capabilities = candidate.capability_snapshot().flags;
    debug_assert_eq!(admitted_capabilities, capabilities());
    reject_unintegrated_runtime(VenueId::Bybit, launch.binding().mode, admitted_capabilities)
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayMode, MutationCapability};

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";

    fn candidate(
        mode: GatewayMode,
    ) -> Result<BybitPhysicalGatewayCandidate, Box<dyn std::error::Error>> {
        let binding = BybitGatewayBinding::new(GatewayBinding::new(
            VenueId::Bybit,
            mode,
            ACCOUNT_ID,
            "BTC/USDT".parse()?,
        )?)?;
        Ok(BybitPhysicalGatewayCandidate::new(
            binding,
            &std::env::temp_dir().join("venue-goal29-bybit-node"),
        ))
    }

    #[test]
    fn test_and_live_candidates_are_inert_and_create_no_capability()
    -> Result<(), Box<dyn std::error::Error>> {
        for mode in [GatewayMode::Test, GatewayMode::Live] {
            let candidate = candidate(mode)?;
            assert_eq!(candidate.binding().mode, mode);
            assert!(!candidate.has_loaded_session());
            assert_eq!(
                candidate.capability_snapshot().flags,
                CapabilityFlags::empty()
            );
            assert!(
                candidate
                    .capability_snapshot()
                    .authorize(candidate.binding(), 0, 1, MutationCapability::Cancel)
                    .is_err()
            );
            assert_eq!(
                candidate.probe_path().file_name(),
                Some(PROBE_FILE.as_ref())
            );
            assert_eq!(
                candidate.instrument_path().file_name(),
                Some(INSTRUMENT_FILE.as_ref())
            );
        }
        Ok(())
    }

    #[test]
    fn shared_delta_is_explicit_and_precredential() -> Result<(), Box<dyn std::error::Error>> {
        let candidate = candidate(GatewayMode::Test)?;
        assert_eq!(
            candidate.fail_before_credentials(),
            BybitPhysicalGatewayBridgeError::SharedAuthority(BYBIT_PHYSICAL_GATEWAY_SHARED_DELTA)
        );
        for required in ["Control", "Canary", "Owner/WAL", "writer", "BBO"] {
            assert!(BYBIT_PHYSICAL_GATEWAY_SHARED_DELTA.contains(required));
        }
        assert!(!candidate.probe_path().exists());
        assert!(!candidate.instrument_path().exists());
        Ok(())
    }
}
