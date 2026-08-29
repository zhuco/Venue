use std::process::ExitCode;

use venue_gateway_api::{CapabilitySnapshot, GatewayBinding, VenueId};
use venue_gateway_okx::{OkxConfig, OkxPhysicalCandidate, capabilities};
use venue_node::{
    AdapterIsolation, DispatchPermit, GatewayDispatchResult, GatewayRecoveryPermit, NodeError,
    NodeLaunch, PhysicalGateway, SignedReadbackReceipt, SignedReadbackRequest,
    reject_unintegrated_runtime, report_result,
};

const PROGRAM: &str = "venue-node-okx";

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    assert_candidate_bridge::<OkxNodePhysicalCandidate>();
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

fn assert_candidate_bridge<G: PhysicalGateway>() {}

/// Compile-time bridge from the validated OKX physical candidate to the fixed node contract.
/// The shared host currently has no recovery-time constructor that can supply this value with an
/// Owner route, applied Control state and a post-connect full signed generation. Consequently the
/// binary never constructs it and remains fail-closed before credentials or network access.
struct OkxNodePhysicalCandidate {
    candidate: OkxPhysicalCandidate,
}

impl PhysicalGateway for OkxNodePhysicalCandidate {
    type Error = OkxNodeBridgeError;

    fn binding(&self) -> &GatewayBinding {
        self.candidate.binding()
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.candidate.capability_snapshot()
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        if permit.binding() != self.candidate.binding()
            || self.candidate.private_generation() <= permit.private_generation_floor()
        {
            return Err(OkxNodeBridgeError::Scope);
        }
        // A durable probe predates recovery. It cannot be relabelled as the required fresh private
        // generation, and opening credentials/network here without the shared collector would
        // bypass the account-wide signed readback gate.
        Err(OkxNodeBridgeError::FreshReadbackUnavailable)
    }

    fn signed_readback(
        &mut self,
        _request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        Err(OkxNodeBridgeError::FreshReadbackUnavailable)
    }

    fn verify_signed_readback(&self, _receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        Err(OkxNodeBridgeError::FreshReadbackUnavailable)
    }

    fn dispatch(&mut self, _permit: DispatchPermit) -> GatewayDispatchResult {
        // `connect_after_recovery` cannot succeed until the shared post-recovery collector exists.
        // Preserve UNKNOWN/no-resubmit semantics if a future caller violates that sequencing.
        GatewayDispatchResult::Unknown
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum OkxNodeBridgeError {
    #[error("OKX node physical candidate does not match the recovered binding or generation")]
    Scope,
    #[error("OKX node lacks a post-recovery full signed readback collector")]
    FreshReadbackUnavailable,
}
