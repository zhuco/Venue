use venue_gateway_api::CapabilitySnapshot;

use crate::{
    HyperliquidCapabilityProbeEvidence, HyperliquidCredentials, HyperliquidError,
    HyperliquidProbeCollectionScope,
};
/// Validated packaging for persisted read-only probe evidence.
///
/// This remains candidate evidence. In particular, constructing this value does not advertise
/// adapter capability, acquire a writer, create a WAL, or make a request dispatchable. It cannot
/// construct, sign, or dispatch a production action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidNodeCandidate {
    evidence: HyperliquidCapabilityProbeEvidence,
    candidate_capability: CapabilitySnapshot,
}

impl HyperliquidNodeCandidate {
    /// Restores immutable probe evidence and rejects any payload/commitment, binding, mode, vault,
    /// coin, generation, freshness, or withdrawal-capability mismatch before exposing it.
    pub fn from_persisted_slice(
        expected_scope: &HyperliquidProbeCollectionScope,
        credentials: &HyperliquidCredentials,
        encoded: &[u8],
        now_ms: u64,
    ) -> Result<Self, HyperliquidError> {
        let evidence: HyperliquidCapabilityProbeEvidence =
            serde_json::from_slice(encoded).map_err(|_| HyperliquidError::CapabilityProbe)?;
        if evidence.collection_scope() != expected_scope {
            return Err(HyperliquidError::CapabilityProbe);
        }
        let candidate_capability = evidence.candidate_capability_snapshot(
            expected_scope.binding(),
            credentials,
            now_ms,
        )?;
        Ok(Self {
            evidence,
            candidate_capability,
        })
    }

    /// Returns non-authoritative candidate evidence for an outer safety host to inspect. The
    /// adapter-wide `capabilities()` function deliberately remains empty.
    #[must_use]
    pub const fn candidate_capability_snapshot(&self) -> &CapabilitySnapshot {
        &self.candidate_capability
    }

    #[must_use]
    pub const fn evidence(&self) -> &HyperliquidCapabilityProbeEvidence {
        &self.evidence
    }
}
