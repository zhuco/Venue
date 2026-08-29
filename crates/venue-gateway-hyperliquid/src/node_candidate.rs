use venue_gateway_api::{CapabilitySnapshot, GatewayBinding};

use crate::{
    HyperliquidAloOrder, HyperliquidCancel, HyperliquidCapabilityProbeEvidence,
    HyperliquidCredentials, HyperliquidError, HyperliquidIocReduceOnlyOrder, HyperliquidNonceStore,
    HyperliquidPerpMeta, HyperliquidPhysicalDispatch, HyperliquidPrivateStreamBinding,
    build_alo_place_request, build_cancel_request, build_ioc_reduce_only_request,
    reserve_next_nonce,
};

/// Validated packaging for a persisted probe and the three narrow one-shot action builders.
///
/// This remains candidate evidence. In particular, constructing this value does not advertise
/// adapter capability, acquire a writer, create a WAL, or make a request dispatchable. The caller
/// must already own the durable nonce store used below and must pass the resulting linear dispatch
/// through the account-node safety host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidNodeCandidate {
    evidence: HyperliquidCapabilityProbeEvidence,
    candidate_capability: CapabilitySnapshot,
}

impl HyperliquidNodeCandidate {
    /// Restores immutable probe evidence and rejects any payload/commitment, binding, mode, vault,
    /// coin, generation, freshness, or withdrawal-capability mismatch before exposing it.
    pub fn from_persisted_slice(
        expected_binding: &GatewayBinding,
        encoded: &[u8],
        now_ms: u64,
    ) -> Result<Self, HyperliquidError> {
        let evidence: HyperliquidCapabilityProbeEvidence =
            serde_json::from_slice(encoded).map_err(|_| HyperliquidError::CapabilityProbe)?;
        let candidate_capability =
            evidence.candidate_capability_snapshot(expected_binding, now_ms)?;
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

    /// Reserves and durably reads back one nonce before building a consumed ALO dispatch.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_alo<S: HyperliquidNonceStore>(
        &self,
        store: &mut S,
        meta: &HyperliquidPerpMeta,
        private_binding: &HyperliquidPrivateStreamBinding,
        credentials: &HyperliquidCredentials,
        now_ms: u64,
        order: HyperliquidAloOrder,
        expires_after_ms: Option<u64>,
    ) -> Result<HyperliquidPhysicalDispatch, HyperliquidError> {
        self.validate_action_scope(meta, private_binding, credentials, now_ms)?;
        if order.scope() != &meta.scope {
            return Err(HyperliquidError::Binding);
        }
        let nonce = reserve_next_nonce(store, credentials.agent_address(), now_ms)?;
        HyperliquidPhysicalDispatch::new(
            build_alo_place_request(credentials, nonce, order, expires_after_ms)?,
            private_binding,
        )
    }

    /// Reserves and durably reads back one nonce before building a consumed exact-order cancel.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_cancel<S: HyperliquidNonceStore>(
        &self,
        store: &mut S,
        meta: &HyperliquidPerpMeta,
        private_binding: &HyperliquidPrivateStreamBinding,
        credentials: &HyperliquidCredentials,
        now_ms: u64,
        cancel: HyperliquidCancel,
        expires_after_ms: Option<u64>,
    ) -> Result<HyperliquidPhysicalDispatch, HyperliquidError> {
        self.validate_action_scope(meta, private_binding, credentials, now_ms)?;
        if cancel.scope() != &meta.scope {
            return Err(HyperliquidError::Binding);
        }
        let nonce = reserve_next_nonce(store, credentials.agent_address(), now_ms)?;
        HyperliquidPhysicalDispatch::new(
            build_cancel_request(credentials, nonce, cancel, expires_after_ms)?,
            private_binding,
        )
    }

    /// Reserves and durably reads back one nonce before building a consumed IOC reduce-only
    /// dispatch. This helper does not imply common `PLACE_MARKET` capability.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_ioc_reduce_only<S: HyperliquidNonceStore>(
        &self,
        store: &mut S,
        meta: &HyperliquidPerpMeta,
        private_binding: &HyperliquidPrivateStreamBinding,
        credentials: &HyperliquidCredentials,
        now_ms: u64,
        order: HyperliquidIocReduceOnlyOrder,
        expires_after_ms: Option<u64>,
    ) -> Result<HyperliquidPhysicalDispatch, HyperliquidError> {
        self.validate_action_scope(meta, private_binding, credentials, now_ms)?;
        if order.scope() != &meta.scope {
            return Err(HyperliquidError::Binding);
        }
        let nonce = reserve_next_nonce(store, credentials.agent_address(), now_ms)?;
        HyperliquidPhysicalDispatch::new(
            build_ioc_reduce_only_request(credentials, nonce, order, expires_after_ms)?,
            private_binding,
        )
    }

    fn validate_action_scope(
        &self,
        meta: &HyperliquidPerpMeta,
        private_binding: &HyperliquidPrivateStreamBinding,
        credentials: &HyperliquidCredentials,
        now_ms: u64,
    ) -> Result<(), HyperliquidError> {
        if now_ms < self.candidate_capability.observed_ms
            || now_ms >= self.candidate_capability.expires_ms
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        self.evidence
            .validate_node_scope(meta, private_binding, credentials)
    }
}
