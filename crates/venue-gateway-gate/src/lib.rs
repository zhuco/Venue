//! Gate.io gateway protocol boundary.
//!
//! This crate contains deterministic Gate.io protocol behavior, bounded async transport, and
//! immutable LIVE request binding. It deliberately has no capability issuer, writer, WAL,
//! owner registry, retry loop, or mutation authority: a safe host must supply those controls.

mod config;
mod credentials;
pub mod endpoints;
mod execution;
mod order_families;
mod orders;
mod private;
mod private_surface;
mod public;
mod recovery;
mod recovery_session;
mod risk;
mod sign;
mod transport;

pub use config::{GateConfig, GateProductScope};
pub use credentials::GateCredentials;
pub use execution::{
    GateAcceptedMutation, GateCancelIntent, GateDispatchUnknown, GateExactOrderReadback,
    GateExactReadbackRequest, GateExecutionError, GateMutationKind, GateMutationSettlement,
    GatePreparedMutation, GateSettlementFinality, prepare_cancel, prepare_limit_post_only,
    prepare_reduce_once, settle_exact_readback,
};
pub use order_families::{
    GATE_STAGE7_ORDER_PROFILE_VERSION, GateStage7OrderFamilyCandidate, GateStage7OrderFamilyError,
    GateStage7OrderFamilyEvidence, GateStage7OrderFamilyScope, GateStage7UnsupportedEvidence,
    GateStage7UnsupportedOrderFamily, validate_stage7_order_families,
};
pub use orders::{
    GATE_PRIVATE_MAX_PAGES, GATE_PRIVATE_PAGE_LIMIT, GateFillRecord, GateFillsReadback,
    GateOrderPayloadError, GateRegularOrdersReadback, collect_fill_pages,
    collect_regular_order_pages, parse_fill_record, parse_regular_order,
};
pub use private::{GatePrivatePayloadError, optional_price, parse_account_balance, parse_position};
pub use private_surface::{
    GateFillsCursor, GatePreparedPrivateRead, GatePrivateReadError, GatePrivateReadSource,
    GatePrivateReadbackCandidate, GateRawPrivateResponse, prepare_private_read,
    validate_private_readback,
};
pub use public::*;
pub use recovery::{
    GateFreshRecoveryCandidate, GateFreshRecoveryCollector, GateFreshRecoveryError,
    GateFreshRecoveryRawResponse, GateOwnedOpenOrder, GateRecoveryAuthorityRoots,
    GateRecoveryCollectionStart, GateRecoveryCoverage, GateRecoveryOwnerRoute,
    GateRecoveryPreparedRead, GateRecoveryScope, GateRecoverySurface,
    GateRecoverySurfaceCommitment, GateRecoverySymbolReadback, GateRecoverySymbolScope,
    GateUnknownOpenOrder, GateUnknownOpenOrderReason,
};
pub use recovery_session::GateAuthenticatedRecoverySession;
pub(crate) use recovery_session::GateAuthenticatedRecoverySessionLease;
pub use risk::{
    GateContractRules, GateRiskAccountMode, GateRiskError, GateRiskReadback, decimal,
    decimal_value, dual_position_side, object, parse_dual_position_mode, parse_risk_snapshots,
    parse_risk_snapshots_with_unified, requires_unified_single_currency, text,
    validate_risk_readback_window,
};
pub use sign::{
    GatePrivateChannel, GateRestSignedHeaders, GateWebSocketAuth, sign_rest,
    sign_websocket_subscription,
};
use thiserror::Error;
pub use transport::{
    GateHttpTransport, GateMutationDispatch, GatePrivateWsFrame, GatePrivateWsTransport,
    GateTransportError, GateTransportLimits, connect_private_ws,
};
use venue_gateway_api::{GatewayApiError, GatewayBinding, VenueId};

/// A validated Gate.io identity. Holding this value grants no read or mutation capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateGatewayBinding(GatewayBinding);

impl GateGatewayBinding {
    pub fn new(binding: GatewayBinding) -> Result<Self, GateGatewayBindingError> {
        binding
            .validate()
            .map_err(GateGatewayBindingError::Gateway)?;
        if binding.venue != VenueId::Gate {
            return Err(GateGatewayBindingError::Venue);
        }
        Ok(Self(binding))
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.0
    }

    #[must_use]
    pub const fn config(&self) -> GateConfig {
        GateConfig::for_mode(self.0.mode)
    }

    pub(crate) fn validate_request_binding(
        &self,
        candidate: &GatewayBinding,
    ) -> Result<(), GateGatewayBindingError> {
        candidate
            .validate()
            .map_err(GateGatewayBindingError::Gateway)?;
        if candidate != &self.0 {
            return Err(GateGatewayBindingError::Venue);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GateGatewayBindingError {
    #[error("Gate.io gateway binding must use venue=gate")]
    Venue,
    #[error(transparent)]
    Gateway(#[from] GatewayApiError),
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GateProtocolError {
    #[error("Gate.io credentials are unavailable or empty")]
    Credentials,
    #[error("Gate.io signing input is invalid")]
    SigningInput,
    #[error("Gate.io gateway supports only USDT-settled perpetual futures")]
    ProductScope,
}

#[cfg(test)]
mod binding_tests {
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    fn binding(
        venue: VenueId,
        mode: GatewayMode,
    ) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            venue,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "DOGE/USDT".parse()?,
        )?)
    }

    #[test]
    fn gate_binding_accepts_only_gate_live_identity() -> Result<(), Box<dyn std::error::Error>> {
        let validated = GateGatewayBinding::new(binding(VenueId::Gate, GatewayMode::Live)?)?;
        assert_eq!(validated.gateway_binding().mode, GatewayMode::Live);
        assert_eq!(validated.gateway_binding().venue, VenueId::Gate);
        assert_eq!(
            GateGatewayBinding::new(binding(VenueId::Bitget, GatewayMode::Live)?),
            Err(GateGatewayBindingError::Venue)
        );
        Ok(())
    }
}
