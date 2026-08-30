mod binding;
mod capability;
mod config;
mod credentials;
#[cfg(test)]
#[allow(dead_code)]
mod execution;
mod models;
#[cfg(test)]
#[allow(dead_code)]
mod physical;
mod private;
mod private_ws;
mod public;
mod readback;
mod recovery_collector;
mod sign;
mod trade_mode;
mod transport;

use venue_domain::domain::{FieldState, Fill};
use venue_gateway_api::CapabilityFlags;

pub use binding::{OkxGatewayBinding, OkxGatewayBindingError};
#[cfg(test)]
pub(crate) use capability::validate_mutation_capability_fixture;
pub use capability::{
    OKX_CAPABILITY_PROBE_SCHEMA_VERSION, OKX_LEGACY_CAPABILITY_PROBE_EVIDENCE_CLASS,
    OkxCapabilityCandidate, OkxCapabilityProbeEvidence, OkxCapabilityProbeScope,
    OkxMutationProbeEvidence, OkxPrivateStreamProbeEvidence, OkxPrivateStreamProbeFrame,
    OkxProbeHttpResponse, PersistedOkxCapabilityProbe, load_capability_probe,
    persist_capability_probe, validate_capability_candidate, validate_read_capability_candidate,
};
pub use config::{OkxConfig, endpoints};
pub use credentials::OkxCredentials;
#[cfg(test)]
pub(crate) use execution::{
    OkxAcceptedCancel, OkxAcceptedOrder, OkxCancelRequest, OkxExecutionScope, OkxOrderReadback,
    OkxOrderReadbackRequest, OkxPlaceIntent, OkxPlaceRequest, OkxPrivateRequest,
    OkxUnknownCancelReadbackRequest, OkxUnknownCancelResolution, OkxUnknownOrderReadback,
    OkxUnknownOrderReadbackRequest, build_cancel_order_readback_request, build_cancel_request,
    build_order_readback_request, build_place_request, build_unknown_cancel_readback_request,
    build_unknown_order_readback_request_after, parse_cancel_ack, parse_order_detail,
    parse_place_ack, parse_unknown_cancel_readback, parse_unknown_order_readback,
};
#[cfg(test)]
pub(crate) use physical::{
    OkxDispatchOnceResult, OkxPhysicalCandidate, OkxPhysicalError, OkxPhysicalReadbackResult,
};
pub use private::{
    OkxAccountLevel, OkxAccountProfile, OkxApiPermission, OkxPage, OkxPageState, OkxTimedBalance,
    OkxTimedOrder, OkxTimedPosition, parse_account_profile, parse_balance, parse_fills_page,
    parse_orders_page, parse_positions,
};
pub use private_ws::{
    OkxAccountSnapshotState, OkxActivePrivateSubscription, OkxEventWindow,
    OkxPositionSnapshotState, OkxPrivateSubscription, OkxPrivateWsScope, OkxPrivateWsSession,
    OkxWsBatch, OkxWsDelivery, OkxWsLoginFrame, activate_private_subscription,
    build_private_subscribe, build_ws_login, parse_ws_account, parse_ws_login_ack, parse_ws_orders,
    parse_ws_positions,
};
pub use public::{OkxInstrument, parse_bbo, parse_instrument};
pub use readback::{
    OKX_PRIVATE_MAX_PAGES, OKX_PRIVATE_PAGE_LIMIT, OKX_PRIVATE_READBACK_SCHEMA_VERSION,
    OkxAlgoOrderKind, OkxCanonicalOrder, OkxOrderFamilyReadback, OkxPositionFact,
    OkxPositionFactSource, OkxPrivatePageAdvance, OkxPrivateReadRequest, OkxPrivateReadScope,
    OkxPrivateReadbackCandidate, OkxPrivateSurface, OkxRawPrivatePage, advance_private_page,
    build_account_config_request, build_algo_orders_request, build_balance_request,
    build_fills_request, build_positions_request, build_regular_orders_request,
    complete_private_readback,
};
pub use recovery_collector::{
    OKX_FRESH_RECOVERY_SCHEMA_VERSION, OkxFreshRecoveryCollector, OkxFreshRecoveryError,
    OkxFreshRecoveryEvidence, OkxFreshRecoveryFace, OkxFreshRecoveryOutcome, OkxFreshRecoveryScope,
    OkxFreshRecoverySurface, OkxFreshRecoveryUnknown, OkxFreshRecoveryUnknownIssue,
    OkxFreshRecoveryUnknownKind, OkxOwnerRoute, OkxRecoveryAuthoritySnapshot,
    OkxRecoveryConfiguration,
};
pub(crate) use sign::{SignedHeaders, sign};
pub use trade_mode::OkxTradeMode;
#[cfg(test)]
pub(crate) use transport::{OkxCancelOnceOutcome, OkxPlaceOnceOutcome};
pub use transport::{
    OkxHttpResponse, OkxHttpTransport, OkxPrivateWsTransport, OkxReceivedPrivateFrame,
    OkxTransportError,
};

/// No account capability is advertised until authenticated readback, private stream, writer,
/// WAL, and UNKNOWN reconciliation are all connected.
#[must_use]
pub const fn capabilities() -> CapabilityFlags {
    CapabilityFlags::empty()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OkxPositionMode {
    Net,
    LongShort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OkxError {
    #[error("OKX credentials are unavailable or empty")]
    Credentials,
    #[error("OKX signing input is invalid")]
    SigningInput,
    #[error("OKX response payload is invalid or incomplete")]
    Payload,
    #[error("OKX rejected the request")]
    Rejected,
    #[error("OKX response does not match the fixed gateway binding")]
    Binding,
    #[error("OKX response position side does not match the verified account mode")]
    PositionMode,
    #[error("OKX venue sequence is missing, repeated, or out of order")]
    Sequence,
    #[error("OKX pagination cursor is invalid or the page is not closed")]
    Pagination,
    #[error("OKX quantity or price cannot be represented exactly by the bound instrument")]
    Precision,
    #[error("OKX mutation or readback identity is invalid or ambiguous")]
    Identity,
    #[error("OKX capability probe evidence is incomplete, stale, or invalid")]
    Capability,
    #[error("OKX capability probe evidence could not be persisted or recovered")]
    Persistence,
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    fn config(mode: GatewayMode) -> Result<OkxConfig, Box<dyn std::error::Error>> {
        Ok(OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?)
    }

    #[test]
    fn one_binding_selects_only_live_production_transport() -> Result<(), Box<dyn std::error::Error>>
    {
        let live = config(GatewayMode::Live)?;
        assert_eq!(live.rest_origin(), "https://www.okx.com");
        assert_eq!(live.public_ws(), "wss://ws.okx.com:8443/ws/v5/public");
        assert_eq!(live.private_ws(), "wss://ws.okx.com:8443/ws/v5/private");
        assert_eq!(live.gateway_binding().mode, GatewayMode::Live);
        assert_eq!(live.gateway_binding().symbol.to_string(), "BTC/USDT");
        assert_eq!(capabilities(), CapabilityFlags::empty());
        Ok(())
    }

    #[test]
    fn signing_preserves_the_okx_fixed_vector() -> Result<(), OkxError> {
        let credentials = OkxCredentials::from_values("key", "mysecret", "pass")?;
        let config = config(GatewayMode::Live).map_err(|_| OkxError::Binding)?;
        let headers = sign(
            &credentials,
            &config,
            "2020-12-08T09:08:57.715Z",
            "GET",
            "/api/v5/account/balance",
            &[],
        )?;
        assert_eq!(
            headers.get("OK-ACCESS-SIGN"),
            Some("7dqjFHmbJfEEOQc+0wMh6KyqlUAh5C2x6vqL7qZTilE=")
        );
        assert_eq!(headers.get("x-simulated-trading"), None);
        Ok(())
    }

    #[test]
    fn live_signing_omits_simulated_trading_header() -> Result<(), OkxError> {
        let credentials = OkxCredentials::from_values("key", "mysecret", "pass")?;
        let config = config(GatewayMode::Live).map_err(|_| OkxError::Binding)?;
        let headers = sign(
            &credentials,
            &config,
            "2020-12-08T09:08:57.715Z",
            "GET",
            "/api/v5/account/balance",
            &[],
        )?;
        assert_eq!(headers.get("x-simulated-trading"), None);
        Ok(())
    }
}
