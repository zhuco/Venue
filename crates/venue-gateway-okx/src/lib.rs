mod binding;
mod config;
mod credentials;
mod execution;
mod models;
mod private;
mod public;
mod sign;

use venue_domain::domain::{FieldState, Fill};
use venue_gateway_api::CapabilityFlags;

pub use binding::{OkxGatewayBinding, OkxGatewayBindingError};
pub use config::{OkxConfig, endpoints};
pub use credentials::OkxCredentials;
pub use execution::{
    OkxAcceptedCancel, OkxAcceptedOrder, OkxCancelRequest, OkxExecutionScope,
    OkxOrderReadbackRequest, OkxPlaceIntent, OkxPlaceRequest, OkxPrivateRequest, OkxTradeMode,
    build_cancel_request, build_order_readback_request, build_place_request, parse_cancel_ack,
    parse_order_detail, parse_place_ack,
};
pub use private::{
    OkxAccountLevel, OkxAccountProfile, OkxPage, OkxPageState, OkxTimedBalance, OkxTimedOrder,
    OkxTimedPosition, parse_account_profile, parse_balance, parse_fills_page, parse_orders_page,
    parse_positions,
};
pub use public::{OkxInstrument, parse_bbo, parse_instrument};
pub use sign::{SignedHeaders, request_path, sign};

/// No account capability is advertised until authenticated readback, private stream, writer,
/// WAL, and UNKNOWN reconciliation are all connected.
#[must_use]
pub const fn capabilities() -> CapabilityFlags {
    CapabilityFlags::empty()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    fn one_binding_selects_only_its_test_or_live_transport()
    -> Result<(), Box<dyn std::error::Error>> {
        let test = config(GatewayMode::Test)?;
        let live = config(GatewayMode::Live)?;
        assert!(test.simulated_trading());
        assert!(!live.simulated_trading());
        assert!(test.private_ws().contains("wspap.okx.com"));
        assert!(live.private_ws().contains("ws.okx.com"));
        assert_eq!(test.gateway_binding().mode, GatewayMode::Test);
        assert_eq!(live.gateway_binding().mode, GatewayMode::Live);
        assert_eq!(test.gateway_binding().symbol.to_string(), "BTC/USDT");
        assert_eq!(capabilities(), CapabilityFlags::empty());
        Ok(())
    }

    #[test]
    fn signing_preserves_the_okx_fixed_vector() -> Result<(), OkxError> {
        let credentials = OkxCredentials::from_values("key", "mysecret", "pass")?;
        let config = config(GatewayMode::Test).map_err(|_| OkxError::Binding)?;
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
        assert_eq!(headers.get("x-simulated-trading"), Some("1"));
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
