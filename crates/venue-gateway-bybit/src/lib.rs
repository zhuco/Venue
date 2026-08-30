mod binding;
mod config;
mod credentials;
mod evidence;
#[cfg(test)]
mod execution;
#[cfg(test)]
mod physical;
mod private;
mod public;
mod recovery;
mod sign;
mod transport;
use venue_gateway_api::CapabilityFlags;

pub use binding::BybitGatewayBinding;
pub use config::{BybitConfig, endpoints};
pub use credentials::BybitCredentials;
pub use evidence::*;
#[cfg(test)]
pub use execution::*;
#[cfg(test)]
pub use physical::*;
pub use private::*;
pub use public::*;
pub use recovery::*;
pub use sign::SignedHeaders;
pub(crate) use sign::sign;
pub use transport::{
    BybitHttpTransport, BybitPrivateWsTransport, BybitRawPrivateFrame, BybitTransportError,
    BybitTransportLimits, connect_private_ws, connect_private_ws_for_generations,
};

/// Production has no constructible physical session until Owner/WAL/writer/Canary authority is
/// integrated. The mutation implementation exists only in this crate's unit-test build.
#[cfg(not(test))]
pub enum BybitSynchronousPhysicalSession {}

/// No account capability is advertised until authenticated readback, private stream, writer,
/// WAL, and UNKNOWN reconciliation are all connected.
#[must_use]
pub const fn capabilities() -> CapabilityFlags {
    CapabilityFlags::empty()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BybitError {
    #[error("Bybit credentials are unavailable or empty")]
    Credentials,
    #[error("Bybit signing input is invalid")]
    SigningInput,
    #[error("Bybit response payload is invalid or incomplete")]
    Payload,
    #[error("Bybit rejected the request")]
    Rejected,
    #[error("Bybit response does not match the fixed gateway binding")]
    Binding,
    #[error("Bybit private pagination is incomplete, mixed, or unbounded")]
    Pagination,
    #[error("Bybit private history window is invalid or unbounded")]
    Clock,
    #[error("Bybit canonical order-family evidence is invalid or incomplete")]
    OrderFamily,
    #[error("Bybit normalized private projection does not replay its raw evidence")]
    Projection,
    #[error("Bybit API-key or capability candidate evidence is invalid")]
    Capability,
}

#[cfg(test)]
mod private_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    fn gateway_binding(
        mode: GatewayMode,
        account_id: &str,
        symbol: &str,
    ) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Bybit,
            mode,
            account_id,
            symbol.parse()?,
        )?)
    }

    #[test]
    fn binding_accepts_only_live_and_uses_production_endpoints()
    -> Result<(), Box<dyn std::error::Error>> {
        let rejected = serde_json::from_str::<GatewayBinding>(
            r#"{"venue":"bybit","mode":"TEST","trading_account_id":"00000000-0000-4000-8000-000000000001","symbol":"BTC/USDT"}"#,
        );
        let live = BybitGatewayBinding::new(gateway_binding(
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT",
        )?)?;
        let live = live.config();
        assert!(rejected.is_err());
        assert_eq!(live.rest_origin(), "https://api.bybit.com");
        assert_eq!(live.public_ws(), "wss://stream.bybit.com/v5/public/linear");
        assert_eq!(live.private_ws(), "wss://stream.bybit.com/v5/private");
        assert_eq!(live.mode(), GatewayMode::Live);
        assert_eq!(capabilities(), CapabilityFlags::empty());
        Ok(())
    }

    #[test]
    fn signing_preserves_the_bybit_v5_fixed_vector() -> Result<(), Box<dyn std::error::Error>> {
        let credentials = BybitCredentials::from_values("test", "secret")?;
        let gateway_binding = gateway_binding(
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT",
        )?;
        let binding = BybitGatewayBinding::new(gateway_binding.clone())?;
        let headers = sign(
            &credentials,
            &binding,
            &gateway_binding,
            1_670_000_000_000,
            b"accountType=UNIFIED",
        )?;
        assert_eq!(
            headers.get("X-BAPI-SIGN"),
            Some("8ed52aa3777e158a21222a41d3f0d807d97753d6add49376c12241e0e77a2c9e")
        );
        assert_eq!(headers.get("X-BAPI-SIGN-TYPE"), Some("2"));
        Ok(())
    }

    #[test]
    fn binding_rejects_wrong_account_and_wrong_symbol() -> Result<(), Box<dyn std::error::Error>> {
        let account_id = "00000000-0000-4000-8000-000000000001";
        let configured = gateway_binding(GatewayMode::Live, account_id, "BTC/USDT")?;
        let binding = BybitGatewayBinding::new(configured.clone())?;
        let credentials = BybitCredentials::from_values("test", "secret")?;

        for request_binding in [
            gateway_binding(
                GatewayMode::Live,
                "00000000-0000-4000-8000-000000000002",
                "BTC/USDT",
            )?,
            gateway_binding(GatewayMode::Live, account_id, "ETH/USDT")?,
        ] {
            assert_eq!(
                sign(
                    &credentials,
                    &binding,
                    &request_binding,
                    1_670_000_000_000,
                    b"accountType=UNIFIED",
                )
                .err(),
                Some(BybitError::Binding)
            );
        }
        assert_eq!(binding.gateway_binding(), &configured);
        assert_eq!(binding.config().mode(), GatewayMode::Live);
        Ok(())
    }

    #[test]
    fn binding_rejects_a_non_bybit_venue() -> Result<(), Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        assert_eq!(BybitGatewayBinding::new(binding), Err(BybitError::Binding));
        Ok(())
    }
}
