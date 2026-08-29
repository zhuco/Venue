mod binding;
mod config;
mod credentials;
mod private;
mod public;
mod sign;
use venue_gateway_api::CapabilityFlags;

pub use binding::BybitGatewayBinding;
pub use config::{BybitConfig, endpoints};
pub use credentials::BybitCredentials;
pub use private::*;
pub use public::*;
pub use sign::{SignedHeaders, sign};

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
}

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
    fn bindings_select_only_testnet_or_live_endpoints() -> Result<(), Box<dyn std::error::Error>> {
        let test = BybitGatewayBinding::new(gateway_binding(
            GatewayMode::Test,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT",
        )?)?;
        let live = BybitGatewayBinding::new(gateway_binding(
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT",
        )?)?;
        let test = test.config();
        let live = live.config();
        assert_eq!(test.rest_origin(), "https://api-testnet.bybit.com");
        assert_eq!(live.rest_origin(), "https://api.bybit.com");
        assert_ne!(test.private_ws(), live.private_ws());
        assert_eq!(test.mode(), GatewayMode::Test);
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
    fn binding_rejects_cross_mode_wrong_account_and_wrong_symbol()
    -> Result<(), Box<dyn std::error::Error>> {
        let account_id = "00000000-0000-4000-8000-000000000001";
        let configured = gateway_binding(GatewayMode::Live, account_id, "BTC/USDT")?;
        let binding = BybitGatewayBinding::new(configured.clone())?;
        let credentials = BybitCredentials::from_values("test", "secret")?;

        for request_binding in [
            gateway_binding(GatewayMode::Test, account_id, "BTC/USDT")?,
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
