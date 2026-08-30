use venue_gateway_api::{GatewayApiError, GatewayBinding, GatewayMode, VenueId};

/// A validated OKX process identity. Holding it grants no read or mutation capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxGatewayBinding(GatewayBinding);

impl OkxGatewayBinding {
    pub fn new(binding: GatewayBinding) -> Result<Self, OkxGatewayBindingError> {
        binding
            .validate()
            .map_err(OkxGatewayBindingError::Gateway)?;
        if binding.venue != VenueId::Okx {
            return Err(OkxGatewayBindingError::Venue);
        }
        if binding.mode != GatewayMode::Live {
            return Err(OkxGatewayBindingError::Mode);
        }
        Ok(Self(binding))
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OkxGatewayBindingError {
    #[error("OKX gateway binding must use venue=okx")]
    Venue,
    #[error("OKX gateway binding must use mode=LIVE")]
    Mode,
    #[error(transparent)]
    Gateway(#[from] GatewayApiError),
}

#[cfg(test)]
mod tests {
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
            "BTC/USDT".parse()?,
        )?)
    }

    #[test]
    fn okx_binding_accepts_only_live_and_keeps_exact_account_symbol()
    -> Result<(), Box<dyn std::error::Error>> {
        let validated = OkxGatewayBinding::new(binding(VenueId::Okx, GatewayMode::Live)?)?;
        assert_eq!(validated.gateway_binding().venue, VenueId::Okx);
        assert_eq!(validated.gateway_binding().mode, GatewayMode::Live);
        assert_eq!(
            validated.gateway_binding().trading_account_id,
            "00000000-0000-4000-8000-000000000001"
        );
        assert_eq!(validated.gateway_binding().symbol.to_string(), "BTC/USDT");
        let rejected = serde_json::from_str::<GatewayBinding>(
            r#"{"venue":"okx","mode":"TEST","trading_account_id":"00000000-0000-4000-8000-000000000001","symbol":"BTC/USDT"}"#,
        );
        assert!(rejected.is_err());
        assert_eq!(
            OkxGatewayBinding::new(binding(VenueId::Bybit, GatewayMode::Live)?),
            Err(OkxGatewayBindingError::Venue)
        );
        Ok(())
    }

    #[test]
    fn invalid_account_identity_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let invalid = GatewayBinding {
            venue: VenueId::Okx,
            mode: GatewayMode::Live,
            trading_account_id: "account-alias".to_owned(),
            symbol: "BTC/USDT".parse()?,
        };
        assert!(matches!(
            OkxGatewayBinding::new(invalid),
            Err(OkxGatewayBindingError::Gateway(_))
        ));
        Ok(())
    }
}
