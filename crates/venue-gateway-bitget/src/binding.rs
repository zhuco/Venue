use serde::Deserialize;
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

/// The native UTA product/account mode is deployment identity, not mutation authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BitgetAccountBinding {
    UtaUsdtFuturesHedge,
}

impl BitgetAccountBinding {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UtaUsdtFuturesHedge => "uta_usdt_futures_hedge",
        }
    }

    /// This proves only canonical venue/mode/account/symbol identity. Capability evidence,
    /// private readback, writer ownership, and WAL remain mandatory at the mutation boundary.
    pub fn validate_gateway_binding(
        self,
        binding: &GatewayBinding,
    ) -> Result<(), BitgetBindingError> {
        binding
            .validate()
            .map_err(|_| BitgetBindingError::Gateway)?;
        if binding.venue != VenueId::Bitget {
            return Err(BitgetBindingError::Venue);
        }
        match binding.mode {
            GatewayMode::Test | GatewayMode::Live => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetBindingError {
    #[error("Bitget gateway binding is invalid")]
    Gateway,
    #[error("gateway binding does not select Bitget")]
    Venue,
}

#[cfg(test)]
mod tests {
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
    fn serde_keeps_the_existing_uta_wire_value() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            serde_json::from_str::<BitgetAccountBinding>(r#""uta_usdt_futures_hedge""#)?,
            BitgetAccountBinding::UtaUsdtFuturesHedge
        );
        assert_eq!(
            BitgetAccountBinding::UtaUsdtFuturesHedge.as_str(),
            "uta_usdt_futures_hedge"
        );
        Ok(())
    }

    #[test]
    fn gateway_identity_accepts_only_bitget_test_or_live() -> Result<(), Box<dyn std::error::Error>>
    {
        let profile = BitgetAccountBinding::UtaUsdtFuturesHedge;
        assert!("SHADOW".parse::<GatewayMode>().is_err());
        assert_eq!(
            profile.validate_gateway_binding(&binding(VenueId::Bitget, GatewayMode::Test)?),
            Ok(())
        );
        assert_eq!(
            profile.validate_gateway_binding(&binding(VenueId::Bitget, GatewayMode::Live)?),
            Ok(())
        );
        assert_eq!(
            profile.validate_gateway_binding(&binding(VenueId::Gate, GatewayMode::Live)?),
            Err(BitgetBindingError::Venue)
        );
        Ok(())
    }
}
