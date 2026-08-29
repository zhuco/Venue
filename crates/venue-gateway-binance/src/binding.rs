use serde::Deserialize;
use venue_domain::domain::Symbol;
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

#[must_use]
pub fn native_symbol(symbol: &Symbol) -> String {
    format!("{}{}", symbol.base(), symbol.quote())
}

/// The native private API family is a deployment binding, never mutation capability evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BinanceAccountBinding {
    PortfolioMarginUm,
}

impl BinanceAccountBinding {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PortfolioMarginUm => "portfolio_margin_um",
        }
    }

    /// Validates only the canonical gateway identity. Stage 7 capability evidence remains a
    /// separate runtime/WAL boundary and cannot be constructed from this result.
    pub fn validate_gateway_binding(
        self,
        binding: &GatewayBinding,
    ) -> Result<(), BinanceBindingError> {
        binding
            .validate()
            .map_err(|_| BinanceBindingError::Gateway)?;
        if binding.venue != VenueId::Binance {
            return Err(BinanceBindingError::Venue);
        }
        match binding.mode {
            GatewayMode::Test | GatewayMode::Live => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceBindingError {
    #[error("Binance gateway binding is invalid")]
    Gateway,
    #[error("gateway binding does not select Binance")]
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
    fn serde_keeps_the_existing_portfolio_margin_wire_value()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            serde_json::from_str::<BinanceAccountBinding>(r#""portfolio_margin_um""#)?,
            BinanceAccountBinding::PortfolioMarginUm
        );
        assert_eq!(
            BinanceAccountBinding::PortfolioMarginUm.as_str(),
            "portfolio_margin_um"
        );
        Ok(())
    }

    #[test]
    fn gateway_identity_accepts_only_binance_test_or_live() -> Result<(), Box<dyn std::error::Error>>
    {
        let profile = BinanceAccountBinding::PortfolioMarginUm;
        assert!("SHADOW".parse::<GatewayMode>().is_err());
        assert_eq!(
            profile.validate_gateway_binding(&binding(VenueId::Binance, GatewayMode::Test)?),
            Ok(())
        );
        assert_eq!(
            profile.validate_gateway_binding(&binding(VenueId::Binance, GatewayMode::Live)?),
            Ok(())
        );
        assert_eq!(
            profile.validate_gateway_binding(&binding(VenueId::Bybit, GatewayMode::Live)?),
            Err(BinanceBindingError::Venue)
        );
        Ok(())
    }

    #[test]
    fn native_symbol_is_derived_only_from_the_canonical_symbol()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(native_symbol(&"btc/usdt".parse()?), "BTCUSDT");
        Ok(())
    }
}
