use venue_gateway_api::{GatewayBinding, GatewayMode};

use crate::{BinanceAccountBinding, BinanceAuthError};

/// Fixed Portfolio Margin endpoints selected only through a validated gateway binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceConfig {
    binding: GatewayBinding,
    account_binding: BinanceAccountBinding,
    portfolio_rest_origin: &'static str,
    usd_m_public_rest_origin: &'static str,
    public_stream_origin: &'static str,
    private_stream_origin: &'static str,
}

impl BinanceConfig {
    pub fn for_binding(
        account_binding: BinanceAccountBinding,
        binding: &GatewayBinding,
    ) -> Result<Self, BinanceAuthError> {
        account_binding
            .validate_gateway_binding(binding)
            .map_err(|_| BinanceAuthError::Binding)?;
        let (
            portfolio_rest_origin,
            usd_m_public_rest_origin,
            public_stream_origin,
            private_stream_origin,
        ) = match binding.mode {
            GatewayMode::Test => (
                "https://testnet.binancefuture.com",
                "https://testnet.binancefuture.com",
                "wss://fstream.binancefuture.com",
                "wss://fstream.binancefuture.com/pm/ws",
            ),
            GatewayMode::Live => (
                "https://papi.binance.com",
                "https://fapi.binance.com",
                "wss://fstream.binance.com",
                "wss://fstream.binance.com/pm/ws",
            ),
        };
        Ok(Self {
            binding: binding.clone(),
            account_binding,
            portfolio_rest_origin,
            usd_m_public_rest_origin,
            public_stream_origin,
            private_stream_origin,
        })
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.binding.mode
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn account_binding(&self) -> BinanceAccountBinding {
        self.account_binding
    }

    #[must_use]
    pub const fn portfolio_rest_origin(&self) -> &'static str {
        self.portfolio_rest_origin
    }

    #[must_use]
    pub const fn usd_m_public_rest_origin(&self) -> &'static str {
        self.usd_m_public_rest_origin
    }

    #[must_use]
    pub const fn public_stream_origin(&self) -> &'static str {
        self.public_stream_origin
    }

    #[must_use]
    pub const fn private_stream_origin(&self) -> &'static str {
        self.private_stream_origin
    }

    pub(crate) fn validate_binding(
        &self,
        binding: &GatewayBinding,
    ) -> Result<(), BinanceAuthError> {
        self.account_binding
            .validate_gateway_binding(binding)
            .map_err(|_| BinanceAuthError::Binding)?;
        if &self.binding != binding {
            return Err(BinanceAuthError::Binding);
        }
        Ok(())
    }
}

pub mod endpoints {
    pub const LISTEN_KEY: &str = "/papi/v1/listenKey";
    pub const ACCOUNT: &str = "/papi/v1/account";
    pub const ACCOUNT_CONFIG: &str = "/papi/v1/um/accountConfig";
    pub const POSITION_MODE: &str = "/papi/v1/um/positionSide/dual";
    pub const POSITIONS: &str = "/papi/v1/um/positionRisk";
    pub const OPEN_ORDERS: &str = "/papi/v1/um/openOrders";
    pub const OPEN_ALGO_ORDERS: &str = "/papi/v1/um/algo/openAlgoOrders";
    pub const USER_TRADES: &str = "/papi/v1/um/userTrades";
    pub const ORDER: &str = "/papi/v1/um/order";
}

#[cfg(test)]
mod tests {
    use venue_gateway_api::{GatewayBinding, VenueId};

    use super::*;

    fn binding(mode: GatewayMode) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            VenueId::Binance,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    #[test]
    fn test_and_live_origins_are_fixed_and_disjoint() -> Result<(), Box<dyn std::error::Error>> {
        let test = BinanceConfig::for_binding(
            BinanceAccountBinding::PortfolioMarginUm,
            &binding(GatewayMode::Test)?,
        )?;
        assert_eq!(test.mode(), GatewayMode::Test);
        assert_eq!(
            test.portfolio_rest_origin(),
            "https://testnet.binancefuture.com"
        );
        assert_eq!(
            test.usd_m_public_rest_origin(),
            "https://testnet.binancefuture.com"
        );
        assert_eq!(
            test.public_stream_origin(),
            "wss://fstream.binancefuture.com"
        );
        assert_eq!(
            test.private_stream_origin(),
            "wss://fstream.binancefuture.com/pm/ws"
        );

        let live = BinanceConfig::for_binding(
            BinanceAccountBinding::PortfolioMarginUm,
            &binding(GatewayMode::Live)?,
        )?;
        assert_eq!(live.mode(), GatewayMode::Live);
        assert_eq!(live.portfolio_rest_origin(), "https://papi.binance.com");
        assert_eq!(live.usd_m_public_rest_origin(), "https://fapi.binance.com");
        assert_eq!(live.public_stream_origin(), "wss://fstream.binance.com");
        assert_eq!(
            live.private_stream_origin(),
            "wss://fstream.binance.com/pm/ws"
        );
        assert_ne!(test.portfolio_rest_origin(), live.portfolio_rest_origin());
        assert_ne!(test.private_stream_origin(), live.private_stream_origin());
        Ok(())
    }

    #[test]
    fn endpoint_config_rejects_a_non_binance_binding() -> Result<(), Box<dyn std::error::Error>> {
        let wrong = GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        assert_eq!(
            BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &wrong),
            Err(BinanceAuthError::Binding)
        );
        Ok(())
    }
}
