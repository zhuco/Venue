use crate::domain::Symbol;

use super::{
    binance::{BinanceRiskReadback, PrivateError, PrivateReadbackError, PrivateRest},
    binance_portfolio, binance_private,
    binance_private::PrivateParseError,
};

impl PrivateRest {
    pub(crate) fn risk_readback(
        &self,
        symbol: &Symbol,
        private_generation: u64,
        _requested_at_ms: u64,
        max_age_ms: u64,
    ) -> Result<BinanceRiskReadback, PrivateReadbackError> {
        self.risk_readback_for_account(
            symbol,
            "portfolio_margin_um",
            private_generation,
            max_age_ms,
        )
    }

    pub(crate) fn grid_risk_readback(
        &self,
        symbol: &Symbol,
        account: &str,
        private_generation: u64,
        max_age_ms: u64,
    ) -> Result<BinanceRiskReadback, PrivateReadbackError> {
        self.risk_readback_for_account(symbol, account, private_generation, max_age_ms)
    }

    fn risk_readback_for_account(
        &self,
        symbol: &Symbol,
        account: &str,
        private_generation: u64,
        max_age_ms: u64,
    ) -> Result<BinanceRiskReadback, PrivateReadbackError> {
        let started_at_ms = self
            .authoritative_now_ms()
            .map_err(PrivateReadbackError::AccountRequest)?;
        // Every surface is independent. Join the complete tuple before assigning one generation;
        // this keeps the acquisition window within the same freshness budget without publishing
        // a partial private observation.
        let (account_result, positions, position_mode, account_config, conversion) =
            std::thread::scope(|scope| {
                let account = scope.spawn(|| self.account());
                let positions = scope.spawn(|| self.positions(symbol));
                let position_mode = scope.spawn(|| self.position_mode());
                let account_config = scope.spawn(|| self.um_account_config());
                let conversion = scope.spawn(|| self.portfolio_asset_index_price(symbol.quote()));
                (
                    account.join().unwrap_or(Err(PrivateError::Http)),
                    positions.join().unwrap_or(Err(PrivateError::Http)),
                    position_mode.join().unwrap_or(Err(PrivateError::Http)),
                    account_config.join().unwrap_or(Err(PrivateError::Http)),
                    conversion.join().unwrap_or(Err(PrivateError::Http)),
                )
            });
        let account_payload = account_result.map_err(PrivateReadbackError::AccountRequest)?;
        let positions_payload = positions.map_err(PrivateReadbackError::UmAccountRequest)?;
        let position_mode_payload =
            position_mode.map_err(PrivateReadbackError::PositionModeRequest)?;
        let account_config_payload =
            account_config.map_err(PrivateReadbackError::AccountConfigRequest)?;
        let conversion_payload = conversion.map_err(PrivateReadbackError::AccountRequest)?;
        let observed_at_ms = self
            .authoritative_now_ms()
            .map_err(PrivateReadbackError::AccountRequest)?;
        binance_private::validate_risk_readback_window(started_at_ms, observed_at_ms, max_age_ms)
            .map_err(PrivateReadbackError::Parse)?;
        let capabilities =
            binance_portfolio::capabilities(&account_config_payload, &position_mode_payload)
                .map_err(PrivateReadbackError::Parse)?;
        let conversion = binance_portfolio::parse_usd_conversion_evidence(
            &conversion_payload,
            symbol
                .quote()
                .parse()
                .map_err(|_| PrivateReadbackError::Parse(PrivateParseError::Payload))?,
            private_generation,
            observed_at_ms,
            max_age_ms,
        )
        .map_err(PrivateReadbackError::Parse)?;
        let (account, legs) = binance_portfolio::parse_risk_snapshots(
            &account_payload,
            &positions_payload,
            symbol,
            account,
            capabilities,
            private_generation,
            observed_at_ms,
            Some(&conversion),
        )
        .map_err(PrivateReadbackError::Parse)?;
        Ok(BinanceRiskReadback {
            raw_private_payloads: vec![
                account_payload,
                positions_payload,
                position_mode_payload,
                account_config_payload,
                conversion_payload,
            ],
            account,
            legs,
        })
    }
}
