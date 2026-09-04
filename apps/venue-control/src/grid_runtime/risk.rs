use super::*;

impl BinanceGridRuntime {
    pub(super) async fn risk_facts(
        &mut self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        private: &PrivateFacts,
        _now: u64,
    ) -> Result<Option<GridRiskFacts>, BinanceGridRuntimeError> {
        if !record.instance.config.profit_reduction.enabled {
            return Ok(None);
        }
        let state = self
            .markets
            .get(&record.instance.instance_id)
            .ok_or(BinanceGridRuntimeError::Market)?;
        let conversion = state
            .quote_usd_evidence(
                projection.private_generation,
                record.instance.config.reset_policy.stale_market_ms,
            )
            .await
            .map_err(|_| BinanceGridRuntimeError::Market)?;
        self.hot_path
            .cache_conversion(record.instance.instance_id.clone(), conversion.clone());
        Self::risk_from_conversion(record, projection, private, conversion).map(Some)
    }

    pub(super) fn risk_from_conversion(
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        private: &PrivateFacts,
        conversion: venue_gateway_binance::portfolio::UsdConversionEvidence,
    ) -> Result<GridRiskFacts, BinanceGridRuntimeError> {
        let mut usd_assets = projection
            .assets
            .iter()
            .filter(|asset| asset.asset == "USD");
        let equity = usd_assets
            .next()
            .filter(|asset| asset.equity > Decimal::ZERO)
            .ok_or(BinanceGridRuntimeError::Facts)?
            .equity;
        if usd_assets.next().is_some() {
            return Err(BinanceGridRuntimeError::Facts);
        }
        if conversion.private_generation != projection.private_generation
            || conversion.asset.as_str() != record.instance.symbol.quote()
            || conversion.usd_per_asset <= Decimal::ZERO
        {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let usd = Asset::new("USD").map_err(|_| BinanceGridRuntimeError::Facts)?;
        let account = AccountRiskSnapshot {
            exchange: "binance".to_owned(),
            account: record.instance.trading_account_id.clone(),
            risk_currency: usd.clone(),
            account_equity: equity,
            private_generation: projection.private_generation,
            observed_at_ms: projection.observed_ms,
            source_status: RiskSourceStatus::Complete,
        };
        let mut legs = Vec::new();
        for position in &private.positions {
            if position.quantity.is_zero() {
                continue;
            }
            let entry = position.entry_price.ok_or(BinanceGridRuntimeError::Facts)?;
            let mark = position.mark_price.ok_or(BinanceGridRuntimeError::Facts)?;
            if entry <= Decimal::ZERO || mark <= Decimal::ZERO {
                return Err(BinanceGridRuntimeError::Facts);
            }
            let quote_notional = position
                .quantity
                .checked_mul(mark)
                .ok_or(BinanceGridRuntimeError::Facts)?;
            let notional = quote_notional
                .checked_mul(conversion.usd_per_asset)
                .ok_or(BinanceGridRuntimeError::Facts)?;
            let price_delta = match position.position_side {
                PositionSide::Long => mark.checked_sub(entry),
                PositionSide::Short => entry.checked_sub(mark),
                PositionSide::Net => None,
            }
            .ok_or(BinanceGridRuntimeError::Facts)?;
            let pnl = price_delta
                .checked_mul(position.quantity)
                .and_then(|value| value.checked_mul(conversion.usd_per_asset))
                .ok_or(BinanceGridRuntimeError::Facts)?;
            legs.push(LegRiskSnapshot {
                symbol: position.symbol.clone(),
                position_side: position.position_side,
                quantity: position.quantity,
                mark_price: Price::new(mark).map_err(|_| BinanceGridRuntimeError::Facts)?,
                contract_multiplier: conversion.usd_per_asset,
                notional,
                unrealized_pnl: pnl,
                risk_currency: usd.clone(),
                private_generation: projection.private_generation,
                observed_at_ms: projection.observed_ms,
            });
        }
        let quote_per_risk_unit = Decimal::ONE
            .checked_div(conversion.usd_per_asset)
            .filter(|value| *value > Decimal::ZERO)
            .ok_or(BinanceGridRuntimeError::Facts)?;
        Ok(GridRiskFacts {
            account,
            legs,
            conversion: GridRiskConversion {
                risk_currency: usd,
                quote_currency: conversion.asset,
                quote_per_risk_unit,
                private_generation: projection.private_generation,
                observed_at_ms: conversion.observed_at_ms,
            },
        })
    }
}
