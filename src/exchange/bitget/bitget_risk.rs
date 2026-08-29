use rust_decimal::Decimal;
use serde_json::Value;

use crate::domain::{
    AccountRiskSnapshot, Asset, LegRiskSnapshot, Price, RiskSourceStatus, Symbol,
    validate_risk_snapshot_pair,
};

use super::{BitgetError, decimal, native_symbol, object, parse_position_side, text};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetRiskReadback {
    pub raw_payloads: Vec<String>,
    pub account: AccountRiskSnapshot,
    pub legs: Vec<LegRiskSnapshot>,
}

const RISK_SNAPSHOT_MAX_AGE_MS: u64 = 3_000;

pub(super) fn validate_risk_readback_window(
    started_at_ms: u64,
    observed_at_ms: u64,
) -> Result<(), BitgetError> {
    if started_at_ms == 0
        || observed_at_ms < started_at_ms
        || observed_at_ms.saturating_sub(started_at_ms) > RISK_SNAPSHOT_MAX_AGE_MS
    {
        return Err(BitgetError::RiskSnapshot);
    }
    Ok(())
}

/// UTA exposes account `usdtEquity` separately from asset `balance`; only the former is valid for
/// account-level risk. Position quantity, mark, PnL and Hedge side stay symbol scoped.
pub fn parse_risk_snapshots(
    assets_value: &Value,
    position_values: &[Value],
    symbol: &Symbol,
    account: &str,
    private_generation: u64,
    observed_at_ms: u64,
) -> Result<(AccountRiskSnapshot, Vec<LegRiskSnapshot>), BitgetError> {
    let assets = object(assets_value)?;
    let risk_currency: Asset = "USDT".parse().map_err(|_| BitgetError::RiskSnapshot)?;
    let account_snapshot = AccountRiskSnapshot {
        exchange: "bitget".to_owned(),
        account: account.to_owned(),
        risk_currency: risk_currency.clone(),
        account_equity: decimal(assets, "usdtEquity")?,
        private_generation,
        observed_at_ms,
        source_status: RiskSourceStatus::Complete,
    };
    let native = native_symbol(symbol)?;
    let mut legs = Vec::new();
    for value in position_values {
        let position = object(value)?;
        if position.get("symbol").and_then(Value::as_str) != Some(native.as_str()) {
            continue;
        }
        if text(position, "marginCoin")? != "USDT" || text(position, "holdMode")? != "hedge_mode" {
            return Err(BitgetError::PositionMode);
        }
        let quantity = decimal(position, "total")?;
        if quantity.is_zero() {
            continue;
        }
        if quantity.is_sign_negative() {
            return Err(BitgetError::RiskSnapshot);
        }
        let mark_price =
            Price::new(decimal(position, "markPrice")?).map_err(|_| BitgetError::RiskSnapshot)?;
        let leg = LegRiskSnapshot {
            symbol: symbol.clone(),
            position_side: parse_position_side(text(position, "posSide")?)?,
            quantity,
            mark_price,
            contract_multiplier: Decimal::ONE,
            notional: quantity * mark_price.value(),
            unrealized_pnl: decimal(position, "unrealisedPnl")?,
            risk_currency: risk_currency.clone(),
            private_generation,
            observed_at_ms,
        };
        validate_risk_snapshot_pair(&account_snapshot, &leg, observed_at_ms, 0)
            .map_err(|_| BitgetError::RiskSnapshot)?;
        legs.push(leg);
    }
    account_snapshot
        .validate_at(observed_at_ms, 0)
        .map_err(|_| BitgetError::RiskSnapshot)?;
    Ok((account_snapshot, legs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn risk_readback_window_is_bounded() {
        assert!(validate_risk_readback_window(1_000, 4_000).is_ok());
        assert!(matches!(
            validate_risk_readback_window(1_000, 4_001),
            Err(BitgetError::RiskSnapshot)
        ));
        assert!(matches!(
            validate_risk_readback_window(2_000, 1_999),
            Err(BitgetError::RiskSnapshot)
        ));
    }
}
