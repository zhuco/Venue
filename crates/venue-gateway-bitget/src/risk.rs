use std::str::FromStr;

use rust_decimal::Decimal;
use serde_json::{Map, Value};
use venue_domain::domain::{
    AccountRiskSnapshot, Asset, LegRiskSnapshot, PositionSide, Price, RiskSourceStatus, Symbol,
    validate_risk_snapshot_pair,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetRiskReadback {
    pub raw_payloads: Vec<String>,
    pub account: AccountRiskSnapshot,
    pub legs: Vec<LegRiskSnapshot>,
}

const RISK_SNAPSHOT_MAX_AGE_MS: u64 = 3_000;

pub fn validate_risk_readback_window(
    started_at_ms: u64,
    observed_at_ms: u64,
) -> Result<(), BitgetRiskError> {
    if started_at_ms == 0
        || observed_at_ms < started_at_ms
        || observed_at_ms.saturating_sub(started_at_ms) > RISK_SNAPSHOT_MAX_AGE_MS
    {
        return Err(BitgetRiskError::RiskSnapshot);
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
) -> Result<(AccountRiskSnapshot, Vec<LegRiskSnapshot>), BitgetRiskError> {
    let assets = object(assets_value)?;
    let risk_currency: Asset = "USDT".parse().map_err(|_| BitgetRiskError::RiskSnapshot)?;
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
            return Err(BitgetRiskError::PositionMode);
        }
        let quantity = decimal(position, "total")?;
        if quantity.is_zero() {
            continue;
        }
        if quantity.is_sign_negative() {
            return Err(BitgetRiskError::RiskSnapshot);
        }
        let mark_price = Price::new(decimal(position, "markPrice")?)
            .map_err(|_| BitgetRiskError::RiskSnapshot)?;
        let notional = quantity
            .checked_mul(mark_price.value())
            .ok_or(BitgetRiskError::RiskSnapshot)?;
        let leg = LegRiskSnapshot {
            symbol: symbol.clone(),
            position_side: parse_position_side(text(position, "posSide")?)?,
            quantity,
            mark_price,
            contract_multiplier: Decimal::ONE,
            notional,
            unrealized_pnl: decimal(position, "unrealisedPnl")?,
            risk_currency: risk_currency.clone(),
            private_generation,
            observed_at_ms,
        };
        validate_risk_snapshot_pair(&account_snapshot, &leg, observed_at_ms, 0)
            .map_err(|_| BitgetRiskError::RiskSnapshot)?;
        legs.push(leg);
    }
    account_snapshot
        .validate_at(observed_at_ms, 0)
        .map_err(|_| BitgetRiskError::RiskSnapshot)?;
    Ok((account_snapshot, legs))
}

#[doc(hidden)]
pub fn native_symbol(symbol: &Symbol) -> Result<String, BitgetRiskError> {
    crate::public::native_symbol(symbol).map_err(|_| BitgetRiskError::Symbol)
}

#[doc(hidden)]
pub fn object(value: &Value) -> Result<&Map<String, Value>, BitgetRiskError> {
    value.as_object().ok_or(BitgetRiskError::Payload)
}

#[doc(hidden)]
pub fn text<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, BitgetRiskError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(BitgetRiskError::Payload)
}

#[doc(hidden)]
pub fn decimal(object: &Map<String, Value>, field: &str) -> Result<Decimal, BitgetRiskError> {
    match object.get(field) {
        Some(Value::String(value)) => {
            Decimal::from_str(value).map_err(|_| BitgetRiskError::Payload)
        }
        Some(Value::Number(value)) => {
            Decimal::from_str(&value.to_string()).map_err(|_| BitgetRiskError::Payload)
        }
        _ => Err(BitgetRiskError::Payload),
    }
}

#[doc(hidden)]
pub fn parse_position_side(value: &str) -> Result<PositionSide, BitgetRiskError> {
    match value {
        "long" => Ok(PositionSide::Long),
        "short" => Ok(PositionSide::Short),
        _ => Err(BitgetRiskError::Payload),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetRiskError {
    #[error("Bitget returned an invalid or incomplete risk payload")]
    Payload,
    #[error("Bitget risk symbol is outside the USDT perpetual deployment")]
    Symbol,
    #[error("Bitget risk position is not in Hedge mode")]
    PositionMode,
    #[error("Bitget risk snapshot is incomplete or internally inconsistent")]
    RiskSnapshot,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn risk_readback_window_is_bounded() {
        assert!(validate_risk_readback_window(1_000, 4_000).is_ok());
        assert_eq!(
            validate_risk_readback_window(1_000, 4_001),
            Err(BitgetRiskError::RiskSnapshot)
        );
        assert_eq!(
            validate_risk_readback_window(2_000, 1_999),
            Err(BitgetRiskError::RiskSnapshot)
        );
    }

    #[test]
    fn uta_risk_uses_usdt_equity_and_preserves_hedge_leg() -> Result<(), Box<dyn std::error::Error>>
    {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        let (account, legs) = parse_risk_snapshots(
            &json!({"accountEquity":"21", "usdtEquity":"20"}),
            &[json!({
                "symbol":"DOGEUSDT", "marginCoin":"USDT", "holdMode":"hedge_mode",
                "posSide":"long", "total":"600", "markPrice":"0.1",
                "unrealisedPnl":"1.1"
            })],
            &symbol,
            "uta_usdt_futures_hedge",
            8,
            1_000,
        )?;
        assert_eq!(account.account_equity, Decimal::from(20));
        assert_eq!(legs[0].notional, Decimal::from(60));
        assert_eq!(legs[0].position_side, PositionSide::Long);
        Ok(())
    }

    #[test]
    fn uta_risk_rejects_missing_equity_and_non_hedge_positions()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        assert!(
            parse_risk_snapshots(
                &json!({"assets":[{"coin":"USDT","balance":"20"}]}),
                &[],
                &symbol,
                "uta_usdt_futures_hedge",
                8,
                1_000,
            )
            .is_err()
        );
        assert_eq!(
            parse_risk_snapshots(
                &json!({"usdtEquity":"20"}),
                &[json!({
                    "symbol":"DOGEUSDT", "marginCoin":"USDT", "holdMode":"one_way_mode",
                    "posSide":"long", "total":"600", "markPrice":"0.1",
                    "unrealisedPnl":"1.1"
                })],
                &symbol,
                "uta_usdt_futures_hedge",
                8,
                1_000,
            ),
            Err(BitgetRiskError::PositionMode)
        );
        Ok(())
    }
}
