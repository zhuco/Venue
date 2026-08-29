use rust_decimal::Decimal;
use serde_json::{Map, Value};
use venue_domain::domain::{AccountBalance, Asset, Position, PositionSide, Price, Symbol};

use crate::risk::{self, BitgetRiskError};

/// Normalizes the signed UTA account payload without owning its HTTP request or evidence record.
pub fn parse_balance(value: &Value) -> Result<AccountBalance, BitgetAccountError> {
    let payload = object(value)?;
    let asset = payload
        .get("assets")
        .and_then(Value::as_array)
        .and_then(|assets| {
            assets
                .iter()
                .find(|asset| asset.get("coin").and_then(Value::as_str) == Some("USDT"))
        })
        .ok_or(BitgetAccountError::Payload)?;
    let asset = object(asset)?;
    let balance = AccountBalance {
        asset: Asset::new("USDT").map_err(|_| BitgetAccountError::Payload)?,
        wallet_balance: decimal(asset, "balance")?,
        available_balance: decimal(asset, "available")?,
        initial_margin: optional_decimal(payload.get("imr"))?,
        maintenance_margin: optional_decimal(payload.get("mmr"))?,
    };
    balance
        .validate()
        .map_err(|_| BitgetAccountError::Payload)?;
    Ok(balance)
}

/// Returns the exact account-mode fact; callers decide whether a non-Hedge account is admissible.
pub fn is_hedge_mode(value: &Value) -> Result<bool, BitgetAccountError> {
    Ok(object(value)?.get("holdMode").and_then(Value::as_str) == Some("hedge_mode"))
}

/// Normalizes one signed UTA Hedge position for the fixed canonical symbol.
pub fn parse_position(value: &Value, symbol: &Symbol) -> Result<Position, BitgetAccountError> {
    let object = object(value)?;
    if text(object, "symbol")? != native_symbol(symbol)?
        || text(object, "marginCoin")? != "USDT"
        || text(object, "holdMode")? != "hedge_mode"
    {
        return Err(BitgetAccountError::Payload);
    }
    Ok(Position {
        symbol: symbol.clone(),
        side: position_side(text(object, "posSide")?)?,
        quantity: decimal(object, "total")?,
        entry_price: optional_price(object.get("avgPrice"))?,
        mark_price: optional_price(object.get("markPrice"))?,
    })
}

#[doc(hidden)]
pub fn optional_decimal(value: Option<&Value>) -> Result<Decimal, BitgetAccountError> {
    match value {
        None | Some(Value::Null) => Ok(Decimal::ZERO),
        Some(Value::String(value)) if value.is_empty() => Ok(Decimal::ZERO),
        value => decimal_value(value),
    }
}

#[doc(hidden)]
pub fn optional_price(value: Option<&Value>) -> Result<Option<Price>, BitgetAccountError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value == "0" || value.is_empty() => Ok(None),
        value => Price::new(decimal_value(value)?)
            .map(Some)
            .map_err(|_| BitgetAccountError::Payload),
    }
}

fn native_symbol(symbol: &Symbol) -> Result<String, BitgetAccountError> {
    crate::public::native_symbol(symbol).map_err(|_| BitgetAccountError::Symbol)
}

fn object(value: &Value) -> Result<&Map<String, Value>, BitgetAccountError> {
    risk::object(value).map_err(map_risk_error)
}

fn text<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, BitgetAccountError> {
    risk::text(object, field).map_err(map_risk_error)
}

fn decimal(object: &Map<String, Value>, field: &str) -> Result<Decimal, BitgetAccountError> {
    risk::decimal(object, field).map_err(map_risk_error)
}

fn decimal_value(value: Option<&Value>) -> Result<Decimal, BitgetAccountError> {
    risk::decimal_value(value).map_err(map_risk_error)
}

fn position_side(value: &str) -> Result<PositionSide, BitgetAccountError> {
    risk::parse_position_side(value).map_err(map_risk_error)
}

const fn map_risk_error(error: BitgetRiskError) -> BitgetAccountError {
    match error {
        BitgetRiskError::Symbol => BitgetAccountError::Symbol,
        BitgetRiskError::Payload
        | BitgetRiskError::PositionMode
        | BitgetRiskError::RiskSnapshot => BitgetAccountError::Payload,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetAccountError {
    #[error("Bitget account payload is invalid or incomplete")]
    Payload,
    #[error("Bitget account symbol is outside the USDT perpetual deployment")]
    Symbol,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn account_balance_selects_usdt_and_preserves_margin_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let balance = parse_balance(&json!({
            "imr":"2.5", "mmr":1,
            "assets":[
                {"coin":"BTC", "balance":"3", "available":"2"},
                {"coin":"USDT", "balance":"20", "available":"17.5"}
            ]
        }))?;
        assert_eq!(balance.asset.as_str(), "USDT");
        assert_eq!(balance.wallet_balance, Decimal::from(20));
        assert_eq!(balance.available_balance, Decimal::new(175, 1));
        assert_eq!(balance.initial_margin, Decimal::new(25, 1));
        assert_eq!(balance.maintenance_margin, Decimal::ONE);
        Ok(())
    }

    #[test]
    fn settings_mode_is_exact_and_payload_shape_is_required() {
        assert_eq!(is_hedge_mode(&json!({"holdMode":"hedge_mode"})), Ok(true));
        assert_eq!(
            is_hedge_mode(&json!({"holdMode":"one_way_mode"})),
            Ok(false)
        );
        assert_eq!(is_hedge_mode(&json!([])), Err(BitgetAccountError::Payload));
    }

    #[test]
    fn position_is_bound_to_usdt_hedge_and_normalizes_optional_prices()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        let position = parse_position(
            &json!({
                "symbol":"DOGEUSDT", "marginCoin":"USDT", "holdMode":"hedge_mode",
                "posSide":"short", "total":"12", "avgPrice":"0", "markPrice":"0.1"
            }),
            &symbol,
        )?;
        assert_eq!(position.side, PositionSide::Short);
        assert_eq!(position.quantity, Decimal::from(12));
        assert_eq!(position.entry_price, None);
        assert_eq!(
            position.mark_price.map(Price::value),
            Some(Decimal::new(1, 1))
        );
        assert_eq!(
            parse_position(
                &json!({
                    "symbol":"DOGEUSDT", "marginCoin":"USDT", "holdMode":"one_way_mode",
                    "posSide":"short", "total":"12", "avgPrice":"0", "markPrice":"0.1"
                }),
                &symbol,
            ),
            Err(BitgetAccountError::Payload)
        );
        Ok(())
    }

    #[test]
    fn account_numbers_fail_closed() {
        assert!(
            parse_balance(&json!({
                "assets":[{"coin":"USDT", "balance":"-1", "available":"0"}]
            }))
            .is_err()
        );
        assert_eq!(
            optional_price(Some(&json!("not-a-decimal"))),
            Err(BitgetAccountError::Payload)
        );
    }
}
