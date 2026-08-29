use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::domain::{AccountBalance, Asset, Position, Price, Symbol};

use crate::{GateContractRules, decimal, decimal_value, dual_position_side, object, text};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GatePrivatePayloadError {
    #[error("Gate.io private payload is invalid or incomplete")]
    Payload,
    #[error("Gate.io private payload belongs to another symbol or product")]
    Symbol,
}

pub fn parse_position(
    value: &Value,
    symbol: &Symbol,
    rules: &GateContractRules,
) -> Result<Position, GatePrivatePayloadError> {
    let position = object(value).map_err(|_| GatePrivatePayloadError::Payload)?;
    if text(position, "contract").map_err(|_| GatePrivatePayloadError::Payload)?
        != rules.native_symbol
        || symbol != &rules.instrument.symbol
        || rules.quanto_multiplier <= Decimal::ZERO
    {
        return Err(GatePrivatePayloadError::Symbol);
    }
    let side =
        dual_position_side(text(position, "mode").map_err(|_| GatePrivatePayloadError::Payload)?)
            .map_err(|_| GatePrivatePayloadError::Payload)?;
    let quantity = decimal(position, "size")
        .map_err(|_| GatePrivatePayloadError::Payload)?
        .abs()
        .checked_mul(rules.quanto_multiplier)
        .ok_or(GatePrivatePayloadError::Payload)?;
    Ok(Position {
        symbol: symbol.clone(),
        side,
        quantity,
        entry_price: optional_price(position.get("entry_price"))?,
        mark_price: optional_price(position.get("mark_price"))?,
    })
}

pub fn parse_account_balance(value: &Value) -> Result<AccountBalance, GatePrivatePayloadError> {
    let account = object(value).map_err(|_| GatePrivatePayloadError::Payload)?;
    let asset = Asset::new("USDT").map_err(|_| GatePrivatePayloadError::Payload)?;
    let available_balance =
        decimal(account, "available").map_err(|_| GatePrivatePayloadError::Payload)?;
    let reported_total = decimal(account, "total").map_err(|_| GatePrivatePayloadError::Payload)?;
    // Signed dual-position payloads can report transient negative total while available remains
    // the admissible balance needed to reduce an existing hedge leg.
    let wallet_balance = if reported_total.is_sign_negative() {
        available_balance
    } else {
        reported_total
    };
    let initial_margin = optional_decimal(account.get("position_initial_margin"))?
        .checked_add(optional_decimal(account.get("order_initial_margin"))?)
        .ok_or(GatePrivatePayloadError::Payload)?;
    let maintenance_margin = optional_decimal(account.get("maintenance_margin"))?;
    let balance = AccountBalance {
        asset,
        wallet_balance,
        available_balance,
        initial_margin,
        maintenance_margin,
    };
    balance
        .validate()
        .map_err(|_| GatePrivatePayloadError::Payload)?;
    Ok(balance)
}

pub fn optional_price(value: Option<&Value>) -> Result<Option<Price>, GatePrivatePayloadError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        value => {
            let price = decimal_value(value).map_err(|_| GatePrivatePayloadError::Payload)?;
            if price.is_zero() {
                Ok(None)
            } else {
                Price::new(price)
                    .map(Some)
                    .map_err(|_| GatePrivatePayloadError::Payload)
            }
        }
    }
}

fn optional_decimal(value: Option<&Value>) -> Result<Decimal, GatePrivatePayloadError> {
    match value {
        None | Some(Value::Null) => Ok(Decimal::ZERO),
        Some(Value::String(value)) if value.is_empty() => Ok(Decimal::ZERO),
        value => decimal_value(value).map_err(|_| GatePrivatePayloadError::Payload),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use venue_domain::domain::{Amount, Instrument, MarketKind, PositionSide};

    use super::*;

    fn rules() -> Result<GateContractRules, Box<dyn std::error::Error>> {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        Ok(GateContractRules {
            native_symbol: "DOGE_USDT".to_owned(),
            instrument: Instrument {
                settlement_asset: Some("USDT".parse()?),
                minimum_notional: Amount::new("USDT".parse()?, Decimal::ZERO),
                symbol,
                market: MarketKind::LinearPerpetual,
                generation: 1,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::new(1, 1),
            },
            quanto_multiplier: Decimal::new(1, 1),
            minimum_contracts: Decimal::ONE,
            decimal_contracts: false,
        })
    }

    #[test]
    fn signed_account_balance_preserves_the_negative_total_fallback()
    -> Result<(), GatePrivatePayloadError> {
        let balance = parse_account_balance(&json!({
            "total":"-0.002", "available":"15.1",
            "position_initial_margin":"1", "order_initial_margin":null,
            "maintenance_margin":"0.5"
        }))?;
        assert_eq!(balance.wallet_balance, Decimal::new(151, 1));
        assert_eq!(balance.available_balance, Decimal::new(151, 1));
        assert_eq!(balance.initial_margin, Decimal::ONE);
        Ok(())
    }

    #[test]
    fn position_requires_exact_contract_and_dual_side() -> Result<(), Box<dyn std::error::Error>> {
        let rules = rules()?;
        let position = parse_position(
            &json!({
                "contract":"DOGE_USDT", "mode":"dual_short", "size":"-7",
                "entry_price":"0.09", "mark_price":0
            }),
            &rules.instrument.symbol,
            &rules,
        )?;
        assert_eq!(position.side, PositionSide::Short);
        assert_eq!(position.quantity, Decimal::new(7, 1));
        assert_eq!(position.mark_price, None);

        assert_eq!(
            parse_position(
                &json!({"contract":"BTC_USDT", "mode":"dual_short", "size":"1"}),
                &rules.instrument.symbol,
                &rules,
            ),
            Err(GatePrivatePayloadError::Symbol)
        );
        assert_eq!(
            parse_position(
                &json!({"contract":"DOGE_USDT", "mode":"single", "size":"1"}),
                &rules.instrument.symbol,
                &rules,
            ),
            Err(GatePrivatePayloadError::Payload)
        );
        Ok(())
    }
}
