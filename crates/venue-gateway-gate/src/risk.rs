use std::str::FromStr;

use rust_decimal::Decimal;
use serde_json::{Map, Value};
use venue_domain::domain::{
    AccountRiskSnapshot, Amount, Asset, Instrument, LegRiskSnapshot, MarketKind, PositionSide,
    Price, RiskSourceStatus, Symbol, validate_risk_snapshot_pair,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateContractRules {
    pub native_symbol: String,
    pub instrument: Instrument,
    pub quanto_multiplier: Decimal,
    pub minimum_contracts: Decimal,
    pub decimal_contracts: bool,
}

impl GateContractRules {
    #[must_use]
    pub fn minimum_quantity(&self) -> Decimal {
        self.quanto_multiplier * self.minimum_contracts
    }

    pub fn native_contracts_checked(&self, quantity: Decimal) -> Result<Decimal, GateRiskError> {
        if quantity <= Decimal::ZERO
            || self.quanto_multiplier <= Decimal::ZERO
            || self.minimum_contracts <= Decimal::ZERO
        {
            return Err(GateRiskError::Quantity);
        }
        let contracts = quantity
            .checked_div(self.quanto_multiplier)
            .ok_or(GateRiskError::Quantity)?;
        if contracts < self.minimum_contracts {
            return Err(GateRiskError::Quantity);
        }
        if !self.decimal_contracts && contracts.fract() != Decimal::ZERO {
            return Err(GateRiskError::Quantity);
        }
        Ok(contracts)
    }
}

/// Normalizes one fresh Gate USDT perpetual contract entry. The caller supplies the public
/// catalogue response and retains its transport ownership; no cached/static contract rule may
/// be used to admit an account-wide entry risk calculation.
pub fn parse_contract_rules(
    value: &Value,
    symbol: Symbol,
    generation: u64,
) -> Result<GateContractRules, GateRiskError> {
    if generation == 0 || symbol.quote() != "USDT" {
        return Err(GateRiskError::RiskSnapshot);
    }
    let item = object(value)?;
    let native_symbol = text(item, "name")?.to_owned();
    if native_symbol != format!("{}_USDT", symbol.base())
        || item.get("in_delisting").and_then(Value::as_bool) != Some(false)
        || matches!(
            item.get("status").and_then(Value::as_str),
            Some("delisted" | "offline")
        )
    {
        return Err(GateRiskError::RiskSnapshot);
    }
    let quanto_multiplier = decimal(item, "quanto_multiplier")?;
    let minimum_contracts = decimal(item, "order_size_min")?.max(Decimal::ONE);
    let price_tick =
        Price::new(decimal(item, "order_price_round")?).map_err(|_| GateRiskError::RiskSnapshot)?;
    let instrument = Instrument {
        symbol,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(Asset::new("USDT").map_err(|_| GateRiskError::RiskSnapshot)?),
        generation,
        price_tick,
        quantity_step: quanto_multiplier,
        minimum_notional: Amount::new(
            Asset::new("USDT").map_err(|_| GateRiskError::RiskSnapshot)?,
            Decimal::ZERO,
        ),
    };
    instrument
        .validate()
        .map_err(|_| GateRiskError::RiskSnapshot)?;
    if quanto_multiplier <= Decimal::ZERO || minimum_contracts <= Decimal::ZERO {
        return Err(GateRiskError::Quantity);
    }
    Ok(GateContractRules {
        native_symbol,
        instrument,
        quanto_multiplier,
        minimum_contracts,
        decimal_contracts: item
            .get("enable_decimal")
            .and_then(Value::as_bool)
            .ok_or(GateRiskError::Payload)?,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateRiskReadback {
    pub raw_payloads: Vec<String>,
    pub account_mode: GateRiskAccountMode,
    pub account: AccountRiskSnapshot,
    pub legs: Vec<LegRiskSnapshot>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateRiskAccountMode {
    Classic,
    EvolvedClassicCross,
    UnifiedSingleCurrency,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GateRiskError {
    #[error("Gate.io returned an invalid or incomplete payload")]
    Payload,
    #[error("Gate.io account is not in the required dual-position mode")]
    PositionMode,
    #[error("Gate.io risk account mode cannot be proven from signed fields")]
    RiskAccountMode,
    #[error("Gate.io risk snapshot is incomplete or internally inconsistent")]
    RiskSnapshot,
    #[error("Gate.io physical quantity is invalid for the selected contract")]
    Quantity,
}

const RISK_SNAPSHOT_MAX_AGE_MS: u64 = 3_000;

pub fn validate_risk_readback_window(
    started_at_ms: u64,
    observed_at_ms: u64,
) -> Result<(), GateRiskError> {
    if started_at_ms == 0
        || observed_at_ms < started_at_ms
        || observed_at_ms.saturating_sub(started_at_ms) > RISK_SNAPSHOT_MAX_AGE_MS
    {
        return Err(GateRiskError::RiskSnapshot);
    }
    Ok(())
}

pub fn object(value: &Value) -> Result<&Map<String, Value>, GateRiskError> {
    value.as_object().ok_or(GateRiskError::Payload)
}

pub fn text<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, GateRiskError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(GateRiskError::Payload)
}

pub fn decimal(object: &Map<String, Value>, field: &str) -> Result<Decimal, GateRiskError> {
    decimal_value(object.get(field))
}

pub fn decimal_value(value: Option<&Value>) -> Result<Decimal, GateRiskError> {
    match value {
        Some(Value::String(value)) => Decimal::from_str(value).map_err(|_| GateRiskError::Payload),
        Some(Value::Number(value)) => {
            Decimal::from_str(&value.to_string()).map_err(|_| GateRiskError::Payload)
        }
        _ => Err(GateRiskError::Payload),
    }
}

pub fn parse_dual_position_mode(value: &Value) -> Result<bool, GateRiskError> {
    matches!(
        object(value)?.get("position_mode").and_then(Value::as_str),
        Some("dual")
    )
    .then_some(true)
    .ok_or(GateRiskError::PositionMode)
}

pub fn dual_position_side(value: &str) -> Result<PositionSide, GateRiskError> {
    match value {
        "dual_long" => Ok(PositionSide::Long),
        "dual_short" => Ok(PositionSide::Short),
        _ => Err(GateRiskError::PositionMode),
    }
}

pub fn requires_unified_single_currency(value: &Value) -> Result<bool, GateRiskError> {
    match object(value)?.get("margin_mode") {
        Some(Value::Number(mode)) => match mode.as_u64() {
            Some(3) => Ok(true),
            Some(0) => Ok(false),
            Some(_) | None => Err(GateRiskError::RiskAccountMode),
        },
        None | Some(Value::Null) => Ok(false),
        Some(_) => Err(GateRiskError::RiskAccountMode),
    }
}

/// Normalizes Gate account equity and dual Hedge legs into USDT. Field presence is the account
/// mode proof: classic uses total+UPL, evolved classic cross uses cross_margin_balance.
#[allow(clippy::too_many_arguments)]
pub fn parse_risk_snapshots(
    account_value: &Value,
    position_values: &[Value],
    symbol: &Symbol,
    rules: &GateContractRules,
    account: &str,
    private_generation: u64,
    observed_at_ms: u64,
) -> Result<
    (
        GateRiskAccountMode,
        AccountRiskSnapshot,
        Vec<LegRiskSnapshot>,
    ),
    GateRiskError,
> {
    parse_risk_snapshots_inner(
        account_value,
        None,
        None,
        position_values,
        symbol,
        rules,
        account,
        private_generation,
        observed_at_ms,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn parse_risk_snapshots_with_unified(
    account_value: &Value,
    unified_mode_value: &Value,
    unified_account_value: &Value,
    position_values: &[Value],
    symbol: &Symbol,
    rules: &GateContractRules,
    account: &str,
    private_generation: u64,
    observed_at_ms: u64,
) -> Result<
    (
        GateRiskAccountMode,
        AccountRiskSnapshot,
        Vec<LegRiskSnapshot>,
    ),
    GateRiskError,
> {
    parse_risk_snapshots_inner(
        account_value,
        Some(unified_mode_value),
        Some(unified_account_value),
        position_values,
        symbol,
        rules,
        account,
        private_generation,
        observed_at_ms,
    )
}

#[allow(clippy::too_many_arguments)]
fn parse_risk_snapshots_inner(
    account_value: &Value,
    unified_mode_value: Option<&Value>,
    unified_account_value: Option<&Value>,
    position_values: &[Value],
    symbol: &Symbol,
    rules: &GateContractRules,
    account: &str,
    private_generation: u64,
    observed_at_ms: u64,
) -> Result<
    (
        GateRiskAccountMode,
        AccountRiskSnapshot,
        Vec<LegRiskSnapshot>,
    ),
    GateRiskError,
> {
    if account.trim().is_empty()
        || private_generation == 0
        || observed_at_ms == 0
        || symbol != &rules.instrument.symbol
        || rules.native_symbol.is_empty()
        || rules.quanto_multiplier <= Decimal::ZERO
        || rules.instrument.validate().is_err()
    {
        return Err(GateRiskError::RiskSnapshot);
    }
    if !parse_dual_position_mode(account_value)? {
        return Err(GateRiskError::PositionMode);
    }
    let account_object = object(account_value)?;
    let cross_margin_balance = match account_object.get("cross_margin_balance") {
        Some(Value::String(value)) if !value.is_empty() => {
            Some(Decimal::from_str(value).map_err(|_| GateRiskError::RiskSnapshot)?)
        }
        Some(Value::Number(value)) => {
            Some(Decimal::from_str(&value.to_string()).map_err(|_| GateRiskError::RiskSnapshot)?)
        }
        None | Some(Value::Null) | Some(Value::String(_)) => None,
        Some(_) => return Err(GateRiskError::RiskSnapshot),
    };
    let unified_single_currency = requires_unified_single_currency(account_value)?;
    let unified_single_currency_equity = match (unified_mode_value, unified_account_value) {
        (Some(mode_value), Some(account_value)) => {
            let mode = object(mode_value)?;
            let unified_account = object(account_value)?;
            if mode.get("mode").and_then(Value::as_str) != Some("single_currency")
                || unified_account.get("mode").and_then(Value::as_str) != Some("single_currency")
                || unified_account.get("locked").and_then(Value::as_bool) != Some(false)
            {
                return Err(GateRiskError::RiskAccountMode);
            }
            let balances = unified_account
                .get("balances")
                .and_then(Value::as_object)
                .ok_or(GateRiskError::RiskSnapshot)?;
            let usdt = balances
                .get("USDT")
                .and_then(Value::as_object)
                .ok_or(GateRiskError::RiskSnapshot)?;
            Some(decimal(usdt, "margin_balance")?)
        }
        (None, None) => None,
        _ => return Err(GateRiskError::RiskAccountMode),
    };
    let (mode, account_equity) = if unified_single_currency {
        (
            GateRiskAccountMode::UnifiedSingleCurrency,
            unified_single_currency_equity.ok_or(GateRiskError::RiskAccountMode)?,
        )
    } else if cross_margin_balance.is_some_and(|value| value > Decimal::ZERO) {
        (
            GateRiskAccountMode::EvolvedClassicCross,
            cross_margin_balance.ok_or(GateRiskError::RiskSnapshot)?,
        )
    } else if account_object.contains_key("total") && account_object.contains_key("unrealised_pnl")
    {
        (
            GateRiskAccountMode::Classic,
            decimal(account_object, "total")?
                .checked_add(decimal(account_object, "unrealised_pnl")?)
                .ok_or(GateRiskError::RiskSnapshot)?,
        )
    } else {
        return Err(GateRiskError::RiskAccountMode);
    };
    let risk_currency: Asset = "USDT".parse().map_err(|_| GateRiskError::RiskSnapshot)?;
    let account_snapshot = AccountRiskSnapshot {
        exchange: "gate".to_owned(),
        account: account.to_owned(),
        risk_currency: risk_currency.clone(),
        account_equity,
        private_generation,
        observed_at_ms,
        source_status: RiskSourceStatus::Complete,
    };
    let mut legs = Vec::new();
    for value in position_values {
        let position = object(value)?;
        if position.get("contract").and_then(Value::as_str) != Some(rules.native_symbol.as_str()) {
            continue;
        }
        let raw_contracts = decimal(position, "size")?.abs();
        if raw_contracts.is_zero() {
            continue;
        }
        let position_side = dual_position_side(text(position, "mode")?)?;
        let leg = LegRiskSnapshot {
            symbol: symbol.clone(),
            position_side,
            quantity: raw_contracts
                .checked_mul(rules.quanto_multiplier)
                .ok_or(GateRiskError::RiskSnapshot)?,
            mark_price: Price::new(decimal(position, "mark_price")?)
                .map_err(|_| GateRiskError::RiskSnapshot)?,
            contract_multiplier: Decimal::ONE,
            notional: decimal(position, "value")?.abs(),
            unrealized_pnl: decimal(position, "unrealised_pnl")?,
            risk_currency: risk_currency.clone(),
            private_generation,
            observed_at_ms,
        };
        validate_risk_snapshot_pair(&account_snapshot, &leg, observed_at_ms, 0)
            .map_err(|_| GateRiskError::RiskSnapshot)?;
        legs.push(leg);
    }
    account_snapshot
        .validate_at(observed_at_ms, 0)
        .map_err(|_| GateRiskError::RiskSnapshot)?;
    Ok((mode, account_snapshot, legs))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use venue_domain::domain::{Amount, MarketKind};

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
    fn quantity_conversion_and_window_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let rules = rules()?;
        assert_eq!(
            rules.native_contracts_checked(Decimal::new(7, 1))?,
            Decimal::new(7, 0)
        );
        assert_eq!(
            rules.native_contracts_checked(Decimal::new(15, 2)),
            Err(GateRiskError::Quantity)
        );
        let mut invalid = rules.clone();
        invalid.quanto_multiplier = Decimal::ZERO;
        assert_eq!(
            invalid.native_contracts_checked(Decimal::ONE),
            Err(GateRiskError::Quantity)
        );
        assert!(validate_risk_readback_window(1_000, 4_000).is_ok());
        assert_eq!(
            validate_risk_readback_window(1_000, 4_001),
            Err(GateRiskError::RiskSnapshot)
        );
        Ok(())
    }

    #[test]
    fn classic_and_unified_risk_are_normalized_from_signed_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let rules = rules()?;
        let positions = vec![json!({
            "contract":"DOGE_USDT", "mode":"dual_short", "size":"-7",
            "mark_price":"0.1", "value":"0.07", "unrealised_pnl":"1.2"
        })];
        let (mode, account, legs) = parse_risk_snapshots(
            &json!({"position_mode":"dual", "total":"20", "unrealised_pnl":"2"}),
            &positions,
            &rules.instrument.symbol,
            &rules,
            "usdt_futures_dual",
            5,
            1_000,
        )?;
        assert_eq!(mode, GateRiskAccountMode::Classic);
        assert_eq!(account.account_equity, Decimal::new(22, 0));
        assert_eq!(legs[0].position_side, PositionSide::Short);

        let (mode, unified, _) = parse_risk_snapshots_with_unified(
            &json!({"position_mode":"dual", "margin_mode":3}),
            &json!({"mode":"single_currency"}),
            &json!({
                "mode":"single_currency", "locked":false,
                "balances":{"USDT":{"margin_balance":"22.5"}}
            }),
            &positions,
            &rules.instrument.symbol,
            &rules,
            "usdt_futures",
            6,
            2_000,
        )?;
        assert_eq!(mode, GateRiskAccountMode::UnifiedSingleCurrency);
        assert_eq!(unified.account_equity, Decimal::new(225, 1));
        Ok(())
    }
}
