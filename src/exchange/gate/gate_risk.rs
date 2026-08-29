use std::str::FromStr;

use rust_decimal::Decimal;
use serde_json::Value;

use crate::domain::{
    AccountRiskSnapshot, Asset, LegRiskSnapshot, PositionSide, Price, RiskSourceStatus, Symbol,
    validate_risk_snapshot_pair,
};

use super::{GateContractRules, GateError, decimal, object, parse_dual_position_mode, text};

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

const RISK_SNAPSHOT_MAX_AGE_MS: u64 = 3_000;

pub(super) fn validate_risk_readback_window(
    started_at_ms: u64,
    observed_at_ms: u64,
) -> Result<(), GateError> {
    if started_at_ms == 0
        || observed_at_ms < started_at_ms
        || observed_at_ms.saturating_sub(started_at_ms) > RISK_SNAPSHOT_MAX_AGE_MS
    {
        return Err(GateError::RiskSnapshot);
    }
    Ok(())
}

pub(crate) fn requires_unified_single_currency(value: &Value) -> Result<bool, GateError> {
    match object(value)?.get("margin_mode") {
        Some(Value::Number(mode)) => match mode.as_u64() {
            Some(3) => Ok(true),
            Some(0) => Ok(false),
            Some(_) | None => Err(GateError::RiskAccountMode),
        },
        None | Some(Value::Null) => Ok(false),
        Some(_) => Err(GateError::RiskAccountMode),
    }
}

/// Normalizes Gate account equity and dual Hedge legs into USDT. Field presence is the account
/// mode proof: classic uses total+UPL, evolved classic cross uses cross_margin_balance.
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
    GateError,
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
pub(crate) fn parse_risk_snapshots_with_unified(
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
    GateError,
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
    GateError,
> {
    if !parse_dual_position_mode(account_value)? {
        return Err(GateError::PositionMode);
    }
    let account_object = object(account_value)?;
    let cross_margin_balance = match account_object.get("cross_margin_balance") {
        Some(Value::String(value)) if !value.is_empty() => {
            Some(Decimal::from_str(value).map_err(|_| GateError::RiskSnapshot)?)
        }
        Some(Value::Number(value)) => {
            Some(Decimal::from_str(&value.to_string()).map_err(|_| GateError::RiskSnapshot)?)
        }
        None | Some(Value::Null) | Some(Value::String(_)) => None,
        Some(_) => return Err(GateError::RiskSnapshot),
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
                return Err(GateError::RiskAccountMode);
            }
            let balances = unified_account
                .get("balances")
                .and_then(Value::as_object)
                .ok_or(GateError::RiskSnapshot)?;
            let usdt = balances
                .get("USDT")
                .and_then(Value::as_object)
                .ok_or(GateError::RiskSnapshot)?;
            Some(decimal(usdt, "margin_balance")?)
        }
        (None, None) => None,
        _ => return Err(GateError::RiskAccountMode),
    };
    let (mode, account_equity) = if unified_single_currency {
        (
            GateRiskAccountMode::UnifiedSingleCurrency,
            unified_single_currency_equity.ok_or(GateError::RiskAccountMode)?,
        )
    } else if cross_margin_balance.is_some_and(|value| value > Decimal::ZERO) {
        (
            GateRiskAccountMode::EvolvedClassicCross,
            cross_margin_balance.ok_or(GateError::RiskSnapshot)?,
        )
    } else if account_object.contains_key("total") && account_object.contains_key("unrealised_pnl")
    {
        (
            GateRiskAccountMode::Classic,
            decimal(account_object, "total")? + decimal(account_object, "unrealised_pnl")?,
        )
    } else {
        return Err(GateError::RiskAccountMode);
    };
    let risk_currency: Asset = "USDT".parse().map_err(|_| GateError::RiskSnapshot)?;
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
        let position_side = match text(position, "mode")? {
            "dual_long" => PositionSide::Long,
            "dual_short" => PositionSide::Short,
            _ => return Err(GateError::PositionMode),
        };
        let leg = LegRiskSnapshot {
            symbol: symbol.clone(),
            position_side,
            quantity: raw_contracts * rules.quanto_multiplier,
            mark_price: Price::new(decimal(position, "mark_price")?)
                .map_err(|_| GateError::RiskSnapshot)?,
            contract_multiplier: Decimal::ONE,
            notional: decimal(position, "value")?.abs(),
            unrealized_pnl: decimal(position, "unrealised_pnl")?,
            risk_currency: risk_currency.clone(),
            private_generation,
            observed_at_ms,
        };
        validate_risk_snapshot_pair(&account_snapshot, &leg, observed_at_ms, 0)
            .map_err(|_| GateError::RiskSnapshot)?;
        legs.push(leg);
    }
    account_snapshot
        .validate_at(observed_at_ms, 0)
        .map_err(|_| GateError::RiskSnapshot)?;
    Ok((mode, account_snapshot, legs))
}
