use super::*;
use venue_domain::domain::Asset;
use venue_execution::AccountRiskEvidence;
use venue_gateway_binance::{BinanceInstrumentRules, BinanceMarkPrice};

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CopyRiskContext {
    pub max_order_notional: Decimal,
    pub max_total_notional: Decimal,
    pub max_deviation_bps: u32,
    pub source_price: Decimal,
    pub source_occurred_ms: u64,
}

impl CopyRiskContext {
    pub fn validate(&self) -> Result<(), BinanceExecutionError> {
        if [
            self.max_order_notional,
            self.max_total_notional,
            self.source_price,
        ]
        .iter()
        .any(|value| *value <= Decimal::ZERO || *value == Decimal::MAX)
            || self.max_order_notional > self.max_total_notional
            || self.max_deviation_bps > venue_control_protocol::kol::MAX_DEVIATION_BPS
            || self.source_occurred_ms == 0
        {
            return Err(BinanceExecutionError::Risk(
                CopyRiskRejection::InvalidPolicy,
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyRiskRejection {
    InvalidPolicy,
    PriceStale,
    PriceDeviation,
    TotalLimit,
    AccountFacts,
}

impl CopyRiskRejection {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidPolicy => "copy_risk_policy_invalid",
            Self::PriceStale => "copy_price_stale",
            Self::PriceDeviation => "copy_price_deviation",
            Self::TotalLimit => "copy_total_notional_limit",
            Self::AccountFacts => "copy_account_risk_unavailable",
        }
    }
}

pub(super) fn clip_open_quantity(
    context: &CopyRiskContext,
    binding: &GatewayBinding,
    risk: &AccountRiskEvidence,
    mark: &BinanceMarkPrice,
    rules: &BinanceInstrumentRules,
    requested: Decimal,
    now_ms: u64,
) -> Result<Decimal, BinanceExecutionError> {
    context.validate()?;
    let invalid = || BinanceExecutionError::Risk(CopyRiskRejection::AccountFacts);
    risk.validate_for(binding, now_ms).map_err(|_| invalid())?;
    if mark.symbol != binding.symbol
        || mark.observed_at_ms == 0
        || mark.observed_at_ms > now_ms
        || now_ms - mark.observed_at_ms > 5_000
        || context.source_occurred_ms > now_ms
    {
        return Err(BinanceExecutionError::Risk(CopyRiskRejection::PriceStale));
    }
    let deviation = (mark.price.value() - context.source_price)
        .abs()
        .checked_mul(Decimal::from(10_000))
        .ok_or_else(invalid)?;
    let maximum = context
        .source_price
        .checked_mul(Decimal::from(context.max_deviation_bps))
        .ok_or_else(invalid)?;
    if deviation > maximum {
        return Err(BinanceExecutionError::Risk(
            CopyRiskRejection::PriceDeviation,
        ));
    }
    let total = risk
        .signed_position_total()
        .map_err(|_| invalid())?
        .checked_add(risk.open_entry_order_total().map_err(|_| invalid())?)
        .ok_or_else(invalid)?;
    let remaining = context
        .max_total_notional
        .checked_sub(total)
        .ok_or_else(invalid)?;
    if remaining <= Decimal::ZERO {
        return Err(BinanceExecutionError::Risk(CopyRiskRejection::TotalLimit));
    }
    let asset = Asset::new(binding.symbol.quote()).map_err(|_| invalid())?;
    let unit = risk
        .value_in_usdt(&asset, mark.price.value())
        .map_err(|_| invalid())?;
    if unit <= Decimal::ZERO {
        return Err(invalid());
    }
    let notional_limit = remaining.min(context.max_order_notional);
    let ceiling = notional_limit.checked_div(unit).ok_or_else(invalid)?;
    let quantity = normalize_quantity(requested.min(ceiling).min(rules.maximum_quantity), rules)?;
    if quantity
        .checked_mul(unit)
        .is_none_or(|value| value > notional_limit)
    {
        return Err(invalid());
    }
    check_minimum_notional_at_price(mark.price, quantity, rules)?;
    Ok(quantity)
}

pub(super) fn check_minimum_notional_at_price(
    price: venue_domain::domain::Price,
    quantity: Decimal,
    rules: &BinanceInstrumentRules,
) -> Result<(), BinanceExecutionError> {
    quantity
        .checked_mul(price.value())
        .filter(|value| *value >= rules.instrument.minimum_notional.value)
        .map(|_| ())
        .ok_or(BinanceExecutionError::Invalid)
}

pub(super) fn check_mirror_limit_risk(
    context: &CopyRiskContext,
    binding: &GatewayBinding,
    risk: &AccountRiskEvidence,
    rules: &BinanceInstrumentRules,
    requested: Decimal,
    price: Decimal,
    now: u64,
) -> Result<(), BinanceExecutionError> {
    context.validate()?;
    let invalid = || BinanceExecutionError::Risk(CopyRiskRejection::AccountFacts);
    risk.validate_for(binding, now).map_err(|_| invalid())?;
    if price != context.source_price || context.source_occurred_ms > now {
        return Err(BinanceExecutionError::Risk(
            CopyRiskRejection::PriceDeviation,
        ));
    }
    let total = risk
        .signed_position_total()
        .map_err(|_| invalid())?
        .checked_add(risk.open_entry_order_total().map_err(|_| invalid())?)
        .ok_or_else(invalid)?;
    let asset = Asset::new(binding.symbol.quote()).map_err(|_| invalid())?;
    let unit = risk.value_in_usdt(&asset, price).map_err(|_| invalid())?;
    let notional = normalize_quantity(requested, rules)?
        .checked_mul(unit)
        .ok_or_else(invalid)?;
    if notional > context.max_order_notional
        || total.checked_add(notional).ok_or_else(invalid)? > context.max_total_notional
    {
        return Err(BinanceExecutionError::Risk(CopyRiskRejection::TotalLimit));
    }
    Ok(())
}
