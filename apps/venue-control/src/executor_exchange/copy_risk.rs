use super::*;
use venue_domain::domain::Asset;
use venue_execution::AccountRiskEvidence;
use venue_gateway_binance::{BinanceInstrumentRules, BinanceMarkPrice};

#[cfg(test)]
#[path = "copy_rounding_tests.rs"]
mod rounding_tests;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CopyRiskContext {
    /// Persisted per command so recovery never changes an older order's rounding policy.
    #[serde(default)]
    pub round_open_quantity_up: bool,
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
    let quantity = if context.round_open_quantity_up {
        normalize_mirror_open_quantity(requested, price, rules)?
    } else {
        normalize_quantity(requested, rules)?
    };
    let notional = quantity.checked_mul(unit).ok_or_else(invalid)?;
    let order_limit = if context.round_open_quantity_up && notional > context.max_order_notional {
        normalize_mirror_open_quantity(
            context
                .max_order_notional
                .checked_div(unit)
                .ok_or_else(invalid)?,
            price,
            rules,
        )?
        .checked_mul(unit)
        .ok_or_else(invalid)?
    } else {
        context.max_order_notional
    };
    if notional > order_limit
        || total.checked_add(notional).ok_or_else(invalid)? > context.max_total_notional
    {
        return Err(BinanceExecutionError::Risk(CopyRiskRejection::TotalLimit));
    }
    Ok(())
}

pub(super) fn normalize_request_quantity(
    request: &ExecutionRequest,
    quantity: Decimal,
    rules: &BinanceInstrumentRules,
) -> Result<Decimal, BinanceExecutionError> {
    if request.origin == venue_control_protocol::kol::ExecutorCommandOrigin::Copy
        && request
            .copy_risk
            .as_ref()
            .is_some_and(|risk| risk.round_open_quantity_up)
        && let ExecutionOrderKind::Limit {
            reducing: false,
            price,
            ..
        } = request.order_kind
    {
        return normalize_mirror_open_quantity(quantity, price, rules);
    }
    normalize_quantity(quantity, rules)
}

/// Smallest valid opening size at the source limit price. Closing intents keep their
/// inventory-bounded floor policy and must never be enlarged to satisfy an opening minimum.
pub(super) fn normalize_mirror_open_quantity(
    requested: Decimal,
    price: Decimal,
    rules: &BinanceInstrumentRules,
) -> Result<Decimal, BinanceExecutionError> {
    let invalid = || BinanceExecutionError::Invalid;
    let step = rules.instrument.quantity_step;
    let minimum = rules.instrument.minimum_notional.value;
    if requested <= Decimal::ZERO
        || price <= Decimal::ZERO
        || step <= Decimal::ZERO
        || rules.minimum_quantity <= Decimal::ZERO
        || minimum < Decimal::ZERO
    {
        return Err(invalid());
    }
    let target = requested
        .max(rules.minimum_quantity)
        .max(minimum.checked_div(price).ok_or_else(invalid)?);
    // Remainder arithmetic avoids a rounded Decimal division losing the final lot.
    let remainder = target.checked_rem(step).ok_or_else(invalid)?;
    let mut quantity = if remainder > Decimal::ZERO {
        target
            .checked_sub(remainder)
            .and_then(|n| n.checked_add(step))
            .ok_or_else(invalid)?
    } else {
        target
    };
    if quantity.checked_mul(price).ok_or_else(invalid)? < minimum {
        quantity = quantity.checked_add(step).ok_or_else(invalid)?;
    }
    normalize_quantity(quantity, rules)
}
