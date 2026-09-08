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
    /// Missing policy belongs to an already persisted command and retains its original limits.
    #[serde(default)]
    pub notional_limit_policy: CopyNotionalLimitPolicy,
    /// Persisted per command so recovery never changes an older order's rounding policy.
    #[serde(default)]
    pub round_open_quantity_up: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_quantity_rounding: Option<CopyOpenQuantityRounding>,
    pub max_order_notional: Decimal,
    pub max_total_notional: Decimal,
    pub max_deviation_bps: u32,
    pub source_price: Decimal,
    pub source_occurred_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyNotionalLimitPolicy {
    #[default]
    StoredLimits,
    ExchangeAccount,
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
    if context.notional_limit_policy == CopyNotionalLimitPolicy::ExchangeAccount {
        let quantity = normalize_copy_open_quantity(
            context,
            requested.min(rules.maximum_quantity),
            mark.price.value(),
            rules,
        )?;
        check_minimum_notional_at_price(mark.price, quantity, rules)?;
        return Ok(quantity);
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
    let bounded = requested.min(ceiling).min(rules.maximum_quantity);
    let quantity = normalize_copy_open_quantity(context, bounded, mark.price.value(), rules)?;
    let notional = quantity.checked_mul(unit).ok_or_else(invalid)?;
    let order_limit = minimum_rounding_order_limit(
        context,
        context.max_order_notional,
        notional,
        unit,
        mark.price.value(),
        rules,
    )?;
    if notional > remaining || notional > order_limit {
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
    if context.notional_limit_policy == CopyNotionalLimitPolicy::ExchangeAccount {
        normalize_copy_open_quantity(context, requested, price, rules)?;
        return Ok(());
    }
    let total = risk
        .signed_position_total()
        .map_err(|_| invalid())?
        .checked_add(risk.open_entry_order_total().map_err(|_| invalid())?)
        .ok_or_else(invalid)?;
    let asset = Asset::new(binding.symbol.quote()).map_err(|_| invalid())?;
    let unit = risk.value_in_usdt(&asset, price).map_err(|_| invalid())?;
    let quantity = normalize_copy_open_quantity(context, requested, price, rules)?;
    let notional = quantity.checked_mul(unit).ok_or_else(invalid)?;
    let order_limit = minimum_rounding_order_limit(
        context,
        context.max_order_notional,
        notional,
        unit,
        price,
        rules,
    )?;
    if notional > order_limit
        || total.checked_add(notional).ok_or_else(invalid)? > context.max_total_notional
    {
        return Err(BinanceExecutionError::Risk(CopyRiskRejection::TotalLimit));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CopyOpenQuantityRounding {
    MinimumUpOtherwiseDown,
}

pub(super) fn normalize_request_quantity(
    request: &ExecutionRequest,
    quantity: Decimal,
    rules: &BinanceInstrumentRules,
) -> Result<Decimal, BinanceExecutionError> {
    if request.origin == venue_control_protocol::kol::ExecutorCommandOrigin::Copy
        && let Some(risk) = request.copy_risk.as_ref()
        && let ExecutionOrderKind::Limit {
            reducing: false,
            price,
            ..
        } = request.order_kind
    {
        return normalize_copy_open_quantity(risk, quantity, price, rules);
    }
    normalize_quantity(quantity, rules)
}

pub(super) fn normalize_copy_open_quantity(
    context: &CopyRiskContext,
    requested: Decimal,
    price: Decimal,
    rules: &BinanceInstrumentRules,
) -> Result<Decimal, BinanceExecutionError> {
    match context.open_quantity_rounding {
        Some(CopyOpenQuantityRounding::MinimumUpOtherwiseDown) => {
            normalize_mirror_open_quantity_bounded(requested, price, rules)
        }
        None if context.round_open_quantity_up => {
            normalize_mirror_open_quantity(requested, price, rules)
        }
        None => normalize_quantity(requested, rules),
    }
}

fn minimum_rounding_order_limit(
    context: &CopyRiskContext,
    configured_limit: Decimal,
    actual_notional: Decimal,
    unit: Decimal,
    price: Decimal,
    rules: &BinanceInstrumentRules,
) -> Result<Decimal, BinanceExecutionError> {
    if actual_notional <= configured_limit
        || (context.open_quantity_rounding.is_none() && !context.round_open_quantity_up)
    {
        return Ok(configured_limit);
    }
    normalize_copy_open_quantity(
        context,
        configured_limit
            .checked_div(unit)
            .ok_or(BinanceExecutionError::Invalid)?,
        price,
        rules,
    )?
    .checked_mul(unit)
    .ok_or(BinanceExecutionError::Invalid)
}

/// Floors an opening quantity when that lot remains exchange-compliant. A smaller request is
/// enlarged only to the first lot satisfying the exchange quantity and notional minimums.
pub(super) fn normalize_mirror_open_quantity_bounded(
    requested: Decimal,
    price: Decimal,
    rules: &BinanceInstrumentRules,
) -> Result<Decimal, BinanceExecutionError> {
    if let Ok(quantity) = normalize_quantity(requested, rules)
        && quantity
            .checked_mul(price)
            .is_some_and(|notional| notional >= rules.instrument.minimum_notional.value)
    {
        return Ok(quantity);
    }
    normalize_mirror_open_quantity(requested, price, rules)
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
