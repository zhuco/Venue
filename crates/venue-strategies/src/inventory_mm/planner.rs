use rust_decimal::Decimal;
use venue_domain::domain::{FieldState, MarketKind, OrderSide, OrderState, PositionSide, Price};

use super::{MmAction, MmConfig, MmControl, MmError, MmInput, MmPlan, MmQuote, MmReason};

type Result<T> = std::result::Result<T, MmError>;

impl MmConfig {
    pub fn validate(&self) -> Result<()> {
        let amounts = [
            &self.order_notional,
            &self.max_leg_notional,
            &self.max_gross_notional,
            &self.max_net_notional,
        ];
        if amounts
            .iter()
            .any(|v| v.value <= Decimal::ZERO || v.asset.as_str() != self.symbol.quote())
            || self.max_leg_notional.value < self.order_notional.value
            || self.max_gross_notional.value < self.max_leg_notional.value
            || self.max_net_notional.value < self.order_notional.value
            || self.max_net_notional.value > self.max_leg_notional.value
            || self.base_half_spread_bps <= Decimal::ZERO
            || self.base_half_spread_bps >= Decimal::from(1_000)
            || self.inventory_skew_bps < Decimal::ZERO
            || self.inventory_skew_bps >= Decimal::from(1_000)
            || self.volatility_multiplier < Decimal::ZERO
            || !(2..=10_000).contains(&self.volatility_period)
            || self.refresh_interval_ms == 0
            || self.max_market_age_ms == 0
            || self.max_private_age_ms == 0
            || self.max_loss_quote.value <= Decimal::ZERO
            || self.max_drawdown_quote.value <= Decimal::ZERO
            || self.max_loss_quote.asset != self.max_drawdown_quote.asset
        {
            return Err(MmError::Config);
        }
        Ok(())
    }
}

pub fn plan(input: &MmInput) -> Result<MmPlan> {
    input.config.validate()?;
    validate_facts(input)?;
    let mark = input.mark_price.value();
    let net = mul(sub(input.long_quantity, input.short_quantity)?, mark)?;
    let gross = mul(add(input.long_quantity, input.short_quantity)?, mark)?;
    let spread = input.config.base_half_spread_bps.max(mul(
        input.volatility_bps,
        input.config.volatility_multiplier,
    )?);
    let owned: Vec<String> = input
        .live_orders
        .iter()
        .filter(|o| active(o.state) && input.owned_order_ids.contains(&o.order_id))
        .map(|o| o.order_id.clone())
        .collect();
    let output = |action| MmPlan {
        action,
        net_notional: net,
        gross_notional: gross,
        half_spread_bps: spread,
    };
    let halt = |reason| {
        output(MmAction::Halt {
            reason,
            cancel_order_ids: owned.clone(),
        })
    };
    if input.control == MmControl::Stop {
        return Ok(halt(MmReason::Stopped));
    }
    if input.unknown_results
        || input
            .live_orders
            .iter()
            .any(|o| o.state == OrderState::Unknown)
    {
        return Ok(halt(MmReason::UnknownResults));
    }
    if !fresh(
        input.market_observed_at_ms,
        input.now_ms,
        input.config.max_market_age_ms,
    ) || input
        .account
        .validate_at(input.now_ms, input.config.max_private_age_ms)
        .is_err()
    {
        return Ok(halt(MmReason::StaleFacts));
    }
    if sub(input.equity_baseline.value, input.account.account_equity)?
        >= input.config.max_loss_quote.value
    {
        return Ok(halt(MmReason::LossLimit));
    }
    if sub(input.equity_peak.value, input.account.account_equity)?
        >= input.config.max_drawdown_quote.value
    {
        return Ok(halt(MmReason::DrawdownLimit));
    }
    // Quotes would be economically meaningless at a 100% half spread; wait, never fabricate a price.
    if spread >= Decimal::from(10_000) {
        return Ok(halt(MmReason::NoSafeQuote));
    }
    let existing = signed_quotes(input)?;
    let mut outstanding = existing.clone();
    outstanding.extend(input.pending_quotes.iter().cloned());
    let breached = net.abs() > input.config.max_net_notional.value
        || gross > input.config.max_gross_notional.value
        || mul(input.long_quantity.max(input.short_quantity), mark)?
            > input.config.max_leg_notional.value;
    let outstanding_safe = risk_permits(input, &outstanding, breached)?;
    let own_increases_risk = input
        .live_orders
        .iter()
        .filter(|o| active(o.state) && input.owned_order_ids.contains(&o.order_id))
        .map(order_quote)
        .collect::<Result<Vec<_>>>()?
        .iter()
        .any(|q| {
            !q.reduce_only
                || (net > Decimal::ZERO && q.side == OrderSide::Buy)
                || (net < Decimal::ZERO && q.side == OrderSide::Sell)
        });
    if !owned.is_empty()
        && ((breached && own_increases_risk) || !outstanding_safe || owned.len() > 2)
    {
        return Ok(output(MmAction::CancelThenReplan {
            order_ids: owned,
            reason: MmReason::RiskReduction,
        }));
    }
    if !input.pending_quotes.is_empty() {
        return Ok(output(MmAction::Keep {
            reason: MmReason::PendingConfirmation,
        }));
    }
    // Reservation price uses the same mark-valued net inventory as the limits. Positive net shifts both quotes down.
    let mid = div(
        add(input.best_bid.value(), input.best_ask.value())?,
        Decimal::from(2),
    )?;
    let normalized_net = div(net, input.config.max_net_notional.value)?
        .max(-Decimal::ONE)
        .min(Decimal::ONE);
    let skew = mul(normalized_net, input.config.inventory_skew_bps)?;
    let center = mul(mid, sub(Decimal::ONE, div(skew, Decimal::from(10_000))?)?)?;
    let width = mul(mid, div(spread, Decimal::from(10_000))?)?;
    let raw_bid = sub(center, width)?.min(input.best_bid.value());
    let raw_ask = add(center, width)?.max(input.best_ask.value());
    let bid = input
        .instrument
        .price
        .floor(raw_bid)
        .map_err(|_| MmError::Facts)?;
    let ask = ceil_step(raw_ask, input.instrument.price.step)?;
    if bid < input.instrument.price.minimum || ask > input.maximum_price.value() || bid >= ask {
        return Ok(halt(MmReason::NoSafeQuote));
    }
    let mut foreign = Vec::new();
    for order in &input.live_orders {
        if active(order.state) && !input.owned_order_ids.contains(&order.order_id) {
            foreign.push(order_quote(order)?);
        }
    }
    let sides = if net >= Decimal::ZERO {
        [OrderSide::Sell, OrderSide::Buy]
    } else {
        [OrderSide::Buy, OrderSide::Sell]
    };
    let mut desired = Vec::new();
    for side in sides {
        let price = Price::new(if side == OrderSide::Buy { bid } else { ask })
            .map_err(|_| MmError::Facts)?;
        let mut reservations = foreign.clone();
        reservations.extend(desired.iter().cloned());
        if let Some(quote) = quote_for_side(input, side, price, &reservations, breached, net)? {
            reservations.push(quote.clone());
            if risk_permits(input, &reservations, breached)? {
                desired.push(quote);
            }
        }
    }
    let mut own_quotes = Vec::new();
    for order in &input.live_orders {
        if active(order.state) && input.owned_order_ids.contains(&order.order_id) {
            own_quotes.push(order_quote(order)?);
        }
    }
    if !owned.is_empty() {
        // A serial sender can retire one unsent side after the first side changes private facts.
        // Complete only an exact surviving subset of today's fully risk-checked target. Changed
        // prices, partial fills or inventory still take the cancel-and-replan path below.
        if own_quotes.len() == 1 && desired.len() == 2 && desired.contains(&own_quotes[0]) {
            return Ok(output(MmAction::Quote {
                quotes: desired
                    .into_iter()
                    .filter(|q| !own_quotes.contains(q))
                    .collect(),
                reason: MmReason::Normal,
            }));
        }
        let same =
            own_quotes.len() == desired.len() && own_quotes.iter().all(|q| desired.contains(q));
        let before_refresh = input.previous_quotes_at_ms.is_some_and(|at| {
            input.now_ms >= at && input.now_ms - at < input.config.refresh_interval_ms
        });
        if same || (before_refresh && outstanding_safe && !desired.is_empty()) {
            return Ok(output(MmAction::Keep {
                reason: MmReason::Normal,
            }));
        }
        return Ok(output(MmAction::CancelThenReplan {
            order_ids: owned,
            reason: MmReason::Replacement,
        }));
    }
    if desired.is_empty() {
        return Ok(output(MmAction::Keep {
            reason: MmReason::NoSafeQuote,
        }));
    }
    Ok(output(MmAction::Quote {
        quotes: desired,
        reason: if breached {
            MmReason::RiskReduction
        } else {
            MmReason::Normal
        },
    }))
}

fn validate_facts(i: &MmInput) -> Result<()> {
    i.instrument.validate().map_err(|_| MmError::Facts)?;
    if !i.instrument.trading_enabled
        || i.instrument.instrument.symbol != i.config.symbol
        || i.instrument.instrument.market != MarketKind::LinearPerpetual
        || i.instrument.contract.is_some()
        || i.best_bid >= i.best_ask
        || i.long_quantity < Decimal::ZERO
        || i.short_quantity < Decimal::ZERO
        || i.maximum_quantity < i.instrument.quantity.minimum
        || i.maximum_price.value() < i.instrument.price.minimum
        || i.volatility_bps < Decimal::ZERO
        || i.available_open_notional.value < Decimal::ZERO
        || i.available_open_notional.asset.as_str() != i.config.symbol.quote()
        || i.equity_baseline.asset != i.account.risk_currency
        || i.equity_peak.asset != i.account.risk_currency
        || i.config.max_loss_quote.asset != i.account.risk_currency
        || i.equity_baseline.value <= Decimal::ZERO
        || i.equity_peak.value < i.equity_baseline.value
        || i.equity_peak.value < i.account.account_equity
    {
        return Err(MmError::Facts);
    }
    let mut ids = std::collections::BTreeSet::new();
    for o in &i.live_orders {
        if o.symbol != i.config.symbol || !ids.insert(&o.order_id) {
            return Err(MmError::Facts);
        }
        o.validate().map_err(|_| MmError::Facts)?;
    }
    for quote in &i.pending_quotes {
        validate_quote(quote)?;
    }
    Ok(())
}

fn active(state: OrderState) -> bool {
    matches!(
        state,
        OrderState::New | OrderState::PartiallyFilled | OrderState::Unknown
    )
}
fn fresh(at: u64, now: u64, max_age: u64) -> bool {
    at > 0 && at <= now && now - at <= max_age
}
fn signed_quotes(i: &MmInput) -> Result<Vec<MmQuote>> {
    i.live_orders
        .iter()
        .filter(|o| active(o.state))
        .map(order_quote)
        .collect()
}
fn order_quote(o: &venue_domain::domain::Order) -> Result<MmQuote> {
    let position_side = match o.position_side {
        FieldState::Known(side) => side,
        _ => return Err(MmError::Facts),
    };
    // Exchange Hedge close orders may report reduceOnly=false. Direction and explicit leg define the intent.
    let reduce_only = matches!(
        (o.side, position_side),
        (OrderSide::Sell, PositionSide::Long) | (OrderSide::Buy, PositionSide::Short)
    );
    let quote = MmQuote {
        side: o.side,
        position_side,
        price: o.limit_price.ok_or(MmError::Facts)?,
        quantity: sub(o.quantity, o.filled_quantity)?,
        reduce_only,
    };
    validate_quote(&quote)?;
    Ok(quote)
}
fn validate_quote(q: &MmQuote) -> Result<()> {
    let close = matches!(
        (q.side, q.position_side),
        (OrderSide::Sell, PositionSide::Long) | (OrderSide::Buy, PositionSide::Short)
    );
    if q.quantity <= Decimal::ZERO || q.position_side == PositionSide::Net || q.reduce_only != close
    {
        return Err(MmError::Facts);
    }
    Ok(())
}

fn quote_for_side(
    i: &MmInput,
    side: OrderSide,
    price: Price,
    reserved: &[MmQuote],
    reduction: bool,
    net: Decimal,
) -> Result<Option<MmQuote>> {
    let close_leg = if side == OrderSide::Buy {
        PositionSide::Short
    } else {
        PositionSide::Long
    };
    let close_total = if side == OrderSide::Buy {
        i.short_quantity
    } else {
        i.long_quantity
    };
    let mut close_reserved = Decimal::ZERO;
    for q in reserved
        .iter()
        .filter(|q| q.reduce_only && q.position_side == close_leg)
    {
        close_reserved = add(close_reserved, q.quantity)?;
    }
    let remaining = sub(close_total, close_reserved)?.max(Decimal::ZERO);
    let raw = div(i.config.order_notional.value, price.value())?;
    let close_quantity = i
        .instrument
        .quantity
        .floor(raw.min(remaining).min(i.maximum_quantity))
        .map_err(|_| MmError::Facts)?;
    if close_quantity >= i.instrument.quantity.minimum {
        // In a limit breach only reduce the heavier leg; when perfectly hedged both closes remain bounded by net risk.
        if reduction
            && ((net > Decimal::ZERO && side == OrderSide::Buy)
                || (net < Decimal::ZERO && side == OrderSide::Sell))
        {
            return Ok(None);
        }
        let quantity = if reduction && net != Decimal::ZERO {
            i.instrument
                .quantity
                .floor(close_quantity.min(div(net.abs(), i.mark_price.value())?))
                .map_err(|_| MmError::Facts)?
        } else {
            close_quantity
        };
        if quantity < i.instrument.quantity.minimum {
            return Ok(None);
        }
        return Ok(Some(MmQuote {
            side,
            position_side: close_leg,
            price,
            quantity,
            reduce_only: true,
        }));
    }
    if reduction {
        return Ok(None);
    }
    // Canonical Precision owns rounding. Only the minimum-legal opening boundary rounds upward.
    let minimum = div(
        i.instrument.instrument.minimum_notional.value,
        price.value(),
    )?
    .max(i.instrument.quantity.minimum);
    let mut quantity = i
        .instrument
        .quantity
        .floor(raw)
        .map_err(|_| MmError::Facts)?;
    if quantity < minimum {
        quantity = ceil_step(minimum, i.instrument.quantity.step)?;
    }
    if quantity < i.instrument.quantity.minimum || quantity > i.maximum_quantity {
        return Ok(None);
    }
    let notional = mul(quantity, price.value().max(i.mark_price.value()))?;
    let mut reserved_open = Decimal::ZERO;
    for q in reserved.iter().filter(|q| !q.reduce_only) {
        reserved_open = add(
            reserved_open,
            mul(q.quantity, q.price.value().max(i.mark_price.value()))?,
        )?;
    }
    if add(reserved_open, notional)? > i.available_open_notional.value {
        return Ok(None);
    }
    Ok(Some(MmQuote {
        side,
        position_side: if side == OrderSide::Buy {
            PositionSide::Long
        } else {
            PositionSide::Short
        },
        price,
        quantity,
        reduce_only: false,
    }))
}

/// Every subset of fills is bounded. A close is never automatically safe: buy-close-short increases net.
fn risk_permits(i: &MmInput, orders: &[MmQuote], reduction: bool) -> Result<bool> {
    let mut long_open = Decimal::ZERO;
    let mut short_open = Decimal::ZERO;
    let mut long_close = Decimal::ZERO;
    let mut short_close = Decimal::ZERO;
    for q in orders {
        validate_quote(q)?;
        match (q.position_side, q.reduce_only) {
            (PositionSide::Long, false) => long_open = add(long_open, q.quantity)?,
            (PositionSide::Short, false) => short_open = add(short_open, q.quantity)?,
            (PositionSide::Long, true) => long_close = add(long_close, q.quantity)?,
            (PositionSide::Short, true) => short_close = add(short_close, q.quantity)?,
            _ => return Err(MmError::Facts),
        }
    }
    if long_close > i.long_quantity || short_close > i.short_quantity {
        return Ok(false);
    }
    let mark = i.mark_price.value();
    let long = mul(i.long_quantity, mark)?;
    let short = mul(i.short_quantity, mark)?;
    let largest_leg = long.max(short);
    let gross = add(long, short)?;
    let net = sub(long, short)?;
    let leg_cap = if reduction {
        i.config.max_leg_notional.value.max(largest_leg)
    } else {
        i.config.max_leg_notional.value
    };
    let gross_cap = if reduction {
        i.config.max_gross_notional.value.max(gross)
    } else {
        i.config.max_gross_notional.value
    };
    let net_cap = if reduction {
        i.config.max_net_notional.value.max(net.abs())
    } else {
        i.config.max_net_notional.value
    };
    let long_max = add(long, mul(long_open, mark)?)?;
    let short_max = add(short, mul(short_open, mark)?)?;
    let max_net = add(net, mul(add(long_open, short_close)?, mark)?)?;
    let min_net = sub(net, mul(add(short_open, long_close)?, mark)?)?;
    Ok(long_max <= leg_cap
        && short_max <= leg_cap
        && add(long_max, short_max)? <= gross_cap
        && max_net <= net_cap
        && min_net >= -net_cap)
}

fn ceil_step(value: Decimal, step: Decimal) -> Result<Decimal> {
    let units = div(value, step)?.ceil();
    mul(units, step)
}
fn add(a: Decimal, b: Decimal) -> Result<Decimal> {
    a.checked_add(b).ok_or(MmError::Arithmetic)
}
fn sub(a: Decimal, b: Decimal) -> Result<Decimal> {
    a.checked_sub(b).ok_or(MmError::Arithmetic)
}
fn mul(a: Decimal, b: Decimal) -> Result<Decimal> {
    a.checked_mul(b).ok_or(MmError::Arithmetic)
}
fn div(a: Decimal, b: Decimal) -> Result<Decimal> {
    a.checked_div(b).ok_or(MmError::Arithmetic)
}
