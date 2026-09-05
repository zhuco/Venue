//! Pure decision layer for support-martingale.
//!
//! The planner consumes signed facts and normalized Binance reference data. It emits semantic
//! decisions only; persistence, command identities, cancellation and exchange adapters belong to
//! the caller.

use std::collections::BTreeSet;

use rust_decimal::Decimal;
use venue_control_protocol::support_martingale::{
    SupportMartingaleInstance, SupportMartingaleSymbolState,
};
use venue_domain::{Price, Symbol};
use venue_execution::{DurableMarketFacts, SignedAccountSnapshot};
use venue_strategies::support_martingale::{
    EntryKind, MarketEnvironment, PositionCost, SupportMartingaleConfig as CoreConfig, SupportZone,
    TakeProfitBlock, TakeProfitInput, TakeProfitResult, calculate_take_profit,
    classify_environment, detect_supports, evaluate_callback,
};

use super::reference_market::ReferenceSnapshot;

fn max_basis() -> Decimal {
    Decimal::new(1, 2)
}
fn conservative_entry_fee() -> Decimal {
    Decimal::new(1, 3)
}
fn conservative_exit_fee() -> Decimal {
    Decimal::new(1, 3)
}

#[derive(Clone, Debug)]
pub struct PlannerInput<'a> {
    pub instance: &'a SupportMartingaleInstance,
    pub symbol: &'a Symbol,
    pub reference: &'a ReferenceSnapshot,
    pub account: &'a SignedAccountSnapshot,
    pub execution_market: &'a DurableMarketFacts,
    pub consumed_supports: &'a BTreeSet<String>,
    pub last_support_lower: Option<Decimal>,
    pub current_take_profit: Option<&'a TakeProfitOrder>,
    pub prefer_add_after_cancel: bool,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TakeProfitOrder {
    pub client_order_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub filled_quantity: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Plan {
    Noop(NoopReason),
    MarketEntry {
        symbol: Symbol,
        support_id: String,
        support_lower: Decimal,
        support_upper: Decimal,
        layer: u16,
        notional: Decimal,
        quantity: Decimal,
        estimated: bool,
    },
    CancelTakeProfit {
        symbol: Symbol,
        client_order_id: String,
    },
    LimitTakeProfit {
        symbol: Symbol,
        price: Price,
        quantity: Decimal,
        estimated: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoopReason {
    InvalidInput,
    StaleAccount,
    MissingReference,
    BasisTooWide,
    EnvironmentBlocked,
    NoSupport,
    SupportConsumed,
    BudgetExhausted,
    PositionLimit,
    QuantityTooSmall,
    ExitOnly,
    TakeProfitBlocked,
    WaitingForCancel,
}

pub fn plan(input: &PlannerInput<'_>) -> Plan {
    if input.now_ms == 0
        || input.execution_market.binding.symbol != *input.symbol
        || input.account.binding().symbol != *input.symbol
    {
        return Plan::Noop(NoopReason::InvalidInput);
    }
    if input.now_ms < input.account.observed_at_ms()
        || input.now_ms.saturating_sub(input.account.observed_at_ms()) > 5_000
    {
        return Plan::Noop(NoopReason::StaleAccount);
    }
    let Some(state) = input
        .instance
        .symbols
        .iter()
        .find(|state| state.symbol == *input.symbol)
    else {
        return Plan::Noop(NoopReason::InvalidInput);
    };
    if state.quantity > Decimal::ZERO
        && state.take_profit_price.is_some()
        && state.layer > 0
        && input.current_take_profit.is_none()
    { /* maintenance continues below */ }
    let core = match core_config(input.instance, input.symbol) {
        Ok(value) => value,
        Err(_) => return Plan::Noop(NoopReason::InvalidInput),
    };
    if state.quantity > Decimal::ZERO {
        if let Some(position) = input
            .account
            .positions()
            .iter()
            .find(|p| p.symbol == *input.symbol && p.quantity > Decimal::ZERO)
        {
            if input.current_take_profit.is_none() && !input.prefer_add_after_cancel {
                return plan_take_profit(
                    input,
                    state,
                    position.entry_price.or(state.average_price),
                    position.quantity,
                );
            }
        }
    }
    let Some(reference) = input.reference.symbols.get(input.symbol) else {
        return Plan::Noop(NoopReason::MissingReference);
    };
    let reference_mid = match reference
        .ticker
        .bid_price
        .value()
        .checked_add(reference.ticker.ask_price.value())
        .and_then(|v| v.checked_div(Decimal::from(2)))
    {
        Some(v) => v,
        None => return Plan::Noop(NoopReason::MissingReference),
    };
    let execution_mid = input.execution_market.reference_price.value();
    let Some(basis) = execution_mid
        .checked_div(reference_mid)
        .and_then(|v| v.checked_sub(Decimal::ONE))
    else {
        return Plan::Noop(NoopReason::BasisTooWide);
    };
    if basis.abs() > max_basis() {
        return Plan::Noop(NoopReason::BasisTooWide);
    }
    let has_position = state.quantity > Decimal::ZERO;
    if let Some(order) = input.current_take_profit {
        if order.filled_quantity > Decimal::ZERO {
            return Plan::Noop(NoopReason::ExitOnly);
        }
    }
    let target_environment = match classify_environment(&reference.four_hour, &core, input.now_ms) {
        Ok(value) => value,
        Err(_) => return Plan::Noop(NoopReason::MissingReference),
    };
    let btc_symbol = match Symbol::new("BTC", "USDT") {
        Ok(value) => value,
        Err(_) => return Plan::Noop(NoopReason::MissingReference),
    };
    let mut btc_core = core.clone();
    btc_core.symbol = btc_symbol;
    let btc_environment =
        match classify_environment(&input.reference.btc_environment, &btc_core, input.now_ms) {
            Ok(value) => value,
            Err(_) => return Plan::Noop(NoopReason::MissingReference),
        };
    if !has_position
        && !matches!(
            target_environment.environment,
            MarketEnvironment::Up | MarketEnvironment::Range
        )
        || !has_position
            && !matches!(
                btc_environment.environment,
                MarketEnvironment::Up | MarketEnvironment::Range
            )
    {
        return Plan::Noop(NoopReason::EnvironmentBlocked);
    }
    if state.layer >= input.instance.config.max_entries {
        return Plan::Noop(NoopReason::BudgetExhausted);
    }
    let active = input
        .instance
        .symbols
        .iter()
        .filter(|value| value.quantity > Decimal::ZERO)
        .count() as u16;
    if !has_position && active >= input.instance.config.max_active_positions {
        return Plan::Noop(NoopReason::PositionLimit);
    }
    let kind = if has_position {
        EntryKind::Add
    } else {
        EntryKind::First
    };
    let supports = match detect_supports(&reference.one_hour, &core, input.now_ms) {
        Ok(value) => value,
        Err(_) => return Plan::Noop(NoopReason::NoSupport),
    };
    if has_position
        && state
            .average_price
            .is_some_and(|average| execution_mid >= average)
    {
        return Plan::Noop(NoopReason::NoSupport);
    }
    let support = select_support(
        &supports,
        input.consumed_supports,
        reference_mid,
        input.last_support_lower,
    );
    let Some(support) = support else {
        return Plan::Noop(NoopReason::NoSupport);
    };
    let Some(signal) = evaluate_callback(
        &reference.fifteen_minutes,
        support,
        &core,
        kind,
        input.now_ms,
    )
    .ok()
    .flatten() else {
        return Plan::Noop(NoopReason::NoSupport);
    };
    if has_position {
        if let Some(order) = input.current_take_profit {
            return Plan::CancelTakeProfit {
                symbol: input.symbol.clone(),
                client_order_id: order.client_order_id.clone(),
            };
        }
    }
    let mut scale = Decimal::ONE;
    for _ in 0..state.layer {
        scale = match scale.checked_mul(input.instance.config.size_multiplier) {
            Some(value) => value,
            None => return Plan::Noop(NoopReason::BudgetExhausted),
        };
    }
    let notional = match input
        .instance
        .config
        .first_order_notional
        .checked_mul(scale)
    {
        Some(value) => value,
        None => return Plan::Noop(NoopReason::BudgetExhausted),
    };
    if state
        .invested
        .checked_add(notional)
        .is_none_or(|value| value > input.instance.config.total_budget)
    {
        return Plan::Noop(NoopReason::BudgetExhausted);
    }
    let Some(quantity) = input
        .execution_market
        .metadata
        .contract
        .as_ref()
        .and_then(|contract| {
            contract
                .lots_for_quote_notional(notional, Some(input.execution_market.reference_price))
                .ok()
        })
        .or_else(|| notional.checked_div(execution_mid))
    else {
        return Plan::Noop(NoopReason::QuantityTooSmall);
    };
    let step = input.execution_market.metadata.instrument.quantity_step;
    let quantity = quantity - quantity % step;
    if quantity <= Decimal::ZERO || quantity < input.execution_market.metadata.quantity.minimum {
        return Plan::Noop(NoopReason::QuantityTooSmall);
    }
    let _ = signal;
    Plan::MarketEntry {
        symbol: input.symbol.clone(),
        support_id: support.id.clone(),
        support_lower: support.lower.value(),
        support_upper: support.upper.value(),
        layer: state.layer,
        notional,
        quantity,
        estimated: true,
    }
}

fn plan_take_profit(
    input: &PlannerInput<'_>,
    state: &SupportMartingaleSymbolState,
    entry_price: Option<Decimal>,
    quantity: Decimal,
) -> Plan {
    let Some(entry_price) = entry_price else {
        return Plan::Noop(NoopReason::TakeProfitBlocked);
    };
    let Some(buy_notional) = entry_price.checked_mul(quantity) else {
        return Plan::Noop(NoopReason::TakeProfitBlocked);
    };
    let result = calculate_take_profit(&TakeProfitInput {
        position: PositionCost {
            quantity,
            buy_notional,
            entry_fee: buy_notional * conservative_entry_fee(),
            funding: Decimal::ZERO,
        },
        target_profit_rate: input.instance.config.target_profit_rate,
        minimum_profit_quote: input.instance.config.minimum_profit_quote,
        exit_fee_rate: Some(conservative_exit_fee()),
        tick_size: input
            .execution_market
            .metadata
            .instrument
            .price_tick
            .value(),
        minimum_price: None,
        maximum_price: input.execution_market.maximum_price,
    });
    match result {
        TakeProfitResult::Ready { price, .. } => Plan::LimitTakeProfit {
            symbol: state.symbol.clone(),
            price,
            quantity,
            estimated: true,
        },
        TakeProfitResult::Blocked {
            reason:
                TakeProfitBlock::PriceOutOfRange
                | TakeProfitBlock::MissingExitFee
                | TakeProfitBlock::ZeroQuantity
                | TakeProfitBlock::Arithmetic
                | TakeProfitBlock::InvalidTick,
        } => Plan::Noop(NoopReason::TakeProfitBlocked),
    }
}

pub fn plan_take_profit_only(
    instance: &SupportMartingaleInstance,
    symbol: &Symbol,
    account: &SignedAccountSnapshot,
    execution_market: &DurableMarketFacts,
) -> Plan {
    let Some(state) = instance
        .symbols
        .iter()
        .find(|state| state.symbol == *symbol)
    else {
        return Plan::Noop(NoopReason::InvalidInput);
    };
    let Some(position) = account
        .positions()
        .iter()
        .find(|position| position.symbol == *symbol && position.quantity > Decimal::ZERO)
    else {
        return Plan::Noop(NoopReason::TakeProfitBlocked);
    };
    let placeholder = ReferenceSnapshot {
        fetched_at_ms: account.observed_at_ms(),
        btc_environment: Vec::new(),
        symbols: std::collections::BTreeMap::new(),
    };
    let consumed = BTreeSet::new();
    plan_take_profit(
        &PlannerInput {
            instance,
            symbol,
            reference: &placeholder,
            account,
            execution_market,
            consumed_supports: &consumed,
            last_support_lower: None,
            current_take_profit: None,
            prefer_add_after_cancel: false,
            now_ms: account.observed_at_ms(),
        },
        state,
        position.entry_price.or(state.average_price),
        position.quantity,
    )
}

fn select_support<'a>(
    supports: &'a [SupportZone],
    consumed: &BTreeSet<String>,
    reference_price: Decimal,
    last_support_lower: Option<Decimal>,
) -> Option<&'a SupportZone> {
    supports
        .iter()
        .filter(|support| {
            !consumed.contains(&support.id)
                && support.upper.value() < reference_price
                && last_support_lower.is_none_or(|lower| support.upper.value() < lower)
        })
        .max_by_key(|support| support.confirmed_at_ms)
}

fn core_config(instance: &SupportMartingaleInstance, symbol: &Symbol) -> Result<CoreConfig, ()> {
    let mut config = CoreConfig::research_default().map_err(|_| ())?;
    config.symbol = symbol.clone();
    config.first_order_notional = instance.config.first_order_notional;
    config.max_entries = u8::try_from(instance.config.max_entries).map_err(|_| ())?;
    config.size_multiplier = instance.config.size_multiplier;
    config.target_profit_rate = instance.config.target_profit_rate;
    config.minimum_profit_quote = instance.config.minimum_profit_quote;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zone(id: &str, lower: i64, upper: i64, confirmed_at_ms: u64) -> SupportZone {
        let lower = match Price::new(Decimal::from(lower)) {
            Ok(value) => value,
            Err(_) => return zone("fallback", 1, 2, confirmed_at_ms),
        };
        let upper = match Price::new(Decimal::from(upper)) {
            Ok(value) => value,
            Err(_) => return zone("fallback", 1, 2, confirmed_at_ms),
        };
        SupportZone {
            id: id.to_owned(),
            lower,
            upper,
            source_price: lower,
            confirmed_at_ms,
            version: confirmed_at_ms,
        }
    }

    #[test]
    fn support_selection_is_lower_than_average_and_deduplicated()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol = Symbol::new("SOL", "USDT")?;
        let _state = SupportMartingaleSymbolState {
            symbol,
            cycle_id: Some("c".to_owned()),
            layer: 1,
            average_price: Some(Decimal::from(100)),
            quantity: Decimal::ONE,
            invested: Decimal::from(10),
            take_profit_price: None,
            net_pnl: None,
            status: "holding".to_owned(),
        };
        let supports = vec![
            zone("used", 70, 72, 1),
            zone("higher", 80, 82, 2),
            zone("lower", 60, 62, 3),
        ];
        let consumed = BTreeSet::from(["used".to_owned()]);
        let selected = select_support(
            &supports,
            &consumed,
            Decimal::from(90),
            Some(Decimal::from(70)),
        );
        assert_eq!(selected.map(|value| value.id.as_str()), Some("lower"));
        Ok(())
    }
}
