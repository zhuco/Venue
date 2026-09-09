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
        for_stop_loss: bool,
    },
    MarketStopLoss {
        symbol: Symbol,
        quantity: Decimal,
        position_generation: u64,
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
    BtcDown,
    BtcWarmup,
    BtcNeutral,
    CallbackNotConfirmed,
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
    if input.instance.config.entry_mode
        == venue_control_protocol::support_martingale::MartingaleEntryMode::FixedPrice
    {
        return fixed_entry(input, state);
    }
    let Some(reference) = input.reference.symbols.get(input.symbol) else {
        return Plan::Noop(NoopReason::MissingReference);
    };
    if reference.ticker.exchange_time_ms > input.now_ms.saturating_add(2_000)
        || input
            .now_ms
            .saturating_sub(reference.ticker.exchange_time_ms)
            > 5_000
    {
        return Plan::Noop(NoopReason::MissingReference);
    }
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
    if !has_position {
        if let Some(reason) = btc_entry_block(
            btc_environment.environment,
            input.instance.config.allow_btc_neutral,
        ) {
            return Plan::Noop(reason);
        }
        if !matches!(
            target_environment.environment,
            MarketEnvironment::Up | MarketEnvironment::Range
        ) {
            return Plan::Noop(NoopReason::EnvironmentBlocked);
        }
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
    let mut has_candidate = false;
    let support = select_support(
        &supports,
        input.consumed_supports,
        reference_mid,
        input.last_support_lower,
        |support| {
            has_candidate = true;
            evaluate_callback(
                &reference.fifteen_minutes,
                support,
                &core,
                kind,
                input.now_ms,
            )
            .ok()
            .flatten()
            .is_some()
        },
    );
    let Some(support) = support else {
        return Plan::Noop(if has_candidate {
            NoopReason::CallbackNotConfirmed
        } else {
            NoopReason::NoSupport
        });
    };
    entry_at_support(input, state, support)
}

fn btc_entry_block(environment: MarketEnvironment, allow_neutral: bool) -> Option<NoopReason> {
    match environment {
        MarketEnvironment::Up | MarketEnvironment::Range => None,
        MarketEnvironment::Neutral if allow_neutral => None,
        MarketEnvironment::Neutral => Some(NoopReason::BtcNeutral),
        MarketEnvironment::Down => Some(NoopReason::BtcDown),
        MarketEnvironment::Warmup => Some(NoopReason::BtcWarmup),
    }
}

#[test]
fn btc_neutral_opt_in_never_allows_down_or_warmup() {
    for allow in [false, true] {
        assert_eq!(
            btc_entry_block(MarketEnvironment::Down, allow),
            Some(NoopReason::BtcDown)
        );
        assert_eq!(
            btc_entry_block(MarketEnvironment::Warmup, allow),
            Some(NoopReason::BtcWarmup)
        );
        assert_eq!(btc_entry_block(MarketEnvironment::Up, allow), None);
        assert_eq!(btc_entry_block(MarketEnvironment::Range, allow), None);
    }
    assert_eq!(
        btc_entry_block(MarketEnvironment::Neutral, false),
        Some(NoopReason::BtcNeutral)
    );
    assert_eq!(btc_entry_block(MarketEnvironment::Neutral, true), None);
}

fn fixed_entry(input: &PlannerInput<'_>, state: &SupportMartingaleSymbolState) -> Plan {
    let Some(parameters) = input
        .instance
        .config
        .symbol_parameters
        .iter()
        .find(|p| p.symbol == *input.symbol)
    else {
        return Plan::Noop(NoopReason::InvalidInput);
    };
    let Some(first) = parameters.entry_price else {
        return Plan::Noop(NoopReason::InvalidInput);
    };
    let mut target = first;
    for _ in 0..state.layer {
        let Some(next) = target.checked_mul(Decimal::ONE - parameters.add_drop_rate) else {
            return Plan::Noop(NoopReason::InvalidInput);
        };
        target = next;
    }
    let price = input.execution_market.reference_price.value();
    if price > target
        || input
            .current_take_profit
            .is_some_and(|order| order.filled_quantity > Decimal::ZERO)
    {
        return Plan::Noop(NoopReason::NoSupport);
    }
    if parameters
        .stop_loss
        .and_then(|sl| sl.trigger_price(state.average_price.unwrap_or(first)))
        .is_some_and(|stop| price <= stop)
    {
        return Plan::Noop(NoopReason::ExitOnly);
    }
    let Ok(target) = Price::new(target) else {
        return Plan::Noop(NoopReason::InvalidInput);
    };
    let support = SupportZone {
        id: format!(
            "fixed:{}:{}:{}",
            input.symbol,
            state
                .cycle_id
                .as_deref()
                .map(str::to_owned)
                .unwrap_or_else(|| input.instance.revision.to_string()),
            state.layer
        ),
        lower: target,
        upper: target,
        source_price: target,
        confirmed_at_ms: input.now_ms,
        version: 1,
    };
    entry_at_support(input, state, &support)
}

fn entry_at_support(
    input: &PlannerInput<'_>,
    state: &SupportMartingaleSymbolState,
    support: &SupportZone,
) -> Plan {
    if state.layer >= input.instance.config.max_entries {
        return Plan::Noop(NoopReason::BudgetExhausted);
    }
    if state.quantity.is_zero()
        && input
            .instance
            .symbols
            .iter()
            .filter(|s| s.quantity > Decimal::ZERO)
            .count()
            >= usize::from(input.instance.config.max_active_positions)
    {
        return Plan::Noop(NoopReason::PositionLimit);
    }
    if state.quantity > Decimal::ZERO {
        if let Some(order) = input.current_take_profit {
            return Plan::CancelTakeProfit {
                symbol: input.symbol.clone(),
                client_order_id: order.client_order_id.clone(),
                for_stop_loss: false,
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
    let Some((quantity, actual_notional)) = entry_quantity(
        &input.execution_market.metadata,
        input.execution_market.maximum_quantity,
        notional,
        input.execution_market.reference_price,
    ) else {
        return Plan::Noop(NoopReason::QuantityTooSmall);
    };
    if state
        .invested
        .checked_add(actual_notional)
        .is_none_or(|value| value > input.instance.config.total_budget)
    {
        return Plan::Noop(NoopReason::BudgetExhausted);
    }
    Plan::MarketEntry {
        symbol: input.symbol.clone(),
        support_id: support.id.clone(),
        support_lower: support.lower.value(),
        support_upper: support.upper.value(),
        layer: state.layer,
        notional: actual_notional,
        quantity,
        estimated: true,
    }
}

/// Rounds an opening quantity upward so the persisted notional reflects the actual contract
/// amount.  The final dispatch guard repeats these precision, quantity, and minimum-notional
/// checks against fresh market facts.
fn entry_quantity(
    metadata: &venue_domain::InstrumentMetadata,
    maximum_quantity: Option<Decimal>,
    requested_notional: Decimal,
    price: Price,
) -> Option<(Decimal, Decimal)> {
    if requested_notional <= Decimal::ZERO
        || price.value() <= Decimal::ZERO
        || metadata.validate().is_err()
        || !metadata.trading_enabled
    {
        return None;
    }
    let target_notional = requested_notional.max(metadata.instrument.minimum_notional.value);
    let probe_quantity = ceil_to_step(
        metadata.quantity.minimum.max(metadata.quantity.step),
        metadata.quantity.step,
    )?;
    let probe_notional = entry_raw_notional(metadata, probe_quantity, price)?;
    let unit_notional = probe_notional.checked_div(probe_quantity)?;
    let raw_quantity = target_notional.checked_div(unit_notional)?;
    let mut quantity = ceil_to_step(
        raw_quantity.max(metadata.quantity.minimum),
        metadata.quantity.step,
    )?;
    let raw_actual = entry_raw_notional(metadata, quantity, price)?;
    if raw_actual < target_notional {
        // Decimal division can round a boundary down; final notional is checked again below.
        quantity = quantity.checked_add(metadata.quantity.step)?;
    }
    if !metadata.quantity.accepts(quantity).ok()?
        || maximum_quantity.is_some_and(|maximum| quantity > maximum)
    {
        return None;
    }
    let actual_notional = metadata.quote_notional(quantity, Some(price)).ok()?.value;
    if actual_notional < target_notional
        || actual_notional < metadata.instrument.minimum_notional.value
    {
        return None;
    }
    Some((quantity, actual_notional))
}

fn entry_raw_notional(
    metadata: &venue_domain::InstrumentMetadata,
    quantity: Decimal,
    price: Price,
) -> Option<Decimal> {
    match &metadata.contract {
        Some(contract) => contract.quote_notional(quantity, Some(price)).ok(),
        None => quantity.checked_mul(price.value()),
    }
}

fn ceil_to_step(value: Decimal, step: Decimal) -> Option<Decimal> {
    if value < Decimal::ZERO || step <= Decimal::ZERO {
        return None;
    }
    let remainder = value % step;
    if remainder.is_zero() {
        Some(value)
    } else {
        value.checked_add(step - remainder)
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
    mut callback_confirmed: impl FnMut(&SupportZone) -> bool,
) -> Option<&'a SupportZone> {
    supports
        .iter()
        .filter(|support| {
            !consumed.contains(&support.id)
                && support.upper.value() < reference_price
                && last_support_lower.is_none_or(|lower| support.upper.value() < lower)
        })
        // A newer untouched region must not hide an older region's fresh callback.
        .filter(|support| callback_confirmed(support))
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
    use std::collections::BTreeMap;

    use super::*;
    use venue_control_protocol::support_martingale::{
        SupportMartingaleHealth, SupportMartingaleLifecycle,
    };
    use venue_domain::{Amount, Asset, Instrument, InstrumentMetadata, MarketKind, Precision};
    use venue_execution::{SignedAccountPositionMode, SignedAccountSnapshot};
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

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
            health_reason: None,
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
            |_| true,
        );
        assert_eq!(selected.map(|value| value.id.as_str()), Some("lower"));
        Ok(())
    }

    #[test]
    fn newer_support_without_callback_does_not_hide_confirmed_support() {
        let supports = vec![zone("rebound", 60, 62, 1), zone("untouched", 70, 72, 2)];
        let consumed = BTreeSet::new();
        let selected = select_support(&supports, &consumed, Decimal::from(90), None, |support| {
            support.id == "rebound"
        });
        assert_eq!(selected.map(|support| support.id.as_str()), Some("rebound"));
        let consumed = BTreeSet::from(["rebound".to_owned()]);
        assert!(
            select_support(
                &supports,
                &consumed,
                Decimal::from(90),
                None,
                |support| support.id == "rebound"
            )
            .is_none()
        );
        assert!(
            select_support(
                &supports,
                &BTreeSet::new(),
                Decimal::from(90),
                Some(Decimal::from(60)),
                |support| support.id == "rebound"
            )
            .is_none()
        );
    }

    fn spot_metadata(
        minimum_notional: Decimal,
    ) -> Result<InstrumentMetadata, Box<dyn std::error::Error>> {
        let symbol = Symbol::new("BTC", "USDT")?;
        let quote = Asset::new("USDT")?;
        Ok(InstrumentMetadata::new(
            Instrument {
                symbol,
                market: MarketKind::Spot,
                settlement_asset: None,
                generation: 1,
                price_tick: Price::new(Decimal::ONE)?,
                quantity_step: Decimal::new(1, 2),
                minimum_notional: Amount::new(quote, minimum_notional),
            },
            Precision::new(Decimal::ONE, Decimal::ONE)?,
            Precision::new(Decimal::new(1, 2), Decimal::new(1, 2))?,
            None,
            true,
        )?)
    }

    fn plan_entry(
        minimum_notional: Decimal,
        total_budget: Decimal,
        maximum_quantity: Option<Decimal>,
    ) -> Result<Plan, Box<dyn std::error::Error>> {
        let symbol = Symbol::new("BTC", "USDT")?;
        let metadata = spot_metadata(minimum_notional)?;
        let binding = GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            "00000000-0000-0000-0000-000000000001",
            symbol.clone(),
        )?;
        let market = DurableMarketFacts {
            binding: binding.clone(),
            metadata,
            reference_price: Price::new(Decimal::from(100))?,
            observed_at_ms: 1,
            maximum_quantity,
            maximum_price: None,
        };
        let account = SignedAccountSnapshot::complete(
            binding,
            1,
            1,
            1,
            1,
            SignedAccountPositionMode::Hedge,
            Vec::new(),
            Vec::new(),
            "cursor".to_owned(),
            Vec::new(),
        )?;
        let instance = SupportMartingaleInstance {
            instance_id: "instance".to_owned(),
            owner_user_id: "user".to_owned(),
            credential_id: "credential".to_owned(),
            trading_account_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            execution_venue: VenueId::Bybit,
            mode: GatewayMode::Live,
            config: venue_control_protocol::support_martingale::SupportMartingaleConfig {
                allow_btc_neutral: false,
                entry_mode: Default::default(),
                symbol_parameters: Vec::new(),
                reference_venue: VenueId::Binance,
                execution_venue: VenueId::Bybit,
                symbols: vec![symbol.clone()],
                total_budget,
                first_order_notional: Decimal::from(5),
                max_entries: 10,
                size_multiplier: Decimal::ONE,
                target_profit_rate: Decimal::new(5, 3),
                minimum_profit_quote: Decimal::ZERO,
                max_active_positions: 1,
            },
            lifecycle: SupportMartingaleLifecycle::Running,
            health: SupportMartingaleHealth::Healthy,
            revision: 1,
            reserved_budget: Decimal::ZERO,
            symbols: vec![SupportMartingaleSymbolState {
                symbol: symbol.clone(),
                cycle_id: None,
                layer: 0,
                average_price: None,
                quantity: Decimal::ZERO,
                invested: Decimal::ZERO,
                take_profit_price: None,
                net_pnl: None,
                status: "idle".to_owned(),
                health_reason: None,
            }],
        };
        let reference = ReferenceSnapshot {
            fetched_at_ms: 1,
            btc_environment: Vec::new(),
            symbols: BTreeMap::new(),
        };
        let support = zone("support", 90, 100, 1);
        let state = &instance.symbols[0];
        Ok(entry_at_support(
            &PlannerInput {
                instance: &instance,
                symbol: &symbol,
                reference: &reference,
                account: &account,
                execution_market: &market,
                consumed_supports: &BTreeSet::new(),
                last_support_lower: None,
                current_take_profit: None,
                prefer_add_after_cancel: false,
                now_ms: 1,
            },
            state,
            &support,
        ))
    }

    #[test]
    fn entry_quantity_rounds_up_and_returns_actual_notional()
    -> Result<(), Box<dyn std::error::Error>> {
        let metadata = spot_metadata(Decimal::new(54, 1))?;
        let (quantity, actual) = entry_quantity(
            &metadata,
            Some(Decimal::new(6, 2)),
            Decimal::from(5),
            Price::new(Decimal::from(100))?,
        )
        .ok_or("entry quantity")?;
        assert_eq!(quantity, Decimal::new(6, 2));
        assert_eq!(actual, Decimal::from(6));
        Ok(())
    }

    #[test]
    fn entry_quantity_uses_contract_value_for_upward_rounding()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut metadata = spot_metadata(Decimal::from(5))?;
        metadata.instrument.market = MarketKind::LinearPerpetual;
        metadata.instrument.settlement_asset = Some(Asset::new("USDT")?);
        metadata.instrument.quantity_step = Decimal::ONE;
        metadata.quantity = Precision::new(Decimal::ONE, Decimal::ONE)?;
        metadata.contract = Some(venue_domain::ContractSpec::new(
            Decimal::new(3, 2),
            venue_domain::ValueUnit::Base,
            metadata.quantity.clone(),
        )?);
        assert_eq!(
            entry_quantity(
                &metadata,
                Some(Decimal::from(2)),
                Decimal::from(5),
                Price::new(Decimal::from(100))?
            ),
            Some((Decimal::from(2), Decimal::from(6)))
        );
        Ok(())
    }

    #[test]
    fn entry_quantity_rejects_unrepresentable_maximum() -> Result<(), Box<dyn std::error::Error>> {
        let metadata = spot_metadata(Decimal::new(54, 1))?;
        assert!(
            entry_quantity(
                &metadata,
                Some(Decimal::new(5, 2)),
                Decimal::from(5),
                Price::new(Decimal::from(100))?,
            )
            .is_none()
        );
        Ok(())
    }

    #[test]
    fn plan_entry_uses_exact_notional_when_step_is_exact() -> Result<(), Box<dyn std::error::Error>>
    {
        assert!(matches!(
            plan_entry(Decimal::from(5), Decimal::from(10), Some(Decimal::ONE))?,
            Plan::MarketEntry { notional, quantity, .. }
                if notional == Decimal::from(5) && quantity == Decimal::new(5, 2)
        ));
        Ok(())
    }

    #[test]
    fn plan_entry_persists_upward_minimum_notional() -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            plan_entry(Decimal::new(54, 1), Decimal::from(10), Some(Decimal::new(6, 2)))?,
            Plan::MarketEntry { notional, quantity, .. }
                if notional == Decimal::from(6) && quantity == Decimal::new(6, 2)
        ));
        Ok(())
    }

    #[test]
    fn plan_entry_rejects_maximum_quantity_and_budget_overrun()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            plan_entry(
                Decimal::new(54, 1),
                Decimal::from(10),
                Some(Decimal::new(5, 2))
            )?,
            Plan::Noop(NoopReason::QuantityTooSmall)
        ));
        assert!(matches!(
            plan_entry(
                Decimal::new(54, 1),
                Decimal::new(55, 1),
                Some(Decimal::new(6, 2))
            )?,
            Plan::Noop(NoopReason::BudgetExhausted)
        ));
        Ok(())
    }
}
