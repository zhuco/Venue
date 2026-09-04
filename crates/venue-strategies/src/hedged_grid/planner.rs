use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{
    AccountRiskSnapshot, Amount, Asset, InstrumentMetadata, LegRiskSnapshot, OrderSide,
    PositionSide, Price, Symbol, ValueUnit, validate_risk_snapshot_pair,
};

use super::{
    GridInventory, GridOrderIntent, GridOrderKey, GridOrderRole, GridPosition, MAX_GRID_COUNT,
    MIN_GRID_COUNT,
};

pub const GRID_PLANNER_SCHEMA_VERSION: u16 = 1;

/// Versioned strategy inputs. Every threshold is supplied by configuration; no legacy release
/// constant is inherited by the stateless planner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridPlannerConfig {
    pub instance_id: String,
    pub revision: u64,
    pub symbol: Symbol,
    pub order_notional: Amount,
    pub maximum_grid_notional: Amount,
    #[serde(with = "rust_decimal::serde::str")]
    pub spacing_rate: Decimal,
    pub grid_count: u8,
    pub replenishment: Option<GridReplenishmentPolicy>,
    pub profit_reduction: Option<GridProfitReductionPolicy>,
    pub reset_policy: GridResetPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridReplenishmentPolicy {
    pub minimum_leg_notional: Amount,
    pub target_leg_notional: Amount,
    pub max_single_notional: Amount,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridProfitReductionPolicy {
    #[serde(with = "rust_decimal::serde::str")]
    pub inventory_equity_multiple: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_profit_rate: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub reduction_fraction: Decimal,
    pub max_single_notional: Amount,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridResetPolicy {
    pub max_market_age_ms: u64,
    pub max_private_age_ms: u64,
    pub convergence_timeout_ms: u64,
    pub failure_threshold: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridBestBook {
    pub bid: Price,
    pub ask: Price,
    pub observed_at_ms: u64,
}

/// A real trade or mark observation, not a fabricated bid/ask spread. Post-only enforcement
/// belongs to the exchange when the strategy intentionally does not consume a live order book.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridReferencePrice {
    pub price: Price,
    pub observed_at_ms: u64,
}

/// Adapter-normalized exchange filters missing from canonical metadata. Native filter names and
/// symbols stay outside the strategy boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridInstrumentLimits {
    #[serde(with = "rust_decimal::serde::str")]
    pub minimum_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub maximum_quantity: Decimal,
    pub minimum_price: Price,
    pub maximum_price: Price,
}

/// Quantities already reserved by manual orders or other strategy instances. Orders owned by this
/// grid are supplied separately and must not be included here.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct GridCloseReservations {
    #[serde(with = "rust_decimal::serde::str")]
    pub long_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub short_quantity: Decimal,
}

/// Minimal fallback needed when step and quantity cannot be derived from the signed order surface.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridRollingAnchor {
    pub revision: u64,
    pub instrument_generation: u64,
    pub anchor_price: Price,
    pub step: Price,
    #[serde(with = "rust_decimal::serde::str")]
    pub grid_quantity: Decimal,
}

/// Driver-facing semantic coordinates. `sequence` is the immutable lane identity stored in
/// `GridOrderKey::level`; `grid_level` is the current closest-to-market rank used only when a new
/// physical order identity is encoded.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridSemanticOrderKey {
    pub revision: u64,
    pub position: GridPosition,
    pub role: GridOrderRole,
    pub grid_level: u8,
    pub sequence: u64,
}

/// A maker execution is only a recomputation hint. The order surface and inventory in the same
/// input remain the signed post-fill authority. For a complete fill, `source_order` is the original
/// ledger intent; for a partial fill, the signed `owned_orders` entry carries remaining quantity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridMakerFill {
    pub fill_id: String,
    pub source_order: GridOrderIntent,
    pub complete: bool,
    pub maker: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridRiskFacts {
    pub account: AccountRiskSnapshot,
    pub legs: Vec<LegRiskSnapshot>,
    pub conversion: GridRiskConversion,
}

/// Signed conversion evidence joining account-risk values to the configured quote asset. The
/// rate is quote units per one risk-currency unit; callers must not assume stablecoin parity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridRiskConversion {
    pub risk_currency: Asset,
    pub quote_currency: Asset,
    #[serde(with = "rust_decimal::serde::str")]
    pub quote_per_risk_unit: Decimal,
    pub private_generation: u64,
    pub observed_at_ms: u64,
}

/// Reconciler progress is explicit input rather than hidden planner state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct GridConvergenceFacts {
    pub pending_since_ms: Option<u64>,
    pub consecutive_failures: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridPlannerControl {
    Run,
    Stop,
    Reset,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridPlannerInput {
    pub config: GridPlannerConfig,
    pub instrument: InstrumentMetadata,
    pub instrument_limits: GridInstrumentLimits,
    pub book: Option<GridBestBook>,
    pub reference_price: Option<GridReferencePrice>,
    pub inventory: GridInventory,
    pub owned_orders: Vec<GridOrderIntent>,
    pub maker_fills: Vec<GridMakerFill>,
    /// Durable Place intents from predecessor batches which are not yet present in signed
    /// exchange facts. They shape projected planning but cannot be filled or cancelled yet.
    pub pending_place_keys: BTreeSet<GridOrderKey>,
    pub other_close_reservations: GridCloseReservations,
    pub rolling_anchor: Option<GridRollingAnchor>,
    pub convergence: GridConvergenceFacts,
    pub risk: Option<GridRiskFacts>,
    pub control: GridPlannerControl,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridPlan {
    pub schema_version: u16,
    pub instance_id: String,
    pub revision: u64,
    pub directive: GridPlanDirective,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GridPlanDirective {
    Converge {
        rolling_anchor: GridRollingAnchor,
        desired_orders: Vec<GridOrderIntent>,
    },
    Replenish {
        adjustments: Vec<GridInventoryAdjustment>,
        cancel_owned_orders: bool,
        require_fresh_private_facts: bool,
    },
    ReduceExposure {
        reductions: Vec<GridExposureReduction>,
        cancel_owned_orders: bool,
        require_fresh_private_facts: bool,
    },
    ResetRequired {
        trigger: GridResetTrigger,
        cancel_owned_orders: bool,
        keep_positions: bool,
        require_fresh_facts: bool,
    },
    Stop {
        cancel_owned_orders: bool,
        flatten_positions: bool,
    },
    Blocked {
        reason: GridBlockedReason,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridInventoryAdjustment {
    pub position: GridPosition,
    pub side: OrderSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub target_notional: Amount,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GridExposureReduction {
    pub position: GridPosition,
    pub side: OrderSide,
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub maximum_notional: Amount,
    pub close_only: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridResetTrigger {
    Manual,
    RevisionMismatch,
    InstrumentGenerationChanged,
    InvalidOwnedOrder,
    DuplicateOwnedOrder,
    IncompleteOwnedSurface,
    CompletedFillStillOpen,
    ConflictingFillEvidence,
    RollingConflict,
    PriceWouldCrossBook,
    ConvergenceTimedOut,
    FailureThresholdReached,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GridBlockedReason {
    InvalidMarketFacts,
    StaleMarketFacts,
    InvalidPrivateFacts,
    StalePrivateFacts,
    MissingRiskFacts,
    InvalidRiskFacts,
    ReductionBelowMinimum,
    MakerPriceWouldCrossBook,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GridPlannerError {
    #[error("grid planner configuration is invalid")]
    Config,
    #[error("instrument metadata is invalid or incompatible with grid configuration")]
    Instrument,
    #[error("adapter-normalized instrument limits are invalid")]
    InstrumentLimits,
    #[error("planned order is outside adapter-normalized instrument limits")]
    OrderOutsideInstrumentLimits,
    #[error("grid price or quantity normalization failed")]
    Normalization,
    #[error("desired opening orders exceed the configured grid notional limit")]
    OpenNotionalLimit,
    #[error("grid planner decimal arithmetic overflowed")]
    Arithmetic,
}

/// A zero-sized type makes the stateless boundary explicit and leaves no place for checkpoint,
/// actor, journal, or exchange authority to accumulate.
#[derive(Clone, Copy, Debug, Default)]
pub struct GridPlanner;

impl GridPlanner {
    pub fn plan(input: &GridPlannerInput) -> Result<GridPlan, GridPlannerError> {
        validate_identity(&input.config)?;
        if input.control == GridPlannerControl::Stop {
            return Ok(plan_with(
                input,
                GridPlanDirective::Stop {
                    cancel_owned_orders: true,
                    flatten_positions: false,
                },
            ));
        }
        if input.control == GridPlannerControl::Reset {
            return Ok(reset_plan(input, GridResetTrigger::Manual));
        }

        validate_config(&input.config)?;
        validate_instrument(&input.config, &input.instrument, &input.instrument_limits)?;

        if let Some(reason) = validate_market_facts(input) {
            return Ok(blocked_plan(input, reason));
        }
        if let Some(reason) = validate_private_facts(input) {
            return Ok(blocked_plan(input, reason));
        }

        let surface = match validated_surface(input) {
            Ok(surface) => surface,
            Err(trigger) => return Ok(reset_plan(input, trigger)),
        };
        enforce_maximum_grid_notional(input, surface.values())?;

        if input.convergence.consecutive_failures >= input.config.reset_policy.failure_threshold {
            return Ok(reset_plan(input, GridResetTrigger::FailureThresholdReached));
        }
        if let Some(pending_since_ms) = input.convergence.pending_since_ms {
            if pending_since_ms > input.now_ms {
                return Ok(blocked_plan(input, GridBlockedReason::InvalidPrivateFacts));
            }
            if input.now_ms.saturating_sub(pending_since_ms)
                >= input.config.reset_policy.convergence_timeout_ms
            {
                return Ok(reset_plan(input, GridResetTrigger::ConvergenceTimedOut));
            }
        }

        if let Some(directive) = exposure_directive(input)? {
            return Ok(plan_with(input, directive));
        }
        if let Some(directive) = replenishment_directive(input)? {
            return Ok(plan_with(input, directive));
        }

        let (rolling_anchor, desired_orders) = if surface.is_empty() && input.maker_fills.is_empty()
        {
            initial_surface(input)?
        } else {
            match rolled_surface(input, surface) {
                Ok(surface) => surface,
                Err(GridResetTrigger::PriceWouldCrossBook) => {
                    return Ok(blocked_plan(
                        input,
                        GridBlockedReason::MakerPriceWouldCrossBook,
                    ));
                }
                Err(trigger) => return Ok(reset_plan(input, trigger)),
            }
        };
        enforce_maximum_grid_notional(input, desired_orders.iter())?;
        Ok(plan_with(
            input,
            GridPlanDirective::Converge {
                rolling_anchor,
                desired_orders,
            },
        ))
    }

    /// Maps one desired order to the protocol's bounded grid coordinate without confusing the
    /// legacy field name `level` with its new meaning as a monotonic lane sequence.
    pub fn semantic_order_key(
        desired_orders: &[GridOrderIntent],
        key: &GridOrderKey,
    ) -> Result<GridSemanticOrderKey, GridPlannerError> {
        let target = desired_orders
            .iter()
            .find(|order| &order.key == key)
            .ok_or(GridPlannerError::Normalization)?;
        let mut lane = desired_orders
            .iter()
            .filter(|order| {
                order.key.epoch == key.epoch
                    && order.key.position == key.position
                    && order.key.role == key.role
            })
            .collect::<Vec<_>>();
        lane.sort_by(|left, right| match target.side {
            OrderSide::Buy => right
                .price
                .cmp(&left.price)
                .then_with(|| left.key.level.cmp(&right.key.level)),
            OrderSide::Sell => left
                .price
                .cmp(&right.price)
                .then_with(|| left.key.level.cmp(&right.key.level)),
        });
        if lane.len() > usize::from(MAX_GRID_COUNT)
            || lane.windows(2).any(|pair| pair[0].price == pair[1].price)
        {
            return Err(GridPlannerError::Normalization);
        }
        let grid_level = lane
            .iter()
            .position(|order| order.key == *key)
            .and_then(|index| u8::try_from(index + 1).ok())
            .ok_or(GridPlannerError::Normalization)?;
        Ok(GridSemanticOrderKey {
            revision: key.epoch,
            position: key.position,
            role: key.role,
            grid_level,
            sequence: key.level,
        })
    }
}

fn validate_config(config: &GridPlannerConfig) -> Result<(), GridPlannerError> {
    validate_identity(config)?;
    let quote_matches = config.order_notional.asset.as_str() == config.symbol.quote()
        && config.maximum_grid_notional.asset == config.order_notional.asset;
    if !quote_matches
        || config.order_notional.value <= Decimal::ZERO
        || config.maximum_grid_notional.value <= Decimal::ZERO
        || config.spacing_rate <= Decimal::ZERO
        || config.spacing_rate >= Decimal::ONE
        || !(MIN_GRID_COUNT..=MAX_GRID_COUNT).contains(&config.grid_count)
        || config.reset_policy.max_market_age_ms == 0
        || config.reset_policy.max_private_age_ms == 0
        || config.reset_policy.convergence_timeout_ms == 0
        || config.reset_policy.failure_threshold == 0
    {
        return Err(GridPlannerError::Config);
    }
    if let Some(policy) = &config.replenishment {
        let same_asset = policy.minimum_leg_notional.asset == config.order_notional.asset
            && policy.target_leg_notional.asset == config.order_notional.asset
            && policy.max_single_notional.asset == config.order_notional.asset;
        if !same_asset
            || policy.minimum_leg_notional.value <= Decimal::ZERO
            || policy.target_leg_notional.value <= policy.minimum_leg_notional.value
            || policy.max_single_notional.value <= Decimal::ZERO
        {
            return Err(GridPlannerError::Config);
        }
    }
    if let Some(policy) = &config.profit_reduction {
        if policy.max_single_notional.asset != config.order_notional.asset
            || policy.inventory_equity_multiple <= Decimal::ZERO
            || policy.minimum_profit_rate <= Decimal::ZERO
            || policy.minimum_profit_rate > Decimal::ONE
            || policy.reduction_fraction <= Decimal::ZERO
            || policy.reduction_fraction > Decimal::ONE
            || policy.max_single_notional.value <= Decimal::ZERO
        {
            return Err(GridPlannerError::Config);
        }
    }
    Ok(())
}

fn validate_identity(config: &GridPlannerConfig) -> Result<(), GridPlannerError> {
    let valid_id = !config.instance_id.is_empty()
        && config.instance_id.len() <= 64
        && config
            .instance_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    if !valid_id || config.revision == 0 {
        return Err(GridPlannerError::Config);
    }
    Ok(())
}

fn validate_instrument(
    config: &GridPlannerConfig,
    instrument: &InstrumentMetadata,
    limits: &GridInstrumentLimits,
) -> Result<(), GridPlannerError> {
    instrument
        .validate()
        .map_err(|_| GridPlannerError::Instrument)?;
    if !instrument.trading_enabled
        || instrument.instrument.symbol != config.symbol
        || instrument.instrument.minimum_notional.asset != config.order_notional.asset
    {
        return Err(GridPlannerError::Instrument);
    }
    let exchange_minimum_notional = instrument.instrument.minimum_notional.value;
    if config.replenishment.as_ref().is_some_and(|policy| {
        policy.target_leg_notional.value < exchange_minimum_notional
            || policy.max_single_notional.value < exchange_minimum_notional
    }) || config
        .profit_reduction
        .as_ref()
        .is_some_and(|policy| policy.max_single_notional.value < exchange_minimum_notional)
    {
        return Err(GridPlannerError::Config);
    }
    let quantity_aligned = limits.minimum_quantity % instrument.quantity.step == Decimal::ZERO
        && limits.maximum_quantity % instrument.quantity.step == Decimal::ZERO;
    let price_aligned = limits.minimum_price.value() % instrument.price.step == Decimal::ZERO
        && limits.maximum_price.value() % instrument.price.step == Decimal::ZERO;
    if limits.minimum_quantity < instrument.quantity.minimum
        || limits.maximum_quantity < limits.minimum_quantity
        || limits.minimum_price.value() < instrument.price.minimum
        || limits.maximum_price < limits.minimum_price
        || !quantity_aligned
        || !price_aligned
    {
        return Err(GridPlannerError::InstrumentLimits);
    }
    Ok(())
}

fn validate_market_facts(input: &GridPlannerInput) -> Option<GridBlockedReason> {
    if let Some(reference) = &input.reference_price {
        if input.book.is_some()
            || input.now_ms == 0
            || reference.observed_at_ms == 0
            || reference.observed_at_ms > input.now_ms
        {
            return Some(GridBlockedReason::InvalidMarketFacts);
        }
        return (input.now_ms.saturating_sub(reference.observed_at_ms)
            > input.config.reset_policy.max_private_age_ms)
            .then_some(GridBlockedReason::StaleMarketFacts);
    }
    let Some(book) = &input.book else {
        return Some(GridBlockedReason::InvalidMarketFacts);
    };
    if input.now_ms == 0
        || book.observed_at_ms == 0
        || book.observed_at_ms > input.now_ms
        || book.bid >= book.ask
    {
        return Some(GridBlockedReason::InvalidMarketFacts);
    }
    if input.now_ms.saturating_sub(book.observed_at_ms)
        > input.config.reset_policy.max_market_age_ms
    {
        return Some(GridBlockedReason::StaleMarketFacts);
    }
    None
}

fn validate_private_facts(input: &GridPlannerInput) -> Option<GridBlockedReason> {
    if input.inventory.validate().is_err()
        || input.inventory.private_observed_at_ms == 0
        || input.inventory.private_observed_at_ms > input.now_ms
        || input
            .other_close_reservations
            .long_quantity
            .is_sign_negative()
        || input
            .other_close_reservations
            .short_quantity
            .is_sign_negative()
        || input.other_close_reservations.long_quantity > input.inventory.long_quantity
        || input.other_close_reservations.short_quantity > input.inventory.short_quantity
    {
        return Some(GridBlockedReason::InvalidPrivateFacts);
    }
    if input
        .now_ms
        .saturating_sub(input.inventory.private_observed_at_ms)
        > input.config.reset_policy.max_private_age_ms
    {
        return Some(GridBlockedReason::StalePrivateFacts);
    }
    None
}

fn validated_surface(
    input: &GridPlannerInput,
) -> Result<BTreeMap<GridOrderKey, GridOrderIntent>, GridResetTrigger> {
    let mut surface = BTreeMap::new();
    let mut lane_prices = BTreeSet::new();
    for order in &input.owned_orders {
        if order.validate().is_err()
            || order.key.epoch != input.config.revision
            || !input.price_within_limits(order.price)
            || !input.quantity_within_limits(order.quantity)
        {
            return Err(if order.key.epoch != input.config.revision {
                GridResetTrigger::RevisionMismatch
            } else {
                GridResetTrigger::InvalidOwnedOrder
            });
        }
        // A signed resting order and the newest BBO are not an atomic snapshot. Crossing
        // may mean its fill is in flight; only authenticated order/fill facts retire it.
        if !lane_prices.insert((order.key.position, order.key.role, order.price))
            || surface.insert(order.key.clone(), order.clone()).is_some()
        {
            return Err(GridResetTrigger::DuplicateOwnedOrder);
        }
    }
    Ok(surface)
}

fn initial_surface(
    input: &GridPlannerInput,
) -> Result<(GridRollingAnchor, Vec<GridOrderIntent>), GridPlannerError> {
    let midpoint = input
        .reference_value()
        .ok_or(GridPlannerError::Arithmetic)?;
    let anchor_value = input
        .instrument
        .price
        .floor(midpoint)
        .map_err(|_| GridPlannerError::Normalization)?;
    let anchor_price = Price::new(anchor_value).map_err(|_| GridPlannerError::Normalization)?;
    let raw_step = anchor_value
        .checked_mul(input.config.spacing_rate)
        .ok_or(GridPlannerError::Arithmetic)?;
    let step_value =
        ceil_to_step(raw_step, input.instrument.price.step)?.max(input.instrument.price.step);
    let step = Price::new(step_value).map_err(|_| GridPlannerError::Normalization)?;
    let outer_distance = step_value
        .checked_mul(Decimal::from(input.config.grid_count))
        .ok_or(GridPlannerError::Arithmetic)?;
    let lowest_price_value = anchor_value
        .checked_sub(outer_distance)
        .ok_or(GridPlannerError::Arithmetic)?;
    let lowest_price = Price::new(lowest_price_value).map_err(|_| GridPlannerError::Config)?;
    let target_notional = input
        .config
        .order_notional
        .value
        .max(input.instrument.instrument.minimum_notional.value);
    let grid_quantity = quantity_for_notional_ceil(
        &input.instrument,
        &input.instrument_limits,
        target_notional,
        lowest_price,
    )?;
    let anchor = GridRollingAnchor {
        revision: input.config.revision,
        instrument_generation: input.instrument.instrument.generation,
        anchor_price,
        step,
        grid_quantity,
    };
    let mut desired = Vec::with_capacity(usize::from(input.config.grid_count) * 4);
    for position in [GridPosition::Long, GridPosition::Short] {
        for level in 1..=input.config.grid_count {
            desired.push(order_from_anchor(
                &anchor,
                position,
                GridOrderRole::Open,
                u64::from(level),
            )?);
        }
        for level in 1..=input.config.grid_count {
            desired.push(order_from_anchor(
                &anchor,
                position,
                GridOrderRole::Close,
                u64::from(level),
            )?);
        }
    }
    let desired = clip_close_orders(input, desired)?;
    validate_generated_orders(input, &desired)?;
    Ok((anchor, desired))
}

fn rolled_surface(
    input: &GridPlannerInput,
    mut surface: BTreeMap<GridOrderKey, GridOrderIntent>,
) -> Result<(GridRollingAnchor, Vec<GridOrderIntent>), GridResetTrigger> {
    if !input
        .pending_place_keys
        .iter()
        .all(|key| surface.contains_key(key))
    {
        return Err(GridResetTrigger::ConflictingFillEvidence);
    }
    let mut anchor = match input.rolling_anchor.clone() {
        Some(anchor) => validate_anchor(input, anchor)?,
        None => infer_anchor(input, &surface)?,
    };
    let mut fill_ids = BTreeMap::<String, GridMakerFill>::new();
    let mut completed_sources = BTreeSet::new();
    for fill in &input.maker_fills {
        if fill.fill_id.trim().is_empty()
            || fill.source_order.validate().is_err()
            || fill.source_order.key.epoch != input.config.revision
            || !fill.maker
        {
            return Err(GridResetTrigger::ConflictingFillEvidence);
        }
        if let Some(previous) = fill_ids.insert(fill.fill_id.clone(), fill.clone()) {
            if previous != *fill {
                return Err(GridResetTrigger::ConflictingFillEvidence);
            }
        }
        if input.pending_place_keys.contains(&fill.source_order.key)
            || (fill.complete && !completed_sources.insert(fill.source_order.key.clone()))
        {
            return Err(GridResetTrigger::ConflictingFillEvidence);
        }
    }
    let mut complete_fills = fill_ids
        .into_values()
        .filter(|fill| fill.complete)
        .collect::<Vec<_>>();
    complete_fills.sort_by(|left, right| {
        left.source_order
            .key
            .cmp(&right.source_order.key)
            .then_with(|| left.fill_id.cmp(&right.fill_id))
    });

    let mut pre_fill = surface.clone();
    for fill in &complete_fills {
        if surface.contains_key(&fill.source_order.key) {
            return Err(GridResetTrigger::CompletedFillStillOpen);
        }
        match pre_fill.insert(fill.source_order.key.clone(), fill.source_order.clone()) {
            Some(existing) if existing != fill.source_order => {
                return Err(GridResetTrigger::ConflictingFillEvidence);
            }
            _ => {}
        }
    }
    if !complete_fills.is_empty() && !complete_surface_shape(input, &pre_fill) {
        return Err(GridResetTrigger::IncompleteOwnedSurface);
    }
    if complete_fills.is_empty() && !complete_surface_shape(input, &surface) {
        return Err(GridResetTrigger::IncompleteOwnedSurface);
    }

    let mut cancellable = surface
        .keys()
        .filter(|key| !input.pending_place_keys.contains(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut next_levels = next_levels(&pre_fill)?;
    let mut rolled_keys = BTreeSet::new();
    for fill in complete_fills {
        let position = fill.source_order.key.position;
        let cancel_role = opposite_role(fill.source_order.key.role);
        let cancel_key = cancellation_candidate(
            &surface,
            &cancellable,
            position,
            cancel_role,
            fill.source_order.side,
        )
        .ok_or(GridResetTrigger::RollingConflict)?;
        cancellable.remove(&cancel_key);
        surface.remove(&cancel_key);

        let prices = [GridOrderRole::Open, GridOrderRole::Close]
            .into_iter()
            .map(|role| {
                rolling_price(&surface, &fill.source_order, role, anchor.step)
                    .map(|price| (role, price))
            })
            .collect::<Result<Vec<_>, _>>()?;
        anchor.grid_quantity = normalized_rolled_quantity(input, anchor.grid_quantity, &prices)?;
        for (role, price) in prices {
            let level = next_level(&mut next_levels, position, role)?;
            let order = order_at_price(
                input.config.revision,
                position,
                role,
                level,
                price,
                anchor.grid_quantity,
            )?;
            if surface.values().any(|existing| {
                existing.key.position == position
                    && existing.key.role == role
                    && existing.price == order.price
            }) || surface.insert(order.key.clone(), order.clone()).is_some()
            {
                return Err(GridResetTrigger::RollingConflict);
            }
            rolled_keys.insert(order.key);
        }
    }
    for key in rolled_keys {
        let order = surface
            .get_mut(&key)
            .ok_or(GridResetTrigger::RollingConflict)?;
        order.quantity = anchor.grid_quantity;
    }
    let desired = clip_close_orders(input, surface.into_values().collect())
        .map_err(|_| GridResetTrigger::RollingConflict)?;
    // Check only the final new placements, not signed resting orders or intermediate orders
    // removed by a later fill in this batch. A crossing Maker target waits, never resets.
    if desired.iter().any(|order| {
        !input
            .owned_orders
            .iter()
            .any(|owned| owned.key == order.key)
            && input
                .book
                .as_ref()
                .is_some_and(|book| crosses_book(order, book))
    }) {
        return Err(GridResetTrigger::PriceWouldCrossBook);
    }
    if validate_generated_orders(input, &desired).is_err() {
        return Err(GridResetTrigger::InvalidOwnedOrder);
    }
    Ok((anchor, desired))
}

fn normalized_rolled_quantity(
    input: &GridPlannerInput,
    current: Decimal,
    prices: &[(GridOrderRole, Price)],
) -> Result<Decimal, GridResetTrigger> {
    let mut quantity = current;
    for (_, price) in prices {
        if raw_quote_notional(&input.instrument, quantity, *price)
            .map_err(|_| GridResetTrigger::RollingConflict)?
            < input.instrument.instrument.minimum_notional.value
        {
            quantity = quantity_for_notional_ceil(
                &input.instrument,
                &input.instrument_limits,
                input.instrument.instrument.minimum_notional.value,
                *price,
            )
            .map_err(|_| GridResetTrigger::InvalidOwnedOrder)?;
        }
    }
    Ok(quantity)
}

fn validate_anchor(
    input: &GridPlannerInput,
    anchor: GridRollingAnchor,
) -> Result<GridRollingAnchor, GridResetTrigger> {
    if anchor.revision != input.config.revision {
        return Err(GridResetTrigger::RevisionMismatch);
    }
    if anchor.instrument_generation != input.instrument.instrument.generation {
        return Err(GridResetTrigger::InstrumentGenerationChanged);
    }
    if anchor.grid_quantity <= Decimal::ZERO
        || !input.price_within_limits(anchor.anchor_price)
        || anchor.step.value() % input.instrument.price.step != Decimal::ZERO
        || !input.quantity_within_limits(anchor.grid_quantity)
    {
        return Err(GridResetTrigger::InvalidOwnedOrder);
    }
    Ok(anchor)
}

fn infer_anchor(
    input: &GridPlannerInput,
    surface: &BTreeMap<GridOrderKey, GridOrderIntent>,
) -> Result<GridRollingAnchor, GridResetTrigger> {
    let grid_quantity = surface
        .values()
        .find(|order| order.key.role == GridOrderRole::Open)
        .map(|order| order.quantity)
        .ok_or(GridResetTrigger::IncompleteOwnedSurface)?;
    if surface
        .values()
        .filter(|order| order.key.role == GridOrderRole::Open)
        .any(|order| order.quantity != grid_quantity)
    {
        return Err(GridResetTrigger::InvalidOwnedOrder);
    }
    let mut differences = Vec::new();
    for position in [GridPosition::Long, GridPosition::Short] {
        for role in [GridOrderRole::Open, GridOrderRole::Close] {
            let mut prices = surface
                .values()
                .filter(|order| order.key.position == position && order.key.role == role)
                .map(|order| order.price.value())
                .collect::<Vec<_>>();
            prices.sort();
            differences.extend(prices.windows(2).filter_map(|pair| {
                pair[1]
                    .checked_sub(pair[0])
                    .filter(|difference| *difference > Decimal::ZERO)
            }));
        }
    }
    let step_value = differences.into_iter().min().unwrap_or_else(|| {
        let midpoint = input.reference_value().unwrap_or(Decimal::ZERO);
        ceil_to_step(
            midpoint * input.config.spacing_rate,
            input.instrument.price.step,
        )
        .unwrap_or(input.instrument.price.step)
    });
    if step_value <= Decimal::ZERO || step_value % input.instrument.price.step != Decimal::ZERO {
        return Err(GridResetTrigger::InvalidOwnedOrder);
    }
    let midpoint = input
        .reference_value()
        .ok_or(GridResetTrigger::RollingConflict)?;
    let anchor_value = input
        .instrument
        .price
        .floor(midpoint)
        .map_err(|_| GridResetTrigger::RollingConflict)?;
    Ok(GridRollingAnchor {
        revision: input.config.revision,
        instrument_generation: input.instrument.instrument.generation,
        anchor_price: Price::new(anchor_value).map_err(|_| GridResetTrigger::RollingConflict)?,
        step: Price::new(step_value).map_err(|_| GridResetTrigger::RollingConflict)?,
        grid_quantity,
    })
}

fn complete_surface_shape(
    input: &GridPlannerInput,
    surface: &BTreeMap<GridOrderKey, GridOrderIntent>,
) -> bool {
    [GridPosition::Long, GridPosition::Short]
        .into_iter()
        .all(|position| {
            let opens = surface
                .values()
                .filter(|order| {
                    order.key.position == position && order.key.role == GridOrderRole::Open
                })
                .count();
            let closes = surface
                .values()
                .filter(|order| {
                    order.key.position == position && order.key.role == GridOrderRole::Close
                })
                .count();
            opens == usize::from(input.config.grid_count)
                && closes <= usize::from(input.config.grid_count)
        })
}

fn cancellation_candidate(
    surface: &BTreeMap<GridOrderKey, GridOrderIntent>,
    cancellable: &BTreeSet<GridOrderKey>,
    position: GridPosition,
    role: GridOrderRole,
    source_side: OrderSide,
) -> Option<GridOrderKey> {
    let candidates = surface.iter().filter(|(key, _)| {
        cancellable.contains(*key) && key.position == position && key.role == role
    });
    match source_side {
        OrderSide::Sell => candidates
            .min_by_key(|(_, order)| order.price)
            .map(|(key, _)| key.clone()),
        OrderSide::Buy => candidates
            .max_by_key(|(_, order)| order.price)
            .map(|(key, _)| key.clone()),
    }
}

fn rolling_price(
    surface: &BTreeMap<GridOrderKey, GridOrderIntent>,
    source: &GridOrderIntent,
    role: GridOrderRole,
    step: Price,
) -> Result<Price, GridResetTrigger> {
    let value = if role == source.key.role {
        let lane = surface
            .values()
            .filter(|order| order.key.position == source.key.position && order.key.role == role);
        match source.side {
            OrderSide::Sell => lane
                .map(|order| order.price.value())
                .max()
                .unwrap_or(source.price.value())
                .checked_add(step.value()),
            OrderSide::Buy => lane
                .map(|order| order.price.value())
                .min()
                .unwrap_or(source.price.value())
                .checked_sub(step.value()),
        }
    } else {
        match source.side {
            OrderSide::Sell => source.price.value().checked_sub(step.value()),
            OrderSide::Buy => source.price.value().checked_add(step.value()),
        }
    }
    .ok_or(GridResetTrigger::RollingConflict)?;
    Price::new(value).map_err(|_| GridResetTrigger::RollingConflict)
}

fn next_levels(
    surface: &BTreeMap<GridOrderKey, GridOrderIntent>,
) -> Result<BTreeMap<(GridPosition, GridOrderRole), u64>, GridResetTrigger> {
    let mut next = BTreeMap::new();
    for key in surface.keys() {
        let entry = next.entry((key.position, key.role)).or_insert(0_u64);
        *entry = (*entry).max(key.level);
    }
    Ok(next)
}

fn next_level(
    levels: &mut BTreeMap<(GridPosition, GridOrderRole), u64>,
    position: GridPosition,
    role: GridOrderRole,
) -> Result<u64, GridResetTrigger> {
    let value = levels.entry((position, role)).or_insert(0);
    *value = value
        .checked_add(1)
        .ok_or(GridResetTrigger::RollingConflict)?;
    Ok(*value)
}

fn exposure_directive(
    input: &GridPlannerInput,
) -> Result<Option<GridPlanDirective>, GridPlannerError> {
    let Some(policy) = &input.config.profit_reduction else {
        return Ok(None);
    };
    let Some(risk) = &input.risk else {
        return Ok(Some(GridPlanDirective::Blocked {
            reason: GridBlockedReason::MissingRiskFacts,
        }));
    };
    if risk.account.private_generation != input.inventory.private_generation {
        return Ok(Some(GridPlanDirective::Blocked {
            reason: GridBlockedReason::InvalidRiskFacts,
        }));
    }
    let conversion = &risk.conversion;
    if conversion.risk_currency != risk.account.risk_currency
        || conversion.quote_currency.as_str() != input.config.symbol.quote()
        || conversion.quote_per_risk_unit <= Decimal::ZERO
        || conversion.private_generation != input.inventory.private_generation
        || conversion.observed_at_ms == 0
        || conversion.observed_at_ms > input.now_ms
        || input.now_ms.saturating_sub(conversion.observed_at_ms)
            > input.config.reset_policy.max_private_age_ms
    {
        return Ok(Some(GridPlanDirective::Blocked {
            reason: GridBlockedReason::InvalidRiskFacts,
        }));
    }
    let mut by_position = BTreeMap::new();
    for leg in &risk.legs {
        let position = match leg.position_side {
            PositionSide::Long => GridPosition::Long,
            PositionSide::Short => GridPosition::Short,
            PositionSide::Net => {
                return Ok(Some(GridPlanDirective::Blocked {
                    reason: GridBlockedReason::InvalidRiskFacts,
                }));
            }
        };
        if validate_risk_snapshot_pair(
            &risk.account,
            leg,
            input.now_ms,
            input.config.reset_policy.max_private_age_ms,
        )
        .is_err()
            || leg.symbol != input.config.symbol
            || leg.private_generation != input.inventory.private_generation
            || by_position.insert(position, leg).is_some()
        {
            return Ok(Some(GridPlanDirective::Blocked {
                reason: GridBlockedReason::InvalidRiskFacts,
            }));
        }
    }
    for position in [GridPosition::Long, GridPosition::Short] {
        let inventory_quantity = inventory_quantity(&input.inventory, position);
        match by_position.get(&position) {
            Some(leg) if leg.quantity != inventory_quantity => {
                return Ok(Some(GridPlanDirective::Blocked {
                    reason: GridBlockedReason::InvalidRiskFacts,
                }));
            }
            None if inventory_quantity > Decimal::ZERO => {
                return Ok(Some(GridPlanDirective::Blocked {
                    reason: GridBlockedReason::MissingRiskFacts,
                }));
            }
            _ => {}
        }
    }

    let equity_limit = risk
        .account
        .account_equity
        .checked_mul(policy.inventory_equity_multiple)
        .ok_or(GridPlannerError::Arithmetic)?;
    let mut reductions = Vec::new();
    for position in [GridPosition::Long, GridPosition::Short] {
        let Some(leg) = by_position.get(&position) else {
            continue;
        };
        let profit_rate = leg
            .unrealized_pnl
            .checked_div(leg.notional)
            .ok_or(GridPlannerError::Arithmetic)?;
        if leg.notional < equity_limit || profit_rate < policy.minimum_profit_rate {
            continue;
        }
        let fraction_risk_notional = leg
            .notional
            .checked_mul(policy.reduction_fraction)
            .ok_or(GridPlannerError::Arithmetic)?;
        let fraction_quote_notional = fraction_risk_notional
            .checked_mul(conversion.quote_per_risk_unit)
            .ok_or(GridPlannerError::Arithmetic)?;
        let target_notional = fraction_quote_notional.min(policy.max_single_notional.value);
        let mut quantity = quantity_for_notional_floor(
            &input.instrument,
            &input.instrument_limits,
            target_notional,
            input.inventory.mark_price,
        )?;
        let available = inventory_quantity(&input.inventory, position)
            .checked_sub(reserved_quantity(&input.other_close_reservations, position))
            .ok_or(GridPlannerError::Arithmetic)?;
        quantity = input
            .instrument
            .quantity
            .floor(quantity.min(available))
            .map_err(|_| GridPlannerError::Normalization)?;
        if quantity <= Decimal::ZERO
            || !input.quantity_within_limits(quantity)
            || raw_quote_notional(&input.instrument, quantity, input.inventory.mark_price)?
                < input.instrument.instrument.minimum_notional.value
        {
            return Ok(Some(GridPlanDirective::Blocked {
                reason: GridBlockedReason::ReductionBelowMinimum,
            }));
        }
        reductions.push(GridExposureReduction {
            position,
            side: position.closing_side(),
            quantity,
            maximum_notional: policy.max_single_notional.clone(),
            close_only: true,
        });
    }
    if reductions.is_empty() {
        Ok(None)
    } else {
        Ok(Some(GridPlanDirective::ReduceExposure {
            reductions,
            cancel_owned_orders: true,
            require_fresh_private_facts: true,
        }))
    }
}

fn replenishment_directive(
    input: &GridPlannerInput,
) -> Result<Option<GridPlanDirective>, GridPlannerError> {
    let Some(policy) = &input.config.replenishment else {
        return Ok(None);
    };
    let mut adjustments = Vec::new();
    for position in [GridPosition::Long, GridPosition::Short] {
        let current_notional = raw_quote_notional(
            &input.instrument,
            inventory_quantity(&input.inventory, position),
            input.inventory.mark_price,
        )?;
        if current_notional >= policy.minimum_leg_notional.value {
            continue;
        }
        let missing = policy
            .target_leg_notional
            .value
            .checked_sub(current_notional)
            .ok_or(GridPlannerError::Arithmetic)?;
        let target = missing.min(policy.max_single_notional.value);
        let quantity = quantity_for_notional_ceil(
            &input.instrument,
            &input.instrument_limits,
            target.max(input.instrument.instrument.minimum_notional.value),
            input.inventory.mark_price,
        )?;
        adjustments.push(GridInventoryAdjustment {
            position,
            side: position.opening_side(),
            quantity,
            target_notional: Amount::new(policy.target_leg_notional.asset.clone(), target),
        });
    }
    if adjustments.is_empty() {
        Ok(None)
    } else {
        Ok(Some(GridPlanDirective::Replenish {
            adjustments,
            cancel_owned_orders: true,
            require_fresh_private_facts: true,
        }))
    }
}

fn clip_close_orders(
    input: &GridPlannerInput,
    orders: Vec<GridOrderIntent>,
) -> Result<Vec<GridOrderIntent>, GridPlannerError> {
    let mut open_orders = orders
        .iter()
        .filter(|order| order.key.role == GridOrderRole::Open)
        .cloned()
        .collect::<Vec<_>>();
    let mut close_orders = Vec::new();
    for position in [GridPosition::Long, GridPosition::Short] {
        let mut lane = orders
            .iter()
            .filter(|order| {
                order.key.position == position && order.key.role == GridOrderRole::Close
            })
            .cloned()
            .collect::<Vec<_>>();
        lane.sort_by(|left, right| match position {
            GridPosition::Long => left.price.cmp(&right.price),
            GridPosition::Short => right.price.cmp(&left.price),
        });
        let mut available = inventory_quantity(&input.inventory, position)
            .checked_sub(reserved_quantity(&input.other_close_reservations, position))
            .ok_or(GridPlannerError::Arithmetic)?;
        for mut order in lane {
            let quantity = input
                .instrument
                .quantity
                .floor(order.quantity.min(available))
                .map_err(|_| GridPlannerError::Normalization)?;
            if quantity < input.instrument_limits.minimum_quantity
                || raw_quote_notional(&input.instrument, quantity, order.price)?
                    < input.instrument.instrument.minimum_notional.value
            {
                continue;
            }
            order.quantity = quantity;
            available = available
                .checked_sub(quantity)
                .ok_or(GridPlannerError::Arithmetic)?;
            close_orders.push(order);
        }
    }
    open_orders.extend(close_orders);
    open_orders.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(open_orders)
}

fn order_from_anchor(
    anchor: &GridRollingAnchor,
    position: GridPosition,
    role: GridOrderRole,
    level: u64,
) -> Result<GridOrderIntent, GridPlannerError> {
    let distance = anchor
        .step
        .value()
        .checked_mul(Decimal::from(level))
        .ok_or(GridPlannerError::Arithmetic)?;
    let lower = anchor
        .anchor_price
        .value()
        .checked_sub(distance)
        .ok_or(GridPlannerError::Arithmetic)?;
    let upper = anchor
        .anchor_price
        .value()
        .checked_add(distance)
        .ok_or(GridPlannerError::Arithmetic)?;
    let value =
        match (position, role) {
            (GridPosition::Long, GridOrderRole::Open)
            | (GridPosition::Short, GridOrderRole::Close) => lower,
            (GridPosition::Short, GridOrderRole::Open)
            | (GridPosition::Long, GridOrderRole::Close) => upper,
        };
    let price = Price::new(value).map_err(|_| GridPlannerError::Normalization)?;
    order_at_price(
        anchor.revision,
        position,
        role,
        level,
        price,
        anchor.grid_quantity,
    )
    .map_err(|_| GridPlannerError::Normalization)
}

fn order_at_price(
    revision: u64,
    position: GridPosition,
    role: GridOrderRole,
    level: u64,
    price: Price,
    quantity: Decimal,
) -> Result<GridOrderIntent, GridResetTrigger> {
    let order = GridOrderIntent {
        key: GridOrderKey {
            epoch: revision,
            position,
            role,
            level,
        },
        side: match role {
            GridOrderRole::Open => position.opening_side(),
            GridOrderRole::Close => position.closing_side(),
        },
        price,
        quantity,
        reduce_only: role == GridOrderRole::Close,
    };
    order
        .validate()
        .map_err(|_| GridResetTrigger::InvalidOwnedOrder)?;
    Ok(order)
}

fn quantity_for_notional_ceil(
    instrument: &InstrumentMetadata,
    limits: &GridInstrumentLimits,
    notional: Decimal,
    price: Price,
) -> Result<Decimal, GridPlannerError> {
    if notional <= Decimal::ZERO {
        return Err(GridPlannerError::Normalization);
    }
    let divisor = quantity_notional_divisor(instrument, price)?;
    let raw = notional
        .checked_div(divisor)
        .ok_or(GridPlannerError::Arithmetic)?;
    let quantity = ceil_to_step(raw, instrument.quantity.step)?.max(limits.minimum_quantity);
    if quantity > limits.maximum_quantity {
        return Err(GridPlannerError::OrderOutsideInstrumentLimits);
    }
    if !instrument
        .quantity
        .accepts(quantity)
        .map_err(|_| GridPlannerError::Normalization)?
    {
        return Err(GridPlannerError::Normalization);
    }
    Ok(quantity)
}

fn quantity_for_notional_floor(
    instrument: &InstrumentMetadata,
    limits: &GridInstrumentLimits,
    notional: Decimal,
    price: Price,
) -> Result<Decimal, GridPlannerError> {
    if notional <= Decimal::ZERO {
        return Err(GridPlannerError::Normalization);
    }
    let divisor = quantity_notional_divisor(instrument, price)?;
    let quantity = instrument
        .quantity
        .floor(
            notional
                .checked_div(divisor)
                .ok_or(GridPlannerError::Arithmetic)?
                .min(limits.maximum_quantity),
        )
        .map_err(|_| GridPlannerError::Normalization)?;
    Ok(quantity)
}

fn quantity_notional_divisor(
    instrument: &InstrumentMetadata,
    price: Price,
) -> Result<Decimal, GridPlannerError> {
    match &instrument.contract {
        Some(contract) => match contract.value_unit {
            ValueUnit::Quote => Ok(contract.value_per_lot),
            ValueUnit::Base => contract
                .value_per_lot
                .checked_mul(price.value())
                .ok_or(GridPlannerError::Arithmetic),
        },
        None => Ok(price.value()),
    }
}

fn raw_quote_notional(
    instrument: &InstrumentMetadata,
    quantity: Decimal,
    price: Price,
) -> Result<Decimal, GridPlannerError> {
    quantity
        .checked_mul(quantity_notional_divisor(instrument, price)?)
        .ok_or(GridPlannerError::Arithmetic)
}

fn enforce_maximum_grid_notional<'a>(
    input: &GridPlannerInput,
    orders: impl IntoIterator<Item = &'a GridOrderIntent>,
) -> Result<(), GridPlannerError> {
    let mut total = Decimal::ZERO;
    for order in orders
        .into_iter()
        .filter(|order| order.key.role == GridOrderRole::Open)
    {
        total = total
            .checked_add(raw_quote_notional(
                &input.instrument,
                order.quantity,
                order.price,
            )?)
            .ok_or(GridPlannerError::Arithmetic)?;
        if total > input.config.maximum_grid_notional.value {
            return Err(GridPlannerError::OpenNotionalLimit);
        }
    }
    Ok(())
}

fn validate_generated_orders(
    input: &GridPlannerInput,
    orders: &[GridOrderIntent],
) -> Result<(), GridPlannerError> {
    for order in orders {
        if order.validate().is_err()
            || !input.price_within_limits(order.price)
            || !input.quantity_within_limits(order.quantity)
        {
            return Err(GridPlannerError::OrderOutsideInstrumentLimits);
        }
    }
    Ok(())
}

impl GridPlannerInput {
    fn reference_value(&self) -> Option<Decimal> {
        if let Some(reference) = &self.reference_price {
            return Some(reference.price.value());
        }
        let book = self.book.as_ref()?;
        book.bid
            .value()
            .checked_add(book.ask.value())?
            .checked_div(Decimal::from(2))
    }
    fn price_within_limits(&self, price: Price) -> bool {
        price >= self.instrument_limits.minimum_price
            && price <= self.instrument_limits.maximum_price
            && price.value() % self.instrument.price.step == Decimal::ZERO
    }

    fn quantity_within_limits(&self, quantity: Decimal) -> bool {
        quantity >= self.instrument_limits.minimum_quantity
            && quantity <= self.instrument_limits.maximum_quantity
            && quantity % self.instrument.quantity.step == Decimal::ZERO
    }
}

fn ceil_to_step(value: Decimal, step: Decimal) -> Result<Decimal, GridPlannerError> {
    if value < Decimal::ZERO || step <= Decimal::ZERO {
        return Err(GridPlannerError::Normalization);
    }
    let remainder = value % step;
    if remainder == Decimal::ZERO {
        Ok(value)
    } else {
        value
            .checked_add(step - remainder)
            .ok_or(GridPlannerError::Arithmetic)
    }
}

fn crosses_book(order: &GridOrderIntent, book: &GridBestBook) -> bool {
    match order.side {
        OrderSide::Buy => order.price >= book.ask,
        OrderSide::Sell => order.price <= book.bid,
    }
}

const fn opposite_role(role: GridOrderRole) -> GridOrderRole {
    match role {
        GridOrderRole::Open => GridOrderRole::Close,
        GridOrderRole::Close => GridOrderRole::Open,
    }
}

fn inventory_quantity(inventory: &GridInventory, position: GridPosition) -> Decimal {
    match position {
        GridPosition::Long => inventory.long_quantity,
        GridPosition::Short => inventory.short_quantity,
    }
}

fn reserved_quantity(reservations: &GridCloseReservations, position: GridPosition) -> Decimal {
    match position {
        GridPosition::Long => reservations.long_quantity,
        GridPosition::Short => reservations.short_quantity,
    }
}

fn plan_with(input: &GridPlannerInput, directive: GridPlanDirective) -> GridPlan {
    GridPlan {
        schema_version: GRID_PLANNER_SCHEMA_VERSION,
        instance_id: input.config.instance_id.clone(),
        revision: input.config.revision,
        directive,
    }
}

fn reset_plan(input: &GridPlannerInput, trigger: GridResetTrigger) -> GridPlan {
    plan_with(
        input,
        GridPlanDirective::ResetRequired {
            trigger,
            cancel_owned_orders: true,
            keep_positions: true,
            require_fresh_facts: true,
        },
    )
}

fn blocked_plan(input: &GridPlannerInput, reason: GridBlockedReason) -> GridPlan {
    plan_with(input, GridPlanDirective::Blocked { reason })
}

#[cfg(test)]
#[path = "planner_tests.rs"]
mod tests;
