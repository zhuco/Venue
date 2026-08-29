use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{
        Amount, FieldState, Instrument, OrderCommand, OrderOwner, OrderPurpose, OrderSide,
        PositionSide, Price, Symbol,
    },
    risk::{AccountRiskView, HardRiskLimits, RiskError, authorize_entry},
};

/// Exact scope that is allowed to request a single canary preflight.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryBinding {
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
    pub owner: OrderOwner,
    pub release_id: String,
    pub position_side: PositionSide,
}

/// One signed private-scope observation. Every externally-derived field stays explicit so an
/// absent, null, or malformed value cannot silently become a safe default.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanarySnapshot {
    pub binding: CanaryBinding,
    pub observed_at_ms: u64,
    pub generation: u64,
    pub instrument_generation: FieldState<u64>,
    pub can_trade: FieldState<bool>,
    pub hedge_mode: FieldState<bool>,
    pub positions: Vec<CanaryPosition>,
    pub open_orders: FieldState<u32>,
    pub available_margin: FieldState<Amount>,
    pub owner_conflict: FieldState<bool>,
    pub execution_unknown: FieldState<bool>,
}

/// A private position remains side-tagged; `NET`, absent, or duplicated hedge legs cannot prove
/// a stable empty hedge scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanaryPosition {
    pub side: FieldState<PositionSide>,
    pub quantity: FieldState<Decimal>,
}

#[derive(Clone, Copy, Debug)]
pub struct CanaryPreflightInput<'a> {
    pub binding: &'a CanaryBinding,
    pub snapshots: &'a [CanarySnapshot],
    pub instrument: &'a Instrument,
    pub reference_price: Price,
    pub now_ms: u64,
    pub maximum_evidence_age_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanaryPreflightApproval {
    pub quantity: Decimal,
    pub notional: Amount,
    pub final_generation: u64,
    pub valid_until_ms: u64,
}

/// Validates the G6 stable-empty scope and sizes the smallest rule-compliant entry. This is a
/// pure admission proof: it owns no credentials, network handle, journal, or mutation path.
pub fn authorize_canary_preflight(
    input: CanaryPreflightInput<'_>,
) -> Result<CanaryPreflightApproval, CanaryPreflightError> {
    validate_binding(input.binding)?;
    input
        .instrument
        .validate()
        .map_err(CanaryPreflightError::Instrument)?;
    if input.instrument.symbol != input.binding.symbol {
        return Err(CanaryPreflightError::InstrumentBinding);
    }
    if input.maximum_evidence_age_ms == 0 || input.snapshots.len() < 2 {
        return Err(CanaryPreflightError::Evidence);
    }

    let mut prior_generation = None;
    let mut prior_observed_at = None;
    for snapshot in input.snapshots {
        validate_snapshot(
            snapshot,
            input.binding,
            input.instrument,
            input.now_ms,
            input.maximum_evidence_age_ms,
        )?;
        if prior_generation.is_some_and(|prior| snapshot.generation < prior)
            || prior_observed_at.is_some_and(|prior| snapshot.observed_at_ms <= prior)
        {
            return Err(CanaryPreflightError::SnapshotSequence);
        }
        prior_generation = Some(snapshot.generation);
        prior_observed_at = Some(snapshot.observed_at_ms);
    }

    let asset = input
        .instrument
        .settlement_asset
        .clone()
        .ok_or(CanaryPreflightError::Settlement)?;
    if asset.as_str() != "USDT" || input.instrument.minimum_notional.asset != asset {
        return Err(CanaryPreflightError::Settlement);
    }
    let cap = Amount::new(
        asset.clone(),
        Decimal::new(super::CANARY_MAX_ENTRY_NOTIONAL_USDT, 0),
    );
    let available_margin = match &input
        .snapshots
        .last()
        .ok_or(CanaryPreflightError::Evidence)?
        .available_margin
    {
        FieldState::Known(amount) if amount.asset == asset && !amount.value.is_sign_negative() => {
            amount.clone()
        }
        _ => return Err(CanaryPreflightError::Margin),
    };
    let quantity = minimum_compliant_quantity(input.instrument, input.reference_price)?;
    let order = OrderCommand {
        command_id: crate::domain::CommandId::new("canary-preflight")
            .map_err(CanaryPreflightError::Command)?,
        client_order_id: crate::domain::CommandId::new("canary-preflight-client")
            .map_err(CanaryPreflightError::Command)?,
        owner: input.binding.owner.clone(),
        side: entry_side(input.binding.position_side)?,
        position_side: input.binding.position_side,
        quantity,
        limit_price: input.reference_price,
        reduce_only: false,
    };
    let approval = authorize_entry(
        &order,
        input.instrument,
        &AccountRiskView {
            available_margin,
            unresolved_commands: 0,
        },
        &HardRiskLimits {
            max_entry_notional: cap,
        },
    )
    .map_err(CanaryPreflightError::Risk)?;
    Ok(CanaryPreflightApproval {
        quantity,
        notional: approval.notional,
        final_generation: prior_generation.ok_or(CanaryPreflightError::Evidence)?,
        valid_until_ms: input
            .snapshots
            .last()
            .ok_or(CanaryPreflightError::Evidence)?
            .observed_at_ms
            .checked_add(input.maximum_evidence_age_ms)
            .ok_or(CanaryPreflightError::Evidence)?,
    })
}

fn validate_binding(binding: &CanaryBinding) -> Result<(), CanaryPreflightError> {
    if binding.exchange.trim().is_empty()
        || binding.account.trim().is_empty()
        || binding.release_id.trim().is_empty()
        || binding.owner.exchange != binding.exchange
        || binding.owner.account != binding.account
        || binding.owner.symbol != binding.symbol
        || binding.owner.purpose != OrderPurpose::Entry
    {
        return Err(CanaryPreflightError::Binding);
    }
    binding
        .owner
        .validate()
        .map_err(CanaryPreflightError::Command)?;
    entry_side(binding.position_side)?;
    Ok(())
}

fn validate_snapshot(
    snapshot: &CanarySnapshot,
    binding: &CanaryBinding,
    instrument: &Instrument,
    now_ms: u64,
    maximum_age_ms: u64,
) -> Result<(), CanaryPreflightError> {
    if snapshot.binding != *binding || snapshot.generation == 0 {
        return Err(CanaryPreflightError::Binding);
    }
    if snapshot.observed_at_ms == 0 || snapshot.observed_at_ms > now_ms {
        return Err(CanaryPreflightError::Evidence);
    }
    if now_ms - snapshot.observed_at_ms > maximum_age_ms {
        return Err(CanaryPreflightError::Evidence);
    }
    if snapshot.instrument_generation != FieldState::Known(instrument.generation) {
        return Err(CanaryPreflightError::InstrumentGeneration);
    }
    if snapshot.can_trade != FieldState::Known(true)
        || snapshot.hedge_mode != FieldState::Known(true)
    {
        return Err(CanaryPreflightError::Capability);
    }
    if snapshot.open_orders != FieldState::Known(0) {
        return Err(CanaryPreflightError::OpenOrders);
    }
    let settlement_asset = instrument
        .settlement_asset
        .as_ref()
        .ok_or(CanaryPreflightError::Settlement)?;
    if !matches!(
        &snapshot.available_margin,
        FieldState::Known(amount)
            if &amount.asset == settlement_asset && !amount.value.is_sign_negative()
    ) {
        return Err(CanaryPreflightError::Margin);
    }
    if snapshot.owner_conflict != FieldState::Known(false) {
        return Err(CanaryPreflightError::OwnerConflict);
    }
    if snapshot.execution_unknown != FieldState::Known(false) {
        return Err(CanaryPreflightError::ExecutionUnknown);
    }
    validate_flat_hedge_positions(&snapshot.positions)
}

fn validate_flat_hedge_positions(positions: &[CanaryPosition]) -> Result<(), CanaryPreflightError> {
    if positions.len() != 2 {
        return Err(CanaryPreflightError::Position);
    }
    let mut long = false;
    let mut short = false;
    for position in positions {
        if position.quantity != FieldState::Known(Decimal::ZERO) {
            return Err(CanaryPreflightError::Position);
        }
        match &position.side {
            FieldState::Known(PositionSide::Long) if !long => long = true,
            FieldState::Known(PositionSide::Short) if !short => short = true,
            FieldState::Known(PositionSide::Net)
            | FieldState::Known(PositionSide::Long)
            | FieldState::Known(PositionSide::Short)
            | FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => return Err(CanaryPreflightError::Position),
        }
    }
    if long && short {
        Ok(())
    } else {
        Err(CanaryPreflightError::Position)
    }
}

fn minimum_compliant_quantity(
    instrument: &Instrument,
    reference_price: Price,
) -> Result<Decimal, CanaryPreflightError> {
    let required_quantity = instrument.minimum_notional.value / reference_price.value();
    let minimum_steps = (required_quantity / instrument.quantity_step).ceil();
    let steps = if minimum_steps < Decimal::ONE {
        Decimal::ONE
    } else {
        minimum_steps
    };
    let quantity = steps * instrument.quantity_step;
    if !quantity.is_sign_positive() || quantity.is_zero() {
        return Err(CanaryPreflightError::Quantity);
    }
    Ok(quantity)
}

fn entry_side(position_side: PositionSide) -> Result<OrderSide, CanaryPreflightError> {
    match position_side {
        PositionSide::Long => Ok(OrderSide::Buy),
        PositionSide::Short => Ok(OrderSide::Sell),
        PositionSide::Net => Err(CanaryPreflightError::Position),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CanaryPreflightError {
    #[error("canary binding is incomplete or differs from the private snapshot")]
    Binding,
    #[error("canary evidence is missing, stale, future-dated, or has only one sample")]
    Evidence,
    #[error("canary snapshot generation or timestamp regressed")]
    SnapshotSequence,
    #[error("private trading or hedge-mode capability is not positively known")]
    Capability,
    #[error("the scoped hedge positions are not exactly flat LONG and SHORT legs")]
    Position,
    #[error("the scope has open orders or lacks an authoritative order count")]
    OpenOrders,
    #[error("available margin is absent, invalid, or not denominated in the settlement asset")]
    Margin,
    #[error("the scope has an owner conflict or owner-conflict status is unknown")]
    OwnerConflict,
    #[error("the scope has unknown execution state or its status is unknown")]
    ExecutionUnknown,
    #[error("instrument is invalid: {0}")]
    Instrument(crate::domain::InstrumentError),
    #[error("instrument does not match the requested canary binding")]
    InstrumentBinding,
    #[error("private snapshot instrument generation is absent or inconsistent")]
    InstrumentGeneration,
    #[error("canary requires a USDT-settled instrument with matching minimum-notional asset")]
    Settlement,
    #[error("minimum compliant quantity is invalid")]
    Quantity,
    #[error("canary command identity is invalid: {0}")]
    Command(crate::domain::CommandError),
    #[error("minimum canary order violates normalized risk: {0}")]
    Risk(RiskError),
}
