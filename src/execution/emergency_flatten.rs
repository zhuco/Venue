use crate::{
    domain::{
        Amount, CommandId, Instrument, OrderCommand, OrderOwner, OrderPurpose, OrderSide, Position,
        PositionSide, Price,
    },
    risk::authorize_reduction,
};

use super::{CanaryEvidenceBinding, WriterSession, gate::command_fingerprint};

/// An emergency close proof is intentionally much shorter than a normal writer lease. It is a
/// semantic authorization only; a later exchange adapter must independently prove that its Hedge
/// reduce-only wire form is supported before any dispatch is enabled.
pub const EMERGENCY_FLATTEN_PERMIT_TTL_MS: u64 = 500;

/// Read-only state captured while the dispatch guard is held by the single writer. The guard is
/// deliberately not represented here, so this module cannot obtain a mutation handle itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EmergencyDispatchState {
    pub now_ms: u64,
    pub private_generation: u64,
    pub readback_generation: u64,
    pub position_generation: u64,
    pub private_readback_valid_until_ms: u64,
    pub reconciliation_clean: bool,
    pub reconciliation_valid_until_ms: u64,
    /// UNKNOWN protection/cancel records do not block risk reduction; only an unresolved entry
    /// or reduction can make another full-size reduction ambiguous.
    pub entry_or_reduce_wal_clean: bool,
    pub filled_at_ms: u64,
    pub unprotected_deadline_ms: u64,
    pub dispatch_writer_generation: u64,
    pub dispatch_writer_revision: u64,
}

/// The immutable hard limits which governed the Canary are rechecked but never used to reduce
/// the close quantity: flattening a full authoritative position must not be blocked by movement
/// in its current notional.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmergencyRiskEnvelope {
    pub quote_cap: Amount,
    pub risk_cap: Amount,
    pub valid_until_ms: u64,
}

/// Inputs necessary to turn one already-authoritative non-flat hedge position into a semantic
/// full reduction. There is no entry quantity, exchange client, journal, or dispatch guard.
#[derive(Clone, Copy, Debug)]
pub struct EmergencyFlattenInput<'a> {
    pub binding: &'a CanaryEvidenceBinding,
    pub authoritative_position: &'a Position,
    pub writer: &'a WriterSession,
    pub dispatch: EmergencyDispatchState,
    pub instrument: &'a Instrument,
    pub market_price: Price,
    pub market_price_valid_until_ms: u64,
    pub risk: &'a EmergencyRiskEnvelope,
    pub command_id: &'a CommandId,
    pub client_order_id: &'a CommandId,
    pub owner: &'a OrderOwner,
}

/// The only output is a full semantic reduction plus a command-bound, 500 ms-or-less permit.
/// No conversion to a REST request exists in this module.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmergencyFlattenAuthorization {
    pub command: OrderCommand,
    permit: EmergencyFlattenPermit,
}

impl EmergencyFlattenAuthorization {
    pub const fn permit(&self) -> EmergencyFlattenPermit {
        self.permit
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EmergencyFlattenPermit {
    command_sha256: [u8; 32],
    valid_until_ms: u64,
}

impl EmergencyFlattenPermit {
    pub const fn valid_until_ms(self) -> u64 {
        self.valid_until_ms
    }
}

pub fn authorize_emergency_flatten(
    input: EmergencyFlattenInput<'_>,
) -> Result<EmergencyFlattenAuthorization, EmergencyFlattenError> {
    validate_binding(input.binding)?;
    validate_writer(input.writer, input.binding, input.dispatch)?;
    validate_dispatch(input.dispatch, input.writer)?;
    validate_risk(input.risk, input.binding, input.dispatch.now_ms)?;
    validate_position(input.authoritative_position, input.binding)?;
    validate_instrument_and_price(
        input.instrument,
        input.market_price,
        input.market_price_valid_until_ms,
        input.binding,
        input.dispatch.now_ms,
    )?;
    validate_owner(input.owner, input.binding)?;

    let command = OrderCommand {
        time_in_force: Default::default(),
        command_id: input.command_id.clone(),
        client_order_id: input.client_order_id.clone(),
        owner: input.owner.clone(),
        side: close_side(input.authoritative_position.side)?,
        position_side: input.authoritative_position.side,
        quantity: input.authoritative_position.quantity,
        limit_price: input.market_price,
        reduce_only: true,
    };
    authorize_reduction(&command, input.instrument, input.authoritative_position)
        .map_err(EmergencyFlattenError::Risk)?;
    let ttl_valid_until_ms = input
        .dispatch
        .now_ms
        .checked_add(EMERGENCY_FLATTEN_PERMIT_TTL_MS)
        .ok_or(EmergencyFlattenError::Permit)?;
    let valid_until_ms = [
        input.binding.valid_until_ms,
        input.writer.valid_until_ms,
        input.dispatch.private_readback_valid_until_ms,
        input.dispatch.reconciliation_valid_until_ms,
        input.market_price_valid_until_ms,
        input.risk.valid_until_ms,
        ttl_valid_until_ms,
    ]
    .into_iter()
    .min()
    .ok_or(EmergencyFlattenError::Permit)?;
    if valid_until_ms <= input.dispatch.now_ms {
        return Err(EmergencyFlattenError::Expired);
    }
    Ok(EmergencyFlattenAuthorization {
        permit: EmergencyFlattenPermit {
            command_sha256: command_fingerprint(&command),
            valid_until_ms,
        },
        command,
    })
}

/// Rechecks the exact full reduction at the WAL boundary. This does not send it to an exchange.
pub fn validate_emergency_flatten_permit(
    permit: EmergencyFlattenPermit,
    command: &OrderCommand,
    now_ms: u64,
) -> Result<(), EmergencyFlattenError> {
    if permit.command_sha256 != command_fingerprint(command) {
        return Err(EmergencyFlattenError::CommandFingerprint);
    }
    if permit.valid_until_ms <= now_ms {
        return Err(EmergencyFlattenError::PermitExpired);
    }
    Ok(())
}

fn validate_binding(binding: &CanaryEvidenceBinding) -> Result<(), EmergencyFlattenError> {
    if binding.canary_id.trim().is_empty()
        || binding.exchange.trim().is_empty()
        || binding.account.trim().is_empty()
        || binding.owner_scope.trim().is_empty()
        || binding.release_id.trim().is_empty()
        || !matches!(
            binding.position_side,
            PositionSide::Long | PositionSide::Short
        )
        || binding.quote_cap.asset.as_str() != "USDT"
        || binding.quote_cap.asset != binding.risk_cap.asset
        || binding.symbol.quote() != binding.quote_cap.asset.as_str()
        || !positive_within_canary_cap(binding.quote_cap.value)
        || !positive_within_canary_cap(binding.risk_cap.value)
        || binding.risk_cap.value > binding.quote_cap.value
    {
        return Err(EmergencyFlattenError::Binding);
    }
    Ok(())
}

fn validate_writer(
    writer: &WriterSession,
    binding: &CanaryEvidenceBinding,
    dispatch: EmergencyDispatchState,
) -> Result<(), EmergencyFlattenError> {
    if writer.scope.exchange != binding.exchange
        || writer.scope.account != binding.account
        || writer.scope.symbol != binding.symbol
        || writer.scope.owner_scope != binding.owner_scope
        || writer.token.trim().is_empty()
        || writer.generation == 0
        || writer.revision == 0
        || writer.readback_generation == 0
        || writer.valid_until_ms <= dispatch.now_ms
    {
        return Err(EmergencyFlattenError::Writer);
    }
    Ok(())
}

fn validate_dispatch(
    dispatch: EmergencyDispatchState,
    writer: &WriterSession,
) -> Result<(), EmergencyFlattenError> {
    if dispatch.private_generation == 0
        || dispatch.private_generation != dispatch.readback_generation
        || dispatch.readback_generation != writer.readback_generation
        || dispatch.position_generation != writer.readback_generation
        || dispatch.dispatch_writer_generation != writer.generation
        || dispatch.dispatch_writer_revision != writer.revision
    {
        return Err(EmergencyFlattenError::Generation);
    }
    if !dispatch.entry_or_reduce_wal_clean {
        return Err(EmergencyFlattenError::CommandWal);
    }
    if !dispatch.reconciliation_clean {
        return Err(EmergencyFlattenError::Reconciliation);
    }
    if dispatch.private_readback_valid_until_ms <= dispatch.now_ms
        || dispatch.reconciliation_valid_until_ms <= dispatch.now_ms
    {
        return Err(EmergencyFlattenError::Expired);
    }
    if dispatch.filled_at_ms == 0
        || dispatch.now_ms < dispatch.filled_at_ms
        || dispatch.unprotected_deadline_ms
            != dispatch
                .filled_at_ms
                .checked_add(super::MAX_UNPROTECTED_MS)
                .ok_or(EmergencyFlattenError::Deadline)?
    {
        return Err(EmergencyFlattenError::Deadline);
    }
    Ok(())
}

fn validate_risk(
    risk: &EmergencyRiskEnvelope,
    binding: &CanaryEvidenceBinding,
    now_ms: u64,
) -> Result<(), EmergencyFlattenError> {
    if risk.quote_cap != binding.quote_cap
        || risk.risk_cap != binding.risk_cap
        || risk.valid_until_ms <= now_ms
    {
        return Err(EmergencyFlattenError::RiskEnvelope);
    }
    Ok(())
}

fn validate_position(
    position: &Position,
    binding: &CanaryEvidenceBinding,
) -> Result<(), EmergencyFlattenError> {
    if position.symbol != binding.symbol
        || position.side != binding.position_side
        || !matches!(position.side, PositionSide::Long | PositionSide::Short)
        || !position.quantity.is_sign_positive()
        || position.quantity.is_zero()
    {
        return Err(EmergencyFlattenError::Position);
    }
    Ok(())
}

fn validate_instrument_and_price(
    instrument: &Instrument,
    market_price: Price,
    market_price_valid_until_ms: u64,
    binding: &CanaryEvidenceBinding,
    now_ms: u64,
) -> Result<(), EmergencyFlattenError> {
    instrument
        .validate()
        .map_err(EmergencyFlattenError::Instrument)?;
    if instrument.symbol != binding.symbol
        || instrument
            .settlement_asset
            .as_ref()
            .is_none_or(|asset| asset.as_str() != "USDT")
        || instrument.minimum_notional.asset.as_str() != "USDT"
    {
        return Err(EmergencyFlattenError::InstrumentBinding);
    }
    if market_price_valid_until_ms <= now_ms {
        return Err(EmergencyFlattenError::MarketPrice);
    }
    if (market_price.value() % instrument.price_tick.value()) != rust_decimal::Decimal::ZERO {
        return Err(EmergencyFlattenError::MarketPrice);
    }
    Ok(())
}

fn validate_owner(
    owner: &OrderOwner,
    binding: &CanaryEvidenceBinding,
) -> Result<(), EmergencyFlattenError> {
    owner.validate().map_err(|_| EmergencyFlattenError::Owner)?;
    if owner.exchange != binding.exchange
        || owner.account != binding.account
        || owner.symbol != binding.symbol
        || owner.purpose != OrderPurpose::Reduce
    {
        return Err(EmergencyFlattenError::Owner);
    }
    Ok(())
}

fn close_side(position_side: PositionSide) -> Result<OrderSide, EmergencyFlattenError> {
    match position_side {
        PositionSide::Long => Ok(OrderSide::Sell),
        PositionSide::Short => Ok(OrderSide::Buy),
        PositionSide::Net => Err(EmergencyFlattenError::Position),
    }
}

fn positive_within_canary_cap(value: rust_decimal::Decimal) -> bool {
    value.is_sign_positive()
        && !value.is_zero()
        && value <= rust_decimal::Decimal::new(super::CANARY_MAX_ENTRY_NOTIONAL_USDT, 0)
}

#[derive(Debug, thiserror::Error)]
pub enum EmergencyFlattenError {
    #[error("Canary binding is invalid or exceeds the 10 USDT envelope")]
    Binding,
    #[error("writer session or exact writer scope is invalid")]
    Writer,
    #[error("private, readback, position, or writer generation is inconsistent")]
    Generation,
    #[error("execution WAL has an unresolved entry or reduction command")]
    CommandWal,
    #[error("reconciliation is not clean")]
    Reconciliation,
    #[error("required emergency evidence is expired")]
    Expired,
    #[error("unprotected deadline is absent or not exactly 1500 ms after the fill")]
    Deadline,
    #[error("risk envelope is absent, expired, or differs from the Canary binding")]
    RiskEnvelope,
    #[error("authoritative position is not the exact non-flat Hedge side")]
    Position,
    #[error("instrument scope or rules are invalid")]
    Instrument(crate::domain::InstrumentError),
    #[error("instrument does not match the USDT Canary binding")]
    InstrumentBinding,
    #[error("market price is stale or violates the instrument tick")]
    MarketPrice,
    #[error("reduction owner is invalid or out of scope")]
    Owner,
    #[error("constructed reduction violates normalized hard risk: {0}")]
    Risk(crate::risk::RiskError),
    #[error("emergency permit cannot be constructed")]
    Permit,
    #[error("emergency permit does not bind this exact reduction command")]
    CommandFingerprint,
    #[error("emergency permit is expired")]
    PermitExpired,
}
