use sha2::{Digest, Sha256};

use crate::{
    domain::{
        FieldState, Position, PositionSide, StopMarketCloseAllCommand,
        StopMarketFullPositionCommand,
    },
    exchange::binance_private::{
        AlgoOrderReadback, ConditionalStrategyReadback, ConditionalStrategyStatus,
    },
};

use super::WriterSession;

/// Private readback fencing paired with the conditional-strategy observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtectionEvidence {
    pub private_generation: u64,
    pub readback_generation: u64,
    pub valid_until_ms: u64,
    pub observed_at_ms: u64,
}

/// A protected predecessor may only produce this protection/stop proof when its writer is
/// explicitly constrained to protection-only work. It cannot obtain a normal writer permit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CustodyWriterRole {
    pub predecessor_protected: bool,
    pub protection_only: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct ProtectionCustodyInput<'a> {
    pub command: &'a StopMarketCloseAllCommand,
    pub position: &'a Position,
    pub strategy: &'a ConditionalStrategyReadback,
    pub writer: &'a WriterSession,
    pub evidence: ProtectionEvidence,
    pub writer_role: CustodyWriterRole,
    pub now_ms: u64,
}

/// A custody proof is intentionally not an entry permit. It only proves the exchange-resident
/// close-all STOP_MARKET protection that already covers the full authoritative hedge leg.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectionCustody {
    pub command_id: String,
    pub client_strategy_id: String,
    pub venue_strategy_id: String,
    pub symbol: crate::domain::Symbol,
    pub position_side: PositionSide,
    pub full_position_quantity: rust_decimal::Decimal,
    pub private_generation: u64,
    pub writer_generation: u64,
    pub valid_until_ms: u64,
    pub content_sha256: String,
}

#[derive(Clone, Copy, Debug)]
pub struct AlgoProtectionCustodyInput<'a> {
    pub command: &'a StopMarketFullPositionCommand,
    pub position: &'a Position,
    pub algo: &'a AlgoOrderReadback,
    pub writer: &'a WriterSession,
    pub evidence: ProtectionEvidence,
    pub writer_role: CustodyWriterRole,
    pub now_ms: u64,
}

/// Exact proof for the current quantity-bound PAPI UM Algo STOP_MARKET family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlgoProtectionCustody {
    pub command_id: String,
    pub client_algo_id: String,
    pub venue_algo_id: String,
    pub symbol: crate::domain::Symbol,
    pub position_side: PositionSide,
    pub full_position_quantity: rust_decimal::Decimal,
    pub private_generation: u64,
    pub writer_generation: u64,
    pub valid_until_ms: u64,
    pub content_sha256: String,
}

impl AlgoProtectionCustody {
    pub const fn permits_entry(&self) -> bool {
        false
    }

    pub const fn permits_protection_or_stop(&self) -> bool {
        true
    }
}

impl ProtectionCustody {
    pub const fn permits_entry(&self) -> bool {
        false
    }

    pub const fn permits_protection_or_stop(&self) -> bool {
        true
    }
}

/// Validates the exact stage-4 conditional protection readback. This has no exchange client or
/// mutation capability; it is a pure proof over already authoritative inputs.
pub fn prove_protection_custody(
    input: ProtectionCustodyInput<'_>,
) -> Result<ProtectionCustody, ProtectionCustodyError> {
    input
        .command
        .validate()
        .map_err(ProtectionCustodyError::Command)?;
    validate_writer(input.writer, input.command)?;
    validate_evidence(input.evidence, input.writer, input.command, input.now_ms)?;
    validate_position(input.position, input.command)?;
    validate_strategy(input.strategy, input.command)?;
    if input.writer_role.predecessor_protected && !input.writer_role.protection_only {
        return Err(ProtectionCustodyError::ProtectedPredecessor);
    }

    let valid_until_ms = input
        .writer
        .valid_until_ms
        .min(input.evidence.valid_until_ms);
    let custody = ProtectionCustody {
        command_id: input.command.command_id.as_str().to_owned(),
        client_strategy_id: input.command.client_strategy_id.as_str().to_owned(),
        venue_strategy_id: input.strategy.strategy_id.clone(),
        symbol: input.command.owner.symbol.clone(),
        position_side: input.command.position_side,
        full_position_quantity: input.position.quantity,
        private_generation: input.evidence.private_generation,
        writer_generation: input.writer.generation,
        valid_until_ms,
        content_sha256: String::new(),
    };
    Ok(ProtectionCustody {
        content_sha256: content_summary(&custody),
        ..custody
    })
}

pub fn prove_algo_protection_custody(
    input: AlgoProtectionCustodyInput<'_>,
) -> Result<AlgoProtectionCustody, ProtectionCustodyError> {
    input
        .command
        .validate()
        .map_err(ProtectionCustodyError::Command)?;
    validate_algo_writer(input.writer, input.command)?;
    validate_algo_evidence(input.evidence, input.writer, input.command, input.now_ms)?;
    validate_algo_position(input.position, input.command)?;
    validate_algo_readback(input.algo, input.command)?;
    if input.writer_role.predecessor_protected && !input.writer_role.protection_only {
        return Err(ProtectionCustodyError::ProtectedPredecessor);
    }
    // The entry lease may expire while Binance private readback is in flight. Algo custody cannot
    // permit entry, so its freshness follows the exact private evidence rather than extending
    // or depending on the predecessor's mutation lease.
    let valid_until_ms = input.evidence.valid_until_ms;
    let custody = AlgoProtectionCustody {
        command_id: input.command.command_id.as_str().to_owned(),
        client_algo_id: input.command.client_algo_id.as_str().to_owned(),
        venue_algo_id: input.algo.algo_id.clone(),
        symbol: input.command.owner.symbol.clone(),
        position_side: input.command.position_side,
        full_position_quantity: input.command.quantity,
        private_generation: input.evidence.private_generation,
        writer_generation: input.writer.generation,
        valid_until_ms,
        content_sha256: String::new(),
    };
    Ok(AlgoProtectionCustody {
        content_sha256: algo_content_summary(&custody),
        ..custody
    })
}

fn validate_algo_writer(
    writer: &WriterSession,
    command: &StopMarketFullPositionCommand,
) -> Result<(), ProtectionCustodyError> {
    if writer.scope.exchange != command.owner.exchange
        || writer.scope.account != command.owner.account
        || writer.scope.symbol != command.owner.symbol
        || writer.generation == 0
        || writer.revision == 0
        || writer.readback_generation == 0
        || writer.token.trim().is_empty()
    {
        return Err(ProtectionCustodyError::Writer);
    }
    Ok(())
}

fn validate_algo_evidence(
    evidence: ProtectionEvidence,
    writer: &WriterSession,
    command: &StopMarketFullPositionCommand,
    now_ms: u64,
) -> Result<(), ProtectionCustodyError> {
    if evidence.private_generation == 0
        || evidence.readback_generation == 0
        || evidence.readback_generation > evidence.private_generation
        || command.position_generation != writer.readback_generation
        || evidence.readback_generation < command.position_generation
        || evidence.observed_at_ms == 0
        || evidence.observed_at_ms > now_ms
        || evidence.valid_until_ms <= now_ms
    {
        return Err(ProtectionCustodyError::Evidence);
    }
    Ok(())
}

fn validate_algo_position(
    position: &Position,
    command: &StopMarketFullPositionCommand,
) -> Result<(), ProtectionCustodyError> {
    if position.symbol != command.owner.symbol
        || position.side != command.position_side
        || !matches!(position.side, PositionSide::Long | PositionSide::Short)
        || !position.quantity.is_sign_positive()
        || position.quantity.is_zero()
        || position.quantity != command.quantity
    {
        return Err(ProtectionCustodyError::Position);
    }
    Ok(())
}

fn validate_algo_readback(
    algo: &AlgoOrderReadback,
    command: &StopMarketFullPositionCommand,
) -> Result<(), ProtectionCustodyError> {
    if algo.algo_id.trim().is_empty()
        || algo.client_algo_id != command.client_algo_id.as_str()
        || algo.status != ConditionalStrategyStatus::Current
        || algo.order_type != FieldState::Known("STOP_MARKET".to_owned())
        || algo.side != FieldState::Known(command.side)
        || algo.position_side != FieldState::Known(command.position_side)
        || algo.quantity != FieldState::Known(command.quantity)
        || algo.trigger_price != FieldState::Known(command.trigger_price)
        || algo.working_type != FieldState::Known("MARK_PRICE".to_owned())
        || !matches!(algo.close_position, FieldState::Missing | FieldState::Null)
        || algo.reduce_only != FieldState::Known(true)
    {
        return Err(ProtectionCustodyError::Strategy);
    }
    Ok(())
}

fn algo_content_summary(custody: &AlgoProtectionCustody) -> String {
    let material = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        custody.command_id,
        custody.client_algo_id,
        custody.venue_algo_id,
        custody.symbol,
        custody.position_side as u8,
        custody.full_position_quantity,
        custody.private_generation,
        custody.writer_generation,
        custody.valid_until_ms,
        "stage4_algo_stop_market_full_position",
    );
    format!("{:x}", Sha256::digest(material.as_bytes()))
}

fn validate_writer(
    writer: &WriterSession,
    command: &StopMarketCloseAllCommand,
) -> Result<(), ProtectionCustodyError> {
    if writer.scope.exchange != command.owner.exchange
        || writer.scope.account != command.owner.account
        || writer.scope.symbol != command.owner.symbol
        || writer.generation == 0
        || writer.revision == 0
        || writer.readback_generation == 0
        || writer.token.trim().is_empty()
    {
        return Err(ProtectionCustodyError::Writer);
    }
    Ok(())
}

fn validate_evidence(
    evidence: ProtectionEvidence,
    writer: &WriterSession,
    command: &StopMarketCloseAllCommand,
    now_ms: u64,
) -> Result<(), ProtectionCustodyError> {
    if evidence.private_generation == 0
        || evidence.readback_generation == 0
        || evidence.readback_generation > evidence.private_generation
        || evidence.readback_generation != command.position_generation
        || evidence.readback_generation != writer.readback_generation
        || evidence.observed_at_ms == 0
        || evidence.observed_at_ms > now_ms
        || evidence.valid_until_ms <= now_ms
        || writer.valid_until_ms <= now_ms
    {
        return Err(ProtectionCustodyError::Evidence);
    }
    Ok(())
}

fn validate_position(
    position: &Position,
    command: &StopMarketCloseAllCommand,
) -> Result<(), ProtectionCustodyError> {
    if position.symbol != command.owner.symbol
        || position.side != command.position_side
        || !matches!(position.side, PositionSide::Long | PositionSide::Short)
        || !position.quantity.is_sign_positive()
        || position.quantity.is_zero()
    {
        return Err(ProtectionCustodyError::Position);
    }
    Ok(())
}

fn validate_strategy(
    strategy: &ConditionalStrategyReadback,
    command: &StopMarketCloseAllCommand,
) -> Result<(), ProtectionCustodyError> {
    if strategy.strategy_id.trim().is_empty()
        || strategy.status != ConditionalStrategyStatus::Current
        || strategy.side != FieldState::Known(command.side)
        || strategy.position_side != FieldState::Known(command.position_side)
        || strategy.stop_price != FieldState::Known(command.stop_price)
        || strategy.close_position != FieldState::Known(true)
    {
        return Err(ProtectionCustodyError::Strategy);
    }
    Ok(())
}

fn content_summary(custody: &ProtectionCustody) -> String {
    let material = format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        custody.command_id,
        custody.client_strategy_id,
        custody.venue_strategy_id,
        custody.symbol,
        custody.position_side as u8,
        custody.full_position_quantity,
        custody.private_generation,
        custody.writer_generation,
        custody.valid_until_ms,
        "stage4_stop_market_close_position",
    );
    format!("{:x}", Sha256::digest(material.as_bytes()))
}

#[derive(Debug, thiserror::Error)]
pub enum ProtectionCustodyError {
    #[error("stop-market close-all command is invalid: {0}")]
    Command(crate::domain::CommandError),
    #[error("writer session does not exactly bind the command scope")]
    Writer,
    #[error("private/readback generation or freshness evidence is invalid")]
    Evidence,
    #[error("authoritative hedge position is empty, net, or mismatched")]
    Position,
    #[error("conditional strategy is not the exact current close-all stop")]
    Strategy,
    #[error("a protected predecessor needs a protection-only writer role")]
    ProtectedPredecessor,
}
