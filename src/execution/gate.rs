use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    domain::{Amount, OrderCommand, OrderPurpose, OrderSide, PositionSide},
    exchange::private_session::PrivateSessionState,
};

pub const CANARY_MAX_ENTRY_NOTIONAL_USDT: i64 = 10;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RunMode {
    Shadow,
    Canary,
    Live,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    InstrumentRules,
    PublicMarket,
    PrivateReadback,
    PrivateStream,
    PlaceLimit,
    Cancel,
    ReduceOnly,
    Reconciliation,
    /// A bound low-balance 3×3 grid proved its replenishment, rolling fill, restart and cleanup.
    GridLifecycle,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CapabilityEvidence {
    pub evidence_hash: String,
    pub generation: u64,
    pub verified_at_ms: u64,
    pub valid_until_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateInput {
    pub mode: RunMode,
    pub now_ms: u64,
    pub capabilities: BTreeMap<Capability, CapabilityEvidence>,
    pub private_session: PrivateSessionState,
    pub private_generation: u64,
    pub readback_generation: u64,
    pub private_readback_valid_until_ms: u64,
    pub instrument_generation: u64,
    pub binding_instrument_generation: u64,
    pub instrument_valid_until_ms: u64,
    pub account_readback_fresh: bool,
    pub reconciliation_clean: bool,
    pub reconciliation_valid_until_ms: u64,
    pub command_wal_clean: bool,
    pub single_writer: bool,
    pub writer_lease_valid_until_ms: u64,
    pub protection_verified: bool,
    pub protection_valid_until_ms: u64,
    pub max_entry_notional: Amount,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateDecision {
    ShadowOnly,
    CanaryPermit {
        command_sha256: [u8; 32],
        valid_until_ms: u64,
    },
}

/// Rechecks a command-bound gate decision immediately before a mutation is journaled.
/// The permit expiry is capped by all evidence used to issue it; a later disconnect must obtain
/// a new gate decision after its signed readback.
/// A Shadow decision is deliberately not convertible to a mutation permit.
pub fn validate_canary_permit(
    decision: &GateDecision,
    command: &OrderCommand,
    now_ms: u64,
) -> Result<(), GateError> {
    match decision {
        GateDecision::ShadowOnly => Err(GateError::Shadow),
        GateDecision::CanaryPermit {
            command_sha256,
            valid_until_ms,
        } if *command_sha256 != command_fingerprint(command) => Err(GateError::CommandFingerprint),
        GateDecision::CanaryPermit { valid_until_ms, .. } if *valid_until_ms > now_ms => Ok(()),
        GateDecision::CanaryPermit { .. } => Err(GateError::PermitExpired),
    }
}

pub fn evaluate_gate(input: &GateInput, command: &OrderCommand) -> Result<GateDecision, GateError> {
    if input.mode == RunMode::Shadow {
        return Ok(GateDecision::ShadowOnly);
    }
    if input.mode == RunMode::Live {
        return Err(GateError::LiveReleaseUnavailable);
    }
    for capability in required_capabilities() {
        let evidence = input
            .capabilities
            .get(&capability)
            .ok_or(GateError::Capability(capability))?;
        if evidence.evidence_hash.is_empty()
            || evidence.generation == 0
            || evidence.verified_at_ms == 0
            || evidence.valid_until_ms <= input.now_ms
        {
            return Err(GateError::Capability(capability));
        }
    }
    if input.private_session != PrivateSessionState::Ready || !input.account_readback_fresh {
        return Err(GateError::PrivateReadback);
    }
    if input.private_readback_valid_until_ms <= input.now_ms {
        return Err(GateError::PrivateReadbackExpired);
    }
    if input.private_generation == 0
        || input.private_generation != input.readback_generation
        || input.instrument_generation == 0
        || input.instrument_generation != input.binding_instrument_generation
    {
        return Err(GateError::Generation);
    }
    if !input.reconciliation_clean {
        return Err(GateError::Reconciliation);
    }
    if input.reconciliation_valid_until_ms <= input.now_ms {
        return Err(GateError::ReconciliationExpired);
    }
    if !input.command_wal_clean {
        return Err(GateError::CommandWal);
    }
    if !input.single_writer {
        return Err(GateError::Writer);
    }
    if input.writer_lease_valid_until_ms <= input.now_ms {
        return Err(GateError::WriterLeaseExpired);
    }
    if !input.protection_verified {
        return Err(GateError::Protection);
    }
    if input.protection_valid_until_ms <= input.now_ms {
        return Err(GateError::ProtectionExpired);
    }
    if input.instrument_valid_until_ms <= input.now_ms {
        return Err(GateError::InstrumentExpired);
    }
    if input.max_entry_notional.asset.as_str() != "USDT"
        || !input.max_entry_notional.value.is_sign_positive()
        || input.max_entry_notional.value.is_zero()
        || input.max_entry_notional.value
            > rust_decimal::Decimal::new(CANARY_MAX_ENTRY_NOTIONAL_USDT, 0)
    {
        return Err(GateError::CanaryNotional);
    }
    let capability_valid_until_ms = input
        .capabilities
        .values()
        .map(|evidence| evidence.valid_until_ms)
        .min()
        .ok_or(GateError::Capability(Capability::InstrumentRules))?;
    let valid_until_ms = [
        capability_valid_until_ms,
        input.private_readback_valid_until_ms,
        input.instrument_valid_until_ms,
        input.reconciliation_valid_until_ms,
        input.writer_lease_valid_until_ms,
        input.protection_valid_until_ms,
    ]
    .into_iter()
    .min()
    .ok_or(GateError::Capability(Capability::InstrumentRules))?;
    Ok(GateDecision::CanaryPermit {
        command_sha256: command_fingerprint(command),
        valid_until_ms,
    })
}

/// A versioned, length-delimited canonical encoding prevents ambiguous concatenation and avoids
/// floating-point serialization. Decimal scale is normalized because it has no venue meaning.
pub(super) fn command_fingerprint(command: &OrderCommand) -> [u8; 32] {
    let mut canonical = b"venue.canary.order.v2\0".to_vec();
    for field in [
        command.command_id.as_str().as_bytes(),
        command.client_order_id.as_str().as_bytes(),
        command.owner.strategy_instance_id.as_bytes(),
        command.owner.run_id.as_bytes(),
        command.owner.exchange.as_bytes(),
        command.owner.account.as_bytes(),
        command.owner.symbol.base().as_bytes(),
        command.owner.symbol.quote().as_bytes(),
        purpose_name(command.owner.purpose).as_bytes(),
        side_name(command.side).as_bytes(),
        position_side_name(command.position_side).as_bytes(),
    ] {
        append_length_delimited(&mut canonical, field);
    }
    append_length_delimited(
        &mut canonical,
        command.quantity.clone().normalize().to_string().as_bytes(),
    );
    append_length_delimited(
        &mut canonical,
        command
            .limit_price
            .value()
            .normalize()
            .to_string()
            .as_bytes(),
    );
    canonical.push(u8::from(command.reduce_only));
    Sha256::digest(canonical).into()
}

fn append_length_delimited(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

const fn purpose_name(value: OrderPurpose) -> &'static str {
    match value {
        OrderPurpose::Entry => "entry",
        OrderPurpose::Protection => "protection",
        OrderPurpose::TakeProfit => "take_profit",
        OrderPurpose::Reduce => "reduce",
        OrderPurpose::ExposureTakeProfit => "exposure_take_profit",
    }
}

const fn side_name(value: OrderSide) -> &'static str {
    match value {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

const fn position_side_name(value: PositionSide) -> &'static str {
    match value {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
        PositionSide::Net => "net",
    }
}

fn required_capabilities() -> [Capability; 8] {
    [
        Capability::InstrumentRules,
        Capability::PublicMarket,
        Capability::PrivateReadback,
        Capability::PrivateStream,
        Capability::PlaceLimit,
        Capability::Cancel,
        Capability::ReduceOnly,
        Capability::Reconciliation,
    ]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GateError {
    #[error("capability {0:?} is absent, unverified, or expired")]
    Capability(Capability),
    #[error("private session or signed account readback is not ready")]
    PrivateReadback,
    #[error("private signed readback evidence expired")]
    PrivateReadbackExpired,
    #[error("reconciliation is not clean")]
    Reconciliation,
    #[error("reconciliation evidence expired")]
    ReconciliationExpired,
    #[error("execution WAL has a prepared, submitted, or unknown command")]
    CommandWal,
    #[error("more than one mutation writer is active")]
    Writer,
    #[error("single-writer lease expired")]
    WriterLeaseExpired,
    #[error("reduce-only protection is not verified")]
    Protection,
    #[error("protection verification evidence expired")]
    ProtectionExpired,
    #[error("private readback or instrument generation does not match the active binding")]
    Generation,
    #[error("instrument rule evidence expired")]
    InstrumentExpired,
    #[error(
        "long-running Live requires a parameter release and strategy lifecycle, unavailable in stage 4"
    )]
    LiveReleaseUnavailable,
    #[error("canary entry notional must be positive USDT and no more than 10")]
    CanaryNotional,
    #[error("the canary permit expired before the command could be journaled")]
    PermitExpired,
    #[error("the canary permit was issued for a different order command")]
    CommandFingerprint,
    #[error("Shadow mode cannot authorize a mutation")]
    Shadow,
}

#[cfg(test)]
mod tests {
    use crate::{
        domain::{
            Amount, Asset, CommandId, OrderCommand, OrderOwner, OrderPurpose, OrderSide,
            PositionSide, Price,
        },
        exchange::private_session::PrivateSessionState,
    };

    use super::*;

    fn canary() -> Result<GateInput, Box<dyn std::error::Error>> {
        let capabilities = required_capabilities()
            .into_iter()
            .map(|capability| {
                (
                    capability,
                    CapabilityEvidence {
                        evidence_hash: format!("evidence_{capability:?}"),
                        generation: 1,
                        verified_at_ms: 1,
                        valid_until_ms: 10,
                    },
                )
            })
            .collect();
        Ok(GateInput {
            mode: RunMode::Canary,
            now_ms: 2,
            capabilities,
            private_session: PrivateSessionState::Ready,
            private_generation: 1,
            readback_generation: 1,
            private_readback_valid_until_ms: 8,
            instrument_generation: 1,
            binding_instrument_generation: 1,
            instrument_valid_until_ms: 9,
            account_readback_fresh: true,
            reconciliation_clean: true,
            reconciliation_valid_until_ms: 7,
            command_wal_clean: true,
            single_writer: true,
            writer_lease_valid_until_ms: 6,
            protection_verified: true,
            protection_valid_until_ms: 5,
            max_entry_notional: Amount::new(
                "USDT".parse::<Asset>()?,
                rust_decimal::Decimal::new(5, 0),
            ),
        })
    }

    fn command() -> Result<OrderCommand, Box<dyn std::error::Error>> {
        Ok(OrderCommand {
            command_id: CommandId::new("canary_1")?,
            client_order_id: CommandId::new("venue_canary_1")?,
            owner: OrderOwner {
                strategy_instance_id: "scalping_1".to_owned(),
                run_id: "canary_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: rust_decimal::Decimal::new(50, 0),
            limit_price: Price::new(rust_decimal::Decimal::new(1, 1))?,
            reduce_only: false,
        })
    }

    #[test]
    fn canary_requires_every_independent_evidence() -> Result<(), Box<dyn std::error::Error>> {
        let input = canary()?;
        let command = command()?;
        assert!(matches!(
            evaluate_gate(&input, &command)?,
            GateDecision::CanaryPermit {
                valid_until_ms: 5,
                ..
            }
        ));
        let mut missing = input;
        missing.capabilities.remove(&Capability::Cancel);
        assert!(matches!(
            evaluate_gate(&missing, &command),
            Err(GateError::Capability(Capability::Cancel))
        ));
        Ok(())
    }

    #[test]
    fn shadow_never_becomes_a_mutation_authorization() -> Result<(), Box<dyn std::error::Error>> {
        let mut input = canary()?;
        let command = command()?;
        input.mode = RunMode::Shadow;
        input.capabilities.clear();
        assert_eq!(evaluate_gate(&input, &command)?, GateDecision::ShadowOnly);
        Ok(())
    }

    #[test]
    fn live_stays_closed_until_a_strategy_release_exists() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut input = canary()?;
        let command = command()?;
        input.mode = RunMode::Live;
        assert!(matches!(
            evaluate_gate(&input, &command),
            Err(GateError::LiveReleaseUnavailable)
        ));
        Ok(())
    }

    #[test]
    fn only_a_current_canary_permit_authorizes_its_bound_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let command = command()?;
        assert!(matches!(
            validate_canary_permit(&GateDecision::ShadowOnly, &command, 2),
            Err(GateError::Shadow)
        ));
        let expired = GateDecision::CanaryPermit {
            command_sha256: command_fingerprint(&command),
            valid_until_ms: 2,
        };
        assert!(matches!(
            validate_canary_permit(&expired, &command, 2),
            Err(GateError::PermitExpired)
        ));
        let current = GateDecision::CanaryPermit {
            command_sha256: command_fingerprint(&command),
            valid_until_ms: 3,
        };
        assert!(validate_canary_permit(&current, &command, 2).is_ok());

        let mut different = command.clone();
        different.limit_price = Price::new(rust_decimal::Decimal::new(2, 1))?;
        assert!(matches!(
            validate_canary_permit(&current, &different, 2),
            Err(GateError::CommandFingerprint)
        ));
        let mut different_position_side = command;
        different_position_side.position_side = PositionSide::Short;
        assert!(matches!(
            validate_canary_permit(&current, &different_position_side, 2),
            Err(GateError::CommandFingerprint)
        ));
        Ok(())
    }

    #[test]
    fn canary_permit_is_capped_by_private_and_writer_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let input = canary()?;
        let command = command()?;
        assert!(matches!(
            evaluate_gate(&input, &command)?,
            GateDecision::CanaryPermit {
                valid_until_ms: 5,
                ..
            }
        ));
        let mut expired = input;
        expired.private_readback_valid_until_ms = 2;
        assert!(matches!(
            evaluate_gate(&expired, &command),
            Err(GateError::PrivateReadbackExpired)
        ));
        Ok(())
    }

    #[test]
    fn operator_canary_budget_is_hard_capped_at_ten_usdt() -> Result<(), Box<dyn std::error::Error>>
    {
        let command = command()?;
        let mut input = canary()?;
        input.max_entry_notional.value =
            rust_decimal::Decimal::new(CANARY_MAX_ENTRY_NOTIONAL_USDT, 0);
        assert!(matches!(
            evaluate_gate(&input, &command)?,
            GateDecision::CanaryPermit { .. }
        ));

        input.max_entry_notional.value = rust_decimal::Decimal::new(1001, 2);
        assert!(matches!(
            evaluate_gate(&input, &command),
            Err(GateError::CanaryNotional)
        ));
        input.max_entry_notional.value = rust_decimal::Decimal::ZERO;
        assert!(matches!(
            evaluate_gate(&input, &command),
            Err(GateError::CanaryNotional)
        ));
        Ok(())
    }
}
