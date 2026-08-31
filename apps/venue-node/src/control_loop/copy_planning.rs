use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use venue_control_protocol::{
    CopyLifecyclePolicy, CopyPlanningFact, CopyPlanningFactRole, CopyRelationRecord,
    ExecutionFactBinding, MAX_COPY_PLANNING_FACT_TTL_MS, MAX_COPY_PLANNING_FACTS,
};
use venue_domain::{Amount, Asset, PositionSide};
use venue_runtime::{
    AccountInstrumentIdentity, SignedAccountPositionMode, SignedAccountSnapshot,
    account::InstanceLifecycle,
};

use super::ControlResidentLoopError;

/// Derives immutable, non-executable Copy planning evidence only from a current signed account
/// page and current adapter rules. Any ambiguity suppresses the fact rather than expressing an
/// empty position, parity FX conversion, or configuration-derived instrument identity.
pub(super) fn signed_copy_planning_facts(
    signed: &SignedAccountSnapshot,
    instrument: &AccountInstrumentIdentity,
    relations: &[CopyRelationRecord],
    binding: &ExecutionFactBinding,
    lifecycle: Option<InstanceLifecycle>,
    leader_capital: Option<&Amount>,
    generated_ms: u64,
) -> Result<Vec<CopyPlanningFact>, ControlResidentLoopError> {
    if lifecycle != Some(InstanceLifecycle::Running)
        || !signed.unknown_results().is_empty()
        || signed.observed_at_ms() == 0
        || signed.observed_at_ms() > generated_ms
        || signed.binding().venue != binding.venue
        || signed.binding().mode != binding.mode
        || signed.binding().trading_account_id != binding.trading_account_id
        || instrument.identity.symbol != binding.symbol
        || instrument.rules_generation != signed.rules_generation()
        || signed
            .open_orders()
            .iter()
            .any(|order| order.symbol == binding.symbol && order.external)
    {
        return Ok(Vec::new());
    }
    let expires_ms = signed
        .observed_at_ms()
        .checked_add(MAX_COPY_PLANNING_FACT_TTL_MS)
        .ok_or(ControlResidentLoopError::ProjectionEncoding)?;
    if generated_ms >= expires_ms {
        return Ok(Vec::new());
    }
    let quote = Asset::new(binding.symbol.quote())
        .map_err(|_| ControlResidentLoopError::ProjectionScope)?;
    let Ok(quote_net_exposure) = signed_quote_net_exposure(signed, binding, &quote) else {
        return Ok(Vec::new());
    };
    let follower_available_margin = signed
        .balances()
        .iter()
        .filter(|balance| balance.asset == quote)
        .collect::<Vec<_>>();
    let follower_available_margin = (follower_available_margin.len() == 1)
        .then(|| follower_available_margin[0].available_margin)
        .flatten()
        .map(|value| Amount::new(quote.clone(), value));
    let leader_capital = leader_capital.filter(|capital| {
        capital.asset == quote && capital.value.is_sign_positive() && !capital.value.is_zero()
    });
    let mut facts = Vec::new();
    for relation in relations {
        if facts.len() == MAX_COPY_PLANNING_FACTS {
            break;
        }
        if relation.validate().is_err()
            || relation.relation.lifecycle != CopyLifecyclePolicy::Active
            || relation.relation.leader.symbol.quote() != relation.relation.follower.symbol.quote()
        {
            continue;
        }
        let role = if relation_matches(&relation.relation.leader, binding) {
            if leader_capital.is_none() {
                continue;
            }
            CopyPlanningFactRole::Leader
        } else if relation_matches(&relation.relation.follower, binding) {
            if follower_available_margin.is_none() {
                continue;
            }
            CopyPlanningFactRole::Follower
        } else {
            continue;
        };
        let mut fact = CopyPlanningFact {
            role,
            relation_id: relation.relation.relation_id.clone(),
            relation_revision: relation.revision,
            policy_digest: relation.relation.policy_digest(),
            binding: binding.clone(),
            instrument: instrument.identity.clone(),
            private_generation: signed.private_generation(),
            rules_generation: signed.rules_generation(),
            observed_ms: signed.observed_at_ms(),
            expires_ms,
            quote_net_exposure: quote_net_exposure.clone(),
            follower_available_margin: (role == CopyPlanningFactRole::Follower)
                .then(|| follower_available_margin.clone())
                .flatten(),
            leader_configured_capital: (role == CopyPlanningFactRole::Leader)
                .then(|| leader_capital.cloned())
                .flatten(),
            fact_digest: [0; 32],
        };
        fact.fact_digest = planning_fact_digest(&fact)?;
        if fact.validate().is_ok() {
            facts.push(fact);
        }
    }
    Ok(facts)
}

fn relation_matches(
    endpoint: &venue_control_protocol::CopyRelationBinding,
    binding: &ExecutionFactBinding,
) -> bool {
    endpoint.venue == binding.venue
        && endpoint.mode == binding.mode
        && endpoint.trading_account_id == binding.trading_account_id
        && endpoint.symbol == binding.symbol
        && endpoint.instance_id == binding.instance_id
}

fn signed_quote_net_exposure(
    signed: &SignedAccountSnapshot,
    binding: &ExecutionFactBinding,
    quote: &Asset,
) -> Result<Amount, ControlResidentLoopError> {
    let rows = signed
        .positions()
        .iter()
        .filter(|row| row.symbol == binding.symbol)
        .collect::<Vec<_>>();
    let complete = match signed.position_mode() {
        SignedAccountPositionMode::Net => {
            // A complete account-wide Net collection commonly omits a flat selected symbol.
            // That absence is an explicit zero only in Net mode; Hedge mode still requires both
            // authoritative legs so a missing leg cannot be relabelled as flat.
            rows.is_empty() || (rows.len() == 1 && rows[0].position_side == PositionSide::Net)
        }
        SignedAccountPositionMode::Hedge => {
            rows.len() == 2
                && rows.iter().any(|row| row.position_side == PositionSide::Long)
                && rows.iter().any(|row| row.position_side == PositionSide::Short)
                && rows.iter().all(|row| row.quantity >= Decimal::ZERO)
                // Copy uses one signed target.  Opposing live hedge legs cannot be truthfully
                // collapsed into a single leader exposure.
                && rows.iter().filter(|row| !row.quantity.is_zero()).count() <= 1
        }
    };
    if !complete {
        return Err(ControlResidentLoopError::ProjectionScope);
    }
    let mut exposure = Decimal::ZERO;
    for row in rows {
        if row.quantity.is_zero() {
            continue;
        }
        let mark = row
            .mark_price
            .filter(|mark| mark.is_sign_positive() && !mark.is_zero())
            .ok_or(ControlResidentLoopError::ProjectionScope)?;
        let signed_quantity = match row.position_side {
            PositionSide::Long => row.quantity,
            PositionSide::Short => Decimal::ZERO
                .checked_sub(row.quantity)
                .ok_or(ControlResidentLoopError::ProjectionEncoding)?,
            PositionSide::Net => row.quantity,
        };
        exposure = exposure
            .checked_add(
                signed_quantity
                    .checked_mul(mark)
                    .ok_or(ControlResidentLoopError::ProjectionEncoding)?,
            )
            .ok_or(ControlResidentLoopError::ProjectionEncoding)?;
    }
    Ok(Amount::new(quote.clone(), exposure))
}

fn planning_fact_digest(fact: &CopyPlanningFact) -> Result<[u8; 32], ControlResidentLoopError> {
    let encoded = serde_json::to_vec(&(
        fact.role,
        &fact.relation_id,
        fact.relation_revision,
        fact.policy_digest,
        &fact.binding,
        &fact.instrument,
        fact.private_generation,
        fact.rules_generation,
        fact.observed_ms,
        fact.expires_ms,
        &fact.quote_net_exposure,
        &fact.follower_available_margin,
        &fact.leader_configured_capital,
    ))
    .map_err(|_| ControlResidentLoopError::ProjectionEncoding)?;
    let mut digest = Sha256::new();
    digest.update(b"venue.node.copy-planning-fact.v1");
    digest.update(encoded);
    Ok(digest.finalize().into())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    use super::*;

    #[test]
    fn complete_net_snapshot_uses_an_absent_selected_position_as_signed_zero()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "DOGE/USDT".parse()?,
        )?;
        let snapshot = SignedAccountSnapshot::complete(
            binding.clone(),
            1,
            1,
            1,
            1,
            SignedAccountPositionMode::Net,
            Vec::new(),
            Vec::new(),
            "cursor:0".to_owned(),
            Vec::new(),
        )?;
        assert_eq!(
            signed_quote_net_exposure(
                &snapshot,
                &ExecutionFactBinding {
                    venue: VenueId::Binance,
                    mode: GatewayMode::Live,
                    trading_account_id: binding.trading_account_id.clone(),
                    symbol: binding.symbol.clone(),
                    instance_id: "copy-follower".to_owned(),
                    config_epoch: 1,
                },
                &Asset::new("USDT")?,
            )?,
            Amount::new(Asset::new("USDT")?, Decimal::ZERO)
        );
        Ok(())
    }

    #[test]
    fn incomplete_hedge_snapshot_does_not_turn_missing_legs_into_zero()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "DOGE/USDT".parse()?,
        )?;
        let snapshot = SignedAccountSnapshot::complete(
            binding.clone(),
            1,
            1,
            1,
            1,
            SignedAccountPositionMode::Hedge,
            Vec::new(),
            Vec::new(),
            "cursor:0".to_owned(),
            Vec::new(),
        )?;
        assert!(
            signed_quote_net_exposure(
                &snapshot,
                &ExecutionFactBinding {
                    venue: VenueId::Binance,
                    mode: GatewayMode::Live,
                    trading_account_id: binding.trading_account_id.clone(),
                    symbol: binding.symbol.clone(),
                    instance_id: "copy-follower".to_owned(),
                    config_epoch: 1,
                },
                &Asset::new("USDT")?,
            )
            .is_err()
        );
        Ok(())
    }
}
