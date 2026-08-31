use super::*;
use crate::{CopyPlanningSnapshot, FrozenCapitalSnapshot, relation_commitment};
use sha2::{Digest, Sha256};
use venue_copy::{
    CopyId, DeliveryBinding, derive_copy_identities, derive_target_observation_identity,
};
use venue_domain::domain::Amount;

pub(super) fn assemble(
    scope: &CopyObserverScope,
    relation: &CopyRelationRecord,
    leader: &CopyPlanningFact,
    follower: &CopyPlanningFact,
    now_ms: u64,
) -> Result<Option<CopyLeaderEnvelope>, CopyRepositoryError> {
    for (fact, role, binding) in [
        (
            leader,
            CopyPlanningFactRole::Leader,
            &relation.relation.leader,
        ),
        (
            follower,
            CopyPlanningFactRole::Follower,
            &relation.relation.follower,
        ),
    ] {
        fact.validate()
            .map_err(|_| CopyRepositoryError::CorruptData)?;
        if fact.role != role
            || !matches_binding(binding, fact)
            || fact.relation_id != relation.relation.relation_id
            || fact.relation_revision != relation.revision
            || fact.policy_digest != relation.relation.policy_digest()
            || now_ms < fact.observed_ms
            || now_ms >= fact.expires_ms
        {
            return Ok(None);
        }
    }
    let config = &relation.relation;
    if config.lifecycle != CopyLifecyclePolicy::Active
        || scope.venue != config.follower.venue
        || scope.trading_account_id != config.follower.trading_account_id
        || scope.mode != config.follower.mode
        || leader.quote_net_exposure.asset != follower.quote_net_exposure.asset
        || leader.instrument.market != follower.instrument.market
    {
        return Ok(None);
    }
    let quote = follower.quote_net_exposure.asset.clone();
    let amount = |value| Amount::new(quote.clone(), value);
    let capital = FrozenCapitalSnapshot {
        generation: follower.private_generation,
        observed_ms: leader.observed_ms.max(follower.observed_ms),
        expires_ms: leader.expires_ms.min(follower.expires_ms),
        leader_strategy_capital: leader
            .leader_configured_capital
            .clone()
            .ok_or(CopyRepositoryError::CorruptData)?,
        leader_target_exposure: leader.quote_net_exposure.clone(),
        follower_configured_capital: amount(config.allocated_capital),
        follower_allocated_capital: amount(config.allocated_capital),
        follower_available_margin: follower
            .follower_available_margin
            .clone()
            .ok_or(CopyRepositoryError::CorruptData)?,
        follower_managed_exposure: follower.quote_net_exposure.clone(),
        margin_safety_reserve_rate: config.safety_reserve_rate,
        exposure_multiplier: config.multiplier,
    };
    let commitment = relation_commitment(relation).map_err(|_| CopyRepositoryError::InvalidData)?;
    let identity = derive_target_observation_identity(
        &commitment,
        &CopyId::parse(&scope.trading_account_id).map_err(|_| CopyRepositoryError::InvalidData)?,
        &hash(b"leader-binding", &leader.binding)?,
        &hash(b"follower-binding", &follower.binding)?,
        &hash(b"paired-facts", &(leader, follower))?,
    )
    .map_err(|_| CopyRepositoryError::InvalidData)?;
    let identities =
        derive_copy_identities(&identity.input).map_err(|_| CopyRepositoryError::InvalidData)?;
    let snapshot = CopyPlanningSnapshot {
        instrument_generation: follower.rules_generation,
        delivery_expires_at_ms: capital.expires_ms,
        binding: DeliveryBinding {
            relation: commitment,
            leader_id: identity.leader_id,
            follower_id: identity.follower_id,
            follower_binding_id: identity.follower_binding_id,
            follower_instance_id: follower.binding.instance_id.clone(),
            account_id: scope.trading_account_id.clone(),
            instrument: follower.instrument.clone(),
            policy_id: identity.policy_id,
        },
        capital,
    };
    let source = serde_json::json!({
        "relation_id": config.relation_id, "revision": relation.revision,
        "policy_digest": config.policy_digest(), "leader_binding": leader.binding,
        "follower_binding": follower.binding, "leader_exposure": leader.quote_net_exposure,
        "leader_capital": leader.leader_configured_capital,
        "follower_margin": follower.follower_available_margin,
    });
    let payload =
        serde_json::json!({"semantic_action": "FOLLOW_TARGET", "node_target_source": source});
    let envelope = CopyLeaderEnvelope {
        scope: scope.clone(),
        intent: CopyLeaderIntent {
            intent_id: identities.child_order_id,
            snapshot_id: identities.planning_snapshot_id,
            identity_input: identity.input,
            intent_digest: hash(b"target-intent", &payload)?,
            intent_payload: payload,
            observed_at_ms: snapshot.capital.observed_ms,
        },
        snapshot: CopyLeaderSnapshot {
            snapshot_id: identities.planning_snapshot_id,
            generation: snapshot.capital.generation,
            observed_at_ms: snapshot.capital.observed_ms,
            expires_at_ms: snapshot.capital.expires_ms,
            snapshot_digest: hash(b"target-snapshot", &snapshot)?,
            snapshot_payload: encode(&snapshot)?,
        },
        outbox_digest: hash(b"target-event", &(scope, &snapshot, leader, follower))?,
    };
    let planned = plan_observed_copy_job(
        ObservedCopyIntent {
            envelope: envelope.clone(),
            event_digest: envelope.outbox_digest,
            event_sequence: 1,
        },
        now_ms,
    )
    .map_err(|_| CopyRepositoryError::InvalidData)?;
    if planned.target.delta_exposure.value.is_zero() {
        return Ok(None);
    }
    // Relation limits are additional to account admission, not a replacement for the Node's
    // latest risk/Owner/WAL checks. Never silently clip the leader's immutable target.
    if planned.target.target_exposure.value.abs() > config.risk.max_total_notional
        || planned.target.exposure_ratio.abs() > config.risk.max_leverage
    {
        return Ok(None);
    }
    Ok(Some(envelope))
}

pub(super) fn hash<T: serde::Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<[u8; 32], CopyRepositoryError> {
    let bytes = serde_json::to_vec(value).map_err(|_| CopyRepositoryError::CorruptData)?;
    let mut hash = Sha256::new();
    hash.update(b"venue-copy-node-source-v1\0");
    hash.update(domain);
    hash.update([0]);
    hash.update(bytes);
    Ok(hash.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use venue_control_protocol::{CopyRiskPolicy, ExecutionFactBinding, GatewayMode, VenueId};
    use venue_domain::domain::{Asset, InstrumentIdentity, MarketKind};

    fn inputs() -> Result<
        (
            CopyObserverScope,
            CopyRelationRecord,
            CopyPlanningFact,
            CopyPlanningFact,
        ),
        Box<dyn std::error::Error>,
    > {
        let endpoint = |account: &str,
                        instance: &str|
         -> Result<CopyRelationBinding, Box<dyn std::error::Error>> {
            Ok(CopyRelationBinding {
                venue: VenueId::Binance,
                mode: GatewayMode::Live,
                trading_account_id: account.into(),
                instance_id: instance.into(),
                symbol: "BTC/USDT".parse()?,
            })
        };
        let relation = CopyRelationRecord {
            revision: 1,
            relation: CopyRelationConfig {
                relation_id: "00000000-0000-4000-8000-000000000099".into(),
                leader: endpoint("00000000-0000-4000-8000-000000000001", "leader")?,
                follower: endpoint("00000000-0000-4000-8000-000000000002", "copy")?,
                allocated_capital: Decimal::TEN,
                multiplier: Decimal::from(2),
                safety_reserve_rate: Decimal::ZERO,
                lifecycle: CopyLifecyclePolicy::Active,
                risk: CopyRiskPolicy {
                    max_total_notional: Decimal::TEN,
                    max_order_notional: Decimal::TEN,
                    max_leverage: Decimal::from(2),
                },
            },
        };
        let build = |binding: &CopyRelationBinding,
                     role|
         -> Result<CopyPlanningFact, Box<dyn std::error::Error>> {
            let asset = Asset::new("USDT")?;
            let amount = |value| Amount::new(asset.clone(), Decimal::from(value));
            Ok(CopyPlanningFact {
                role,
                relation_id: relation.relation.relation_id.clone(),
                relation_revision: 1,
                policy_digest: relation.relation.policy_digest(),
                binding: ExecutionFactBinding {
                    venue: binding.venue,
                    mode: binding.mode,
                    trading_account_id: binding.trading_account_id.clone(),
                    symbol: binding.symbol.clone(),
                    instance_id: binding.instance_id.clone(),
                    config_epoch: 1,
                },
                instrument: InstrumentIdentity {
                    symbol: binding.symbol.clone(),
                    market: MarketKind::LinearPerpetual,
                    settlement_asset: Some(asset.clone()),
                },
                private_generation: 1,
                rules_generation: 1,
                observed_ms: 100,
                expires_ms: 200,
                quote_net_exposure: amount(if role == CopyPlanningFactRole::Leader {
                    20
                } else {
                    0
                }),
                leader_configured_capital: (role == CopyPlanningFactRole::Leader)
                    .then(|| amount(100)),
                follower_available_margin: (role == CopyPlanningFactRole::Follower)
                    .then(|| amount(100)),
                fact_digest: [1; 32],
            })
        };
        let leader = build(&relation.relation.leader, CopyPlanningFactRole::Leader)?;
        let follower = build(&relation.relation.follower, CopyPlanningFactRole::Follower)?;
        let scope = CopyObserverScope {
            observer_id: "node-facts".into(),
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: follower.binding.trading_account_id.clone(),
        };
        Ok((scope, relation, leader, follower))
    }

    #[test]
    fn paired_facts_are_frozen_and_replay_stable() -> Result<(), Box<dyn std::error::Error>> {
        let (scope, relation, leader, follower) = inputs()?;
        let first = assemble(&scope, &relation, &leader, &follower, 101)?.ok_or("missing input")?;
        assert_eq!(
            Some(first.clone()),
            assemble(&scope, &relation, &leader, &follower, 102)?
        );
        let planned = plan_observed_copy_job(
            ObservedCopyIntent {
                envelope: first.clone(),
                event_sequence: 1,
                event_digest: first.outbox_digest,
            },
            103,
        )?;
        assert_eq!(planned.target.target_exposure.value, Decimal::from(4));
        assert_eq!(
            planned.frozen_capital.leader_target_exposure.value,
            Decimal::from(20)
        );
        assert_eq!(
            planned.job.manifest.binding.account_id,
            scope.trading_account_id
        );
        Ok(())
    }

    #[test]
    fn stale_paused_foreign_or_wrong_revision_facts_never_make_jobs()
    -> Result<(), Box<dyn std::error::Error>> {
        let (scope, relation, leader, follower) = inputs()?;
        assert!(assemble(&scope, &relation, &leader, &follower, 200)?.is_none());
        let mut changed = relation.clone();
        changed.relation.lifecycle = CopyLifecyclePolicy::Paused;
        assert!(assemble(&scope, &changed, &leader, &follower, 101)?.is_none());
        let mut changed = follower.clone();
        changed.relation_revision += 1;
        assert!(assemble(&scope, &relation, &leader, &changed, 101)?.is_none());
        let mut wrong = scope.clone();
        wrong.trading_account_id = leader.binding.trading_account_id.clone();
        assert!(assemble(&wrong, &relation, &leader, &follower, 101)?.is_none());
        let mut excessive = leader.clone();
        excessive.quote_net_exposure.value = Decimal::from(1000);
        assert!(assemble(&scope, &relation, &excessive, &follower, 101)?.is_none());
        Ok(())
    }

    #[test]
    fn settled_drift_uses_a_new_fresh_job_and_never_renews_the_old_child()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::super::repair::assemble_repair;
        use venue_copy::{AuthoritativePositionSnapshot, DriftRepairRequest};
        let (scope, relation, mut leader, mut follower) = inputs()?;
        let original =
            assemble(&scope, &relation, &leader, &follower, 101)?.ok_or("original input")?;
        let planned = plan_observed_copy_job(
            ObservedCopyIntent {
                event_digest: original.outbox_digest,
                envelope: original,
                event_sequence: 1,
            },
            101,
        )?;
        let mut closing_exposure = planned.target.target_exposure.clone();
        closing_exposure.value -= Decimal::ONE;
        let projection = CopyDriftProjection {
            source_job_id: planned.job.identities.job_id,
            receipt_sequence: 2,
            position: AuthoritativePositionSnapshot {
                binding: planned.job.manifest.binding.clone(),
                generation: 2,
                observed_at_ms: 110,
                expires_at_ms: 190,
                exposure: closing_exposure.clone(),
                fact_digest: [7; 32],
            },
            target: planned.target.clone(),
            repair: None,
            projected_at_ms: 111,
        };
        // Old observation/job have expired. A genuinely new pair may create a new job; the
        // historical ledger alone cannot extend the old observation or execution request.
        leader.observed_ms = 210;
        leader.expires_ms = 300;
        follower.observed_ms = 220;
        follower.expires_ms = 290;
        follower.private_generation = 3;
        follower.quote_net_exposure = closing_exposure;
        follower.fact_digest = [8; 32];
        let fresh = assemble(&scope, &relation, &leader, &follower, 221)?.ok_or("fresh input")?;
        let repair = assemble_repair(fresh.clone(), &follower, &planned.job, &projection, 221)?
            .ok_or("repair input")?;
        let request: DriftRepairRequest = serde_json::from_value(
            repair.intent.intent_payload["drift_repair"]["request"].clone(),
        )?;
        let next = plan_observed_copy_job(
            ObservedCopyIntent {
                event_digest: repair.outbox_digest,
                envelope: repair.clone(),
                event_sequence: 2,
            },
            221,
        )?;
        assert_eq!(request.identities, next.job.identities);
        assert_ne!(request.identities.job_id, planned.job.identities.job_id);
        assert_eq!(request.supersedes_job_id, planned.job.identities.job_id);
        assert_eq!(request.delta_exposure.value, Decimal::ONE);
        assert_eq!(request.expires_at_ms, 290);
        assert_eq!(planned.job.manifest.expires_at_ms, 200);
        assert_eq!(next.target.target_exposure, planned.target.target_exposure);
        assert_eq!(next.target.snapshot_generation, 3);
        let mut invalid = projection.clone();
        invalid.target.target_exposure.value += Decimal::ONE;
        assert!(assemble_repair(fresh.clone(), &follower, &planned.job, &invalid, 221).is_err());
        let mut old_follower = follower.clone();
        old_follower.private_generation = 1;
        assert!(
            assemble_repair(fresh.clone(), &old_follower, &planned.job, &projection, 221)?
                .is_none()
        );
        assert!(assemble_repair(fresh, &follower, &planned.job, &projection, 290).is_err());
        Ok(())
    }
}
