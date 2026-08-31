//! Builds immutable planner inputs from the existing durable Node projection inbox.
//! No exchange client, second fact journal, or account execution authority lives here.

use super::*;
use venue_control_protocol::{
    CopyLifecyclePolicy, CopyPlanningFact, CopyPlanningFactRole, CopyRelationBinding,
    CopyRelationRecord, NodeProjectionEnvelope,
};

#[path = "copy_planning_expiry.rs"]
mod expiry;
#[path = "copy_planning_input.rs"]
mod input;
#[path = "copy_planning_repair.rs"]
mod repair;

pub(super) async fn store_next_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    scope: &CopyObserverScope,
    now_ms: u64,
) -> Result<bool, CopyRepositoryError> {
    let rows = sqlx::query(
        "SELECT *, \
         (to_jsonb(leader_venue)=config_json#>'{leader,venue}' \
          AND to_jsonb(leader_mode)=config_json#>'{leader,mode}' \
          AND to_jsonb(leader_account_id)=config_json#>'{leader,trading_account_id}' \
          AND to_jsonb(leader_instance_id)=config_json#>'{leader,instance_id}' \
          AND to_jsonb(leader_symbol)=config_json#>'{leader,symbol}' \
          AND to_jsonb(follower_venue)=config_json#>'{follower,venue}' \
          AND to_jsonb(follower_mode)=config_json#>'{follower,mode}' \
          AND to_jsonb(follower_account_id)=config_json#>'{follower,trading_account_id}' \
          AND to_jsonb(follower_instance_id)=config_json#>'{follower,instance_id}' \
          AND to_jsonb(follower_symbol)=config_json#>'{follower,symbol}' \
          AND to_jsonb(relation_id)=config_json->'relation_id' \
          AND to_jsonb(lifecycle)=config_json->'lifecycle') AS consistent \
         FROM venue_copy_relation_configs WHERE follower_venue=$1 \
          AND follower_mode='LIVE' AND follower_account_id=$2 \
         ORDER BY relation_id LIMIT 1001 FOR SHARE",
    )
    .bind(scope.venue.as_str())
    .bind(&scope.trading_account_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(database_error)?;
    if rows.len() > 1000 {
        return Err(CopyRepositoryError::InvalidData);
    }
    for row in rows {
        if row
            .try_get::<Option<bool>, _>("consistent")
            .map_err(database_error)?
            != Some(true)
        {
            return Err(CopyRepositoryError::CorruptData);
        }
        let relation = CopyRelationRecord {
            relation: decode(row.try_get("config_json").map_err(database_error)?)?,
            revision: from_i64(row.try_get("revision").map_err(database_error)?)?,
        };
        relation
            .validate()
            .map_err(|_| CopyRepositoryError::CorruptData)?;
        if relation.relation.lifecycle != CopyLifecyclePolicy::Active {
            continue;
        }
        let Some(leader) =
            latest_fact(transaction, &relation, CopyPlanningFactRole::Leader, now_ms).await?
        else {
            continue;
        };
        let Some(follower) = latest_fact(
            transaction,
            &relation,
            CopyPlanningFactRole::Follower,
            now_ms,
        )
        .await?
        else {
            continue;
        };
        let Some(mut envelope) = input::assemble(scope, &relation, &leader, &follower, now_ms)?
        else {
            continue;
        };
        // The source signature excludes observation clocks but includes economic inputs. A new
        // polling generation must not retry a rejected or already executed identical target.
        let latest = sqlx::query(
            "SELECT i.intent_json FROM venue_copy_leader_intents i \
             JOIN venue_copy_observer_outbox o USING(intent_id) \
             WHERE i.venue=$1 AND i.mode='LIVE' AND i.trading_account_id=$2 \
              AND i.intent_json#>>'{intent_payload,node_target_source,relation_id}'=$3 \
             ORDER BY o.event_sequence DESC LIMIT 1",
        )
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .bind(&relation.relation.relation_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(database_error)?;
        let mut unchanged = None;
        if let Some(row) = latest {
            let prior: CopyLeaderIntent =
                decode(row.try_get("intent_json").map_err(database_error)?)?;
            if prior.intent_payload.get("node_target_source")
                == envelope.intent.intent_payload.get("node_target_source")
            {
                unchanged = Some(prior);
            }
        }
        // Claims or execution evidence keep the planning fence, including across revisions.
        // An expired job is eligible only if neither delivery surface ever exposed it.
        let unsettled = sqlx::query(
            "SELECT j.job_json, j.relation_id, j.relation_revision, j.policy_digest FROM venue_copy_jobs j \
             WHERE j.venue=$1 AND j.mode='LIVE' AND j.trading_account_id=$2 \
              AND NOT EXISTS(SELECT 1 FROM venue_copy_ledger l WHERE l.job_id=j.job_id) \
              AND NOT EXISTS(SELECT 1 FROM venue_copy_delivery_receipts r \
                 WHERE r.job_id=j.job_id AND r.status='rejected') \
              AND NOT EXISTS(SELECT 1 FROM venue_account_deliveries d \
                 JOIN venue_copy_delivery_outbox o ON o.job_id=j.job_id \
                 WHERE d.delivery_id=('copy:' || j.job_id) \
                   AND d.delivery_state='expired_unclaimed' \
                   AND o.delivery_state='expired_unclaimed') ORDER BY j.job_id LIMIT 1001",
        )
        .bind(scope.venue.as_str())
        .bind(&scope.trading_account_id)
        .fetch_all(&mut **transaction)
        .await
        .map_err(database_error)?;
        if unsettled.len() > 1000 {
            return Err(CopyRepositoryError::InvalidData);
        }
        let mut busy = false;
        let mut expired = Vec::new();
        for row in unsettled {
            let job: CopyJob = decode(row.try_get("job_json").map_err(database_error)?)?;
            validate_job_relation_columns(&row, &job)?;
            if job.scope.venue != scope.venue
                || job.scope.mode != scope.mode
                || job.scope.trading_account_id != scope.trading_account_id
            {
                return Err(CopyRepositoryError::CorruptData);
            }
            if job.manifest.binding.relation.relation_id.to_string()
                == relation.relation.relation_id
            {
                if expiry::lock_unclaimed_expired(transaction, &job, &leader, &follower, now_ms)
                    .await?
                {
                    expired.push(job);
                    continue;
                }
                busy = true;
            }
        }
        if busy {
            continue;
        }
        if let Some(prior) = unchanged.filter(|prior| {
            !expired.iter().any(|job| {
                job.intent_id == prior.intent_id
                    && job.identities.planning_snapshot_id == prior.snapshot_id
                    && venue_copy::derive_copy_identities(&prior.identity_input)
                        .is_ok_and(|identities| identities == job.identities)
            })
        }) {
            let Some(repaired) =
                repair::from_reconciled_source(transaction, &prior, envelope, &follower, now_ms)
                    .await?
            else {
                continue;
            };
            envelope = repaired;
        }
        if !expired.is_empty() {
            expiry::bind_successor(&mut envelope, &expired, now_ms)?;
            // All candidates remain locked; any later failure to store/plan the new job rolls
            // back both delivery markers along with the new immutable input.
            expiry::mark_expired(transaction, &expired, now_ms).await?;
        }
        leader_input::store_in_transaction(transaction, &envelope, now_ms).await?;
        return Ok(true);
    }
    Ok(false)
}

async fn latest_fact(
    transaction: &mut Transaction<'_, Postgres>,
    relation: &CopyRelationRecord,
    role: CopyPlanningFactRole,
    now_ms: u64,
) -> Result<Option<CopyPlanningFact>, CopyRepositoryError> {
    let expected = match role {
        CopyPlanningFactRole::Leader => &relation.relation.leader,
        CopyPlanningFactRole::Follower => &relation.relation.follower,
    };
    // Select the latest envelope before looking for a fact. A newer paused/empty observation
    // must suppress, not resurrect, an older active fact from another node name.
    let row = sqlx::query(
        "SELECT n.*, s.config_epoch AS current_epoch \
         FROM venue_account_node_projection_inbox n \
         JOIN venue_control_strategy_scopes s ON s.venue=n.venue AND s.mode=n.mode \
          AND s.trading_account_id=n.trading_account_id AND s.instance_id=n.instance_id \
         WHERE n.venue=$1 AND n.mode='LIVE' AND n.trading_account_id=$2 \
          AND n.instance_id=$3 AND s.symbol=$4 \
         ORDER BY (n.envelope_json#>>'{snapshot,generated_ms}')::bigint DESC, \
          (n.envelope_json#>>'{binding,config_epoch}')::bigint DESC, n.node_generation DESC \
         LIMIT 1",
    )
    .bind(expected.venue.as_str())
    .bind(&expected.trading_account_id)
    .bind(&expected.instance_id)
    .bind(expected.symbol.to_string())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let envelope: NodeProjectionEnvelope =
        decode(row.try_get("envelope_json").map_err(database_error)?)?;
    envelope
        .validate()
        .map_err(|_| CopyRepositoryError::CorruptData)?;
    if envelope.node_id
        != row
            .try_get::<String, _>("node_id")
            .map_err(database_error)?
        || envelope.node_generation
            != from_i64(row.try_get("node_generation").map_err(database_error)?)?
        || envelope.sequence
            != from_i64(row.try_get("projection_sequence").map_err(database_error)?)?
        || envelope.digest != digest(row.try_get("projection_digest").map_err(database_error)?)?
        || envelope.binding.venue != expected.venue
        || envelope.binding.mode != expected.mode
        || envelope.binding.trading_account_id != expected.trading_account_id
        || envelope.binding.instance_id != expected.instance_id
    {
        return Err(CopyRepositoryError::CorruptData);
    }
    if envelope.binding.config_epoch
        != from_i64(row.try_get("current_epoch").map_err(database_error)?)?
        || envelope.binding.symbol != expected.symbol
        || envelope.snapshot.generated_ms > now_ms
        || envelope.snapshot.connection != venue_control_protocol::ConnectionState::Live
        || envelope
            .snapshot
            .accounts
            .iter()
            .any(|account| account.health != venue_control_protocol::HealthState::Healthy)
        || envelope.snapshot.strategies.iter().any(|strategy| {
            strategy.lifecycle != venue_control_protocol::StrategyLifecycle::Running
        })
    {
        return Ok(None);
    }
    let private_generation = envelope
        .snapshot
        .accounts
        .first()
        .ok_or(CopyRepositoryError::CorruptData)?
        .private_generation;
    let fact = envelope.copy_planning_facts.into_iter().find(|fact| {
        fact.role == role
            && fact.relation_id == relation.relation.relation_id
            && fact.relation_revision == relation.revision
            && fact.policy_digest == relation.relation.policy_digest()
            && fact.observed_ms <= now_ms
            && now_ms < fact.expires_ms
            && fact.private_generation == private_generation
            && matches_binding(expected, fact)
    });
    Ok(fact)
}

fn matches_binding(expected: &CopyRelationBinding, fact: &CopyPlanningFact) -> bool {
    expected.venue == fact.binding.venue
        && expected.mode == fact.binding.mode
        && expected.trading_account_id == fact.binding.trading_account_id
        && expected.instance_id == fact.binding.instance_id
        && expected.symbol == fact.binding.symbol
}
