//! Atomic Grid place/cancel batches committed ahead of the singleton Executor.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use sqlx::Row;
use venue_control_protocol::grid::{GridAnchor, GridInstanceState, GridInstanceSummary};

use crate::{
    executor_store::{account_queue_has_capacity, lock_account_command_queue},
    kol_executor::BinanceCommandLedgerError,
};

use super::{
    BinanceGridStore, GridCommandIntent, GridDesiredOrder, GridFillAllocation, GridLedgerCommand,
    GridOrderOwnership, GridStoreError, bounded, bytes_digest, command_columns, corrupt_row,
    database_error, decimal, decimal_text, digest, integer, ms, order_side_name, owned_state_name,
    ownership_identity_digest, position_side_name, role_name, unsigned, validate_command,
    validate_fill, validate_ids, validate_ownership,
};

pub const MAX_GRID_MUTATION_BATCH_COMMANDS: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridBatchPlacement {
    pub command: GridLedgerCommand,
    pub ownership: GridOrderOwnership,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridMutationBatch {
    pub batch_id: String,
    pub instance_id: String,
    pub expected_instance_revision: u64,
    pub config_revision: u64,
    pub plan_revision: u64,
    pub desired_digest: [u8; 32],
    pub placements: Vec<GridBatchPlacement>,
    pub cancellations: Vec<GridLedgerCommand>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridMutationBatchReceipt {
    pub batch_id: String,
    pub command_count: u16,
    pub batch_digest: [u8; 32],
    pub inserted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridPlanMutationBatch {
    pub mutation: GridMutationBatch,
    pub expected_plan_revision: u64,
    /// Exact desired surface consumed by this planner turn. `None` is valid only when the
    /// instance has not established a desired surface yet.
    pub expected_desired_digest: Option<[u8; 32]>,
    /// Exact tail observed by the planner. A successor may be committed only if this is still the
    /// instance tail, which prevents two authenticated fills from branching the projected plan.
    pub predecessor_batch_id: Option<String>,
    pub expected_private_generation: u64,
    pub expected_private_observed_ms: u64,
    /// Earliest authenticated private-stream receive time which caused this plan. Normal signed
    /// convergence has no event origin and leaves this empty.
    pub source_event_received_ms: Option<u64>,
    pub require_empty_account_queue: bool,
    pub anchor: GridAnchor,
    pub desired_orders: Vec<GridDesiredOrder>,
    pub fill_allocations: Vec<GridFillAllocation>,
    pub last_facts_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridPlanMutationCommit {
    pub receipt: GridMutationBatchReceipt,
    pub instance: GridInstanceSummary,
}

#[derive(Serialize)]
struct BatchPlacementIdentity<'a> {
    command: &'a GridLedgerCommand,
    ownership_digest: [u8; 32],
}

#[derive(Serialize)]
struct BatchIdentity<'a> {
    batch_id: &'a str,
    instance_id: &'a str,
    expected_instance_revision: u64,
    config_revision: u64,
    plan_revision: u64,
    desired_digest: [u8; 32],
    placements: Vec<BatchPlacementIdentity<'a>>,
    cancellations: &'a [GridLedgerCommand],
}

#[derive(Serialize)]
struct PlanBatchIdentity<'a> {
    mutation_digest: [u8; 32],
    expected_plan_revision: u64,
    expected_desired_digest: Option<[u8; 32]>,
    predecessor_batch_id: Option<&'a str>,
    expected_private_generation: u64,
    expected_private_observed_ms: u64,
    source_event_received_ms: Option<u64>,
    require_empty_account_queue: bool,
    anchor: &'a GridAnchor,
    desired_orders: &'a [GridDesiredOrder],
    fill_allocations: Vec<&'a GridFillAllocation>,
    last_facts_ms: u64,
}

struct LockedInstance {
    owner_user_id: String,
    trading_account_id: String,
    credential_id: String,
    symbol: String,
    state: GridInstanceState,
    convergence_started_ms: Option<i64>,
    desired_digest: Option<[u8; 32]>,
    tail_batch_id: Option<String>,
}

impl BinanceGridStore {
    /// Inserts one ordered Planner diff and every place ownership row in one transaction.
    /// An exact durable receipt makes a full replay idempotent; any partial identity collision is
    /// rejected instead of silently accepting an incomplete mutation surface.
    pub async fn enqueue_mutation_batch(
        &self,
        batch: &GridMutationBatch,
        now_ms: u64,
    ) -> Result<GridMutationBatchReceipt, GridStoreError> {
        let batch_digest = validate_batch(batch, now_ms, false)?;
        let command_count = command_count(batch)?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        if let Some(row) = load_receipt(&mut tx, &batch.batch_id).await? {
            verify_replay(&mut tx, batch, batch_digest, command_count, None, &row).await?;
            tx.commit().await.map_err(database_error)?;
            return Ok(receipt(batch, command_count, batch_digest, false));
        }
        let instance = lock_instance(
            &mut tx,
            batch,
            batch.plan_revision,
            Some(batch.desired_digest),
        )
        .await?;
        admit_account_batch(&mut tx, &instance, usize::from(command_count), false).await?;
        validate_instance_boundary(&mut tx, batch, &instance).await?;
        for placement in &batch.placements {
            validate_desired_placement(&mut tx, batch, placement).await?;
        }
        insert_receipt(&mut tx, batch, batch_digest, command_count, None, now_ms).await?;
        insert_mutations(&mut tx, batch, &instance, now_ms).await?;
        tx.commit().await.map_err(database_error)?;
        Ok(receipt(batch, command_count, batch_digest, true))
    }

    /// Atomically advances a normal convergence plan together with the fills that caused it and
    /// the first bounded mutation batch. A crash can therefore expose neither an unallocated fill
    /// nor a desired surface whose corresponding commands were never committed.
    pub async fn commit_plan_mutation_batch(
        &self,
        plan: &GridPlanMutationBatch,
        now_ms: u64,
    ) -> Result<GridPlanMutationCommit, GridStoreError> {
        let mutation_digest = validate_batch(&plan.mutation, now_ms, true)?;
        validate_plan_batch(plan, now_ms)?;
        let batch_digest = plan_batch_digest(plan, mutation_digest)?;
        let command_count = command_count(&plan.mutation)?;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        if let Some(row) = load_receipt(&mut tx, &plan.mutation.batch_id).await? {
            verify_replay(
                &mut tx,
                &plan.mutation,
                batch_digest,
                command_count,
                Some(plan),
                &row,
            )
            .await?;
            let owner_user_id: String = sqlx::query_scalar(
                "SELECT owner_user_id FROM venue_binance_grid_instances WHERE instance_id=$1",
            )
            .bind(&plan.mutation.instance_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(database_error)?;
            tx.commit().await.map_err(database_error)?;
            let instance = self
                .load_owned(&owner_user_id, &plan.mutation.instance_id)
                .await?;
            return Ok(GridPlanMutationCommit {
                receipt: receipt(&plan.mutation, command_count, batch_digest, false),
                instance: instance.ok_or(GridStoreError::Corrupt)?,
            });
        }
        let instance = lock_instance(
            &mut tx,
            &plan.mutation,
            plan.expected_plan_revision,
            plan.expected_desired_digest,
        )
        .await?;
        if instance.state != GridInstanceState::Running {
            return Err(GridStoreError::Conflict);
        }
        if instance.desired_digest != plan.expected_desired_digest
            || instance.tail_batch_id != plan.predecessor_batch_id
        {
            return Err(GridStoreError::Conflict);
        }
        admit_account_batch(
            &mut tx,
            &instance,
            usize::from(command_count),
            plan.require_empty_account_queue,
        )
        .await?;
        validate_private_projection(&mut tx, &instance, plan).await?;
        validate_instance_boundary(&mut tx, &plan.mutation, &instance).await?;
        validate_plan_fills(plan, &instance)?;
        insert_receipt(
            &mut tx,
            &plan.mutation,
            batch_digest,
            command_count,
            Some(plan),
            now_ms,
        )
        .await?;
        super::surface::write_anchor(
            &mut tx,
            &plan.mutation.instance_id,
            plan.mutation.config_revision,
            &plan.anchor,
            now_ms,
        )
        .await?;
        super::surface::replace_desired_rows(
            &mut tx,
            &plan.mutation.instance_id,
            &instance.symbol,
            plan.mutation.config_revision,
            plan.mutation.plan_revision,
            plan.mutation.desired_digest,
            &plan.desired_orders,
            now_ms,
        )
        .await?;
        for fill in &plan.fill_allocations {
            insert_fill_allocation(&mut tx, fill).await?;
        }
        refresh_allocated_owner_progress(&mut tx, plan).await?;
        for placement in &plan.mutation.placements {
            validate_desired_placement(&mut tx, &plan.mutation, placement).await?;
        }
        insert_mutations(&mut tx, &plan.mutation, &instance, now_ms).await?;
        let dirty: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE command_origin='grid' \
             AND grid_instance_id=$1 AND command_state IN \
             ('pending','sending','accepted','reconcile_required'))",
        )
        .bind(&plan.mutation.instance_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(database_error)?;
        let convergence_started = if dirty {
            Some(
                if plan.mutation.plan_revision != plan.expected_plan_revision {
                    ms(now_ms)?
                } else {
                    instance.convergence_started_ms.unwrap_or(ms(now_ms)?)
                },
            )
        } else {
            None
        };
        let updated = sqlx::query(
            "UPDATE venue_binance_grid_instances SET plan_revision=$1,desired_digest=$2,dirty=$3,\
             convergence_started_ms=$4,consecutive_failures=CASE WHEN $3 THEN consecutive_failures ELSE 0 END,\
             last_facts_ms=$5,grid_tail_batch_id=$6,revision=revision+1,updated_ms=$7 \
             WHERE instance_id=$8 AND revision=$9 AND current_config_revision=$10 \
               AND plan_revision=$11 AND instance_state='running'",
        )
        .bind(integer(plan.mutation.plan_revision)?)
        .bind(plan.mutation.desired_digest.as_slice())
        .bind(dirty)
        .bind(convergence_started)
        .bind(ms(plan.last_facts_ms)?)
        .bind(&plan.mutation.batch_id)
        .bind(ms(now_ms)?)
        .bind(&plan.mutation.instance_id)
        .bind(integer(plan.mutation.expected_instance_revision)?)
        .bind(integer(plan.mutation.config_revision)?)
        .bind(integer(plan.expected_plan_revision)?)
        .execute(&mut *tx)
        .await
        .map_err(database_error)?;
        if updated.rows_affected() != 1 {
            return Err(GridStoreError::Conflict);
        }
        tx.commit().await.map_err(database_error)?;
        let summary = self
            .load_owned(&instance.owner_user_id, &plan.mutation.instance_id)
            .await?
            .ok_or(GridStoreError::Corrupt)?;
        Ok(GridPlanMutationCommit {
            receipt: receipt(&plan.mutation, command_count, batch_digest, true),
            instance: summary,
        })
    }
}

async fn admit_account_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    instance: &LockedInstance,
    command_count: usize,
    require_empty: bool,
) -> Result<(), GridStoreError> {
    let queue_depth = lock_account_command_queue(
        &mut **tx,
        &instance.owner_user_id,
        &instance.trading_account_id,
        &instance.credential_id,
    )
    .await
    .map_err(grid_admission_error)?;
    if (require_empty && queue_depth != 0)
        || !account_queue_has_capacity(queue_depth, command_count)
    {
        return Err(GridStoreError::Conflict);
    }
    Ok(())
}

async fn validate_private_projection(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    instance: &LockedInstance,
    plan: &GridPlanMutationBatch,
) -> Result<(), GridStoreError> {
    let row = sqlx::query(
        "SELECT owner_user_id,trading_account_id,observed_ms,private_generation \
         FROM venue_binance_account_projections WHERE credential_id=$1 FOR UPDATE",
    )
    .bind(&instance.credential_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(database_error)?
    .ok_or(GridStoreError::Conflict)?;
    let owner_user_id: String = row.try_get("owner_user_id").map_err(corrupt_row)?;
    let trading_account_id: String = row.try_get("trading_account_id").map_err(corrupt_row)?;
    let observed_ms = unsigned(row.try_get("observed_ms").map_err(corrupt_row)?)?;
    let private_generation = unsigned(row.try_get("private_generation").map_err(corrupt_row)?)?;
    if owner_user_id != instance.owner_user_id
        || trading_account_id != instance.trading_account_id
        || observed_ms != plan.expected_private_observed_ms
        || private_generation != plan.expected_private_generation
    {
        return Err(GridStoreError::Conflict);
    }
    Ok(())
}

fn grid_admission_error(error: BinanceCommandLedgerError) -> GridStoreError {
    match error {
        BinanceCommandLedgerError::Conflict => GridStoreError::Conflict,
        BinanceCommandLedgerError::Unavailable => GridStoreError::Unavailable,
    }
}

fn command_count(batch: &GridMutationBatch) -> Result<u16, GridStoreError> {
    u16::try_from(batch.placements.len() + batch.cancellations.len())
        .map_err(|_| GridStoreError::Invalid)
}

fn plan_batch_digest(
    plan: &GridPlanMutationBatch,
    mutation_digest: [u8; 32],
) -> Result<[u8; 32], GridStoreError> {
    let mut fill_allocations = plan.fill_allocations.iter().collect::<Vec<_>>();
    fill_allocations.sort_by(|left, right| {
        (
            left.trading_account_id.as_str(),
            left.symbol.to_string(),
            left.native_trade_id.as_str(),
        )
            .cmp(&(
                right.trading_account_id.as_str(),
                right.symbol.to_string(),
                right.native_trade_id.as_str(),
            ))
    });
    digest(&PlanBatchIdentity {
        mutation_digest,
        expected_plan_revision: plan.expected_plan_revision,
        expected_desired_digest: plan.expected_desired_digest,
        predecessor_batch_id: plan.predecessor_batch_id.as_deref(),
        expected_private_generation: plan.expected_private_generation,
        expected_private_observed_ms: plan.expected_private_observed_ms,
        source_event_received_ms: plan.source_event_received_ms,
        require_empty_account_queue: plan.require_empty_account_queue,
        anchor: &plan.anchor,
        desired_orders: &plan.desired_orders,
        fill_allocations,
        last_facts_ms: plan.last_facts_ms,
    })
}

fn receipt(
    batch: &GridMutationBatch,
    command_count: u16,
    batch_digest: [u8; 32],
    inserted: bool,
) -> GridMutationBatchReceipt {
    GridMutationBatchReceipt {
        batch_id: batch.batch_id.clone(),
        command_count,
        batch_digest,
        inserted,
    }
}

async fn load_receipt(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch_id: &str,
) -> Result<Option<sqlx::postgres::PgRow>, GridStoreError> {
    sqlx::query(
        "SELECT instance_id,expected_instance_revision,config_revision,plan_revision,\
         desired_digest,batch_digest,command_count,private_generation,private_observed_ms,\
         instrument_generation,source_event_received_ms,input_desired_digest,predecessor_batch_id \
         FROM venue_binance_grid_mutation_batches \
         WHERE batch_id=$1 FOR UPDATE",
    )
    .bind(batch_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(database_error)
}

async fn insert_receipt(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch: &GridMutationBatch,
    batch_digest: [u8; 32],
    command_count: u16,
    plan: Option<&GridPlanMutationBatch>,
    now_ms: u64,
) -> Result<(), GridStoreError> {
    let private_generation = plan
        .map(|plan| integer(plan.expected_private_generation))
        .transpose()?;
    let private_observed_ms = plan
        .map(|plan| ms(plan.expected_private_observed_ms))
        .transpose()?;
    let instrument_generation = plan
        .map(|plan| integer(plan.anchor.instrument_generation))
        .transpose()?;
    let source_event_received_ms = plan
        .and_then(|plan| plan.source_event_received_ms)
        .map(ms)
        .transpose()?;
    sqlx::query(
        "INSERT INTO venue_binance_grid_mutation_batches \
         (batch_id,instance_id,expected_instance_revision,config_revision,plan_revision,\
          desired_digest,batch_digest,command_count,private_generation,private_observed_ms,\
          instrument_generation,source_event_received_ms,input_desired_digest,\
          predecessor_batch_id,created_ms) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
    )
    .bind(&batch.batch_id)
    .bind(&batch.instance_id)
    .bind(integer(batch.expected_instance_revision)?)
    .bind(integer(batch.config_revision)?)
    .bind(integer(batch.plan_revision)?)
    .bind(batch.desired_digest.as_slice())
    .bind(batch_digest.as_slice())
    .bind(i16::try_from(command_count).map_err(|_| GridStoreError::Invalid)?)
    .bind(private_generation)
    .bind(private_observed_ms)
    .bind(instrument_generation)
    .bind(source_event_received_ms)
    .bind(
        plan.and_then(|plan| plan.expected_desired_digest)
            .map(|digest| digest.to_vec()),
    )
    .bind(plan.and_then(|plan| plan.predecessor_batch_id.as_deref()))
    .bind(ms(now_ms)?)
    .execute(&mut **tx)
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn validate_instance_boundary(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch: &GridMutationBatch,
    instance: &LockedInstance,
) -> Result<(), GridStoreError> {
    if (!batch.placements.is_empty() && instance.state != GridInstanceState::Running)
        || (batch.placements.is_empty()
            && !batch.cancellations.is_empty()
            && !cancel_state_allowed(instance.state))
    {
        return Err(GridStoreError::Conflict);
    }
    let legacy: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM venue_control_strategy_scopes WHERE venue='binance' \
         AND mode='LIVE' AND trading_account_id=$1)",
    )
    .bind(&instance.trading_account_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(database_error)?;
    if legacy {
        return Err(GridStoreError::Conflict);
    }
    Ok(())
}

async fn insert_mutations(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch: &GridMutationBatch,
    instance: &LockedInstance,
    now_ms: u64,
) -> Result<(), GridStoreError> {
    let mut dispatch_sequence = 1_u64;
    for placement in &batch.placements {
        insert_command(
            tx,
            batch,
            instance,
            &placement.command,
            dispatch_sequence,
            None,
            now_ms,
        )
        .await?;
        insert_ownership(tx, instance, &placement.ownership).await?;
        dispatch_sequence = dispatch_sequence
            .checked_add(1)
            .ok_or(GridStoreError::Invalid)?;
    }
    for command in &batch.cancellations {
        let target = match &command.intent {
            GridCommandIntent::Cancel {
                target_client_order_id,
            } => target_client_order_id,
            _ => return Err(GridStoreError::Invalid),
        };
        let native_order_id =
            load_cancel_native_id(tx, &batch.instance_id, &instance.trading_account_id, target)
                .await?;
        insert_command(
            tx,
            batch,
            instance,
            command,
            dispatch_sequence,
            Some(&native_order_id),
            now_ms,
        )
        .await?;
        dispatch_sequence = dispatch_sequence
            .checked_add(1)
            .ok_or(GridStoreError::Invalid)?;
    }
    Ok(())
}

fn validate_plan_batch(plan: &GridPlanMutationBatch, now_ms: u64) -> Result<(), GridStoreError> {
    super::surface::validate_surface_input(
        &plan.mutation.instance_id,
        plan.expected_plan_revision,
        plan.mutation.plan_revision,
        &plan.desired_orders,
        plan.last_facts_ms,
        now_ms,
    )?;
    plan.anchor
        .validate()
        .map_err(|_| GridStoreError::Invalid)?;
    if plan.expected_private_generation == 0
        || plan.expected_private_observed_ms == 0
        || plan.expected_private_observed_ms > plan.last_facts_ms
        || plan.source_event_received_ms.is_some_and(|received| {
            received < plan.expected_private_observed_ms || received > plan.last_facts_ms
        })
        || plan.anchor.revision != plan.mutation.plan_revision
        || plan.anchor.observed_ms > plan.last_facts_ms
        || plan
            .predecessor_batch_id
            .as_deref()
            .is_some_and(|predecessor| {
                !bounded(predecessor, 1, 64) || predecessor == plan.mutation.batch_id
            })
    {
        return Err(GridStoreError::Invalid);
    }
    let desired_by_client = plan
        .desired_orders
        .iter()
        .map(|order| (order.client_order_id.as_str(), order))
        .collect::<std::collections::BTreeMap<_, _>>();
    for placement in &plan.mutation.placements {
        let Some(desired) = desired_by_client.get(placement.command.client_order_id.as_str())
        else {
            return Err(GridStoreError::Invalid);
        };
        let GridCommandIntent::LimitPostOnly {
            key,
            quantity,
            limit_price,
        } = &placement.command.intent
        else {
            return Err(GridStoreError::Invalid);
        };
        if desired.key != *key
            || desired.quantity != *quantity
            || desired.limit_price != *limit_price
        {
            return Err(GridStoreError::Invalid);
        }
    }
    Ok(())
}

fn validate_plan_fills(
    plan: &GridPlanMutationBatch,
    instance: &LockedInstance,
) -> Result<(), GridStoreError> {
    let mut identities = BTreeSet::new();
    for fill in &plan.fill_allocations {
        validate_fill(fill)?;
        if fill.instance_id != plan.mutation.instance_id
            || fill.trading_account_id != instance.trading_account_id
            || fill.config_revision != plan.mutation.config_revision
            || fill.symbol.to_string() != instance.symbol
            || fill.observed_ms > plan.last_facts_ms
            || !identities.insert((
                &fill.trading_account_id,
                &fill.symbol,
                &fill.native_trade_id,
            ))
        {
            return Err(GridStoreError::Invalid);
        }
    }
    Ok(())
}

async fn insert_fill_allocation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    fill: &GridFillAllocation,
) -> Result<(), GridStoreError> {
    let allocation_digest = digest(fill)?;
    let inserted = sqlx::query(
        "INSERT INTO venue_binance_grid_fill_allocations \
         (trading_account_id,symbol,native_trade_id,instance_id,config_revision,client_order_id,\
          position_side,order_role,quantity,price,maker,occurred_ms,observed_ms,allocation_digest) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING",
    )
    .bind(&fill.trading_account_id)
    .bind(fill.symbol.to_string())
    .bind(&fill.native_trade_id)
    .bind(&fill.instance_id)
    .bind(integer(fill.config_revision)?)
    .bind(&fill.client_order_id)
    .bind(position_side_name(fill.position_side))
    .bind(role_name(fill.role))
    .bind(decimal_text(fill.quantity))
    .bind(decimal_text(fill.price))
    .bind(fill.maker)
    .bind(fill.occurred_ms.map(ms).transpose()?)
    .bind(ms(fill.observed_ms)?)
    .bind(allocation_digest.as_slice())
    .execute(&mut **tx)
    .await
    .map_err(database_error)?;
    if inserted.rows_affected() == 0 {
        let durable: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT allocation_digest FROM venue_binance_grid_fill_allocations \
             WHERE trading_account_id=$1 AND symbol=$2 AND native_trade_id=$3",
        )
        .bind(&fill.trading_account_id)
        .bind(fill.symbol.to_string())
        .bind(&fill.native_trade_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(database_error)?;
        if durable.as_deref() != Some(allocation_digest.as_slice()) {
            return Err(GridStoreError::Conflict);
        }
    }
    Ok(())
}

async fn refresh_allocated_owner_progress(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    plan: &GridPlanMutationBatch,
) -> Result<(), GridStoreError> {
    let mut affected = BTreeMap::new();
    for fill in &plan.fill_allocations {
        affected
            .entry(fill.client_order_id.as_str())
            .and_modify(|observed: &mut u64| *observed = (*observed).max(fill.observed_ms))
            .or_insert(fill.observed_ms);
    }
    for (client_order_id, latest_observed_ms) in affected {
        let owner = sqlx::query(
            "SELECT quantity,filled_quantity,order_state,last_seen_ms \
             FROM venue_binance_grid_order_owners WHERE instance_id=$1 \
             AND trading_account_id=$2 AND client_order_id=$3 FOR UPDATE",
        )
        .bind(&plan.mutation.instance_id)
        .bind(
            plan.fill_allocations
                .first()
                .map(|fill| fill.trading_account_id.as_str())
                .ok_or(GridStoreError::Invalid)?,
        )
        .bind(client_order_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(database_error)?
        .ok_or(GridStoreError::Conflict)?;
        let quantities = sqlx::query(
            "SELECT quantity FROM venue_binance_grid_fill_allocations WHERE instance_id=$1 \
             AND client_order_id=$2 ORDER BY native_trade_id FOR SHARE",
        )
        .bind(&plan.mutation.instance_id)
        .bind(client_order_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(database_error)?;
        let allocated =
            quantities
                .into_iter()
                .try_fold(rust_decimal::Decimal::ZERO, |total, row| {
                    total
                        .checked_add(decimal(row.try_get("quantity").map_err(corrupt_row)?)?)
                        .ok_or(GridStoreError::Corrupt)
                })?;
        let original = decimal(owner.try_get("quantity").map_err(corrupt_row)?)?;
        let current = decimal(owner.try_get("filled_quantity").map_err(corrupt_row)?)?;
        let current_terminal = owner
            .try_get::<String, _>("order_state")
            .map_err(corrupt_row)?
            == "terminal";
        let (filled, terminal) =
            next_owner_progress(original, current, allocated, current_terminal)?;
        let last_seen =
            unsigned(owner.try_get("last_seen_ms").map_err(corrupt_row)?)?.max(latest_observed_ms);
        sqlx::query(
            "UPDATE venue_binance_grid_order_owners SET filled_quantity=$1,order_state=$2,\
             last_seen_ms=$3 WHERE instance_id=$4 AND client_order_id=$5",
        )
        .bind(decimal_text(filled))
        .bind(if terminal { "terminal" } else { "working" })
        .bind(ms(last_seen)?)
        .bind(&plan.mutation.instance_id)
        .bind(client_order_id)
        .execute(&mut **tx)
        .await
        .map_err(database_error)?;
    }
    Ok(())
}

fn next_owner_progress(
    original: rust_decimal::Decimal,
    current: rust_decimal::Decimal,
    allocated: rust_decimal::Decimal,
    current_terminal: bool,
) -> Result<(rust_decimal::Decimal, bool), GridStoreError> {
    if original <= rust_decimal::Decimal::ZERO
        || current < rust_decimal::Decimal::ZERO
        || allocated < rust_decimal::Decimal::ZERO
        || current > original
        || allocated > original
    {
        return Err(GridStoreError::Conflict);
    }
    let filled = current.max(allocated);
    Ok((filled, current_terminal || filled == original))
}

fn validate_batch(
    batch: &GridMutationBatch,
    now_ms: u64,
    allow_empty: bool,
) -> Result<[u8; 32], GridStoreError> {
    validate_ids(&[&batch.instance_id])?;
    let count = batch.placements.len() + batch.cancellations.len();
    if now_ms == 0
        || !bounded(&batch.batch_id, 1, 64)
        || batch.expected_instance_revision == 0
        || batch.config_revision == 0
        || batch.plan_revision == 0
        || (!allow_empty && count == 0)
        || count > MAX_GRID_MUTATION_BATCH_COMMANDS
    {
        return Err(GridStoreError::Invalid);
    }
    let mut command_ids = BTreeSet::new();
    let mut client_ids = BTreeSet::new();
    let mut semantic_keys = BTreeSet::new();
    let mut placement_identities = Vec::with_capacity(batch.placements.len());
    for placement in &batch.placements {
        validate_command(&placement.command, now_ms)?;
        validate_ownership(&placement.ownership)?;
        let (key, quantity, limit_price) = match &placement.command.intent {
            GridCommandIntent::LimitPostOnly {
                key,
                quantity,
                limit_price,
            } => (key, quantity, limit_price),
            _ => return Err(GridStoreError::Invalid),
        };
        if !command_matches_batch(&placement.command, batch)
            || placement.ownership.instance_id != batch.instance_id
            || placement.ownership.config_revision != batch.config_revision
            || placement.ownership.plan_revision != batch.plan_revision
            || placement.ownership.key != *key
            || placement.ownership.place_command_id != placement.command.command_id
            || placement.ownership.client_order_id != placement.command.client_order_id
            || placement.ownership.quantity != *quantity
            || placement.ownership.limit_price != *limit_price
            || !placement.ownership.filled_quantity.is_zero()
            || placement.ownership.native_order_id.is_some()
            || placement.ownership.first_seen_ms != now_ms
            || placement.ownership.last_seen_ms != now_ms
            || !insert_unique(
                &placement.command,
                &mut command_ids,
                &mut client_ids,
                &mut semantic_keys,
            )
        {
            return Err(GridStoreError::Invalid);
        }
        placement_identities.push(BatchPlacementIdentity {
            command: &placement.command,
            ownership_digest: ownership_identity_digest(&placement.ownership)?,
        });
    }
    for command in &batch.cancellations {
        validate_command(command, now_ms)?;
        if !matches!(command.intent, GridCommandIntent::Cancel { .. })
            || !command_matches_batch(command, batch)
            || !insert_unique(
                command,
                &mut command_ids,
                &mut client_ids,
                &mut semantic_keys,
            )
        {
            return Err(GridStoreError::Invalid);
        }
    }
    digest(&BatchIdentity {
        batch_id: &batch.batch_id,
        instance_id: &batch.instance_id,
        expected_instance_revision: batch.expected_instance_revision,
        config_revision: batch.config_revision,
        plan_revision: batch.plan_revision,
        desired_digest: batch.desired_digest,
        placements: placement_identities,
        cancellations: &batch.cancellations,
    })
}

fn insert_unique<'a>(
    command: &'a GridLedgerCommand,
    command_ids: &mut BTreeSet<&'a str>,
    client_ids: &mut BTreeSet<&'a str>,
    semantic_keys: &mut BTreeSet<&'a str>,
) -> bool {
    command_ids.insert(&command.command_id)
        && client_ids.insert(&command.client_order_id)
        && semantic_keys.insert(&command.semantic_key)
}

fn command_matches_batch(command: &GridLedgerCommand, batch: &GridMutationBatch) -> bool {
    command.instance_id == batch.instance_id
        && command.config_revision == batch.config_revision
        && command.plan_revision == batch.plan_revision
        && command.source_digest == batch.desired_digest
}

async fn lock_instance(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch: &GridMutationBatch,
    expected_plan_revision: u64,
    expected_desired_digest: Option<[u8; 32]>,
) -> Result<LockedInstance, GridStoreError> {
    let row = sqlx::query(
        "SELECT owner_user_id,trading_account_id,credential_id,symbol,instance_state,revision,\
         current_config_revision,plan_revision,desired_digest,convergence_started_ms,\
         grid_tail_batch_id \
         FROM venue_binance_grid_instances \
         WHERE instance_id=$1 FOR UPDATE",
    )
    .bind(&batch.instance_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(database_error)?
    .ok_or(GridStoreError::Forbidden)?;
    let desired = row
        .try_get::<Option<Vec<u8>>, _>("desired_digest")
        .map_err(corrupt_row)?
        .map(bytes_digest)
        .transpose()?;
    if unsigned(row.try_get("revision").map_err(corrupt_row)?)? != batch.expected_instance_revision
        || unsigned(
            row.try_get("current_config_revision")
                .map_err(corrupt_row)?,
        )? != batch.config_revision
        || unsigned(row.try_get("plan_revision").map_err(corrupt_row)?)? != expected_plan_revision
        || expected_desired_digest.is_some_and(|expected| desired != Some(expected))
    {
        return Err(GridStoreError::Conflict);
    }
    Ok(LockedInstance {
        owner_user_id: row.try_get("owner_user_id").map_err(corrupt_row)?,
        trading_account_id: row.try_get("trading_account_id").map_err(corrupt_row)?,
        credential_id: row.try_get("credential_id").map_err(corrupt_row)?,
        symbol: row.try_get("symbol").map_err(corrupt_row)?,
        state: super::decode_state(row.try_get("instance_state").map_err(corrupt_row)?)?,
        convergence_started_ms: row.try_get("convergence_started_ms").map_err(corrupt_row)?,
        desired_digest: desired,
        tail_batch_id: row.try_get("grid_tail_batch_id").map_err(corrupt_row)?,
    })
}

async fn validate_desired_placement(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch: &GridMutationBatch,
    placement: &GridBatchPlacement,
) -> Result<(), GridStoreError> {
    let (key, quantity, limit_price) = match &placement.command.intent {
        GridCommandIntent::LimitPostOnly {
            key,
            quantity,
            limit_price,
        } => (key, quantity, limit_price),
        _ => return Err(GridStoreError::Invalid),
    };
    let desired = sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM venue_binance_grid_desired_orders WHERE instance_id=$1 \
         AND config_revision=$2 AND plan_revision=$3 AND desired_digest=$4 AND semantic_key=$5 \
         AND client_order_id=$6 AND position_side=$7 AND order_role=$8 AND grid_level=$9 \
         AND order_sequence=$10 AND quantity=$11 AND limit_price=$12 FOR SHARE",
    )
    .bind(&batch.instance_id)
    .bind(integer(batch.config_revision)?)
    .bind(integer(batch.plan_revision)?)
    .bind(batch.desired_digest.as_slice())
    .bind(&placement.command.semantic_key)
    .bind(&placement.command.client_order_id)
    .bind(position_side_name(key.position_side))
    .bind(role_name(key.role))
    .bind(i16::try_from(key.level).map_err(|_| GridStoreError::Invalid)?)
    .bind(integer(key.sequence)?)
    .bind(decimal_text(*quantity))
    .bind(decimal_text(*limit_price))
    .fetch_optional(&mut **tx)
    .await
    .map_err(database_error)?;
    desired.map(|_| ()).ok_or(GridStoreError::Conflict)
}

async fn load_cancel_native_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    instance_id: &str,
    trading_account_id: &str,
    target_client_order_id: &str,
) -> Result<String, GridStoreError> {
    sqlx::query_scalar(
        "SELECT native_order_id FROM venue_binance_grid_order_owners WHERE instance_id=$1 \
         AND trading_account_id=$2 AND client_order_id=$3 AND native_order_id IS NOT NULL \
         FOR SHARE",
    )
    .bind(instance_id)
    .bind(trading_account_id)
    .bind(target_client_order_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(database_error)?
    .ok_or(GridStoreError::Conflict)
}

#[allow(clippy::too_many_arguments)]
async fn insert_command(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch: &GridMutationBatch,
    instance: &LockedInstance,
    command: &GridLedgerCommand,
    dispatch_sequence: u64,
    selected_native_order_id: Option<&str>,
    now_ms: u64,
) -> Result<(), GridStoreError> {
    let (phase, kind, position_side, order_side, quantity, limit_price, target) =
        command_columns(&command.intent)?;
    sqlx::query(
        "INSERT INTO venue_binance_commands \
         (command_id,command_origin,request_id,relation_id,relation_revision,target_revision,\
          owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,\
          order_kind,order_side,requested_quantity,limit_price,rule_version,client_order_id,\
          command_state,source_digest,created_ms,updated_ms,grid_instance_id,grid_config_revision,\
          grid_plan_revision,grid_semantic_key,selected_native_order_id,target_client_order_id,\
          grid_batch_id,dispatch_sequence) \
         VALUES ($1,'grid',NULL,NULL,NULL,NULL,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,\
                 'pending',$14,$15,$15,$16,$17,$18,$19,$20,$21,$22,$23)",
    )
    .bind(&command.command_id)
    .bind(&instance.owner_user_id)
    .bind(&instance.trading_account_id)
    .bind(&instance.credential_id)
    .bind(&instance.symbol)
    .bind(position_side)
    .bind(phase)
    .bind(kind)
    .bind(order_side)
    .bind(quantity)
    .bind(limit_price)
    .bind(&command.rule_version)
    .bind(&command.client_order_id)
    .bind(command.source_digest.as_slice())
    .bind(ms(now_ms)?)
    .bind(&batch.instance_id)
    .bind(integer(batch.config_revision)?)
    .bind(integer(batch.plan_revision)?)
    .bind(&command.semantic_key)
    .bind(selected_native_order_id)
    .bind(target)
    .bind(&batch.batch_id)
    .bind(integer(dispatch_sequence)?)
    .execute(&mut **tx)
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn insert_ownership(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    instance: &LockedInstance,
    ownership: &GridOrderOwnership,
) -> Result<(), GridStoreError> {
    if ownership.trading_account_id != instance.trading_account_id
        || ownership.symbol.to_string() != instance.symbol
    {
        return Err(GridStoreError::Conflict);
    }
    let ownership_digest = ownership_identity_digest(ownership)?;
    sqlx::query(
        "INSERT INTO venue_binance_grid_order_owners \
         (trading_account_id,client_order_id,instance_id,config_revision,plan_revision,\
          semantic_key,place_command_id,symbol,position_side,order_role,grid_level,order_sequence,\
          order_side,quantity,filled_quantity,limit_price,native_order_id,ownership_source,\
          order_state,ownership_digest,first_seen_ms,last_seen_ms) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NULL,'executor',\
                 $17,$18,$19,$19)",
    )
    .bind(&ownership.trading_account_id)
    .bind(&ownership.client_order_id)
    .bind(&ownership.instance_id)
    .bind(integer(ownership.config_revision)?)
    .bind(integer(ownership.plan_revision)?)
    .bind(ownership.key.encoded())
    .bind(&ownership.place_command_id)
    .bind(ownership.symbol.to_string())
    .bind(position_side_name(ownership.key.position_side))
    .bind(role_name(ownership.key.role))
    .bind(i16::try_from(ownership.key.level).map_err(|_| GridStoreError::Invalid)?)
    .bind(integer(ownership.key.sequence)?)
    .bind(order_side_name(ownership.key.order_side()))
    .bind(decimal_text(ownership.quantity))
    .bind(decimal_text(ownership.filled_quantity))
    .bind(decimal_text(ownership.limit_price))
    .bind(owned_state_name(ownership.state))
    .bind(ownership_digest.as_slice())
    .bind(ms(ownership.first_seen_ms)?)
    .execute(&mut **tx)
    .await
    .map_err(database_error)?;
    Ok(())
}

async fn verify_replay(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch: &GridMutationBatch,
    batch_digest: [u8; 32],
    command_count: u16,
    plan: Option<&GridPlanMutationBatch>,
    row: &sqlx::postgres::PgRow,
) -> Result<(), GridStoreError> {
    let stored_private_generation = row
        .try_get::<Option<i64>, _>("private_generation")
        .map_err(corrupt_row)?
        .map(unsigned)
        .transpose()?;
    let stored_private_observed_ms = row
        .try_get::<Option<i64>, _>("private_observed_ms")
        .map_err(corrupt_row)?
        .map(unsigned)
        .transpose()?;
    let stored_instrument_generation = row
        .try_get::<Option<i64>, _>("instrument_generation")
        .map_err(corrupt_row)?
        .map(unsigned)
        .transpose()?;
    let stored_source_event_received_ms = row
        .try_get::<Option<i64>, _>("source_event_received_ms")
        .map_err(corrupt_row)?
        .map(unsigned)
        .transpose()?;
    let stored_input_desired_digest = row
        .try_get::<Option<Vec<u8>>, _>("input_desired_digest")
        .map_err(corrupt_row)?
        .map(bytes_digest)
        .transpose()?;
    let stored_predecessor_batch_id = row
        .try_get::<Option<String>, _>("predecessor_batch_id")
        .map_err(corrupt_row)?;
    let facts_exact = match plan {
        Some(plan) => {
            stored_private_generation == Some(plan.expected_private_generation)
                && stored_private_observed_ms == Some(plan.expected_private_observed_ms)
                && stored_instrument_generation == Some(plan.anchor.instrument_generation)
                && stored_source_event_received_ms == plan.source_event_received_ms
                && stored_input_desired_digest == plan.expected_desired_digest
                && stored_predecessor_batch_id == plan.predecessor_batch_id
        }
        None => {
            stored_private_generation.is_none()
                && stored_private_observed_ms.is_none()
                && stored_instrument_generation.is_none()
                && stored_source_event_received_ms.is_none()
                && stored_input_desired_digest.is_none()
                && stored_predecessor_batch_id.is_none()
        }
    };
    let exact = row
        .try_get::<String, _>("instance_id")
        .map_err(corrupt_row)?
        == batch.instance_id
        && unsigned(
            row.try_get("expected_instance_revision")
                .map_err(corrupt_row)?,
        )? == batch.expected_instance_revision
        && unsigned(row.try_get("config_revision").map_err(corrupt_row)?)? == batch.config_revision
        && unsigned(row.try_get("plan_revision").map_err(corrupt_row)?)? == batch.plan_revision
        && bytes_digest(row.try_get("desired_digest").map_err(corrupt_row)?)?
            == batch.desired_digest
        && bytes_digest(row.try_get("batch_digest").map_err(corrupt_row)?)? == batch_digest
        && u16::try_from(
            row.try_get::<i16, _>("command_count")
                .map_err(corrupt_row)?,
        )
        .map_err(|_| GridStoreError::Corrupt)?
            == command_count
        && facts_exact;
    if !exact {
        return Err(GridStoreError::Conflict);
    }
    let materialized: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM venue_binance_commands WHERE grid_batch_id=$1 \
         AND dispatch_sequence BETWEEN 1 AND $2",
    )
    .bind(&batch.batch_id)
    .bind(i64::from(command_count))
    .fetch_one(&mut **tx)
    .await
    .map_err(database_error)?;
    let owned: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM venue_binance_grid_order_owners o \
         JOIN venue_binance_commands c ON c.command_id=o.place_command_id \
         WHERE c.grid_batch_id=$1",
    )
    .bind(&batch.batch_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(database_error)?;
    if materialized != i64::from(command_count)
        || owned != i64::try_from(batch.placements.len()).map_err(|_| GridStoreError::Corrupt)?
    {
        return Err(GridStoreError::Corrupt);
    }
    Ok(())
}

const fn cancel_state_allowed(state: GridInstanceState) -> bool {
    matches!(
        state,
        GridInstanceState::Running
            | GridInstanceState::Paused
            | GridInstanceState::StopPending
            | GridInstanceState::Blocked
            | GridInstanceState::ResetRequired
            | GridInstanceState::NeedsAttention
    )
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_control_protocol::grid::{GridOrderRole, GridOrderSemanticKey};
    use venue_domain::{PositionSide, Symbol};

    use super::*;
    use crate::grid_store::GridOwnedOrderState;

    #[test]
    fn batch_shape_is_place_first_and_rejects_duplicate_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut batch = fixture_batch()?;
        assert!(validate_batch(&batch, 10, false).is_ok());
        assert_eq!(batch.placements.len(), 2);
        assert_eq!(batch.cancellations.len(), 2);
        batch.cancellations[0].command_id = batch.placements[0].command.command_id.clone();
        assert_eq!(
            validate_batch(&batch, 10, false),
            Err(GridStoreError::Invalid)
        );
        Ok(())
    }

    #[test]
    fn stable_batch_digest_is_independent_of_mutable_ownership_observation()
    -> Result<(), Box<dyn std::error::Error>> {
        let batch = fixture_batch()?;
        let first = validate_batch(&batch, 10, false)?;
        let mut replay = batch;
        replay.placements[0].ownership.last_seen_ms = 11;
        assert_eq!(
            validate_batch(&replay, 11, false),
            Err(GridStoreError::Invalid)
        );
        for placement in &mut replay.placements {
            placement.ownership.first_seen_ms = 11;
            placement.ownership.last_seen_ms = 11;
        }
        assert_eq!(validate_batch(&replay, 11, false)?, first);
        Ok(())
    }

    #[test]
    fn allocated_fills_advance_owner_monotonically_and_only_full_is_terminal() {
        assert_eq!(
            next_owner_progress(Decimal::from(2), Decimal::ZERO, Decimal::ONE, false),
            Ok((Decimal::ONE, false))
        );
        assert_eq!(
            next_owner_progress(Decimal::from(2), Decimal::new(15, 1), Decimal::ONE, false),
            Ok((Decimal::new(15, 1), false))
        );
        assert_eq!(
            next_owner_progress(Decimal::from(2), Decimal::ONE, Decimal::from(2), false),
            Ok((Decimal::from(2), true))
        );
        assert_eq!(
            next_owner_progress(Decimal::from(2), Decimal::ONE, Decimal::from(3), false),
            Err(GridStoreError::Conflict)
        );
    }

    #[test]
    fn plan_receipt_digest_is_independent_of_fill_arrival_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let batch = fixture_batch()?;
        let mutation_digest = validate_batch(&batch, 10, false)?;
        let desired_orders = batch
            .placements
            .iter()
            .map(|placement| {
                let GridCommandIntent::LimitPostOnly {
                    key,
                    quantity,
                    limit_price,
                } = &placement.command.intent
                else {
                    return Err("fixture command is not a place");
                };
                Ok(GridDesiredOrder {
                    key: key.clone(),
                    client_order_id: placement.command.client_order_id.clone(),
                    quantity: *quantity,
                    limit_price: *limit_price,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fill = |native_trade_id: &str| GridFillAllocation {
            instance_id: batch.instance_id.clone(),
            trading_account_id: batch.placements[0].ownership.trading_account_id.clone(),
            config_revision: batch.config_revision,
            client_order_id: batch.placements[0].ownership.client_order_id.clone(),
            native_trade_id: native_trade_id.into(),
            symbol: batch.placements[0].ownership.symbol.clone(),
            position_side: batch.placements[0].ownership.key.position_side,
            role: batch.placements[0].ownership.key.role,
            quantity: Decimal::new(5, 1),
            price: batch.placements[0].ownership.limit_price,
            maker: Some(true),
            occurred_ms: Some(9),
            observed_ms: 10,
        };
        let fill_allocations = vec![fill("trade-2"), fill("trade-1")];
        let mut plan = GridPlanMutationBatch {
            mutation: batch,
            expected_plan_revision: 1,
            expected_desired_digest: Some([2; 32]),
            predecessor_batch_id: None,
            expected_private_generation: 1,
            expected_private_observed_ms: 10,
            source_event_received_ms: None,
            require_empty_account_queue: false,
            anchor: GridAnchor {
                revision: 2,
                instrument_generation: 1,
                price: Decimal::from(100),
                price_step: Decimal::ONE,
                grid_quantity: Decimal::ONE,
                source_native_trade_id: Some("trade-2".into()),
                observed_ms: 10,
            },
            desired_orders,
            fill_allocations,
            last_facts_ms: 10,
        };
        let forward = plan_batch_digest(&plan, mutation_digest)?;
        plan.fill_allocations.reverse();
        assert_eq!(plan_batch_digest(&plan, mutation_digest)?, forward);
        plan.expected_private_generation += 1;
        assert_ne!(plan_batch_digest(&plan, mutation_digest)?, forward);
        plan.expected_private_generation -= 1;
        plan.expected_private_observed_ms -= 1;
        assert_ne!(plan_batch_digest(&plan, mutation_digest)?, forward);
        plan.expected_private_observed_ms += 1;
        plan.source_event_received_ms = Some(10);
        assert_ne!(plan_batch_digest(&plan, mutation_digest)?, forward);
        plan.source_event_received_ms = None;
        plan.require_empty_account_queue = true;
        assert_ne!(plan_batch_digest(&plan, mutation_digest)?, forward);
        plan.require_empty_account_queue = false;
        plan.expected_desired_digest = Some([9; 32]);
        assert_ne!(plan_batch_digest(&plan, mutation_digest)?, forward);
        plan.expected_desired_digest = Some([2; 32]);
        plan.predecessor_batch_id = Some("gb-prior".into());
        assert_ne!(plan_batch_digest(&plan, mutation_digest)?, forward);
        Ok(())
    }

    fn fixture_batch() -> Result<GridMutationBatch, Box<dyn std::error::Error>> {
        let instance_id = "00000000-0000-4000-8000-000000000001";
        let account_id = "00000000-0000-4000-8000-000000000002";
        let symbol: Symbol = "BTC/USDT".parse()?;
        let desired_digest = [3; 32];
        let placements = [
            (
                PositionSide::Long,
                GridOrderRole::Close,
                "gp-one",
                "vgp-one",
                101,
            ),
            (
                PositionSide::Short,
                GridOrderRole::Open,
                "gp-two",
                "vgp-two",
                99,
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (side, role, command_id, client_id, price))| {
            let key = GridOrderSemanticKey {
                position_side: side,
                role,
                level: 1,
                sequence: u64::try_from(index + 1)?,
            };
            let command = GridLedgerCommand {
                command_id: command_id.into(),
                client_order_id: client_id.into(),
                instance_id: instance_id.into(),
                config_revision: 1,
                plan_revision: 2,
                semantic_key: key.encoded(),
                rule_version: "binance-pm-um-grid-r1".into(),
                source_digest: desired_digest,
                intent: GridCommandIntent::LimitPostOnly {
                    key: key.clone(),
                    quantity: Decimal::ONE,
                    limit_price: Decimal::from(price),
                },
            };
            Ok::<_, Box<dyn std::error::Error>>(GridBatchPlacement {
                ownership: GridOrderOwnership {
                    instance_id: instance_id.into(),
                    trading_account_id: account_id.into(),
                    config_revision: 1,
                    plan_revision: 2,
                    key,
                    place_command_id: command_id.into(),
                    client_order_id: client_id.into(),
                    symbol: symbol.clone(),
                    quantity: Decimal::ONE,
                    filled_quantity: Decimal::ZERO,
                    limit_price: Decimal::from(price),
                    native_order_id: None,
                    state: GridOwnedOrderState::Working,
                    first_seen_ms: 10,
                    last_seen_ms: 10,
                },
                command,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
        let cancellations = ["old-one", "old-two"]
            .into_iter()
            .enumerate()
            .map(|(index, target)| GridLedgerCommand {
                command_id: format!("gc-{index}"),
                client_order_id: format!("vgc-{index}"),
                instance_id: instance_id.into(),
                config_revision: 1,
                plan_revision: 2,
                semantic_key: format!("cancel:{target}"),
                rule_version: "binance-pm-um-grid".into(),
                source_digest: desired_digest,
                intent: GridCommandIntent::Cancel {
                    target_client_order_id: target.into(),
                },
            })
            .collect();
        Ok(GridMutationBatch {
            batch_id: "gb-0123456789abcdef".into(),
            instance_id: instance_id.into(),
            expected_instance_revision: 3,
            config_revision: 1,
            plan_revision: 2,
            desired_digest,
            placements,
            cancellations,
        })
    }
}
