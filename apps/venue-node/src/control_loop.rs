use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    future::Future,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountBalanceSummary, AccountDeliveryBinding,
    AccountHealthFact, AccountSummary, CONTROL_SCHEMA_VERSION, ConnectionState, ControlSnapshot,
    CopyDriftFact, CopyExecutionEvidence, CopyExecutionEvidenceEncoding, CopyExecutionFact,
    CopyExecutionPhaseProjection, CopyExecutionStateProjection, CopyLedgerFact,
    CopyLifecyclePolicy, CopyRelationRecord, ExecutionFactBinding, ExecutionFactsSnapshot,
    HealthState, NodeProjectionEnvelope, ReconciliationFact, SignedFillFact, SignedOrderFact,
    SignedPositionFact, StrategyKind as ProjectionStrategyKind, StrategyLifecycle, StrategySummary,
};
use venue_domain::{FieldState, PositionSide};
use venue_runtime::{
    AccountPhysicalGateway, CommandState, StrategyBinding, StrategyKind,
    account::{AccountHealth, InstanceLifecycle},
};

use crate::{
    ControlDeliveryDriver, ControlDeliveryDriverError, ControlDeliveryInbox, ControlDeliveryWork,
    ControlHttpClient, ControlHttpClientConfig, CopySemanticDelivery, NodeError, NodeLaunch,
    NodeProjectionOutbox, NodeProjectionOutboxError, NodeRuntimeConfig,
    OpaqueControlDeliveryJournal, ProductionResident,
};
use crate::{
    control_shutdown_journal::{ControlShutdownJournal, ShutdownPhase},
    production_resident::control::{cancel_command, reduce_command},
};

const MAX_CONSECUTIVE_TRANSPORT_FAILURES: u32 = 8;
const MAX_BACKOFF: Duration = Duration::from_secs(5);
// A complete account snapshot is a comparatively expensive signed read.  It is deliberately
// independent of Control's delivery cadence: a fast local poll must not turn into an exchange
// REST hot loop, while a disconnected Control process must not starve Runtime of private facts.
const MIN_SIGNED_PRIVATE_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

type FileDriver = ControlDeliveryDriver<OpaqueControlDeliveryJournal>;
type FileOutbox = NodeProjectionOutbox<OpaqueControlDeliveryJournal>;

mod copy_planning;
mod copy_reconciliation;
mod projection_digest;
#[cfg(any(
    feature = "binance",
    feature = "bitget",
    feature = "gate",
    feature = "bybit",
    feature = "okx",
    feature = "hyperliquid"
))]
mod public_stream;
mod pump;
use projection_digest::{envelope_digest, projection_digest_for};

/// The production resident Control loop. Every configured `(symbol, instance, epoch)` receives
/// its own durable inbox and projection cursor; the account Runtime remains the only Actor host.
pub struct ControlResidentLoop<G> {
    resident: ProductionResident<G>,
    client: ControlHttpClient,
    drivers: BTreeMap<String, FileDriver>,
    outboxes: BTreeMap<String, FileOutbox>,
    copy_jobs: BTreeMap<String, crate::CopyDeliveryJournal>,
    shutdowns: BTreeMap<String, ControlShutdownJournal>,
    bindings: BTreeMap<String, StrategyBinding>,
    copy_leader_capitals: BTreeMap<String, venue_domain::Amount>,
    copy_planning_instances: BTreeSet<String>,
    poll_interval: Duration,
    projection_interval: Duration,
    last_projection_ms: u64,
}

impl<G: AccountPhysicalGateway> ControlResidentLoop<G> {
    pub fn open(
        launch: &NodeLaunch,
        config: &NodeRuntimeConfig,
        resident: ProductionResident<G>,
    ) -> Result<Self, ControlResidentLoopError> {
        let client = ControlHttpClient::new(ControlHttpClientConfig::local(
            config.control.loopback_origin.clone(),
        ))?;
        let mut drivers = BTreeMap::new();
        let mut outboxes = BTreeMap::new();
        let mut copy_jobs = BTreeMap::new();
        let mut shutdowns = BTreeMap::new();
        let mut bindings = BTreeMap::new();
        let mut copy_leader_capitals = BTreeMap::new();
        let mut copy_planning_instances = BTreeSet::new();
        for strategy in &config.strategies {
            let binding = config
                .binding_for(strategy)
                .map_err(|_| ControlResidentLoopError::Config)?;
            let delivery = AccountDeliveryBinding {
                venue: config.venue,
                mode: config.mode,
                trading_account_id: config.trading_account_id.clone(),
                symbol: strategy.symbol.clone(),
                instance_id: strategy.instance_id.clone(),
                config_epoch: strategy.config_epoch,
            };
            let root = scoped_root(launch, strategy)?;
            fs::create_dir_all(&root).map_err(|_| ControlResidentLoopError::Artifacts)?;
            let inbox = ControlDeliveryInbox::recover(
                OpaqueControlDeliveryJournal::open(root.join("inbox.jsonl"))?,
                delivery.clone(),
                config.node_id.clone(),
            )?;
            let driver = ControlDeliveryDriver::new(
                client.clone(),
                inbox,
                config.control.lease_duration_ms,
                config.control.claim_limit,
            )?;
            let outbox = NodeProjectionOutbox::recover(
                OpaqueControlDeliveryJournal::open(root.join("projection.jsonl"))?,
                delivery.clone(),
                config.node_id.clone(),
            )?;
            let copy = crate::CopyDeliveryJournal::recover(
                root.join("copy-jobs.jsonl"),
                delivery.clone(),
            )?;
            let shutdown = ControlShutdownJournal::recover(
                root.join("control-shutdown.jsonl"),
                delivery.clone(),
            )?;
            if drivers
                .insert(strategy.instance_id.clone(), driver)
                .is_some()
                || outboxes
                    .insert(strategy.instance_id.clone(), outbox)
                    .is_some()
                || copy_jobs
                    .insert(strategy.instance_id.clone(), copy)
                    .is_some()
                || shutdowns
                    .insert(strategy.instance_id.clone(), shutdown)
                    .is_some()
                || bindings
                    .insert(strategy.instance_id.clone(), binding)
                    .is_some()
            {
                return Err(ControlResidentLoopError::Config);
            }
            if let Some(capital) = config.copy_leader_capital(strategy)
                && copy_leader_capitals
                    .insert(strategy.instance_id.clone(), capital)
                    .is_some()
            {
                return Err(ControlResidentLoopError::Config);
            }
            if strategy.strategy_kind == StrategyKind::Copy
                || config.copy_leader_capital(strategy).is_some()
            {
                copy_planning_instances.insert(strategy.instance_id.clone());
            }
        }
        Ok(Self {
            resident,
            client,
            drivers,
            outboxes,
            copy_jobs,
            shutdowns,
            bindings,
            copy_leader_capitals,
            copy_planning_instances,
            poll_interval: Duration::from_millis(config.control.poll_interval_ms),
            projection_interval: Duration::from_millis(config.control.projection_interval_ms),
            last_projection_ms: 0,
        })
    }

    /// Keeps retrying unavailable loopback Control with a capped backoff. Durability, scope, and
    /// protocol failures are never converted into retries that could conceal a fenced inbox.
    /// The resident and its synchronous adapter never execute inside Tokio.  The loop enters
    /// this dedicated runtime solely for loopback HTTP and signal-aware waiting, so adapters
    /// that own a Tokio runtime for signed reads cannot nest `block_on`.
    pub fn run(self) -> Result<(), NodeError> {
        self.run_with_signed_private_refresh()
    }

    /// Pumps adapter-authenticated account facts even when Control has no work.  This is the
    /// only generic non-Binance resident input currently available across all fixed adapters:
    /// each adapter proves a complete signed account snapshot through the Host, which then
    /// persists and routes it into Runtime.  Public BBO alone is intentionally not substituted
    /// for a bridged market book, so an incomplete venue feed cannot manufacture Scalping input.
    fn run_with_signed_private_refresh(self) -> Result<(), NodeError> {
        let mut last_refresh_ms = None;
        self.run_with_private_pump(move |resident| {
            let now = now_ms().map_err(|_| NodeError::ResidentRuntime)?;
            if last_refresh_ms.is_some_and(|previous| {
                now.saturating_sub(previous)
                    < MIN_SIGNED_PRIVATE_REFRESH_INTERVAL.as_millis() as u64
            }) {
                return Ok(false);
            }
            let refreshed = resident.refresh_signed_snapshot()?;
            last_refresh_ms = Some(now);
            Ok(refreshed.private_generation() != 0)
        })
    }

    fn tick(
        &mut self,
        http_runtime: &tokio::runtime::Runtime,
        now: u64,
    ) -> Result<bool, ControlResidentLoopError> {
        if self.last_projection_ms == 0 {
            // The first projection registers the exact durable strategy scope in Control. An
            // empty production database must see that scope before a claim can be authorized.
            if !self.publish_projections(http_runtime, now)? {
                return Ok(false);
            }
            self.last_projection_ms = now;
        }
        let ids = self.drivers.keys().cloned().collect::<Vec<_>>();
        for instance_id in ids {
            let mut copy_reconciliation_turns = Vec::new();
            let Some(work) = ({
                let driver = self
                    .drivers
                    .get_mut(&instance_id)
                    .ok_or(ControlResidentLoopError::Config)?;
                http_runtime.block_on(interruptible(async {
                    driver
                        .poll(now)
                        .await
                        .map_err(ControlResidentLoopError::from)
                }))?
            }) else {
                return Ok(false);
            };
            for item in work {
                match item {
                    ControlDeliveryWork::Actor(turn) => {
                        let binding = self
                            .bindings
                            .get(&instance_id)
                            .ok_or(ControlResidentLoopError::Config)?;
                        let action = match turn.payload() {
                            venue_control_protocol::AccountDeliveryPayload::ControlCommand(
                                command,
                            ) => Some(command.action),
                            venue_control_protocol::AccountDeliveryPayload::CopySemanticJob(_) => {
                                None
                            }
                        };
                        match action {
                            Some(venue_control_protocol::ControlAction::Trade) => {
                                let completion = match self.resident.apply_manual_trade(binding, &turn) {
                                    Ok(crate::production_resident::manual::ManualTradeOutcome::Applied(applied)) => completion_unless_expired(turn.applied_from_runtime(
                                        &applied,
                                        fresh_completion_ms()?,
                                        "manual trade Accepted and confirmed by a fresh signed account readback",
                                    ))?,
                                    Ok(crate::production_resident::manual::ManualTradeOutcome::Rejected { applied, detail }) => completion_unless_expired(turn.rejected(
                                        fresh_completion_ms()?,
                                        applied.durable_fact_digest().ok_or(ControlResidentLoopError::Config)?,
                                        detail,
                                    ))?,
                                    Ok(crate::production_resident::manual::ManualTradeOutcome::Unknown { applied, detail }) => completion_unless_expired(turn.unknown(
                                        fresh_completion_ms()?,
                                        applied.durable_fact_digest().ok_or(ControlResidentLoopError::Config)?,
                                        detail,
                                    ))?,
                                    Err(NodeError::ResidentRuntime) => completion_unless_expired(turn.rejected(
                                        fresh_completion_ms()?,
                                        [0; 32],
                                        "manual trade was rejected: unsupported binding, unsafe recovery state, or invalid signed scope",
                                    ))?,
                                    Err(error) => return Err(ControlResidentLoopError::Resident(error)),
                                };
                                let Some(completion) = completion else {
                                    continue;
                                };
                                if !self.submit_actor_completion(
                                    http_runtime,
                                    &instance_id,
                                    completion,
                                    now,
                                )? {
                                    return Ok(false);
                                }
                            }
                            Some(action) => {
                                let venue_control_protocol::AccountDeliveryPayload::ControlCommand(
                                    command,
                                ) = turn.payload()
                                else {
                                    return Err(ControlResidentLoopError::Config);
                                };
                                let applied = self
                                    .resident
                                    .apply_control_delivery(
                                        binding,
                                        &turn.lease().delivery_id,
                                        command,
                                    )
                                    .map_err(ControlResidentLoopError::Resident)?;
                                if matches!(
                                    action,
                                    venue_control_protocol::ControlAction::Stop
                                        | venue_control_protocol::ControlAction::Flatten
                                ) {
                                    self.shutdowns
                                        .get_mut(&instance_id)
                                        .ok_or(ControlResidentLoopError::Config)?
                                        .begin(action)
                                        .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
                                }
                                let completion =
                                    completion_unless_expired(turn.applied_from_runtime(
                                        &applied,
                                        fresh_completion_ms()?,
                                        control_applied_detail(action),
                                    ))?;
                                let Some(completion) = completion else {
                                    continue;
                                };
                                if !self.submit_actor_completion(
                                    http_runtime,
                                    &instance_id,
                                    completion,
                                    now,
                                )? {
                                    return Ok(false);
                                }
                            }
                            None => {
                                if !self.process_copy_actor_turn(
                                    http_runtime,
                                    &instance_id,
                                    turn,
                                    binding.clone(),
                                    now,
                                )? {
                                    return Ok(false);
                                }
                            }
                        }
                    }
                    ControlDeliveryWork::Reconcile(turn) => {
                        let binding = self
                            .bindings
                            .get(&instance_id)
                            .ok_or(ControlResidentLoopError::Config)?;
                        let is_manual_trade = matches!(
                            turn.payload(),
                            venue_control_protocol::AccountDeliveryPayload::ControlCommand(command)
                                if command.action == venue_control_protocol::ControlAction::Trade
                        );
                        let is_copy_delivery = matches!(
                            turn.payload(),
                            venue_control_protocol::AccountDeliveryPayload::CopySemanticJob(_)
                        );
                        if is_copy_delivery {
                            // Copy reconciliation is completed only after the already durable
                            // child command has newer signed proof.  Keep the exact lease turn
                            // until the common status-first pass below has updated its journal.
                            let venue_control_protocol::AccountDeliveryPayload::CopySemanticJob(
                                job,
                            ) = turn.payload()
                            else {
                                return Err(ControlResidentLoopError::Copy);
                            };
                            let durable = self
                                .copy_jobs
                                .get(&instance_id)
                                .ok_or(ControlResidentLoopError::Config)?
                                .jobs()
                                .get(&job.job_id);
                            if let Some(durable) = durable {
                                let original = durable
                                    .turn
                                    .restore()
                                    .map_err(|_| ControlResidentLoopError::Copy)?;
                                if original.payload() != turn.payload()
                                    || original.lease().delivery_id != turn.lease().delivery_id
                                    || original.lease().binding != turn.lease().binding
                                {
                                    return Err(ControlResidentLoopError::Copy);
                                }
                                self.copy_jobs
                                    .get_mut(&instance_id)
                                    .ok_or(ControlResidentLoopError::Config)?
                                    .freeze_cross_zero_continuation(job.job_id.clone())?;
                            }
                            copy_reconciliation_turns.push(turn);
                            continue;
                        }
                        if !is_manual_trade {
                            let venue_control_protocol::AccountDeliveryPayload::ControlCommand(
                                command,
                            ) = turn.payload()
                            else {
                                continue;
                            };
                            if let Some(account_fact_digest) = self
                                .resident
                                .reconcile_control_delivery(
                                    binding,
                                    &turn.lease().delivery_id,
                                    command,
                                )
                                .map_err(ControlResidentLoopError::Resident)?
                            {
                                if matches!(
                                    command.action,
                                    venue_control_protocol::ControlAction::Stop
                                        | venue_control_protocol::ControlAction::Flatten
                                ) && self
                                    .shutdowns
                                    .get(&instance_id)
                                    .and_then(ControlShutdownJournal::operation)
                                    .is_none_or(|operation| operation.action != command.action)
                                {
                                    continue;
                                }
                                let completion = completion_unless_expired(turn.reconciled(
                                    fresh_completion_ms()?,
                                    account_fact_digest,
                                    "exact request-bound lifecycle Actor checkpoint and Runtime state reconciled",
                                ))?;
                                if let Some(completion) = completion
                                    && !self.submit_reconciliation(
                                        http_runtime,
                                        &instance_id,
                                        completion,
                                        fresh_completion_ms()?,
                                    )?
                                {
                                    return Ok(false);
                                }
                            }
                            // Stop/Flatten keep their dedicated signed shutdown recovery after
                            // the semantic receipt. An older action-only checkpoint remains
                            // unresolved for an operator.
                            continue;
                        }
                        match self
                            .resident
                            .reconcile_manual_trade(binding, &turn)
                            .map_err(ControlResidentLoopError::Resident)?
                        {
                            crate::production_resident::manual::ManualTradeReconciliation::Pending => {}
                            crate::production_resident::manual::ManualTradeReconciliation::Reconciled {
                                account_fact_digest,
                                detail,
                            } => {
                                let completion = completion_unless_expired(turn.reconciled(
                                    fresh_completion_ms()?,
                                    account_fact_digest,
                                    detail,
                                ))?;
                                let Some(completion) = completion else {
                                    continue;
                                };
                                if !self.submit_reconciliation(
                                    http_runtime,
                                    &instance_id,
                                    completion,
                                    now,
                                )? {
                                    return Ok(false);
                                }
                            }
                        }
                    }
                }
            }
            self.recover_copy_actor_markers(&instance_id, now)?;
            if !self.reconcile_copy_jobs(&instance_id, http_runtime, now)? {
                return Ok(false);
            }
            for turn in copy_reconciliation_turns {
                if !self.complete_copy_reconciliation_turn(http_runtime, &instance_id, turn, now)? {
                    return Ok(false);
                }
            }
            self.advance_control_shutdown(&instance_id)?;
        }
        if self.last_projection_ms == 0
            || now.saturating_sub(self.last_projection_ms)
                >= self.projection_interval.as_millis() as u64
        {
            if !self.publish_projections(http_runtime, now)? {
                return Ok(false);
            }
            self.last_projection_ms = now;
        }
        Ok(true)
    }

    fn process_copy_actor_turn(
        &mut self,
        http_runtime: &tokio::runtime::Runtime,
        instance_id: &str,
        turn: crate::ActorDeliveryTurn,
        binding: StrategyBinding,
        now: u64,
    ) -> Result<bool, ControlResidentLoopError> {
        let venue_control_protocol::AccountDeliveryPayload::CopySemanticJob(job) = turn.payload()
        else {
            return Err(ControlResidentLoopError::Copy);
        };
        let job_id = job.job_id.clone();
        let persisted_turn = turn.persist_copy_turn()?;
        let existing = self
            .copy_jobs
            .get(instance_id)
            .and_then(|journal| journal.jobs().get(&job_id))
            .cloned();
        if let Some(existing) = existing {
            let journal = self
                .copy_jobs
                .get_mut(instance_id)
                .ok_or(ControlResidentLoopError::Config)?;
            journal.retain_delivery(job_id.clone(), persisted_turn, existing.relation)?;
            let delivery = CopySemanticDelivery::from_recovered_actor_turn(&turn, now)
                .map_err(|_| ControlResidentLoopError::Copy)?;
            let applied = self
                .resident
                .recover_copy_actor_applied(delivery, &binding)
                .map_err(|_| ControlResidentLoopError::Copy)?;
            let Some(applied) = applied else {
                // The request reached the local journal but no runtime Actor receipt survived.
                // Replanning under a later snapshot could preserve a child id while changing its
                // meaning, so report the durable gap as Unknown and never dispatch a replacement.
                let completion = turn.unknown(
                    now,
                    projection_digest_for("copy-missing-actor-applied", &job_id)?,
                    "copy request is durable but no matching Actor Applied receipt recovered; manual reconciliation required",
                )?;
                return self.submit_actor_completion(http_runtime, instance_id, completion, now);
            };
            self.copy_jobs
                .get_mut(instance_id)
                .ok_or(ControlResidentLoopError::Config)?
                .persist_actor_applied(job_id, now)?;
            let completion = turn.applied_from_copy_runtime(
                &applied,
                now,
                "recovered durable copy actor receipt; physical child remains subject to signed reconciliation",
            )?;
            return self.submit_actor_completion(http_runtime, instance_id, completion, now);
        }

        let Some(relations) = http_runtime.block_on(interruptible(async {
            self.client
                .copy_relations()
                .await
                .map_err(ControlResidentLoopError::from)
        }))?
        else {
            return Ok(false);
        };
        let delivery = CopySemanticDelivery::from_actor_turn(&turn, now)
            .map_err(|_| ControlResidentLoopError::Copy)?;
        let relation =
            copy_relation_for(&delivery, &relations).ok_or(ControlResidentLoopError::Copy)?;
        let (resident, copy_jobs) = (&mut self.resident, &mut self.copy_jobs);
        let journal = copy_jobs
            .get_mut(instance_id)
            .ok_or(ControlResidentLoopError::Config)?;
        journal.retain_delivery(job_id.clone(), persisted_turn, relation.clone())?;
        let result = resident
            .apply_copy_delivery(delivery, &binding, &relation, |request| {
                journal
                    .persist_request(job_id.clone(), request.clone())
                    .map_err(|_| crate::CopySemanticError::RuntimeUnavailable)
            })
            .map_err(|_| ControlResidentLoopError::Copy)?;
        journal.persist_actor_applied(job_id.clone(), now)?;
        if let Some(execution) = result.execution {
            journal.persist_execution(job_id.clone(), execution)?;
        } else {
            journal.persist_no_physical_delta(job_id.clone())?;
        }
        let completion = turn.applied_from_copy_runtime(
            &result.applied,
            now,
            "copy semantic turn durable; physical child remains subject to signed reconciliation",
        )?;
        self.submit_actor_completion(http_runtime, instance_id, completion, now)
    }

    fn reconcile_copy_jobs(
        &mut self,
        instance_id: &str,
        http_runtime: &tokio::runtime::Runtime,
        now: u64,
    ) -> Result<bool, ControlResidentLoopError> {
        let binding = self
            .bindings
            .get(instance_id)
            .cloned()
            .ok_or(ControlResidentLoopError::Config)?;
        let pending = self
            .copy_jobs
            .get(instance_id)
            .ok_or(ControlResidentLoopError::Config)?
            .jobs()
            .iter()
            .filter_map(|(job_id, job)| {
                (job.actor_applied_ms.is_some()
                    && !job.no_physical_delta
                    && !job.execution.as_ref().is_some_and(|execution| {
                        execution.state == venue_copy::CopyExecutionState::Reconciled
                    }))
                .then(|| {
                    job.request.clone().map(|request| {
                        (
                            job_id.clone(),
                            job.turn.clone(),
                            job.relation.clone(),
                            request,
                            job.fills.clone(),
                            job.reconciliation_only,
                        )
                    })
                })?
            })
            .collect::<Vec<_>>();
        for (job_id, turn, relation, request, previous_fills, reconciliation_only) in pending {
            let delivery = CopySemanticDelivery::from_recovered_actor_turn(&turn.restore()?, now)
                .map_err(|_| ControlResidentLoopError::Copy)?;
            let reconciliation = self
                .resident
                .reconcile_copy_delivery(delivery, &binding, &request, &previous_fills)
                .map_err(|_| ControlResidentLoopError::Copy)?;
            let continue_cross_zero = reconciliation.execution.state
                == venue_copy::CopyExecutionState::Reconciled
                && request.phase == venue_copy::CopyExecutionPhase::ReduceToZero
                && !request.target_exposure.value.is_zero()
                && reconciliation.position.exposure.value.is_zero();
            self.copy_jobs
                .get_mut(instance_id)
                .ok_or(ControlResidentLoopError::Config)?
                .persist_reconciliation(
                    job_id.clone(),
                    reconciliation.execution.clone(),
                    reconciliation.position,
                    reconciliation.fills.clone(),
                )?;
            if continue_cross_zero && !reconciliation_only {
                let delivery =
                    CopySemanticDelivery::from_recovered_actor_turn(&turn.restore()?, now)
                        .map_err(|_| ControlResidentLoopError::Copy)?;
                // A stale durable relation remains valid for reconciling its already admitted
                // reduce child, but it cannot open the opposite side. Re-read Control's current
                // configuration and require the same exact active commitment before phase two.
                let Some(relations) = http_runtime.block_on(interruptible(async {
                    self.client
                        .copy_relations()
                        .await
                        .map_err(ControlResidentLoopError::from)
                }))?
                else {
                    return Ok(false);
                };
                let current_relation = relations
                    .iter()
                    .find(|candidate| {
                        candidate.relation.relation_id == relation.relation.relation_id
                    })
                    .cloned()
                    .ok_or(ControlResidentLoopError::Copy)?;
                self.copy_jobs
                    .get_mut(instance_id)
                    .ok_or(ControlResidentLoopError::Config)?
                    .observe_relation(current_relation.clone())?;
                let exact_relation = copy_relation_for(&delivery, &relations)
                    .ok_or(ControlResidentLoopError::Copy)?;
                if exact_relation != current_relation {
                    return Err(ControlResidentLoopError::Copy);
                }
                let (resident, copy_jobs) = (&mut self.resident, &mut self.copy_jobs);
                let journal = copy_jobs
                    .get_mut(instance_id)
                    .ok_or(ControlResidentLoopError::Config)?;
                let next = resident
                    .continue_cross_zero_copy_delivery(
                        delivery,
                        &binding,
                        &exact_relation,
                        &reconciliation.execution,
                        &reconciliation.fills,
                        |next_request| {
                            journal
                                .persist_next_adjust_phase(job_id.clone(), next_request.clone())
                                .map_err(|_| crate::CopySemanticError::RuntimeUnavailable)
                        },
                    )
                    .map_err(|_| ControlResidentLoopError::Copy)?;
                journal.persist_actor_applied(job_id.clone(), now)?;
                if let Some(execution) = next.execution {
                    journal.persist_execution(job_id, execution)?;
                } else {
                    journal.persist_no_physical_delta(job_id)?;
                }
            }
        }
        Ok(true)
    }

    fn recover_copy_actor_markers(
        &mut self,
        instance_id: &str,
        now: u64,
    ) -> Result<(), ControlResidentLoopError> {
        let binding = self
            .bindings
            .get(instance_id)
            .cloned()
            .ok_or(ControlResidentLoopError::Config)?;
        let pending = self
            .copy_jobs
            .get(instance_id)
            .ok_or(ControlResidentLoopError::Config)?
            .jobs()
            .iter()
            .filter(|(_, job)| job.request.is_some() && job.actor_applied_ms.is_none())
            .map(|(job_id, job)| (job_id.clone(), job.turn.clone()))
            .collect::<Vec<_>>();
        for (job_id, turn) in pending {
            let delivery = CopySemanticDelivery::from_recovered_actor_turn(&turn.restore()?, now)
                .map_err(|_| ControlResidentLoopError::Copy)?;
            if self
                .resident
                .recover_copy_actor_applied(delivery, &binding)
                .map_err(|_| ControlResidentLoopError::Copy)?
                .is_some()
            {
                self.copy_jobs
                    .get_mut(instance_id)
                    .ok_or(ControlResidentLoopError::Config)?
                    .persist_actor_applied(job_id, now)?;
            }
        }
        Ok(())
    }

    /// Drives only the physical side effects already selected by a durable Stop/Flatten record.
    /// A semantic Control receipt is intentionally not a completion: every cancellation and
    /// reduction remains in the account WAL and is settled from a newer signed snapshot.
    fn advance_control_shutdown(
        &mut self,
        instance_id: &str,
    ) -> Result<(), ControlResidentLoopError> {
        let binding = self
            .bindings
            .get(instance_id)
            .cloned()
            .ok_or(ControlResidentLoopError::Config)?;
        let operation = self
            .shutdowns
            .get(instance_id)
            .and_then(|journal| journal.operation().cloned());
        let Some(operation) = operation else {
            return Ok(());
        };
        if matches!(
            operation.phase,
            ShutdownPhase::Reconciled | ShutdownPhase::NeedsAttention
        ) {
            return Ok(());
        }
        let snapshot = self
            .resident
            .control_shutdown_snapshot(&binding)
            .map_err(ControlResidentLoopError::Resident)?;
        if snapshot.connection_generation == 0
            || snapshot.private_generation == 0
            || snapshot.has_scope_conflict
        {
            self.shutdowns
                .get_mut(instance_id)
                .ok_or(ControlResidentLoopError::Config)?
                .set_phase(ShutdownPhase::NeedsAttention)
                .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
            return Ok(());
        }

        // A planned identity survives a crash before/after WAL preparation. Only a missing or
        // Prepared exact identity can be submitted; Submitted/Unknown is read back, never sent
        // again. Rejected shutdown mutations need an operator investigation.
        for planned in operation.commands.values() {
            match self
                .resident
                .reconcile_control_shutdown_command(planned.command.command_id())
                .map_err(ControlResidentLoopError::Resident)?
            {
                None | Some(CommandState::Prepared) => {
                    self.resident
                        .submit_control_shutdown_command(&binding, planned.command.clone())
                        .map_err(ControlResidentLoopError::Resident)?;
                    return Ok(());
                }
                Some(CommandState::Submitted | CommandState::Unknown { .. }) => return Ok(()),
                Some(CommandState::Rejected { .. }) => {
                    self.shutdowns
                        .get_mut(instance_id)
                        .ok_or(ControlResidentLoopError::Config)?
                        .set_phase(ShutdownPhase::NeedsAttention)
                        .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
                    return Ok(());
                }
                Some(CommandState::Accepted { .. }) => {}
            }
        }

        if !snapshot.owned_open_orders.is_empty() {
            self.shutdowns
                .get_mut(instance_id)
                .ok_or(ControlResidentLoopError::Config)?
                .set_phase(ShutdownPhase::CancelOwnedOrders)
                .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
            if let Some(order) = snapshot.owned_open_orders.first() {
                let previously_accepted_for_target = operation.commands.values().any(|planned| {
                    matches!(
                        &planned.command,
                        venue_domain::ExecutionCommand::Cancel(cancel)
                            if cancel.target_client_order_id == order.client_order_id
                    )
                });
                if previously_accepted_for_target {
                    // Reaching here means the prior identity is terminal Accepted (all
                    // Submitted/Unknown identities returned above), yet a newer signed snapshot
                    // still contains its target. A new cancel would be a semantic retry hidden
                    // behind a fresh generation, so require attention instead.
                    self.shutdowns
                        .get_mut(instance_id)
                        .ok_or(ControlResidentLoopError::Config)?
                        .set_phase(ShutdownPhase::NeedsAttention)
                        .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
                    return Ok(());
                }
                let command = cancel_command(&binding, order, snapshot.private_generation)
                    .map_err(ControlResidentLoopError::Resident)?;
                let command_id = command.command_id().clone();
                let already_planned = self
                    .shutdowns
                    .get(instance_id)
                    .and_then(|journal| journal.operation())
                    .is_some_and(|operation| operation.commands.contains_key(&command_id));
                if !already_planned {
                    self.shutdowns
                        .get_mut(instance_id)
                        .ok_or(ControlResidentLoopError::Config)?
                        .plan_command(command.clone(), snapshot.private_generation)
                        .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
                }
                self.resident
                    .submit_control_shutdown_command(&binding, command)
                    .map_err(ControlResidentLoopError::Resident)?;
                return Ok(());
            }
        }

        let nonzero_legs = snapshot
            .symbol_legs
            .iter()
            .filter(|leg| !leg.quantity.is_zero())
            .cloned()
            .collect::<Vec<_>>();
        if operation.action == venue_control_protocol::ControlAction::Stop {
            let phase = if nonzero_legs.is_empty() {
                ShutdownPhase::Reconciled
            } else {
                ShutdownPhase::ResidualPositionCustody
            };
            self.shutdowns
                .get_mut(instance_id)
                .ok_or(ControlResidentLoopError::Config)?
                .set_phase(phase)
                .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
            return Ok(());
        }
        if nonzero_legs.is_empty() {
            self.shutdowns
                .get_mut(instance_id)
                .ok_or(ControlResidentLoopError::Config)?
                .set_phase(ShutdownPhase::Reconciled)
                .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
            return Ok(());
        }
        self.shutdowns
            .get_mut(instance_id)
            .ok_or(ControlResidentLoopError::Config)?
            .set_phase(ShutdownPhase::ReduceOwnedPosition)
            .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
        if let Some(leg) = nonzero_legs.first() {
            let prior_quantity = operation.commands.values().find_map(|planned| {
                let venue_domain::ExecutionCommand::MarketReduce(reduce) = &planned.command else {
                    return None;
                };
                (reduce.position_side == leg.position_side).then_some(reduce.quantity)
            });
            if prior_quantity.is_some_and(|quantity| leg.quantity.abs() >= quantity) {
                // An accepted reduce did not result in a smaller signed leg. Do not create an
                // equal/larger replacement market order under a later generation.
                self.shutdowns
                    .get_mut(instance_id)
                    .ok_or(ControlResidentLoopError::Config)?
                    .set_phase(ShutdownPhase::NeedsAttention)
                    .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
                return Ok(());
            }
            let command = reduce_command(&binding, snapshot.private_generation, leg)
                .map_err(ControlResidentLoopError::Resident)?;
            let command_id = command.command_id().clone();
            let already_planned = self
                .shutdowns
                .get(instance_id)
                .and_then(|journal| journal.operation())
                .is_some_and(|operation| operation.commands.contains_key(&command_id));
            if !already_planned {
                self.shutdowns
                    .get_mut(instance_id)
                    .ok_or(ControlResidentLoopError::Config)?
                    .plan_command(command.clone(), snapshot.private_generation)
                    .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
            }
            self.resident
                .submit_control_shutdown_command(&binding, command)
                .map_err(ControlResidentLoopError::Resident)?;
            return Ok(());
        }
        Ok(())
    }

    fn submit_actor_completion(
        &mut self,
        http_runtime: &tokio::runtime::Runtime,
        instance_id: &str,
        completion: crate::ActorDeliveryCompletion,
        now: u64,
    ) -> Result<bool, ControlResidentLoopError> {
        let driver = self
            .drivers
            .get_mut(instance_id)
            .ok_or(ControlResidentLoopError::Config)?;
        Ok(http_runtime
            .block_on(interruptible(async {
                driver
                    .submit_actor_completion(completion, now)
                    .await
                    .map_err(ControlResidentLoopError::from)
            }))?
            .is_some())
    }

    fn submit_reconciliation(
        &mut self,
        http_runtime: &tokio::runtime::Runtime,
        instance_id: &str,
        completion: crate::ReconciliationCompletion,
        now: u64,
    ) -> Result<bool, ControlResidentLoopError> {
        let driver = self
            .drivers
            .get_mut(instance_id)
            .ok_or(ControlResidentLoopError::Config)?;
        Ok(http_runtime
            .block_on(interruptible(async {
                driver
                    .submit_reconciliation(completion, now)
                    .await
                    .map_err(ControlResidentLoopError::from)
            }))?
            .is_some())
    }

    fn publish_projections(
        &mut self,
        http_runtime: &tokio::runtime::Runtime,
        now: u64,
    ) -> Result<bool, ControlResidentLoopError> {
        let signed = self
            .resident
            .refresh_signed_snapshot()
            .map_err(ControlResidentLoopError::Resident)?;
        let planning_enabled = self
            .outboxes
            .keys()
            .any(|instance_id| self.copy_planning_instances.contains(instance_id));
        let relations = if planning_enabled {
            let Some(relations) = http_runtime.block_on(interruptible(async {
                self.client
                    .copy_relations()
                    .await
                    .map_err(ControlResidentLoopError::from)
            }))?
            else {
                return Ok(false);
            };
            relations
        } else {
            Vec::new()
        };
        // A rules read may take most of the private-observation window. Stamp the envelope only
        // after that read, so Copy evidence cannot be emitted merely because the tick began while
        // the signed page was fresh.
        let generated_ms = if planning_enabled {
            now_ms()
                .map_err(|_| ControlResidentLoopError::ProjectionEncoding)?
                .max(signed.observed_at_ms())
        } else {
            now.max(signed.observed_at_ms())
        };
        let wal_fill_owners = signed
            .fills()
            .iter()
            .filter_map(|fill| {
                self.resident
                    .owner_for_signed_fill(fill)
                    .map(|owner| ((fill.symbol.clone(), fill.fill_id.clone()), owner))
            })
            .collect::<BTreeMap<_, _>>();
        let ids = self.outboxes.keys().cloned().collect::<Vec<_>>();
        for instance_id in ids {
            let binding = self
                .bindings
                .get(&instance_id)
                .ok_or(ControlResidentLoopError::Config)?;
            let node_id = self
                .drivers
                .get(&instance_id)
                .map(|driver| driver.inbox().node_id().to_owned())
                .ok_or(ControlResidentLoopError::Config)?;
            let delivery = delivery_binding(binding);
            let fact_binding = ExecutionFactBinding {
                venue: delivery.venue,
                mode: delivery.mode,
                trading_account_id: delivery.trading_account_id.clone(),
                symbol: delivery.symbol.clone(),
                instance_id: delivery.instance_id.clone(),
                config_epoch: delivery.config_epoch,
            };
            let projection = projection_from_signed(
                &signed,
                &wal_fill_owners,
                binding,
                self.resident.strategy_lifecycle(binding),
                self.resident.scalping_entry_safety_unwired(binding),
                self.resident.runtime().health(),
                self.resident.has_unresolved(),
                generated_ms,
                self.copy_jobs
                    .get(&instance_id)
                    .ok_or(ControlResidentLoopError::Config)?,
            )?;
            let copy_execution_evidence = copy_execution_evidence(
                self.copy_jobs
                    .get(&instance_id)
                    .ok_or(ControlResidentLoopError::Config)?,
                &fact_binding,
                generated_ms,
            )?;
            let copy_planning_facts = self
                .copy_planning_instances
                .contains(&instance_id)
                .then_some(())
                .and_then(|_| self.resident.current_instrument_for(&delivery.symbol).ok())
                .as_ref()
                .map(|instrument| {
                    copy_planning::signed_copy_planning_facts(
                        &signed,
                        instrument,
                        &relations,
                        &fact_binding,
                        self.resident.strategy_lifecycle(binding),
                        self.copy_leader_capitals.get(&instance_id),
                        generated_ms,
                    )
                })
                .transpose()?
                .unwrap_or_default();
            let acknowledged = {
                let outbox = self
                    .outboxes
                    .get_mut(&instance_id)
                    .ok_or(ControlResidentLoopError::Config)?;
                let (sequence, previous_digest) = outbox.next_cursor(1);
                let digest = envelope_digest(
                    &projection.0,
                    &projection.1,
                    &copy_execution_evidence,
                    &copy_planning_facts,
                    sequence,
                    previous_digest,
                )?;
                outbox.enqueue(NodeProjectionEnvelope {
                    schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
                    binding: delivery,
                    node_id,
                    node_generation: 1,
                    sequence,
                    previous_digest,
                    digest,
                    copy_execution_evidence,
                    copy_planning_facts,
                    snapshot: projection.0,
                    facts: projection.1,
                })?;
                http_runtime.block_on(interruptible(async {
                    outbox
                        .flush(&self.client)
                        .await
                        .map_err(ControlResidentLoopError::from)
                }))?
            };
            let Some(acknowledged) = acknowledged else {
                return Ok(false);
            };
            self.mark_copy_execution_evidence(&instance_id, &acknowledged)?;
        }
        Ok(true)
    }

    fn mark_copy_execution_evidence(
        &mut self,
        instance_id: &str,
        acknowledged: &[NodeProjectionEnvelope],
    ) -> Result<(), ControlResidentLoopError> {
        let journal = self
            .copy_jobs
            .get_mut(instance_id)
            .ok_or(ControlResidentLoopError::Config)?;
        for envelope in acknowledged {
            for evidence in &envelope.copy_execution_evidence {
                let phase = match evidence.phase {
                    CopyExecutionPhaseProjection::ReduceToZero => {
                        venue_copy::CopyExecutionPhase::ReduceToZero
                    }
                    CopyExecutionPhaseProjection::Adjust => venue_copy::CopyExecutionPhase::Adjust,
                };
                journal.mark_execution_projected(
                    evidence.job_id.clone(),
                    phase,
                    evidence.result_sha256,
                )?;
            }
        }
        Ok(())
    }
}

async fn interruptible<T, F>(operation: F) -> Result<Option<T>, ControlResidentLoopError>
where
    F: Future<Output = Result<T, ControlResidentLoopError>>,
{
    tokio::select! {
        result = operation => result.map(Some),
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|_| ControlResidentLoopError::Signal)?;
            Ok(None)
        }
    }
}

fn wait_or_interrupt(
    runtime: &tokio::runtime::Runtime,
    duration: Duration,
) -> Result<bool, ControlResidentLoopError> {
    runtime.block_on(async {
        tokio::select! {
            _ = tokio::time::sleep(duration) => Ok(true),
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|_| ControlResidentLoopError::Signal)?;
                Ok(false)
            }
        }
    })
}

fn scoped_root(
    launch: &NodeLaunch,
    strategy: &crate::NodeRuntimeStrategy,
) -> Result<PathBuf, ControlResidentLoopError> {
    if strategy.instance_id.is_empty() || strategy.config_epoch == 0 {
        return Err(ControlResidentLoopError::Config);
    }
    Ok(launch
        .artifacts_root()
        .join("control")
        .join(strategy.symbol.to_string().replace('/', "_"))
        .join(&strategy.instance_id)
        .join(strategy.config_epoch.to_string()))
}

fn delivery_binding(binding: &StrategyBinding) -> AccountDeliveryBinding {
    AccountDeliveryBinding {
        venue: binding.key.account.exchange,
        mode: venue_gateway_api::GatewayMode::Live,
        trading_account_id: binding.key.account.account.clone(),
        symbol: binding.key.symbol.clone(),
        instance_id: binding.key.instance_id.clone(),
        config_epoch: 1,
    }
}

#[allow(clippy::too_many_arguments)]
fn projection_from_signed(
    signed: &venue_runtime::SignedAccountSnapshot,
    wal_fill_owners: &BTreeMap<(venue_domain::Symbol, String), venue_domain::OrderOwner>,
    binding: &StrategyBinding,
    lifecycle: Option<InstanceLifecycle>,
    scalping_entry_safety_unwired: bool,
    runtime_health: AccountHealth,
    unresolved: bool,
    generated_ms: u64,
    copy_jobs: &crate::CopyDeliveryJournal,
) -> Result<(ControlSnapshot, ExecutionFactsSnapshot), ControlResidentLoopError> {
    let delivery = delivery_binding(binding);
    if signed.binding().venue != delivery.venue
        || signed.binding().mode != delivery.mode
        || signed.binding().trading_account_id != delivery.trading_account_id
        || signed.observed_at_ms() == 0
        || signed.observed_at_ms() > generated_ms
    {
        return Err(ControlResidentLoopError::ProjectionScope);
    }
    let fact_binding = ExecutionFactBinding {
        venue: delivery.venue,
        mode: delivery.mode,
        trading_account_id: delivery.trading_account_id.clone(),
        symbol: delivery.symbol.clone(),
        instance_id: delivery.instance_id.clone(),
        config_epoch: delivery.config_epoch,
    };
    let balances = signed
        .balances()
        .iter()
        .map(|balance| AccountBalanceSummary {
            asset: balance.asset.clone(),
            equity: balance.equity,
            available_margin: balance.available_margin,
        })
        .collect::<Vec<_>>();
    let mut owned_order_ids = std::collections::BTreeSet::new();
    let mut orders = Vec::new();
    for order in signed.open_orders() {
        let Some(owner) = order.owner.as_ref() else {
            continue;
        };
        if order.external || !binding.matches_owner(owner) {
            continue;
        }
        let order_id = order
            .venue_order_id
            .clone()
            .unwrap_or_else(|| order.client_order_id.clone());
        owned_order_ids.insert(order_id.clone());
        owned_order_ids.insert(order.client_order_id.clone());
        orders.push(SignedOrderFact {
            binding: fact_binding.clone(),
            order_id,
            client_order_id: Some(order.client_order_id.clone()),
            state: order.state,
            side: order.side,
            position_side: order.position_side,
            quantity: order.quantity,
            filled_quantity: order.filled_quantity,
            limit_price: order.limit_price,
            reduce_only: order.reduce_only,
            signed_generation: signed.private_generation(),
            observed_ms: signed.observed_at_ms(),
            fact_digest: projection_digest_for("order", order)?,
        });
    }
    let positions = signed
        .positions()
        .iter()
        .filter(|position| position.symbol == delivery.symbol)
        .map(|position| {
            Ok(SignedPositionFact {
                binding: fact_binding.clone(),
                position_side: position.position_side,
                quantity: position.quantity,
                entry_price: position.entry_price,
                mark_price: position.mark_price,
                signed_generation: signed.private_generation(),
                observed_ms: signed.observed_at_ms(),
                fact_digest: projection_digest_for("position", position)?,
            })
        })
        .collect::<Result<Vec<_>, ControlResidentLoopError>>()?;
    let mut fills = Vec::new();
    for fill in signed.fills() {
        let wal_owned = wal_fill_owners
            .get(&(fill.symbol.clone(), fill.fill_id.clone()))
            .is_some_and(|owner| binding.matches_owner(owner));
        if fill.symbol != delivery.symbol
            || (!wal_owned && !owned_order_ids.contains(&fill.order_id))
        {
            continue;
        }
        let execution_sequence = match fill.execution_sequence {
            FieldState::Known(sequence) => Some(sequence),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => None,
        };
        let position_side = match fill.position_side {
            FieldState::Known(side) => Some(side),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => None,
        };
        let occurred_ms = fill.exchange_time_ms.unwrap_or(signed.observed_at_ms());
        fills.push(SignedFillFact {
            binding: fact_binding.clone(),
            fill_id: fill.fill_id.clone(),
            order_id: fill.order_id.clone(),
            side: fill.side,
            position_side,
            quantity: fill.quantity,
            price: fill.price.value(),
            execution_sequence,
            occurred_ms,
            signed_generation: signed.private_generation(),
            fact_digest: projection_digest_for("fill", fill)?,
        });
    }
    append_reconciled_copy_fills(&mut fills, copy_jobs, &fact_binding, generated_ms)?;
    let (long_quantity, short_quantity) =
        position_quantities(signed.positions(), &delivery.symbol)?;
    let health = projection_health(runtime_health, unresolved);
    let strategy_lifecycle = projection_lifecycle(lifecycle, health, scalping_entry_safety_unwired);
    let snapshot = ControlSnapshot {
        schema_version: CONTROL_SCHEMA_VERSION,
        generated_ms,
        connection: match health {
            HealthState::Healthy => ConnectionState::Live,
            HealthState::Recovering => ConnectionState::Connecting,
            HealthState::NeedsAttention | HealthState::Unknown | HealthState::Stopped => {
                ConnectionState::Degraded
            }
        },
        accounts: vec![AccountSummary {
            venue: delivery.venue,
            mode: delivery.mode,
            trading_account_id: delivery.trading_account_id.clone(),
            health,
            equity: None,
            available_margin: None,
            unrealized_pnl: None,
            balances,
            private_generation: signed.private_generation(),
            writer_generation: 0,
            last_reconciled_ms: signed.observed_at_ms(),
        }],
        strategies: vec![StrategySummary {
            instance_id: delivery.instance_id.clone(),
            kind: projection_kind(binding.key.strategy_kind),
            venue: delivery.venue,
            mode: delivery.mode,
            trading_account_id: delivery.trading_account_id.clone(),
            symbol: delivery.symbol.clone(),
            lifecycle: strategy_lifecycle,
            config_epoch: delivery.config_epoch,
            open_orders: u32::try_from(orders.len())
                .map_err(|_| ControlResidentLoopError::ProjectionEncoding)?,
            long_quantity,
            short_quantity,
            realized_pnl: None,
            unrealized_pnl: None,
            last_receipt_ms: signed.observed_at_ms(),
            attention: if scalping_entry_safety_unwired
                && lifecycle == Some(InstanceLifecycle::Running)
                && health == HealthState::Healthy
            {
                Some("scalping entry remains blocked: signed safety and StopMarket protection are unavailable".to_owned())
            } else {
                (health != HealthState::Healthy)
                    .then(|| "runtime recovery or unresolved state requires attention".to_owned())
            },
        }],
        copy_relations: Vec::new(),
        markets: Vec::new(),
        ledger: Vec::new(),
    };
    let facts = ExecutionFactsSnapshot {
        schema_version: CONTROL_SCHEMA_VERSION,
        generated_ms,
        orders,
        positions,
        fills,
        reconciliation: vec![ReconciliationFact {
            binding: fact_binding.clone(),
            signed_generation: signed.private_generation(),
            reconciled_ms: signed.observed_at_ms(),
            complete_order_families: signed.unknown_results().is_empty(),
            complete_position_legs: true,
            fact_digest: projection_digest_for("reconciliation", signed)?,
        }],
        copy_ledger: copy_ledger_facts(copy_jobs, &fact_binding, generated_ms)?,
        drift: copy_drift_facts(copy_jobs, &fact_binding, generated_ms)?,
        execution: copy_execution_facts(copy_jobs, &fact_binding, generated_ms)?,
        // Snapshot has no signed cross-currency risk aggregate. Do not derive one from balance
        // or position values, which would silently manufacture an FX conversion.
        risk: Vec::new(),
        health: vec![AccountHealthFact {
            venue: delivery.venue,
            mode: delivery.mode,
            trading_account_id: delivery.trading_account_id,
            health,
            private_generation: signed.private_generation(),
            last_reconciled_ms: signed.observed_at_ms(),
            observed_ms: signed.observed_at_ms(),
            fact_digest: projection_digest_for("health", signed)?,
        }],
    };
    Ok((snapshot, facts))
}

fn position_quantities(
    positions: &[venue_runtime::SignedAccountPositionFact],
    symbol: &venue_domain::Symbol,
) -> Result<(rust_decimal::Decimal, rust_decimal::Decimal), ControlResidentLoopError> {
    let mut long = rust_decimal::Decimal::ZERO;
    let mut short = rust_decimal::Decimal::ZERO;
    for position in positions
        .iter()
        .filter(|position| &position.symbol == symbol)
    {
        let quantity = position.quantity.abs();
        match position.position_side {
            PositionSide::Long => {
                long = long
                    .checked_add(quantity)
                    .ok_or(ControlResidentLoopError::ProjectionEncoding)?;
            }
            PositionSide::Short => {
                short = short
                    .checked_add(quantity)
                    .ok_or(ControlResidentLoopError::ProjectionEncoding)?;
            }
            PositionSide::Net
                if position.quantity.is_sign_positive() && !position.quantity.is_zero() =>
            {
                long = long
                    .checked_add(quantity)
                    .ok_or(ControlResidentLoopError::ProjectionEncoding)?;
            }
            PositionSide::Net if position.quantity.is_sign_negative() => {
                short = short
                    .checked_add(quantity)
                    .ok_or(ControlResidentLoopError::ProjectionEncoding)?;
            }
            PositionSide::Net => {}
        }
    }
    Ok((long, short))
}

fn projection_health(runtime_health: AccountHealth, unresolved: bool) -> HealthState {
    if unresolved {
        HealthState::NeedsAttention
    } else {
        match runtime_health {
            AccountHealth::Ready => HealthState::Healthy,
            AccountHealth::Starting => HealthState::Recovering,
            AccountHealth::Frozen => HealthState::NeedsAttention,
        }
    }
}

fn projection_lifecycle(
    lifecycle: Option<InstanceLifecycle>,
    health: HealthState,
    scalping_entry_safety_unwired: bool,
) -> StrategyLifecycle {
    match lifecycle {
        Some(InstanceLifecycle::Registered | InstanceLifecycle::Recovering) => {
            StrategyLifecycle::Rebuilding
        }
        Some(InstanceLifecycle::Running)
            if health == HealthState::Healthy && scalping_entry_safety_unwired =>
        {
            StrategyLifecycle::NeedsAttention
        }
        Some(InstanceLifecycle::Running) if health == HealthState::Healthy => {
            StrategyLifecycle::Running
        }
        Some(InstanceLifecycle::Running) => StrategyLifecycle::NeedsAttention,
        Some(InstanceLifecycle::Paused) => StrategyLifecycle::Paused,
        Some(InstanceLifecycle::Stopping) => StrategyLifecycle::Stopping,
        Some(InstanceLifecycle::Faulted | InstanceLifecycle::NeedsAttention) | None => {
            StrategyLifecycle::NeedsAttention
        }
    }
}

fn projection_kind(kind: StrategyKind) -> ProjectionStrategyKind {
    match kind {
        StrategyKind::HedgedGrid => ProjectionStrategyKind::Grid,
        StrategyKind::Scalping => ProjectionStrategyKind::Scalping,
        StrategyKind::Manual => ProjectionStrategyKind::Manual,
        StrategyKind::Copy => ProjectionStrategyKind::Copy,
    }
}

fn copy_relation_for(
    delivery: &CopySemanticDelivery,
    relations: &[CopyRelationRecord],
) -> Option<CopyRelationRecord> {
    let commitment = &delivery.manifest().binding.relation;
    relations
        .iter()
        .find(|record| {
            record.relation.lifecycle == CopyLifecyclePolicy::Active
                && record.relation.relation_id == commitment.relation_id.to_string()
                && record.revision == commitment.revision
                && record.relation.policy_digest() == commitment.policy_digest
        })
        .cloned()
}

const MAX_COPY_EXECUTION_EVIDENCE_BYTES: usize = 48 * 1024;
const MAX_COPY_EXECUTION_RESULT_BYTES: usize = 16 * 1024;

/// Selects only durable Copy results whose exact Control echo has not yet been recorded. The
/// current and prior cross-zero results stay in the existing Copy journal; projection merely
/// transports their fixed result encoding and never reconstructs them from UI facts.
fn copy_execution_evidence(
    journal: &crate::CopyDeliveryJournal,
    binding: &ExecutionFactBinding,
    generated_ms: u64,
) -> Result<Vec<CopyExecutionEvidence>, ControlResidentLoopError> {
    let mut evidence = Vec::new();
    let mut encoded_bytes = 0_usize;
    for (job_id, job) in journal
        .jobs()
        .iter()
        .filter(|(_, job)| copy_job_matches_binding(job, binding))
    {
        let mut reduce_pending = false;
        for completed in &job.prior_phases {
            if completed.request.phase != venue_copy::CopyExecutionPhase::ReduceToZero {
                continue;
            }
            let encoded = serde_json::to_string(&completed.execution)
                .map_err(|_| ControlResidentLoopError::ProjectionEncoding)?;
            let digest: [u8; 32] = Sha256::digest(encoded.as_bytes()).into();
            if !job.projected_executions.iter().any(|projected| {
                projected.phase == venue_copy::CopyExecutionPhase::ReduceToZero
                    && projected.result_sha256 == digest
            }) {
                reduce_pending = true;
                break;
            }
        }
        let mut phases = job
            .prior_phases
            .iter()
            .map(|completed| (&completed.request, &completed.execution))
            .collect::<Vec<_>>();
        if let (Some(request), Some(execution)) = (&job.request, &job.execution) {
            phases.push((request, execution));
        }
        for (request, result) in phases {
            if request != &result.request
                || (result.state == venue_copy::CopyExecutionState::Reconciled
                    && result.fact_digest == [0; 32])
                || result.observed_at_ms == 0
                || result.observed_at_ms > generated_ms
            {
                return Err(ControlResidentLoopError::ProjectionEncoding);
            }
            let phase = copy_execution_phase(request.phase);
            let result_bytes = serde_json::to_string(result)
                .map_err(|_| ControlResidentLoopError::ProjectionEncoding)?;
            let result_sha256 = Sha256::digest(result_bytes.as_bytes()).into();
            if job.projected_executions.iter().any(|projected| {
                projected.phase == request.phase && projected.result_sha256 == result_sha256
            }) {
                continue;
            }
            // A cross-zero Adjust is evidence only after the preceding Reduce is either already
            // echoed or placed earlier in this same bounded batch.
            if request.phase == venue_copy::CopyExecutionPhase::Adjust && reduce_pending {
                break;
            }
            let result_len = result_bytes.len();
            if result_len == 0 || result_len > MAX_COPY_EXECUTION_RESULT_BYTES {
                return Err(ControlResidentLoopError::ProjectionEncoding);
            }
            let next = encoded_bytes
                .checked_add(result_len.saturating_add(256))
                .ok_or(ControlResidentLoopError::ProjectionEncoding)?;
            if next > MAX_COPY_EXECUTION_EVIDENCE_BYTES || evidence.len() == 16 {
                return Ok(evidence);
            }
            evidence.push(CopyExecutionEvidence {
                encoding: CopyExecutionEvidenceEncoding::VenueCopyExecutionResultV1,
                relation_id: job.relation.relation.relation_id.clone(),
                relation_revision: job.relation.revision,
                job_id: job_id.clone(),
                binding: binding.clone(),
                phase,
                state: copy_execution_state(result.state),
                command_id: result.command_id.clone(),
                observed_ms: result.observed_at_ms,
                result_fact_digest: result.fact_digest,
                result_sha256,
                result_bytes,
            });
            encoded_bytes = next;
            if request.phase == venue_copy::CopyExecutionPhase::ReduceToZero {
                reduce_pending = false;
            }
        }
    }
    Ok(evidence)
}

fn copy_execution_phase(phase: venue_copy::CopyExecutionPhase) -> CopyExecutionPhaseProjection {
    match phase {
        venue_copy::CopyExecutionPhase::ReduceToZero => CopyExecutionPhaseProjection::ReduceToZero,
        venue_copy::CopyExecutionPhase::Adjust => CopyExecutionPhaseProjection::Adjust,
    }
}

fn copy_execution_facts(
    journal: &crate::CopyDeliveryJournal,
    binding: &ExecutionFactBinding,
    generated_ms: u64,
) -> Result<Vec<CopyExecutionFact>, ControlResidentLoopError> {
    journal
        .jobs()
        .iter()
        .filter(|(_, job)| copy_job_matches_binding(job, binding) && job.actor_applied_ms.is_some())
        .map(|(job_id, job)| {
            let (state, command_id, observed_ms) = match &job.execution {
                Some(execution) => (
                    copy_execution_state(execution.state),
                    execution.command_id.clone(),
                    execution.observed_at_ms,
                ),
                None => (
                    CopyExecutionStateProjection::SemanticApplied,
                    None,
                    job.actor_applied_ms
                        .ok_or(ControlResidentLoopError::ProjectionEncoding)?,
                ),
            };
            if observed_ms == 0 || observed_ms > generated_ms {
                return Err(ControlResidentLoopError::ProjectionEncoding);
            }
            let fact = CopyExecutionFact {
                relation_id: job.relation.relation.relation_id.clone(),
                relation_revision: job.relation.revision,
                job_id: job_id.clone(),
                binding: binding.clone(),
                state,
                command_id,
                observed_ms,
                fact_digest: projection_digest_for("copy-execution", &(job_id, &job.execution))?,
            };
            Ok(fact)
        })
        .collect()
}

fn append_reconciled_copy_fills(
    facts: &mut Vec<SignedFillFact>,
    journal: &crate::CopyDeliveryJournal,
    binding: &ExecutionFactBinding,
    generated_ms: u64,
) -> Result<(), ControlResidentLoopError> {
    let mut known = facts
        .iter()
        .map(|fact| (fact.fill_id.clone(), fact.clone()))
        .collect::<BTreeMap<_, _>>();
    for (_, job) in journal
        .jobs()
        .iter()
        .filter(|(_, job)| copy_job_matches_binding(job, binding))
    {
        for completed in &job.prior_phases {
            append_reconciled_copy_fill_set(
                facts,
                &mut known,
                &completed.execution,
                &completed.position,
                &completed.fills,
                binding,
                generated_ms,
            )?;
        }
        if let (Some(position), Some(execution)) = (&job.position, &job.execution) {
            append_reconciled_copy_fill_set(
                facts,
                &mut known,
                execution,
                position,
                &job.fills,
                binding,
                generated_ms,
            )?;
        }
    }
    Ok(())
}

fn append_reconciled_copy_fill_set(
    facts: &mut Vec<SignedFillFact>,
    known: &mut BTreeMap<String, SignedFillFact>,
    execution: &venue_copy::CopyExecutionResult,
    position: &venue_copy::AuthoritativePositionSnapshot,
    fills: &[venue_domain::Fill],
    binding: &ExecutionFactBinding,
    generated_ms: u64,
) -> Result<(), ControlResidentLoopError> {
    if execution.state != venue_copy::CopyExecutionState::Reconciled
        || position.observed_at_ms == 0
        || position.observed_at_ms > generated_ms
    {
        return Ok(());
    }
    for fill in fills {
        let fact = reconciled_copy_fill_fact(fill, position, binding, generated_ms)?;
        match known.get(&fact.fill_id).cloned() {
            Some(existing) => {
                // A later signed page may repeat precisely this fill with a newer account
                // generation. Generation is observation metadata, not a second economic fill;
                // every other normalized field (including the canonical digest) stays exact.
                let mut comparable = fact.clone();
                comparable.signed_generation = existing.signed_generation;
                if comparable != existing {
                    return Err(ControlResidentLoopError::ProjectionEncoding);
                }
                if fact.signed_generation > existing.signed_generation {
                    let Some(current) = facts
                        .iter_mut()
                        .find(|current| current.fill_id == fact.fill_id)
                    else {
                        return Err(ControlResidentLoopError::ProjectionEncoding);
                    };
                    *current = fact.clone();
                    known.insert(fact.fill_id.clone(), fact);
                }
            }
            None => {
                known.insert(fact.fill_id.clone(), fact.clone());
                facts.push(fact);
            }
        }
    }
    Ok(())
}

fn reconciled_copy_fill_fact(
    fill: &venue_domain::Fill,
    position: &venue_copy::AuthoritativePositionSnapshot,
    binding: &ExecutionFactBinding,
    generated_ms: u64,
) -> Result<SignedFillFact, ControlResidentLoopError> {
    if fill.symbol != binding.symbol {
        return Err(ControlResidentLoopError::ProjectionScope);
    }
    let occurred_ms = fill.exchange_time_ms.unwrap_or(position.observed_at_ms);
    if occurred_ms == 0 || occurred_ms > generated_ms {
        return Err(ControlResidentLoopError::ProjectionEncoding);
    }
    Ok(SignedFillFact {
        binding: binding.clone(),
        fill_id: fill.fill_id.clone(),
        order_id: fill.order_id.clone(),
        side: fill.side,
        position_side: field_state_option(fill.position_side.clone()),
        quantity: fill.quantity,
        price: fill.price.value(),
        execution_sequence: field_state_option(fill.execution_sequence.clone()),
        occurred_ms,
        signed_generation: position.generation,
        // The signed account page and the Copy reconciliation may observe the same normalized
        // exchange fill. Their identity must be byte-for-byte identical rather than source-tagged.
        fact_digest: projection_digest_for("fill", fill)?,
    })
}

fn field_state_option<T>(state: FieldState<T>) -> Option<T> {
    match state {
        FieldState::Known(value) => Some(value),
        FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => None,
    }
}

fn copy_ledger_facts(
    journal: &crate::CopyDeliveryJournal,
    binding: &ExecutionFactBinding,
    generated_ms: u64,
) -> Result<Vec<CopyLedgerFact>, ControlResidentLoopError> {
    journal
        .jobs()
        .iter()
        .filter_map(|(job_id, job)| {
            (copy_job_matches_binding(job, binding) && job.actor_applied_ms.is_some()).then_some((
                job_id,
                job.position.as_ref(),
                job,
            ))
        })
        .filter_map(|(job_id, position, job)| position.map(|position| (job_id, position, job)))
        .map(|(job_id, position, job)| {
            if position.observed_at_ms == 0 || position.observed_at_ms > generated_ms {
                return Err(ControlResidentLoopError::ProjectionEncoding);
            }
            Ok(CopyLedgerFact {
                relation_id: job.relation.relation.relation_id.clone(),
                relation_revision: job.relation.revision,
                job_id: job_id.clone(),
                binding: binding.clone(),
                ledger_sequence: None,
                managed_exposure: position.exposure.value,
                signed_generation: position.generation,
                observed_ms: position.observed_at_ms,
                fact_digest: projection_digest_for("copy-ledger", &(job_id, position))?,
            })
        })
        .collect()
}

fn copy_drift_facts(
    journal: &crate::CopyDeliveryJournal,
    binding: &ExecutionFactBinding,
    generated_ms: u64,
) -> Result<Vec<CopyDriftFact>, ControlResidentLoopError> {
    journal
        .jobs()
        .iter()
        .filter_map(|(job_id, job)| {
            let request = job.request.as_ref()?;
            let position = job.position.as_ref()?;
            (copy_job_matches_binding(job, binding) && job.actor_applied_ms.is_some())
                .then_some((job_id, job, request, position))
        })
        .map(|(job_id, job, request, position)| {
            if position.observed_at_ms == 0 || position.observed_at_ms > generated_ms {
                return Err(ControlResidentLoopError::ProjectionEncoding);
            }
            let reconciled = job.execution.as_ref().is_some_and(|execution| {
                execution.state == venue_copy::CopyExecutionState::Reconciled
            });
            Ok(CopyDriftFact {
                relation_id: job.relation.relation.relation_id.clone(),
                relation_revision: job.relation.revision,
                job_id: job_id.clone(),
                binding: binding.clone(),
                target_exposure: request.target_exposure.value,
                actual_exposure: position.exposure.value,
                repair_pending: !reconciled
                    || position.exposure.value != request.target_exposure.value,
                signed_generation: position.generation,
                observed_ms: position.observed_at_ms,
                fact_digest: projection_digest_for(
                    "copy-drift",
                    &(job_id, request, position, &job.execution),
                )?,
            })
        })
        .collect()
}

fn copy_job_matches_binding(
    job: &crate::copy_delivery_journal::DurableCopyJob,
    binding: &ExecutionFactBinding,
) -> bool {
    let relation = &job.relation.relation;
    let Some(request) = job.request.as_ref() else {
        return false;
    };
    request.binding.account_id == binding.trading_account_id
        && request.binding.follower_instance_id == binding.instance_id
        && request.binding.instrument.symbol == binding.symbol
        && relation.follower.venue == binding.venue
        && relation.follower.mode == binding.mode
        && relation.follower.trading_account_id == binding.trading_account_id
        && relation.follower.instance_id == binding.instance_id
        && relation.follower.symbol == binding.symbol
}

fn copy_execution_state(state: venue_copy::CopyExecutionState) -> CopyExecutionStateProjection {
    match state {
        venue_copy::CopyExecutionState::Prepared => CopyExecutionStateProjection::Prepared,
        venue_copy::CopyExecutionState::Submitted => CopyExecutionStateProjection::Submitted,
        venue_copy::CopyExecutionState::Accepted => CopyExecutionStateProjection::Accepted,
        venue_copy::CopyExecutionState::Rejected => CopyExecutionStateProjection::Rejected,
        venue_copy::CopyExecutionState::Unknown => CopyExecutionStateProjection::Unknown,
        venue_copy::CopyExecutionState::Reconciled => CopyExecutionStateProjection::Reconciled,
    }
}

fn control_applied_detail(action: venue_control_protocol::ControlAction) -> &'static str {
    match action {
        venue_control_protocol::ControlAction::Pause => "pause intent durable in resident actor",
        venue_control_protocol::ControlAction::Resume => "resume intent durable in resident actor",
        venue_control_protocol::ControlAction::Stop => {
            "stop intent durable; owned-order cancellation awaits newer signed facts"
        }
        venue_control_protocol::ControlAction::Flatten => {
            "flatten intent durable; signed zero positions and orders are still required"
        }
        venue_control_protocol::ControlAction::Trade => "unsupported manual trade",
    }
}

fn now_ms() -> Result<u64, ()> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ())?
        .as_millis()
        .try_into()
        .map_err(|_| ())
}

fn fresh_completion_ms() -> Result<u64, ControlResidentLoopError> {
    now_ms().map_err(|_| ControlResidentLoopError::Signal)
}

fn completion_unless_expired<T>(
    completion: Result<T, crate::ControlDeliveryError>,
) -> Result<Option<T>, ControlResidentLoopError> {
    match completion {
        Ok(completion) => Ok(Some(completion)),
        Err(crate::ControlDeliveryError::LeaseExpired) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn backoff(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(6);
    Duration::from_millis(100_u64.saturating_mul(1_u64 << exponent)).min(MAX_BACKOFF)
}

#[derive(Debug, thiserror::Error)]
pub enum ControlResidentLoopError {
    #[error("node runtime configuration is invalid")]
    Config,
    #[error("node control artifacts are unavailable")]
    Artifacts,
    #[error("signed snapshot does not match the configured account scope")]
    ProjectionScope,
    #[error("signed projection could not be encoded without losing evidence")]
    ProjectionEncoding,
    #[error("control loop signal listener failed")]
    Signal,
    #[error("resident actor rejected the delivery: {0}")]
    Resident(NodeError),
    #[error("copy delivery cannot be safely applied or reconciled")]
    Copy,
    #[error("control shutdown recovery cannot safely continue")]
    ControlShutdown,
    #[error(transparent)]
    Delivery(#[from] ControlDeliveryDriverError),
    #[error(transparent)]
    Inbox(#[from] crate::ControlDeliveryError),
    #[error(transparent)]
    Journal(#[from] crate::ControlDeliveryJournalError),
    #[error(transparent)]
    Http(#[from] crate::ControlHttpClientError),
    #[error(transparent)]
    Outbox(#[from] NodeProjectionOutboxError),
    #[error(transparent)]
    CopyJournal(#[from] crate::CopyDeliveryJournalError),
    #[error(transparent)]
    ShutdownJournal(#[from] crate::control_shutdown_journal::ControlShutdownJournalError),
}

impl ControlResidentLoopError {
    fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Delivery(ControlDeliveryDriverError::Http(
                crate::ControlHttpClientError::Transport
                    | crate::ControlHttpClientError::Timeout
                    | crate::ControlHttpClientError::HttpStatus(_)
            )) | Self::Outbox(NodeProjectionOutboxError::Http(
                crate::ControlHttpClientError::Transport
                    | crate::ControlHttpClientError::Timeout
                    | crate::ControlHttpClientError::HttpStatus(_)
            ))
        )
    }
}

#[cfg(test)]
mod control_loop_tests;
#[cfg(test)]
#[path = "control_loop/manual_trade_e2e_tests.rs"]
mod manual_trade_e2e_tests;
