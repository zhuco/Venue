use std::{
    collections::VecDeque,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use rust_decimal::Decimal;

pub use venue_runtime::shared::{
    PrivateBootstrapScope, PrivateExposure, PrivateFactsClockRoot, PrivateFactsEffect,
    PrivateFactsFailureStage, PrivateFactsReadiness, PrivateFactsSnapshot, PrivateFactsWorkerState,
    PrivateReadbackTicket,
};
use venue_runtime::shared::{
    PrivateFactsScheduleError, PrivateFactsSchedulePolicy, PrivateFactsScheduler,
};

use crate::{
    config::BinanceAccountBinding,
    domain::{
        AccountBalance, CommandId, FieldState, Order, Position, PositionSide, Price, Symbol,
        is_canonical_trading_account_id,
    },
    exchange::{
        binance::{
            PrivateCredentials, PrivateError, PrivateListenKey, PrivateReadbackError, PrivateRest,
            PrivateStreamSocket, RecentFillsCursor, RecentFillsReadback, native_symbol,
        },
        binance_portfolio,
        binance_private::{self, AlgoOrderReadback, PrivateParseError, PrivateReadback},
        private_session::{
            PrivateEvidenceSession, PrivateSessionBinding, PrivateSessionError,
            PrivateSessionState, PrivateSignal,
        },
    },
    execution::{
        AlgoProtectionCustodyInput, CommandJournal, CommandJournalError, CustodyWriterRole,
        FillRecoveryBatch, FillRecoveryCoordinator, FillRecoveryError, FillRecoveryReport,
        PrivateProjectionResolverInput, ProtectionEvidence, ReadbackBatch, ReconciliationError,
        ReconciliationReport, WriterLeaseAuthority, WriterLeaseError, WriterScope,
        prove_algo_protection_custody, resolve_private_facts_projection,
    },
    risk::AccountRiskView,
    storage::{
        FillCursor, FillCursorError, FillCursorStore, Journal, PrivateEvidenceError,
        PrivateEvidenceJournal, StorageError,
    },
};

use super::PrivateFactsProjectionInput;

const EXCHANGE: &str = "binance";
const BASE_BACKOFF_MS: u64 = 250;
const MAX_BACKOFF_MS: u64 = 30_000;
const KEEPALIVE_MS: u64 = 20 * 60 * 1_000;
const STREAM_BURST_COALESCE_MS: u64 = 100;
// The resident also services four public sockets on the same bounded loop. A 1ms idle private
// poll preserves private priority without letting public depth/trade sockets build minutes of
// backpressure before the feature window can become Ready.
const FRAME_POLL_MS: u64 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePrivateFactsWorkerConfig {
    pub account: String,
    pub symbol: Symbol,
    pub artifacts_root: PathBuf,
    pub initial_fill_recovery_from_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePrivateProjectionAuthorityConfig {
    pub binding: crate::strategy::scalping::StrategyBinding,
    pub command_journal_path: PathBuf,
    pub writer_authority_path: PathBuf,
    pub custody_max_stale_ms: u64,
}

impl BinancePrivateProjectionAuthorityConfig {
    fn validate(
        &self,
        worker: &BinancePrivateFactsWorkerConfig,
    ) -> Result<(), PrivateFactsWorkerError> {
        if self.binding.validate().is_err()
            || self.binding.exchange != EXCHANGE
            || self.binding.account != worker.account
            || self.binding.symbol != worker.symbol
            || !self.command_journal_path.is_absolute()
            || !self.writer_authority_path.is_absolute()
            || self.custody_max_stale_ms == 0
        {
            return Err(PrivateFactsWorkerError::Config);
        }
        Ok(())
    }
}

impl BinancePrivateFactsWorkerConfig {
    fn validate(&self) -> Result<(), PrivateFactsWorkerError> {
        if !is_canonical_trading_account_id(&self.account)
            || !self.artifacts_root.is_absolute()
            || self.initial_fill_recovery_from_ms == 0
        {
            return Err(PrivateFactsWorkerError::Config);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DurableStreamFullFill {
    pub fill_id: String,
    pub private_generation: u64,
    pub client_order_id: String,
    pub event_time_ms: u64,
    pub received_at_ms: u64,
    pub fill_price: Price,
    pub maker: FieldState<bool>,
}

#[derive(Debug)]
pub struct BinancePrivateFactsWorker {
    config: BinancePrivateFactsWorkerConfig,
    projection_authority: Option<BinancePrivateProjectionAuthorityConfig>,
    session: Arc<Mutex<PrivateEvidenceSession>>,
    facts: Journal,
    fill_cursor: FillCursorStore,
    fill_recovery: FillRecoveryCoordinator,
    scheduler: PrivateFactsScheduler,
    ready_projection: Option<PrivateFactsReadiness>,
    ready_snapshot: Option<PrivateFactsSnapshot>,
    ready_authority_projection: Option<PrivateFactsProjectionInput>,
    defer_routine_stream_readback: bool,
    durable_stream_full_fills: VecDeque<DurableStreamFullFill>,
}

struct PrivateFactsBootstrap {
    generation: u64,
    target_through_ms: u64,
    account: PrivateReadback,
    fills: RecentFillsReadback,
    open_algo_orders: Vec<AlgoOrderReadback>,
}

struct TransportBootstrap {
    ticket: PrivateReadbackTicket,
    target_through_ms: u64,
    balances: Vec<AccountBalance>,
    positions: Option<Vec<Position>>,
    one_way_position: Option<bool>,
    can_trade: Option<bool>,
    orders: Option<Vec<Order>>,
    open_algo_orders: Option<Vec<AlgoOrderReadback>>,
}

fn readiness_from_bootstrap(bootstrap: &PrivateFactsBootstrap) -> PrivateFactsReadiness {
    let exposure = if bootstrap
        .account
        .positions
        .iter()
        .any(|position| !position.quantity.is_zero())
    {
        PrivateExposure::Open
    } else {
        PrivateExposure::Flat
    };
    PrivateFactsReadiness {
        generation: bootstrap.generation,
        observed_at_ms: bootstrap.target_through_ms,
        root_cause_fact_id: String::new(),
        exposure,
        ordinary_order_debt: !bootstrap.account.orders.is_empty(),
        algo_order_debt: !bootstrap.open_algo_orders.is_empty(),
    }
}

impl BinancePrivateProjectionAuthorityConfig {
    fn resolve(
        &self,
        bootstrap: &PrivateFactsBootstrap,
        readiness: PrivateFactsReadiness,
        now_ms: u64,
    ) -> Result<PrivateFactsProjectionInput, PrivateFactsWorkerError> {
        let journal = CommandJournal::open(self.command_journal_path.clone())?;
        let writer_authority = WriterLeaseAuthority::open(
            self.writer_authority_path.clone(),
            WriterScope {
                exchange: self.binding.exchange.clone(),
                account: self.binding.account.clone(),
                symbol: self.binding.symbol.clone(),
                owner_scope: self.binding.owner_scope.clone(),
            },
        )?;
        let writer = writer_authority.active_session()?;
        let ordinary_ids = ordinary_client_ids(&bootstrap.account.orders).unwrap_or_default();
        let algo_ids = algo_client_ids(&bootstrap.open_algo_orders).unwrap_or_default();
        let unresolved_commands = u32::from(journal.has_unresolved());
        let account_risk = exact_account_risk(
            &bootstrap.account.balances,
            &self.binding.risk_budget.asset,
            unresolved_commands,
        );
        let algo_custodies = writer.as_ref().map_or_else(Vec::new, |writer| {
            self.prove_algo_custodies(
                bootstrap,
                readiness.clone(),
                now_ms,
                &journal,
                writer,
                &algo_ids,
            )
        });
        Ok(resolve_private_facts_projection(
            PrivateProjectionResolverInput {
                binding: &self.binding,
                readiness,
                positions: &bootstrap.account.positions,
                open_ordinary_client_ids: &ordinary_ids,
                open_algo_client_ids: &algo_ids,
                journal: &journal,
                writer: writer.as_ref(),
                algo_custodies: &algo_custodies,
                account_risk: account_risk.as_ref(),
                now_ms,
            },
        ))
    }

    fn prove_algo_custodies(
        &self,
        bootstrap: &PrivateFactsBootstrap,
        readiness: PrivateFactsReadiness,
        now_ms: u64,
        journal: &CommandJournal,
        writer: &crate::execution::WriterSession,
        algo_ids: &[CommandId],
    ) -> Vec<crate::execution::AlgoProtectionCustody> {
        let Some(valid_until_ms) = readiness
            .observed_at_ms
            .checked_add(self.custody_max_stale_ms)
        else {
            return Vec::new();
        };
        bootstrap
            .open_algo_orders
            .iter()
            .filter_map(|algo| {
                let client_id = CommandId::new(&algo.client_algo_id).ok()?;
                if !algo_ids.contains(&client_id) {
                    return None;
                }
                let command = journal.stop_full_by_client_id(&client_id)?;
                if command.owner.purpose != crate::domain::OrderPurpose::Protection {
                    return None;
                }
                let position = bootstrap
                    .account
                    .positions
                    .iter()
                    .find(|position| position.side == command.position_side)?;
                prove_algo_protection_custody(AlgoProtectionCustodyInput {
                    command,
                    position,
                    algo,
                    writer,
                    evidence: ProtectionEvidence {
                        private_generation: readiness.generation,
                        readback_generation: readiness.generation,
                        valid_until_ms,
                        observed_at_ms: readiness.observed_at_ms,
                    },
                    writer_role: CustodyWriterRole {
                        predecessor_protected: false,
                        protection_only: false,
                    },
                    now_ms,
                })
                .ok()
            })
            .collect()
    }
}

fn ordinary_client_ids(orders: &[Order]) -> Option<Vec<CommandId>> {
    orders
        .iter()
        .map(|order| match &order.client_order_id {
            FieldState::Known(client_id) => CommandId::new(client_id).ok(),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => None,
        })
        .collect()
}

fn algo_client_ids(orders: &[AlgoOrderReadback]) -> Option<Vec<CommandId>> {
    orders
        .iter()
        .map(|order| CommandId::new(&order.client_algo_id).ok())
        .collect()
}

fn exact_account_risk(
    balances: &[AccountBalance],
    asset: &crate::domain::Asset,
    unresolved_commands: u32,
) -> Option<AccountRiskView> {
    let mut matching = balances.iter().filter(|balance| &balance.asset == asset);
    let balance = matching.next()?;
    if matching.next().is_some() {
        return None;
    }
    AccountRiskView::from_balance(balance, unresolved_commands).ok()
}

impl BinancePrivateFactsWorker {
    pub fn open(config: BinancePrivateFactsWorkerConfig) -> Result<Self, PrivateFactsWorkerError> {
        Self::open_inner(config, None)
    }

    pub fn open_with_projection_authority(
        config: BinancePrivateFactsWorkerConfig,
        authority: BinancePrivateProjectionAuthorityConfig,
    ) -> Result<Self, PrivateFactsWorkerError> {
        Self::open_inner(config, Some(authority))
    }

    fn open_inner(
        config: BinancePrivateFactsWorkerConfig,
        projection_authority: Option<BinancePrivateProjectionAuthorityConfig>,
    ) -> Result<Self, PrivateFactsWorkerError> {
        config.validate()?;
        if let Some(authority) = &projection_authority {
            authority.validate(&config)?;
        }
        fs::create_dir_all(&config.artifacts_root).map_err(|source| {
            PrivateFactsWorkerError::Io {
                path: config.artifacts_root.clone(),
                source,
            }
        })?;
        let session = PrivateEvidenceSession::open_durable(
            PrivateEvidenceJournal::open(config.artifacts_root.join("private_evidence.jsonl"))?,
            config.artifacts_root.join("private_session.json"),
            PrivateSessionBinding {
                exchange: EXCHANGE.to_owned(),
                account: config.account.clone(),
                symbol: config.symbol.clone(),
            },
        )?;
        let facts = Journal::open(config.artifacts_root.join("facts.jsonl"))?;
        let fill_cursor = FillCursorStore::new(config.artifacts_root.join("fill_cursor.json"));
        match fill_cursor.load()? {
            Some(cursor)
                if cursor.exchange == EXCHANGE
                    && cursor.account == config.account
                    && cursor.symbol == config.symbol => {}
            Some(_) => return Err(PrivateFactsWorkerError::Config),
            None => {
                fill_cursor.compare_and_swap(
                    None,
                    &FillCursor {
                        schema_version: 1,
                        exchange: EXCHANGE.to_owned(),
                        account: config.account.clone(),
                        symbol: config.symbol.clone(),
                        generation: 1,
                        connection_epoch: 1,
                        observed_through_ms: config.initial_fill_recovery_from_ms,
                        last_trade_id: None,
                        last_event_time_ms: None,
                    },
                )?;
            }
        }
        let fill_recovery = FillRecoveryCoordinator::recover(&facts, &fill_cursor)?;
        let scheduler = PrivateFactsScheduler::new(
            PrivateFactsSchedulePolicy {
                base_backoff_ms: BASE_BACKOFF_MS,
                max_backoff_ms: MAX_BACKOFF_MS,
                keepalive_ms: KEEPALIVE_MS,
                stream_burst_coalesce_ms: STREAM_BURST_COALESCE_MS,
                frame_poll_ms: FRAME_POLL_MS,
            },
            config.account.clone(),
        )?;
        Ok(Self {
            config,
            projection_authority,
            session: Arc::new(Mutex::new(session)),
            facts,
            fill_cursor,
            fill_recovery,
            scheduler,
            ready_projection: None,
            ready_snapshot: None,
            ready_authority_projection: None,
            defer_routine_stream_readback: false,
            durable_stream_full_fills: VecDeque::new(),
        })
    }

    pub fn next_effect(
        &mut self,
        now_ms: u64,
    ) -> Result<Option<PrivateFactsEffect>, PrivateFactsWorkerError> {
        if self.scheduler.pending_effect_id().is_some() {
            return Ok(None);
        }
        if self.scheduler.stream_readback_due(now_ms) {
            self.begin_stream_readback()?;
        }
        if self.scheduler.periodic_readback_due(now_ms) {
            // A due fallback readback must not jump ahead of an already-buffered user-stream
            // fill. Poll the socket at least once at/after the deadline; only an empty poll
            // permits the slower seven-scope refresh on the following turn.
            self.begin_periodic_readback()?;
        }
        let (generation, evidence_sequence) = {
            let session = self.lock_session()?;
            if self.scheduler.state() == PrivateFactsWorkerState::NeedsBootstrap
                && session.state() != PrivateSessionState::NeedsReadback
            {
                return Err(PrivateFactsWorkerError::State);
            }
            (session.generation(), session.journal().last_sequence())
        };
        self.scheduler
            .next_effect(now_ms, generation, evidence_sequence)
            .map_err(Into::into)
    }

    pub fn complete_connect(&mut self, effect_id: u64) -> Result<u64, PrivateFactsWorkerError> {
        self.scheduler.begin_connect_completion(effect_id)?;
        let generation = self.lock_session()?.on_reconnect()?;
        self.scheduler.finish_connect_completion()?;
        self.ready_projection = None;
        self.ready_snapshot = None;
        self.ready_authority_projection = None;
        Ok(generation)
    }

    pub fn complete_no_frame(&mut self, effect_id: u64) -> Result<(), PrivateFactsWorkerError> {
        self.scheduler
            .complete_no_frame(effect_id)
            .map_err(Into::into)
    }

    pub fn complete_keepalive(
        &mut self,
        effect_id: u64,
        now_ms: u64,
    ) -> Result<(), PrivateFactsWorkerError> {
        let generation = self.session_generation()?;
        if let Err(error) = self
            .scheduler
            .complete_keepalive(effect_id, generation, now_ms)
        {
            if error == PrivateFactsScheduleError::Generation {
                return self.fail(now_ms, PrivateFactsWorkerError::StaleEpoch);
            }
            return Err(error.into());
        }
        if self.scheduler.state() != PrivateFactsWorkerState::Ready {
            return self.fail(now_ms, PrivateFactsWorkerError::StaleEpoch);
        }
        Ok(())
    }

    pub fn complete_frame(
        &mut self,
        effect_id: u64,
        sequence: u64,
        received_at_ms: u64,
        payload: String,
        now_ms: u64,
    ) -> Result<PrivateSignal, PrivateFactsWorkerError> {
        let generation = self.session_generation()?;
        if let Err(error) = self
            .scheduler
            .complete_frame(effect_id, generation, sequence)
        {
            if error == PrivateFactsScheduleError::Generation {
                return self.fail(now_ms, PrivateFactsWorkerError::SequenceGap);
            }
            return Err(error.into());
        }
        if self.scheduler.state() != PrivateFactsWorkerState::Ready {
            return self.fail(now_ms, PrivateFactsWorkerError::SequenceGap);
        }
        let ingested = {
            let mut session = self.lock_session()?;
            session.ingest(received_at_ms, payload.clone())
        };
        let signal = match ingested {
            Ok(signal) => signal,
            Err(error) => return self.fail(now_ms, error.into()),
        };
        // Normalize a fast-path fill only after the raw frame and its session state are durable.
        // A process crash can therefore replay the evidence instead of losing a fill that was
        // already allowed to influence strategy state.
        let durable_fill = self
            .defer_routine_stream_readback
            .then(|| {
                parse_durable_stream_full_fill(
                    &payload,
                    &self.config.symbol,
                    generation,
                    received_at_ms,
                )
            })
            .flatten();
        match signal {
            PrivateSignal::ReadbackRequired if self.defer_routine_stream_readback => {
                if let Some(fill) = durable_fill {
                    self.durable_stream_full_fills.push_back(fill);
                }
                Ok(signal)
            }
            PrivateSignal::OrderLifecycleDebounced if self.defer_routine_stream_readback => {
                Ok(signal)
            }
            PrivateSignal::ReadbackRequired | PrivateSignal::RiskAlert => {
                self.scheduler.require_immediate_readback();
                self.ready_projection = None;
                self.ready_snapshot = None;
                self.ready_authority_projection = None;
                Ok(signal)
            }
            PrivateSignal::OrderLifecycleDebounced => {
                self.ready_projection = None;
                self.ready_snapshot = None;
                self.ready_authority_projection = None;
                self.scheduler.schedule_stream_readback(now_ms)?;
                Ok(signal)
            }
            PrivateSignal::StreamExpired { .. } => self.fail(
                now_ms,
                PrivateFactsWorkerError::Session(PrivateSessionError::ReconnectRequired),
            ),
        }
    }

    pub fn complete_transport_failure(
        &mut self,
        effect_id: u64,
        now_ms: u64,
    ) -> Result<(), PrivateFactsWorkerError> {
        self.scheduler
            .complete_transport_failure(effect_id, now_ms)?;
        self.force_fence(now_ms);
        Ok(())
    }

    pub fn entry_ready(&self) -> bool {
        let Ok(session) = self.session.lock() else {
            return false;
        };
        self.scheduler.state() == PrivateFactsWorkerState::Ready
            && session.state() == PrivateSessionState::Ready
            && self
                .fill_recovery
                .epoch_gate()
                .allows_ready(session.generation())
    }

    pub fn state(&self) -> PrivateFactsWorkerState {
        self.scheduler.state()
    }

    /// Enables a caller-owned same-connection readback cadence. This is a liveness fallback for
    /// runtimes that do not attach projection authority; stream events remain the fast path.
    pub(crate) fn set_periodic_readback_interval(
        &mut self,
        interval_ms: u64,
    ) -> Result<(), PrivateFactsWorkerError> {
        self.scheduler
            .set_periodic_readback_interval(interval_ms)
            .map_err(Into::into)
    }

    /// Grid fills are already durable in the private evidence journal. This mode lets the
    /// symbol actor consume a complete owned fill immediately and defers ordinary account/order
    /// reconciliation to the configured cadence. Risk events, disconnects and malformed frames
    /// keep their existing fail-closed behavior.
    pub(crate) fn enable_durable_fill_fast_path(&mut self) {
        self.defer_routine_stream_readback = true;
    }

    pub(crate) fn take_durable_stream_full_fill(&mut self) -> Option<DurableStreamFullFill> {
        self.durable_stream_full_fills.pop_front()
    }

    pub fn generation(&self) -> Result<u64, PrivateFactsWorkerError> {
        self.session_generation()
    }

    pub fn last_failure_stage(&self) -> Option<PrivateFactsFailureStage> {
        self.scheduler.last_failure_stage()
    }

    /// True only while a scheduled same-generation refresh is replacing an otherwise valid
    /// signed readback. Stream signals, mutations, transport failures, and reconnects clear it.
    pub fn periodic_readback_in_progress(&self) -> bool {
        self.scheduler.periodic_readback_in_progress()
    }

    fn record_failure(&mut self, stage: PrivateFactsFailureStage) {
        self.scheduler.record_failure(stage);
    }

    fn clear_failure(&mut self) {
        self.scheduler.clear_failure();
    }

    /// Revokes the current Ready identity after a local mutation and schedules a fresh private
    /// stream/readback generation. Callers cannot keep using the prior private projection while
    /// this transition is pending.
    pub fn request_post_mutation_reconciliation(
        &mut self,
        now_ms: u64,
    ) -> Result<(), PrivateFactsWorkerError> {
        if now_ms == 0 {
            return Err(PrivateFactsWorkerError::Effect);
        }
        self.fence(now_ms)
    }

    /// Returns readiness only while this exact worker is still backed by a durable Ready session
    /// and an admitted fill epoch. Callers must combine it with their own anonymous account/risk
    /// projection; this method never creates an entry permit.
    pub fn readiness(&self) -> Result<Option<PrivateFactsReadiness>, PrivateFactsWorkerError> {
        let session = self.lock_session()?;
        let readiness = self.ready_projection.clone().filter(|projection| {
            (self.scheduler.state() == PrivateFactsWorkerState::Ready
                && session.state() == PrivateSessionState::Ready
                && self
                    .fill_recovery
                    .epoch_gate()
                    .allows_ready(session.generation()))
                && projection.generation == session.generation()
        });
        Ok(readiness)
    }

    /// Returns the exact normalized facts committed with the current durable Ready generation.
    /// A socket notification alone never creates or changes this value.
    pub fn snapshot(&self) -> Result<Option<PrivateFactsSnapshot>, PrivateFactsWorkerError> {
        let Some(readiness) = self.readiness()? else {
            return Ok(None);
        };
        Ok(self.ready_snapshot.clone().filter(|snapshot| {
            snapshot.generation == readiness.generation
                && snapshot.observed_at_ms == readiness.observed_at_ms
        }))
    }

    /// Returns only the anonymous four-way projection produced from the same committed bootstrap
    /// identity. A worker opened without a projection authority always returns `None` here.
    pub fn authoritative_projections(
        &self,
    ) -> Result<Option<PrivateFactsProjectionInput>, PrivateFactsWorkerError> {
        let Some(readiness) = self.readiness()? else {
            return Ok(None);
        };
        Ok(self.ready_authority_projection.filter(|projection| {
            [
                (
                    projection.execution.generation,
                    projection.execution.observed_at_ms,
                ),
                (projection.owner.generation, projection.owner.observed_at_ms),
                (
                    projection.protection.generation,
                    projection.protection.observed_at_ms,
                ),
                (
                    projection.risk_budget.generation,
                    projection.risk_budget.observed_at_ms,
                ),
            ]
            .into_iter()
            .all(|identity| identity == (readiness.generation, readiness.observed_at_ms))
        }))
    }

    /// Binds deadline evaluation to the durable private evidence checkpoint that admitted this
    /// readiness. Sequence zero is the valid initial checkpoint before any stream frame exists.
    /// Repeated calls return the same root until a newer bootstrap is committed.
    pub fn authoritative_clock_root(
        &self,
    ) -> Result<Option<PrivateFactsClockRoot>, PrivateFactsWorkerError> {
        let Some(readiness) = self.readiness()? else {
            return Ok(None);
        };
        Ok(Some(PrivateFactsClockRoot {
            observed_at_ms: readiness.observed_at_ms,
            root_cause_fact_id: readiness.root_cause_fact_id,
        }))
    }

    fn complete_bootstrap_scope(
        &mut self,
        effect_id: u64,
        ticket: PrivateReadbackTicket,
        scope: PrivateBootstrapScope,
        now_ms: u64,
    ) -> Result<(), PrivateFactsWorkerError> {
        if let Err(error) = self
            .scheduler
            .complete_bootstrap_scope(effect_id, ticket, scope)
        {
            if error == PrivateFactsScheduleError::Generation {
                return self.fail(now_ms, PrivateFactsWorkerError::StaleEpoch);
            }
            return Err(error.into());
        }
        let ticket_is_current = {
            let session = self.lock_session()?;
            session
                .validate_readback_ticket(ticket.generation(), ticket.evidence_sequence())
                .is_ok()
        };
        if !ticket_is_current {
            return self.fail(now_ms, PrivateFactsWorkerError::StaleEpoch);
        }
        Ok(())
    }

    fn complete_bootstrap(
        &mut self,
        effect_id: u64,
        bootstrap: PrivateFactsBootstrap,
        now_ms: u64,
    ) -> Result<PrivateFactsCommitReport, PrivateFactsWorkerError> {
        let ticket = match self.scheduler.complete_bootstrap(effect_id) {
            Ok(ticket) => ticket,
            Err(_) => return self.fail(now_ms, PrivateFactsWorkerError::Effect),
        };
        if let Err(error) = self.validate_bootstrap(ticket, &bootstrap) {
            return self.fail(now_ms, error);
        }
        let projection = readiness_from_bootstrap(&bootstrap);
        let fills = binance_private::parse_fills(&bootstrap.fills.payload, &self.config.symbol)?;
        let snapshot = PrivateFactsSnapshot {
            generation: bootstrap.generation,
            observed_at_ms: bootstrap.target_through_ms,
            can_trade: bootstrap.account.capabilities.can_trade,
            hedge_position: bootstrap.account.capabilities.hedge_position,
            positions: bootstrap.account.positions.clone(),
            orders: bootstrap.account.orders.clone(),
            fills,
        };
        let authority_projection = match self
            .projection_authority
            .as_ref()
            .map(|authority| authority.resolve(&bootstrap, projection.clone(), now_ms))
            .transpose()
        {
            Ok(projection) => projection,
            Err(error) => return self.fail(now_ms, error),
        };
        match self.commit_bootstrap(ticket, bootstrap) {
            Ok(report) => {
                let authority_interval = self
                    .projection_authority
                    .as_ref()
                    .map(|authority| (authority.custody_max_stale_ms / 2).max(1));
                self.scheduler.mark_ready(now_ms, authority_interval);
                let evidence_sequence = self.lock_session()?.journal().last_sequence();
                let mut projection = projection;
                projection.root_cause_fact_id = format!(
                    "private-readback:{}:{}:{}",
                    projection.generation, projection.observed_at_ms, evidence_sequence
                );
                self.ready_projection = Some(projection);
                self.ready_snapshot = Some(snapshot);
                self.ready_authority_projection = authority_projection;
                Ok(report)
            }
            Err(error) => self.fail(now_ms, error),
        }
    }

    fn commit_bootstrap(
        &mut self,
        ticket: PrivateReadbackTicket,
        bootstrap: PrivateFactsBootstrap,
    ) -> Result<PrivateFactsCommitReport, PrivateFactsWorkerError> {
        self.validate_bootstrap(ticket, &bootstrap)?;
        let session = Arc::clone(&self.session);
        let mut session = session.lock().map_err(|_| PrivateFactsWorkerError::Lock)?;
        if session.generation() != ticket.generation()
            || session.state() != PrivateSessionState::NeedsReadback
            || session.journal().last_sequence() != ticket.evidence_sequence()
        {
            return Err(PrivateFactsWorkerError::StaleEpoch);
        }
        let guard = session.begin_readback_confirmation(ticket.generation())?;
        let fills = self.fill_recovery.accept_batch(
            &mut self.facts,
            &self.fill_cursor,
            FillRecoveryBatch {
                exchange: EXCHANGE,
                account: &self.config.account,
                symbol: &self.config.symbol,
                readback: bootstrap.fills,
                received_at_ms: bootstrap.target_through_ms,
                native_epoch: ticket.generation(),
                hub_bootstrap_generation: ticket.generation(),
            },
        )?;
        if !self
            .fill_recovery
            .epoch_gate()
            .allows_ready(ticket.generation())
        {
            return Err(PrivateFactsWorkerError::StaleEpoch);
        }
        let account = self.fill_recovery.accept_account_readback(
            &mut self.facts,
            ReadbackBatch {
                generation: ticket.generation(),
                received_at_ms: bootstrap.target_through_ms,
                balances: &bootstrap.account.balances,
                positions: &bootstrap.account.positions,
                orders: &bootstrap.account.orders,
                fills: &[],
            },
        )?;
        session.finish_readback_confirmation(guard)?;
        Ok(PrivateFactsCommitReport { fills, account })
    }

    fn validate_bootstrap(
        &self,
        ticket: PrivateReadbackTicket,
        bootstrap: &PrivateFactsBootstrap,
    ) -> Result<(), PrivateFactsWorkerError> {
        if bootstrap.generation != ticket.generation()
            || bootstrap.target_through_ms == 0
            || bootstrap.fills.cursor.observed_through_ms < bootstrap.target_through_ms
            || !bootstrap.account.capabilities.can_trade
            || bootstrap.account.capabilities.one_way_position
            || !bootstrap.account.capabilities.hedge_position
            || bootstrap.account.balances.is_empty()
            || !bootstrap.account.fills.is_empty()
            || bootstrap.account.positions.len() != 2
            || bootstrap
                .account
                .orders
                .iter()
                .any(|order| order.symbol != self.config.symbol)
        {
            return Err(PrivateFactsWorkerError::IncompleteBootstrap);
        }
        let mut has_long = false;
        let mut has_short = false;
        for position in &bootstrap.account.positions {
            if position.symbol != self.config.symbol {
                return Err(PrivateFactsWorkerError::IncompleteBootstrap);
            }
            match position.side {
                PositionSide::Long if !has_long => has_long = true,
                PositionSide::Short if !has_short => has_short = true,
                PositionSide::Long | PositionSide::Short | PositionSide::Net => {
                    return Err(PrivateFactsWorkerError::IncompleteBootstrap);
                }
            }
        }
        // Open orders and Algo orders are retained as anonymous debt in the committed readiness.
        // The worker cannot decide whether they are owned execution or valid protection; the
        // same-generation owner/execution/protection projections make that decision downstream.
        if !has_long || !has_short {
            return Err(PrivateFactsWorkerError::IncompleteBootstrap);
        }
        Ok(())
    }

    fn current_recent_fills_cursor(&self) -> Result<RecentFillsCursor, PrivateFactsWorkerError> {
        let cursor = self
            .fill_cursor
            .load()?
            .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?;
        Ok(RecentFillsCursor {
            observed_through_ms: cursor.observed_through_ms,
            last_trade_id: cursor.last_trade_id,
            last_event_time_ms: cursor.last_event_time_ms,
        })
    }

    /// Schedules a same-generation signed account refresh before the authority freshness window
    /// expires. It does not reconnect the user stream or synthesize an event.
    fn begin_periodic_readback(&mut self) -> Result<(), PrivateFactsWorkerError> {
        {
            let mut session = self.lock_session()?;
            match session.state() {
                PrivateSessionState::Ready => session.require_fresh_readback()?,
                PrivateSessionState::NeedsReadback if self.defer_routine_stream_readback => {}
                _ => return Err(PrivateFactsWorkerError::State),
            }
        }
        self.scheduler.begin_periodic_readback()?;
        self.ready_projection = None;
        self.ready_snapshot = None;
        self.ready_authority_projection = None;
        Ok(())
    }

    fn begin_stream_readback(&mut self) -> Result<(), PrivateFactsWorkerError> {
        self.scheduler.begin_stream_readback()?;
        self.ready_projection = None;
        self.ready_snapshot = None;
        self.ready_authority_projection = None;
        Ok(())
    }

    fn fail<T>(
        &mut self,
        now_ms: u64,
        error: PrivateFactsWorkerError,
    ) -> Result<T, PrivateFactsWorkerError> {
        self.force_fence(now_ms);
        Err(error)
    }

    pub(crate) fn force_fence(&mut self, now_ms: u64) {
        if self.fence(now_ms).is_err() {
            if let Ok(mut session) = self.session.lock() {
                session.fail_closed_in_memory();
            }
            self.fill_recovery.fence_epoch();
            self.ready_projection = None;
            self.ready_snapshot = None;
            self.ready_authority_projection = None;
        }
    }

    fn fence(&mut self, now_ms: u64) -> Result<(), PrivateFactsWorkerError> {
        self.fill_recovery.fence_epoch();
        self.scheduler.enter_backoff(now_ms);
        self.ready_projection = None;
        self.ready_authority_projection = None;
        let disconnect = self.lock_session()?.on_disconnect();
        if disconnect.is_err()
            && let Ok(mut session) = self.session.lock()
        {
            session.fail_closed_in_memory();
        }
        disconnect.map(|_| ()).map_err(Into::into)
    }

    fn session_generation(&self) -> Result<u64, PrivateFactsWorkerError> {
        Ok(self.lock_session()?.generation())
    }

    fn lock_session(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, PrivateEvidenceSession>, PrivateFactsWorkerError> {
        self.session
            .lock()
            .map_err(|_| PrivateFactsWorkerError::Lock)
    }

    fn idle_wait(&self, now_ms: u64) -> Duration {
        self.scheduler.idle_wait(now_ms)
    }
}

pub struct BinancePrivateFactsTransport {
    client: Arc<PrivateRest>,
    listen_key: Option<PrivateListenKey>,
    socket: Option<PrivateStreamSocket>,
    next_sequence: u64,
    bootstrap: Option<TransportBootstrap>,
}

impl BinancePrivateFactsTransport {
    pub fn production(
        credentials: PrivateCredentials,
        binding: BinanceAccountBinding,
    ) -> Result<Self, PrivateFactsWorkerError> {
        Ok(Self {
            client: Arc::new(PrivateRest::production(credentials, binding)?),
            listen_key: None,
            socket: None,
            next_sequence: 1,
            bootstrap: None,
        })
    }

    pub fn authoritative_now_ms(&self) -> Result<u64, PrivateFactsWorkerError> {
        self.client.authoritative_now_ms().map_err(Into::into)
    }

    pub(crate) fn order_by_client_id(
        &self,
        symbol: &Symbol,
        client_order_id: &str,
    ) -> Result<String, PrivateFactsWorkerError> {
        self.client
            .order_by_client_id(symbol, client_order_id)
            .map_err(Into::into)
    }

    pub(crate) fn private_rest(&self) -> &PrivateRest {
        self.client.as_ref()
    }

    pub(crate) fn private_rest_handle(&self) -> Arc<PrivateRest> {
        Arc::clone(&self.client)
    }

    fn connect(&mut self) -> Result<(), PrivateFactsWorkerError> {
        self.close();
        let listen_key = self.client.create_user_stream()?;
        let mut socket = PrivateStreamSocket::connect(&listen_key)?;
        if let Err(error) = socket.set_read_timeout(Duration::from_millis(FRAME_POLL_MS)) {
            return Err(error.into());
        }
        self.listen_key = Some(listen_key);
        self.socket = Some(socket);
        self.next_sequence = 1;
        Ok(())
    }

    fn bootstrap_scope(
        &mut self,
        worker: &BinancePrivateFactsWorker,
        ticket: PrivateReadbackTicket,
        scope: PrivateBootstrapScope,
    ) -> Result<Option<PrivateFactsBootstrap>, PrivateFactsWorkerError> {
        match scope {
            PrivateBootstrapScope::Account => {
                let target_through_ms = self.client.authoritative_now_ms()?;
                let payload = self.client.account()?;
                let balance = binance_portfolio::parse_account_balance(&payload)?;
                self.bootstrap = Some(TransportBootstrap {
                    ticket,
                    target_through_ms,
                    balances: vec![balance],
                    positions: None,
                    one_way_position: None,
                    can_trade: None,
                    orders: None,
                    open_algo_orders: None,
                });
                Ok(None)
            }
            PrivateBootstrapScope::Positions => {
                self.ensure_bootstrap_ticket(ticket)?;
                let payload = self.client.positions(&worker.config.symbol)?;
                let positions = binance_private::parse_positions(&payload, &worker.config.symbol)?;
                self.bootstrap_mut()?.positions = Some(positions);
                Ok(None)
            }
            PrivateBootstrapScope::PositionMode => {
                self.ensure_bootstrap_ticket(ticket)?;
                let payload = self.client.position_mode()?;
                self.bootstrap_mut()?.one_way_position =
                    Some(binance_private::parse_portfolio_position_mode(&payload)?);
                Ok(None)
            }
            PrivateBootstrapScope::AccountConfig => {
                self.ensure_bootstrap_ticket(ticket)?;
                let payload = self.client.um_account_config()?;
                self.bootstrap_mut()?.can_trade = Some(binance_private::parse_can_trade(&payload)?);
                Ok(None)
            }
            PrivateBootstrapScope::Orders => {
                self.ensure_bootstrap_ticket(ticket)?;
                let payload = self.client.open_orders(&worker.config.symbol)?;
                self.bootstrap_mut()?.orders = Some(binance_private::parse_orders(
                    &payload,
                    &worker.config.symbol,
                )?);
                Ok(None)
            }
            PrivateBootstrapScope::AlgoOrders => {
                self.ensure_bootstrap_ticket(ticket)?;
                let payload = self.client.open_algo_orders(&worker.config.symbol)?;
                self.bootstrap_mut()?.open_algo_orders = Some(
                    binance_private::parse_open_algo_orders(&payload, &worker.config.symbol)?,
                );
                Ok(None)
            }
            PrivateBootstrapScope::Fills => {
                self.ensure_bootstrap_ticket(ticket)?;
                let target_through_ms = self.bootstrap_ref()?.target_through_ms;
                let fills = self.client.recent_fills_since(
                    &worker.config.symbol,
                    worker.current_recent_fills_cursor()?,
                    target_through_ms,
                )?;
                let progress = self
                    .bootstrap
                    .take()
                    .ok_or(PrivateFactsWorkerError::State)?;
                let one_way_position = progress
                    .one_way_position
                    .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?;
                let positions = binance_portfolio::complete_scoped_positions(
                    progress
                        .positions
                        .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?,
                    &worker.config.symbol,
                    !one_way_position,
                );
                Ok(Some(PrivateFactsBootstrap {
                    generation: ticket.generation(),
                    target_through_ms,
                    account: PrivateReadback {
                        capabilities: binance_private::PrivateAccountCapabilities {
                            can_trade: progress
                                .can_trade
                                .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?,
                            one_way_position,
                            hedge_position: !one_way_position,
                        },
                        balances: progress.balances,
                        positions,
                        orders: progress
                            .orders
                            .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?,
                        fills: Vec::new(),
                    },
                    fills,
                    open_algo_orders: progress
                        .open_algo_orders
                        .ok_or(PrivateFactsWorkerError::IncompleteBootstrap)?,
                }))
            }
        }
    }

    fn ensure_bootstrap_ticket(
        &self,
        ticket: PrivateReadbackTicket,
    ) -> Result<(), PrivateFactsWorkerError> {
        if self.bootstrap_ref()?.ticket != ticket {
            return Err(PrivateFactsWorkerError::StaleEpoch);
        }
        Ok(())
    }

    fn bootstrap_ref(&self) -> Result<&TransportBootstrap, PrivateFactsWorkerError> {
        self.bootstrap
            .as_ref()
            .ok_or(PrivateFactsWorkerError::State)
    }

    fn bootstrap_mut(&mut self) -> Result<&mut TransportBootstrap, PrivateFactsWorkerError> {
        self.bootstrap
            .as_mut()
            .ok_or(PrivateFactsWorkerError::State)
    }

    fn poll_frame(&mut self) -> Result<Option<(u64, String)>, PrivateFactsWorkerError> {
        let payload = {
            let socket = self.socket.as_mut().ok_or(PrivateFactsWorkerError::State)?;
            socket.next_text_when_ready()?
        };
        let Some(payload) = payload else {
            return Ok(None);
        };
        let listen_key = self
            .listen_key
            .as_ref()
            .ok_or(PrivateFactsWorkerError::State)?;
        let payload = crate::exchange::binance::sanitize_private_stream_payload_for_transport(
            listen_key, payload,
        )?;
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .ok_or(PrivateFactsWorkerError::SequenceGap)?;
        Ok(Some((sequence, payload)))
    }

    fn keepalive(&self) -> Result<(), PrivateFactsWorkerError> {
        if self.listen_key.is_none() {
            return Err(PrivateFactsWorkerError::State);
        }
        self.client.keepalive_user_stream()?;
        Ok(())
    }

    pub fn close(&mut self) {
        self.socket = None;
        self.next_sequence = 1;
        self.bootstrap = None;
        // The PAPI listen key is account-scoped and may be shared by independently restartable
        // symbol workers. Dropping one worker must never close the remote stream for the others.
        self.listen_key = None;
    }
}

impl Drop for BinancePrivateFactsTransport {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivateFactsTurn {
    Idle,
    Connected,
    Bootstrap(PrivateBootstrapScope),
    Ready(PrivateFactsCommitReport),
    Frame,
    Keepalive,
    Fenced,
}

fn parse_durable_stream_full_fill(
    payload: &str,
    symbol: &Symbol,
    private_generation: u64,
    received_at_ms: u64,
) -> Option<DurableStreamFullFill> {
    let event = serde_json::from_str::<serde_json::Value>(payload).ok()?;
    if event.get("e")?.as_str()? != "ORDER_TRADE_UPDATE" {
        return None;
    }
    let order = event.get("o")?;
    if order.get("s")?.as_str()? != native_symbol(symbol)
        || order.get("x")?.as_str()? != "TRADE"
        || order.get("X")?.as_str()? != "FILLED"
    {
        return None;
    }
    let fill_id = json_scalar_string(order.get("t")?)?;
    if fill_id == "0" {
        return None;
    }
    let fill_price = Price::new(order.get("L")?.as_str()?.parse::<Decimal>().ok()?).ok()?;
    let maker = FieldState::Known(order.get("m")?.as_bool()?);
    Some(DurableStreamFullFill {
        fill_id,
        private_generation,
        client_order_id: order.get("c")?.as_str()?.to_owned(),
        event_time_ms: json_u64(event.get("E")?)?,
        received_at_ms,
        fill_price,
        maker,
    })
}

fn json_scalar_string(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_u64().map(|value| value.to_string()))
}

fn json_u64(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

pub fn drive_binance_private_facts_turn(
    worker: &mut BinancePrivateFactsWorker,
    transport: &mut BinancePrivateFactsTransport,
    now_ms: u64,
) -> Result<PrivateFactsTurn, PrivateFactsWorkerError> {
    let Some(effect) = worker.next_effect(now_ms)? else {
        return Ok(PrivateFactsTurn::Idle);
    };
    match effect {
        PrivateFactsEffect::Connect { effect_id } => match transport.connect() {
            Ok(()) => match worker.complete_connect(effect_id) {
                Ok(_) => {
                    worker.clear_failure();
                    Ok(PrivateFactsTurn::Connected)
                }
                Err(_) => {
                    worker.record_failure(PrivateFactsFailureStage::ConnectCompletion);
                    transport.close();
                    worker.force_fence(now_ms);
                    Ok(PrivateFactsTurn::Fenced)
                }
            },
            Err(_) => {
                worker.record_failure(PrivateFactsFailureStage::Connect);
                worker.complete_transport_failure(effect_id, now_ms)?;
                Ok(PrivateFactsTurn::Fenced)
            }
        },
        PrivateFactsEffect::Bootstrap {
            effect_id,
            ticket,
            scope,
        } => match transport.bootstrap_scope(worker, ticket, scope) {
            Ok(None) if scope != PrivateBootstrapScope::Fills => {
                match worker.complete_bootstrap_scope(effect_id, ticket, scope, now_ms) {
                    Ok(()) => {
                        worker.clear_failure();
                        Ok(PrivateFactsTurn::Bootstrap(scope))
                    }
                    Err(_) => {
                        worker.record_failure(PrivateFactsFailureStage::BootstrapCompletion(scope));
                        transport.close();
                        Ok(PrivateFactsTurn::Fenced)
                    }
                }
            }
            Ok(Some(bootstrap)) if scope == PrivateBootstrapScope::Fills => {
                match worker.complete_bootstrap(effect_id, bootstrap, now_ms) {
                    Ok(report) => {
                        worker.clear_failure();
                        Ok(PrivateFactsTurn::Ready(report))
                    }
                    Err(_) => {
                        worker.record_failure(PrivateFactsFailureStage::BootstrapCompletion(scope));
                        transport.close();
                        Ok(PrivateFactsTurn::Fenced)
                    }
                }
            }
            Ok(None | Some(_)) => {
                worker.record_failure(PrivateFactsFailureStage::BootstrapTransport(scope));
                transport.close();
                worker.complete_transport_failure(effect_id, now_ms)?;
                Ok(PrivateFactsTurn::Fenced)
            }
            Err(_) => {
                worker.record_failure(PrivateFactsFailureStage::BootstrapTransport(scope));
                transport.close();
                worker.complete_transport_failure(effect_id, now_ms)?;
                Ok(PrivateFactsTurn::Fenced)
            }
        },
        PrivateFactsEffect::ReceiveFrame { effect_id, .. } => match transport.poll_frame() {
            Ok(Some((sequence, payload))) => {
                match worker.complete_frame(effect_id, sequence, now_ms, payload, now_ms) {
                    Ok(_) => {
                        worker.clear_failure();
                        Ok(PrivateFactsTurn::Frame)
                    }
                    Err(_) => {
                        worker.record_failure(PrivateFactsFailureStage::FrameCompletion);
                        transport.close();
                        Ok(PrivateFactsTurn::Fenced)
                    }
                }
            }
            Ok(None) => {
                worker.complete_no_frame(effect_id)?;
                Ok(PrivateFactsTurn::Idle)
            }
            Err(_) => {
                worker.record_failure(PrivateFactsFailureStage::FrameTransport);
                transport.close();
                worker.complete_transport_failure(effect_id, now_ms)?;
                Ok(PrivateFactsTurn::Fenced)
            }
        },
        PrivateFactsEffect::Keepalive { effect_id, .. } => match transport.keepalive() {
            Ok(()) => match worker.complete_keepalive(effect_id, now_ms) {
                Ok(()) => {
                    worker.clear_failure();
                    Ok(PrivateFactsTurn::Keepalive)
                }
                Err(_) => {
                    worker.record_failure(PrivateFactsFailureStage::KeepaliveCompletion);
                    transport.close();
                    Ok(PrivateFactsTurn::Fenced)
                }
            },
            Err(_) => {
                worker.record_failure(PrivateFactsFailureStage::Keepalive);
                transport.close();
                worker.complete_transport_failure(effect_id, now_ms)?;
                Ok(PrivateFactsTurn::Fenced)
            }
        },
    }
}

pub fn run_binance_private_facts_worker(
    worker: &mut BinancePrivateFactsWorker,
    transport: &mut BinancePrivateFactsTransport,
    shutdown: &AtomicBool,
    mut now_ms: impl FnMut() -> Result<u64, PrivateFactsWorkerError>,
) -> Result<(), PrivateFactsWorkerError> {
    while !shutdown.load(Ordering::Acquire) {
        let now = now_ms()?;
        match drive_binance_private_facts_turn(worker, transport, now) {
            Ok(PrivateFactsTurn::Idle) => thread::sleep(worker.idle_wait(now)),
            Ok(_) => {}
            Err(_) => {
                transport.close();
                worker.force_fence(now);
                thread::sleep(worker.idle_wait(now));
            }
        }
    }
    transport.close();
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum PrivateFactsWorkerError {
    #[error("private facts worker configuration is invalid")]
    Config,
    #[error("private facts worker issued or completed an invalid effect")]
    Effect,
    #[error("private facts worker lifecycle is invalid")]
    State,
    #[error("private facts worker lock is poisoned")]
    Lock,
    #[error("private stream sequence is missing or belongs to another epoch")]
    SequenceGap,
    #[error("private readback belongs to a stale session epoch")]
    StaleEpoch,
    #[error("private signed bootstrap is incomplete")]
    IncompleteBootstrap,
    #[error("private stream reader closed")]
    StreamClosed,
    #[error("private facts worker I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("private stream session failed: {0}")]
    Session(#[from] PrivateSessionError),
    #[error("private evidence failed: {0}")]
    Evidence(#[from] PrivateEvidenceError),
    #[error("private facts journal failed: {0}")]
    Storage(#[from] StorageError),
    #[error("private fill cursor failed: {0}")]
    FillCursor(#[from] FillCursorError),
    #[error("private fill recovery failed: {0}")]
    FillRecovery(#[from] FillRecoveryError),
    #[error("private reconciliation failed: {0}")]
    Reconciliation(#[from] ReconciliationError),
    #[error("private projection command journal failed: {0}")]
    CommandJournal(#[from] CommandJournalError),
    #[error("private projection writer authority failed: {0}")]
    WriterLease(#[from] WriterLeaseError),
    #[error("Binance private transport failed: {0}")]
    Private(#[from] PrivateError),
    #[error("Binance private readback failed: {0}")]
    Readback(#[from] PrivateReadbackError),
    #[error("Binance private payload failed: {0}")]
    Parse(#[from] PrivateParseError),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrivateFactsCommitReport {
    pub fills: FillRecoveryReport,
    pub account: ReconciliationReport,
}

impl From<PrivateFactsScheduleError> for PrivateFactsWorkerError {
    fn from(error: PrivateFactsScheduleError) -> Self {
        match error {
            PrivateFactsScheduleError::Policy => Self::Config,
            PrivateFactsScheduleError::Effect => Self::Effect,
            PrivateFactsScheduleError::State => Self::State,
            PrivateFactsScheduleError::Generation => Self::StaleEpoch,
        }
    }
}

#[cfg(test)]
#[path = "private_facts_worker_tests.rs"]
mod tests;
