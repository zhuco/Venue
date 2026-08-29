use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    backoff::jittered_exponential_delay_ms,
    config::Config,
    domain::{
        CancelCommand, CommandId, FieldState, MarketOrderCommand, MarketReduceCommand,
        OrderCommand, OrderOwner, OrderPurpose, OrderSide, OrderState, PositionSide, Price,
    },
    exchange::{
        binance::{
            BinanceError, PrivateCredentials, PrivateError, PublicError, PublicRest,
            parse_depth_best_prices, parse_instrument,
        },
        binance_private,
    },
    execution::{
        CommandJournal, CommandJournalError, CommandState, FlatReceipt, WriterLeaseAuthority,
        WriterLeaseError, WriterScope, WriterSession, sha256_hex,
    },
    storage::{ProjectionStore, StorageError},
    strategy::hedged_grid::{
        GridAction, GridDecision, GridEpoch, GridInventory, GridOrderIntent, GridOrderKey,
        GridOrderRole, GridPhase, GridPosition, GridResetReason, HedgedGridBinding,
        HedgedGridError, HedgedGridParams, HedgedGridState, OwnedGridFill,
    },
};

use super::{
    BinancePrivateFactsTransport, BinancePrivateFactsWorker, BinancePrivateFactsWorkerConfig,
    PrivateFactsSnapshot, PrivateFactsTurn, PrivateFactsWorkerError,
    drive_binance_private_facts_turn, hedged_grid_hot_path::HedgedGridHotPath,
};

const EXCHANGE: &str = "binance";
pub(in crate::runtime) const GRID_CONTROL_FILE: &str = "hedged_grid_control.json";
pub(in crate::runtime) const GRID_CHECKPOINT_FILE: &str = "hedged_grid_state.json";
pub(in crate::runtime) const COMMAND_FILE: &str = "commands.jsonl";
pub(in crate::runtime) const WRITER_FILE: &str = "writer.json";
const INITIAL_FILL_LOOKBACK_MS: u64 = 60_000;
const GRID_PRIVATE_READBACK_INTERVAL_MS: u64 = 10 * 60 * 1_000;
const STARTUP_RETRY_BASE_MS: u64 = 250;
const STARTUP_RETRY_CAP_MS: u64 = 5_000;
pub(super) const GRID_REJECTED_RESET_DELAY_MS: u64 = 30_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HedgedGridLiveRequest {
    pub artifacts_root: PathBuf,
    pub max_turns: Option<u64>,
    pub reset_on_start: bool,
    pub skip_inventory_replenishment_until_recovered: bool,
    pub confirm_mainnet_grid_mutations: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HedgedGridLiveReport {
    pub turns: u64,
    pub phase: GridPhase,
    pub private_generation: u64,
    pub checkpoint_path: PathBuf,
    pub stopped: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HedgedGridControlTarget {
    Running,
    Stop,
    Reset,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::runtime) struct HedgedGridControl {
    pub(in crate::runtime) schema_version: u16,
    pub(in crate::runtime) binding: HedgedGridBinding,
    pub(in crate::runtime) target: HedgedGridControlTarget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::runtime) struct HedgedGridCheckpoint {
    pub(in crate::runtime) schema_version: u16,
    pub(in crate::runtime) state: HedgedGridState,
}

#[path = "binance_exposure.rs"]
mod binance_exposure;
#[path = "hedged_grid_legacy_dispatch.rs"]
mod legacy_dispatch;
pub(in crate::runtime) use legacy_dispatch::LegacyGridMutationEndpoint;
use legacy_dispatch::{BinanceLegacyGridMutationEndpoint, dispatch_mutations_with_endpoint};

#[derive(Clone)]
pub(super) enum GridMutation {
    Place(OrderCommand),
    Market(MarketOrderCommand),
    Reduce(MarketReduceCommand),
    Cancel(CancelCommand),
}

impl GridMutation {
    pub(super) fn command_id(&self) -> &CommandId {
        match self {
            Self::Place(command) => &command.command_id,
            Self::Market(command) => &command.command_id,
            Self::Reduce(command) => &command.command_id,
            Self::Cancel(command) => &command.command_id,
        }
    }

    pub(super) fn prepare(&self, journal: &mut CommandJournal) -> Result<(), CommandJournalError> {
        match self {
            Self::Place(command) => journal.prepare_place(command.clone()).map(|_| ()),
            Self::Market(command) => journal.prepare_market(command.clone()).map(|_| ()),
            Self::Reduce(command) => journal.prepare_market_reduce(command.clone()).map(|_| ()),
            Self::Cancel(command) => journal.prepare_cancel(command.clone()).map(|_| ()),
        }
    }

    pub(super) fn submit(
        &self,
        private: &crate::exchange::binance::PrivateRest,
    ) -> Result<String, PrivateError> {
        match self {
            Self::Place(command) => private.place_limit_post_only(command),
            Self::Market(command) => private.place_market(command),
            Self::Reduce(command) => private.place_market_reduce(command),
            Self::Cancel(command) => private.cancel_by_client_id(
                &command.owner.symbol,
                command.target_client_order_id.as_str(),
            ),
        }
    }
}

pub fn set_hedged_grid_control(
    artifacts_root: &Path,
    trading_account_id: &str,
    target: HedgedGridControlTarget,
) -> Result<(), HedgedGridLiveError> {
    if !artifacts_root.is_absolute() {
        return Err(HedgedGridLiveError::ArtifactsRoot);
    }
    fs::create_dir_all(artifacts_root).map_err(|source| HedgedGridLiveError::Io {
        path: artifacts_root.to_path_buf(),
        source,
    })?;
    let binding = phase_one_binding_for_account(trading_account_id)?;
    let control = HedgedGridControl {
        schema_version: 1,
        binding,
        target,
    };
    ProjectionStore::new(artifacts_root.join(GRID_CONTROL_FILE)).save(&control)?;
    Ok(())
}

pub(in crate::runtime) fn request_existing_hedged_grid_stop(
    artifacts_root: &Path,
) -> Result<(), HedgedGridLiveError> {
    if !artifacts_root.is_absolute() {
        return Err(HedgedGridLiveError::ArtifactsRoot);
    }
    let store = ProjectionStore::new(artifacts_root.join(GRID_CONTROL_FILE));
    let mut control = store
        .load::<HedgedGridControl>()?
        .ok_or(HedgedGridLiveError::Stopped)?;
    if control.schema_version != 1
        || control.binding.exchange != EXCHANGE
        || control.binding.config_version != "phase1"
        || control.binding.symbol.to_string() != "SOL/USDC"
    {
        return Err(HedgedGridLiveError::Binding);
    }
    control.target = HedgedGridControlTarget::Stop;
    store.save(&control)?;
    Ok(())
}

/// Runs one symbol actor against the account-level private worker. It intentionally has no
/// Shadow or replay branch: all physical actions remain scoped by the fixed binding, writer
/// lease, durable command WAL and the worker's next authoritative private generation.
pub fn run_hedged_grid_live(
    cfg: &Config,
    request: HedgedGridLiveRequest,
) -> Result<HedgedGridLiveReport, HedgedGridLiveError> {
    if !request.confirm_mainnet_grid_mutations {
        return Err(HedgedGridLiveError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(HedgedGridLiveError::ArtifactsRoot);
    }
    let binding = phase_one_binding_for_account(&cfg.trading_account_id)?;
    binding.validate().map_err(HedgedGridLiveError::Strategy)?;
    if cfg.symbol != binding.symbol
        || cfg.binance.as_ref().is_none_or(|binding| {
            binding.account_binding != crate::config::BinanceAccountBinding::PortfolioMarginUm
        })
    {
        return Err(HedgedGridLiveError::Binding);
    }
    let writer_scope = WriterScope {
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        owner_scope: binding.owner_scope.clone(),
    };
    let _account_writer_root =
        super::stage7_writer_registry::acquire(&writer_scope, &request.artifacts_root).map_err(
            |error| HedgedGridLiveError::WriterRegistry {
                reason: error.to_string(),
            },
        )?;

    let control_store = ProjectionStore::new(request.artifacts_root.join(GRID_CONTROL_FILE));
    let control = control_store
        .load::<HedgedGridControl>()?
        .ok_or(HedgedGridLiveError::Stopped)?;
    validate_control(&control, &binding)?;
    if control.target == HedgedGridControlTarget::Stop {
        return Err(HedgedGridLiveError::Stopped);
    }

    let grid_config = cfg
        .hedged_grid
        .ok_or(HedgedGridLiveError::GridConfigRequired)?;
    let release_params = HedgedGridParams::phase_one(grid_config.grid_count)?;
    let checkpoint_path = request.artifacts_root.join(GRID_CHECKPOINT_FILE);
    let checkpoint_store = ProjectionStore::new(&checkpoint_path);
    let mut state = load_state(&checkpoint_store, &binding, &release_params)?;
    if resume_stopping_state_if_requested(&mut state, control.target)? {
        save_state(&checkpoint_store, &state)?;
        set_hedged_grid_control(
            &request.artifacts_root,
            &binding.account,
            HedgedGridControlTarget::Running,
        )?;
    }
    let previous_grid_count = state.params.grid_count;
    if apply_release_params(&mut state, release_params, request.reset_on_start)? {
        info!(
            event = "hedged_grid_parameter_release_applied",
            previous_grid_count,
            grid_count = state.params.grid_count,
            inventory_replenish_grid_count = state.params.inventory_replenish_grid_count,
            "已将检查点升级为当前网格参数并持久化重置态；本轮会重建订单"
        );
        save_state(&checkpoint_store, &state)?;
        set_hedged_grid_control(
            &request.artifacts_root,
            &binding.account,
            HedgedGridControlTarget::Running,
        )?;
    }
    let mut commands = CommandJournal::open(request.artifacts_root.join(COMMAND_FILE))?;
    let _ = commands.fence_interrupted_dispatches()?;
    let mut restart_without_replenishment_pending =
        request.skip_inventory_replenishment_until_recovered;
    if restart_without_replenishment_pending && state.phase != GridPhase::BlockedUnknown {
        state.request_restart_without_replenishment()?;
        save_state(&checkpoint_store, &state)?;
        restart_without_replenishment_pending = false;
    }

    let mut worker = BinancePrivateFactsWorker::open(BinancePrivateFactsWorkerConfig {
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        artifacts_root: request.artifacts_root.join("private"),
        initial_fill_recovery_from_ms: wall_clock_ms()?.saturating_sub(INITIAL_FILL_LOOKBACK_MS),
    })?;
    worker.enable_durable_fill_fast_path();
    worker.set_periodic_readback_interval(GRID_PRIVATE_READBACK_INTERVAL_MS)?;
    let mut transport = open_grid_private_transport(&cfg, &control_store, &binding)?;
    if begin_rejected_epoch_recovery(&mut state, &commands, transport.authoritative_now_ms()?)? {
        save_state(&checkpoint_store, &state)?;
    }
    let public = PublicRest::production()?;
    let instrument = open_grid_public_instrument(&public, &control_store, &binding)?;
    if instrument
        .settlement_asset
        .as_ref()
        .is_none_or(|asset| asset.as_str() != "USDC")
    {
        return Err(HedgedGridLiveError::Instrument);
    }

    let authority = WriterLeaseAuthority::open(
        request.artifacts_root.join(WRITER_FILE),
        WriterScope {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            owner_scope: binding.owner_scope.clone(),
        },
    )?;
    let mut writer: Option<WriterSession> = None;
    let mut turns = 0_u64;
    let mut last_generation = 0_u64;
    let mut hot_path = HedgedGridHotPath::default();
    let mut hot_reconciliation_pending = false;
    let mut blocked_reconciliation_wakeup_requested = false;
    let mut exposure = binance_exposure::BinanceExposureRuntime::open(
        grid_config.exposure_take_profit,
        &binding,
        &request.artifacts_root,
    )?;
    if let Some(exposure) = exposure.as_mut() {
        exposure.recover_unjournaled(&commands)?;
    }

    loop {
        if request.max_turns.is_some_and(|limit| turns >= limit) {
            return Ok(HedgedGridLiveReport {
                turns,
                phase: state.phase,
                private_generation: last_generation,
                checkpoint_path,
                stopped: false,
            });
        }
        turns = turns.checked_add(1).ok_or(HedgedGridLiveError::Clock)?;
        let now_ms = transport.authoritative_now_ms()?;
        hot_reconciliation_pending |= hot_path.drain_completions(
            &mut state,
            &checkpoint_store,
            &mut commands,
            &binding,
            &instrument,
            now_ms,
        )?;
        if !hot_path.has_in_flight() {
            if hot_reconciliation_pending {
                worker.request_post_mutation_reconciliation(now_ms)?;
                blocked_reconciliation_wakeup_requested = state.phase == GridPhase::BlockedUnknown;
                hot_reconciliation_pending = false;
            }
            hot_path.release_dispatcher_if_idle();
        }
        if state.blocked_reconciliation_is_due(now_ms) && !blocked_reconciliation_wakeup_requested {
            worker.request_post_mutation_reconciliation(now_ms)?;
            blocked_reconciliation_wakeup_requested = true;
        }
        let target = read_control(&control_store, &binding)?;
        if target == HedgedGridControlTarget::Stop {
            if state.phase != GridPhase::Stopping {
                state.phase = GridPhase::Stopping;
                save_state(&checkpoint_store, &state)?;
            }
            if !state.owned_orders.is_empty() && !hot_path.has_in_flight() {
                if let Some(session) = writer.as_ref() {
                    dispatch_reset_orders(
                        &mut commands,
                        &transport,
                        &authority,
                        session,
                        &binding,
                        &state,
                        None,
                        now_ms,
                    )?;
                    state.owned_orders.clear();
                    save_state(&checkpoint_store, &state)?;
                    worker.request_post_mutation_reconciliation(now_ms)?;
                }
            } else {
                return Ok(HedgedGridLiveReport {
                    turns,
                    phase: state.phase,
                    private_generation: last_generation,
                    checkpoint_path,
                    stopped: true,
                });
            }
        } else if target == HedgedGridControlTarget::Reset && state.phase == GridPhase::Running {
            let _ = state.request_reset(GridResetReason::Manual)?;
            save_state(&checkpoint_store, &state)?;
            set_hedged_grid_control(
                &request.artifacts_root,
                &binding.account,
                HedgedGridControlTarget::Running,
            )?;
        }

        if let (Some(exposure), Some(session)) = (exposure.as_mut(), writer.as_ref())
            && target == HedgedGridControlTarget::Running
            && state.phase == GridPhase::Running
            && last_generation > 0
            && !hot_path.has_in_flight()
            && !commands.has_unresolved()
            && exposure.due(now_ms)
        {
            match exposure.poll(
                &mut commands,
                &transport,
                &authority,
                session,
                &binding,
                &instrument,
                last_generation,
                now_ms,
            ) {
                Ok(true) => {
                    worker.request_post_mutation_reconciliation(now_ms)?;
                    continue;
                }
                Ok(false) => {}
                Err(error @ HedgedGridLiveError::Exposure { .. }) => {
                    warn!(
                        event = "binance_exposure_snapshot_failed_closed",
                        reason = %error,
                        "Binance风险快照失败关闭；保留原网格并请求签名对账"
                    );
                    worker.request_post_mutation_reconciliation(now_ms)?;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }

        match drive_binance_private_facts_turn(&mut worker, &mut transport, now_ms)? {
            PrivateFactsTurn::Ready(_) => {
                if hot_path.has_in_flight() {
                    continue;
                }
                let snapshot = worker.snapshot()?.ok_or(HedgedGridLiveError::Snapshot)?;
                last_generation = snapshot.generation;
                recover_absent_grid_unknowns(&mut commands, &transport, &binding, &snapshot)?;
                if commands.has_unresolved() {
                    if exposure
                        .as_ref()
                        .is_some_and(binance_exposure::BinanceExposureRuntime::has_pending)
                    {
                        worker.request_post_mutation_reconciliation(now_ms)?;
                        thread::sleep(Duration::from_millis(250));
                        continue;
                    }
                    if state.phase == GridPhase::BlockedUnknown {
                        blocked_reconciliation_wakeup_requested = false;
                        thread::sleep(Duration::from_millis(250));
                        continue;
                    }
                    return Err(HedgedGridLiveError::Unresolved);
                }
                if let Some(exposure) = exposure.as_mut()
                    && exposure.has_pending()
                    && exposure.settle(
                        &mut commands,
                        &transport,
                        &snapshot,
                        &binding,
                        &mut state,
                        &checkpoint_store,
                        &instrument,
                        &authority,
                        writer.as_ref(),
                        &request.artifacts_root,
                        now_ms,
                    )?
                {
                    worker.request_post_mutation_reconciliation(now_ms)?;
                    thread::sleep(Duration::from_millis(250));
                    continue;
                }
                if state.phase == GridPhase::BlockedUnknown {
                    if !state.blocked_reconciliation_is_due(now_ms) {
                        // The first readback may settle UNKNOWN command receipts immediately, but
                        // it must not turn a short-lived rejection into reset churn. A second
                        // authoritative generation is scheduled when the durable 30s grace ends.
                        blocked_reconciliation_wakeup_requested = false;
                        continue;
                    }
                    let owned = recovered_owned_orders(&commands, &binding, &snapshot)?;
                    state.reconcile_blocked_orders(owned)?;
                    save_state(&checkpoint_store, &state)?;
                    blocked_reconciliation_wakeup_requested = false;
                }
                if restart_without_replenishment_pending {
                    state.request_restart_without_replenishment()?;
                    save_state(&checkpoint_store, &state)?;
                    restart_without_replenishment_pending = false;
                }
                // Bootstrap has seven signed scopes and may outlive a writer lease. Re-read the
                // already synchronized Binance clock only after Ready, before creating or using
                // any physical dispatch authority.
                let ready_now_ms = transport.authoritative_now_ms()?;
                let session = active_writer(
                    &authority,
                    writer.take(),
                    ready_now_ms,
                    snapshot.observed_at_ms,
                    Some(&snapshot),
                    &state,
                )?;
                writer = Some(session.clone());
                if target == HedgedGridControlTarget::Stop {
                    dispatch_reset_orders(
                        &mut commands,
                        &transport,
                        &authority,
                        &session,
                        &binding,
                        &state,
                        Some(&snapshot),
                        ready_now_ms,
                    )?;
                    state.owned_orders.clear();
                    save_state(&checkpoint_store, &state)?;
                    worker.request_post_mutation_reconciliation(ready_now_ms)?;
                    continue;
                }
                let mutated = match drive_ready(
                    &mut state,
                    &checkpoint_store,
                    &mut commands,
                    &transport,
                    &public,
                    &instrument,
                    &authority,
                    &session,
                    &binding,
                    &snapshot,
                    ready_now_ms,
                ) {
                    Ok(mutated) => mutated,
                    Err(HedgedGridLiveError::Public(
                        PublicError::Http(_)
                        | PublicError::TransportRetriesExhausted
                        | PublicError::RateLimited
                        | PublicError::ServerFailure(_),
                    )) => {
                        // Public depth is advisory for mark fallback/epoch anchoring. A transient
                        // REST or proxy-tunnel failure must leave the durable grid untouched and
                        // retry the resident loop, not terminate a healthy private user stream.
                        thread::sleep(Duration::from_millis(250));
                        continue;
                    }
                    Err(HedgedGridLiveError::Strategy(HedgedGridError::Phase)) => {
                        // A fill/readback may advance the durable grid boundary just before this
                        // turn uses its snapshot. The snapshot is then stale for that transition,
                        // not a reason to stop the symbol. Obtain one fresh private generation.
                        warn!(
                            event = "grid_phase_transition_deferred",
                            phase = ?state.phase,
                            "网格状态已推进，延后本轮并等待最新私有事实"
                        );
                        worker.request_post_mutation_reconciliation(ready_now_ms)?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if should_reconcile_after_grid_mutation(
                    mutated,
                    commands.has_unresolved(),
                    state.phase,
                ) {
                    worker.request_post_mutation_reconciliation(ready_now_ms)?;
                    blocked_reconciliation_wakeup_requested =
                        state.phase == GridPhase::BlockedUnknown;
                }
            }
            PrivateFactsTurn::Frame => {
                while let Some(fill) = worker.take_durable_stream_full_fill() {
                    let Some(session) = writer.as_ref() else {
                        return Err(HedgedGridLiveError::Writer(WriterLeaseError::NoWriter));
                    };
                    let fill_now_ms = transport.authoritative_now_ms()?;
                    match hot_path.queue_durable_stream_fill(
                        &mut state,
                        &checkpoint_store,
                        &mut commands,
                        &transport,
                        &authority,
                        session,
                        &binding,
                        &instrument,
                        fill,
                        fill_now_ms,
                    ) {
                        Ok(reconciliation_needed) => {
                            hot_reconciliation_pending |= reconciliation_needed;
                        }
                        Err(HedgedGridLiveError::Strategy(HedgedGridError::Rolling)) => {
                            if state.phase == GridPhase::Running {
                                state.block_for_order_reconciliation()?;
                                state.defer_blocked_reconciliation_until(
                                    fill_now_ms.saturating_add(GRID_REJECTED_RESET_DELAY_MS),
                                )?;
                            }
                            save_state(&checkpoint_store, &state)?;
                            hot_reconciliation_pending = true;
                        }
                        Err(HedgedGridLiveError::Strategy(HedgedGridError::Phase)) => {
                            warn!(
                                event = "grid_stream_phase_transition_deferred",
                                phase = ?state.phase,
                                "成交处理遇到已推进的网格状态，等待私有对账"
                            );
                            hot_reconciliation_pending = true;
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            PrivateFactsTurn::Idle => thread::sleep(Duration::from_millis(1)),
            PrivateFactsTurn::Connected
            | PrivateFactsTurn::Bootstrap(_)
            | PrivateFactsTurn::Keepalive
            | PrivateFactsTurn::Fenced => {}
        }

        hot_reconciliation_pending |= hot_path.drain_completions(
            &mut state,
            &checkpoint_store,
            &mut commands,
            &binding,
            &instrument,
            now_ms,
        )?;
    }
}

fn should_reconcile_after_grid_mutation(
    mutated: bool,
    has_unresolved_commands: bool,
    phase: GridPhase,
) -> bool {
    mutated
        && (has_unresolved_commands
            || matches!(
                phase,
                GridPhase::BlockedUnknown
                    | GridPhase::ReplenishingInventory
                    | GridPhase::ResettingGrid
            ))
}

fn open_grid_private_transport(
    cfg: &Config,
    control_store: &ProjectionStore,
    binding: &HedgedGridBinding,
) -> Result<BinancePrivateFactsTransport, HedgedGridLiveError> {
    let mut failures = 0_u8;
    loop {
        let credentials = PrivateCredentials::from_environment()?;
        let account_binding = cfg
            .binance
            .as_ref()
            .ok_or(HedgedGridLiveError::Binding)?
            .account_binding;
        match BinancePrivateFactsTransport::production(credentials, account_binding) {
            Ok(transport) => return Ok(transport),
            Err(error) if retryable_private_transport_startup_failure(&error) => {
                failures = failures.saturating_add(1);
                if read_control(control_store, binding)? == HedgedGridControlTarget::Stop {
                    return Err(HedgedGridLiveError::Stopped);
                }
                warn!(
                    event = "grid_private_transport_startup_retry",
                    reason = %error,
                    "私有连接初始化暂不可用，短暂退避后重试"
                );
                let delay = jittered_exponential_delay_ms(
                    STARTUP_RETRY_BASE_MS,
                    STARTUP_RETRY_CAP_MS,
                    failures,
                    &binding.account,
                    wall_clock_ms()?,
                );
                thread::sleep(Duration::from_millis(delay));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn retryable_private_transport_startup_failure(error: &PrivateFactsWorkerError) -> bool {
    matches!(
        error,
        PrivateFactsWorkerError::Private(
            PrivateError::Http | PrivateError::Clock | PrivateError::RateLimited(_)
        )
    )
}

/// Exchange metadata is required before a grid can calculate compliant order quantities. A
/// temporary public endpoint outage must leave the resident available to recover, not turn it
/// into a stopped strategy before any mutation was attempted.
fn open_grid_public_instrument(
    public: &PublicRest,
    control_store: &ProjectionStore,
    binding: &HedgedGridBinding,
) -> Result<crate::domain::Instrument, HedgedGridLiveError> {
    let mut failures = 0_u8;
    loop {
        match public.exchange_info() {
            Ok(exchange_info) => {
                return parse_instrument(&exchange_info, binding.symbol.clone(), 1)
                    .map_err(HedgedGridLiveError::from);
            }
            Err(error) if retryable_public_startup_failure(&error) => {
                failures = failures.saturating_add(1);
                if read_control(control_store, binding)? == HedgedGridControlTarget::Stop {
                    return Err(HedgedGridLiveError::Stopped);
                }
                warn!(
                    event = "grid_public_instrument_startup_retry",
                    reason = %error,
                    "公共交易规则暂不可用，短暂退避后重试"
                );
                let delay = jittered_exponential_delay_ms(
                    STARTUP_RETRY_BASE_MS,
                    STARTUP_RETRY_CAP_MS,
                    failures,
                    &binding.account,
                    wall_clock_ms()?,
                );
                thread::sleep(Duration::from_millis(delay));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn retryable_public_startup_failure(error: &PublicError) -> bool {
    matches!(
        error,
        PublicError::Http(_)
            | PublicError::TransportRetriesExhausted
            | PublicError::RateLimited
            | PublicError::ServerFailure(_)
    )
}

#[allow(clippy::too_many_arguments)]
fn drive_ready(
    state: &mut HedgedGridState,
    store: &ProjectionStore,
    commands: &mut CommandJournal,
    transport: &BinancePrivateFactsTransport,
    public: &PublicRest,
    instrument: &crate::domain::Instrument,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    snapshot: &PrivateFactsSnapshot,
    now_ms: u64,
) -> Result<bool, HedgedGridLiveError> {
    if !snapshot.can_trade || !snapshot.hedge_position || commands.has_unresolved() {
        return Err(HedgedGridLiveError::Unresolved);
    }
    if state.epoch.is_none() && !snapshot.orders.is_empty() {
        return Err(HedgedGridLiveError::ForeignOrders);
    }
    let (bid, ask) = parse_depth_best_prices(&public.depth_snapshot(&binding.symbol, 5)?)?;
    let public_mark = Price::new((bid.value() + ask.value()) / Decimal::TWO)
        .map_err(|_| HedgedGridLiveError::Instrument)?;
    let private_generation = strategy_private_generation(state, snapshot)?;
    let inventory = inventory(snapshot, public_mark, private_generation)?;
    if state.phase == GridPhase::ReplenishingInventory {
        state.observe_replenishment_inventory(inventory.clone())?;
        save_state(store, state)?;
        if inventory.notional(GridPosition::Long) < state.params.order_notional.value
            || inventory.notional(GridPosition::Short) < state.params.order_notional.value
        {
            // A market response is not an inventory fact. Wait for a later durable private
            // generation instead of submitting the same replenishment again.
            return Ok(false);
        }
        let _ = state.settle_pending_replenishments()?;
        save_state(store, state)?;
        return install_epoch(
            state, store, commands, transport, public, instrument, authority, writer, binding,
            &inventory, now_ms,
        );
    }
    if state.phase == GridPhase::ResettingGrid {
        let _ = state.observe_inventory(inventory.clone())?;
        save_state(store, state)?;
        let reason = effective_reset_reason(state, &inventory)?;
        return handle_actions(
            state,
            store,
            commands,
            transport,
            public,
            instrument,
            authority,
            writer,
            binding,
            &inventory,
            snapshot,
            vec![GridAction::Reset { reason }],
            now_ms,
        );
    }
    let decision = state.observe_inventory(inventory.clone())?;
    save_state(store, state)?;
    match decision {
        GridDecision::Actions(actions) => {
            return handle_actions(
                state, store, commands, transport, public, instrument, authority, writer, binding,
                &inventory, snapshot, actions, now_ms,
            );
        }
        GridDecision::Blocked => return Err(HedgedGridLiveError::Unresolved),
        GridDecision::Noop => {}
    }

    if matches!(
        state.inventory_recovery,
        crate::strategy::hedged_grid::InventoryRecoveryState::ReanchorPending { .. }
    ) {
        // The fill entrance committed the exact identity and price first. This resident already
        // holds the binding writer, so a second fsync makes the cancellation boundary resumable.
        state.begin_reanchor_rebuild()?;
        save_state(store, state)?;
        return Ok(true);
    }

    if state.phase != GridPhase::Running {
        return Ok(false);
    }

    if matches!(
        state.inventory_recovery,
        crate::strategy::hedged_grid::InventoryRecoveryState::Rebuilding { .. }
    ) {
        if snapshot.orders.len() != state.owned_orders.len()
            || !snapshot_contains_only_owned_orders(state, snapshot)
        {
            // The install response is not an order fact. Keep the durable rebuilding identity
            // and force another complete private generation until every desired identity is
            // visible together.
            return Ok(true);
        }
        match state.complete_reanchor_rebuild() {
            Ok(()) | Err(HedgedGridError::Inventory) => save_state(store, state)?,
            Err(error) => return Err(error.into()),
        }
    }

    if !state.pending_transactions.is_empty() {
        let actions = state
            .pending_transactions
            .values()
            .cloned()
            .map(GridAction::Dispatch)
            .collect();
        return handle_actions(
            state, store, commands, transport, public, instrument, authority, writer, binding,
            &inventory, snapshot, actions, now_ms,
        );
    }

    // The private worker commits a complete fill page before it advances its cursor. Reserve every
    // terminal owned order from that page before any dispatch/readback return; otherwise a mirrored
    // LONG/SHORT burst would process only the first fill and permanently skip its partner.
    let confirmed_fills = match confirmed_missing_owned_fills(
        state,
        commands,
        transport,
        binding,
        snapshot,
        private_generation,
    )? {
        MissingOwnedOrders::Filled(fills) => fills,
        MissingOwnedOrders::Rebuild => {
            let owned = recovered_owned_orders(commands, binding, snapshot)?;
            state.begin_reconciliation_reset(owned)?;
            save_state(store, state)?;
            return handle_actions(
                state,
                store,
                commands,
                transport,
                public,
                instrument,
                authority,
                writer,
                binding,
                &inventory,
                snapshot,
                vec![GridAction::Reset {
                    reason: GridResetReason::Reconciliation,
                }],
                now_ms,
            );
        }
    };
    let actions = match reserve_confirmed_fills(state, store, confirmed_fills) {
        Ok(actions) => actions,
        Err(HedgedGridLiveError::Strategy(HedgedGridError::Rolling)) => {
            let owned = recovered_owned_orders(commands, binding, snapshot)?;
            state.begin_reconciliation_reset(owned)?;
            save_state(store, state)?;
            return handle_actions(
                state,
                store,
                commands,
                transport,
                public,
                instrument,
                authority,
                writer,
                binding,
                &inventory,
                snapshot,
                vec![GridAction::Reset {
                    reason: GridResetReason::Reconciliation,
                }],
                now_ms,
            );
        }
        Err(error) => return Err(error),
    };
    if actions.is_empty() {
        return Ok(false);
    }
    handle_actions(
        state, store, commands, transport, public, instrument, authority, writer, binding,
        &inventory, snapshot, actions, now_ms,
    )
}

fn effective_reset_reason(
    state: &HedgedGridState,
    inventory: &GridInventory,
) -> Result<GridResetReason, HedgedGridLiveError> {
    let reason = state.reset_reason.ok_or(HedgedGridLiveError::Checkpoint)?;
    if reason == GridResetReason::InventoryLow
        && inventory.notional(GridPosition::Long) >= state.params.order_notional.value
        && inventory.notional(GridPosition::Short) >= state.params.order_notional.value
    {
        // The leg may recover while reset cancellations/readbacks are in flight. Re-evaluate the
        // current authoritative inventory instead of trying to replenish a no-longer-low leg.
        return Ok(GridResetReason::InventoryReplenished);
    }
    Ok(reason)
}

pub(super) fn reserve_confirmed_fills(
    state: &mut HedgedGridState,
    store: &ProjectionStore,
    fills: Vec<OwnedGridFill>,
) -> Result<Vec<GridAction>, HedgedGridLiveError> {
    if fills.is_empty() {
        return Ok(Vec::new());
    }
    let mut next = state.clone();
    let mut actions = Vec::new();
    for fill in fills {
        match super::hedged_grid::apply_owned_grid_fill(
            &mut next,
            fill,
            super::hedged_grid::GridFillProjection::SignedInventoryIncluded,
        )? {
            super::hedged_grid::GridFillApplication::Rolling(mut fill_actions) => {
                actions.append(&mut fill_actions);
            }
            super::hedged_grid::GridFillApplication::ReanchorPending => {
                if !actions.is_empty() {
                    return Err(HedgedGridLiveError::Strategy(HedgedGridError::Phase));
                }
                save_state(store, &next)?;
                *state = next;
                return Ok(Vec::new());
            }
            super::hedged_grid::GridFillApplication::Noop => {}
            super::hedged_grid::GridFillApplication::TakerInventoryOnly => {
                save_state(store, &next)?;
                *state = next;
                return Err(HedgedGridLiveError::PostOnlyFillBecameTaker);
            }
            super::hedged_grid::GridFillApplication::AwaitLiquidityEvidence => {
                save_state(store, &next)?;
                *state = next;
                return Err(HedgedGridLiveError::FillLiquidityUnknown);
            }
        }
    }
    save_state(store, &next)?;
    *state = next;
    Ok(actions)
}

#[allow(clippy::too_many_arguments)]
fn handle_actions(
    state: &mut HedgedGridState,
    store: &ProjectionStore,
    commands: &mut CommandJournal,
    transport: &BinancePrivateFactsTransport,
    public: &PublicRest,
    instrument: &crate::domain::Instrument,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    inventory: &GridInventory,
    snapshot: &PrivateFactsSnapshot,
    actions: Vec<GridAction>,
    now_ms: u64,
) -> Result<bool, HedgedGridLiveError> {
    if !actions.is_empty()
        && actions
            .iter()
            .all(|action| matches!(action, GridAction::Dispatch(_)))
    {
        let transactions = actions
            .into_iter()
            .filter_map(|action| match action {
                GridAction::Dispatch(transaction) => Some(transaction),
                GridAction::Reset { .. }
                | GridAction::Place(_)
                | GridAction::Replenish(_)
                | GridAction::ReanchorAtFill { .. } => None,
            })
            .collect::<Vec<_>>();
        return handle_dispatch_transactions(
            state,
            store,
            commands,
            transport,
            instrument,
            authority,
            writer,
            binding,
            transactions,
            now_ms,
        );
    }

    for action in actions {
        match action {
            GridAction::Reset { reason } => {
                let reset_result = dispatch_reset_orders(
                    commands,
                    transport,
                    authority,
                    writer,
                    binding,
                    state,
                    Some(snapshot),
                    now_ms,
                );
                match reset_result {
                    Ok(()) => {}
                    Err(HedgedGridLiveError::Unresolved | HedgedGridLiveError::Rejected) => {
                        save_state(store, state)?;
                        return Ok(true);
                    }
                    Err(error) => return Err(error),
                }
                if let Err(error) = state.reset_orders_settled() {
                    warn!(
                        event = "grid_reset_settlement_phase_mismatch",
                        phase = ?state.phase,
                        pending_transactions = state.pending_transactions.len(),
                        reason = %error,
                        "重置撤单结算的状态不匹配"
                    );
                    return Err(error.into());
                }
                save_state(store, state)?;
                if reason == GridResetReason::InventoryLow {
                    state.reconcile_replenishment_round(highest_durable_replenishment_round(
                        commands, binding,
                    )?)?;
                    let replenishment_decision = match state.begin_replenishment() {
                        Ok(decision) => decision,
                        Err(error) => {
                            warn!(
                                event = "grid_replenishment_phase_mismatch",
                                phase = ?state.phase,
                                reset_reason = ?state.reset_reason,
                                pending_transactions = state.pending_transactions.len(),
                                reason = %error,
                                "库存补充状态不匹配"
                            );
                            return Err(error.into());
                        }
                    };
                    let GridDecision::Actions(actions) = replenishment_decision else {
                        return Err(HedgedGridLiveError::Strategy(HedgedGridError::Phase));
                    };
                    let replenishments = actions
                        .into_iter()
                        .filter_map(|action| match action {
                            GridAction::Replenish(value) => Some(value),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    let mutations = replenishments
                        .iter()
                        .map(|replenishment| {
                            market_command(binding, replenishment, instrument, inventory)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    save_state(store, state)?;
                    dispatch_batch(commands, transport, authority, writer, mutations)?;
                    return Ok(true);
                }
                return install_epoch(
                    state, store, commands, transport, public, instrument, authority, writer,
                    binding, inventory, now_ms,
                );
            }
            GridAction::Dispatch(_) | GridAction::ReanchorAtFill { .. } => {
                return Err(HedgedGridLiveError::Dispatch);
            }
            GridAction::Place(_) | GridAction::Replenish(_) => {
                return Err(HedgedGridLiveError::Strategy(HedgedGridError::Phase));
            }
        }
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
fn handle_dispatch_transactions(
    state: &mut HedgedGridState,
    store: &ProjectionStore,
    commands: &mut CommandJournal,
    transport: &BinancePrivateFactsTransport,
    instrument: &crate::domain::Instrument,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    transactions: Vec<crate::strategy::hedged_grid::GridTransaction>,
    now_ms: u64,
) -> Result<bool, HedgedGridLiveError> {
    let endpoint = BinanceLegacyGridMutationEndpoint {
        private: transport.private_rest(),
    };
    handle_dispatch_transactions_with_endpoint(
        state,
        store,
        commands,
        authority,
        writer,
        binding,
        instrument,
        &endpoint,
        transactions,
        now_ms,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_dispatch_transactions_with_endpoint<E: LegacyGridMutationEndpoint>(
    state: &mut HedgedGridState,
    store: &ProjectionStore,
    commands: &mut CommandJournal,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    instrument: &crate::domain::Instrument,
    endpoint: &E,
    transactions: Vec<crate::strategy::hedged_grid::GridTransaction>,
    now_ms: u64,
) -> Result<bool, HedgedGridLiveError> {
    let result = dispatch_reserved_transactions_with_endpoint(
        commands,
        authority,
        writer,
        binding,
        instrument,
        endpoint,
        &transactions,
    );
    for transaction in &transactions {
        log_grid_transaction_result(commands, binding, instrument, transaction, &result);
        state.settle_transaction(&transaction.id, result.is_ok())?;
    }
    if matches!(
        &result,
        Err(HedgedGridLiveError::Unresolved | HedgedGridLiveError::Rejected)
    ) {
        let not_before_ms = now_ms.saturating_add(GRID_REJECTED_RESET_DELAY_MS);
        state.defer_blocked_reconciliation_until(not_before_ms)?;
        warn!(
            event = "grid_rejected_reset_deferred",
            not_before_ms,
            delay_ms = GRID_REJECTED_RESET_DELAY_MS,
            "补撤请求未全部成功，30秒后仍未收敛才重置网格"
        );
    }
    save_state(store, state)?;
    match result {
        Ok(()) | Err(HedgedGridLiveError::Unresolved | HedgedGridLiveError::Rejected) => Ok(true),
        Err(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_reserved_transactions_with_endpoint<E: LegacyGridMutationEndpoint>(
    commands: &mut CommandJournal,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    instrument: &crate::domain::Instrument,
    endpoint: &E,
    transactions: &[crate::strategy::hedged_grid::GridTransaction],
) -> Result<(), HedgedGridLiveError> {
    for transaction in transactions {
        info!(
            event = "grid_fill_transaction",
            transaction_id = %transaction.id,
            fill_id = %transaction.source_fill_id,
            source = ?transaction.source_order,
            place_1 = ?transaction.places[0].key,
            place_1_quantity = %transaction.places[0].quantity,
            place_2 = ?transaction.places[1].key,
            place_2_quantity = %transaction.places[1].quantity,
            cancel = ?transaction.cancel,
            "成交生成补2撤1事务"
        );
    }
    let mutations = unsettled_transaction_mutations(commands, binding, instrument, transactions)?;
    dispatch_batch_with_endpoint(commands, authority, writer, endpoint, mutations)
}

pub(super) fn log_grid_transaction_result(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    instrument: &crate::domain::Instrument,
    transaction: &crate::strategy::hedged_grid::GridTransaction,
    result: &Result<(), HedgedGridLiveError>,
) {
    let place_outcomes = transaction.places.each_ref().map(|order| {
        place_command(binding, instrument, order)
            .ok()
            .and_then(|mutation| commands.receipt(mutation.command_id()))
            .map_or("missing", |receipt| command_state_name(&receipt.state))
    });
    let cancel_outcome = latest_cancel_state(commands, binding, &transaction.cancel);
    let batch_outcome = if result.is_ok() { "accepted" } else { "failed" };
    info!(
        event = "grid_fill_transaction_result",
        transaction_id = %transaction.id,
        fill_id = %transaction.source_fill_id,
        place_1 = place_outcomes[0],
        place_2 = place_outcomes[1],
        cancel = cancel_outcome,
        batch = batch_outcome,
        "补2撤1事务完成"
    );
}

fn latest_cancel_state(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    key: &GridOrderKey,
) -> &'static str {
    let Ok(GridMutation::Cancel(base)) = cancel_command(binding, key) else {
        return "invalid";
    };
    let mut latest = None;
    for attempt in 1_u64..=64 {
        let value = if attempt == 1 {
            base.command_id.as_str().to_owned()
        } else {
            format!("{}_a{attempt}", base.command_id.as_str())
        };
        let Ok(command_id) = CommandId::new(value) else {
            return "invalid";
        };
        let Some(receipt) = commands.receipt(&command_id) else {
            break;
        };
        latest = Some(command_state_name(&receipt.state));
    }
    latest.unwrap_or("missing")
}

const fn command_state_name(state: &CommandState) -> &'static str {
    match state {
        CommandState::Prepared => "prepared",
        CommandState::Submitted => "submitted",
        CommandState::Accepted { .. } => "accepted",
        CommandState::Rejected { .. } => "rejected",
        CommandState::Unknown { .. } => "unknown",
    }
}

pub(super) fn unsettled_transaction_mutations(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    instrument: &crate::domain::Instrument,
    transactions: &[crate::strategy::hedged_grid::GridTransaction],
) -> Result<Vec<GridMutation>, HedgedGridLiveError> {
    let mut mutations = Vec::new();
    for transaction in transactions {
        for place in transaction
            .places
            .iter()
            .map(|order| place_command(binding, instrument, order))
        {
            let place = place?;
            match commands
                .receipt(place.command_id())
                .map(|receipt| &receipt.state)
            {
                None => mutations.push(place),
                Some(CommandState::Accepted { .. }) => {}
                Some(CommandState::Rejected { .. }) => {
                    return Err(HedgedGridLiveError::Rejected);
                }
                Some(
                    CommandState::Prepared | CommandState::Submitted | CommandState::Unknown { .. },
                ) => return Err(HedgedGridLiveError::Unresolved),
            }
        }
        if !accepted_cancel_exists(commands, binding, &transaction.cancel)? {
            mutations.push(next_cancel_command(commands, binding, &transaction.cancel)?);
        }
    }
    Ok(mutations)
}

pub(super) fn accepted_cancel_exists(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    key: &GridOrderKey,
) -> Result<bool, HedgedGridLiveError> {
    let GridMutation::Cancel(base) = cancel_command(binding, key)? else {
        return Err(HedgedGridLiveError::Dispatch);
    };
    let base_id = base.command_id.as_str();
    let mut attempt = 1_u64;
    loop {
        let value = if attempt == 1 {
            base_id.to_owned()
        } else {
            format!("{base_id}_a{attempt}")
        };
        let command_id = CommandId::new(value).map_err(|_| HedgedGridLiveError::Identifier)?;
        let Some(receipt) = commands.receipt(&command_id) else {
            return Ok(false);
        };
        match receipt.state {
            CommandState::Accepted { .. } => return Ok(true),
            CommandState::Rejected { .. } => {}
            CommandState::Prepared | CommandState::Submitted | CommandState::Unknown { .. } => {
                return Err(HedgedGridLiveError::Unresolved);
            }
        }
        attempt = attempt
            .checked_add(1)
            .ok_or(HedgedGridLiveError::Identifier)?;
    }
}

#[allow(clippy::too_many_arguments)]
fn install_epoch(
    state: &mut HedgedGridState,
    store: &ProjectionStore,
    commands: &mut CommandJournal,
    transport: &BinancePrivateFactsTransport,
    public: &PublicRest,
    instrument: &crate::domain::Instrument,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    inventory: &GridInventory,
    now_ms: u64,
) -> Result<bool, HedgedGridLiveError> {
    let (bid, ask) = parse_depth_best_prices(&public.depth_snapshot(&binding.symbol, 5)?)?;
    let epoch = match &state.inventory_recovery {
        crate::strategy::hedged_grid::InventoryRecoveryState::Rebuilding { fill_price, .. } => {
            super::hedged_grid::epoch_at_anchor(state, instrument, *fill_price)?
        }
        crate::strategy::hedged_grid::InventoryRecoveryState::Inactive
        | crate::strategy::hedged_grid::InventoryRecoveryState::Deficient { .. }
        | crate::strategy::hedged_grid::InventoryRecoveryState::AwaitingNextOwnedFill { .. }
        | crate::strategy::hedged_grid::InventoryRecoveryState::ReanchorPending { .. } => {
            epoch(state, instrument, bid, ask)?
        }
    };
    let install_decision = match state.install_epoch(epoch) {
        Ok(decision) => decision,
        Err(error) => {
            warn!(
                event = "grid_epoch_install_phase_mismatch",
                phase = ?state.phase,
                pending_transactions = state.pending_transactions.len(),
                reason = %error,
                "网格安装状态不匹配"
            );
            return Err(error.into());
        }
    };
    let GridDecision::Actions(actions) = install_decision else {
        return Err(HedgedGridLiveError::Strategy(HedgedGridError::Phase));
    };
    let orders = actions
        .into_iter()
        .filter_map(|action| match action {
            GridAction::Place(order) => Some(order),
            _ => None,
        })
        .collect::<Vec<_>>();
    let opening_count = |position| {
        orders
            .iter()
            .filter(|order| order.key.position == position && order.key.role == GridOrderRole::Open)
            .count()
    };
    let closing_quantity = |position| {
        orders
            .iter()
            .filter(|order| {
                order.key.position == position && order.key.role == GridOrderRole::Close
            })
            .map(|order| order.quantity)
            .sum::<Decimal>()
    };
    if opening_count(GridPosition::Long) != usize::from(state.params.grid_count)
        || opening_count(GridPosition::Short) != usize::from(state.params.grid_count)
        || closing_quantity(GridPosition::Long) > inventory.long_quantity
        || closing_quantity(GridPosition::Short) > inventory.short_quantity
        || (!state.suppress_replenishment_until_inventory_recovers
            && (inventory.notional(GridPosition::Long) < state.params.order_notional.value
                || inventory.notional(GridPosition::Short) < state.params.order_notional.value))
    {
        return Err(HedgedGridLiveError::Inventory);
    }
    save_state(store, state)?;
    // Initial/reset installation is a slow-path scaffold, not a fill transaction. Install the
    // inventory-backed reduce-only exits install before the configured openings. Each wave remains
    // concurrent; the split avoids a single oversized installation burst. The hot fill path is
    // separate: every 2-place + 1-cancel transaction is queued immediately and adjacent
    // transactions may overlap on the network.
    let mut closing = Vec::new();
    let mut opening = Vec::new();
    for order in &orders {
        let mutation = place_command(binding, instrument, order)?;
        if order.reduce_only {
            closing.push(mutation);
        } else {
            opening.push(mutation);
        }
    }
    let install_result =
        dispatch_epoch_waves(commands, transport, authority, writer, closing, opening);
    match install_result {
        Ok(()) => Ok(true),
        Err(HedgedGridLiveError::Rejected | HedgedGridLiveError::Unresolved) => {
            state.block_for_order_reconciliation()?;
            state.defer_blocked_reconciliation_until(
                now_ms.saturating_add(GRID_REJECTED_RESET_DELAY_MS),
            )?;
            save_state(store, state)?;
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

fn dispatch_reset_orders(
    commands: &mut CommandJournal,
    transport: &BinancePrivateFactsTransport,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    state: &HedgedGridState,
    snapshot: Option<&PrivateFactsSnapshot>,
    _now_ms: u64,
) -> Result<(), HedgedGridLiveError> {
    let mut mutations = Vec::new();
    if let Some(snapshot) = snapshot {
        for order in &snapshot.orders {
            if !matches!(order.state, OrderState::New | OrderState::PartiallyFilled) {
                continue;
            }
            let FieldState::Known(client_id) = &order.client_order_id else {
                return Err(HedgedGridLiveError::Unresolved);
            };
            let key = parse_grid_client_order_id(client_id)
                .map_err(|_| HedgedGridLiveError::ForeignOrders)?;
            if !state.owned_orders.contains_key(&key) {
                return Err(HedgedGridLiveError::ForeignOrders);
            }
            mutations.push(next_cancel_command(commands, binding, &key)?);
        }
    } else {
        let keys = state.owned_orders.keys().cloned().collect::<Vec<_>>();
        let private = transport.private_rest();
        let readbacks = thread::scope(|scope| {
            let handles = keys
                .iter()
                .cloned()
                .map(|key| {
                    scope.spawn(move || {
                        let client_order_id = client_order_id(&key)?;
                        let payload = private
                            .order_by_client_id(&binding.symbol, client_order_id.as_str())?;
                        let order = binance_private::parse_order(&payload, &binding.symbol)?;
                        Ok::<_, HedgedGridLiveError>((key, order))
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().map_err(|_| HedgedGridLiveError::Dispatch)?)
                .collect::<Result<Vec<_>, _>>()
        })?;
        for (key, order) in readbacks {
            match order.state {
                OrderState::New | OrderState::PartiallyFilled => {
                    mutations.push(next_cancel_command(commands, binding, &key)?);
                }
                OrderState::Filled
                | OrderState::Cancelled
                | OrderState::Expired
                | OrderState::Rejected => {}
                OrderState::Unknown => return Err(HedgedGridLiveError::Unresolved),
            }
        }
    }
    dispatch_batch(commands, transport, authority, writer, mutations)
}

fn dispatch_batch(
    commands: &mut CommandJournal,
    transport: &BinancePrivateFactsTransport,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    mutations: Vec<GridMutation>,
) -> Result<(), HedgedGridLiveError> {
    let endpoint = BinanceLegacyGridMutationEndpoint {
        private: transport.private_rest(),
    };
    dispatch_batch_with_endpoint(commands, authority, writer, &endpoint, mutations)
}

fn dispatch_batch_with_endpoint<E: LegacyGridMutationEndpoint>(
    commands: &mut CommandJournal,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    endpoint: &E,
    mutations: Vec<GridMutation>,
) -> Result<(), HedgedGridLiveError> {
    if mutations.is_empty() {
        return Ok(());
    }
    if commands.has_unresolved() {
        return Err(HedgedGridLiveError::Unresolved);
    }
    let _guard = authority.persistent_dispatch_guard(writer)?;
    dispatch_mutations_with_endpoint(commands, endpoint, mutations)
}

#[allow(clippy::too_many_arguments)]
fn dispatch_epoch_waves(
    commands: &mut CommandJournal,
    transport: &BinancePrivateFactsTransport,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    closing: Vec<GridMutation>,
    opening: Vec<GridMutation>,
) -> Result<(), HedgedGridLiveError> {
    if commands.has_unresolved() {
        return Err(HedgedGridLiveError::Unresolved);
    }
    let _guard = authority.persistent_dispatch_guard(writer)?;
    let private = transport.private_rest();
    dispatch_mutations_under_guard(commands, private, closing)?;
    dispatch_mutations_under_guard(commands, private, opening)
}

fn dispatch_mutations_under_guard(
    commands: &mut CommandJournal,
    private: &crate::exchange::binance::PrivateRest,
    mutations: Vec<GridMutation>,
) -> Result<(), HedgedGridLiveError> {
    let endpoint = BinanceLegacyGridMutationEndpoint { private };
    dispatch_mutations_with_endpoint(commands, &endpoint, mutations)
}

fn begin_rejected_epoch_recovery(
    state: &mut HedgedGridState,
    commands: &CommandJournal,
    now_ms: u64,
) -> Result<bool, HedgedGridLiveError> {
    if state.phase != GridPhase::Running || !state.pending_transactions.is_empty() {
        return Ok(false);
    }
    let mut rejected = BTreeSet::new();
    for key in state.owned_orders.keys() {
        let client_id = client_order_id(key)?;
        let Some(command_id) = commands.command_id_by_client_id(&client_id) else {
            continue;
        };
        if commands
            .receipt(command_id)
            .is_some_and(|receipt| matches!(receipt.state, CommandState::Rejected { .. }))
        {
            rejected.insert(key.clone());
        }
    }
    if rejected.is_empty() {
        return Ok(false);
    }
    state.block_for_order_reconciliation()?;
    state
        .defer_blocked_reconciliation_until(now_ms.saturating_add(GRID_REJECTED_RESET_DELAY_MS))?;
    Ok(true)
}

pub(super) fn settle_mutation(
    commands: &mut CommandJournal,
    mutation: GridMutation,
    outcome: Result<String, PrivateError>,
) -> Result<(), HedgedGridLiveError> {
    match outcome {
        Ok(payload) => {
            let symbol = match &mutation {
                GridMutation::Place(command) => &command.owner.symbol,
                GridMutation::Market(command) => &command.owner.symbol,
                GridMutation::Reduce(command) => &command.owner.symbol,
                GridMutation::Cancel(command) => &command.owner.symbol,
            };
            let order = binance_private::parse_order(&payload, symbol);
            let accepted = match (&mutation, order) {
                (GridMutation::Place(command), Ok(order))
                    if valid_place_response(&order, &command.client_order_id) =>
                {
                    Some((order.order_id, order.quantity))
                }
                (GridMutation::Market(command), Ok(order))
                    if valid_place_response(&order, &command.client_order_id) =>
                {
                    Some((order.order_id, order.quantity))
                }
                (GridMutation::Reduce(command), Ok(order))
                    if valid_place_response(&order, &command.client_order_id) =>
                {
                    Some((order.order_id, order.quantity))
                }
                (GridMutation::Cancel(_), Ok(order))
                    if matches!(
                        order.state,
                        OrderState::Cancelled
                            | OrderState::Filled
                            | OrderState::Expired
                            | OrderState::Rejected
                    ) =>
                {
                    Some((order.order_id, order.quantity))
                }
                _ => None,
            };
            match accepted {
                Some((venue_order_id, observed_quantity)) => {
                    commands.transition(
                        mutation.command_id(),
                        CommandState::Accepted {
                            venue_order_id: venue_order_id.clone(),
                        },
                    )?;
                    info!(
                        event = "grid_mutation_result",
                        command_id = mutation.command_id().as_str(),
                        mutation = mutation_kind(&mutation),
                        outcome = "accepted",
                        venue_order_id = %venue_order_id,
                        observed_quantity = %observed_quantity,
                        "网格请求完成"
                    );
                    Ok(())
                }
                None => {
                    commands.transition(
                        mutation.command_id(),
                        CommandState::Unknown {
                            reason: "grid_response_requires_signed_readback".to_owned(),
                        },
                    )?;
                    warn!(
                        event = "grid_mutation_result",
                        command_id = mutation.command_id().as_str(),
                        mutation = mutation_kind(&mutation),
                        outcome = "unknown",
                        reason = "grid_response_requires_signed_readback",
                        "网格请求响应需要签名回查"
                    );
                    Err(HedgedGridLiveError::Unresolved)
                }
            }
        }
        Err(error @ (PrivateError::Rejected { .. } | PrivateError::RateLimited(_))) => {
            let reason = error.to_string();
            commands.transition(
                mutation.command_id(),
                CommandState::Rejected {
                    reason: reason.clone(),
                },
            )?;
            warn!(
                event = "grid_mutation_result",
                command_id = mutation.command_id().as_str(),
                mutation = mutation_kind(&mutation),
                outcome = "rejected",
                reason = %reason,
                "网格请求被交易所拒绝"
            );
            Err(HedgedGridLiveError::Rejected)
        }
        Err(error) => {
            let reason = error.to_string();
            commands.transition(
                mutation.command_id(),
                CommandState::Unknown {
                    reason: reason.clone(),
                },
            )?;
            warn!(
                event = "grid_mutation_result",
                command_id = mutation.command_id().as_str(),
                mutation = mutation_kind(&mutation),
                outcome = "unknown",
                reason = %reason,
                "网格请求结果不确定"
            );
            Err(HedgedGridLiveError::Unresolved)
        }
    }
}

fn valid_place_response(order: &crate::domain::Order, client_id: &CommandId) -> bool {
    matches!(
        order.state,
        OrderState::New | OrderState::PartiallyFilled | OrderState::Filled
    ) && matches!(&order.client_order_id, FieldState::Known(value) if value == client_id.as_str())
}

fn mutation_kind(mutation: &GridMutation) -> &'static str {
    match mutation {
        GridMutation::Place(_) => "place_limit",
        GridMutation::Market(_) => "place_market",
        GridMutation::Reduce(_) => "market_reduce",
        GridMutation::Cancel(_) => "cancel",
    }
}

#[path = "hedged_grid_fill_readback.rs"]
mod hedged_grid_fill_readback;
#[cfg(test)]
use hedged_grid_fill_readback::order_confirmed_fills;
use hedged_grid_fill_readback::{MissingOwnedOrders, confirmed_missing_owned_fills};

pub(super) fn market_command(
    binding: &HedgedGridBinding,
    replenishment: &crate::strategy::hedged_grid::GridReplenishment,
    instrument: &crate::domain::Instrument,
    inventory: &GridInventory,
) -> Result<GridMutation, HedgedGridLiveError> {
    let one_grid_quantity = align_up(
        replenishment.target_notional.value / Decimal::from(3_u8) / inventory.mark_price.value(),
        instrument.quantity_step,
    )?;
    let quantity = one_grid_quantity * Decimal::from(3_u8);
    let side = match replenishment.position {
        GridPosition::Long => PositionSide::Long,
        GridPosition::Short => PositionSide::Short,
    };
    let id = replenishment_client_order_id(replenishment)?;
    let command_id =
        CommandId::new(format!("{id}_cmd")).map_err(|_| HedgedGridLiveError::Identifier)?;
    let client_order_id = CommandId::new(id).map_err(|_| HedgedGridLiveError::Identifier)?;
    Ok(GridMutation::Market(MarketOrderCommand {
        command_id,
        client_order_id,
        owner: owner(binding, OrderPurpose::Entry),
        position_side: side,
        side: match replenishment.position {
            GridPosition::Long => OrderSide::Buy,
            GridPosition::Short => OrderSide::Sell,
        },
        quantity,
        reduce_only: false,
    }))
}

fn replenishment_client_order_id(
    replenishment: &crate::strategy::hedged_grid::GridReplenishment,
) -> Result<String, HedgedGridLiveError> {
    if replenishment.round == 0 || replenishment.private_generation == 0 {
        return Err(HedgedGridLiveError::Identifier);
    }
    Ok(match replenishment.position {
        GridPosition::Long => format!("hgm_r{}_long", replenishment.round),
        GridPosition::Short => format!("hgm_r{}_short", replenishment.round),
    })
}

pub(super) fn highest_durable_replenishment_round(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
) -> Result<u64, HedgedGridLiveError> {
    let mut highest = 0_u64;
    for command in commands.commands().filter(|command| {
        command.owner().is_some_and(|owner| {
            owner.strategy_instance_id == binding.strategy_instance_id
                && owner.run_id == binding.run_id
                && owner.exchange == binding.exchange
                && owner.account == binding.account
                && owner.symbol == binding.symbol
        })
    }) {
        let Some(client_id) = command.native_client_id() else {
            continue;
        };
        if let Some(round) = parse_replenishment_round(client_id.as_str())? {
            highest = highest.max(round);
        }
    }
    Ok(highest)
}

fn parse_replenishment_round(id: &str) -> Result<Option<u64>, HedgedGridLiveError> {
    let Some(encoded) = id.strip_prefix("hgm_r") else {
        return Ok(None);
    };
    let round = encoded
        .strip_suffix("_long")
        .or_else(|| encoded.strip_suffix("_short"))
        .ok_or(HedgedGridLiveError::Identifier)?
        .parse::<u64>()
        .map_err(|_| HedgedGridLiveError::Identifier)?;
    if round == 0 {
        return Err(HedgedGridLiveError::Identifier);
    }
    Ok(Some(round))
}

pub(super) fn place_command(
    binding: &HedgedGridBinding,
    instrument: &crate::domain::Instrument,
    order: &GridOrderIntent,
) -> Result<GridMutation, HedgedGridLiveError> {
    let client_order_id = client_order_id(&order.key)?;
    let command_id = CommandId::new(format!("{}_cmd", client_order_id.as_str()))
        .map_err(|_| HedgedGridLiveError::Identifier)?;
    let purpose = match order.key.role {
        GridOrderRole::Open => OrderPurpose::Entry,
        GridOrderRole::Close => OrderPurpose::Reduce,
    };
    Ok(GridMutation::Place(OrderCommand {
        command_id,
        client_order_id,
        owner: owner(binding, purpose),
        side: order.side,
        position_side: match order.key.position {
            GridPosition::Long => PositionSide::Long,
            GridPosition::Short => PositionSide::Short,
        },
        quantity: grid_order_quantity(binding, instrument, order)?,
        limit_price: order.price,
        reduce_only: order.reduce_only,
    }))
}

pub(super) fn cancel_command(
    binding: &HedgedGridBinding,
    key: &GridOrderKey,
) -> Result<GridMutation, HedgedGridLiveError> {
    let target_client_order_id = client_order_id(key)?;
    let command_id = CommandId::new(
        format!(
            "hgc_e{}_{}_{:?}_{}",
            key.epoch,
            position_name(key.position),
            key.role,
            key.level
        )
        .to_lowercase(),
    )
    .map_err(|_| HedgedGridLiveError::Identifier)?;
    let purpose = match key.role {
        GridOrderRole::Open => OrderPurpose::Entry,
        GridOrderRole::Close => OrderPurpose::Reduce,
    };
    Ok(GridMutation::Cancel(CancelCommand {
        command_id,
        owner: owner(binding, purpose),
        target_client_order_id,
    }))
}

pub(super) fn next_cancel_command(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    key: &GridOrderKey,
) -> Result<GridMutation, HedgedGridLiveError> {
    let GridMutation::Cancel(mut cancel) = cancel_command(binding, key)? else {
        return Err(HedgedGridLiveError::Dispatch);
    };
    let base = cancel.command_id.as_str().to_owned();
    let mut attempt = 1_u64;
    loop {
        let candidate = if attempt == 1 {
            base.clone()
        } else {
            format!("{base}_a{attempt}")
        };
        let command_id = CommandId::new(candidate).map_err(|_| HedgedGridLiveError::Identifier)?;
        if commands.receipt(&command_id).is_none() {
            cancel.command_id = command_id;
            return Ok(GridMutation::Cancel(cancel));
        }
        attempt = attempt
            .checked_add(1)
            .ok_or(HedgedGridLiveError::Identifier)?;
    }
}

#[path = "hedged_grid_identity.rs"]
mod hedged_grid_identity;
pub(super) use hedged_grid_identity::client_order_id;
use hedged_grid_identity::{owner, position_name};

pub(super) fn epoch(
    state: &HedgedGridState,
    instrument: &crate::domain::Instrument,
    bid: Price,
    ask: Price,
) -> Result<GridEpoch, HedgedGridLiveError> {
    super::hedged_grid::epoch_at_midpoint(state, instrument, bid, ask).map_err(Into::into)
}

#[path = "hedged_grid_inventory.rs"]
mod hedged_grid_inventory;
pub(super) use hedged_grid_inventory::{inventory, strategy_private_generation};

#[path = "hedged_grid_recovery.rs"]
mod hedged_grid_recovery;
pub(super) use hedged_grid_recovery::*;

#[path = "hedged_grid_support.rs"]
mod hedged_grid_support;
pub use hedged_grid_support::HedgedGridLiveError;
#[cfg(test)]
pub(super) use hedged_grid_support::phase_one_binding;
use hedged_grid_support::validate_control;
pub(super) use hedged_grid_support::{
    align_up, apply_release_params, grid_order_quantity, legacy_exposure_is_settled, load_state,
    phase_one_binding_for_account, read_control, resume_stopping_state_if_requested, save_state,
    wall_clock_ms,
};

#[cfg(test)]
#[path = "hedged_grid_live_tests.rs"]
mod tests;
