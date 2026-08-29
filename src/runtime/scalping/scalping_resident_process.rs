use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use crate::{
    config::{BinanceAccountBinding, Config},
    controller::{
        ControlAuthority, ControlTarget, ScalpingControllerSource, ScalpingControllerSourceError,
    },
    exchange::binance::{PrivateCredentials, PublicError, PublicRest},
    execution::WriterScope,
    indicator::FeatureState,
    market::SessionState,
    strategy::scalping::{
        EpisodeAction, EpisodeState, ScalpingDecision, ScalpingParams, SemanticIntent,
        StrategyBinding,
    },
};

use super::{
    BinancePrivateFactsTransport, BinancePrivateFactsWorker, BinancePrivateFactsWorkerConfig,
    BinancePrivateProjectionAuthorityConfig, BinancePublicCaptureTransport, ControlDisposition,
    DeadlineClockObservation, EntryDisposition, LifecycleReport, PrivateEntryGate,
    PrivateEntryGateInput, PrivateEntryGateReport, PrivateExposure, PrivateFacts,
    PrivateFactsProjectionInput, PrivateFactsReadiness, PrivateFactsTurn, PrivateFactsWorkerError,
    PrivateFactsWorkerState, ScalpingLiveDriver, ScalpingLiveGateway, ScalpingLiveGatewayConfig,
    ScalpingProtectedGateway, ScalpingResidentRuntime, ScalpingResidentRuntimeError,
    ScalpingResidentSources, ScalpingResidentSourcesConfig, ScalpingResidentSourcesError,
    ScalpingShadowHost, ShadowDisposition, drive_binance_private_facts_turn,
    recover_absent_unknown_scalping_entry, recover_unknown_scalping_cancels,
};

// Private facts retain strict turn priority, then the resident gives the four serial public
// sockets a bounded catch-up slice. Thirty-two one-effect calls absorb active trade bursts while
// preserving a private turn between bounded slices and avoiding a background or unbounded drain.
const PUBLIC_EFFECTS_PER_PRIVATE_TURN: usize = 32;

const EXCHANGE: &str = "binance";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingShadowResidentRequest {
    pub artifacts_root: PathBuf,
    pub binding_path: PathBuf,
    pub initial_fill_recovery_from_ms: u64,
    pub max_turns: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingShadowResidentReport {
    pub turns: u64,
    pub worker_state: PrivateFactsWorkerState,
    pub disposition: ShadowDisposition,
    pub private_safe: bool,
    pub public_generation: u64,
    pub public_session_state: SessionState,
    pub public_feature_state: FeatureState,
    pub deadline_pending: bool,
    pub public_in_flight: bool,
    pub pending_mark: bool,
    pub pending_preparation: bool,
    pub checkpoint_path: PathBuf,
}

/// The only resident request that can execute strategy mutations. Its confirmation is intentionally
/// part of the runtime request as well as the CLI grammar, so library callers cannot silently
/// acquire the Live path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingLiveResidentRequest {
    pub artifacts_root: PathBuf,
    pub binding_path: PathBuf,
    pub initial_fill_recovery_from_ms: u64,
    pub max_turns: Option<u64>,
    pub confirm_mainnet_strategy_mutations: bool,
}

pub type ScalpingLiveResidentReport = ScalpingShadowResidentReport;
pub type ScalpingLiveResidentError = ScalpingShadowResidentError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResidentMode {
    Shadow,
    Live,
}

impl ResidentMode {
    fn live(self) -> bool {
        self == Self::Live
    }
}

/// Runs signed private readback, recovered public capture, deadline semantics, and the strategy
/// coordinator.  A fresh safe private projection and controller authorization admit direct
/// semantic intents; Core quote/risk valuation, calibration, and evidence bundles are not entry
/// prerequisites. This resident remains mutation-free.
pub fn run_scalping_shadow_resident(
    config: &Config,
    request: ScalpingShadowResidentRequest,
) -> Result<ScalpingShadowResidentReport, ScalpingShadowResidentError> {
    run_scalping_resident(config, request, ResidentMode::Shadow)
}

/// Runs the explicit confirmed mainnet strategy path. Every mutation is emitted only by the
/// resident-owned Live driver and is fenced until a newer private worker generation is durable.
pub fn run_scalping_live_resident(
    config: &Config,
    request: ScalpingLiveResidentRequest,
) -> Result<ScalpingLiveResidentReport, ScalpingLiveResidentError> {
    if !request.confirm_mainnet_strategy_mutations {
        return Err(ScalpingShadowResidentError::LiveConfirmation);
    }
    run_scalping_resident(
        config,
        ScalpingShadowResidentRequest {
            artifacts_root: request.artifacts_root,
            binding_path: request.binding_path,
            initial_fill_recovery_from_ms: request.initial_fill_recovery_from_ms,
            max_turns: request.max_turns,
        },
        ResidentMode::Live,
    )
}

fn run_scalping_resident(
    config: &Config,
    request: ScalpingShadowResidentRequest,
    mode: ResidentMode,
) -> Result<ScalpingShadowResidentReport, ScalpingShadowResidentError> {
    validate_request(&request)?;
    fs::create_dir_all(&request.artifacts_root).map_err(|source| {
        ScalpingShadowResidentError::Io {
            path: request.artifacts_root.clone(),
            source,
        }
    })?;
    let binding = load_binding(&request.binding_path)?;
    validate_binding(config, &binding)?;
    let _account_writer_root = mode
        .live()
        .then(|| {
            super::stage7_writer_registry::acquire(
                &WriterScope {
                    exchange: binding.exchange.clone(),
                    account: binding.account.clone(),
                    symbol: binding.symbol.clone(),
                    owner_scope: binding.owner_scope.clone(),
                },
                &request.artifacts_root,
            )
            .map_err(|error| ScalpingShadowResidentError::WriterRegistry {
                reason: error.to_string(),
            })
        })
        .transpose()?;
    let binance = config
        .binance
        .as_ref()
        .ok_or(ScalpingShadowResidentError::Binding)?;
    if mode.live() {
        let _ = recover_absent_unknown_scalping_entry(
            &request.artifacts_root,
            &binding,
            binance.account_binding,
        )?;
    }

    let checkpoint_path = request.artifacts_root.join(if mode.live() {
        "scalping_live_host.json"
    } else {
        "scalping_shadow_host.json"
    });
    let worker_config = BinancePrivateFactsWorkerConfig {
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        artifacts_root: request.artifacts_root.join("private"),
        initial_fill_recovery_from_ms: request.initial_fill_recovery_from_ms,
    };
    let authority = BinancePrivateProjectionAuthorityConfig {
        binding: binding.clone(),
        command_journal_path: request.artifacts_root.join("commands.jsonl"),
        writer_authority_path: request.artifacts_root.join("writer.json"),
        custody_max_stale_ms: binance.private_custody_max_stale_ms,
    };
    let mut worker =
        BinancePrivateFactsWorker::open_with_projection_authority(worker_config, authority)?;
    let credentials =
        PrivateCredentials::from_environment().map_err(PrivateFactsWorkerError::from)?;
    let mut transport =
        BinancePrivateFactsTransport::production(credentials, binance.account_binding)?;
    let params = ScalpingParams::for_binding(&binding);
    let host =
        ScalpingShadowHost::open_or_restore(&checkpoint_path, binding.clone(), params.clone())?;
    let resident = ScalpingResidentRuntime::new(host);
    let mut sources = ScalpingResidentSources::open_recovered(
        resident,
        ScalpingResidentSourcesConfig {
            artifacts_root: request.artifacts_root.clone(),
            binding: binding.clone(),
            params: params.clone(),
            mark_stale_after_ms: params.max_data_age_ms,
        },
    )?;
    let mut public_transport =
        BinancePublicCaptureTransport::new(binding.symbol.clone(), PublicRest::production()?);
    let mut controller_source = ScalpingControllerSource::open(
        request.artifacts_root.join("controller.json"),
        request.artifacts_root.join("controller_source.json"),
        binding.clone(),
    )?;
    let mut gate = PrivateEntryGate::with_periodic_retention(binance.private_custody_max_stale_ms);
    let mut live_driver = mode
        .live()
        .then(|| ScalpingLiveDriver::open(request.artifacts_root.clone(), binding.clone()))
        .transpose()?;
    let shutdown = shutdown_flag(request.max_turns)?;
    let mut turns = 0_u64;
    let mut last_deadline_now_ms = None;
    let mut controller_private: Option<PrivateFacts> = None;
    let mut last_now_ms = None;

    let loop_result: Result<(), ScalpingShadowResidentError> = (|| {
        sources.drain_pending_deadline()?;
        let recovery_now_ms = transport.authoritative_now_ms()?;
        let _ = recovery_now_ms;
        while !shutdown.load(Ordering::Acquire)
            && request.max_turns.is_none_or(|limit| turns < limit)
        {
            sources.drain_pending_deadline()?;
            let now_ms = transport.authoritative_now_ms()?;
            last_now_ms = Some(now_ms);
            let turn = drive_binance_private_facts_turn(&mut worker, &mut transport, now_ms)?;
            if let Some(driver) = live_driver.as_mut() {
                reconcile_live_private(
                    driver,
                    &mut sources,
                    &mut worker,
                    &request.artifacts_root,
                    &binding,
                    binance.account_binding,
                    now_ms,
                )?;
            }
            let mutation_pending = live_driver
                .as_ref()
                .is_some_and(ScalpingLiveDriver::awaits_private_reconciliation);
            let active_episode = sources.status().active_episode && !mutation_pending;
            let entry_requested = live_driver
                .as_ref()
                .is_none_or(ScalpingLiveDriver::can_accept_entry);
            let private_gate = gate.observe_authoritative_worker(
                &worker,
                PrivateEntryGateInput {
                    active_episode,
                    entry_requested,
                    now_ms,
                },
            );
            if periodic_reconnect_required(
                entry_requested,
                worker.periodic_readback_in_progress(),
                &private_gate,
            ) {
                // A scheduled same-generation refresh may retain authority only inside the
                // configured freshness TTL. Once it expires, reconnect so the controller can
                // recover on a strictly newer private generation instead of remaining fenced.
                worker.force_fence(now_ms);
            }
            update_controller_private(&mut controller_private, &private_gate);
            let authority = controller_private
                .as_ref()
                .map(|private| controller_authority(&binding, private));
            let controller = controller_source.observe(authority.as_ref(), now_ms)?;
            let private_gate = (private_gate.forwarded_private.is_some()
                || private_gate.control.is_some())
            .then_some(private_gate);
            let deadline_clock = worker.authoritative_clock_root()?.and_then(|root| {
                let advances = last_deadline_now_ms.is_none_or(|previous| now_ms > previous);
                if advances {
                    last_deadline_now_ms = Some(now_ms);
                    Some(DeadlineClockObservation {
                        now_ms,
                        root_cause_fact_id: root.root_cause_fact_id,
                    })
                } else {
                    None
                }
            });
            let control_report =
                sources.drive_control_private_phase(Some(controller), private_gate)?;
            if let Some(driver) = live_driver.as_mut() {
                begin_live_exit_if_requested(driver, &sources, &control_report)?;
                reconcile_live_private(
                    driver,
                    &mut sources,
                    &mut worker,
                    &request.artifacts_root,
                    &binding,
                    binance.account_binding,
                    now_ms,
                )?;
            }
            if !live_driver
                .as_ref()
                .is_some_and(ScalpingLiveDriver::awaits_private_reconciliation)
            {
                let deadline_report = sources.drive_episode_deadline(deadline_clock.clone())?;
                if let Some(driver) = live_driver.as_mut() {
                    begin_live_exit_if_requested(driver, &sources, &deadline_report)?;
                    reconcile_live_private(
                        driver,
                        &mut sources,
                        &mut worker,
                        &request.artifacts_root,
                        &binding,
                        binance.account_binding,
                        now_ms,
                    )?;
                }
            }
            for _ in 0..PUBLIC_EFFECTS_PER_PRIVATE_TURN {
                let source_status = sources.status();
                if live_driver
                    .as_ref()
                    .is_some_and(ScalpingLiveDriver::awaits_private_reconciliation)
                    || !should_poll_public(&source_status)
                {
                    break;
                }
                let public_now_ms = public_capture_now(now_ms, transport.authoritative_now_ms()?)?;
                let public_report = sources.drive_public_once(
                    &mut public_transport,
                    public_now_ms,
                    deadline_clock.as_ref(),
                )?;
                if let Some(driver) = live_driver.as_mut() {
                    begin_live_exit_if_requested(driver, &sources, &public_report)?;
                    submit_live_intent_if_requested(
                        driver,
                        &public_report,
                        &mut worker,
                        &request.artifacts_root,
                        &binding,
                        binance.account_binding,
                        now_ms,
                    )?;
                    reconcile_live_private(
                        driver,
                        &mut sources,
                        &mut worker,
                        &request.artifacts_root,
                        &binding,
                        binance.account_binding,
                        now_ms,
                    )?;
                }
            }
            turns = turns
                .checked_add(1)
                .ok_or(ScalpingShadowResidentError::Request)?;
            if turn == PrivateFactsTurn::Idle {
                thread::sleep(Duration::from_millis(1));
            }
        }
        Ok(())
    })();
    if let Err(primary) = loop_result {
        let cleanup = fail_closed_finalize(&mut sources, &mut gate, &mut worker, last_now_ms);
        transport.close();
        return Err(ScalpingShadowResidentError::RunFailed {
            primary: primary.to_string(),
            cleanup,
        });
    }

    let convergence = (|| -> Result<ScalpingShadowResidentReport, ScalpingShadowResidentError> {
        sources.drain_pending_deadline()?;
        let stop = sources.drive_control_private(None, Some(stop_report()), None)?;
        let disposition = stop
            .resident
            .and_then(|report| report.private_gate)
            .map_or(ShadowDisposition::RemainFenced, |report| report.disposition);
        sources.drain_pending_deadline()?;
        let status = sources.status();
        let private_safe = private_safe(
            &worker,
            last_now_ms.unwrap_or_default(),
            binance.private_custody_max_stale_ms,
        )?;
        let live_quiescent = live_driver
            .as_ref()
            .is_none_or(ScalpingLiveDriver::is_quiescent);
        let worker_state = worker.state();
        if !private_safe
            || !live_quiescent
            || disposition != ShadowDisposition::StopAndProtect
            || status.deadline_pending
            || status.public_in_flight
            || status.pending_mark
            || status.pending_preparation
            || status.awaiting_private_recovery
        {
            return Err(ScalpingShadowResidentError::UnsafeShutdown {
                worker_state,
                last_failure_stage: worker.last_failure_stage(),
            });
        }
        Ok(ScalpingShadowResidentReport {
            turns,
            worker_state,
            disposition,
            private_safe,
            public_generation: status.public_generation,
            public_session_state: status.public_session_state,
            public_feature_state: status.public_feature_state,
            deadline_pending: status.deadline_pending,
            public_in_flight: status.public_in_flight,
            pending_mark: status.pending_mark,
            pending_preparation: status.pending_preparation,
            checkpoint_path,
        })
    })();
    match convergence {
        Ok(report) => {
            transport.close();
            Ok(report)
        }
        Err(primary) => {
            let cleanup = fail_closed_finalize(&mut sources, &mut gate, &mut worker, last_now_ms);
            transport.close();
            Err(ScalpingShadowResidentError::RunFailed {
                primary: primary.to_string(),
                cleanup,
            })
        }
    }
}

fn controller_authority(binding: &StrategyBinding, private: &PrivateFacts) -> ControlAuthority {
    ControlAuthority {
        generation: private.generation,
        parameter_release_id: binding.parameter_release_id.clone(),
        private_snapshot_ready: private.safety.private_snapshot_ready,
        execution_unknown: private.safety.execution_unknown,
        protection_complete: private.safety.protection
            == crate::strategy::scalping::ProtectionState::Complete,
        owner_conflict: private.safety.owner_conflict,
    }
}

fn update_controller_private(
    controller_private: &mut Option<PrivateFacts>,
    report: &PrivateEntryGateReport,
) {
    if !report.entry_ready {
        *controller_private = None;
    } else if let Some(private) = &report.forwarded_private {
        *controller_private = Some(private.clone());
    }
}

fn periodic_reconnect_required(
    entry_requested: bool,
    periodic_readback_in_progress: bool,
    report: &PrivateEntryGateReport,
) -> bool {
    entry_requested && periodic_readback_in_progress && !report.entry_ready
}

fn submit_live_intent_if_requested(
    driver: &mut ScalpingLiveDriver,
    report: &super::ScalpingResidentSourcesTurnReport,
    worker: &mut BinancePrivateFactsWorker,
    artifacts_root: &std::path::Path,
    binding: &StrategyBinding,
    account_binding: BinanceAccountBinding,
    now_ms: u64,
) -> Result<(), ScalpingShadowResidentError> {
    if !driver.can_accept_entry() {
        return Ok(());
    }
    let Some(intent) = persisted_entry_intent(report) else {
        return Ok(());
    };
    driver.begin_entry(intent)?;
    let readiness = worker
        .readiness()?
        .ok_or(ScalpingShadowResidentError::LivePrivate)?;
    let mut gateway = ScalpingLiveGateway::open(
        ScalpingLiveGatewayConfig {
            artifacts_root: artifacts_root.to_path_buf(),
            binding: binding.clone(),
            private_generation: readiness.generation,
        },
        account_binding,
        now_ms,
    )?;
    let outcome = gateway.submit_intent(intent, now_ms)?;
    driver.record_entry_outcome(&intent.intent_id, &outcome)?;
    worker.request_post_mutation_reconciliation(now_ms)?;
    Ok(())
}

fn reconcile_live_private(
    driver: &mut ScalpingLiveDriver,
    sources: &mut ScalpingResidentSources,
    worker: &mut BinancePrivateFactsWorker,
    artifacts_root: &std::path::Path,
    binding: &StrategyBinding,
    account_binding: BinanceAccountBinding,
    now_ms: u64,
) -> Result<(), ScalpingShadowResidentError> {
    let Some((readiness, projections)) = live_private(worker)? else {
        return Ok(());
    };
    if readiness.exposure == PrivateExposure::Flat {
        if recover_unknown_scalping_cancels(
            artifacts_root,
            binding,
            account_binding,
            readiness.generation,
            readiness.observed_at_ms,
        )? {
            worker.request_post_mutation_reconciliation(now_ms)?;
            return Ok(());
        }
        if let Some((protection_client_algo_id, target_client_algo_id)) =
            driver.ready_protection_ids()
        {
            if readiness.algo_order_debt {
                let mut gateway = ScalpingProtectedGateway::open(
                    artifacts_root.to_path_buf(),
                    binding.clone(),
                    account_binding,
                    now_ms,
                )?;
                let _ = gateway.cancel_one_known_algo_after_flat(
                    &protection_client_algo_id,
                    target_client_algo_id.as_ref(),
                    now_ms,
                )?;
                worker.request_post_mutation_reconciliation(now_ms)?;
                return Ok(());
            }
            if driver.recover_ready_protected_flat(&readiness, projections)? {
                return Ok(());
            }
        }
    }
    driver.reconcile_entry(sources, &readiness, projections)?;
    if driver.exit_needs_gateway() {
        let mut gateway = ScalpingProtectedGateway::open(
            artifacts_root.to_path_buf(),
            binding.clone(),
            account_binding,
            now_ms,
        )?;
        let report = driver.drive_exit(&mut gateway, &readiness, projections, now_ms)?;
        if report.post_mutation_reconciliation {
            worker.request_post_mutation_reconciliation(now_ms)?;
        }
    }
    if live_episode_is_terminal_flat(sources) {
        let _ = driver.reconcile_terminal_flat(&readiness, projections)?;
    }
    Ok(())
}

fn live_private(
    worker: &BinancePrivateFactsWorker,
) -> Result<Option<(PrivateFactsReadiness, PrivateFactsProjectionInput)>, PrivateFactsWorkerError> {
    if worker.state() != PrivateFactsWorkerState::Ready || worker.periodic_readback_in_progress() {
        return Ok(None);
    }
    Ok(worker.readiness()?.zip(worker.authoritative_projections()?))
}

fn persisted_entry_intent(
    report: &super::ScalpingResidentSourcesTurnReport,
) -> Option<&SemanticIntent> {
    let decision = report
        .resident
        .as_ref()?
        .market
        .as_ref()?
        .decision
        .as_ref()?;
    match decision {
        ScalpingDecision::Intent(intent) => Some(intent),
        ScalpingDecision::Prepared(_) | ScalpingDecision::Noop(_) => None,
    }
}

fn begin_live_exit_if_requested(
    driver: &mut ScalpingLiveDriver,
    sources: &ScalpingResidentSources,
    report: &super::ScalpingResidentSourcesTurnReport,
) -> Result<(), ScalpingShadowResidentError> {
    if driver.has_active_exit()
        || !report
            .resident
            .as_ref()
            .and_then(|resident| resident.episode.as_ref())
            .is_some_and(|episode| {
                episode
                    .episode_actions
                    .iter()
                    .any(|action| matches!(action, EpisodeAction::Exit { .. }))
            })
    {
        return Ok(());
    }
    let hard_stop_distance_bps = sources
        .resident()
        .host()
        .checkpoint()
        .strategy
        .episode
        .as_ref()
        .ok_or(ScalpingShadowResidentError::LiveExit)?
        .frozen_intent
        .hard_stop_distance_bps;
    driver.begin_exit(hard_stop_distance_bps)?;
    Ok(())
}

/// An exchange stop can reach flat before the next public mark is consumed. The semantic host
/// must first persist a terminal episode state; only then may the live driver retire its writer
/// from the same strictly newer private proof.
fn live_episode_is_terminal_flat(sources: &ScalpingResidentSources) -> bool {
    sources
        .resident()
        .host()
        .checkpoint()
        .strategy
        .episode
        .as_ref()
        .is_none_or(|episode| {
            matches!(
                episode.state,
                EpisodeState::Cooldown | EpisodeState::StoppedFlat
            )
        })
}

fn public_capture_now(
    private_turn_started_at_ms: u64,
    sampled_after_private_ms: u64,
) -> Result<u64, ScalpingShadowResidentError> {
    if private_turn_started_at_ms == 0 || sampled_after_private_ms < private_turn_started_at_ms {
        return Err(ScalpingShadowResidentError::ClockRegression);
    }
    Ok(sampled_after_private_ms)
}

fn should_poll_public(status: &super::ScalpingResidentSourcesStatus) -> bool {
    !status.deadline_pending && !status.awaiting_private_recovery
}

fn fail_closed_finalize(
    sources: &mut ScalpingResidentSources,
    gate: &mut PrivateEntryGate,
    worker: &mut BinancePrivateFactsWorker,
    now_ms: Option<u64>,
) -> Vec<String> {
    let mut errors = Vec::new();
    if let Some(now_ms) = now_ms {
        worker.force_fence(now_ms);
    }
    if let Err(error) = sources.drain_pending_deadline() {
        errors.push(format!("deadline preflight: {error}"));
    }
    if let Some(now_ms) = now_ms
        && let Err(error) = persist_worker_fence(sources, gate, worker, now_ms)
    {
        errors.push(format!("private fence: {error}"));
    }
    if let Err(error) = sources.drive_control_private(None, Some(stop_report()), None) {
        errors.push(format!("stop-and-protect: {error}"));
    }
    if let Err(error) = sources.drain_pending_deadline() {
        errors.push(format!("deadline drain: {error}"));
    }
    errors
}

fn validate_request(
    request: &ScalpingShadowResidentRequest,
) -> Result<(), ScalpingShadowResidentError> {
    if !request.artifacts_root.is_absolute()
        || !request.binding_path.is_absolute()
        || request.initial_fill_recovery_from_ms == 0
        || request.max_turns == Some(0)
    {
        return Err(ScalpingShadowResidentError::Request);
    }
    Ok(())
}

fn load_binding(path: &PathBuf) -> Result<StrategyBinding, ScalpingShadowResidentError> {
    let bytes = fs::read(path).map_err(|source| ScalpingShadowResidentError::Io {
        path: path.clone(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(ScalpingShadowResidentError::BindingDecode)
}

fn validate_binding(
    config: &Config,
    binding: &StrategyBinding,
) -> Result<(), ScalpingShadowResidentError> {
    if binding.validate().is_err()
        || binding.exchange != EXCHANGE
        || binding.account != config.trading_account_id
        || binding.symbol != config.symbol
        || binding.risk_budget.asset.as_str() != "USDT"
        || config.binance.as_ref().is_none_or(|binding| {
            binding.account_binding != BinanceAccountBinding::PortfolioMarginUm
        })
    {
        return Err(ScalpingShadowResidentError::Binding);
    }
    Ok(())
}

fn shutdown_flag(max_turns: Option<u64>) -> Result<Arc<AtomicBool>, ScalpingShadowResidentError> {
    let shutdown = Arc::new(AtomicBool::new(false));
    if max_turns.is_none() {
        let signal = Arc::clone(&shutdown);
        ctrlc::set_handler(move || signal.store(true, Ordering::Release))
            .map_err(|error| ScalpingShadowResidentError::Signal(error.to_string()))?;
    }
    Ok(shutdown)
}

fn persist_worker_fence(
    sources: &mut ScalpingResidentSources,
    gate: &mut PrivateEntryGate,
    worker: &BinancePrivateFactsWorker,
    now_ms: u64,
) -> Result<(), ScalpingShadowResidentError> {
    let report = gate.observe_authoritative_worker(
        worker,
        PrivateEntryGateInput {
            active_episode: sources
                .resident()
                .host()
                .checkpoint()
                .strategy
                .episode
                .is_some(),
            entry_requested: false,
            now_ms,
        },
    );
    if report.control.is_some() || report.forwarded_private.is_some() {
        sources.drive_control_private(None, Some(report), None)?;
    }
    Ok(())
}

fn stop_report() -> PrivateEntryGateReport {
    PrivateEntryGateReport {
        lifecycle: LifecycleReport {
            entry: EntryDisposition::Disarmed,
            control: ControlDisposition::StopAndProtect,
        },
        entry_ready: false,
        forwarded_private: None,
        control: Some(ControlTarget::StopAndProtect),
    }
}

fn private_safe(
    worker: &BinancePrivateFactsWorker,
    now_ms: u64,
    custody_max_stale_ms: u64,
) -> Result<bool, PrivateFactsWorkerError> {
    let Some(readiness) = worker.readiness()? else {
        return Ok(false);
    };
    let Some(projections) = worker.authoritative_projections()? else {
        return Ok(false);
    };
    Ok(shutdown_private_safe(
        &readiness,
        &projections,
        now_ms,
        custody_max_stale_ms,
    ))
}

fn shutdown_private_safe(
    readiness: &super::PrivateFactsReadiness,
    projections: &super::PrivateFactsProjectionInput,
    now_ms: u64,
    custody_max_stale_ms: u64,
) -> bool {
    now_ms != 0
        && readiness.observed_at_ms <= now_ms
        && now_ms.saturating_sub(readiness.observed_at_ms) <= custody_max_stale_ms
        && !readiness.ordinary_order_debt
        && !readiness.algo_order_debt
        && projections.execution.generation == readiness.generation
        && projections.execution.observed_at_ms == readiness.observed_at_ms
        && projections.owner.generation == readiness.generation
        && projections.owner.observed_at_ms == readiness.observed_at_ms
        && projections.protection.generation == readiness.generation
        && projections.protection.observed_at_ms == readiness.observed_at_ms
        && projections.risk_budget.generation == readiness.generation
        && projections.risk_budget.observed_at_ms == readiness.observed_at_ms
        && projections.execution.value == super::ExecutionProjection::Known
        && projections.owner.value == super::OwnerProjection::Clear
        && projections.protection.value == super::ProtectionProjection::Complete
        && readiness.exposure == PrivateExposure::Flat
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingShadowResidentError {
    #[error(
        "resident Shadow request requires absolute paths, a positive fill floor, and positive max turns"
    )]
    Request,
    #[error(
        "resident Shadow binding is invalid or differs from the configured Binance account/symbol"
    )]
    Binding,
    #[error("resident Shadow binding JSON is invalid: {0}")]
    BindingDecode(serde_json::Error),
    #[error("resident Shadow filesystem failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("resident Shadow signal handler failed: {0}")]
    Signal(String),
    #[error("resident Shadow private worker failed: {0}")]
    Worker(#[from] PrivateFactsWorkerError),
    #[error("resident Shadow durable orchestration failed: {0}")]
    Runtime(#[from] ScalpingResidentRuntimeError),
    #[error("resident Shadow source orchestration failed: {0}")]
    Sources(#[from] ScalpingResidentSourcesError),
    #[error("resident Shadow public client failed: {0}")]
    Public(#[from] PublicError),
    #[error("resident Shadow controller source failed: {0}")]
    Controller(#[from] ScalpingControllerSourceError),
    #[error("resident Shadow public clock regressed behind the private turn")]
    ClockRegression,
    #[error("resident Shadow run failed: {primary}; fail-closed cleanup: {cleanup:?}")]
    RunFailed {
        primary: String,
        cleanup: Vec<String>,
    },
    #[error("resident Shadow host failed: {0}")]
    Host(#[from] super::ScalpingShadowHostError),
    #[error(
        "resident Shadow stopped without a complete flat/protected private projection (worker={worker_state:?}, last_failure_stage={last_failure_stage:?})"
    )]
    UnsafeShutdown {
        worker_state: PrivateFactsWorkerState,
        last_failure_stage: Option<super::PrivateFactsFailureStage>,
    },
    #[error("mainnet strategy mutations require explicit live confirmation")]
    LiveConfirmation,
    #[error("Live settlement requires one complete current private-worker projection")]
    LivePrivate,
    #[error("a persisted Live exit action has no active semantic episode")]
    LiveExit,
    #[error("resident Live gateway failed closed: {0}")]
    Live(#[from] super::ScalpingLiveGatewayError),
    #[error("resident Live account writer registry failed closed: {reason}")]
    WriterRegistry { reason: String },
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::{
        config::DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
        domain::{Amount, Asset},
        runtime::{
            CustodyStatus, ExecutionProjection, OwnerProjection, PrivateFactsProjectionInput,
            PrivateFactsReadiness, PrivateProjection, ProtectionProjection, RiskBudgetProjection,
            ScalpingResidentSourcesStatus,
        },
        strategy::scalping::StrategyKind,
    };

    use super::*;

    fn config() -> Result<Config, toml::de::Error> {
        toml::from_str(
            "trading_account_id = '00000000-0000-4000-8000-000000000001'\nsymbol = 'SOL/USDT'\n[binance]\naccount_binding = 'portfolio_margin_um'",
        )
    }

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding {
            strategy_kind: StrategyKind::Scalping,
            strategy_instance_id: "resident-sol".to_owned(),
            run_id: "shadow-1".to_owned(),
            exchange: EXCHANGE.to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "SOL/USDT".parse()?,
            parameter_release_id: "scalping-shadow-v1".to_owned(),
            owner_scope: "resident-sol:shadow-1".to_owned(),
            risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
        })
    }

    #[test]
    fn request_and_binding_are_explicit_and_scope_bound() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempdir()?;
        let request = ScalpingShadowResidentRequest {
            artifacts_root: directory.path().join("artifacts"),
            binding_path: directory.path().join("binding.json"),
            initial_fill_recovery_from_ms: 100,
            max_turns: Some(1),
        };
        validate_request(&request)?;
        validate_binding(&config()?, &binding()?)?;

        let mut wrong = binding()?;
        wrong.account = "primary".to_owned();
        assert!(matches!(
            validate_binding(&config()?, &wrong),
            Err(ScalpingShadowResidentError::Binding)
        ));
        let mut invalid = request;
        invalid.max_turns = Some(0);
        assert!(matches!(
            validate_request(&invalid),
            Err(ScalpingShadowResidentError::Request)
        ));
        Ok(())
    }

    #[test]
    fn live_request_cannot_bypass_explicit_mutation_confirmation()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let result = run_scalping_live_resident(
            &config()?,
            ScalpingLiveResidentRequest {
                artifacts_root: directory.path().join("live-artifacts"),
                binding_path: directory.path().join("binding.json"),
                initial_fill_recovery_from_ms: 100,
                max_turns: Some(1),
                confirm_mainnet_strategy_mutations: false,
            },
        );
        assert!(matches!(
            result,
            Err(ScalpingShadowResidentError::LiveConfirmation)
        ));
        Ok(())
    }

    #[test]
    fn binding_file_round_trips_without_runtime_defaults() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempdir()?;
        let path = directory.path().join("binding.json");
        let expected = binding()?;
        fs::write(&path, serde_json::to_vec(&expected)?)?;

        assert_eq!(load_binding(&path)?, expected);
        Ok(())
    }

    #[test]
    fn shutdown_report_is_always_stop_and_protect() {
        let report = stop_report();
        assert_eq!(report.control, Some(ControlTarget::StopAndProtect));
        assert_eq!(report.lifecycle.control, ControlDisposition::StopAndProtect);
        assert!(!report.entry_ready);
        assert!(report.forwarded_private.is_none());
    }

    #[test]
    fn expired_periodic_readback_requires_new_generation_only_for_idle_entry_path() {
        let fenced = stop_report();
        assert!(periodic_reconnect_required(true, true, &fenced));
        assert!(!periodic_reconnect_required(false, true, &fenced));
        assert!(!periodic_reconnect_required(true, false, &fenced));

        let ready = PrivateEntryGateReport {
            lifecycle: LifecycleReport {
                entry: EntryDisposition::Armed,
                control: ControlDisposition::None,
            },
            entry_ready: true,
            forwarded_private: None,
            control: None,
        };
        assert!(!periodic_reconnect_required(true, true, &ready));
    }

    #[test]
    fn open_private_is_forwarded_to_episode_but_never_to_controller_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        let private = PrivateFacts {
            generation: 7,
            observed_at_ms: 1_000,
            root_cause_fact_id: "private-readback:7:1000:11".to_owned(),
            safety: crate::strategy::scalping::SafetyProjection {
                private_snapshot_ready: true,
                exposure: crate::strategy::scalping::ExposureState::Open,
                execution_unknown: false,
                protection: crate::strategy::scalping::ProtectionState::Complete,
                owner_conflict: false,
                risk_budget_available: true,
            },
            custody: CustodyStatus::Complete,
        };
        let report = PrivateEntryGateReport {
            lifecycle: LifecycleReport {
                entry: EntryDisposition::Disarmed,
                control: ControlDisposition::None,
            },
            entry_ready: false,
            forwarded_private: Some(private.clone()),
            control: None,
        };
        let mut controller_private = Some(private);

        update_controller_private(&mut controller_private, &report);

        assert!(report.forwarded_private.is_some());
        assert!(controller_private.is_none());
        Ok(())
    }

    #[test]
    fn duplicate_ready_projection_keeps_controller_authority() {
        let private = PrivateFacts {
            generation: 7,
            observed_at_ms: 1_000,
            root_cause_fact_id: "private-readback:7:1000:11".to_owned(),
            safety: crate::strategy::scalping::SafetyProjection {
                private_snapshot_ready: true,
                exposure: crate::strategy::scalping::ExposureState::Flat,
                execution_unknown: false,
                protection: crate::strategy::scalping::ProtectionState::Complete,
                owner_conflict: false,
                risk_budget_available: true,
            },
            custody: CustodyStatus::Complete,
        };
        let report = PrivateEntryGateReport {
            lifecycle: LifecycleReport {
                entry: EntryDisposition::Armed,
                control: ControlDisposition::None,
            },
            entry_ready: true,
            forwarded_private: None,
            control: None,
        };
        let mut controller_private = Some(private.clone());

        update_controller_private(&mut controller_private, &report);

        assert_eq!(controller_private, Some(private));
    }

    #[test]
    fn shutdown_requires_flat_even_with_known_complete_custody() {
        let readiness = PrivateFactsReadiness {
            generation: 2,
            observed_at_ms: 1_000,
            root_cause_fact_id: "private-readback:2:1000:3".to_owned(),
            exposure: PrivateExposure::Flat,
            ordinary_order_debt: false,
            algo_order_debt: false,
        };
        let projections = PrivateFactsProjectionInput {
            execution: PrivateProjection {
                generation: 2,
                observed_at_ms: 1_000,
                value: ExecutionProjection::Known,
            },
            owner: PrivateProjection {
                generation: 2,
                observed_at_ms: 1_000,
                value: OwnerProjection::Clear,
            },
            protection: PrivateProjection {
                generation: 2,
                observed_at_ms: 1_000,
                value: ProtectionProjection::Complete,
            },
            risk_budget: PrivateProjection {
                generation: 2,
                observed_at_ms: 1_000,
                value: RiskBudgetProjection::Available,
            },
        };
        assert!(shutdown_private_safe(
            &readiness,
            &projections,
            1_001,
            DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
        ));

        let open = PrivateFactsReadiness {
            exposure: PrivateExposure::Open,
            ..readiness.clone()
        };
        assert!(!shutdown_private_safe(
            &open,
            &projections,
            1_001,
            DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
        ));

        let ordinary_debt = PrivateFactsReadiness {
            ordinary_order_debt: true,
            ..readiness.clone()
        };
        assert!(!shutdown_private_safe(
            &ordinary_debt,
            &projections,
            1_001,
            DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
        ));
        let algo_debt = PrivateFactsReadiness {
            algo_order_debt: true,
            ..readiness.clone()
        };
        assert!(!shutdown_private_safe(
            &algo_debt,
            &projections,
            1_001,
            DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
        ));

        let mismatched = PrivateFactsProjectionInput {
            execution: PrivateProjection {
                generation: 3,
                ..projections.execution
            },
            ..projections
        };
        assert!(!shutdown_private_safe(
            &readiness,
            &mismatched,
            1_001,
            DEFAULT_PRIVATE_CUSTODY_MAX_STALE_MS,
        ));
    }

    #[test]
    fn public_capture_uses_the_authoritative_sample_taken_after_private_work() {
        assert!(matches!(public_capture_now(1_000, 1_750), Ok(1_750)));
        assert!(matches!(
            public_capture_now(1_000, 999),
            Err(ScalpingShadowResidentError::ClockRegression)
        ));
    }

    #[test]
    fn stopped_flat_instance_keeps_read_only_public_warmup_running() {
        let mut status = ScalpingResidentSourcesStatus {
            public_generation: 1,
            public_session_state: SessionState::Snapshotting,
            public_feature_state: FeatureState::Warmup,
            public_in_flight: false,
            deadline_pending: false,
            awaiting_private_recovery: false,
            latest_private: true,
            pending_mark: false,
            pending_preparation: false,
            active_episode: false,
            control_stopped: true,
            applied_risk_ack_proof_id: None,
            applied_risk_ack_cursor_sequence: None,
            applied_risk_fenced: false,
        };
        assert!(should_poll_public(&status));
        status.active_episode = true;
        assert!(should_poll_public(&status));
        status.deadline_pending = true;
        assert!(!should_poll_public(&status));
    }
}
