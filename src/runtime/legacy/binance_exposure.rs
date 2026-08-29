use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    config::ExposureTakeProfitConfig,
    exchange::{binance_private, grid::GridRiskReadback},
    storage::{PrivateEvidence, PrivateEvidenceJournal},
    strategy::hedged_grid::{
        ExposureEpisodeState, ExposureGuardDecision, ExposureGuardState, GridPosition,
    },
};

use super::*;

const CHECKPOINT_FILE: &str = "hedged_grid_exposure_state.json";
const EVIDENCE_FILE: &str = "hedged_grid_exposure_evidence.jsonl";
const RECEIPT_FILE: &str = "exposure_take_profit.jsonl";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BinanceExposureCheckpoint {
    schema_version: u16,
    binding: HedgedGridBinding,
    guard: ExposureGuardState,
    pending: Option<crate::runtime::hedged_grid::ExposureReductionPending>,
    private_generation: u64,
}

pub(super) fn legacy_checkpoint_is_settled(
    artifacts_root: &Path,
    binding: &HedgedGridBinding,
) -> Result<bool, HedgedGridLiveError> {
    match ProjectionStore::new(artifacts_root.join(CHECKPOINT_FILE))
        .load::<BinanceExposureCheckpoint>()?
    {
        None => Ok(true),
        Some(checkpoint) => {
            checkpoint
                .guard
                .validate_checkpoint()
                .map_err(exposure_error)?;
            let active = |state: &ExposureEpisodeState| {
                matches!(
                    state,
                    ExposureEpisodeState::TriggerPersisted { .. }
                        | ExposureEpisodeState::Reducing { .. }
                        | ExposureEpisodeState::Reconciling { .. }
                )
            };
            Ok(checkpoint.schema_version == 1
                && checkpoint.binding == *binding
                && checkpoint.guard.binding == *binding
                && checkpoint.pending.is_none()
                && !active(&checkpoint.guard.long.state)
                && !active(&checkpoint.guard.short.state))
        }
    }
}

pub(super) struct BinanceExposureRuntime {
    settings: crate::runtime::hedged_grid::ExposureRuntimeSettings,
    store: ProjectionStore,
    evidence: PrivateEvidenceJournal,
    checkpoint: BinanceExposureCheckpoint,
    next_snapshot_ms: u64,
}

impl BinanceExposureRuntime {
    pub(super) fn open(
        config: Option<ExposureTakeProfitConfig>,
        binding: &HedgedGridBinding,
        artifacts_root: &Path,
    ) -> Result<Option<Self>, HedgedGridLiveError> {
        let Some(config) = config else {
            return Ok(None);
        };
        let settings = crate::runtime::hedged_grid::ExposureRuntimeSettings::try_from(config)
            .map_err(exposure_error)?;
        let store = ProjectionStore::new(artifacts_root.join(CHECKPOINT_FILE));
        let checkpoint = match store.load::<BinanceExposureCheckpoint>()? {
            None => BinanceExposureCheckpoint {
                schema_version: 1,
                binding: binding.clone(),
                guard: ExposureGuardState::new(binding.clone(), settings.guard.clone())
                    .map_err(exposure_error)?,
                pending: None,
                private_generation: 0,
            },
            Some(checkpoint)
                if checkpoint.schema_version == 1
                    && checkpoint.binding == *binding
                    && checkpoint.guard.binding == *binding
                    && checkpoint.guard.params == settings.guard =>
            {
                checkpoint
                    .guard
                    .validate_checkpoint()
                    .map_err(exposure_error)?;
                if let Some(pending) = &checkpoint.pending {
                    pending.validate_identity().map_err(exposure_error)?;
                }
                checkpoint
            }
            Some(_) => return Err(HedgedGridLiveError::Checkpoint),
        };
        store.save(&checkpoint)?;
        Ok(Some(Self {
            settings,
            store,
            evidence: PrivateEvidenceJournal::open(artifacts_root.join(EVIDENCE_FILE))
                .map_err(exposure_error)?,
            checkpoint,
            next_snapshot_ms: 0,
        }))
    }

    pub(super) fn due(&self, now_ms: u64) -> bool {
        now_ms >= self.next_snapshot_ms
    }

    pub(super) fn has_pending(&self) -> bool {
        self.checkpoint.pending.is_some()
    }

    pub(super) fn recover_unjournaled(
        &mut self,
        commands: &CommandJournal,
    ) -> Result<(), HedgedGridLiveError> {
        let Some(pending) = self.checkpoint.pending.as_mut() else {
            return Ok(());
        };
        let Some(command) = pending.command.as_ref() else {
            return Ok(());
        };
        if commands.receipt(&command.command_id).is_some() {
            return Ok(());
        }
        self.checkpoint
            .guard
            .recover_unprepared_trigger(pending.action.position, &pending.action.risk_episode_id)
            .map_err(exposure_error)?;
        pending.command = None;
        self.save()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn poll(
        &mut self,
        commands: &mut CommandJournal,
        transport: &BinancePrivateFactsTransport,
        authority: &WriterLeaseAuthority,
        writer: &WriterSession,
        binding: &HedgedGridBinding,
        instrument: &crate::domain::Instrument,
        worker_generation: u64,
        now_ms: u64,
    ) -> Result<bool, HedgedGridLiveError> {
        self.next_snapshot_ms = now_ms.saturating_add(self.settings.snapshot_interval_ms);
        if !self.settings.shadow && self.checkpoint.guard.release_shadow_latches() {
            self.save()?;
        }
        let resumed = self.checkpoint.pending.clone();
        if resumed
            .as_ref()
            .is_some_and(|pending| pending.command.is_some())
        {
            return Ok(true);
        }
        let (action, trigger_account) = if let Some(pending) = resumed {
            (pending.action, pending.review_account)
        } else {
            let readback = self.readback(transport, binding, worker_generation, now_ms)?;
            let snapshot_now_ms = transport.authoritative_now_ms()?;
            let selected = crate::runtime::hedged_grid::select_binding_risk_snapshot(
                &readback,
                binding,
                snapshot_now_ms,
                self.settings.guard.max_snapshot_age_ms,
            )
            .map_err(exposure_error)?;
            let mut trigger = None;
            for leg in [selected.long.as_ref(), selected.short.as_ref()]
                .into_iter()
                .flatten()
            {
                if let ExposureGuardDecision::ReduceProfitableExposure(action) = self
                    .checkpoint
                    .guard
                    .evaluate(&selected.account, leg, selected.validated_at_ms)
                    .map_err(exposure_error)?
                {
                    trigger = Some((action, leg.clone()));
                    break;
                }
            }
            for position in [GridPosition::Long, GridPosition::Short] {
                if selected.leg(position).is_none() {
                    self.checkpoint
                        .guard
                        .observe_flat(position, selected.account.private_generation)
                        .map_err(exposure_error)?;
                }
            }
            let Some((action, trigger_leg)) = trigger else {
                self.save()?;
                return Ok(false);
            };
            self.checkpoint.pending = Some(crate::runtime::hedged_grid::ExposureReductionPending {
                action: action.clone(),
                review_account: selected.account.clone(),
                review_leg: trigger_leg,
                command: None,
            });
            self.save()?;
            (action, selected.account)
        };

        if self.settings.shadow {
            info!(
                event = "grid_exposure_take_profit_would_trigger",
                exchange = %binding.exchange,
                account = %binding.account,
                symbol = %binding.symbol,
                position_side = ?action.position,
                account_equity = %trigger_account.account_equity,
                position_unrealized_pnl = %action.unrealized_pnl,
                position_notional = %action.position_notional,
                risk_episode_id = %action.risk_episode_id,
                "Binance高暴露浮盈减仓影子命中"
            );
            self.checkpoint
                .guard
                .mark_shadow_latched(action.position, &action.risk_episode_id)
                .map_err(exposure_error)?;
            self.checkpoint.pending = None;
            self.save()?;
            return Ok(false);
        }

        self.record_open_orders(transport, binding, worker_generation, now_ms)?;
        let review_now_ms = transport.authoritative_now_ms()?;
        let review = self.readback(transport, binding, worker_generation, review_now_ms)?;
        let review_validation_now_ms = transport.authoritative_now_ms()?;
        let selected = crate::runtime::hedged_grid::select_binding_risk_snapshot(
            &review,
            binding,
            review_validation_now_ms,
            self.settings.guard.max_snapshot_age_ms,
        )
        .map_err(exposure_error)?;
        let Some(review_leg) = selected.leg(action.position).cloned() else {
            return self.cancel_before_submit(
                action.position,
                &action.risk_episode_id,
                selected.account.private_generation,
            );
        };
        let mut proof = ExposureGuardState::new(binding.clone(), self.settings.guard.clone())
            .map_err(exposure_error)?;
        if !matches!(
            proof
                .evaluate(
                    &selected.account,
                    &review_leg,
                    selected.validated_at_ms,
                )
                .map_err(exposure_error)?,
            ExposureGuardDecision::ReduceProfitableExposure(ref fresh)
                if fresh.position == action.position
        ) {
            return self.cancel_before_submit(
                action.position,
                &action.risk_episode_id,
                selected.account.private_generation,
            );
        }
        let plan = crate::runtime::hedged_grid::plan_market_reduction(
            binding,
            &action,
            &selected.account,
            &review_leg,
            instrument,
            selected.validated_at_ms,
            self.settings.guard.max_snapshot_age_ms,
        )
        .map_err(exposure_error)?;
        let crate::runtime::hedged_grid::MarketReductionPlan::Authorized { command, .. } = plan
        else {
            warn!(
                event = "grid_exposure_take_profit_skipped_below_minimum",
                exchange = %binding.exchange,
                symbol = %binding.symbol,
                risk_episode_id = %action.risk_episode_id,
                "Binance规整后的风险减仓量低于可关闭最小量"
            );
            return self.cancel_before_submit(
                action.position,
                &action.risk_episode_id,
                selected.account.private_generation,
            );
        };
        self.checkpoint.pending = Some(crate::runtime::hedged_grid::ExposureReductionPending {
            action: action.clone(),
            review_account: selected.account,
            review_leg,
            command: Some(command.clone()),
        });
        self.checkpoint
            .guard
            .mark_reducing(action.position, &action.risk_episode_id)
            .map_err(exposure_error)?;
        self.save()?;
        let dispatch = dispatch_batch(
            commands,
            transport,
            authority,
            writer,
            vec![GridMutation::Reduce(command)],
        );
        match dispatch {
            Ok(()) | Err(HedgedGridLiveError::Rejected) => {
                self.checkpoint
                    .guard
                    .mark_reconciling(action.position, &action.risk_episode_id)
                    .map_err(exposure_error)?;
                self.save()?;
                Ok(true)
            }
            Err(HedgedGridLiveError::Unresolved) => {
                self.save()?;
                Ok(true)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn settle(
        &mut self,
        commands: &mut CommandJournal,
        transport: &BinancePrivateFactsTransport,
        snapshot: &PrivateFactsSnapshot,
        binding: &HedgedGridBinding,
        state: &mut HedgedGridState,
        state_store: &ProjectionStore,
        instrument: &crate::domain::Instrument,
        authority: &WriterLeaseAuthority,
        writer: Option<&WriterSession>,
        artifacts_root: &Path,
        now_ms: u64,
    ) -> Result<bool, HedgedGridLiveError> {
        let Some(pending) = self.checkpoint.pending.clone() else {
            return Ok(false);
        };
        let Some(command) = pending.command.as_ref() else {
            return Ok(true);
        };
        let receipt = commands
            .receipt(&command.command_id)
            .ok_or(HedgedGridLiveError::Unresolved)?;
        let episode_state = match pending.action.position {
            GridPosition::Long => self.checkpoint.guard.long.state.clone(),
            GridPosition::Short => self.checkpoint.guard.short.state.clone(),
        };
        if matches!(episode_state, ExposureEpisodeState::Latched { .. }) {
            return self.repair_latched_exposure(
                commands,
                transport,
                snapshot,
                binding,
                state,
                state_store,
                instrument,
                authority,
                writer,
                now_ms,
            );
        }
        if matches!(episode_state, ExposureEpisodeState::Reducing { .. })
            && matches!(
                receipt.state,
                CommandState::Accepted { .. } | CommandState::Rejected { .. }
            )
        {
            self.checkpoint
                .guard
                .mark_reconciling(pending.action.position, &pending.action.risk_episode_id)
                .map_err(exposure_error)?;
            self.save()?;
        }
        let venue_order_id = match &receipt.state {
            CommandState::Accepted { venue_order_id } => Some(venue_order_id.as_str()),
            CommandState::Rejected { .. } => None,
            CommandState::Prepared | CommandState::Submitted | CommandState::Unknown { .. } => {
                return Ok(true);
            }
        };
        let fills = snapshot
            .fills
            .iter()
            .filter(|fill| venue_order_id.is_some_and(|order_id| fill.order_id == order_id))
            .cloned()
            .map(|fill| crate::runtime::hedged_grid::associate_reduction_fill(command, fill))
            .collect::<Vec<_>>();
        let terminal = if matches!(receipt.state, CommandState::Rejected { .. }) {
            true
        } else {
            match transport
                .order_by_client_id(&pending.review_leg.symbol, command.client_order_id.as_str())
            {
                Ok(payload) => matches!(
                    binance_private::parse_order(&payload, &pending.review_leg.symbol)?.state,
                    OrderState::Filled
                        | OrderState::Cancelled
                        | OrderState::Expired
                        | OrderState::Rejected
                ),
                Err(PrivateFactsWorkerError::Private(PrivateError::Rejected {
                    api_code: Some(-2013),
                    ..
                })) => !fills.is_empty(),
                Err(_) => false,
            }
        };
        if !terminal {
            return Ok(true);
        }
        if fills.is_empty() {
            warn!(
                event = "grid_exposure_take_profit_zero_fill",
                exchange = %pending.review_account.exchange,
                symbol = %pending.review_leg.symbol,
                risk_episode_id = %pending.action.risk_episode_id,
                "Binance风险减仓终态无成交，不生成成功收据"
            );
        } else {
            let settled = self.readback(transport, binding, snapshot.generation, now_ms)?;
            let audit = crate::runtime::hedged_grid::summarize_reduction_fills(
                command,
                &pending.action,
                &pending.review_account,
                &pending.review_leg,
                &fills,
                settled.account.private_generation,
            )
            .map_err(exposure_error)?;
            let receipt_written = append_audit(artifacts_root, &audit)?;
            if receipt_written {
                info!(
                event = %audit.event,
                exchange = %audit.exchange,
                account = %audit.account,
                symbol = %audit.symbol,
                position_side = ?audit.position_side,
                account_equity = %audit.account_equity,
                position_unrealized_pnl = %audit.position_unrealized_pnl,
                position_notional_before = %audit.position_notional_before,
                requested_reduce_ratio = %audit.requested_reduce_ratio,
                executed_reduce_quantity = %audit.executed_reduce_quantity,
                executed_reduce_notional = %audit.executed_reduce_notional,
                average_fill_price = %audit.average_fill_price.value(),
                risk_currency = %audit.risk_currency,
                risk_episode_id = %audit.risk_episode_id,
                trigger_generation = audit.trigger_generation,
                settled_generation = audit.settled_generation,
                    "Binance高暴露浮盈仓位已按实际成交降险"
                );
            }
        }
        self.checkpoint
            .guard
            .mark_latched(
                pending.action.position,
                &pending.action.risk_episode_id,
                self.checkpoint.private_generation,
            )
            .map_err(exposure_error)?;
        self.save()?;
        // Preserve the episode for one normal grid turn so every concurrently signed maker fill
        // is replayed before a later generation repairs the physical ladder at the same anchor.
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    fn repair_latched_exposure(
        &mut self,
        commands: &mut CommandJournal,
        transport: &BinancePrivateFactsTransport,
        snapshot: &PrivateFactsSnapshot,
        binding: &HedgedGridBinding,
        state: &mut HedgedGridState,
        state_store: &ProjectionStore,
        instrument: &crate::domain::Instrument,
        authority: &WriterLeaseAuthority,
        writer: Option<&WriterSession>,
        now_ms: u64,
    ) -> Result<bool, HedgedGridLiveError> {
        if state.phase != GridPhase::Running || !state.pending_transactions.is_empty() {
            return Ok(false);
        }
        let signed = recovered_owned_orders(commands, binding, snapshot)?;
        let mark_price = state
            .inventory
            .as_ref()
            .map(|inventory| inventory.mark_price)
            .or_else(|| {
                self.checkpoint
                    .pending
                    .as_ref()
                    .map(|pending| pending.review_leg.mark_price)
            })
            .ok_or(HedgedGridLiveError::Snapshot)?;
        let final_inventory = inventory(
            snapshot,
            mark_price,
            strategy_private_generation(state, snapshot)?,
        )?;
        let crate::runtime::hedged_grid::ExposureLadderRepairPlan::Ready {
            target,
            cancel,
            place,
        } = crate::runtime::hedged_grid::plan_same_anchor_exposure_repair(
            state,
            &final_inventory,
            &signed,
        )?
        else {
            return Ok(false);
        };

        // Never chase an absent identity which already has a WAL history. It must first be
        // settled by exact order/fill recovery; only a never-prepared desired identity may place.
        for intent in &place {
            let client_id = client_order_id(&intent.key)?;
            if commands.command_id_by_client_id(&client_id).is_some() {
                return Ok(false);
            }
        }
        let mut accepted_cancel_pending = false;
        let cancel = cancel
            .into_iter()
            .filter_map(
                |key| match accepted_cancel_exists(commands, binding, &key) {
                    Ok(true) => {
                        accepted_cancel_pending = true;
                        None
                    }
                    Ok(false) => Some(Ok(key)),
                    Err(error) => Some(Err(error)),
                },
            )
            .collect::<Result<Vec<_>, HedgedGridLiveError>>()?;
        let dispatched = !cancel.is_empty() || !place.is_empty();
        if accepted_cancel_pending && !dispatched {
            return Ok(true);
        }
        let dispatch_writer = if dispatched {
            let Some(writer) = writer else {
                return Ok(false);
            };
            Some(writer)
        } else {
            None
        };

        state.owned_orders = target;
        state.reconcile_order_sequences();
        save_state(state_store, state)?;
        let cancellations = cancel
            .iter()
            .map(|key| next_cancel_command(commands, binding, key))
            .collect::<Result<Vec<_>, HedgedGridLiveError>>()?;
        let placements = place
            .iter()
            .map(|intent| place_command(binding, instrument, intent))
            .collect::<Result<Vec<_>, HedgedGridLiveError>>()?;
        for mutations in [cancellations, placements] {
            if mutations.is_empty() {
                continue;
            }
            match dispatch_batch(
                commands,
                transport,
                authority,
                dispatch_writer.ok_or(HedgedGridLiveError::Writer(WriterLeaseError::NoWriter))?,
                mutations,
            ) {
                Ok(()) => {}
                Err(HedgedGridLiveError::Rejected | HedgedGridLiveError::Unresolved) => {
                    if state.phase == GridPhase::Running {
                        state.block_for_order_reconciliation()?;
                        state.defer_blocked_reconciliation_until(
                            now_ms.saturating_add(GRID_REJECTED_RESET_DELAY_MS),
                        )?;
                    }
                    save_state(state_store, state)?;
                    return Ok(true);
                }
                Err(error) => return Err(error),
            }
        }
        if dispatched {
            return Ok(true);
        }

        self.checkpoint.pending = None;
        self.save()?;
        Ok(false)
    }

    fn readback(
        &mut self,
        transport: &BinancePrivateFactsTransport,
        binding: &HedgedGridBinding,
        worker_generation: u64,
        now_ms: u64,
    ) -> Result<GridRiskReadback, HedgedGridLiveError> {
        let generation = self
            .checkpoint
            .private_generation
            .max(self.evidence.last_generation())
            .max(worker_generation)
            .checked_add(1)
            .ok_or(HedgedGridLiveError::Clock)?;
        let readback = transport
            .private_rest()
            .risk_readback(
                &binding.symbol,
                generation,
                now_ms,
                self.settings.guard.max_snapshot_age_ms,
            )
            .map_err(exposure_error)?;
        if readback.raw_private_payloads.is_empty() {
            return Err(HedgedGridLiveError::Snapshot);
        }
        for payload in &readback.raw_private_payloads {
            self.evidence
                .append(
                    PrivateEvidence::new(generation, now_ms, payload.clone())
                        .map_err(exposure_error)?,
                )
                .map_err(exposure_error)?;
        }
        self.checkpoint.private_generation = generation;
        self.save()?;
        Ok(GridRiskReadback {
            raw_private_payloads: readback.raw_private_payloads,
            account: readback.account,
            legs: readback.legs,
        })
    }

    fn record_open_orders(
        &mut self,
        transport: &BinancePrivateFactsTransport,
        binding: &HedgedGridBinding,
        worker_generation: u64,
        now_ms: u64,
    ) -> Result<(), HedgedGridLiveError> {
        let payload = transport
            .private_rest()
            .open_orders(&binding.symbol)
            .map_err(exposure_error)?;
        let orders =
            binance_private::parse_orders(&payload, &binding.symbol).map_err(exposure_error)?;
        if orders
            .iter()
            .any(|order| !matches!(&order.client_order_id, crate::domain::FieldState::Known(_)))
        {
            return Err(HedgedGridLiveError::Snapshot);
        }
        let generation = self
            .checkpoint
            .private_generation
            .max(self.evidence.last_generation())
            .max(worker_generation)
            .checked_add(1)
            .ok_or(HedgedGridLiveError::Clock)?;
        self.evidence
            .append(PrivateEvidence::new(generation, now_ms, payload).map_err(exposure_error)?)
            .map_err(exposure_error)?;
        self.checkpoint.private_generation = generation;
        self.save()
    }

    fn cancel_before_submit(
        &mut self,
        position: GridPosition,
        risk_episode_id: &str,
        generation: u64,
    ) -> Result<bool, HedgedGridLiveError> {
        self.checkpoint
            .guard
            .mark_latched(position, risk_episode_id, generation)
            .map_err(exposure_error)?;
        self.checkpoint.pending = None;
        self.save()?;
        Ok(false)
    }

    fn save(&self) -> Result<(), HedgedGridLiveError> {
        self.store.save(&self.checkpoint).map_err(Into::into)
    }
}

fn append_audit(
    artifacts_root: &Path,
    audit: &crate::runtime::hedged_grid::ExposureReductionAudit,
) -> Result<bool, HedgedGridLiveError> {
    let path = artifacts_root.join(RECEIPT_FILE);
    crate::runtime::hedged_grid::append_reduction_audit_once(&path, audit).map_err(exposure_error)
}

fn exposure_error(error: impl std::fmt::Display) -> HedgedGridLiveError {
    HedgedGridLiveError::Exposure {
        reason: error.to_string(),
    }
}
