use std::collections::BTreeMap;

use venue_domain::domain::{CommandId, DomainEvent, EventId, NativeOrderFamily, OrderOwner};
use venue_gateway_api::VenueId;
use venue_runtime::{
    SignedAccountSnapshot, StrategyBinding,
    account::{AccountLanePriority, AccountPrivateFactInput},
    strategy::StrategyInput,
};

use super::{
    ProductionResident,
    grid::{GridDispatchPlan, SignedGridFillApplication},
    persist_anchor,
};
use crate::NodeError;

const MAX_SIGNED_GRID_CATCH_UP_ROUNDS: usize = 64;
const MAX_SIGNED_GRID_CATCH_UP_FILLS: usize = 1_000;
const GRID_STARTUP_RECONCILIATION_STEPS_PER_TARGET: usize = 8;
const GRID_STARTUP_RECONCILIATION_FIXED_STEPS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SignedGridRecoveryOutcome {
    Converged,
    UnexplainedSurface,
}

impl<G: venue_runtime::AccountPhysicalGateway> ProductionResident<G> {
    /// A place-only install persists its semantic routes before the first physical child. If a
    /// process stops mid-batch, bind only exact WAL-Accepted children, discard Absent/Rejected
    /// children, and move the accepted subset into the ordinary signed cancellation drain.
    pub(super) fn recover_unconfirmed_grid_install_on_startup(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), NodeError> {
        let _snapshot = self.host.latest_signed_snapshot().ok_or_else(|| {
            self.grid_recovery_error("unconfirmed Grid install snapshot is missing")
        })?;
        let plan = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .unconfirmed_install_plan()
            .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
        let mut accepted = Vec::new();
        for command in &plan.commands {
            let status = self
                .host
                .command_status(command.command_id())
                .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
            if status.is_some()
                && self.host.command_snapshot(command.command_id()).as_ref() != Some(command)
            {
                return Err(self.grid_recovery_error(
                    "unconfirmed Grid install WAL bytes differ from its semantic checkpoint",
                ));
            }
            match status.map(|status| status.state().clone()) {
                None | Some(venue_runtime::CommandState::Rejected { .. }) => {}
                Some(venue_runtime::CommandState::Accepted { venue_order_id }) => {
                    accepted.push((command.command_id().clone(), venue_order_id));
                }
                Some(venue_runtime::CommandState::Prepared) => {
                    return Err(self.grid_recovery_error(
                        "Host startup left an unretired Prepared Grid install child",
                    ));
                }
                Some(
                    venue_runtime::CommandState::Submitted
                    | venue_runtime::CommandState::Unknown { .. },
                ) => {
                    return Err(self.grid_recovery_error(
                        "unconfirmed Grid install contains an unresolved WAL child",
                    ));
                }
            }
        }
        let mut reconciled = self
            .grid_bridges
            .get(&binding.key)
            .cloned()
            .ok_or(NodeError::ResidentRuntime)?;
        reconciled
            .bind_accepted_install_routes(&plan, &accepted)
            .and_then(|()| reconciled.begin_unconfirmed_install_reconciliation())
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "unconfirmed Grid install could not enter reconciliation: {error}"
                ))
            })?;
        self.grid_bridges.insert(binding.key.clone(), reconciled);
        self.persist_grid_reconciliation_checkpoint(binding)?;
        self.reset_grid_on_startup(binding)
    }

    /// A fully terminal Rejected rolling batch is not Unknown: when every other reserved command
    /// is absent and the fresh signed surface exactly matches the reducer rollback, retire the
    /// stale price ladder through the same Host/WAL lane and schedule one current-market rebuild.
    pub(super) fn reset_grid_on_startup(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), NodeError> {
        let mut snapshot = self
            .host
            .latest_signed_snapshot()
            .ok_or_else(|| self.grid_recovery_error("startup Grid recovery snapshot is missing"))?;
        if !self
            .grid_bridges
            .get(&binding.key)
            .is_some_and(super::grid::GridBridgeState::has_startup_reconciliation)
        {
            let transaction_ids = self
                .grid_bridges
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .pending_transaction_command_ids()
                .map_err(|error| {
                    self.grid_recovery_error(&format!(
                        "terminal Grid transaction ids could not be reconstructed: {error}"
                    ))
                })?
                .into_iter()
                .map(|(transaction_id, _)| transaction_id)
                .collect::<Vec<_>>();
            let mut reconciled = self
                .grid_bridges
                .get(&binding.key)
                .cloned()
                .ok_or(NodeError::ResidentRuntime)?;
            if transaction_ids.is_empty() {
                reconciled.begin_startup_reconciliation().map_err(|error| {
                    self.grid_recovery_error(&format!(
                        "startup Grid reset could not begin: {error}"
                    ))
                })?;
            } else {
                reconciled
                    .abandon_pending_for_reconciliation(&transaction_ids)
                    .map_err(|error| {
                        self.grid_recovery_error(&format!(
                            "startup Grid transactions could not roll back: {error}"
                        ))
                    })?;
            }
            self.grid_bridges.insert(binding.key.clone(), reconciled);
            self.persist_grid_reconciliation_checkpoint(binding)?;
        }

        // Rollback happens before catch-up so a fill of the old transaction's cancellation target
        // is an ordinary owned fill in ResettingGrid, not a reason to revive or reject the stale
        // transaction. The episode checkpoint above makes this ordering crash-recoverable.
        for fill in self.unique_owned_signed_grid_fills(binding, &snapshot)? {
            if self.classify_signed_grid_fill(binding, &fill)? == SignedGridFillApplication::Apply {
                let _plans = self.stage_signed_grid_fill(binding, &snapshot, fill)?;
            }
        }
        snapshot = self.refresh_signed_snapshot()?;
        let settled_absent = self
            .grid_bridges
            .get_mut(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .settle_signed_absent_reconciliation_orders(snapshot.open_orders())
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "startup Grid signed subset could not retire absent orders: {error}"
                ))
            })?;
        if settled_absent > 0 {
            self.persist_grid_reconciliation_checkpoint(binding)?;
        }
        if !self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .signed_desired_matches(snapshot.open_orders())
        {
            return Err(self.grid_recovery_error(
                "startup Grid rollback and signed fill catch-up do not match the fresh surface",
            ));
        }
        self.drain_startup_grid_reconciliation(binding, snapshot)
    }

    fn drain_startup_grid_reconciliation(
        &mut self,
        binding: &StrategyBinding,
        mut snapshot: SignedAccountSnapshot,
    ) -> Result<(), NodeError> {
        let max_steps = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .grid
            .owned_orders
            .len()
            .saturating_mul(GRID_STARTUP_RECONCILIATION_STEPS_PER_TARGET)
            .saturating_add(GRID_STARTUP_RECONCILIATION_FIXED_STEPS);
        for _ in 0..max_steps {
            let mut applied_fill = false;
            for fill in self.unique_owned_signed_grid_fills(binding, &snapshot)? {
                if self.classify_signed_grid_fill(binding, &fill)?
                    == SignedGridFillApplication::Apply
                {
                    let _plans = self.stage_signed_grid_fill(binding, &snapshot, fill)?;
                    applied_fill = true;
                }
            }
            if applied_fill {
                snapshot = self.refresh_signed_snapshot()?;
                continue;
            }

            let settled_absent = self
                .grid_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .settle_signed_absent_reconciliation_orders(snapshot.open_orders())
                .map_err(|error| {
                    self.grid_recovery_error(&format!(
                        "startup Grid signed subset changed outside owned routes: {error}"
                    ))
                })?;
            if settled_absent > 0 {
                self.persist_grid_reconciliation_checkpoint(binding)?;
                continue;
            }

            let Some(target) = self
                .grid_bridges
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .reconciliation_target()
                .map_err(|error| {
                    self.grid_recovery_error(&format!(
                        "startup Grid reconciliation target is invalid: {error}"
                    ))
                })?
            else {
                if !snapshot.open_orders().is_empty() {
                    return Err(self.grid_recovery_error(
                        "startup Grid reconciliation drained locally but signed orders remain",
                    ));
                }
                let expected = self
                    .grid_bridges
                    .get(&binding.key)
                    .ok_or(NodeError::ResidentRuntime)?
                    .expected_signed_surface()?;
                self.host
                    .confirm_managed_grid_surface(&mut self.runtime, binding, expected)
                    .map_err(|error| {
                        self.grid_recovery_error(&format!(
                            "startup Grid signed-empty confirmation failed: {error}"
                        ))
                    })?;
                if self
                    .grid_bridges
                    .get(&binding.key)
                    .ok_or(NodeError::ResidentRuntime)?
                    .needs_reconciliation_rebuild()
                {
                    self.grid_bootstrap_pending.insert(binding.key.clone());
                } else {
                    self.runtime
                        .request_pause(&binding.key)
                        .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
                }
                return Ok(());
            };

            let attempt = self
                .grid_bridges
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .reconciliation_attempt(&target)
                .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
            if let Some(attempt) = attempt {
                let plan = self
                    .grid_bridges
                    .get(&binding.key)
                    .ok_or(NodeError::ResidentRuntime)?
                    .reconciliation_cancel_plan(&target, attempt)
                    .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
                let command_id = plan
                    .commands
                    .first()
                    .map(venue_domain::domain::ExecutionCommand::command_id)
                    .ok_or(NodeError::ResidentRuntime)?;
                let status = self
                    .host
                    .command_status(command_id)
                    .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
                if status.is_some()
                    && self.host.command_snapshot(command_id).as_ref() != plan.commands.first()
                {
                    return Err(self.grid_recovery_error(
                        "startup Grid cancellation WAL bytes differ from its episode plan",
                    ));
                }
                match status.map(|status| status.state().clone()) {
                    Some(venue_runtime::CommandState::Accepted { .. }) => {
                        let mut settled = self
                            .grid_bridges
                            .get(&binding.key)
                            .cloned()
                            .ok_or(NodeError::ResidentRuntime)?;
                        settled
                            .settle_reconciliation_cancel(&target)
                            .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
                        if !settled.signed_desired_matches(snapshot.open_orders()) {
                            return Err(self.grid_recovery_error(
                                "Accepted startup Grid cancellation is not absent from signed facts",
                            ));
                        }
                        self.grid_bridges.insert(binding.key.clone(), settled);
                        self.persist_grid_reconciliation_checkpoint(binding)?;
                        continue;
                    }
                    Some(venue_runtime::CommandState::Rejected { .. }) => {
                        if !self
                            .grid_bridges
                            .get(&binding.key)
                            .ok_or(NodeError::ResidentRuntime)?
                            .signed_desired_matches(snapshot.open_orders())
                        {
                            return Err(self.grid_recovery_error(
                                "Rejected startup Grid cancellation has an unexplained signed gap",
                            ));
                        }
                    }
                    Some(venue_runtime::CommandState::Prepared) => {
                        return Err(self.grid_recovery_error(
                            "Host startup left an unretired Prepared Grid cancellation",
                        ));
                    }
                    Some(
                        venue_runtime::CommandState::Submitted
                        | venue_runtime::CommandState::Unknown { .. },
                    ) => {
                        return Err(self.grid_recovery_error(
                            "startup Grid cancellation outcome is unresolved",
                        ));
                    }
                    None => {
                        if !self
                            .grid_bridges
                            .get(&binding.key)
                            .ok_or(NodeError::ResidentRuntime)?
                            .signed_desired_matches(snapshot.open_orders())
                        {
                            return Err(self.grid_recovery_error(
                                "startup Grid cancellation WAL is absent but signed surface changed",
                            ));
                        }
                        let applied = self.persist_grid_reconciliation_checkpoint(binding)?;
                        self.dispatch_grid_reconciliation_cancel(
                            binding, &snapshot, &applied, &plan,
                        )?;
                        snapshot = self.refresh_signed_snapshot()?;
                        continue;
                    }
                }
            } else if !self
                .grid_bridges
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .signed_desired_matches(snapshot.open_orders())
            {
                return Err(self.grid_recovery_error(
                    "startup Grid reconciliation surface changed before cancellation",
                ));
            }

            let attempt = self
                .grid_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .advance_reconciliation_attempt(&target)
                .map_err(|error| {
                    grid_recovery_error_for(
                        self.host.binding().venue,
                        format!("startup Grid cancellation attempt could not advance: {error}"),
                    )
                })?;
            let applied = self.persist_grid_reconciliation_checkpoint(binding)?;
            let plan = self
                .grid_bridges
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .reconciliation_cancel_plan(&target, attempt)
                .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
            self.dispatch_grid_reconciliation_cancel(binding, &snapshot, &applied, &plan)?;
            snapshot = self.refresh_signed_snapshot()?;
        }
        Err(self.grid_recovery_error("startup Grid reconciliation exceeded its bounded budget"))
    }

    pub(super) fn persist_grid_reconciliation_checkpoint(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<venue_runtime::AppliedStrategyTurnReceipt, NodeError> {
        let replay = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .checkpoint_bytes()?;
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(|error| self.grid_recovery_error(&error.to_string()))?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        Ok(applied)
    }

    fn dispatch_grid_reconciliation_cancel(
        &mut self,
        binding: &StrategyBinding,
        snapshot: &SignedAccountSnapshot,
        applied: &venue_runtime::AppliedStrategyTurnReceipt,
        plan: &GridDispatchPlan,
    ) -> Result<(), NodeError> {
        let signed_surface = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .expected_signed_surface()?;
        if !self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .signed_desired_matches(snapshot.open_orders())
        {
            return Err(self.grid_recovery_error(
                "startup Grid cancellation lost its signed surface authority",
            ));
        }
        self.host
            .prepare_and_admit_managed_grid_batch(
                &mut self.runtime,
                binding,
                applied,
                AccountLanePriority::Critical,
                signed_surface,
                &plan.commands,
            )
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "startup Grid cancellation was rejected before dispatch: {error}"
                ))
            })?;
        self.runtime
            .dispatch_next_with_host(&mut self.host)
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "startup Grid cancellation dispatch stopped: {error}"
                ))
            })?;
        Ok(())
    }

    /// Applies only WAL-owned signed fills that explain an exact managed-surface gap. This path
    /// runs before startup surface confirmation and after a rolling readback observes a second
    /// concurrent fill. Missing orders without signed fills never enter the reducer.
    pub(super) fn recover_grid_from_latest_signed_fills(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), NodeError> {
        let snapshot = self
            .host
            .latest_signed_snapshot()
            .ok_or_else(|| self.grid_recovery_error("signed Grid recovery snapshot is missing"))?;
        match self.recover_grid_from_signed_fills(binding, snapshot)? {
            SignedGridRecoveryOutcome::Converged => Ok(()),
            SignedGridRecoveryOutcome::UnexplainedSurface => Err(self.grid_recovery_error(
                "managed Grid surface differs, but no WAL-owned signed fill explains it",
            )),
        }
    }

    pub(super) fn recover_grid_from_signed_fills(
        &mut self,
        binding: &StrategyBinding,
        mut snapshot: SignedAccountSnapshot,
    ) -> Result<SignedGridRecoveryOutcome, NodeError> {
        let mut applied_fills = 0_usize;
        for _ in 0..MAX_SIGNED_GRID_CATCH_UP_ROUNDS {
            let fills = self.unique_owned_signed_grid_fills(binding, &snapshot)?;
            let mut applied_this_round = 0_usize;
            let mut consumed_fill_ids = Vec::new();
            for fill in fills {
                match self.classify_signed_grid_fill(binding, &fill)? {
                    SignedGridFillApplication::Apply => {
                        let fill_id = fill.fill_id.clone();
                        let _plans = self.stage_signed_grid_fill(binding, &snapshot, fill)?;
                        consumed_fill_ids.push(fill_id);
                        applied_this_round = applied_this_round.saturating_add(1);
                    }
                    SignedGridFillApplication::ExactDuplicate => {
                        consumed_fill_ids.push(fill.fill_id.clone());
                    }
                    SignedGridFillApplication::Irrelevant => {}
                }
            }
            if !consumed_fill_ids.is_empty() {
                self.host
                    .acknowledge_signed_fills(&consumed_fill_ids)
                    .map_err(|error| {
                        self.grid_recovery_error(&format!(
                            "signed Grid fill acknowledgement could not persist: {error}"
                        ))
                    })?;
            }
            applied_fills = applied_fills.saturating_add(applied_this_round);
            if applied_fills > MAX_SIGNED_GRID_CATCH_UP_FILLS {
                return Err(self.grid_recovery_error(
                    "signed Grid fill catch-up exceeded its bounded fill budget",
                ));
            }

            let pending = self
                .grid_bridges
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .pending_dispatch_plans()
                .map_err(|error| {
                    self.grid_recovery_error(&format!(
                        "durable pending Grid plans are invalid: {error}"
                    ))
                })?;
            if !pending.is_empty() {
                if !self
                    .grid_bridges
                    .get(&binding.key)
                    .ok_or(NodeError::ResidentRuntime)?
                    .signed_pending_surface_matches(snapshot.open_orders())
                {
                    if applied_this_round == 0 {
                        return Ok(SignedGridRecoveryOutcome::UnexplainedSurface);
                    }
                    snapshot = self.refresh_signed_snapshot()?;
                    continue;
                }
                for plan in pending {
                    let signed_surface = self.prove_pending_grid_surface(binding, &snapshot)?;
                    self.dispatch_recovered_grid_plan(binding, signed_surface, &plan)?;
                    snapshot = self.refresh_signed_snapshot()?;
                }
                continue;
            }

            let exact = self
                .grid_bridges
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .signed_desired_matches(snapshot.open_orders());
            if exact {
                let expected = self
                    .grid_bridges
                    .get(&binding.key)
                    .ok_or(NodeError::ResidentRuntime)?
                    .expected_signed_surface()?;
                self.host
                    .confirm_managed_grid_surface(&mut self.runtime, binding, expected)
                    .map_err(|error| {
                        self.grid_recovery_error(&format!(
                            "signed Grid catch-up converged but surface confirmation failed: {error}"
                        ))
                    })?;
                return Ok(SignedGridRecoveryOutcome::Converged);
            }
            if applied_this_round > 0 {
                snapshot = self.refresh_signed_snapshot()?;
                continue;
            }
            return Ok(SignedGridRecoveryOutcome::UnexplainedSurface);
        }
        Err(self.grid_recovery_error(
            "signed Grid fill catch-up exceeded its bounded convergence rounds",
        ))
    }

    fn unique_owned_signed_grid_fills(
        &self,
        binding: &StrategyBinding,
        snapshot: &SignedAccountSnapshot,
    ) -> Result<Vec<venue_domain::Fill>, NodeError> {
        let mut by_id = BTreeMap::<String, venue_domain::Fill>::new();
        let mut fills = Vec::new();
        for fill in snapshot.fills() {
            if let Some(previous) = by_id.get(&fill.fill_id) {
                if previous != fill {
                    return Err(self.grid_recovery_error(
                        "one signed Grid snapshot contains conflicting duplicate fill identities",
                    ));
                }
                continue;
            }
            by_id.insert(fill.fill_id.clone(), fill.clone());
            if self
                .owner_for_signed_fill(fill)
                .is_some_and(|owner| binding.matches_owner(&owner))
            {
                fills.push(fill.clone());
            }
        }
        Ok(fills)
    }

    pub(super) fn classify_signed_grid_fill(
        &self,
        binding: &StrategyBinding,
        fill: &venue_domain::Fill,
    ) -> Result<SignedGridFillApplication, NodeError> {
        let bridge = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        let application = bridge.signed_fill_application(fill).map_err(|error| {
            self.grid_recovery_error(&format!(
                "signed Grid fill {} conflicts with its durable route: {error}",
                fill.fill_id
            ))
        })?;
        if application != SignedGridFillApplication::Irrelevant {
            return Ok(application);
        }
        let Some(accepted_command) = self
            .host
            .command_snapshot_by_venue_order_id(NativeOrderFamily::UmOrder, &fill.order_id)
        else {
            return Ok(SignedGridFillApplication::Irrelevant);
        };
        bridge
            .signed_retired_fill_application(fill, &accepted_command)
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "signed Grid fill {} conflicts with its retired WAL route: {error}",
                    fill.fill_id
                ))
            })
    }

    fn stage_signed_grid_fill(
        &mut self,
        binding: &StrategyBinding,
        snapshot: &SignedAccountSnapshot,
        fill: venue_domain::Fill,
    ) -> Result<Vec<GridDispatchPlan>, NodeError> {
        let venue = self.host.binding().venue;
        if snapshot.private_generation() == 0
            || snapshot.private_generation() != self.runtime.active_private_generation()
            || snapshot.binding().venue.as_str() != binding.key.account.exchange.as_str()
            || snapshot.binding().trading_account_id != binding.key.account.account
            || fill.symbol != binding.key.symbol
        {
            return Err(grid_recovery_error_for(
                venue,
                format!(
                    "signed Grid fill metadata does not match the active Runtime: fill={}, signed_private_generation={}, active_private_generation={}",
                    fill.fill_id,
                    snapshot.private_generation(),
                    self.runtime.active_private_generation()
                ),
            ));
        }
        let venue = snapshot.binding().venue.as_str();
        let signed_fill_id = fill.fill_id.clone();
        let event_id = EventId::new(format!("{venue}-fill-{}", fill.fill_id)).map_err(|error| {
            grid_recovery_error_for(
                self.host.binding().venue,
                format!("signed Grid fill event id is invalid: {error}"),
            )
        })?;
        let report = self
            .runtime
            .ingest_private(
                AccountPrivateFactInput::new(
                    event_id,
                    self.runtime.connection_generation(),
                    snapshot.observed_at_ms(),
                    Some(NativeOrderFamily::UmOrder),
                    DomainEvent::Fill(fill),
                )
                .map_err(|error| {
                    grid_recovery_error_for(
                        self.host.binding().venue,
                        format!("signed Grid private fact is invalid: {error}"),
                    )
                })?,
            )
            .map_err(|error| {
                grid_recovery_error_for(
                    self.host.binding().venue,
                    format!("signed Grid private fact ingestion failed: {error}"),
                )
            })?;
        if report.reconcile.is_some() || report.duplicate || report.pending_batch {
            return Err(grid_recovery_error_for(
                self.host.binding().venue,
                format!(
                    "signed Grid private fact was not an immediately deliverable new fact: fill={}, reconcile={}, duplicate={}, pending_batch={}",
                    signed_fill_id,
                    report.reconcile.is_some(),
                    report.duplicate,
                    report.pending_batch
                ),
            ));
        }
        let delivery = report
            .deliveries
            .iter()
            .find(|delivery| delivery.target == binding.key)
            .ok_or_else(|| {
                grid_recovery_error_for(
                    self.host.binding().venue,
                    format!(
                        "signed Grid private fact had no delivery for the registered binding: fill={}",
                        signed_fill_id
                    ),
                )
            })?;
        let turn = self
            .runtime
            .begin_private_strategy_turn(binding)
            .map_err(|error| {
                grid_recovery_error_for(
                    self.host.binding().venue,
                    format!("signed Grid private turn could not begin: {error}"),
                )
            })?
            .ok_or_else(|| {
                grid_recovery_error_for(
                    self.host.binding().venue,
                    format!(
                        "signed Grid private delivery was not available after ingestion: fill={}",
                        signed_fill_id
                    ),
                )
            })?;
        let StrategyInput::Private(fact) = turn.input() else {
            return Err(grid_recovery_error_for(
                self.host.binding().venue,
                "signed Grid recovery received a non-private actor turn",
            ));
        };
        if delivery.target != binding.key {
            return Err(grid_recovery_error_for(
                self.host.binding().venue,
                "signed Grid private delivery target changed before actor application",
            ));
        }
        let DomainEvent::Fill(fill) = fact.record().event.clone() else {
            return Err(grid_recovery_error_for(
                self.host.binding().venue,
                "signed Grid private actor turn did not contain a Fill",
            ));
        };
        let application = self.classify_signed_grid_fill(binding, &fill)?;
        let bridge = self.grid_bridges.get_mut(&binding.key).ok_or_else(|| {
            grid_recovery_error_for(
                self.host.binding().venue,
                "signed Grid bridge disappeared before private actor application",
            )
        })?;
        let decision = match application {
            SignedGridFillApplication::ExactDuplicate => {
                venue_strategies::hedged_grid::GridDecision::Noop
            }
            SignedGridFillApplication::Apply => bridge
                .observe_persisted_fill(&fill, snapshot.private_generation())
                .map_err(|error| {
                    grid_recovery_error_for(
                        self.host.binding().venue,
                        format!(
                            "signed Grid reducer rejected fill {}: {error}",
                            fill.fill_id
                        ),
                    )
                })?,
            SignedGridFillApplication::Irrelevant => {
                return Err(grid_recovery_error_for(
                    self.host.binding().venue,
                    format!(
                        "signed Grid fill {} has no current or retired WAL route",
                        fill.fill_id
                    ),
                ));
            }
        };
        let plans = match &decision {
            venue_strategies::hedged_grid::GridDecision::Noop => Vec::new(),
            venue_strategies::hedged_grid::GridDecision::Actions(actions) => actions
                .iter()
                .map(|action| bridge.plan_dispatch(action))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    grid_recovery_error_for(
                        self.host.binding().venue,
                        format!("signed Grid dispatch plan is invalid: {error}"),
                    )
                })?,
            venue_strategies::hedged_grid::GridDecision::Blocked => {
                return Err(grid_recovery_error_for(
                    self.host.binding().venue,
                    "signed Grid reducer entered Blocked while applying a fill",
                ));
            }
        };
        let replay = bridge.checkpoint_bytes()?;
        let applied = self
            .runtime
            .persist_private_strategy_turn(binding, replay)
            .map_err(|error| {
                grid_recovery_error_for(
                    self.host.binding().venue,
                    format!("signed Grid private actor turn could not persist: {error}"),
                )
            })?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        Ok(plans)
    }

    fn dispatch_recovered_grid_plan(
        &mut self,
        binding: &StrategyBinding,
        signed_surface: BTreeMap<CommandId, OrderOwner>,
        plan: &GridDispatchPlan,
    ) -> Result<(), NodeError> {
        self.host
            .confirm_managed_grid_surface(&mut self.runtime, binding, signed_surface.clone())
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "signed Grid recovery surface authority was rejected: {error}"
                ))
            })?;
        let replay = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .checkpoint_bytes()
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "signed Grid recovery checkpoint could not be re-encoded: {error}"
                ))
            })?;
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "signed Grid recovery dispatch turn could not persist: {error}"
                ))
            })?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        if let Err(error) = self.host.prepare_and_admit_managed_grid_batch(
            &mut self.runtime,
            binding,
            &applied,
            AccountLanePriority::Normal,
            signed_surface,
            &plan.commands,
        ) {
            let _cleanup = self
                .host
                .reject_prepared_batch(&mut self.runtime, "grid_signed_catch_up_rejected");
            let _pause = self.pause_grid_after_bootstrap_failure(binding);
            return Err(self.grid_recovery_error(&format!(
                "signed Grid catch-up batch rejected before dispatch: {error}"
            )));
        }
        for _ in &plan.commands {
            if let Err(error) = self.runtime.dispatch_next_with_host(&mut self.host) {
                let _cleanup = self
                    .host
                    .reject_prepared_batch(&mut self.runtime, "grid_signed_catch_up_stopped");
                let _pause = self.pause_grid_after_bootstrap_failure(binding);
                return Err(self.grid_recovery_error(&format!(
                    "signed Grid catch-up dispatch stopped: {error}"
                )));
            }
        }
        let mut accepted = Vec::with_capacity(plan.commands.len());
        for command in &plan.commands {
            let status = self
                .host
                .command_status(command.command_id())
                .map_err(|error| self.grid_recovery_error(&error.to_string()))?
                .ok_or(NodeError::ResidentRuntime)?;
            match status.state() {
                venue_runtime::CommandState::Accepted { venue_order_id } => {
                    accepted.push((command.command_id().clone(), venue_order_id.clone()));
                }
                venue_runtime::CommandState::Prepared
                | venue_runtime::CommandState::Submitted
                | venue_runtime::CommandState::Rejected { .. }
                | venue_runtime::CommandState::Unknown { .. } => {
                    let _cleanup = self.host.reject_prepared_batch(
                        &mut self.runtime,
                        "grid_signed_catch_up_nonaccepted",
                    );
                    let _pause = self.pause_grid_after_bootstrap_failure(binding);
                    return Err(self.grid_recovery_error(
                        "signed Grid catch-up command did not reach Accepted",
                    ));
                }
            }
        }
        let venue = self.host.binding().venue;
        let replay = {
            let bridge = self
                .grid_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            bridge
                .bind_accepted_plan(plan, &accepted)
                .map_err(|error| {
                    grid_recovery_error_for(
                        venue,
                        format!("signed Grid accepted routes could not settle: {error}"),
                    )
                })?;
            bridge.checkpoint_bytes().map_err(|error| {
                grid_recovery_error_for(
                    venue,
                    format!("signed Grid accepted checkpoint could not encode: {error}"),
                )
            })?
        };
        let accepted_turn = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "signed Grid accepted dispatch turn could not persist: {error}"
                ))
            })?;
        persist_anchor(&self.artifacts_root, binding, &accepted_turn)
    }

    fn prove_pending_grid_surface(
        &self,
        binding: &StrategyBinding,
        snapshot: &SignedAccountSnapshot,
    ) -> Result<BTreeMap<CommandId, OrderOwner>, NodeError> {
        let bridge = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        if !bridge.signed_pending_surface_matches(snapshot.open_orders()) {
            return Err(self.grid_recovery_error(
                "signed Grid pre-dispatch surface differs from the durable pending checkpoint",
            ));
        }
        bridge.expected_pending_signed_surface().map_err(|error| {
            self.grid_recovery_error(&format!(
                "signed Grid pre-dispatch checkpoint surface is invalid: {error}"
            ))
        })
    }

    pub(super) fn grid_recovery_error(&self, message: &str) -> NodeError {
        grid_recovery_error_for(self.host.binding().venue, message)
    }
}

fn grid_recovery_error_for(venue: VenueId, message: impl Into<String>) -> NodeError {
    NodeError::LiveHost {
        venue,
        message: message.into(),
    }
}
