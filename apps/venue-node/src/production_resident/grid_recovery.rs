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
const MAX_SIGNED_GRID_RESUME_BATCHES: usize = 64;

impl<G: venue_runtime::AccountPhysicalGateway> ProductionResident<G> {
    /// Continues only the exact reducer transactions whose complete deterministic command set is
    /// proven absent from Host WAL by registration. The current signed surface must still prove
    /// every cancellation target before each batch; any WAL presence takes the paused path.
    pub(super) fn resume_grid_unsubmitted_transactions(
        &mut self,
        binding: &StrategyBinding,
    ) -> Result<(), NodeError> {
        let snapshot = self
            .host
            .latest_signed_snapshot()
            .ok_or_else(|| self.grid_recovery_error("signed Grid resume snapshot is missing"))?;
        let mut snapshot = snapshot;
        let mut resumed_batches = 0_usize;
        loop {
            snapshot = self.converge_signed_grid_fills_before_pending_resume(binding, snapshot)?;
            let pending_command_ids = self
                .grid_bridges
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .pending_transaction_command_ids()
                .map_err(|error| {
                    self.grid_recovery_error(&format!(
                        "pending Grid transaction ids could not be reconstructed: {error}"
                    ))
                })?;
            if !super::all_pending_grid_commands_are_absent(&pending_command_ids, |id| {
                self.host.command_status(id).map_err(|error| {
                    self.grid_recovery_error(&format!(
                        "pending Grid command WAL classification failed: {error}"
                    ))
                })
            })? {
                return Err(self.grid_recovery_error(
                    "a staged Grid catch-up command already exists in WAL; signed reconciliation is required",
                ));
            }
            let plan = self
                .grid_bridges
                .get(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?
                .pending_dispatch_plans()
                .map_err(|error| {
                    self.grid_recovery_error(&format!(
                        "pending Grid transaction could not be reconstructed: {error}"
                    ))
                })?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    self.grid_recovery_error(
                        "pending Grid resume was selected without a transaction",
                    )
                })?;
            let signed_surface = self.prove_pending_grid_surface(binding, &snapshot)?;
            if resumed_batches >= MAX_SIGNED_GRID_RESUME_BATCHES {
                return Err(self
                    .grid_recovery_error("pending Grid resume exceeded its bounded batch budget"));
            }
            self.dispatch_recovered_grid_plan(binding, signed_surface, &plan)?;
            resumed_batches = resumed_batches.saturating_add(1);
            snapshot = self.refresh_signed_snapshot()?;
            let pending_remain = self
                .grid_bridges
                .get(&binding.key)
                .is_some_and(|bridge| !bridge.grid.pending_transactions.is_empty());
            if !pending_remain {
                return self.recover_grid_from_signed_fills(binding, snapshot);
            }
        }
    }

    /// A stopped writer can accumulate additional fills after the first pending checkpoint. Apply
    /// all newly signed, still-routed fills to the Actor before authorizing any old transaction;
    /// the resulting complete pending surface must then match the exchange exactly.
    fn converge_signed_grid_fills_before_pending_resume(
        &mut self,
        binding: &StrategyBinding,
        mut snapshot: SignedAccountSnapshot,
    ) -> Result<SignedAccountSnapshot, NodeError> {
        let mut applied_fills = 0_usize;
        for _ in 0..MAX_SIGNED_GRID_CATCH_UP_ROUNDS {
            let fills = self.unique_owned_signed_grid_fills(binding, &snapshot)?;
            let mut applied_this_round = 0_usize;
            for fill in fills {
                match self.classify_signed_grid_fill(binding, &fill)? {
                    SignedGridFillApplication::Apply => {
                        let _plans = self.stage_signed_grid_fill(binding, &snapshot, fill)?;
                        applied_this_round = applied_this_round.saturating_add(1);
                    }
                    SignedGridFillApplication::ExactDuplicate
                    | SignedGridFillApplication::Irrelevant => {}
                }
            }
            applied_fills = applied_fills.saturating_add(applied_this_round);
            if applied_fills > MAX_SIGNED_GRID_CATCH_UP_FILLS {
                return Err(self.grid_recovery_error(
                    "signed Grid pre-resume fill catch-up exceeded its bounded fill budget",
                ));
            }
            snapshot = self.refresh_signed_snapshot()?;
            if self
                .grid_bridges
                .get(&binding.key)
                .is_some_and(|bridge| bridge.signed_pending_surface_matches(snapshot.open_orders()))
            {
                return Ok(snapshot);
            }
            let mut has_new_signed_fill = false;
            for fill in self.unique_owned_signed_grid_fills(binding, &snapshot)? {
                if self.classify_signed_grid_fill(binding, &fill)?
                    == SignedGridFillApplication::Apply
                {
                    has_new_signed_fill = true;
                }
            }
            if !has_new_signed_fill {
                return Err(self.grid_recovery_error(
                    "pending Grid surface differs after a fresh signed read, but no new WAL-owned fill explains it",
                ));
            }
        }
        Err(self.grid_recovery_error(
            "signed Grid pre-resume catch-up exceeded its bounded convergence rounds",
        ))
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
        self.recover_grid_from_signed_fills(binding, snapshot)
    }

    pub(super) fn recover_grid_from_signed_fills(
        &mut self,
        binding: &StrategyBinding,
        mut snapshot: SignedAccountSnapshot,
    ) -> Result<(), NodeError> {
        let mut applied_fills = 0_usize;
        for _ in 0..MAX_SIGNED_GRID_CATCH_UP_ROUNDS {
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
                return Ok(());
            }

            let fills = self.unique_owned_signed_grid_fills(binding, &snapshot)?;
            let mut applied_this_round = 0_usize;
            let mut plans = Vec::new();
            for fill in fills {
                match self.classify_signed_grid_fill(binding, &fill)? {
                    SignedGridFillApplication::Apply => {
                        plans.extend(self.stage_signed_grid_fill(binding, &snapshot, fill)?);
                        applied_this_round = applied_this_round.saturating_add(1);
                    }
                    SignedGridFillApplication::ExactDuplicate
                    | SignedGridFillApplication::Irrelevant => {}
                }
            }
            if applied_this_round == 0 {
                return Err(self.grid_recovery_error(
                    "managed Grid surface differs, but no new WAL-owned signed fill explains it",
                ));
            }
            applied_fills = applied_fills.saturating_add(applied_this_round);
            if applied_fills > MAX_SIGNED_GRID_CATCH_UP_FILLS {
                return Err(self.grid_recovery_error(
                    "signed Grid fill catch-up exceeded its bounded fill budget",
                ));
            }
            if plans.is_empty() {
                snapshot = self.refresh_signed_snapshot()?;
                continue;
            }
            for plan in plans {
                let signed_surface = self.prove_pending_grid_surface(binding, &snapshot)?;
                self.dispatch_recovered_grid_plan(binding, signed_surface, &plan)?;
                snapshot = self.refresh_signed_snapshot()?;
            }
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

    fn classify_signed_grid_fill(
        &self,
        binding: &StrategyBinding,
        fill: &venue_domain::Fill,
    ) -> Result<SignedGridFillApplication, NodeError> {
        self.grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .signed_fill_application(fill)
            .map_err(|error| {
                self.grid_recovery_error(&format!(
                    "signed Grid fill {} conflicts with its durable route: {error}",
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
        let bridge = self.grid_bridges.get_mut(&binding.key).ok_or_else(|| {
            grid_recovery_error_for(
                self.host.binding().venue,
                "signed Grid bridge disappeared before private actor application",
            )
        })?;
        let decision = bridge
            .observe_persisted_fill(&fill, snapshot.private_generation())
            .map_err(|error| {
                grid_recovery_error_for(
                    self.host.binding().venue,
                    format!(
                        "signed Grid reducer rejected fill {}: {error}",
                        fill.fill_id
                    ),
                )
            })?;
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

    fn grid_recovery_error(&self, message: &str) -> NodeError {
        grid_recovery_error_for(self.host.binding().venue, message)
    }
}

fn grid_recovery_error_for(venue: VenueId, message: impl Into<String>) -> NodeError {
    NodeError::LiveHost {
        venue,
        message: message.into(),
    }
}
