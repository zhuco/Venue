use std::collections::BTreeMap;

use venue_domain::domain::{
    CommandId, DomainEvent, EventId, NativeOrderFamily, OrderOwner, OrderPurpose, OrderState,
};
use venue_runtime::{
    SignedAccountSnapshot, StrategyBinding,
    account::{AccountLanePriority, AccountPrivateFactInput},
    strategy::StrategyInput,
};

use super::{ProductionResident, grid::GridDispatchPlan, persist_anchor, resident_error};
use crate::NodeError;

const MAX_SIGNED_GRID_CATCH_UP_ROUNDS: usize = 64;
const MAX_SIGNED_GRID_CATCH_UP_FILLS: usize = 1_000;

impl<G: venue_runtime::AccountPhysicalGateway> ProductionResident<G> {
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

            let mut signed_surface = signed_managed_surface(binding, &snapshot)?;
            let fills = snapshot
                .fills()
                .iter()
                .filter(|fill| {
                    self.owner_for_signed_fill(fill)
                        .is_some_and(|owner| binding.matches_owner(&owner))
                        && self
                            .grid_bridges
                            .get(&binding.key)
                            .is_some_and(|bridge| bridge.has_accepted_route_for_fill(fill))
                })
                .cloned()
                .collect::<Vec<_>>();
            if fills.is_empty() {
                return Err(self.grid_recovery_error(
                    "managed Grid surface differs, but no new WAL-owned signed fill explains it",
                ));
            }
            applied_fills = applied_fills.saturating_add(fills.len());
            if applied_fills > MAX_SIGNED_GRID_CATCH_UP_FILLS {
                return Err(self.grid_recovery_error(
                    "signed Grid fill catch-up exceeded its bounded fill budget",
                ));
            }

            let mut plans = Vec::new();
            for fill in fills {
                plans.extend(self.stage_signed_grid_fill(binding, &snapshot, fill)?);
            }
            if plans.is_empty() {
                return Err(self.grid_recovery_error(
                    "signed Grid fills changed no complete owned order while the surface still differs",
                ));
            }
            for plan in plans {
                self.dispatch_recovered_grid_plan(binding, signed_surface, &plan)?;
                snapshot = self.refresh_signed_snapshot()?;
                signed_surface = signed_managed_surface(binding, &snapshot)?;
            }
        }
        Err(self.grid_recovery_error(
            "signed Grid fill catch-up exceeded its bounded convergence rounds",
        ))
    }

    fn stage_signed_grid_fill(
        &mut self,
        binding: &StrategyBinding,
        snapshot: &SignedAccountSnapshot,
        fill: venue_domain::Fill,
    ) -> Result<Vec<GridDispatchPlan>, NodeError> {
        if snapshot.private_generation() == 0
            || snapshot.private_generation() != self.runtime.active_private_generation()
            || snapshot.binding().venue.as_str() != binding.key.account.exchange.as_str()
            || snapshot.binding().trading_account_id != binding.key.account.account
            || fill.symbol != binding.key.symbol
        {
            return Err(NodeError::ResidentRuntime);
        }
        let venue = snapshot.binding().venue.as_str();
        let event_id = EventId::new(format!("{venue}-fill-{}", fill.fill_id))
            .map_err(|_| NodeError::ResidentRuntime)?;
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
                .map_err(|_| NodeError::ResidentRuntime)?,
            )
            .map_err(resident_error)?;
        if report.reconcile.is_some() || report.duplicate || report.pending_batch {
            return Err(NodeError::ResidentRuntime);
        }
        let delivery = report
            .deliveries
            .iter()
            .find(|delivery| delivery.target == binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        let turn = self
            .runtime
            .begin_private_strategy_turn(binding)
            .map_err(resident_error)?
            .ok_or(NodeError::ResidentRuntime)?;
        let StrategyInput::Private(fact) = turn.input() else {
            return Err(NodeError::ResidentRuntime);
        };
        if delivery.target != binding.key {
            return Err(NodeError::ResidentRuntime);
        }
        let DomainEvent::Fill(fill) = fact.record().event.clone() else {
            return Err(NodeError::ResidentRuntime);
        };
        let bridge = self
            .grid_bridges
            .get_mut(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?;
        let decision = bridge
            .observe_persisted_fill(&fill, snapshot.private_generation())
            .map_err(|_| NodeError::ResidentRuntime)?;
        let plans = match &decision {
            venue_strategies::hedged_grid::GridDecision::Noop => Vec::new(),
            venue_strategies::hedged_grid::GridDecision::Actions(actions) => actions
                .iter()
                .map(|action| bridge.plan_dispatch(action))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| NodeError::ResidentRuntime)?,
            venue_strategies::hedged_grid::GridDecision::Blocked => {
                return Err(NodeError::ResidentRuntime);
            }
        };
        let replay = bridge.checkpoint_bytes()?;
        let applied = self
            .runtime
            .persist_private_strategy_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &applied)?;
        Ok(plans)
    }

    fn dispatch_recovered_grid_plan(
        &mut self,
        binding: &StrategyBinding,
        signed_surface: BTreeMap<CommandId, OrderOwner>,
        plan: &GridDispatchPlan,
    ) -> Result<(), NodeError> {
        let replay = self
            .grid_bridges
            .get(&binding.key)
            .ok_or(NodeError::ResidentRuntime)?
            .checkpoint_bytes()?;
        let applied = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
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
        let replay = {
            let bridge = self
                .grid_bridges
                .get_mut(&binding.key)
                .ok_or(NodeError::ResidentRuntime)?;
            bridge
                .bind_accepted_plan(plan, &accepted)
                .map_err(|_| NodeError::ResidentRuntime)?;
            bridge.checkpoint_bytes()?
        };
        let accepted_turn = self
            .runtime
            .persist_resident_semantic_turn(binding, replay)
            .map_err(resident_error)?;
        persist_anchor(&self.artifacts_root, binding, &accepted_turn)
    }

    fn grid_recovery_error(&self, message: &str) -> NodeError {
        NodeError::LiveHost {
            venue: self.host.binding().venue,
            message: message.to_owned(),
        }
    }
}

fn signed_managed_surface(
    binding: &StrategyBinding,
    snapshot: &SignedAccountSnapshot,
) -> Result<BTreeMap<CommandId, OrderOwner>, NodeError> {
    let mut surface = BTreeMap::new();
    for order in snapshot.open_orders() {
        let client = CommandId::new(order.client_order_id.clone())
            .map_err(|_| NodeError::ResidentRuntime)?;
        let owner = order.owner.clone().ok_or(NodeError::ResidentRuntime)?;
        if order.external
            || order.symbol != binding.key.symbol
            || order.family != NativeOrderFamily::UmOrder
            || !binding.matches_owner(&owner)
            || !matches!(owner.purpose, OrderPurpose::Entry | OrderPurpose::Reduce)
            || !matches!(
                order.state,
                Some(OrderState::New | OrderState::PartiallyFilled)
            )
            || surface.insert(client, owner).is_some()
        {
            return Err(NodeError::ResidentRuntime);
        }
    }
    Ok(surface)
}
