use super::*;

#[cfg(test)]
#[path = "reconcile_tests.rs"]
mod tests;

impl BinanceGridRuntime {
    pub(super) async fn reconcile_desired(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        desired: &GridDesiredSurface,
        now: u64,
    ) -> Result<ReconcileResult, BinanceGridRuntimeError> {
        if desired.instance_id != record.instance.instance_id
            || desired.symbol != record.instance.symbol
            || desired.config_revision != record.instance.config_revision
        {
            return Err(BinanceGridRuntimeError::Facts);
        }
        let desired_by_id = desired
            .orders
            .iter()
            .map(|order| (order.client_order_id.as_str(), order))
            .collect::<BTreeMap<_, _>>();
        let statuses = self
            .store
            .load_grid_commands(
                &record.instance.instance_id,
                record.instance.config_revision,
                desired.plan_revision,
            )
            .await?;
        let mut place_statuses = statuses
            .iter()
            .filter(|status| status.order_kind == ExecutorOrderKind::LimitPostOnly)
            .map(|status| (status.command_id.clone(), (status.state, status.updated_ms)))
            .collect::<BTreeMap<_, _>>();
        let prior_plans = prior_command_surfaces(
            desired,
            &actual.ownership,
            record.instance.config_revision,
            desired.plan_revision,
        );
        for (config_revision, plan_revision) in prior_plans {
            for status in self
                .store
                .load_grid_commands(&record.instance.instance_id, config_revision, plan_revision)
                .await?
            {
                if status.order_kind == ExecutorOrderKind::LimitPostOnly {
                    place_statuses
                        .entry(status.command_id)
                        .or_insert((status.state, status.updated_ms));
                }
            }
        }
        let mut facts_changed = false;
        for (client_order_id, order) in &actual.orders {
            if let Some(wanted) = desired_by_id.get(client_order_id.as_str()) {
                match actual_matches_desired(order, wanted)? {
                    DesiredOrderMatch::Exact => {}
                    DesiredOrderMatch::Partial => facts_changed = true,
                    DesiredOrderMatch::Conflict => {
                        return Ok(ReconcileResult::ResetRequired);
                    }
                }
            }
        }
        let mut missing = desired
            .orders
            .iter()
            .filter(|wanted| !actual.orders.contains_key(&wanted.client_order_id))
            .collect::<Vec<_>>();
        missing.sort_by_key(|order| order_priority(order));
        let mut placements = Vec::new();
        let mut completed_clients = BTreeSet::new();
        let mut unresolved_existing = 0_usize;
        let mut failed_clients = Vec::new();
        let mut failed_cancel = false;
        let mut count_failure = false;
        for wanted in &missing {
            if let Some(owner) = actual.ownership.get(&wanted.client_order_id) {
                let status = place_statuses.get(&owner.place_command_id).copied();
                match missing_place_result(
                    status,
                    projection.observed_ms,
                    record.instance.updated_ms,
                ) {
                    MissingPlaceResult::Pending => {
                        unresolved_existing = unresolved_existing.saturating_add(1);
                    }
                    MissingPlaceResult::Failed(count) => {
                        failed_clients.push(wanted.client_order_id.clone());
                        count_failure |= count;
                    }
                    MissingPlaceResult::FactsChanged => {
                        facts_changed = true;
                        completed_clients.insert(wanted.client_order_id.clone());
                    }
                    MissingPlaceResult::ResetRequired => {
                        return Ok(ReconcileResult::ResetRequired);
                    }
                }
            } else {
                placements.push(*wanted);
            }
        }
        if !desired.orders.is_empty()
            && !desired_closes_fit(
                desired,
                &private_facts(record, projection, actual)?.inventory,
                &actual.other_close_reservations,
                &actual.orders,
                &actual.ownership,
                &completed_clients,
            )?
        {
            return Ok(ReconcileResult::ResetRequired);
        }
        let mut cancellations = Vec::new();
        if !facts_changed {
            for client_order_id in actual.orders.keys() {
                if !desired_by_id.contains_key(client_order_id.as_str()) {
                    let prior = statuses.iter().find(|status| {
                        status.order_kind == ExecutorOrderKind::CancelExact
                            && status.target_client_order_id.as_deref()
                                == Some(client_order_id.as_str())
                    });
                    match prior.map(|status| status.state) {
                        Some(state) if is_nonterminal(state) => {}
                        Some(ExecutorCommandState::Reconciled)
                            if prior.is_some_and(|status| {
                                projection.observed_ms <= status.updated_ms
                            }) => {}
                        Some(ExecutorCommandState::Rejected | ExecutorCommandState::Cancelled) => {
                            failed_cancel = true;
                            count_failure |= prior.is_some_and(|status| {
                                status.updated_ms >= record.instance.updated_ms
                            });
                        }
                        Some(_) => return Ok(ReconcileResult::ResetRequired),
                        None => cancellations.push(client_order_id.as_str()),
                    }
                }
            }
        }
        if !failed_clients.is_empty() || failed_cancel {
            return Ok(ReconcileResult::Failed {
                clients: failed_clients,
                count: count_failure,
            });
        }
        let in_flight = statuses
            .iter()
            .filter(|status| is_nonterminal(status.state))
            .count();
        let new_placement_count = placements.len();
        if unresolved_existing == 0 && (!placements.is_empty() || !cancellations.is_empty()) {
            let generation = record
                .instance
                .anchor
                .as_ref()
                .map_or(1, |anchor| anchor.instrument_generation);
            let batch = prepare_mutation_batch(
                record,
                desired,
                placements,
                cancellations,
                in_flight,
                generation,
                now,
            )?;
            if !batch.placements.is_empty() || !batch.cancellations.is_empty() {
                let receipt = self.store.enqueue_mutation_batch(&batch, now).await?;
                if receipt.command_count != 0 {
                    self.hot_path.wake_commands();
                }
                return Ok(ReconcileResult::Pending);
            }
        }
        if unresolved_existing != 0 || new_placement_count != 0 {
            return Ok(ReconcileResult::Pending);
        }
        if facts_changed {
            return Ok(ReconcileResult::FactsChanged);
        }
        if !actual
            .orders
            .keys()
            .all(|client| desired_by_id.contains_key(client.as_str()))
        {
            return Ok(ReconcileResult::Pending);
        }
        if self
            .store
            .has_nonterminal_grid_mutations(&record.instance.instance_id, None)
            .await?
        {
            Ok(ReconcileResult::Pending)
        } else {
            Ok(ReconcileResult::Converged)
        }
    }

    pub(super) async fn finish_reconcile(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        desired: &GridDesiredSurface,
        result: ReconcileResult,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        match result {
            ReconcileResult::Pending => {
                self.store
                    .update_convergence(
                        &GridConvergenceUpdate {
                            instance_id: record.instance.instance_id.clone(),
                            expected_instance_revision: record.instance.revision,
                            expected_state: record.instance.state,
                            expected_plan_revision: desired.plan_revision,
                            next_plan_revision: desired.plan_revision,
                            desired_digest: desired.desired_digest,
                            dirty: true,
                            consecutive_failures: record.instance.consecutive_failures,
                            last_facts_ms: projection.observed_ms,
                        },
                        now,
                    )
                    .await?;
                Ok(true)
            }
            ReconcileResult::Failed { clients, count } => {
                let summary = self
                    .store
                    .update_convergence(
                        &GridConvergenceUpdate {
                            instance_id: record.instance.instance_id.clone(),
                            expected_instance_revision: record.instance.revision,
                            expected_state: record.instance.state,
                            expected_plan_revision: desired.plan_revision,
                            next_plan_revision: desired.plan_revision,
                            desired_digest: desired.desired_digest,
                            dirty: true,
                            consecutive_failures: record
                                .instance
                                .consecutive_failures
                                .saturating_add(u16::from(count)),
                            last_facts_ms: projection.observed_ms,
                        },
                        now,
                    )
                    .await?;
                if summary.state == GridInstanceState::ResetRequired {
                    return Ok(true);
                }
                let next = summary
                    .plan_revision
                    .checked_add(1)
                    .ok_or(BinanceGridRuntimeError::Facts)?;
                let mut orders = desired.orders.clone();
                for client in clients {
                    let failed = orders
                        .iter_mut()
                        .find(|order| order.client_order_id == client)
                        .ok_or(BinanceGridRuntimeError::Facts)?;
                    failed.client_order_id = durable_id(
                        "vgp",
                        &summary.instance_id,
                        summary.config_revision,
                        next,
                        &failed.key.encoded(),
                        36,
                    );
                }
                let mut next_anchor = summary.anchor.clone();
                let digest = match next_anchor.as_mut() {
                    Some(anchor) => {
                        anchor.revision = next;
                        desired_digest(&planner_anchor(anchor, summary.config_revision)?, &orders)
                    }
                    None if orders.is_empty() => desired.desired_digest,
                    None => return Err(BinanceGridRuntimeError::Facts),
                };
                self.store
                    .commit_plan_surface(
                        &summary.instance_id,
                        summary.revision,
                        summary.config_revision,
                        summary.plan_revision,
                        next,
                        next_anchor.as_ref(),
                        digest,
                        &orders,
                        projection.observed_ms,
                        now,
                    )
                    .await?;
                Ok(true)
            }
            ReconcileResult::Converged | ReconcileResult::FactsChanged => {
                if record.instance.dirty {
                    self.store
                        .update_convergence(
                            &GridConvergenceUpdate {
                                instance_id: record.instance.instance_id.clone(),
                                expected_instance_revision: record.instance.revision,
                                expected_state: record.instance.state,
                                expected_plan_revision: desired.plan_revision,
                                next_plan_revision: desired.plan_revision,
                                desired_digest: desired.desired_digest,
                                dirty: false,
                                consecutive_failures: 0,
                                last_facts_ms: projection.observed_ms,
                            },
                            now,
                        )
                        .await?;
                }
                Ok(true)
            }
            ReconcileResult::ResetRequired => {
                self.store
                    .settle_runtime_state(
                        &record.instance.instance_id,
                        record.instance.state,
                        GridInstanceState::ResetRequired,
                        Some("surface_conflict"),
                        now,
                    )
                    .await?;
                Ok(true)
            }
        }
    }

    pub(super) async fn finish_stop(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let desired = empty_surface(record, empty_digest(), record.instance.plan_revision);
        let result = self
            .reconcile_desired(record, projection, actual, &desired, now)
            .await?;
        if result == ReconcileResult::Converged
            && self.lifecycle_commands_observed(record, projection).await?
        {
            self.store
                .settle_runtime_state(
                    &record.instance.instance_id,
                    GridInstanceState::StopPending,
                    GridInstanceState::Stopped,
                    None,
                    now,
                )
                .await?;
        } else {
            self.settle_lifecycle_timeout(record, now).await?;
        }
        Ok(true)
    }

    pub(super) async fn finish_pause(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let desired = empty_surface(record, empty_digest(), record.instance.plan_revision);
        let result = self
            .reconcile_desired(record, projection, actual, &desired, now)
            .await?;
        if result == ReconcileResult::Converged
            && self.lifecycle_commands_observed(record, projection).await?
        {
            self.store
                .update_convergence(
                    &GridConvergenceUpdate {
                        instance_id: record.instance.instance_id.clone(),
                        expected_instance_revision: record.instance.revision,
                        expected_state: GridInstanceState::Paused,
                        expected_plan_revision: record.instance.plan_revision,
                        next_plan_revision: record.instance.plan_revision,
                        desired_digest: desired.desired_digest,
                        dirty: false,
                        consecutive_failures: 0,
                        last_facts_ms: projection.observed_ms,
                    },
                    now,
                )
                .await?;
        } else {
            self.settle_lifecycle_timeout(record, now).await?;
        }
        Ok(true)
    }

    pub(super) async fn finish_reset(
        &mut self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
        actual: &ActualSurface,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let desired = empty_surface(record, empty_digest(), record.instance.plan_revision);
        let result = self
            .reconcile_desired(record, projection, actual, &desired, now)
            .await?;
        if result == ReconcileResult::Converged
            && self.lifecycle_commands_observed(record, projection).await?
        {
            let _ = self.refresh_market(record, projection, now).await?;
            self.store
                .settle_runtime_state(
                    &record.instance.instance_id,
                    GridInstanceState::ResetRequired,
                    GridInstanceState::Running,
                    None,
                    now,
                )
                .await?;
        } else {
            self.settle_lifecycle_timeout(record, now).await?;
        }
        Ok(true)
    }

    async fn lifecycle_commands_observed(
        &self,
        record: &GridRuntimeRecord,
        projection: &TerminalAccountProjection,
    ) -> Result<bool, BinanceGridRuntimeError> {
        if self
            .store
            .has_nonterminal_grid_mutations(&record.instance.instance_id, None)
            .await?
        {
            return Ok(false);
        }
        let latest_current_plan = self
            .store
            .load_grid_commands(
                &record.instance.instance_id,
                record.instance.config_revision,
                record.instance.plan_revision,
            )
            .await?
            .into_iter()
            .map(|command| command.updated_ms)
            .max()
            .unwrap_or(record.instance.updated_ms);
        let latest = self
            .store
            .latest_grid_command_updated_ms(&record.instance.instance_id)
            .await?
            .unwrap_or(record.instance.updated_ms)
            .max(latest_current_plan)
            .max(record.instance.updated_ms);
        Ok(projection.observed_ms > latest)
    }

    pub(super) async fn settle_lifecycle_timeout(
        &self,
        record: &GridRuntimeRecord,
        now: u64,
    ) -> Result<bool, BinanceGridRuntimeError> {
        let Some(code) = lifecycle_timeout_code(
            record.instance.state,
            record.instance.convergence_started_ms,
            record.instance.config.reset_policy.convergence_timeout_ms,
            now,
        ) else {
            return Ok(false);
        };
        self.store
            .settle_runtime_state(
                &record.instance.instance_id,
                record.instance.state,
                GridInstanceState::NeedsAttention,
                Some(code),
                now,
            )
            .await?;
        Ok(true)
    }
}
