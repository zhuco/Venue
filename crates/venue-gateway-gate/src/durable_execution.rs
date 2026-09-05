//! Durable PostgreSQL executor bridge for Gate.io.

use super::*;

impl GateAccountGateway {
    pub fn durable_order_observation(
        &mut self,
        command: &ExecutionCommand,
    ) -> Result<Option<venue_execution::DurableOrderObservation>, GateAccountGatewayError> {
        let ExecutionCommand::PlaceLimit(order) = command else {
            return Ok(None);
        };
        if !venue_execution::validate_durable_command(self.binding.gateway_binding(), command) {
            return Ok(None);
        }
        let rules = self.registered_rules(&order.owner.symbol)?.clone();
        let request = prepare_exact_readback_by_client_id(
            &self.binding,
            &rules,
            order.client_order_id.as_str(),
        )
        .map_err(|_| GateAccountGatewayError::Readback)?;
        let readback = self
            .runtime
            .block_on(self.transport.execute_exact_readback(
                &self.binding,
                &self.credentials,
                &rules,
                &request,
                now_ms()?,
            ))
            .map_err(|_| GateAccountGatewayError::Readback)?;
        if !recovery_order_matches_command(command, &readback.order) {
            return Ok(None);
        }
        Ok(Some(venue_execution::DurableOrderObservation {
            client_order_id: order.client_order_id.as_str().to_owned(),
            native_order_id: readback.order.order_id,
            state: readback.order.state,
            filled_quantity: readback.order.filled_quantity,
        }))
    }

    pub fn durable_market_facts(
        &mut self,
    ) -> Result<venue_execution::DurableMarketFacts, GateAccountGatewayError> {
        let facts = self.fresh_grid_bootstrap_market()?;
        let reference_price =
            Price::new((facts.bid.value() + facts.ask.value()) / Decimal::from(2))
                .map_err(|_| GateAccountGatewayError::Readback)?;
        let metadata = venue_domain::domain::InstrumentMetadata::new(
            facts.rules.instrument.clone(),
            venue_domain::domain::Precision::new(
                facts.rules.instrument.price_tick.value(),
                facts.rules.instrument.price_tick.value(),
            )
            .map_err(|_| GateAccountGatewayError::Readback)?,
            venue_domain::domain::Precision::new(
                facts.rules.instrument.quantity_step,
                facts.rules.minimum_quantity(),
            )
            .map_err(|_| GateAccountGatewayError::Readback)?,
            None,
            true,
        )
        .map_err(|_| GateAccountGatewayError::Readback)?;
        Ok(venue_execution::DurableMarketFacts {
            binding: self.binding.gateway_binding().clone(),
            metadata,
            reference_price,
            observed_at_ms: facts.observed_at_ms,
            maximum_quantity: facts
                .rules
                .maximum_contracts
                .and_then(|v| v.checked_mul(facts.rules.quanto_multiplier)),
            maximum_price: None,
        })
    }

    /// Returns the user id repeated and cross-checked by both signed Hedge position legs during
    /// the latest complete private snapshot.
    pub fn verified_account_identity(&self) -> Result<String, GateAccountGatewayError> {
        if self.private.user_id.trim().is_empty() {
            Err(GateAccountGatewayError::Readback)
        } else {
            Ok(self.private.user_id.clone())
        }
    }

    /// Verifies the current key's signed Gate APIv4 permission groups immediately before a
    /// committed mutation. Reconciliation deliberately does not call this method.
    pub fn verify_strategy_permissions(&mut self) -> Result<(), GateAccountGatewayError> {
        let payload = self
            .runtime
            .block_on(self.transport.execute_account_risk_read(
                &self.binding,
                &self.credentials,
                &self.rules,
                endpoints::ACCOUNT_MAIN_KEYS,
                "",
                now_ms()?,
            ))
            .map_err(GateAccountGatewayError::Transport)?;
        verify_key_permissions(&payload, &self.private.user_id)
    }
}

fn verify_key_permissions(
    payload: &str,
    expected_user_id: &str,
) -> Result<(), GateAccountGatewayError> {
    let root: Value =
        serde_json::from_str(payload).map_err(|_| GateAccountGatewayError::Readback)?;
    let key = root.as_object().ok_or(GateAccountGatewayError::Readback)?;
    if key.get("state").and_then(Value::as_i64) != Some(1)
        || key
            .get("user_id")
            .and_then(Value::as_i64)
            .is_none_or(|value| value.to_string() != expected_user_id)
    {
        return Err(GateAccountGatewayError::Readback);
    }
    let permissions = key
        .get("perms")
        .and_then(Value::as_array)
        .ok_or(GateAccountGatewayError::Readback)?;
    let futures_write = permissions.iter().any(|permission| {
        permission.get("name").and_then(Value::as_str) == Some("futures")
            && permission.get("read_only").and_then(Value::as_bool) == Some(false)
    });
    let withdrawal_enabled = permissions.iter().any(|permission| {
        matches!(
            permission.get("name").and_then(Value::as_str),
            Some("withdrawal" | "withdraw")
        )
    });
    if futures_write && !withdrawal_enabled {
        Ok(())
    } else {
        Err(GateAccountGatewayError::Readback)
    }
}

impl GateAccountGateway {
    fn execute_command(
        &mut self,
        command: &ExecutionCommand,
        require_anchor_symbol: bool,
        context: Option<&venue_execution::DurableExecutionContext>,
    ) -> AccountGatewayResult {
        if command.validate().is_err() {
            return rejected("gate_permit_binding");
        }
        let owner = command.mutation_owner();
        let binding = self.binding.gateway_binding();
        if owner.exchange != binding.venue.as_str()
            || owner.account != binding.trading_account_id
            || (require_anchor_symbol && owner.symbol != binding.symbol)
            || !self.rules_catalog.contains_key(&owner.symbol)
        {
            return rejected("gate_permit_binding");
        }
        let symbol = owner.symbol.clone();
        if self.refresh_private_for(&symbol).is_err() {
            return rejected("gate_preflight_failed");
        }
        let rules = match self.registered_rules(&symbol) {
            Ok(value) => value,
            Err(_) => return rejected("gate_symbol_unconfigured"),
        };
        let prepared = match command {
            ExecutionCommand::PlaceLimit(command) => prepare_limit(&self.binding, &rules, command),
            ExecutionCommand::PlaceMarket(command) => {
                crate::execution::prepare_market(&self.binding, &rules, command)
            }
            ExecutionCommand::StopMarketFullPosition(command) => {
                prepare_stop_market(&self.binding, &rules, command)
            }
            ExecutionCommand::MarketReduce(command) => {
                prepare_reduce_once(&self.binding, &rules, command)
            }
            ExecutionCommand::Cancel(command) => {
                let target = context
                    .and_then(|value| value.target_native_order_id.clone())
                    .or_else(|| {
                        regular_venue_order_id_for_client_id(
                            &self.private.order_families.regular().orders,
                            command.target_client_order_id.as_str(),
                        )
                    });
                match target {
                    Some(venue_order_id) => {
                        match context.and_then(|value| value.target_command.as_ref()) {
                            Some(ExecutionCommand::StopMarketFullPosition(stop)) => {
                                prepare_price_cancel(
                                    &self.binding,
                                    &rules,
                                    command,
                                    stop,
                                    &venue_order_id,
                                )
                            }
                            _ => prepare_cancel(
                                &self.binding,
                                &rules,
                                &crate::GateCancelIntent {
                                    command: command.clone(),
                                    venue_order_id,
                                },
                            ),
                        }
                    }
                    None => return rejected("gate_cancel_target_unproven"),
                }
            }
            ExecutionCommand::StopMarketCloseAll(_) => {
                return rejected("gate_command_unsupported");
            }
        };
        let prepared = match prepared {
            Ok(value) => value,
            Err(_) => return rejected("gate_intent_rejected"),
        };
        match self.runtime.block_on(self.transport.execute_mutation(
            &self.binding,
            &self.credentials,
            &rules,
            prepared,
            match now_ms() {
                Ok(value) => value,
                Err(_) => return rejected("gate_clock"),
            },
        )) {
            Ok(GateMutationDispatch::Accepted(accepted)) => {
                self.settle_exact(&rules, &accepted.readback)
            }
            Ok(GateMutationDispatch::Unknown(unknown)) => {
                self.settle_exact(&rules, &unknown.readback)
            }
            Err(GateTransportError::VenueRejected) => rejected("gate_venue_rejected"),
            Err(_) => AccountGatewayResult::Unknown,
        }
    }

    pub(super) fn dispatch_permit(
        &mut self,
        permit: AccountDispatchPermit,
    ) -> AccountGatewayResult {
        if permit.binding() != self.binding.gateway_binding() {
            return rejected("gate_permit_binding");
        }
        self.execute_command(permit.command(), false, None)
    }
}

impl venue_execution::DurableAccountGateway for GateAccountGateway {
    fn execute_committed_with_context(
        &mut self,
        command: &ExecutionCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        if !venue_execution::validate_durable_context(command, context) {
            return rejected("gate_durable_context");
        }
        if !venue_execution::validate_durable_command(self.binding.gateway_binding(), command) {
            return rejected("gate_durable_scope");
        }
        if self.verify_strategy_permissions().is_err() {
            return AccountGatewayResult::Unknown;
        }
        self.execute_command(command, true, Some(context))
    }

    fn execute_committed(&mut self, command: &ExecutionCommand) -> AccountGatewayResult {
        if !venue_execution::validate_durable_command(self.binding.gateway_binding(), command) {
            return rejected("gate_durable_scope");
        }
        if self.verify_strategy_permissions().is_err() {
            return AccountGatewayResult::Unknown;
        }
        if matches!(command, ExecutionCommand::Cancel(_)) {
            return rejected("gate_durable_context");
        }
        self.execute_command(command, true, None)
    }

    fn reconcile_committed(&mut self, command: &ExecutionCommand) -> AccountGatewayResult {
        if matches!(command, ExecutionCommand::Cancel(_)) {
            return AccountGatewayResult::Unknown;
        }
        let request = match venue_execution::AccountRecoveryRequest::for_durable_commands(
            self.binding.gateway_binding().clone(),
            vec![command.clone()],
        ) {
            Ok(request) => request,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let report = match self.reconcile(&request) {
            Ok(report) => report,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        match report.outcomes().first().map(|outcome| outcome.state()) {
            Some(venue_execution::AccountRecoveryState::Accepted { venue_order_id }) => {
                AccountGatewayResult::Accepted {
                    venue_order_id: venue_order_id.clone(),
                }
            }
            Some(venue_execution::AccountRecoveryState::Rejected { reason }) => {
                AccountGatewayResult::Rejected {
                    reason: reason.clone(),
                }
            }
            _ => AccountGatewayResult::Unknown,
        }
    }

    fn reconcile_committed_with_context(
        &mut self,
        command: &ExecutionCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        if !venue_execution::validate_durable_context(command, context) {
            return AccountGatewayResult::Unknown;
        }
        let ExecutionCommand::Cancel(cancel) = command else {
            return self.reconcile_committed(command);
        };
        let (Some(target), Some(native_order_id)) = (
            context.target_command.as_ref(),
            context.target_native_order_id.as_deref(),
        ) else {
            return AccountGatewayResult::Unknown;
        };
        let rules = match self.registered_rules(&cancel.owner.symbol) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let prepared = match target {
            ExecutionCommand::StopMarketFullPosition(stop) => {
                prepare_price_cancel(&self.binding, &rules, cancel, stop, native_order_id)
            }
            _ => prepare_cancel(
                &self.binding,
                &rules,
                &crate::GateCancelIntent {
                    command: cancel.clone(),
                    venue_order_id: native_order_id.to_owned(),
                },
            ),
        };
        let unknown = match prepared.and_then(|value| mutation_unknown(value, 1)) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let timestamp = match now_ms() {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let readback = match self.runtime.block_on(self.transport.execute_exact_readback(
            &self.binding,
            &self.credentials,
            &rules,
            &unknown.readback,
            timestamp,
        )) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        match settle_exact_readback(&unknown.readback, &readback) {
            Ok(settlement) if recovery_order_matches_command(command, &settlement.order) => {
                AccountGatewayResult::Accepted {
                    venue_order_id: native_order_id.to_owned(),
                }
            }
            _ => AccountGatewayResult::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::verify_key_permissions;

    #[test]
    fn gate_main_key_permissions_require_futures_write_without_withdrawal() {
        let payload = include_str!("../tests/fixtures/gate_account_main_keys.json");
        assert!(verify_key_permissions(payload, "123").is_ok());
    }

    #[test]
    fn gate_permissions_fail_closed_for_withdrawal_or_ambiguous_masks() {
        let withdrawal = r#"{"state":1,"user_id":123,"perms":[{"name":"futures","read_only":false},{"name":"withdrawal","read_only":false}]}"#;
        assert!(verify_key_permissions(withdrawal, "123").is_err());
        let wrong_shape =
            r#"[{"state":1,"user_id":123,"perms":[{"name":"futures","read_only":false}]}]"#;
        assert!(verify_key_permissions(wrong_shape, "123").is_err());
    }
}
