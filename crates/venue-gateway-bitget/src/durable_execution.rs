//! Durable PostgreSQL executor bridge for Bitget.

use super::*;

impl BitgetAccountGateway {
    pub fn durable_order_observation(
        &mut self,
        command: &ExecutionCommand,
    ) -> Result<Option<venue_execution::DurableOrderObservation>, BitgetAccountGatewayError> {
        let ExecutionCommand::PlaceLimit(order) = command else {
            return Ok(None);
        };
        if !venue_execution::validate_durable_command(self.transport_binding(), command) {
            return Ok(None);
        }
        let symbol = order.owner.symbol.clone();
        self.refresh_rules_for_symbols(std::iter::once(symbol.clone()))?;
        let rules = self.registered_rules(&symbol)?;
        let unknown = crate::BitgetUnknownMutation {
            binding: self.binding_for(&symbol),
            attempt_id: self.next_attempt_id()?,
            generation: rules.snapshot.metadata.instrument.generation,
            kind: BitgetMutationKind::Place,
            order_id: None,
            client_order_id: Some(order.client_order_id.as_str().to_owned()),
            dispatched_at_ms: 1,
            reason: crate::BitgetUnknownReason::AmbiguousResponse,
            expected_time_in_force: Some(match order.time_in_force {
                LimitTimeInForce::PostOnly => crate::BitgetTimeInForce::PostOnly,
                LimitTimeInForce::Gtc => crate::BitgetTimeInForce::GoodTillCancelled,
            }),
            expected_strategy: None,
        };
        let request = build_unknown_recovery_readback_request(
            &unknown,
            rules.snapshot.metadata.instrument.generation,
        )
        .map_err(|_| BitgetAccountGatewayError::Readback)?;
        let readback = self
            .runtime
            .block_on(self.transport.execute_exact_readback(
                &self.credentials,
                request,
                now_ms()?,
            ))?;
        let Some(observed) = readback.order else {
            return Ok(None);
        };
        if !command_matches_readback_order(command, &observed) {
            return Ok(None);
        }
        Ok(Some(venue_execution::DurableOrderObservation {
            client_order_id: order.client_order_id.as_str().to_owned(),
            native_order_id: observed.order_id,
            state: observed.state,
            filled_quantity: observed.filled_quantity,
        }))
    }

    pub fn durable_market_facts(
        &mut self,
    ) -> Result<venue_execution::DurableMarketFacts, BitgetAccountGatewayError> {
        let symbol = self.transport_binding().symbol.clone();
        self.refresh_rules_for_symbols(std::iter::once(symbol.clone()))?;
        let rules = self.registered_rules(&symbol)?;
        let binding = self.binding_for(&symbol);
        let response = self
            .runtime
            .block_on(self.transport.fetch_ticker_for(&binding))?;
        let received_at_ms = response.received_at_ms;
        let payload =
            String::from_utf8(response.payload).map_err(|_| BitgetAccountGatewayError::Readback)?;
        let ticker = parse_rest_ticker(
            BitgetRawPublicPayload::new(
                BitgetPublicSource::RestTicker,
                symbol,
                rules.snapshot.metadata.instrument.generation,
                received_at_ms,
                payload,
            )
            .map_err(|_| BitgetAccountGatewayError::Readback)?,
        )
        .map_err(|_| BitgetAccountGatewayError::Readback)?;
        let reference_price = Price::new(
            ticker
                .bbo
                .bid_price
                .value()
                .checked_add(ticker.bbo.ask_price.value())
                .and_then(|value| value.checked_div(Decimal::from(2)))
                .ok_or(BitgetAccountGatewayError::Readback)?,
        )
        .map_err(|_| BitgetAccountGatewayError::Readback)?;
        Ok(venue_execution::DurableMarketFacts {
            binding: self.transport_binding().clone(),
            metadata: rules.snapshot.metadata.clone(),
            reference_price,
            observed_at_ms: received_at_ms,
            maximum_quantity: rules.maximum_market_order_quantity,
            maximum_price: None,
        })
    }

    /// The UID comes only from Bitget's signed account-info response.  A configured account
    /// label or credential hash is never treated as the exchange identity.
    pub fn verified_account_identity(&mut self) -> Result<String, BitgetAccountGatewayError> {
        let timestamp_ms = now_ms()?;
        let payload = self.runtime.block_on(
            self.transport
                .fetch_account_info(&self.credentials, timestamp_ms),
        )?;
        let info = parse_account_info(&payload).ok_or(BitgetAccountGatewayError::Readback)?;
        Ok(info.user_id)
    }

    /// Rechecks the key permissions immediately before a committed mutation. Recovery only
    /// needs the signed order identity and deliberately does not call this gate.
    pub fn verify_strategy_permissions(&mut self) -> Result<(), BitgetAccountGatewayError> {
        let timestamp_ms = now_ms()?;
        let payload = self.runtime.block_on(
            self.transport
                .fetch_account_info(&self.credentials, timestamp_ms),
        )?;
        let info = parse_account_info(&payload).ok_or(BitgetAccountGatewayError::Readback)?;
        validate_strategy_permissions(&info)
    }
}

struct BitgetAccountInfo {
    user_id: String,
    perm_type: String,
    permissions: Vec<String>,
}

fn parse_account_info(payload: &[u8]) -> Option<BitgetAccountInfo> {
    let root: Value = serde_json::from_slice(payload).ok()?;
    if root.get("code").and_then(Value::as_str) != Some("00000") {
        return None;
    }
    let data = root.get("data")?.as_object()?;
    let user_id = data.get("userId")?.as_str()?.trim();
    let perm_type = data.get("permType")?.as_str()?.trim();
    let permissions = data
        .get("permissions")?
        .as_array()?
        .iter()
        .map(|value| value.as_str().map(str::to_owned))
        .collect::<Option<Vec<_>>>()?;
    if user_id.is_empty()
        || perm_type.is_empty()
        || permissions.iter().any(|value| value.is_empty())
    {
        return None;
    }
    Some(BitgetAccountInfo {
        user_id: user_id.to_owned(),
        perm_type: perm_type.to_owned(),
        permissions,
    })
}

fn validate_strategy_permissions(
    info: &BitgetAccountInfo,
) -> Result<(), BitgetAccountGatewayError> {
    if !matches!(
        info.perm_type.as_str(),
        "read-and-write" | "read_and_write" | "read_write"
    ) || !info.permissions.iter().any(|value| value == "uta_trade")
        || info.permissions.iter().any(|value| value == "withdraw")
    {
        return Err(BitgetAccountGatewayError::Readback);
    }
    Ok(())
}

impl BitgetAccountGateway {
    fn execute_command(
        &mut self,
        command: &ExecutionCommand,
        require_anchor_symbol: bool,
        context: Option<&venue_execution::DurableExecutionContext>,
    ) -> AccountGatewayResult {
        if command.validate().is_err() {
            return rejected("bitget_preflight_failed");
        }
        let owner = command.mutation_owner();
        if owner.exchange != self.transport_binding().venue.as_str()
            || owner.account != self.transport_binding().trading_account_id
            || (require_anchor_symbol && owner.symbol != self.transport_binding().symbol)
            || !self.rules_catalog.contains_key(&owner.symbol)
        {
            return rejected("bitget_preflight_failed");
        }
        let symbol = owner.symbol.clone();
        if self.refresh_private_for(&symbol).is_err() {
            return rejected("bitget_preflight_failed");
        }
        let rules = match self.registered_rules(&symbol) {
            Ok(value) => value,
            Err(_) => return rejected("bitget_symbol_unconfigured"),
        };
        let attempt = match self.next_attempt_id() {
            Ok(value) => value,
            Err(_) => return rejected("bitget_attempt_exhausted"),
        };
        let now = match now_ms() {
            Ok(value) => value,
            Err(_) => return rejected("bitget_clock"),
        };
        let prepared = match (command, context) {
            (ExecutionCommand::Cancel(_cancel), Some(context)) => {
                let Some(target) = context.target_command.as_ref() else {
                    return rejected("bitget_cancel_target_unproven");
                };
                let Some(native_order_id) = context.target_native_order_id.as_deref() else {
                    return rejected("bitget_cancel_target_unproven");
                };
                match target {
                    ExecutionCommand::StopMarketFullPosition(target) => {
                        crate::prepare_strategy_cancel_request(
                            self.transport_binding(),
                            &self.config,
                            rules.snapshot.metadata.instrument.generation,
                            attempt,
                            target,
                            native_order_id,
                        )
                    }
                    _ => crate::prepare_cancel_request(
                        self.transport_binding(),
                        &self.config,
                        rules.snapshot.metadata.instrument.generation,
                        attempt,
                        &crate::BitgetCancelIntent {
                            order_id: Some(native_order_id.to_owned()),
                            client_order_id: None,
                        },
                    ),
                }
                .map_err(|_| ())
            }
            _ => prepare_node_mutation(&self.private, &rules, &self.config, command, attempt, now)
                .map(|value| value.into_mutation())
                .map_err(|_| ()),
        };
        let prepared = match prepared {
            Ok(value) => value,
            Err(()) => return rejected("bitget_intent_rejected"),
        };
        match self.runtime.block_on(self.transport.execute_mutation_once(
            &self.credentials,
            prepared,
            now,
        )) {
            Ok(BitgetMutationOutcome::Acknowledged(ack)) => {
                let request = match build_ack_readback_request(&ack) {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let readback = match now_ms().ok().and_then(|timestamp| {
                    self.runtime
                        .block_on(self.transport.execute_exact_readback(
                            &self.credentials,
                            request,
                            timestamp,
                        ))
                        .ok()
                }) {
                    Some(value) => value,
                    None => return AccountGatewayResult::Unknown,
                };
                match settle_ack_readback(&ack, &readback) {
                    Ok(settlement) => match settlement.order {
                        Some(order) => AccountGatewayResult::Accepted {
                            venue_order_id: order.order_id,
                        },
                        None => AccountGatewayResult::Unknown,
                    },
                    Err(_) => AccountGatewayResult::Unknown,
                }
            }
            Ok(BitgetMutationOutcome::Rejected) => rejected("bitget_venue_rejected"),
            Ok(BitgetMutationOutcome::Unknown(_)) | Err(_) => AccountGatewayResult::Unknown,
        }
    }

    pub(super) fn dispatch_permit(
        &mut self,
        permit: AccountDispatchPermit,
    ) -> AccountGatewayResult {
        if permit.binding() != self.transport_binding() {
            return rejected("bitget_preflight_failed");
        }
        self.execute_command(permit.command(), false, None)
    }
}

impl venue_execution::DurableAccountGateway for BitgetAccountGateway {
    fn execute_committed_with_context(
        &mut self,
        command: &ExecutionCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        if !venue_execution::validate_durable_context(command, context) {
            return rejected("bitget_durable_context");
        }
        if !venue_execution::validate_durable_command(self.transport_binding(), command) {
            return rejected("bitget_durable_scope");
        }
        if self.verify_strategy_permissions().is_err() {
            return AccountGatewayResult::Unknown;
        }
        self.execute_command(command, true, Some(context))
    }

    fn execute_committed(&mut self, command: &ExecutionCommand) -> AccountGatewayResult {
        if !venue_execution::validate_durable_command(self.transport_binding(), command) {
            return rejected("bitget_durable_scope");
        }
        if self.verify_strategy_permissions().is_err() {
            return AccountGatewayResult::Unknown;
        }
        if matches!(command, ExecutionCommand::Cancel(_)) {
            return rejected("bitget_durable_context");
        }
        self.execute_command(command, true, None)
    }

    fn reconcile_committed(&mut self, command: &ExecutionCommand) -> AccountGatewayResult {
        if matches!(command, ExecutionCommand::Cancel(_)) {
            return AccountGatewayResult::Unknown;
        }
        self.reconcile_without_context(command)
    }

    fn reconcile_committed_with_context(
        &mut self,
        command: &ExecutionCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        if !venue_execution::validate_durable_context(command, context) {
            return AccountGatewayResult::Unknown;
        }
        let ExecutionCommand::Cancel(_) = command else {
            return self.reconcile_without_context(command);
        };
        self.reconcile_cancel_with_context(command, context)
    }
}

impl BitgetAccountGateway {
    fn reconcile_without_context(&mut self, command: &ExecutionCommand) -> AccountGatewayResult {
        let binding = self.transport_binding().clone();
        let request = match venue_execution::AccountRecoveryRequest::for_durable_commands(
            binding,
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

    fn reconcile_cancel_with_context(
        &mut self,
        command: &ExecutionCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        let (Some(target), Some(native_order_id)) = (
            context.target_command.as_ref(),
            context.target_native_order_id.as_ref(),
        ) else {
            return AccountGatewayResult::Unknown;
        };
        let expected_strategy = match target {
            ExecutionCommand::StopMarketFullPosition(target) => {
                Some(crate::execution::BitgetExpectedStrategy {
                    client_order_id: target.client_algo_id.as_str().to_owned(),
                    order_id: Some(native_order_id.clone()),
                    symbol: target.owner.symbol.clone(),
                    side: target.side,
                    position_side: target.position_side,
                    quantity: target.quantity,
                    trigger_price: target.trigger_price,
                    take_profit: target.owner.purpose
                        == venue_domain::domain::OrderPurpose::TakeProfit,
                })
            }
            _ => None,
        };
        let unknown = crate::BitgetUnknownMutation {
            binding: self.transport_binding().clone(),
            attempt_id: match self.next_attempt_id() {
                Ok(value) => value,
                Err(_) => return AccountGatewayResult::Unknown,
            },
            generation: self.transport.generation(),
            kind: if expected_strategy.is_some() {
                BitgetMutationKind::CancelStrategy
            } else {
                BitgetMutationKind::Cancel
            },
            order_id: Some(native_order_id.clone()),
            client_order_id: target
                .native_client_id()
                .map(|value| value.as_str().to_owned()),
            dispatched_at_ms: 1,
            reason: crate::BitgetUnknownReason::AmbiguousResponse,
            expected_time_in_force: None,
            expected_strategy,
        };
        let request =
            match build_unknown_recovery_readback_request(&unknown, self.transport.generation()) {
                Ok(value) => value,
                Err(_) => return AccountGatewayResult::Unknown,
            };
        let readback = match now_ms().ok().and_then(|timestamp| {
            self.runtime
                .block_on(self.transport.execute_exact_readback(
                    &self.credentials,
                    request,
                    timestamp,
                ))
                .ok()
        }) {
            Some(value) => value,
            None => return AccountGatewayResult::Unknown,
        };
        match settle_unknown_readback(&unknown, &readback) {
            Ok(settlement)
                if settlement
                    .order
                    .as_ref()
                    .is_some_and(|order| command_matches_readback_order(command, order)) =>
            {
                AccountGatewayResult::Accepted {
                    venue_order_id: native_order_id.clone(),
                }
            }
            _ => AccountGatewayResult::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_account_info, validate_strategy_permissions};

    #[test]
    fn account_uid_is_read_from_signed_response_shape() {
        let payload = include_bytes!("../tests/fixtures/bitget_account_info.json");
        let info = parse_account_info(payload).expect("fixture shape");
        assert_eq!(info.user_id, "123456789");
        assert!(validate_strategy_permissions(&info).is_ok());
    }

    #[test]
    fn account_uid_fails_closed_when_missing_or_empty() {
        assert!(parse_account_info(br#"{"code":"00000","data":{}}"#).is_none());
        assert!(parse_account_info(br#"{"code":"00000","data":{"userId":""}}"#).is_none());
        assert!(parse_account_info(br#"{"code":"43001","data":{"userId":"x"}}"#).is_none());
    }

    #[test]
    fn account_permissions_fail_closed_for_read_only_or_withdrawal() {
        let read_only = br#"{"code":"00000","data":{"userId":"1","permType":"read-only","permissions":["uta_trade"]}}"#;
        let withdrawal = br#"{"code":"00000","data":{"userId":"1","permType":"read-and-write","permissions":["uta_trade","withdraw"]}}"#;
        assert!(
            parse_account_info(read_only)
                .and_then(|info| validate_strategy_permissions(&info).ok())
                .is_none()
        );
        assert!(
            parse_account_info(withdrawal)
                .and_then(|info| validate_strategy_permissions(&info).ok())
                .is_none()
        );
    }

    #[test]
    fn account_permissions_accept_closed_read_write_spellings_seen_on_uta() {
        for spelling in ["read-and-write", "read_and_write", "read_write"] {
            let payload = format!(
                r#"{{"code":"00000","data":{{"userId":"1","permType":"{spelling}","permissions":["uta_trade"]}}}}"#
            );
            let accepted = parse_account_info(payload.as_bytes())
                .and_then(|info| validate_strategy_permissions(&info).ok());
            assert!(accepted.is_some(), "spelling={spelling}");
        }
    }
}
