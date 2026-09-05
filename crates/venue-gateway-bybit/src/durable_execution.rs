//! Durable PostgreSQL executor bridge for Bybit.

use super::*;

pub(super) fn validate_stop_position(
    command: &venue_domain::domain::StopMarketFullPositionCommand,
    positions: &BybitPositionReadback,
    symbol: &Symbol,
) -> Result<Price, ()> {
    let matching = positions
        .positions
        .iter()
        .filter(|value| {
            value.position.symbol == *symbol && value.position.side == command.position_side
        })
        .collect::<Vec<_>>();
    let [position] = matching.as_slice() else {
        return Err(());
    };
    command
        .validate_with_authoritative_position(&position.position)
        .map_err(|_| ())?;
    if position.position.quantity.is_zero() || command.quantity > position.position.quantity {
        return Err(());
    }
    position.position.mark_price.ok_or(())
}

impl BybitAccountGateway {
    pub fn durable_order_observation(
        &mut self,
        command: &ExecutionCommand,
    ) -> Result<Option<venue_execution::DurableOrderObservation>, BybitAccountGatewayError> {
        let ExecutionCommand::PlaceLimit(order) = command else {
            return Ok(None);
        };
        if !venue_execution::validate_durable_command(self.binding.gateway_binding(), command) {
            return Ok(None);
        }
        let symbol = order.owner.symbol.clone();
        self.refresh_rules_for(&symbol)?;
        let generation = self
            .symbol_catalog
            .get(&symbol)
            .ok_or(BybitAccountGatewayError::Binding)?
            .rules
            .instrument
            .generation;
        let attempt = self.take_attempt_id()?;
        let lookup =
            BybitOrderLookup::by_client_order_id(order.client_order_id.as_str().to_owned())
                .map_err(|_| BybitAccountGatewayError::Readback)?;
        let now = unix_ms().map_err(BybitAccountGatewayError::Transport)?;
        let scope = self
            .symbol_catalog
            .get(&symbol)
            .ok_or(BybitAccountGatewayError::Binding)?;
        let readback = self.runtime.block_on(fetch_exact_readback(
            &scope.binding,
            &self.credentials,
            &scope.transport,
            generation,
            attempt,
            lookup,
            NativeOrderFamily::UmOrder,
            now,
        ))?;
        if !readback.command_matches(command) {
            return Ok(None);
        }
        let observed = readback
            .open_orders
            .first()
            .map(|v| &v.order)
            .or_else(|| readback.history.first().map(|v| &v.order))
            .ok_or(BybitAccountGatewayError::Readback)?;
        Ok(Some(venue_execution::DurableOrderObservation {
            client_order_id: order.client_order_id.as_str().to_owned(),
            native_order_id: observed.order_id.clone(),
            state: observed.state,
            filled_quantity: observed.filled_quantity,
        }))
    }
    /// Returns a fresh, adapter-normalized public market fact.  The reference is taken from the
    /// signed-generation-fenced REST BBO; callers must never manufacture bounds when a venue
    /// omits one.
    pub fn durable_market_facts(
        &mut self,
    ) -> Result<venue_execution::DurableMarketFacts, BybitAccountGatewayError> {
        let symbol = self.binding.gateway_binding().symbol.clone();
        self.refresh_rules_for(&symbol)?;
        let bbo = self.current_market_bbo_for(&symbol)?;
        let rules = self
            .symbol_catalog
            .get(&symbol)
            .ok_or(BybitAccountGatewayError::Binding)?
            .rules
            .clone();
        let bid = bbo
            .snapshot
            .bids
            .first()
            .ok_or(BybitAccountGatewayError::Instrument)?
            .price
            .value();
        let ask = bbo
            .snapshot
            .asks
            .first()
            .ok_or(BybitAccountGatewayError::Instrument)?
            .price
            .value();
        let reference_price = Price::new((bid + ask) / rust_decimal::Decimal::from(2))
            .map_err(|_| BybitAccountGatewayError::Instrument)?;
        let metadata = venue_domain::domain::InstrumentMetadata::new(
            rules.instrument.clone(),
            venue_domain::domain::Precision::new(
                rules.instrument.price_tick.value(),
                rules.minimum_price.value(),
            )
            .map_err(|_| BybitAccountGatewayError::Instrument)?,
            venue_domain::domain::Precision::new(
                rules.instrument.quantity_step,
                rules.minimum_quantity,
            )
            .map_err(|_| BybitAccountGatewayError::Instrument)?,
            None,
            true,
        )
        .map_err(|_| BybitAccountGatewayError::Instrument)?;
        Ok(venue_execution::DurableMarketFacts {
            binding: self.binding.gateway_binding().clone(),
            metadata,
            reference_price,
            observed_at_ms: bbo.response_time_ms,
            maximum_quantity: Some(rules.maximum_market_quantity),
            maximum_price: Some(rules.maximum_price),
        })
    }

    /// Reads `/v5/user/query-api` through the existing signed transport and returns its
    /// exchange-issued `userID`. A configured label or API-key hash is never substituted.
    pub fn verified_account_identity(&mut self) -> Result<String, BybitAccountGatewayError> {
        let attempt_id = self.take_attempt_id()?;
        let request = prepare_private_request(
            &self.binding,
            self.rules.instrument.generation,
            attempt_id,
            0,
            BybitPrivateSource::ApiKeyInfo,
            None,
            None,
            None,
        )
        .map_err(|_| BybitAccountGatewayError::AccountIdentity)?;
        let raw = self
            .runtime
            .block_on(self.transport.execute_private_read(
                &self.binding,
                &self.credentials,
                &request,
                unix_ms().map_err(BybitAccountGatewayError::Transport)?,
            ))
            .map_err(|_| BybitAccountGatewayError::AccountIdentity)?;
        parse_api_key_evidence(&self.binding, &self.credentials, &raw)
            .map_err(|_| BybitAccountGatewayError::AccountIdentity)?;
        parse_verified_user_id(&raw.payload).ok_or(BybitAccountGatewayError::AccountIdentity)
    }
}

fn parse_verified_user_id(payload: &[u8]) -> Option<String> {
    let root: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let value = root.get("result")?.get("userID")?;
    match value {
        serde_json::Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
        serde_json::Value::Number(value) if value.as_u64().is_some_and(|value| value > 0) => {
            Some(value.to_string())
        }
        _ => None,
    }
}

impl venue_execution::DurableAccountGateway for BybitAccountGateway {
    fn execute_committed_with_context(
        &mut self,
        command: &ExecutionCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        if !venue_execution::validate_durable_context(command, context)
            || !venue_execution::validate_durable_command(self.binding.gateway_binding(), command)
        {
            return rejected("bybit_durable_context");
        }
        match command {
            ExecutionCommand::Cancel(cancel) => self.execute_exact_cancel(cancel, context),
            _ => self.execute_command(command, false),
        }
    }

    fn execute_committed(&mut self, command: &ExecutionCommand) -> AccountGatewayResult {
        if !venue_execution::validate_durable_command(self.binding.gateway_binding(), command) {
            return AccountGatewayResult::Rejected {
                reason: "bybit_durable_scope".to_owned(),
            };
        }
        if matches!(command, ExecutionCommand::Cancel(_)) {
            return rejected("bybit_cancel_context_required");
        }
        self.execute_command(command, false)
    }

    fn reconcile_committed_with_context(
        &mut self,
        command: &ExecutionCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        if !venue_execution::validate_durable_context(command, context)
            || !venue_execution::validate_durable_command(self.binding.gateway_binding(), command)
        {
            return AccountGatewayResult::Unknown;
        }
        match command {
            ExecutionCommand::Cancel(cancel) => self.reconcile_exact_cancel(cancel, context),
            _ => self.reconcile_committed(command),
        }
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
}

impl BybitAccountGateway {
    fn exact_cancel_target<'a>(
        &self,
        context: &'a venue_execution::DurableExecutionContext,
    ) -> Option<(&'a ExecutionCommand, &'a str, NativeOrderFamily)> {
        let target = context.target_command.as_ref()?;
        if !venue_execution::validate_durable_command(self.binding.gateway_binding(), target) {
            return None;
        }
        let native_id = context
            .target_native_order_id
            .as_deref()
            .filter(|value| !value.is_empty())?;
        Some((target, native_id, bybit_command_family(target)))
    }

    fn exact_cancel_readback(
        &mut self,
        context: &venue_execution::DurableExecutionContext,
    ) -> Result<(ExecutionCommand, String, BybitClosedOrderReadback), BybitAccountGatewayError>
    {
        let (target, native_id, family) = self
            .exact_cancel_target(context)
            .ok_or(BybitAccountGatewayError::Readback)?;
        let target = target.clone();
        let native_id = native_id.to_owned();
        let symbol = target.mutation_owner().symbol.clone();
        self.refresh_rules_for(&symbol)?;
        let generation = {
            let scope = self
                .symbol_catalog
                .get(&symbol)
                .ok_or(BybitAccountGatewayError::Binding)?;
            scope.rules.instrument.generation
        };
        let attempt_id = self.take_attempt_id()?;
        let scope = self
            .symbol_catalog
            .get(&symbol)
            .ok_or(BybitAccountGatewayError::Binding)?;
        let readback = self.runtime.block_on(fetch_exact_readback(
            &scope.binding,
            &self.credentials,
            &scope.transport,
            generation,
            attempt_id,
            BybitOrderLookup::by_order_id(native_id.clone())
                .map_err(|_| BybitAccountGatewayError::Readback)?,
            family,
            unix_ms().map_err(BybitAccountGatewayError::Transport)?,
        ))?;
        if !readback.command_matches(&target) {
            return Err(BybitAccountGatewayError::Readback);
        }
        Ok((target, native_id, readback))
    }

    fn execute_exact_cancel(
        &mut self,
        _cancel: &venue_domain::domain::CancelCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        let (target, native_id, readback) = match self.exact_cancel_readback(context) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let settlement = match readback.exact_settlement() {
            Ok(Some(value)) => value,
            _ => return AccountGatewayResult::Unknown,
        };
        if matches!(
            settlement.state,
            OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
        ) {
            return AccountGatewayResult::Accepted {
                venue_order_id: native_id,
            };
        }
        let symbol = target.mutation_owner().symbol.clone();
        let scope = match self.symbol_catalog.get(&symbol) {
            Some(scope) => scope,
            None => return AccountGatewayResult::Unknown,
        };
        let request = match prepare_cancel_request(
            &scope.binding,
            &self.identity,
            &scope.rules,
            &BybitCancelIntent {
                order_id: Some(native_id.clone()),
                client_order_id: None,
            },
        ) {
            Ok(value) => value,
            Err(_) => return rejected("bybit_exact_cancel_intent"),
        };
        let now = match unix_ms() {
            Ok(value) => value,
            Err(_) => return rejected("bybit_clock"),
        };
        match self.runtime.block_on(scope.transport.execute_order(
            &scope.binding,
            &self.credentials,
            &request,
            now,
        )) {
            Ok(ack) if ack.order_id.as_deref() == Some(native_id.as_str()) => {
                AccountGatewayResult::Accepted {
                    venue_order_id: native_id,
                }
            }
            Ok(_) => AccountGatewayResult::Unknown,
            Err(BybitTransportError::Rejected) => rejected("bybit_venue_rejected"),
            Err(_) => AccountGatewayResult::Unknown,
        }
    }

    fn reconcile_exact_cancel(
        &mut self,
        _cancel: &venue_domain::domain::CancelCommand,
        context: &venue_execution::DurableExecutionContext,
    ) -> AccountGatewayResult {
        let (_, native_id, readback) = match self.exact_cancel_readback(context) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        match readback.exact_settlement() {
            Ok(Some(settlement))
                if matches!(
                    settlement.state,
                    OrderState::Filled
                        | OrderState::Cancelled
                        | OrderState::Expired
                        | OrderState::Rejected
                ) =>
            {
                AccountGatewayResult::Accepted {
                    venue_order_id: native_id,
                }
            }
            _ => AccountGatewayResult::Unknown,
        }
    }
}

fn rejected(reason: &str) -> AccountGatewayResult {
    AccountGatewayResult::Rejected {
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_verified_user_id, should_reject_existing_position};

    #[test]
    fn query_api_user_id_is_exchange_identity() {
        let payload = include_bytes!("../fixtures/api-key-info.json");
        assert_eq!(
            parse_verified_user_id(payload),
            Some("123456789".to_owned())
        );
    }

    #[test]
    fn query_api_user_id_fails_closed_when_missing_or_invalid() {
        assert_eq!(
            parse_verified_user_id(br#"{"retCode":0,"result":{}}"#),
            None
        );
        assert_eq!(
            parse_verified_user_id(br#"{"retCode":0,"result":{"userID":0}}"#),
            None
        );
    }

    #[test]
    fn durable_entry_does_not_inherit_canary_empty_account_gate() {
        assert!(should_reject_existing_position(true, false, true));
        assert!(!should_reject_existing_position(false, false, true));
        assert!(!should_reject_existing_position(false, true, true));
    }
}
