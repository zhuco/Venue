//! Durable PostgreSQL executor bridge for Bybit.

use super::*;
use rust_decimal::Decimal;
use std::collections::BTreeSet;

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
        let (client_order_id, symbol, market) = match command {
            ExecutionCommand::PlaceLimit(order) => (
                order.client_order_id.as_str(),
                order.owner.symbol.clone(),
                false,
            ),
            ExecutionCommand::PlaceMarket(order) => (
                order.client_order_id.as_str(),
                order.owner.symbol.clone(),
                true,
            ),
            _ => return Ok(None),
        };
        if !venue_execution::validate_durable_command(self.binding.gateway_binding(), command) {
            return Ok(None);
        }
        self.refresh_rules_for(&symbol)?;
        let generation = self
            .symbol_catalog
            .get(&symbol)
            .ok_or(BybitAccountGatewayError::Binding)?
            .rules
            .instrument
            .generation;
        let attempt = self.take_attempt_id()?;
        let lookup = BybitOrderLookup::by_client_order_id(client_order_id.to_owned())
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
            lookup.clone(),
            NativeOrderFamily::UmOrder,
            now,
        ))?;
        if !readback.command_matches(command) {
            return Ok(None);
        }
        let observed = readback
            .exact_order_evidence()
            .map_err(|_| BybitAccountGatewayError::Readback)?
            .ok_or(BybitAccountGatewayError::Readback)?;
        let (average_price, cumulative_fee) = if market {
            if !terminal_state(observed.order.state) {
                return Ok(None);
            }
            let history_window =
                BybitHistoryWindow::new(now.saturating_sub(HISTORY_WINDOW_MS).max(1), now)
                    .map_err(|_| BybitAccountGatewayError::Readback)?;
            let fills = self.runtime.block_on(fetch_exact_executions(
                &scope.binding,
                &self.credentials,
                &scope.transport,
                generation,
                attempt,
                history_window,
                lookup,
                std::slice::from_ref(&observed),
            ))?;
            aggregate_market_fills(command, &observed, &fills)?
        } else {
            (FieldState::Missing, FieldState::Missing)
        };
        Ok(Some(venue_execution::DurableOrderObservation {
            client_order_id: client_order_id.to_owned(),
            native_order_id: observed.order.order_id.clone(),
            state: observed.order.state,
            filled_quantity: observed.order.filled_quantity,
            average_price,
            cumulative_fee,
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

#[allow(clippy::too_many_arguments)]
async fn fetch_exact_executions(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
    history_window: BybitHistoryWindow,
    lookup: BybitOrderLookup,
    evidence: &[crate::BybitOrderEvidence],
) -> Result<crate::BybitFillReadback, BybitAccountGatewayError> {
    let mut pages = Vec::new();
    let mut cursor = None;
    for page_index in 0..EXACT_READBACK_MAX_PAGES {
        let request = prepare_private_request(
            binding,
            generation,
            attempt_id,
            page_index,
            BybitPrivateSource::Executions,
            cursor.as_deref(),
            Some(history_window.clone()),
            Some(lookup.clone()),
        )
        .map_err(|_| BybitAccountGatewayError::Readback)?;
        let raw = execute_private(binding, credentials, transport, &request)
            .await
            .map_err(BybitAccountGatewayError::OrderTransport)?;
        let page = crate::parse_execution_page(binding, &raw, evidence)
            .map_err(|_| BybitAccountGatewayError::Readback)?;
        cursor = page.meta.next_cursor.clone();
        pages.push(page);
        if cursor.is_none() {
            return crate::complete_execution_pages(binding, &pages, evidence)
                .map_err(|_| BybitAccountGatewayError::Readback);
        }
    }
    Err(BybitAccountGatewayError::Readback)
}

fn aggregate_market_fills(
    command: &ExecutionCommand,
    observed: &crate::BybitOrderEvidence,
    fills: &crate::BybitFillReadback,
) -> Result<(FieldState<Price>, FieldState<Amount>), BybitAccountGatewayError> {
    let ExecutionCommand::PlaceMarket(order) = command else {
        return Err(BybitAccountGatewayError::Readback);
    };
    if fills.binding.symbol != order.owner.symbol
        || fills.generation == 0
        || fills.attempt_id == 0
        || !terminal_state(observed.order.state)
    {
        return Err(BybitAccountGatewayError::Readback);
    }
    let mut fill_ids = BTreeSet::new();
    let mut quantity = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut fee: Option<Amount> = None;
    for item in &fills.fills {
        if !fill_ids.insert(item.fill.fill_id.as_str())
            || item.fill.order_id != observed.order.order_id
            || item.client_order_id != FieldState::Known(order.client_order_id.as_str().to_owned())
        {
            return Err(BybitAccountGatewayError::Readback);
        }
        quantity = quantity
            .checked_add(item.fill.quantity)
            .ok_or(BybitAccountGatewayError::Readback)?;
        notional = notional
            .checked_add(
                item.fill
                    .quantity
                    .checked_mul(item.fill.price.value())
                    .ok_or(BybitAccountGatewayError::Readback)?,
            )
            .ok_or(BybitAccountGatewayError::Readback)?;
        let FieldState::Known(item_fee) = &item.fill.fee else {
            return Err(BybitAccountGatewayError::Readback);
        };
        match &mut fee {
            Some(total) if total.asset == item_fee.asset => {
                total.value = total
                    .value
                    .checked_add(item_fee.value)
                    .ok_or(BybitAccountGatewayError::Readback)?;
            }
            None => fee = Some(item_fee.clone()),
            Some(_) => return Err(BybitAccountGatewayError::Readback),
        }
    }
    if quantity != observed.order.filled_quantity {
        return Err(BybitAccountGatewayError::Readback);
    }
    if quantity.is_zero() {
        if !fills.fills.is_empty() || fee.is_some() || !notional.is_zero() {
            return Err(BybitAccountGatewayError::Readback);
        }
        return Ok((FieldState::Missing, FieldState::Missing));
    }
    let average = notional
        .checked_div(quantity)
        .and_then(|value| Price::new(value).ok())
        .ok_or(BybitAccountGatewayError::Readback)?;
    let fee = fee.ok_or(BybitAccountGatewayError::Readback)?;
    Ok((FieldState::Known(average), FieldState::Known(fee)))
}

const fn terminal_state(state: OrderState) -> bool {
    matches!(
        state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    )
}

#[cfg(test)]
mod market_observation_tests {
    use super::*;
    use venue_domain::domain::{CommandId, MarketOrderCommand, Order, OrderOwner, OrderPurpose};
    use venue_gateway_api::{GatewayMode, VenueId};

    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000001";

    fn market_command() -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::PlaceMarket(MarketOrderCommand {
            command_id: CommandId::new("bybit-market-command")?,
            client_order_id: CommandId::new("bybit-market-client")?,
            owner: OrderOwner {
                strategy_instance_id: "acceptance".into(),
                run_id: "run-1".into(),
                exchange: "bybit".into(),
                account: ACCOUNT_ID.into(),
                symbol: "DOGE/USDT".parse()?,
                purpose: OrderPurpose::Entry,
            },
            position_side: PositionSide::Short,
            side: OrderSide::Sell,
            quantity: Decimal::from(50),
            reduce_only: false,
        }))
    }

    fn observed_market() -> Result<crate::BybitOrderEvidence, Box<dyn std::error::Error>> {
        Ok(crate::BybitOrderEvidence {
            order: Order {
                order_id: "native-market-1".into(),
                client_order_id: FieldState::Known("bybit-market-client".into()),
                symbol: "DOGE/USDT".parse()?,
                side: OrderSide::Sell,
                position_side: FieldState::Known(PositionSide::Short),
                purpose: FieldState::Known(OrderPurpose::Entry),
                state: OrderState::Filled,
                quantity: Decimal::from(50),
                filled_quantity: Decimal::from(50),
                limit_price: None,
                time_in_force: FieldState::NotApplicable,
                average_price: FieldState::Known(Price::new(Decimal::new(106, 3))?),
                reduce_only: false,
            },
            family: NativeOrderFamily::UmOrder,
            native_order_type: "Market".into(),
            native_time_in_force: "IOC".into(),
            position_idx: 2,
            stop_order_type: None,
            trigger_price: None,
            trigger_direction: None,
            trigger_by: None,
            close_on_trigger: false,
            created_at_ms: 1_000,
            updated_at_ms: 1_100,
        })
    }

    fn fill(
        id: &str,
        quantity: Decimal,
        price: Decimal,
        fee_asset: &str,
        fee: Decimal,
    ) -> Result<crate::BybitFill, Box<dyn std::error::Error>> {
        Ok(crate::BybitFill {
            fill: Fill {
                fill_id: id.into(),
                execution_sequence: FieldState::Known(if id == "fill-1" { 2 } else { 1 }),
                order_id: "native-market-1".into(),
                symbol: "DOGE/USDT".parse()?,
                side: OrderSide::Sell,
                position_side: FieldState::Known(PositionSide::Short),
                quantity,
                price: Price::new(price)?,
                fee: FieldState::Known(Amount::new(Asset::new(fee_asset)?, fee)),
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(false),
                exchange_time_ms: Some(if id == "fill-1" { 1_100 } else { 1_090 }),
            },
            client_order_id: FieldState::Known("bybit-market-client".into()),
            closed_size: Decimal::ZERO,
            native_order_sequence: if id == "fill-1" { 2 } else { 1 },
        })
    }

    fn fill_readback() -> Result<crate::BybitFillReadback, Box<dyn std::error::Error>> {
        Ok(crate::BybitFillReadback {
            raw_pages: vec![],
            binding: GatewayBinding::new(
                VenueId::Bybit,
                GatewayMode::Live,
                ACCOUNT_ID,
                "DOGE/USDT".parse()?,
            )?,
            generation: 7,
            attempt_id: 11,
            observed_at_ms: 1_200,
            fills: vec![
                fill(
                    "fill-1",
                    Decimal::from(20),
                    Decimal::new(10, 2),
                    "USDT",
                    Decimal::new(1, 3),
                )?,
                fill(
                    "fill-2",
                    Decimal::from(30),
                    Decimal::new(11, 2),
                    "USDT",
                    Decimal::new(2, 3),
                )?,
            ],
        })
    }

    #[test]
    fn market_observation_aggregates_exact_fills_average_and_fee_asset()
    -> Result<(), Box<dyn std::error::Error>> {
        let (average, fee) =
            aggregate_market_fills(&market_command()?, &observed_market()?, &fill_readback()?)?;
        assert_eq!(
            average,
            FieldState::Known(Price::new(Decimal::new(106, 3))?)
        );
        assert_eq!(
            fee,
            FieldState::Known(Amount::new(Asset::new("USDT")?, Decimal::new(3, 3)))
        );
        Ok(())
    }

    #[test]
    fn market_observation_rejects_duplicate_fills_quantity_gaps_and_mixed_fee_assets()
    -> Result<(), Box<dyn std::error::Error>> {
        let command = market_command()?;
        let observed = observed_market()?;
        let mut duplicate = fill_readback()?;
        duplicate.fills[1].fill.fill_id = "fill-1".into();
        assert!(aggregate_market_fills(&command, &observed, &duplicate).is_err());

        let mut incomplete = fill_readback()?;
        incomplete.fills.pop();
        assert!(aggregate_market_fills(&command, &observed, &incomplete).is_err());

        let mut mixed_fee = fill_readback()?;
        mixed_fee.fills[1].fill.fee =
            FieldState::Known(Amount::new(Asset::new("USDC")?, Decimal::new(2, 3)));
        assert!(aggregate_market_fills(&command, &observed, &mixed_fee).is_err());
        Ok(())
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
