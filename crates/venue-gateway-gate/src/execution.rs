use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use venue_domain::domain::{
    CancelCommand, FieldState, MarketReduceCommand, Order, OrderCommand, OrderSide, OrderState,
    PositionSide, Price,
};
use venue_gateway_api::GatewayBinding;

use crate::{
    GateContractRules, GateCredentials, GateGatewayBinding, GateOrderPayloadError,
    GateProtocolError, GateRestSignedHeaders, endpoints, parse_regular_order, sign_rest,
};

const MAX_CLIENT_ORDER_SUFFIX_BYTES: usize = 28;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateMutationKind {
    PlacePostOnly,
    Cancel,
    ReduceOnce,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExpectedOrder {
    order_id: Option<String>,
    client_order_id: String,
    side: Option<OrderSide>,
    position_side: Option<PositionSide>,
    quantity: Option<Decimal>,
    limit_price: Option<Price>,
    reduce_only: Option<bool>,
}

/// A prepared request is intentionally not `Clone`. The async transport consumes it and returns
/// either an ACK-bound readback or an UNKNOWN-bound readback, so it cannot implement a retry loop.
#[derive(Debug, Eq, PartialEq)]
pub struct GatePreparedMutation {
    binding: GatewayBinding,
    generation: u64,
    origin: &'static str,
    method: &'static str,
    endpoint: String,
    body: Vec<u8>,
    kind: GateMutationKind,
    expected: ExpectedOrder,
    reduce_episode: Option<(String, u64)>,
}

impl GatePreparedMutation {
    #[must_use]
    pub const fn kind(&self) -> GateMutationKind {
        self.kind
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn reduce_episode_id(&self) -> Option<&str> {
        self.reduce_episode
            .as_ref()
            .map(|(episode, _)| episode.as_str())
    }

    #[must_use]
    pub fn position_generation(&self) -> Option<u64> {
        self.reduce_episode
            .as_ref()
            .map(|(_, generation)| *generation)
    }

    pub(crate) fn validate(
        &self,
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
    ) -> Result<(), GateExecutionError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| GateExecutionError::Binding)?;
        validate_scope(binding, rules, self.generation)?;
        if self.origin != binding.config().rest_origin()
            || !matches!(self.method, "POST" | "DELETE")
            || self.body.is_empty() && self.method == "POST"
            || !self.body.is_empty() && self.method == "DELETE"
            || self.endpoint.is_empty()
        {
            return Err(GateExecutionError::Binding);
        }
        Ok(())
    }

    pub(crate) fn sign(
        &self,
        credentials: &GateCredentials,
        timestamp_sec: i64,
    ) -> Result<GateRestSignedHeaders, GateExecutionError> {
        sign_rest(
            credentials,
            timestamp_sec,
            self.method,
            &self.endpoint,
            "",
            &self.body,
        )
        .map_err(|_| GateExecutionError::Signing)
    }
}

pub fn prepare_limit_post_only(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    command: &OrderCommand,
) -> Result<GatePreparedMutation, GateExecutionError> {
    command.validate().map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &command.owner.exchange,
        &command.owner.account,
    )?;
    validate_step(
        command.limit_price.value(),
        rules.instrument.price_tick.value(),
    )?;
    let contracts = rules
        .native_contracts_checked(command.quantity)
        .map_err(|_| GateExecutionError::Rules)?;
    let signed_contracts = signed_contracts(contracts, command.side);
    let notional = command
        .quantity
        .checked_mul(command.limit_price.value())
        .ok_or(GateExecutionError::Rules)?;
    if notional < rules.instrument.minimum_notional.value {
        return Err(GateExecutionError::Rules);
    }
    let client_order_id = command.client_order_id.as_str().to_owned();
    let body = PlaceBody {
        contract: &rules.native_symbol,
        size: decimal_wire(signed_contracts),
        price: decimal_wire(command.limit_price.value()),
        tif: "poc",
        reduce_only: command.reduce_only,
        text: native_client_id(&client_order_id)?,
    };
    prepared_place(
        binding,
        rules,
        GateMutationKind::PlacePostOnly,
        body,
        ExpectedOrder {
            order_id: None,
            client_order_id,
            side: Some(command.side),
            position_side: Some(command.position_side),
            quantity: Some(command.quantity),
            limit_price: Some(command.limit_price),
            reduce_only: Some(command.reduce_only),
        },
        None,
    )
}

pub fn prepare_reduce_once(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    command: &MarketReduceCommand,
) -> Result<GatePreparedMutation, GateExecutionError> {
    command.validate().map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &command.owner.exchange,
        &command.owner.account,
    )?;
    let contracts = rules
        .native_contracts_checked(command.quantity)
        .map_err(|_| GateExecutionError::Rules)?;
    let client_order_id = command.client_order_id.as_str().to_owned();
    let body = PlaceBody {
        contract: &rules.native_symbol,
        size: decimal_wire(signed_contracts(contracts, command.side)),
        price: "0".to_owned(),
        tif: "ioc",
        reduce_only: true,
        text: native_client_id(&client_order_id)?,
    };
    prepared_place(
        binding,
        rules,
        GateMutationKind::ReduceOnce,
        body,
        ExpectedOrder {
            order_id: None,
            client_order_id,
            side: Some(command.side),
            position_side: Some(command.position_side),
            quantity: Some(command.quantity),
            limit_price: None,
            reduce_only: Some(true),
        },
        Some((
            command.risk_episode_id.as_str().to_owned(),
            command.position_generation,
        )),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateCancelIntent {
    pub command: CancelCommand,
    pub venue_order_id: String,
}

pub fn prepare_cancel(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    intent: &GateCancelIntent,
) -> Result<GatePreparedMutation, GateExecutionError> {
    intent
        .command
        .validate()
        .map_err(|_| GateExecutionError::Intent)?;
    validate_owner(
        binding,
        rules,
        &intent.command.owner.exchange,
        &intent.command.owner.account,
    )?;
    if !valid_native_order_id(&intent.venue_order_id) {
        return Err(GateExecutionError::Intent);
    }
    let client_order_id = intent.command.target_client_order_id.as_str().to_owned();
    native_client_id(&client_order_id)?;
    Ok(GatePreparedMutation {
        binding: binding.gateway_binding().clone(),
        generation: rules.instrument.generation,
        origin: binding.config().rest_origin(),
        method: "DELETE",
        endpoint: format!("{}/{}", endpoints::FUTURES_ORDER, intent.venue_order_id),
        body: Vec::new(),
        kind: GateMutationKind::Cancel,
        expected: ExpectedOrder {
            order_id: Some(intent.venue_order_id.clone()),
            client_order_id,
            side: None,
            position_side: None,
            quantity: None,
            limit_price: None,
            reduce_only: None,
        },
        reduce_episode: None,
    })
}

fn prepared_place(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    kind: GateMutationKind,
    body: PlaceBody<'_>,
    expected: ExpectedOrder,
    reduce_episode: Option<(String, u64)>,
) -> Result<GatePreparedMutation, GateExecutionError> {
    Ok(GatePreparedMutation {
        binding: binding.gateway_binding().clone(),
        generation: rules.instrument.generation,
        origin: binding.config().rest_origin(),
        method: "POST",
        endpoint: endpoints::FUTURES_ORDER.to_owned(),
        body: serde_json::to_vec(&body).map_err(|_| GateExecutionError::Payload)?,
        kind,
        expected,
        reduce_episode,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateExactReadbackRequest {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub endpoint: String,
    pub not_before_ms: u64,
    pub mutation_kind: GateMutationKind,
    expected: ExpectedOrder,
    ack_order: Option<Order>,
}

impl GateExactReadbackRequest {
    pub(crate) fn validate(
        &self,
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
    ) -> Result<(), GateExecutionError> {
        binding
            .validate_request_binding(&self.binding)
            .map_err(|_| GateExecutionError::Binding)?;
        validate_scope(binding, rules, self.generation)?;
        if self.not_before_ms == 0 || !self.endpoint.starts_with(endpoints::FUTURES_ORDER) {
            return Err(GateExecutionError::Binding);
        }
        Ok(())
    }

    pub(crate) fn sign(
        &self,
        credentials: &GateCredentials,
        timestamp_sec: i64,
    ) -> Result<GateRestSignedHeaders, GateExecutionError> {
        sign_rest(credentials, timestamp_sec, "GET", &self.endpoint, "", &[])
            .map_err(|_| GateExecutionError::Signing)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateAcceptedMutation {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub kind: GateMutationKind,
    pub accepted_at_ms: u64,
    pub order: Order,
    pub readback: GateExactReadbackRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateDispatchUnknown {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub kind: GateMutationKind,
    pub unknown_at_ms: u64,
    pub readback: GateExactReadbackRequest,
}

pub(crate) fn parse_mutation_ack(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    request: GatePreparedMutation,
    payload: &[u8],
    received_at_ms: u64,
) -> Result<GateAcceptedMutation, GateExecutionError> {
    request.validate(binding, rules)?;
    if received_at_ms == 0 {
        return Err(GateExecutionError::Payload);
    }
    let value: Value = serde_json::from_slice(payload).map_err(|_| GateExecutionError::Payload)?;
    let order = parse_regular_order(&value, &rules.instrument.symbol, rules)
        .map_err(GateExecutionError::Order)?;
    if !matches_expected(&order, &request.expected) {
        return Err(GateExecutionError::Binding);
    }
    let mut expected = request.expected;
    expected.order_id = Some(order.order_id.clone());
    expected.side = Some(order.side);
    expected.position_side = known_position_side(&order);
    expected.quantity = Some(order.quantity);
    expected.limit_price = order.limit_price;
    expected.reduce_only = Some(order.reduce_only);
    let readback = exact_readback(
        &request.binding,
        request.generation,
        request.kind,
        expected,
        Some(order.clone()),
        received_at_ms,
    )?;
    Ok(GateAcceptedMutation {
        binding: request.binding,
        generation: request.generation,
        kind: request.kind,
        accepted_at_ms: received_at_ms,
        order,
        readback,
    })
}

pub(crate) fn mutation_unknown(
    request: GatePreparedMutation,
    unknown_at_ms: u64,
) -> Result<GateDispatchUnknown, GateExecutionError> {
    let readback = exact_readback(
        &request.binding,
        request.generation,
        request.kind,
        request.expected,
        None,
        unknown_at_ms,
    )?;
    Ok(GateDispatchUnknown {
        binding: request.binding,
        generation: request.generation,
        kind: request.kind,
        unknown_at_ms,
        readback,
    })
}

fn exact_readback(
    binding: &GatewayBinding,
    generation: u64,
    mutation_kind: GateMutationKind,
    expected: ExpectedOrder,
    ack_order: Option<Order>,
    not_before_ms: u64,
) -> Result<GateExactReadbackRequest, GateExecutionError> {
    if not_before_ms == 0 {
        return Err(GateExecutionError::Payload);
    }
    let identity = expected
        .order_id
        .clone()
        .unwrap_or(native_client_id(&expected.client_order_id)?);
    Ok(GateExactReadbackRequest {
        binding: binding.clone(),
        generation,
        endpoint: format!("{}/{}", endpoints::FUTURES_ORDER, identity),
        not_before_ms,
        mutation_kind,
        expected,
        ack_order,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateExactOrderReadback {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub requested_at_ms: u64,
    pub received_at_ms: u64,
    pub raw_payload: String,
    pub order: Order,
}

impl GateExactOrderReadback {
    pub fn from_response(
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
        request: &GateExactReadbackRequest,
        requested_at_ms: u64,
        received_at_ms: u64,
        payload: String,
    ) -> Result<Self, GateExecutionError> {
        request.validate(binding, rules)?;
        if requested_at_ms < request.not_before_ms
            || received_at_ms < requested_at_ms
            || payload.is_empty()
        {
            return Err(GateExecutionError::Readback);
        }
        let value: Value =
            serde_json::from_str(&payload).map_err(|_| GateExecutionError::Readback)?;
        let order = parse_regular_order(&value, &rules.instrument.symbol, rules)
            .map_err(|_| GateExecutionError::Readback)?;
        if !matches_expected(&order, &request.expected)
            || request
                .ack_order
                .as_ref()
                .is_some_and(|ack| !same_order_semantics(ack, &order))
        {
            return Err(GateExecutionError::Readback);
        }
        Ok(Self {
            binding: request.binding.clone(),
            generation: request.generation,
            requested_at_ms,
            received_at_ms,
            raw_payload: payload,
            order,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateSettlementFinality {
    Working,
    Terminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateMutationSettlement {
    pub kind: GateMutationKind,
    pub order: Order,
    pub finality: GateSettlementFinality,
    pub settled_at_ms: u64,
}

pub fn settle_exact_readback(
    request: &GateExactReadbackRequest,
    readback: &GateExactOrderReadback,
) -> Result<GateMutationSettlement, GateExecutionError> {
    if readback.binding != request.binding
        || readback.generation != request.generation
        || readback.requested_at_ms < request.not_before_ms
        || !matches_expected(&readback.order, &request.expected)
    {
        return Err(GateExecutionError::Binding);
    }
    let finality = if matches!(
        readback.order.state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    ) {
        GateSettlementFinality::Terminal
    } else {
        GateSettlementFinality::Working
    };
    if request.mutation_kind == GateMutationKind::Cancel
        && finality != GateSettlementFinality::Terminal
    {
        return Err(GateExecutionError::Unsettled);
    }
    Ok(GateMutationSettlement {
        kind: request.mutation_kind,
        order: readback.order.clone(),
        finality,
        settled_at_ms: readback.received_at_ms,
    })
}

fn validate_scope(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    generation: u64,
) -> Result<(), GateExecutionError> {
    if binding.gateway_binding().symbol != rules.instrument.symbol
        || generation == 0
        || generation != rules.instrument.generation
        || rules.instrument.validate().is_err()
        || rules.native_symbol.trim().is_empty()
        || rules.quanto_multiplier <= Decimal::ZERO
    {
        return Err(GateExecutionError::Binding);
    }
    Ok(())
}

fn validate_owner(
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    exchange: &str,
    account: &str,
) -> Result<(), GateExecutionError> {
    validate_scope(binding, rules, rules.instrument.generation)?;
    if exchange != "gate"
        || account != binding.gateway_binding().trading_account_id.as_str()
        || rules.instrument.symbol != binding.gateway_binding().symbol
    {
        return Err(GateExecutionError::Binding);
    }
    Ok(())
}

fn matches_expected(order: &Order, expected: &ExpectedOrder) -> bool {
    expected
        .order_id
        .as_ref()
        .is_none_or(|value| value == &order.order_id)
        && matches!(
            &order.client_order_id,
            FieldState::Known(actual) if actual == &expected.client_order_id
        )
        && expected.side.is_none_or(|value| value == order.side)
        && expected
            .position_side
            .is_none_or(|value| Some(value) == known_position_side(order))
        && expected
            .quantity
            .is_none_or(|value| value == order.quantity)
        && expected
            .limit_price
            .is_none_or(|value| Some(value) == order.limit_price)
        && expected
            .reduce_only
            .is_none_or(|value| value == order.reduce_only)
}

fn same_order_semantics(left: &Order, right: &Order) -> bool {
    left.order_id == right.order_id
        && left.client_order_id == right.client_order_id
        && left.symbol == right.symbol
        && left.side == right.side
        && left.position_side == right.position_side
        && left.quantity == right.quantity
        && left.limit_price == right.limit_price
        && left.reduce_only == right.reduce_only
}

fn known_position_side(order: &Order) -> Option<PositionSide> {
    match order.position_side {
        FieldState::Known(value) => Some(value),
        _ => None,
    }
}

fn validate_step(value: Decimal, step: Decimal) -> Result<(), GateExecutionError> {
    if value > Decimal::ZERO && step > Decimal::ZERO && value % step == Decimal::ZERO {
        Ok(())
    } else {
        Err(GateExecutionError::Rules)
    }
}

fn signed_contracts(contracts: Decimal, side: OrderSide) -> Decimal {
    match side {
        OrderSide::Buy => contracts,
        OrderSide::Sell => -contracts,
    }
}

fn native_client_id(client_order_id: &str) -> Result<String, GateExecutionError> {
    if !(1..=MAX_CLIENT_ORDER_SUFFIX_BYTES).contains(&client_order_id.len())
        || !client_order_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(GateExecutionError::ClientOrderId);
    }
    Ok(format!("t-{client_order_id}"))
}

fn valid_native_order_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn decimal_wire(value: Decimal) -> String {
    value.normalize().to_string()
}

#[derive(Serialize)]
struct PlaceBody<'a> {
    contract: &'a str,
    size: String,
    price: String,
    tif: &'static str,
    reduce_only: bool,
    text: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GateExecutionError {
    #[error("Gate mutation does not match the exact gateway binding, owner, or generation")]
    Binding,
    #[error("Gate mutation intent is invalid or ambiguous")]
    Intent,
    #[error("Gate mutation violates current contract quantity, tick, or notional rules")]
    Rules,
    #[error("Gate client order identity must be 1..=28 safe ASCII bytes")]
    ClientOrderId,
    #[error("Gate mutation payload or acknowledgement is invalid")]
    Payload,
    #[error("Gate rejected the mutation request")]
    VenueRejected,
    #[error("Gate mutation or exact readback could not be signed")]
    Signing,
    #[error("Gate exact signed readback is missing, conflicting, or stale")]
    Readback,
    #[error("Gate mutation remains UNKNOWN and must not be retried")]
    Unsettled,
    #[error(transparent)]
    Order(#[from] GateOrderPayloadError),
}

impl From<GateProtocolError> for GateExecutionError {
    fn from(_: GateProtocolError) -> Self {
        Self::Signing
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::domain::{
        Amount, CommandId, Instrument, MarketKind, OrderOwner, OrderPurpose,
    };
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    fn facts() -> Result<(GateGatewayBinding, GateContractRules), Box<dyn std::error::Error>> {
        let binding = GateGatewayBinding::new(GatewayBinding::new(
            VenueId::Gate,
            GatewayMode::Test,
            ACCOUNT,
            "DOGE/USDT".parse()?,
        )?)?;
        let rules = GateContractRules {
            native_symbol: "DOGE_USDT".to_owned(),
            instrument: Instrument {
                symbol: "DOGE/USDT".parse()?,
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some("USDT".parse()?),
                generation: 7,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::new(1, 1),
                minimum_notional: Amount::new("USDT".parse()?, Decimal::ZERO),
            },
            quanto_multiplier: Decimal::new(1, 1),
            minimum_contracts: Decimal::ONE,
            decimal_contracts: false,
        };
        Ok((binding, rules))
    }

    fn owner(purpose: OrderPurpose) -> Result<OrderOwner, Box<dyn std::error::Error>> {
        Ok(OrderOwner {
            strategy_instance_id: "grid".to_owned(),
            run_id: "run".to_owned(),
            exchange: "gate".to_owned(),
            account: ACCOUNT.to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose,
        })
    }

    fn limit() -> Result<OrderCommand, Box<dyn std::error::Error>> {
        Ok(OrderCommand {
            command_id: CommandId::new("command")?,
            client_order_id: CommandId::new("grid_long_1")?,
            owner: owner(OrderPurpose::Entry)?,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::new(1, 1))?,
            reduce_only: false,
        })
    }

    fn ack_payload(status: &str, finish_as: &str) -> String {
        format!(
            r#"{{"id":"9001","contract":"DOGE_USDT","size":"10","left":"10","is_reduce_only":false,"status":"{status}","finish_as":"{finish_as}","price":"0.1","fill_price":"0","text":"t-grid_long_1"}}"#
        )
    }

    #[test]
    fn post_only_preserves_contract_count_poc_reduce_and_client_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let request = prepare_limit_post_only(&binding, &rules, &limit()?)?;
        assert_eq!(request.kind(), GateMutationKind::PlacePostOnly);
        assert_eq!(
            std::str::from_utf8(request.body())?,
            r#"{"contract":"DOGE_USDT","size":"10","price":"0.1","tif":"poc","reduce_only":false,"text":"t-grid_long_1"}"#
        );
        Ok(())
    }

    #[test]
    fn ack_is_only_accepted_until_a_later_exact_signed_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let accepted = parse_mutation_ack(
            &binding,
            &rules,
            prepare_limit_post_only(&binding, &rules, &limit()?)?,
            ack_payload("open", "").as_bytes(),
            1_000,
        )?;
        assert_eq!(accepted.readback.endpoint, "/futures/usdt/orders/9001");
        let readback = GateExactOrderReadback::from_response(
            &binding,
            &rules,
            &accepted.readback,
            1_001,
            1_002,
            ack_payload("open", ""),
        )?;
        let settled = settle_exact_readback(&accepted.readback, &readback)?;
        assert_eq!(settled.finality, GateSettlementFinality::Working);
        Ok(())
    }

    #[test]
    fn timeout_plan_uses_exact_client_readback_and_never_reconstructs_a_place()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let unknown =
            mutation_unknown(prepare_limit_post_only(&binding, &rules, &limit()?)?, 1_000)?;
        assert_eq!(
            unknown.readback.endpoint,
            "/futures/usdt/orders/t-grid_long_1"
        );
        assert_eq!(unknown.kind, GateMutationKind::PlacePostOnly);
        Ok(())
    }

    #[test]
    fn quantity_direction_and_client_id_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let mut command = limit()?;
        command.quantity = Decimal::new(15, 2);
        assert_eq!(
            prepare_limit_post_only(&binding, &rules, &command),
            Err(GateExecutionError::Rules)
        );
        let mut command = limit()?;
        command.client_order_id = CommandId::new("12345678901234567890123456789")?;
        assert_eq!(
            prepare_limit_post_only(&binding, &rules, &command),
            Err(GateExecutionError::ClientOrderId)
        );
        Ok(())
    }

    #[test]
    fn reduce_once_and_cancel_preserve_exact_native_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules) = facts()?;
        let reduce = MarketReduceCommand {
            command_id: CommandId::new("reduce")?,
            client_order_id: CommandId::new("ord-etp-l-0000000000000001")?,
            owner: owner(OrderPurpose::ExposureTakeProfit)?,
            position_side: PositionSide::Long,
            side: OrderSide::Sell,
            quantity: Decimal::ONE,
            risk_episode_id: CommandId::new("episode")?,
            position_generation: 9,
        };
        let request = prepare_reduce_once(&binding, &rules, &reduce)?;
        assert_eq!(request.kind(), GateMutationKind::ReduceOnce);
        assert_eq!(request.reduce_episode_id(), Some("episode"));
        assert_eq!(request.position_generation(), Some(9));
        assert_eq!(
            std::str::from_utf8(request.body())?,
            r#"{"contract":"DOGE_USDT","size":"-10","price":"0","tif":"ioc","reduce_only":true,"text":"t-ord-etp-l-0000000000000001"}"#
        );

        let cancel = GateCancelIntent {
            command: CancelCommand {
                command_id: CommandId::new("cancel")?,
                owner: owner(OrderPurpose::Entry)?,
                target_client_order_id: CommandId::new("grid_long_1")?,
            },
            venue_order_id: "9001".to_owned(),
        };
        let cancel = prepare_cancel(&binding, &rules, &cancel)?;
        assert_eq!(cancel.kind(), GateMutationKind::Cancel);
        assert_eq!(cancel.endpoint(), "/futures/usdt/orders/9001");
        assert!(cancel.body().is_empty());
        let accepted = parse_mutation_ack(
            &binding,
            &rules,
            cancel,
            ack_payload("finished", "cancelled").as_bytes(),
            2_000,
        )?;
        let readback = GateExactOrderReadback::from_response(
            &binding,
            &rules,
            &accepted.readback,
            2_001,
            2_002,
            ack_payload("finished", "cancelled"),
        )?;
        assert_eq!(
            settle_exact_readback(&accepted.readback, &readback)?.finality,
            GateSettlementFinality::Terminal
        );
        Ok(())
    }
}
