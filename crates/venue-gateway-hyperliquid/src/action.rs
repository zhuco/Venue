use std::fmt;

use k256::ecdsa::{RecoveryId, Signature, SigningKey};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use venue_domain::domain::{FieldState, LimitTimeInForce, OrderSide, OrderState, Price};
use venue_gateway_api::GatewayMode;

use crate::{
    HyperliquidConfig, HyperliquidCredentials, HyperliquidError, HyperliquidOrderLookup,
    HyperliquidOrderStatus, HyperliquidPayloadScope, HyperliquidPerpMeta,
    HyperliquidPrivateStreamBinding, HyperliquidReadBinding, PersistedNonce,
    build_order_status_request, endpoints,
};

const MAX_WIRE_DECIMALS: u32 = 8;
const MAX_PERP_PRICE_DECIMALS: u32 = 6;
const MAX_PRICE_SIGNIFICANT_DIGITS: u32 = 5;
const MAX_REJECTION_BYTES: usize = 1_024;
const EIP712_DOMAIN_TYPE: &[u8] =
    b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";
const AGENT_TYPE: &[u8] = b"Agent(string source,bytes32 connectionId)";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HyperliquidSource {
    Live,
}

impl HyperliquidSource {
    #[must_use]
    pub const fn live() -> Self {
        Self::Live
    }

    #[must_use]
    pub const fn mode(self) -> GatewayMode {
        match self {
            Self::Live => GatewayMode::Live,
        }
    }

    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Live => "a",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HyperliquidActionKind {
    AloPlace,
    GtcPlace,
    Cancel,
    IocReduceOnly,
    IocMarket,
    TriggerMarket,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HyperliquidLimitOrder {
    scope: HyperliquidPayloadScope,
    asset: u32,
    is_buy: bool,
    price: String,
    size: String,
    reduce_only: bool,
    client_order_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidAloOrder(HyperliquidLimitOrder);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidGtcOrder(HyperliquidLimitOrder);

impl HyperliquidLimitOrder {
    fn new(
        meta: &HyperliquidPerpMeta,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
        reduce_only: bool,
        client_order_id: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        validate_trade_meta(meta)?;
        Ok(Self {
            scope: meta.scope.clone(),
            asset: meta.asset_index,
            is_buy: matches!(side, OrderSide::Buy),
            price: price_wire(price, meta.size_decimals)?,
            size: decimal_wire(size, meta.size_decimals.min(MAX_WIRE_DECIMALS))?,
            reduce_only,
            client_order_id: canonical_client_order_id(client_order_id.into())?,
        })
    }
}

impl HyperliquidAloOrder {
    pub fn new(
        meta: &HyperliquidPerpMeta,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
        reduce_only: bool,
        client_order_id: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        HyperliquidLimitOrder::new(meta, side, price, size, reduce_only, client_order_id).map(Self)
    }

    #[must_use]
    pub const fn scope(&self) -> &HyperliquidPayloadScope {
        &self.0.scope
    }
}

impl HyperliquidGtcOrder {
    pub fn new(
        meta: &HyperliquidPerpMeta,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
        reduce_only: bool,
        client_order_id: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        HyperliquidLimitOrder::new(meta, side, price, size, reduce_only, client_order_id).map(Self)
    }

    #[must_use]
    pub const fn scope(&self) -> &HyperliquidPayloadScope {
        &self.0.scope
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidIocReduceOnlyOrder {
    scope: HyperliquidPayloadScope,
    asset: u32,
    is_buy: bool,
    price: String,
    size: String,
    client_order_id: String,
}

impl HyperliquidIocReduceOnlyOrder {
    pub fn new(
        meta: &HyperliquidPerpMeta,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
        client_order_id: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        validate_trade_meta(meta)?;
        Ok(Self {
            scope: meta.scope.clone(),
            asset: meta.asset_index,
            is_buy: matches!(side, OrderSide::Buy),
            price: price_wire(price, meta.size_decimals)?,
            size: decimal_wire(size, meta.size_decimals.min(MAX_WIRE_DECIMALS))?,
            client_order_id: canonical_client_order_id(client_order_id.into())?,
        })
    }

    /// Hyperliquid represents a market entry as an IOC limit with a bounded executable price.
    pub fn new_market(
        meta: &HyperliquidPerpMeta,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
        client_order_id: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        Self::new(meta, side, price, size, client_order_id)
    }

    #[must_use]
    pub const fn scope(&self) -> &HyperliquidPayloadScope {
        &self.scope
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidTriggerMarketOrder {
    scope: HyperliquidPayloadScope,
    asset: u32,
    is_buy: bool,
    limit_price: String,
    trigger_price: String,
    size: String,
    client_order_id: String,
    tpsl: &'static str,
}

impl HyperliquidTriggerMarketOrder {
    pub fn new(
        meta: &HyperliquidPerpMeta,
        side: OrderSide,
        limit_price: Decimal,
        trigger_price: Decimal,
        size: Decimal,
        tpsl: &'static str,
        client_order_id: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        validate_trade_meta(meta)?;
        if limit_price <= Decimal::ZERO
            || trigger_price <= Decimal::ZERO
            || size <= Decimal::ZERO
            || !matches!(tpsl, "sl" | "tp")
        {
            return Err(HyperliquidError::Action);
        }
        Ok(Self {
            scope: meta.scope.clone(),
            asset: meta.asset_index,
            is_buy: matches!(side, OrderSide::Buy),
            limit_price: price_wire(limit_price, meta.size_decimals)?,
            trigger_price: price_wire(trigger_price, meta.size_decimals)?,
            size: decimal_wire(size, meta.size_decimals.min(MAX_WIRE_DECIMALS))?,
            client_order_id: canonical_client_order_id(client_order_id.into())?,
            tpsl,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidCancel {
    scope: HyperliquidPayloadScope,
    asset: u32,
    order_id: u64,
}

impl HyperliquidCancel {
    pub fn new(meta: &HyperliquidPerpMeta, order_id: u64) -> Result<Self, HyperliquidError> {
        if order_id == 0 {
            return Err(HyperliquidError::Action);
        }
        Ok(Self {
            scope: meta.scope.clone(),
            asset: meta.asset_index,
            order_id,
        })
    }

    #[must_use]
    pub const fn scope(&self) -> &HyperliquidPayloadScope {
        &self.scope
    }
}

pub struct HyperliquidExchangeRequest {
    binding: HyperliquidReadBinding,
    mode: GatewayMode,
    source: HyperliquidSource,
    rest_origin: &'static str,
    kind: HyperliquidActionKind,
    nonce: u64,
    expires_after_ms: Option<u64>,
    vault_address: Option<String>,
    connection_id: [u8; 32],
    expected: ResponseExpectation,
    readback_target: ReadbackTarget,
    body: Vec<u8>,
}

impl fmt::Debug for HyperliquidExchangeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HyperliquidExchangeRequest")
            .field("binding", &self.binding)
            .field("mode", &self.mode)
            .field("source", &self.source)
            .field("kind", &self.kind)
            .field("nonce", &self.nonce)
            .field("expires_after_ms", &self.expires_after_ms)
            .field("vault_address", &self.vault_address)
            .field("body", &"[SIGNED]")
            .finish()
    }
}

impl HyperliquidExchangeRequest {
    #[must_use]
    pub const fn binding(&self) -> &HyperliquidReadBinding {
        &self.binding
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.mode
    }

    #[must_use]
    pub const fn source(&self) -> HyperliquidSource {
        self.source
    }

    #[must_use]
    pub const fn rest_origin(&self) -> &'static str {
        self.rest_origin
    }

    #[must_use]
    pub const fn endpoint(&self) -> &'static str {
        endpoints::EXCHANGE
    }

    #[must_use]
    pub const fn kind(&self) -> HyperliquidActionKind {
        self.kind
    }

    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }

    #[must_use]
    pub const fn expires_after_ms(&self) -> Option<u64> {
        self.expires_after_ms
    }

    #[must_use]
    pub fn vault_address(&self) -> Option<&str> {
        self.vault_address.as_deref()
    }

    #[must_use]
    pub const fn connection_id(&self) -> [u8; 32] {
        self.connection_id
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// The synchronous `/exchange` response is only an acknowledgement. Accepted mutations are not
/// terminal until a generation-bound, read-only `orderStatus` query converges them.
pub enum HyperliquidExchangeOutcome {
    Resting {
        order_id: u64,
    },
    Filled {
        order_id: u64,
        total_size: Decimal,
        average_price: Price,
    },
    Cancelled {
        order_id: u64,
    },
    Rejected {
        reason: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidExchangeReadbackPlan {
    binding: HyperliquidPrivateStreamBinding,
    nonce: u64,
    kind: HyperliquidActionKind,
    lookup: HyperliquidOrderLookup,
    acknowledgement: Option<HyperliquidExchangeOutcome>,
    target: ReadbackTarget,
}

impl HyperliquidExchangeReadbackPlan {
    #[must_use]
    pub const fn binding(&self) -> &HyperliquidPrivateStreamBinding {
        &self.binding
    }

    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }

    #[must_use]
    pub const fn kind(&self) -> HyperliquidActionKind {
        self.kind
    }

    #[must_use]
    pub const fn lookup(&self) -> &HyperliquidOrderLookup {
        &self.lookup
    }

    #[must_use]
    pub const fn acknowledgement(&self) -> Option<&HyperliquidExchangeOutcome> {
        self.acknowledgement.as_ref()
    }

    pub fn order_status_request(
        &self,
        meta: &HyperliquidPerpMeta,
    ) -> Result<crate::HyperliquidInfoRequest, HyperliquidError> {
        if meta.scope != *self.binding.scope() {
            return Err(HyperliquidError::Readback);
        }
        build_order_status_request(meta, &self.lookup)
    }

    pub fn reconcile(
        &self,
        status: Option<&HyperliquidOrderStatus>,
    ) -> Result<HyperliquidExchangeConvergence, HyperliquidError> {
        if let Some(HyperliquidExchangeOutcome::Rejected { reason }) = &self.acknowledgement {
            return Ok(HyperliquidExchangeConvergence::Rejected {
                reason: reason.clone(),
            });
        }
        let Some(status) = status else {
            return Ok(HyperliquidExchangeConvergence::PendingUnknown);
        };
        match status {
            HyperliquidOrderStatus::Unknown { scope, lookup, .. } => {
                if scope != self.binding.scope() || lookup != &self.lookup {
                    return Err(HyperliquidError::Readback);
                }
                Ok(HyperliquidExchangeConvergence::PendingUnknown)
            }
            HyperliquidOrderStatus::Known {
                scope,
                order_id,
                client_order_id,
                side,
                limit_price,
                original_quantity,
                remaining_quantity,
                reduce_only,
                native_order_type,
                time_in_force,
                trigger_price,
                is_position_tpsl,
                state,
                exchange_time_ms,
                ..
            } => {
                if scope != self.binding.scope()
                    || !self.lookup_matches(*order_id, client_order_id)
                    || !self.target.matches_order(
                        client_order_id,
                        *side,
                        *limit_price,
                        *original_quantity,
                        native_order_type,
                        time_in_force.as_deref(),
                        *reduce_only,
                        *trigger_price,
                        *is_position_tpsl,
                    )
                    || (self.kind == HyperliquidActionKind::AloPlace
                        && (native_order_type != "Limit"
                            || time_in_force.as_deref() != Some("Alo")))
                    || (self.kind == HyperliquidActionKind::GtcPlace
                        && (native_order_type != "Limit"
                            || time_in_force.as_deref() != Some("Gtc")))
                    || (matches!(
                        self.kind,
                        HyperliquidActionKind::IocReduceOnly | HyperliquidActionKind::IocMarket
                    ) && !matches!(
                        (native_order_type.as_str(), time_in_force.as_deref()),
                        ("Limit", Some("Ioc")) | ("Market", Some("FrontendMarket"))
                    ))
                    || (self.kind == HyperliquidActionKind::TriggerMarket
                        && !(matches!(
                            native_order_type.as_str(),
                            "Stop Market" | "Take Profit Market"
                        ) && time_in_force.is_none()
                            && trigger_price.is_some()
                            && *is_position_tpsl))
                {
                    return Err(HyperliquidError::Readback);
                }
                if *remaining_quantity > *original_quantity || !self.state_converges(*state) {
                    return Err(HyperliquidError::Readback);
                }
                if self.cancel_still_open(*state) {
                    return Ok(HyperliquidExchangeConvergence::PendingUnknown);
                }
                Ok(HyperliquidExchangeConvergence::Confirmed {
                    order_id: *order_id,
                    state: *state,
                    exchange_time_ms: *exchange_time_ms,
                })
            }
        }
    }

    fn lookup_matches(&self, order_id: u64, client_order_id: &FieldState<String>) -> bool {
        match &self.lookup {
            HyperliquidOrderLookup::OrderId(expected) => order_id == *expected,
            HyperliquidOrderLookup::ClientOrderId(expected) => matches!(
                client_order_id,
                FieldState::Known(actual) if actual.eq_ignore_ascii_case(expected)
            ),
        }
    }

    fn state_converges(&self, state: OrderState) -> bool {
        match (&self.acknowledgement, self.kind) {
            (
                Some(HyperliquidExchangeOutcome::Resting { .. }),
                HyperliquidActionKind::AloPlace
                | HyperliquidActionKind::GtcPlace
                | HyperliquidActionKind::TriggerMarket,
            ) => {
                matches!(
                    state,
                    OrderState::New
                        | OrderState::PartiallyFilled
                        | OrderState::Filled
                        | OrderState::Cancelled
                        | OrderState::Expired
                )
            }
            (
                Some(HyperliquidExchangeOutcome::Filled { .. }),
                HyperliquidActionKind::GtcPlace
                | HyperliquidActionKind::IocReduceOnly
                | HyperliquidActionKind::IocMarket
                | HyperliquidActionKind::TriggerMarket,
            ) => state == OrderState::Filled,
            (Some(HyperliquidExchangeOutcome::Cancelled { .. }), HyperliquidActionKind::Cancel) => {
                matches!(state, OrderState::Filled | OrderState::Cancelled)
            }
            (
                None,
                HyperliquidActionKind::AloPlace
                | HyperliquidActionKind::GtcPlace
                | HyperliquidActionKind::TriggerMarket,
            ) => state != OrderState::Unknown,
            (None, HyperliquidActionKind::IocReduceOnly | HyperliquidActionKind::IocMarket) => {
                matches!(
                    state,
                    OrderState::Filled | OrderState::Cancelled | OrderState::Rejected
                )
            }
            (None, HyperliquidActionKind::Cancel) => state != OrderState::Unknown,
            (Some(HyperliquidExchangeOutcome::Rejected { .. }), _) => true,
            _ => false,
        }
    }

    fn cancel_still_open(&self, state: OrderState) -> bool {
        self.kind == HyperliquidActionKind::Cancel
            && self.acknowledgement.is_none()
            && matches!(state, OrderState::New | OrderState::PartiallyFilled)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HyperliquidExchangeConvergence {
    /// An absent/unknown status is not proof that the mutation did not reach the venue. The exact
    /// signed request must remain UNKNOWN and must not be submitted again.
    PendingUnknown,
    Rejected {
        reason: String,
    },
    Confirmed {
        order_id: u64,
        state: OrderState,
        exchange_time_ms: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReadbackTarget {
    Order {
        client_order_id: String,
        side: OrderSide,
        limit_price: Price,
        quantity: Decimal,
        native_order_type: Option<&'static str>,
        time_in_force: Option<LimitTimeInForce>,
        reduce_only: bool,
        trigger_price: Option<Price>,
        is_position_tpsl: bool,
    },
    Cancel {
        order_id: u64,
    },
}

impl ReadbackTarget {
    fn matches_order(
        &self,
        client_order_id: &FieldState<String>,
        side: OrderSide,
        limit_price: Price,
        quantity: Decimal,
        native_order_type: &str,
        time_in_force: Option<&str>,
        reduce_only: bool,
        trigger_price: Option<Price>,
        is_position_tpsl: bool,
    ) -> bool {
        match self {
            Self::Order {
                client_order_id: expected_client_order_id,
                side: expected_side,
                limit_price: expected_limit_price,
                quantity: expected_quantity,
                native_order_type: expected_native_order_type,
                time_in_force: expected_time_in_force,
                reduce_only: expected_reduce_only,
                trigger_price: expected_trigger_price,
                is_position_tpsl: expected_is_position_tpsl,
            } => {
                matches!(
                    client_order_id,
                    FieldState::Known(actual)
                        if actual.eq_ignore_ascii_case(expected_client_order_id)
                ) && side == *expected_side
                    && limit_price == *expected_limit_price
                    && quantity == *expected_quantity
                    && expected_native_order_type
                        .is_none_or(|expected| native_order_type == expected)
                    && expected_time_in_force.is_none_or(|expected| {
                        matches!(
                            (expected, time_in_force),
                            (LimitTimeInForce::PostOnly, Some("Alo"))
                                | (LimitTimeInForce::Gtc, Some("Gtc"))
                        )
                    })
                    && reduce_only == *expected_reduce_only
                    && trigger_price == *expected_trigger_price
                    && is_position_tpsl == *expected_is_position_tpsl
            }
            Self::Cancel { .. } => true,
        }
    }
}

pub fn begin_exchange_readback(
    request: &HyperliquidExchangeRequest,
    acknowledgement: Option<&HyperliquidExchangeOutcome>,
    binding: &HyperliquidPrivateStreamBinding,
) -> Result<HyperliquidExchangeReadbackPlan, HyperliquidError> {
    if binding.scope().binding() != request.binding()
        || binding.mode() != request.mode()
        || binding.generation() == 0
    {
        return Err(HyperliquidError::Readback);
    }
    validate_acknowledgement(request, acknowledgement)?;
    let lookup = match acknowledgement {
        Some(HyperliquidExchangeOutcome::Resting { order_id })
        | Some(HyperliquidExchangeOutcome::Filled { order_id, .. })
        | Some(HyperliquidExchangeOutcome::Cancelled { order_id }) => {
            HyperliquidOrderLookup::order_id(*order_id)?
        }
        Some(HyperliquidExchangeOutcome::Rejected { .. }) | None => {
            match &request.readback_target {
                ReadbackTarget::Order {
                    client_order_id, ..
                } => HyperliquidOrderLookup::client_order_id(client_order_id.clone())?,
                ReadbackTarget::Cancel { order_id } => HyperliquidOrderLookup::order_id(*order_id)?,
            }
        }
    };
    Ok(HyperliquidExchangeReadbackPlan {
        binding: binding.clone(),
        nonce: request.nonce,
        kind: request.kind,
        lookup,
        acknowledgement: acknowledgement.cloned(),
        target: request.readback_target.clone(),
    })
}

fn validate_acknowledgement(
    request: &HyperliquidExchangeRequest,
    acknowledgement: Option<&HyperliquidExchangeOutcome>,
) -> Result<(), HyperliquidError> {
    match (request.kind, acknowledgement) {
        (_, None) => Ok(()),
        (
            HyperliquidActionKind::AloPlace,
            Some(HyperliquidExchangeOutcome::Resting { order_id }),
        ) if *order_id > 0 => Ok(()),
        (
            HyperliquidActionKind::TriggerMarket,
            Some(HyperliquidExchangeOutcome::Resting { order_id }),
        ) if *order_id > 0 => Ok(()),
        (
            HyperliquidActionKind::GtcPlace,
            Some(HyperliquidExchangeOutcome::Resting { order_id }),
        ) if *order_id > 0 => Ok(()),
        (
            HyperliquidActionKind::GtcPlace
            | HyperliquidActionKind::IocReduceOnly
            | HyperliquidActionKind::IocMarket
            | HyperliquidActionKind::TriggerMarket,
            Some(HyperliquidExchangeOutcome::Filled {
                order_id,
                total_size,
                ..
            }),
        ) if *order_id > 0
            && *total_size > Decimal::ZERO
            && matches!(
                request.readback_target,
                ReadbackTarget::Order { quantity, .. } if *total_size <= quantity
            ) =>
        {
            Ok(())
        }
        (
            HyperliquidActionKind::Cancel,
            Some(HyperliquidExchangeOutcome::Cancelled { order_id }),
        ) if matches!(
            request.readback_target,
            ReadbackTarget::Cancel { order_id: expected } if *order_id == expected
        ) =>
        {
            Ok(())
        }
        (_, Some(HyperliquidExchangeOutcome::Rejected { reason }))
            if !reason.is_empty() && reason.len() <= MAX_REJECTION_BYTES =>
        {
            Ok(())
        }
        _ => Err(HyperliquidError::Readback),
    }
}

pub(crate) fn build_alo_place_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    order: HyperliquidAloOrder,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    build_limit_place_request(
        credentials,
        nonce,
        order.0,
        LimitTimeInForce::PostOnly,
        expires_after_ms,
    )
}

pub(crate) fn build_gtc_place_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    order: HyperliquidGtcOrder,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    build_limit_place_request(
        credentials,
        nonce,
        order.0,
        LimitTimeInForce::Gtc,
        expires_after_ms,
    )
}

fn build_limit_place_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    order: HyperliquidLimitOrder,
    time_in_force: LimitTimeInForce,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    let quantity = decimal_from_wire(&order.size)?;
    let (wire_tif, kind, acknowledgement) = match time_in_force {
        LimitTimeInForce::PostOnly => (
            "Alo",
            HyperliquidActionKind::AloPlace,
            ResponseExpectation::Alo,
        ),
        LimitTimeInForce::Gtc => (
            "Gtc",
            HyperliquidActionKind::GtcPlace,
            ResponseExpectation::Gtc {
                expected_size: quantity,
            },
        ),
    };
    let readback_target = ReadbackTarget::Order {
        client_order_id: order.client_order_id.clone(),
        side: if order.is_buy {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        limit_price: Price::new(decimal_from_wire(&order.price)?)
            .map_err(|_| HyperliquidError::Action)?,
        quantity,
        native_order_type: Some("Limit"),
        time_in_force: Some(time_in_force),
        reduce_only: order.reduce_only,
        trigger_price: None,
        is_position_tpsl: false,
    };
    let action = Action::Order(OrderAction {
        kind: "order",
        orders: vec![OrderWire {
            asset: order.asset,
            is_buy: order.is_buy,
            price: order.price,
            size: order.size,
            reduce_only: order.reduce_only,
            order_type: HyperliquidOrderType::Limit(LimitOrderType {
                limit: LimitTif { tif: wire_tif },
            }),
            client_order_id: order.client_order_id,
        }],
        grouping: "na",
    });
    signed_request(
        order.scope,
        credentials,
        nonce,
        expires_after_ms,
        kind,
        ResponseContract {
            acknowledgement,
            readback_target,
        },
        action,
    )
}

pub(crate) fn build_ioc_reduce_only_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    order: HyperliquidIocReduceOnlyOrder,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    let expected_size = decimal_from_wire(&order.size)?;
    let readback_target = ReadbackTarget::Order {
        client_order_id: order.client_order_id.clone(),
        side: if order.is_buy {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        limit_price: Price::new(decimal_from_wire(&order.price)?)
            .map_err(|_| HyperliquidError::Action)?,
        quantity: expected_size,
        native_order_type: None,
        time_in_force: None,
        reduce_only: true,
        trigger_price: None,
        is_position_tpsl: false,
    };
    let action = Action::Order(OrderAction {
        kind: "order",
        orders: vec![OrderWire {
            asset: order.asset,
            is_buy: order.is_buy,
            price: order.price,
            size: order.size,
            reduce_only: true,
            order_type: HyperliquidOrderType::Limit(LimitOrderType {
                limit: LimitTif { tif: "Ioc" },
            }),
            client_order_id: order.client_order_id,
        }],
        grouping: "na",
    });
    signed_request(
        order.scope,
        credentials,
        nonce,
        expires_after_ms,
        HyperliquidActionKind::IocReduceOnly,
        ResponseContract {
            acknowledgement: ResponseExpectation::Ioc { expected_size },
            readback_target,
        },
        action,
    )
}

pub(crate) fn build_ioc_market_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    order: HyperliquidIocReduceOnlyOrder,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    let expected_size = decimal_from_wire(&order.size)?;
    let readback_target = ReadbackTarget::Order {
        client_order_id: order.client_order_id.clone(),
        side: if order.is_buy {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        limit_price: Price::new(decimal_from_wire(&order.price)?)
            .map_err(|_| HyperliquidError::Action)?,
        quantity: expected_size,
        native_order_type: None,
        time_in_force: None,
        reduce_only: false,
        trigger_price: None,
        is_position_tpsl: false,
    };
    let action = Action::Order(OrderAction {
        kind: "order",
        orders: vec![OrderWire {
            asset: order.asset,
            is_buy: order.is_buy,
            price: order.price,
            size: order.size,
            reduce_only: false,
            order_type: HyperliquidOrderType::Limit(LimitOrderType {
                limit: LimitTif { tif: "Ioc" },
            }),
            client_order_id: order.client_order_id,
        }],
        grouping: "na",
    });
    signed_request(
        order.scope,
        credentials,
        nonce,
        expires_after_ms,
        HyperliquidActionKind::IocMarket,
        ResponseContract {
            acknowledgement: ResponseExpectation::Ioc { expected_size },
            readback_target,
        },
        action,
    )
}

pub(crate) fn build_trigger_market_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    order: HyperliquidTriggerMarketOrder,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    let quantity = decimal_from_wire(&order.size)?;
    let trigger = Price::new(decimal_from_wire(&order.trigger_price)?)
        .map_err(|_| HyperliquidError::Action)?;
    let limit =
        Price::new(decimal_from_wire(&order.limit_price)?).map_err(|_| HyperliquidError::Action)?;
    let readback_target = ReadbackTarget::Order {
        client_order_id: order.client_order_id.clone(),
        side: if order.is_buy {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        limit_price: limit,
        quantity,
        native_order_type: Some(if order.tpsl == "tp" {
            "Take Profit Market"
        } else {
            "Stop Market"
        }),
        time_in_force: None,
        reduce_only: true,
        trigger_price: Some(trigger),
        is_position_tpsl: true,
    };
    let action = Action::Order(OrderAction {
        kind: "order",
        orders: vec![OrderWire {
            asset: order.asset,
            is_buy: order.is_buy,
            price: order.limit_price,
            size: order.size,
            reduce_only: true,
            order_type: HyperliquidOrderType::Trigger(TriggerOrderType {
                trigger: TriggerSpec {
                    is_market: true,
                    trigger_px: order.trigger_price,
                    tpsl: order.tpsl,
                },
            }),
            client_order_id: order.client_order_id,
        }],
        grouping: "positionTpsl",
    });
    signed_request(
        order.scope,
        credentials,
        nonce,
        expires_after_ms,
        HyperliquidActionKind::TriggerMarket,
        ResponseContract {
            acknowledgement: ResponseExpectation::Trigger,
            readback_target,
        },
        action,
    )
}

pub(crate) fn build_cancel_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    cancel: HyperliquidCancel,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    let readback_target = ReadbackTarget::Cancel {
        order_id: cancel.order_id,
    };
    let action = Action::Cancel(CancelAction {
        kind: "cancel",
        cancels: vec![CancelWire {
            asset: cancel.asset,
            order_id: cancel.order_id,
        }],
    });
    signed_request(
        cancel.scope,
        credentials,
        nonce,
        expires_after_ms,
        HyperliquidActionKind::Cancel,
        ResponseContract {
            acknowledgement: ResponseExpectation::Cancel {
                order_id: cancel.order_id,
            },
            readback_target,
        },
        action,
    )
}

pub fn parse_exchange_ack(
    payload: &[u8],
    request: &HyperliquidExchangeRequest,
) -> Result<HyperliquidExchangeOutcome, HyperliquidError> {
    let envelope: ExchangeEnvelope =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Response)?;
    match envelope {
        ExchangeEnvelope::Err(reason) => Ok(HyperliquidExchangeOutcome::Rejected {
            reason: rejection(reason)?,
        }),
        ExchangeEnvelope::Ok(response) => {
            let expected_type = match request.kind {
                HyperliquidActionKind::AloPlace
                | HyperliquidActionKind::GtcPlace
                | HyperliquidActionKind::IocReduceOnly
                | HyperliquidActionKind::IocMarket
                | HyperliquidActionKind::TriggerMarket => "order",
                HyperliquidActionKind::Cancel => "cancel",
            };
            if response.kind != expected_type || response.data.statuses.len() != 1 {
                return Err(HyperliquidError::Response);
            }
            let status = response
                .data
                .statuses
                .into_iter()
                .next()
                .ok_or(HyperliquidError::Response)?;
            match (&request.expected, status) {
                (ResponseExpectation::Alo, ExchangeStatus::Resting(value)) if value.oid > 0 => {
                    Ok(HyperliquidExchangeOutcome::Resting {
                        order_id: value.oid,
                    })
                }
                (ResponseExpectation::Gtc { .. }, ExchangeStatus::Resting(value))
                    if value.oid > 0 =>
                {
                    Ok(HyperliquidExchangeOutcome::Resting {
                        order_id: value.oid,
                    })
                }
                (ResponseExpectation::Trigger, ExchangeStatus::Resting(value)) if value.oid > 0 => {
                    Ok(HyperliquidExchangeOutcome::Resting {
                        order_id: value.oid,
                    })
                }
                (
                    ResponseExpectation::Gtc { expected_size }
                    | ResponseExpectation::Ioc { expected_size },
                    ExchangeStatus::Filled(value),
                ) => {
                    let total_size = decimal_from_wire(&value.total_size)?;
                    let average_price = Price::new(decimal_from_wire(&value.average_price)?)
                        .map_err(|_| HyperliquidError::Response)?;
                    if value.oid == 0 || total_size > *expected_size {
                        return Err(HyperliquidError::Response);
                    }
                    Ok(HyperliquidExchangeOutcome::Filled {
                        order_id: value.oid,
                        total_size,
                        average_price,
                    })
                }
                (ResponseExpectation::Cancel { order_id }, ExchangeStatus::Success) => {
                    Ok(HyperliquidExchangeOutcome::Cancelled {
                        order_id: *order_id,
                    })
                }
                (_, ExchangeStatus::Error(reason)) => Ok(HyperliquidExchangeOutcome::Rejected {
                    reason: rejection(reason)?,
                }),
                _ => Err(HyperliquidError::Response),
            }
        }
    }
}

/// Compatibility name for the synchronous exchange acknowledgement parser. A successful return is
/// not a terminal mutation receipt; use `begin_exchange_readback` and read-only `orderStatus`.
pub fn parse_exchange_response(
    payload: &[u8],
    request: &HyperliquidExchangeRequest,
) -> Result<HyperliquidExchangeOutcome, HyperliquidError> {
    parse_exchange_ack(payload, request)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResponseExpectation {
    Alo,
    Gtc { expected_size: Decimal },
    Ioc { expected_size: Decimal },
    Trigger,
    Cancel { order_id: u64 },
}

struct ResponseContract {
    acknowledgement: ResponseExpectation,
    readback_target: ReadbackTarget,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Action {
    Order(OrderAction),
    Cancel(CancelAction),
}

#[derive(Serialize)]
struct OrderAction {
    #[serde(rename = "type")]
    kind: &'static str,
    orders: Vec<OrderWire>,
    grouping: &'static str,
}

#[derive(Serialize)]
struct OrderWire {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "b")]
    is_buy: bool,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "s")]
    size: String,
    #[serde(rename = "r")]
    reduce_only: bool,
    #[serde(rename = "t")]
    order_type: HyperliquidOrderType,
    #[serde(rename = "c")]
    client_order_id: String,
}

#[derive(Serialize)]
struct LimitOrderType {
    limit: LimitTif,
}

#[derive(Serialize)]
#[serde(untagged)]
enum HyperliquidOrderType {
    Limit(LimitOrderType),
    Trigger(TriggerOrderType),
}

#[derive(Serialize)]
struct LimitTif {
    tif: &'static str,
}

#[derive(Serialize)]
struct TriggerOrderType {
    trigger: TriggerSpec,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TriggerSpec {
    is_market: bool,
    trigger_px: String,
    tpsl: &'static str,
}

#[derive(Serialize)]
struct CancelAction {
    #[serde(rename = "type")]
    kind: &'static str,
    cancels: Vec<CancelWire>,
}

#[derive(Serialize)]
struct CancelWire {
    #[serde(rename = "a")]
    asset: u32,
    #[serde(rename = "o")]
    order_id: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SignedEnvelope<'a> {
    action: &'a Action,
    nonce: u64,
    signature: WireSignature,
    vault_address: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_after: Option<u64>,
}

#[derive(Serialize)]
struct WireSignature {
    r: String,
    s: String,
    v: u8,
}

#[derive(Deserialize)]
#[serde(
    tag = "status",
    content = "response",
    rename_all = "lowercase",
    deny_unknown_fields
)]
enum ExchangeEnvelope {
    Ok(ExchangeResponse),
    Err(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExchangeResponse {
    #[serde(rename = "type")]
    kind: String,
    data: ExchangeData,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExchangeData {
    statuses: Vec<ExchangeStatus>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum ExchangeStatus {
    Success,
    WaitingForFill,
    WaitingForTrigger,
    Error(String),
    Resting(RestingStatus),
    Filled(FilledStatus),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestingStatus {
    oid: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilledStatus {
    #[serde(rename = "totalSz")]
    total_size: String,
    #[serde(rename = "avgPx")]
    average_price: String,
    oid: u64,
}

fn signed_request(
    scope: HyperliquidPayloadScope,
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    expires_after_ms: Option<u64>,
    kind: HyperliquidActionKind,
    response_contract: ResponseContract,
    action: Action,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    if !scope
        .user_address()
        .eq_ignore_ascii_case(credentials.user_address())
        || !nonce
            .agent_address()
            .eq_ignore_ascii_case(credentials.api_wallet_address())
        || expires_after_ms.is_some_and(|value| value <= nonce.value())
    {
        return Err(HyperliquidError::Binding);
    }
    let source = HyperliquidSource::live();
    let connection_id = action_hash(
        &action,
        credentials.vault_address(),
        nonce.value(),
        expires_after_ms,
    )?;
    let signature = sign_agent(&credentials.signing_key()?, source, connection_id)?;
    let body = serde_json::to_vec(&SignedEnvelope {
        action: &action,
        nonce: nonce.value(),
        signature,
        vault_address: credentials.vault_address(),
        expires_after: expires_after_ms,
    })
    .map_err(|_| HyperliquidError::Action)?;
    let config = HyperliquidConfig::for_binding(scope.binding().gateway());
    Ok(HyperliquidExchangeRequest {
        binding: scope.binding().clone(),
        mode: config.mode(),
        source,
        rest_origin: config.rest_origin(),
        kind,
        nonce: nonce.value(),
        expires_after_ms,
        vault_address: credentials.vault_address().map(str::to_owned),
        connection_id,
        expected: response_contract.acknowledgement,
        readback_target: response_contract.readback_target,
        body,
    })
}

fn action_hash<T: Serialize>(
    action: &T,
    vault_address: Option<&str>,
    nonce: u64,
    expires_after_ms: Option<u64>,
) -> Result<[u8; 32], HyperliquidError> {
    let mut packed = rmp_serde::to_vec_named(action).map_err(|_| HyperliquidError::Signing)?;
    packed.extend_from_slice(&nonce.to_be_bytes());
    match vault_address {
        None => packed.push(0),
        Some(address) => {
            packed.push(1);
            packed.extend_from_slice(&crate::credentials::address_bytes(address)?);
        }
    }
    if let Some(expires_after_ms) = expires_after_ms {
        packed.push(0);
        packed.extend_from_slice(&expires_after_ms.to_be_bytes());
    }
    Ok(keccak(&packed))
}

fn sign_agent(
    signing_key: &SigningKey,
    source: HyperliquidSource,
    connection_id: [u8; 32],
) -> Result<WireSignature, HyperliquidError> {
    let digest = agent_digest(source, connection_id);
    let (signature, recovery_id) = signing_key
        .sign_prehash_recoverable(&digest)
        .map_err(|_| HyperliquidError::Signing)?;
    wire_signature(&signature, recovery_id)
}

fn agent_digest(source: HyperliquidSource, connection_id: [u8; 32]) -> [u8; 32] {
    let mut domain = Vec::with_capacity(160);
    domain.extend_from_slice(&keccak(EIP712_DOMAIN_TYPE));
    domain.extend_from_slice(&keccak(b"Exchange"));
    domain.extend_from_slice(&keccak(b"1"));
    let mut chain_id = [0_u8; 32];
    chain_id[30..].copy_from_slice(&1337_u16.to_be_bytes());
    domain.extend_from_slice(&chain_id);
    domain.extend_from_slice(&[0; 32]);
    let domain_separator = keccak(&domain);

    let mut agent = Vec::with_capacity(96);
    agent.extend_from_slice(&keccak(AGENT_TYPE));
    agent.extend_from_slice(&keccak(source.as_wire().as_bytes()));
    agent.extend_from_slice(&connection_id);
    let struct_hash = keccak(&agent);

    let mut digest = Vec::with_capacity(66);
    digest.extend_from_slice(b"\x19\x01");
    digest.extend_from_slice(&domain_separator);
    digest.extend_from_slice(&struct_hash);
    keccak(&digest)
}

fn wire_signature(
    signature: &Signature,
    recovery_id: RecoveryId,
) -> Result<WireSignature, HyperliquidError> {
    let bytes = signature.to_bytes();
    let v = recovery_id
        .to_byte()
        .checked_add(27)
        .ok_or(HyperliquidError::Signing)?;
    Ok(WireSignature {
        r: hex_32(&bytes[..32])?,
        s: hex_32(&bytes[32..])?,
        v,
    })
}

fn hex_32(bytes: &[u8]) -> Result<String, HyperliquidError> {
    if bytes.len() != 32 {
        return Err(HyperliquidError::Signing);
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(66);
    output.push_str("0x");
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(output)
}

fn keccak(value: &[u8]) -> [u8; 32] {
    Keccak256::digest(value).into()
}

fn validate_trade_meta(meta: &HyperliquidPerpMeta) -> Result<(), HyperliquidError> {
    if !meta.trading_enabled || meta.size_decimals > MAX_PERP_PRICE_DECIMALS {
        Err(HyperliquidError::Action)
    } else {
        Ok(())
    }
}

fn price_wire(value: Decimal, size_decimals: u32) -> Result<String, HyperliquidError> {
    let normalized = value.normalize();
    let max_scale = MAX_PERP_PRICE_DECIMALS
        .checked_sub(size_decimals)
        .ok_or(HyperliquidError::Action)?;
    if normalized.scale() != 0
        && (normalized.scale() > max_scale
            || decimal_digits(normalized.mantissa().unsigned_abs()) > MAX_PRICE_SIGNIFICANT_DIGITS)
    {
        return Err(HyperliquidError::Action);
    }
    decimal_wire(normalized, MAX_WIRE_DECIMALS)
}

fn decimal_digits(mut value: u128) -> u32 {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn decimal_wire(value: Decimal, max_scale: u32) -> Result<String, HyperliquidError> {
    let normalized = value.normalize();
    if normalized <= Decimal::ZERO || normalized.scale() > max_scale {
        return Err(HyperliquidError::Action);
    }
    let wire = normalized.to_string();
    if wire.contains(['e', 'E']) {
        return Err(HyperliquidError::Action);
    }
    Ok(wire)
}

fn decimal_from_wire(value: &str) -> Result<Decimal, HyperliquidError> {
    let parsed = value
        .parse::<Decimal>()
        .map_err(|_| HyperliquidError::Response)?;
    if parsed <= Decimal::ZERO || parsed.normalize().scale() > MAX_WIRE_DECIMALS {
        return Err(HyperliquidError::Response);
    }
    Ok(parsed)
}

fn canonical_client_order_id(value: String) -> Result<String, HyperliquidError> {
    match HyperliquidOrderLookup::client_order_id(value).map_err(|_| HyperliquidError::Action)? {
        HyperliquidOrderLookup::ClientOrderId(value) => Ok(value),
        HyperliquidOrderLookup::OrderId(_) => Err(HyperliquidError::Action),
    }
}

fn rejection(reason: String) -> Result<String, HyperliquidError> {
    if reason.is_empty()
        || reason.len() > MAX_REJECTION_BYTES
        || reason.chars().any(char::is_control)
    {
        Err(HyperliquidError::Response)
    } else {
        Ok(reason)
    }
}

#[cfg(test)]
mod tests;
