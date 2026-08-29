use std::fmt;

use k256::ecdsa::{RecoveryId, Signature, SigningKey};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use venue_domain::domain::{OrderSide, Price};
use venue_gateway_api::GatewayMode;

use crate::{
    HyperliquidConfig, HyperliquidCredentials, HyperliquidError, HyperliquidOrderLookup,
    HyperliquidPayloadScope, HyperliquidPerpMeta, HyperliquidReadBinding, PersistedNonce,
    endpoints,
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
    Test,
    Live,
}

impl HyperliquidSource {
    #[must_use]
    pub const fn for_mode(mode: GatewayMode) -> Self {
        match mode {
            GatewayMode::Test => Self::Test,
            GatewayMode::Live => Self::Live,
        }
    }

    #[must_use]
    pub const fn mode(self) -> GatewayMode {
        match self {
            Self::Test => GatewayMode::Test,
            Self::Live => GatewayMode::Live,
        }
    }

    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Test => "b",
            Self::Live => "a",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HyperliquidActionKind {
    AloPlace,
    Cancel,
    IocReduceOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidAloOrder {
    scope: HyperliquidPayloadScope,
    asset: u32,
    is_buy: bool,
    price: String,
    size: String,
    reduce_only: bool,
    client_order_id: String,
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

    #[must_use]
    pub const fn scope(&self) -> &HyperliquidPayloadScope {
        &self.scope
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

    #[must_use]
    pub const fn scope(&self) -> &HyperliquidPayloadScope {
        &self.scope
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

pub fn build_alo_place_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    order: HyperliquidAloOrder,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    let action = Action::Order(OrderAction {
        kind: "order",
        orders: vec![OrderWire {
            asset: order.asset,
            is_buy: order.is_buy,
            price: order.price,
            size: order.size.clone(),
            reduce_only: order.reduce_only,
            order_type: LimitOrderType {
                limit: LimitTif { tif: "Alo" },
            },
            client_order_id: order.client_order_id,
        }],
        grouping: "na",
    });
    signed_request(
        order.scope,
        credentials,
        nonce,
        expires_after_ms,
        HyperliquidActionKind::AloPlace,
        ResponseExpectation::Alo,
        action,
    )
}

pub fn build_ioc_reduce_only_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    order: HyperliquidIocReduceOnlyOrder,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    let expected_size = decimal_from_wire(&order.size)?;
    let action = Action::Order(OrderAction {
        kind: "order",
        orders: vec![OrderWire {
            asset: order.asset,
            is_buy: order.is_buy,
            price: order.price,
            size: order.size,
            reduce_only: true,
            order_type: LimitOrderType {
                limit: LimitTif { tif: "Ioc" },
            },
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
        ResponseExpectation::Ioc { expected_size },
        action,
    )
}

pub fn build_cancel_request(
    credentials: &HyperliquidCredentials,
    nonce: PersistedNonce,
    cancel: HyperliquidCancel,
    expires_after_ms: Option<u64>,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
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
        ResponseExpectation::Cancel {
            order_id: cancel.order_id,
        },
        action,
    )
}

pub fn parse_exchange_response(
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
                HyperliquidActionKind::AloPlace | HyperliquidActionKind::IocReduceOnly => "order",
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
                (ResponseExpectation::Ioc { expected_size }, ExchangeStatus::Filled(value)) => {
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResponseExpectation {
    Alo,
    Ioc { expected_size: Decimal },
    Cancel { order_id: u64 },
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
    order_type: LimitOrderType,
    #[serde(rename = "c")]
    client_order_id: String,
}

#[derive(Serialize)]
struct LimitOrderType {
    limit: LimitTif,
}

#[derive(Serialize)]
struct LimitTif {
    tif: &'static str,
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
    expected: ResponseExpectation,
    action: Action,
) -> Result<HyperliquidExchangeRequest, HyperliquidError> {
    if !scope
        .user_address()
        .eq_ignore_ascii_case(credentials.user_address())
        || !nonce
            .agent_address()
            .eq_ignore_ascii_case(credentials.agent_address())
        || expires_after_ms.is_some_and(|value| value <= nonce.value())
    {
        return Err(HyperliquidError::Binding);
    }
    let source = HyperliquidSource::for_mode(scope.mode());
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
        expected,
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
mod tests {
    use super::*;
    use crate::{
        HyperliquidGatewayBinding, HyperliquidNonceStore, NonceCheckpoint, reserve_next_nonce,
    };
    use venue_gateway_api::{GatewayBinding, VenueId};

    const USER: &str = "0x0000000000000000000000000000000000000001";
    const AGENT: &str = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a";
    const AGENT_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    #[derive(Default)]
    struct MemoryNonceStore {
        checkpoint: Option<NonceCheckpoint>,
    }

    impl HyperliquidNonceStore for MemoryNonceStore {
        fn load(
            &mut self,
            _agent_address: &str,
        ) -> Result<Option<NonceCheckpoint>, HyperliquidError> {
            Ok(self.checkpoint.clone())
        }

        fn persist(&mut self, checkpoint: &NonceCheckpoint) -> Result<(), HyperliquidError> {
            self.checkpoint = Some(checkpoint.clone());
            Ok(())
        }
    }

    #[derive(Serialize)]
    struct OfficialOrderAction {
        #[serde(rename = "type")]
        kind: &'static str,
        orders: Vec<OfficialOrder>,
        grouping: &'static str,
    }

    #[derive(Serialize)]
    struct OfficialOrder {
        #[serde(rename = "a")]
        asset: u32,
        #[serde(rename = "b")]
        is_buy: bool,
        #[serde(rename = "p")]
        price: &'static str,
        #[serde(rename = "s")]
        size: &'static str,
        #[serde(rename = "r")]
        reduce_only: bool,
        #[serde(rename = "t")]
        order_type: LimitOrderType,
    }

    #[test]
    fn official_python_sdk_order_signature_vector_matches() -> Result<(), HyperliquidError> {
        let action = OfficialOrderAction {
            kind: "order",
            orders: vec![OfficialOrder {
                asset: 1,
                is_buy: true,
                price: "100",
                size: "100",
                reduce_only: false,
                order_type: LimitOrderType {
                    limit: LimitTif { tif: "Gtc" },
                },
            }],
            grouping: "na",
        };
        let key = SigningKey::from_slice(&hex_key(
            "0123456789012345678901234567890123456789012345678901234567890123",
        )?)
        .map_err(|_| HyperliquidError::Signing)?;
        let connection_id = action_hash(&action, None, 0, None)?;
        let signature = sign_agent(&key, HyperliquidSource::Live, connection_id)?;
        assert_eq!(
            signature.r,
            "0xd65369825a9df5d80099e513cce430311d7d26ddf477f5b3a33d2806b100d78e"
        );
        assert_eq!(
            signature.s,
            "0x2b54116ff64054968aa237c20ca9ff68000f977c93289157748a3162b6ea940e"
        );
        assert_eq!(signature.v, 28);
        Ok(())
    }

    #[test]
    fn narrow_action_wires_are_bound_signed_and_strict() -> Result<(), Box<dyn std::error::Error>> {
        let meta = meta(GatewayMode::Test, USER)?;
        let credentials =
            HyperliquidCredentials::from_values(USER, USER, None, "venue-agent", AGENT, AGENT_KEY)?;
        let mut nonce_store = MemoryNonceStore::default();
        let nonce = reserve_next_nonce(&mut nonce_store, AGENT, 1_700_000_000_000)?;
        let alo = HyperliquidAloOrder::new(
            &meta,
            OrderSide::Buy,
            Decimal::new(6_500_500, 3),
            Decimal::new(4, 1),
            false,
            "0x00000000000000000000000000000001",
        )?;
        let request = build_alo_place_request(&credentials, nonce, alo, Some(1_700_000_001_000))?;
        assert_eq!(request.source(), HyperliquidSource::Test);
        assert_eq!(request.endpoint(), "/exchange");
        let body: serde_json::Value = serde_json::from_slice(request.body())?;
        assert_eq!(
            body["action"],
            serde_json::json!({
                "type":"order",
                "orders":[{
                    "a":0,
                    "b":true,
                    "p":"6500.5",
                    "s":"0.4",
                    "r":false,
                    "t":{"limit":{"tif":"Alo"}},
                    "c":"0x00000000000000000000000000000001"
                }],
                "grouping":"na"
            })
        );
        assert!(body["vaultAddress"].is_null());
        assert_eq!(body["expiresAfter"], 1_700_000_001_000_u64);
        assert!(body["signature"]["v"].as_u64().is_some());
        assert!(matches!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":77}}]}}}"#,
                &request,
            )?,
            HyperliquidExchangeOutcome::Resting { order_id: 77 }
        ));
        assert_eq!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.4","avgPx":"6500","oid":77}}]}}}"#,
                &request,
            ),
            Err(HyperliquidError::Response)
        );
        assert_eq!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":["waitingForFill"]}},"unexpected":true}"#,
                &request,
            ),
            Err(HyperliquidError::Response)
        );
        assert!(matches!(
            HyperliquidAloOrder::new(
                &meta,
                OrderSide::Buy,
                Decimal::new(65_000_500, 3),
                Decimal::new(4, 1),
                false,
                "0x00000000000000000000000000000003",
            ),
            Err(HyperliquidError::Action)
        ));
        Ok(())
    }

    #[test]
    fn vault_ioc_and_cancel_keep_exact_scope_and_response_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        const VAULT: &str = "0x0000000000000000000000000000000000000002";
        let meta = meta(GatewayMode::Live, VAULT)?;
        let credentials = HyperliquidCredentials::from_values(
            USER,
            VAULT,
            Some(VAULT.to_owned()),
            "venue-agent",
            AGENT,
            AGENT_KEY,
        )?;
        let mut store = MemoryNonceStore::default();
        let ioc_nonce = reserve_next_nonce(&mut store, AGENT, 1_700_000_000_000)?;
        let ioc = HyperliquidIocReduceOnlyOrder::new(
            &meta,
            OrderSide::Sell,
            Decimal::new(64_000, 0),
            Decimal::new(3, 1),
            "0x00000000000000000000000000000002",
        )?;
        let ioc_request = build_ioc_reduce_only_request(&credentials, ioc_nonce, ioc, None)?;
        assert_eq!(ioc_request.source(), HyperliquidSource::Live);
        let body: serde_json::Value = serde_json::from_slice(ioc_request.body())?;
        assert_eq!(body["vaultAddress"], VAULT);
        assert_eq!(body["action"]["orders"][0]["r"], true);
        assert_eq!(body["action"]["orders"][0]["t"]["limit"]["tif"], "Ioc");
        assert!(matches!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"filled":{"totalSz":"0.2","avgPx":"63999.5","oid":88}}]}}}"#,
                &ioc_request,
            )?,
            HyperliquidExchangeOutcome::Filled { order_id: 88, .. }
        ));
        assert!(matches!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"order","data":{"statuses":[{"error":"IocCancel"}]}}}"#,
                &ioc_request,
            )?,
            HyperliquidExchangeOutcome::Rejected { .. }
        ));

        let cancel_nonce = reserve_next_nonce(&mut store, AGENT, 1_700_000_000_001)?;
        let cancel_request = build_cancel_request(
            &credentials,
            cancel_nonce,
            HyperliquidCancel::new(&meta, 88)?,
            None,
        )?;
        assert!(matches!(
            parse_exchange_response(
                br#"{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}"#,
                &cancel_request,
            )?,
            HyperliquidExchangeOutcome::Cancelled { order_id: 88 }
        ));
        Ok(())
    }

    fn meta(
        mode: GatewayMode,
        user: &str,
    ) -> Result<HyperliquidPerpMeta, Box<dyn std::error::Error>> {
        let gateway = HyperliquidGatewayBinding::new(GatewayBinding::new(
            VenueId::Hyperliquid,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDC".parse()?,
        )?)?;
        let read = HyperliquidReadBinding::new(gateway, user)?;
        Ok(crate::parse_perp_meta(
            br#"{"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":50}]}"#,
            &read,
        )?)
    }

    fn hex_key(value: &str) -> Result<[u8; 32], HyperliquidError> {
        if value.len() != 64 {
            return Err(HyperliquidError::Signing);
        }
        let mut output = [0_u8; 32];
        let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
        if !remainder.is_empty() {
            return Err(HyperliquidError::Signing);
        }
        for (index, pair) in pairs.iter().enumerate() {
            let high = hex_nibble(pair[0]).ok_or(HyperliquidError::Signing)?;
            let low = hex_nibble(pair[1]).ok_or(HyperliquidError::Signing)?;
            output[index] = (high << 4) | low;
        }
        Ok(output)
    }

    fn hex_nibble(value: u8) -> Option<u8> {
        match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            _ => None,
        }
    }
}
