use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rust_decimal::Decimal;
use serde::Deserialize;
use venue_domain::domain::{FieldState, OrderSide, OrderState, Price};
use venue_gateway_api::GatewayMode;

use crate::{
    HyperliquidConfig, HyperliquidError, HyperliquidFill, HyperliquidPayloadScope,
    HyperliquidPerpMeta,
    models::{EventEnvelope, UserFillRow, UserFillsData, WsOrderUpdateRow},
    protocol::{canonical_cloid, decimal, normalize_fill, normalized_order_status, side},
};

const MAX_PRIVATE_EVENTS_PER_FRAME: usize = 2_000;
const MAX_RECENT_FILL_IDENTITIES: usize = 8_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidPrivateStreamBinding {
    scope: HyperliquidPayloadScope,
    generation: u64,
}

impl HyperliquidPrivateStreamBinding {
    pub fn new(meta: &HyperliquidPerpMeta, generation: u64) -> Result<Self, HyperliquidError> {
        if generation == 0 {
            return Err(HyperliquidError::Binding);
        }
        Ok(Self {
            scope: meta.scope.clone(),
            generation,
        })
    }

    #[must_use]
    pub const fn scope(&self) -> &HyperliquidPayloadScope {
        &self.scope
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.scope.mode()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HyperliquidPrivateSubscriptionKind {
    OrderUpdates,
    UserFills,
    UserEvents,
}

impl HyperliquidPrivateSubscriptionKind {
    const fn protocol_name(self) -> &'static str {
        match self {
            Self::OrderUpdates => "orderUpdates",
            Self::UserFills => "userFills",
            Self::UserEvents => "userEvents",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidPrivateSubscription {
    binding: HyperliquidPrivateStreamBinding,
    kind: HyperliquidPrivateSubscriptionKind,
    websocket: &'static str,
    body: Vec<u8>,
}

/// Initial `twapStates` snapshot for the default perp dex. It is intentionally separate from
/// the long-lived private decoder: bootstrap needs one bounded, complete response before it can
/// make any account-risk claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidTwapStatesSnapshot {
    binding: HyperliquidPrivateStreamBinding,
    states: Vec<HyperliquidTwapState>,
}

impl HyperliquidTwapStatesSnapshot {
    #[must_use]
    pub const fn binding(&self) -> &HyperliquidPrivateStreamBinding {
        &self.binding
    }

    #[must_use]
    pub fn states(&self) -> &[HyperliquidTwapState] {
        &self.states
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidTwapState {
    pub twap_id: u64,
    pub coin: String,
    pub side: OrderSide,
    pub quantity: Decimal,
    pub executed_quantity: Decimal,
    pub executed_notional: Decimal,
    pub reduce_only: bool,
    pub timestamp_ms: u64,
}

pub fn build_twap_states_subscription(
    binding: &HyperliquidPrivateStreamBinding,
) -> Result<Vec<u8>, HyperliquidError> {
    serde_json::to_vec(&serde_json::json!({
        "method": "subscribe",
        "subscription": {"type": "twapStates", "user": binding.scope.user_address(), "dex": ""},
    }))
    .map_err(|_| HyperliquidError::Payload)
}

pub fn parse_twap_states_snapshot(
    payload: &[u8],
    binding: &HyperliquidPrivateStreamBinding,
) -> Result<HyperliquidTwapStatesSnapshot, HyperliquidError> {
    let envelope: EventEnvelope =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if envelope.channel != "twapStates" {
        return Err(HyperliquidError::Payload);
    }
    let raw: TwapStatesData =
        serde_json::from_value(envelope.data).map_err(|_| HyperliquidError::Payload)?;
    if !raw.user.eq_ignore_ascii_case(binding.scope.user_address()) || !raw.dex.is_empty() {
        return Err(HyperliquidError::Binding);
    }
    let mut ids = BTreeSet::new();
    let states = raw
        .states
        .into_iter()
        .map(|(twap_id, state)| {
            if twap_id == 0
                || !ids.insert(twap_id)
                || !state
                    .user
                    .eq_ignore_ascii_case(binding.scope.user_address())
                || state.coin.is_empty()
                || state.coin != state.coin.trim()
                || state.timestamp == 0
            {
                return Err(HyperliquidError::Payload);
            }
            let quantity = decimal(&state.sz)?;
            let executed_quantity = decimal(&state.executed_sz)?;
            let executed_notional = decimal(&state.executed_ntl)?;
            if quantity <= Decimal::ZERO
                || executed_quantity.is_sign_negative()
                || executed_quantity > quantity
                || executed_notional.is_sign_negative()
            {
                return Err(HyperliquidError::Payload);
            }
            Ok(HyperliquidTwapState {
                twap_id,
                coin: state.coin,
                side: side(&state.side)?,
                quantity,
                executed_quantity,
                executed_notional,
                reduce_only: state.reduce_only,
                timestamp_ms: state.timestamp,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(HyperliquidTwapStatesSnapshot {
        binding: binding.clone(),
        states,
    })
}

pub fn parse_all_mids_snapshot(
    payload: &[u8],
) -> Result<BTreeMap<String, Decimal>, HyperliquidError> {
    let envelope: EventEnvelope =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if envelope.channel != "allMids" {
        return Err(HyperliquidError::Payload);
    }
    let mids = envelope
        .data
        .get("mids")
        .and_then(serde_json::Value::as_object)
        .ok_or(HyperliquidError::Payload)?;
    let mut result = BTreeMap::new();
    for (coin, value) in mids {
        if coin.is_empty() || coin != coin.trim() {
            return Err(HyperliquidError::Payload);
        }
        let value = value
            .as_str()
            .ok_or(HyperliquidError::Payload)
            .and_then(decimal)?;
        if value <= Decimal::ZERO || result.insert(coin.clone(), value).is_some() {
            return Err(HyperliquidError::Payload);
        }
    }
    if result.is_empty() {
        return Err(HyperliquidError::Payload);
    }
    Ok(result)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TwapStatesData {
    dex: String,
    user: String,
    states: Vec<(u64, TwapStateData)>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TwapStateData {
    coin: String,
    user: String,
    side: String,
    sz: String,
    executed_sz: String,
    executed_ntl: String,
    #[serde(rename = "reduceOnly")]
    reduce_only: bool,
    timestamp: u64,
}

impl HyperliquidPrivateSubscription {
    #[must_use]
    pub const fn binding(&self) -> &HyperliquidPrivateStreamBinding {
        &self.binding
    }

    #[must_use]
    pub const fn kind(&self) -> HyperliquidPrivateSubscriptionKind {
        self.kind
    }

    #[must_use]
    pub const fn websocket(&self) -> &'static str {
        self.websocket
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

pub fn build_private_subscription(
    binding: &HyperliquidPrivateStreamBinding,
    kind: HyperliquidPrivateSubscriptionKind,
) -> Result<HyperliquidPrivateSubscription, HyperliquidError> {
    let subscription = match kind {
        HyperliquidPrivateSubscriptionKind::UserFills => serde_json::json!({
            "type": kind.protocol_name(),
            "user": binding.scope.user_address(),
            "aggregateByTime": false,
        }),
        HyperliquidPrivateSubscriptionKind::OrderUpdates
        | HyperliquidPrivateSubscriptionKind::UserEvents => serde_json::json!({
            "type": kind.protocol_name(),
            "user": binding.scope.user_address(),
        }),
    };
    let config = HyperliquidConfig::for_binding(binding.scope.binding().gateway());
    Ok(HyperliquidPrivateSubscription {
        binding: binding.clone(),
        kind,
        websocket: config.websocket(),
        body: serde_json::to_vec(&serde_json::json!({
            "method": "subscribe",
            "subscription": subscription,
        }))
        .map_err(|_| HyperliquidError::Payload)?,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidOrderUpdate {
    pub binding: HyperliquidPrivateStreamBinding,
    pub native_coin: String,
    pub order_id: u64,
    pub client_order_id: FieldState<String>,
    pub side: OrderSide,
    pub limit_price: Price,
    pub original_quantity: Decimal,
    pub remaining_quantity: Decimal,
    pub raw_status: String,
    pub state: OrderState,
    pub order_time_ms: u64,
    pub event_time_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HyperliquidFillStream {
    UserFills,
    UserEvents,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidFillUpdate {
    pub binding: HyperliquidPrivateStreamBinding,
    pub stream: HyperliquidFillStream,
    pub snapshot: FieldState<bool>,
    pub fill: HyperliquidFill,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HyperliquidPrivateEvent {
    Order(HyperliquidOrderUpdate),
    Fill(HyperliquidFillUpdate),
}

impl HyperliquidPrivateEvent {
    fn event_time_ms(&self) -> u64 {
        match self {
            Self::Order(update) => update.event_time_ms,
            Self::Fill(update) => update.fill.fill.exchange_time_ms.unwrap_or_default(),
        }
    }

    fn event_id(&self) -> String {
        match self {
            Self::Order(update) => format!(
                "order:{}:{}:{}",
                update.order_id, update.raw_status, update.event_time_ms
            ),
            Self::Fill(update) => update.fill.fill.fill_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct EventFrontier {
    time_ms: Option<u64>,
    ids: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SeenFill {
    first_stream: HyperliquidFillStream,
    counterpart_seen: bool,
    fill: HyperliquidFill,
}

impl EventFrontier {
    fn accept(&mut self, event: &HyperliquidPrivateEvent) -> Result<(), HyperliquidError> {
        let time_ms = event.event_time_ms();
        if time_ms == 0 || self.time_ms.is_some_and(|frontier| time_ms < frontier) {
            return Err(HyperliquidError::Payload);
        }
        if self.time_ms != Some(time_ms) {
            self.time_ms = Some(time_ms);
            self.ids.clear();
        }
        if !self.ids.insert(event.event_id()) {
            return Err(HyperliquidError::Payload);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidPrivateStreamDecoder {
    binding: HyperliquidPrivateStreamBinding,
    order_frontier: EventFrontier,
    fill_frontier: EventFrontier,
    seen_fills: BTreeMap<String, SeenFill>,
    seen_fill_order: VecDeque<String>,
}

impl HyperliquidPrivateStreamDecoder {
    #[must_use]
    pub fn new(binding: HyperliquidPrivateStreamBinding) -> Self {
        Self {
            binding,
            order_frontier: EventFrontier::default(),
            fill_frontier: EventFrontier::default(),
            seen_fills: BTreeMap::new(),
            seen_fill_order: VecDeque::new(),
        }
    }

    #[must_use]
    pub const fn binding(&self) -> &HyperliquidPrivateStreamBinding {
        &self.binding
    }

    pub fn decode(
        &mut self,
        payload: &[u8],
        frame_generation: u64,
        received_at_ms: u64,
    ) -> Result<Vec<HyperliquidPrivateEvent>, HyperliquidError> {
        if frame_generation != self.binding.generation || received_at_ms == 0 {
            return Err(HyperliquidError::Binding);
        }
        let envelope: EventEnvelope =
            serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
        match envelope.channel.as_str() {
            "orderUpdates" => self.decode_orders(envelope.data, received_at_ms),
            "userFills" => self.decode_user_fills(envelope.data, received_at_ms),
            "userEvents" | "user" => self.decode_user_events(envelope.data, received_at_ms),
            _ => Err(HyperliquidError::Payload),
        }
    }

    fn decode_orders(
        &mut self,
        data: serde_json::Value,
        received_at_ms: u64,
    ) -> Result<Vec<HyperliquidPrivateEvent>, HyperliquidError> {
        let rows: Vec<WsOrderUpdateRow> =
            serde_json::from_value(data).map_err(|_| HyperliquidError::Payload)?;
        frame_len(rows.len())?;
        let events = rows
            .into_iter()
            .map(|row| normalize_order(row, &self.binding))
            .collect::<Result<Vec<_>, _>>()?;
        let mut next = self.order_frontier.clone();
        for event in &events {
            validate_received_at(event, received_at_ms)?;
            next.accept(event)?;
        }
        self.order_frontier = next;
        Ok(events)
    }

    fn decode_user_fills(
        &mut self,
        data: serde_json::Value,
        received_at_ms: u64,
    ) -> Result<Vec<HyperliquidPrivateEvent>, HyperliquidError> {
        let data: UserFillsData =
            serde_json::from_value(data).map_err(|_| HyperliquidError::Payload)?;
        if !data
            .user
            .eq_ignore_ascii_case(self.binding.scope.user_address())
        {
            return Err(HyperliquidError::Binding);
        }
        let is_snapshot = data.is_snapshot.ok_or(HyperliquidError::Payload)?;
        self.decode_fills(
            data.fills,
            HyperliquidFillStream::UserFills,
            FieldState::Known(is_snapshot),
            is_snapshot,
            received_at_ms,
        )
    }

    fn decode_user_events(
        &mut self,
        data: serde_json::Value,
        received_at_ms: u64,
    ) -> Result<Vec<HyperliquidPrivateEvent>, HyperliquidError> {
        let object = data.as_object().ok_or(HyperliquidError::Payload)?;
        if object.contains_key("funding")
            || object.contains_key("liquidation")
            || object.contains_key("nonUserCancel")
        {
            return Err(HyperliquidError::Payload);
        }
        let fills: Vec<UserFillRow> = serde_json::from_value(
            object
                .get("fills")
                .cloned()
                .ok_or(HyperliquidError::Payload)?,
        )
        .map_err(|_| HyperliquidError::Payload)?;
        self.decode_fills(
            fills,
            HyperliquidFillStream::UserEvents,
            FieldState::NotApplicable,
            false,
            received_at_ms,
        )
    }

    fn decode_fills(
        &mut self,
        rows: Vec<UserFillRow>,
        stream: HyperliquidFillStream,
        snapshot: FieldState<bool>,
        sort_snapshot: bool,
        received_at_ms: u64,
    ) -> Result<Vec<HyperliquidPrivateEvent>, HyperliquidError> {
        frame_len(rows.len())?;
        if sort_snapshot && self.fill_frontier.time_ms.is_some() {
            return Err(HyperliquidError::Payload);
        }
        let mut events = rows
            .into_iter()
            .map(|row| {
                Ok(HyperliquidPrivateEvent::Fill(HyperliquidFillUpdate {
                    binding: self.binding.clone(),
                    stream,
                    snapshot: snapshot.clone(),
                    fill: normalize_fill(row, &self.binding.scope)?,
                }))
            })
            .collect::<Result<Vec<_>, HyperliquidError>>()?;
        if sort_snapshot {
            events.sort_by_key(HyperliquidPrivateEvent::event_time_ms);
        }
        let mut next_frontier = self.fill_frontier.clone();
        let mut next_seen = self.seen_fills.clone();
        let mut next_order = self.seen_fill_order.clone();
        let mut unique = Vec::with_capacity(events.len());
        for event in events {
            validate_received_at(&event, received_at_ms)?;
            let HyperliquidPrivateEvent::Fill(update) = &event else {
                return Err(HyperliquidError::Payload);
            };
            let fill_id = update.fill.fill.fill_id.clone();
            if let Some(seen) = next_seen.get_mut(&fill_id) {
                if seen.fill != update.fill
                    || seen.first_stream == update.stream
                    || seen.counterpart_seen
                {
                    return Err(HyperliquidError::Payload);
                }
                seen.counterpart_seen = true;
                continue;
            }
            next_frontier.accept(&event)?;
            next_seen.insert(
                fill_id.clone(),
                SeenFill {
                    first_stream: update.stream,
                    counterpart_seen: false,
                    fill: update.fill.clone(),
                },
            );
            next_order.push_back(fill_id);
            while next_order.len() > MAX_RECENT_FILL_IDENTITIES {
                if let Some(expired) = next_order.pop_front() {
                    next_seen.remove(&expired);
                }
            }
            unique.push(event);
        }
        self.fill_frontier = next_frontier;
        self.seen_fills = next_seen;
        self.seen_fill_order = next_order;
        Ok(unique)
    }
}

fn normalize_order(
    row: WsOrderUpdateRow,
    binding: &HyperliquidPrivateStreamBinding,
) -> Result<HyperliquidPrivateEvent, HyperliquidError> {
    if row.order.coin != binding.scope.native_coin() {
        return Err(HyperliquidError::Binding);
    }
    if row.order.oid == 0 || row.order.timestamp == 0 || row.status_timestamp < row.order.timestamp
    {
        return Err(HyperliquidError::Payload);
    }
    let original_quantity = decimal(&row.order.orig_sz)?;
    let remaining_quantity = decimal(&row.order.sz)?;
    if original_quantity <= Decimal::ZERO
        || remaining_quantity.is_sign_negative()
        || remaining_quantity > original_quantity
    {
        return Err(HyperliquidError::Payload);
    }
    let client_order_id = match row.order.cloid {
        Some(value) => FieldState::Known(canonical_cloid(value)?),
        None => FieldState::Missing,
    };
    let state = normalized_order_status(&row.status)?;
    if (state == OrderState::Filled && !remaining_quantity.is_zero())
        || (matches!(state, OrderState::New | OrderState::PartiallyFilled)
            && remaining_quantity.is_zero())
    {
        return Err(HyperliquidError::Payload);
    }
    Ok(HyperliquidPrivateEvent::Order(HyperliquidOrderUpdate {
        binding: binding.clone(),
        native_coin: row.order.coin,
        order_id: row.order.oid,
        client_order_id,
        side: side(&row.order.side)?,
        limit_price: Price::new(decimal(&row.order.limit_px)?)
            .map_err(|_| HyperliquidError::Payload)?,
        original_quantity,
        remaining_quantity,
        state,
        raw_status: row.status,
        order_time_ms: row.order.timestamp,
        event_time_ms: row.status_timestamp,
    }))
}

fn validate_received_at(
    event: &HyperliquidPrivateEvent,
    received_at_ms: u64,
) -> Result<(), HyperliquidError> {
    if event.event_time_ms() > received_at_ms {
        Err(HyperliquidError::Payload)
    } else {
        Ok(())
    }
}

fn frame_len(len: usize) -> Result<(), HyperliquidError> {
    if len == 0 || len > MAX_PRIVATE_EVENTS_PER_FRAME {
        Err(HyperliquidError::Payload)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod twap_tests {
    use super::*;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    fn binding() -> Result<HyperliquidPrivateStreamBinding, Box<dyn std::error::Error>> {
        let gateway = crate::HyperliquidGatewayBinding::new(GatewayBinding::new(
            VenueId::Hyperliquid,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDC".parse()?,
        )?)?;
        let read = crate::HyperliquidReadBinding::new(
            gateway,
            "0x0000000000000000000000000000000000000001",
        )?;
        let meta = crate::parse_perp_meta(include_bytes!("../fixtures/perp-meta.json"), &read)?;
        Ok(HyperliquidPrivateStreamBinding::new(&meta, 7)?)
    }

    #[test]
    fn twap_snapshot_requires_exact_user_dex_and_parent_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let request: serde_json::Value =
            serde_json::from_slice(&build_twap_states_subscription(&binding)?)?;
        assert_eq!(request["subscription"]["type"], "twapStates");
        assert_eq!(
            request["subscription"]["user"],
            binding.scope().user_address()
        );
        assert_eq!(request["subscription"]["dex"], "");
        let payload = br#"{"channel":"twapStates","data":{"dex":"","user":"0x0000000000000000000000000000000000000001","states":[[9,{"coin":"BTC","user":"0x0000000000000000000000000000000000000001","side":"B","sz":"2","executedSz":"0.5","executedNtl":"30000","minutes":10,"reduceOnly":false,"randomize":false,"timestamp":1700000000000}]]}}"#;
        let parsed = parse_twap_states_snapshot(payload, &binding)?;
        assert_eq!(parsed.states()[0].twap_id, 9);
        assert_eq!(parsed.states()[0].quantity, Decimal::new(2, 0));
        let wrong_dex = br#"{"channel":"twapStates","data":{"dex":"other","user":"0x0000000000000000000000000000000000000001","states":[]}}"#;
        assert!(parse_twap_states_snapshot(wrong_dex, &binding).is_err());
        Ok(())
    }
}
