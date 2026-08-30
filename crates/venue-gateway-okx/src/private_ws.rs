use std::collections::BTreeSet;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use venue_domain::domain::{FieldState, OrderPurpose, OrderSide, PositionSide};
use venue_gateway_api::GatewayBinding;

use crate::models::{BalanceRow, OrderRow, PositionRow};
use crate::private::{
    OkxAccountLevel, OkxAccountProfile, OkxTimedBalance, OkxTimedOrder, OkxTimedPosition, boolean,
    normalize_balance_row, normalize_order_row, normalize_position_row, order_side, position_side,
};
use crate::public::positive_u64;
use crate::{OkxConfig, OkxCredentials, OkxError, OkxInstrument, OkxPositionMode, OkxTradeMode};

const LOGIN_PATH: &str = "/users/self/verify";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxPrivateWsScope {
    gateway_binding: GatewayBinding,
    native_instrument_id: String,
    instrument_generation: u64,
    private_generation: u64,
    uid: String,
    main_uid: String,
    account_level: OkxAccountLevel,
    position_mode: OkxPositionMode,
    trade_mode: OkxTradeMode,
}

impl OkxPrivateWsScope {
    fn new(
        config: &OkxConfig,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
        trade_mode: OkxTradeMode,
        private_generation: u64,
    ) -> Result<Self, OkxError> {
        instrument.validate_scope(config)?;
        if profile.uid().is_empty()
            || profile.main_uid().is_empty()
            || private_generation == 0
            || !profile.supports_trade_mode(trade_mode)
        {
            return Err(OkxError::Binding);
        }
        Ok(Self {
            gateway_binding: config.gateway_binding().clone(),
            native_instrument_id: instrument.native_id().to_owned(),
            instrument_generation: instrument.instrument().generation,
            private_generation,
            uid: profile.uid().to_owned(),
            main_uid: profile.main_uid().to_owned(),
            account_level: profile.level(),
            position_mode: profile.position_mode(),
            trade_mode,
        })
    }

    fn validate(
        &self,
        config: &OkxConfig,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
        trade_mode: OkxTradeMode,
        private_generation: u64,
    ) -> Result<(), OkxError> {
        if self != &Self::new(config, instrument, profile, trade_mode, private_generation)? {
            return Err(OkxError::Binding);
        }
        Ok(())
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.gateway_binding
    }

    #[must_use]
    pub fn native_instrument_id(&self) -> &str {
        &self.native_instrument_id
    }

    #[must_use]
    pub const fn instrument_generation(&self) -> u64 {
        self.instrument_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn trade_mode(&self) -> OkxTradeMode {
        self.trade_mode
    }

    #[must_use]
    pub fn uid(&self) -> &str {
        &self.uid
    }

    #[must_use]
    pub fn main_uid(&self) -> &str {
        &self.main_uid
    }
}

pub struct OkxWsLoginFrame {
    scope: OkxPrivateWsScope,
    endpoint: &'static str,
    payload: SecretString,
}

impl OkxWsLoginFrame {
    #[must_use]
    pub const fn scope(&self) -> &OkxPrivateWsScope {
        &self.scope
    }

    #[must_use]
    pub const fn endpoint(&self) -> &'static str {
        self.endpoint
    }

    /// The complete login frame contains API credentials and must remain secret until the
    /// transport writes it to the already-bound private WebSocket.
    #[must_use]
    pub const fn secret_payload(&self) -> &SecretString {
        &self.payload
    }
}

#[derive(Serialize)]
struct LoginFrame<'a> {
    op: &'static str,
    args: [LoginArg<'a>; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginArg<'a> {
    api_key: &'a str,
    passphrase: &'a str,
    timestamp: &'a str,
    sign: &'a str,
}

pub fn build_ws_login(
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    trade_mode: OkxTradeMode,
    private_generation: u64,
    credentials: &OkxCredentials,
    timestamp_seconds: &str,
) -> Result<OkxWsLoginFrame, OkxError> {
    validate_seconds(timestamp_seconds)?;
    let scope =
        OkxPrivateWsScope::new(config, instrument, profile, trade_mode, private_generation)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(credentials.api_secret.expose_secret().as_bytes())
        .map_err(|_| OkxError::SigningInput)?;
    mac.update(timestamp_seconds.as_bytes());
    mac.update(b"GET");
    mac.update(LOGIN_PATH.as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let wire = LoginFrame {
        op: "login",
        args: [LoginArg {
            api_key: credentials.api_key.expose_secret(),
            passphrase: credentials.passphrase.expose_secret(),
            timestamp: timestamp_seconds,
            sign: &signature,
        }],
    };
    let payload =
        SecretString::from(serde_json::to_string(&wire).map_err(|_| OkxError::SigningInput)?);
    Ok(OkxWsLoginFrame {
        scope,
        endpoint: config.private_ws(),
        payload,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxPrivateWsSession {
    scope: OkxPrivateWsScope,
    endpoint: &'static str,
    connection_id: String,
}

impl OkxPrivateWsSession {
    #[must_use]
    pub const fn scope(&self) -> &OkxPrivateWsScope {
        &self.scope
    }

    #[must_use]
    pub const fn endpoint(&self) -> &'static str {
        self.endpoint
    }

    #[must_use]
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    fn validate(
        &self,
        config: &OkxConfig,
        instrument: &OkxInstrument,
        profile: &OkxAccountProfile,
    ) -> Result<(), OkxError> {
        self.scope.validate(
            config,
            instrument,
            profile,
            self.scope.trade_mode,
            self.scope.private_generation,
        )?;
        if self.endpoint != config.private_ws() || self.connection_id.is_empty() {
            return Err(OkxError::Binding);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventReply {
    event: String,
    code: String,
    #[serde(default)]
    msg: String,
    conn_id: String,
}

pub fn parse_ws_login_ack(
    payload: &[u8],
    login: &OkxWsLoginFrame,
) -> Result<OkxPrivateWsSession, OkxError> {
    let reply: EventReply = serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    if reply.event != "login" || reply.code != "0" || !reply.msg.is_empty() {
        return Err(OkxError::Rejected);
    }
    validate_connection_id(&reply.conn_id)?;
    Ok(OkxPrivateWsSession {
        scope: login.scope.clone(),
        endpoint: login.endpoint,
        connection_id: reply.conn_id,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxPrivateSubscription {
    scope: OkxPrivateWsScope,
    connection_id: String,
    request_id: String,
    payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxActivePrivateSubscription {
    scope: OkxPrivateWsScope,
    connection_id: String,
    request_id: String,
    config: OkxConfig,
    instrument: OkxInstrument,
    profile: OkxAccountProfile,
}

impl OkxActivePrivateSubscription {
    #[must_use]
    pub const fn scope(&self) -> &OkxPrivateWsScope {
        &self.scope
    }

    #[must_use]
    pub const fn account_profile(&self) -> &OkxAccountProfile {
        &self.profile
    }

    #[must_use]
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
}

impl OkxPrivateSubscription {
    #[must_use]
    pub const fn scope(&self) -> &OkxPrivateWsScope {
        &self.scope
    }

    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Serialize)]
struct SubscribeFrame<'a> {
    id: &'a str,
    op: &'static str,
    args: [SubscriptionArg<'a>; 3],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionArg<'a> {
    channel: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    inst_type: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inst_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ccy: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    extra_params: Option<&'static str>,
}

pub fn build_private_subscribe(
    session: &OkxPrivateWsSession,
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
    request_id: &str,
) -> Result<OkxPrivateSubscription, OkxError> {
    session.validate(config, instrument, profile)?;
    validate_request_id(request_id)?;
    let wire = SubscribeFrame {
        id: request_id,
        op: "subscribe",
        args: [
            SubscriptionArg {
                channel: "orders",
                inst_type: Some("SWAP"),
                inst_id: Some(instrument.native_id()),
                ccy: None,
                extra_params: None,
            },
            SubscriptionArg {
                channel: "account",
                inst_type: None,
                inst_id: None,
                ccy: Some(config.gateway_binding().symbol.quote()),
                extra_params: None,
            },
            SubscriptionArg {
                channel: "positions",
                inst_type: Some("SWAP"),
                inst_id: Some(instrument.native_id()),
                ccy: None,
                extra_params: Some(r#"{"updateInterval":"0"}"#),
            },
        ],
    };
    Ok(OkxPrivateSubscription {
        scope: session.scope.clone(),
        connection_id: session.connection_id.clone(),
        request_id: request_id.to_owned(),
        payload: serde_json::to_vec(&wire).map_err(|_| OkxError::Payload)?,
    })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscribeReply {
    id: String,
    event: String,
    arg: SubscribeReplyArg,
    #[serde(default)]
    code: String,
    #[serde(default)]
    msg: String,
    conn_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscribeReplyArg {
    channel: String,
    #[serde(default)]
    inst_type: String,
    #[serde(default)]
    inst_id: String,
    #[serde(default)]
    ccy: String,
}

pub fn activate_private_subscription(
    acknowledgements: &[&[u8]],
    requested: &OkxPrivateSubscription,
    session: &OkxPrivateWsSession,
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
) -> Result<OkxActivePrivateSubscription, OkxError> {
    validate_subscription(requested, session, config, instrument, profile)?;
    let mut channels = BTreeSet::new();
    for payload in acknowledgements {
        let reply: SubscribeReply =
            serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
        if reply.id != requested.request_id
            || reply.conn_id != session.connection_id
            || reply.event != "subscribe"
            || !(reply.code.is_empty() || reply.code == "0")
            || !reply.msg.is_empty()
            || !valid_ack_arg(&reply.arg, config, instrument)
            || !channels.insert(reply.arg.channel)
        {
            return Err(OkxError::Binding);
        }
    }
    if channels
        != BTreeSet::from([
            "account".to_owned(),
            "orders".to_owned(),
            "positions".to_owned(),
        ])
    {
        return Err(OkxError::Binding);
    }
    Ok(OkxActivePrivateSubscription {
        scope: requested.scope.clone(),
        connection_id: requested.connection_id.clone(),
        request_id: requested.request_id.clone(),
        config: config.clone(),
        instrument: instrument.clone(),
        profile: profile.clone(),
    })
}

fn valid_ack_arg(arg: &SubscribeReplyArg, config: &OkxConfig, instrument: &OkxInstrument) -> bool {
    match arg.channel.as_str() {
        "orders" | "positions" => {
            arg.inst_type == "SWAP" && arg.inst_id == instrument.native_id() && arg.ccy.is_empty()
        }
        "account" => {
            arg.inst_type.is_empty()
                && arg.inst_id.is_empty()
                && arg.ccy == config.gateway_binding().symbol.quote()
        }
        _ => false,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxWsBatch<T> {
    pub instrument_generation: u64,
    pub private_generation: u64,
    pub event_time_ms: Option<u64>,
    pub items: Vec<T>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OkxWsDelivery<T> {
    PendingSnapshot { next_page: u32 },
    Batch(OkxWsBatch<T>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SnapshotPending<T> {
    private_generation: u64,
    next_page: u32,
    event_time_ms: Option<u64>,
    items: Vec<T>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OkxAccountSnapshotState {
    pending: Option<SnapshotPending<OkxTimedBalance>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OkxPositionSnapshotState {
    pending: Option<SnapshotPending<OkxTimedPosition>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OkxEventWindow {
    received_at_ms: u64,
    previous_event_time_ms: Option<u64>,
}

impl OkxEventWindow {
    pub fn new(received_at_ms: u64, previous_event_time_ms: Option<u64>) -> Result<Self, OkxError> {
        if received_at_ms == 0 {
            return Err(OkxError::Sequence);
        }
        Ok(Self {
            received_at_ms,
            previous_event_time_ms,
        })
    }
}

#[derive(Deserialize)]
struct Push<T> {
    arg: PushArg,
    #[serde(rename = "eventType", default)]
    event_type: Option<String>,
    #[serde(rename = "curPage", default)]
    cur_page: Option<u32>,
    #[serde(rename = "lastPage", default)]
    last_page: Option<bool>,
    data: Vec<T>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsOrderRow {
    inst_type: String,
    inst_id: String,
    td_mode: String,
    ord_id: String,
    #[serde(default)]
    cl_ord_id: String,
    side: String,
    pos_side: String,
    sz: String,
    acc_fill_sz: String,
    px: String,
    avg_px: String,
    reduce_only: String,
    state: String,
    u_time: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WsPositionRow {
    inst_type: String,
    inst_id: String,
    mgn_mode: String,
    pos_side: String,
    pos: String,
    avg_px: String,
    mark_px: String,
    u_time: String,
    p_time: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PushArg {
    channel: String,
    uid: String,
    #[serde(default)]
    inst_type: String,
    #[serde(default)]
    inst_id: String,
    #[serde(default)]
    ccy: String,
}

pub fn parse_ws_orders(
    payload: &[u8],
    active: &OkxActivePrivateSubscription,
    private_generation: u64,
    window: OkxEventWindow,
) -> Result<OkxWsBatch<OkxTimedOrder>, OkxError> {
    validate_active(active, private_generation)?;
    let push: Push<WsOrderRow> = serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    if push.event_type.is_some() || push.cur_page.is_some() || push.last_page.is_some() {
        return Err(OkxError::Pagination);
    }
    validate_arg(
        &push.arg,
        "orders",
        &active.profile,
        &active.config,
        &active.instrument,
        true,
    )?;
    let mut identities = BTreeSet::new();
    let mut event_time_ms = 0;
    let mut items = Vec::with_capacity(push.data.len());
    for row in push.data {
        if row.td_mode != active.scope.trade_mode().wire_value() {
            return Err(OkxError::Binding);
        }
        if row.ord_id.is_empty()
            || !row.ord_id.bytes().all(|byte| byte.is_ascii_digit())
            || !identities.insert(row.ord_id.clone())
            || (!row.cl_ord_id.is_empty()
                && ((row.cl_ord_id.len() > 32)
                    || !row
                        .cl_ord_id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric())))
        {
            return Err(OkxError::Identity);
        }
        let row_time = positive_u64(&row.u_time)?;
        validate_event_time(
            row_time,
            window.received_at_ms,
            window.previous_event_time_ms,
        )?;
        event_time_ms = event_time_ms.max(row_time);
        items.push(normalize_ws_order(
            OrderRow {
                inst_type: row.inst_type,
                inst_id: row.inst_id,
                ord_id: row.ord_id,
                cl_ord_id: row.cl_ord_id,
                side: row.side,
                pos_side: row.pos_side,
                sz: row.sz,
                acc_fill_sz: row.acc_fill_sz,
                px: row.px,
                avg_px: row.avg_px,
                reduce_only: row.reduce_only,
                state: row.state,
                u_time: row.u_time,
            },
            &active.instrument,
            &active.profile,
        )?);
    }
    finish_batch(active, Some(event_time_ms), items, false)
}

pub fn parse_ws_account(
    payload: &[u8],
    active: &OkxActivePrivateSubscription,
    private_generation: u64,
    state: &mut OkxAccountSnapshotState,
    window: OkxEventWindow,
) -> Result<OkxWsDelivery<OkxTimedBalance>, OkxError> {
    validate_active(active, private_generation)?;
    let push: Push<BalanceRow> = serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    validate_arg(
        &push.arg,
        "account",
        &active.profile,
        &active.config,
        &active.instrument,
        false,
    )?;
    if push.data.len() > 1 {
        return Err(OkxError::Payload);
    }
    let mut items = Vec::with_capacity(push.data.len());
    let mut event_time_ms = None;
    for row in &push.data {
        let row_time = positive_u64(&row.u_time)?;
        validate_event_time(
            row_time,
            window.received_at_ms,
            window.previous_event_time_ms,
        )?;
        event_time_ms = Some(event_time_ms.unwrap_or(0).max(row_time));
        items.push(normalize_balance_row(row, &active.config)?);
    }
    collect_account_page(
        active,
        state,
        push.event_type,
        push.cur_page,
        push.last_page,
        event_time_ms,
        items,
    )
}

pub fn parse_ws_positions(
    payload: &[u8],
    active: &OkxActivePrivateSubscription,
    private_generation: u64,
    state: &mut OkxPositionSnapshotState,
    window: OkxEventWindow,
) -> Result<OkxWsDelivery<OkxTimedPosition>, OkxError> {
    validate_active(active, private_generation)?;
    let push: Push<WsPositionRow> =
        serde_json::from_slice(payload).map_err(|_| OkxError::Payload)?;
    validate_arg(
        &push.arg,
        "positions",
        &active.profile,
        &active.config,
        &active.instrument,
        true,
    )?;
    let event_type = push.event_type;
    let cur_page = push.cur_page;
    let last_page = push.last_page;
    let mut sides = BTreeSet::new();
    let mut event_time_ms = None;
    let mut items = Vec::with_capacity(push.data.len());
    for row in push.data {
        if row.mgn_mode != active.scope.trade_mode().wire_value() {
            return Err(OkxError::Binding);
        }
        if !sides.insert(row.pos_side.clone()) {
            return Err(OkxError::Identity);
        }
        let update_time = positive_u64(&row.u_time)?;
        let push_time = positive_u64(&row.p_time)?;
        if update_time > push_time {
            return Err(OkxError::Sequence);
        }
        validate_event_time(
            push_time,
            window.received_at_ms,
            window.previous_event_time_ms,
        )?;
        event_time_ms = Some(event_time_ms.unwrap_or(0).max(push_time));
        let position = normalize_position_row(
            PositionRow {
                inst_type: row.inst_type,
                inst_id: row.inst_id,
                pos_side: row.pos_side,
                pos: row.pos,
                avg_px: row.avg_px,
                mark_px: row.mark_px,
                u_time: row.u_time,
            },
            &active.instrument,
            &active.profile,
            true,
        )?
        .ok_or(OkxError::Payload)?;
        items.push(position);
    }
    collect_position_page(
        active,
        state,
        event_type,
        cur_page,
        last_page,
        event_time_ms,
        items,
    )
}

fn collect_account_page(
    active: &OkxActivePrivateSubscription,
    state: &mut OkxAccountSnapshotState,
    event_type: Option<String>,
    cur_page: Option<u32>,
    last_page: Option<bool>,
    event_time_ms: Option<u64>,
    items: Vec<OkxTimedBalance>,
) -> Result<OkxWsDelivery<OkxTimedBalance>, OkxError> {
    match event_type.as_deref() {
        Some("event_update") => {
            if cur_page.is_some()
                || last_page.is_some()
                || state.pending.is_some()
                || items.is_empty()
            {
                return Err(OkxError::Pagination);
            }
            Ok(OkxWsDelivery::Batch(finish_batch(
                active,
                event_time_ms,
                items,
                false,
            )?))
        }
        Some("snapshot") => collect_snapshot_page(
            active,
            &mut state.pending,
            cur_page,
            last_page,
            event_time_ms,
            items,
        ),
        _ => Err(OkxError::Pagination),
    }
}

fn collect_position_page(
    active: &OkxActivePrivateSubscription,
    state: &mut OkxPositionSnapshotState,
    event_type: Option<String>,
    cur_page: Option<u32>,
    last_page: Option<bool>,
    event_time_ms: Option<u64>,
    items: Vec<OkxTimedPosition>,
) -> Result<OkxWsDelivery<OkxTimedPosition>, OkxError> {
    match event_type.as_deref() {
        Some("event_update") => {
            if cur_page.is_some()
                || last_page.is_some()
                || state.pending.is_some()
                || items.is_empty()
            {
                return Err(OkxError::Pagination);
            }
            Ok(OkxWsDelivery::Batch(finish_batch(
                active,
                event_time_ms,
                items,
                false,
            )?))
        }
        Some("snapshot") => collect_snapshot_page(
            active,
            &mut state.pending,
            cur_page,
            last_page,
            event_time_ms,
            items,
        ),
        _ => Err(OkxError::Pagination),
    }
}

fn collect_snapshot_page<T: Clone>(
    active: &OkxActivePrivateSubscription,
    pending: &mut Option<SnapshotPending<T>>,
    current_page: Option<u32>,
    last_page: Option<bool>,
    event_time_ms: Option<u64>,
    items: Vec<T>,
) -> Result<OkxWsDelivery<T>, OkxError> {
    let current_page = current_page
        .filter(|page| *page > 0)
        .ok_or(OkxError::Pagination)?;
    let last_page = last_page.ok_or(OkxError::Pagination)?;
    let mut next = match pending.clone() {
        None if current_page == 1 => SnapshotPending {
            private_generation: active.scope.private_generation(),
            next_page: 2,
            event_time_ms: None,
            items: Vec::new(),
        },
        Some(value)
            if value.private_generation == active.scope.private_generation()
                && value.next_page == current_page =>
        {
            value
        }
        _ => return Err(OkxError::Pagination),
    };
    next.event_time_ms = match (next.event_time_ms, event_time_ms) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    };
    next.items.extend(items);
    if last_page {
        *pending = None;
        Ok(OkxWsDelivery::Batch(finish_batch(
            active,
            next.event_time_ms,
            next.items,
            true,
        )?))
    } else {
        next.next_page = current_page.checked_add(1).ok_or(OkxError::Pagination)?;
        let expected = next.next_page;
        *pending = Some(next);
        Ok(OkxWsDelivery::PendingSnapshot {
            next_page: expected,
        })
    }
}

fn validate_active(
    active: &OkxActivePrivateSubscription,
    private_generation: u64,
) -> Result<(), OkxError> {
    if active.connection_id.is_empty()
        || active.request_id.is_empty()
        || private_generation != active.scope.private_generation()
    {
        return Err(OkxError::Binding);
    }
    active.scope.validate(
        &active.config,
        &active.instrument,
        &active.profile,
        active.scope.trade_mode,
        active.scope.private_generation,
    )
}

fn validate_subscription(
    subscription: &OkxPrivateSubscription,
    session: &OkxPrivateWsSession,
    config: &OkxConfig,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
) -> Result<(), OkxError> {
    session.validate(config, instrument, profile)?;
    if subscription.scope != session.scope
        || subscription.connection_id != session.connection_id
        || subscription.request_id.is_empty()
    {
        return Err(OkxError::Binding);
    }
    Ok(())
}

fn normalize_ws_order(
    row: OrderRow,
    instrument: &OkxInstrument,
    profile: &OkxAccountProfile,
) -> Result<OkxTimedOrder, OkxError> {
    let mut semantic_reduce = None;
    if profile.position_mode() == OkxPositionMode::LongShort {
        // OKX does not define reduceOnly for FUTURES/SWAP long/short mode. Requiring the wire
        // value to remain false prevents it from contradicting side + posSide.
        if boolean(&row.reduce_only)? {
            return Err(OkxError::PositionMode);
        }
        let side = order_side(&row.side)?;
        let leg = position_side(
            profile.position_mode(),
            &row.pos_side,
            rust_decimal::Decimal::ONE,
        )?;
        semantic_reduce = Some(matches!(
            (leg, side),
            (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy)
        ));
    }
    let mut timed = normalize_order_row(row, instrument, profile, true)?;
    if semantic_reduce == Some(true) {
        timed.order.reduce_only = true;
        timed.order.purpose = FieldState::Known(OrderPurpose::Reduce);
        timed.order.validate().map_err(|_| OkxError::Payload)?;
    }
    Ok(timed)
}

fn validate_arg(
    arg: &PushArg,
    channel: &str,
    profile: &OkxAccountProfile,
    config: &OkxConfig,
    instrument: &OkxInstrument,
    instrument_scoped: bool,
) -> Result<(), OkxError> {
    let valid_scope = if instrument_scoped {
        arg.inst_type == "SWAP" && arg.inst_id == instrument.native_id() && arg.ccy.is_empty()
    } else {
        arg.inst_type.is_empty()
            && arg.inst_id.is_empty()
            && (arg.ccy.is_empty() || arg.ccy == config.gateway_binding().symbol.quote())
    };
    if arg.channel != channel || arg.uid != profile.uid() || !valid_scope {
        return Err(OkxError::Binding);
    }
    Ok(())
}

fn finish_batch<T>(
    active: &OkxActivePrivateSubscription,
    event_time_ms: Option<u64>,
    items: Vec<T>,
    allow_empty: bool,
) -> Result<OkxWsBatch<T>, OkxError> {
    if (!allow_empty && items.is_empty()) || event_time_ms == Some(0) {
        return Err(OkxError::Payload);
    }
    Ok(OkxWsBatch {
        instrument_generation: active.scope.instrument_generation(),
        private_generation: active.scope.private_generation(),
        event_time_ms,
        items,
    })
}

fn validate_event_time(
    event_time_ms: u64,
    received_at_ms: u64,
    previous_event_time_ms: Option<u64>,
) -> Result<(), OkxError> {
    if received_at_ms == 0
        || event_time_ms > received_at_ms
        || previous_event_time_ms.is_some_and(|previous| event_time_ms < previous)
    {
        return Err(OkxError::Sequence);
    }
    Ok(())
}

fn validate_seconds(value: &str) -> Result<(), OkxError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(OkxError::SigningInput);
    }
    value
        .parse::<u64>()
        .ok()
        .filter(|seconds| *seconds > 0)
        .map(|_| ())
        .ok_or(OkxError::SigningInput)
}

fn validate_request_id(value: &str) -> Result<(), OkxError> {
    if !(1..=32).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(OkxError::Identity);
    }
    Ok(())
}

fn validate_connection_id(value: &str) -> Result<(), OkxError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(OkxError::Identity);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::domain::{OrderState, PositionSide};
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    const INSTRUMENT: &[u8] = include_bytes!("../fixtures/linear-swap-instrument.json");
    const PROFILE: &[u8] = include_bytes!("../fixtures/account-config.json");
    const ORDER_PUSH: &[u8] = include_bytes!("../fixtures/ws-orders.json");
    const ACCOUNT_PUSH: &[u8] = include_bytes!("../fixtures/ws-account.json");
    const POSITION_PUSH: &[u8] = include_bytes!("../fixtures/ws-positions.json");
    const ORDER_ACK: &[u8] = br#"{"id":"request1","event":"subscribe","arg":{"channel":"orders","instType":"SWAP","instId":"BTC-USDT-SWAP"},"connId":"connection1"}"#;
    const ACCOUNT_ACK: &[u8] = br#"{"id":"request1","event":"subscribe","arg":{"channel":"account","ccy":"USDT"},"connId":"connection1"}"#;
    const POSITION_ACK: &[u8] = br#"{"id":"request1","event":"subscribe","arg":{"channel":"positions","instType":"SWAP","instId":"BTC-USDT-SWAP"},"connId":"connection1"}"#;

    fn scope(
        mode: GatewayMode,
    ) -> Result<(OkxConfig, OkxInstrument, OkxAccountProfile), Box<dyn std::error::Error>> {
        let config = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?;
        let instrument = crate::parse_instrument(INSTRUMENT, &config, 9)?;
        let profile = crate::parse_account_profile(PROFILE, crate::OkxPositionMode::LongShort)?;
        Ok((config, instrument, profile))
    }

    fn session(
        mode: GatewayMode,
    ) -> Result<
        (
            OkxConfig,
            OkxInstrument,
            OkxAccountProfile,
            OkxPrivateWsSession,
        ),
        Box<dyn std::error::Error>,
    > {
        let (config, instrument, profile) = scope(mode)?;
        let login = build_ws_login(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            17,
            &OkxCredentials::from_values("key", "mysecret", "pass")?,
            "1538054050",
        )?;
        let session = parse_ws_login_ack(
            br#"{"event":"login","code":"0","msg":"","connId":"connection1"}"#,
            &login,
        )?;
        Ok((config, instrument, profile, session))
    }

    fn active(
        mode: GatewayMode,
    ) -> Result<
        (
            OkxConfig,
            OkxInstrument,
            OkxAccountProfile,
            OkxActivePrivateSubscription,
        ),
        Box<dyn std::error::Error>,
    > {
        let (config, instrument, profile, session) = session(mode)?;
        let requested =
            build_private_subscribe(&session, &config, &instrument, &profile, "request1")?;
        let active = activate_private_subscription(
            &[ORDER_ACK, ACCOUNT_ACK, POSITION_ACK],
            &requested,
            &session,
            &config,
            &instrument,
            &profile,
        )?;
        Ok((config, instrument, profile, active))
    }

    #[test]
    fn login_and_subscriptions_are_exact_and_environment_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, instrument, profile) = scope(GatewayMode::Live)?;
        let login = build_ws_login(
            &config,
            &instrument,
            &profile,
            OkxTradeMode::Cross,
            17,
            &OkxCredentials::from_values("key", "mysecret", "pass")?,
            "1538054050",
        )?;
        assert_eq!(login.endpoint(), "wss://ws.okx.com:8443/ws/v5/private");
        assert_eq!(login.scope().private_generation(), 17);
        assert_eq!(login.scope().trade_mode(), OkxTradeMode::Cross);
        assert_eq!(
            login.secret_payload().expose_secret(),
            r#"{"op":"login","args":[{"apiKey":"key","passphrase":"pass","timestamp":"1538054050","sign":"m+lzVL6siKIpimAa/6y8lHpWZe0SCpehAqymC8Nel0A="}]}"#
        );
        let session = parse_ws_login_ack(
            br#"{"event":"login","code":"0","msg":"","connId":"connection1"}"#,
            &login,
        )?;
        let subscribe =
            build_private_subscribe(&session, &config, &instrument, &profile, "request1")?;
        assert_eq!(
            std::str::from_utf8(subscribe.payload())?,
            r#"{"id":"request1","op":"subscribe","args":[{"channel":"orders","instType":"SWAP","instId":"BTC-USDT-SWAP"},{"channel":"account","ccy":"USDT"},{"channel":"positions","instType":"SWAP","instId":"BTC-USDT-SWAP","extraParams":"{\"updateInterval\":\"0\"}"}]}"#
        );
        assert_eq!(
            activate_private_subscription(
                &[ORDER_ACK, ACCOUNT_ACK],
                &subscribe,
                &session,
                &config,
                &instrument,
                &profile
            ),
            Err(OkxError::Binding)
        );
        assert!(
            activate_private_subscription(
                &[ORDER_ACK, ACCOUNT_ACK, POSITION_ACK],
                &subscribe,
                &session,
                &config,
                &instrument,
                &profile
            )
            .is_ok()
        );

        let wrong = OkxConfig::for_binding(GatewayBinding::new(
            VenueId::Okx,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "ETH/USDT".parse()?,
        )?)?;
        assert_eq!(
            build_private_subscribe(&session, &wrong, &instrument, &profile, "request2"),
            Err(OkxError::Binding)
        );
        assert_eq!(
            build_ws_login(
                &config,
                &instrument,
                &profile,
                OkxTradeMode::Cross,
                0,
                &OkxCredentials::from_values("key", "mysecret", "pass")?,
                "1538054050",
            )
            .err(),
            Some(OkxError::Binding)
        );
        Ok(())
    }

    #[test]
    fn private_deltas_reuse_normalization_and_preserve_generation_and_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_config, _instrument, _profile, active) = active(GatewayMode::Live)?;
        let orders = parse_ws_orders(
            ORDER_PUSH,
            &active,
            17,
            OkxEventWindow::new(1_787_911_201_000, None)?,
        )?;
        assert_eq!(orders.instrument_generation, 9);
        assert_eq!(orders.private_generation, 17);
        assert_eq!(orders.event_time_ms, Some(1_787_911_200_700));
        assert_eq!(orders.items[0].order.state, OrderState::Filled);

        let OkxWsDelivery::Batch(account) = parse_ws_account(
            ACCOUNT_PUSH,
            &active,
            17,
            &mut OkxAccountSnapshotState::default(),
            OkxEventWindow::new(1_787_911_201_000, None)?,
        )?
        else {
            return Err("account snapshot not closed".into());
        };
        assert_eq!(account.items[0].balance.asset.as_str(), "USDT");

        let OkxWsDelivery::Batch(positions) = parse_ws_positions(
            POSITION_PUSH,
            &active,
            17,
            &mut OkxPositionSnapshotState::default(),
            OkxEventWindow::new(1_787_911_201_000, None)?,
        )?
        else {
            return Err("position snapshot not closed".into());
        };
        assert_eq!(positions.items[0].position.side, PositionSide::Long);
        assert_eq!(
            positions.items[0].position.quantity,
            rust_decimal::Decimal::ZERO
        );
        Ok(())
    }

    #[test]
    fn wrong_uid_future_time_and_stale_generation_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_config, _instrument, _profile, active) = active(GatewayMode::Live)?;
        assert_eq!(
            parse_ws_orders(
                ORDER_PUSH,
                &active,
                17,
                OkxEventWindow::new(1_787_911_200_699, None)?
            ),
            Err(OkxError::Sequence)
        );
        let wrong_uid = br#"{"arg":{"channel":"orders","uid":"other","instType":"SWAP","instId":"BTC-USDT-SWAP"},"data":[]}"#;
        assert_eq!(
            parse_ws_orders(
                wrong_uid,
                &active,
                17,
                OkxEventWindow::new(1_787_911_201_000, None)?
            ),
            Err(OkxError::Binding)
        );
        assert_eq!(
            parse_ws_positions(
                POSITION_PUSH,
                &active,
                16,
                &mut OkxPositionSnapshotState::default(),
                OkxEventWindow::new(1_787_911_201_000, None)?
            ),
            Err(OkxError::Binding)
        );

        let mut wrong_order_mode: serde_json::Value = serde_json::from_slice(ORDER_PUSH)?;
        wrong_order_mode["data"][0]["tdMode"] = serde_json::json!("isolated");
        assert_eq!(
            parse_ws_orders(
                &serde_json::to_vec(&wrong_order_mode)?,
                &active,
                17,
                OkxEventWindow::new(1_787_911_201_000, None)?,
            ),
            Err(OkxError::Binding)
        );
        let mut wrong_position_mode: serde_json::Value = serde_json::from_slice(POSITION_PUSH)?;
        wrong_position_mode["data"][0]["mgnMode"] = serde_json::json!("isolated");
        assert_eq!(
            parse_ws_positions(
                &serde_json::to_vec(&wrong_position_mode)?,
                &active,
                17,
                &mut OkxPositionSnapshotState::default(),
                OkxEventWindow::new(1_787_911_201_000, None)?,
            ),
            Err(OkxError::Binding)
        );
        Ok(())
    }

    #[test]
    fn snapshot_pages_must_close_and_empty_closed_snapshot_is_explicit()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, _, _, active) = active(GatewayMode::Live)?;
        let mut first: serde_json::Value = serde_json::from_slice(ACCOUNT_PUSH)?;
        first["lastPage"] = serde_json::json!(false);
        let mut state = OkxAccountSnapshotState::default();
        assert_eq!(
            parse_ws_account(
                &serde_json::to_vec(&first)?,
                &active,
                17,
                &mut state,
                OkxEventWindow::new(1_787_911_201_000, None)?
            )?,
            OkxWsDelivery::PendingSnapshot { next_page: 2 }
        );
        let mut next_generation = active.clone();
        next_generation.scope.private_generation = 18;
        assert_eq!(
            parse_ws_account(
                br#"{"arg":{"channel":"account","uid":"fixture-sub-account"},"eventType":"snapshot","curPage":2,"lastPage":true,"data":[]}"#,
                &next_generation,
                18,
                &mut state.clone(),
                OkxEventWindow::new(1_787_911_201_000, None)?,
            ),
            Err(OkxError::Pagination)
        );
        let empty = br#"{"arg":{"channel":"account","uid":"fixture-sub-account"},"eventType":"snapshot","curPage":2,"lastPage":true,"data":[]}"#;
        let OkxWsDelivery::Batch(closed) = parse_ws_account(
            empty,
            &active,
            17,
            &mut state,
            OkxEventWindow::new(1_787_911_201_000, None)?,
        )?
        else {
            return Err("snapshot not closed".into());
        };
        assert_eq!(closed.items.len(), 1);

        let empty_first = empty.to_vec();
        let empty_first = String::from_utf8(empty_first)?.replace("\"curPage\":2", "\"curPage\":1");
        let OkxWsDelivery::Batch(empty_closed) = parse_ws_account(
            empty_first.as_bytes(),
            &active,
            17,
            &mut OkxAccountSnapshotState::default(),
            OkxEventWindow::new(1_787_911_201_000, None)?,
        )?
        else {
            return Err("empty snapshot not closed".into());
        };
        assert!(empty_closed.items.is_empty());
        assert_eq!(empty_closed.event_time_ms, None);
        Ok(())
    }
}
