use std::{
    collections::BTreeSet,
    net::TcpStream,
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use hmac::{Hmac, Mac};
use reqwest::{
    StatusCode,
    blocking::{Client, Response},
};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256, Sha512};
use tungstenite::{
    Message, WebSocket, client::IntoClientRequest, http::HeaderValue, stream::MaybeTlsStream,
};

use crate::domain::{
    AccountBalance, Amount, Asset, FieldState, Fill, Instrument, MarketKind, MarketOrderCommand,
    MarketReduceCommand, Order, OrderCommand, OrderSide, OrderState, Position, PositionSide, Price,
    Symbol,
};
use crate::exchange::websocket;
pub use venue_gateway_gate::{GateContractRules, GateRiskAccountMode, GateRiskReadback};
use venue_gateway_gate::{decimal, decimal_value, object, optional_price, text};

const API_BASE_URL: &str = "https://api.gateio.ws/api/v4";
const SETTLE: &str = "usdt";
const CLIENT_ORDER_PREFIX: &str = "t-";
const MAX_CLIENT_ORDER_SUFFIX_BYTES: usize = 28;
const READBACK_PAGE_SIZE: usize = 100;
const MAX_READBACK_PAGES: usize = 1_000;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const WS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const PUBLIC_READINESS_TIMEOUT: Duration = Duration::from_millis(1);
const MAX_PUBLIC_CONTROL_FRAMES_PER_READ: usize = 32;

type HmacSha512 = Hmac<Sha512>;

/// Credentials are process-only. They deliberately implement neither `Debug` nor serialization.
#[derive(Clone)]
pub struct GateCredentials {
    key: String,
    secret: String,
}

impl GateCredentials {
    pub fn from_environment() -> Result<Self, GateError> {
        let key = crate::credential_env::required("GATEIO_API_KEY")
            .map_err(|_| GateError::Credentials)?;
        let secret = crate::credential_env::required("GATEIO_API_SECRET")
            .map_err(|_| GateError::Credentials)?;
        Ok(Self { key, secret })
    }

    /// A non-secret account-key binding for append-only capability evidence.
    pub fn api_key_sha256(&self) -> String {
        hex(&Sha256::digest(self.key.as_bytes()))
    }
}

trait GateContractRulesCompat {
    fn native_contracts(&self, quantity: Decimal) -> Result<Decimal, GateError>;
}

impl GateContractRulesCompat for GateContractRules {
    fn native_contracts(&self, quantity: Decimal) -> Result<Decimal, GateError> {
        self.native_contracts_checked(quantity).map_err(Into::into)
    }
}

pub fn parse_risk_snapshots(
    account_value: &Value,
    position_values: &[Value],
    symbol: &Symbol,
    rules: &GateContractRules,
    account: &str,
    private_generation: u64,
    observed_at_ms: u64,
) -> Result<
    (
        GateRiskAccountMode,
        crate::domain::AccountRiskSnapshot,
        Vec<crate::domain::LegRiskSnapshot>,
    ),
    GateError,
> {
    venue_gateway_gate::parse_risk_snapshots(
        account_value,
        position_values,
        symbol,
        rules,
        account,
        private_generation,
        observed_at_ms,
    )
    .map_err(Into::into)
}

pub(crate) mod gate_risk {
    use super::*;

    pub(crate) fn validate_risk_readback_window(
        started_at_ms: u64,
        observed_at_ms: u64,
    ) -> Result<(), GateError> {
        venue_gateway_gate::validate_risk_readback_window(started_at_ms, observed_at_ms)
            .map_err(Into::into)
    }

    pub(crate) fn requires_unified_single_currency(value: &Value) -> Result<bool, GateError> {
        venue_gateway_gate::requires_unified_single_currency(value).map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn parse_risk_snapshots_with_unified(
        account_value: &Value,
        unified_mode_value: &Value,
        unified_account_value: &Value,
        position_values: &[Value],
        symbol: &Symbol,
        rules: &GateContractRules,
        account: &str,
        private_generation: u64,
        observed_at_ms: u64,
    ) -> Result<
        (
            GateRiskAccountMode,
            crate::domain::AccountRiskSnapshot,
            Vec<crate::domain::LegRiskSnapshot>,
        ),
        GateError,
    > {
        venue_gateway_gate::parse_risk_snapshots_with_unified(
            account_value,
            unified_mode_value,
            unified_account_value,
            position_values,
            symbol,
            rules,
            account,
            private_generation,
            observed_at_ms,
        )
        .map_err(Into::into)
    }
}

pub struct GatePublicRest {
    client: Client,
}

/// Credential-free Gate futures public transport. It exposes raw data only; the stage-7 public
/// capture boundary persists and normalizes every frame before any quote is consumed.
pub(crate) struct GatePublicStream {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
    last_heartbeat_at: Instant,
}

impl GatePublicRest {
    pub fn production() -> Result<Self, GateError> {
        let client = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .map_err(|_| GateError::Http)?;
        Ok(Self { client })
    }

    pub fn server_time_ms(&self) -> Result<u64, GateError> {
        let value = self.get_json("/spot/time")?;
        timestamp_ms(value.get("server_time"))
    }

    pub fn contract_rules(
        &self,
        symbol: &Symbol,
        generation: u64,
    ) -> Result<GateContractRules, GateError> {
        let native_symbol = native_symbol(symbol)?;
        let value = self.get_json(&format!("/futures/{SETTLE}/contracts/{native_symbol}"))?;
        parse_contract_rules(&value, symbol.clone(), generation)
    }

    pub fn best_bid_ask(&self, symbol: &Symbol) -> Result<(Price, Price), GateError> {
        let native_symbol = native_symbol(symbol)?;
        let value = self.get_json(&format!(
            "/futures/{SETTLE}/order_book?contract={}&limit=1",
            encode_component(&native_symbol)
        ))?;
        parse_best_bid_ask(&value)
    }

    pub(crate) fn order_book_snapshot_raw(
        &self,
        path_and_query: &str,
    ) -> Result<String, GateError> {
        response_text(
            self.client
                .get(format!("{API_BASE_URL}{path_and_query}"))
                .header("Accept", "application/json")
                .header("X-Gate-Size-Decimal", "1")
                .send()
                .map_err(|_| GateError::Http)?,
        )
    }

    fn get_json(&self, path_and_query: &str) -> Result<Value, GateError> {
        let response = self
            .client
            .get(format!("{API_BASE_URL}{path_and_query}"))
            .header("Accept", "application/json")
            .send()
            .map_err(|_| GateError::Http)?;
        response_json(response)
    }
}

impl GatePublicStream {
    pub(crate) fn connect(
        binding: &crate::exchange::gate_public::GatePublicBinding,
    ) -> Result<Self, GateError> {
        let mut request = "wss://fx-ws.gateio.ws/v4/ws/usdt"
            .into_client_request()
            .map_err(|_| GateError::WebSocket)?;
        request
            .headers_mut()
            .insert("X-Gate-Size-Decimal", HeaderValue::from_static("1"));
        let (mut socket, _) = websocket::connect_tls(request).map_err(|_| GateError::WebSocket)?;
        let requests = crate::exchange::gate_public::grid_public_subscriptions(binding, 100, 20)
            .map_err(|_| GateError::WebSocket)?;
        let requests = requests.as_array().ok_or(GateError::WebSocket)?;
        let expected = requests
            .iter()
            .map(|request| {
                request
                    .get("channel")
                    .and_then(Value::as_str)
                    .ok_or(GateError::WebSocket)
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        for request in requests {
            let mut request = request.clone();
            request
                .as_object_mut()
                .ok_or(GateError::WebSocket)?
                .insert("time".to_owned(), Value::from(wall_clock_ms()? / 1_000));
            socket
                .send(Message::Text(
                    serde_json::to_string(&request)
                        .map_err(|_| GateError::WebSocket)?
                        .into(),
                ))
                .map_err(|_| GateError::WebSocket)?;
        }
        let mut stream = Self {
            socket,
            last_heartbeat_at: Instant::now(),
        };
        stream.set_read_timeout(Duration::from_secs(5))?;
        stream.await_subscriptions(&expected)?;
        stream.set_read_timeout(PUBLIC_READINESS_TIMEOUT)?;
        Ok(stream)
    }

    fn set_read_timeout(&mut self, timeout: Duration) -> Result<(), GateError> {
        let result = match self.socket.get_mut() {
            MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(timeout)),
            MaybeTlsStream::Rustls(stream) => stream.sock.set_read_timeout(Some(timeout)),
            _ => return Err(GateError::WebSocket),
        };
        result.map_err(|_| GateError::WebSocket)
    }

    fn await_subscriptions(&mut self, expected: &BTreeSet<&str>) -> Result<(), GateError> {
        let mut acknowledged = BTreeSet::new();
        while acknowledged.len() < expected.len() {
            match self.socket.read() {
                Ok(Message::Text(raw_text)) => {
                    let value = parse_json(&raw_text)?;
                    let object = object(&value)?;
                    if !object.get("error").is_none_or(Value::is_null) {
                        return Err(GateError::WebSocket);
                    }
                    if object.get("event").and_then(Value::as_str) != Some("subscribe") {
                        continue;
                    }
                    let channel = text(object, "channel")?;
                    if !expected.contains(channel) {
                        return Err(GateError::WebSocket);
                    }
                    acknowledged.insert(channel.to_owned());
                }
                Ok(Message::Ping(payload)) => self
                    .socket
                    .send(Message::Pong(payload))
                    .map_err(|_| GateError::WebSocket)?,
                Ok(Message::Close(_)) => return Err(GateError::StreamClosed),
                Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => {}
                Err(tungstenite::Error::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    return Err(GateError::WebSocket);
                }
                Err(_) => return Err(GateError::WebSocket),
            }
        }
        Ok(())
    }

    pub(crate) fn next_raw_event(&mut self) -> Result<Option<String>, GateError> {
        send_ws_heartbeat_if_due(&mut self.socket, &mut self.last_heartbeat_at)?;
        for _ in 0..MAX_PUBLIC_CONTROL_FRAMES_PER_READ {
            match self.socket.read() {
                Ok(Message::Text(text)) => {
                    let value = parse_json(&text)?;
                    let object = object(&value)?;
                    if !object.get("error").is_none_or(Value::is_null) {
                        return Err(GateError::WebSocket);
                    }
                    if object.get("channel").and_then(Value::as_str) == Some("futures.pong") {
                        continue;
                    }
                    return Ok(Some(text.to_string()));
                }
                Ok(Message::Ping(payload)) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .map_err(|_| GateError::WebSocket)?;
                }
                Ok(Message::Close(_)) => return Err(GateError::StreamClosed),
                Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => {}
                Err(tungstenite::Error::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    return Ok(None);
                }
                Err(_) => return Err(GateError::WebSocket),
            }
        }
        Err(GateError::WebSocket)
    }
}

/// Gate Futures REST adapter for the exact hedged-grid command surface. It carries no strategy
/// state and receives only normalized commands at this boundary.
#[derive(Clone)]
pub struct GatePrivateRest {
    client: Client,
    credentials: GateCredentials,
    server_offset_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatePrivateReadback {
    pub user_id: String,
    pub raw_payloads: Vec<String>,
    pub signed_regular_order_payloads: Vec<String>,
    pub balance: AccountBalance,
    pub dual_position_mode: bool,
    pub positions: Vec<Position>,
    pub orders: Vec<Order>,
    pub fills: Vec<GateFill>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

pub struct GatePrivateStream {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
    last_heartbeat_at: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatePrivateEvent {
    Fill { value: Value, raw_payload: String },
    Order { raw_payload: String },
    Position { raw_payload: String },
    Balance { raw_payload: String },
}

impl GatePrivateRest {
    pub fn production(credentials: GateCredentials) -> Result<Self, GateError> {
        let public = GatePublicRest::production()?;
        let local_time_ms = wall_clock_ms()?;
        // Gate accepts a normal current Unix timestamp.  The public time route is preferred to
        // remove local skew, but a malformed time payload must not prevent a signed readback or
        // emergency flatten when the local clock itself is available.  Other transport failures
        // remain fatal; this fallback is deliberately limited to the time-payload boundary.
        let server_offset_ms = match public.server_time_ms() {
            Ok(server_time_ms) => {
                let offset = i128::from(server_time_ms) - i128::from(local_time_ms);
                i64::try_from(offset).map_err(|_| GateError::Clock)?
            }
            Err(GateError::Clock) => 0,
            Err(error) => return Err(error),
        };
        Ok(Self {
            client: public.client,
            credentials,
            server_offset_ms,
        })
    }

    pub fn credentials_api_key_sha256(&self) -> String {
        self.credentials.api_key_sha256()
    }

    pub fn place_limit_post_only(
        &self,
        command: &OrderCommand,
        rules: &GateContractRules,
    ) -> Result<String, GateError> {
        command.validate().map_err(|_| GateError::Command)?;
        if command.owner.symbol != rules.instrument.symbol
            || command.reduce_only != is_reduce(command)
        {
            return Err(GateError::Command);
        }
        let size = signed_contracts(rules.native_contracts(command.quantity)?, command.side)?;
        let body = json!({
            "contract": rules.native_symbol,
            "size": decimal_string(size),
            "price": decimal_string(command.limit_price.value()),
            "tif": "poc",
            "reduce_only": command.reduce_only,
            "text": native_client_order_id(command.client_order_id.as_str())?,
        });
        let order = self.signed_json("POST", "/futures/usdt/orders", "", Some(&body))?;
        exact_accepted_order_id(&order, command.client_order_id.as_str())
    }

    pub fn place_market(
        &self,
        command: &MarketOrderCommand,
        rules: &GateContractRules,
    ) -> Result<String, GateError> {
        command.validate().map_err(|_| GateError::Command)?;
        if command.owner.symbol != rules.instrument.symbol || command.reduce_only {
            return Err(GateError::Command);
        }
        let size = signed_contracts(rules.native_contracts(command.quantity)?, command.side)?;
        let body = json!({
            "contract": rules.native_symbol,
            "size": decimal_string(size),
            "price": "0",
            "tif": "ioc",
            "reduce_only": false,
            "text": native_client_order_id(command.client_order_id.as_str())?,
        });
        let order = self.signed_json("POST", "/futures/usdt/orders", "", Some(&body))?;
        exact_accepted_order_id(&order, command.client_order_id.as_str())
    }

    /// Canary-only proof that a market reduction cannot reverse the independently read leg.
    pub fn place_reduce_only_market(
        &self,
        command: &OrderCommand,
        rules: &GateContractRules,
    ) -> Result<String, GateError> {
        command.validate().map_err(|_| GateError::Command)?;
        if command.owner.symbol != rules.instrument.symbol
            || !command.reduce_only
            || !is_reduce(command)
        {
            return Err(GateError::Command);
        }
        let size = signed_contracts(rules.native_contracts(command.quantity)?, command.side)?;
        let body = json!({
            "contract": rules.native_symbol,
            "size": decimal_string(size),
            "price": "0",
            "tif": "ioc",
            "reduce_only": true,
            "text": native_client_order_id(command.client_order_id.as_str())?,
        });
        let order = self.signed_json("POST", "/futures/usdt/orders", "", Some(&body))?;
        exact_accepted_order_id(&order, command.client_order_id.as_str())
    }

    pub fn place_market_reduce(
        &self,
        command: &MarketReduceCommand,
        rules: &GateContractRules,
    ) -> Result<String, GateError> {
        let body = market_reduce_body(command, rules)?;
        let order = self.signed_json("POST", "/futures/usdt/orders", "", Some(&body))?;
        exact_accepted_order_id(&order, command.client_order_id.as_str())
    }

    pub fn order_by_client_id(
        &self,
        symbol: &Symbol,
        client_order_id: &str,
    ) -> Result<Value, GateError> {
        let _native = native_symbol(symbol)?;
        let text = native_client_order_id(client_order_id)?;
        // Gate accepts the `t-` custom identity in the single-order path. Listing orders and
        // filtering locally would be ambiguous after pagination or concurrent foreign activity.
        // A transport or business rejection is not proof the client identity is absent. Preserve
        // it as Unknown so WAL recovery never clears an ambiguous place/cancel by assumption.
        exact_order_result(self.signed_json(
            "GET",
            &format!("/futures/usdt/orders/{}", encode_component(&text)),
            "",
            None,
        ))
    }

    pub fn cancel_by_client_id(
        &self,
        symbol: &Symbol,
        client_order_id: &str,
    ) -> Result<String, GateError> {
        let order = self.order_by_client_id(symbol, client_order_id)?;
        let order_id = identifier(order.get("id"))?;
        let value = self.signed_json(
            "DELETE",
            &format!("/futures/usdt/orders/{order_id}"),
            "",
            None,
        )?;
        exact_accepted_order_id(&value, client_order_id)
    }

    pub fn verify_post_only_order_by_client_id(
        &self,
        symbol: &Symbol,
        client_order_id: &str,
    ) -> Result<(), GateError> {
        let order = self.order_by_client_id(symbol, client_order_id)?;
        if is_post_only_order(&order) {
            Ok(())
        } else {
            Err(GateError::Command)
        }
    }

    /// A complete signed readback proves read access, not mutation permission.
    pub fn readback(
        &self,
        symbol: &Symbol,
        rules: &GateContractRules,
    ) -> Result<GatePrivateReadback, GateError> {
        let native = native_symbol(symbol)?;
        let account_raw = self.signed_text("GET", "/futures/usdt/accounts", "", None)?;
        let positions_raw = self.signed_text(
            "GET",
            &format!("/futures/usdt/dual_comp/positions/{native}"),
            "",
            None,
        )?;
        let (orders_raw, orders) = self.read_all_open_orders(&native)?;
        let (fills_raw, fills) = self.read_all_fills(&native)?;
        let account = parse_json(&account_raw).map_err(|_| GateError::PrivatePayload("account"))?;
        let positions =
            parse_json(&positions_raw).map_err(|_| GateError::PrivatePayload("positions"))?;
        let positions = positions
            .as_array()
            .ok_or(GateError::PrivatePayload("positions"))?;
        // `holding=false` returns the virtual zero-size record as well. Its user identifier is
        // the signed account-to-private-stream binding; the futures account payload itself does
        // not contain a UID.
        let user_id = positions
            .iter()
            .find(|position| {
                position
                    .get("contract")
                    .and_then(Value::as_str)
                    .is_some_and(|contract| contract == native)
            })
            .and_then(|position| object(position).ok())
            .and_then(|position| position.get("user"))
            .and_then(|value| identifier(Some(value)).ok())
            .ok_or(GateError::PrivatePayload("positions"))?;
        let positions = positions
            .iter()
            .filter(|position| {
                position
                    .get("contract")
                    .and_then(Value::as_str)
                    .is_some_and(|contract| contract == native)
            })
            .map(|position| {
                parse_position(position, symbol, rules)
                    .map_err(|_| GateError::PrivatePayload("positions"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let orders = orders
            .iter()
            .map(|order| {
                parse_order(order, symbol, rules).map_err(|_| GateError::PrivatePayload("orders"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fills = fills
            .iter()
            .map(|fill| {
                Ok::<GateFill, GateError>(GateFill {
                    fill: parse_fill(fill, symbol, rules)
                        .map_err(|_| GateError::PrivatePayload("fills"))?,
                    client_order_id: parse_fill_client_order_id(fill)
                        .map_err(|_| GateError::PrivatePayload("fills"))?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let balance =
            parse_account_balance(&account).map_err(|_| GateError::PrivatePayload("account"))?;
        let dual_position_mode =
            parse_dual_position_mode(&account).map_err(|_| GateError::PrivatePayload("account"))?;
        Ok(GatePrivateReadback {
            user_id,
            raw_payloads: {
                let mut payloads = vec![account_raw, positions_raw];
                payloads.extend(orders_raw.clone());
                payloads.extend(fills_raw);
                payloads
            },
            signed_regular_order_payloads: orders_raw,
            balance,
            dual_position_mode,
            positions,
            orders,
            fills,
        })
    }

    pub fn connect_private_stream(
        &self,
        user_id: &str,
        symbol: &Symbol,
    ) -> Result<GatePrivateStream, GateError> {
        let native = native_symbol(symbol)?;
        GatePrivateStream::connect(
            &self.credentials,
            self.authoritative_now_ms()? / 1_000,
            user_id,
            &native,
        )
    }

    pub fn risk_readback(
        &self,
        symbol: &Symbol,
        rules: &GateContractRules,
        account: &str,
        private_generation: u64,
    ) -> Result<GateRiskReadback, GateError> {
        let native = native_symbol(symbol)?;
        let started_at_ms = self.authoritative_now_ms()?;
        let account_raw = self.signed_text("GET", "/futures/usdt/accounts", "", None)?;
        let positions_raw = self.signed_text(
            "GET",
            &format!("/futures/usdt/dual_comp/positions/{native}"),
            "",
            None,
        )?;
        let account_value = parse_json(&account_raw)?;
        let position_value = parse_json(&positions_raw)?;
        let positions = position_value.as_array().ok_or(GateError::RiskSnapshot)?;
        let needs_unified = gate_risk::requires_unified_single_currency(&account_value)?;
        let (unified_mode_raw, unified_account_raw) = if needs_unified {
            (
                Some(self.signed_text("GET", "/unified/unified_mode", "", None)?),
                Some(self.signed_text("GET", "/unified/accounts", "currency=USDT", None)?),
            )
        } else {
            (None, None)
        };
        let observed_at_ms = self.authoritative_now_ms()?;
        gate_risk::validate_risk_readback_window(started_at_ms, observed_at_ms)?;
        let (account_mode, account, legs) = match (&unified_mode_raw, &unified_account_raw) {
            (Some(mode), Some(unified)) => gate_risk::parse_risk_snapshots_with_unified(
                &account_value,
                &parse_json(mode)?,
                &parse_json(unified)?,
                positions,
                symbol,
                rules,
                account,
                private_generation,
                observed_at_ms,
            )?,
            (None, None) => parse_risk_snapshots(
                &account_value,
                positions,
                symbol,
                rules,
                account,
                private_generation,
                observed_at_ms,
            )?,
            _ => return Err(GateError::RiskAccountMode),
        };
        let mut raw_payloads = vec![account_raw, positions_raw];
        raw_payloads.extend(unified_mode_raw);
        raw_payloads.extend(unified_account_raw);
        Ok(GateRiskReadback {
            raw_payloads,
            account_mode,
            account,
            legs,
        })
    }

    fn read_all_open_orders(&self, native: &str) -> Result<(Vec<String>, Vec<Value>), GateError> {
        self.read_all_pages(native, "/futures/usdt/orders", "status=open")
    }

    fn read_all_fills(&self, native: &str) -> Result<(Vec<String>, Vec<Value>), GateError> {
        self.read_all_pages(native, "/futures/usdt/my_trades", "")
    }

    /// A repeated immutable pagination ID is ambiguous and therefore rejected.
    fn read_all_pages(
        &self,
        native: &str,
        path: &str,
        fixed_query: &str,
    ) -> Result<(Vec<String>, Vec<Value>), GateError> {
        let mut payloads = Vec::new();
        let mut values = Vec::new();
        let mut seen_ids = BTreeSet::new();
        let mut cursor = None;
        for _ in 0..MAX_READBACK_PAGES {
            let mut query = format!(
                "contract={}&limit={READBACK_PAGE_SIZE}",
                encode_component(native)
            );
            if !fixed_query.is_empty() {
                query.push('&');
                query.push_str(fixed_query);
            }
            if let Some(last_id) = cursor.as_deref() {
                query.push_str("&last_id=");
                query.push_str(&encode_component(last_id));
            }
            let payload = self.signed_text("GET", path, &query, None)?;
            let json = parse_json(&payload)?;
            let page = json.as_array().ok_or(GateError::Payload)?;
            let page_len = page.len();
            let mut last_id = None;
            for value in page {
                let id = identifier(value.get("id"))?;
                if !seen_ids.insert(id.clone()) {
                    return Err(GateError::Pagination);
                }
                last_id = Some(id);
                values.push(value.clone());
            }
            payloads.push(payload);
            if page_len < READBACK_PAGE_SIZE {
                return Ok((payloads, values));
            }
            cursor = last_id;
        }
        Err(GateError::Pagination)
    }

    fn signed_json(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<&Value>,
    ) -> Result<Value, GateError> {
        let text = self.signed_text(method, path, query, body)?;
        parse_json(&text)
    }

    fn signed_text(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<&Value>,
    ) -> Result<String, GateError> {
        let body_bytes = match body {
            Some(value) => serde_json::to_vec(value).map_err(|_| GateError::Payload)?,
            None => Vec::new(),
        };
        let now_ms = self.authoritative_now_ms()?;
        let timestamp = (now_ms / 1_000).to_string();
        let signature = gate_signature(
            &self.credentials.secret,
            method,
            path,
            query,
            &body_bytes,
            &timestamp,
        )?;
        let url = if query.is_empty() {
            format!("{API_BASE_URL}{path}")
        } else {
            format!("{API_BASE_URL}{path}?{query}")
        };
        let mut request = self
            .client
            .request(method.parse().map_err(|_| GateError::Payload)?, url)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("KEY", &self.credentials.key)
            .header("Timestamp", timestamp)
            .header("SIGN", signature)
            .header("X-Gate-Size-Decimal", "1");
        if !body_bytes.is_empty() {
            request = request.body(body_bytes);
        }
        response_text(request.send().map_err(|_| GateError::Http)?)
    }

    fn authoritative_now_ms(&self) -> Result<u64, GateError> {
        let local = i128::from(wall_clock_ms()?);
        let adjusted = local + i128::from(self.server_offset_ms);
        u64::try_from(adjusted).map_err(|_| GateError::Clock)
    }
}

fn exact_order_result(result: Result<Value, GateError>) -> Result<Value, GateError> {
    match result {
        Err(GateError::Rejected { label }) if label == "ORDER_NOT_FOUND" => {
            Err(GateError::OrderAbsent)
        }
        result => result,
    }
}

impl GatePrivateStream {
    fn connect(
        credentials: &GateCredentials,
        timestamp: u64,
        user_id: &str,
        native_symbol: &str,
    ) -> Result<Self, GateError> {
        if user_id.trim().is_empty() {
            return Err(GateError::Payload);
        }
        let mut request = "wss://fx-ws.gateio.ws/v4/ws/usdt"
            .into_client_request()
            .map_err(|_| GateError::WebSocket)?;
        request
            .headers_mut()
            .insert("X-Gate-Size-Decimal", HeaderValue::from_static("1"));
        let (mut socket, _) = websocket::connect_tls(request).map_err(|_| GateError::WebSocket)?;
        let expected_channels = private_subscription_channels(user_id, &native_symbol);
        for (channel, payload) in expected_channels.iter().cloned() {
            let message = private_subscription(credentials, timestamp, channel, payload)?;
            socket
                .send(Message::Text(message.into()))
                .map_err(|_| GateError::WebSocket)?;
        }
        let mut stream = Self {
            socket,
            last_heartbeat_at: Instant::now(),
        };
        stream.set_read_timeout(Duration::from_secs(5))?;
        let expected = expected_channels
            .into_iter()
            .map(|(channel, _)| channel)
            .collect::<BTreeSet<_>>();
        stream.await_subscriptions(&expected)?;
        // The runtime must never block its control/reconciliation loop on an idle private feed.
        stream.set_read_timeout(Duration::from_millis(100))?;
        Ok(stream)
    }

    pub fn set_read_timeout(&mut self, timeout: Duration) -> Result<(), GateError> {
        let result = match self.socket.get_mut() {
            MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(timeout)),
            MaybeTlsStream::Rustls(stream) => stream.sock.set_read_timeout(Some(timeout)),
            _ => return Err(GateError::WebSocket),
        };
        result.map_err(|_| GateError::WebSocket)
    }

    fn await_subscriptions(&mut self, expected: &BTreeSet<&str>) -> Result<(), GateError> {
        let mut acknowledged = BTreeSet::new();
        while acknowledged.len() < expected.len() {
            match self.socket.read() {
                Ok(Message::Text(text)) => {
                    if let Some(channel) = parse_private_subscription_ack(&text)? {
                        if !expected.contains(channel.as_str()) {
                            return Err(GateError::WebSocket);
                        }
                        acknowledged.insert(channel);
                    }
                }
                Ok(Message::Ping(payload)) => self
                    .socket
                    .send(Message::Pong(payload))
                    .map_err(|_| GateError::WebSocket)?,
                Ok(Message::Close(_)) => return Err(GateError::StreamClosed),
                Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => {}
                Err(tungstenite::Error::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    return Err(GateError::WebSocket);
                }
                Err(_) => return Err(GateError::WebSocket),
            }
        }
        Ok(())
    }

    /// Receives at most one frame. A read timeout represents no new event; a close, malformed
    /// payload or remote error is a generation-fencing failure for the caller.
    pub fn next_event_when_ready(&mut self) -> Result<Option<GatePrivateEvent>, GateError> {
        send_ws_heartbeat_if_due(&mut self.socket, &mut self.last_heartbeat_at)?;
        match self.socket.read() {
            Ok(Message::Text(text)) => parse_private_event(&text),
            Ok(Message::Ping(payload)) => {
                self.socket
                    .send(Message::Pong(payload))
                    .map_err(|_| GateError::WebSocket)?;
                Ok(None)
            }
            Ok(Message::Close(_)) => Err(GateError::StreamClosed),
            Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => Ok(None),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(_) => Err(GateError::WebSocket),
        }
    }
}

pub fn native_symbol(symbol: &Symbol) -> Result<String, GateError> {
    if symbol.quote() != "USDT" {
        return Err(GateError::Symbol);
    }
    Ok(format!("{}_USDT", symbol.base()))
}

pub fn parse_contract_rules(
    value: &Value,
    symbol: Symbol,
    generation: u64,
) -> Result<GateContractRules, GateError> {
    if generation == 0 || !matches!(symbol.quote(), "USDT") {
        return Err(GateError::Instrument);
    }
    let object = object(value)?;
    let native = text(object, "name")?.to_owned();
    if native != native_symbol(&symbol)?
        || bool_field(object, "in_delisting")?
        || matches!(
            object.get("status").and_then(Value::as_str),
            Some("delisted" | "offline")
        )
    {
        return Err(GateError::Instrument);
    }
    let quanto_multiplier = decimal(object, "quanto_multiplier")?;
    let minimum_contracts = decimal(object, "order_size_min")?.max(Decimal::ONE);
    let quantity_step = quanto_multiplier;
    let price_tick =
        Price::new(decimal(object, "order_price_round")?).map_err(|_| GateError::Instrument)?;
    let instrument = Instrument {
        symbol,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(Asset::new("USDT").map_err(|_| GateError::Instrument)?),
        generation,
        price_tick,
        quantity_step,
        minimum_notional: Amount::new(
            Asset::new("USDT").map_err(|_| GateError::Instrument)?,
            Decimal::ZERO,
        ),
    };
    instrument.validate().map_err(|_| GateError::Instrument)?;
    Ok(GateContractRules {
        native_symbol: native,
        instrument,
        quanto_multiplier,
        minimum_contracts,
        decimal_contracts: bool_field(object, "enable_decimal")?,
    })
}

pub fn parse_best_bid_ask(value: &Value) -> Result<(Price, Price), GateError> {
    let object = object(value)?;
    let bid = book_price(object.get("bids"))?;
    let ask = book_price(object.get("asks"))?;
    if bid >= ask {
        return Err(GateError::Payload);
    }
    Ok((bid, ask))
}

pub fn parse_order(
    value: &Value,
    symbol: &Symbol,
    rules: &GateContractRules,
) -> Result<Order, GateError> {
    let object = object(value)?;
    if text(object, "contract")? != rules.native_symbol || symbol != &rules.instrument.symbol {
        return Err(GateError::Symbol);
    }
    let signed_size = decimal(object, "size")?;
    let signed_left = decimal(object, "left")?;
    let quantity = signed_size.abs() * rules.quanto_multiplier;
    let left = signed_left.abs() * rules.quanto_multiplier;
    if left > quantity {
        return Err(GateError::Payload);
    }
    let reduce_only = bool_field(object, "is_reduce_only")?;
    let side = if signed_size.is_sign_positive() {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    };
    let position_side = match (reduce_only, side) {
        (false, OrderSide::Buy) | (true, OrderSide::Sell) => PositionSide::Long,
        (false, OrderSide::Sell) | (true, OrderSide::Buy) => PositionSide::Short,
    };
    let state = gate_order_state(object, quantity, left)?;
    let order = Order {
        order_id: identifier(object.get("id"))?,
        client_order_id: gate_client_order_id(object.get("text")),
        symbol: symbol.clone(),
        side,
        position_side: FieldState::Known(position_side),
        purpose: FieldState::Missing,
        state,
        quantity,
        filled_quantity: quantity - left,
        limit_price: optional_price(object.get("price"))?,
        average_price: optional_price_state(object.get("fill_price"))?,
        reduce_only,
    };
    order.validate().map_err(|_| GateError::Payload)?;
    Ok(order)
}

pub fn parse_position(
    value: &Value,
    symbol: &Symbol,
    rules: &GateContractRules,
) -> Result<Position, GateError> {
    venue_gateway_gate::parse_position(value, symbol, rules).map_err(Into::into)
}

pub fn parse_fill(
    value: &Value,
    symbol: &Symbol,
    rules: &GateContractRules,
) -> Result<Fill, GateError> {
    let object = object(value)?;
    if text(object, "contract")? != rules.native_symbol || symbol != &rules.instrument.symbol {
        return Err(GateError::Symbol);
    }
    let signed_size = decimal(object, "size")?;
    let side = if signed_size.is_sign_positive() {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    };
    let position_side = fill_position_side(object.get("text"));
    let fill_id = identifier(object.get("id"))?;
    let fill = Fill {
        execution_sequence: execution_sequence(&fill_id),
        fill_id,
        order_id: identifier(object.get("order_id"))?,
        symbol: symbol.clone(),
        side,
        position_side,
        quantity: signed_size.abs() * rules.quanto_multiplier,
        price: Price::new(decimal(object, "price")?).map_err(|_| GateError::Payload)?,
        fee: optional_usdt_amount(object.get("fee"))?,
        realized_pnl: optional_usdt_amount(object.get("pnl"))?,
        maker: optional_maker(object.get("role")),
        exchange_time_ms: optional_timestamp_ms(
            object
                .get("create_time_ms")
                .or_else(|| object.get("create_time")),
        )?,
    };
    fill.validate().map_err(|_| GateError::Payload)?;
    Ok(fill)
}

fn execution_sequence(fill_id: &str) -> FieldState<u64> {
    fill_id
        .parse::<u64>()
        .map(FieldState::Known)
        .unwrap_or(FieldState::Unavailable {
            reason: crate::domain::UnknownReason::ParseFailure,
        })
}

pub fn parse_fill_client_order_id(value: &Value) -> Result<FieldState<String>, GateError> {
    Ok(gate_client_order_id(object(value)?.get("text")))
}

fn fill_position_side(value: Option<&Value>) -> FieldState<PositionSide> {
    let Some(text) = value.and_then(Value::as_str) else {
        return FieldState::Unavailable {
            reason: crate::domain::UnknownReason::Ambiguous,
        };
    };
    if text.starts_with("t-ord-etp-") {
        return canonical_exposure_position_side(text).map_or(
            FieldState::Unavailable {
                reason: crate::domain::UnknownReason::Ambiguous,
            },
            FieldState::Known,
        );
    }
    if text.contains("_long_") {
        FieldState::Known(PositionSide::Long)
    } else if text.contains("_short_") {
        FieldState::Known(PositionSide::Short)
    } else {
        FieldState::Unavailable {
            reason: crate::domain::UnknownReason::Ambiguous,
        }
    }
}

/// Gate echoes Stage 7's risk-reduction client identifier verbatim.  Only the exact bounded
/// format is evidence of a hedge side; all other opaque ETP-looking text remains fail-closed.
fn canonical_exposure_position_side(text: &str) -> Option<PositionSide> {
    let (prefix, side) = if text.starts_with("t-ord-etp-l-") {
        ("t-ord-etp-l-", PositionSide::Long)
    } else if text.starts_with("t-ord-etp-s-") {
        ("t-ord-etp-s-", PositionSide::Short)
    } else {
        return None;
    };
    let suffix = text.strip_prefix(prefix)?;
    (suffix.len() == 16
        && suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')))
    .then_some(side)
}

pub fn parse_account_balance(value: &Value) -> Result<AccountBalance, GateError> {
    venue_gateway_gate::parse_account_balance(value).map_err(Into::into)
}

pub fn parse_dual_position_mode(value: &Value) -> Result<bool, GateError> {
    venue_gateway_gate::parse_dual_position_mode(value).map_err(Into::into)
}

fn signed_contracts(contracts: Decimal, side: OrderSide) -> Result<Decimal, GateError> {
    if !contracts.is_sign_positive() || contracts.is_zero() {
        return Err(GateError::Quantity);
    }
    Ok(match side {
        OrderSide::Buy => contracts,
        OrderSide::Sell => -contracts,
    })
}

fn market_reduce_body(
    command: &MarketReduceCommand,
    rules: &GateContractRules,
) -> Result<Value, GateError> {
    command.validate().map_err(|_| GateError::Command)?;
    if command.owner.exchange != "gate" || command.owner.symbol != rules.instrument.symbol {
        return Err(GateError::Command);
    }
    let size = signed_contracts(rules.native_contracts(command.quantity)?, command.side)?;
    Ok(json!({
        "contract": rules.native_symbol,
        "size": decimal_string(size),
        "price": "0",
        "tif": "ioc",
        "reduce_only": true,
        "text": native_client_order_id(command.client_order_id.as_str())?,
    }))
}

fn is_reduce(command: &OrderCommand) -> bool {
    matches!(
        command.owner.purpose,
        crate::domain::OrderPurpose::Protection
            | crate::domain::OrderPurpose::TakeProfit
            | crate::domain::OrderPurpose::Reduce
    )
}

fn native_client_order_id(client_order_id: &str) -> Result<String, GateError> {
    if client_order_id.len() > MAX_CLIENT_ORDER_SUFFIX_BYTES
        || client_order_id.is_empty()
        || !client_order_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(GateError::ClientOrderId);
    }
    Ok(format!("{CLIENT_ORDER_PREFIX}{client_order_id}"))
}

pub(crate) fn client_order_id_is_valid(client_order_id: &str) -> bool {
    native_client_order_id(client_order_id).is_ok()
}

fn gate_client_order_id(value: Option<&Value>) -> FieldState<String> {
    match value.and_then(Value::as_str) {
        Some(value) => match value.strip_prefix(CLIENT_ORDER_PREFIX) {
            Some(client_order_id) if !client_order_id.is_empty() => {
                FieldState::Known(client_order_id.to_owned())
            }
            _ => FieldState::Unavailable {
                reason: crate::domain::UnknownReason::Ambiguous,
            },
        },
        None => FieldState::Missing,
    }
}

fn exact_accepted_order_id(
    value: &Value,
    expected_client_order_id: &str,
) -> Result<String, GateError> {
    let object = object(value)?;
    match gate_client_order_id(object.get("text")) {
        FieldState::Known(client_order_id) if client_order_id == expected_client_order_id => {
            identifier(object.get("id"))
        }
        FieldState::Known(_)
        | FieldState::Missing
        | FieldState::Null
        | FieldState::Unavailable { .. }
        | FieldState::NotApplicable => Err(GateError::Payload),
    }
}

fn is_post_only_order(value: &Value) -> bool {
    object(value)
        .ok()
        .and_then(|order| order.get("tif"))
        .and_then(Value::as_str)
        .is_some_and(|tif| tif.eq_ignore_ascii_case("poc"))
}

fn gate_order_state(
    object: &Map<String, Value>,
    quantity: Decimal,
    left: Decimal,
) -> Result<OrderState, GateError> {
    match text(object, "status")? {
        "open" if left == quantity => Ok(OrderState::New),
        "open" if left > Decimal::ZERO => Ok(OrderState::PartiallyFilled),
        "finished" => match text(object, "finish_as")? {
            "filled" => Ok(OrderState::Filled),
            "cancelled" | "ioc" | "poc" | "stp" | "reduce_only" | "position_closed"
            | "reduce_out" => Ok(OrderState::Cancelled),
            "rejected" => Ok(OrderState::Rejected),
            _ => Ok(OrderState::Unknown),
        },
        _ => Ok(OrderState::Unknown),
    }
}

fn gate_signature(
    secret: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
    timestamp: &str,
) -> Result<String, GateError> {
    let body_hash = hex(&Sha512::digest(body));
    let payload = format!("{method}\n/api/v4{path}\n{query}\n{body_hash}\n{timestamp}");
    let mut mac =
        HmacSha512::new_from_slice(secret.as_bytes()).map_err(|_| GateError::Credentials)?;
    mac.update(payload.as_bytes());
    Ok(hex(&mac.finalize().into_bytes()))
}

fn private_subscription(
    credentials: &GateCredentials,
    timestamp: u64,
    channel: &str,
    payload: Value,
) -> Result<String, GateError> {
    let signature_payload = format!("channel={channel}&event=subscribe&time={timestamp}");
    let mut mac = HmacSha512::new_from_slice(credentials.secret.as_bytes())
        .map_err(|_| GateError::Credentials)?;
    mac.update(signature_payload.as_bytes());
    serde_json::to_string(&json!({
        "time": timestamp,
        "channel": channel,
        "event": "subscribe",
        "payload": payload,
        "auth": {
            "method": "api_key",
            "KEY": credentials.key,
            "SIGN": hex(&mac.finalize().into_bytes()),
        }
    }))
    .map_err(|_| GateError::Payload)
}

fn send_ws_heartbeat_if_due(
    socket: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    last_heartbeat_at: &mut Instant,
) -> Result<(), GateError> {
    if last_heartbeat_at.elapsed() < WS_HEARTBEAT_INTERVAL {
        return Ok(());
    }
    let timestamp = wall_clock_ms()? / 1_000;
    socket
        .send(Message::Ping(Vec::new().into()))
        .map_err(|_| GateError::WebSocket)?;
    socket
        .send(Message::Text(gate_futures_ping(timestamp)?.into()))
        .map_err(|_| GateError::WebSocket)?;
    *last_heartbeat_at = Instant::now();
    Ok(())
}

fn gate_futures_ping(timestamp: u64) -> Result<String, GateError> {
    serde_json::to_string(&json!({
        "time": timestamp,
        "channel": "futures.ping",
    }))
    .map_err(|_| GateError::Payload)
}

fn private_subscription_channels(user_id: &str, native_symbol: &str) -> [(&'static str, Value); 4] {
    [
        ("futures.orders", json!([user_id, native_symbol])),
        ("futures.usertrades", json!([user_id, native_symbol])),
        ("futures.positions", json!([user_id, native_symbol])),
        ("futures.balances", json!([user_id])),
    ]
}

fn parse_private_event(payload: &str) -> Result<Option<GatePrivateEvent>, GateError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| GateError::Payload)?;
    let object = object(&value)?;
    if object.get("error").is_some_and(|value| !value.is_null()) {
        return Err(GateError::WebSocket);
    }
    if object.get("event").and_then(Value::as_str) != Some("update") {
        return Ok(None);
    }
    let result = object.get("result").cloned().ok_or(GateError::Payload)?;
    let raw_payload = payload.to_owned();
    match object.get("channel").and_then(Value::as_str) {
        Some("futures.usertrades") => Ok(Some(GatePrivateEvent::Fill {
            value: result,
            raw_payload,
        })),
        Some("futures.orders") => Ok(Some(GatePrivateEvent::Order { raw_payload })),
        Some("futures.positions") => Ok(Some(GatePrivateEvent::Position { raw_payload })),
        Some("futures.balances") => Ok(Some(GatePrivateEvent::Balance { raw_payload })),
        _ => Err(GateError::Payload),
    }
}

fn parse_private_subscription_ack(payload: &str) -> Result<Option<String>, GateError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| GateError::Payload)?;
    let object = object(&value)?;
    if object.get("error").is_some_and(|value| !value.is_null()) {
        return Err(GateError::WebSocket);
    }
    if object.get("event").and_then(Value::as_str) != Some("subscribe") {
        return Ok(None);
    }
    let status = object
        .get("result")
        .and_then(Value::as_object)
        .and_then(|result| result.get("status"))
        .and_then(Value::as_str);
    if status != Some("success") {
        return Err(GateError::WebSocket);
    }
    object
        .get("channel")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .map(Some)
        .ok_or(GateError::Payload)
}

fn response_json(response: Response) -> Result<Value, GateError> {
    parse_json(&response_text(response)?)
}

fn response_text(response: Response) -> Result<String, GateError> {
    let status = response.status();
    let text = response.text().map_err(|_| GateError::Http)?;
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err(GateError::RateLimited);
    }
    if status.is_server_error() {
        return Err(GateError::Http);
    }
    if status.is_client_error() {
        let label = gate_error_label(&text, status);
        if matches!(label.as_str(), "TOO_FAST" | "TOO_MANY_REQUESTS") {
            return Err(GateError::RateLimited);
        }
        return Err(GateError::Rejected { label });
    }
    Ok(text)
}

fn gate_error_label(text: &str, status: StatusCode) -> String {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|value| {
            value
                .get("label")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .filter(|label| {
            !label.is_empty()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        })
        .unwrap_or_else(|| format!("HTTP_{}", status.as_u16()))
}

fn parse_json(text: &str) -> Result<Value, GateError> {
    serde_json::from_str(text).map_err(|_| GateError::Payload)
}

fn identifier(value: Option<&Value>) -> Result<String, GateError> {
    match value {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(value.to_owned()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => Err(GateError::Payload),
    }
}

fn bool_field(object: &Map<String, Value>, field: &str) -> Result<bool, GateError> {
    object
        .get(field)
        .and_then(Value::as_bool)
        .ok_or(GateError::Payload)
}

fn optional_price_state(value: Option<&Value>) -> Result<FieldState<Price>, GateError> {
    match value {
        None => Ok(FieldState::Missing),
        Some(Value::Null) => Ok(FieldState::Null),
        Some(Value::String(value)) if value.is_empty() => Ok(FieldState::Unavailable {
            reason: crate::domain::UnknownReason::VenueUnavailable,
        }),
        value => {
            let price = decimal_value(value)?;
            if price.is_zero() {
                Ok(FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::VenueUnavailable,
                })
            } else {
                Price::new(price)
                    .map(FieldState::Known)
                    .map_err(|_| GateError::Payload)
            }
        }
    }
}

fn optional_usdt_amount(value: Option<&Value>) -> Result<FieldState<Amount>, GateError> {
    match value {
        None => Ok(FieldState::Missing),
        Some(Value::Null) => Ok(FieldState::Null),
        Some(Value::String(value)) if value.is_empty() => Ok(FieldState::Unavailable {
            reason: crate::domain::UnknownReason::VenueUnavailable,
        }),
        value => Ok(FieldState::Known(Amount::new(
            Asset::new("USDT").map_err(|_| GateError::Payload)?,
            decimal_value(value)?,
        ))),
    }
}

fn optional_maker(value: Option<&Value>) -> FieldState<bool> {
    match value.and_then(Value::as_str) {
        Some("maker") => FieldState::Known(true),
        Some("taker") => FieldState::Known(false),
        Some(_) => FieldState::Unavailable {
            reason: crate::domain::UnknownReason::Ambiguous,
        },
        None => FieldState::Missing,
    }
}

fn optional_timestamp_ms(value: Option<&Value>) -> Result<Option<u64>, GateError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        value => timestamp_ms(value).map(Some),
    }
}

fn book_price(value: Option<&Value>) -> Result<Price, GateError> {
    let values = value.and_then(Value::as_array).ok_or(GateError::Payload)?;
    let first = values.first().ok_or(GateError::Payload)?;
    let value = match first {
        // Gate v4 documents both the historical tuple form and the current object form.
        Value::Array(values) => values.first(),
        Value::Object(values) => values.get("p"),
        _ => None,
    };
    Price::new(decimal_value(value)?).map_err(|_| GateError::Payload)
}

fn timestamp_ms(value: Option<&Value>) -> Result<u64, GateError> {
    let raw = match value {
        Some(Value::String(value)) => Decimal::from_str(value).map_err(|_| GateError::Clock)?,
        Some(Value::Number(value)) => {
            Decimal::from_str(&value.to_string()).map_err(|_| GateError::Clock)?
        }
        _ => return Err(GateError::Clock),
    };
    if !raw.is_sign_positive() || raw.is_zero() {
        return Err(GateError::Clock);
    }
    let milliseconds = if raw < Decimal::from(100_000_000_000_u64) {
        raw.checked_mul(Decimal::from(1_000_u16))
            .ok_or(GateError::Clock)?
    } else {
        raw
    };
    milliseconds
        .trunc()
        .to_string()
        .parse::<u64>()
        .map_err(|_| GateError::Clock)
}

fn wall_clock_ms() -> Result<u64, GateError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GateError::Clock)
        .and_then(|duration| u64::try_from(duration.as_millis()).map_err(|_| GateError::Clock))
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn encode_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GateError {
    #[error("Gate.io credentials are missing or empty")]
    Credentials,
    #[error("Gate.io transport failed")]
    Http,
    #[error("Gate.io rate limited the request")]
    RateLimited,
    #[error("Gate.io rejected the request ({label})")]
    Rejected { label: String },
    #[error("Gate.io rejected a private readback ({label})")]
    PrivateReadbackRejected { label: String },
    #[error("Gate.io returned an invalid or incomplete payload")]
    Payload,
    #[error("Gate.io private {0} payload is invalid or incomplete")]
    PrivatePayload(&'static str),
    #[error("Gate.io private pagination did not produce one complete, non-overlapping result")]
    Pagination,
    #[error("Gate.io server clock is unavailable or invalid")]
    Clock,
    #[error("Gate.io symbol is outside the USDT perpetual deployment")]
    Symbol,
    #[error("Gate.io contract rules are not usable for this grid")]
    Instrument,
    #[error("Gate.io account is not in the required dual-position mode")]
    PositionMode,
    #[error("Gate.io risk account mode cannot be proven from signed fields")]
    RiskAccountMode,
    #[error("Gate.io risk snapshot is incomplete or internally inconsistent")]
    RiskSnapshot,
    #[error("Gate.io physical quantity is invalid for the selected contract")]
    Quantity,
    #[error("Gate.io client order identity exceeds its native contract")]
    ClientOrderId,
    #[error("Gate.io command is invalid for the normalized order semantics")]
    Command,
    #[error("Gate.io could not find the exact client order identity")]
    OrderAbsent,
    #[error("Gate.io private WebSocket is unavailable or returned an invalid event")]
    WebSocket,
    #[error("Gate.io private WebSocket closed")]
    StreamClosed,
}

impl From<venue_gateway_gate::GateRiskError> for GateError {
    fn from(value: venue_gateway_gate::GateRiskError) -> Self {
        match value {
            venue_gateway_gate::GateRiskError::Payload => Self::Payload,
            venue_gateway_gate::GateRiskError::PositionMode => Self::PositionMode,
            venue_gateway_gate::GateRiskError::RiskAccountMode => Self::RiskAccountMode,
            venue_gateway_gate::GateRiskError::RiskSnapshot => Self::RiskSnapshot,
            venue_gateway_gate::GateRiskError::Quantity => Self::Quantity,
        }
    }
}

impl From<venue_gateway_gate::GatePrivatePayloadError> for GateError {
    fn from(value: venue_gateway_gate::GatePrivatePayloadError) -> Self {
        match value {
            venue_gateway_gate::GatePrivatePayloadError::Payload => Self::Payload,
            venue_gateway_gate::GatePrivatePayloadError::Symbol => Self::Symbol,
        }
    }
}

#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;
