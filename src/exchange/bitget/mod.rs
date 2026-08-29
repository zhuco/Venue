use std::{
    collections::BTreeSet,
    net::TcpStream,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use reqwest::{
    StatusCode,
    blocking::{Client, Response},
};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tungstenite::{Message, WebSocket, client::IntoClientRequest, stream::MaybeTlsStream};

use crate::domain::{
    AccountBalance, Amount, Asset, FieldState, Fill, Instrument, MarketKind, MarketOrderCommand,
    MarketReduceCommand, Order, OrderCommand, OrderSide, OrderState, Position, PositionSide, Price,
    Symbol,
};
use crate::exchange::websocket;

use venue_gateway_bitget::account as bitget_account;
use venue_gateway_bitget::risk as bitget_risk;
pub use venue_gateway_bitget::risk::BitgetRiskReadback;

pub fn parse_risk_snapshots(
    assets_value: &Value,
    position_values: &[Value],
    symbol: &Symbol,
    account: &str,
    private_generation: u64,
    observed_at_ms: u64,
) -> Result<
    (
        crate::domain::AccountRiskSnapshot,
        Vec<crate::domain::LegRiskSnapshot>,
    ),
    BitgetError,
> {
    bitget_risk::parse_risk_snapshots(
        assets_value,
        position_values,
        symbol,
        account,
        private_generation,
        observed_at_ms,
    )
    .map_err(BitgetError::from)
}

const API_BASE_URL: &str = "https://api.bitget.com";
const PRIVATE_WS_URL: &str = "wss://ws.bitget.com/v3/ws/private";
const PUBLIC_WS_URL: &str = "wss://ws.bitget.com/v3/ws/public";
const FUTURES_CATEGORY: &str = "USDT-FUTURES";
const MAX_CLIENT_ORDER_ID_BYTES: usize = 32;
const FILL_PAGE_SIZE: usize = 100;
const MAX_FILL_PAGES: usize = 900;
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const WEBSOCKET_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(25);
const PUBLIC_READINESS_TIMEOUT: Duration = Duration::from_millis(1);
const MAX_FILL_HISTORY_WINDOW_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

type HmacSha256 = Hmac<Sha256>;

/// Credentials stay process-local and are deliberately neither serializable nor debuggable.
#[derive(Clone)]
pub struct BitgetCredentials {
    key: String,
    secret: String,
    passphrase: String,
}

impl BitgetCredentials {
    pub fn from_environment() -> Result<Self, BitgetError> {
        let key = crate::credential_env::required("BITGET_API_KEY")
            .map_err(|_| BitgetError::Credentials)?;
        let secret = crate::credential_env::required("BITGET_API_SECRET")
            .map_err(|_| BitgetError::Credentials)?;
        let passphrase =
            crate::credential_env::required_any(&["BITGET_API_PASSPHRASE", "BITGET_PASSPHRASE"])
                .map_err(|_| BitgetError::Credentials)?;
        Ok(Self {
            key,
            secret,
            passphrase,
        })
    }

    pub fn api_key_sha256(&self) -> String {
        hex(Sha256::digest(self.key.as_bytes()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetContractRules {
    pub native_symbol: String,
    pub instrument: Instrument,
    pub minimum_quantity: Decimal,
    pub minimum_notional: Decimal,
}

pub struct BitgetPublicRest {
    client: Client,
}

/// Credential-free UTA public transport.  It returns the original text frame so the caller can
/// fsync it before selecting a parser or allowing a market observation to influence execution.
pub(crate) struct BitgetPublicStream {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
    last_heartbeat: Instant,
}

impl BitgetPublicRest {
    pub fn production() -> Result<Self, BitgetError> {
        Ok(Self {
            client: Client::builder()
                .timeout(HTTP_TIMEOUT)
                .build()
                .map_err(|_| BitgetError::Http)?,
        })
    }

    pub fn server_time_ms(&self) -> Result<u64, BitgetError> {
        let value = self.get_json("/api/v2/public/time")?;
        let data = bitget_data(&value)?;
        timestamp_ms(data.get("serverTime"))
    }

    pub fn contract_rules(
        &self,
        symbol: &Symbol,
        generation: u64,
    ) -> Result<BitgetContractRules, BitgetError> {
        let native = native_symbol(symbol)?;
        let value = self.get_json(&format!(
            "/api/v3/market/instruments?category={FUTURES_CATEGORY}&symbol={native}"
        ))?;
        let data = bitget_data(&value)?;
        let rules = data.as_array().ok_or(BitgetError::Payload)?;
        let rule = rules
            .iter()
            .find(|rule| rule.get("symbol").and_then(Value::as_str) == Some(native.as_str()))
            .ok_or(BitgetError::Instrument)?;
        parse_contract_rules(rule, symbol.clone(), generation)
    }

    pub fn best_bid_ask(&self, symbol: &Symbol) -> Result<(Price, Price), BitgetError> {
        let native = native_symbol(symbol)?;
        let value = self.get_json(&format!(
            "/api/v3/market/tickers?category={FUTURES_CATEGORY}&symbol={native}"
        ))?;
        let values = bitget_data(&value)?
            .as_array()
            .ok_or(BitgetError::Payload)?;
        let ticker = values
            .iter()
            .find(|ticker| ticker.get("symbol").and_then(Value::as_str) == Some(native.as_str()))
            .ok_or(BitgetError::Payload)?;
        let object = object(ticker)?;
        let bid = Price::new(decimal(object, "bid1Price")?).map_err(|_| BitgetError::Payload)?;
        let ask = Price::new(decimal(object, "ask1Price")?).map_err(|_| BitgetError::Payload)?;
        if bid >= ask {
            return Err(BitgetError::Payload);
        }
        Ok((bid, ask))
    }

    pub(crate) fn market_payload_raw(&self, path_and_query: &str) -> Result<String, BitgetError> {
        response_text(
            self.client
                .get(format!("{API_BASE_URL}{path_and_query}"))
                .send()
                .map_err(|_| BitgetError::Http)?,
        )
    }

    fn get_json(&self, path_and_query: &str) -> Result<Value, BitgetError> {
        response_json(
            self.client
                .get(format!("{API_BASE_URL}{path_and_query}"))
                .send()
                .map_err(|_| BitgetError::Http)?,
        )
    }
}

impl BitgetPublicStream {
    pub(crate) fn connect(symbol: &Symbol) -> Result<Self, BitgetError> {
        let request = PUBLIC_WS_URL
            .into_client_request()
            .map_err(|_| BitgetError::WebSocketEndpoint)?;
        let (socket, _) =
            websocket::connect_tls(request).map_err(|error| BitgetError::WebSocketConnect {
                reason: error.to_string(),
            })?;
        let mut stream = Self {
            socket,
            last_heartbeat: Instant::now(),
        };
        stream.set_read_timeout(Duration::from_secs(5))?;
        let request = crate::exchange::bitget_public::public_subscriptions(symbol)
            .map_err(|_| BitgetError::WebSocketSubscribe)?;
        stream.send_json(&request)?;
        stream.await_subscriptions(&["books", "publicTrade"])?;
        stream.set_read_timeout(PUBLIC_READINESS_TIMEOUT)?;
        Ok(stream)
    }

    fn set_read_timeout(&mut self, timeout: Duration) -> Result<(), BitgetError> {
        let result = match self.socket.get_mut() {
            MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(timeout)),
            MaybeTlsStream::Rustls(stream) => stream.sock.set_read_timeout(Some(timeout)),
            _ => return Err(BitgetError::WebSocketSetup),
        };
        result.map_err(|_| BitgetError::WebSocketSetup)
    }

    fn send_json(&mut self, value: &Value) -> Result<(), BitgetError> {
        self.socket
            .send(Message::Text(
                serde_json::to_string(value)
                    .map_err(|_| BitgetError::Payload)?
                    .into(),
            ))
            .map_err(|_| BitgetError::WebSocket)
    }

    fn await_subscriptions(&mut self, expected: &[&str]) -> Result<(), BitgetError> {
        let expected = expected.iter().copied().collect::<BTreeSet<_>>();
        let mut acknowledged = BTreeSet::new();
        while acknowledged.len() < expected.len() {
            let Some(value) = self.next_json()? else {
                return Err(BitgetError::WebSocketSubscribe);
            };
            let object = object(&value)?;
            if object.get("event").and_then(Value::as_str) == Some("error") {
                return Err(BitgetError::WebSocketSubscribe);
            }
            if object.get("event").and_then(Value::as_str) != Some("subscribe") {
                continue;
            }
            let topic = object
                .get("arg")
                .and_then(Value::as_object)
                .and_then(|argument| argument.get("topic"))
                .and_then(Value::as_str)
                .ok_or(BitgetError::Payload)?;
            if !expected.contains(topic) {
                return Err(BitgetError::WebSocketSubscribe);
            }
            acknowledged.insert(topic.to_owned());
        }
        Ok(())
    }

    pub(crate) fn next_raw_event(&mut self) -> Result<Option<String>, BitgetError> {
        self.send_heartbeat_if_due()?;
        match self.socket.read() {
            Ok(Message::Text(raw_text)) => {
                if raw_text.as_str() == "ping" {
                    self.socket
                        .send(Message::Text("pong".into()))
                        .map_err(|_| BitgetError::WebSocket)?;
                    return Ok(None);
                }
                if raw_text.as_str() == "pong" {
                    return Ok(None);
                }
                let value = parse_json(&raw_text)?;
                let object = object(&value)?;
                if object.get("event").and_then(Value::as_str) == Some("error") {
                    return Err(BitgetError::WebSocket);
                }
                if object.get("event").is_some() {
                    return Ok(None);
                }
                Ok(Some(raw_text.to_string()))
            }
            Ok(Message::Ping(payload)) => {
                self.socket
                    .send(Message::Pong(payload))
                    .map_err(|_| BitgetError::WebSocket)?;
                Ok(None)
            }
            Ok(Message::Close(_)) => Err(BitgetError::StreamClosed),
            Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => Ok(None),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(_) => Err(BitgetError::WebSocket),
        }
    }

    fn next_json(&mut self) -> Result<Option<Value>, BitgetError> {
        match self.socket.read() {
            Ok(Message::Text(text)) if text.as_str() == "ping" => {
                self.socket
                    .send(Message::Text("pong".into()))
                    .map_err(|_| BitgetError::WebSocket)?;
                Ok(None)
            }
            Ok(Message::Text(text)) if text.as_str() == "pong" => Ok(None),
            Ok(Message::Text(text)) => serde_json::from_str(&text)
                .map(Some)
                .map_err(|_| BitgetError::Payload),
            Ok(Message::Ping(payload)) => {
                self.socket
                    .send(Message::Pong(payload))
                    .map_err(|_| BitgetError::WebSocket)?;
                Ok(None)
            }
            Ok(Message::Close(_)) => Err(BitgetError::StreamClosed),
            Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => Ok(None),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(_) => Err(BitgetError::WebSocket),
        }
    }

    fn send_heartbeat_if_due(&mut self) -> Result<(), BitgetError> {
        if self.last_heartbeat.elapsed() < WEBSOCKET_HEARTBEAT_INTERVAL {
            return Ok(());
        }
        self.socket
            .send(Message::Text("ping".into()))
            .map_err(|_| BitgetError::WebSocket)?;
        self.last_heartbeat = Instant::now();
        Ok(())
    }
}

#[derive(Clone)]
pub struct BitgetPrivateRest {
    client: Client,
    credentials: BitgetCredentials,
    server_offset_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetPrivateReadback {
    pub raw_payloads: Vec<String>,
    /// Exact signed `/api/v3/trade/unfilled-orders` pages for the normal UTA family. Stage 7
    /// keeps this separate from unrelated account/position/fill responses when proving custody.
    pub signed_regular_order_payloads: Vec<String>,
    pub balance: AccountBalance,
    pub hedge_position: bool,
    pub positions: Vec<Position>,
    pub orders: Vec<Order>,
    pub fills: Vec<BitgetFill>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitgetFill {
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
}

pub struct BitgetPrivateStream {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
    symbol: String,
    last_heartbeat: Instant,
}

impl BitgetPrivateRest {
    pub fn production(credentials: BitgetCredentials) -> Result<Self, BitgetError> {
        let public = BitgetPublicRest::production()?;
        let server_time_ms = public.server_time_ms()?;
        let local_time_ms = wall_clock_ms()?;
        let offset = i128::from(server_time_ms) - i128::from(local_time_ms);
        Ok(Self {
            client: public.client,
            credentials,
            server_offset_ms: i64::try_from(offset).map_err(|_| BitgetError::Clock)?,
        })
    }

    pub fn credentials_api_key_sha256(&self) -> String {
        self.credentials.api_key_sha256()
    }

    pub fn place_limit_post_only(
        &self,
        command: &OrderCommand,
        rules: &BitgetContractRules,
    ) -> Result<String, BitgetError> {
        command.validate().map_err(|_| BitgetError::Command)?;
        if command.owner.symbol != rules.instrument.symbol
            || command.reduce_only != is_reduce(command)
        {
            return Err(BitgetError::Command);
        }
        let body = json!({
            "category": FUTURES_CATEGORY,
            "symbol": rules.native_symbol,
            "orderType": "limit",
            "qty": decimal_string(command.quantity),
            "price": decimal_string(command.limit_price.value()),
            "side": native_side(command.side),
            "posSide": native_position_side(command.position_side)?,
            "timeInForce": "post_only",
            "clientOid": native_client_order_id(command.client_order_id.as_str())?,
        });
        let value = self.signed_json("POST", "/api/v3/trade/place-order", "", Some(&body))?;
        accepted_order_id(&value, command.client_order_id.as_str())
    }

    pub fn place_market(
        &self,
        command: &MarketOrderCommand,
        rules: &BitgetContractRules,
    ) -> Result<String, BitgetError> {
        command.validate().map_err(|_| BitgetError::Command)?;
        if command.owner.symbol != rules.instrument.symbol || command.reduce_only {
            return Err(BitgetError::Command);
        }
        let body = json!({
            "category": FUTURES_CATEGORY,
            "symbol": rules.native_symbol,
            "orderType": "market",
            "qty": decimal_string(command.quantity),
            "side": native_side(command.side),
            "posSide": native_position_side(command.position_side)?,
            "clientOid": native_client_order_id(command.client_order_id.as_str())?,
        });
        let value = self.signed_json("POST", "/api/v3/trade/place-order", "", Some(&body))?;
        accepted_order_id(&value, command.client_order_id.as_str())
    }

    pub fn place_reduce_only_market(
        &self,
        command: &OrderCommand,
        rules: &BitgetContractRules,
    ) -> Result<String, BitgetError> {
        command.validate().map_err(|_| BitgetError::Command)?;
        if command.owner.symbol != rules.instrument.symbol
            || !command.reduce_only
            || !is_reduce(command)
        {
            return Err(BitgetError::Command);
        }
        // Hedge mode closes through the opposite side plus the same posSide; Bitget documents
        // reduceOnly as a one-way-mode field, so it is intentionally omitted here.
        let body = json!({
            "category": FUTURES_CATEGORY,
            "symbol": rules.native_symbol,
            "orderType": "market",
            "qty": decimal_string(command.quantity),
            "side": native_side(command.side),
            "posSide": native_position_side(command.position_side)?,
            "clientOid": native_client_order_id(command.client_order_id.as_str())?,
        });
        let value = self.signed_json("POST", "/api/v3/trade/place-order", "", Some(&body))?;
        accepted_order_id(&value, command.client_order_id.as_str())
    }

    pub fn place_market_reduce(
        &self,
        command: &MarketReduceCommand,
        rules: &BitgetContractRules,
    ) -> Result<String, BitgetError> {
        let body = market_reduce_body(command, rules)?;
        let value = self.signed_json("POST", "/api/v3/trade/place-order", "", Some(&body))?;
        accepted_order_id(&value, command.client_order_id.as_str())
    }

    pub fn cancel_by_client_id(&self, client_order_id: &str) -> Result<String, BitgetError> {
        let body = json!({
            "category": FUTURES_CATEGORY,
            "clientOid": native_client_order_id(client_order_id)?,
        });
        let value = self.signed_json("POST", "/api/v3/trade/cancel-order", "", Some(&body))?;
        accepted_order_id(&value, client_order_id)
    }

    pub fn order_by_client_id(
        &self,
        symbol: &Symbol,
        client_order_id: &str,
    ) -> Result<Value, BitgetError> {
        let native = native_symbol(symbol)?;
        let query = format!(
            "clientOid={}",
            encode_component(&native_client_order_id(client_order_id)?)
        );
        let value = self.signed_json("GET", "/api/v3/trade/order-info", &query, None)?;
        // A business error is not proof that this identity is absent. Permission, clock and
        // transient venue failures must leave the WAL command Unknown until a later exact
        // readback supplies a concrete order fact.
        let data = bitget_data(&value)?.clone();
        if data.get("symbol").and_then(Value::as_str) != Some(native.as_str()) {
            return Err(BitgetError::OrderAbsent);
        }
        Ok(data)
    }

    pub fn verify_post_only_order_by_client_id(
        &self,
        symbol: &Symbol,
        client_order_id: &str,
    ) -> Result<(), BitgetError> {
        let order = self.order_by_client_id(symbol, client_order_id)?;
        if is_post_only_order(&order) {
            Ok(())
        } else {
            Err(BitgetError::Command)
        }
    }

    pub fn readback(
        &self,
        symbol: &Symbol,
        _rules: &BitgetContractRules,
        fill_history_start_ms: Option<u64>,
    ) -> Result<BitgetPrivateReadback, BitgetError> {
        let native = native_symbol(symbol)?;
        let positions_query = format!("category={FUTURES_CATEGORY}&symbol={native}");
        // These five signed surfaces are independent facts from one reconciliation turn. Fetch
        // them concurrently, then parse and admit the complete tuple below; no partial result is
        // exposed and no mutation can run until every scoped request has joined successfully.
        let (assets_result, settings_result, positions_result, orders_result, fills_result) =
            thread::scope(|scope| {
                let assets =
                    scope.spawn(|| self.signed_text("GET", "/api/v3/account/assets", "", None));
                let settings =
                    scope.spawn(|| self.signed_text("GET", "/api/v3/account/settings", "", None));
                let positions = scope.spawn(|| {
                    self.signed_text(
                        "GET",
                        "/api/v3/position/current-position",
                        &positions_query,
                        None,
                    )
                });
                let orders = scope.spawn(|| self.read_all_open_orders(&native));
                let fills = scope.spawn(|| self.read_all_fills(fill_history_start_ms));
                (
                    assets.join().unwrap_or(Err(BitgetError::Http)),
                    settings.join().unwrap_or(Err(BitgetError::Http)),
                    positions.join().unwrap_or(Err(BitgetError::Http)),
                    orders.join().unwrap_or(Err(BitgetError::Http)),
                    fills.join().unwrap_or(Err(BitgetError::Http)),
                )
            });
        // A failed face invalidates the whole observation. The resident may retry the complete
        // reconciliation turn, but successful faces from different attempts are never combined
        // into one private generation.
        let (asset_raw, settings_raw, positions_raw, (orders_raw, orders), (fills_raw, fills)) =
            complete_private_readback_tuple((
                assets_result,
                settings_result,
                positions_result,
                orders_result,
                fills_result,
            ))?;
        let asset_json = parse_json(&asset_raw).map_err(|error| readback_error("assets", error))?;
        let settings_json =
            parse_json(&settings_raw).map_err(|error| readback_error("settings", error))?;
        let positions_json =
            parse_json(&positions_raw).map_err(|error| readback_error("positions", error))?;
        let assets = bitget_data(&asset_json).map_err(|error| readback_error("assets", error))?;
        let settings =
            bitget_data(&settings_json).map_err(|error| readback_error("settings", error))?;
        let positions = list_data(
            bitget_data(&positions_json).map_err(|error| readback_error("positions", error))?,
        )
        .map_err(|error| readback_shape("positions", bitget_data(&positions_json), error))?;
        let positions = positions
            .iter()
            .filter(|position| {
                position.get("symbol").and_then(Value::as_str) == Some(native.as_str())
            })
            .map(|position| parse_position(position, symbol))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| readback_error("positions", error))?;
        let orders = orders
            .iter()
            .filter(|order| order.get("symbol").and_then(Value::as_str) == Some(native.as_str()))
            .map(|order| parse_regular_open_order(order, symbol))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| readback_error("orders", error))?;
        let mut seen_fill_ids = BTreeSet::new();
        let fills = fills
            .iter()
            .filter(|fill| fill.get("symbol").and_then(Value::as_str) == Some(native.as_str()))
            .map(|fill| {
                let fill_id = identifier(fill.get("execId"))?;
                if !seen_fill_ids.insert(fill_id) {
                    return Err(BitgetError::Pagination);
                }
                Ok(BitgetFill {
                    fill: parse_fill(fill, symbol)?,
                    client_order_id: client_order_id(fill.get("clientOid")),
                })
            })
            .collect::<Result<Vec<_>, BitgetError>>()
            .map_err(|error| readback_error("fills", error))?;
        let mut raw_payloads = vec![asset_raw, settings_raw, positions_raw];
        raw_payloads.extend(orders_raw.iter().cloned());
        raw_payloads.extend(fills_raw);
        Ok(BitgetPrivateReadback {
            raw_payloads,
            signed_regular_order_payloads: orders_raw,
            balance: parse_balance(assets).map_err(|error| readback_error("assets", error))?,
            hedge_position: bitget_account::is_hedge_mode(settings)
                .map_err(BitgetError::from)
                .map_err(|error| readback_error("settings", error))?,
            positions,
            orders,
            fills,
        })
    }

    pub fn connect_private_stream(
        &self,
        symbol: &Symbol,
    ) -> Result<BitgetPrivateStream, BitgetError> {
        BitgetPrivateStream::connect(
            &self.credentials,
            self.authoritative_now_ms()?,
            native_symbol(symbol)?,
        )
    }

    pub fn risk_readback(
        &self,
        symbol: &Symbol,
        account: &str,
        private_generation: u64,
    ) -> Result<BitgetRiskReadback, BitgetError> {
        let started_at_ms = self.authoritative_now_ms()?;
        let native = native_symbol(symbol)?;
        let positions_query = format!(
            "category={}&symbol={}",
            encode_component(FUTURES_CATEGORY),
            encode_component(&native)
        );
        let assets_raw = self.signed_text("GET", "/api/v3/account/assets", "", None)?;
        let settings_raw = self.signed_text("GET", "/api/v3/account/settings", "", None)?;
        let positions_raw = self.signed_text(
            "GET",
            "/api/v3/position/current-position",
            &positions_query,
            None,
        )?;
        let observed_at_ms = self.authoritative_now_ms()?;
        bitget_risk::validate_risk_readback_window(started_at_ms, observed_at_ms)?;
        let assets_json = parse_json(&assets_raw)?;
        let settings_json = parse_json(&settings_raw)?;
        let positions_json = parse_json(&positions_raw)?;
        let assets = bitget_data(&assets_json)?;
        let settings = bitget_data(&settings_json)?;
        if !bitget_account::is_hedge_mode(settings).map_err(BitgetError::from)? {
            return Err(BitgetError::PositionMode);
        }
        let positions = list_data(bitget_data(&positions_json)?)?;
        let (account, legs) = parse_risk_snapshots(
            assets,
            positions,
            symbol,
            account,
            private_generation,
            observed_at_ms,
        )?;
        Ok(BitgetRiskReadback {
            raw_payloads: vec![assets_raw, settings_raw, positions_raw],
            account,
            legs,
        })
    }

    fn read_all_fills(
        &self,
        fill_history_start_ms: Option<u64>,
    ) -> Result<(Vec<String>, Vec<Value>), BitgetError> {
        let mut payloads = Vec::new();
        let mut fills = Vec::new();
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut seen_fill_ids = BTreeSet::new();
        for _ in 0..MAX_FILL_PAGES {
            let query = fill_history_query(fill_history_start_ms, cursor.as_deref())?;
            let payload = self.signed_text("GET", "/api/v3/trade/fills", &query, None)?;
            let json = parse_json(&payload).map_err(|error| readback_error("fills", error))?;
            let data = bitget_data(&json).map_err(|error| readback_error("fills", error))?;
            let page = list_data(data).map_err(|error| readback_error("fills", error))?;
            for fill in page {
                let fill_id = identifier(fill.get("execId"))
                    .map_err(|error| readback_error("fills", error))?;
                if !seen_fill_ids.insert(fill_id) {
                    return Err(BitgetError::Pagination);
                }
                fills.push(fill.clone());
            }
            payloads.push(payload);
            let Some(next_cursor) =
                fill_history_cursor(data).map_err(|error| readback_error("fills", error))?
            else {
                return Ok((payloads, fills));
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(BitgetError::Pagination);
            }
            cursor = Some(next_cursor);
        }
        Err(BitgetError::Pagination)
    }

    fn read_all_open_orders(&self, native: &str) -> Result<(Vec<String>, Vec<Value>), BitgetError> {
        let mut payloads = Vec::new();
        let mut orders = Vec::new();
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut seen_order_ids = BTreeSet::new();
        for _ in 0..MAX_FILL_PAGES {
            let query = open_orders_query(native, cursor.as_deref());
            let payload = self.signed_text("GET", "/api/v3/trade/unfilled-orders", &query, None)?;
            let json = parse_json(&payload).map_err(|error| readback_error("orders", error))?;
            let data = bitget_data(&json).map_err(|error| readback_error("orders", error))?;
            let page = list_data(data).map_err(|error| readback_error("orders", error))?;
            for order in page {
                let order_id = identifier(order.get("orderId"))
                    .map_err(|error| readback_error("orders", error))?;
                if !seen_order_ids.insert(order_id) {
                    return Err(BitgetError::Pagination);
                }
                orders.push(order.clone());
            }
            payloads.push(payload);
            let Some(next_cursor) =
                fill_history_cursor(data).map_err(|error| readback_error("orders", error))?
            else {
                return Ok((payloads, orders));
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(BitgetError::Pagination);
            }
            cursor = Some(next_cursor);
        }
        Err(BitgetError::Pagination)
    }

    fn signed_json(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<&Value>,
    ) -> Result<Value, BitgetError> {
        parse_json(&self.signed_text(method, path, query, body)?)
    }

    fn signed_text(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<&Value>,
    ) -> Result<String, BitgetError> {
        let body_bytes = match body {
            Some(value) => serde_json::to_vec(value).map_err(|_| BitgetError::Payload)?,
            None => Vec::new(),
        };
        let timestamp = self.authoritative_now_ms()?.to_string();
        let signature = bitget_signature(
            &self.credentials.secret,
            &timestamp,
            method,
            path,
            query,
            &body_bytes,
        )?;
        let url = if query.is_empty() {
            format!("{API_BASE_URL}{path}")
        } else {
            format!("{API_BASE_URL}{path}?{query}")
        };
        let mut request = self
            .client
            .request(method.parse().map_err(|_| BitgetError::Payload)?, url)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("ACCESS-KEY", &self.credentials.key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", timestamp)
            .header("ACCESS-PASSPHRASE", &self.credentials.passphrase)
            .header("locale", "en-US");
        if !body_bytes.is_empty() {
            request = request.body(body_bytes);
        }
        response_text(request.send().map_err(|_| BitgetError::Http)?)
    }

    fn authoritative_now_ms(&self) -> Result<u64, BitgetError> {
        let adjusted = i128::from(wall_clock_ms()?) + i128::from(self.server_offset_ms);
        u64::try_from(adjusted).map_err(|_| BitgetError::Clock)
    }
}

impl BitgetPrivateStream {
    fn connect(
        credentials: &BitgetCredentials,
        timestamp: u64,
        symbol: String,
    ) -> Result<Self, BitgetError> {
        let request = PRIVATE_WS_URL
            .into_client_request()
            .map_err(|_| BitgetError::WebSocketEndpoint)?;
        let (socket, _) =
            websocket::connect_tls(request).map_err(|error| BitgetError::WebSocketConnect {
                reason: error.to_string(),
            })?;
        let mut stream = Self {
            socket,
            symbol,
            last_heartbeat: Instant::now(),
        };
        stream
            .set_read_timeout(Duration::from_secs(5))
            .map_err(|_| BitgetError::WebSocketSetup)?;
        let login = json!({"op":"login","args":[{
            "apiKey": credentials.key,
            "passphrase": credentials.passphrase,
            "timestamp": timestamp.to_string(),
            "sign": bitget_ws_signature(&credentials.secret, timestamp)?,
        }]});
        stream
            .send_json(&login)
            .map_err(|_| BitgetError::WebSocketLogin)?;
        stream.await_login()?;
        stream
            .send_json(&json!({"op":"subscribe","args":[
                {"instType":"UTA","topic":"account"},
                {"instType":"UTA","topic":"position"},
                {"instType":"UTA","topic":"order"},
                {"instType":"UTA","topic":"fill"}
            ]}))
            .map_err(|_| BitgetError::WebSocketSubscribe)?;
        stream.await_subscriptions(&["account", "position", "order", "fill"])?;
        stream
            .set_read_timeout(Duration::from_millis(100))
            .map_err(|_| BitgetError::WebSocketSetup)?;
        Ok(stream)
    }

    fn set_read_timeout(&mut self, timeout: Duration) -> Result<(), BitgetError> {
        let result = match self.socket.get_mut() {
            MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(timeout)),
            MaybeTlsStream::Rustls(stream) => stream.sock.set_read_timeout(Some(timeout)),
            _ => return Err(BitgetError::WebSocket),
        };
        result.map_err(|_| BitgetError::WebSocket)
    }

    fn send_json(&mut self, value: &Value) -> Result<(), BitgetError> {
        self.socket
            .send(Message::Text(
                serde_json::to_string(value)
                    .map_err(|_| BitgetError::Payload)?
                    .into(),
            ))
            .map_err(|_| BitgetError::WebSocket)
    }

    fn await_login(&mut self) -> Result<(), BitgetError> {
        loop {
            let Some(value) = self.next_json().map_err(|_| BitgetError::WebSocketLogin)? else {
                return Err(BitgetError::WebSocketLogin);
            };
            let object = object(&value)?;
            match object.get("event").and_then(Value::as_str) {
                Some("login") if is_success_code(object.get("code")) => {
                    return Ok(());
                }
                Some("error") | Some("login") => return Err(BitgetError::WebSocketLogin),
                _ => {}
            }
        }
    }

    fn await_subscriptions(&mut self, expected: &[&str]) -> Result<(), BitgetError> {
        let expected = expected.iter().copied().collect::<BTreeSet<_>>();
        let mut acknowledged = BTreeSet::new();
        while acknowledged.len() < expected.len() {
            let Some(value) = self
                .next_json()
                .map_err(|_| BitgetError::WebSocketSubscribe)?
            else {
                return Err(BitgetError::WebSocketSubscribe);
            };
            let object = object(&value)?;
            if object.get("event").and_then(Value::as_str) == Some("error") {
                return Err(BitgetError::WebSocketSubscribe);
            }
            if object.get("event").and_then(Value::as_str) != Some("subscribe") {
                continue;
            }
            let topic = object
                .get("arg")
                .and_then(Value::as_object)
                .and_then(|arg| arg.get("topic"))
                .and_then(Value::as_str)
                .ok_or(BitgetError::Payload)?;
            if !expected.contains(topic) {
                return Err(BitgetError::WebSocketSubscribe);
            }
            acknowledged.insert(topic.to_owned());
        }
        Ok(())
    }

    /// Every private update is journaled before a fresh signed REST reconciliation. Bitget has no
    /// cross-channel sequence contract that can safely replace that reconciliation.
    pub fn next_raw_event(&mut self) -> Result<Option<String>, BitgetError> {
        self.send_heartbeat_if_due()?;
        let Some(mut value) = self.next_json()? else {
            return Ok(None);
        };
        let object = object(&value)?;
        if object.get("event").and_then(Value::as_str) == Some("error") {
            return Err(BitgetError::WebSocket);
        }
        if filter_private_event_for_symbol(&mut value, &self.symbol)? {
            return serde_json::to_string(&value)
                .map(Some)
                .map_err(|_| BitgetError::Payload);
        }
        Ok(None)
    }

    fn next_json(&mut self) -> Result<Option<Value>, BitgetError> {
        match self.socket.read() {
            Ok(Message::Text(text)) if text.as_str() == "ping" => {
                self.socket
                    .send(Message::Text("pong".into()))
                    .map_err(|_| BitgetError::WebSocket)?;
                Ok(None)
            }
            Ok(Message::Text(text)) if text.as_str() == "pong" => Ok(None),
            Ok(Message::Text(text)) => serde_json::from_str(&text)
                .map(Some)
                .map_err(|_| BitgetError::Payload),
            Ok(Message::Ping(payload)) => {
                self.socket
                    .send(Message::Pong(payload))
                    .map_err(|_| BitgetError::WebSocket)?;
                Ok(None)
            }
            Ok(Message::Close(_)) => Err(BitgetError::StreamClosed),
            Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => Ok(None),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(_) => Err(BitgetError::WebSocket),
        }
    }

    fn send_heartbeat_if_due(&mut self) -> Result<(), BitgetError> {
        if self.last_heartbeat.elapsed() < WEBSOCKET_HEARTBEAT_INTERVAL {
            return Ok(());
        }
        self.socket
            .send(Message::Text("ping".into()))
            .map_err(|_| BitgetError::WebSocket)?;
        self.last_heartbeat = Instant::now();
        Ok(())
    }
}

fn filter_private_event_for_symbol(value: &mut Value, symbol: &str) -> Result<bool, BitgetError> {
    let topic = object(value)?
        .get("arg")
        .and_then(Value::as_object)
        .and_then(|arg| arg.get("topic"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    match topic.as_deref() {
        Some("account") => Ok(value.get("data").is_some()),
        Some("position" | "order" | "fill") => {
            let rows = value
                .get_mut("data")
                .and_then(Value::as_array_mut)
                .ok_or(BitgetError::Payload)?;
            rows.retain(|row| row.get("symbol").and_then(Value::as_str) == Some(symbol));
            Ok(!rows.is_empty())
        }
        _ => Ok(false),
    }
}

pub fn native_symbol(symbol: &Symbol) -> Result<String, BitgetError> {
    bitget_risk::native_symbol(symbol).map_err(BitgetError::from)
}

pub fn parse_contract_rules(
    value: &Value,
    symbol: Symbol,
    generation: u64,
) -> Result<BitgetContractRules, BitgetError> {
    if generation == 0 {
        return Err(BitgetError::Instrument);
    }
    let object = object(value)?;
    let native = text(object, "symbol")?.to_owned();
    if native != native_symbol(&symbol)?
        || text(object, "category")? != FUTURES_CATEGORY
        || text(object, "type")? != "perpetual"
        || text(object, "status")? != "online"
    {
        return Err(BitgetError::Instrument);
    }
    let quantity_step = decimal(object, "quantityMultiplier")?;
    let price_tick =
        Price::new(decimal(object, "priceMultiplier")?).map_err(|_| BitgetError::Instrument)?;
    let minimum_quantity = decimal(object, "minOrderQty")?;
    let minimum_notional = decimal(object, "minOrderAmount")?;
    let instrument = Instrument {
        symbol,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(Asset::new("USDT").map_err(|_| BitgetError::Instrument)?),
        generation,
        price_tick,
        quantity_step,
        minimum_notional: Amount::new(
            Asset::new("USDT").map_err(|_| BitgetError::Instrument)?,
            minimum_notional,
        ),
    };
    instrument.validate().map_err(|_| BitgetError::Instrument)?;
    Ok(BitgetContractRules {
        native_symbol: native,
        instrument,
        minimum_quantity,
        minimum_notional,
    })
}

pub fn parse_order(value: &Value, symbol: &Symbol) -> Result<Order, BitgetError> {
    let object = object(value)?;
    if text(object, "symbol")? != native_symbol(symbol)?
        || !text(object, "category")?.eq_ignore_ascii_case(FUTURES_CATEGORY)
    {
        return Err(BitgetError::Symbol);
    }
    let position_side = parse_position_side(text(object, "posSide")?)?;
    let side = parse_side(text(object, "side")?)?;
    let reduce_only = parse_reduce_only(object, position_side, side)?;
    let quantity = decimal(object, "qty")?;
    let filled_quantity = optional_decimal(object.get("cumExecQty"))?;
    let order = Order {
        order_id: identifier(object.get("orderId"))?,
        client_order_id: client_order_id(object.get("clientOid")),
        symbol: symbol.clone(),
        side,
        position_side: FieldState::Known(position_side),
        purpose: FieldState::Missing,
        state: parse_order_state(text(object, "orderStatus")?)?,
        quantity,
        filled_quantity,
        limit_price: optional_price(object.get("price"))?,
        average_price: optional_price_state(object.get("avgPrice"))?,
        reduce_only,
    };
    order.validate().map_err(|_| BitgetError::Payload)?;
    Ok(order)
}

fn parse_regular_open_order(value: &Value, symbol: &Symbol) -> Result<Order, BitgetError> {
    if object(value)?.get("delegateType").and_then(Value::as_str) != Some("normal") {
        return Err(BitgetError::Payload);
    }
    parse_order(value, symbol)
}

pub fn parse_position(value: &Value, symbol: &Symbol) -> Result<Position, BitgetError> {
    bitget_account::parse_position(value, symbol).map_err(BitgetError::from)
}

pub fn parse_fill(value: &Value, symbol: &Symbol) -> Result<Fill, BitgetError> {
    let object = object(value)?;
    if text(object, "symbol")? != native_symbol(symbol)?
        || !text(object, "category")?.eq_ignore_ascii_case(FUTURES_CATEGORY)
    {
        return Err(BitgetError::Symbol);
    }
    let fee = object
        .get("feeDetail")
        .and_then(Value::as_array)
        .and_then(|values| values.first())
        .map(parse_fee)
        .transpose()?
        .unwrap_or(FieldState::Missing);
    let fill_id = identifier(object.get("execId"))?;
    let fill = Fill {
        execution_sequence: execution_sequence(&fill_id),
        fill_id,
        order_id: identifier(object.get("orderId"))?,
        symbol: symbol.clone(),
        side: parse_side(text(object, "side")?)?,
        position_side: FieldState::Known(parse_position_side(
            text(object, "holdSide").or_else(|_| text(object, "posSide"))?,
        )?),
        quantity: decimal(object, "execQty")?,
        price: Price::new(decimal(object, "execPrice")?).map_err(|_| BitgetError::Payload)?,
        fee,
        realized_pnl: amount_state(object.get("execPnl"))?,
        maker: match object.get("tradeScope").and_then(Value::as_str) {
            Some("maker") => FieldState::Known(true),
            Some("taker") => FieldState::Known(false),
            Some(_) => FieldState::Unavailable {
                reason: crate::domain::UnknownReason::Ambiguous,
            },
            None => FieldState::Missing,
        },
        exchange_time_ms: optional_timestamp_ms(
            object
                .get("execTime")
                .or_else(|| object.get("updatedTime"))
                .or_else(|| object.get("createdTime")),
        )?,
    };
    fill.validate().map_err(|_| BitgetError::Payload)?;
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

/// Normalizes a private `fill` topic through the exact same parser used by signed history. Frames
/// from other topics are ignored; malformed fill evidence is never downgraded to reconciliation.
pub fn parse_private_fill_message(
    payload: &str,
    symbol: &Symbol,
) -> Result<Vec<BitgetFill>, BitgetError> {
    let value = parse_json(payload)?;
    let root = object(&value)?;
    let topic = root
        .get("arg")
        .and_then(Value::as_object)
        .and_then(|arg| arg.get("topic"))
        .and_then(Value::as_str);
    if topic != Some("fill") {
        return Ok(Vec::new());
    }
    let rows = root
        .get("data")
        .and_then(Value::as_array)
        .ok_or(BitgetError::Payload)?;
    let native = native_symbol(symbol)?;
    rows.iter()
        .filter(|row| row.get("symbol").and_then(Value::as_str) == Some(native.as_str()))
        .map(|row| {
            Ok(BitgetFill {
                fill: parse_fill(row, symbol)?,
                client_order_id: client_order_id(row.get("clientOid")),
            })
        })
        .collect()
}

fn parse_balance(value: &Value) -> Result<AccountBalance, BitgetError> {
    bitget_account::parse_balance(value).map_err(BitgetError::from)
}

fn parse_fee(value: &Value) -> Result<FieldState<Amount>, BitgetError> {
    let object = object(value)?;
    let coin = text(object, "feeCoin")?;
    let fee = decimal(object, "fee")?.abs();
    Ok(FieldState::Known(Amount::new(
        Asset::new(coin).map_err(|_| BitgetError::Payload)?,
        fee,
    )))
}

fn parse_reduce_only(
    object: &Map<String, Value>,
    position_side: PositionSide,
    side: OrderSide,
) -> Result<bool, BitgetError> {
    if object.get("holdMode").and_then(Value::as_str) == Some("hedge_mode") {
        // UTA hedge-mode order readback may return `reduceOnly=NO` and omit `tradeSide` even
        // though its native close semantics are the opposite side on the same position side.
        // Derive that exact exchange rule, while rejecting a present contradictory tradeSide.
        let inferred = matches!(
            (position_side, side),
            (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy)
        );
        let directional_side = match position_side {
            PositionSide::Long => "long",
            PositionSide::Short => "short",
            PositionSide::Net => return Err(BitgetError::Payload),
        };
        return match object.get("tradeSide").and_then(Value::as_str) {
            Some("close") if inferred => Ok(true),
            Some(value)
                if inferred
                    && value
                        .strip_prefix("close_")
                        .is_some_and(|side| side == directional_side) =>
            {
                Ok(true)
            }
            Some("open") if !inferred => Ok(false),
            Some(value)
                if !inferred
                    && value
                        .strip_prefix("open_")
                        .is_some_and(|side| side == directional_side) =>
            {
                Ok(false)
            }
            Some(_) => Err(BitgetError::Payload),
            None => Ok(inferred),
        };
    }
    match object.get("tradeSide").and_then(Value::as_str) {
        Some("open") => Ok(false),
        Some("close") => Ok(true),
        Some(_) => Err(BitgetError::Payload),
        None => match object.get("reduceOnly").and_then(Value::as_str) {
            Some(value) if value.eq_ignore_ascii_case("yes") => Ok(true),
            Some(value) if value.eq_ignore_ascii_case("no") => Ok(false),
            _ => Err(BitgetError::Payload),
        },
    }
}

pub(crate) fn list_data(value: &Value) -> Result<&[Value], BitgetError> {
    match value {
        Value::Array(values) => Ok(values),
        Value::Object(object) => match object.get("list") {
            Some(Value::Array(values)) => Ok(values),
            // Bitget returns `{"list": null}` for a symbol without a hedge leg. It is an
            // authoritative empty collection, not an omitted or ambiguous position response.
            Some(Value::Null) => Ok(&[]),
            _ => Err(BitgetError::Payload),
        },
        _ => Err(BitgetError::Payload),
    }
}

fn fill_history_query(
    fill_history_start_ms: Option<u64>,
    cursor: Option<&str>,
) -> Result<String, BitgetError> {
    let mut query = format!("category={FUTURES_CATEGORY}&limit={FILL_PAGE_SIZE}");
    if let Some(start_ms) = fill_history_start_ms {
        let now_ms = wall_clock_ms()?;
        let earliest_allowed = now_ms.saturating_sub(MAX_FILL_HISTORY_WINDOW_MS);
        query.push_str("&startTime=");
        query.push_str(&start_ms.max(earliest_allowed).to_string());
    }
    if let Some(cursor) = cursor {
        query.push_str("&cursor=");
        query.push_str(&encode_query_component(cursor));
    }
    Ok(query)
}

fn open_orders_query(native: &str, cursor: Option<&str>) -> String {
    let mut query = format!(
        "category={FUTURES_CATEGORY}&symbol={}&limit={FILL_PAGE_SIZE}",
        encode_query_component(native)
    );
    if let Some(cursor) = cursor {
        query.push_str("&cursor=");
        query.push_str(&encode_query_component(cursor));
    }
    query
}

fn fill_history_cursor(data: &Value) -> Result<Option<String>, BitgetError> {
    let Value::Object(object) = data else {
        return Ok(None);
    };
    match object.get("cursor") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(cursor)) if !cursor.is_empty() => Ok(Some(cursor.clone())),
        _ => Err(BitgetError::Payload),
    }
}

fn encode_query_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn parse_side(value: &str) -> Result<OrderSide, BitgetError> {
    match value {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(BitgetError::Payload),
    }
}
fn parse_position_side(value: &str) -> Result<PositionSide, BitgetError> {
    bitget_risk::parse_position_side(value).map_err(BitgetError::from)
}
fn native_side(value: OrderSide) -> &'static str {
    match value {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}
fn native_position_side(value: PositionSide) -> Result<&'static str, BitgetError> {
    match value {
        PositionSide::Long => Ok("long"),
        PositionSide::Short => Ok("short"),
        PositionSide::Net => Err(BitgetError::Command),
    }
}
fn is_reduce(command: &OrderCommand) -> bool {
    matches!(
        (command.position_side, command.side),
        (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy)
    )
}

fn market_reduce_body(
    command: &MarketReduceCommand,
    rules: &BitgetContractRules,
) -> Result<Value, BitgetError> {
    command.validate().map_err(|_| BitgetError::Command)?;
    if command.owner.exchange != "bitget" || command.owner.symbol != rules.instrument.symbol {
        return Err(BitgetError::Command);
    }
    // UTA Hedge mode proves a close by the exact opposite side plus posSide. `reduceOnly` is a
    // one-way-mode field, while `tradeSide` is not part of the UTA place-order contract.
    Ok(json!({
        "category": FUTURES_CATEGORY,
        "symbol": rules.native_symbol,
        "orderType": "market",
        "qty": decimal_string(command.quantity),
        "side": native_side(command.side),
        "posSide": native_position_side(command.position_side)?,
        "clientOid": native_client_order_id(command.client_order_id.as_str())?,
    }))
}
fn parse_order_state(value: &str) -> Result<OrderState, BitgetError> {
    match value {
        "live" | "new" => Ok(OrderState::New),
        "partially_filled" => Ok(OrderState::PartiallyFilled),
        "filled" => Ok(OrderState::Filled),
        "cancelled" => Ok(OrderState::Cancelled),
        "rejected" => Ok(OrderState::Rejected),
        _ => Err(BitgetError::Payload),
    }
}
fn native_client_order_id(value: &str) -> Result<String, BitgetError> {
    if value.is_empty()
        || value.len() > MAX_CLIENT_ORDER_ID_BYTES
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b':' | b'/' | b'_' | b'-')
        })
    {
        return Err(BitgetError::ClientOrderId);
    }
    Ok(value.to_owned())
}

pub(crate) fn client_order_id_is_valid(value: &str) -> bool {
    native_client_order_id(value).is_ok()
}
fn client_order_id(value: Option<&Value>) -> FieldState<String> {
    match value.and_then(Value::as_str) {
        Some(value) if !value.is_empty() => FieldState::Known(value.to_owned()),
        Some(_) => FieldState::Unavailable {
            reason: crate::domain::UnknownReason::Ambiguous,
        },
        None => FieldState::Missing,
    }
}
fn accepted_order_id(value: &Value, expected_client_order_id: &str) -> Result<String, BitgetError> {
    let object = object(bitget_data(value)?)?;
    if object.get("clientOid").and_then(Value::as_str) != Some(expected_client_order_id) {
        return Err(BitgetError::Payload);
    }
    identifier(object.get("orderId"))
}
fn is_post_only_order(value: &Value) -> bool {
    object(value)
        .ok()
        .and_then(|order| order.get("timeInForce"))
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("post_only"))
}
fn bitget_signature(
    secret: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
) -> Result<String, BitgetError> {
    let query = if query.is_empty() {
        String::new()
    } else {
        format!("?{query}")
    };
    let payload = format!(
        "{timestamp}{}{}{}{}",
        method.to_ascii_uppercase(),
        path,
        query,
        String::from_utf8_lossy(body)
    );
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).map_err(|_| BitgetError::Credentials)?;
    mac.update(payload.as_bytes());
    Ok(STANDARD.encode(mac.finalize().into_bytes()))
}
fn bitget_ws_signature(secret: &str, timestamp: u64) -> Result<String, BitgetError> {
    bitget_signature(
        secret,
        &timestamp.to_string(),
        "GET",
        "/user/verify",
        "",
        &[],
    )
}
pub(crate) fn bitget_data(value: &Value) -> Result<&Value, BitgetError> {
    let object = object(value)?;
    let code = object.get("code").and_then(Value::as_str);
    if code != Some("00000") {
        return Err(BitgetError::RejectedCode {
            code: sanitized_rejection_field(code.unwrap_or("missing")),
            message: sanitized_rejection_field(
                object
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("missing"),
            ),
        });
    }
    object.get("data").ok_or(BitgetError::Payload)
}

fn readback_error(surface: &'static str, error: BitgetError) -> BitgetError {
    match error {
        BitgetError::Payload => BitgetError::Readback(surface),
        value => value,
    }
}

fn readback_shape(
    surface: &'static str,
    value: Result<&Value, BitgetError>,
    error: BitgetError,
) -> BitgetError {
    if !matches!(error, BitgetError::Payload) {
        return error;
    }
    let shape = match value {
        Ok(Value::Array(_)) => "array".to_owned(),
        Ok(Value::Object(object)) => format!(
            "object:{}",
            object
                .iter()
                .map(|(key, value)| format!("{key}={}", json_shape(value)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Ok(Value::String(_)) => "string".to_owned(),
        Ok(Value::Number(_)) => "number".to_owned(),
        Ok(Value::Bool(_)) => "boolean".to_owned(),
        Ok(Value::Null) => "null".to_owned(),
        Err(_) => "unavailable".to_owned(),
    };
    BitgetError::ReadbackShape { surface, shape }
}

fn json_shape(value: &Value) -> &'static str {
    match value {
        Value::Array(_) => "array",
        Value::Object(_) => "object",
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Null => "null",
    }
}
fn response_json(response: Response) -> Result<Value, BitgetError> {
    parse_json(&response_text(response)?)
}
fn response_text(response: Response) -> Result<String, BitgetError> {
    let status = response.status();
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err(BitgetError::RateLimited);
    }
    if status.is_server_error() {
        return Err(BitgetError::Http);
    }
    if status.is_client_error() {
        let body = response.text().map_err(|_| BitgetError::Http)?;
        return Err(http_rejection(status, &body));
    }
    response.text().map_err(|_| BitgetError::Http)
}

fn http_rejection(status: StatusCode, body: &str) -> BitgetError {
    let parsed = serde_json::from_str::<Value>(body).ok();
    let code = parsed
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|object| object.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("missing");
    let message = parsed
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|object| object.get("msg"))
        .and_then(Value::as_str)
        .unwrap_or("missing");
    BitgetError::RejectedHttp {
        status: status.as_u16(),
        code: sanitized_rejection_field(code),
        message: sanitized_rejection_field(message),
    }
}

fn sanitized_rejection_field(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(160)
        .collect()
}
fn parse_json(text: &str) -> Result<Value, BitgetError> {
    serde_json::from_str(text).map_err(|_| BitgetError::Payload)
}
fn object(value: &Value) -> Result<&Map<String, Value>, BitgetError> {
    bitget_risk::object(value).map_err(BitgetError::from)
}
fn text<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, BitgetError> {
    bitget_risk::text(object, field).map_err(BitgetError::from)
}
fn identifier(value: Option<&Value>) -> Result<String, BitgetError> {
    match value {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(value.to_owned()),
        Some(Value::Number(value)) => Ok(value.to_string()),
        _ => Err(BitgetError::Payload),
    }
}
fn decimal(object: &Map<String, Value>, field: &str) -> Result<Decimal, BitgetError> {
    bitget_risk::decimal(object, field).map_err(BitgetError::from)
}
fn decimal_value(value: Option<&Value>) -> Result<Decimal, BitgetError> {
    bitget_risk::decimal_value(value).map_err(BitgetError::from)
}
fn optional_decimal(value: Option<&Value>) -> Result<Decimal, BitgetError> {
    bitget_account::optional_decimal(value).map_err(BitgetError::from)
}
fn optional_price(value: Option<&Value>) -> Result<Option<Price>, BitgetError> {
    bitget_account::optional_price(value).map_err(BitgetError::from)
}
fn optional_price_state(value: Option<&Value>) -> Result<FieldState<Price>, BitgetError> {
    match value {
        None => Ok(FieldState::Missing),
        Some(Value::Null) => Ok(FieldState::Null),
        Some(Value::String(value)) if value == "0" || value.is_empty() => {
            Ok(FieldState::Unavailable {
                reason: crate::domain::UnknownReason::VenueUnavailable,
            })
        }
        value => Price::new(decimal_value(value)?)
            .map(FieldState::Known)
            .map_err(|_| BitgetError::Payload),
    }
}
fn amount_state(value: Option<&Value>) -> Result<FieldState<Amount>, BitgetError> {
    match value {
        None => Ok(FieldState::Missing),
        Some(Value::Null) => Ok(FieldState::Null),
        Some(Value::String(value)) if value.is_empty() => Ok(FieldState::Unavailable {
            reason: crate::domain::UnknownReason::VenueUnavailable,
        }),
        value => Ok(FieldState::Known(Amount::new(
            Asset::new("USDT").map_err(|_| BitgetError::Payload)?,
            decimal_value(value)?,
        ))),
    }
}
fn optional_timestamp_ms(value: Option<&Value>) -> Result<Option<u64>, BitgetError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        value => timestamp_ms(value).map(Some),
    }
}
fn timestamp_ms(value: Option<&Value>) -> Result<u64, BitgetError> {
    match value {
        Some(Value::String(value)) => value.parse().map_err(|_| BitgetError::Clock),
        Some(Value::Number(value)) => value.to_string().parse().map_err(|_| BitgetError::Clock),
        _ => Err(BitgetError::Clock),
    }
}
fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}
fn encode_component(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}
fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn wall_clock_ms() -> Result<u64, BitgetError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BitgetError::Clock)
        .and_then(|duration| u64::try_from(duration.as_millis()).map_err(|_| BitgetError::Clock))
}

type PrivateReadbackResults<A, B, C, D, E> = (
    Result<A, BitgetError>,
    Result<B, BitgetError>,
    Result<C, BitgetError>,
    Result<D, BitgetError>,
    Result<E, BitgetError>,
);

type CompletePrivateReadback<A, B, C, D, E> = (A, B, C, D, E);

fn complete_private_readback_tuple<A, B, C, D, E>(
    results: PrivateReadbackResults<A, B, C, D, E>,
) -> Result<CompletePrivateReadback<A, B, C, D, E>, BitgetError> {
    let (assets, settings, positions, orders, fills) = results;
    Ok((assets?, settings?, positions?, orders?, fills?))
}

#[derive(Debug, thiserror::Error)]
pub enum BitgetError {
    #[error("Bitget API credentials are missing or invalid")]
    Credentials,
    #[error("Bitget transport failed")]
    Http,
    #[error("Bitget rate limited the request")]
    RateLimited,
    #[error("Bitget rejected the request")]
    Rejected,
    #[error("Bitget rejected the request with business code {code}: {message}")]
    RejectedCode { code: String, message: String },
    #[error("Bitget rejected the request with HTTP {status}, code {code}: {message}")]
    RejectedHttp {
        status: u16,
        code: String,
        message: String,
    },
    #[error("Bitget returned an invalid or incomplete payload")]
    Payload,
    #[error("Bitget private {0} response is incomplete for a safe readback")]
    Readback(&'static str),
    #[error("Bitget private {surface} data shape is unsupported: {shape}")]
    ReadbackShape {
        surface: &'static str,
        shape: String,
    },
    #[error("Bitget fill history pagination cannot prove a complete, unique result")]
    Pagination,
    #[error("Bitget server clock is unavailable or invalid")]
    Clock,
    #[error("Bitget symbol is outside the USDT perpetual deployment")]
    Symbol,
    #[error("Bitget contract rules are not usable for this grid")]
    Instrument,
    #[error("Bitget account is not in the required Hedge position mode")]
    PositionMode,
    #[error("Bitget risk snapshot is incomplete or internally inconsistent")]
    RiskSnapshot,
    #[error("Bitget client order identity is invalid")]
    ClientOrderId,
    #[error("Bitget command is invalid for the normalized order semantics")]
    Command,
    #[error("Bitget could not find the exact client order identity")]
    OrderAbsent,
    #[error("Bitget private WebSocket is unavailable or returned an invalid event")]
    WebSocket,
    #[error("Bitget private WebSocket endpoint is invalid")]
    WebSocketEndpoint,
    #[error("Bitget private WebSocket connection failed: {reason}")]
    WebSocketConnect { reason: String },
    #[error("Bitget private WebSocket socket setup failed")]
    WebSocketSetup,
    #[error("Bitget private WebSocket login was rejected or timed out")]
    WebSocketLogin,
    #[error("Bitget private WebSocket subscription was rejected or timed out")]
    WebSocketSubscribe,
    #[error("Bitget private WebSocket closed")]
    StreamClosed,
}

impl From<venue_gateway_bitget::risk::BitgetRiskError> for BitgetError {
    fn from(error: venue_gateway_bitget::risk::BitgetRiskError) -> Self {
        match error {
            venue_gateway_bitget::risk::BitgetRiskError::Payload => Self::Payload,
            venue_gateway_bitget::risk::BitgetRiskError::Symbol => Self::Symbol,
            venue_gateway_bitget::risk::BitgetRiskError::PositionMode => Self::PositionMode,
            venue_gateway_bitget::risk::BitgetRiskError::RiskSnapshot => Self::RiskSnapshot,
        }
    }
}

impl From<venue_gateway_bitget::account::BitgetAccountError> for BitgetError {
    fn from(error: venue_gateway_bitget::account::BitgetAccountError) -> Self {
        match error {
            venue_gateway_bitget::account::BitgetAccountError::Payload => Self::Payload,
            venue_gateway_bitget::account::BitgetAccountError::Symbol => Self::Symbol,
        }
    }
}

fn is_success_code(value: Option<&Value>) -> bool {
    matches!(value, Some(Value::String(code)) if code == "0")
        || matches!(value, Some(Value::Number(code)) if code.as_u64() == Some(0))
}

#[cfg(test)]
#[path = "bitget_tests.rs"]
mod tests;
