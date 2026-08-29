use std::{
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    str::FromStr,
    time::Duration,
};

use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tungstenite::{Message, WebSocket, stream::MaybeTlsStream};

use crate::{
    domain::{
        AccountRiskSnapshot, AggressorSide, Amount, Asset, FieldState, Instrument, LegRiskSnapshot,
        MarkFunding, MarketDelta, MarketEvent, MarketKind, MarketLevel, MarketOrderCommand,
        MarketReduceCommand, MarketSnapshot, OrderCommand, Price, PublicBar, PublicTicker,
        PublicTrade, StopMarketCloseAllCommand, StopMarketFullPositionCommand, Symbol,
        UnknownReason,
    },
    market::{RawMarketRecord, RawSource},
};

use super::{
    binance_clock, binance_portfolio,
    binance_private::{self, PrivateParseError, PrivateReadback},
};

mod public_stream;
pub use public_stream::{PublicStream, PublicStreamSocket, depth_stream_url, public_stream_url};
mod market_scan;
pub use market_scan::parse_usdt_perpetual_market_rank_samples;
pub use venue_gateway_binance::native_symbol;

pub const PARSER_SCHEMA_VERSION: u16 = 1;
const REST_BASE_URL: &str = "https://fapi.binance.com";
const PORTFOLIO_REST_BASE_URL: &str = "https://papi.binance.com";
const SPOT_REST_BASE_URL: &str = "https://api.binance.com";
const PORTFOLIO_PRIVATE_STREAM_BASE_URL: &str = "wss://fstream.binance.com/pm/ws";
const PRIVATE_STREAM_CONNECT_TARGET: &str = "fstream.binance.com:443";
const PROXY_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PROXY_RESPONSE_LIMIT: usize = 8 * 1024;
const PUBLIC_GET_TRANSPORT_ATTEMPTS: u8 = 3;
const PUBLIC_GET_RETRY_BASE_MS: u64 = 100;
const USER_TRADES_PAGE_LIMIT: u16 = 1_000;
const USER_TRADES_MAX_PAGES: u32 = 10_000;
const USER_TRADES_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const API_KEY_ENV: &str = "BINANCE_API_KEY";
const API_SECRET_ENV: &str = "BINANCE_API_SECRET";
type HmacSha256 = Hmac<sha2::Sha256>;

fn decode_sha256(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut decoded = [0_u8; 32];
    for (index, slot) in decoded.iter_mut().enumerate() {
        let offset = index.checked_mul(2)?;
        *slot = u8::from_str_radix(value.get(offset..offset.checked_add(2)?)?, 16).ok()?;
    }
    Some(decoded)
}

/// Public-market client. It intentionally has no credential or mutation API.
#[derive(Clone, Debug)]
pub struct PublicRest {
    base_url: String,
    client: reqwest::blocking::Client,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BinanceContractRules {
    pub instrument: Instrument,
    pub minimum_quantity: Decimal,
}

impl PublicRest {
    pub fn production() -> Result<Self, PublicError> {
        Self::with_base_url(REST_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Result<Self, PublicError> {
        let builder = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .http1_only();
        let builder = match configured_reqwest_proxy().map_err(|_| PublicError::Proxy)? {
            Some(proxy) => builder.proxy(proxy).pool_max_idle_per_host(0),
            None => builder,
        };
        let client = builder.build().map_err(http_error)?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            client,
        })
    }

    pub fn exchange_info(&self) -> Result<String, PublicError> {
        self.get("/fapi/v1/exchangeInfo")
    }

    pub(crate) fn contract_rules(
        &self,
        symbol: &Symbol,
        generation: u64,
    ) -> Result<BinanceContractRules, BinanceRulesError> {
        let payload = self.exchange_info()?;
        parse_contract_rules(&payload, symbol.clone(), generation).map_err(Into::into)
    }

    /// Returns the complete USDⓈ-M 24-hour ticker array for adapter-side normalization.
    pub fn ticker_24hr(&self) -> Result<String, PublicError> {
        self.get("/fapi/v1/ticker/24hr")
    }

    pub fn depth_snapshot(&self, symbol: &Symbol, limit: u16) -> Result<String, PublicError> {
        if !matches!(limit, 5 | 10 | 20 | 50 | 100 | 500 | 1000) {
            return Err(PublicError::DepthLimit);
        }
        self.get(&format!(
            "/fapi/v1/depth?symbol={}&limit={limit}",
            native_symbol(symbol)
        ))
    }

    pub fn closed_kline_bootstrap(&self, symbol: &Symbol) -> Result<String, PublicError> {
        self.get(&format!(
            "/fapi/v1/klines?symbol={}&interval=1m&limit=22",
            native_symbol(symbol)
        ))
    }

    fn get(&self, path: &str) -> Result<String, PublicError> {
        for attempt in 0..PUBLIC_GET_TRANSPORT_ATTEMPTS {
            let response = match self.client.get(format!("{}{}", self.base_url, path)).send() {
                Ok(response) => response,
                Err(_source) if attempt + 1 < PUBLIC_GET_TRANSPORT_ATTEMPTS => {
                    std::thread::sleep(Duration::from_millis(
                        PUBLIC_GET_RETRY_BASE_MS * u64::from(attempt + 1),
                    ));
                    continue;
                }
                Err(source) => return Err(http_error(source)),
            };
            match public_http_fault(response.status()) {
                Some(PublicError::RateLimited) => return Err(PublicError::RateLimited),
                Some(PublicError::ServerFailure(status)) => {
                    return Err(PublicError::ServerFailure(status));
                }
                None => {}
                Some(_) => return Err(PublicError::HttpStatus(response.status().as_u16())),
            }
            return response
                .error_for_status()
                .map_err(http_error)?
                .text()
                .map_err(http_error);
        }
        Err(PublicError::TransportRetriesExhausted)
    }
}

/// Credentials exist only in process memory and are never read from project configuration.
pub struct PrivateCredentials {
    pub(super) api_key: String,
    pub(super) secret: Vec<u8>,
}

impl PrivateCredentials {
    pub fn from_environment() -> Result<Self, PrivateError> {
        let api_key =
            crate::credential_env::required(API_KEY_ENV).map_err(|_| PrivateError::Credentials)?;
        let secret = crate::credential_env::required(API_SECRET_ENV)
            .map_err(|_| PrivateError::Credentials)?;
        Ok(Self {
            api_key,
            secret: secret.into_bytes(),
        })
    }
}

/// Signed USDⓈ-M read surface. It has no mutation method; execution is added only after the
/// command WAL and exact readback path are connected in the same change.
pub struct PrivateRest {
    pub(super) base_url: String,
    pub(super) client: reqwest::blocking::Client,
    pub(super) credentials: PrivateCredentials,
    pub(super) clock: binance_clock::ServerClock,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BinanceRiskReadback {
    pub raw_private_payloads: Vec<String>,
    pub account: AccountRiskSnapshot,
    pub legs: Vec<LegRiskSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BinanceGridPrivateReadback {
    pub raw_private_payloads: Vec<String>,
    pub normalized: PrivateReadback,
    pub signed_regular_order_payloads: Vec<String>,
    pub algo_orders: Vec<crate::domain::Order>,
    pub signed_algo_order_payloads: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecentFillsCursor {
    pub observed_through_ms: u64,
    pub last_trade_id: Option<u64>,
    pub last_event_time_ms: Option<u64>,
}

/// One bounded request issued by the cursor paginator. It is public so a
/// local fixture or a credential-owning gateway can inspect the exact query
/// contract without constructing a network client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecentFillsPageRequest {
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub from_id: Option<u64>,
    pub limit: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecentFillsReadback {
    pub payload: String,
    pub cursor: RecentFillsCursor,
    pub pages: u32,
}

/// A listen key is sensitive connection material and deliberately has no Display or Debug impl.
pub struct PrivateListenKey(String);

pub struct PrivateStreamSocket {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HttpConnectProxy {
    host: String,
    port: u16,
}

impl PrivateRest {
    #[cfg(test)]
    pub(crate) fn recovery_test_client(api_key: &str, secret: &[u8]) -> Result<Self, PrivateError> {
        let client = reqwest::blocking::Client::builder()
            .build()
            .map_err(private_http_error)?;
        Ok(Self {
            base_url: "https://recovery.invalid".to_owned(),
            client,
            credentials: PrivateCredentials {
                api_key: api_key.to_owned(),
                secret: secret.to_vec(),
            },
            clock: binance_clock::ServerClock::new(),
        })
    }

    pub fn production(
        credentials: PrivateCredentials,
        binding: crate::config::BinanceAccountBinding,
    ) -> Result<Self, PrivateError> {
        let builder = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .http1_only();
        let builder = match configured_reqwest_proxy()? {
            // Some CONNECT proxies close an idle tunneled HTTP/1.1 connection without notifying
            // the client. A private readback must then reconnect rather than misclassify that
            // stale tunnel as an unknown exchange outcome.
            Some(proxy) => builder.proxy(proxy).pool_max_idle_per_host(0),
            None => builder,
        };
        let client = builder.build().map_err(private_http_error)?;
        let client = Self {
            base_url: match binding {
                crate::config::BinanceAccountBinding::PortfolioMarginUm => {
                    PORTFOLIO_REST_BASE_URL.to_owned()
                }
            },
            client,
            credentials,
            clock: binance_clock::ServerClock::new(),
        };
        binance_clock::synchronize(&client.client, &client.base_url, &client.clock)?;
        Ok(client)
    }

    /// Recovery evidence is authenticated locally with the same in-memory credential that signed
    /// the private requests. Neither API key nor secret is written into the artifact.
    pub(crate) fn recovery_signer_sha256(&self) -> String {
        format!("{:x}", Sha256::digest(self.credentials.api_key.as_bytes()))
    }

    pub(crate) fn sign_recovery_payload_sha256(
        &self,
        payload_sha256: &str,
    ) -> Result<String, PrivateError> {
        let mut signer = HmacSha256::new_from_slice(&self.credentials.secret)
            .map_err(|_| PrivateError::Credentials)?;
        signer.update(b"venue.canary.recovery.readback.v1\0");
        signer.update(payload_sha256.as_bytes());
        Ok(format!("{:x}", signer.finalize().into_bytes()))
    }

    pub(crate) fn verify_recovery_payload_signature(
        &self,
        payload_sha256: &str,
        signature_sha256: &str,
    ) -> bool {
        let Some(signature) = decode_sha256(signature_sha256) else {
            return false;
        };
        let Ok(mut signer) = HmacSha256::new_from_slice(&self.credentials.secret) else {
            return false;
        };
        signer.update(b"venue.canary.recovery.readback.v1\0");
        signer.update(payload_sha256.as_bytes());
        signer.verify_slice(&signature).is_ok()
    }

    pub fn account(&self) -> Result<String, PrivateError> {
        self.signed_get("/papi/v1/account", Vec::new())
    }

    pub(super) fn portfolio_asset_index_price(&self, asset: &str) -> Result<String, PrivateError> {
        let response = self
            .client
            .get(format!(
                "{SPOT_REST_BASE_URL}/sapi/v1/portfolio/asset-index-price"
            ))
            .query(&[("asset", asset)])
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .map_err(private_http_error)?;
        private_response_text(response)
    }

    pub fn create_user_stream(&self) -> Result<PrivateListenKey, PrivateError> {
        let response = self
            .client
            .post(format!("{}/papi/v1/listenKey", self.base_url))
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .map_err(private_http_error)?;
        let payload = private_response_text(response)?;
        let listen_key = serde_json::from_str::<Value>(&payload)
            .ok()
            .and_then(|value| {
                value
                    .get("listenKey")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .filter(|value| !value.is_empty())
            .ok_or(PrivateError::ListenKey)?;
        Ok(PrivateListenKey(listen_key))
    }

    /// Extends the one account-scoped Portfolio Margin user stream.
    ///
    /// Unlike the USD-M listen-key API, PAPI accepts no `listenKey` query parameter here. The
    /// currently active key is selected by the API key in the request header.
    pub fn keepalive_user_stream(&self) -> Result<(), PrivateError> {
        let response = self
            .client
            .put(format!("{}/papi/v1/listenKey", self.base_url))
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .map_err(private_http_error)?;
        private_response_text(response).map(|_| ())
    }

    pub fn positions(&self, symbol: &Symbol) -> Result<String, PrivateError> {
        self.signed_get(
            "/papi/v1/um/positionRisk",
            vec![("symbol", native_symbol(symbol))],
        )
    }

    /// Reads only the requested UM symbol's hedge legs. This bounded path is used after a Canary
    /// fill so protection does not wait on unrelated balance, order, and fill endpoints.
    pub fn position_readback(
        &self,
        symbol: &Symbol,
    ) -> Result<Vec<crate::domain::Position>, PrivateReadbackError> {
        let payload = self
            .positions(symbol)
            .map_err(PrivateReadbackError::UmAccountRequest)?;
        binance_private::parse_positions(&payload, symbol).map_err(PrivateReadbackError::Parse)
    }

    pub fn open_orders(&self, symbol: &Symbol) -> Result<String, PrivateError> {
        self.signed_get(
            "/papi/v1/um/openOrders",
            vec![("symbol", native_symbol(symbol))],
        )
    }

    pub fn open_algo_orders(&self, symbol: &Symbol) -> Result<String, PrivateError> {
        self.signed_get(
            "/papi/v1/um/algo/openAlgoOrders",
            vec![
                ("algoType", "CONDITIONAL".to_owned()),
                ("symbol", native_symbol(symbol)),
            ],
        )
    }

    /// PAPI independently reports hedge versus one-way UM position mode.
    pub fn position_mode(&self) -> Result<String, PrivateError> {
        self.signed_get("/papi/v1/um/positionSide/dual", Vec::new())
    }

    /// PAPI exposes the UM API-key trading permission separately from aggregate account risk.
    pub fn um_account_config(&self) -> Result<String, PrivateError> {
        self.signed_get("/papi/v1/um/accountConfig", Vec::new())
    }

    /// Obtains one signed account/symbol observation. The caller must journal the returned
    /// normalized facts before treating the private session as ready.
    pub fn readback(&self, symbol: &Symbol) -> Result<PrivateReadback, PrivateReadbackError> {
        let mut readback = self.readback_without_fills(symbol)?;
        let fills = self
            .recent_fills(symbol)
            .map_err(PrivateReadbackError::FillsRequest)?;
        readback.fills =
            binance_private::parse_fills(&fills, symbol).map_err(PrivateReadbackError::Parse)?;
        Ok(readback)
    }

    /// Returns the same normalized signed readback together with every exact response body. The
    /// shared grid runtime persists the raw tuple before admitting the normalized facts.
    pub(crate) fn grid_readback(
        &self,
        symbol: &Symbol,
    ) -> Result<BinanceGridPrivateReadback, PrivateReadbackError> {
        let account = self
            .account()
            .map_err(PrivateReadbackError::AccountRequest)?;
        let positions = self
            .positions(symbol)
            .map_err(PrivateReadbackError::UmAccountRequest)?;
        let position_mode = self
            .position_mode()
            .map_err(PrivateReadbackError::PositionModeRequest)?;
        let account_config = self
            .um_account_config()
            .map_err(PrivateReadbackError::AccountConfigRequest)?;
        let orders = self
            .open_orders(symbol)
            .map_err(PrivateReadbackError::OpenOrdersRequest)?;
        let algo_orders = self
            .open_algo_orders(symbol)
            .map_err(PrivateReadbackError::AlgoOrdersRequest)?;
        let fills = self
            .recent_fills(symbol)
            .map_err(PrivateReadbackError::FillsRequest)?;
        let normalized = private_readback_from_payloads(
            symbol,
            &account,
            &positions,
            &account_config,
            &position_mode,
            &orders,
            &fills,
        )?;
        let algo_order_facts = binance_private::parse_open_algo_order_facts(&algo_orders, symbol)
            .map_err(PrivateReadbackError::Parse)?;
        Ok(BinanceGridPrivateReadback {
            raw_private_payloads: vec![
                account,
                positions,
                position_mode,
                account_config,
                orders.clone(),
                algo_orders.clone(),
                fills,
            ],
            normalized,
            signed_regular_order_payloads: vec![orders],
            algo_orders: algo_order_facts,
            signed_algo_order_payloads: vec![algo_orders],
        })
    }

    /// Obtains the fixed signed account/position/order portion used by the resident facts worker.
    /// Fills are deliberately excluded because their only authoritative path is the durable
    /// facts-first recovery cursor.
    pub fn readback_without_fills(
        &self,
        symbol: &Symbol,
    ) -> Result<PrivateReadback, PrivateReadbackError> {
        let account = self
            .account()
            .map_err(PrivateReadbackError::AccountRequest)?;
        let positions = self
            .positions(symbol)
            .map_err(PrivateReadbackError::UmAccountRequest)?;
        let position_mode = self
            .position_mode()
            .map_err(PrivateReadbackError::PositionModeRequest)?;
        let account_config = self
            .um_account_config()
            .map_err(PrivateReadbackError::AccountConfigRequest)?;
        let orders = self
            .open_orders(symbol)
            .map_err(PrivateReadbackError::OpenOrdersRequest)?;
        private_readback_from_payloads(
            symbol,
            &account,
            &positions,
            &account_config,
            &position_mode,
            &orders,
            "[]",
        )
    }

    pub(crate) fn authoritative_now_ms(&self) -> Result<u64, PrivateError> {
        self.clock.now_ms()
    }

    pub fn order_by_client_id(
        &self,
        symbol: &Symbol,
        client_order_id: &str,
    ) -> Result<String, PrivateError> {
        validate_client_order_id(client_order_id)?;
        self.signed_get(
            "/papi/v1/um/order",
            vec![
                ("symbol", native_symbol(symbol)),
                ("origClientOrderId", client_order_id.to_owned()),
            ],
        )
    }

    pub fn conditional_order_by_client_strategy_id(
        &self,
        symbol: &Symbol,
        client_strategy_id: &str,
    ) -> Result<String, PrivateError> {
        self.signed_get(
            "/papi/v1/um/conditional/openOrder",
            vec![
                ("symbol", native_symbol(symbol)),
                ("newClientStrategyId", client_strategy_id.to_owned()),
            ],
        )
    }

    pub fn conditional_order_history_by_client_strategy_id(
        &self,
        symbol: &Symbol,
        client_strategy_id: &str,
    ) -> Result<String, PrivateError> {
        self.signed_get(
            "/papi/v1/um/conditional/orderHistory",
            vec![
                ("symbol", native_symbol(symbol)),
                ("newClientStrategyId", client_strategy_id.to_owned()),
            ],
        )
    }

    /// Current PAPI UM Algo protection has a distinct identity namespace and readback endpoint.
    pub fn algo_order_by_client_algo_id(
        &self,
        client_algo_id: &str,
    ) -> Result<String, PrivateError> {
        self.signed_get(
            "/papi/v1/um/algo/algoOrder",
            vec![("clientAlgoId", client_algo_id.to_owned())],
        )
    }

    /// Algo history is symbol-scoped and returns an array; the parser selects the exact client ID.
    pub fn algo_order_history(&self, symbol: &Symbol) -> Result<String, PrivateError> {
        self.signed_get(
            "/papi/v1/um/algo/allAlgoOrders",
            vec![("symbol", native_symbol(symbol))],
        )
    }

    pub fn recent_fills(&self, symbol: &Symbol) -> Result<String, PrivateError> {
        self.signed_get(
            "/papi/v1/um/userTrades",
            user_trades_parameters(symbol, None, 0, 0),
        )
    }

    /// Reads all user trades after a durable cursor through a bounded set of
    /// PAPI pages. This is the recovery path; `recent_fills` remains a single
    /// snapshot for the existing account readback contract.
    pub fn recent_fills_since(
        &self,
        symbol: &Symbol,
        cursor: RecentFillsCursor,
        target_through_ms: u64,
    ) -> Result<RecentFillsReadback, PrivateError> {
        paginate_recent_fills(cursor, target_through_ms, |request| {
            self.signed_get(
                "/papi/v1/um/userTrades",
                user_trades_parameters(
                    symbol,
                    request.from_id,
                    request.start_time_ms,
                    request.end_time_ms,
                ),
            )
        })
    }

    pub(crate) fn place_limit(&self, command: &OrderCommand) -> Result<String, PrivateError> {
        self.signed_post(
            "/papi/v1/um/order",
            limit_order_parameters(command, LimitTimeInForce::GoodTillCancel)?,
        )
    }

    /// Places a non-marketable PAPI UM limit order. GTX is the only difference from the
    /// ordinary GTC entry; hedge-side identity and client order identity stay unchanged.
    pub(crate) fn place_limit_post_only(
        &self,
        command: &OrderCommand,
    ) -> Result<String, PrivateError> {
        self.signed_post(
            "/papi/v1/um/order",
            limit_order_parameters(command, LimitTimeInForce::PostOnly)?,
        )
    }

    /// Protection probes use IOC so VPN/readback delay can never leave an unfilled remainder live.
    pub(crate) fn place_limit_immediate_or_cancel(
        &self,
        command: &OrderCommand,
    ) -> Result<String, PrivateError> {
        self.signed_post(
            "/papi/v1/um/order",
            limit_order_parameters(command, LimitTimeInForce::ImmediateOrCancel)?,
        )
    }

    /// Places one exposure-increasing PAPI UM market order for inventory replenishment. The
    /// domain command rejects reduce-only use; Hedge-mode position side is still explicit.
    pub(crate) fn place_market(
        &self,
        command: &MarketOrderCommand,
    ) -> Result<String, PrivateError> {
        self.signed_post("/papi/v1/um/order", market_order_parameters(command)?)
    }

    /// PAPI Hedge mode rejects wire-level reduceOnly. Safety is carried by the dedicated command,
    /// exact opposite side, concrete positionSide and frozen private-position generation.
    pub(crate) fn place_market_reduce(
        &self,
        command: &MarketReduceCommand,
    ) -> Result<String, PrivateError> {
        self.signed_post("/papi/v1/um/order", market_reduce_parameters(command)?)
    }

    pub(crate) fn place_stop_market_close_all(
        &self,
        command: &StopMarketCloseAllCommand,
    ) -> Result<String, PrivateError> {
        command.validate().map_err(PrivateError::Command)?;
        if command.owner.exchange != "binance" {
            return Err(PrivateError::Owner);
        }
        self.signed_form_post(
            "/papi/v1/um/conditional/order",
            stop_market_close_all_parameters(command)?,
        )
    }

    /// Places an exact-quantity STOP_MARKET through Binance's current PAPI UM Algo family.
    /// Hedge Mode forbids `reduceOnly`; the semantic full-position constraint is validated before
    /// this adapter and the wire request deliberately contains no such flag.
    pub(crate) fn place_stop_market_full_position(
        &self,
        command: &StopMarketFullPositionCommand,
    ) -> Result<String, PrivateError> {
        command.validate().map_err(PrivateError::Command)?;
        if command.owner.exchange != "binance" {
            return Err(PrivateError::Owner);
        }
        self.signed_form_post(
            "/papi/v1/um/algo/order",
            stop_market_full_position_parameters(command)?,
        )
    }

    pub(crate) fn cancel_by_client_id(
        &self,
        symbol: &Symbol,
        client_order_id: &str,
    ) -> Result<String, PrivateError> {
        validate_client_order_id(client_order_id)?;
        self.signed_delete(
            "/papi/v1/um/order",
            vec![
                ("symbol", native_symbol(symbol)),
                ("origClientOrderId", client_order_id.to_owned()),
            ],
        )
    }

    pub(crate) fn verify_post_only_order_by_client_id(
        &self,
        symbol: &Symbol,
        client_order_id: &str,
    ) -> Result<(), PrivateError> {
        let payload = self.order_by_client_id(symbol, client_order_id)?;
        if is_post_only_order_response(&payload) {
            Ok(())
        } else {
            Err(PrivateError::PostOnlyVerification)
        }
    }

    pub(crate) fn cancel_conditional_by_client_strategy_id(
        &self,
        symbol: &Symbol,
        client_strategy_id: &str,
    ) -> Result<String, PrivateError> {
        self.signed_delete(
            "/papi/v1/um/conditional/order",
            vec![
                ("symbol", native_symbol(symbol)),
                ("newClientStrategyId", client_strategy_id.to_owned()),
            ],
        )
    }

    pub(crate) fn cancel_algo_by_client_algo_id(
        &self,
        client_algo_id: &str,
    ) -> Result<String, PrivateError> {
        self.signed_delete(
            "/papi/v1/um/algo/order",
            vec![("clientAlgoId", client_algo_id.to_owned())],
        )
    }
}

#[derive(Clone, Copy)]
enum LimitTimeInForce {
    GoodTillCancel,
    PostOnly,
    ImmediateOrCancel,
}

impl LimitTimeInForce {
    const fn as_papi(self) -> &'static str {
        match self {
            Self::GoodTillCancel => "GTC",
            Self::PostOnly => "GTX",
            Self::ImmediateOrCancel => "IOC",
        }
    }
}

/// PAPI UM hedge orders require a concrete LONG or SHORT `positionSide`. `reduceOnly` is
/// deliberately absent because Binance rejects it in Hedge mode.
fn limit_order_parameters(
    command: &OrderCommand,
    time_in_force: LimitTimeInForce,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    command.validate().map_err(PrivateError::Command)?;
    if command.owner.exchange != "binance" {
        return Err(PrivateError::Owner);
    }
    validate_client_order_id(command.client_order_id.as_str())?;
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    let position_side = native_position_side(command.position_side)?;
    Ok(vec![
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("type", "LIMIT".to_owned()),
        ("timeInForce", time_in_force.as_papi().to_owned()),
        ("quantity", command.quantity.to_string()),
        ("price", command.limit_price.value().to_string()),
        ("positionSide", position_side.to_owned()),
        ("newOrderRespType", "RESULT".to_owned()),
        (
            "newClientOrderId",
            command.client_order_id.as_str().to_owned(),
        ),
    ])
}

/// PAPI UM market entries use the same stable client identity and Hedge-mode position side as
/// limit entries, but deliberately omit price, time-in-force, and wire-level reduceOnly.
fn market_order_parameters(
    command: &MarketOrderCommand,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    command.validate().map_err(PrivateError::Command)?;
    if command.owner.exchange != "binance" {
        return Err(PrivateError::Owner);
    }
    validate_client_order_id(command.client_order_id.as_str())?;
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    let position_side = native_position_side(command.position_side)?;
    Ok(vec![
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("type", "MARKET".to_owned()),
        ("quantity", command.quantity.to_string()),
        ("positionSide", position_side.to_owned()),
        ("newOrderRespType", "RESULT".to_owned()),
        (
            "newClientOrderId",
            command.client_order_id.as_str().to_owned(),
        ),
    ])
}

fn market_reduce_parameters(
    command: &MarketReduceCommand,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    command.validate().map_err(PrivateError::Command)?;
    if command.owner.exchange != "binance" {
        return Err(PrivateError::Owner);
    }
    validate_client_order_id(command.client_order_id.as_str())?;
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    Ok(vec![
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("type", "MARKET".to_owned()),
        ("quantity", command.quantity.to_string()),
        (
            "positionSide",
            native_position_side(command.position_side)?.to_owned(),
        ),
        ("newOrderRespType", "RESULT".to_owned()),
        (
            "newClientOrderId",
            command.client_order_id.as_str().to_owned(),
        ),
    ])
}

fn private_readback_from_payloads(
    symbol: &Symbol,
    account: &str,
    positions: &str,
    account_config: &str,
    position_mode: &str,
    orders: &str,
    fills: &str,
) -> Result<PrivateReadback, PrivateReadbackError> {
    let capabilities = binance_portfolio::capabilities(account_config, position_mode)
        .map_err(PrivateReadbackError::Parse)?;
    let positions = binance_portfolio::complete_scoped_positions(
        binance_private::parse_positions(positions, symbol).map_err(PrivateReadbackError::Parse)?,
        symbol,
        capabilities.hedge_position,
    );
    Ok(PrivateReadback {
        capabilities,
        balances: vec![
            binance_portfolio::parse_account_balance(account)
                .map_err(PrivateReadbackError::Parse)?,
        ],
        positions,
        orders: binance_private::parse_orders(orders, symbol)
            .map_err(PrivateReadbackError::Parse)?,
        fills: binance_private::parse_fills(fills, symbol).map_err(PrivateReadbackError::Parse)?,
    })
}

fn stop_market_close_all_parameters(
    command: &StopMarketCloseAllCommand,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    Ok(vec![
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("strategyType", "STOP_MARKET".to_owned()),
        ("stopPrice", command.stop_price.value().to_string()),
        ("closePosition", "true".to_owned()),
        (
            "positionSide",
            native_position_side(command.position_side)?.to_owned(),
        ),
        ("workingType", "MARK_PRICE".to_owned()),
        ("priceProtect", "false".to_owned()),
        ("newOrderRespType", "RESULT".to_owned()),
        (
            "newClientStrategyId",
            command.client_strategy_id.as_str().to_owned(),
        ),
    ])
}

fn stop_market_full_position_parameters(
    command: &StopMarketFullPositionCommand,
) -> Result<Vec<(&'static str, String)>, PrivateError> {
    let side = match command.side {
        crate::domain::OrderSide::Buy => "BUY",
        crate::domain::OrderSide::Sell => "SELL",
    };
    let order_type = match command.owner.purpose {
        crate::domain::OrderPurpose::Protection => "STOP_MARKET",
        crate::domain::OrderPurpose::TakeProfit => "TAKE_PROFIT_MARKET",
        _ => return Err(PrivateError::Owner),
    };
    Ok(vec![
        ("algoType", "CONDITIONAL".to_owned()),
        ("symbol", native_symbol(&command.owner.symbol)),
        ("side", side.to_owned()),
        ("type", order_type.to_owned()),
        ("quantity", command.quantity.to_string()),
        (
            "positionSide",
            native_position_side(command.position_side)?.to_owned(),
        ),
        ("triggerPrice", command.trigger_price.value().to_string()),
        ("workingType", "MARK_PRICE".to_owned()),
        ("priceProtect", "false".to_owned()),
        ("newOrderRespType", "RESULT".to_owned()),
        ("clientAlgoId", command.client_algo_id.as_str().to_owned()),
    ])
}

fn native_position_side(
    position_side: crate::domain::PositionSide,
) -> Result<&'static str, PrivateError> {
    match position_side {
        crate::domain::PositionSide::Long => Ok("LONG"),
        crate::domain::PositionSide::Short => Ok("SHORT"),
        crate::domain::PositionSide::Net => Err(PrivateError::Command(
            crate::domain::CommandError::PositionSide,
        )),
    }
}

impl PrivateStreamSocket {
    pub fn connect(listen_key: &PrivateListenKey) -> Result<Self, PrivateError> {
        let url = private_stream_url(listen_key);
        let socket = connect_binance_stream(url.as_str()).map_err(private_websocket_error)?;
        Ok(Self { socket })
    }

    pub(crate) fn set_read_timeout(&mut self, timeout: Duration) -> Result<(), PrivateError> {
        let result = match self.socket.get_mut() {
            MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(timeout)),
            MaybeTlsStream::Rustls(stream) => stream.sock.set_read_timeout(Some(timeout)),
            _ => return Err(PrivateError::WebSocket),
        };
        result.map_err(|_| PrivateError::WebSocket)
    }

    /// Reads at most one application frame. A socket-read timeout is readiness, not failure.
    pub(crate) fn next_text_when_ready(&mut self) -> Result<Option<String>, PrivateError> {
        match self.socket.read() {
            Ok(Message::Text(text)) => Ok(Some(text.to_string())),
            Ok(Message::Ping(payload)) => {
                self.socket
                    .send(Message::Pong(payload))
                    .map_err(private_websocket_error)?;
                Ok(None)
            }
            Ok(Message::Close(_)) => Err(PrivateError::StreamClosed),
            Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => Ok(None),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(private_websocket_error(error)),
        }
    }

    pub fn next_text(&mut self) -> Result<String, PrivateError> {
        loop {
            match self.socket.read().map_err(private_websocket_error)? {
                Message::Text(text) => return Ok(text.to_string()),
                Message::Ping(payload) => self
                    .socket
                    .send(Message::Pong(payload))
                    .map_err(private_websocket_error)?,
                Message::Close(_) => return Err(PrivateError::StreamClosed),
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

pub(super) fn private_response_text(
    response: reqwest::blocking::Response,
) -> Result<String, PrivateError> {
    let status = response.status();
    if status.is_success() {
        return response.text().map_err(private_http_error);
    }
    if status.is_server_error() || status == reqwest::StatusCode::REQUEST_TIMEOUT {
        return Err(PrivateError::Unknown(status.as_u16()));
    }
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status == reqwest::StatusCode::IM_A_TEAPOT
    {
        return Err(PrivateError::RateLimited(status.as_u16()));
    }
    let api_code = response
        .text()
        .ok()
        .and_then(|payload| serde_json::from_str::<Value>(&payload).ok())
        .and_then(|payload| payload.get("code").and_then(Value::as_i64));
    Err(PrivateError::Rejected {
        status: status.as_u16(),
        api_code,
    })
}

fn private_stream_url(listen_key: &PrivateListenKey) -> String {
    format!("{PORTFOLIO_PRIVATE_STREAM_BASE_URL}/{}", listen_key.0)
}

/// `listenKeyExpired` is the only private payload that repeats the connection credential. Verify
/// it belongs to this socket, then redact it before the durable evidence boundary sees it.
pub(crate) fn sanitize_private_stream_payload_for_transport(
    listen_key: &PrivateListenKey,
    payload: String,
) -> Result<String, PrivateError> {
    let Ok(mut value) = serde_json::from_str::<Value>(&payload) else {
        return Ok(payload);
    };
    if value.get("e").and_then(Value::as_str) != Some("listenKeyExpired") {
        return Ok(payload);
    }
    if value.get("listenKey").and_then(Value::as_str) != Some(listen_key.0.as_str()) {
        return Err(PrivateError::ListenKey);
    }
    let object = value.as_object_mut().ok_or(PrivateError::ListenKey)?;
    object.insert(
        "listenKey".to_owned(),
        Value::String("[redacted]".to_owned()),
    );
    serde_json::to_string(&value).map_err(|_| PrivateError::ListenKey)
}

/// Both Binance stream families use `fstream.binance.com`. Public and private streams must take
/// the same explicitly configured proxy route; otherwise a private-ready deployment can never
/// warm its public market source.
pub(super) fn connect_binance_stream(
    url: &str,
) -> Result<WebSocket<MaybeTlsStream<TcpStream>>, tungstenite::Error> {
    match configured_http_connect_proxy().map_err(|_| proxy_websocket_error())? {
        None => connect_direct_binance_stream(url),
        Some(proxy) => {
            let stream = connect_http_tunnel(&proxy).map_err(|_| proxy_websocket_error())?;
            match tungstenite::client_tls(url, stream) {
                Ok((socket, _)) => Ok(socket),
                Err(tungstenite::HandshakeError::Failure(error)) => Err(error),
                Err(tungstenite::HandshakeError::Interrupted(_)) => Err(proxy_websocket_error()),
            }
        }
    }
}

fn connect_direct_binance_stream(
    url: &str,
) -> Result<WebSocket<MaybeTlsStream<TcpStream>>, tungstenite::Error> {
    let address = PRIVATE_STREAM_CONNECT_TARGET
        .to_socket_addrs()
        .map_err(tungstenite::Error::Io)?
        .next()
        .ok_or_else(proxy_websocket_error)?;
    let stream = TcpStream::connect_timeout(&address, PROXY_CONNECT_TIMEOUT)
        .map_err(tungstenite::Error::Io)?;
    stream
        .set_read_timeout(Some(PROXY_CONNECT_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(PROXY_CONNECT_TIMEOUT)))
        .map_err(tungstenite::Error::Io)?;
    match tungstenite::client_tls(url, stream) {
        Ok((socket, _)) => Ok(socket),
        Err(tungstenite::HandshakeError::Failure(error)) => Err(error),
        Err(tungstenite::HandshakeError::Interrupted(_)) => Err(proxy_websocket_error()),
    }
}

fn proxy_websocket_error() -> tungstenite::Error {
    tungstenite::Error::Io(std::io::Error::other("Binance HTTP CONNECT proxy failed"))
}

/// Uses conventional proxy variables in precedence order. A configured but invalid proxy never
/// falls back to a direct connection, because that would defeat an operator's network boundary.
fn configured_http_connect_proxy() -> Result<Option<HttpConnectProxy>, PrivateError> {
    for name in ["HTTPS_PROXY", "ALL_PROXY", "HTTP_PROXY"] {
        let value = match std::env::var(name) {
            Ok(value) if !value.trim().is_empty() => value,
            Ok(_) | Err(std::env::VarError::NotPresent) => continue,
            Err(std::env::VarError::NotUnicode(_)) => return Err(PrivateError::WebSocket),
        };
        return parse_http_connect_proxy(&value).map(Some);
    }
    Ok(None)
}

/// REST and WebSocket traffic must share the operator-selected proxy boundary. Reqwest's ambient
/// proxy discovery is not sufficient here because this deployment deliberately rejects proxy
/// forms it cannot prove as an HTTP CONNECT route.
fn configured_reqwest_proxy() -> Result<Option<reqwest::Proxy>, PrivateError> {
    let Some(proxy) = configured_http_connect_proxy()? else {
        return Ok(None);
    };
    let authority = if proxy.host.contains(':') {
        format!("[{}]:{}", proxy.host, proxy.port)
    } else {
        format!("{}:{}", proxy.host, proxy.port)
    };
    reqwest::Proxy::all(format!("http://{authority}"))
        .map(Some)
        .map_err(|_| PrivateError::Http)
}

/// Deliberately supports only unauthenticated HTTP proxies. HTTPS, SOCKS, credentials and paths
/// require a different handshake and therefore fail closed instead of being guessed at.
fn parse_http_connect_proxy(value: &str) -> Result<HttpConnectProxy, PrivateError> {
    let authority = value
        .trim()
        .strip_prefix("http://")
        .ok_or(PrivateError::WebSocket)?;
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    if authority.is_empty()
        || authority.contains(['/', '?', '#', '@'])
        || authority
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(PrivateError::WebSocket);
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, suffix) = rest.split_once(']').ok_or(PrivateError::WebSocket)?;
        let port = match suffix {
            "" => 80,
            _ => suffix
                .strip_prefix(':')
                .ok_or(PrivateError::WebSocket)?
                .parse::<u16>()
                .map_err(|_| PrivateError::WebSocket)?,
        };
        (host, port)
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) if !host.contains(':') => (
                host,
                port.parse::<u16>().map_err(|_| PrivateError::WebSocket)?,
            ),
            Some(_) => return Err(PrivateError::WebSocket),
            None => (authority, 80),
        }
    };
    if host.is_empty() || port == 0 {
        return Err(PrivateError::WebSocket);
    }
    Ok(HttpConnectProxy {
        host: host.to_owned(),
        port,
    })
}

fn connect_http_tunnel(proxy: &HttpConnectProxy) -> Result<TcpStream, PrivateError> {
    let address = (proxy.host.as_str(), proxy.port)
        .to_socket_addrs()
        .map_err(|_| PrivateError::WebSocket)?
        .next()
        .ok_or(PrivateError::WebSocket)?;
    let mut stream = TcpStream::connect_timeout(&address, PROXY_CONNECT_TIMEOUT)
        .map_err(|_| PrivateError::WebSocket)?;
    stream
        .set_read_timeout(Some(PROXY_CONNECT_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(PROXY_CONNECT_TIMEOUT)))
        .map_err(|_| PrivateError::WebSocket)?;
    let request = proxy_connect_request();
    stream
        .write_all(&request)
        .and_then(|()| stream.flush())
        .map_err(|_| PrivateError::WebSocket)?;
    read_proxy_connect_response(&mut stream)?;
    Ok(stream)
}

fn proxy_connect_request() -> Vec<u8> {
    format!(
        "CONNECT {PRIVATE_STREAM_CONNECT_TARGET} HTTP/1.1\r\nHost: {PRIVATE_STREAM_CONNECT_TARGET}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    )
    .into_bytes()
}

fn read_proxy_connect_response(stream: &mut TcpStream) -> Result<(), PrivateError> {
    let mut response = Vec::with_capacity(512);
    let mut chunk = [0_u8; 512];
    loop {
        let read = stream
            .read(&mut chunk)
            .map_err(|_| PrivateError::WebSocket)?;
        if read == 0 || response.len().saturating_add(read) > PROXY_RESPONSE_LIMIT {
            return Err(PrivateError::WebSocket);
        }
        response.extend_from_slice(&chunk[..read]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            return parse_proxy_connect_response(&response);
        }
    }
}

fn parse_proxy_connect_response(response: &[u8]) -> Result<(), PrivateError> {
    if response.len() > PROXY_RESPONSE_LIMIT {
        return Err(PrivateError::WebSocket);
    }
    let end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or(PrivateError::WebSocket)?;
    if end + 4 != response.len() {
        return Err(PrivateError::WebSocket);
    }
    let header = std::str::from_utf8(&response[..end]).map_err(|_| PrivateError::WebSocket)?;
    let status = header.lines().next().ok_or(PrivateError::WebSocket)?;
    let mut fields = status.split_ascii_whitespace();
    if !matches!(fields.next(), Some("HTTP/1.0" | "HTTP/1.1"))
        || fields.next() != Some("200")
        || fields.next().is_none()
    {
        return Err(PrivateError::WebSocket);
    }
    if header.lines().skip(1).any(|line| line.is_empty()) {
        return Err(PrivateError::WebSocket);
    }
    Ok(())
}

pub(super) fn signed_query(
    secret: &[u8],
    parameters: &[(&str, String)],
) -> Result<String, PrivateError> {
    let payload = parameters
        .iter()
        .map(|(key, value)| format!("{}={}", encode_component(key), encode_component(value)))
        .collect::<Vec<_>>()
        .join("&");
    let mut mac = HmacSha256::new_from_slice(secret).map_err(|_| PrivateError::Credentials)?;
    mac.update(payload.as_bytes());
    let signature = mac.finalize().into_bytes();
    let encoded_signature: String = signature.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!("{payload}&signature={encoded_signature}"))
}

fn encode_component(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![char::from(byte).to_string()]
            }
            _ => vec![format!("%{byte:02X}")],
        })
        .collect()
}

pub(crate) fn client_order_id_is_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 36
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'/' | b':' | b'_' | b'-')
        })
}

fn validate_client_order_id(value: &str) -> Result<(), PrivateError> {
    if client_order_id_is_valid(value) {
        Ok(())
    } else {
        Err(PrivateError::ClientOrderId)
    }
}

pub(crate) fn is_post_only_order_response(payload: &str) -> bool {
    serde_json::from_str::<Value>(payload)
        .ok()
        .and_then(|value| {
            value
                .get("timeInForce")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .is_some_and(|value| value.eq_ignore_ascii_case("GTX"))
}

pub fn public_http_fault(status: reqwest::StatusCode) -> Option<PublicError> {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Some(PublicError::RateLimited);
    }
    if status.is_server_error() {
        return Some(PublicError::ServerFailure(status.as_u16()));
    }
    None
}

pub fn parse_instrument(
    payload: &str,
    symbol: Symbol,
    generation: u64,
) -> Result<Instrument, BinanceError> {
    parse_contract_rules(payload, symbol, generation).map(|rules| rules.instrument)
}

pub(crate) fn parse_contract_rules(
    payload: &str,
    symbol: Symbol,
    generation: u64,
) -> Result<BinanceContractRules, BinanceError> {
    let root: Value = serde_json::from_str(payload).map_err(|_| BinanceError::Payload)?;
    let expected_native = native_symbol(&symbol);
    let entry = root
        .get("symbols")
        .and_then(Value::as_array)
        .and_then(|entries| {
            entries.iter().find(|entry| {
                entry.get("symbol").and_then(Value::as_str) == Some(expected_native.as_str())
            })
        })
        .and_then(Value::as_object)
        .ok_or(BinanceError::Instrument)?;
    if entry.get("status").and_then(Value::as_str) != Some("TRADING")
        || entry.get("contractType").and_then(Value::as_str) != Some("PERPETUAL")
        || entry.get("baseAsset").and_then(Value::as_str) != Some(symbol.base())
        || entry.get("quoteAsset").and_then(Value::as_str) != Some(symbol.quote())
    {
        return Err(BinanceError::Instrument);
    }
    let settlement_asset: Asset = entry
        .get("marginAsset")
        .and_then(Value::as_str)
        .ok_or(BinanceError::Instrument)?
        .parse()
        .map_err(|_| BinanceError::Instrument)?;
    let tick = filter_decimal(entry, "PRICE_FILTER", "tickSize")?;
    let step = filter_decimal(entry, "LOT_SIZE", "stepSize")?;
    let minimum = filter_decimal(entry, "MIN_NOTIONAL", "notional")?;
    let minimum_quantity = filter_decimal(entry, "LOT_SIZE", "minQty")?;
    let instrument = Instrument {
        symbol,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(settlement_asset.clone()),
        generation,
        price_tick: Price::new(tick).map_err(|_| BinanceError::Instrument)?,
        quantity_step: step,
        minimum_notional: Amount::new(settlement_asset, minimum),
    };
    instrument
        .validate()
        .map_err(|_| BinanceError::Instrument)?;
    if minimum_quantity <= Decimal::ZERO
        || minimum_quantity < instrument.quantity_step
        || minimum_quantity % instrument.quantity_step != Decimal::ZERO
    {
        return Err(BinanceError::Instrument);
    }
    Ok(BinanceContractRules {
        instrument,
        minimum_quantity,
    })
}

/// Parses the bounded REST depth snapshot inside the exchange adapter and exposes only normalized
/// top prices. Recovery uses this to price one IOC reduction without leaking raw protocol fields.
pub fn parse_depth_best_prices(payload: &str) -> Result<(Price, Price), BinanceError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| BinanceError::Payload)?;
    let bids = levels(value.get("bids"))?;
    let asks = levels(value.get("asks"))?;
    let best_bid = bids.first().ok_or(BinanceError::Payload)?.price;
    let best_ask = asks.first().ok_or(BinanceError::Payload)?.price;
    if best_bid.value() >= best_ask.value() {
        return Err(BinanceError::Payload);
    }
    Ok((best_bid, best_ask))
}

pub fn normalize(
    record: &RawMarketRecord,
    expected_native: &str,
) -> Result<MarketEvent, BinanceError> {
    if record.parser_schema_version != PARSER_SCHEMA_VERSION {
        return Err(BinanceError::Schema);
    }
    let value: Value = serde_json::from_str(&record.payload).map_err(|_| BinanceError::Payload)?;
    match record.source {
        RawSource::RestSnapshot => {
            parse_snapshot(record, &value, expected_native).map(MarketEvent::Snapshot)
        }
        RawSource::RestKline => parse_rest_bar(record, &value).map(MarketEvent::Bar),
        RawSource::WebSocketDelta => {
            parse_delta(record, &value, expected_native).map(MarketEvent::Delta)
        }
        RawSource::WebSocketTrade => {
            parse_trade(record, &value, expected_native).map(MarketEvent::Trade)
        }
        RawSource::WebSocketKline => {
            parse_bar(record, &value, expected_native).map(MarketEvent::Bar)
        }
        RawSource::WebSocketTicker => {
            parse_ticker(record, &value, expected_native).map(MarketEvent::Ticker)
        }
        RawSource::WebSocketMarkFunding => {
            parse_mark_funding(record, &value, expected_native).map(MarketEvent::MarkFunding)
        }
    }
}

fn parse_snapshot(
    record: &RawMarketRecord,
    value: &Value,
    expected_native: &str,
) -> Result<MarketSnapshot, BinanceError> {
    let object = value.as_object().ok_or(BinanceError::Payload)?;
    check_symbol(
        object.get("symbol").or_else(|| object.get("s")),
        expected_native,
    )?;
    Ok(MarketSnapshot {
        symbol: record.symbol.clone(),
        generation: record.generation,
        sequence: number(object.get("lastUpdateId"))?,
        exchange_time_ms: optional_number(object.get("E"))?,
        bids: levels(object.get("bids").or_else(|| object.get("b")))?,
        asks: levels(object.get("asks").or_else(|| object.get("a")))?,
    })
}

fn parse_delta(
    record: &RawMarketRecord,
    value: &Value,
    expected_native: &str,
) -> Result<MarketDelta, BinanceError> {
    let object = value.as_object().ok_or(BinanceError::Payload)?;
    check_symbol(object.get("s"), expected_native)?;
    if object.get("e").and_then(Value::as_str) != Some("depthUpdate")
        || object.get("st").and_then(Value::as_u64) != Some(1)
        || object.get("T").and_then(Value::as_u64).is_none()
    {
        return Err(BinanceError::Payload);
    }
    let first_sequence = number(object.get("U"))?;
    let sequence = number(object.get("u"))?;
    if sequence < first_sequence {
        return Err(BinanceError::Sequence);
    }
    Ok(MarketDelta {
        symbol: record.symbol.clone(),
        generation: record.generation,
        first_sequence,
        previous_sequence: optional_number(object.get("pu"))?,
        sequence,
        exchange_time_ms: optional_number(object.get("E"))?,
        bids: levels(object.get("b"))?,
        asks: levels(object.get("a"))?,
    })
}

fn parse_trade(
    record: &RawMarketRecord,
    value: &Value,
    expected_native: &str,
) -> Result<PublicTrade, BinanceError> {
    let object = stream_object(value, expected_native, "aggTrade")?;
    let aggressor = match object.get("m") {
        Some(Value::Bool(true)) => FieldState::Known(AggressorSide::Sell),
        Some(Value::Bool(false)) => FieldState::Known(AggressorSide::Buy),
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(_) => FieldState::Unavailable {
            reason: UnknownReason::ParseFailure,
        },
    };
    Ok(PublicTrade {
        symbol: record.symbol.clone(),
        generation: record.generation,
        received_at_ms: record.received_at_ms,
        exchange_time_ms: number(object.get("E"))?,
        transaction_time_ms: number(object.get("T"))?,
        aggregate_trade_id: number(object.get("a"))?,
        first_trade_id: number(object.get("f"))?,
        last_trade_id: number(object.get("l"))?,
        price: required_price(object.get("p"))?,
        quantity: required_decimal(object.get("q"))?,
        quote_quantity: required_decimal(object.get("nq"))?,
        aggressor,
    })
}

fn parse_bar(
    record: &RawMarketRecord,
    value: &Value,
    expected_native: &str,
) -> Result<PublicBar, BinanceError> {
    const INTERVAL_MS: u64 = 60_000;
    let object = kline_stream_object(value, expected_native)?;
    let kline = object
        .get("k")
        .and_then(Value::as_object)
        .ok_or(BinanceError::Payload)?;
    check_symbol(kline.get("s"), expected_native)?;
    if kline.get("i").and_then(Value::as_str) != Some("1m")
        || kline.get("x").and_then(Value::as_bool) != Some(true)
    {
        return Err(BinanceError::Payload);
    }
    let open_time_ms = number(kline.get("t"))?;
    let close_time_ms = number(kline.get("T"))?;
    let open = required_price(kline.get("o"))?;
    let high = required_price(kline.get("h"))?;
    let low = required_price(kline.get("l"))?;
    let close = required_price(kline.get("c"))?;
    if close_time_ms <= open_time_ms
        || high < open.max(close)
        || low > open.min(close)
        || high < low
    {
        return Err(BinanceError::Payload);
    }
    let sequence = open_time_ms
        .checked_div(INTERVAL_MS)
        .and_then(|value| value.checked_add(1))
        .ok_or(BinanceError::Sequence)?;
    Ok(PublicBar {
        symbol: record.symbol.clone(),
        generation: record.generation,
        received_at_ms: record.received_at_ms,
        sequence,
        open_time_ms,
        close_time_ms,
        interval_ms: INTERVAL_MS,
        open,
        high,
        low,
        close,
    })
}

fn parse_rest_bar(record: &RawMarketRecord, value: &Value) -> Result<PublicBar, BinanceError> {
    const INTERVAL_MS: u64 = 60_000;
    let row = value.as_array().ok_or(BinanceError::Payload)?;
    if row.len() < 7 {
        return Err(BinanceError::Payload);
    }
    let open_time_ms = number(row.first())?;
    let close_time_ms = number(row.get(6))?;
    let open = required_price(row.get(1))?;
    let high = required_price(row.get(2))?;
    let low = required_price(row.get(3))?;
    let close = required_price(row.get(4))?;
    if close_time_ms <= open_time_ms
        || high < open.max(close)
        || low > open.min(close)
        || high < low
    {
        return Err(BinanceError::Payload);
    }
    let sequence = open_time_ms
        .checked_div(INTERVAL_MS)
        .and_then(|value| value.checked_add(1))
        .ok_or(BinanceError::Sequence)?;
    Ok(PublicBar {
        symbol: record.symbol.clone(),
        generation: record.generation,
        received_at_ms: record.received_at_ms,
        sequence,
        open_time_ms,
        close_time_ms,
        interval_ms: INTERVAL_MS,
        open,
        high,
        low,
        close,
    })
}

pub fn split_closed_kline_bootstrap(
    payload: &str,
    received_at_ms: u64,
) -> Result<Vec<String>, BinanceError> {
    let rows = serde_json::from_str::<Value>(payload)
        .map_err(|_| BinanceError::Payload)?
        .as_array()
        .cloned()
        .ok_or(BinanceError::Payload)?;
    if rows.is_empty() || rows.len() > 22 || received_at_ms == 0 {
        return Err(BinanceError::Payload);
    }
    let mut closed = rows
        .into_iter()
        .filter(|row| {
            row.as_array()
                .and_then(|values| values.get(6))
                .and_then(Value::as_u64)
                .is_some_and(|close_time| close_time < received_at_ms)
        })
        .map(|row| serde_json::to_string(&row).map_err(|_| BinanceError::Payload))
        .collect::<Result<Vec<_>, _>>()?;
    // ATR14 needs fourteen true ranges, while the same frame's 20-period bandwidth
    // needs twenty-one closes. Bootstrap the larger shared readiness window.
    if closed.len() < 21 {
        return Err(BinanceError::Payload);
    }
    if closed.len() > 21 {
        let surplus = closed.len() - 21;
        closed.drain(..surplus);
    }
    Ok(closed)
}

/// Returns whether a valid Binance one-minute kline update is closed. Open updates are routine
/// transport noise for a domain that deliberately admits completed bars only.
pub fn kline_payload_is_closed(payload: &str, expected_native: &str) -> Result<bool, BinanceError> {
    let value: Value = serde_json::from_str(payload).map_err(|_| BinanceError::Payload)?;
    let object = kline_stream_object(&value, expected_native)?;
    let kline = object
        .get("k")
        .and_then(Value::as_object)
        .ok_or(BinanceError::Payload)?;
    check_symbol(kline.get("s"), expected_native)?;
    if kline.get("i").and_then(Value::as_str) != Some("1m") {
        return Err(BinanceError::Payload);
    }
    kline
        .get("x")
        .and_then(Value::as_bool)
        .ok_or(BinanceError::Payload)
}

fn parse_ticker(
    record: &RawMarketRecord,
    value: &Value,
    expected_native: &str,
) -> Result<PublicTicker, BinanceError> {
    let object = stream_object(value, expected_native, "bookTicker")?;
    Ok(PublicTicker {
        symbol: record.symbol.clone(),
        generation: record.generation,
        received_at_ms: record.received_at_ms,
        exchange_time_ms: number(object.get("E"))?,
        transaction_time_ms: number(object.get("T"))?,
        update_id: number(object.get("u"))?,
        bid_price: required_price(object.get("b"))?,
        bid_quantity: required_decimal(object.get("B"))?,
        ask_price: required_price(object.get("a"))?,
        ask_quantity: required_decimal(object.get("A"))?,
    })
}

fn parse_mark_funding(
    record: &RawMarketRecord,
    value: &Value,
    expected_native: &str,
) -> Result<MarkFunding, BinanceError> {
    let object = stream_object(value, expected_native, "markPriceUpdate")?;
    Ok(MarkFunding {
        symbol: record.symbol.clone(),
        generation: record.generation,
        received_at_ms: record.received_at_ms,
        exchange_time_ms: number(object.get("E"))?,
        next_funding_time_ms: number(object.get("T"))?,
        mark_price: required_price(object.get("p"))?,
        index_price: required_price(object.get("i"))?,
        funding_rate: required_decimal(object.get("r"))?,
        estimated_settle_price: optional_price(object.get("P")),
        predicted_funding_rate: optional_decimal(object.get("ap")),
        unknown_reason: None,
    })
}

fn stream_object<'a>(
    value: &'a Value,
    expected_native: &str,
    expected_event: &str,
) -> Result<&'a serde_json::Map<String, Value>, BinanceError> {
    let object = value.as_object().ok_or(BinanceError::Payload)?;
    check_symbol(object.get("s"), expected_native)?;
    if object.get("e").and_then(Value::as_str) != Some(expected_event)
        || object.get("st").and_then(Value::as_u64) != Some(1)
    {
        return Err(BinanceError::Payload);
    }
    Ok(object)
}

fn kline_stream_object<'a>(
    value: &'a Value,
    expected_native: &str,
) -> Result<&'a serde_json::Map<String, Value>, BinanceError> {
    let object = value.as_object().ok_or(BinanceError::Payload)?;
    check_symbol(object.get("s"), expected_native)?;
    let valid_symbol_type = match object.get("st") {
        None | Some(Value::Null) => true,
        Some(Value::Number(value)) => value.as_u64() == Some(1),
        Some(_) => false,
    };
    if object.get("e").and_then(Value::as_str) != Some("kline") || !valid_symbol_type {
        return Err(BinanceError::Payload);
    }
    Ok(object)
}

fn check_symbol(value: Option<&Value>, expected: &str) -> Result<(), BinanceError> {
    match value {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(actual)) if actual == expected => Ok(()),
        _ => Err(BinanceError::Symbol),
    }
}
fn number(value: Option<&Value>) -> Result<u64, BinanceError> {
    value
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or(BinanceError::Payload)
}
fn optional_number(value: Option<&Value>) -> Result<Option<u64>, BinanceError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or(BinanceError::Payload),
    }
}
fn filter_decimal(
    entry: &serde_json::Map<String, Value>,
    filter_type: &str,
    field: &str,
) -> Result<Decimal, BinanceError> {
    let raw = entry
        .get("filters")
        .and_then(Value::as_array)
        .and_then(|filters| {
            filters.iter().find(|filter| {
                filter.get("filterType").and_then(Value::as_str) == Some(filter_type)
            })
        })
        .and_then(|filter| filter.get(field))
        .and_then(Value::as_str)
        .ok_or(BinanceError::Instrument)?;
    Decimal::from_str(raw).map_err(|_| BinanceError::Instrument)
}
fn levels(value: Option<&Value>) -> Result<Vec<MarketLevel>, BinanceError> {
    value
        .and_then(Value::as_array)
        .ok_or(BinanceError::Payload)?
        .iter()
        .map(level)
        .collect()
}
fn level(value: &Value) -> Result<MarketLevel, BinanceError> {
    let fields = value.as_array().ok_or(BinanceError::Payload)?;
    let price = Decimal::from_str(
        fields
            .first()
            .and_then(Value::as_str)
            .ok_or(BinanceError::Payload)?,
    )
    .map_err(|_| BinanceError::Payload)?;
    let quantity = Decimal::from_str(
        fields
            .get(1)
            .and_then(Value::as_str)
            .ok_or(BinanceError::Payload)?,
    )
    .map_err(|_| BinanceError::Payload)?;
    if quantity.is_sign_negative() {
        return Err(BinanceError::Payload);
    }
    Ok(MarketLevel {
        price: Price::new(price).map_err(|_| BinanceError::Payload)?,
        quantity,
    })
}
fn required_decimal(value: Option<&Value>) -> Result<Decimal, BinanceError> {
    Decimal::from_str(value.and_then(Value::as_str).ok_or(BinanceError::Payload)?)
        .map_err(|_| BinanceError::Payload)
}
fn required_price(value: Option<&Value>) -> Result<Price, BinanceError> {
    Price::new(required_decimal(value)?).map_err(|_| BinanceError::Payload)
}
fn optional_decimal(value: Option<&Value>) -> FieldState<Decimal> {
    match value {
        None => FieldState::Missing,
        Some(Value::Null) => FieldState::Null,
        Some(value) => value
            .as_str()
            .and_then(|raw| Decimal::from_str(raw).ok())
            .map(FieldState::Known)
            .unwrap_or(FieldState::Unavailable {
                reason: UnknownReason::ParseFailure,
            }),
    }
}
fn optional_price(value: Option<&Value>) -> FieldState<Price> {
    match optional_decimal(value) {
        FieldState::Known(value) => {
            Price::new(value)
                .map(FieldState::Known)
                .unwrap_or(FieldState::Unavailable {
                    reason: UnknownReason::ParseFailure,
                })
        }
        FieldState::Missing => FieldState::Missing,
        FieldState::Null => FieldState::Null,
        FieldState::Unavailable { reason } => FieldState::Unavailable { reason },
        FieldState::NotApplicable => FieldState::NotApplicable,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceError {
    #[error("Binance payload has an invalid shape")]
    Payload,
    #[error("Binance payload symbol does not match adapter mapping")]
    Symbol,
    #[error("Binance delta sequence is invalid")]
    Sequence,
    #[error("raw record uses an unsupported parser schema")]
    Schema,
    #[error("Binance instrument rule is absent or incompatible")]
    Instrument,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BinanceRulesError {
    #[error(transparent)]
    Public(#[from] PublicError),
    #[error(transparent)]
    Parse(#[from] BinanceError),
}

#[derive(Debug, thiserror::Error)]
pub enum PublicError {
    #[error("Binance public HTTP request failed: {0}")]
    Http(#[source] Box<reqwest::Error>),
    #[error("Binance public HTTP transport retry budget was exhausted")]
    TransportRetriesExhausted,
    #[error("Binance public proxy configuration is invalid")]
    Proxy,
    #[error("Binance public WebSocket failed: {0}")]
    WebSocket(#[source] Box<tungstenite::Error>),
    #[error("Binance depth limit is unsupported")]
    DepthLimit,
    #[error("Binance public HTTP request was rate limited")]
    RateLimited,
    #[error("Binance public HTTP server failed with status {0}")]
    ServerFailure(u16),
    #[error("Binance public HTTP returned status {0}")]
    HttpStatus(u16),
    #[error("Binance public WebSocket closed")]
    Closed,
}

#[derive(Debug, thiserror::Error)]
pub enum PrivateError {
    #[error("Binance private credentials are absent or invalid")]
    Credentials,
    #[error("Binance private listen key response is invalid")]
    ListenKey,
    #[error("Binance private WebSocket closed")]
    StreamClosed,
    #[error("Binance server time cannot produce a signed request timestamp")]
    Clock,
    #[error("order command is invalid: {0}")]
    Command(crate::domain::CommandError),
    #[error("order command owner does not target Binance")]
    Owner,
    #[error("Binance client order identity is invalid")]
    ClientOrderId,
    #[error("Binance exact order readback did not prove GTX post-only semantics")]
    PostOnlyVerification,
    #[error("Binance rejected the private request with status {status}, API code {api_code:?}")]
    Rejected { status: u16, api_code: Option<i64> },
    #[error("Binance rate limited the private request with status {0}")]
    RateLimited(u16),
    #[error("Binance private request outcome is unknown after status {0}")]
    Unknown(u16),
    #[error("Binance private HTTP transport failed")]
    Http,
    #[error("Binance private WebSocket TLS handshake failed")]
    WebSocketTls,
    #[error("Binance private WebSocket HTTP handshake failed with status {0}")]
    WebSocketHandshake(u16),
    #[error("Binance private WebSocket I/O failed")]
    WebSocketIo,
    #[error("Binance private WebSocket transport failed")]
    WebSocket,
    #[error("Binance PAPI user-trades page is invalid or regressed")]
    FillPage,
    #[error("Binance PAPI user-trades pagination exceeded its bounded page budget")]
    FillPageLimit,
}

#[derive(Debug, thiserror::Error)]
pub enum PrivateReadbackError {
    #[error("Binance PAPI account readback request failed: {0}")]
    AccountRequest(PrivateError),
    #[error("Binance PAPI UM account readback request failed: {0}")]
    UmAccountRequest(PrivateError),
    #[error("Binance PAPI UM position-mode readback request failed: {0}")]
    PositionModeRequest(PrivateError),
    #[error("Binance PAPI UM account-config readback request failed: {0}")]
    AccountConfigRequest(PrivateError),
    #[error("Binance PAPI UM open-order readback request failed: {0}")]
    OpenOrdersRequest(PrivateError),
    #[error("Binance PAPI UM algo open-order readback request failed: {0}")]
    AlgoOrdersRequest(PrivateError),
    #[error("Binance PAPI UM fill readback request failed: {0}")]
    FillsRequest(PrivateError),
    #[error("Binance signed readback parse failed: {0}")]
    Parse(PrivateParseError),
}

fn http_error(source: reqwest::Error) -> PublicError {
    PublicError::Http(Box::new(source))
}

fn websocket_error(source: tungstenite::Error) -> PublicError {
    PublicError::WebSocket(Box::new(source))
}

pub(super) fn private_http_error(_: reqwest::Error) -> PrivateError {
    // Request URLs can contain a sensitive listen key or signed query. Keep them out of any
    // user-facing error while preserving the fail-closed transport classification.
    PrivateError::Http
}

fn private_websocket_error(error: tungstenite::Error) -> PrivateError {
    // The PAPI private-stream URL contains the listen key, so tungstenite's URL-rich errors
    // must never cross the adapter boundary.
    match error {
        tungstenite::Error::Tls(_) => PrivateError::WebSocketTls,
        tungstenite::Error::Http(response) => {
            PrivateError::WebSocketHandshake(response.status().as_u16())
        }
        tungstenite::Error::Io(_) => PrivateError::WebSocketIo,
        _ => PrivateError::WebSocket,
    }
}

fn user_trades_parameters(
    symbol: &Symbol,
    from_id: Option<u64>,
    start_time_ms: u64,
    end_time_ms: u64,
) -> Vec<(&'static str, String)> {
    let mut parameters = vec![("symbol", native_symbol(symbol))];
    if let Some(from_id) = from_id {
        // PAPI UM userTrades rejects fromId combined with either time bound. The caller's
        // durable cursor still bounds progression through its strictly advancing native ID.
        parameters.push(("fromId", from_id.to_string()));
    } else {
        if start_time_ms > 0 {
            parameters.push(("startTime", start_time_ms.to_string()));
        }
        if end_time_ms > 0 {
            parameters.push(("endTime", end_time_ms.to_string()));
        }
    }
    parameters.push(("limit", USER_TRADES_PAGE_LIMIT.to_string()));
    parameters
}

#[path = "binance_fill_pagination.rs"]
mod binance_fill_pagination;
pub use binance_fill_pagination::paginate_recent_fills;

#[cfg(test)]
#[path = "binance_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "binance_private_readback_tests.rs"]
mod binance_private_readback_tests;
#[cfg(test)]
#[path = "binance_recovery_tests.rs"]
mod binance_recovery_tests;
