use std::{
    sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Bytes, BytesMut};
use secrecy::ExposeSecret;
use tokio::time::timeout;
use venue_domain::domain::Asset;

use crate::execution::{
    BinanceExactOrderReadback, BinanceMutationAck, BinancePreparedMutation,
    parse_exact_order_readback, parse_mutation_ack,
};
use crate::readback::{BinancePrivateReadRequest, BinancePrivateReadScope, BinanceRawPrivatePage};
use crate::{
    BinanceConfig, BinanceCredentials, BinanceHttpMethod, BinanceRestSignInput,
    SignedBinanceRestRequest, endpoints, sign_rest,
};

const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_TRANSPORT_BYTES: usize = 2 * 1024 * 1024;
const TIME_SYNC_ATTEMPTS: usize = 6;
static NEXT_TRANSPORT_INSTANCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BinanceTransportLimits {
    operation_timeout: Duration,
    maximum_body_bytes: usize,
}

impl BinanceTransportLimits {
    pub fn new(
        operation_timeout: Duration,
        maximum_body_bytes: usize,
    ) -> Result<Self, BinanceTransportError> {
        if operation_timeout.is_zero()
            || operation_timeout > MAX_OPERATION_TIMEOUT
            || maximum_body_bytes == 0
            || maximum_body_bytes > MAX_TRANSPORT_BYTES
        {
            return Err(BinanceTransportError::Limits);
        }
        Ok(Self {
            operation_timeout,
            maximum_body_bytes,
        })
    }

    #[must_use]
    pub const fn operation_timeout(self) -> Duration {
        self.operation_timeout
    }

    #[must_use]
    pub const fn maximum_body_bytes(self) -> usize {
        self.maximum_body_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceHttpResponse {
    pub requested_at_ms: u64,
    pub received_at_ms: u64,
    pub status: u16,
    pub payload: Bytes,
}

pub struct BinanceHttpTransport {
    client: reqwest::Client,
    config: BinanceConfig,
    instrument_generation: u64,
    private_generation: u64,
    endpoint: String,
    limits: BinanceTransportLimits,
    instance_serial: u64,
    fixed_endpoint: bool,
    clock_offset_ms: AtomicI64,
    clock_synchronized: AtomicBool,
}

impl BinanceHttpTransport {
    pub fn new(
        config: BinanceConfig,
        instrument_generation: u64,
        private_generation: u64,
        limits: BinanceTransportLimits,
    ) -> Result<Self, BinanceTransportError> {
        let endpoint = config.portfolio_rest_origin().to_owned();
        Self::build(
            config,
            instrument_generation,
            private_generation,
            endpoint,
            limits,
            true,
        )
    }

    #[cfg(test)]
    pub(crate) fn with_endpoint(
        config: BinanceConfig,
        instrument_generation: u64,
        private_generation: u64,
        endpoint: String,
        limits: BinanceTransportLimits,
    ) -> Result<Self, BinanceTransportError> {
        Self::build(
            config,
            instrument_generation,
            private_generation,
            endpoint,
            limits,
            false,
        )
    }

    fn build(
        config: BinanceConfig,
        instrument_generation: u64,
        private_generation: u64,
        endpoint: String,
        limits: BinanceTransportLimits,
        require_fixed_endpoint: bool,
    ) -> Result<Self, BinanceTransportError> {
        if instrument_generation == 0
            || private_generation == 0
            || endpoint.is_empty()
            || require_fixed_endpoint && endpoint != config.portfolio_rest_origin()
        {
            return Err(BinanceTransportError::Binding);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(limits.operation_timeout)
            .timeout(limits.operation_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| BinanceTransportError::Http)?;
        let instance_serial = NEXT_TRANSPORT_INSTANCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| BinanceTransportError::Protocol)?;
        Ok(Self {
            client,
            config,
            instrument_generation,
            private_generation,
            endpoint,
            limits,
            instance_serial,
            fixed_endpoint: require_fixed_endpoint,
            clock_offset_ms: AtomicI64::new(0),
            clock_synchronized: AtomicBool::new(cfg!(test)),
        })
    }

    /// Installs the lowest-RTT midpoint sample from Binance before any signed request. A newly
    /// constructed production transport is deliberately unusable for signing until this succeeds.
    pub async fn synchronize_clock(&self) -> Result<(), BinanceTransportError> {
        self.clock_synchronized.store(false, Ordering::Release);
        let mut best = None;
        for _ in 0..TIME_SYNC_ATTEMPTS {
            let before = unix_ms()?;
            let response = match self
                .send_bounded(
                    self.client
                        .get(format!("{}{}", self.endpoint, endpoints::SERVER_TIME)),
                    before,
                    false,
                )
                .await
            {
                Ok(response) => response,
                Err(_) => continue,
            };
            let server_time = serde_json::from_slice::<serde_json::Value>(&response.payload)
                .ok()
                .and_then(|value| value.get("serverTime").and_then(serde_json::Value::as_u64));
            let Some(server_time) = server_time else {
                continue;
            };
            let round_trip_ms = response.received_at_ms.saturating_sub(before);
            let midpoint = before.saturating_add(round_trip_ms / 2);
            let offset = time_offset_ms(server_time, midpoint);
            if best.is_none_or(|(best_rtt, _)| round_trip_ms < best_rtt) {
                best = Some((round_trip_ms, offset));
            }
        }
        let (_, offset) = best.ok_or(BinanceTransportError::Clock)?;
        self.clock_offset_ms.store(offset, Ordering::Relaxed);
        self.clock_synchronized.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn inherit_synchronized_clock(
        &mut self,
        previous: &Self,
    ) -> Result<(), BinanceTransportError> {
        if !previous.clock_synchronized.load(Ordering::Acquire) {
            return Err(BinanceTransportError::Clock);
        }
        self.clock_offset_ms.store(
            previous.clock_offset_ms.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.clock_synchronized.store(true, Ordering::Release);
        Ok(())
    }

    pub(crate) fn signing_timestamp_ms(&self) -> Result<u64, BinanceTransportError> {
        if !self.clock_synchronized.load(Ordering::Acquire) {
            return Err(BinanceTransportError::Clock);
        }
        authoritative_timestamp(unix_ms()?, self.clock_offset_ms.load(Ordering::Relaxed))
    }

    #[must_use]
    pub const fn config(&self) -> &BinanceConfig {
        &self.config
    }

    pub(crate) const fn recovery_instrument_generation(&self) -> u64 {
        self.instrument_generation
    }

    pub(crate) const fn recovery_private_generation(&self) -> u64 {
        self.private_generation
    }

    pub(crate) const fn recovery_instance_serial(&self) -> u64 {
        self.instance_serial
    }

    pub(crate) const fn recovery_uses_fixed_endpoint(&self) -> bool {
        self.fixed_endpoint
    }

    pub(crate) const fn recovery_limits(&self) -> BinanceTransportLimits {
        self.limits
    }

    pub async fn execute_read(
        &self,
        credentials: &BinanceCredentials,
        request: &BinancePrivateReadRequest,
        timestamp_ms: u64,
    ) -> Result<BinanceRawPrivatePage, BinanceTransportError> {
        self.validate_scope(request.scope())?;
        let response = match self
            .execute_signed(
                credentials,
                request.scope(),
                request.method(),
                request.path(),
                request.parameters(),
                timestamp_ms,
                false,
            )
            .await
        {
            Ok(response) => response,
            Err(BinanceTransportError::TimestampRejected) => {
                self.synchronize_clock().await?;
                self.execute_signed(
                    credentials,
                    request.scope(),
                    request.method(),
                    request.path(),
                    request.parameters(),
                    self.signing_timestamp_ms()?,
                    false,
                )
                .await?
            }
            Err(error) => return Err(error),
        };
        BinanceRawPrivatePage::new(
            request,
            response.requested_at_ms,
            response.received_at_ms,
            response.payload,
        )
        .map_err(|_| BinanceTransportError::Payload)
    }

    /// Reads the production USD-M exchange catalogue through the fixed public origin. This is
    /// intentionally separate from the signed PAPI transport: callers use it only to prove
    /// fresh contract rules before normalizing account-wide risk.
    pub async fn fetch_usd_m_exchange_info(
        &self,
    ) -> Result<BinanceHttpResponse, BinanceTransportError> {
        let requested_at_ms = unix_ms()?;
        let url = format!(
            "{}/fapi/v1/exchangeInfo",
            self.config.usd_m_public_rest_origin()
        );
        self.send_bounded(self.client.get(url), requested_at_ms, false)
            .await
    }

    /// Reads exactly one USD-M asset-index pair from Binance's fixed public origin. The endpoint
    /// is public and deliberately carries no account credentials; its bounded response is only
    /// conversion evidence and never an account fact.
    pub async fn fetch_usd_m_asset_index(
        &self,
        asset: &Asset,
    ) -> Result<BinanceHttpResponse, BinanceTransportError> {
        let requested_at_ms = unix_ms()?;
        let url = self.usd_m_asset_index_url(asset)?;
        self.send_bounded(self.client.get(url), requested_at_ms, false)
            .await
    }

    pub(crate) fn usd_m_asset_index_url(
        &self,
        asset: &Asset,
    ) -> Result<String, BinanceTransportError> {
        let symbol = format!("{}USD", asset.as_str());
        if symbol.len() > 32 || !symbol.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(BinanceTransportError::Binding);
        }
        Ok(format!(
            "{}{}?symbol={symbol}",
            self.config.usd_m_public_rest_origin(),
            endpoints::USD_M_ASSET_INDEX
        ))
    }

    /// Bounded production BBO read used only to normalize a semantic limit intent. It shares the
    /// adapter client, body limit and LIVE origin; callers still validate symbol/time/price.
    pub async fn fetch_usd_m_book_ticker(
        &self,
        native_symbol: &str,
    ) -> Result<BinanceHttpResponse, BinanceTransportError> {
        if native_symbol.is_empty()
            || !native_symbol
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(BinanceTransportError::Binding);
        }
        let requested_at_ms = unix_ms()?;
        let url = format!(
            "{}/fapi/v1/ticker/bookTicker?symbol={native_symbol}",
            self.config.usd_m_public_rest_origin()
        );
        self.send_bounded(self.client.get(url), requested_at_ms, false)
            .await
    }

    /// Bounded credential-free depth snapshot for the public diff-depth bridge.  This is a read
    /// only market fact; the caller must still require the first queued delta to bridge it.
    pub async fn fetch_usd_m_depth_snapshot(
        &self,
        native_symbol: &str,
    ) -> Result<BinanceHttpResponse, BinanceTransportError> {
        if native_symbol.is_empty()
            || !native_symbol
                .bytes()
                .all(|value| value.is_ascii_uppercase() || value.is_ascii_digit())
        {
            return Err(BinanceTransportError::Binding);
        }
        let requested_at_ms = unix_ms()?;
        let url = format!(
            "{}/fapi/v1/depth?symbol={native_symbol}&limit=1000",
            self.config.usd_m_public_rest_origin()
        );
        self.send_bounded(self.client.get(url), requested_at_ms, false)
            .await
    }

    /// Performs exactly one physical mutation dispatch. Timeout, disconnect, and ambiguous server
    /// status are UNKNOWN and are never retried by this transport.
    pub async fn dispatch_once(
        &self,
        credentials: &BinanceCredentials,
        scope: &BinancePrivateReadScope,
        request: &BinancePreparedMutation,
        timestamp_ms: u64,
    ) -> Result<BinanceMutationAck, BinanceTransportError> {
        self.validate_scope(scope)?;
        request
            .validate(scope)
            .map_err(|_| BinanceTransportError::Binding)?;
        let response = match self
            .execute_signed(
                credentials,
                scope,
                request.method(),
                request.path(),
                request.parameters(),
                timestamp_ms,
                true,
            )
            .await
        {
            Ok(response) => response,
            Err(BinanceTransportError::TimestampRejected) => {
                self.synchronize_clock().await?;
                return Err(BinanceTransportError::TimestampRejected);
            }
            Err(error) => return Err(error),
        };
        parse_mutation_ack(request, scope, &response.payload, response.received_at_ms)
            .map_err(|_| BinanceTransportError::Ack)
    }

    /// Dispatches once and then performs one separately signed exact lookup. Failure of either
    /// network operation never causes a second mutation dispatch.
    pub async fn dispatch_then_exact_readback(
        &self,
        credentials: &BinanceCredentials,
        scope: &BinancePrivateReadScope,
        request: &BinancePreparedMutation,
        dispatch_timestamp_ms: u64,
    ) -> BinancePhysicalMutationOutcome {
        let ack = match self
            .dispatch_once(credentials, scope, request, dispatch_timestamp_ms)
            .await
        {
            Ok(ack) => ack,
            Err(error) if error.is_unknown_dispatch() => {
                return BinancePhysicalMutationOutcome::DispatchUnknown { error };
            }
            Err(error) => return BinancePhysicalMutationOutcome::DispatchFailed { error },
        };
        let exact_request = match request.exact_readback_request(scope) {
            Ok(request) => request,
            Err(_) => {
                return BinancePhysicalMutationOutcome::AckedReadbackUnknown {
                    ack,
                    error: BinanceTransportError::Binding,
                };
            }
        };
        let page = match self
            .execute_read(
                credentials,
                &exact_request,
                match self.signing_timestamp_ms() {
                    Ok(value) => value,
                    Err(error) => {
                        return BinancePhysicalMutationOutcome::AckedReadbackUnknown { ack, error };
                    }
                },
            )
            .await
        {
            Ok(page) => page,
            Err(error) => {
                return BinancePhysicalMutationOutcome::AckedReadbackUnknown { ack, error };
            }
        };
        match parse_exact_order_readback(&ack, &exact_request, &page) {
            Ok(readback) => BinancePhysicalMutationOutcome::ReadBack {
                ack,
                readback: Box::new(readback),
            },
            Err(_) => BinancePhysicalMutationOutcome::AckedReadbackUnknown {
                ack,
                error: BinanceTransportError::Ack,
            },
        }
    }

    pub async fn create_listen_key(
        &self,
        credentials: &BinanceCredentials,
    ) -> Result<crate::BinanceListenKey, BinanceTransportError> {
        let response = self
            .execute_api_key_request(credentials, BinanceHttpMethod::Post)
            .await?;
        crate::BinanceListenKey::from_response(
            self.config.gateway_binding(),
            self.instrument_generation,
            self.private_generation,
            &response.payload,
        )
    }

    pub async fn keepalive_listen_key(
        &self,
        credentials: &BinanceCredentials,
    ) -> Result<(), BinanceTransportError> {
        self.execute_api_key_request(credentials, BinanceHttpMethod::Put)
            .await
            .map(|_| ())
    }

    async fn execute_api_key_request(
        &self,
        credentials: &BinanceCredentials,
        method: BinanceHttpMethod,
    ) -> Result<BinanceHttpResponse, BinanceTransportError> {
        let requested_at_ms = unix_ms()?;
        let url = format!("{}{}", self.endpoint, endpoints::LISTEN_KEY);
        let builder = match method {
            BinanceHttpMethod::Post => self.client.post(url),
            BinanceHttpMethod::Put => self.client.put(url),
            BinanceHttpMethod::Get | BinanceHttpMethod::Delete => {
                return Err(BinanceTransportError::Protocol);
            }
        }
        .header("X-MBX-APIKEY", credentials.api_key.expose_secret());
        self.send_bounded(builder, requested_at_ms, false).await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "binding, method, path, parameters, clock, and mutation classification are one physical request"
    )]
    async fn execute_signed(
        &self,
        credentials: &BinanceCredentials,
        scope: &BinancePrivateReadScope,
        method: BinanceHttpMethod,
        path: &str,
        parameters: &[(String, String)],
        timestamp_ms: u64,
        mutation: bool,
    ) -> Result<BinanceHttpResponse, BinanceTransportError> {
        if timestamp_ms == 0 || !self.clock_synchronized.load(Ordering::Acquire) {
            return Err(BinanceTransportError::Clock);
        }
        let parameter_refs = parameters
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let signed = sign_rest(
            credentials,
            &self.config,
            &BinanceRestSignInput {
                binding: scope.binding(),
                method,
                path,
                parameters: &parameter_refs,
                recv_window_ms: 5_000,
                timestamp_ms,
            },
        )
        .map_err(|_| BinanceTransportError::Signing)?;
        self.send_signed(signed, mutation).await
    }

    async fn send_signed(
        &self,
        signed: SignedBinanceRestRequest,
        mutation: bool,
    ) -> Result<BinanceHttpResponse, BinanceTransportError> {
        if signed.origin() != self.config.portfolio_rest_origin()
            || !signed.authentication_material_is_present()
        {
            return Err(BinanceTransportError::Binding);
        }
        let url = format!("{}{}?{}", self.endpoint, signed.path(), signed.query());
        let builder = match signed.method() {
            BinanceHttpMethod::Get => self.client.get(url),
            BinanceHttpMethod::Post => self.client.post(url),
            BinanceHttpMethod::Delete => self.client.delete(url),
            BinanceHttpMethod::Put => self.client.put(url),
        }
        .header("X-MBX-APIKEY", signed.api_key());
        self.send_bounded(builder, unix_ms()?, mutation).await
    }

    async fn send_bounded(
        &self,
        builder: reqwest::RequestBuilder,
        requested_at_ms: u64,
        mutation: bool,
    ) -> Result<BinanceHttpResponse, BinanceTransportError> {
        let response = timeout(self.limits.operation_timeout, builder.send())
            .await
            .map_err(|_| BinanceTransportError::Timeout)?
            .map_err(map_reqwest)?;
        let status = response.status().as_u16();
        let successful = response.status().is_success();
        if response
            .content_length()
            .is_some_and(|length| length > self.limits.maximum_body_bytes as u64)
        {
            return Err(BinanceTransportError::BodyTooLarge);
        }
        let payload = timeout(self.limits.operation_timeout, async {
            let mut response = response;
            let mut body = BytesMut::new();
            while let Some(chunk) = response.chunk().await.map_err(map_reqwest)? {
                let next = body
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(BinanceTransportError::BodyTooLarge)?;
                if next > self.limits.maximum_body_bytes {
                    return Err(BinanceTransportError::BodyTooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            Ok::<Bytes, BinanceTransportError>(body.freeze())
        })
        .await
        .map_err(|_| BinanceTransportError::Timeout)??;
        if !successful {
            return Err(classify_http_error(status, &payload, mutation));
        }
        Ok(BinanceHttpResponse {
            requested_at_ms,
            received_at_ms: unix_ms()?,
            status,
            payload,
        })
    }

    fn validate_scope(&self, scope: &BinancePrivateReadScope) -> Result<(), BinanceTransportError> {
        if scope.binding() != self.config.gateway_binding()
            || scope.instrument_generation() != self.instrument_generation
            || scope.private_generation() != self.private_generation
            || self.endpoint.is_empty()
        {
            return Err(BinanceTransportError::Binding);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum BinancePhysicalMutationOutcome {
    ReadBack {
        ack: BinanceMutationAck,
        readback: Box<BinanceExactOrderReadback>,
    },
    AckedReadbackUnknown {
        ack: BinanceMutationAck,
        error: BinanceTransportError,
    },
    DispatchUnknown {
        error: BinanceTransportError,
    },
    DispatchFailed {
        error: BinanceTransportError,
    },
}

fn map_reqwest(error: reqwest::Error) -> BinanceTransportError {
    if error.is_timeout() {
        BinanceTransportError::Timeout
    } else {
        // After `send` begins, reqwest cannot prove that an EOF/body/protocol failure happened
        // before a mutation reached Binance. Conservatively classify every non-timeout transport
        // failure as disconnected/UNKNOWN; callers may only issue an exact signed readback.
        BinanceTransportError::Disconnected
    }
}

fn unix_ms() -> Result<u64, BinanceTransportError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceTransportError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| BinanceTransportError::Clock)
}

fn authoritative_timestamp(local_ms: u64, offset_ms: i64) -> Result<u64, BinanceTransportError> {
    if offset_ms >= 0 {
        local_ms
            .checked_add(offset_ms as u64)
            .ok_or(BinanceTransportError::Clock)
    } else {
        local_ms
            .checked_sub(offset_ms.unsigned_abs())
            .ok_or(BinanceTransportError::Clock)
    }
}

fn time_offset_ms(server_time_ms: u64, local_midpoint_ms: u64) -> i64 {
    let difference = i128::from(server_time_ms) - i128::from(local_midpoint_ms);
    difference.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn classify_http_error(status: u16, payload: &[u8], mutation: bool) -> BinanceTransportError {
    if serde_json::from_slice::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| value.get("code").and_then(serde_json::Value::as_i64))
        == Some(-1021)
    {
        BinanceTransportError::TimestampRejected
    } else if mutation && (status >= 500 || status == 408) {
        BinanceTransportError::AmbiguousStatus(status)
    } else {
        BinanceTransportError::HttpStatus(status)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceTransportError {
    #[error("Binance transport limits are invalid or exceed the hard maximum")]
    Limits,
    #[error("Binance transport does not match the fixed binding, endpoint, or generation")]
    Binding,
    #[error("Binance request signing failed")]
    Signing,
    #[error("Binance HTTP transport failed")]
    Http,
    #[error("Binance HTTP request timed out with an UNKNOWN mutation outcome")]
    Timeout,
    #[error("Binance connection ended with an UNKNOWN mutation outcome")]
    Disconnected,
    #[error("Binance returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("Binance returned ambiguous HTTP status {0} after dispatch")]
    AmbiguousStatus(u16),
    #[error("Binance response exceeded the bounded body or frame limit")]
    BodyTooLarge,
    #[error("Binance response or mutation acknowledgement is invalid")]
    Ack,
    #[error("Binance transport payload is invalid")]
    Payload,
    #[error("Binance transport clock is invalid or regressed")]
    Clock,
    #[error("Binance explicitly rejected the signed request timestamp")]
    TimestampRejected,
    #[error("Binance transport protocol state is invalid")]
    Protocol,
    #[error("Binance private stream ended")]
    EndOfStream,
}

impl BinanceTransportError {
    #[must_use]
    pub const fn is_unknown_dispatch(self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Disconnected | Self::AmbiguousStatus(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        time::sleep,
    };
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    use super::*;
    use crate::execution::prepared_for_transport_test;
    use crate::{
        BinanceAccountBinding, BinanceInstrumentRules, BinanceMutationKind,
        BinancePrivateReadScope, build_account_request, parse_instrument_rules,
    };

    const EXCHANGE_INFO: &str = include_str!("../tests/fixtures/exchange_info_btcusdt.json");
    const ACK: &[u8] = include_bytes!("../fixtures/place-order-ack.json");
    const EXACT: &[u8] = include_bytes!("../fixtures/exact-order-readback.json");
    const ACCOUNT: &[u8] = include_bytes!("../fixtures/portfolio-account.json");

    enum Behavior {
        Body(&'static [u8]),
        Status(u16, &'static [u8]),
        Partial(&'static [u8], usize),
        Delay(Duration),
    }

    async fn fake_http(
        behaviors: Vec<Behavior>,
    ) -> Result<(String, Arc<AtomicUsize>), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let count = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::clone(&count);
        tokio::spawn(async move {
            for behavior in behaviors {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                accepted.fetch_add(1, Ordering::SeqCst);
                let mut request = vec![0_u8; 16 * 1024];
                let _ = stream.read(&mut request).await;
                match behavior {
                    Behavior::Body(body) => {
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(header.as_bytes()).await;
                        let _ = stream.write_all(body).await;
                    }
                    Behavior::Status(status, body) => {
                        let header = format!(
                            "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(header.as_bytes()).await;
                        let _ = stream.write_all(body).await;
                    }
                    Behavior::Partial(body, sent_bytes) => {
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(header.as_bytes()).await;
                        let _ = stream.write_all(&body[..sent_bytes.min(body.len())]).await;
                    }
                    Behavior::Delay(duration) => sleep(duration).await,
                }
            }
        });
        Ok((format!("http://{address}"), count))
    }

    fn facts(
        account: &str,
    ) -> Result<
        (
            BinanceConfig,
            BinanceInstrumentRules,
            BinancePrivateReadScope,
        ),
        Box<dyn std::error::Error>,
    > {
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            account,
            "BTC/USDT".parse()?,
        )?;
        let config =
            BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)?;
        let rules = parse_instrument_rules(EXCHANGE_INFO, binding.symbol.clone(), 7)?;
        let scope = BinancePrivateReadScope::new(&config, &rules, 17, 11, 900)?;
        Ok((config, rules, scope))
    }

    #[test]
    fn transport_limits_are_finite_and_bounded() {
        assert_eq!(
            BinanceTransportLimits::new(Duration::ZERO, 1),
            Err(BinanceTransportError::Limits)
        );
        assert_eq!(
            BinanceTransportLimits::new(Duration::from_secs(61), 1),
            Err(BinanceTransportError::Limits)
        );
        assert_eq!(
            BinanceTransportLimits::new(Duration::from_secs(1), MAX_TRANSPORT_BYTES + 1),
            Err(BinanceTransportError::Limits)
        );
    }

    #[test]
    fn asset_index_request_uses_documented_fixed_public_pair()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, _, _) = facts("00000000-0000-4000-8000-000000000001")?;
        let transport = BinanceHttpTransport::new(
            config,
            7,
            17,
            BinanceTransportLimits::new(Duration::from_secs(1), 1024)?,
        )?;
        assert_eq!(
            transport.usd_m_asset_index_url(&"USDC".parse()?)?,
            "https://fapi.binance.com/fapi/v1/assetIndex?symbol=USDCUSD"
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_timeout_body_limit_generation_and_binding_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let credentials = BinanceCredentials::from_values("key", "secret")?;
        let (config, _, scope) = facts("00000000-0000-4000-8000-000000000001")?;
        let limits = BinanceTransportLimits::new(Duration::from_millis(20), 64)?;

        let (slow_endpoint, _) =
            fake_http(vec![Behavior::Delay(Duration::from_millis(100))]).await?;
        let slow =
            BinanceHttpTransport::with_endpoint(config.clone(), 7, 17, slow_endpoint, limits)?;
        let request = build_account_request(&scope)?;
        assert_eq!(
            slow.execute_read(&credentials, &request, 1_000).await,
            Err(BinanceTransportError::Timeout)
        );

        let oversized: &'static [u8] = Box::leak(vec![b'x'; 65].into_boxed_slice());
        let (large_endpoint, _) = fake_http(vec![Behavior::Body(oversized)]).await?;
        let large =
            BinanceHttpTransport::with_endpoint(config.clone(), 7, 17, large_endpoint, limits)?;
        assert_eq!(
            large.execute_read(&credentials, &request, 1_000).await,
            Err(BinanceTransportError::BodyTooLarge)
        );

        let (_, _, wrong_scope) = facts("00000000-0000-4000-8000-000000000002")?;
        assert_eq!(
            large
                .execute_read(&credentials, &build_account_request(&wrong_scope)?, 1_000)
                .await,
            Err(BinanceTransportError::Binding)
        );
        Ok(())
    }

    #[tokio::test]
    async fn ack_disconnect_is_unknown_and_mutation_is_dispatched_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let credentials = BinanceCredentials::from_values("key", "secret")?;
        let (config, _, scope) = facts("00000000-0000-4000-8000-000000000001")?;
        let (endpoint, count) = fake_http(vec![Behavior::Partial(ACK, ACK.len() / 2)]).await?;
        let transport = BinanceHttpTransport::with_endpoint(
            config,
            7,
            17,
            endpoint,
            BinanceTransportLimits::new(Duration::from_secs(1), 1024)?,
        )?;
        let request =
            prepared_for_transport_test(&scope, BinanceMutationKind::PlaceLimit, "venue_place_1");

        let error = match transport
            .dispatch_once(&credentials, &scope, &request, unix_ms()?)
            .await
        {
            Ok(_) => return Err("the fake unexpectedly acknowledged a closed connection".into()),
            Err(error) => error,
        };
        assert!(error.is_unknown_dispatch());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn timestamp_rejected_mutation_resyncs_clock_but_never_replays_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let credentials = BinanceCredentials::from_values("key", "secret")?;
        let (config, _, scope) = facts("00000000-0000-4000-8000-000000000001")?;
        let server_time = unix_ms()?;
        let time_payload: &'static [u8] = Box::leak(
            format!(r#"{{"serverTime":{server_time}}}"#)
                .into_bytes()
                .into_boxed_slice(),
        );
        let mut behaviors = vec![Behavior::Status(
            400,
            br#"{"code":-1021,"msg":"timestamp outside recvWindow"}"#,
        )];
        behaviors.extend((0..TIME_SYNC_ATTEMPTS).map(|_| Behavior::Body(time_payload)));
        let (endpoint, count) = fake_http(behaviors).await?;
        let transport = BinanceHttpTransport::with_endpoint(
            config,
            7,
            17,
            endpoint,
            BinanceTransportLimits::new(Duration::from_secs(1), 4096)?,
        )?;
        let request =
            prepared_for_transport_test(&scope, BinanceMutationKind::PlaceLimit, "venue_place_1");

        assert_eq!(
            transport
                .dispatch_once(&credentials, &scope, &request, unix_ms()?)
                .await,
            Err(BinanceTransportError::TimestampRejected)
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            1 + TIME_SYNC_ATTEMPTS,
            "one mutation plus bounded read-only clock samples"
        );
        Ok(())
    }

    #[tokio::test]
    async fn successful_ack_is_followed_by_one_separately_signed_exact_readback()
    -> Result<(), Box<dyn std::error::Error>> {
        let credentials = BinanceCredentials::from_values("key", "secret")?;
        let (config, _, scope) = facts("00000000-0000-4000-8000-000000000001")?;
        let (endpoint, count) = fake_http(vec![Behavior::Body(ACK), Behavior::Body(EXACT)]).await?;
        let transport = BinanceHttpTransport::with_endpoint(
            config,
            7,
            17,
            endpoint,
            BinanceTransportLimits::new(Duration::from_secs(1), 4096)?,
        )?;
        let request =
            prepared_for_transport_test(&scope, BinanceMutationKind::PlaceLimit, "venue_place_1");

        let outcome = transport
            .dispatch_then_exact_readback(&credentials, &scope, &request, unix_ms()?)
            .await;
        assert!(matches!(
            outcome,
            BinancePhysicalMutationOutcome::ReadBack { ref ack, ref readback }
                if ack.order_id == "401" && readback.order.order_id == "401"
        ));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn api_key_listen_key_request_uses_live_scope_with_loopback_fixture()
    -> Result<(), Box<dyn std::error::Error>> {
        let credentials = BinanceCredentials::from_values("key", "secret")?;
        let (config, _, _) = facts("00000000-0000-4000-8000-000000000001")?;
        assert_eq!(config.mode(), GatewayMode::Live);
        assert_eq!(config.portfolio_rest_origin(), "https://papi.binance.com");
        let (endpoint, _) =
            fake_http(vec![Behavior::Body(br#"{"listenKey":"test-listen-key"}"#)]).await?;
        let transport = BinanceHttpTransport::with_endpoint(
            config,
            7,
            17,
            endpoint,
            BinanceTransportLimits::new(Duration::from_secs(1), 1024)?,
        )?;
        assert!(
            format!("{:?}", transport.create_listen_key(&credentials).await?).contains("redacted")
        );
        Ok(())
    }

    #[tokio::test]
    async fn server_clock_uses_midpoint_sample_before_signed_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let (config, _, _) = facts("00000000-0000-4000-8000-000000000001")?;
        let server_time = unix_ms()?.saturating_add(250);
        let payload: &'static [u8] = Box::leak(
            format!(r#"{{"serverTime":{server_time}}}"#)
                .into_bytes()
                .into_boxed_slice(),
        );
        let (endpoint, count) = fake_http(
            (0..TIME_SYNC_ATTEMPTS)
                .map(|_| Behavior::Body(payload))
                .collect(),
        )
        .await?;
        let transport = BinanceHttpTransport::with_endpoint(
            config,
            7,
            17,
            endpoint,
            BinanceTransportLimits::new(Duration::from_secs(1), 1024)?,
        )?;
        transport.synchronize_clock().await?;
        let signed = transport.signing_timestamp_ms()?;
        assert!(signed >= server_time.saturating_sub(50));
        assert_eq!(count.load(Ordering::SeqCst), TIME_SYNC_ATTEMPTS);
        Ok(())
    }

    #[tokio::test]
    async fn signed_wire_offset_never_relabels_local_request_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let credentials = BinanceCredentials::from_values("key", "secret")?;
        let (config, _, scope) = facts("00000000-0000-4000-8000-000000000001")?;
        for offset in [-5_000_i64, 5_000_i64] {
            let (endpoint, _) = fake_http(vec![Behavior::Body(ACCOUNT)]).await?;
            let transport = BinanceHttpTransport::with_endpoint(
                config.clone(),
                7,
                17,
                endpoint,
                BinanceTransportLimits::new(Duration::from_secs(1), 4096)?,
            )?;
            transport.clock_offset_ms.store(offset, Ordering::Relaxed);
            transport.clock_synchronized.store(true, Ordering::Release);
            let before = unix_ms()?;
            let page = transport
                .execute_read(
                    &credentials,
                    &build_account_request(&scope)?,
                    transport.signing_timestamp_ms()?,
                )
                .await?;
            assert!(page.requested_at_ms >= before);
            assert!(page.received_at_ms >= page.requested_at_ms);
            assert!(page.received_at_ms.saturating_sub(page.requested_at_ms) < 1_000);
        }
        Ok(())
    }

    #[test]
    fn midpoint_offset_and_checked_adjustment_match_clock_contract() {
        assert_eq!(time_offset_ms(10_090, 10_050), 40);
        assert_eq!(authoritative_timestamp(20_000, 40), Ok(20_040));
        assert_eq!(authoritative_timestamp(20_000, -40), Ok(19_960));
        assert_eq!(
            authoritative_timestamp(u64::MAX, 1),
            Err(BinanceTransportError::Clock)
        );
        assert_eq!(
            classify_http_error(400, br#"{"code":-1021,"msg":"timestamp"}"#, false),
            BinanceTransportError::TimestampRejected
        );
        assert_eq!(
            classify_http_error(503, br#"{"code":-1000}"#, true),
            BinanceTransportError::AmbiguousStatus(503)
        );
    }

    #[tokio::test]
    async fn ordinary_signed_read_uses_the_fixture_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let credentials = BinanceCredentials::from_values("key", "secret")?;
        let (config, _, scope) = facts("00000000-0000-4000-8000-000000000001")?;
        let (endpoint, _) = fake_http(vec![Behavior::Body(ACCOUNT)]).await?;
        let transport = BinanceHttpTransport::with_endpoint(
            config,
            7,
            17,
            endpoint,
            BinanceTransportLimits::new(Duration::from_secs(1), 4096)?,
        )?;
        let page = transport
            .execute_read(&credentials, &build_account_request(&scope)?, unix_ms()?)
            .await?;
        assert_eq!(page.payload.as_ref(), ACCOUNT);
        Ok(())
    }
}
