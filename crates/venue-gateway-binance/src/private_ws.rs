use std::{
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Error as WebSocketError, Message, protocol::WebSocketConfig},
};
use venue_gateway_api::GatewayBinding;

use crate::{BinanceConfig, BinanceTransportError, BinanceTransportLimits};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READINESS_TIMEOUT: Duration = Duration::from_millis(1);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

pub struct BinanceListenKey {
    binding: GatewayBinding,
    instrument_generation: u64,
    private_generation: u64,
    secret: SecretString,
}

impl BinanceListenKey {
    pub(crate) fn from_response(
        binding: &GatewayBinding,
        instrument_generation: u64,
        private_generation: u64,
        payload: &[u8],
    ) -> Result<Self, BinanceTransportError> {
        if instrument_generation == 0 || private_generation == 0 {
            return Err(BinanceTransportError::Binding);
        }
        let value: Value =
            serde_json::from_slice(payload).map_err(|_| BinanceTransportError::Payload)?;
        let listen_key = value
            .get("listenKey")
            .and_then(Value::as_str)
            .filter(|value| {
                (1..=256).contains(&value.len())
                    && value.bytes().all(|byte| byte.is_ascii_graphic())
            })
            .ok_or(BinanceTransportError::Payload)?;
        Ok(Self {
            binding: binding.clone(),
            instrument_generation,
            private_generation,
            secret: SecretString::from(listen_key.to_owned()),
        })
    }

    fn expose(&self) -> &str {
        self.secret.expose_secret()
    }
}

impl fmt::Debug for BinanceListenKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BinanceListenKey([redacted])")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRawPrivateFrame {
    pub binding: GatewayBinding,
    pub instrument_generation: u64,
    pub private_generation: u64,
    pub received_at_ms: u64,
    pub payload: Bytes,
}

pub struct BinancePrivateWsTransport<S = MaybeTlsStream<TcpStream>> {
    stream: WebSocketStream<S>,
    binding: GatewayBinding,
    instrument_generation: u64,
    private_generation: u64,
    endpoint: String,
    listen_key: BinanceListenKey,
    limits: BinanceTransportLimits,
    next_heartbeat_at: Instant,
}

impl<S> BinancePrivateWsTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
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
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Polls at most one frame with the fixed 1 ms post-handshake readiness budget. Idle sockets
    /// return `None`; disconnects fail closed and require a new, higher private generation.
    pub async fn poll_raw_frame(
        &mut self,
    ) -> Result<Option<BinanceRawPrivateFrame>, BinanceTransportError> {
        if Instant::now() >= self.next_heartbeat_at {
            self.send(Message::Ping(Bytes::new())).await?;
            self.next_heartbeat_at = Instant::now() + HEARTBEAT_INTERVAL;
        }
        let message = match timeout(READINESS_TIMEOUT, self.stream.next()).await {
            Err(_) => return Ok(None),
            Ok(None) => return Err(BinanceTransportError::EndOfStream),
            Ok(Some(Err(error))) => return Err(map_websocket(error)),
            Ok(Some(Ok(message))) => message,
        };
        match message {
            Message::Text(value) => self.frame(value.as_bytes()).map(Some),
            Message::Binary(_) => Err(BinanceTransportError::Protocol),
            Message::Ping(value) => {
                self.send(Message::Pong(value)).await?;
                Ok(None)
            }
            Message::Pong(_) => Ok(None),
            Message::Close(_) => Err(BinanceTransportError::EndOfStream),
            Message::Frame(_) => Err(BinanceTransportError::Protocol),
        }
    }

    async fn send(&mut self, message: Message) -> Result<(), BinanceTransportError> {
        timeout(self.limits.operation_timeout(), self.stream.send(message))
            .await
            .map_err(|_| BinanceTransportError::Timeout)?
            .map_err(map_websocket)
    }

    fn frame(&self, payload: &[u8]) -> Result<BinanceRawPrivateFrame, BinanceTransportError> {
        if payload.is_empty() || payload.len() > self.limits.maximum_body_bytes() {
            return Err(BinanceTransportError::BodyTooLarge);
        }
        let payload = sanitize_payload(&self.listen_key, payload)?;
        Ok(BinanceRawPrivateFrame {
            binding: self.binding.clone(),
            instrument_generation: self.instrument_generation,
            private_generation: self.private_generation,
            received_at_ms: unix_ms()?,
            payload,
        })
    }
}

pub async fn connect_private_ws(
    config: &BinanceConfig,
    instrument_generation: u64,
    private_generation: u64,
    listen_key: BinanceListenKey,
    limits: BinanceTransportLimits,
) -> Result<BinancePrivateWsTransport, BinanceTransportError> {
    let endpoint = format!(
        "{}/{}",
        config.private_stream_origin().trim_end_matches('/'),
        listen_key.expose()
    );
    connect_private_ws_endpoint(
        config,
        instrument_generation,
        private_generation,
        endpoint,
        listen_key,
        limits,
        true,
    )
    .await
}

async fn connect_private_ws_endpoint(
    config: &BinanceConfig,
    instrument_generation: u64,
    private_generation: u64,
    endpoint: String,
    listen_key: BinanceListenKey,
    limits: BinanceTransportLimits,
    require_fixed_endpoint: bool,
) -> Result<BinancePrivateWsTransport, BinanceTransportError> {
    let fixed_prefix = format!("{}/", config.private_stream_origin().trim_end_matches('/'));
    if instrument_generation == 0
        || private_generation == 0
        || listen_key.binding != *config.gateway_binding()
        || listen_key.instrument_generation != instrument_generation
        || listen_key.private_generation != private_generation
        || endpoint.is_empty()
        || require_fixed_endpoint && !endpoint.starts_with(&fixed_prefix)
    {
        return Err(BinanceTransportError::Binding);
    }
    let websocket_config = WebSocketConfig::default()
        .max_message_size(Some(limits.maximum_body_bytes()))
        .max_frame_size(Some(limits.maximum_body_bytes()));
    let connect_budget = limits.operation_timeout().min(CONNECT_TIMEOUT);
    let (stream, response) = timeout(
        connect_budget,
        connect_async_with_config(&endpoint, Some(websocket_config), false),
    )
    .await
    .map_err(|_| BinanceTransportError::Timeout)?
    .map_err(map_websocket)?;
    if response.status() != tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS {
        return Err(BinanceTransportError::HttpStatus(
            response.status().as_u16(),
        ));
    }
    Ok(BinancePrivateWsTransport {
        stream,
        binding: config.gateway_binding().clone(),
        instrument_generation,
        private_generation,
        endpoint: redact_endpoint(&endpoint),
        listen_key,
        limits,
        next_heartbeat_at: Instant::now() + HEARTBEAT_INTERVAL,
    })
}

fn sanitize_payload(
    listen_key: &BinanceListenKey,
    payload: &[u8],
) -> Result<Bytes, BinanceTransportError> {
    let Ok(mut value) = serde_json::from_slice::<Value>(payload) else {
        return Ok(Bytes::copy_from_slice(payload));
    };
    if value.get("e").and_then(Value::as_str) != Some("listenKeyExpired") {
        return Ok(Bytes::copy_from_slice(payload));
    }
    if value.get("listenKey").and_then(Value::as_str) != Some(listen_key.expose()) {
        return Err(BinanceTransportError::Binding);
    }
    let object = value
        .as_object_mut()
        .ok_or(BinanceTransportError::Payload)?;
    object.insert(
        "listenKey".to_owned(),
        Value::String("[redacted]".to_owned()),
    );
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|_| BinanceTransportError::Payload)
}

fn redact_endpoint(endpoint: &str) -> String {
    endpoint.rsplit_once('/').map_or_else(
        || "[redacted]".to_owned(),
        |(prefix, _)| format!("{prefix}/[redacted]"),
    )
}

fn map_websocket(error: WebSocketError) -> BinanceTransportError {
    if matches!(error, WebSocketError::Capacity(_)) {
        BinanceTransportError::BodyTooLarge
    } else {
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

#[cfg(test)]
pub(crate) async fn connect_private_ws_for_test(
    config: &BinanceConfig,
    instrument_generation: u64,
    private_generation: u64,
    endpoint: String,
    listen_key: BinanceListenKey,
    limits: BinanceTransportLimits,
) -> Result<
    BinancePrivateWsTransport<tokio_tungstenite::MaybeTlsStream<TcpStream>>,
    BinanceTransportError,
> {
    connect_private_ws_endpoint(
        config,
        instrument_generation,
        private_generation,
        endpoint,
        listen_key,
        limits,
        false,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{net::TcpListener, task::yield_now};
    use tokio_tungstenite::accept_async;
    use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

    fn config() -> Result<BinanceConfig, Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Test,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        Ok(BinanceConfig::for_binding(
            crate::BinanceAccountBinding::PortfolioMarginUm,
            &binding,
        )?)
    }

    #[tokio::test]
    async fn private_ws_preserves_generations_and_redacts_expired_listen_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut socket) = accept_async(stream).await else {
                return;
            };
            let payload = r#"{"e":"listenKeyExpired","E":123,"listenKey":"test-listen-key"}"#;
            let _ = socket.send(Message::Text(payload.into())).await;
        });
        let config = config()?;
        let wrong_generation_key = BinanceListenKey::from_response(
            config.gateway_binding(),
            7,
            18,
            br#"{"listenKey":"wrong-generation"}"#,
        )?;
        assert!(matches!(
            connect_private_ws_for_test(
                &config,
                7,
                17,
                endpoint.clone(),
                wrong_generation_key,
                BinanceTransportLimits::new(Duration::from_secs(1), 4096)?,
            )
            .await,
            Err(BinanceTransportError::Binding)
        ));
        let listen_key = BinanceListenKey::from_response(
            config.gateway_binding(),
            7,
            17,
            br#"{"listenKey":"test-listen-key"}"#,
        )?;
        let mut transport = connect_private_ws_for_test(
            &config,
            7,
            17,
            endpoint,
            listen_key,
            BinanceTransportLimits::new(Duration::from_secs(1), 4096)?,
        )
        .await?;

        let mut received = None;
        for _ in 0..100 {
            if let Some(frame) = transport.poll_raw_frame().await? {
                received = Some(frame);
                break;
            }
            yield_now().await;
        }
        let frame = received.ok_or("private frame was not received")?;
        assert_eq!(frame.instrument_generation, 7);
        assert_eq!(frame.private_generation, 17);
        assert!(std::str::from_utf8(&frame.payload)?.contains("[redacted]"));
        assert!(!std::str::from_utf8(&frame.payload)?.contains("test-listen-key"));
        assert!(transport.endpoint().ends_with("/[redacted]"));
        Ok(())
    }

    #[test]
    fn listen_key_never_implements_secret_revealing_debug() -> Result<(), Box<dyn std::error::Error>>
    {
        let config = config()?;
        let key = BinanceListenKey::from_response(
            config.gateway_binding(),
            7,
            17,
            br#"{"listenKey":"secret-key"}"#,
        )?;
        assert_eq!(format!("{key:?}"), "BinanceListenKey([redacted])");
        assert_eq!(
            sanitize_payload(&key, br#"{"e":"listenKeyExpired","listenKey":"wrong-key"}"#,),
            Err(BinanceTransportError::Binding)
        );
        Ok(())
    }
}
