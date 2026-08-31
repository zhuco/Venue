use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
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

use crate::{BinanceConfig, BinanceTransportError, BinanceTransportLimits, native_symbol};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READINESS_TIMEOUT: Duration = Duration::from_millis(1);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

/// One bounded public wire frame. It is intentionally adapter-local evidence: callers consume
/// only the normalized market facts produced from it and never persist this raw payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceRawPublicFrame {
    pub binding: GatewayBinding,
    pub instrument_generation: u64,
    pub received_at_ms: u64,
    pub payload: Bytes,
}

/// Read-only combined Binance Futures stream for precisely one account-bound canonical symbol.
/// It contains no account credential, private generation, writer, or mutation permit.
pub struct BinancePublicWsTransport<S = MaybeTlsStream<TcpStream>> {
    stream: WebSocketStream<S>,
    binding: GatewayBinding,
    instrument_generation: u64,
    endpoint: String,
    limits: BinanceTransportLimits,
    next_heartbeat_at: Instant,
}

impl<S> BinancePublicWsTransport<S>
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
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Polls at most one frame. Idle is not evidence of a market update; disconnects are errors
    /// so the gateway must fail closed rather than relabeling a replacement stream in place.
    pub async fn poll_raw_frame(
        &mut self,
    ) -> Result<Option<BinanceRawPublicFrame>, BinanceTransportError> {
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
            Message::Binary(_) | Message::Frame(_) => Err(BinanceTransportError::Protocol),
            Message::Ping(value) => {
                self.send(Message::Pong(value)).await?;
                Ok(None)
            }
            Message::Pong(_) => Ok(None),
            Message::Close(_) => Err(BinanceTransportError::EndOfStream),
        }
    }

    async fn send(&mut self, message: Message) -> Result<(), BinanceTransportError> {
        timeout(self.limits.operation_timeout(), self.stream.send(message))
            .await
            .map_err(|_| BinanceTransportError::Timeout)?
            .map_err(map_websocket)
    }

    fn frame(&self, payload: &[u8]) -> Result<BinanceRawPublicFrame, BinanceTransportError> {
        if payload.is_empty() || payload.len() > self.limits.maximum_body_bytes() {
            return Err(BinanceTransportError::BodyTooLarge);
        }
        Ok(BinanceRawPublicFrame {
            binding: self.binding.clone(),
            instrument_generation: self.instrument_generation,
            received_at_ms: unix_ms()?,
            payload: Bytes::copy_from_slice(payload),
        })
    }
}

/// Connects to Binance's fixed production combined-stream endpoint. The sequence includes the
/// exact public fact families needed by the existing shared scalping feature source; it performs
/// no REST, private-stream, or mutation operation.
pub async fn connect_public_ws(
    config: &BinanceConfig,
    instrument_generation: u64,
    limits: BinanceTransportLimits,
) -> Result<BinancePublicWsTransport, BinanceTransportError> {
    let native = native_symbol(&config.gateway_binding().symbol).to_ascii_lowercase();
    let endpoint = format!(
        "{}/stream?streams={native}@bookTicker/{native}@depth@100ms/{native}@aggTrade/{native}@kline_1m",
        config.public_stream_origin().trim_end_matches('/')
    );
    connect_public_ws_endpoint(config, instrument_generation, endpoint, limits, true).await
}

async fn connect_public_ws_endpoint(
    config: &BinanceConfig,
    instrument_generation: u64,
    endpoint: String,
    limits: BinanceTransportLimits,
    require_fixed_endpoint: bool,
) -> Result<BinancePublicWsTransport, BinanceTransportError> {
    let native = native_symbol(&config.gateway_binding().symbol).to_ascii_lowercase();
    let fixed = format!(
        "{}/stream?streams={native}@bookTicker/{native}@depth@100ms/{native}@aggTrade/{native}@kline_1m",
        config.public_stream_origin().trim_end_matches('/')
    );
    if instrument_generation == 0
        || endpoint.is_empty()
        || require_fixed_endpoint && endpoint != fixed
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
    Ok(BinancePublicWsTransport {
        stream,
        binding: config.gateway_binding().clone(),
        instrument_generation,
        endpoint,
        limits,
        next_heartbeat_at: Instant::now() + HEARTBEAT_INTERVAL,
    })
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
pub(crate) async fn connect_public_ws_for_test(
    config: &BinanceConfig,
    instrument_generation: u64,
    endpoint: String,
    limits: BinanceTransportLimits,
) -> Result<BinancePublicWsTransport<MaybeTlsStream<TcpStream>>, BinanceTransportError> {
    connect_public_ws_endpoint(config, instrument_generation, endpoint, limits, false).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{net::TcpListener, task::yield_now};
    use tokio_tungstenite::accept_async;
    use venue_gateway_api::{GatewayMode, VenueId};

    fn config() -> Result<BinanceConfig, Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?;
        Ok(BinanceConfig::for_binding(
            crate::BinanceAccountBinding::PortfolioMarginUm,
            &binding,
        )?)
    }

    #[tokio::test]
    async fn public_ws_preserves_scope_generation_and_payload_bound()
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
            let _ = socket.send(Message::Text(
                r#"{"e":"bookTicker","E":1000,"T":999,"s":"BTCUSDT","u":1,"b":"100","B":"2","a":"101","A":"3"}"#.into(),
            )).await;
        });
        let config = config()?;
        let mut transport = connect_public_ws_for_test(
            &config,
            7,
            endpoint,
            BinanceTransportLimits::new(Duration::from_secs(1), 4096)?,
        )
        .await?;
        let mut frame = None;
        for _ in 0..100 {
            if let Some(value) = transport.poll_raw_frame().await? {
                frame = Some(value);
                break;
            }
            yield_now().await;
        }
        let frame = frame.ok_or("public frame was not received")?;
        assert_eq!(frame.binding, *config.gateway_binding());
        assert_eq!(frame.instrument_generation, 7);
        assert!(std::str::from_utf8(&frame.payload)?.contains("bookTicker"));
        assert!(!transport.endpoint().contains("listenKey"));
        Ok(())
    }
}
