//! Authenticated UTA v3 private WebSocket with ACK-gated raw delivery.

use std::{collections::VecDeque, fmt, time::Duration};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
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

use crate::{
    BitgetAccountBinding, BitgetConfig, BitgetCredentials, BitgetTransportError,
    BitgetTransportLimits, transport::unix_ms, ws_sign,
};

const PRIVATE_TOPICS: [&str; 3] = ["account", "position", "order"];
const MAX_PRE_LIVE_FRAMES: usize = 256;
const MAX_PRE_LIVE_BYTES: usize = 1024 * 1024;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Clone, Eq, PartialEq)]
pub struct BitgetRawPrivateFrame {
    pub binding: GatewayBinding,
    pub generation: u64,
    pub topic: String,
    pub received_at_ms: u64,
    pub payload: Bytes,
}

impl fmt::Debug for BitgetRawPrivateFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BitgetRawPrivateFrame")
            .field("binding", &self.binding)
            .field("generation", &self.generation)
            .field("topic", &self.topic)
            .field("received_at_ms", &self.received_at_ms)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

pub struct BitgetPrivateWsTransport<S = MaybeTlsStream<TcpStream>> {
    stream: WebSocketStream<S>,
    binding: GatewayBinding,
    generation: u64,
    endpoint: String,
    connection_id: String,
    limits: BitgetTransportLimits,
    pre_live_frames: VecDeque<BitgetRawPrivateFrame>,
    buffered_bytes: usize,
    next_heartbeat_at: Instant,
}

impl<S> BitgetPrivateWsTransport<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    pub async fn next_frame(&mut self) -> Result<BitgetRawPrivateFrame, BitgetTransportError> {
        if let Some(frame) = self.pre_live_frames.pop_front() {
            self.buffered_bytes = self.buffered_bytes.saturating_sub(frame.payload.len());
            return Ok(frame);
        }
        if Instant::now() >= self.next_heartbeat_at {
            send_message(
                &mut self.stream,
                Message::Text("ping".into()),
                self.limits.operation_timeout(),
            )
            .await?;
            self.next_heartbeat_at = Instant::now() + HEARTBEAT_INTERVAL;
        }
        let operation = async {
            loop {
                let message = self
                    .stream
                    .next()
                    .await
                    .ok_or(BitgetTransportError::Disconnected)?
                    .map_err(map_websocket)?;
                match message {
                    Message::Text(value) if value.as_str() == "pong" => continue,
                    Message::Text(value) if value.as_str() == "ping" => {
                        self.stream
                            .send(Message::Text("pong".into()))
                            .await
                            .map_err(map_websocket)?;
                    }
                    Message::Text(value) => {
                        return make_raw_frame(
                            &self.binding,
                            self.generation,
                            Bytes::copy_from_slice(value.as_bytes()),
                            self.limits.maximum_body_bytes(),
                            unix_ms()?,
                        );
                    }
                    Message::Ping(value) => {
                        self.stream
                            .send(Message::Pong(value))
                            .await
                            .map_err(map_websocket)?;
                    }
                    Message::Pong(_) => {}
                    Message::Binary(_) | Message::Frame(_) => {
                        return Err(BitgetTransportError::Protocol);
                    }
                    Message::Close(_) => return Err(BitgetTransportError::Disconnected),
                }
            }
        };
        timeout(self.limits.operation_timeout(), operation)
            .await
            .map_err(|_| BitgetTransportError::Timeout)?
    }
}

pub async fn connect_private_ws(
    binding: GatewayBinding,
    credentials: &BitgetCredentials,
    generation: u64,
    now_ms: u64,
    limits: BitgetTransportLimits,
) -> Result<BitgetPrivateWsTransport, BitgetTransportError> {
    let config = BitgetConfig::for_mode(binding.mode);
    validate_binding(&binding, &config, generation)?;
    let endpoint = config.private_ws();
    let websocket = WebSocketConfig::default()
        .max_message_size(Some(limits.maximum_body_bytes()))
        .max_frame_size(Some(limits.maximum_body_bytes()));
    let (stream, _) = timeout(
        limits.operation_timeout(),
        connect_async_with_config(endpoint, Some(websocket), true),
    )
    .await
    .map_err(|_| BitgetTransportError::Timeout)?
    .map_err(map_websocket)?;
    authenticate_private_stream(
        stream,
        endpoint.to_owned(),
        binding,
        config,
        credentials,
        generation,
        now_ms,
        limits,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn authenticate_private_stream<S>(
    mut stream: WebSocketStream<S>,
    endpoint: String,
    binding: GatewayBinding,
    config: BitgetConfig,
    credentials: &BitgetCredentials,
    generation: u64,
    now_ms: u64,
    limits: BitgetTransportLimits,
) -> Result<BitgetPrivateWsTransport<S>, BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    validate_binding(&binding, &config, generation)?;
    if now_ms < 1_000 || endpoint.is_empty() {
        return Err(BitgetTransportError::Clock);
    }
    let timestamp_s = now_ms / 1_000;
    let signature = ws_sign(credentials, timestamp_s).map_err(|_| BitgetTransportError::Signing)?;
    let login = LoginFrame {
        operation: "login",
        args: [LoginArg {
            api_key: credentials.api_key.expose_secret(),
            passphrase: credentials.passphrase.expose_secret(),
            timestamp: timestamp_s.to_string(),
            sign: &signature,
        }],
    };
    let login_payload = SecretString::from(
        serde_json::to_string(&login).map_err(|_| BitgetTransportError::Protocol)?,
    );
    send_secret(&mut stream, &login_payload, limits.operation_timeout()).await?;
    let login_ack = read_json(&mut stream, limits).await?;
    validate_login_ack(&login_ack)?;

    let subscribe = SubscribeFrame {
        operation: "subscribe",
        args: PRIVATE_TOPICS.map(|topic| SubscriptionArg {
            instrument_type: "UTA",
            topic,
        }),
    };
    let payload = serde_json::to_string(&subscribe).map_err(|_| BitgetTransportError::Protocol)?;
    send_message(
        &mut stream,
        Message::Text(payload.into()),
        limits.operation_timeout(),
    )
    .await?;
    let (connection_id, pre_live_frames, buffered_bytes) =
        read_subscription_acks(&mut stream, &binding, generation, limits).await?;
    Ok(BitgetPrivateWsTransport {
        stream,
        binding,
        generation,
        endpoint,
        connection_id,
        limits,
        pre_live_frames,
        buffered_bytes,
        next_heartbeat_at: Instant::now() + HEARTBEAT_INTERVAL,
    })
}

async fn read_subscription_acks<S>(
    stream: &mut WebSocketStream<S>,
    binding: &GatewayBinding,
    generation: u64,
    limits: BitgetTransportLimits,
) -> Result<(String, VecDeque<BitgetRawPrivateFrame>, usize), BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let operation = async {
        let mut pending = PRIVATE_TOPICS.into_iter().collect::<Vec<_>>();
        let mut connection_id = None;
        let mut frames = VecDeque::new();
        let mut buffered_bytes = 0_usize;
        while !pending.is_empty() {
            let message = stream
                .next()
                .await
                .ok_or(BitgetTransportError::Disconnected)?
                .map_err(map_websocket)?;
            let payload = match message {
                Message::Text(value) if value.as_str() == "ping" => {
                    stream
                        .send(Message::Text("pong".into()))
                        .await
                        .map_err(map_websocket)?;
                    continue;
                }
                Message::Text(value) => Bytes::copy_from_slice(value.as_bytes()),
                Message::Ping(value) => {
                    stream
                        .send(Message::Pong(value))
                        .await
                        .map_err(map_websocket)?;
                    continue;
                }
                Message::Pong(_) => continue,
                Message::Binary(_) | Message::Frame(_) => {
                    return Err(BitgetTransportError::Protocol);
                }
                Message::Close(_) => return Err(BitgetTransportError::Disconnected),
            };
            if payload.len() > limits.maximum_body_bytes() {
                return Err(BitgetTransportError::BodyTooLarge);
            }
            let value: Value =
                serde_json::from_slice(&payload).map_err(|_| BitgetTransportError::Protocol)?;
            if value.get("arg").is_some() && value.get("data").is_some() {
                let frame = make_raw_frame(
                    binding,
                    generation,
                    payload,
                    limits.maximum_body_bytes(),
                    unix_ms()?,
                )?;
                buffered_bytes = buffered_bytes
                    .checked_add(frame.payload.len())
                    .ok_or(BitgetTransportError::BodyTooLarge)?;
                if frames.len() >= MAX_PRE_LIVE_FRAMES || buffered_bytes > MAX_PRE_LIVE_BYTES {
                    return Err(BitgetTransportError::BodyTooLarge);
                }
                frames.push_back(frame);
                continue;
            }
            let ack: EventAck =
                serde_json::from_value(value).map_err(|_| BitgetTransportError::Protocol)?;
            let topic = validate_subscription_ack(&ack)?;
            let Some(index) = pending.iter().position(|candidate| candidate == &topic) else {
                return Err(BitgetTransportError::Protocol);
            };
            pending.swap_remove(index);
            if let Some(actual) = ack.connection_id {
                if actual.is_empty()
                    || connection_id
                        .as_ref()
                        .is_some_and(|expected| expected != &actual)
                {
                    return Err(BitgetTransportError::Protocol);
                }
                connection_id = Some(actual);
            }
        }
        Ok((
            connection_id.ok_or(BitgetTransportError::Protocol)?,
            frames,
            buffered_bytes,
        ))
    };
    timeout(limits.operation_timeout(), operation)
        .await
        .map_err(|_| BitgetTransportError::Timeout)?
}

fn make_raw_frame(
    binding: &GatewayBinding,
    generation: u64,
    payload: Bytes,
    maximum_body_bytes: usize,
    received_at_ms: u64,
) -> Result<BitgetRawPrivateFrame, BitgetTransportError> {
    if generation == 0
        || received_at_ms == 0
        || payload.is_empty()
        || payload.len() > maximum_body_bytes
    {
        return Err(BitgetTransportError::Protocol);
    }
    let value: Value =
        serde_json::from_slice(&payload).map_err(|_| BitgetTransportError::Protocol)?;
    let argument = value
        .get("arg")
        .and_then(Value::as_object)
        .ok_or(BitgetTransportError::Protocol)?;
    if argument.get("instType").and_then(Value::as_str) != Some("UTA")
        || !value.get("data").is_some_and(Value::is_array)
    {
        return Err(BitgetTransportError::Protocol);
    }
    let topic = argument
        .get("topic")
        .and_then(Value::as_str)
        .ok_or(BitgetTransportError::Protocol)?;
    if !PRIVATE_TOPICS.contains(&topic) {
        return Err(BitgetTransportError::Protocol);
    }
    Ok(BitgetRawPrivateFrame {
        binding: binding.clone(),
        generation,
        topic: topic.to_owned(),
        received_at_ms,
        payload,
    })
}

async fn read_json<S>(
    stream: &mut WebSocketStream<S>,
    limits: BitgetTransportLimits,
) -> Result<Value, BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let operation = async {
        loop {
            let message = stream
                .next()
                .await
                .ok_or(BitgetTransportError::Disconnected)?
                .map_err(map_websocket)?;
            match message {
                Message::Text(value) => {
                    if value.len() > limits.maximum_body_bytes() {
                        return Err(BitgetTransportError::BodyTooLarge);
                    }
                    return serde_json::from_str(&value)
                        .map_err(|_| BitgetTransportError::Protocol);
                }
                Message::Ping(value) => {
                    stream
                        .send(Message::Pong(value))
                        .await
                        .map_err(map_websocket)?;
                }
                Message::Pong(_) => {}
                Message::Binary(_) | Message::Frame(_) => {
                    return Err(BitgetTransportError::Protocol);
                }
                Message::Close(_) => return Err(BitgetTransportError::Disconnected),
            }
        }
    };
    timeout(limits.operation_timeout(), operation)
        .await
        .map_err(|_| BitgetTransportError::Timeout)?
}

fn validate_login_ack(value: &Value) -> Result<(), BitgetTransportError> {
    let object = value.as_object().ok_or(BitgetTransportError::Protocol)?;
    if object.get("event").and_then(Value::as_str) != Some("login")
        || object.get("code").and_then(Value::as_str) != Some("0")
        || object
            .get("msg")
            .and_then(Value::as_str)
            .is_some_and(|message| !message.is_empty())
    {
        return Err(BitgetTransportError::Protocol);
    }
    Ok(())
}

fn validate_subscription_ack(ack: &EventAck) -> Result<String, BitgetTransportError> {
    if ack.event != "subscribe"
        || ack
            .code
            .as_deref()
            .is_some_and(|code| code != "0" && code != "00000")
        || ack
            .message
            .as_deref()
            .is_some_and(|message| !message.is_empty())
        || ack.argument.instrument_type != "UTA"
        || !PRIVATE_TOPICS.contains(&ack.argument.topic.as_str())
    {
        return Err(BitgetTransportError::Protocol);
    }
    Ok(ack.argument.topic.clone())
}

fn validate_binding(
    binding: &GatewayBinding,
    config: &BitgetConfig,
    generation: u64,
) -> Result<(), BitgetTransportError> {
    BitgetAccountBinding::UtaUsdtFuturesHedge
        .validate_gateway_binding(binding)
        .map_err(|_| BitgetTransportError::Binding)?;
    if binding.mode != config.mode() || generation == 0 {
        return Err(BitgetTransportError::Binding);
    }
    Ok(())
}

async fn send_secret<S>(
    stream: &mut WebSocketStream<S>,
    payload: &SecretString,
    operation_timeout: Duration,
) -> Result<(), BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    send_message(
        stream,
        Message::Text(payload.expose_secret().to_owned().into()),
        operation_timeout,
    )
    .await
}

async fn send_message<S>(
    stream: &mut WebSocketStream<S>,
    message: Message,
    operation_timeout: Duration,
) -> Result<(), BitgetTransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    timeout(operation_timeout, stream.send(message))
        .await
        .map_err(|_| BitgetTransportError::Timeout)?
        .map_err(map_websocket)
}

fn map_websocket(error: WebSocketError) -> BitgetTransportError {
    if matches!(error, WebSocketError::Capacity(_)) {
        BitgetTransportError::BodyTooLarge
    } else {
        BitgetTransportError::Disconnected
    }
}

#[derive(Serialize)]
struct LoginFrame<'a> {
    #[serde(rename = "op")]
    operation: &'static str,
    args: [LoginArg<'a>; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginArg<'a> {
    api_key: &'a str,
    passphrase: &'a str,
    timestamp: String,
    sign: &'a str,
}

#[derive(Serialize)]
struct SubscribeFrame {
    #[serde(rename = "op")]
    operation: &'static str,
    args: [SubscriptionArg; 3],
}

#[derive(Clone, Copy, Serialize)]
struct SubscriptionArg {
    #[serde(rename = "instType")]
    instrument_type: &'static str,
    topic: &'static str,
}

#[derive(Deserialize)]
struct EventAck {
    event: String,
    code: Option<String>,
    #[serde(rename = "msg")]
    message: Option<String>,
    #[serde(rename = "arg")]
    argument: EventArgument,
    #[serde(rename = "connId")]
    connection_id: Option<String>,
}

#[derive(Deserialize)]
struct EventArgument {
    #[serde(rename = "instType")]
    instrument_type: String,
    topic: String,
}

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    fn binding(
        mode: GatewayMode,
    ) -> Result<GatewayBinding, Box<dyn std::error::Error + Send + Sync>> {
        Ok(GatewayBinding::new(
            VenueId::Bitget,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)
    }

    fn limits() -> Result<BitgetTransportLimits, BitgetTransportError> {
        BitgetTransportLimits::new(Duration::from_secs(1), 64 * 1024)
    }

    #[tokio::test]
    async fn private_delivery_waits_for_all_three_exact_subscription_acks()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            let mut websocket = accept_async(socket).await?;
            let login = websocket.next().await.ok_or("missing login")??;
            assert!(login.into_text()?.contains("\"op\":\"login\""));
            websocket
                .send(Message::Text(
                    r#"{"event":"login","code":"0","msg":""}"#.into(),
                ))
                .await?;
            let subscribe = websocket.next().await.ok_or("missing subscribe")??;
            assert!(subscribe.into_text()?.contains("\"topic\":\"order\""));
            websocket
                .send(Message::Text(
                    r#"{"action":"snapshot","arg":{"instType":"UTA","topic":"account"},"data":[]}"#
                        .into(),
                ))
                .await?;
            for topic in ["account", "position", "order"] {
                websocket
                    .send(Message::Text(
                        format!(
                            r#"{{"event":"subscribe","arg":{{"instType":"UTA","topic":"{topic}"}},"connId":"connection-1"}}"#
                        )
                        .into(),
                    ))
                    .await?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        let binding = binding(GatewayMode::Test)?;
        let config = BitgetConfig::for_mode(GatewayMode::Test);
        let (stream, _) = tokio_tungstenite::connect_async(&endpoint).await?;
        let mut transport = authenticate_private_stream(
            stream,
            endpoint,
            binding.clone(),
            config,
            &BitgetCredentials::from_values("key", "secret", "pass")?,
            7,
            1_700_000_000_000,
            limits()?,
        )
        .await?;
        assert_eq!(transport.connection_id(), "connection-1");
        let frame = transport.next_frame().await?;
        assert_eq!(frame.binding, binding);
        assert_eq!(frame.generation, 7);
        assert_eq!(frame.topic, "account");
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_or_wrong_topic_ack_fails_closed()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await?;
            let mut websocket = accept_async(socket).await?;
            let _ = websocket.next().await;
            websocket
                .send(Message::Text(
                    r#"{"event":"login","code":"0","msg":""}"#.into(),
                ))
                .await?;
            let _ = websocket.next().await;
            for _ in 0..2 {
                websocket
                    .send(Message::Text(
                        r#"{"event":"subscribe","arg":{"instType":"UTA","topic":"account"},"connId":"c"}"#.into(),
                    ))
                    .await?;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        let binding = binding(GatewayMode::Live)?;
        let config = BitgetConfig::for_mode(GatewayMode::Live);
        let (stream, _) = tokio_tungstenite::connect_async(&endpoint).await?;
        assert!(
            authenticate_private_stream(
                stream,
                endpoint,
                binding,
                config,
                &BitgetCredentials::from_values("key", "secret", "pass")?,
                7,
                1_700_000_000_000,
                limits()?,
            )
            .await
            .is_err()
        );
        server.await??;
        Ok(())
    }

    #[test]
    fn demo_and_live_private_endpoints_never_alias() {
        let demo = BitgetConfig::for_mode(GatewayMode::Test);
        let live = BitgetConfig::for_mode(GatewayMode::Live);
        assert!(demo.private_ws().contains("wspap.bitget.com"));
        assert!(!live.private_ws().contains("wspap"));
        assert_ne!(demo.private_ws(), live.private_ws());
    }
}
