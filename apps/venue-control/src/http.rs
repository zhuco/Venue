//! Strict localhost HTTP/1.1 and SSE adapter for the transport-neutral control service.
//!
//! HTTP success exposes only query data or a durable control inbox receipt; it never grants an
//! account node mutation authority.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{self, MissedTickBehavior},
};
use venue_control_protocol::ControlCommandRequest;

use crate::{ControlRepository, ControlService, RepositoryError, ServiceError};

const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_HEADER_LINES: usize = 64;
const MAX_HEADER_LINE_BYTES: usize = 1_024;
const DEFAULT_REQUEST_BODY_LIMIT: usize = 64 * 1024;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_EVENT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_EVENT_KEEP_ALIVE: Duration = Duration::from_secs(15);
const DEFAULT_EVENT_PAGE_LIMIT: u32 = 128;

#[derive(Clone, Debug)]
pub struct ControlHttpConfig {
    pub request_body_limit: usize,
    pub request_timeout: Duration,
    pub event_poll_interval: Duration,
    pub event_keep_alive: Duration,
    pub event_page_limit: u32,
}

impl Default for ControlHttpConfig {
    fn default() -> Self {
        Self {
            request_body_limit: DEFAULT_REQUEST_BODY_LIMIT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            event_poll_interval: DEFAULT_EVENT_POLL_INTERVAL,
            event_keep_alive: DEFAULT_EVENT_KEEP_ALIVE,
            event_page_limit: DEFAULT_EVENT_PAGE_LIMIT,
        }
    }
}

impl ControlHttpConfig {
    fn validate(&self) -> Result<(), HttpServerError> {
        if self.request_body_limit == 0
            || self.request_timeout.is_zero()
            || self.event_poll_interval.is_zero()
            || self.event_keep_alive.is_zero()
            || !(1..=1_000).contains(&self.event_page_limit)
        {
            return Err(HttpServerError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HttpServerError {
    #[error("control HTTP configuration contains an invalid bound")]
    InvalidConfig,
    #[error("control HTTP server failed: {0}")]
    Serve(#[from] std::io::Error),
}

struct HttpState<R> {
    service: Arc<ControlService<R>>,
    config: ControlHttpConfig,
    shutdown: watch::Receiver<bool>,
}

impl<R> Clone for HttpState<R> {
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
            config: self.config.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

struct HttpRequest {
    method: Method,
    target: String,
    last_event_id: Option<i64>,
    body: Vec<u8>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Method {
    Get,
    Post,
}

#[derive(Clone, Copy)]
enum HttpError {
    BadRequest,
    PayloadTooLarge,
    Timeout,
    Unavailable,
    Conflict,
    Internal,
}

#[must_use]
pub fn control_shutdown_channel() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

pub async fn serve_local<R>(
    listener: TcpListener,
    service: Arc<ControlService<R>>,
    config: ControlHttpConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), HttpServerError>
where
    R: ControlRepository + 'static,
{
    config.validate()?;
    let state = HttpState {
        service,
        config,
        shutdown: shutdown.clone(),
    };
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                tokio::spawn(handle_connection(stream, state.clone()));
            }
        }
    }
}

async fn handle_connection<R>(mut stream: TcpStream, state: HttpState<R>)
where
    R: ControlRepository + 'static,
{
    let request = match time::timeout(
        state.config.request_timeout,
        read_request(&mut stream, state.config.request_body_limit),
    )
    .await
    {
        Ok(Ok(request)) => request,
        Ok(Err(error)) => {
            let _ = write_error(&mut stream, error).await;
            return;
        }
        Err(_) => {
            let _ = write_error(&mut stream, HttpError::Timeout).await;
            return;
        }
    };
    let _ = dispatch(&mut stream, &state, request).await;
}

async fn dispatch<R>(
    stream: &mut TcpStream,
    state: &HttpState<R>,
    request: HttpRequest,
) -> Result<(), ()>
where
    R: ControlRepository + 'static,
{
    let Some((path, query)) = split_target(&request.target) else {
        return write_error(stream, HttpError::BadRequest)
            .await
            .map_err(|_| ());
    };
    match (request.method, path) {
        (Method::Get, "/v2/ui/snapshot") if query.is_none() => {
            let snapshot = match call(state, state.service.snapshot()).await {
                Ok(snapshot) => snapshot,
                Err(error) => return write_error(stream, error).await.map_err(|_| ()),
            };
            let body = serde_json::to_vec(&snapshot).map_err(|_| ())?;
            write_response(stream, "200 OK", "application/json", "close", &body)
                .await
                .map_err(|_| ())
        }
        (Method::Post, "/v2/control/commands") if query.is_none() => {
            let command = match serde_json::from_slice::<ControlCommandRequest>(&request.body) {
                Ok(command) => command,
                Err(_) => {
                    return write_error(stream, HttpError::BadRequest)
                        .await
                        .map_err(|_| ());
                }
            };
            let observed_ms = match now_ms() {
                Ok(observed_ms) => observed_ms,
                Err(error) => return write_error(stream, error).await.map_err(|_| ()),
            };
            let receipt =
                match call(state, state.service.submit_command(&command, observed_ms)).await {
                    Ok(receipt) => receipt,
                    Err(error) => return write_error(stream, error).await.map_err(|_| ()),
                };
            let body = serde_json::to_vec(&receipt).map_err(|_| ())?;
            write_response(stream, "200 OK", "application/json", "close", &body)
                .await
                .map_err(|_| ())
        }
        (Method::Get, "/v2/ui/events") => {
            let cursor = match event_cursor(query, request.last_event_id) {
                Ok(cursor) => cursor,
                Err(error) => return write_error(stream, error).await.map_err(|_| ()),
            };
            write_sse_headers(stream).await.map_err(|_| ())?;
            stream_events(stream, state, cursor).await;
            Ok(())
        }
        _ => write_error(stream, HttpError::BadRequest)
            .await
            .map_err(|_| ()),
    }
}

fn split_target(target: &str) -> Option<(&str, Option<&str>)> {
    let (path, query) = target
        .split_once('?')
        .map_or((target, None), |(path, query)| (path, Some(query)));
    (!path.is_empty() && path.starts_with('/')).then_some((path, query))
}

fn event_cursor(query: Option<&str>, last_event_id: Option<i64>) -> Result<i64, HttpError> {
    let query_cursor = match query {
        None => None,
        Some(value) => Some(
            value
                .strip_prefix("after=")
                .ok_or(HttpError::BadRequest)?
                .parse::<i64>()
                .map_err(|_| HttpError::BadRequest)?,
        ),
    };
    let cursor = match (query_cursor, last_event_id) {
        (Some(query), Some(header)) if query != header => return Err(HttpError::BadRequest),
        (Some(query), _) => query,
        (_, Some(header)) => header,
        (None, None) => 0,
    };
    (cursor >= 0).then_some(cursor).ok_or(HttpError::BadRequest)
}

async fn read_request(stream: &mut TcpStream, body_limit: usize) -> Result<HttpRequest, HttpError> {
    let mut bytes = Vec::with_capacity(2_048);
    let header_end = loop {
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() >= MAX_HEADER_BYTES {
            return Err(HttpError::PayloadTooLarge);
        }
        let mut chunk = [0_u8; 1_024];
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|_| HttpError::BadRequest)?;
        if count == 0 {
            return Err(HttpError::BadRequest);
        }
        bytes.extend_from_slice(&chunk[..count]);
    };
    let header =
        std::str::from_utf8(&bytes[..header_end - 4]).map_err(|_| HttpError::BadRequest)?;
    let mut lines = header.split("\r\n");
    let request_line = lines.next().ok_or(HttpError::BadRequest)?;
    let mut parts = request_line.split(' ');
    let (method, target) = match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("GET"), Some(target), Some("HTTP/1.1"), None) => (Method::Get, target.to_owned()),
        (Some("POST"), Some(target), Some("HTTP/1.1"), None) => (Method::Post, target.to_owned()),
        _ => return Err(HttpError::BadRequest),
    };
    let mut content_length = None;
    let mut last_event_id = None;
    for (index, line) in lines.enumerate() {
        if index >= MAX_HEADER_LINES || line.len() > MAX_HEADER_LINE_BYTES {
            return Err(HttpError::PayloadTooLarge);
        }
        let (name, value) = line.split_once(':').ok_or(HttpError::BadRequest)?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length")
            && content_length
                .replace(value.parse::<usize>().map_err(|_| HttpError::BadRequest)?)
                .is_some()
        {
            return Err(HttpError::BadRequest);
        }
        if name.eq_ignore_ascii_case("last-event-id")
            && last_event_id
                .replace(value.parse::<i64>().map_err(|_| HttpError::BadRequest)?)
                .is_some()
        {
            return Err(HttpError::BadRequest);
        }
        if name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("upgrade")
            || name.eq_ignore_ascii_case("expect")
        {
            return Err(HttpError::BadRequest);
        }
    }
    let content_length = content_length.unwrap_or(0);
    if content_length > body_limit || (method == Method::Get && content_length != 0) {
        return Err(HttpError::PayloadTooLarge);
    }
    let mut body = bytes[header_end..].to_vec();
    if body.len() > content_length {
        return Err(HttpError::BadRequest);
    }
    while body.len() < content_length {
        let remaining = content_length - body.len();
        let mut chunk = vec![0_u8; remaining.min(1_024)];
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|_| HttpError::BadRequest)?;
        if count == 0 {
            return Err(HttpError::BadRequest);
        }
        body.extend_from_slice(&chunk[..count]);
    }
    Ok(HttpRequest {
        method,
        target,
        last_event_id,
        body,
    })
}

async fn call<R, T>(
    state: &HttpState<R>,
    operation: impl std::future::Future<Output = Result<T, ServiceError>>,
) -> Result<T, HttpError> {
    match time::timeout(state.config.request_timeout, operation).await {
        Ok(result) => result.map_err(map_service_error),
        Err(_) => Err(HttpError::Timeout),
    }
}

async fn stream_events<R>(stream: &mut TcpStream, state: &HttpState<R>, mut cursor: i64)
where
    R: ControlRepository + 'static,
{
    let mut shutdown = state.shutdown.clone();
    let mut poll = time::interval(state.config.event_poll_interval);
    let mut keep_alive = time::interval(state.config.event_keep_alive);
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    keep_alive.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
            _ = keep_alive.tick() => if write_sse(stream, b": keep-alive\n\n", state.config.request_timeout).await.is_err() { return; },
            _ = poll.tick() => {
                let events = match call(state, state.service.events(cursor, state.config.event_page_limit)).await { Ok(events) => events, Err(_) => return };
                for event in events {
                    let payload = match serde_json::to_string(&event.event) { Ok(payload) if payload.len() <= state.config.request_body_limit => payload, _ => return };
                    let frame = format!("id: {}\nevent: control\ndata: {payload}\n\n", event.sequence);
                    if write_sse(stream, frame.as_bytes(), state.config.request_timeout).await.is_err() { return; }
                    cursor = event.sequence;
                }
            }
        }
    }
}

async fn write_sse(
    writer: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
    timeout: Duration,
) -> Result<(), ()> {
    time::timeout(timeout, writer.write_all(bytes))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;
    writer.flush().await.map_err(|_| ())
}

async fn write_sse_headers(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n").await
}

async fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    connection: &str,
    body: &[u8],
) -> Result<(), std::io::Error> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: {connection}\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await
}

async fn write_error(stream: &mut TcpStream, error: HttpError) -> Result<(), std::io::Error> {
    let (status, code) = match error {
        HttpError::BadRequest => ("400 Bad Request", "invalid_request"),
        HttpError::PayloadTooLarge => ("413 Payload Too Large", "payload_too_large"),
        HttpError::Timeout => ("504 Gateway Timeout", "request_timeout"),
        HttpError::Unavailable => ("503 Service Unavailable", "service_unavailable"),
        HttpError::Conflict => ("409 Conflict", "command_conflict"),
        HttpError::Internal => ("500 Internal Server Error", "internal_error"),
    };
    let body = format!("{{\"error\":\"{code}\"}}");
    write_response(stream, status, "application/json", "close", body.as_bytes()).await
}

fn map_service_error(error: ServiceError) -> HttpError {
    match error {
        ServiceError::SnapshotUnavailable | ServiceError::Repository(RepositoryError::Database) => {
            HttpError::Unavailable
        }
        ServiceError::StaleOrMismatchedScope
        | ServiceError::Repository(RepositoryError::ReplayConflict) => HttpError::Conflict,
        ServiceError::Protocol(_)
        | ServiceError::InvalidObservedTime
        | ServiceError::InvalidLimit
        | ServiceError::InvalidDelivery(_) => HttpError::BadRequest,
        ServiceError::Repository(_) => HttpError::Internal,
    }
}

fn now_ms() -> Result<u64, HttpError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| HttpError::Unavailable)?
        .as_millis()
        .try_into()
        .map_err(|_| HttpError::Unavailable)
}

#[cfg(test)]
mod tests;
