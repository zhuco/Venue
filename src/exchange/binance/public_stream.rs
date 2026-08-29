use std::{net::TcpStream, time::Duration};

use tungstenite::{Message, WebSocket, stream::MaybeTlsStream};

use crate::domain::Symbol;

use super::{PublicError, connect_binance_stream, native_symbol, websocket_error};

const PUBLIC_STREAM_BASE_URL: &str = "wss://fstream.binance.com/public/ws";
const MARKET_STREAM_BASE_URL: &str = "wss://fstream.binance.com/market/ws";
// The resident polls four public sockets serially. Keep an idle stream from consuming a large
// share of the turn budget, otherwise 100ms depth and aggregate-trade frames accumulate until
// Binance closes the backpressured connection before the 1m feature window can become Ready.
const PUBLIC_READINESS_TIMEOUT: Duration = Duration::from_millis(1);
const KLINE_DRAIN_TIMEOUT: Duration = Duration::from_millis(1);
const MAX_OPEN_KLINES_PER_EFFECT: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicStream {
    DiffDepth,
    AggTrade,
    Kline1m,
    BookTicker,
    MarkFunding,
}

pub struct PublicStreamSocket {
    socket: WebSocket<MaybeTlsStream<TcpStream>>,
}

impl PublicStreamSocket {
    pub fn connect(symbol: &Symbol, stream: PublicStream) -> Result<Self, PublicError> {
        let url = public_stream_url(symbol, stream);
        let socket = connect_binance_stream(url.as_str()).map_err(websocket_error)?;
        let mut stream = Self { socket };
        stream.set_read_timeout(PUBLIC_READINESS_TIMEOUT)?;
        Ok(stream)
    }

    pub fn set_read_timeout(&mut self, timeout: Duration) -> Result<(), PublicError> {
        let result = match self.socket.get_mut() {
            MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(timeout)),
            MaybeTlsStream::Rustls(stream) => stream.sock.set_read_timeout(Some(timeout)),
            _ => {
                return Err(PublicError::WebSocket(Box::new(tungstenite::Error::Io(
                    std::io::Error::other("unsupported public stream"),
                ))));
            }
        };
        result.map_err(|source| PublicError::WebSocket(Box::new(tungstenite::Error::Io(source))))
    }

    /// Reads exactly one socket frame. A timeout, Ping/Pong, or non-text frame returns readiness
    /// without consuming another frame; callers must schedule the next turn explicitly.
    pub fn next_text_when_ready(&mut self) -> Result<Option<String>, PublicError> {
        match self.socket.read() {
            Ok(Message::Text(text)) => Ok(Some(text.to_string())),
            Ok(Message::Ping(payload)) => {
                self.socket
                    .send(Message::Pong(payload))
                    .map_err(websocket_error)?;
                Ok(None)
            }
            Ok(Message::Close(_)) => Err(PublicError::Closed),
            Ok(Message::Binary(_) | Message::Pong(_) | Message::Frame(_)) => Ok(None),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(websocket_error(error)),
        }
    }

    /// Drains a bounded sequence of text frames while preserving their socket order. Aggregate
    /// trades use this path and remain individually validated and journaled by the worker.
    pub fn next_text_batch_when_ready(
        &mut self,
        max_frames: usize,
    ) -> Result<Vec<String>, PublicError> {
        if max_frames == 0 {
            return Ok(Vec::new());
        }
        let Some(first) = self.next_text_when_ready()? else {
            return Ok(Vec::new());
        };
        let mut payloads = Vec::with_capacity(max_frames.min(32));
        payloads.push(first);
        for _ in 1..max_frames {
            match self.next_text_when_ready()? {
                Some(payload) => payloads.push(payload),
                None => break,
            }
        }
        Ok(payloads)
    }

    /// Coalesces only schema-valid open-kline updates. A closed or malformed payload is returned
    /// immediately so the worker can persist/validate it; the bounded drain cannot hide faults.
    pub fn next_kline_text_when_ready(&mut self) -> Result<Option<String>, PublicError> {
        let Some(first) = self.next_text_when_ready()? else {
            return Ok(None);
        };
        if !is_clearly_open_kline(&first) {
            return Ok(Some(first));
        }
        self.set_read_timeout(KLINE_DRAIN_TIMEOUT)?;
        let result = (|| {
            let mut latest_open = first;
            for _ in 1..MAX_OPEN_KLINES_PER_EFFECT {
                match self.next_text_when_ready()? {
                    Some(payload) if is_clearly_open_kline(&payload) => latest_open = payload,
                    Some(payload) => return Ok(Some(payload)),
                    None => break,
                }
            }
            Ok(Some(latest_open))
        })();
        let restored = self.set_read_timeout(PUBLIC_READINESS_TIMEOUT);
        match (result, restored) {
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            (Ok(payload), Ok(())) => Ok(payload),
        }
    }

    pub fn next_text(&mut self) -> Result<String, PublicError> {
        loop {
            match self.socket.read().map_err(websocket_error)? {
                Message::Text(text) => return Ok(text.to_string()),
                Message::Ping(payload) => self
                    .socket
                    .send(Message::Pong(payload))
                    .map_err(websocket_error)?,
                Message::Close(_) => return Err(PublicError::Closed),
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

fn is_clearly_open_kline(payload: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| value.get("k")?.get("x")?.as_bool())
        == Some(false)
}

pub fn depth_stream_url(symbol: &Symbol) -> String {
    public_stream_url(symbol, PublicStream::DiffDepth)
}

pub fn public_stream_url(symbol: &Symbol, stream: PublicStream) -> String {
    let native = native_symbol(symbol).to_ascii_lowercase();
    let (base_url, suffix) = match stream {
        PublicStream::DiffDepth => (PUBLIC_STREAM_BASE_URL, "depth@100ms"),
        PublicStream::BookTicker => (PUBLIC_STREAM_BASE_URL, "bookTicker"),
        PublicStream::AggTrade => (MARKET_STREAM_BASE_URL, "aggTrade"),
        PublicStream::Kline1m => (MARKET_STREAM_BASE_URL, "kline_1m"),
        PublicStream::MarkFunding => (MARKET_STREAM_BASE_URL, "markPrice@1s"),
    };
    format!("{base_url}/{native}@{suffix}")
}

#[cfg(test)]
mod tests {
    use super::is_clearly_open_kline;

    #[test]
    fn only_explicit_open_kline_payloads_are_safe_to_coalesce() {
        assert!(is_clearly_open_kline(r#"{"k":{"x":false}}"#));
        assert!(!is_clearly_open_kline(r#"{"k":{"x":true}}"#));
        assert!(!is_clearly_open_kline(r#"{"k":{}}"#));
        assert!(!is_clearly_open_kline("not-json"));
    }
}
