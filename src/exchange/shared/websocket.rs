use std::{
    env,
    io::{Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::Url;
use tungstenite::{
    WebSocket,
    handshake::client::{Request, Response},
    stream::MaybeTlsStream,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PROXY_RESPONSE_BYTES: usize = 16 * 1024;

pub(crate) fn connect_tls(
    request: Request,
) -> Result<(WebSocket<MaybeTlsStream<TcpStream>>, Response), WebSocketTransportError> {
    let deadline = ConnectDeadline::new();
    match https_proxy()? {
        Some(proxy) => {
            let stream = proxy_tunnel(
                &proxy,
                request
                    .uri()
                    .host()
                    .ok_or(WebSocketTransportError::Endpoint)?,
                &deadline,
            )?;
            tls_handshake(request, stream, &deadline)
        }
        None => direct_tls(request, &deadline),
    }
}

#[derive(Clone, Copy)]
struct ConnectDeadline {
    expires_at: Instant,
}

impl ConnectDeadline {
    fn new() -> Self {
        Self {
            expires_at: Instant::now() + CONNECT_TIMEOUT,
        }
    }

    fn remaining(self) -> Option<Duration> {
        self.expires_at
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
    }
}

/// `tungstenite::connect` leaves the TCP phase unbounded. A private stream must never hold the
/// sole Stage-7 control loop forever while its exchange endpoint is unreachable, so establish
/// both TCP and TLS under the same finite transport boundary.
fn direct_tls(
    request: Request,
    deadline: &ConnectDeadline,
) -> Result<(WebSocket<MaybeTlsStream<TcpStream>>, Response), WebSocketTransportError> {
    let host = request
        .uri()
        .host()
        .ok_or(WebSocketTransportError::Endpoint)?
        .to_owned();
    let port = request.uri().port_u16().unwrap_or(443);
    let addresses = resolve_addresses(host, port, *deadline)
        .ok_or(WebSocketTransportError::DirectConnection)?;
    let stream =
        connect_resolved(addresses, *deadline).ok_or(WebSocketTransportError::DirectConnection)?;
    set_connect_timeouts(
        &stream,
        deadline
            .remaining()
            .ok_or(WebSocketTransportError::DirectConnection)?,
        WebSocketTransportError::DirectConnection,
    )?;
    tls_handshake(request, stream, deadline)
}

fn tls_handshake(
    request: Request,
    stream: TcpStream,
    deadline: &ConnectDeadline,
) -> Result<(WebSocket<MaybeTlsStream<TcpStream>>, Response), WebSocketTransportError> {
    set_connect_timeouts(
        &stream,
        deadline
            .remaining()
            .ok_or(WebSocketTransportError::Handshake)?,
        WebSocketTransportError::Handshake,
    )?;
    let result = finish_before_deadline("venue-ws-tls", *deadline, move || {
        tungstenite::client_tls(request, stream).map_err(|_| WebSocketTransportError::Handshake)
    })
    .ok_or(WebSocketTransportError::Handshake)?;
    let (mut socket, response) = result?;
    clear_connect_timeouts(socket.get_mut())?;
    Ok((socket, response))
}

fn clear_connect_timeouts(
    stream: &mut MaybeTlsStream<TcpStream>,
) -> Result<(), WebSocketTransportError> {
    let result = match stream {
        MaybeTlsStream::Plain(stream) => stream
            .set_read_timeout(None)
            .and_then(|()| stream.set_write_timeout(None)),
        MaybeTlsStream::Rustls(stream) => stream
            .sock
            .set_read_timeout(None)
            .and_then(|()| stream.sock.set_write_timeout(None)),
        _ => return Err(WebSocketTransportError::Handshake),
    };
    result.map_err(|_| WebSocketTransportError::Handshake)
}

struct Proxy {
    host: String,
    port: u16,
    authorization: Option<String>,
}

fn https_proxy() -> Result<Option<Proxy>, WebSocketTransportError> {
    let value = env::var("HTTPS_PROXY")
        .or_else(|_| env::var("https_proxy"))
        .or_else(|_| env::var("ALL_PROXY"))
        .or_else(|_| env::var("all_proxy"));
    let Ok(value) = value else {
        return Ok(None);
    };
    let url = Url::parse(&value).map_err(|_| WebSocketTransportError::ProxyConfiguration)?;
    if url.scheme() != "http" {
        return Err(WebSocketTransportError::ProxyConfiguration);
    }
    let host = url
        .host_str()
        .ok_or(WebSocketTransportError::ProxyConfiguration)?
        .to_owned();
    let port = url
        .port_or_known_default()
        .ok_or(WebSocketTransportError::ProxyConfiguration)?;
    let authorization = if url.username().is_empty() {
        None
    } else {
        Some(STANDARD.encode(format!(
            "{}:{}",
            url.username(),
            url.password().unwrap_or_default()
        )))
    };
    Ok(Some(Proxy {
        host,
        port,
        authorization,
    }))
}

fn proxy_tunnel(
    proxy: &Proxy,
    target_host: &str,
    deadline: &ConnectDeadline,
) -> Result<TcpStream, WebSocketTransportError> {
    let addresses = resolve_addresses(proxy.host.clone(), proxy.port, *deadline)
        .ok_or(WebSocketTransportError::ProxyConnection)?;
    let mut stream =
        connect_resolved(addresses, *deadline).ok_or(WebSocketTransportError::ProxyConnection)?;
    set_connect_timeouts(
        &stream,
        deadline
            .remaining()
            .ok_or(WebSocketTransportError::ProxyConnection)?,
        WebSocketTransportError::ProxyConnection,
    )?;
    let authorization = proxy
        .authorization
        .as_ref()
        .map(|encoded| format!("Proxy-Authorization: Basic {encoded}\r\n"))
        .unwrap_or_default();
    let target_host = target_host.to_owned();
    finish_before_deadline("venue-ws-proxy", *deadline, move || {
        write!(
            stream,
            "CONNECT {target_host}:443 HTTP/1.1\r\nHost: {target_host}:443\r\nProxy-Connection: Keep-Alive\r\n{authorization}\r\n"
        )
        .map_err(|_| WebSocketTransportError::ProxyConnection)?;
        stream
            .flush()
            .map_err(|_| WebSocketTransportError::ProxyConnection)?;
        let response = read_proxy_response(&mut stream)?;
        let status = response
            .split_whitespace()
            .nth(1)
            .ok_or(WebSocketTransportError::ProxyResponse)?;
        if status != "200" {
            return Err(WebSocketTransportError::ProxyResponse);
        }
        Ok(stream)
    })
    .ok_or(WebSocketTransportError::ProxyConnection)?
}

fn resolve_addresses(
    host: String,
    port: u16,
    deadline: ConnectDeadline,
) -> Option<Vec<SocketAddr>> {
    finish_before_deadline("venue-ws-dns", deadline, move || {
        (host.as_str(), port)
            .to_socket_addrs()
            .ok()
            .map(|addresses| addresses.collect::<Vec<_>>())
    })
    .flatten()
}

fn finish_before_deadline<T: Send + 'static>(
    thread_name: &str,
    deadline: ConnectDeadline,
    task: impl FnOnce() -> T + Send + 'static,
) -> Option<T> {
    let _ = deadline.remaining()?;
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name(thread_name.to_owned())
        .spawn(move || {
            let _ = sender.send(task());
        })
        .ok()?;
    receiver.recv_timeout(deadline.remaining()?).ok()
}

fn connect_resolved(addresses: Vec<SocketAddr>, deadline: ConnectDeadline) -> Option<TcpStream> {
    first_connected(addresses, deadline, |address, timeout| {
        TcpStream::connect_timeout(&address, timeout).ok()
    })
}

fn first_connected<T>(
    addresses: Vec<SocketAddr>,
    deadline: ConnectDeadline,
    mut connect: impl FnMut(SocketAddr, Duration) -> Option<T>,
) -> Option<T> {
    addresses.into_iter().find_map(|address| {
        let remaining = deadline.remaining()?;
        connect(address, remaining)
    })
}

fn set_connect_timeouts(
    stream: &TcpStream,
    timeout: Duration,
    error: WebSocketTransportError,
) -> Result<(), WebSocketTransportError> {
    stream
        .set_read_timeout(Some(timeout))
        .and_then(|()| stream.set_write_timeout(Some(timeout)))
        .map_err(|_| error)
}

fn read_proxy_response(stream: &mut TcpStream) -> Result<String, WebSocketTransportError> {
    let mut response = Vec::new();
    while !response.ends_with(b"\r\n\r\n") {
        if response.len() >= MAX_PROXY_RESPONSE_BYTES {
            return Err(WebSocketTransportError::ProxyResponse);
        }
        let mut byte = [0_u8; 1];
        stream
            .read_exact(&mut byte)
            .map_err(|_| WebSocketTransportError::ProxyResponse)?;
        response.push(byte[0]);
    }
    String::from_utf8(response).map_err(|_| WebSocketTransportError::ProxyResponse)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WebSocketTransportError {
    #[error("WebSocket endpoint is invalid")]
    Endpoint,
    #[error("HTTPS proxy configuration is unsupported")]
    ProxyConfiguration,
    #[error("HTTPS proxy connection failed")]
    ProxyConnection,
    #[error("HTTPS proxy refused the WebSocket tunnel")]
    ProxyResponse,
    #[error("WebSocket TLS or upgrade handshake failed")]
    Handshake,
    #[error("direct WebSocket connection failed")]
    DirectConnection,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_addresses_share_one_decreasing_connect_budget() {
        let addresses = vec![
            SocketAddr::from(([127, 0, 0, 1], 1)),
            SocketAddr::from(([127, 0, 0, 1], 2)),
        ];
        let deadline = ConnectDeadline {
            expires_at: Instant::now() + Duration::from_secs(1),
        };
        let mut budgets = Vec::new();
        let connected = first_connected(addresses, deadline, |address, timeout| {
            budgets.push(timeout);
            if address.port() == 2 { Some(()) } else { None }
        });

        assert_eq!(connected, Some(()));
        assert_eq!(budgets.len(), 2);
        assert!(budgets[1] <= budgets[0]);
    }

    #[test]
    fn expired_deadline_attempts_no_address() {
        let deadline = ConnectDeadline {
            expires_at: Instant::now(),
        };
        let mut attempts = 0;
        let connected = first_connected(
            vec![SocketAddr::from(([127, 0, 0, 1], 1))],
            deadline,
            |_, _| {
                attempts += 1;
                Some(())
            },
        );

        assert_eq!(connected, None);
        assert_eq!(attempts, 0);
    }

    #[test]
    fn blocking_phase_cannot_outlive_the_shared_deadline() {
        let deadline = ConnectDeadline {
            expires_at: Instant::now() + Duration::from_millis(10),
        };
        let result = finish_before_deadline("venue-ws-deadline-test", deadline, || {
            thread::sleep(Duration::from_millis(100));
            7_u8
        });

        assert_eq!(result, None);
    }
}
