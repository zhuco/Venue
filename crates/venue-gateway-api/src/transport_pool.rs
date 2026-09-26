//! Shared transport resources for the five synchronous account adapters. No credentials,
//! signatures, request bodies, account facts or dispatch authority enter these caches.
use std::{sync::OnceLock, time::Duration};

pub fn account_runtime() -> Result<&'static tokio::runtime::Runtime, &'static str> {
    static RUNTIME: OnceLock<Result<tokio::runtime::Runtime, std::io::Error>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("venue-account-transport")
                .enable_all()
                .build()
        })
        .as_ref()
        .map_err(|_| "account transport runtime unavailable")
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct ClientKey {
    connect_timeout: Duration,
    request_timeout: Option<Duration>,
    no_proxy: bool,
    no_retry: bool,
}

pub fn account_http_client(
    connect_timeout: Duration,
    request_timeout: Option<Duration>,
    no_proxy: bool,
    no_retry: bool,
) -> Result<reqwest::Client, reqwest::Error> {
    static CLIENTS: OnceLock<parking_lot::Mutex<Vec<(ClientKey, reqwest::Client)>>> =
        OnceLock::new();
    let key = ClientKey {
        connect_timeout,
        request_timeout,
        no_proxy,
        no_retry,
    };
    let mut clients = CLIENTS.get_or_init(Default::default).lock();
    if let Some((_, client)) = clients.iter().find(|(existing, _)| *existing == key) {
        return Ok(client.clone());
    }
    let mut builder = reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(32);
    if let Some(timeout) = request_timeout {
        builder = builder.timeout(timeout);
    }
    if no_proxy {
        builder = builder.no_proxy();
    }
    if no_retry {
        builder = builder.retry(reqwest::retry::never());
    }
    let client = builder.build()?;
    if clients.len() < 16 {
        clients.push((key, client.clone()));
    }
    Ok(client)
}

#[cfg(test)]
mod tests {
    #[test]
    fn account_construction_reuses_one_runtime() {
        let first = super::account_runtime().ok();
        let second = super::account_runtime().ok();
        assert!(first.is_some());
        assert!(first.zip(second).is_some_and(|(a, b)| std::ptr::eq(a, b)));
    }
}
