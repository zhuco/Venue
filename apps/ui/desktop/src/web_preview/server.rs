use super::{Series, Snapshot};
use crate::{
    market::{LocalMarketStore, MarketSelection},
    market_client::{LocalMarketClient, LocalMarketClientEvent},
    model::{MarketInstrument, MarketQuote, MarketServer},
};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use venue_control_protocol::MarketSummary;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const ADDRESS: &str = "127.0.0.1:8877";

/// This local development host has no Control proxy, login route or mutation route.
pub fn serve(directory: &Path) -> Result<()> {
    let directory = directory.canonicalize()?;
    if !directory.join("venueflow.js").is_file() || !directory.join("venueflow_bg.wasm").is_file() {
        return Err("build the WASM bundle before starting the preview".into());
    }
    let listener = TcpListener::bind(ADDRESS)?;
    listener.set_nonblocking(true)?;
    let client = Arc::new(LocalMarketClient::start_with_context(
        MarketServer::Binance,
        None,
    )?);
    let state = Arc::new(parking_lot::Mutex::new(PublicState::default()));
    let connections = Arc::new(AtomicUsize::new(0));
    println!("VenueFlow browser preview: http://{ADDRESS} (public market only)");
    loop {
        for event in client.drain(1024) {
            state.lock().apply(event);
        }
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                stream.set_write_timeout(Some(Duration::from_secs(10)))?;
                // Idle browser preconnections must not block market reads on another socket.
                if connections.load(Ordering::Relaxed) >= 8 {
                    continue;
                }
                connections.fetch_add(1, Ordering::Relaxed);
                let active = connections.clone();
                let directory = directory.clone();
                let client = client.clone();
                let state = state.clone();
                let spawned = std::thread::Builder::new()
                    .name("web-preview-http".into())
                    .spawn(move || {
                        if let Err(error) = handle(&mut stream, &directory, &client, &state) {
                            if error.downcast_ref::<std::io::Error>().is_none() {
                                let _ = respond(
                                    &mut stream,
                                    400,
                                    "text/plain",
                                    b"Preview request unavailable",
                                );
                            }
                        }
                        active.fetch_sub(1, Ordering::Relaxed);
                    });
                if spawned.is_err() {
                    connections.fetch_sub(1, Ordering::Relaxed);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10))
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[derive(Default)]
struct PublicState {
    store: LocalMarketStore,
    instruments: BTreeMap<String, MarketInstrument>,
    quotes: BTreeMap<String, MarketQuote>,
}
impl PublicState {
    fn apply(&mut self, event: LocalMarketClientEvent) {
        match event {
            LocalMarketClientEvent::Market(envelope) => {
                let _ = self.store.apply(*envelope);
            }
            LocalMarketClientEvent::Catalog(items) => {
                self.instruments = items
                    .into_iter()
                    .map(|item| (item.symbol.clone(), item))
                    .collect();
            }
            LocalMarketClientEvent::Quotes(items) => {
                for item in items {
                    self.quotes.insert(item.symbol.clone(), item);
                }
            }
            LocalMarketClientEvent::WorkerFailed(_) => {
                let _ = self.store.replace([]);
                self.quotes.clear();
            }
            _ => {}
        }
    }
    fn snapshot(
        &mut self,
        selections: Vec<(String, crate::chart::ChartInterval)>,
        client: &LocalMarketClient,
    ) -> Result<Snapshot> {
        if selections.is_empty() || selections.len() > 8 {
            return Err("invalid selection count".into());
        }
        let requested = selections
            .iter()
            .map(|(symbol, interval)| MarketSelection::binance_usd_m(symbol, *interval))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if let Some(generation) = self.store.replace(requested.clone())? {
            client.replace_subscriptions(generation, requested.clone())?;
        }
        let now = venue_gateway_api::display::received_ms()?;
        self.store.refresh_staleness(now, 5_000);
        let series = requested
            .iter()
            .filter_map(|selection| {
                let view = self.store.view(selection)?;
                let symbol = selection.binding.symbol.to_string();
                let instrument = self.instruments.get(&symbol);
                let market =
                    view.last
                        .zip(view.bid)
                        .zip(view.ask)
                        .and_then(|((last, bid), ask)| {
                            let quote = self.quotes.get(&symbol)?;
                            Some(MarketSummary {
                                symbol: selection.binding.symbol.clone(),
                                last,
                                bid,
                                ask,
                                change_percent_24h: quote.change_percent_24h,
                                bars: Vec::new(),
                                bids: view.bids.clone(),
                                asks: view.asks.clone(),
                                trades: view.trades.clone(),
                                indicators: Vec::new(),
                            })
                        });
                Some(Series {
                    symbol,
                    interval: selection.interval,
                    bars: view.bars.clone(),
                    market,
                    status: format!("{:?}", view.status),
                    price_scale: instrument.map_or(8, |item| item.price_scale as usize),
                    quantity_scale: instrument.map_or(8, |item| item.quantity_scale as usize),
                })
            })
            .collect();
        Ok(Snapshot {
            selections,
            symbols: self.instruments.keys().cloned().collect(),
            series,
        })
    }
}

fn handle(
    stream: &mut TcpStream,
    directory: &Path,
    client: &LocalMarketClient,
    state: &parking_lot::Mutex<PublicState>,
) -> Result<()> {
    let mut request = Vec::new();
    let mut buffer = [0u8; 2048];
    while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
        let size = stream.read(&mut buffer)?;
        if size == 0 || request.len() + size > 8192 {
            return Err("invalid headers".into());
        }
        request.extend_from_slice(&buffer[..size]);
    }
    let request = std::str::from_utf8(&request)?;
    let mut lines = request.split("\r\n");
    let mut first = lines.next().ok_or("missing request")?.split_whitespace();
    let method = first.next().ok_or("missing method")?;
    let target = first.next().ok_or("missing target")?;
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .collect::<Vec<_>>();
    let host = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("host"))
        .map(|(_, value)| value.trim());
    if host != Some(ADDRESS)
        || headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("origin") && value.trim() != format!("http://{ADDRESS}")
        })
    {
        return respond(stream, 403, "text/plain", b"Invalid preview origin");
    }
    if method != "GET" {
        return respond(stream, 405, "text/plain", b"Read-only preview");
    }
    let url = reqwest::Url::parse(&format!("http://{ADDRESS}{target}"))?;
    if url.path() == "/preview/markets" {
        let query = url
            .query_pairs()
            .find(|(name, _)| name == "selections")
            .ok_or("missing selections")?
            .1;
        let selections = serde_json::from_str(&query)?;
        return match state.lock().snapshot(selections, client) {
            Ok(snapshot) => respond(
                stream,
                200,
                "application/json",
                &serde_json::to_vec(&snapshot)?,
            ),
            Err(_) => respond(
                stream,
                503,
                "text/plain",
                b"Waiting for public market connection",
            ),
        };
    }
    let (file, mime) = match url.path() {
        "/" | "/index.html" => ("index.html", "text/html; charset=utf-8"),
        "/venueflow.js" => ("venueflow.js", "text/javascript"),
        "/venueflow_bg.wasm" => ("venueflow_bg.wasm", "application/wasm"),
        "/preview-bootstrap.js" => ("preview-bootstrap.js", "text/javascript"),
        "/preview-font.ttc" => ("preview-font.ttc", "font/collection"),
        _ => {
            return respond(
                stream,
                404,
                "text/plain",
                b"Not available in the public preview",
            );
        }
    };
    respond(stream, 200, mime, &std::fs::read(directory.join(file))?)
}

fn respond(stream: &mut TcpStream, status: u16, mime: &str, body: &[u8]) -> Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status} Response\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    Ok(())
}
