use super::*;
use crate::model::MarketServer;
use venue_gateway_api::display::{Book, Instrument, Quote};
pub(super) async fn ensure_clock(http: &reqwest::Client) -> Result<(), String> {
    if venue_gateway_api::display::clock::needs_refresh() {
        if let Err(error) = venue_gateway_bybit::display::synchronize_display_clock(http).await {
            // A brief time-source outage can use the bounded monotonic holdover.
            venue_gateway_api::display::received_ms().map_err(|_| error)?;
        }
    }
    Ok(())
}
fn now_ms() -> u64 {
    venue_gateway_api::display::received_ms().unwrap_or(0)
}
async fn catalog(server: MarketServer, http: &reqwest::Client) -> Result<Vec<Instrument>, String> {
    ensure_clock(http).await?;
    match server {
        MarketServer::Bybit => venue_gateway_bybit::display::catalog(http).await,
        MarketServer::Bitget => venue_gateway_bitget::display::catalog(http).await,
        MarketServer::Gate => venue_gateway_gate::display::catalog(http).await,
        MarketServer::Okx => venue_gateway_okx::display::catalog(http).await,
        MarketServer::Hyperliquid => venue_gateway_hyperliquid::display::catalog(http).await,
        _ => Err("unsupported display source".into()),
    }
}
async fn candles(
    server: MarketServer,
    http: &reqwest::Client,
    instrument: &Instrument,
    selection: &MarketSelection,
    generation: u64,
    before: Option<u64>,
) -> Result<Vec<PublicBar>, String> {
    ensure_clock(http).await?;
    let ms = selection.interval.duration_ms();
    let now = now_ms();
    match server {
        MarketServer::Bybit => {
            venue_gateway_bybit::display::candles(http, instrument, ms, generation, now, before)
                .await
        }
        MarketServer::Bitget => {
            venue_gateway_bitget::display::candles(http, instrument, ms, generation, now, before)
                .await
        }
        MarketServer::Gate => {
            venue_gateway_gate::display::candles(http, instrument, ms, generation, now, before)
                .await
        }
        MarketServer::Okx => {
            venue_gateway_okx::display::candles(http, instrument, ms, generation, now, before).await
        }
        MarketServer::Hyperliquid => {
            venue_gateway_hyperliquid::display::candles(
                http, instrument, ms, generation, now, before,
            )
            .await
        }
        _ => Err("unsupported display source".into()),
    }
}
async fn book(
    server: MarketServer,
    http: &reqwest::Client,
    instrument: &Instrument,
) -> Result<Book, String> {
    ensure_clock(http).await?;
    match server {
        MarketServer::Bybit => venue_gateway_bybit::display::book(http, instrument).await,
        MarketServer::Bitget => venue_gateway_bitget::display::book(http, instrument).await,
        MarketServer::Gate => venue_gateway_gate::display::book(http, instrument).await,
        MarketServer::Okx => venue_gateway_okx::display::book(http, instrument).await,
        MarketServer::Hyperliquid => {
            venue_gateway_hyperliquid::display::book(http, instrument).await
        }
        _ => Err("unsupported display source".into()),
    }
}
async fn trades(
    server: MarketServer,
    http: &reqwest::Client,
    instrument: &Instrument,
    generation: u64,
) -> Result<Vec<PublicTrade>, String> {
    ensure_clock(http).await?;
    let now = now_ms();
    match server {
        MarketServer::Bybit => {
            venue_gateway_bybit::display::trades(http, instrument, generation, now).await
        }
        MarketServer::Bitget => {
            venue_gateway_bitget::display::trades(http, instrument, generation, now).await
        }
        MarketServer::Gate => {
            venue_gateway_gate::display::trades(http, instrument, generation, now).await
        }
        MarketServer::Okx => {
            venue_gateway_okx::display::trades(http, instrument, generation, now).await
        }
        MarketServer::Hyperliquid => {
            venue_gateway_hyperliquid::display::trades(http, instrument, generation, now).await
        }
        _ => Err("unsupported display source".into()),
    }
}
async fn quotes(
    server: MarketServer,
    http: &reqwest::Client,
    instruments: &[Instrument],
) -> Result<Vec<Quote>, String> {
    ensure_clock(http).await?;
    match server {
        MarketServer::Bybit => venue_gateway_bybit::display::quotes(http, instruments).await,
        MarketServer::Bitget => venue_gateway_bitget::display::quotes(http, instruments).await,
        MarketServer::Gate => venue_gateway_gate::display::quotes(http, instruments).await,
        MarketServer::Okx => venue_gateway_okx::display::quotes(http, instruments).await,
        MarketServer::Hyperliquid => {
            venue_gateway_hyperliquid::display::quotes(http, instruments).await
        }
        _ => Err("unsupported display source".into()),
    }
}
pub(super) async fn run(
    server: MarketServer,
    commands: Receiver<LocalMarketCommand>,
    events: Sender<LocalMarketClientEvent>,
    history: Receiver<crate::market::HistoryRequest>,
) {
    let http = match reqwest::Client::builder()
        .https_only(true)
        .connect_timeout(CONNECT_BUDGET)
        .timeout(CONNECT_BUDGET)
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(http) => http,
        Err(_) => {
            let _ = events.try_send(LocalMarketClientEvent::WorkerFailed(
                "public HTTP client unavailable".into(),
            ));
            return;
        }
    };
    let (tx, mut rx) = mpsc::channel(COMMAND_CAPACITY);
    tokio::task::spawn_blocking(move || {
        while let Ok(command) = commands.recv() {
            let stop = matches!(command, LocalMarketCommand::Stop);
            if tx.blocking_send(command).is_err() || stop {
                break;
            }
        }
    });
    let mut emitter = EventEmitter::new(events.clone());
    let instruments = loop {
        let result = tokio::select! {
            result=catalog(server,&http)=>result,
            command=rx.recv()=>{
                match command {
                    Some(LocalMarketCommand::Stop)|None=>return,
                    Some(command)=>{ // Keep the latest replacement while catalogue I/O is retried.
                        return run_with_pending(server,http,rx,history,emitter,events,command).await;
                    }
                }
            }
        };
        match result {
            Ok(items) => break items,
            Err(error) => {
                let _ = events.try_send(LocalMarketClientEvent::CatalogUnavailable(error));
                tokio::select! {_=tokio::time::sleep(Duration::from_secs(5))=>{},command=rx.recv()=>{if let Some(command)=command{return run_with_pending(server,http,rx,history,emitter,events,command).await;}return;}}
            }
        }
    };
    publish_catalog(&instruments, &events);
    subscription_loop(
        server,
        &http,
        &instruments,
        &mut rx,
        &history,
        &mut emitter,
        None,
    )
    .await;
}
async fn run_with_pending(
    server: MarketServer,
    http: reqwest::Client,
    mut rx: mpsc::Receiver<LocalMarketCommand>,
    history: Receiver<crate::market::HistoryRequest>,
    mut emitter: EventEmitter,
    events: Sender<LocalMarketClientEvent>,
    mut pending: LocalMarketCommand,
) {
    let instruments = loop {
        let result = tokio::select! {
            result=catalog(server,&http)=>result,
            command=rx.recv()=>{match command{Some(LocalMarketCommand::Stop)|None=>return,Some(c)=>{pending=c;continue;}}}
        };
        match result {
            Ok(items) => break items,
            Err(error) => {
                let _ = events.try_send(LocalMarketClientEvent::CatalogUnavailable(error));
                tokio::select! {_=tokio::time::sleep(Duration::from_secs(5))=>{},command=rx.recv()=>{match command{Some(LocalMarketCommand::Stop)|None=>return,Some(c)=>pending=c}}}
            }
        }
    };
    publish_catalog(&instruments, &events);
    subscription_loop(
        server,
        &http,
        &instruments,
        &mut rx,
        &history,
        &mut emitter,
        Some(pending),
    )
    .await;
}
fn publish_catalog(instruments: &[Instrument], events: &Sender<LocalMarketClientEvent>) {
    let _ = events.try_send(LocalMarketClientEvent::Catalog(
        instruments
            .iter()
            .map(|i| MarketInstrument {
                symbol: i.symbol.to_string(),
                price_scale: i.price_scale,
                quantity_scale: i.quantity_scale,
            })
            .collect(),
    ));
}
async fn subscription_loop(
    server: MarketServer,
    http: &reqwest::Client,
    instruments: &[Instrument],
    commands: &mut mpsc::Receiver<LocalMarketCommand>,
    history: &Receiver<crate::market::HistoryRequest>,
    emitter: &mut EventEmitter,
    mut pending: Option<LocalMarketCommand>,
) {
    loop {
        let command = match pending.take() {
            Some(c) => c,
            None => match commands.recv().await {
                Some(c) => c,
                None => return,
            },
        };
        let LocalMarketCommand::Replace {
            generation,
            selections,
        } = command
        else {
            return;
        };
        let mut initialized = BTreeSet::new();
        let mut seen = std::collections::VecDeque::new();
        loop {
            let work = async {
                match quotes(server, http, instruments).await {
                    Ok(quotes) => emitter.emit(LocalMarketClientEvent::Quotes(
                        quotes
                            .into_iter()
                            .map(|q| MarketQuote {
                                symbol: q.symbol.to_string(),
                                last: q.last,
                                change_percent_24h: q.change_percent,
                                quote_volume_24h: q.quote_volume,
                                exchange_time_ms: q.time_ms,
                                received_ms: now_ms(),
                            })
                            .collect(),
                    ))?,
                    Err(e) => {
                        emitter.emit(LocalMarketClientEvent::QuotesUnavailable(e))?;
                    }
                }
                for selection in &selections {
                    let Some(instrument) = instruments
                        .iter()
                        .find(|i| i.symbol == selection.binding.symbol)
                    else {
                        emitter.status_all(
                            generation,
                            std::slice::from_ref(selection),
                            MarketStatus::Offline,
                            Some("Market not listed on selected exchange".into()),
                        )?;
                        continue;
                    };
                    let result = refresh(
                        server,
                        http,
                        instrument,
                        selection,
                        generation,
                        &mut initialized,
                        &mut seen,
                        emitter,
                    )
                    .await;
                    if let Err(error) = result {
                        initialized.remove(selection);
                        emitter.status_all(
                            generation,
                            std::slice::from_ref(selection),
                            MarketStatus::Offline,
                            Some(error),
                        )?;
                    }
                }
                while let Ok(request) = history.try_recv() {
                    let result = if request.generation != generation {
                        Err("expired history request".into())
                    } else if let Some(i) = instruments
                        .iter()
                        .find(|i| i.symbol == request.selection.binding.symbol)
                    {
                        candles(
                            server,
                            http,
                            i,
                            &request.selection,
                            generation,
                            Some(request.before),
                        )
                        .await
                        .map(|bars| {
                            bars.into_iter()
                                .filter(|b| b.close_time_ms < now_ms())
                                .collect()
                        })
                    } else {
                        Err("market not listed".into())
                    };
                    emitter.emit(LocalMarketClientEvent::History { request, result })?;
                }
                Ok::<(), String>(())
            };
            tokio::select! {
                command=commands.recv()=>{pending=command;break;},
                result=work=>{if result.is_err(){return;}}
            }
            tokio::select! {
                command=commands.recv()=>{pending=command;break;},
                _=tokio::time::sleep(Duration::from_secs(2))=>{}
            }
        }
        if pending.is_none() {
            return;
        }
    }
}
async fn refresh(
    server: MarketServer,
    http: &reqwest::Client,
    instrument: &Instrument,
    selection: &MarketSelection,
    generation: u64,
    initialized: &mut BTreeSet<MarketSelection>,
    seen: &mut std::collections::VecDeque<String>,
    emitter: &mut EventEmitter,
) -> Result<(), String> {
    let (bars, book, prints) = tokio::join!(
        candles(server, http, instrument, selection, generation, None),
        book(server, http, instrument),
        trades(server, http, instrument, generation)
    );
    let received = now_ms();
    let send = |emitter: &mut EventEmitter, payload, event_time_ms| {
        emitter.emit(LocalMarketClientEvent::Market(Box::new(MarketEnvelope {
            generation,
            selection: selection.clone(),
            received_ms: received,
            event_time_ms,
            payload,
        })))
    };
    let bars = bars?;
    if !initialized.contains(selection) {
        send(
            emitter,
            MarketPayload::RestHistory {
                bars: bars
                    .iter()
                    .filter(|b| b.close_time_ms < received)
                    .cloned()
                    .collect(),
            },
            received,
        )?;
        initialized.insert(selection.clone());
    }
    // Replaying a bounded candle tail also closes bars after a disconnected polling interval.
    for bar in bars
        .into_iter()
        .rev()
        .take(2)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        let closed = bar.close_time_ms < received;
        let event_time = bar.close_time_ms.min(received);
        send(
            emitter,
            MarketPayload::WsBar {
                bar: ui_bar_from_closed(&bar)?,
                study_bar: Box::new(bar),
                closed,
            },
            event_time,
        )?;
    }
    let book = book?;
    if book.time_ms > received || received.saturating_sub(book.time_ms) > 15_000 {
        return Err("public book timestamp is stale or in the future".into());
    }
    let levels = |values: Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>| {
        values
            .into_iter()
            .map(|(price, quantity)| UiBookLevel { price, quantity })
            .collect()
    };
    send(
        emitter,
        MarketPayload::BookSnapshot {
            bids: levels(book.bids),
            asks: levels(book.asks),
        },
        book.time_ms,
    )?;
    for print in prints? {
        let key = format!(
            "{:?}:{}:{}",
            selection.interval, instrument.symbol, print.aggregate_trade_id
        );
        if seen.contains(&key) || print.transaction_time_ms > received {
            continue;
        }
        seen.push_back(key);
        while seen.len() > 1600 {
            seen.pop_front();
        }
        let time = print.transaction_time_ms;
        send(emitter, MarketPayload::Trade(ui_trade(print)), time)?;
    }
    emitter.status_all(
        generation,
        std::slice::from_ref(selection),
        MarketStatus::Live,
        Some("REST snapshots · 2s minimum refresh · recent trades only".into()),
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires deployed public HTTPS relay; reads public market data only"]
    async fn live_multi_venue_display() -> Result<(), String> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|e| e.to_string())?;
        let mut failures = Vec::new();
        for server in MarketServer::ALL
            .into_iter()
            .filter(|s| *s != MarketServer::Binance)
            .filter(|s| std::env::var("VENUE_DISPLAY_TEST_VENUE").map_or(true, |v| v == s.label()))
        {
            let result=async {
                let instruments=catalog(server,&http).await.map_err(|e|format!("catalogue: {e}"))?;
                let instrument=instruments.iter().find(|i|i.symbol.base()=="BTC").ok_or("missing BTC")?;
                let quotes=quotes(server,&http,&instruments).await.map_err(|e|format!("quotes: {e}"))?;
                if quotes.is_empty(){return Err("empty quotes".into());}
                let book=book(server,&http,instrument).await.map_err(|e|format!("book: {e}"))?;
                if book.bids.is_empty() || book.asks.is_empty() {return Err("empty book".into());}
                let prints=trades(server,&http,instrument,1).await.map_err(|e|format!("trades: {e}"))?;
                if prints.is_empty() || prints.iter().any(|t|!t.is_valid()){return Err("invalid or empty trades".into());}
                for interval in ChartInterval::ALL {
                    let selection=MarketSelection::for_server(server,&instrument.symbol.to_string(),interval).map_err(|e|e.to_string())?;
                    let bars=candles(server,&http,instrument,&selection,1,None).await.map_err(|e|format!("{interval:?} candles: {e}"))?;
                    if bars.len()<2 || bars.iter().any(|b|!b.is_valid()){return Err("invalid or empty candles".into());}
                    let before=bars.first().ok_or("empty history")?.open_time_ms;
                    let page=candles(server,&http,instrument,&selection,1,Some(before)).await.map_err(|e|format!("{interval:?} history: {e}"))?;
                    if page.is_empty() || page.iter().any(|b|b.open_time_ms>=before){return Err("invalid backward history".into());}
                }
                println!("{server:?}: catalogue={}, quotes={}, book={}/{}, trades={}, all 6 intervals and history pages OK",instruments.len(),quotes.len(),book.bids.len(),book.asks.len(),prints.len());
                Ok::<(),String>(())
            }.await;
            if let Err(e) = result {
                failures.push(format!("{server:?}: {e}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures.join("; "))
        }
    }
}
