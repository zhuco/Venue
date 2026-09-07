use super::*;
use venue_gateway_bybit::display::stream::{Decoder, ENDPOINT, Frame, PING};

pub(super) async fn run(
    http: &reqwest::Client,
    instruments: &[Instrument],
    generation: u64,
    selections: Vec<MarketSelection>,
    commands: &mut mpsc::Receiver<LocalMarketCommand>,
    history: &Receiver<crate::market::HistoryRequest>,
    events: MarketSender,
) -> Option<LocalMarketCommand> {
    let mut workers = tokio::task::JoinSet::new();
    for selection in selections.iter().cloned() {
        let Some(instrument) = instruments
            .iter()
            .find(|i| i.symbol == selection.binding.symbol)
            .cloned()
        else {
            continue;
        };
        let http = http.clone();
        let events = events.clone();
        workers.spawn(async move {
            let mut emitter = EventEmitter::new(events);
            loop {
                if emitter
                    .status_all(
                        generation,
                        std::slice::from_ref(&selection),
                        MarketStatus::Connecting,
                        None,
                    )
                    .is_err()
                {
                    return;
                }
                let result = stream(&http, &instrument, &selection, generation, &mut emitter).await;
                if let Err(error) = result {
                    if emitter
                        .status_all(
                            generation,
                            std::slice::from_ref(&selection),
                            MarketStatus::Resyncing,
                            Some(error),
                        )
                        .is_err()
                    {
                        return;
                    }
                    // REST remains a visible fallback; its latency does not block other panes.
                    let _ = refresh(
                        MarketServer::Bybit,
                        &http,
                        &instrument,
                        &selection,
                        generation,
                        &mut BTreeSet::new(),
                        &mut std::collections::VecDeque::new(),
                        &mut emitter,
                    )
                    .await;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
    let http_quotes = http.clone();
    let quote_instruments = instruments.to_vec();
    let quote_events = events.clone();
    workers.spawn(async move {
        loop {
            if let Ok(rows) = quotes(MarketServer::Bybit, &http_quotes, &quote_instruments).await {
                let rows = rows
                    .into_iter()
                    .map(|q| MarketQuote {
                        symbol: q.symbol.to_string(),
                        last: q.last,
                        change_percent_24h: q.change_percent,
                        quote_volume_24h: q.quote_volume,
                        exchange_time_ms: q.time_ms,
                        received_ms: now_ms(),
                    })
                    .collect();
                if quote_events
                    .try_send(LocalMarketClientEvent::Quotes(rows))
                    .is_err()
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    let history = history.clone();
    let history_http = http.clone();
    let history_instruments = instruments.to_vec();
    workers.spawn(async move {
        loop {
            if let Ok(request) = history.try_recv() {
                let result = if request.generation != generation {
                    Err("expired history request".into())
                } else if let Some(instrument) = history_instruments
                    .iter()
                    .find(|i| i.symbol == request.selection.binding.symbol)
                {
                    candles(
                        MarketServer::Bybit,
                        &history_http,
                        instrument,
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
                if events
                    .try_send(LocalMarketClientEvent::History { request, result })
                    .is_err()
                {
                    return;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    });
    // Dropping the set cancels sockets, history reads and timers together on every selection change.
    tokio::select! {
        command = commands.recv() => command,
        _ = workers.join_next() => {
            tokio::select! {
                command = commands.recv() => command,
                _ = tokio::time::sleep(Duration::from_millis(250)) => Some(LocalMarketCommand::Replace { generation, selections }),
            }
        }
    }
}

async fn stream(
    http: &reqwest::Client,
    instrument: &Instrument,
    selection: &MarketSelection,
    generation: u64,
    emitter: &mut EventEmitter,
) -> Result<(), String> {
    ensure_clock(http).await?;
    let mut decoder = Decoder::new(
        instrument.clone(),
        selection.interval.duration_ms(),
        generation,
    )?;
    let host = reqwest::Url::parse(ENDPOINT)
        .map_err(|_| "invalid public endpoint")?
        .host_str()
        .ok_or("missing public host")?
        .to_owned();
    let proxy = ProxySetting::from_environment(&host);
    let mut socket = timeout(
        CONNECT_BUDGET,
        connect_public_websocket(ENDPOINT, websocket_config(), &proxy),
    )
    .await
    .map_err(|_| "Bybit public connect timed out")??;
    timeout(
        Duration::from_secs(2),
        socket.send(Message::Text(decoder.subscription()?.into())),
    )
    .await
    .map_err(|_| "Bybit subscribe timed out")?
    .map_err(|_| "Bybit public subscribe failed")?;
    let bars = candles(
        MarketServer::Bybit,
        http,
        instrument,
        selection,
        generation,
        None,
    )
    .await?;
    let received = now_ms();
    emit(
        emitter,
        selection,
        generation,
        MarketPayload::RestHistory {
            bars: bars
                .iter()
                .filter(|b| b.close_time_ms < received)
                .cloned()
                .collect(),
        },
        received,
    )?;
    for bar in bars.into_iter().filter(|b| b.close_time_ms >= received) {
        emit(
            emitter,
            selection,
            generation,
            MarketPayload::WsBar {
                bar: ui_bar_from_closed(&bar)?,
                study_bar: Box::new(bar),
                closed: false,
            },
            received,
        )?;
    }
    let mut ping = tokio::time::interval(Duration::from_secs(20));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_frame = Instant::now();
    let mut live = false;
    loop {
        let message = tokio::select! {
            message = socket.next() => message.ok_or("Bybit public stream closed")?.map_err(|_| "Bybit public read failed")?,
            _ = ping.tick() => {
                if last_frame.elapsed() > Duration::from_secs(40) { return Err("Bybit public heartbeat expired".into()); }
                timeout(Duration::from_secs(2),socket.send(Message::Text(PING.into()))).await.map_err(|_| "Bybit ping timed out")?.map_err(|_| "Bybit ping failed")?;
                continue;
            }
        };
        last_frame = Instant::now();
        let payload = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => {
                String::from_utf8(bytes.to_vec()).map_err(|_| "invalid Bybit text")?
            }
            Message::Ping(bytes) => {
                socket
                    .send(Message::Pong(bytes))
                    .await
                    .map_err(|_| "Bybit pong failed")?;
                continue;
            }
            Message::Close(_) => return Err("Bybit public stream closed".into()),
            _ => continue,
        };
        for frame in decoder.parse(&payload, now_ms())? {
            match frame {
                Frame::Book(book) => {
                    let levels = |rows: Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>| {
                        rows.into_iter()
                            .map(|(price, quantity)| UiBookLevel { price, quantity })
                            .collect()
                    };
                    emit(
                        emitter,
                        selection,
                        generation,
                        MarketPayload::BookSnapshot {
                            bids: levels(book.bids),
                            asks: levels(book.asks),
                        },
                        book.time_ms,
                    )?;
                    if !live {
                        emitter.status_all(
                            generation,
                            std::slice::from_ref(selection),
                            MarketStatus::Live,
                            Some("WebSocket live public market".into()),
                        )?;
                        live = true;
                    }
                }
                Frame::Trades(trades) => {
                    for trade in trades {
                        let time = trade.exchange_time_ms;
                        emit(
                            emitter,
                            selection,
                            generation,
                            MarketPayload::Trade(ui_trade(trade)),
                            time,
                        )?;
                    }
                }
                Frame::Bar {
                    bar,
                    closed,
                    event_time_ms,
                } => emit(
                    emitter,
                    selection,
                    generation,
                    MarketPayload::WsBar {
                        bar: ui_bar_from_closed(&bar)?,
                        study_bar: Box::new(bar),
                        closed,
                    },
                    event_time_ms,
                )?,
            }
        }
    }
}

fn emit(
    emitter: &mut EventEmitter,
    selection: &MarketSelection,
    generation: u64,
    payload: MarketPayload,
    event_time_ms: u64,
) -> Result<(), String> {
    emitter.emit(LocalMarketClientEvent::Market(Box::new(MarketEnvelope {
        generation,
        selection: selection.clone(),
        received_ms: now_ms(),
        event_time_ms,
        payload,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "reads only live Bybit public markets for 50 seconds; no credentials or orders"]
    fn live_bybit_display_stream() -> Result<(), String> {
        let selections = ["BTC/USDT", "DOGE/USDT"]
            .into_iter()
            .map(|symbol| {
                MarketSelection::for_server(MarketServer::Bybit, symbol, ChartInterval::OneMinute)
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let client = LocalMarketClient::start_with_context(MarketServer::Bybit, None)
            .map_err(|e| e.to_string())?;
        client
            .replace_subscriptions(1, selections.clone())
            .map_err(|e| e.to_string())?;
        let mut live = BTreeSet::new();
        let mut reducers = selections
            .into_iter()
            .map(|s| crate::market::LocalMarketReducer::new(s).map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let (mut books, mut trades, mut bars) = (0, 0, 0);
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(50) {
            for event in client.drain(10_000) {
                if let LocalMarketClientEvent::Market(event) = event {
                    match &event.payload {
                        MarketPayload::Status {
                            status: MarketStatus::Live,
                            detail: Some(detail),
                        } if detail.starts_with("WebSocket") => {
                            live.insert(event.selection.binding.symbol.to_string());
                        }
                        MarketPayload::BookSnapshot { .. } => books += 1,
                        MarketPayload::Trade(_) => trades += 1,
                        MarketPayload::WsBar { .. } => bars += 1,
                        MarketPayload::Status {
                            detail: Some(detail),
                            ..
                        } => eprintln!("Bybit status: {detail}"),
                        _ => {}
                    }
                    if let Some(reducer) = reducers
                        .iter_mut()
                        .find(|r| r.view().selection == event.selection)
                    {
                        reducer.apply(*event).map_err(|e| e.to_string())?;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        println!("Bybit WebSocket: live={live:?}, books={books}, trades={trades}, candles={bars}");
        if live.len() != 2 || books < 100 || trades == 0 || bars == 0 {
            return Err("Bybit WebSocket display did not become live".into());
        }
        let stopped = Instant::now();
        drop(client);
        if stopped.elapsed() > Duration::from_secs(2) {
            return Err("Bybit subscription cancellation stalled".into());
        }
        Ok(())
    }
}
