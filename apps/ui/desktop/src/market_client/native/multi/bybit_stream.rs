use super::*;
use venue_gateway_bybit::display::stream::{Decoder, ENDPOINT, Frame, PING};

pub(super) async fn run(
    http: &reqwest::Client,
    instruments: &[Instrument],
    generation: u64,
    selections: Vec<MarketSelection>,
    commands: &mut mpsc::Receiver<LocalMarketCommand>,
    history: &Receiver<crate::market::HistoryRequest>,
    shared_history: &Receiver<crate::market::SharedHistoryRequest>,
    source_demands: &tokio::sync::watch::Receiver<SourceDemands>,
    events: MarketSender,
) -> Option<LocalMarketCommand> {
    let mut demand_changes = source_demands.clone();
    demand_changes.borrow_and_update();
    let mut workers = tokio::task::JoinSet::new();
    let mut derivative_scopes = std::collections::BTreeMap::<venue_gateway_api::PublicMarketBinding, Vec<MarketSelection>>::new();
    for selection in &selections {
        derivative_scopes.entry(selection.binding.clone()).or_default().push(selection.clone());
    }
    let mut owned_derivatives = BTreeSet::new();
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
        let derivative_destinations = if owned_derivatives.insert(selection.binding.clone()) {
            derivative_scopes.get(&selection.binding).cloned().unwrap_or_default()
        } else { Vec::new() };
        let source_demand = source_demands.borrow().get(&selection.binding).copied()
            .unwrap_or(SharedSourceDemand { minute: true, day: true });
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
                let result = stream(&http, &instrument, &selection, generation,
                    &derivative_destinations, source_demand, &mut emitter).await;
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
                        None,
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
    let shared_history = shared_history.clone();
    let shared_http = http.clone();
    let shared_instruments = instruments.to_vec();
    let shared_events = events.clone();
    let shared_demands = source_demands.clone();
    workers.spawn(async move {
        loop {
            if let Ok(request) = shared_history.try_recv() {
                let demanded = source_is_requested(&shared_demands.borrow(),
                    &request.binding, request.interval);
                let result = if request.generation != generation || !matches!(request.interval,
                    ChartInterval::OneMinute | ChartInterval::OneDay) {
                    Err("expired or invalid shared history".into())
                } else if !demanded {
                    Err("shared source has no consumer".into())
                } else if let Some(instrument) = shared_instruments.iter()
                    .find(|instrument| instrument.symbol == request.binding.symbol) {
                    let selection = MarketSelection { binding: request.binding.clone(), interval: request.interval };
                    tokio::select! {
                        result = candles_inner(MarketServer::Bybit, &shared_http, instrument, &selection,
                            generation, Some(request.before), request.gap_after) => result
                            .map(|bars| bars.into_iter().filter(|bar| bar.close_time_ms < now_ms()).collect()),
                        _ = wait_until_source_disabled(shared_demands.clone(), request.binding.clone(),
                            request.interval) => Err("shared source has no consumer".into()),
                    }
                } else { Err("market not listed".into()) };
                if shared_events.send_timeout(LocalMarketClientEvent::SharedHistory { request, result },
                    COMMAND_SEND_TIMEOUT).is_err() { return; }
            } else { tokio::time::sleep(Duration::from_millis(100)).await; }
        }
    });
    for (binding, destinations) in derivative_scopes {
        let Some(instrument) = instruments.iter().find(|instrument| instrument.symbol == binding.symbol).cloned() else { continue; };
        let http = http.clone();
        let events = events.clone();
        workers.spawn(async move {
            loop {
                let now = now_ms();
                let cache = public_cache::PublicHistoryCache::local();
                let cached = cache.as_ref().and_then(|cache| cache.interest(&binding, generation));
                let result = if let Some(samples) = cached.as_ref().filter(|samples|
                    public_cache::PublicHistoryCache::interest_fresh(samples, now)) {
                    Ok(samples.clone())
                } else {
                    let since = cached.as_ref().and_then(|samples| samples.last())
                        .map(|sample| sample.exchange_time_ms);
                    venue_gateway_bybit::display::open_interest_history(&http,
                        &instrument, generation, now, since).await.and_then(|fresh| {
                            if fresh.is_empty() { return Err("Bybit OI history has no completed samples".into()); }
                            Ok(fresh)
                        }).map(|fresh| {
                            cache.as_ref().and_then(|cache|
                                cache.remember_interest(&binding, generation, fresh.clone()))
                                .unwrap_or(fresh)
                        })
                };
                let (payload, event_time_ms, received_ms, pause) = match result {
                    Ok(samples) => {
                        let time = samples.last().map_or(now, |sample| sample.exchange_time_ms);
                        let received = samples.last().map_or(now, |sample| sample.received_at_ms);
                        (MarketPayload::OpenInterestHistory(samples), time, received, 300_000)
                    }
                    Err(error) => (MarketPayload::OpenInterestHistoryUnavailable(format!("Bybit OI history: {error}")),
                        now, now, 60_000),
                };
                for selection in &destinations {
                    if events.send_timeout(LocalMarketClientEvent::Market(Box::new(MarketEnvelope {
                        generation, selection: selection.clone(), event_time_ms, received_ms,
                        payload: payload.clone(),
                    })), COMMAND_SEND_TIMEOUT).is_err() { return; }
                }
                tokio::time::sleep(Duration::from_millis(pause)).await;
            }
        });
    }
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
        changed = demand_changes.changed() => changed.ok().map(|()|
            LocalMarketCommand::Replace { generation, selections: selections.clone() }),
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
    derivative_destinations: &[MarketSelection],
    source_demand: SharedSourceDemand,
    emitter: &mut EventEmitter,
) -> Result<(), String> {
    ensure_clock(http).await?;
    let shared_owner = !derivative_destinations.is_empty();
    let mut decoder = Decoder::new_with_sources(
        instrument.clone(),
        selection.interval.duration_ms(),
        generation,
        shared_owner,
        shared_owner && source_demand.minute,
        shared_owner && source_demand.day,
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
    let initial = initial_visible_history(MarketServer::Bybit, http, instrument, selection,
        generation, bars.iter().filter(|bar| bar.close_time_ms < received)
            .cloned().collect()).await;
    emit(
        emitter,
        selection,
        generation,
        MarketPayload::RestHistory {
            bars: initial,
        },
        received,
    )?;
    if !derivative_destinations.is_empty() {
        seed_shared(http, instrument, selection, generation, &bars, source_demand, emitter).await;
    }
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
                Frame::Derivatives { funding, interest } => {
                    if let Some(funding) = funding {
                        let time = funding.exchange_time_ms;
                        for destination in derivative_destinations {
                            emit(emitter, destination, generation, MarketPayload::Funding(funding.clone()), time)?;
                        }
                    }
                    if let Some(interest) = interest {
                        let time = interest.exchange_time_ms;
                        for destination in derivative_destinations {
                            emit(emitter, destination, generation,
                                MarketPayload::OpenInterestCurrent(interest.clone()), time)?;
                        }
                    }
                }
                Frame::BaseMinuteBar { bar, closed } => {
                    if source_demand.minute {
                        emitter.emit(LocalMarketClientEvent::BaseMinuteBar { generation,
                            binding: selection.binding.clone(), bar, confirmed: closed })?;
                    }
                }
                Frame::SessionDayBar { bar, closed } => {
                    if source_demand.day {
                        emitter.emit(LocalMarketClientEvent::SessionDayBar { generation,
                            binding: selection.binding.clone(), bar, confirmed: closed })?;
                    }
                }
            }
        }
    }
}

async fn seed_shared(http: &reqwest::Client, instrument: &Instrument,
    selection: &MarketSelection, generation: u64, display: &[PublicBar],
    source_demand: SharedSourceDemand, emitter: &mut EventEmitter) {
    for interval in [ChartInterval::OneMinute, ChartInterval::OneDay] {
        if !(if interval == ChartInterval::OneMinute { source_demand.minute } else { source_demand.day }) {
            continue;
        }
        let source_selection = MarketSelection { binding: selection.binding.clone(), interval };
        let source = if selection.interval == interval { display.to_vec() }
            else { match candles(MarketServer::Bybit, http, instrument, &source_selection,
                generation, None).await { Ok(bars) => bars, Err(_) => continue } };
        let cached = public_cache::PublicHistoryCache::local()
            .and_then(|cache| cache.recent(&source_selection, generation));
        let source = history::merge_latest(cached, source, generation);
        let source = if interval == ChartInterval::OneMinute { source.into_iter().rev().take(360)
            .collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>() } else { source };
        let now = now_ms();
        let forming = source.last().filter(|bar| bar.close_time_ms >= now).cloned();
        let closed = source.into_iter().filter(|bar| bar.close_time_ms < now).collect();
        let event = if interval == ChartInterval::OneMinute {
            LocalMarketClientEvent::BaseMinuteHistory { generation,
                binding: selection.binding.clone(), bars: closed, forming }
        } else {
            LocalMarketClientEvent::SessionDayHistory { generation,
                binding: selection.binding.clone(), bars: closed, forming }
        };
        if emitter.emit(event).is_err() { return; }
    }
}

fn emit(
    emitter: &mut EventEmitter,
    selection: &MarketSelection,
    generation: u64,
    payload: MarketPayload,
    event_time_ms: u64,
) -> Result<(), String> {
    let received_ms = match &payload {
        MarketPayload::Funding(funding) => funding.received_at_ms,
        MarketPayload::OpenInterestCurrent(sample) => sample.received_at_ms,
        _ => now_ms(),
    };
    emitter.emit(LocalMarketClientEvent::Market(Box::new(MarketEnvelope {
        generation,
        selection: selection.clone(),
        received_ms,
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
