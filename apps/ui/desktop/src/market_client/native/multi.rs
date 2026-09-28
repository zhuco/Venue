use super::*;
mod bybit_stream;
use crate::model::MarketServer;
use venue_gateway_api::PublicMarketBinding;
use venue_gateway_api::display::{Book, Instrument, Quote};
pub(super) async fn ensure_clock(http: &reqwest::Client) -> Result<(), String> {
    // Refresh before the five-minute expiry, without a per-minute request or
    // simultaneous history/chart workers issuing duplicate time requests.
    static LAST_ATTEMPT: tokio::sync::Mutex<Option<(Instant, bool)>> =
        tokio::sync::Mutex::const_new(None);
    let mut last = LAST_ATTEMPT.lock().await;
    if last
        .as_ref()
        .is_none_or(|(at, ok)| at.elapsed() >= Duration::from_secs(if *ok { 270 } else { 15 }))
    {
        if let Err(error) = venue_gateway_bybit::display::synchronize_display_clock(http).await {
            *last = Some((Instant::now(), false));
            venue_gateway_api::display::received_ms().map_err(|_| error)?;
        } else {
            *last = Some((Instant::now(), true));
        }
    }
    venue_gateway_api::display::received_ms()?;
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
    candles_inner(
        server, http, instrument, selection, generation, before, None,
    )
    .await
}

async fn candles_inner(
    server: MarketServer,
    http: &reqwest::Client,
    instrument: &Instrument,
    selection: &MarketSelection,
    generation: u64,
    before: Option<u64>,
    gap_after: Option<u64>,
) -> Result<Vec<PublicBar>, String> {
    if let Some(before) = before
        && let Some(bars) = public_cache::PublicHistoryCache::local()
            .and_then(|cache| cache.page(selection, before, generation))
        && crate::market::history_page_covers_gap(&bars, before, gap_after)
    {
        return Ok(bars);
    }
    ensure_clock(http).await?;
    let ms = selection.interval.duration_ms();
    let now = now_ms();
    let cache = public_cache::PublicHistoryCache::local();
    let recent_last = if before.is_none() {
        cache
            .as_ref()
            .and_then(|cache| cache.recent_last_open(selection, generation))
    } else {
        None
    };
    let limit = if before.is_some() {
        200
    } else {
        recent_last
            .map(|last| {
                let current_open = now - now % ms;
                usize::try_from(current_open.saturating_sub(last) / ms)
                    .unwrap_or(200)
                    .saturating_add(1)
                    .clamp(2, 200)
            })
            .unwrap_or(INITIAL_VISIBLE_HISTORY_LIMIT)
    };
    let bars = match server {
        MarketServer::Bybit => {
            venue_gateway_bybit::display::candles(
                http, instrument, ms, generation, now, before, limit,
            )
            .await
        }
        MarketServer::Bitget => {
            venue_gateway_bitget::display::candles(
                http, instrument, ms, generation, now, before, limit,
            )
            .await
        }
        MarketServer::Gate => {
            venue_gateway_gate::display::candles(
                http, instrument, ms, generation, now, before, limit,
            )
            .await
        }
        MarketServer::Okx => {
            venue_gateway_okx::display::candles(
                http, instrument, ms, generation, now, before, limit,
            )
            .await
        }
        MarketServer::Hyperliquid => {
            venue_gateway_hyperliquid::display::candles(
                http, instrument, ms, generation, now, before, limit,
            )
            .await
        }
        _ => Err("unsupported display source".into()),
    }?;
    if let Some(cache) = cache {
        let closed = bars
            .iter()
            .filter(|bar| bar.close_time_ms < now)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(before) = before {
            cache.remember_page(selection, before, &closed);
        } else {
            cache.remember_recent(selection, &closed);
        }
    }
    Ok(bars)
}

async fn initial_visible_history(
    server: MarketServer,
    http: &reqwest::Client,
    instrument: &Instrument,
    selection: &MarketSelection,
    generation: u64,
    fresh: Vec<PublicBar>,
) -> Vec<PublicBar> {
    let cached = public_cache::PublicHistoryCache::local()
        .and_then(|cache| cache.recent(selection, generation));
    let mut bars = history::merge_latest(cached, fresh, generation);
    if bars.len() < INITIAL_VISIBLE_HISTORY_LIMIT
        && let Some(first) = bars.first().map(|bar| bar.open_time_ms)
        && first > 0
    {
        if let Ok(older) =
            candles(server, http, instrument, selection, generation, Some(first)).await
        {
            bars = history::merge_latest(Some(older), bars, generation);
        }
    }
    bars.into_iter()
        .rev()
        .take(INITIAL_VISIBLE_HISTORY_LIMIT)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect()
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
    events: MarketSender,
    history: Receiver<crate::market::HistoryRequest>,
    shared_history: Receiver<crate::market::SharedHistoryRequest>,
    source_demands: tokio::sync::watch::Receiver<SourceDemands>,
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
                        return run_with_pending(server,http,rx,history,shared_history,source_demands,emitter,events,command).await;
                    }
                }
            }
        };
        match result {
            Ok(items) => break items,
            Err(error) => {
                let _ = events.try_send(LocalMarketClientEvent::CatalogUnavailable(error));
                tokio::select! {_=tokio::time::sleep(Duration::from_secs(5))=>{},command=rx.recv()=>{if let Some(command)=command{return run_with_pending(server,http,rx,history,shared_history,source_demands,emitter,events,command).await;}return;}}
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
        &shared_history,
        &source_demands,
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
    shared_history: Receiver<crate::market::SharedHistoryRequest>,
    source_demands: tokio::sync::watch::Receiver<SourceDemands>,
    mut emitter: EventEmitter,
    events: MarketSender,
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
        &shared_history,
        &source_demands,
        &mut emitter,
        Some(pending),
    )
    .await;
}
fn publish_catalog(instruments: &[Instrument], events: &MarketSender) {
    let _ = events.try_send(LocalMarketClientEvent::Catalog(
        instruments
            .iter()
            .map(|i| MarketInstrument {
                symbol: i.symbol.to_string(),
                price_tick: i.price_tick,
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
    shared_history: &Receiver<crate::market::SharedHistoryRequest>,
    source_demands: &tokio::sync::watch::Receiver<SourceDemands>,
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
        if server == MarketServer::Bybit {
            pending = bybit_stream::run(
                http,
                instruments,
                generation,
                selections,
                commands,
                history,
                shared_history,
                source_demands,
                emitter.events.clone(),
            )
            .await;
            if pending.is_none() {
                return;
            }
            continue;
        }
        let mut shared_workers = tokio::task::JoinSet::new();
        let shared_requests = shared_history.clone();
        let shared_http = http.clone();
        let shared_instruments = instruments.to_vec();
        let shared_events = emitter.events.clone();
        let shared_demands = source_demands.clone();
        shared_workers.spawn(async move {
            loop {
                let request = match shared_requests.try_recv() {
                    Ok(request) => request,
                    Err(crossbeam_channel::TryRecvError::Disconnected) => return,
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let demanded = source_is_requested(&shared_demands.borrow(),
                    &request.binding, request.interval);
                let result = if request.generation != generation || request.binding.venue != server.venue()
                    || !matches!(request.interval, ChartInterval::OneMinute | ChartInterval::OneDay) {
                    Err("expired or invalid shared history".into())
                } else if !demanded {
                    Err("shared source has no consumer".into())
                } else if let Some(instrument) = shared_instruments.iter()
                    .find(|item| item.symbol == request.binding.symbol) {
                    let selection = MarketSelection { binding: request.binding.clone(), interval: request.interval };
                    tokio::select! {
                        result = candles_inner(server, &shared_http, instrument, &selection, generation,
                            Some(request.before), request.gap_after) => result
                            .map(|bars| bars.into_iter().filter(|bar| bar.close_time_ms < now_ms()).collect()),
                        _ = wait_until_source_disabled(shared_demands.clone(), request.binding.clone(),
                            request.interval) => Err("shared source has no consumer".into()),
                    }
                } else { Err("market not listed".into()) };
                if shared_events.send_timeout(LocalMarketClientEvent::SharedHistory { request, result },
                    COMMAND_SEND_TIMEOUT).is_err() { return; }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
        let mut initialized = BTreeSet::new();
        let mut previewed = BTreeSet::new();
        let mut seen = std::collections::VecDeque::new();
        let mut shared_initialized = BTreeSet::new();
        let mut shared_minute_due = std::collections::BTreeMap::new();
        let mut shared_day_due = std::collections::BTreeMap::new();
        let mut derivative_due = std::collections::BTreeMap::new();
        let mut interest_history_due = std::collections::BTreeMap::new();
        let mut sampled_interest_seen = std::collections::BTreeMap::new();
        loop {
            let work = async {
                match quotes(server, http, instruments).await {
                    Ok(mut quotes) => {
                        if server == MarketServer::Okx {
                            for quote in &mut quotes {
                                let Some(selection) = selections
                                    .iter()
                                    .find(|item| item.binding.symbol == quote.symbol)
                                else {
                                    continue;
                                };
                                if derivative_due
                                    .get(&selection.binding)
                                    .is_some_and(|until| *until > now_ms())
                                {
                                    continue;
                                }
                                let Some(instrument) =
                                    instruments.iter().find(|item| item.symbol == quote.symbol)
                                else {
                                    continue;
                                };
                                match venue_gateway_okx::display::current_derivatives(
                                    http, instrument,
                                )
                                .await
                                {
                                    Ok(value) => quote.derivatives = Some(value),
                                    Err(error) => {
                                        derivative_due.insert(
                                            selection.binding.clone(),
                                            now_ms().saturating_add(15_000),
                                        );
                                        for target in selections
                                            .iter()
                                            .filter(|item| item.binding == selection.binding)
                                        {
                                            emitter.emit(LocalMarketClientEvent::Market(
                                                Box::new(MarketEnvelope {
                                                    generation,
                                                    selection: target.clone(),
                                                    event_time_ms: now_ms(),
                                                    received_ms: now_ms(),
                                                    payload: MarketPayload::OpenInterestUnavailable(
                                                        error.clone(),
                                                    ),
                                                }),
                                            ))?;
                                        }
                                    }
                                }
                            }
                        }
                        for quote in &quotes {
                            let Some(derivative) = quote.derivatives.as_ref() else {
                                continue;
                            };
                            let Some(binding) = selections
                                .iter()
                                .find(|selection| selection.binding.symbol == quote.symbol)
                                .map(|selection| selection.binding.clone())
                            else {
                                continue;
                            };
                            if derivative_due
                                .get(&binding)
                                .is_some_and(|until| *until > now_ms())
                            {
                                continue;
                            }
                            let mut local_current = None;
                            for selection in &selections {
                                if selection.binding.symbol != quote.symbol {
                                    continue;
                                }
                                let received = now_ms();
                                let source_time = |native: u64| {
                                    if native > 0 && native <= received {
                                        Some((native, venue_domain::MarketTimeSource::Exchange))
                                    } else if native == 0 && derivative.use_local_observation_time {
                                        Some((
                                            received,
                                            venue_domain::MarketTimeSource::LocalObservation,
                                        ))
                                    } else {
                                        None
                                    }
                                };
                                if let Some((rate, (event_time, time_source))) =
                                    derivative.funding_rate.zip(source_time(
                                        derivative.funding_time_ms.unwrap_or(quote.time_ms),
                                    ))
                                {
                                    let price = |value: Option<rust_decimal::Decimal>| {
                                        value
                                            .and_then(|value| venue_domain::Price::new(value).ok())
                                            .map_or(
                                                venue_domain::FieldState::Unavailable {
                                                    reason:
                                                        venue_domain::UnknownReason::SourceOmitted,
                                                },
                                                venue_domain::FieldState::Known,
                                            )
                                    };
                                    let funding = venue_domain::MarkFunding {
                                        symbol: quote.symbol.clone(),
                                        generation,
                                        received_at_ms: received,
                                        exchange_time_ms: event_time,
                                        time_source,
                                        next_funding_time_ms: derivative.next_funding_time_ms,
                                        mark_price: price(derivative.mark_price),
                                        index_price: price(derivative.index_price),
                                        funding_rate: rate,
                                        estimated_settle_price:
                                            venue_domain::FieldState::Unavailable {
                                                reason: venue_domain::UnknownReason::SourceOmitted,
                                            },
                                        predicted_funding_rate:
                                            venue_domain::FieldState::Unavailable {
                                                reason: venue_domain::UnknownReason::SourceOmitted,
                                            },
                                        unknown_reason: None,
                                    };
                                    emitter.emit(LocalMarketClientEvent::Market(Box::new(
                                        MarketEnvelope {
                                            generation,
                                            selection: selection.clone(),
                                            event_time_ms: event_time,
                                            received_ms: received,
                                            payload: MarketPayload::Funding(funding),
                                        },
                                    )))?;
                                }
                                if let Some((quantity, (event_time, time_source))) =
                                    derivative.open_interest_base.zip(source_time(
                                        derivative.open_interest_time_ms.unwrap_or(quote.time_ms),
                                    ))
                                {
                                    let sample = venue_domain::OpenInterestSample {
                                        symbol: quote.symbol.clone(),
                                        generation,
                                        received_at_ms: received,
                                        exchange_time_ms: event_time,
                                        time_source,
                                        sampling_interval_ms: None,
                                        native_quantity: derivative
                                            .open_interest_native_quantity
                                            .unwrap_or(quantity),
                                        native_unit: derivative
                                            .open_interest_native_unit
                                            .clone()
                                            .unwrap_or(venue_domain::OpenInterestUnit::BaseAsset),
                                        base_quantity: venue_domain::FieldState::Known(quantity),
                                        quote_notional: venue_domain::FieldState::Unavailable {
                                            reason: venue_domain::UnknownReason::SourceOmitted,
                                        },
                                        quote_asset: None,
                                    };
                                    if matches!(
                                        server,
                                        MarketServer::Bitget | MarketServer::Hyperliquid
                                    ) && local_current.is_none()
                                    {
                                        local_current = Some(sample.clone());
                                    }
                                    emitter.emit(LocalMarketClientEvent::Market(Box::new(
                                        MarketEnvelope {
                                            generation,
                                            selection: selection.clone(),
                                            event_time_ms: event_time,
                                            received_ms: received,
                                            payload: MarketPayload::OpenInterestCurrent(sample),
                                        },
                                    )))?;
                                }
                            }
                            if let Some(current) = local_current
                                && let Some(samples) = public_cache::PublicHistoryCache::local()
                                    .and_then(|cache| {
                                        cache.observe_interest(&binding, generation, &current)
                                    })
                                && let Some(last) = samples.last()
                                && sampled_interest_seen.get(&binding)
                                    != Some(&last.exchange_time_ms)
                            {
                                sampled_interest_seen
                                    .insert(binding.clone(), last.exchange_time_ms);
                                for selection in selections
                                    .iter()
                                    .filter(|selection| selection.binding == binding)
                                {
                                    emitter.emit(LocalMarketClientEvent::Market(Box::new(
                                        MarketEnvelope {
                                            generation,
                                            selection: selection.clone(),
                                            event_time_ms: last.exchange_time_ms,
                                            received_ms: current.received_at_ms,
                                            payload: MarketPayload::OpenInterestHistory(
                                                samples.clone(),
                                            ),
                                        },
                                    )))?;
                                }
                            }
                            derivative_due.insert(binding, now_ms().saturating_add(15_000));
                        }
                        emitter.emit(LocalMarketClientEvent::Quotes(
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
                        ))?
                    }
                    Err(e) => {
                        emitter.emit(LocalMarketClientEvent::QuotesUnavailable(e))?;
                    }
                }
                let mut public_snapshots = std::collections::BTreeMap::<
                    PublicMarketBinding,
                    (Book, Vec<PublicTrade>),
                >::new();
                let mut visible_source_bars =
                    std::collections::BTreeMap::<MarketSelection, Vec<PublicBar>>::new();
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
                    if !initialized.contains(selection)
                        && !previewed.contains(selection)
                        && let Some(bars) = public_cache::PublicHistoryCache::local()
                            .and_then(|cache| cache.recent(selection, generation))
                        && !bars.is_empty()
                    {
                        let bars = bars
                            .into_iter()
                            .rev()
                            .take(INITIAL_VISIBLE_HISTORY_LIMIT)
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect::<Vec<_>>();
                        let received_ms = now_ms();
                        let event_time_ms = bars
                            .last()
                            .map_or(received_ms, |bar| bar.close_time_ms.min(received_ms));
                        emitter.emit(LocalMarketClientEvent::Market(Box::new(MarketEnvelope {
                            generation,
                            selection: selection.clone(),
                            event_time_ms,
                            received_ms,
                            payload: MarketPayload::RestHistory { bars },
                        })))?;
                        previewed.insert(selection.clone());
                    }
                    let result = refresh(
                        server,
                        http,
                        instrument,
                        selection,
                        generation,
                        &mut initialized,
                        &mut seen,
                        emitter,
                        public_snapshots.get(&selection.binding),
                    )
                    .await;
                    match result {
                        Ok((snapshot, source_bars)) => {
                            if let Some(snapshot) = snapshot {
                                public_snapshots.insert(selection.binding.clone(), snapshot);
                            }
                            if let Some(bars) = source_bars {
                                visible_source_bars.insert(selection.clone(), bars);
                            }
                        }
                        Err(error) => {
                            initialized.remove(selection);
                            emitter.status_all(
                                generation,
                                std::slice::from_ref(selection),
                                MarketStatus::Offline,
                                Some(error),
                            )?;
                        }
                    }
                }
                let mut bindings = BTreeSet::new();
                for selection in &selections {
                    if !bindings.insert(selection.binding.clone()) {
                        continue;
                    }
                    let Some(instrument) = instruments
                        .iter()
                        .find(|item| item.symbol == selection.binding.symbol)
                    else {
                        continue;
                    };
                    for interval in [ChartInterval::OneMinute, ChartInterval::OneDay] {
                        let shared_key = (selection.binding.clone(), interval);
                        let fresh = shared_initialized.contains(&shared_key);
                        let due = if interval == ChartInterval::OneMinute {
                            &mut shared_minute_due
                        } else {
                            &mut shared_day_due
                        };
                        let demand = source_demands
                            .borrow()
                            .get(&selection.binding)
                            .copied()
                            .unwrap_or(SharedSourceDemand {
                                minute: true,
                                day: true,
                            });
                        if !(if interval == ChartInterval::OneMinute {
                            demand.minute
                        } else {
                            demand.day
                        }) {
                            shared_initialized.remove(&shared_key);
                            due.remove(&selection.binding);
                            continue;
                        }
                        if fresh
                            && due
                                .get(&selection.binding)
                                .is_some_and(|until| *until > now_ms())
                        {
                            continue;
                        }
                        let source_selection = MarketSelection {
                            binding: selection.binding.clone(),
                            interval,
                        };
                        let result = if let Some(bars) = visible_source_bars.get(&source_selection)
                        {
                            Ok(bars.clone())
                        } else {
                            candles(
                                server,
                                http,
                                instrument,
                                &source_selection,
                                generation,
                                None,
                            )
                            .await
                        };
                        let Ok(mut bars) = result else {
                            due.insert(selection.binding.clone(), now_ms().saturating_add(30_000));
                            continue;
                        };
                        if !fresh {
                            if let Some(cached) = public_cache::PublicHistoryCache::local()
                                .and_then(|cache| cache.recent(&source_selection, generation))
                            {
                                bars = history::merge_latest(Some(cached), bars, generation);
                            }
                        }
                        if !fresh && interval == ChartInterval::OneMinute {
                            bars = bars
                                .into_iter()
                                .rev()
                                .take(360)
                                .collect::<Vec<_>>()
                                .into_iter()
                                .rev()
                                .collect();
                        }
                        let now = now_ms();
                        let forming = bars.last().filter(|bar| bar.close_time_ms >= now).cloned();
                        let closed = bars
                            .into_iter()
                            .filter(|bar| bar.close_time_ms < now)
                            .collect::<Vec<_>>();
                        if !fresh {
                            let event = if interval == ChartInterval::OneMinute {
                                LocalMarketClientEvent::BaseMinuteHistory {
                                    generation,
                                    binding: selection.binding.clone(),
                                    bars: closed,
                                    forming,
                                }
                            } else {
                                LocalMarketClientEvent::SessionDayHistory {
                                    generation,
                                    binding: selection.binding.clone(),
                                    bars: closed,
                                    forming,
                                }
                            };
                            emitter.emit(event)?;
                        } else {
                            for bar in closed.into_iter().rev().take(2).rev() {
                                let event = if interval == ChartInterval::OneMinute {
                                    LocalMarketClientEvent::BaseMinuteBar {
                                        generation,
                                        binding: selection.binding.clone(),
                                        bar,
                                        confirmed: true,
                                    }
                                } else {
                                    LocalMarketClientEvent::SessionDayBar {
                                        generation,
                                        binding: selection.binding.clone(),
                                        bar,
                                        confirmed: true,
                                    }
                                };
                                emitter.emit(event)?;
                            }
                            if let Some(bar) = forming {
                                let event = if interval == ChartInterval::OneMinute {
                                    LocalMarketClientEvent::BaseMinuteBar {
                                        generation,
                                        binding: selection.binding.clone(),
                                        bar,
                                        confirmed: false,
                                    }
                                } else {
                                    LocalMarketClientEvent::SessionDayBar {
                                        generation,
                                        binding: selection.binding.clone(),
                                        bar,
                                        confirmed: false,
                                    }
                                };
                                emitter.emit(event)?;
                            }
                        }
                        due.insert(
                            selection.binding.clone(),
                            now.saturating_add(if interval == ChartInterval::OneMinute {
                                15_000
                            } else {
                                1_800_000
                            }),
                        );
                        shared_initialized.insert(shared_key);
                    }
                }
                if matches!(server, MarketServer::Gate | MarketServer::Okx) {
                    for binding in &bindings {
                        let now = now_ms();
                        if interest_history_due
                            .get(binding)
                            .is_some_and(|until| *until > now)
                        {
                            continue;
                        }
                        let Some(instrument) = instruments
                            .iter()
                            .find(|item| item.symbol == binding.symbol)
                        else {
                            continue;
                        };
                        let cache = public_cache::PublicHistoryCache::local();
                        let cached = cache
                            .as_ref()
                            .and_then(|cache| cache.interest(binding, generation));
                        let warm = cached.as_ref().is_some_and(|samples| {
                            public_cache::PublicHistoryCache::interest_fresh(samples, now)
                        });
                        let result = if let Some(samples) = cached.as_ref().filter(|_| warm) {
                            Ok(samples.clone())
                        } else if server == MarketServer::Gate {
                            venue_gateway_gate::display::open_interest_history(
                                http, instrument, generation, now,
                            )
                            .await
                        } else {
                            let since = cached
                                .as_ref()
                                .and_then(|samples| samples.last())
                                .map(|sample| sample.exchange_time_ms);
                            venue_gateway_okx::display::open_interest_history(
                                http, instrument, generation, now, since,
                            )
                            .await
                        };
                        let result = result.and_then(|samples| {
                            if samples.is_empty() {
                                Err("OI history has no completed samples".into())
                            } else {
                                Ok(samples)
                            }
                        });
                        let (payload, next) = match result {
                            Ok(samples) => (
                                MarketPayload::OpenInterestHistory(if warm {
                                    samples
                                } else {
                                    cache
                                        .as_ref()
                                        .and_then(|cache| {
                                            cache.remember_interest(
                                                binding,
                                                generation,
                                                samples.clone(),
                                            )
                                        })
                                        .unwrap_or(samples)
                                }),
                                now / 300_000 * 300_000 + 310_000,
                            ),
                            Err(error) => (
                                MarketPayload::OpenInterestHistoryUnavailable(error),
                                now.saturating_add(60_000),
                            ),
                        };
                        interest_history_due.insert(binding.clone(), next);
                        let event_time = match &payload {
                            MarketPayload::OpenInterestHistory(samples) => {
                                samples.last().map_or(now, |sample| sample.exchange_time_ms)
                            }
                            _ => now,
                        };
                        for selection in selections.iter().filter(|item| item.binding == *binding) {
                            emitter.emit(LocalMarketClientEvent::Market(Box::new(
                                MarketEnvelope {
                                    generation,
                                    selection: selection.clone(),
                                    event_time_ms: event_time,
                                    received_ms: now,
                                    payload: payload.clone(),
                                },
                            )))?;
                        }
                    }
                }
                // Give quotes and derivative snapshots another turn while older
                // chart pages are still being requested. A deep history scroll can
                // enqueue hundreds of pages, so draining the entire channel here
                // would make otherwise healthy Funding/OI appear stale.
                for _ in 0..2 {
                    let Ok(request) = history.try_recv() else {
                        break;
                    };
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
    shared: Option<&(Book, Vec<PublicTrade>)>,
) -> Result<(Option<(Book, Vec<PublicTrade>)>, Option<Vec<PublicBar>>), String> {
    let (bars, fetched) = if shared.is_some() {
        (
            candles(server, http, instrument, selection, generation, None).await?,
            None,
        )
    } else {
        let (bars, book, prints) = tokio::join!(
            candles(server, http, instrument, selection, generation, None),
            book(server, http, instrument),
            trades(server, http, instrument, generation)
        );
        (bars?, Some((book?, prints?)))
    };
    let (book, prints) = shared
        .or(fetched.as_ref())
        .ok_or("missing public snapshot")?;
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
    let mut source_bars = matches!(
        selection.interval,
        ChartInterval::OneMinute | ChartInterval::OneDay
    )
    .then(|| bars.clone());
    if !initialized.contains(selection) {
        let closed = bars
            .iter()
            .filter(|bar| bar.close_time_ms < received)
            .cloned()
            .collect::<Vec<_>>();
        let history =
            initial_visible_history(server, http, instrument, selection, generation, closed).await;
        if let Some(source) = &mut source_bars {
            *source =
                history::merge_latest(Some(history.clone()), std::mem::take(source), generation);
        }
        send(
            emitter,
            MarketPayload::RestHistory { bars: history },
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
    if book.time_ms > received || received.saturating_sub(book.time_ms) > 15_000 {
        return Err("public book timestamp is stale or in the future".into());
    }
    let levels = |values: &[(rust_decimal::Decimal, rust_decimal::Decimal)]| {
        values
            .iter()
            .map(|(price, quantity)| UiBookLevel {
                price: *price,
                quantity: *quantity,
            })
            .collect()
    };
    send(
        emitter,
        MarketPayload::BookSnapshot {
            bids: levels(&book.bids),
            asks: levels(&book.asks),
        },
        book.time_ms,
    )?;
    for print in prints {
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
        send(emitter, MarketPayload::Trade(ui_trade(print.clone())), time)?;
    }
    emitter.status_all(
        generation,
        std::slice::from_ref(selection),
        MarketStatus::Live,
        Some("REST snapshots · 2s minimum refresh · recent trades only".into()),
    )?;
    Ok((fetched, source_bars))
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
            let result = async {
                let instruments = catalog(server, &http)
                    .await
                    .map_err(|e| format!("catalogue: {e}"))?;
                let instrument = instruments
                    .iter()
                    .find(|i| i.symbol.base() == "BTC")
                    .ok_or("missing BTC")?;
                let quotes = quotes(server, &http, &instruments)
                    .await
                    .map_err(|e| format!("quotes: {e}"))?;
                if quotes.is_empty() {
                    return Err("empty quotes".into());
                }
                let book = book(server, &http, instrument)
                    .await
                    .map_err(|e| format!("book: {e}"))?;
                if book.bids.is_empty() || book.asks.is_empty() {
                    return Err("empty book".into());
                }
                let prints = trades(server, &http, instrument, 1)
                    .await
                    .map_err(|e| format!("trades: {e}"))?;
                if prints.is_empty() || prints.iter().any(|t| !t.is_valid()) {
                    return Err("invalid or empty trades".into());
                }
                for interval in ChartInterval::ALL {
                    let selection = MarketSelection::for_server(
                        server,
                        &instrument.symbol.to_string(),
                        interval,
                    )
                    .map_err(|e| e.to_string())?;
                    let fresh = candles(server, &http, instrument, &selection, 1, None)
                        .await
                        .map_err(|e| format!("{interval:?} candles: {e}"))?;
                    let bars = initial_visible_history(server, &http, instrument,
                        &selection, 1, fresh).await;
                    if bars.len() < 2 || bars.iter().any(|bar| !bar.is_valid()) {
                        return Err("invalid or empty candles".into());
                    }
                    let observed = now_ms();
                    let closed = bars
                        .iter()
                        .filter(|bar| bar.close_time_ms < observed)
                        .cloned()
                        .collect::<Vec<_>>();
                    let mut reducer = crate::market::LocalMarketReducer::new(selection.clone())
                        .map_err(|error| format!("{interval:?} reducer: {error}"))?;
                    reducer
                        .apply(MarketEnvelope {
                            generation: 1,
                            selection: selection.clone(),
                            event_time_ms: observed,
                            received_ms: observed,
                            payload: MarketPayload::RestHistory { bars: closed },
                        })
                        .map_err(|error| format!("{interval:?} study ingest: {error}"))?;
                    if reducer.view().bars.len() < 2 {
                        return Err(format!("{interval:?} history was not displayed"));
                    }
                    let before = bars.first().ok_or("empty history")?.open_time_ms;
                    let page = candles(server, &http, instrument, &selection, 1, Some(before))
                        .await
                        .map_err(|e| format!("{interval:?} history: {e}"))?;
                    if page.is_empty()
                        || page.iter().any(|bar| bar.open_time_ms >= before)
                    {
                        return Err("invalid backward history".into());
                    }
                }
                println!(
                    "{server:?}: catalogue={}, quotes={}, book={}/{}, trades={}, all 6 intervals and history pages OK",
                    instruments.len(),
                    quotes.len(),
                    book.bids.len(),
                    book.asks.len(),
                    prints.len()
                );
                Ok::<(), String>(())
            }
            .await;
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
