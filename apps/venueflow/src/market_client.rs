#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::{
        collections::BTreeSet,
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
    use futures_util::{SinkExt as _, StreamExt as _};
    use tokio::{runtime::Builder, sync::mpsc, time::timeout};
    use tokio_tungstenite::{
        connect_async_with_config,
        tungstenite::{Message, protocol::WebSocketConfig},
    };
    use venue_control_protocol::{AggressorSide as UiAggressorSide, UiBar, UiBookLevel, UiTrade};
    use venue_domain::domain::{
        AggressorSide, FieldState, MarketSnapshot, PublicBar, PublicTicker, PublicTrade,
    };
    use venue_gateway_binance::{
        BinanceFormingBar, BinanceKlineInterval, BinancePublicKline, native_symbol,
        parse_public_market_agg_trade, parse_public_market_bbo,
        parse_public_market_depth20_snapshot, parse_public_market_kline,
        parse_public_market_rest_klines,
    };

    use crate::{
        chart::ChartInterval,
        market::{MarketEnvelope, MarketPayload, MarketSelection, MarketStatus},
    };

    const REST_KLINES_ENDPOINT: &str = "https://fapi.binance.com/fapi/v1/klines";
    const COMBINED_STREAM_ENDPOINT: &str = "wss://fstream.binance.com/stream";
    const CONNECT_BUDGET: Duration = Duration::from_secs(10);
    const HTTP_BODY_LIMIT: usize = 1024 * 1024;
    const WS_FRAME_LIMIT: usize = 1024 * 1024;
    const COMMAND_CAPACITY: usize = 8;
    const EVENT_CAPACITY: usize = 8_192;
    const MAX_SUBSCRIPTIONS: usize = 8;
    const DEFAULT_HISTORY_LIMIT: usize = 1_000;
    const REPAINT_INTERVAL: Duration = Duration::from_millis(50);
    const PING_INTERVAL: Duration = Duration::from_secs(15);
    const PONG_DEADLINE: Duration = Duration::from_secs(45);
    const COMMAND_SEND_TIMEOUT: Duration = Duration::from_millis(250);

    #[derive(Clone, Debug, PartialEq)]
    pub enum LocalMarketClientEvent {
        Market(MarketEnvelope),
        RepaintRequested,
        WorkerFailed(String),
    }

    #[derive(Clone, Debug)]
    enum LocalMarketCommand {
        Replace {
            generation: u64,
            selections: Vec<MarketSelection>,
        },
        Stop,
    }

    #[derive(Debug)]
    pub struct LocalMarketClient {
        commands: Sender<LocalMarketCommand>,
        events: Receiver<LocalMarketClientEvent>,
        worker: Option<thread::JoinHandle<()>>,
    }

    impl LocalMarketClient {
        pub fn start() -> Result<Self, LocalMarketClientError> {
            let (command_tx, command_rx) = bounded(COMMAND_CAPACITY);
            let (event_tx, event_rx) = bounded(EVENT_CAPACITY);
            let worker = thread::Builder::new()
                .name("venueflow-local-market".to_owned())
                .spawn(move || worker_main(command_rx, event_tx))
                .map_err(|_| LocalMarketClientError::ThreadStart)?;
            Ok(Self {
                commands: command_tx,
                events: event_rx,
                worker: Some(worker),
            })
        }

        /// Atomically replaces every public subscription. One non-zero generation must bind the
        /// complete set so delayed REST and websocket results from the old set remain rejectable.
        pub fn replace_subscriptions(
            &self,
            generation: u64,
            selections: Vec<MarketSelection>,
        ) -> Result<(), LocalMarketClientError> {
            if generation == 0 {
                return Err(LocalMarketClientError::Generation);
            }
            let selections = validate_selections(selections)?;
            self.commands
                .send_timeout(
                    LocalMarketCommand::Replace {
                        generation,
                        selections,
                    },
                    COMMAND_SEND_TIMEOUT,
                )
                .map_err(|_| LocalMarketClientError::CommandUnavailable)
        }

        /// Drains at most `limit` events without blocking the UI thread.
        pub fn drain(&self, limit: usize) -> Vec<LocalMarketClientEvent> {
            self.events.try_iter().take(limit).collect()
        }
    }

    impl Drop for LocalMarketClient {
        fn drop(&mut self) {
            if self
                .commands
                .send_timeout(LocalMarketCommand::Stop, COMMAND_SEND_TIMEOUT)
                .is_ok()
                && let Some(worker) = self.worker.take()
            {
                let _ = worker.join();
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
    pub enum LocalMarketClientError {
        #[error("local market generation must be non-zero")]
        Generation,
        #[error("local market selection is invalid")]
        Selection,
        #[error("local market supports at most eight simultaneous selections")]
        TooManySelections,
        #[error("local market command channel is unavailable")]
        CommandUnavailable,
        #[error("failed to start local market worker")]
        ThreadStart,
    }

    fn validate_selections(
        selections: Vec<MarketSelection>,
    ) -> Result<Vec<MarketSelection>, LocalMarketClientError> {
        if selections.len() > MAX_SUBSCRIPTIONS {
            return Err(LocalMarketClientError::TooManySelections);
        }
        let mut unique = BTreeSet::new();
        for selection in selections {
            selection
                .validate()
                .map_err(|_| LocalMarketClientError::Selection)?;
            unique.insert(selection);
        }
        Ok(unique.into_iter().collect())
    }

    fn worker_main(
        command_rx: Receiver<LocalMarketCommand>,
        event_tx: Sender<LocalMarketClientEvent>,
    ) {
        let runtime = match Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = event_tx.try_send(LocalMarketClientEvent::WorkerFailed(format!(
                    "local market runtime unavailable: {error}"
                )));
                return;
            }
        };
        runtime.block_on(supervisor(command_rx, event_tx));
        runtime.shutdown_background();
    }

    async fn supervisor(
        command_rx: Receiver<LocalMarketCommand>,
        event_tx: Sender<LocalMarketClientEvent>,
    ) {
        let (async_tx, mut async_rx) = mpsc::channel(COMMAND_CAPACITY);
        let bridge = tokio::task::spawn_blocking(move || {
            while let Ok(command) = command_rx.recv() {
                let stop = matches!(command, LocalMarketCommand::Stop);
                if async_tx.blocking_send(command).is_err() || stop {
                    break;
                }
            }
        });
        let http = match reqwest::Client::builder()
            .connect_timeout(CONNECT_BUDGET)
            .timeout(CONNECT_BUDGET)
            .https_only(true)
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                let _ = event_tx.try_send(LocalMarketClientEvent::WorkerFailed(format!(
                    "local market HTTP client unavailable: {error}"
                )));
                bridge.abort();
                return;
            }
        };
        let mut pending = None;
        loop {
            let command = match pending.take() {
                Some(command) => command,
                None => match async_rx.recv().await {
                    Some(command) => command,
                    None => break,
                },
            };
            match command {
                LocalMarketCommand::Stop => break,
                LocalMarketCommand::Replace {
                    generation,
                    selections,
                } if selections.is_empty() => {
                    let _ = generation;
                }
                LocalMarketCommand::Replace {
                    generation,
                    selections,
                } => {
                    pending = run_subscription_set(
                        generation,
                        selections,
                        &http,
                        &mut async_rx,
                        &event_tx,
                    )
                    .await;
                }
            }
        }
        bridge.abort();
    }

    async fn run_subscription_set(
        generation: u64,
        selections: Vec<MarketSelection>,
        http: &reqwest::Client,
        commands: &mut mpsc::Receiver<LocalMarketCommand>,
        event_tx: &Sender<LocalMarketClientEvent>,
    ) -> Option<LocalMarketCommand> {
        let mut attempt = 0_u32;
        let mut emitter = EventEmitter::new(event_tx.clone());
        loop {
            if let Err(error) =
                emitter.status_all(generation, &selections, MarketStatus::LoadingHistory, None)
            {
                return worker_failure(event_tx, error);
            }
            match load_history(generation, &selections, http, commands, &mut emitter).await {
                AttemptResult::Interrupted(command) => return Some(command),
                AttemptResult::Failed(error) => {
                    if let Some(command) = retry_after_error(
                        generation,
                        &selections,
                        attempt,
                        error,
                        commands,
                        &mut emitter,
                    )
                    .await
                    {
                        return Some(command);
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                AttemptResult::Completed(()) => {}
            }
            if let Err(error) =
                emitter.status_all(generation, &selections, MarketStatus::Connecting, None)
            {
                return worker_failure(event_tx, error);
            }
            let url = combined_stream_url(&selections);
            let websocket_config = WebSocketConfig::default()
                .max_message_size(Some(WS_FRAME_LIMIT))
                .max_frame_size(Some(WS_FRAME_LIMIT));
            let connection = tokio::select! {
                command = commands.recv() => return command,
                result = timeout(
                    CONNECT_BUDGET,
                    connect_async_with_config(&url, Some(websocket_config), false),
                ) => result,
            };
            let mut websocket = match connection {
                Ok(Ok((websocket, _))) => websocket,
                Ok(Err(error)) => {
                    if let Some(command) = retry_after_error(
                        generation,
                        &selections,
                        attempt,
                        format!("websocket connect failed: {error}"),
                        commands,
                        &mut emitter,
                    )
                    .await
                    {
                        return Some(command);
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
                Err(_) => {
                    if let Some(command) = retry_after_error(
                        generation,
                        &selections,
                        attempt,
                        "websocket connect timed out".to_owned(),
                        commands,
                        &mut emitter,
                    )
                    .await
                    {
                        return Some(command);
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
            };
            if let Err(error) =
                emitter.status_all(generation, &selections, MarketStatus::Live, None)
            {
                return worker_failure(event_tx, error);
            }
            attempt = 0;
            let mut heartbeat = tokio::time::interval(PING_INTERVAL);
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_pong = Instant::now();
            let session_error = loop {
                tokio::select! {
                    command = commands.recv() => return command,
                    _ = heartbeat.tick() => {
                        if last_pong.elapsed() > PONG_DEADLINE {
                            break "websocket pong deadline exceeded".to_owned();
                        }
                        if let Err(error) = websocket.send(Message::Ping(Vec::new().into())).await {
                            break format!("websocket ping failed: {error}");
                        }
                    }
                    frame = websocket.next() => {
                        match frame {
                            Some(Ok(Message::Text(payload))) => {
                                let received_ms = now_ms();
                                if payload.len() > WS_FRAME_LIMIT {
                                    break "websocket text frame exceeded 1 MiB".to_owned();
                                }
                                if let Err(error) = dispatch_payload(
                                    payload.as_ref(),
                                    generation,
                                    &selections,
                                    received_ms,
                                    &mut emitter,
                                ) {
                                    break error;
                                }
                            }
                            Some(Ok(Message::Ping(payload))) => {
                                if let Err(error) = websocket.send(Message::Pong(payload)).await {
                                    break format!("websocket pong failed: {error}");
                                }
                            }
                            Some(Ok(Message::Pong(_))) => last_pong = Instant::now(),
                            Some(Ok(Message::Close(_))) => break "websocket closed".to_owned(),
                            Some(Ok(Message::Binary(_))) => {
                                break "unexpected websocket binary frame".to_owned();
                            }
                            Some(Ok(Message::Frame(_))) => {}
                            Some(Err(error)) => break format!("websocket receive failed: {error}"),
                            None => break "websocket stream ended".to_owned(),
                        }
                    }
                }
            };
            if let Some(command) = retry_after_error(
                generation,
                &selections,
                attempt,
                session_error,
                commands,
                &mut emitter,
            )
            .await
            {
                return Some(command);
            }
            attempt = attempt.saturating_add(1);
        }
    }

    async fn load_history(
        generation: u64,
        selections: &[MarketSelection],
        http: &reqwest::Client,
        commands: &mut mpsc::Receiver<LocalMarketCommand>,
        emitter: &mut EventEmitter,
    ) -> AttemptResult<()> {
        for selection in selections {
            let history = tokio::select! {
                command = commands.recv() => {
                    return match command {
                        Some(command) => AttemptResult::Interrupted(command),
                        None => AttemptResult::Failed("local market command bridge ended".to_owned()),
                    };
                }
                result = fetch_history(http, selection, generation, DEFAULT_HISTORY_LIMIT) => result,
            };
            let (bars, received_ms, event_time_ms) = match history {
                Ok(history) => history,
                Err(error) => return AttemptResult::Failed(error),
            };
            let envelope = MarketEnvelope {
                generation,
                selection: selection.clone(),
                event_time_ms,
                received_ms,
                payload: MarketPayload::RestHistory { bars },
            };
            if let Err(error) = emitter.emit(LocalMarketClientEvent::Market(envelope)) {
                return AttemptResult::Failed(error);
            }
        }
        AttemptResult::Completed(())
    }

    async fn fetch_history(
        http: &reqwest::Client,
        selection: &MarketSelection,
        generation: u64,
        limit: usize,
    ) -> Result<(Vec<UiBar>, u64, u64), String> {
        let url = rest_klines_url(selection, limit)?;
        let response = http
            .get(url)
            .send()
            .await
            .map_err(|error| format!("history request failed: {error}"))?;
        if !response.status().is_success() {
            return Err(format!(
                "history request returned HTTP {}",
                response.status().as_u16()
            ));
        }
        if response
            .content_length()
            .is_some_and(|size| size > HTTP_BODY_LIMIT as u64)
        {
            return Err("history response exceeded 1 MiB".to_owned());
        }
        let mut stream = response.bytes_stream();
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| format!("history body failed: {error}"))?;
            let new_len = body
                .len()
                .checked_add(chunk.len())
                .ok_or_else(|| "history response length overflow".to_owned())?;
            if new_len > HTTP_BODY_LIMIT {
                return Err("history response exceeded 1 MiB".to_owned());
            }
            body.extend_from_slice(&chunk);
        }
        let payload =
            std::str::from_utf8(&body).map_err(|_| "history response was not UTF-8".to_owned())?;
        let received_ms = now_ms();
        let interval = binance_interval(selection.interval);
        let bars = parse_public_market_rest_klines(
            payload,
            &selection.binding,
            generation,
            received_ms,
            interval,
        )
        .map_err(|error| format!("history parse failed: {error}"))?;
        let event_time_ms = bars
            .last()
            .map_or(received_ms, |bar| bar.close_time_ms.min(received_ms));
        let bars = bars
            .into_iter()
            .map(ui_bar_from_closed)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((bars, received_ms, event_time_ms))
    }

    async fn retry_after_error(
        generation: u64,
        selections: &[MarketSelection],
        attempt: u32,
        error: String,
        commands: &mut mpsc::Receiver<LocalMarketCommand>,
        emitter: &mut EventEmitter,
    ) -> Option<LocalMarketCommand> {
        if emitter
            .status_all(generation, selections, MarketStatus::Resyncing, Some(error))
            .is_err()
        {
            return Some(LocalMarketCommand::Stop);
        }
        tokio::select! {
            command = commands.recv() => command,
            _ = tokio::time::sleep(retry_delay(attempt, generation)) => None,
        }
    }

    fn dispatch_payload(
        payload: &str,
        generation: u64,
        selections: &[MarketSelection],
        received_ms: u64,
        emitter: &mut EventEmitter,
    ) -> Result<(), String> {
        for selection in selections {
            if let Ok(kline) =
                parse_public_market_kline(payload, &selection.binding, generation, received_ms)
            {
                let (event_time_ms, bar, closed) = match kline {
                    BinancePublicKline::Forming(envelope) => {
                        let event_time_ms = envelope.exchange_event_time_ms();
                        let fact = envelope.into_fact();
                        if fact.interval != binance_interval(selection.interval) {
                            continue;
                        }
                        (event_time_ms, ui_bar_from_forming(fact), false)
                    }
                    BinancePublicKline::Closed(envelope) => {
                        let event_time_ms = envelope.exchange_event_time_ms();
                        let fact = envelope.into_fact();
                        if fact.interval_ms != selection.interval.duration_ms() {
                            continue;
                        }
                        (event_time_ms, ui_bar_from_closed(fact)?, true)
                    }
                };
                emitter.emit(LocalMarketClientEvent::Market(MarketEnvelope {
                    generation,
                    selection: selection.clone(),
                    event_time_ms,
                    received_ms,
                    payload: MarketPayload::WsBar { bar, closed },
                }))?;
                continue;
            }
            if let Ok(envelope) =
                parse_public_market_bbo(payload, &selection.binding, generation, received_ms)
            {
                let event_time_ms = envelope.exchange_event_time_ms();
                let (bid, ask) = ui_bbo(envelope.into_fact());
                emitter.emit(LocalMarketClientEvent::Market(MarketEnvelope {
                    generation,
                    selection: selection.clone(),
                    event_time_ms,
                    received_ms,
                    payload: MarketPayload::Bbo { bid, ask },
                }))?;
                continue;
            }
            if let Ok(envelope) =
                parse_public_market_agg_trade(payload, &selection.binding, generation, received_ms)
            {
                let event_time_ms = envelope.exchange_event_time_ms();
                let trade = ui_trade(envelope.into_fact());
                emitter.emit(LocalMarketClientEvent::Market(MarketEnvelope {
                    generation,
                    selection: selection.clone(),
                    event_time_ms,
                    received_ms,
                    payload: MarketPayload::Trade(trade),
                }))?;
                continue;
            }
            if let Ok(envelope) =
                parse_public_market_depth20_snapshot(payload, &selection.binding, generation)
            {
                let event_time_ms = envelope.exchange_event_time_ms();
                let (bids, asks) = ui_book_from_snapshot(envelope.into_fact());
                emitter.emit(LocalMarketClientEvent::Market(MarketEnvelope {
                    generation,
                    selection: selection.clone(),
                    event_time_ms,
                    received_ms,
                    payload: MarketPayload::BookSnapshot { bids, asks },
                }))?;
            }
        }
        Ok(())
    }

    struct EventEmitter {
        events: Sender<LocalMarketClientEvent>,
        last_repaint: Option<Instant>,
    }

    impl EventEmitter {
        fn new(events: Sender<LocalMarketClientEvent>) -> Self {
            Self {
                events,
                last_repaint: None,
            }
        }

        fn emit(&mut self, event: LocalMarketClientEvent) -> Result<(), String> {
            match self.events.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    return Err("local market event queue is full".to_owned());
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err("local market event receiver is disconnected".to_owned());
                }
            }
            let repaint_due = self
                .last_repaint
                .is_none_or(|last| last.elapsed() >= REPAINT_INTERVAL);
            if repaint_due {
                match self
                    .events
                    .try_send(LocalMarketClientEvent::RepaintRequested)
                {
                    Ok(()) => self.last_repaint = Some(Instant::now()),
                    Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Disconnected(_)) => {
                        return Err("local market event receiver is disconnected".to_owned());
                    }
                }
            }
            Ok(())
        }

        fn status_all(
            &mut self,
            generation: u64,
            selections: &[MarketSelection],
            status: MarketStatus,
            detail: Option<String>,
        ) -> Result<(), String> {
            let timestamp = now_ms();
            for selection in selections {
                self.emit(LocalMarketClientEvent::Market(MarketEnvelope {
                    generation,
                    selection: selection.clone(),
                    event_time_ms: timestamp,
                    received_ms: timestamp,
                    payload: MarketPayload::Status {
                        status,
                        detail: detail.clone(),
                    },
                }))?;
            }
            Ok(())
        }
    }

    enum AttemptResult<T> {
        Completed(T),
        Interrupted(LocalMarketCommand),
        Failed(String),
    }

    fn worker_failure(
        events: &Sender<LocalMarketClientEvent>,
        error: String,
    ) -> Option<LocalMarketCommand> {
        let _ = events.try_send(LocalMarketClientEvent::WorkerFailed(error));
        Some(LocalMarketCommand::Stop)
    }

    fn rest_klines_url(selection: &MarketSelection, limit: usize) -> Result<String, String> {
        if limit == 0 || limit > 1_000 {
            return Err("history limit must be between 1 and 1000".to_owned());
        }
        selection
            .validate()
            .map_err(|_| "invalid public market selection".to_owned())?;
        Ok(format!(
            "{REST_KLINES_ENDPOINT}?symbol={}&interval={}&limit={limit}",
            native_symbol(&selection.binding.symbol),
            selection.interval.label(),
        ))
    }

    fn combined_stream_url(selections: &[MarketSelection]) -> String {
        let mut streams = Vec::new();
        for selection in selections {
            let symbol = native_symbol(&selection.binding.symbol).to_ascii_lowercase();
            for stream in [
                format!("{symbol}@kline_{}", selection.interval.label()),
                format!("{symbol}@bookTicker"),
                format!("{symbol}@aggTrade"),
                format!("{symbol}@depth20@100ms"),
            ] {
                if !streams.contains(&stream) {
                    streams.push(stream);
                }
            }
        }
        format!("{COMBINED_STREAM_ENDPOINT}?streams={}", streams.join("/"))
    }

    const fn binance_interval(interval: ChartInterval) -> BinanceKlineInterval {
        match interval {
            ChartInterval::OneMinute => BinanceKlineInterval::OneMinute,
            ChartInterval::FiveMinutes => BinanceKlineInterval::FiveMinutes,
            ChartInterval::FifteenMinutes => BinanceKlineInterval::FifteenMinutes,
            ChartInterval::OneHour => BinanceKlineInterval::OneHour,
            ChartInterval::FourHours => BinanceKlineInterval::FourHours,
            ChartInterval::OneDay => BinanceKlineInterval::OneDay,
        }
    }

    fn ui_bar_from_forming(bar: BinanceFormingBar) -> UiBar {
        UiBar {
            open_time_ms: bar.open_time_ms,
            open: bar.open.value(),
            high: bar.high.value(),
            low: bar.low.value(),
            close: bar.close.value(),
            volume: bar.base_volume,
        }
    }

    fn ui_bar_from_closed(bar: PublicBar) -> Result<UiBar, String> {
        let FieldState::Known(volume) = bar.base_volume else {
            return Err("normalized bar has no known base volume".to_owned());
        };
        Ok(UiBar {
            open_time_ms: bar.open_time_ms,
            open: bar.open.value(),
            high: bar.high.value(),
            low: bar.low.value(),
            close: bar.close.value(),
            volume,
        })
    }

    fn ui_bbo(ticker: PublicTicker) -> (rust_decimal::Decimal, rust_decimal::Decimal) {
        (ticker.bid_price.value(), ticker.ask_price.value())
    }

    fn ui_book_from_snapshot(snapshot: MarketSnapshot) -> (Vec<UiBookLevel>, Vec<UiBookLevel>) {
        let bids = snapshot
            .bids
            .into_iter()
            .map(|level| UiBookLevel {
                price: level.price.value(),
                quantity: level.quantity,
            })
            .collect();
        let asks = snapshot
            .asks
            .into_iter()
            .map(|level| UiBookLevel {
                price: level.price.value(),
                quantity: level.quantity,
            })
            .collect();
        (bids, asks)
    }

    fn ui_trade(trade: PublicTrade) -> UiTrade {
        let aggressor = match trade.aggressor {
            FieldState::Known(AggressorSide::Buy) => UiAggressorSide::Buy,
            FieldState::Known(AggressorSide::Sell) => UiAggressorSide::Sell,
            _ => UiAggressorSide::Unknown,
        };
        UiTrade {
            trade_id: trade.aggregate_trade_id.to_string(),
            occurred_ms: trade.transaction_time_ms,
            price: trade.price.value(),
            quantity: trade.quantity,
            aggressor,
        }
    }

    fn retry_delay(attempt: u32, generation: u64) -> Duration {
        let shift = attempt.min(7);
        let base_ms = 250_u64.saturating_mul(1_u64 << shift).min(25_000);
        let mixed = generation
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .rotate_left(attempt % 63)
            ^ u64::from(attempt).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let jitter_ceiling = (base_ms / 5).max(1);
        let jitter_ms = mixed % (jitter_ceiling + 1);
        Duration::from_millis(base_ms.saturating_add(jitter_ms).min(30_000))
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            })
    }

    #[cfg(test)]
    mod tests {
        use rust_decimal::Decimal;
        use venue_domain::domain::{Price, Symbol};

        use super::*;

        fn selection(symbol: &str, interval: ChartInterval) -> Result<MarketSelection, String> {
            MarketSelection::binance_usd_m(symbol, interval).map_err(|error| error.to_string())
        }

        #[test]
        fn builds_fixed_rest_url_and_enforces_limit() -> Result<(), String> {
            let selection = selection("BTC/USDT", ChartInterval::FiveMinutes)?;
            assert_eq!(
                rest_klines_url(&selection, 1_000)?,
                "https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=5m&limit=1000"
            );
            assert!(rest_klines_url(&selection, 0).is_err());
            assert!(rest_klines_url(&selection, 1_001).is_err());
            Ok(())
        }

        #[test]
        fn builds_one_combined_stream_for_all_facts() -> Result<(), String> {
            let selection = selection("ETH/USDT", ChartInterval::OneHour)?;
            assert_eq!(
                combined_stream_url(&[selection]),
                concat!(
                    "wss://fstream.binance.com/stream?streams=",
                    "ethusdt@kline_1h/ethusdt@bookTicker/",
                    "ethusdt@aggTrade/ethusdt@depth20@100ms"
                )
            );
            Ok(())
        }

        #[test]
        fn combined_stream_deduplicates_shared_symbol_facts() -> Result<(), String> {
            let one_minute = selection("BTC/USDT", ChartInterval::OneMinute)?;
            let five_minutes = selection("BTC/USDT", ChartInterval::FiveMinutes)?;
            let url = combined_stream_url(&[one_minute, five_minutes]);
            assert_eq!(url.matches("btcusdt@bookTicker").count(), 1);
            assert_eq!(url.matches("btcusdt@aggTrade").count(), 1);
            assert_eq!(url.matches("btcusdt@depth20@100ms").count(), 1);
            assert_eq!(url.matches("btcusdt@kline_").count(), 2);
            Ok(())
        }

        #[test]
        fn interval_mapping_is_exhaustive() {
            for interval in ChartInterval::ALL {
                assert_eq!(binance_interval(interval).as_str(), interval.label());
            }
        }

        #[test]
        fn converts_normalized_forming_bar_without_json() -> Result<(), Box<dyn std::error::Error>>
        {
            let bar = BinanceFormingBar {
                symbol: "BTC/USDT".parse::<Symbol>()?,
                generation: 3,
                received_at_ms: 60_001,
                exchange_time_ms: 60_000,
                sequence: 2,
                open_time_ms: 60_000,
                close_time_ms: 119_999,
                interval: BinanceKlineInterval::OneMinute,
                open: Price::new(Decimal::new(100, 0))?,
                high: Price::new(Decimal::new(110, 0))?,
                low: Price::new(Decimal::new(90, 0))?,
                close: Price::new(Decimal::new(105, 0))?,
                base_volume: Decimal::new(12, 0),
                quote_volume: Decimal::new(1_200, 0),
                trade_count: 5,
                taker_buy_base_volume: Decimal::new(4, 0),
                taker_buy_quote_volume: Decimal::new(400, 0),
            };
            let ui = ui_bar_from_forming(bar);
            assert_eq!(ui.open_time_ms, 60_000);
            assert_eq!(ui.close, Decimal::new(105, 0));
            assert_eq!(ui.volume, Decimal::new(12, 0));
            Ok(())
        }

        #[test]
        fn retry_delay_is_jittered_and_bounded() {
            let values = (0..20)
                .map(|attempt| retry_delay(attempt, 7))
                .collect::<Vec<_>>();
            assert!(values.iter().all(|value| {
                *value >= Duration::from_millis(250) && *value <= Duration::from_secs(30)
            }));
            assert_ne!(retry_delay(3, 7), retry_delay(3, 8));
            assert_eq!(retry_delay(99, 7), retry_delay(99, 7));
            assert!(retry_delay(99, 7) >= Duration::from_secs(25));
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::*;
