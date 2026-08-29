use super::*;

#[derive(Debug)]
pub(super) struct BinancePublicMarket {
    symbol: Symbol,
    pub(super) generation: u64,
    book: OrderBook,
    last_ready_exchange_time_ms: Option<u64>,
}

impl BinancePublicMarket {
    pub(super) fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            generation: 1,
            book: OrderBook::default(),
            last_ready_exchange_time_ms: None,
        }
    }

    pub(super) fn seed_generation(
        &mut self,
        minimum_generation: u64,
    ) -> Result<(), GridVenueError> {
        if self.generation >= minimum_generation {
            return Ok(());
        }
        self.generation = minimum_generation;
        self.clear();
        Ok(())
    }

    pub(super) fn reset(&mut self) -> Result<(), GridVenueError> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(GridVenueError::PublicPayload)?;
        self.clear();
        Ok(())
    }

    pub(super) fn clear(&mut self) {
        self.book = OrderBook::default();
        self.last_ready_exchange_time_ms = None;
    }

    pub(super) fn accept(&mut self, payload: GridPublicPayload) -> Result<(), GridVenueError> {
        if payload.generation != self.generation {
            return Err(GridVenueError::PublicPayload);
        }
        let source = match payload.source {
            GridPublicPayloadSource::RestSnapshot => RawSource::RestSnapshot,
            GridPublicPayloadSource::WebSocketDepth => RawSource::WebSocketDelta,
            GridPublicPayloadSource::RestTicker
            | GridPublicPayloadSource::WebSocketBbo
            | GridPublicPayloadSource::WebSocketTrade
            | GridPublicPayloadSource::WebSocketMark => {
                return Err(GridVenueError::PublicPayload);
            }
        };
        let record = RawMarketRecord::new(
            source,
            self.symbol.clone(),
            payload.generation,
            payload.received_at_ms,
            payload.payload,
        )
        .map_err(|_| GridVenueError::PublicPayload)?;
        match crate::exchange::binance::normalize(
            &record,
            &crate::exchange::binance::native_symbol(&self.symbol),
        )
        .map_err(|_| GridVenueError::PublicParse)?
        {
            MarketEvent::Snapshot(snapshot) if source == RawSource::RestSnapshot => {
                self.book.apply_snapshot(snapshot);
                self.last_ready_exchange_time_ms = None;
                Ok(())
            }
            MarketEvent::Delta(delta) if source == RawSource::WebSocketDelta => {
                let exchange_time_ms = delta
                    .exchange_time_ms
                    .ok_or(GridVenueError::PublicPayload)?;
                if self
                    .book
                    .apply_delta_if_fresh(delta)
                    .map_err(|_| GridVenueError::PublicSequence)?
                {
                    self.last_ready_exchange_time_ms = Some(exchange_time_ms);
                }
                Ok(())
            }
            _ => Err(GridVenueError::PublicPayload),
        }
    }

    pub(super) fn best_bid_ask(&self, now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        if !self.book.bridged()
            || self
                .last_ready_exchange_time_ms
                .is_none_or(|exchange_time_ms| {
                    now_ms < exchange_time_ms || now_ms - exchange_time_ms > PUBLIC_FRESHNESS_MS
                })
        {
            return Err(GridVenueError::PublicNotReady);
        }
        let bid = self
            .book
            .bids()
            .first()
            .map(|level| level.price)
            .ok_or(GridVenueError::PublicNotReady)?;
        let ask = self
            .book
            .asks()
            .first()
            .map(|level| level.price)
            .ok_or(GridVenueError::PublicNotReady)?;
        if bid >= ask {
            return Err(GridVenueError::PublicNotReady);
        }
        Ok((bid, ask))
    }
}

#[derive(Debug)]
pub(super) struct GatePublicMarket {
    pub(super) binding: GatePublicBinding,
    bridge: GateOrderBookBridge,
    book: OrderBook,
    last_ready_exchange_time_ms: Option<u64>,
}

impl GatePublicMarket {
    pub(super) fn new(rules: &GateContractRules) -> Result<Self, GridVenueError> {
        let binding = GatePublicBinding::new(
            rules.instrument.symbol.clone(),
            rules.native_symbol.clone(),
            rules.quanto_multiplier,
        )
        .map_err(|_| GridVenueError::PublicPayload)?;
        let bridge = GateOrderBookBridge::new(binding.clone(), 1, GATE_PUBLIC_MAX_BUFFERED_DELTAS)
            .map_err(|_| GridVenueError::PublicPayload)?;
        Ok(Self {
            binding,
            bridge,
            book: OrderBook::default(),
            last_ready_exchange_time_ms: None,
        })
    }

    pub(super) fn generation(&self) -> u64 {
        self.bridge.generation()
    }

    pub(super) fn seed_generation(
        &mut self,
        minimum_generation: u64,
    ) -> Result<(), GridVenueError> {
        if self.generation() >= minimum_generation {
            return Ok(());
        }
        self.bridge
            .reset_generation(minimum_generation)
            .map_err(|_| GridVenueError::PublicPayload)?;
        self.book = OrderBook::default();
        self.last_ready_exchange_time_ms = None;
        Ok(())
    }

    pub(super) fn reset(&mut self) -> Result<(), GridVenueError> {
        let generation = self
            .generation()
            .checked_add(1)
            .ok_or(GridVenueError::PublicPayload)?;
        self.bridge
            .reset_generation(generation)
            .map_err(|_| GridVenueError::PublicPayload)?;
        self.book = OrderBook::default();
        self.last_ready_exchange_time_ms = None;
        Ok(())
    }

    pub(super) fn accept(&mut self, payload: GridPublicPayload) -> Result<(), GridVenueError> {
        if payload.generation != self.generation() {
            return Err(GridVenueError::PublicPayload);
        }
        match payload.source {
            GridPublicPayloadSource::RestSnapshot => {
                let raw = GatePublicRawPayload::new(
                    &self.binding,
                    GatePublicPayloadKind::RestOrderBookSnapshot,
                    payload.generation,
                    payload.received_at_ms,
                    payload.payload,
                )
                .map_err(|_| GridVenueError::PublicParse)?;
                let snapshot = parse_rest_snapshot(&self.binding, raw)
                    .map_err(|_| GridVenueError::PublicParse)?;
                let actions = self
                    .bridge
                    .receive_snapshot(snapshot)
                    .map_err(|_| GridVenueError::PublicSequence)?;
                self.apply_bridge_actions(actions)
            }
            GridPublicPayloadSource::RestTicker => Err(GridVenueError::PublicPayload),
            GridPublicPayloadSource::WebSocketDepth => {
                let raw = GatePublicRawPayload::new(
                    &self.binding,
                    GatePublicPayloadKind::WebSocketOrderBookDelta,
                    payload.generation,
                    payload.received_at_ms,
                    payload.payload,
                )
                .map_err(|_| GridVenueError::PublicParse)?;
                let delta =
                    parse_ws_delta(&self.binding, raw).map_err(|_| GridVenueError::PublicParse)?;
                let actions = self
                    .bridge
                    .receive_delta(delta)
                    .map_err(|_| GridVenueError::PublicSequence)?;
                self.apply_bridge_actions(actions)
            }
            GridPublicPayloadSource::WebSocketBbo => {
                let raw = GatePublicRawPayload::new(
                    &self.binding,
                    GatePublicPayloadKind::WebSocketBookTicker,
                    payload.generation,
                    payload.received_at_ms,
                    payload.payload,
                )
                .map_err(|_| GridVenueError::PublicParse)?;
                let _ = parse_ws_book_ticker(&self.binding, raw)
                    .map_err(|_| GridVenueError::PublicParse)?;
                Ok(())
            }
            GridPublicPayloadSource::WebSocketMark => {
                let raw = GatePublicRawPayload::new(
                    &self.binding,
                    GatePublicPayloadKind::WebSocketTicker,
                    payload.generation,
                    payload.received_at_ms,
                    payload.payload,
                )
                .map_err(|_| GridVenueError::PublicParse)?;
                let _ = parse_ws_mark_price(&self.binding, raw)
                    .map_err(|_| GridVenueError::PublicParse)?;
                Ok(())
            }
            GridPublicPayloadSource::WebSocketTrade => {
                let raw = GatePublicRawPayload::new(
                    &self.binding,
                    GatePublicPayloadKind::WebSocketTrade,
                    payload.generation,
                    payload.received_at_ms,
                    payload.payload,
                )
                .map_err(|_| GridVenueError::PublicParse)?;
                let _ =
                    parse_ws_trades(&self.binding, raw).map_err(|_| GridVenueError::PublicParse)?;
                Ok(())
            }
        }
    }

    pub(super) fn apply_bridge_actions(
        &mut self,
        actions: Vec<GateBookBridgeAction>,
    ) -> Result<(), GridVenueError> {
        for action in actions {
            match action {
                GateBookBridgeAction::Buffered | GateBookBridgeAction::IgnoredStale => {}
                GateBookBridgeAction::ReplaceSnapshot(snapshot) => {
                    self.last_ready_exchange_time_ms = Some(snapshot.freshness.exchange_time_ms);
                    self.book.apply_snapshot(snapshot.value);
                }
                GateBookBridgeAction::ApplyDelta(delta) => {
                    self.last_ready_exchange_time_ms = Some(delta.freshness.exchange_time_ms);
                    let book_generation = self.book.generation();
                    let book_sequence = self.book.sequence();
                    // Gate's U/u range is already validated by `GateOrderBookBridge`.  The
                    // generic `OrderBook` also accepts an exact predecessor marker, so make
                    // that already-proven continuity explicit instead of applying a second,
                    // venue-specific interpretation of the inclusive range.
                    let mut value = delta.value;
                    value.previous_sequence = book_sequence;
                    let delta_generation = value.generation;
                    let first_sequence = value.first_sequence;
                    let sequence = value.sequence;
                    self.book
                        .apply_delta(value)
                        .map_err(|_| GridVenueError::PublicBook {
                            book_generation,
                            book_sequence,
                            delta_generation,
                            first_sequence,
                            sequence,
                        })?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn best_bid_ask(&self, now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        if !self.bridge.is_ready()
            || self
                .last_ready_exchange_time_ms
                .is_none_or(|exchange_time_ms| {
                    now_ms < exchange_time_ms || now_ms - exchange_time_ms > PUBLIC_FRESHNESS_MS
                })
        {
            return Err(GridVenueError::PublicNotReady);
        }
        let bid = self
            .book
            .bids()
            .first()
            .map(|level| level.price)
            .ok_or(GridVenueError::PublicNotReady)?;
        let ask = self
            .book
            .asks()
            .first()
            .map(|level| level.price)
            .ok_or(GridVenueError::PublicNotReady)?;
        if bid >= ask {
            return Err(GridVenueError::PublicNotReady);
        }
        Ok((bid, ask))
    }
}

#[derive(Debug)]
pub(super) struct BitgetPublicMarket {
    symbol: Symbol,
    sequencer: BitgetBookSequencer,
    transport_generation: u64,
    generation_poisoned: bool,
    book: OrderBook,
    last_ready_exchange_time_ms: Option<u64>,
}

impl BitgetPublicMarket {
    pub(super) fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            sequencer: BitgetBookSequencer::new(),
            transport_generation: 1,
            generation_poisoned: false,
            book: OrderBook::default(),
            last_ready_exchange_time_ms: None,
        }
    }

    pub(super) fn generation(&self) -> u64 {
        self.transport_generation
    }

    pub(super) fn seed_generation(
        &mut self,
        minimum_generation: u64,
    ) -> Result<(), GridVenueError> {
        if self.generation_poisoned {
            return Err(GridVenueError::PublicSequence);
        }
        if self.generation() >= minimum_generation {
            return Ok(());
        }
        self.sequencer
            .reset_generation(minimum_generation)
            .map_err(|_| GridVenueError::PublicPayload)?;
        self.transport_generation = minimum_generation;
        self.book = OrderBook::default();
        self.last_ready_exchange_time_ms = None;
        Ok(())
    }

    pub(super) fn reset(&mut self) {
        // Every transport connection owns one immutable generation. PublicRuntime fsyncs a
        // bounded batch before parsing it, so a replacement books snapshot must not advance the
        // generation underneath later publicTrade frames captured from that same connection.
        // Advance once here, before reconnect, and clear the sequencer's prior active snapshot.
        let current = self
            .transport_generation
            .max(self.sequencer.next_generation());
        let advanced = current
            .checked_add(1)
            .and_then(|next| self.sequencer.reset_generation(next).ok().map(|()| next));
        match advanced {
            Some(next) => self.transport_generation = next,
            None => self.generation_poisoned = true,
        }
        self.clear_book();
    }

    pub(super) fn clear_book(&mut self) {
        self.book = OrderBook::default();
        self.last_ready_exchange_time_ms = None;
    }

    pub(super) fn accept(&mut self, payload: GridPublicPayload) -> Result<(), GridVenueError> {
        if self.generation_poisoned || payload.generation != self.generation() {
            return Err(GridVenueError::PublicPayload);
        }
        match payload.source {
            GridPublicPayloadSource::RestSnapshot => {
                let raw = BitgetRawPublicPayload::new(
                    BitgetPublicSource::RestOrderBook,
                    self.symbol.clone(),
                    payload.generation,
                    payload.received_at_ms,
                    payload.payload,
                )
                .map_err(|_| GridVenueError::PublicPayload)?;
                let _ = parse_rest_orderbook(raw).map_err(|_| GridVenueError::PublicPayload)?;
                Ok(())
            }
            GridPublicPayloadSource::RestTicker => {
                let raw = BitgetRawPublicPayload::new(
                    BitgetPublicSource::RestTicker,
                    self.symbol.clone(),
                    payload.generation,
                    payload.received_at_ms,
                    payload.payload,
                )
                .map_err(|_| GridVenueError::PublicPayload)?;
                let _ = parse_rest_ticker(raw).map_err(|_| GridVenueError::PublicPayload)?;
                Ok(())
            }
            GridPublicPayloadSource::WebSocketDepth => {
                let raw = BitgetRawPublicPayload::new(
                    BitgetPublicSource::WebSocketBooks,
                    self.symbol.clone(),
                    payload.generation,
                    payload.received_at_ms,
                    payload.payload,
                )
                .map_err(|_| GridVenueError::PublicPayload)?;
                let message =
                    parse_books_message(raw).map_err(|_| GridVenueError::PublicPayload)?;
                let status = self
                    .sequencer
                    .accept(&message)
                    .map_err(|_| GridVenueError::PublicPayload)?;
                self.apply_book(status, &message)
            }
            GridPublicPayloadSource::WebSocketTrade => {
                let raw = BitgetRawPublicPayload::new(
                    BitgetPublicSource::WebSocketPublicTrade,
                    self.symbol.clone(),
                    payload.generation,
                    payload.received_at_ms,
                    payload.payload,
                )
                .map_err(|_| GridVenueError::PublicPayload)?;
                let _ =
                    parse_public_trade_message(raw).map_err(|_| GridVenueError::PublicPayload)?;
                Ok(())
            }
            GridPublicPayloadSource::WebSocketBbo | GridPublicPayloadSource::WebSocketMark => {
                Err(GridVenueError::PublicPayload)
            }
        }
    }

    pub(super) fn apply_book(
        &mut self,
        status: BitgetBookSequenceStatus,
        message: &crate::exchange::bitget_public::BitgetBooksMessage,
    ) -> Result<(), GridVenueError> {
        let Some(generation) = status.active_generation() else {
            // The surrounding PublicRuntime resets the transport after this error. Only clear
            // the unusable book here so that transport generation advances exactly once.
            self.clear_book();
            return Err(GridVenueError::PublicPayload);
        };
        match message
            .normalize(generation)
            .map_err(|_| GridVenueError::PublicPayload)?
        {
            MarketEvent::Snapshot(snapshot) => {
                self.book.apply_snapshot(snapshot);
                self.last_ready_exchange_time_ms = None;
            }
            MarketEvent::Delta(delta) => {
                self.book
                    .apply_delta(delta)
                    .map_err(|_| GridVenueError::PublicPayload)?;
                if status.ready() {
                    self.last_ready_exchange_time_ms = Some(message.exchange_time_ms);
                }
            }
            MarketEvent::Ticker(_)
            | MarketEvent::Trade(_)
            | MarketEvent::Bar(_)
            | MarketEvent::MarkFunding(_) => return Err(GridVenueError::PublicPayload),
        }
        Ok(())
    }

    pub(super) fn best_bid_ask(&self, now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        if self.sequencer.ready_generation().is_none()
            || self
                .last_ready_exchange_time_ms
                .is_none_or(|exchange_time_ms| {
                    now_ms < exchange_time_ms || now_ms - exchange_time_ms > PUBLIC_FRESHNESS_MS
                })
        {
            return Err(GridVenueError::PublicNotReady);
        }
        let bid = self
            .book
            .bids()
            .first()
            .map(|level| level.price)
            .ok_or(GridVenueError::PublicNotReady)?;
        let ask = self
            .book
            .asks()
            .first()
            .map(|level| level.price)
            .ok_or(GridVenueError::PublicNotReady)?;
        if bid >= ask {
            return Err(GridVenueError::PublicNotReady);
        }
        Ok((bid, ask))
    }
}
