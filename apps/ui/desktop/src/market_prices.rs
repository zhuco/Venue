//! One display-price policy for charts, books, alerts and position valuation.
//! Private order, fill, position and asset facts are never inferred here.

use rust_decimal::Decimal;
use venue_domain::PositionSide;

use crate::model::AppModel;

const MAX_PRICE_AGE_MS: u64 = 5_000;

pub(crate) fn now_ms() -> u64 {
    #[cfg(all(not(target_arch = "wasm32"), not(test)))]
    {
        venue_gateway_api::display::received_ms().unwrap_or(0)
    }
    // Offline UI fixtures inject local timestamps without starting the public clock service.
    #[cfg(any(target_arch = "wasm32", test))]
    {
        crate::account_center::now_ms()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct PriceObservation {
    pub value: Decimal,
    pub event_ms: u64,
    pub received_ms: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct MarketPrices {
    pub last: Option<PriceObservation>,
    pub mark: Option<PriceObservation>,
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    book_clock: Option<(u64, u64)>,
}

pub(crate) fn fresh(event: u64, received: u64, now: u64) -> bool {
    [event, received]
        .into_iter()
        .all(|time| time > 0 && time <= now && now - time <= MAX_PRICE_AGE_MS)
}

impl PriceObservation {
    fn fresh(value: Decimal, event_ms: u64, received_ms: u64, now: u64) -> Option<Self> {
        (value > Decimal::ZERO && fresh(event_ms, received_ms, now)).then_some(Self {
            value,
            event_ms,
            received_ms,
        })
    }
}

impl MarketPrices {
    pub fn reference(self) -> Option<PriceObservation> {
        self.last.or(self.mark)
    }

    pub fn reference_price(self) -> Option<Decimal> {
        self.reference().map(|price| price.value)
    }

    pub fn is_mark(self) -> bool {
        self.last.is_none() && self.mark.is_some()
    }

    pub fn position_price(self, side: PositionSide, quantity: Decimal) -> Option<Decimal> {
        match side {
            PositionSide::Short => self.ask,
            PositionSide::Net if quantity < Decimal::ZERO => self.ask,
            _ => self.bid,
        }
        .or(self.reference_price())
    }

    pub(crate) fn observe_last(
        &mut self,
        value: Decimal,
        event_ms: u64,
        received_ms: u64,
        now: u64,
    ) {
        if let Some(price) = PriceObservation::fresh(value, event_ms, received_ms, now)
            && self.last.is_none_or(|previous| {
                (event_ms, received_ms) > (previous.event_ms, previous.received_ms)
            })
        {
            self.last = Some(price);
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observe_view(&mut self, view: &crate::market::LocalMarketView, now: u64) {
        if let (Some(price), Some(event), Some(received)) = (
            view.last,
            view.last_price_event_ms,
            view.last_price_received_ms,
        ) {
            self.observe_last(price, event, received, now);
        }
        self.observe_book(
            view.bid,
            view.ask,
            view.book_event_ms,
            view.book_received_ms,
            now,
        );
    }

    pub(crate) fn observe_book(
        &mut self,
        bid: Option<Decimal>,
        ask: Option<Decimal>,
        event: Option<u64>,
        received: Option<u64>,
        now: u64,
    ) {
        if let (Some(event), Some(received)) = (event, received)
            && fresh(event, received, now)
            && bid.is_none_or(|value| value > Decimal::ZERO)
            && ask.is_none_or(|value| value > Decimal::ZERO)
            && bid.zip(ask).is_none_or(|(bid, ask)| bid < ask)
            && self
                .book_clock
                .is_none_or(|clock| (event, received) > clock)
        {
            self.book_clock = Some((event, received));
            self.bid = bid;
            self.ask = ask;
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn from_store(
    store: &crate::market::LocalMarketStore,
    venue: venue_gateway_api::VenueId,
    symbol: &str,
    now: u64,
) -> MarketPrices {
    let mut prices = MarketPrices::default();
    for view in store.views_for_market(venue, symbol) {
        prices.observe_view(view, now);
    }
    prices
}

impl AppModel {
    /// Tab captions retain the last trade even when it is too old for execution/valuation.
    pub(crate) fn last_trade_for_tab(&self, symbol: &str, now: u64) -> Option<PriceObservation> {
        let mut latest = self.market_prices(symbol, now).last;
        let mut observe = |value: Decimal, event_ms: u64, received_ms: u64| {
            if value > Decimal::ZERO
                && event_ms > 0
                && received_ms > 0
                && event_ms <= now.saturating_add(2_000)
                && received_ms <= now.saturating_add(2_000)
                && latest
                    .is_none_or(|old| (event_ms, received_ms) > (old.event_ms, old.received_ms))
            {
                latest = Some(PriceObservation {
                    value,
                    event_ms,
                    received_ms,
                });
            }
        };
        #[cfg(not(target_arch = "wasm32"))]
        for view in self
            .local_markets
            .views_for_market(self.preferences.market_server.venue(), symbol)
        {
            if let (Some(price), Some(event), Some(received)) = (
                view.last,
                view.last_price_event_ms,
                view.last_price_received_ms,
            ) {
                observe(price, event, received);
            }
        }
        if self.preferences.market_server != crate::model::MarketServer::Hyperliquid
            && let Some(quote) = self.local_quotes.get(symbol)
        {
            observe(quote.last, quote.exchange_time_ms, quote.received_ms);
        }
        latest
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn market_depth(
        &self,
        symbol: &str,
        now: u64,
    ) -> Option<&crate::market::LocalMarketView> {
        self.local_markets
            .views_for_market(self.preferences.market_server.venue(), symbol)
            .filter(|view| {
                view.depth_event_ms
                    .zip(view.depth_received_ms)
                    .is_some_and(|(event, received)| fresh(event, received, now))
            })
            .max_by_key(|view| (view.depth_event_ms, view.depth_received_ms))
    }

    pub(crate) fn market_prices(&self, symbol: &str, now: u64) -> MarketPrices {
        #[cfg(not(target_arch = "wasm32"))]
        let mut prices = from_store(
            &self.local_markets,
            self.preferences.market_server.venue(),
            symbol,
            now,
        );
        #[cfg(target_arch = "wasm32")]
        let mut prices = MarketPrices::default();
        #[cfg(all(target_arch = "wasm32", feature = "preview"))]
        if self.preferences.market_server == crate::model::MarketServer::Binance {
            let public_now = self.browser_market.public_now_ms(now);
            for series in self.browser_market.series_for_symbol(symbol) {
                series.observe_prices(&mut prices, public_now);
            }
        }
        // The ticker collection is cleared when its worker/venue changes. It supplies
        // unsubscribed symbols; a tied stream observation takes precedence.
        if let Some(quote) = self.local_quotes.get(symbol) {
            if self.preferences.market_server == crate::model::MarketServer::Hyperliquid {
                prices.mark = PriceObservation::fresh(
                    quote.last,
                    quote.exchange_time_ms,
                    quote.received_ms,
                    now,
                );
            } else {
                prices.observe_last(quote.last, quote.exchange_time_ms, quote.received_ms, now);
            }
        }
        prices
    }

    pub(crate) fn position_market_price(
        &self,
        position: &venue_control_protocol::kol::TerminalPosition,
        now: u64,
    ) -> Option<Decimal> {
        if self
            .selected_execution_credential()
            .is_none_or(|credential| credential.venue != self.preferences.market_server.venue())
        {
            return position.mark_price;
        }
        self.market_prices(&position.symbol.to_string(), now)
            .position_price(position.position_side, position.quantity)
            .or(position.mark_price)
    }
}

#[cfg(test)]
mod tests;
