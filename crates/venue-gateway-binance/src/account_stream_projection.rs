//! Read-only account state derived from a REST bootstrap and one uninterrupted authenticated
//! socket. It is deliberately not a recovery journal: restart and gaps require a new bootstrap.

use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::{
    LimitTimeInForce, NativeOrderFamily, OrderSide, OrderState, PositionSide, Symbol,
};
use venue_execution::{SignedAccountOrderFact, SignedAccountPositionFact, SignedAccountSnapshot};

use super::{BinanceAccountGateway, BinanceAccountGatewayError};
use crate::BinanceRawPrivateFrame;

pub(super) struct AccountStreamProjection {
    baseline: SignedAccountSnapshot,
    orders: BTreeMap<String, SignedAccountOrderFact>,
    positions: BTreeMap<(Symbol, PositionSide), SignedAccountPositionFact>,
    expected_quantities: BTreeMap<(Symbol, PositionSide), Decimal>,
    seen_trades: BTreeMap<(Symbol, u64), crate::private::StreamFill>,
    position_times: BTreeMap<(Symbol, PositionSide), u64>,
    trade_times: BTreeMap<(Symbol, PositionSide), u64>,
    trade_cursors: BTreeMap<Symbol, (u64, u64)>,
    event_times: BTreeMap<String, u64>,
    last_published_ms: u64,
    last_change_received_ms: u64,
}

impl AccountStreamProjection {
    pub(super) fn new(baseline: SignedAccountSnapshot) -> Self {
        Self {
            orders: baseline
                .open_orders()
                .iter()
                .cloned()
                .map(|order| (order.client_order_id.clone(), order))
                .collect(),
            positions: baseline
                .positions()
                .iter()
                .cloned()
                .map(|position| ((position.symbol.clone(), position.position_side), position))
                .collect(),
            expected_quantities: baseline
                .positions()
                .iter()
                .map(|position| {
                    (
                        (position.symbol.clone(), position.position_side),
                        position.quantity,
                    )
                })
                .collect(),
            seen_trades: BTreeMap::new(),
            position_times: BTreeMap::new(),
            trade_times: BTreeMap::new(),
            trade_cursors: BTreeMap::new(),
            event_times: BTreeMap::new(),
            last_published_ms: baseline.observed_at_ms(),
            last_change_received_ms: baseline.observed_at_ms(),
            baseline,
        }
    }

    pub(super) fn apply(
        &mut self,
        frame: &BinanceRawPrivateFrame,
        symbols: &BTreeSet<Symbol>,
    ) -> Result<(), BinanceAccountGatewayError> {
        let value: Value = serde_json::from_slice(&frame.payload).map_err(|_| invalid())?;
        let event = text(&value, "e")?;
        if matches!(event, "balanceUpdate" | "outboundAccountPosition") {
            // These change the cached PM equity, not UM orders or positions. Equity remains
            // explicitly timestamped at bootstrap and is verified before profit reduction.
            number(&value, "E")?;
            return Ok(());
        }
        if !matches!(event, "ACCOUNT_UPDATE" | "ORDER_TRADE_UPDATE") {
            return Err(invalid());
        }
        if text(&value, "fs")? != "UM" {
            return Err(invalid());
        }
        let event_ms = number(&value, "E")?;
        let transaction_ms = number(&value, "T")?;
        // Event types have independent ordered clocks. Never compare ACCOUNT_UPDATE's T to
        // ORDER_TRADE_UPDATE's E or infer a missing fill from an idle socket.
        if self
            .event_times
            .get(event)
            .is_some_and(|previous| event_ms < *previous)
        {
            return Err(invalid());
        }
        self.event_times.insert(event.to_owned(), event_ms);
        if event_ms <= self.baseline.observed_at_ms() {
            return Ok(());
        }
        if event == "ACCOUNT_UPDATE" {
            let update = value.get("a").ok_or_else(invalid)?;
            if !matches!(
                text(update, "m")?,
                "ORDER"
                    | "FUNDING_FEE"
                    | "DEPOSIT"
                    | "WITHDRAW"
                    | "ASSET_TRANSFER"
                    | "MARGIN_TRANSFER"
            ) {
                return Err(invalid());
            }
            let positions = match update.get("P") {
                Some(Value::Array(positions)) => positions.as_slice(),
                None if text(update, "m")? != "ORDER" => &[],
                _ => return Err(invalid()),
            };
            if positions.is_empty() && text(update, "m")? == "ORDER" {
                return Err(invalid());
            }
            let mut changed = false;
            for row in positions {
                // The authenticated account stream is shared by every enabled strategy on the
                // account. An unrelated UM leg is not a gap in this projection; only the
                // configured symbols contribute to its order/position continuity proof.
                let Some(symbol) = scoped_symbol(row, symbols)? else {
                    continue;
                };
                let side = position_side(row)?;
                let quantity = decimal(row, "pa")?;
                if (side == PositionSide::Long && quantity < Decimal::ZERO)
                    || (side == PositionSide::Short && quantity > Decimal::ZERO)
                {
                    return Err(invalid());
                }
                let quantity = quantity.abs();
                let entry = decimal(row, "ep")?;
                let key = (symbol.clone(), side);
                // UM position updates carry entry and PnL, not a mark-price observation. Do not
                // manufacture a fresh mark from rounded PnL; the consumer obtains its own mark.
                let mark = self
                    .positions
                    .get(&key)
                    .and_then(|position| position.mark_price);
                if !quantity.is_zero() && entry <= Decimal::ZERO {
                    return Err(invalid());
                }
                if text(update, "m")? == "ORDER" {
                    self.position_times.insert(key.clone(), transaction_ms);
                }
                self.positions.insert(
                    key,
                    SignedAccountPositionFact {
                        symbol,
                        position_side: side,
                        quantity,
                        entry_price: (entry > Decimal::ZERO).then_some(entry),
                        mark_price: mark,
                    },
                );
                changed = true;
            }
            if changed {
                self.last_change_received_ms = frame.received_at_ms;
            }
        } else {
            let order = value.get("o").ok_or_else(invalid)?;
            let Some(symbol) = scoped_symbol(order, symbols)? else {
                return Ok(());
            };
            let side = position_side(order)?;
            let client = text(order, "c")?.to_owned();
            let native = number(order, "i")?.to_string();
            let state = super::snapshot_order_state(text(order, "X")?).map_err(|_| invalid())?;
            let quantity = decimal(order, "q")?;
            let filled = decimal(order, "z")?;
            if quantity <= Decimal::ZERO || filled < Decimal::ZERO || filled > quantity {
                return Err(invalid());
            }
            let prior = self.orders.get(&client);
            if prior.is_some_and(|prior| {
                prior.venue_order_id.as_deref() != Some(native.as_str())
                    || prior.symbol != symbol
                    || prior.position_side != side
                    || prior.quantity != quantity
                    || prior.filled_quantity.is_some_and(|before| before > filled)
            }) {
                return Err(invalid());
            }
            let execution = text(order, "x")?;
            if execution == "TRADE" {
                let id = order.get("t").and_then(Value::as_u64).ok_or_else(invalid)?;
                let payload = std::str::from_utf8(&frame.payload).map_err(|_| invalid())?;
                let normalized = crate::private::parse_stream_fill(payload, &symbol)
                    .map_err(|_| invalid())?
                    .ok_or_else(invalid)?;
                let identity = (symbol.clone(), id);
                if let Some(previous) = self.seen_trades.get(&identity) {
                    return if previous == &normalized {
                        Ok(())
                    } else {
                        Err(invalid())
                    };
                }
                if self
                    .trade_cursors
                    .get(&symbol)
                    .is_some_and(|previous| id <= previous.0)
                {
                    return Err(invalid());
                }
                let key = (symbol.clone(), side);
                let before = self
                    .expected_quantities
                    .get(&key)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                let opening = matches!(
                    (side, normalized.fill.side),
                    (PositionSide::Long, OrderSide::Buy) | (PositionSide::Short, OrderSide::Sell)
                );
                let after = if opening {
                    before.checked_add(normalized.fill.quantity)
                } else {
                    before.checked_sub(normalized.fill.quantity)
                }
                .filter(|quantity| *quantity >= Decimal::ZERO)
                .ok_or_else(invalid)?;
                self.expected_quantities.insert(key, after);
                self.seen_trades.insert(identity, normalized);
                while self.seen_trades.len() > 256 {
                    let oldest = self
                        .seen_trades
                        .iter()
                        .min_by_key(|(_, fill)| fill.fill.exchange_time_ms)
                        .map(|(key, _)| key.clone())
                        .ok_or_else(invalid)?;
                    self.seen_trades.remove(&oldest);
                }
                let previous = self
                    .trade_cursors
                    .entry(symbol.clone())
                    .or_insert((id, transaction_ms));
                if id >= previous.0 {
                    *previous = (id, transaction_ms);
                }
                self.trade_times
                    .insert((symbol.clone(), side), transaction_ms);
            }
            if matches!(state, OrderState::New | OrderState::PartiallyFilled) {
                if text(order, "o")? != "LIMIT" {
                    return Err(invalid());
                }
                let price = decimal(order, "p")?;
                if price <= Decimal::ZERO {
                    return Err(invalid());
                }
                let time_in_force = match text(order, "f")? {
                    "GTX" => LimitTimeInForce::PostOnly,
                    "GTC" => LimitTimeInForce::Gtc,
                    _ => return Err(invalid()),
                };
                let created_at_ms = prior
                    .and_then(|prior| prior.created_at_ms)
                    .or_else(|| (execution == "NEW").then_some(transaction_ms));
                let fact = SignedAccountOrderFact {
                    client_order_id: client.clone(),
                    venue_order_id: Some(native),
                    symbol,
                    family: NativeOrderFamily::UmOrder,
                    side: match text(order, "S")? {
                        "BUY" => OrderSide::Buy,
                        "SELL" => OrderSide::Sell,
                        _ => return Err(invalid()),
                    },
                    position_side: side,
                    quantity,
                    limit_price: Some(price),
                    time_in_force: Some(time_in_force),
                    created_at_ms,
                    reduce_only: order
                        .get("R")
                        .and_then(Value::as_bool)
                        .ok_or_else(invalid)?,
                    owner: None,
                    external: true,
                    state: Some(state),
                    filled_quantity: Some(filled),
                };
                self.orders.insert(client, fact);
                if self.orders.len() > 2_000 {
                    return Err(invalid());
                }
            } else {
                self.orders.remove(&client);
            }
            self.last_change_received_ms = frame.received_at_ms;
        }
        Ok(())
    }

    fn snapshot(
        &self,
        observed_ms: u64,
        private_generation: u64,
    ) -> Result<Option<SignedAccountSnapshot>, BinanceAccountGatewayError> {
        let incomplete = self.trade_times.iter().any(|(key, time)| {
            self.position_times
                .get(key)
                .is_none_or(|position| position < time)
        }) || self.expected_quantities.iter().any(|(key, expected)| {
            self.positions.get(key).map(|position| position.quantity) != Some(*expected)
        }) || self.positions.iter().any(|(key, position)| {
            self.expected_quantities
                .get(key)
                .copied()
                .unwrap_or(Decimal::ZERO)
                != position.quantity
        });
        if incomplete && observed_ms.saturating_sub(self.last_change_received_ms) > 5_000 {
            eprintln!(
                "Authenticated position quantities or trade coverage did not converge: trade_times={:?} position_times={:?}",
                self.trade_times, self.position_times
            );
            return Err(invalid());
        }
        if observed_ms <= self.last_published_ms || incomplete {
            return Ok(None);
        }
        let mut cursor = super::parse_snapshot_fills_cursor(Some(self.baseline.fills_cursor()))
            .map_err(|_| invalid())?;
        for (symbol, (trade_id, time)) in &self.trade_cursors {
            let entry = cursor
                .by_native_symbol
                .entry(crate::native_symbol(symbol))
                .or_insert(super::RecentFillsCursor {
                    observed_through_ms: *time,
                    last_trade_id: None,
                    last_event_time_ms: None,
                });
            if entry
                .last_trade_id
                .is_none_or(|previous| *trade_id >= previous)
            {
                entry.last_trade_id = Some(*trade_id);
                entry.last_event_time_ms = Some(*time);
                entry.observed_through_ms = entry.observed_through_ms.max(*time);
            }
        }
        let snapshot = SignedAccountSnapshot::complete(
            self.baseline.binding().clone(),
            observed_ms,
            self.baseline.connection_generation(),
            private_generation,
            self.baseline.rules_generation(),
            self.baseline.position_mode(),
            self.orders.values().cloned().collect(),
            self.positions.values().cloned().collect(),
            cursor.encode(),
            Vec::new(),
        )
        .and_then(|snapshot| snapshot.with_balances(self.baseline.balances().to_vec()))
        .and_then(|snapshot| snapshot.with_stream_origin(self.baseline.observed_at_ms()))
        .map_err(|_| invalid())?;
        Ok(Some(snapshot))
    }
}

impl BinanceAccountGateway {
    pub fn install_stream_projection(
        &mut self,
        snapshot: SignedAccountSnapshot,
    ) -> Result<(), BinanceAccountGatewayError> {
        if snapshot.binding() != self.config.gateway_binding()
            || snapshot.private_generation() != self.private_generation
            || self.private_stream.is_none()
        {
            return Err(invalid());
        }
        self.stream_projection = Some(AccountStreamProjection::new(snapshot));
        Ok(())
    }

    /// No HTTP. The observation time is the latest real frame/Pong, never the local poll time.
    /// Balances retain the bootstrap value; callers must independently verify PM equity before
    /// using it for a new risk action. The snapshot is a read model, not a dispatch permission.
    pub fn stream_projection_snapshot(
        &self,
    ) -> Result<Option<SignedAccountSnapshot>, BinanceAccountGatewayError> {
        let Some(state) = &self.stream_projection else {
            return Ok(None);
        };
        let Some(stream) = &self.private_stream else {
            return Ok(None);
        };
        state.snapshot(stream.last_received_at_ms(), self.private_generation)
    }

    pub fn accept_stream_projection(&mut self, observed_ms: u64) {
        if let Some(state) = &mut self.stream_projection {
            state.last_published_ms = observed_ms;
        }
    }

    pub fn private_stream_recovery_ready(&self) -> bool {
        self.private_stream.is_some()
    }
}

fn invalid() -> BinanceAccountGatewayError {
    BinanceAccountGatewayError::PrivateStream
}
fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str, BinanceAccountGatewayError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(invalid)
}
fn number(value: &Value, name: &str) -> Result<u64, BinanceAccountGatewayError> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(invalid)
}
fn decimal(value: &Value, name: &str) -> Result<Decimal, BinanceAccountGatewayError> {
    text(value, name)?.parse().map_err(|_| invalid())
}
fn position_side(value: &Value) -> Result<PositionSide, BinanceAccountGatewayError> {
    match text(value, "ps")? {
        "LONG" => Ok(PositionSide::Long),
        "SHORT" => Ok(PositionSide::Short),
        _ => Err(invalid()),
    }
}
fn scoped_symbol(
    value: &Value,
    symbols: &BTreeSet<Symbol>,
) -> Result<Option<Symbol>, BinanceAccountGatewayError> {
    let native = text(value, "s")?;
    Ok(symbols
        .iter()
        .find(|symbol| crate::native_symbol(symbol) == native)
        .cloned())
}

#[cfg(test)]
#[path = "account_stream_projection_tests.rs"]
mod tests;
