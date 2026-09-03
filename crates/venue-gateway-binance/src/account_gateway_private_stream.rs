use std::{
    collections::BTreeSet,
    str,
    time::{Duration, Instant},
};

use rust_decimal::Decimal;
use serde_json::Value;
use venue_domain::domain::{FieldState, Fill, OrderState};
use venue_gateway_api::GatewayBinding;

use super::BinanceAccountGatewayError;
use crate::BinanceRawPrivateFrame;

pub(super) const PRIVATE_STREAM_MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct PrivateStreamReconnectState {
    retry_at: Option<Instant>,
    failures: u32,
    outage_reported: bool,
}

impl PrivateStreamReconnectState {
    pub(super) fn waiting(&self, now: Instant) -> bool {
        self.retry_at.is_some_and(|deadline| now < deadline)
    }

    /// Returns true only for the first failure in one outage generation. The caller uses that
    /// edge to request one signed supervision; later retries stay read-only until a valid frame.
    pub(super) fn record_failure(
        &mut self,
        now: Instant,
        connection_generation: u64,
        private_generation: u64,
    ) -> bool {
        self.failures = self.failures.saturating_add(1);
        self.retry_at = now.checked_add(private_stream_reconnect_delay(
            connection_generation,
            private_generation,
            self.failures,
        ));
        let first = !self.outage_reported;
        self.outage_reported = true;
        first
    }

    pub(super) fn record_valid_frame(&mut self) {
        self.retry_at = None;
        self.failures = 0;
        self.outage_reported = false;
    }

    /// A completed websocket handshake establishes a new loss boundary. Keep the accumulated
    /// failure count until one valid account frame arrives, but require a newly established
    /// socket loss to trigger its own signed reconciliation edge.
    pub(super) fn record_connected(&mut self) {
        self.retry_at = None;
        self.outage_reported = false;
    }

    #[cfg(test)]
    pub(super) fn retry_deadline(&self) -> Option<Instant> {
        self.retry_at
    }
}

pub(super) fn private_stream_reconnect_delay(
    connection_generation: u64,
    private_generation: u64,
    failures: u32,
) -> Duration {
    let exponent = failures.saturating_sub(1).min(6);
    let base_ms = 1_000_u64.saturating_mul(1_u64 << exponent);
    let jitter_seed = connection_generation
        ^ private_generation.rotate_left(17)
        ^ u64::from(failures).rotate_left(31);
    let jitter_ms = jitter_seed % (base_ms / 4 + 1);
    Duration::from_millis(base_ms.saturating_add(jitter_ms)).min(PRIVATE_STREAM_MAX_RECONNECT_DELAY)
}

/// Sanitized authenticated execution evidence. Native frames and listen keys stay in the
/// adapter; the account runtime still persists the resulting domain fact before strategy use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinancePrivateFillEvent {
    /// Process-local generation of the authenticated socket that carried the raw frame.
    pub stream_private_generation: u64,
    /// Latest complete signed account generation under which the adapter admitted the frame.
    pub private_generation: u64,
    pub received_at_ms: u64,
    pub fill: Fill,
    pub client_order_id: FieldState<String>,
    pub original_quantity: FieldState<Decimal>,
    pub cumulative_filled_quantity: FieldState<Decimal>,
    pub order_state: FieldState<OrderState>,
}

impl BinancePrivateFillEvent {
    /// Returns the exact authenticated order progress required by the Grid fast path. A normal
    /// KOL fill may still be consumed without it, but must not be used to infer order completion.
    #[must_use]
    pub fn complete_order_progress(&self) -> Option<(Decimal, Decimal, OrderState)> {
        match (
            &self.original_quantity,
            &self.cumulative_filled_quantity,
            &self.order_state,
        ) {
            (
                FieldState::Known(original),
                FieldState::Known(cumulative),
                FieldState::Known(state @ (OrderState::PartiallyFilled | OrderState::Filled)),
            ) => Some((*original, *cumulative, *state)),
            _ => None,
        }
    }

    #[must_use]
    pub fn native_order_id(&self) -> &str {
        &self.fill.order_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinancePrivateAccountEvent {
    Fill(BinancePrivateFillEvent),
    ReconcileRequired {
        stream_private_generation: u64,
        private_generation: u64,
        received_at_ms: u64,
    },
}

#[cfg(test)]
pub(super) fn normalize_private_stream_event(
    frame: BinanceRawPrivateFrame,
    binding: &GatewayBinding,
    rules_generation: u64,
    stream_private_generation: u64,
    active_private_generation: u64,
) -> Result<Option<BinancePrivateAccountEvent>, BinanceAccountGatewayError> {
    normalize_private_stream_event_for_symbols(
        frame,
        binding,
        &BTreeSet::from([binding.symbol.clone()]),
        rules_generation,
        stream_private_generation,
        active_private_generation,
    )
}

pub(super) fn normalize_private_stream_event_for_symbols(
    frame: BinanceRawPrivateFrame,
    binding: &GatewayBinding,
    symbols: &BTreeSet<venue_domain::domain::Symbol>,
    rules_generation: u64,
    stream_private_generation: u64,
    active_private_generation: u64,
) -> Result<Option<BinancePrivateAccountEvent>, BinanceAccountGatewayError> {
    if frame.binding != *binding
        || frame.instrument_generation != rules_generation
        || frame.private_generation != stream_private_generation
        || stream_private_generation == 0
        || active_private_generation < stream_private_generation
        || frame.received_at_ms == 0
    {
        return Err(BinanceAccountGatewayError::PrivateStream);
    }
    let payload =
        str::from_utf8(&frame.payload).map_err(|_| BinanceAccountGatewayError::PrivateStream)?;
    let value: Value =
        serde_json::from_str(payload).map_err(|_| BinanceAccountGatewayError::PrivateStream)?;
    let event = value
        .get("e")
        .and_then(Value::as_str)
        .ok_or(BinanceAccountGatewayError::PrivateStream)?;
    if event == "listenKeyExpired" {
        return Err(BinanceAccountGatewayError::PrivateStream);
    }
    let stream_symbol = value
        .get("o")
        .and_then(Value::as_object)
        .and_then(|order| order.get("s"))
        .and_then(Value::as_str)
        .and_then(|native| {
            symbols
                .iter()
                .find(|symbol| crate::native_symbol(symbol) == native)
        });
    let Some(Some(stream)) = stream_symbol
        .map(|symbol| crate::private::parse_stream_fill(payload, symbol))
        .transpose()
        .map_err(|_| BinanceAccountGatewayError::PrivateStream)?
    else {
        let reconcile = match event {
            "ORDER_TRADE_UPDATE" => {
                let order = value
                    .get("o")
                    .and_then(Value::as_object)
                    .ok_or(BinanceAccountGatewayError::PrivateStream)?;
                let execution = order
                    .get("x")
                    .and_then(Value::as_str)
                    .ok_or(BinanceAccountGatewayError::PrivateStream)?;
                let status = order.get("X").and_then(Value::as_str);
                execution != "NEW"
                    || status.is_some_and(|status| {
                        matches!(
                            status,
                            "CANCELED" | "EXPIRED" | "REJECTED" | "EXPIRED_IN_MATCH" | "FILLED"
                        )
                    })
            }
            // Other authenticated account events can change inventory, order semantics,
            // leverage, liquidation custody, or algo orders and therefore require signed facts.
            _ => true,
        };
        return Ok(
            reconcile.then_some(BinancePrivateAccountEvent::ReconcileRequired {
                stream_private_generation: frame.private_generation,
                private_generation: active_private_generation,
                received_at_ms: frame.received_at_ms,
            }),
        );
    };
    if !symbols.contains(&stream.fill.symbol) || stream.fill.fill_id.trim().is_empty() {
        return Err(BinanceAccountGatewayError::PrivateStream);
    }
    Ok(Some(BinancePrivateAccountEvent::Fill(
        BinancePrivateFillEvent {
            stream_private_generation: frame.private_generation,
            private_generation: active_private_generation,
            received_at_ms: frame.received_at_ms,
            fill: stream.fill,
            client_order_id: stream.client_order_id,
            original_quantity: stream.original_quantity,
            cumulative_filled_quantity: stream.cumulative_filled_quantity,
            order_state: stream.order_state,
        },
    )))
}
