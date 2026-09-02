//! KOL-only authenticated stream bridge. It stores normalized fill facts, never native frames.

use std::collections::BTreeSet;

use venue_domain::domain::Symbol;
use venue_gateway_binance::{
    BinanceAccountGateway, BinanceAccountGatewayError, BinanceCredentials,
    BinancePrivateAccountEvent, BinanceTransportLimits, GatewayBinding,
};

use crate::{
    executor_store::{PgExecutorStore, PlannedCopyCommand},
    kol_executor::{BinanceCommandLedgerError, source_fill_from_private},
};

/// One enabled KOL source has one account stream; its finite symbols are only used to validate
/// native frames and fetch the shared rule catalogue. It is not a follower stream.
pub struct BinanceKolPrivateSource {
    kol_user_id: String,
    leader_trading_account_id: String,
    gateway: BinanceAccountGateway,
}

impl BinanceKolPrivateSource {
    pub fn connect(
        kol_user_id: String,
        binding: GatewayBinding,
        symbols: BTreeSet<Symbol>,
        credentials: BinanceCredentials,
        limits: BinanceTransportLimits,
    ) -> Result<Self, BinanceAccountGatewayError> {
        if kol_user_id.is_empty() || binding.trading_account_id.is_empty() || symbols.is_empty() {
            return Err(BinanceAccountGatewayError::Binding);
        }
        let leader_trading_account_id = binding.trading_account_id.clone();
        let gateway = BinanceAccountGateway::connect_with_credentials_for_symbols(
            binding,
            symbols,
            credentials,
            limits,
        )?;
        Ok(Self {
            kol_user_id,
            leader_trading_account_id,
            gateway,
        })
    }

    /// The gateway owns listenKey creation, renewal, redaction, bounded reconnect and signed
    /// gap handling. This bridge intentionally forwards only its normalized event.
    pub fn poll(
        &mut self,
    ) -> Result<Option<BinancePrivateAccountEvent>, BinanceAccountGatewayError> {
        self.gateway.poll_private_fill()
    }

    /// A startup readiness fence: listenKey creation and the authenticated websocket handshake
    /// must succeed before the live executor begins consuming durable copy commands.
    pub fn prime(&mut self) -> Result<(), BinanceAccountGatewayError> {
        self.gateway.prime_private_stream()
    }

    /// Closes a private-stream loss boundary with a fresh signed order/trade/account read.
    /// Only normalized fills cross into the executor; duplicate WS/REST identities are rejected
    /// by the durable native-trade key.
    pub fn reconcile(
        &mut self,
    ) -> Result<Vec<BinancePrivateAccountEvent>, BinanceAccountGatewayError> {
        self.gateway.reconcile_private_stream_gap()
    }

    #[must_use]
    pub fn kol_user_id(&self) -> &str {
        &self.kol_user_id
    }

    #[must_use]
    pub fn leader_trading_account_id(&self) -> &str {
        &self.leader_trading_account_id
    }
}

/// Persists only an authenticated TRADE execution. The unique native trade key in PostgreSQL
/// handles duplicate frames and REST overlap; a stream gap is returned to the caller so it can
/// perform signed order/trade reconciliation without persisting the raw frame.
pub async fn persist_private_event(
    store: &PgExecutorStore,
    source: &BinanceKolPrivateSource,
    event: BinancePrivateAccountEvent,
    now_ms: u64,
) -> Result<Vec<PlannedCopyCommand>, BinanceCommandLedgerError> {
    match event {
        BinancePrivateAccountEvent::Fill(event) => {
            let fill = source_fill_from_private(source.leader_trading_account_id(), &event)?;
            store
                .record_source_fill_and_plan(source.kol_user_id(), &fill, now_ms)
                .await
        }
        BinancePrivateAccountEvent::ReconcileRequired { .. } => Ok(Vec::new()),
    }
}

pub async fn persist_private_event_for_account(
    store: &PgExecutorStore,
    kol_user_id: &str,
    leader_trading_account_id: &str,
    event: BinancePrivateAccountEvent,
    now_ms: u64,
) -> Result<Vec<PlannedCopyCommand>, BinanceCommandLedgerError> {
    match event {
        BinancePrivateAccountEvent::Fill(event) => {
            let fill = source_fill_from_private(leader_trading_account_id, &event)?;
            store
                .record_source_fill_and_plan(kol_user_id, &fill, now_ms)
                .await
        }
        BinancePrivateAccountEvent::ReconcileRequired { .. } => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use venue_domain::domain::{FieldState, Fill, OrderSide, PositionSide, Price};
    use venue_gateway_binance::BinancePrivateFillEvent;

    use super::*;

    #[test]
    fn normalized_partial_trade_preserves_native_identity_without_raw_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let event = BinancePrivateAccountEvent::Fill(BinancePrivateFillEvent {
            stream_private_generation: 2,
            private_generation: 3,
            received_at_ms: 101,
            fill: Fill {
                fill_id: "trade-9".into(),
                execution_sequence: FieldState::Known(9),
                order_id: "order-9".into(),
                symbol: "BTC/USDT".parse()?,
                side: OrderSide::Buy,
                position_side: FieldState::Known(PositionSide::Long),
                quantity: rust_decimal::Decimal::new(5, 4),
                price: Price::new(rust_decimal::Decimal::new(50_000, 0))?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Missing,
                exchange_time_ms: Some(100),
            },
            client_order_id: FieldState::Known("client-9".into()),
        });
        let BinancePrivateAccountEvent::Fill(fill) = event else {
            return Err("fill required".into());
        };
        let normalized = source_fill_from_private("leader", &fill)?;
        assert_eq!(normalized.native_trade_id, "trade-9");
        assert_eq!(normalized.quantity, rust_decimal::Decimal::new(5, 4));
        Ok(())
    }
}
