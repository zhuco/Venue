use rust_decimal::Decimal;
use venue_domain::domain::{FieldState, Fill, OrderSide, OrderState, PositionSide, Price};
use venue_gateway_binance::BinancePrivateFillEvent;

pub(super) fn private_stream_fill(
    fill_id: &str,
    received_at_ms: u64,
    cumulative: Decimal,
    state: OrderState,
) -> Result<BinancePrivateFillEvent, Box<dyn std::error::Error>> {
    Ok(BinancePrivateFillEvent {
        stream_private_generation: 3,
        private_generation: 3,
        received_at_ms,
        fill: Fill {
            fill_id: fill_id.to_owned(),
            execution_sequence: FieldState::Known(received_at_ms),
            order_id: "native-order-batch".to_owned(),
            symbol: "BTC/USDT".parse()?,
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: Decimal::new(1, 3),
            price: Price::new(Decimal::new(50_000, 0))?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(received_at_ms - 1),
        },
        client_order_id: FieldState::Known("client-batch".to_owned()),
        order_type: FieldState::Missing,
        original_quantity: FieldState::Known(Decimal::new(2, 3)),
        cumulative_filled_quantity: FieldState::Known(cumulative),
        order_state: FieldState::Known(state),
    })
}
