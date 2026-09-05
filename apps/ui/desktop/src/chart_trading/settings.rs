use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChartTradingSettings {
    pub quick_order: bool,
    pub quick_buy: bool,
    pub quick_sell: bool,
    pub quick_amount: bool,
    pub current_orders: bool,
    pub order_quantity: bool,
    pub order_lines: bool,
    pub positions: bool,
    pub history: bool,
    pub liquidation: bool,
    pub alerts: bool,
    pub price_lines: bool,
    pub last_price: bool,
    pub mark_price: bool,
    pub bid_ask: bool,
    pub price_labels: bool,
    pub ticks: bool,
    pub tick_prices: bool,
    pub tick_orders: bool,
    pub tick_positions: bool,
    pub order_preview: bool,
}

impl Default for ChartTradingSettings {
    fn default() -> Self {
        Self {
            quick_order: true,
            quick_buy: true,
            quick_sell: true,
            quick_amount: true,
            current_orders: true,
            order_quantity: true,
            order_lines: true,
            positions: true,
            history: true,
            liquidation: false,
            alerts: false,
            price_lines: true,
            last_price: true,
            mark_price: false,
            bid_ask: false,
            price_labels: false,
            ticks: false,
            tick_prices: true,
            tick_orders: true,
            tick_positions: true,
            order_preview: false,
        }
    }
}
