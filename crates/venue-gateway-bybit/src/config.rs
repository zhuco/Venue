use venue_gateway_api::GatewayMode;

const RECV_WINDOW_MS: u64 = 5_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BybitConfig {
    pub rest_origin: &'static str,
    pub public_ws: &'static str,
    pub private_ws: &'static str,
    pub recv_window_ms: u64,
}

impl BybitConfig {
    #[must_use]
    pub const fn for_mode(mode: GatewayMode) -> Self {
        match mode {
            GatewayMode::Test => Self {
                rest_origin: "https://api-testnet.bybit.com",
                public_ws: "wss://stream-testnet.bybit.com/v5/public/linear",
                private_ws: "wss://stream-testnet.bybit.com/v5/private",
                recv_window_ms: RECV_WINDOW_MS,
            },
            GatewayMode::Live => Self {
                rest_origin: "https://api.bybit.com",
                public_ws: "wss://stream.bybit.com/v5/public/linear",
                private_ws: "wss://stream.bybit.com/v5/private",
                recv_window_ms: RECV_WINDOW_MS,
            },
        }
    }
}

pub mod endpoints {
    pub const TIME: &str = "/v5/market/time";
    pub const INSTRUMENTS: &str = "/v5/market/instruments-info";
    pub const ACCOUNT_INFO: &str = "/v5/account/info";
    pub const API_INFO: &str = "/v5/user/query-api";
    pub const BALANCES: &str = "/v5/account/wallet-balance";
    pub const POSITIONS: &str = "/v5/position/list";
    pub const PLACE_ORDER: &str = "/v5/order/create";
    pub const AMEND_ORDER: &str = "/v5/order/amend";
    pub const CANCEL_ORDER: &str = "/v5/order/cancel";
    pub const OPEN_ORDERS: &str = "/v5/order/realtime";
    pub const ORDER_HISTORY: &str = "/v5/order/history";
    pub const EXECUTIONS: &str = "/v5/execution/list";
}
