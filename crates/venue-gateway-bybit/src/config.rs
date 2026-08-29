use venue_gateway_api::GatewayMode;

const RECV_WINDOW_MS: u64 = 5_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BybitConfig {
    mode: GatewayMode,
    rest_origin: &'static str,
    public_ws: &'static str,
    private_ws: &'static str,
    recv_window_ms: u64,
}

impl BybitConfig {
    pub(crate) const fn for_mode(mode: GatewayMode) -> Self {
        match mode {
            GatewayMode::Test => Self {
                mode,
                rest_origin: "https://api-testnet.bybit.com",
                public_ws: "wss://stream-testnet.bybit.com/v5/public/linear",
                private_ws: "wss://stream-testnet.bybit.com/v5/private",
                recv_window_ms: RECV_WINDOW_MS,
            },
            GatewayMode::Live => Self {
                mode,
                rest_origin: "https://api.bybit.com",
                public_ws: "wss://stream.bybit.com/v5/public/linear",
                private_ws: "wss://stream.bybit.com/v5/private",
                recv_window_ms: RECV_WINDOW_MS,
            },
        }
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.mode
    }

    #[must_use]
    pub const fn rest_origin(&self) -> &'static str {
        self.rest_origin
    }

    #[must_use]
    pub const fn public_ws(&self) -> &'static str {
        self.public_ws
    }

    #[must_use]
    pub const fn private_ws(&self) -> &'static str {
        self.private_ws
    }

    #[must_use]
    pub const fn recv_window_ms(&self) -> u64 {
        self.recv_window_ms
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
