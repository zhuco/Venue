use venue_gateway_api::GatewayMode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OkxConfig {
    pub rest_origin: &'static str,
    pub public_ws: &'static str,
    pub private_ws: &'static str,
    pub simulated_trading: bool,
}

impl OkxConfig {
    #[must_use]
    pub const fn for_mode(mode: GatewayMode) -> Self {
        match mode {
            GatewayMode::Test => Self {
                rest_origin: "https://www.okx.com",
                public_ws: "wss://wspap.okx.com:8443/ws/v5/public",
                private_ws: "wss://wspap.okx.com:8443/ws/v5/private",
                simulated_trading: true,
            },
            GatewayMode::Live => Self {
                rest_origin: "https://www.okx.com",
                public_ws: "wss://ws.okx.com:8443/ws/v5/public",
                private_ws: "wss://ws.okx.com:8443/ws/v5/private",
                simulated_trading: false,
            },
        }
    }
}

pub mod endpoints {
    pub const TIME: &str = "/api/v5/public/time";
    pub const INSTRUMENTS: &str = "/api/v5/public/instruments";
    pub const ACCOUNT_CONFIG: &str = "/api/v5/account/config";
    pub const BALANCES: &str = "/api/v5/account/balance";
    pub const POSITIONS: &str = "/api/v5/account/positions";
    pub const PLACE_ORDER: &str = "/api/v5/trade/order";
    pub const AMEND_ORDER: &str = "/api/v5/trade/amend-order";
    pub const CANCEL_ORDER: &str = "/api/v5/trade/cancel-order";
    pub const OPEN_ORDERS: &str = "/api/v5/trade/orders-pending";
    pub const FILLS_HISTORY: &str = "/api/v5/trade/fills-history";
}
