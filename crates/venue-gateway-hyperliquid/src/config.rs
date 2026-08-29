use venue_gateway_api::GatewayMode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HyperliquidConfig {
    pub rest_origin: &'static str,
    pub websocket: &'static str,
}

impl HyperliquidConfig {
    #[must_use]
    pub const fn for_mode(mode: GatewayMode) -> Self {
        match mode {
            GatewayMode::Test => Self {
                rest_origin: "https://api.hyperliquid-testnet.xyz",
                websocket: "wss://api.hyperliquid-testnet.xyz/ws",
            },
            GatewayMode::Live => Self {
                rest_origin: "https://api.hyperliquid.xyz",
                websocket: "wss://api.hyperliquid.xyz/ws",
            },
        }
    }
}

pub mod endpoints {
    pub const INFO: &str = "/info";
    pub const EXCHANGE: &str = "/exchange";
}
