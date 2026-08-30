use venue_gateway_api::GatewayMode;

use crate::HyperliquidGatewayBinding;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HyperliquidConfig {
    mode: GatewayMode,
    rest_origin: &'static str,
    websocket: &'static str,
}

impl HyperliquidConfig {
    #[must_use]
    pub const fn for_binding(binding: &HyperliquidGatewayBinding) -> Self {
        let _ = binding;
        Self {
            mode: GatewayMode::Live,
            rest_origin: "https://api.hyperliquid.xyz",
            websocket: "wss://api.hyperliquid.xyz/ws",
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
    pub const fn websocket(&self) -> &'static str {
        self.websocket
    }
}

pub mod endpoints {
    pub const INFO: &str = "/info";
    pub const EXCHANGE: &str = "/exchange";
}
