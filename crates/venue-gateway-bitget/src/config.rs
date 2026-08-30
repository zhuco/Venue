use venue_gateway_api::GatewayMode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitgetConfig {
    mode: GatewayMode,
    rest_origin: &'static str,
    public_ws: &'static str,
    private_ws: &'static str,
}

impl BitgetConfig {
    #[must_use]
    pub const fn for_mode(mode: GatewayMode) -> Self {
        match mode {
            GatewayMode::Live => Self {
                mode,
                rest_origin: "https://api.bitget.com",
                public_ws: "wss://ws.bitget.com/v3/ws/public",
                private_ws: "wss://ws.bitget.com/v3/ws/private",
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_selects_only_production_endpoints() {
        let live = BitgetConfig::for_mode(GatewayMode::Live);
        assert_eq!(live.rest_origin(), "https://api.bitget.com");
        assert_eq!(live.public_ws(), "wss://ws.bitget.com/v3/ws/public");
        assert_eq!(live.private_ws(), "wss://ws.bitget.com/v3/ws/private");
        assert_eq!(live.mode(), GatewayMode::Live);
    }
}
