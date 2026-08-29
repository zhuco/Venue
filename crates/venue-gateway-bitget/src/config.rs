use venue_gateway_api::GatewayMode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitgetConfig {
    pub rest_origin: &'static str,
    pub public_ws: &'static str,
    pub private_ws: &'static str,
    pub paper_trading: bool,
}

impl BitgetConfig {
    #[must_use]
    pub const fn for_mode(mode: GatewayMode) -> Self {
        match mode {
            GatewayMode::Test => Self {
                rest_origin: "https://api.bitget.com",
                public_ws: "wss://wspap.bitget.com/v3/ws/public",
                private_ws: "wss://wspap.bitget.com/v3/ws/private",
                paper_trading: true,
            },
            GatewayMode::Live => Self {
                rest_origin: "https://api.bitget.com",
                public_ws: "wss://ws.bitget.com/v3/ws/public",
                private_ws: "wss://ws.bitget.com/v3/ws/private",
                paper_trading: false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_exactly_demo_and_live_is_exactly_production() {
        let test = BitgetConfig::for_mode(GatewayMode::Test);
        assert_eq!(test.rest_origin, "https://api.bitget.com");
        assert_eq!(test.public_ws, "wss://wspap.bitget.com/v3/ws/public");
        assert_eq!(test.private_ws, "wss://wspap.bitget.com/v3/ws/private");
        assert!(test.paper_trading);

        let live = BitgetConfig::for_mode(GatewayMode::Live);
        assert_eq!(live.rest_origin, "https://api.bitget.com");
        assert_eq!(live.public_ws, "wss://ws.bitget.com/v3/ws/public");
        assert_eq!(live.private_ws, "wss://ws.bitget.com/v3/ws/private");
        assert!(!live.paper_trading);
    }
}
