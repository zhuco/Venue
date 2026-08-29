use venue_gateway_api::GatewayMode;

use crate::GateProtocolError;

/// Gate transport origins selected only by the validated TEST/LIVE gateway mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GateConfig {
    pub rest_origin: &'static str,
    pub usdt_futures_ws: &'static str,
    pub testnet: bool,
}

impl GateConfig {
    #[must_use]
    pub const fn for_mode(mode: GatewayMode) -> Self {
        match mode {
            GatewayMode::Test => Self {
                rest_origin: "https://api-testnet.gateapi.io/api/v4",
                usdt_futures_ws: "wss://ws-testnet.gate.com/v4/ws/futures/usdt",
                testnet: true,
            },
            GatewayMode::Live => Self {
                rest_origin: "https://api.gateio.ws/api/v4",
                usdt_futures_ws: "wss://fx-ws.gateio.ws/v4/ws/usdt",
                testnet: false,
            },
        }
    }
}

/// The only Gate product scope currently admitted by this gateway.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GateProductScope;

impl GateProductScope {
    pub fn usdt_perpetual(settlement: &str, delivery: bool) -> Result<Self, GateProtocolError> {
        if settlement == "usdt" && !delivery {
            Ok(Self)
        } else {
            Err(GateProtocolError::ProductScope)
        }
    }

    #[must_use]
    pub const fn settlement(self) -> &'static str {
        "usdt"
    }
}
