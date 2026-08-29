use venue_gateway_api::GatewayMode;

use crate::{GateProtocolError, endpoints};

/// Gate transport origins selected only by the validated TEST/LIVE gateway mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GateConfig {
    mode: GatewayMode,
    rest_origin: &'static str,
    usdt_futures_ws: &'static str,
}

impl GateConfig {
    #[must_use]
    pub const fn for_mode(mode: GatewayMode) -> Self {
        match mode {
            GatewayMode::Test => Self {
                mode,
                rest_origin: "https://api-testnet.gateapi.io/api/v4",
                usdt_futures_ws: "wss://ws-testnet.gate.com/v4/ws/futures/usdt",
            },
            GatewayMode::Live => Self {
                mode,
                rest_origin: "https://api.gateio.ws/api/v4",
                usdt_futures_ws: "wss://fx-ws.gateio.ws/v4/ws/usdt",
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
    pub const fn usdt_futures_ws(&self) -> &'static str {
        self.usdt_futures_ws
    }

    #[must_use]
    pub const fn testnet(&self) -> bool {
        matches!(self.mode, GatewayMode::Test)
    }

    pub fn rest_url(&self, endpoint: &str) -> Result<String, GateProtocolError> {
        let canonical_path = endpoints::canonical_rest_path(endpoint)?;
        let host = self
            .rest_origin
            .strip_suffix(endpoints::API_PREFIX)
            .ok_or(GateProtocolError::SigningInput)?;
        Ok(format!("{host}{canonical_path}"))
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
