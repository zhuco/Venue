use venue_gateway_api::{GatewayBinding, GatewayMode};

use crate::{OkxGatewayBinding, OkxGatewayBindingError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OkxConfig {
    binding: OkxGatewayBinding,
    rest_origin: &'static str,
    public_ws: &'static str,
    private_ws: &'static str,
}

impl OkxConfig {
    pub fn for_binding(binding: GatewayBinding) -> Result<Self, OkxGatewayBindingError> {
        let binding = OkxGatewayBinding::new(binding)?;
        let config = Self {
            binding,
            rest_origin: "https://www.okx.com",
            public_ws: "wss://ws.okx.com:8443/ws/v5/public",
            private_ws: "wss://ws.okx.com:8443/ws/v5/private",
        };
        Ok(config)
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        self.binding.gateway_binding()
    }

    #[must_use]
    pub const fn mode(&self) -> GatewayMode {
        self.gateway_binding().mode
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
    pub const OPEN_ALGO_ORDERS: &str = "/api/v5/trade/orders-algo-pending";
    pub const FILLS_HISTORY: &str = "/api/v5/trade/fills-history";
}
