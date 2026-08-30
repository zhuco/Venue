use venue_gateway_api::{GatewayApiError, GatewayBinding, GatewayMode, VenueId};

use crate::{HyperliquidError, credentials};

/// A validated Hyperliquid account and symbol identity. This value grants no capability or
/// mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidGatewayBinding(GatewayBinding);

impl HyperliquidGatewayBinding {
    pub fn new(binding: GatewayBinding) -> Result<Self, HyperliquidGatewayBindingError> {
        binding
            .validate()
            .map_err(HyperliquidGatewayBindingError::Gateway)?;
        if binding.venue != VenueId::Hyperliquid {
            return Err(HyperliquidGatewayBindingError::Venue);
        }
        if binding.mode != GatewayMode::Live {
            return Err(HyperliquidGatewayBindingError::Mode);
        }
        if binding.symbol.quote() != "USDC" {
            return Err(HyperliquidGatewayBindingError::Quote);
        }
        Ok(Self(binding))
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.0
    }
}

/// Secret-free scope for protocol reads. Hyperliquid account endpoints are keyed by the owner,
/// sub-account, or vault address rather than by the Agent/API wallet address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidReadBinding {
    gateway: HyperliquidGatewayBinding,
    user_address: String,
}

impl HyperliquidReadBinding {
    pub fn new(
        gateway: HyperliquidGatewayBinding,
        user_address: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        let user_address = user_address.into();
        if !credentials::valid_address(&user_address) {
            return Err(HyperliquidError::Binding);
        }
        Ok(Self {
            gateway,
            user_address: user_address.to_ascii_lowercase(),
        })
    }

    #[must_use]
    pub const fn gateway(&self) -> &HyperliquidGatewayBinding {
        &self.gateway
    }

    #[must_use]
    pub fn user_address(&self) -> &str {
        &self.user_address
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HyperliquidGatewayBindingError {
    #[error("Hyperliquid gateway binding must use venue=hyperliquid")]
    Venue,
    #[error("Hyperliquid gateway binding must use mode=LIVE")]
    Mode,
    #[error("Hyperliquid perpetual binding must use quote=USDC")]
    Quote,
    #[error(transparent)]
    Gateway(#[from] GatewayApiError),
}
