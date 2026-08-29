use venue_gateway_api::{GatewayApiError, GatewayBinding, VenueId};

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
        Ok(Self(binding))
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HyperliquidGatewayBindingError {
    #[error("Hyperliquid gateway binding must use venue=hyperliquid")]
    Venue,
    #[error(transparent)]
    Gateway(#[from] GatewayApiError),
}
