use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};

use crate::{BybitConfig, BybitError};

/// A secret-free Bybit identity whose endpoint configuration is derived from the same mode.
/// Holding this value grants no read or mutation capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BybitGatewayBinding {
    gateway_binding: GatewayBinding,
    config: BybitConfig,
}

impl BybitGatewayBinding {
    pub fn new(gateway_binding: GatewayBinding) -> Result<Self, BybitError> {
        gateway_binding
            .validate()
            .map_err(|_| BybitError::Binding)?;
        if gateway_binding.venue != VenueId::Bybit || gateway_binding.mode != GatewayMode::Live {
            return Err(BybitError::Binding);
        }
        let config = BybitConfig::live();
        Ok(Self {
            gateway_binding,
            config,
        })
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.gateway_binding
    }

    #[must_use]
    pub const fn config(&self) -> &BybitConfig {
        &self.config
    }

    pub(crate) fn validate_request_binding(
        &self,
        request_binding: &GatewayBinding,
    ) -> Result<(), BybitError> {
        request_binding
            .validate()
            .map_err(|_| BybitError::Binding)?;
        if request_binding != &self.gateway_binding {
            return Err(BybitError::Binding);
        }
        Ok(())
    }
}
