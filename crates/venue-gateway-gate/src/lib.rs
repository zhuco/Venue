//! Gate.io gateway protocol boundary.
//!
//! The first migration slice contains only credential-free public-market protocol behavior and
//! immutable gateway identity validation. It has no HTTP/WebSocket transport, credentials,
//! capability issuer, writer, WAL, or mutation authority.

mod public;

pub use public::*;
use thiserror::Error;
use venue_gateway_api::{GatewayApiError, GatewayBinding, VenueId};

/// A validated Gate.io identity. Holding this value grants no read or mutation capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateGatewayBinding(GatewayBinding);

impl GateGatewayBinding {
    pub fn new(binding: GatewayBinding) -> Result<Self, GateGatewayBindingError> {
        binding
            .validate()
            .map_err(GateGatewayBindingError::Gateway)?;
        if binding.venue != VenueId::Gate {
            return Err(GateGatewayBindingError::Venue);
        }
        Ok(Self(binding))
    }

    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GateGatewayBindingError {
    #[error("Gate.io gateway binding must use venue=gate")]
    Venue,
    #[error(transparent)]
    Gateway(#[from] GatewayApiError),
}

#[cfg(test)]
mod binding_tests {
    use venue_gateway_api::{GatewayMode, VenueId};

    use super::*;

    fn binding(
        venue: VenueId,
        mode: GatewayMode,
    ) -> Result<GatewayBinding, Box<dyn std::error::Error>> {
        Ok(GatewayBinding::new(
            venue,
            mode,
            "00000000-0000-4000-8000-000000000001",
            "DOGE/USDT".parse()?,
        )?)
    }

    #[test]
    fn gate_binding_accepts_only_gate_test_or_live_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        for mode in [GatewayMode::Test, GatewayMode::Live] {
            let validated = GateGatewayBinding::new(binding(VenueId::Gate, mode)?)?;
            assert_eq!(validated.gateway_binding().mode, mode);
            assert_eq!(validated.gateway_binding().venue, VenueId::Gate);
        }
        assert_eq!(
            GateGatewayBinding::new(binding(VenueId::Bitget, GatewayMode::Live)?),
            Err(GateGatewayBindingError::Venue)
        );
        Ok(())
    }
}
