mod binding;
mod config;
mod credentials;
mod execution;
mod instrument;
pub mod portfolio;
pub mod private;
mod private_ws;
mod public;
mod readback;
mod sign;
mod transport;

use venue_gateway_api::CapabilityFlags;

pub use binding::{BinanceAccountBinding, BinanceBindingError, native_symbol};
pub use config::{BinanceConfig, endpoints};
pub use credentials::BinanceCredentials;
pub use execution::*;
pub use instrument::{
    BinanceInstrumentError, BinanceInstrumentRules, parse_instrument_rules,
    parse_native_instrument_rules,
};
pub use private_ws::{
    BinanceListenKey, BinancePrivateWsTransport, BinanceRawPrivateFrame, connect_private_ws,
};
pub use public::{
    BinancePublicEnvelope, BinancePublicError, parse_bbo, parse_closed_bar, parse_depth_delta,
    parse_public_trade,
};
pub use readback::*;
pub use sign::{BinanceHttpMethod, BinanceRestSignInput, SignedBinanceRestRequest, sign_rest};
pub use transport::{
    BinanceHttpResponse, BinanceHttpTransport, BinancePhysicalMutationOutcome,
    BinanceTransportError, BinanceTransportLimits,
};

/// Adapter protocol completeness never grants runtime mutation authority. Stage 7 remains the
/// sole capability/writer/WAL authority until a host validates and wraps this adapter.
#[must_use]
pub const fn capabilities() -> CapabilityFlags {
    CapabilityFlags::empty()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceAuthError {
    #[error("Binance gateway binding does not match the fixed endpoint configuration")]
    Binding,
    #[error("Binance credentials are absent or invalid")]
    Credentials,
    #[error("Binance REST signing input is invalid or ambiguous")]
    SigningInput,
}

#[cfg(test)]
mod tests {
    #[test]
    fn physical_adapter_never_self_grants_runtime_capability() {
        assert!(super::capabilities().is_empty());
    }
}
