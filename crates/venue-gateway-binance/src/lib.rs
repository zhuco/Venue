mod account_gateway;
mod binding;
mod config;
mod credential_probe;
mod credentials;
mod execution;
mod instrument;
pub mod portfolio;
pub mod private;
mod private_ws;
mod public;
mod public_ws;
mod readback;
mod recovery;
mod sign;
mod transport;

use venue_gateway_api::CapabilityFlags;

pub use account_gateway::{
    BinanceAccountGateway, BinanceAccountGatewayError, BinanceGridBootstrapMarketFacts,
    BinancePrivateAccountEvent, BinancePrivateFillEvent, BinancePublicMarketEvent,
};
pub use binding::{BinanceAccountBinding, BinanceBindingError, native_symbol};
pub use config::{BinanceConfig, endpoints};
pub use credential_probe::{BinanceCredentialProbe, BinanceProbeError, probe_credentials};
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
    BinanceFormingBar, BinanceKlineInterval, BinancePublic24hTicker, BinancePublicEnvelope,
    BinancePublicError, BinancePublicInstrument, BinancePublicKline, parse_bbo, parse_closed_bar,
    parse_depth_delta, parse_public_exchange_catalog, parse_public_exchange_info,
    parse_public_market_agg_trade, parse_public_market_bbo, parse_public_market_depth_delta,
    parse_public_market_depth20_snapshot, parse_public_market_kline,
    parse_public_market_rest_depth_snapshot, parse_public_market_rest_klines,
    parse_public_market_ticker_array, parse_public_market_ticker_snapshot, parse_public_trade,
};
pub use public_ws::{BinancePublicWsTransport, BinanceRawPublicFrame, connect_public_ws};
pub use readback::*;
pub use recovery::*;
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
