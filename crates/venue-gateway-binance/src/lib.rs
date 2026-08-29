mod binding;
mod config;
mod credentials;
mod instrument;
pub mod portfolio;
pub mod private;
mod public;
mod sign;

pub use binding::{BinanceAccountBinding, BinanceBindingError, native_symbol};
pub use config::BinanceConfig;
pub use credentials::BinanceCredentials;
pub use instrument::{
    BinanceInstrumentError, BinanceInstrumentRules, parse_instrument_rules,
    parse_native_instrument_rules,
};
pub use public::{
    BinancePublicEnvelope, BinancePublicError, parse_bbo, parse_closed_bar, parse_depth_delta,
    parse_public_trade,
};
pub use sign::{BinanceHttpMethod, BinanceRestSignInput, SignedBinanceRestRequest, sign_rest};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceAuthError {
    #[error("Binance gateway binding does not match the fixed endpoint configuration")]
    Binding,
    #[error("Binance credentials are absent or invalid")]
    Credentials,
    #[error("Binance REST signing input is invalid or ambiguous")]
    SigningInput,
}
