mod binding;
mod config;
mod credentials;
pub mod endpoints;
pub mod public;
mod sign;

pub use binding::{BitgetAccountBinding, BitgetBindingError};
pub use config::BitgetConfig;
pub use credentials::BitgetCredentials;
pub use sign::{SignInput, SignedHeaders, prehash, sign, ws_sign};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetError {
    #[error("Bitget credentials are unavailable or invalid")]
    Credentials,
    #[error("Bitget signing input is invalid")]
    SigningInput,
}
