pub mod account;
mod account_gateway;
mod binding;
mod config;
mod credentials;
pub mod endpoints;
mod execution;
pub mod instrument;
mod node_bridge;
mod order_families;
pub mod private;
mod private_ws;
pub mod public;
mod public_ws;
mod recovery;
pub mod risk;
mod runtime_recovery;
mod sign;
mod transport;

use venue_gateway_api::CapabilityFlags;

pub use account_gateway::{BitgetAccountGateway, BitgetAccountGatewayError};
pub use binding::{BitgetAccountBinding, BitgetBindingError};
pub use config::BitgetConfig;
pub use credentials::BitgetCredentials;
pub use execution::*;
pub use node_bridge::*;
pub use order_families::*;
pub use private_ws::*;
pub use public_ws::{BitgetPublicWsError, BitgetScalpingBookFrame, BitgetScalpingPublicReceiver};
pub use recovery::*;
pub(crate) use runtime_recovery::BitgetRuntimeRecoveryAwaitGuard;
pub use runtime_recovery::{
    BitgetRuntimeRecoveryBundle, BitgetRuntimeRecoveryError, BitgetRuntimeRecoveryRegistration,
    BitgetRuntimeRecoveryRevalidator, BitgetRuntimeRecoveryScope, BitgetRuntimeRecoveryScopeInput,
    BitgetRuntimeStructuredUnknown,
};
pub use sign::{SignInput, SignedHeaders, prehash, sign, ws_sign};
pub use transport::*;

/// Adapter protocol closure does not grant writer, WAL, or account mutation capability.
#[must_use]
pub const fn capabilities() -> CapabilityFlags {
    CapabilityFlags::empty()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BitgetError {
    #[error("Bitget credentials are unavailable or invalid")]
    Credentials,
    #[error("Bitget signing input is invalid")]
    SigningInput,
}
