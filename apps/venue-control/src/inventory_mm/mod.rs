//! Binance-only inventory market making on the shared account-serial execution ledger.
mod admission;
pub(crate) mod dispatch;
mod store;
pub use admission::signed_gate;
mod runtime;
pub use runtime::InventoryMmRuntime;
pub use store::{InventoryMmStore, InventoryMmStoreError, MmCommandIntent, MmCommandRecord};
