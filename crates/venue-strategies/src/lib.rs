//! Pure strategy reducers and semantic-intent models.
//!
//! This crate intentionally has no runtime, network, credential, storage, writer, or mutation
//! dependency. Hosts supply normalized facts and own all persistence and execution authority.

pub mod hedged_grid;
pub mod scalping;
