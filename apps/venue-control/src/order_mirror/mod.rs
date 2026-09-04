//! Order mirroring owns only mappings and ordinary PostgreSQL commands. Account scheduling,
//! exchange transport and ambiguous-request recovery remain in the existing executor.
mod planner;
mod settlement;
mod store;
pub(crate) use settlement::{mirror_send_allowed, settle_mirror_command};
pub use store::run_order_mirror;

pub const MAX_MIRROR_ORDERS_PER_RELATION: usize = 128;
