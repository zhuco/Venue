//! Pure scalping state, semantic-intent model, and logical risk reducer.
//!
//! Feature-frame construction, controller authorization, checkpoint I/O, and execution remain
//! host responsibilities. The constants below name required normalized fact families only.

mod candidate_memory;
mod model;
mod risk;

pub const BOOK_SOURCE: &str = "book";
pub const TRADES_SOURCE: &str = "trades";
pub const BARS_SOURCE: &str = "bars";

pub use candidate_memory::{BreakoutCursor, CandidateMemoryState, SeenCandidate};
pub use model::*;
pub use risk::{RiskFact, RiskGate, RiskLedgerState, RiskRevaluation, RiskSnapshot};
