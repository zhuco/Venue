//! Pure scalping state, semantic-intent model, and logical risk reducer.
//!
//! Feature-frame construction, controller authorization, checkpoint I/O, and execution remain
//! host responsibilities. The constants below name required normalized fact families only.

mod candidate_memory;
mod checkpoint;
mod engine;
mod evidence;
mod fault;
mod model;
mod risk;

pub use venue_domain::domain::{BARS_SOURCE, BOOK_SOURCE, TRADES_SOURCE};

pub use candidate_memory::{
    BreakoutCursor, CandidateMemory, CandidateMemoryRejection, CandidateMemoryState, SeenCandidate,
};
pub use checkpoint::{SCALPING_CHECKPOINT_SCHEMA, ScalpingCheckpoint};
pub use engine::{LifecycleAuthorization, ScalpingStrategy, digest_strategy_binding};
pub use evidence::{EvidenceScore, robust_value, score_candidate_evidence};
pub use model::*;
pub use risk::{RiskFact, RiskGate, RiskLedger, RiskLedgerState, RiskRevaluation, RiskSnapshot};
