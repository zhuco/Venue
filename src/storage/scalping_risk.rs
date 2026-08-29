use crate::{
    domain::Symbol,
    strategy::scalping::{RiskFact, RiskUnit},
};

pub use venue_storage::{ScalpingRiskCommit, ScalpingRiskError};

pub type ScalpingRiskBinding = venue_storage::ScalpingRiskBinding<Symbol, RiskUnit>;
pub type ScalpingRiskFact = venue_storage::ScalpingRiskFact<Symbol, RiskUnit, RiskFact>;
pub type ScalpingRiskCursor = venue_storage::ScalpingRiskCursor<Symbol, RiskUnit>;
pub type ScalpingRiskEntry = venue_storage::ScalpingRiskEntry<Symbol, RiskUnit, RiskFact>;
pub type ScalpingRiskRecord = venue_storage::ScalpingRiskRecord<Symbol, RiskUnit, RiskFact>;
pub type ScalpingRiskRecovery = venue_storage::ScalpingRiskRecovery<Symbol, RiskUnit, RiskFact>;
pub type ScalpingRiskReplay = venue_storage::ScalpingRiskReplay<Symbol, RiskUnit, RiskFact>;
pub type ScalpingRiskJournal = venue_storage::ScalpingRiskJournal<Symbol, RiskUnit, RiskFact>;
