use serde::{Deserialize, Serialize};

use venue_indicators::SourceCursor;

use super::{
    CandidateMemoryState, EpisodeProjection, MarketRegime, RiskLedgerState, ScalpingState,
    StrategyKind,
};

pub const SCALPING_CHECKPOINT_SCHEMA: u16 = 4;

/// Durable, non-authoritative strategy projection. It never stores physical orders, fills,
/// positions, credentials, or an exchange-native symbol.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingCheckpoint {
    pub schema_version: u16,
    pub strategy_kind: StrategyKind,
    pub binding_digest: String,
    pub params_digest: String,
    pub controller_revision: u64,
    pub authority_generation: u64,
    pub regime: Option<MarketRegime>,
    pub regime_entered_at_ms: Option<u64>,
    pub state: ScalpingState,
    #[serde(default)]
    pub episode: Option<EpisodeProjection>,
    pub last_frame_generation: Option<u64>,
    pub last_watermark_ms: Option<u64>,
    pub cursors: std::collections::BTreeMap<String, SourceCursor>,
    pub candidate_memory: CandidateMemoryState,
    #[serde(default)]
    pub risk: RiskLedgerState,
}
