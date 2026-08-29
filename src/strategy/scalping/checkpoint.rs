use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{
    indicator::SourceCursor,
    storage::ProjectionStore,
    strategy::scalping::{
        CandidateMemoryState, EpisodeProjection, MarketRegime, RiskLedgerState, ScalpingError,
        ScalpingState, StrategyKind,
    },
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

#[derive(Debug)]
pub struct ScalpingCheckpointStore {
    store: ProjectionStore,
}

impl ScalpingCheckpointStore {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            store: ProjectionStore::new(path.as_ref().to_path_buf()),
        }
    }

    pub fn load(&self) -> Result<Option<ScalpingCheckpoint>, ScalpingError> {
        self.store
            .load()
            .map_err(|error| ScalpingError::Persistence {
                detail: error.to_string(),
            })
    }

    pub fn save(&self, checkpoint: &ScalpingCheckpoint) -> Result<(), ScalpingError> {
        self.store
            .save(checkpoint)
            .map_err(|error| ScalpingError::Persistence {
                detail: error.to_string(),
            })
    }
}
