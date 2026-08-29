use serde::{Deserialize, Serialize};

pub(super) use crate::domain::validate_config_digest;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountHealth {
    Starting,
    Ready,
    Frozen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountFault {
    PrivateStreamDisconnected,
    PrivateGenerationMismatch,
    PrivateEvidenceGap,
    PrivateEvidenceBatchIncomplete,
    ReconciliationFailed,
    WriterUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceLifecycle {
    Registered,
    Recovering,
    Running,
    Paused,
    Stopping,
    Faulted,
    NeedsAttention,
}

impl InstanceLifecycle {
    #[must_use]
    pub const fn accepts_new_risk(self) -> bool {
        matches!(self, Self::Running)
    }
}
