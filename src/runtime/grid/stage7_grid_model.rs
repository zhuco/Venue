use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    runtime::HedgedGridControlTarget,
    runtime::hedged_grid::ExposureReductionPending,
    strategy::hedged_grid::{ExposureGuardState, GridPhase, HedgedGridBinding, HedgedGridState},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7GridRequest {
    pub artifacts_root: PathBuf,
    pub max_turns: Option<u64>,
    pub reset_on_start: bool,
    pub skip_inventory_replenishment_until_recovered: bool,
    pub confirm_mainnet_grid_mutations: bool,
    pub shadow_only: bool,
    pub stop_after_first_owned_fill: bool,
    /// Optional wall-clock cut-off for a bounded Canary phase. A busy private stream must not
    /// stretch a turn limit into a longer period of live exposure.
    pub wall_clock_deadline_ms: Option<u64>,
    /// Canary verification uses its next stable signed readback for a new order-health report.
    pub force_order_health_check: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7GridReport {
    pub exchange: String,
    pub turns: u64,
    pub phase: GridPhase,
    pub private_generation: u64,
    pub checkpoint_path: PathBuf,
    pub stopped: bool,
    pub shadow_only: bool,
    pub private_stream_connected: bool,
    pub first_owned_fill_observed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7CanaryRequest {
    pub artifacts_root: PathBuf,
    pub confirm_mainnet_grid_mutations: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7CanaryReport {
    pub exchange: String,
    pub symbol: String,
    pub private_generation: u64,
    pub capability_valid_until_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7CanaryRecoveryReport {
    pub exchange: String,
    pub symbol: String,
    pub private_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7GridCanaryReport {
    pub exchange: String,
    pub symbol: String,
    pub private_generation: u64,
    pub capability_valid_until_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7ExecutableHandoffRequest {
    pub artifacts_root: PathBuf,
    pub release_manifest: PathBuf,
    pub confirm_mainnet_nonflat_executable_handoff: bool,
    pub confirm_mainnet_stopped_order_recovery: bool,
    pub archive_resolved_command_wal: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7ExecutableHandoffReport {
    pub exchange: String,
    pub symbol: String,
    pub predecessor_executable_sha256: String,
    pub successor_executable_sha256: String,
    pub private_generation: u64,
    pub writer_generation: u64,
    pub handoff_sha256: String,
    pub positions_preserved: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Stage7GridControl {
    pub(super) schema_version: u16,
    pub(super) binding: HedgedGridBinding,
    pub(super) target: HedgedGridControlTarget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Stage7GridCheckpoint {
    pub(super) schema_version: u16,
    pub(super) binding: HedgedGridBinding,
    pub(super) state: HedgedGridState,
    pub(super) private_generation: u64,
    #[serde(default)]
    pub(super) exposure_guard: Option<ExposureGuardState>,
    #[serde(default)]
    pub(super) pending_exposure_reduction: Option<ExposureReductionPending>,
    /// Safe lower bound for private fills still needed by this artifacts root. Bitget's UTA
    /// endpoint is account-wide; a settled resident advances this with a bounded recovery
    /// overlap instead of re-fetching and re-journaling the root's complete history forever.
    #[serde(default)]
    pub(super) fill_history_start_ms: u64,
    #[serde(default)]
    pub(super) order_health_fenced: bool,
    /// Last successful signed order-health projection for this exact artifacts root. Persisting
    /// the watermark makes the 30-minute obligation survive resident restarts and busy streams.
    #[serde(default)]
    pub(super) last_order_health_checked_at_ms: u64,
}
