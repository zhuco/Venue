use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    config::Config,
    exchange::grid::{BinanceGridVenue, HedgedGridVenue},
    execution::{CommandJournal, WriterLeaseAuthority, sha256_hex},
    storage::{Journal, PrivateEvidenceJournal, ProjectionStore},
    strategy::hedged_grid::{GridPhase, HedgedGridBinding, HedgedGridParams, HedgedGridState},
};

use super::{
    CHECKPOINT_FILE, CONTROL_FILE, PRIVATE_EVIDENCE_FILE, Stage7CanaryVenue, Stage7GridCheckpoint,
    Stage7GridControl, Stage7GridError, binance_binding, canary_cleanup_readback, release_params,
    stage7_canary_support::{
        STAGE7_LIVE_ADMISSION_FILE, Stage7LiveAdmissionEvidence, canonical_digest, valid_sha256,
    },
};
use crate::runtime::{HedgedGridControlTarget, hedged_grid_live};

const BRIDGE_DIRECTORY: &str = "legacy_stage7_bridge";
const ATTESTATION_FILE: &str = "attestation.json";
const RECEIPT_FILE: &str = "receipt.json";
const LEGACY_PRIVATE_DIRECTORY: &str = "private";
const LEGACY_PRIVATE_FILES: [&str; 4] = [
    "private_evidence.jsonl",
    "private_session.json",
    "facts.jsonl",
    "fill_cursor.json",
];
const BRIDGE_AUTHORIZATION: &str = "binance-legacy-phase1-to-shared-grid-v1";
const ADMISSION_VALIDITY_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceLegacyStage7StopRequest {
    pub artifacts_root: PathBuf,
    pub confirm_mainnet_legacy_stop: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceLegacyStage7BridgeRequest {
    pub artifacts_root: PathBuf,
    pub legacy_config_path: PathBuf,
    pub legacy_executable_path: PathBuf,
    pub successor_executable_path: PathBuf,
    pub expected_legacy_executable_sha256: String,
    pub expected_successor_executable_sha256: String,
    pub confirm_mainnet_nonflat_legacy_bridge: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceLegacyStage7BridgeReport {
    pub symbol: String,
    pub private_generation: u64,
    pub writer_generation: u64,
    pub long_quantity: rust_decimal::Decimal,
    pub short_quantity: rust_decimal::Decimal,
    pub attestation_sha256: String,
    pub receipt_sha256: String,
    pub idempotent_replay: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BridgeAttestation {
    schema_version: u16,
    authorization: String,
    request_sha256: String,
    canonical_artifacts_root: String,
    legacy_binding: HedgedGridBinding,
    successor_binding: HedgedGridBinding,
    legacy_parameter_release: HedgedGridParams,
    successor_parameter_release: HedgedGridParams,
    legacy_control_json: String,
    legacy_checkpoint_json: String,
    legacy_control_sha256: String,
    legacy_checkpoint_sha256: String,
    configuration_sha256: String,
    legacy_executable_sha256: String,
    successor_executable_sha256: String,
    legacy_private_evidence_sha256: String,
    legacy_wal_sha256: String,
    legacy_writer_sha256: String,
    writer_generation: u64,
    writer_revision: u64,
    private_generation: u64,
    signed_readback_sha256: String,
    #[serde(with = "rust_decimal::serde::str")]
    long_quantity: rust_decimal::Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    short_quantity: rust_decimal::Decimal,
    successor_checkpoint: Stage7GridCheckpoint,
    successor_control: Stage7GridControl,
    successor_admission: Stage7LiveAdmissionEvidence,
    prepared_at_ms: u64,
    attestation_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BridgeReceipt {
    schema_version: u16,
    authorization: String,
    request_sha256: String,
    attestation_sha256: String,
    checkpoint_sha256: String,
    control_sha256: String,
    admission_sha256: String,
    private_evidence_sha256: String,
    committed_at_ms: u64,
    receipt_sha256: String,
}

#[derive(Serialize)]
struct BridgeRequestDigest<'a> {
    authorization: &'static str,
    canonical_artifacts_root: &'a str,
    legacy_config_path: &'a Path,
    legacy_executable_path: &'a Path,
    successor_executable_path: &'a Path,
    legacy_executable_sha256: &'a str,
    successor_executable_sha256: &'a str,
}

#[derive(Serialize)]
struct SignedReadbackDigest<'a> {
    private_generation: u64,
    observed_at_ms: u64,
    symbol: &'a str,
    #[serde(with = "rust_decimal::serde::str")]
    long_quantity: rust_decimal::Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    short_quantity: rust_decimal::Decimal,
    raw_payload_sha256: Vec<String>,
}

/// This is the only bridge operation allowed to touch legacy control. It requests graceful Stop;
/// the separate finalize operation remains unable to submit, cancel, or flatten an order.
pub fn request_binance_legacy_stage7_stop(
    request: BinanceLegacyStage7StopRequest,
) -> Result<(), Stage7GridError> {
    if !request.confirm_mainnet_legacy_stop {
        return Err(Stage7GridError::Confirmation);
    }
    hedged_grid_live::request_existing_hedged_grid_stop(&request.artifacts_root)?;
    Ok(())
}

pub fn run_binance_legacy_stage7_bridge(
    cfg: &Config,
    request: BinanceLegacyStage7BridgeRequest,
) -> Result<BinanceLegacyStage7BridgeReport, Stage7GridError> {
    let mut venue = BinanceGridVenue::production(cfg.symbol.clone(), 1)?;
    let current_executable =
        std::env::current_exe().map_err(|_| Stage7GridError::ExecutableHandoff)?;
    let current_executable_sha256 = file_sha256(&current_executable)?;
    run_bridge_with_venue(cfg, request, &mut venue, &current_executable_sha256)
}

/// A bridge receipt is an alternative predecessor proof only for the one legacy Binance root.
/// It does not create general Canary capability evidence and cannot authorize another binding,
/// instrument, executable, or parameter release.
pub(super) fn require_binance_legacy_bridge_admission<V: Stage7CanaryVenue>(
    venue: &V,
    binding: &HedgedGridBinding,
    params: &HedgedGridParams,
    current_exposure_take_profit_sha256: &Option<String>,
    artifacts_root: &Path,
    current_executable_sha256: &str,
    now_ms: u64,
) -> Result<bool, Stage7GridError> {
    let bridge_root = artifacts_root.join(BRIDGE_DIRECTORY);
    let Some(receipt) =
        ProjectionStore::new(bridge_root.join(RECEIPT_FILE)).load::<BridgeReceipt>()?
    else {
        return Ok(false);
    };
    receipt.validate()?;
    let attestation = ProjectionStore::new(bridge_root.join(ATTESTATION_FILE))
        .load::<BridgeAttestation>()?
        .ok_or(Stage7GridError::ExecutableHandoff)?;
    attestation.validate()?;
    // A bridge receipt predating forensic recovery cannot retroactively commit the derived
    // journal. Revalidate the active source/manifest/prefix before using it as predecessor proof.
    let _active_private_evidence = super::open_stage7_private_evidence(artifacts_root, binding)?;
    let capability_binding = venue.capability_binding();
    let (admission, bridge_predecessor) =
        super::stage7_executable_handoff::validated_admission_predecessor(
            &capability_binding,
            venue.instrument(),
            venue.minimum_quantity(),
            artifacts_root,
            now_ms,
            current_executable_sha256,
        )?;
    let direct_bridge_admission = admission.schema_version == 1;
    if receipt.attestation_sha256 != attestation.attestation_sha256
        || (direct_bridge_admission
            && receipt.admission_sha256
                != file_sha256(&artifacts_root.join(STAGE7_LIVE_ADMISSION_FILE))?)
        || bridge_predecessor != attestation.successor_admission
        || bridge_predecessor.admission_sha256 != attestation.successor_admission.admission_sha256
        || attestation.successor_binding != *binding
        || attestation.successor_parameter_release != *params
        || attestation.successor_executable_sha256 != bridge_predecessor.executable_sha256
        || admission.executable_sha256 != current_executable_sha256
        || !admission.matches_current_exposure(
            &capability_binding,
            venue.instrument(),
            venue.minimum_quantity(),
            current_executable_sha256,
            current_exposure_take_profit_sha256,
            now_ms,
        )?
    {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    Ok(true)
}

fn run_bridge_with_venue<V: HedgedGridVenue + Stage7CanaryVenue>(
    cfg: &Config,
    request: BinanceLegacyStage7BridgeRequest,
    venue: &mut V,
    current_executable_sha256: &str,
) -> Result<BinanceLegacyStage7BridgeReport, Stage7GridError> {
    if !request.confirm_mainnet_nonflat_legacy_bridge {
        return Err(Stage7GridError::ExecutableHandoffConfirmation);
    }
    if !request.artifacts_root.is_absolute()
        || !request.legacy_config_path.is_absolute()
        || !request.legacy_executable_path.is_absolute()
        || !request.successor_executable_path.is_absolute()
    {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let legacy_expected = normalize_sha256(&request.expected_legacy_executable_sha256)?;
    let successor_expected = normalize_sha256(&request.expected_successor_executable_sha256)?;
    if legacy_expected == successor_expected
        || file_sha256(&request.legacy_executable_path)? != legacy_expected
        || file_sha256(&request.successor_executable_path)? != successor_expected
        || current_executable_sha256.to_ascii_lowercase() != successor_expected
    {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let loaded_config = Config::load(&request.legacy_config_path)
        .map_err(|_| Stage7GridError::ExecutableHandoff)?;
    if loaded_config != *cfg {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let canonical_root =
        fs::canonicalize(&request.artifacts_root).map_err(|source| Stage7GridError::Io {
            path: request.artifacts_root.clone(),
            source,
        })?;
    if canonical_root != request.artifacts_root {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let successor_binding = binance_binding(cfg)?;
    let legacy_binding = hedged_grid_live::phase_one_binding_for_account(&cfg.trading_account_id)?;
    if successor_binding.exchange != "binance"
        || successor_binding.config_version != "shared-grid-v1"
        || successor_binding.account != legacy_binding.account
        || successor_binding.symbol != legacy_binding.symbol
        || successor_binding.owner_scope != legacy_binding.owner_scope
        || venue.exchange() != "binance"
        || venue.instrument().symbol != successor_binding.symbol
    {
        return Err(Stage7GridError::Binding);
    }
    // The frozen bridge may only operate under the exact Stage-7 canonical writer root. This
    // prevents a legacy migration receipt from coexisting with a resident writer for the same
    // account binding.
    let writer_scope = super::stage7_writer_scope(&successor_binding);
    let _canonical_writer_root = super::acquire_stage7_writer_root(&writer_scope, &canonical_root)?;
    let canonical_root_text = canonical_root.to_string_lossy().into_owned();
    let request_sha256 = canonical_digest(&BridgeRequestDigest {
        authorization: BRIDGE_AUTHORIZATION,
        canonical_artifacts_root: &canonical_root_text,
        legacy_config_path: &request.legacy_config_path,
        legacy_executable_path: &request.legacy_executable_path,
        successor_executable_path: &request.successor_executable_path,
        legacy_executable_sha256: &legacy_expected,
        successor_executable_sha256: &successor_expected,
    })?;
    let bridge_root = canonical_root.join(BRIDGE_DIRECTORY);
    fs::create_dir_all(&bridge_root).map_err(|source| Stage7GridError::Io {
        path: bridge_root.clone(),
        source,
    })?;
    let receipt_store = ProjectionStore::new(bridge_root.join(RECEIPT_FILE));
    if let Some(receipt) = receipt_store.load::<BridgeReceipt>()? {
        receipt.validate()?;
        if receipt.request_sha256 != request_sha256
            || file_sha256(&canonical_root.join(CHECKPOINT_FILE))? != receipt.checkpoint_sha256
            || file_sha256(&canonical_root.join(CONTROL_FILE))? != receipt.control_sha256
            || file_sha256(&canonical_root.join(STAGE7_LIVE_ADMISSION_FILE))?
                != receipt.admission_sha256
            || file_sha256(&canonical_root.join(PRIVATE_EVIDENCE_FILE))?
                != receipt.private_evidence_sha256
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        let attestation = ProjectionStore::new(bridge_root.join(ATTESTATION_FILE))
            .load::<BridgeAttestation>()?
            .ok_or(Stage7GridError::ExecutableHandoff)?;
        attestation.validate()?;
        return Ok(report(&attestation, &receipt, true));
    }

    let params = release_params(cfg, &successor_binding)?;
    let legacy_params = HedgedGridParams::phase_one(params.grid_count)?;
    let attestation_store = ProjectionStore::new(bridge_root.join(ATTESTATION_FILE));
    let authority =
        WriterLeaseAuthority::open(canonical_root.join(super::WRITER_FILE), writer_scope)?;
    let writer = authority.active_session()?.ok_or(Stage7GridError::Writer)?;
    let _writer_guard = authority.persistent_dispatch_guard(&writer)?;

    let mut attestation = match attestation_store.load::<BridgeAttestation>()? {
        Some(existing) => {
            existing.validate()?;
            if existing.request_sha256 != request_sha256
                || existing.legacy_binding != legacy_binding
                || existing.successor_binding != successor_binding
                || existing.writer_generation != writer.generation
                || existing.writer_revision != writer.revision
            {
                return Err(Stage7GridError::ExecutableHandoff);
            }
            existing
        }
        None => prepare_attestation(
            cfg,
            venue,
            &canonical_root,
            &request,
            request_sha256.clone(),
            legacy_binding,
            successor_binding.clone(),
            legacy_params,
            params,
            &writer,
            legacy_expected,
            successor_expected,
            venue.capability_binding(),
        )?,
    };
    if attestation_store.load::<BridgeAttestation>()?.is_none() {
        attestation.attestation_sha256 = attestation.expected_sha256()?;
        attestation.validate()?;
        attestation_store.save(&attestation)?;
    }

    // A retry after a crash re-reads signed private state. Quantity must remain identical to the
    // frozen bridge proof; any open order or changed position keeps the root fenced.
    let mut stage7_evidence =
        super::open_stage7_private_evidence(&canonical_root, &successor_binding)?;
    let mut generation = stage7_evidence
        .last_generation()
        .max(attestation.private_generation);
    let (readback, inventory) = canary_cleanup_readback(
        venue,
        &mut stage7_evidence,
        &mut generation,
        &successor_binding,
    )?;
    if !readback
        .all_order_families_empty()
        .map_err(|_| Stage7GridError::OrderFamily)?
        || inventory.long_quantity != attestation.long_quantity
        || inventory.short_quantity != attestation.short_quantity
        || (inventory.long_quantity.is_zero() && inventory.short_quantity.is_zero())
    {
        return Err(Stage7GridError::ExecutableHandoff);
    }

    ProjectionStore::new(canonical_root.join(CHECKPOINT_FILE))
        .save(&attestation.successor_checkpoint)?;
    ProjectionStore::new(canonical_root.join(CONTROL_FILE)).save(&attestation.successor_control)?;
    ProjectionStore::new(canonical_root.join(STAGE7_LIVE_ADMISSION_FILE))
        .save(&attestation.successor_admission)?;
    let mut receipt = BridgeReceipt {
        schema_version: 1,
        authorization: BRIDGE_AUTHORIZATION.to_owned(),
        request_sha256,
        attestation_sha256: attestation.attestation_sha256.clone(),
        checkpoint_sha256: file_sha256(&canonical_root.join(CHECKPOINT_FILE))?,
        control_sha256: file_sha256(&canonical_root.join(CONTROL_FILE))?,
        admission_sha256: file_sha256(&canonical_root.join(STAGE7_LIVE_ADMISSION_FILE))?,
        private_evidence_sha256: file_sha256(&canonical_root.join(PRIVATE_EVIDENCE_FILE))?,
        committed_at_ms: super::wall_clock_ms()?,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt.expected_sha256()?;
    receipt.validate()?;
    receipt_store.save(&receipt)?;
    Ok(report(&attestation, &receipt, false))
}

#[allow(clippy::too_many_arguments)]
fn prepare_attestation<V: HedgedGridVenue + Stage7CanaryVenue>(
    cfg: &Config,
    venue: &mut V,
    root: &Path,
    request: &BinanceLegacyStage7BridgeRequest,
    request_sha256: String,
    legacy_binding: HedgedGridBinding,
    successor_binding: HedgedGridBinding,
    legacy_params: HedgedGridParams,
    successor_params: HedgedGridParams,
    writer: &crate::execution::WriterSession,
    legacy_executable_sha256: String,
    successor_executable_sha256: String,
    capability_binding: crate::execution::CapabilityBinding,
) -> Result<BridgeAttestation, Stage7GridError> {
    let legacy_control_bytes = required_bytes(&root.join(hedged_grid_live::GRID_CONTROL_FILE))?;
    let legacy_checkpoint_bytes =
        required_bytes(&root.join(hedged_grid_live::GRID_CHECKPOINT_FILE))?;
    let legacy_control: hedged_grid_live::HedgedGridControl =
        serde_json::from_slice(&legacy_control_bytes).map_err(|_| Stage7GridError::Control)?;
    let legacy_checkpoint: hedged_grid_live::HedgedGridCheckpoint =
        serde_json::from_slice(&legacy_checkpoint_bytes)
            .map_err(|_| Stage7GridError::Checkpoint)?;
    if legacy_control.schema_version != 1
        || legacy_control.binding != legacy_binding
        || legacy_control.target != HedgedGridControlTarget::Stop
        || legacy_checkpoint.schema_version != 1
        || legacy_checkpoint.state.binding != legacy_binding
        || legacy_checkpoint.state.params != legacy_params
        || legacy_checkpoint.state.phase != GridPhase::Stopping
        || !legacy_checkpoint.state.owned_orders.is_empty()
        || !legacy_checkpoint.state.pending_transactions.is_empty()
        || !legacy_checkpoint.state.pending_replenishments.is_empty()
        || !hedged_grid_live::legacy_exposure_is_settled(root, &legacy_binding)?
    {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let commands = CommandJournal::open(root.join(hedged_grid_live::COMMAND_FILE))?;
    if commands.has_unresolved() {
        return Err(Stage7GridError::Unresolved);
    }
    let private_sha256 = legacy_private_bundle_sha256(root)?;
    venue.verify_current_instrument_rules()?;
    let mut evidence = super::open_stage7_private_evidence(root, &successor_binding)?;
    let legacy_evidence = PrivateEvidenceJournal::open(
        root.join(LEGACY_PRIVATE_DIRECTORY)
            .join(LEGACY_PRIVATE_FILES[0]),
    )?;
    let mut generation = evidence
        .last_generation()
        .max(legacy_evidence.last_generation())
        .max(writer.readback_generation);
    let (readback, inventory) =
        canary_cleanup_readback(venue, &mut evidence, &mut generation, &successor_binding)?;
    if !readback
        .all_order_families_empty()
        .map_err(|_| Stage7GridError::OrderFamily)?
        || readback.raw_private_payloads.is_empty()
        || (inventory.long_quantity.is_zero() && inventory.short_quantity.is_zero())
    {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    let now_ms = super::wall_clock_ms()?;
    let signed_readback_sha256 = canonical_digest(&SignedReadbackDigest {
        private_generation: inventory.private_generation,
        observed_at_ms: inventory.private_observed_at_ms,
        symbol: &successor_binding.symbol.to_string(),
        long_quantity: inventory.long_quantity,
        short_quantity: inventory.short_quantity,
        raw_payload_sha256: readback
            .raw_private_payloads
            .iter()
            .map(|payload| sha256_hex(payload.as_bytes()))
            .collect(),
    })?;
    let mut state =
        HedgedGridState::new_with_params(successor_binding.clone(), successor_params.clone())?;
    state.phase = GridPhase::Stopping;
    // Client identities are durable across the legacy/shared-runtime boundary. Preserve the
    // last logical epoch even though the stopped legacy checkpoint owns no active orders.
    state.epoch = legacy_checkpoint.state.epoch.clone();
    state.inventory = Some(inventory.clone());
    let checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: successor_binding.clone(),
        state,
        private_generation: inventory.private_generation,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: now_ms,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let control = Stage7GridControl {
        schema_version: 1,
        binding: successor_binding.clone(),
        target: HedgedGridControlTarget::Stop,
    };
    if cfg.binance.is_none()
        || capability_binding.exchange != "binance"
        || capability_binding.account_binding != "portfolio_margin_um"
        || capability_binding.symbol != successor_binding.symbol.to_string()
    {
        return Err(Stage7GridError::Binding);
    }
    let admission = Stage7LiveAdmissionEvidence::new_with_exposure(
        capability_binding,
        successor_binding.clone(),
        successor_params.clone(),
        venue.instrument().clone(),
        venue.minimum_quantity(),
        cfg.hedged_grid.and_then(|grid| grid.exposure_take_profit),
        successor_executable_sha256.clone(),
        now_ms,
        now_ms
            .checked_add(ADMISSION_VALIDITY_MS)
            .ok_or(Stage7GridError::Clock)?,
        inventory.private_generation,
        inventory.private_generation,
    )?;
    Ok(BridgeAttestation {
        schema_version: 1,
        authorization: BRIDGE_AUTHORIZATION.to_owned(),
        request_sha256,
        canonical_artifacts_root: root.to_string_lossy().into_owned(),
        legacy_binding,
        successor_binding,
        legacy_parameter_release: legacy_params,
        successor_parameter_release: successor_params,
        legacy_control_json: String::from_utf8(legacy_control_bytes)
            .map_err(|_| Stage7GridError::ExecutableHandoff)?,
        legacy_checkpoint_json: String::from_utf8(legacy_checkpoint_bytes)
            .map_err(|_| Stage7GridError::ExecutableHandoff)?,
        legacy_control_sha256: file_sha256(&root.join(hedged_grid_live::GRID_CONTROL_FILE))?,
        legacy_checkpoint_sha256: file_sha256(&root.join(hedged_grid_live::GRID_CHECKPOINT_FILE))?,
        configuration_sha256: file_sha256(&request.legacy_config_path)?,
        legacy_executable_sha256,
        successor_executable_sha256,
        legacy_private_evidence_sha256: private_sha256,
        legacy_wal_sha256: optional_file_sha256(&root.join(hedged_grid_live::COMMAND_FILE))?,
        legacy_writer_sha256: file_sha256(&root.join(hedged_grid_live::WRITER_FILE))?,
        writer_generation: writer.generation,
        writer_revision: writer.revision,
        private_generation: inventory.private_generation,
        signed_readback_sha256,
        long_quantity: inventory.long_quantity,
        short_quantity: inventory.short_quantity,
        successor_checkpoint: checkpoint,
        successor_control: control,
        successor_admission: admission,
        prepared_at_ms: now_ms,
        attestation_sha256: String::new(),
    })
}

fn legacy_private_bundle_sha256(root: &Path) -> Result<String, Stage7GridError> {
    let private_root = root.join(LEGACY_PRIVATE_DIRECTORY);
    let evidence = PrivateEvidenceJournal::open(private_root.join(LEGACY_PRIVATE_FILES[0]))?;
    if evidence.last_generation() == 0 {
        return Err(Stage7GridError::PrivateEvidence);
    }
    let _facts = Journal::open(private_root.join(LEGACY_PRIVATE_FILES[2]))?;
    let mut bundle = Vec::new();
    for name in LEGACY_PRIVATE_FILES {
        let path = private_root.join(name);
        let bytes = fs::read(&path).map_err(|source| Stage7GridError::Io { path, source })?;
        bundle.extend_from_slice(name.as_bytes());
        bundle.extend_from_slice(sha256_hex(bytes).as_bytes());
    }
    Ok(sha256_hex(bundle))
}

fn required_bytes(path: &Path) -> Result<Vec<u8>, Stage7GridError> {
    let bytes = fs::read(path).map_err(|source| Stage7GridError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if bytes.is_empty() {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    Ok(bytes)
}

fn file_sha256(path: &Path) -> Result<String, Stage7GridError> {
    Ok(sha256_hex(required_bytes(path)?))
}

fn optional_file_sha256(path: &Path) -> Result<String, Stage7GridError> {
    match fs::read(path) {
        Ok(bytes) => Ok(sha256_hex(bytes)),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(sha256_hex([])),
        Err(source) => Err(Stage7GridError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn normalize_sha256(value: &str) -> Result<String, Stage7GridError> {
    if !valid_sha256(value) {
        return Err(Stage7GridError::ExecutableHandoff);
    }
    Ok(value.to_ascii_lowercase())
}

impl BridgeAttestation {
    fn expected_sha256(&self) -> Result<String, Stage7GridError> {
        let mut unhashed = self.clone();
        unhashed.attestation_sha256.clear();
        canonical_digest(&unhashed)
    }

    fn validate(&self) -> Result<(), Stage7GridError> {
        if self.schema_version != 1
            || self.authorization != BRIDGE_AUTHORIZATION
            || !valid_sha256(&self.request_sha256)
            || !valid_sha256(&self.attestation_sha256)
            || self.attestation_sha256 != self.expected_sha256()?
            || self.legacy_binding.config_version != "phase1"
            || self.successor_binding.config_version != "shared-grid-v1"
            || self.legacy_binding.symbol != self.successor_binding.symbol
            || self.successor_checkpoint.binding != self.successor_binding
            || self.successor_checkpoint.state.phase != GridPhase::Stopping
            || self.successor_control.target != HedgedGridControlTarget::Stop
            || self.successor_admission.deployment_binding != self.successor_binding
            || self.private_generation == 0
            || self.writer_generation == 0
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        self.successor_admission.validate()
    }
}

impl BridgeReceipt {
    fn expected_sha256(&self) -> Result<String, Stage7GridError> {
        let mut unhashed = self.clone();
        unhashed.receipt_sha256.clear();
        canonical_digest(&unhashed)
    }

    fn validate(&self) -> Result<(), Stage7GridError> {
        if self.schema_version != 1
            || self.authorization != BRIDGE_AUTHORIZATION
            || !valid_sha256(&self.request_sha256)
            || !valid_sha256(&self.attestation_sha256)
            || !valid_sha256(&self.checkpoint_sha256)
            || !valid_sha256(&self.control_sha256)
            || !valid_sha256(&self.admission_sha256)
            || !valid_sha256(&self.private_evidence_sha256)
            || self.committed_at_ms == 0
            || self.receipt_sha256 != self.expected_sha256()?
        {
            return Err(Stage7GridError::ExecutableHandoff);
        }
        Ok(())
    }
}

fn report(
    attestation: &BridgeAttestation,
    receipt: &BridgeReceipt,
    idempotent_replay: bool,
) -> BinanceLegacyStage7BridgeReport {
    BinanceLegacyStage7BridgeReport {
        symbol: attestation.successor_binding.symbol.to_string(),
        private_generation: attestation.private_generation,
        writer_generation: attestation.writer_generation,
        long_quantity: attestation.long_quantity,
        short_quantity: attestation.short_quantity,
        attestation_sha256: attestation.attestation_sha256.clone(),
        receipt_sha256: receipt.receipt_sha256.clone(),
        idempotent_replay,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use rust_decimal::Decimal;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::{BinanceAccountBinding, BinanceConfig, HedgedGridConfig, LogLevel},
        domain::{
            AccountBalance, Amount, Asset, CancelCommand, CommandId, Instrument, MarketKind,
            MarketOrderCommand, MarketReduceCommand, Order, OrderCommand, OrderOwner, OrderPurpose,
            OrderSide, Position, PositionSide, Price,
        },
        exchange::grid::{
            GridOrderFamilyReadback, GridPrivateEvent, GridVenueError, GridVenueFill,
            GridVenueReadback, HedgedGridMutationClient,
        },
        execution::{CapabilityBinding, WriterScope},
        storage::PrivateEvidence,
    };

    struct NoMutationClient(Arc<AtomicUsize>);

    impl HedgedGridMutationClient for NoMutationClient {
        fn place_limit_post_only(&self, _command: &OrderCommand) -> Result<String, GridVenueError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(GridVenueError::PrivateReadbackRequired)
        }
        fn place_market(&self, _command: &MarketOrderCommand) -> Result<String, GridVenueError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(GridVenueError::PrivateReadbackRequired)
        }
        fn place_market_reduce(
            &self,
            _command: &MarketReduceCommand,
        ) -> Result<String, GridVenueError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(GridVenueError::PrivateReadbackRequired)
        }
        fn cancel_by_client_id(&self, _command: &CancelCommand) -> Result<String, GridVenueError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(GridVenueError::PrivateReadbackRequired)
        }
    }

    struct BridgeVenue {
        instrument: Instrument,
        readbacks: VecDeque<GridVenueReadback>,
        mutations: Arc<AtomicUsize>,
    }

    impl HedgedGridVenue for BridgeVenue {
        fn exchange(&self) -> &'static str {
            "binance"
        }
        fn instrument(&self) -> &Instrument {
            &self.instrument
        }
        fn minimum_quantity(&self) -> Decimal {
            self.instrument.quantity_step
        }
        fn verify_current_instrument_rules(&mut self) -> Result<(), GridVenueError> {
            Ok(())
        }
        fn best_bid_ask(&self, _now_ms: u64) -> Result<(Price, Price), GridVenueError> {
            Err(GridVenueError::PublicNotReady)
        }
        fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
            self.readbacks
                .pop_front()
                .ok_or(GridVenueError::PrivateReadbackRequired)
        }
        fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
            Ok(())
        }
        fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
            Ok(None)
        }
        fn reset_private_stream(&mut self) {}
        fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
            Arc::new(NoMutationClient(self.mutations.clone()))
        }
        fn order_by_client_id(&mut self, _client_order_id: &str) -> Result<Order, GridVenueError> {
            Err(GridVenueError::PrivateReadbackRequired)
        }
        fn verify_post_only_order(&mut self, _client_order_id: &str) -> Result<(), GridVenueError> {
            Err(GridVenueError::PrivateReadbackRequired)
        }
    }

    impl Stage7CanaryVenue for BridgeVenue {
        fn capability_binding(&self) -> CapabilityBinding {
            CapabilityBinding {
                exchange: "binance".to_owned(),
                account_binding: "portfolio_margin_um".to_owned(),
                symbol: "SOL/USDC".to_owned(),
                api_key_sha256: "a".repeat(64),
            }
        }
        fn place_market_reduce(
            &mut self,
            _command: &MarketReduceCommand,
        ) -> Result<String, GridVenueError> {
            self.mutations.fetch_add(1, Ordering::SeqCst);
            Err(GridVenueError::PrivateReadbackRequired)
        }
    }

    struct Fixture {
        _temporary: TempDir,
        root: PathBuf,
        cfg: Config,
        request: BinanceLegacyStage7BridgeRequest,
        successor_sha256: String,
        mutations: Arc<AtomicUsize>,
    }

    fn fixture(
        long: Decimal,
        short: Decimal,
        orders: Vec<Order>,
    ) -> Result<(Fixture, BridgeVenue), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = fs::canonicalize(temporary.path())?;
        let config_path = root.join("venue.toml");
        fs::write(
            &config_path,
            concat!(
                "trading_account_id = \"00000000-0000-4000-8000-000000000001\"\n",
                "symbol = \"SOL/USDC\"\n",
                "[binance]\naccount_binding = \"portfolio_margin_um\"\n",
                "private_custody_max_stale_ms = 5000\n",
                "[hedged_grid]\ngrid_count = 10\n",
            ),
        )?;
        let cfg = Config {
            log: LogLevel::Info,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "SOL/USDC".parse()?,
            binance: Some(BinanceConfig {
                account_binding: BinanceAccountBinding::PortfolioMarginUm,
                private_custody_max_stale_ms: 5_000,
            }),
            gate: None,
            bitget: None,
            hedged_grid: Some(HedgedGridConfig {
                grid_count: 10,
                exposure_take_profit: None,
            }),
        };
        let legacy_executable = root.join("legacy.exe");
        let successor_executable = root.join("successor.exe");
        fs::write(&legacy_executable, b"legacy-binance")?;
        fs::write(&successor_executable, b"shared-stage7")?;
        let legacy_sha256 = file_sha256(&legacy_executable)?;
        let successor_sha256 = file_sha256(&successor_executable)?;
        let legacy_binding = hedged_grid_live::phase_one_binding()?;
        let mut state = HedgedGridState::new_with_params(
            legacy_binding.clone(),
            HedgedGridParams::phase_one(10)?,
        )?;
        state.phase = GridPhase::Stopping;
        ProjectionStore::new(root.join(hedged_grid_live::GRID_CHECKPOINT_FILE)).save(
            &hedged_grid_live::HedgedGridCheckpoint {
                schema_version: 1,
                state,
            },
        )?;
        ProjectionStore::new(root.join(hedged_grid_live::GRID_CONTROL_FILE)).save(
            &hedged_grid_live::HedgedGridControl {
                schema_version: 1,
                binding: legacy_binding.clone(),
                target: HedgedGridControlTarget::Stop,
            },
        )?;
        let authority = WriterLeaseAuthority::open(
            root.join(hedged_grid_live::WRITER_FILE),
            WriterScope {
                exchange: legacy_binding.exchange.clone(),
                account: legacy_binding.account.clone(),
                symbol: legacy_binding.symbol.clone(),
                owner_scope: legacy_binding.owner_scope.clone(),
            },
        )?;
        let _writer = authority.register_initial(1, 1)?;
        let private = root.join(LEGACY_PRIVATE_DIRECTORY);
        fs::create_dir_all(&private)?;
        let mut evidence = PrivateEvidenceJournal::open(private.join(LEGACY_PRIVATE_FILES[0]))?;
        evidence.append(PrivateEvidence::new(1, 1, "{\"legacy\":true}".to_owned())?)?;
        fs::write(private.join(LEGACY_PRIVATE_FILES[1]), b"{}")?;
        fs::write(private.join(LEGACY_PRIVATE_FILES[2]), b"")?;
        fs::write(private.join(LEGACY_PRIVATE_FILES[3]), b"{}")?;
        let instrument = Instrument {
            symbol: cfg.symbol.clone(),
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(Asset::new("USDC")?),
            generation: 1,
            price_tick: Price::new(Decimal::new(1, 2))?,
            quantity_step: Decimal::new(1, 3),
            minimum_notional: Amount::new(Asset::new("USDC")?, Decimal::ZERO),
        };
        let entry_price = Price::new(Decimal::new(100, 0))?;
        let mark_price = Price::new(Decimal::new(101, 0))?;
        let positions = [(PositionSide::Long, long), (PositionSide::Short, short)]
            .into_iter()
            .filter(|(_, quantity)| !quantity.is_zero())
            .map(|(side, quantity)| Position {
                symbol: cfg.symbol.clone(),
                side,
                quantity,
                entry_price: Some(entry_price),
                mark_price: Some(mark_price),
            })
            .collect::<Vec<_>>();
        let readback = GridVenueReadback {
            raw_private_payloads: vec!["{\"signed\":true}".to_owned()],
            order_family_readback: Some(GridOrderFamilyReadback::regular_only_adapter_profile(
                orders.clone(),
                vec!["[]".to_owned()],
            )?),
            balance: AccountBalance {
                // Portfolio Margin account risk is normalized in USDT even for a USDC contract.
                asset: Asset::new("USDT")?,
                wallet_balance: Decimal::new(100, 0),
                available_balance: Decimal::new(100, 0),
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            },
            hedge_position: true,
            positions,
            orders,
            fills: Vec::<GridVenueFill>::new(),
        };
        let mutations = Arc::new(AtomicUsize::new(0));
        let venue = BridgeVenue {
            instrument,
            readbacks: VecDeque::from([readback.clone(), readback]),
            mutations: mutations.clone(),
        };
        let request = BinanceLegacyStage7BridgeRequest {
            artifacts_root: root.clone(),
            legacy_config_path: config_path,
            legacy_executable_path: legacy_executable,
            successor_executable_path: successor_executable,
            expected_legacy_executable_sha256: legacy_sha256,
            expected_successor_executable_sha256: successor_sha256.clone(),
            confirm_mainnet_nonflat_legacy_bridge: true,
        };
        Ok((
            Fixture {
                _temporary: temporary,
                root,
                cfg,
                request,
                successor_sha256,
                mutations,
            },
            venue,
        ))
    }

    #[test]
    fn single_leg_bridge_is_non_mutating_idempotent_and_admits_only_exact_receipt()
    -> Result<(), Box<dyn std::error::Error>> {
        let (fixture, mut venue) = fixture(Decimal::ONE, Decimal::ZERO, Vec::new())?;
        let report = run_bridge_with_venue(
            &fixture.cfg,
            fixture.request.clone(),
            &mut venue,
            &fixture.successor_sha256,
        )?;
        assert!(!report.idempotent_replay);
        assert_eq!(report.long_quantity, Decimal::ONE);
        assert_eq!(report.short_quantity, Decimal::ZERO);
        assert_eq!(fixture.mutations.load(Ordering::SeqCst), 0);
        let replay = run_bridge_with_venue(
            &fixture.cfg,
            fixture.request.clone(),
            &mut venue,
            &fixture.successor_sha256,
        )?;
        assert!(replay.idempotent_replay);
        let binding = binance_binding(&fixture.cfg)?;
        let params = release_params(&fixture.cfg, &binding)?;
        assert!(require_binance_legacy_bridge_admission(
            &venue,
            &binding,
            &params,
            &None,
            &fixture.root,
            &fixture.successor_sha256,
            super::super::wall_clock_ms()?,
        )?);
        fs::write(fixture.root.join(STAGE7_LIVE_ADMISSION_FILE), b"{}")?;
        assert!(
            require_binance_legacy_bridge_admission(
                &venue,
                &binding,
                &params,
                &None,
                &fixture.root,
                &fixture.successor_sha256,
                super::super::wall_clock_ms()?,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn bridge_fails_closed_for_flat_or_visible_orders_without_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (flat, mut flat_venue) = fixture(Decimal::ZERO, Decimal::ZERO, Vec::new())?;
        assert!(
            run_bridge_with_venue(
                &flat.cfg,
                flat.request,
                &mut flat_venue,
                &flat.successor_sha256,
            )
            .is_err()
        );
        assert_eq!(flat.mutations.load(Ordering::SeqCst), 0);

        let (foreign, mut foreign_venue) = fixture(
            Decimal::ZERO,
            Decimal::ONE,
            vec![Order {
                time_in_force: venue_domain::FieldState::Known(Default::default()),
                symbol: "SOL/USDC".parse()?,
                order_id: "foreign".to_owned(),
                client_order_id: crate::domain::FieldState::Missing,
                side: crate::domain::OrderSide::Buy,
                position_side: crate::domain::FieldState::Known(PositionSide::Long),
                purpose: crate::domain::FieldState::Missing,
                state: crate::domain::OrderState::New,
                limit_price: Some(Price::new(Decimal::ONE)?),
                average_price: crate::domain::FieldState::Missing,
                quantity: Decimal::ONE,
                filled_quantity: Decimal::ZERO,
                reduce_only: false,
            }],
        )?;
        assert!(
            run_bridge_with_venue(
                &foreign.cfg,
                foreign.request,
                &mut foreign_venue,
                &foreign.successor_sha256,
            )
            .is_err()
        );
        assert_eq!(foreign.mutations.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn single_short_leg_is_a_valid_nonflat_bridge() -> Result<(), Box<dyn std::error::Error>> {
        let (fixture, mut venue) = fixture(Decimal::ZERO, Decimal::ONE, Vec::new())?;
        let report = run_bridge_with_venue(
            &fixture.cfg,
            fixture.request,
            &mut venue,
            &fixture.successor_sha256,
        )?;
        assert_eq!(report.long_quantity, Decimal::ZERO);
        assert_eq!(report.short_quantity, Decimal::ONE);
        assert_eq!(fixture.mutations.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[test]
    fn unresolved_legacy_wal_blocks_bridge_before_readback_or_mutation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (fixture, mut venue) = fixture(Decimal::ONE, Decimal::ZERO, Vec::new())?;
        let binding = hedged_grid_live::phase_one_binding()?;
        let mut journal = CommandJournal::open(fixture.root.join(hedged_grid_live::COMMAND_FILE))?;
        journal.prepare_place(OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new("hgo_e1_long_open_l1_cmd")?,
            client_order_id: CommandId::new("hgo_e1_long_open_l1")?,
            owner: OrderOwner {
                strategy_instance_id: binding.strategy_instance_id,
                run_id: binding.run_id,
                exchange: binding.exchange,
                account: binding.account,
                symbol: binding.symbol,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        })?;
        assert!(
            run_bridge_with_venue(
                &fixture.cfg,
                fixture.request,
                &mut venue,
                &fixture.successor_sha256,
            )
            .is_err()
        );
        assert_eq!(fixture.mutations.load(Ordering::SeqCst), 0);
        Ok(())
    }
}
