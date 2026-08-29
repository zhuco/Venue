use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    domain::Price,
    execution::{CommandJournal, CommandJournalError, sha256_hex},
    storage::{PrivateEvidence, PrivateEvidenceError, ProjectionStore, StorageError},
    strategy::hedged_grid::{
        GridOrderKey, GridOrderRole, GridPhase, HedgedGridBinding, InventoryDeficiency,
        InventoryRecoveryState,
    },
};

use super::{
    Stage7GridError,
    stage7_canary_support::{STAGE7_LIVE_ADMISSION_FILE, Stage7LiveAdmissionEvidence},
    stage7_executable_handoff::immediate_executable_handoff_private_generation,
    stage7_grid_model::Stage7GridCheckpoint,
};

pub const INVENTORY_RECOVERY_EVIDENCE_FILE: &str = "grid_inventory_recovery_evidence.jsonl";
const SCHEMA_VERSION: u16 = 1;
const CHECKPOINT_FILE: &str = "hedged_grid_state.json";
const COMMAND_FILE: &str = "commands.jsonl";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseIdentity {
    admission_sha256: String,
    executable_sha256: String,
    configuration_sha256: String,
}

impl ReleaseIdentity {
    fn from_admission(admission: &Stage7LiveAdmissionEvidence) -> Self {
        Self {
            admission_sha256: admission.admission_sha256.clone(),
            executable_sha256: admission.executable_sha256.clone(),
            configuration_sha256: admission.configuration_sha256.clone(),
        }
    }

    fn validate(&self) -> Result<(), InventoryRecoveryEvidenceError> {
        if [
            &self.admission_sha256,
            &self.executable_sha256,
            &self.configuration_sha256,
        ]
        .into_iter()
        .all(|value| valid_sha256(value))
        {
            Ok(())
        } else {
            Err(InventoryRecoveryEvidenceError::Release)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryBasis {
    checkpoint_generation: u64,
    inventory_generation: u64,
    inventory_observed_at_ms: u64,
    epoch: u64,
    anchor_price: Price,
    grid_count: u8,
    #[serde(with = "rust_decimal::serde::str")]
    grid_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    required_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    long_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    short_quantity: Decimal,
    private_evidence: PrivateGenerationReference,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateGenerationReference {
    generation: u64,
    first_sequence: u64,
    last_sequence: u64,
    payload_count: u32,
    payloads_sha256: String,
}

impl RecoveryBasis {
    fn deficiency(&self) -> InventoryDeficiency {
        InventoryDeficiency {
            long: self.long_quantity < self.required_quantity,
            short: self.short_quantity < self.required_quantity,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum RecoveryEvidenceEvent {
    Deficient {
        first_seen_generation: u64,
        legs: InventoryDeficiency,
        basis: RecoveryBasis,
    },
    Armed {
        armed_generation: u64,
        basis: RecoveryBasis,
    },
    MakerFillSelected {
        armed_generation: u64,
        fill_generation: u64,
        fill_id: String,
        fill_price: Price,
        source_order: GridOrderKey,
        maker: bool,
        fill_private_evidence: PrivateGenerationReference,
        basis: RecoveryBasis,
    },
    Rebuilding {
        fill_id: String,
        fill_price: Price,
        basis: RecoveryBasis,
    },
    Settled {
        fill_id: String,
        fill_price: Price,
        settled_generation: u64,
        basis: RecoveryBasis,
        long_opening: u8,
        short_opening: u8,
        long_closing: u8,
        short_closing: u8,
        desired_orders: u16,
        observed_orders: u16,
        unresolved_wal: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        passive_book_fallback: Option<crate::strategy::hedged_grid::PassiveBookFallbackAnchor>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryEvidenceRecord {
    schema_version: u16,
    sequence: u64,
    previous_sha256: Option<String>,
    release: ReleaseIdentity,
    binding: HedgedGridBinding,
    episode_id: String,
    evidence: RecoveryEvidenceEvent,
    record_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryRecoveryAcceptanceReport {
    pub exchange: String,
    pub account: String,
    pub symbol: String,
    pub admission_sha256: String,
    pub executable_sha256: String,
    pub configuration_sha256: String,
    pub episode_id: String,
    pub deficient_generation: u64,
    pub armed_generation: u64,
    pub fill_generation: u64,
    pub fill_id: String,
    pub fill_price: Price,
    pub settled_generation: u64,
    pub settlement_checkpoint_generation: u64,
    pub rebuilt_epoch: u64,
    pub rebuilt_anchor: Price,
    pub passive_book_fallback: Option<crate::strategy::hedged_grid::PassiveBookFallbackAnchor>,
    pub desired_orders: u16,
    pub observed_orders: u16,
}

pub(super) fn capture_stage7_checkpoint(
    checkpoint_path: &Path,
    checkpoint: &Stage7GridCheckpoint,
) -> Result<(), Stage7GridError> {
    let root = checkpoint_path
        .parent()
        .ok_or(InventoryRecoveryEvidenceError::Root)?;
    let admission = ProjectionStore::new(root.join(STAGE7_LIVE_ADMISSION_FILE))
        .load::<Stage7LiveAdmissionEvidence>()?;
    let Some(admission) = admission else {
        if root.join(INVENTORY_RECOVERY_EVIDENCE_FILE).exists() {
            return Err(InventoryRecoveryEvidenceError::Release.into());
        }
        return Ok(());
    };
    admission.validate()?;
    if admission.deployment_binding != checkpoint.binding
        || admission.parameter_release != checkpoint.state.params
    {
        return Err(InventoryRecoveryEvidenceError::Binding.into());
    }
    let handoff_private_generation =
        immediate_executable_handoff_private_generation(root, &admission)?;
    capture_checkpoint(
        root,
        checkpoint,
        ReleaseIdentity::from_admission(&admission),
        handoff_private_generation,
    )?;
    Ok(())
}

pub(super) fn capture_stage7_settlement(
    artifacts_root: &Path,
    checkpoint: &Stage7GridCheckpoint,
    commands: &CommandJournal,
    observed_orders: usize,
) -> Result<(), Stage7GridError> {
    let admission = ProjectionStore::new(artifacts_root.join(STAGE7_LIVE_ADMISSION_FILE))
        .load::<Stage7LiveAdmissionEvidence>()?
        .ok_or(InventoryRecoveryEvidenceError::Release)?;
    admission.validate()?;
    append_settlement(
        artifacts_root,
        checkpoint,
        ReleaseIdentity::from_admission(&admission),
        commands.has_unresolved(),
        observed_orders,
    )?;
    Ok(())
}

fn capture_checkpoint(
    root: &Path,
    checkpoint: &Stage7GridCheckpoint,
    release: ReleaseIdentity,
    handoff_private_generation: Option<u64>,
) -> Result<(), InventoryRecoveryEvidenceError> {
    release.validate()?;
    let path = root.join(INVENTORY_RECOVERY_EVIDENCE_FILE);
    let mut records = recover(&path)?;
    let last = records.last();
    if checkpoint.state.inventory_recovery == InventoryRecoveryState::Inactive
        || already_captured(last, &release, &checkpoint.state.inventory_recovery)
    {
        return Ok(());
    }
    if records.iter().all(|record| record.release != release)
        && handoff_private_generation
            .is_some_and(|generation| recovery_predates_generation(checkpoint, generation))
    {
        return Ok(());
    }
    let basis = basis(root, checkpoint)?;
    if let InventoryRecoveryState::Rebuilding {
        fill_id,
        fill_price,
    } = &checkpoint.state.inventory_recovery
        && last.is_some_and(|record| {
            record.release == release
                && matches!(record.evidence, RecoveryEvidenceEvent::Armed { .. })
        })
    {
        if let Some((episode_id, evidence)) = select_fill_event(
            root,
            checkpoint,
            last,
            &release,
            fill_id,
            fill_price,
            basis.clone(),
        )? {
            append(
                &path,
                records.last(),
                release.clone(),
                checkpoint,
                episode_id,
                evidence,
            )?;
        }
        records = recover(&path)?;
    }
    let last = records.last();
    let candidate = match &checkpoint.state.inventory_recovery {
        InventoryRecoveryState::Deficient {
            legs,
            first_seen_generation,
        } => deficient_candidate(
            checkpoint,
            last,
            &release,
            legs,
            *first_seen_generation,
            basis,
        )?,
        InventoryRecoveryState::AwaitingNextOwnedFill { armed_generation } => {
            armed_candidate(last, &release, *armed_generation, basis)?
        }
        InventoryRecoveryState::ReanchorPending {
            fill_id,
            fill_price,
        } => select_fill_event(root, checkpoint, last, &release, fill_id, fill_price, basis)?,
        InventoryRecoveryState::Rebuilding {
            fill_id,
            fill_price,
        } => rebuilding_candidate(last, &release, fill_id, fill_price, basis),
        InventoryRecoveryState::Inactive => None,
    };
    if let Some((episode_id, evidence)) = candidate {
        append(
            &path,
            records.last(),
            release,
            checkpoint,
            episode_id,
            evidence,
        )?;
    }
    Ok(())
}

pub(super) fn recovery_predates_generation(
    checkpoint: &Stage7GridCheckpoint,
    generation: u64,
) -> bool {
    match &checkpoint.state.inventory_recovery {
        InventoryRecoveryState::Inactive => false,
        InventoryRecoveryState::Deficient {
            first_seen_generation,
            ..
        } => *first_seen_generation < generation,
        InventoryRecoveryState::AwaitingNextOwnedFill { armed_generation } => {
            *armed_generation < generation
        }
        InventoryRecoveryState::ReanchorPending { fill_id, .. }
        | InventoryRecoveryState::Rebuilding { fill_id, .. } => checkpoint
            .state
            .owned_fill_records
            .get(fill_id)
            .is_some_and(|record| record.private_generation < generation),
    }
}

fn already_captured(
    last: Option<&RecoveryEvidenceRecord>,
    release: &ReleaseIdentity,
    state: &InventoryRecoveryState,
) -> bool {
    let Some(last) = last.filter(|record| record.release == *release) else {
        return false;
    };
    match (state, &last.evidence) {
        (
            InventoryRecoveryState::Deficient {
                first_seen_generation,
                ..
            },
            RecoveryEvidenceEvent::Deficient {
                first_seen_generation: recorded,
                ..
            },
        ) => first_seen_generation == recorded,
        (
            InventoryRecoveryState::AwaitingNextOwnedFill { armed_generation },
            RecoveryEvidenceEvent::Armed {
                armed_generation: recorded,
                ..
            },
        ) => armed_generation == recorded,
        (
            InventoryRecoveryState::ReanchorPending {
                fill_id,
                fill_price,
            },
            RecoveryEvidenceEvent::MakerFillSelected {
                fill_id: recorded_id,
                fill_price: recorded_price,
                ..
            },
        ) => fill_id == recorded_id && fill_price == recorded_price,
        (
            InventoryRecoveryState::Rebuilding {
                fill_id,
                fill_price,
            },
            RecoveryEvidenceEvent::Rebuilding {
                fill_id: recorded_id,
                fill_price: recorded_price,
                ..
            },
        ) => fill_id == recorded_id && fill_price == recorded_price,
        _ => false,
    }
}

fn deficient_candidate(
    checkpoint: &Stage7GridCheckpoint,
    last: Option<&RecoveryEvidenceRecord>,
    release: &ReleaseIdentity,
    legs: &InventoryDeficiency,
    first_seen_generation: u64,
    basis: RecoveryBasis,
) -> Result<Option<(String, RecoveryEvidenceEvent)>, InventoryRecoveryEvidenceError> {
    if basis.deficiency() != *legs || first_seen_generation > basis.inventory_generation {
        return Err(InventoryRecoveryEvidenceError::Transition);
    }
    let episode_id = episode_id(release, &checkpoint.binding, first_seen_generation, &basis)?;
    if last.is_some_and(|record| {
        record.episode_id == episode_id
            && matches!(record.evidence, RecoveryEvidenceEvent::Deficient { .. })
    }) {
        return Ok(None);
    }
    Ok(Some((
        episode_id,
        RecoveryEvidenceEvent::Deficient {
            first_seen_generation,
            legs: legs.clone(),
            basis,
        },
    )))
}

fn armed_candidate(
    last: Option<&RecoveryEvidenceRecord>,
    release: &ReleaseIdentity,
    armed_generation: u64,
    basis: RecoveryBasis,
) -> Result<Option<(String, RecoveryEvidenceEvent)>, InventoryRecoveryEvidenceError> {
    if basis.deficiency().any() || armed_generation != basis.inventory_generation {
        return Err(InventoryRecoveryEvidenceError::Transition);
    }
    match last {
        Some(record)
            if record.release == *release
                && matches!(record.evidence, RecoveryEvidenceEvent::Armed { .. }) =>
        {
            Ok(None)
        }
        Some(record)
            if record.release == *release
                && matches!(record.evidence, RecoveryEvidenceEvent::Deficient { .. }) =>
        {
            Ok(Some((
                record.episode_id.clone(),
                RecoveryEvidenceEvent::Armed {
                    armed_generation,
                    basis,
                },
            )))
        }
        _ => Ok(None),
    }
}

fn select_fill_event(
    root: &Path,
    checkpoint: &Stage7GridCheckpoint,
    last: Option<&RecoveryEvidenceRecord>,
    release: &ReleaseIdentity,
    fill_id: &str,
    fill_price: &Price,
    basis: RecoveryBasis,
) -> Result<Option<(String, RecoveryEvidenceEvent)>, InventoryRecoveryEvidenceError> {
    let Some(last) = last else {
        return Ok(None);
    };
    if last.release != *release {
        return Ok(None);
    }
    if matches!(
        last.evidence,
        RecoveryEvidenceEvent::MakerFillSelected { .. }
    ) {
        return Ok(None);
    }
    let RecoveryEvidenceEvent::Armed {
        armed_generation, ..
    } = last.evidence
    else {
        return Ok(None);
    };
    let record = checkpoint
        .state
        .owned_fill_records
        .get(fill_id)
        .ok_or(InventoryRecoveryEvidenceError::Transition)?;
    if record.fill_price != *fill_price
        || record.private_generation <= armed_generation
        || record.maker != Some(true)
        || !record.grid_action_emitted
    {
        return Err(InventoryRecoveryEvidenceError::Transition);
    }
    Ok(Some((
        last.episode_id.clone(),
        RecoveryEvidenceEvent::MakerFillSelected {
            armed_generation,
            fill_generation: record.private_generation,
            fill_id: fill_id.to_owned(),
            fill_price: *fill_price,
            source_order: record.source_order.key.clone(),
            maker: true,
            fill_private_evidence: private_generation_reference(
                &super::stage7_private_evidence_path(root, &checkpoint.binding)
                    .map_err(|_| InventoryRecoveryEvidenceError::PrivateReference)?,
                record.private_generation,
            )?,
            basis,
        },
    )))
}

fn rebuilding_candidate(
    last: Option<&RecoveryEvidenceRecord>,
    release: &ReleaseIdentity,
    fill_id: &str,
    fill_price: &Price,
    basis: RecoveryBasis,
) -> Option<(String, RecoveryEvidenceEvent)> {
    match last {
        Some(record)
            if record.release == *release
                && matches!(record.evidence, RecoveryEvidenceEvent::Rebuilding { .. }) =>
        {
            None
        }
        Some(record)
            if record.release == *release
                && matches!(
                    &record.evidence,
                    RecoveryEvidenceEvent::MakerFillSelected {
                        fill_id: previous_id,
                        fill_price: previous_price,
                        ..
                    } if previous_id == fill_id && previous_price == fill_price
                ) =>
        {
            Some((
                record.episode_id.clone(),
                RecoveryEvidenceEvent::Rebuilding {
                    fill_id: fill_id.to_owned(),
                    fill_price: *fill_price,
                    basis,
                },
            ))
        }
        _ => None,
    }
}

fn append_settlement(
    root: &Path,
    checkpoint: &Stage7GridCheckpoint,
    release: ReleaseIdentity,
    unresolved_wal: bool,
    observed_orders: usize,
) -> Result<(), InventoryRecoveryEvidenceError> {
    let path = root.join(INVENTORY_RECOVERY_EVIDENCE_FILE);
    let records = recover(&path)?;
    let Some(last) = records.last() else {
        return Ok(());
    };
    if last.release != release || matches!(last.evidence, RecoveryEvidenceEvent::Settled { .. }) {
        return Ok(());
    }
    let RecoveryEvidenceEvent::Rebuilding {
        fill_id,
        fill_price,
        ..
    } = &last.evidence
    else {
        return Ok(());
    };
    if unresolved_wal
        || checkpoint.state.phase != GridPhase::Running
        || checkpoint.state.inventory_recovery != InventoryRecoveryState::Inactive
        || !checkpoint.state.pending_transactions.is_empty()
        || !checkpoint.state.pending_replenishments.is_empty()
    {
        return Err(InventoryRecoveryEvidenceError::Transition);
    }
    let basis = basis(root, checkpoint)?;
    let passive_book_fallback = checkpoint
        .state
        .epoch
        .as_ref()
        .and_then(|epoch| epoch.passive_book_fallback.clone());
    if basis.deficiency().any()
        || !valid_settlement_anchor(
            fill_id,
            *fill_price,
            basis.anchor_price,
            passive_book_fallback.as_ref(),
        )
    {
        return Err(InventoryRecoveryEvidenceError::Transition);
    }
    let (long_opening, short_opening, long_closing, short_closing) = order_counts(checkpoint)?;
    let desired_orders = checkpoint.state.owned_orders.len();
    if desired_orders != observed_orders
        || [long_opening, short_opening, long_closing, short_closing]
            .into_iter()
            .any(|count| count != checkpoint.state.params.grid_count)
    {
        return Err(InventoryRecoveryEvidenceError::Transition);
    }
    // The exposure lane may advance the checkpoint's private watermark after the grid inventory
    // projection was captured. Settlement is authorized by that inventory projection, so new
    // evidence records its generation instead of the later account-level watermark.
    let settled_generation = basis.inventory_generation;
    append(
        &path,
        records.last(),
        release,
        checkpoint,
        last.episode_id.clone(),
        RecoveryEvidenceEvent::Settled {
            fill_id: fill_id.clone(),
            fill_price: *fill_price,
            settled_generation,
            basis,
            long_opening,
            short_opening,
            long_closing,
            short_closing,
            desired_orders: u16::try_from(desired_orders)
                .map_err(|_| InventoryRecoveryEvidenceError::Transition)?,
            observed_orders: u16::try_from(observed_orders)
                .map_err(|_| InventoryRecoveryEvidenceError::Transition)?,
            unresolved_wal,
            passive_book_fallback,
        },
    )
}

fn valid_settlement_anchor(
    fill_id: &str,
    fill_price: Price,
    rebuilt_anchor: Price,
    fallback: Option<&crate::strategy::hedged_grid::PassiveBookFallbackAnchor>,
) -> bool {
    match fallback {
        None => rebuilt_anchor == fill_price,
        Some(fallback) => {
            fallback.validate().is_ok()
                && fallback.matches_fill(fill_id, fill_price)
                && fallback.anchor_price == rebuilt_anchor
        }
    }
}

fn basis(
    root: &Path,
    checkpoint: &Stage7GridCheckpoint,
) -> Result<RecoveryBasis, InventoryRecoveryEvidenceError> {
    let epoch = checkpoint
        .state
        .epoch
        .as_ref()
        .ok_or(InventoryRecoveryEvidenceError::Transition)?;
    let inventory = checkpoint
        .state
        .inventory
        .as_ref()
        .ok_or(InventoryRecoveryEvidenceError::Transition)?;
    let required_quantity = epoch
        .grid_quantity
        .checked_mul(Decimal::from(checkpoint.state.params.grid_count))
        .ok_or(InventoryRecoveryEvidenceError::Transition)?;
    Ok(RecoveryBasis {
        checkpoint_generation: checkpoint.private_generation,
        inventory_generation: inventory.private_generation,
        inventory_observed_at_ms: inventory.private_observed_at_ms,
        epoch: epoch.epoch,
        anchor_price: epoch.anchor_price,
        grid_count: checkpoint.state.params.grid_count,
        grid_quantity: epoch.grid_quantity,
        required_quantity,
        long_quantity: inventory.long_quantity,
        short_quantity: inventory.short_quantity,
        private_evidence: private_generation_reference(
            &super::stage7_private_evidence_path(root, &checkpoint.binding)
                .map_err(|_| InventoryRecoveryEvidenceError::PrivateReference)?,
            inventory.private_generation,
        )?,
    })
}

fn private_generation_reference(
    path: &Path,
    generation: u64,
) -> Result<PrivateGenerationReference, InventoryRecoveryEvidenceError> {
    let file = File::open(path)?;
    let mut references = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let evidence: PrivateEvidence = serde_json::from_str(&line)?;
        if evidence.generation == generation {
            if !evidence.valid_hash() {
                return Err(InventoryRecoveryEvidenceError::HashChain);
            }
            references.push((evidence.sequence, evidence.payload_sha256));
        }
    }
    generation_reference(generation, &references)
}

fn generation_reference(
    generation: u64,
    references: &[(u64, String)],
) -> Result<PrivateGenerationReference, InventoryRecoveryEvidenceError> {
    let first_sequence = references
        .first()
        .map(|(sequence, _)| *sequence)
        .ok_or(InventoryRecoveryEvidenceError::PrivateReference)?;
    let last_sequence = references
        .last()
        .map(|(sequence, _)| *sequence)
        .ok_or(InventoryRecoveryEvidenceError::PrivateReference)?;
    Ok(PrivateGenerationReference {
        generation,
        first_sequence,
        last_sequence,
        payload_count: u32::try_from(references.len())
            .map_err(|_| InventoryRecoveryEvidenceError::PrivateReference)?,
        payloads_sha256: sha256_hex(&serde_json::to_vec(references)?),
    })
}

fn order_counts(
    checkpoint: &Stage7GridCheckpoint,
) -> Result<(u8, u8, u8, u8), InventoryRecoveryEvidenceError> {
    let mut counts = BTreeMap::new();
    for key in checkpoint.state.owned_orders.keys() {
        let count = counts.entry((key.position, key.role)).or_insert(0_u8);
        *count = count
            .checked_add(1)
            .ok_or(InventoryRecoveryEvidenceError::Transition)?;
    }
    use crate::strategy::hedged_grid::GridPosition::{Long, Short};
    Ok((
        counts
            .get(&(Long, GridOrderRole::Open))
            .copied()
            .unwrap_or(0),
        counts
            .get(&(Short, GridOrderRole::Open))
            .copied()
            .unwrap_or(0),
        counts
            .get(&(Long, GridOrderRole::Close))
            .copied()
            .unwrap_or(0),
        counts
            .get(&(Short, GridOrderRole::Close))
            .copied()
            .unwrap_or(0),
    ))
}

fn episode_id(
    release: &ReleaseIdentity,
    binding: &HedgedGridBinding,
    first_seen_generation: u64,
    basis: &RecoveryBasis,
) -> Result<String, InventoryRecoveryEvidenceError> {
    Ok(sha256_hex(&serde_json::to_vec(&(
        release,
        binding,
        first_seen_generation,
        basis,
    ))?))
}

fn append(
    path: &Path,
    previous: Option<&RecoveryEvidenceRecord>,
    release: ReleaseIdentity,
    checkpoint: &Stage7GridCheckpoint,
    episode_id: String,
    evidence: RecoveryEvidenceEvent,
) -> Result<(), InventoryRecoveryEvidenceError> {
    let sequence = previous
        .map(|record| record.sequence.saturating_add(1))
        .unwrap_or(1);
    let mut record = RecoveryEvidenceRecord {
        schema_version: SCHEMA_VERSION,
        sequence,
        previous_sha256: previous.map(|record| record.record_sha256.clone()),
        release,
        binding: checkpoint.binding.clone(),
        episode_id,
        evidence,
        record_sha256: String::new(),
    };
    record.record_sha256 = record_digest(&record)?;
    let encoded = serde_json::to_vec(&record)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&encoded)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    Ok(())
}

fn recover(path: &Path) -> Result<Vec<RecoveryEvidenceRecord>, InventoryRecoveryEvidenceError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => return Err(source.into()),
    };
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(InventoryRecoveryEvidenceError::Truncated);
    }
    let mut records = Vec::new();
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: RecoveryEvidenceRecord = serde_json::from_slice(line)?;
        if record.schema_version != SCHEMA_VERSION
            || record.sequence != records.len() as u64 + 1
            || record.previous_sha256
                != records
                    .last()
                    .map(|previous: &RecoveryEvidenceRecord| previous.record_sha256.clone())
            || !valid_sha256(&record.episode_id)
            || !valid_sha256(&record.record_sha256)
            || record_digest(&record)? != record.record_sha256
        {
            return Err(InventoryRecoveryEvidenceError::HashChain);
        }
        record.release.validate()?;
        record
            .binding
            .validate()
            .map_err(|_| InventoryRecoveryEvidenceError::Binding)?;
        records.push(record);
    }
    Ok(records)
}

fn record_digest(
    record: &RecoveryEvidenceRecord,
) -> Result<String, InventoryRecoveryEvidenceError> {
    let mut unsigned = record.clone();
    unsigned.record_sha256.clear();
    Ok(sha256_hex(&serde_json::to_vec(&unsigned)?))
}

pub fn verify_stage7_inventory_recovery_evidence(
    artifacts_root: &Path,
) -> Result<InventoryRecoveryAcceptanceReport, InventoryRecoveryEvidenceError> {
    if !artifacts_root.is_absolute() {
        return Err(InventoryRecoveryEvidenceError::Root);
    }
    let admission = ProjectionStore::new(artifacts_root.join(STAGE7_LIVE_ADMISSION_FILE))
        .load::<Stage7LiveAdmissionEvidence>()?
        .ok_or(InventoryRecoveryEvidenceError::Release)?;
    admission
        .validate()
        .map_err(|_| InventoryRecoveryEvidenceError::Release)?;
    let checkpoint = ProjectionStore::new(artifacts_root.join(CHECKPOINT_FILE))
        .load::<Stage7GridCheckpoint>()?
        .ok_or(InventoryRecoveryEvidenceError::Incomplete)?;
    if checkpoint.binding != admission.deployment_binding
        || checkpoint.state.params != admission.parameter_release
    {
        return Err(InventoryRecoveryEvidenceError::Binding);
    }
    let release = ReleaseIdentity::from_admission(&admission);
    let records = recover(&artifacts_root.join(INVENTORY_RECOVERY_EVIDENCE_FILE))?;
    // Establish a complete, structurally valid five-stage episode before touching the private
    // or command journals. Production private evidence can be hundreds of megabytes, while the
    // common "not observed yet" result must remain a cheap read-only check.
    let (accepted, private_references) = accepted_episode(&records, &release, &checkpoint.binding)?;
    validate_checkpoint_episode(&checkpoint, &accepted)?;
    let private_path = super::stage7_private_evidence_path(artifacts_root, &checkpoint.binding)
        .map_err(|_| InventoryRecoveryEvidenceError::PrivateReference)?;
    let command_path = artifacts_root.join(COMMAND_FILE);
    if !private_path.is_file() || !command_path.is_file() {
        return Err(InventoryRecoveryEvidenceError::Incomplete);
    }
    validate_private_references(&private_path, &private_references)?;
    let commands = CommandJournal::open(command_path)?;
    if checkpoint.private_generation < accepted.settled_generation || commands.has_unresolved() {
        return Err(InventoryRecoveryEvidenceError::Incomplete);
    }
    Ok(accepted)
}

fn validate_checkpoint_episode(
    checkpoint: &Stage7GridCheckpoint,
    accepted: &InventoryRecoveryAcceptanceReport,
) -> Result<(), InventoryRecoveryEvidenceError> {
    let state = &checkpoint.state;
    let epoch = state
        .epoch
        .as_ref()
        .ok_or(InventoryRecoveryEvidenceError::Transition)?;
    let inventory = state
        .inventory
        .as_ref()
        .ok_or(InventoryRecoveryEvidenceError::Transition)?;
    let fill = state
        .owned_fill_records
        .get(&accepted.fill_id)
        .ok_or(InventoryRecoveryEvidenceError::Transition)?;
    if checkpoint.schema_version != 1
        || checkpoint.binding != state.binding
        || checkpoint.private_generation < accepted.settlement_checkpoint_generation
        || inventory.private_generation < accepted.settled_generation
        || epoch.epoch < accepted.rebuilt_epoch
        || (epoch.epoch == accepted.rebuilt_epoch && epoch.anchor_price != accepted.rebuilt_anchor)
        || (epoch.epoch == accepted.rebuilt_epoch
            && epoch.passive_book_fallback != accepted.passive_book_fallback)
        || fill.fill_price != accepted.fill_price
        || fill.private_generation != accepted.fill_generation
        || fill.maker != Some(true)
        || !fill.grid_action_emitted
        || fill.retired_without_action
        || state.seen_fill_ids.get(&accepted.fill_id) != Some(&fill.source_order.key)
    {
        return Err(InventoryRecoveryEvidenceError::Transition);
    }
    Ok(())
}

fn accepted_episode(
    records: &[RecoveryEvidenceRecord],
    release: &ReleaseIdentity,
    binding: &HedgedGridBinding,
) -> Result<
    (
        InventoryRecoveryAcceptanceReport,
        Vec<PrivateGenerationReference>,
    ),
    InventoryRecoveryEvidenceError,
> {
    for window in records.windows(5).rev() {
        if window.iter().any(|record| {
            record.release != *release
                || record.binding != *binding
                || record.episode_id != window[0].episode_id
        }) {
            continue;
        }
        if let Some(report) = accepted_window(window, release, binding)? {
            return Ok(report);
        }
    }
    Err(InventoryRecoveryEvidenceError::Incomplete)
}

fn accepted_window(
    window: &[RecoveryEvidenceRecord],
    release: &ReleaseIdentity,
    binding: &HedgedGridBinding,
) -> Result<
    Option<(
        InventoryRecoveryAcceptanceReport,
        Vec<PrivateGenerationReference>,
    )>,
    InventoryRecoveryEvidenceError,
> {
    let RecoveryEvidenceEvent::Deficient {
        first_seen_generation,
        legs,
        basis: deficient,
    } = &window[0].evidence
    else {
        return Ok(None);
    };
    let RecoveryEvidenceEvent::Armed {
        armed_generation,
        basis: armed,
    } = &window[1].evidence
    else {
        return Ok(None);
    };
    let RecoveryEvidenceEvent::MakerFillSelected {
        armed_generation: fill_armed_generation,
        fill_generation,
        fill_id,
        fill_price,
        maker,
        fill_private_evidence,
        basis: selected,
        ..
    } = &window[2].evidence
    else {
        return Ok(None);
    };
    let RecoveryEvidenceEvent::Rebuilding {
        fill_id: rebuilding_fill_id,
        fill_price: rebuilding_fill_price,
        basis: rebuilding,
    } = &window[3].evidence
    else {
        return Ok(None);
    };
    let RecoveryEvidenceEvent::Settled {
        fill_id: settled_fill_id,
        fill_price: settled_fill_price,
        settled_generation,
        basis: settled,
        long_opening,
        short_opening,
        long_closing,
        short_closing,
        desired_orders,
        observed_orders,
        unresolved_wal,
        passive_book_fallback,
    } = &window[4].evidence
    else {
        return Ok(None);
    };
    let counts = [long_opening, short_opening, long_closing, short_closing];
    let private_references = [
        &deficient.private_evidence,
        &armed.private_evidence,
        &selected.private_evidence,
        fill_private_evidence,
        &rebuilding.private_evidence,
        &settled.private_evidence,
    ];
    if !legs.any()
        || deficient.deficiency() != *legs
        || armed.deficiency().any()
        || settled.deficiency().any()
        || private_references.iter().any(|reference| {
            reference.payload_count == 0
                || reference.first_sequence == 0
                || reference.last_sequence < reference.first_sequence
                || reference.generation == 0
                || !valid_sha256(&reference.payloads_sha256)
        })
        || deficient.private_evidence.generation != deficient.inventory_generation
        || armed.private_evidence.generation != armed.inventory_generation
        || selected.private_evidence.generation != selected.inventory_generation
        || fill_private_evidence.generation != *fill_generation
        || rebuilding.private_evidence.generation != rebuilding.inventory_generation
        || settled.private_evidence.generation != settled.inventory_generation
        || settled.inventory_generation > settled.checkpoint_generation
        || (*settled_generation != settled.inventory_generation
            && *settled_generation != settled.checkpoint_generation)
        || *armed_generation != armed.inventory_generation
        || fill_armed_generation != armed_generation
        || *fill_generation <= *armed_generation
        || !maker
        || rebuilding_fill_id != fill_id
        || settled_fill_id != fill_id
        || rebuilding_fill_price != fill_price
        || settled_fill_price != fill_price
        || !valid_settlement_anchor(
            fill_id,
            *fill_price,
            settled.anchor_price,
            passive_book_fallback.as_ref(),
        )
        || *unresolved_wal
        || desired_orders != observed_orders
        || counts.into_iter().any(|count| *count != settled.grid_count)
        || *first_seen_generation > deficient.inventory_generation
        || settled.inventory_generation < *fill_generation
    {
        return Err(InventoryRecoveryEvidenceError::Transition);
    }
    Ok(Some((
        InventoryRecoveryAcceptanceReport {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.to_string(),
            admission_sha256: release.admission_sha256.clone(),
            executable_sha256: release.executable_sha256.clone(),
            configuration_sha256: release.configuration_sha256.clone(),
            episode_id: window[0].episode_id.clone(),
            deficient_generation: *first_seen_generation,
            armed_generation: *armed_generation,
            fill_generation: *fill_generation,
            fill_id: fill_id.clone(),
            fill_price: *fill_price,
            settled_generation: settled.inventory_generation,
            settlement_checkpoint_generation: settled.checkpoint_generation,
            rebuilt_epoch: settled.epoch,
            rebuilt_anchor: settled.anchor_price,
            passive_book_fallback: passive_book_fallback.clone(),
            desired_orders: *desired_orders,
            observed_orders: *observed_orders,
        },
        private_references.into_iter().cloned().collect(),
    )))
}

fn validate_private_references(
    path: &Path,
    references: &[PrivateGenerationReference],
) -> Result<(), InventoryRecoveryEvidenceError> {
    let mut unique = BTreeMap::<(u64, u64), &PrivateGenerationReference>::new();
    for reference in references {
        let expected_count = reference
            .last_sequence
            .checked_sub(reference.first_sequence)
            .and_then(|difference| difference.checked_add(1))
            .ok_or(InventoryRecoveryEvidenceError::PrivateReference)?;
        if expected_count != u64::from(reference.payload_count) {
            return Err(InventoryRecoveryEvidenceError::PrivateReference);
        }
        match unique.insert(
            (reference.first_sequence, reference.last_sequence),
            reference,
        ) {
            Some(previous) if previous != reference => {
                return Err(InventoryRecoveryEvidenceError::PrivateReference);
            }
            Some(_) | None => {}
        }
    }
    let mut occupied = BTreeSet::new();
    for &(first, last) in unique.keys() {
        for sequence in first..=last {
            if !occupied.insert(sequence) {
                return Err(InventoryRecoveryEvidenceError::PrivateReference);
            }
        }
    }
    let maximum_sequence = unique
        .keys()
        .map(|(_, last)| *last)
        .max()
        .ok_or(InventoryRecoveryEvidenceError::PrivateReference)?;
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    if length == 0 {
        return Err(InventoryRecoveryEvidenceError::PrivateReference);
    }
    file.seek(SeekFrom::End(-1))?;
    let mut tail = [0_u8; 1];
    file.read_exact(&mut tail)?;
    if tail[0] != b'\n' {
        return Err(InventoryRecoveryEvidenceError::Truncated);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut line = Vec::new();
    let mut sequence = 0_u64;
    let mut matched = BTreeMap::<(u64, u64), Vec<(u64, String)>>::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        sequence = sequence
            .checked_add(1)
            .ok_or(InventoryRecoveryEvidenceError::PrivateReference)?;
        if occupied.contains(&sequence) {
            let evidence: PrivateEvidence = serde_json::from_slice(&line)?;
            if evidence.sequence != sequence || !evidence.valid_hash() {
                return Err(InventoryRecoveryEvidenceError::PrivateReference);
            }
            let key = unique
                .keys()
                .find(|(first, last)| *first <= sequence && sequence <= *last)
                .copied()
                .ok_or(InventoryRecoveryEvidenceError::PrivateReference)?;
            let reference = unique[&key];
            if evidence.generation != reference.generation {
                return Err(InventoryRecoveryEvidenceError::PrivateReference);
            }
            matched
                .entry(key)
                .or_default()
                .push((sequence, evidence.payload_sha256));
        }
        if sequence >= maximum_sequence {
            break;
        }
    }
    for (key, reference) in unique {
        let payloads = matched
            .remove(&key)
            .ok_or(InventoryRecoveryEvidenceError::PrivateReference)?;
        if generation_reference(reference.generation, &payloads)? != *reference {
            return Err(InventoryRecoveryEvidenceError::PrivateReference);
        }
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Debug, thiserror::Error)]
pub enum InventoryRecoveryEvidenceError {
    #[error("inventory recovery evidence requires an absolute artifacts root")]
    Root,
    #[error("inventory recovery evidence release identity is missing or invalid")]
    Release,
    #[error("inventory recovery evidence binding or parameter release does not match")]
    Binding,
    #[error("inventory recovery evidence transition is inconsistent")]
    Transition,
    #[error("inventory recovery evidence is incomplete")]
    Incomplete,
    #[error("inventory recovery evidence cannot bind a private generation to persisted payloads")]
    PrivateReference,
    #[error("inventory recovery evidence hash chain is invalid")]
    HashChain,
    #[error("inventory recovery evidence has a truncated tail")]
    Truncated,
    #[error("inventory recovery evidence I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("inventory recovery evidence JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Private(#[from] PrivateEvidenceError),
    #[error(transparent)]
    Command(#[from] CommandJournalError),
}
