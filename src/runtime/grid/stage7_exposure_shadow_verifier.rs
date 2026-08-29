use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    path::Path,
};

use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    domain::{AccountRiskSnapshot, LegRiskSnapshot, PositionSide},
    exchange::risk_replay::{RiskReplayRequest, replay_private_risk_payloads},
    runtime::hedged_grid::{
        EXPOSURE_SHADOW_EVIDENCE_FILE, ExposureShadowDecision, ExposureShadowEvidence,
        ExposureShadowEvidenceJournal, ExposureShadowReason,
    },
    storage::{PrivateEvidence, ProjectionStore, StorageError},
    strategy::hedged_grid::{ExposureGuardParams, GridPosition, HedgedGridBinding},
};

use super::{
    CHECKPOINT_FILE,
    stage7_canary_support::{STAGE7_LIVE_ADMISSION_FILE, Stage7LiveAdmissionEvidence},
    stage7_grid_model::Stage7GridCheckpoint,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExposureShadowVerifiedDecision {
    WouldReduce,
    NoMutation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExposureShadowVerifiedReason {
    ThresholdBreached,
    FlatLeg,
    BelowExposureThreshold,
    PnlNotPositive,
    PnlNotStrictlyAboveThreshold,
    GuardDisabled,
    EpisodeSuppressed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerifiedRawRiskEvidenceRef {
    pub sequence: u64,
    pub generation: u64,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExposureShadowLaneReport {
    pub position: GridPosition,
    pub shadow_sequence: u64,
    pub account: AccountRiskSnapshot,
    pub leg: Option<LegRiskSnapshot>,
    pub params: ExposureGuardParams,
    #[serde(with = "rust_decimal::serde::str")]
    pub exposure_notional_threshold: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl_threshold: Decimal,
    pub decision: ExposureShadowVerifiedDecision,
    pub reason: ExposureShadowVerifiedReason,
    pub risk_episode_id: Option<String>,
    pub validated_at_ms: u64,
    pub raw_evidence: Vec<VerifiedRawRiskEvidenceRef>,
    pub semantic_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExposureShadowVerificationReport {
    pub release_id: String,
    pub selection_rule: String,
    pub admission_sha256: String,
    pub executable_sha256: String,
    pub configuration_sha256: String,
    pub parameter_release_sha256: String,
    pub binding: HedgedGridBinding,
    pub long: ExposureShadowLaneReport,
    pub short: ExposureShadowLaneReport,
}

/// Verifies the normalized Shadow projection against the exact durable raw private records.
/// This function only opens existing files for reading; it never creates a journal or contacts a
/// venue.
pub fn verify_stage7_exposure_shadow_evidence(
    artifacts_root: &Path,
) -> Result<ExposureShadowVerificationReport, ExposureShadowVerificationError> {
    if !artifacts_root.is_absolute() {
        return Err(ExposureShadowVerificationError::Root);
    }
    let admission = ProjectionStore::new(artifacts_root.join(STAGE7_LIVE_ADMISSION_FILE))
        .load::<Stage7LiveAdmissionEvidence>()?
        .ok_or(ExposureShadowVerificationError::Release)?;
    admission
        .validate()
        .map_err(|_| ExposureShadowVerificationError::Release)?;
    let checkpoint = ProjectionStore::new(artifacts_root.join(CHECKPOINT_FILE))
        .load::<Stage7GridCheckpoint>()?
        .ok_or(ExposureShadowVerificationError::Incomplete)?;
    let guard = checkpoint
        .exposure_guard
        .as_ref()
        .ok_or(ExposureShadowVerificationError::Incomplete)?;
    if !admission.exposure_release_bound
        || (guard.params.enabled && admission.exposure_take_profit_sha256.is_none())
    {
        return Err(ExposureShadowVerificationError::Release);
    }
    if checkpoint.binding != admission.deployment_binding
        || checkpoint.state.params != admission.parameter_release
        || guard.binding != admission.deployment_binding
    {
        return Err(ExposureShadowVerificationError::Binding);
    }

    let private_path = super::stage7_private_evidence_path(artifacts_root, &checkpoint.binding)
        .map_err(|_| ExposureShadowVerificationError::PrivateReference)?;
    let shadow_path = artifacts_root.join(EXPOSURE_SHADOW_EVIDENCE_FILE);
    if !private_path.is_file()
        || (!shadow_path.is_file() && !shadow_archive_path(&shadow_path).is_file())
    {
        return Err(ExposureShadowVerificationError::Incomplete);
    }
    let shadow = ExposureShadowEvidenceJournal::open(shadow_path)
        .map_err(|_| ExposureShadowVerificationError::ShadowEvidence)?;
    let records = shadow
        .recover()
        .map_err(|_| ExposureShadowVerificationError::ShadowEvidence)?;
    if records.iter().any(|record| {
        record.binding != admission.deployment_binding || record.params != guard.params
    }) {
        return Err(ExposureShadowVerificationError::Binding);
    }
    let long_record = latest_record(&records, GridPosition::Long)?;
    let short_record = latest_record(&records, GridPosition::Short)?;
    let private = verify_private_references(&private_path, &records, [long_record, short_record])?;
    verify_semantic_replay(&admission, long_record, &private)?;
    verify_semantic_replay(&admission, short_record, &private)?;
    let long = lane_report(long_record);
    let short = lane_report(short_record);
    Ok(ExposureShadowVerificationReport {
        release_id: admission.release_id,
        selection_rule: admission.selection_rule,
        admission_sha256: admission.admission_sha256,
        executable_sha256: admission.executable_sha256,
        configuration_sha256: admission.configuration_sha256,
        parameter_release_sha256: admission.parameter_release_sha256,
        binding: admission.deployment_binding,
        long,
        short,
    })
}

fn latest_record(
    records: &[ExposureShadowEvidence],
    position: GridPosition,
) -> Result<&ExposureShadowEvidence, ExposureShadowVerificationError> {
    records
        .iter()
        .rev()
        .find(|record| record.position == position)
        .ok_or(ExposureShadowVerificationError::Incomplete)
}

fn lane_report(record: &ExposureShadowEvidence) -> ExposureShadowLaneReport {
    ExposureShadowLaneReport {
        position: record.position,
        shadow_sequence: record.sequence,
        account: record.account.clone(),
        leg: record.leg.clone(),
        params: record.params.clone(),
        exposure_notional_threshold: record.exposure_notional_threshold,
        unrealized_pnl_threshold: record.unrealized_pnl_threshold,
        decision: match record.decision {
            ExposureShadowDecision::WouldReduce => ExposureShadowVerifiedDecision::WouldReduce,
            ExposureShadowDecision::NoMutation => ExposureShadowVerifiedDecision::NoMutation,
        },
        reason: match record.reason {
            ExposureShadowReason::ThresholdBreached => {
                ExposureShadowVerifiedReason::ThresholdBreached
            }
            ExposureShadowReason::FlatLeg => ExposureShadowVerifiedReason::FlatLeg,
            ExposureShadowReason::BelowExposureThreshold => {
                ExposureShadowVerifiedReason::BelowExposureThreshold
            }
            ExposureShadowReason::PnlNotPositive => ExposureShadowVerifiedReason::PnlNotPositive,
            ExposureShadowReason::PnlNotStrictlyAboveThreshold => {
                ExposureShadowVerifiedReason::PnlNotStrictlyAboveThreshold
            }
            ExposureShadowReason::GuardDisabled => ExposureShadowVerifiedReason::GuardDisabled,
            ExposureShadowReason::EpisodeSuppressed => {
                ExposureShadowVerifiedReason::EpisodeSuppressed
            }
        },
        risk_episode_id: record.risk_episode_id.clone(),
        validated_at_ms: record.validated_at_ms,
        raw_evidence: record
            .raw_evidence
            .iter()
            .map(|reference| VerifiedRawRiskEvidenceRef {
                sequence: reference.sequence,
                generation: reference.generation,
                payload_sha256: reference.payload_sha256.clone(),
            })
            .collect(),
        semantic_sha256: record.semantic_sha256.clone(),
    }
}

fn verify_semantic_replay(
    admission: &Stage7LiveAdmissionEvidence,
    record: &ExposureShadowEvidence,
    private: &BTreeMap<u64, PrivateEvidence>,
) -> Result<(), ExposureShadowVerificationError> {
    if record.raw_evidence.is_empty()
        || record
            .raw_evidence
            .windows(2)
            .any(|pair| pair[0].sequence >= pair[1].sequence)
        || record.raw_evidence.iter().any(|reference| {
            reference.generation != record.account.private_generation
                || private
                    .get(&reference.sequence)
                    .is_none_or(|evidence| evidence.generation != reference.generation)
        })
    {
        return Err(ExposureShadowVerificationError::PrivateReference);
    }
    let payloads = record
        .raw_evidence
        .iter()
        .map(|reference| {
            private
                .get(&reference.sequence)
                .map(|evidence| evidence.payload.clone())
                .ok_or(ExposureShadowVerificationError::PrivateReference)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let replay = replay_private_risk_payloads(
        &RiskReplayRequest {
            exchange: &admission.deployment_binding.exchange,
            account: &admission.deployment_binding.account,
            symbol: &admission.deployment_binding.symbol,
            instrument: &admission.instrument_rules.instrument,
            minimum_quantity: admission.instrument_rules.minimum_quantity,
            private_generation: record.account.private_generation,
            observed_at_ms: record.account.observed_at_ms,
            max_age_ms: record.params.max_snapshot_age_ms,
        },
        &payloads,
    )
    .map_err(|_| ExposureShadowVerificationError::SemanticReplay)?;
    if replay.account != record.account {
        return Err(ExposureShadowVerificationError::SemanticReplay);
    }
    let side = match record.position {
        GridPosition::Long => PositionSide::Long,
        GridPosition::Short => PositionSide::Short,
    };
    let mut matching = replay.legs.iter().filter(|leg| leg.position_side == side);
    let replayed_leg = matching.next();
    if matching.next().is_some() || replayed_leg != record.leg.as_ref() {
        return Err(ExposureShadowVerificationError::SemanticReplay);
    }
    Ok(())
}

fn shadow_archive_path(path: &Path) -> std::path::PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".1");
    value.into()
}

fn verify_private_references(
    path: &Path,
    records: &[ExposureShadowEvidence],
    latest: [&ExposureShadowEvidence; 2],
) -> Result<BTreeMap<u64, PrivateEvidence>, ExposureShadowVerificationError> {
    let mut expected = BTreeMap::new();
    for reference in records.iter().flat_map(|record| &record.raw_evidence) {
        match expected.insert(reference.sequence, reference) {
            Some(previous) if previous != reference => {
                return Err(ExposureShadowVerificationError::PrivateReference);
            }
            Some(_) | None => {}
        }
    }
    let maximum_sequence = expected
        .last_key_value()
        .map(|(sequence, _)| *sequence)
        .ok_or(ExposureShadowVerificationError::Incomplete)?;
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    if length == 0 {
        return Err(ExposureShadowVerificationError::PrivateReference);
    }
    file.seek(SeekFrom::End(-1))?;
    let mut tail = [0_u8; 1];
    file.read_exact(&mut tail)?;
    if tail[0] != b'\n' {
        return Err(ExposureShadowVerificationError::PrivateReference);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut line = Vec::new();
    let mut sequence = 0_u64;
    let mut matched = BTreeMap::new();
    let needed = latest
        .into_iter()
        .flat_map(|record| {
            record
                .raw_evidence
                .iter()
                .map(|reference| reference.sequence)
        })
        .collect::<std::collections::BTreeSet<_>>();
    let mut retained = BTreeMap::new();
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
            .ok_or(ExposureShadowVerificationError::PrivateReference)?;
        if let Some(reference) = expected.get(&sequence) {
            let private: PrivateEvidence = serde_json::from_slice(&line)?;
            if private.sequence != sequence
                || private.generation != reference.generation
                || private.payload_sha256 != reference.payload_sha256
                || !private.valid_hash()
            {
                return Err(ExposureShadowVerificationError::PrivateReference);
            }
            if needed.contains(&sequence) {
                retained.insert(sequence, private);
            }
            matched.insert(sequence, ());
        }
        if sequence >= maximum_sequence {
            break;
        }
    }
    if expected
        .keys()
        .any(|sequence| !matched.contains_key(sequence))
    {
        return Err(ExposureShadowVerificationError::PrivateReference);
    }
    if needed
        .iter()
        .any(|sequence| !retained.contains_key(sequence))
    {
        return Err(ExposureShadowVerificationError::PrivateReference);
    }
    Ok(retained)
}

#[derive(Debug, thiserror::Error)]
pub enum ExposureShadowVerificationError {
    #[error("exposure shadow verification requires an absolute artifacts root")]
    Root,
    #[error("exposure shadow release admission is missing or invalid")]
    Release,
    #[error("exposure shadow evidence does not match the admitted binding or parameter release")]
    Binding,
    #[error("exposure shadow evidence is incomplete")]
    Incomplete,
    #[error("exposure shadow evidence is corrupt or internally inconsistent")]
    ShadowEvidence,
    #[error("exposure shadow evidence references missing or conflicting private evidence")]
    PrivateReference,
    #[error("exposure shadow normalized risk facts do not replay from their raw private evidence")]
    SemanticReplay,
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("exposure shadow verification I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("exposure shadow verification JSON failed: {0}")]
    Json(#[from] serde_json::Error),
}
