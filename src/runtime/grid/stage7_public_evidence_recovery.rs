use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::*;
use crate::runtime::stage7_public_journal::{
    Stage7PublicBinding, Stage7PublicJournal, Stage7PublicRecord,
};

pub(super) const PUBLIC_EVIDENCE_FILE: &str = "public_market.jsonl";
pub(super) const RECOVERED_PUBLIC_EVIDENCE_FILE: &str = "public_market.recovered.jsonl";
pub(super) const PUBLIC_EVIDENCE_RECOVERY_MANIFEST_FILE: &str =
    "public_market_recovery_manifest.json";
const RECOVERY_SCHEMA_VERSION: u16 = 1;
const RECOVERY_AUTHORIZATION: &str = "quarantine-single-contiguous-sequence-fork-v1";
const SELECTION_RULE: &str = "retain-first-physical-record-for-each-canonical-sequence-and-quarantine-one-later-contiguous-fork";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7PublicEvidenceRecoveryRequest {
    pub artifacts_root: PathBuf,
    pub expected_source_sha256: String,
    pub expected_canonical_selection_sha256: String,
    pub expected_quarantine_selection_sha256: String,
    pub expected_coverage_sha256: String,
    pub expected_canonical_tail_sequence: u64,
    pub expected_collision_count: u64,
    pub confirm_public_evidence_forensic_recovery: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7PublicEvidenceRecoveryReport {
    pub exchange: String,
    pub symbol: String,
    pub source_sha256: String,
    pub source_records: u64,
    pub canonical_records: u64,
    pub collision_records: u64,
    pub canonical_selection_sha256: String,
    pub quarantine_selection_sha256: String,
    pub coverage_sha256: String,
    pub recovered_journal_sha256: String,
    pub manifest_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuarantinedCollision {
    physical_record: u64,
    byte_offset: u64,
    sequence: u64,
    first_physical_record: u64,
    first_record_sha256: String,
    generation: u64,
    received_at_ms: u64,
    payload_sha256: String,
    record_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicEvidenceRecoveryManifest {
    schema_version: u16,
    authorization: String,
    selection_rule: String,
    binding: HedgedGridBinding,
    canonical_root_sha256: String,
    source_file: String,
    source_sha256: String,
    source_bytes: u64,
    source_records: u64,
    canonical_records: u64,
    canonical_tail_sequence: u64,
    canonical_max_generation: u64,
    canonical_selection_sha256: String,
    quarantine_selection_sha256: String,
    coverage_sha256: String,
    recovered_file: String,
    recovered_prefix_sha256: String,
    recovered_prefix_bytes: u64,
    collisions: Vec<QuarantinedCollision>,
    control_sha256: String,
    checkpoint_sha256: String,
    command_journal_sha256: String,
    writer_state_sha256: String,
    created_at_ms: u64,
    manifest_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceAnalysis {
    source_sha256: String,
    source_bytes: u64,
    source_records: u64,
    canonical_records: u64,
    canonical_tail_sequence: u64,
    canonical_max_generation: u64,
    canonical_selection_sha256: String,
    quarantine_selection_sha256: String,
    coverage_sha256: String,
    recovered_prefix_sha256: String,
    recovered_prefix_bytes: u64,
    collisions: Vec<QuarantinedCollision>,
}

#[derive(Clone, Copy)]
struct FirstRecord<'a> {
    physical_record: u64,
    record_sha256: &'a str,
}

pub fn recover_stage7_public_evidence(
    cfg: &Config,
    request: Stage7PublicEvidenceRecoveryRequest,
) -> Result<Stage7PublicEvidenceRecoveryReport, Stage7GridError> {
    validate_request(&request)?;
    let binding = stage7_binding(cfg)?;
    if binding.exchange != "binance" {
        return Err(recovery_error(
            "the public-evidence recovery command is restricted to the authorized Binance binding",
        ));
    }
    let canonical_root =
        fs::canonicalize(&request.artifacts_root).map_err(|source| Stage7GridError::Io {
            path: request.artifacts_root.clone(),
            source,
        })?;
    if canonical_root != request.artifacts_root {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let writer_scope = stage7_writer_scope(&binding);
    let canonical_guard = acquire_stage7_writer_root(&writer_scope, &canonical_root)?;
    validate_stopped_root(&canonical_root, &binding, &writer_scope)?;
    let custody_anchors = root_anchor_hashes(&canonical_root)?;

    let source_path = canonical_root.join(PUBLIC_EVIDENCE_FILE);
    let recovered_path = canonical_root.join(RECOVERED_PUBLIC_EVIDENCE_FILE);
    let manifest_path = canonical_root.join(PUBLIC_EVIDENCE_RECOVERY_MANIFEST_FILE);
    let recovered_temporary = sibling(&recovered_path, ".recovering");
    let manifest_temporary = sibling(&manifest_path, ".recovering");
    if recovered_path.exists()
        || manifest_path.exists()
        || recovered_temporary.exists()
        || manifest_temporary.exists()
    {
        return Err(recovery_error(
            "recovery output or crash residue already exists; no artifact is overwritten",
        ));
    }

    let public_binding = public_binding(&binding);
    let expected_source = normalize_sha256(&request.expected_source_sha256)?;
    let expected_canonical_selection =
        normalize_sha256(&request.expected_canonical_selection_sha256)?;
    let expected_quarantine_selection =
        normalize_sha256(&request.expected_quarantine_selection_sha256)?;
    let expected_coverage = normalize_sha256(&request.expected_coverage_sha256)?;
    let first = analyze_source(&source_path, &public_binding, None)?;
    let collision_count = u64::try_from(first.collisions.len())
        .map_err(|_| recovery_error("collision count is not representable"))?;
    if first.source_sha256 != expected_source
        || first.canonical_selection_sha256 != expected_canonical_selection
        || first.quarantine_selection_sha256 != expected_quarantine_selection
        || first.coverage_sha256 != expected_coverage
        || first.canonical_tail_sequence != request.expected_canonical_tail_sequence
        || collision_count != request.expected_collision_count
        || request.expected_collision_count == 0
        || first.source_records
            != first
                .canonical_records
                .checked_add(request.expected_collision_count)
                .ok_or_else(|| recovery_error("record coverage overflow"))?
    {
        return Err(recovery_error(
            "operator anchors do not match the complete public source analysis",
        ));
    }

    let temporary_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&recovered_temporary)
        .map_err(|source| Stage7GridError::Io {
            path: recovered_temporary.clone(),
            source,
        })?;
    let mut writer = BufWriter::new(temporary_file);
    let second = analyze_source(&source_path, &public_binding, Some(&mut writer))?;
    writer.flush().map_err(|source| Stage7GridError::Io {
        path: recovered_temporary.clone(),
        source,
    })?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|source| Stage7GridError::Io {
            path: recovered_temporary.clone(),
            source,
        })?;
    drop(writer);
    if first != second {
        return Err(recovery_error(
            "public source changed between analysis and derived-journal construction",
        ));
    }
    let derived_max_generation = Stage7PublicJournal::max_generation_at_path(
        recovered_temporary.clone(),
        public_binding.clone(),
    )?;
    if derived_max_generation != first.canonical_max_generation {
        return Err(recovery_error(
            "derived public journal does not reproduce the canonical source projection",
        ));
    }
    let source_after = file_snapshot(&source_path)?;
    if source_after.sha256 != first.source_sha256 || source_after.bytes != first.source_bytes {
        return Err(recovery_error(
            "public source changed after derived-journal fsync",
        ));
    }
    if root_anchor_hashes(&canonical_root)? != custody_anchors {
        return Err(recovery_error(
            "stopped-root custody artifacts changed during public recovery",
        ));
    }
    fs::rename(&recovered_temporary, &recovered_path).map_err(|source| Stage7GridError::Io {
        path: recovered_path.clone(),
        source,
    })?;
    sync_parent(&recovered_path)?;
    let recovered_snapshot = file_snapshot(&recovered_path)?;
    if recovered_snapshot.sha256 != first.recovered_prefix_sha256
        || recovered_snapshot.bytes != first.recovered_prefix_bytes
    {
        return Err(recovery_error(
            "persisted derived public journal differs from the analyzed canonical projection",
        ));
    }
    if root_anchor_hashes(&canonical_root)? != custody_anchors {
        return Err(recovery_error(
            "stopped-root custody artifacts changed during public recovery",
        ));
    }

    let mut manifest = PublicEvidenceRecoveryManifest {
        schema_version: RECOVERY_SCHEMA_VERSION,
        authorization: RECOVERY_AUTHORIZATION.to_owned(),
        selection_rule: SELECTION_RULE.to_owned(),
        binding: binding.clone(),
        canonical_root_sha256: canonical_guard.canonical_root_sha256().to_owned(),
        source_file: PUBLIC_EVIDENCE_FILE.to_owned(),
        source_sha256: first.source_sha256.clone(),
        source_bytes: first.source_bytes,
        source_records: first.source_records,
        canonical_records: first.canonical_records,
        canonical_tail_sequence: first.canonical_tail_sequence,
        canonical_max_generation: first.canonical_max_generation,
        canonical_selection_sha256: first.canonical_selection_sha256.clone(),
        quarantine_selection_sha256: first.quarantine_selection_sha256.clone(),
        coverage_sha256: first.coverage_sha256.clone(),
        recovered_file: RECOVERED_PUBLIC_EVIDENCE_FILE.to_owned(),
        recovered_prefix_sha256: first.recovered_prefix_sha256.clone(),
        recovered_prefix_bytes: first.recovered_prefix_bytes,
        collisions: first.collisions.clone(),
        control_sha256: custody_anchors.control_sha256,
        checkpoint_sha256: custody_anchors.checkpoint_sha256,
        command_journal_sha256: custody_anchors.command_journal_sha256,
        writer_state_sha256: custody_anchors.writer_state_sha256,
        created_at_ms: wall_clock_ms()?,
        manifest_sha256: String::new(),
    };
    manifest.manifest_sha256 = manifest.expected_sha256()?;
    manifest.validate_static()?;
    persist_manifest(&manifest_path, &manifest_temporary, &manifest)?;

    Ok(Stage7PublicEvidenceRecoveryReport {
        exchange: binding.exchange,
        symbol: binding.symbol.to_string(),
        source_sha256: first.source_sha256,
        source_records: first.source_records,
        canonical_records: first.canonical_records,
        collision_records: request.expected_collision_count,
        canonical_selection_sha256: first.canonical_selection_sha256,
        quarantine_selection_sha256: first.quarantine_selection_sha256,
        coverage_sha256: first.coverage_sha256,
        recovered_journal_sha256: recovered_snapshot.sha256,
        manifest_sha256: manifest.manifest_sha256,
    })
}

pub(in crate::runtime) fn stage7_public_evidence_path(
    artifacts_root: &Path,
    binding: &HedgedGridBinding,
) -> Result<PathBuf, Stage7GridError> {
    if !artifacts_root.is_absolute() || !artifacts_root.is_dir() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let recovered_path = artifacts_root.join(RECOVERED_PUBLIC_EVIDENCE_FILE);
    let manifest_path = artifacts_root.join(PUBLIC_EVIDENCE_RECOVERY_MANIFEST_FILE);
    if sibling(&recovered_path, ".recovering").exists()
        || sibling(&manifest_path, ".recovering").exists()
    {
        return Err(recovery_error(
            "incomplete public recovery residue fences every Stage-7 entry point",
        ));
    }
    if !manifest_path.exists() {
        if recovered_path.exists() {
            return Err(recovery_error(
                "derived public evidence exists without its immutable manifest",
            ));
        }
        return Ok(artifacts_root.join(PUBLIC_EVIDENCE_FILE));
    }
    if !recovered_path.is_file() {
        return Err(recovery_error(
            "public recovery manifest exists without the derived journal",
        ));
    }
    let manifest = load_manifest(&manifest_path)?;
    let canonical_root =
        fs::canonicalize(artifacts_root).map_err(|source| Stage7GridError::Io {
            path: artifacts_root.to_path_buf(),
            source,
        })?;
    if canonical_root != artifacts_root
        || manifest.binding != *binding
        || manifest.canonical_root_sha256 != canonical_root_sha256(&canonical_root)?
    {
        return Err(recovery_error(
            "public recovery manifest is bound to a different root or deployment binding",
        ));
    }
    let analysis = analyze_source(
        &artifacts_root.join(PUBLIC_EVIDENCE_FILE),
        &public_binding(binding),
        None,
    )?;
    manifest.require_analysis(&analysis)?;
    verify_prefix(
        &recovered_path,
        manifest.recovered_prefix_bytes,
        &manifest.recovered_prefix_sha256,
    )?;
    Ok(recovered_path)
}

fn public_binding(binding: &HedgedGridBinding) -> Stage7PublicBinding {
    Stage7PublicBinding {
        exchange: binding.exchange.clone(),
        symbol: binding.symbol.clone(),
    }
}

fn validate_request(request: &Stage7PublicEvidenceRecoveryRequest) -> Result<(), Stage7GridError> {
    if !request.confirm_public_evidence_forensic_recovery {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute()
        || !request.artifacts_root.is_dir()
        || request.expected_canonical_tail_sequence == 0
        || request.expected_collision_count == 0
    {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let _ = normalize_sha256(&request.expected_source_sha256)?;
    let _ = normalize_sha256(&request.expected_canonical_selection_sha256)?;
    let _ = normalize_sha256(&request.expected_quarantine_selection_sha256)?;
    let _ = normalize_sha256(&request.expected_coverage_sha256)?;
    Ok(())
}

fn validate_stopped_root(
    root: &Path,
    binding: &HedgedGridBinding,
    writer_scope: &WriterScope,
) -> Result<(), Stage7GridError> {
    let control = ProjectionStore::new(root.join(CONTROL_FILE))
        .load::<Stage7GridControl>()?
        .ok_or_else(|| recovery_error("stopped control evidence is missing"))?;
    let checkpoint = ProjectionStore::new(root.join(CHECKPOINT_FILE))
        .load::<Stage7GridCheckpoint>()?
        .ok_or_else(|| recovery_error("stopped checkpoint evidence is missing"))?;
    if control.schema_version != 1
        || control.binding != *binding
        || control.target != HedgedGridControlTarget::Stop
        || checkpoint.schema_version != 1
        || checkpoint.binding != *binding
        || checkpoint.state.binding != *binding
        || checkpoint.state.phase != GridPhase::Stopping
        || !checkpoint.state.owned_orders.is_empty()
        || !checkpoint.state.pending_transactions.is_empty()
        || !checkpoint.state.pending_replenishments.is_empty()
        || checkpoint.private_generation == 0
    {
        return Err(recovery_error(
            "root is not a clean, binding-exact stopped custody state",
        ));
    }
    let commands = CommandJournal::open(root.join(COMMAND_FILE))?;
    if commands.has_unresolved() {
        return Err(recovery_error(
            "command WAL contains unresolved mutation state",
        ));
    }
    let authority = WriterLeaseAuthority::open(root.join(WRITER_FILE), writer_scope.clone())?;
    let writer = authority
        .active_entry_session()?
        .ok_or_else(|| recovery_error("durable predecessor writer identity is missing"))?;
    if writer.scope != *writer_scope || writer.generation == 0 || writer.readback_generation == 0 {
        return Err(recovery_error(
            "durable predecessor writer identity is inconsistent",
        ));
    }
    Ok(())
}

fn analyze_source(
    path: &Path,
    binding: &Stage7PublicBinding,
    mut derived: Option<&mut dyn Write>,
) -> Result<SourceAnalysis, Stage7GridError> {
    let file = File::open(path).map_err(|source| Stage7GridError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let mut source_digest = Sha256::new();
    let mut canonical_digest = Sha256::new();
    let mut quarantine_digest = Sha256::new();
    let mut coverage_digest = Sha256::new();
    let mut recovered_digest = Sha256::new();
    let mut first_records: BTreeMap<u64, (u64, String)> = BTreeMap::new();
    let mut line = Vec::new();
    let mut source_bytes = 0_u64;
    let mut source_records = 0_u64;
    let mut canonical_records = 0_u64;
    let mut canonical_max_generation = 0_u64;
    let mut recovered_prefix_bytes = 0_u64;
    let mut collisions = Vec::new();
    let mut collision_started = false;
    let mut collision_ended = false;
    let mut expected_collision_sequence = 0_u64;

    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|source| Stage7GridError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        let byte_offset = source_bytes;
        source_bytes = source_bytes
            .checked_add(read as u64)
            .ok_or_else(|| recovery_error("public source byte count overflow"))?;
        source_digest.update(&line);
        if line == b"\n" {
            continue;
        }
        if !line.ends_with(b"\n") {
            return Err(recovery_error("public source has a truncated tail"));
        }
        source_records = source_records
            .checked_add(1)
            .ok_or_else(|| recovery_error("public source record count overflow"))?;
        let record_sha256 = digest_bytes(&line);
        let record: Stage7PublicRecord = serde_json::from_slice(&line)
            .map_err(|_| recovery_error("public source JSON is invalid"))?;
        validate_public_record(&record, binding)?;
        let next_sequence = canonical_records
            .checked_add(1)
            .ok_or_else(|| recovery_error("canonical public sequence overflow"))?;
        if record.capture_sequence == next_sequence {
            if collision_started {
                collision_ended = true;
            }
            if first_records
                .insert(
                    record.capture_sequence,
                    (source_records, record_sha256.clone()),
                )
                .is_some()
            {
                return Err(recovery_error(
                    "canonical public sequence was selected twice",
                ));
            }
            commit_selection(
                &mut canonical_digest,
                b'c',
                source_records,
                byte_offset,
                read as u64,
                record.capture_sequence,
                &record_sha256,
            );
            commit_selection(
                &mut coverage_digest,
                b'c',
                source_records,
                byte_offset,
                read as u64,
                record.capture_sequence,
                &record_sha256,
            );
            if let Some(output) = derived.as_deref_mut() {
                output
                    .write_all(&line)
                    .map_err(|source| Stage7GridError::Io {
                        path: path.to_path_buf(),
                        source,
                    })?;
            }
            recovered_digest.update(&line);
            recovered_prefix_bytes = recovered_prefix_bytes
                .checked_add(read as u64)
                .ok_or_else(|| recovery_error("recovered public byte count overflow"))?;
            canonical_records = next_sequence;
            canonical_max_generation = canonical_max_generation.max(record.generation);
            continue;
        }
        if record.capture_sequence > next_sequence || collision_ended {
            return Err(recovery_error(
                "public source contains a gap, reordered canonical row, or multiple collision blocks",
            ));
        }
        let Some((first_physical_record, first_record_sha256)) =
            first_records.get(&record.capture_sequence)
        else {
            return Err(recovery_error(
                "public collision does not refer to an already selected sequence",
            ));
        };
        if !collision_started {
            collision_started = true;
            expected_collision_sequence = record.capture_sequence;
        }
        if record.capture_sequence != expected_collision_sequence {
            return Err(recovery_error(
                "public collision rows are not one sequence-contiguous block",
            ));
        }
        expected_collision_sequence = expected_collision_sequence
            .checked_add(1)
            .ok_or_else(|| recovery_error("public collision sequence overflow"))?;
        let first = FirstRecord {
            physical_record: *first_physical_record,
            record_sha256: first_record_sha256,
        };
        commit_selection(
            &mut quarantine_digest,
            b'q',
            source_records,
            byte_offset,
            read as u64,
            record.capture_sequence,
            &record_sha256,
        );
        commit_selection(
            &mut coverage_digest,
            b'q',
            source_records,
            byte_offset,
            read as u64,
            record.capture_sequence,
            &record_sha256,
        );
        collisions.push(QuarantinedCollision {
            physical_record: source_records,
            byte_offset,
            sequence: record.capture_sequence,
            first_physical_record: first.physical_record,
            first_record_sha256: first.record_sha256.to_owned(),
            generation: record.generation,
            received_at_ms: record.received_at_ms,
            payload_sha256: record.payload_sha256,
            record_sha256,
        });
    }
    if source_records == 0 || canonical_records == 0 || collisions.is_empty() {
        return Err(recovery_error(
            "public source does not contain the authorized non-empty collision shape",
        ));
    }
    Ok(SourceAnalysis {
        source_sha256: finalize_digest(source_digest),
        source_bytes,
        source_records,
        canonical_records,
        canonical_tail_sequence: canonical_records,
        canonical_max_generation,
        canonical_selection_sha256: finalize_digest(canonical_digest),
        quarantine_selection_sha256: finalize_digest(quarantine_digest),
        coverage_sha256: finalize_digest(coverage_digest),
        recovered_prefix_sha256: finalize_digest(recovered_digest),
        recovered_prefix_bytes,
        collisions,
    })
}

fn validate_public_record(
    record: &Stage7PublicRecord,
    binding: &Stage7PublicBinding,
) -> Result<(), Stage7GridError> {
    if record.schema_version != 1
        || record.capture_sequence == 0
        || record.exchange != binding.exchange
        || record.symbol != binding.symbol
        || record.generation == 0
        || record.received_at_ms == 0
        || record.payload.is_empty()
        || record.payload_sha256 != digest_bytes(record.payload.as_bytes())
    {
        return Err(recovery_error(
            "public source record binding, payload, or payload hash is invalid",
        ));
    }
    Ok(())
}

fn commit_selection(
    digest: &mut Sha256,
    class: u8,
    physical_record: u64,
    byte_offset: u64,
    byte_length: u64,
    sequence: u64,
    record_sha256: &str,
) {
    digest.update([class]);
    digest.update(physical_record.to_be_bytes());
    digest.update(byte_offset.to_be_bytes());
    digest.update(byte_length.to_be_bytes());
    digest.update(sequence.to_be_bytes());
    digest.update((record_sha256.len() as u64).to_be_bytes());
    digest.update(record_sha256.as_bytes());
}

impl PublicEvidenceRecoveryManifest {
    fn expected_sha256(&self) -> Result<String, Stage7GridError> {
        let mut unhashed = self.clone();
        unhashed.manifest_sha256.clear();
        serde_json::to_vec(&unhashed)
            .map(|encoded| digest_bytes(&encoded))
            .map_err(|_| recovery_error("public recovery manifest cannot be encoded"))
    }

    fn validate_static(&self) -> Result<(), Stage7GridError> {
        let collision_count = u64::try_from(self.collisions.len())
            .map_err(|_| recovery_error("collision count is not representable"))?;
        if self.schema_version != RECOVERY_SCHEMA_VERSION
            || self.authorization != RECOVERY_AUTHORIZATION
            || self.selection_rule != SELECTION_RULE
            || self.binding.exchange != "binance"
            || self.source_file != PUBLIC_EVIDENCE_FILE
            || self.recovered_file != RECOVERED_PUBLIC_EVIDENCE_FILE
            || self.source_records
                != self
                    .canonical_records
                    .checked_add(collision_count)
                    .ok_or_else(|| recovery_error("manifest record coverage overflow"))?
            || self.canonical_records == 0
            || self.canonical_tail_sequence != self.canonical_records
            || self.canonical_max_generation == 0
            || self.recovered_prefix_bytes == 0
            || self.created_at_ms == 0
            || self.collisions.is_empty()
            || [
                &self.canonical_root_sha256,
                &self.source_sha256,
                &self.canonical_selection_sha256,
                &self.quarantine_selection_sha256,
                &self.coverage_sha256,
                &self.recovered_prefix_sha256,
                &self.control_sha256,
                &self.checkpoint_sha256,
                &self.command_journal_sha256,
                &self.writer_state_sha256,
                &self.manifest_sha256,
            ]
            .into_iter()
            .any(|value| !valid_sha256(value))
            || self.collisions.iter().any(|collision| {
                collision.physical_record == 0
                    || collision.sequence == 0
                    || collision.first_physical_record == 0
                    || collision.first_physical_record >= collision.physical_record
                    || collision.generation == 0
                    || collision.received_at_ms == 0
                    || !valid_sha256(&collision.first_record_sha256)
                    || !valid_sha256(&collision.payload_sha256)
                    || !valid_sha256(&collision.record_sha256)
            })
            || self.manifest_sha256 != self.expected_sha256()?
        {
            return Err(recovery_error("public recovery manifest is invalid"));
        }
        Ok(())
    }

    fn require_analysis(&self, analysis: &SourceAnalysis) -> Result<(), Stage7GridError> {
        self.validate_static()?;
        if self.source_sha256 != analysis.source_sha256
            || self.source_bytes != analysis.source_bytes
            || self.source_records != analysis.source_records
            || self.canonical_records != analysis.canonical_records
            || self.canonical_tail_sequence != analysis.canonical_tail_sequence
            || self.canonical_max_generation != analysis.canonical_max_generation
            || self.canonical_selection_sha256 != analysis.canonical_selection_sha256
            || self.quarantine_selection_sha256 != analysis.quarantine_selection_sha256
            || self.coverage_sha256 != analysis.coverage_sha256
            || self.recovered_prefix_sha256 != analysis.recovered_prefix_sha256
            || self.recovered_prefix_bytes != analysis.recovered_prefix_bytes
            || self.collisions != analysis.collisions
        {
            return Err(recovery_error(
                "public source no longer reproduces the immutable recovery manifest",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RootAnchorHashes {
    control_sha256: String,
    checkpoint_sha256: String,
    command_journal_sha256: String,
    writer_state_sha256: String,
}

fn root_anchor_hashes(root: &Path) -> Result<RootAnchorHashes, Stage7GridError> {
    Ok(RootAnchorHashes {
        control_sha256: file_snapshot(&root.join(CONTROL_FILE))?.sha256,
        checkpoint_sha256: file_snapshot(&root.join(CHECKPOINT_FILE))?.sha256,
        command_journal_sha256: file_snapshot(&root.join(COMMAND_FILE))?.sha256,
        writer_state_sha256: file_snapshot(&root.join(WRITER_FILE))?.sha256,
    })
}

struct FileSnapshot {
    sha256: String,
    bytes: u64,
}

fn file_snapshot(path: &Path) -> Result<FileSnapshot, Stage7GridError> {
    let file = File::open(path).map_err(|source| Stage7GridError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| Stage7GridError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| recovery_error("file byte count overflow"))?;
    }
    Ok(FileSnapshot {
        sha256: finalize_digest(digest),
        bytes,
    })
}

fn verify_prefix(path: &Path, prefix_bytes: u64, expected: &str) -> Result<(), Stage7GridError> {
    let metadata = fs::metadata(path).map_err(|source| Stage7GridError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.len() < prefix_bytes {
        return Err(recovery_error(
            "derived public journal is shorter than its immutable prefix",
        ));
    }
    let file = File::open(path).map_err(|source| Stage7GridError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file).take(prefix_bytes);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut read_total = 0_u64;
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|source| Stage7GridError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        read_total = read_total
            .checked_add(read as u64)
            .ok_or_else(|| recovery_error("prefix byte count overflow"))?;
    }
    if read_total != prefix_bytes || finalize_digest(digest) != expected {
        return Err(recovery_error(
            "derived public journal immutable prefix is changed",
        ));
    }
    Ok(())
}

fn load_manifest(path: &Path) -> Result<PublicEvidenceRecoveryManifest, Stage7GridError> {
    let bytes = fs::read(path).map_err(|source| Stage7GridError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let manifest = serde_json::from_slice::<PublicEvidenceRecoveryManifest>(&bytes)
        .map_err(|_| recovery_error("public recovery manifest cannot be decoded"))?;
    manifest.validate_static()?;
    Ok(manifest)
}

fn persist_manifest(
    path: &Path,
    temporary: &Path,
    manifest: &PublicEvidenceRecoveryManifest,
) -> Result<(), Stage7GridError> {
    let encoded = serde_json::to_vec(manifest)
        .map_err(|_| recovery_error("public recovery manifest cannot be encoded"))?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temporary)
        .map_err(|source| Stage7GridError::Io {
            path: temporary.to_path_buf(),
            source,
        })?;
    file.write_all(&encoded)
        .and_then(|()| file.sync_all())
        .map_err(|source| Stage7GridError::Io {
            path: temporary.to_path_buf(),
            source,
        })?;
    fs::rename(temporary, path).map_err(|source| Stage7GridError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    sync_parent(path)
}

fn canonical_root_sha256(root: &Path) -> Result<String, Stage7GridError> {
    let encoded = root
        .to_str()
        .ok_or_else(|| recovery_error("canonical root path cannot be encoded"))?;
    Ok(digest_bytes(encoded.as_bytes()))
}

fn normalize_sha256(value: &str) -> Result<String, Stage7GridError> {
    if !valid_sha256(value) {
        return Err(recovery_error("operator SHA-256 anchor is invalid"));
    }
    Ok(value.to_ascii_lowercase())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn digest_bytes(bytes: &[u8]) -> String {
    finalize_digest(Sha256::new_with_prefix(bytes))
}

fn finalize_digest(digest: Sha256) -> String {
    let bytes = digest.finalize();
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut sibling = path.as_os_str().to_os_string();
    sibling.push(suffix);
    PathBuf::from(sibling)
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), Stage7GridError> {
    let parent = path
        .parent()
        .ok_or_else(|| recovery_error("public recovery output has no parent directory"))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| Stage7GridError::Io {
            path: parent.to_path_buf(),
            source,
        })
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<(), Stage7GridError> {
    Ok(())
}

fn recovery_error(reason: impl Into<String>) -> Stage7GridError {
    Stage7GridError::PublicEvidenceRecovery {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{domain::Symbol, runtime::stage7_public_journal::Stage7PublicSource};

    fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
        Ok(HedgedGridBinding {
            owner_scope: "hedged_grid_sol_usdc_primary".to_owned(),
            strategy_instance_id: "hedged_grid_sol_usdc".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDC".parse::<Symbol>()?,
            config_version: "shared-grid-v1".to_owned(),
        })
    }

    fn record(
        sequence: u64,
        generation: u64,
        received_at_ms: u64,
        payload: &str,
    ) -> Result<Stage7PublicRecord, Box<dyn std::error::Error>> {
        let binding = binding()?;
        Ok(Stage7PublicRecord {
            schema_version: 1,
            capture_sequence: sequence,
            exchange: binding.exchange,
            symbol: binding.symbol,
            generation,
            source: Stage7PublicSource::WebSocketDepth,
            received_at_ms,
            payload_sha256: digest_bytes(payload.as_bytes()),
            payload: payload.to_owned(),
        })
    }

    fn write_source(
        path: &Path,
        records: &[Stage7PublicRecord],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut file = File::create(path)?;
        for record in records {
            serde_json::to_writer(&mut file, record)?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        Ok(())
    }

    #[test]
    fn one_contiguous_duplicate_block_is_quarantined_without_renumbering()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join(PUBLIC_EVIDENCE_FILE);
        let derived = temporary.path().join(RECOVERED_PUBLIC_EVIDENCE_FILE);
        write_source(
            &source,
            &[
                record(1, 10, 100, r#"{"a":1}"#)?,
                record(2, 11, 101, r#"{"b":2}"#)?,
                record(1, 1, 500, r#"{"fork":1}"#)?,
                record(2, 2, 501, r#"{"fork":2}"#)?,
                record(3, 3, 502, r#"{"c":3}"#)?,
            ],
        )?;
        let file = File::create(&derived)?;
        let mut writer = BufWriter::new(file);
        let public_binding = public_binding(&binding()?);
        let analysis = analyze_source(&source, &public_binding, Some(&mut writer))?;
        writer.flush()?;

        assert_eq!(analysis.source_records, 5);
        assert_eq!(analysis.canonical_records, 3);
        assert_eq!(analysis.collisions.len(), 2);
        assert_eq!(analysis.collisions[0].physical_record, 3);
        assert_eq!(analysis.collisions[0].first_physical_record, 1);
        assert_eq!(
            Stage7PublicJournal::max_generation_at_path(derived, public_binding)?,
            11
        );
        Ok(())
    }

    #[test]
    fn gaps_and_a_second_duplicate_block_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let public_binding = public_binding(&binding()?);
        let gap = temporary.path().join("gap.jsonl");
        write_source(
            &gap,
            &[
                record(1, 1, 1, r#"{"a":1}"#)?,
                record(3, 2, 2, r#"{"c":3}"#)?,
            ],
        )?;
        assert!(matches!(
            analyze_source(&gap, &public_binding, None),
            Err(Stage7GridError::PublicEvidenceRecovery { .. })
        ));

        let second = temporary.path().join("second.jsonl");
        write_source(
            &second,
            &[
                record(1, 1, 1, r#"{"a":1}"#)?,
                record(1, 2, 9, r#"{"fork":1}"#)?,
                record(2, 3, 10, r#"{"b":2}"#)?,
                record(1, 4, 11, r#"{"again":1}"#)?,
            ],
        )?;
        assert!(matches!(
            analyze_source(&second, &public_binding, None),
            Err(Stage7GridError::PublicEvidenceRecovery { .. })
        ));
        Ok(())
    }

    #[test]
    fn normal_root_returns_original_path_without_writing() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let root = fs::canonicalize(temporary.path())?;
        let original = root.join(PUBLIC_EVIDENCE_FILE);
        write_source(&original, &[record(1, 1, 1, r#"{"a":1}"#)?])?;

        assert_eq!(stage7_public_evidence_path(&root, &binding()?)?, original);
        assert!(!root.join(RECOVERED_PUBLIC_EVIDENCE_FILE).exists());
        assert!(!root.join(PUBLIC_EVIDENCE_RECOVERY_MANIFEST_FILE).exists());
        Ok(())
    }

    #[test]
    fn resolver_rejects_source_and_derived_prefix_tampering()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = fs::canonicalize(temporary.path())?;
        let source = root.join(PUBLIC_EVIDENCE_FILE);
        let recovered = root.join(RECOVERED_PUBLIC_EVIDENCE_FILE);
        let deployment_binding = binding()?;
        let public_binding = public_binding(&deployment_binding);
        write_source(
            &source,
            &[
                record(1, 1, 1, r#"{"a":1}"#)?,
                record(1, 2, 9, r#"{"fork":1}"#)?,
                record(2, 3, 10, r#"{"b":2}"#)?,
            ],
        )?;
        let file = File::create(&recovered)?;
        let mut writer = BufWriter::new(file);
        let analysis = analyze_source(&source, &public_binding, Some(&mut writer))?;
        writer.flush()?;
        let mut manifest = PublicEvidenceRecoveryManifest {
            schema_version: RECOVERY_SCHEMA_VERSION,
            authorization: RECOVERY_AUTHORIZATION.to_owned(),
            selection_rule: SELECTION_RULE.to_owned(),
            binding: deployment_binding.clone(),
            canonical_root_sha256: canonical_root_sha256(&root)?,
            source_file: PUBLIC_EVIDENCE_FILE.to_owned(),
            source_sha256: analysis.source_sha256.clone(),
            source_bytes: analysis.source_bytes,
            source_records: analysis.source_records,
            canonical_records: analysis.canonical_records,
            canonical_tail_sequence: analysis.canonical_tail_sequence,
            canonical_max_generation: analysis.canonical_max_generation,
            canonical_selection_sha256: analysis.canonical_selection_sha256.clone(),
            quarantine_selection_sha256: analysis.quarantine_selection_sha256.clone(),
            coverage_sha256: analysis.coverage_sha256.clone(),
            recovered_file: RECOVERED_PUBLIC_EVIDENCE_FILE.to_owned(),
            recovered_prefix_sha256: analysis.recovered_prefix_sha256.clone(),
            recovered_prefix_bytes: analysis.recovered_prefix_bytes,
            collisions: analysis.collisions.clone(),
            control_sha256: digest_bytes(b"control"),
            checkpoint_sha256: digest_bytes(b"checkpoint"),
            command_journal_sha256: digest_bytes(b"wal"),
            writer_state_sha256: digest_bytes(b"writer"),
            created_at_ms: 1,
            manifest_sha256: String::new(),
        };
        manifest.manifest_sha256 = manifest.expected_sha256()?;
        fs::write(
            root.join(PUBLIC_EVIDENCE_RECOVERY_MANIFEST_FILE),
            serde_json::to_vec(&manifest)?,
        )?;
        assert_eq!(
            stage7_public_evidence_path(&root, &deployment_binding)?,
            recovered
        );

        OpenOptions::new()
            .append(true)
            .open(&source)?
            .write_all(b"x")?;
        assert!(matches!(
            stage7_public_evidence_path(&root, &deployment_binding),
            Err(Stage7GridError::PublicEvidenceRecovery { .. })
        ));

        let mut source_bytes = fs::read(&source)?;
        assert_eq!(source_bytes.pop(), Some(b'x'));
        fs::write(&source, source_bytes)?;
        let mut recovered_bytes = fs::read(&recovered)?;
        recovered_bytes[0] ^= 1;
        fs::write(&recovered, recovered_bytes)?;
        assert!(matches!(
            stage7_public_evidence_path(&root, &deployment_binding),
            Err(Stage7GridError::PublicEvidenceRecovery { .. })
        ));
        Ok(())
    }
}
