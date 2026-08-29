use std::{fs, path::PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    indicator::FeatureFrame,
    runtime::{
        AppliedRiskReceipt, BoundRiskRevaluation, ScalpingCoreQuoteReceiptError,
        ScalpingCoreQuoteReceiptSource, ScalpingEntryEvidenceError, ScalpingEvidenceSource,
        ScalpingEvidenceSourceError, ScalpingMarketEvidenceAssembler,
        ScalpingMarketEvidenceAssemblerError, ScalpingMarketEvidenceFence, ScalpingResidentMarket,
        project_scalping_entry_evidence,
    },
    storage::{
        ProjectionStore, ScalpingEvidenceError, ScalpingEvidenceJournal, ScalpingRiskBinding,
        StorageError,
    },
    strategy::scalping::{
        CalibrationBook, CandidateEvidence, CandidatePreparation, EvidenceIdentity,
        RiskRevaluation, ScalpingError, ScalpingParams, SemanticIntent, StrategyBinding,
        risk_revaluation_digest,
    },
};

pub const SCALPING_CANDIDATE_EVIDENCE_SCHEMA_VERSION: u16 = 2;

/// Explicit artifact locations. The coordinator has no default or network-backed source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScalpingCandidateEvidenceConfig {
    pub calibration_artifact_path: PathBuf,
    pub core_quote_receipt_path: PathBuf,
    pub evidence_journal_path: PathBuf,
    pub checkpoint_path: PathBuf,
    pub live_calibration: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingCandidateEvidenceFrameCursor {
    pub generation: u64,
    pub watermark_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScalpingCandidateAppliedRiskCheckpoint {
    pub binding: ScalpingRiskBinding,
    pub proof: RiskRevaluation,
    pub cursor_sequence: u64,
    pub receipt: AppliedRiskReceipt,
}

impl ScalpingCandidateAppliedRiskCheckpoint {
    fn from_bound(applied: &BoundRiskRevaluation, receipt: &AppliedRiskReceipt) -> Self {
        Self {
            binding: applied.binding.clone(),
            proof: applied.proof.clone(),
            cursor_sequence: applied.cursor_sequence,
            receipt: receipt.clone(),
        }
    }

    fn bound(&self) -> BoundRiskRevaluation {
        BoundRiskRevaluation {
            binding: self.binding.clone(),
            proof: self.proof.clone(),
            cursor_sequence: self.cursor_sequence,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScalpingCandidateEvidenceCheckpoint {
    pub schema_version: u16,
    pub binding: StrategyBinding,
    pub pending_preparation: Option<CandidatePreparation>,
    pub applied_risk: Option<ScalpingCandidateAppliedRiskCheckpoint>,
    pub last_frame: Option<ScalpingCandidateEvidenceFrameCursor>,
    #[serde(default)]
    pub last_assembled_evidence: Option<Vec<CandidateEvidence>>,
    pub last_evidence_sequence: Option<u64>,
    pub fenced: bool,
    pub state_digest: String,
}

#[derive(Debug)]
pub struct ScalpingCandidateEvidenceCoordinator {
    binding: StrategyBinding,
    params: ScalpingParams,
    calibration: CalibrationBook,
    quote_source: ScalpingCoreQuoteReceiptSource,
    quote_path: PathBuf,
    evidence_path: PathBuf,
    evidence_journal: ScalpingEvidenceJournal,
    store: ProjectionStore,
    assembler: ScalpingMarketEvidenceAssembler,
    checkpoint: ScalpingCandidateEvidenceCheckpoint,
    live_calibration: bool,
}

impl ScalpingCandidateEvidenceCoordinator {
    pub fn open(
        config: ScalpingCandidateEvidenceConfig,
        binding: StrategyBinding,
        params: ScalpingParams,
    ) -> Result<Self, ScalpingCandidateEvidenceError> {
        binding
            .validate()
            .map_err(ScalpingCandidateEvidenceError::Strategy)?;
        params
            .validate_for(&binding)
            .map_err(ScalpingCandidateEvidenceError::Strategy)?;
        validate_paths(&config)?;

        let calibration_bytes = fs::read(&config.calibration_artifact_path).map_err(|source| {
            ScalpingCandidateEvidenceError::Io {
                path: config.calibration_artifact_path.clone(),
                source,
            }
        })?;
        let calibration = CalibrationBook::from_json(&calibration_bytes, &binding, &params)
            .map_err(ScalpingCandidateEvidenceError::Strategy)?;
        let quote_source =
            ScalpingCoreQuoteReceiptSource::open(&config.core_quote_receipt_path, binding.clone())?;
        let evidence_journal = ScalpingEvidenceJournal::open(&config.evidence_journal_path)?;
        let evidence_source = ScalpingEvidenceSource::from_journal(&evidence_journal)?;
        let store = ProjectionStore::new(&config.checkpoint_path);
        let recovered = store.load::<ScalpingCandidateEvidenceCheckpoint>()?;
        let is_new = recovered.is_none();
        let mut checkpoint = recovered.unwrap_or_else(|| empty_checkpoint(binding.clone()));
        if is_new {
            checkpoint.state_digest = checkpoint_digest(&checkpoint)?;
        }
        validate_checkpoint(&checkpoint, &binding, &params)?;

        let mut assembler = ScalpingMarketEvidenceAssembler::new(evidence_source);
        if let Some(frame) = &checkpoint.last_frame {
            assembler.restore_last_frame(frame.generation, frame.watermark_ms)?;
        }
        if let Some(applied) = &checkpoint.applied_risk {
            assembler.record_applied_risk(applied.bound())?;
        }
        if let Some(preparation) = &checkpoint.pending_preparation {
            assembler.record_preparation(Some(preparation.clone()))?;
        }

        let mut coordinator = Self {
            binding,
            params,
            calibration,
            quote_source,
            quote_path: config.core_quote_receipt_path,
            evidence_path: config.evidence_journal_path,
            evidence_journal,
            store,
            assembler,
            checkpoint,
            live_calibration: config.live_calibration,
        };
        if is_new {
            coordinator.persist_checkpoint()?;
        }
        Ok(coordinator)
    }

    #[must_use]
    pub fn checkpoint(&self) -> &ScalpingCandidateEvidenceCheckpoint {
        &self.checkpoint
    }

    #[must_use]
    pub fn is_fenced(&self) -> bool {
        self.checkpoint.fenced
    }

    /// Reopens the externally appended Core receipt source. It never creates or derives a quote.
    pub fn refresh_core_quote_source(&mut self) -> Result<(), ScalpingCandidateEvidenceError> {
        self.ensure_live()?;
        let source =
            match ScalpingCoreQuoteReceiptSource::open(&self.quote_path, self.binding.clone()) {
                Ok(source) => source,
                Err(error) => return self.fail_closed(error.into()),
            };
        self.quote_source = source;
        Ok(())
    }

    /// Reopens an externally repaired evidence journal. Transient I/O is retryable; identity,
    /// ambiguity, hash, and sequence failures fence the coordinator.
    pub fn refresh_evidence_source(&mut self) -> Result<(), ScalpingCandidateEvidenceError> {
        self.ensure_live()?;
        let journal = match ScalpingEvidenceJournal::open(&self.evidence_path) {
            Ok(journal) => journal,
            Err(error) => {
                let error = ScalpingCandidateEvidenceError::Journal(error);
                if error.is_retryable() {
                    return Err(error);
                }
                return self.fail_closed(error);
            }
        };
        let source = match ScalpingEvidenceSource::from_journal(&journal) {
            Ok(source) => source,
            Err(error) => {
                let error = ScalpingCandidateEvidenceError::Source(error);
                if error.is_retryable() {
                    return Err(error);
                }
                return self.fail_closed(error);
            }
        };
        let sequence = journal.recover()?.last().map(|record| record.sequence);
        if self
            .checkpoint
            .last_evidence_sequence
            .is_some_and(|previous| sequence.is_none_or(|current| current < previous))
        {
            return self.fail_closed(ScalpingCandidateEvidenceError::Checkpoint);
        }
        self.evidence_journal = journal;
        self.assembler.replace_evidence_source(source);
        if sequence.is_some() {
            self.checkpoint.last_evidence_sequence = sequence;
        }
        self.persist_checkpoint()
    }

    /// Records a preparation only after the resident host has durably reported it for frame N.
    pub fn record_preparation(
        &mut self,
        preparation: Option<CandidatePreparation>,
    ) -> Result<(), ScalpingCandidateEvidenceError> {
        self.ensure_live()?;
        if let Some(preparation) = &preparation {
            if preparation.binding_digest != self.binding.digest() {
                return self.fail_closed(ScalpingCandidateEvidenceError::Identity);
            }
        }
        if let Err(error) = self.assembler.record_preparation(preparation.clone()) {
            return self.fail_closed(error.into());
        }
        self.checkpoint.pending_preparation = preparation;
        self.persist_checkpoint()
    }

    /// Reconstructs the N-frame cursor only from a host checkpoint that already durably holds the
    /// exact preparation. This closes the crash window between host persistence and the normal
    /// post-host `record_preparation` call; it never accepts an unbound or regressing cursor.
    pub fn recover_host_preparation(
        &mut self,
        preparation: Option<CandidatePreparation>,
    ) -> Result<(), ScalpingCandidateEvidenceError> {
        let Some(preparation) = preparation else {
            return self.record_preparation(None);
        };
        self.ensure_live()?;
        if preparation.binding_digest != self.binding.digest()
            || preparation.frame_generation == 0
            || preparation.watermark_ms == 0
        {
            return self.fail_closed(ScalpingCandidateEvidenceError::Identity);
        }
        let cursor = ScalpingCandidateEvidenceFrameCursor {
            generation: preparation.frame_generation,
            watermark_ms: preparation.watermark_ms,
        };
        if let Some(previous) = &self.checkpoint.last_frame {
            if cursor.generation < previous.generation
                || (cursor.generation == previous.generation
                    && cursor.watermark_ms < previous.watermark_ms)
            {
                return self.fail_closed(ScalpingCandidateEvidenceError::Checkpoint);
            }
            if previous == &cursor {
                return self.record_preparation(Some(preparation));
            }
        }
        if let Err(error) = self
            .assembler
            .restore_last_frame(cursor.generation, cursor.watermark_ms)
        {
            return self.fail_closed(error.into());
        }
        self.checkpoint.last_frame = Some(cursor);
        self.checkpoint.last_assembled_evidence = None;
        self.record_preparation(Some(preparation))
    }

    /// Replays a coordinator-committed market exactly once into the host delivery phase. It never
    /// assembles again, so a crash after coordinator persistence cannot regress its frame cursor.
    pub fn recover_assembled_market(
        &self,
        frame: FeatureFrame,
        decision_at_ms: u64,
    ) -> Result<ScalpingResidentMarket, ScalpingCandidateEvidenceError> {
        self.ensure_live()?;
        let Some(last_frame) = &self.checkpoint.last_frame else {
            return Err(ScalpingCandidateEvidenceError::Checkpoint);
        };
        if last_frame.generation != frame.generation
            || last_frame.watermark_ms != frame.watermark_ms
            || decision_at_ms < frame.watermark_ms
        {
            return Err(ScalpingCandidateEvidenceError::Checkpoint);
        }
        let evidence = self
            .checkpoint
            .last_assembled_evidence
            .clone()
            .ok_or(ScalpingCandidateEvidenceError::Checkpoint)?;
        Ok(ScalpingResidentMarket {
            frame,
            decision_at_ms,
            evidence,
            direct_admission: false,
        })
    }

    /// Records only a proof whose host application has already produced the exact receipt.
    pub fn record_applied_risk(
        &mut self,
        applied: BoundRiskRevaluation,
        receipt: AppliedRiskReceipt,
    ) -> Result<(), ScalpingCandidateEvidenceError> {
        self.ensure_live()?;
        if !valid_applied_risk(&self.binding, &self.params, &applied, &receipt)? {
            return self.fail_closed(ScalpingCandidateEvidenceError::RiskIdentity);
        }
        if let Err(error) = self.assembler.record_applied_risk(applied.clone()) {
            return self.fail_closed(error.into());
        }
        let changed = self
            .checkpoint
            .applied_risk
            .as_ref()
            .is_some_and(|previous| previous.bound() != applied);
        self.checkpoint.applied_risk = Some(ScalpingCandidateAppliedRiskCheckpoint::from_bound(
            &applied, &receipt,
        ));
        if changed {
            self.checkpoint.pending_preparation = None;
        }
        self.persist_checkpoint()
    }

    /// Produces one market value. A preparation recorded after frame N can only join this next
    /// strictly advancing frame, after every projected bundle has been journaled and refreshed.
    pub fn assemble(
        &mut self,
        frame: FeatureFrame,
        observed_at_ms: u64,
    ) -> Result<ScalpingResidentMarket, ScalpingCandidateEvidenceError> {
        self.ensure_live()?;
        if let Err(error) = self.assembler.validate_next_input(&frame, observed_at_ms) {
            return self.fail_closed(error.into());
        }
        if let Err(error) = self.project_pending(observed_at_ms) {
            if error.is_retryable() {
                return Err(error);
            }
            return self.fail_closed(error);
        }
        let market = match self.assembler.assemble(frame, observed_at_ms) {
            Ok(market) => market,
            Err(error) if ScalpingMarketEvidenceAssembler::is_retryable_error(&error) => {
                return Err(error.into());
            }
            Err(error) => return self.fail_closed(error.into()),
        };
        self.checkpoint.last_frame = Some(ScalpingCandidateEvidenceFrameCursor {
            generation: market.frame.generation,
            watermark_ms: market.frame.watermark_ms,
        });
        self.checkpoint.last_assembled_evidence = Some(market.evidence.clone());
        self.checkpoint.pending_preparation = None;
        self.persist_checkpoint()?;
        Ok(market)
    }

    pub fn fence(&mut self) -> Result<(), ScalpingCandidateEvidenceError> {
        self.assembler
            .fence(ScalpingMarketEvidenceFence::PrivateFenced);
        self.checkpoint.pending_preparation = None;
        self.checkpoint.fenced = true;
        self.persist_checkpoint()
    }

    fn project_pending(
        &mut self,
        observed_at_ms: u64,
    ) -> Result<(), ScalpingCandidateEvidenceError> {
        let Some(preparation) = self.checkpoint.pending_preparation.clone() else {
            return Ok(());
        };
        let Some(applied) = self.checkpoint.applied_risk.clone() else {
            return Ok(());
        };
        let applied_bound = applied.bound();
        let mut bundles = Vec::new();
        for candidate in &preparation.candidates {
            let Some(quote_record) =
                self.quote_source
                    .lookup(&preparation, candidate, observed_at_ms)?
            else {
                continue;
            };
            let slice =
                match self
                    .calibration
                    .lookup(&preparation, candidate, self.live_calibration)
                {
                    Ok(slice) => slice,
                    Err(_) => continue,
                };
            let valid_until_ms = preparation
                .valid_until_ms
                .min(candidate.valid_until_ms)
                .min(slice.valid_until_ms);
            let calibration_identity = evidence_identity(
                "calibration",
                &preparation,
                candidate,
                slice.model_generation,
                self.calibration.artifact_digest(),
                valid_until_ms,
                &format!(
                    "{}:{}",
                    self.calibration.artifact_digest(),
                    slice.model_generation
                ),
            );
            let calibration = self.calibration.project_evidence(
                &preparation,
                candidate,
                calibration_identity,
                self.live_calibration,
            )?;
            let receipt = &quote_record.receipt;
            let projection = project_scalping_entry_evidence(
                &self.binding,
                &preparation,
                candidate,
                &calibration,
                &receipt.limits,
                &receipt.private,
                &receipt.quote_authority,
                &receipt.quote,
                &applied_bound,
                &applied.receipt,
                observed_at_ms,
            )?;
            bundles.push(projection.bundle);
        }
        if bundles.is_empty() {
            return Ok(());
        }
        let mut last_sequence = self.checkpoint.last_evidence_sequence;
        for bundle in bundles {
            last_sequence = Some(self.evidence_journal.append(bundle)?);
        }
        let source = ScalpingEvidenceSource::from_journal(&self.evidence_journal)?;
        self.assembler.replace_evidence_source(source);
        self.checkpoint.last_evidence_sequence = last_sequence;
        self.persist_checkpoint()
    }

    fn ensure_live(&self) -> Result<(), ScalpingCandidateEvidenceError> {
        if self.checkpoint.fenced {
            Err(ScalpingCandidateEvidenceError::Fenced)
        } else {
            Ok(())
        }
    }

    fn fail_closed<T>(
        &mut self,
        error: ScalpingCandidateEvidenceError,
    ) -> Result<T, ScalpingCandidateEvidenceError> {
        let _ = self.fence();
        Err(error)
    }

    fn persist_checkpoint(&mut self) -> Result<(), ScalpingCandidateEvidenceError> {
        self.checkpoint.state_digest = checkpoint_digest(&self.checkpoint)?;
        self.store.save(&self.checkpoint)?;
        Ok(())
    }
}

impl ScalpingCandidateEvidenceError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Journal(ScalpingEvidenceError::Io { .. }) => true,
            Self::Source(error) => ScalpingEvidenceSource::is_retryable_error(error),
            _ => false,
        }
    }
}

fn empty_checkpoint(binding: StrategyBinding) -> ScalpingCandidateEvidenceCheckpoint {
    ScalpingCandidateEvidenceCheckpoint {
        schema_version: SCALPING_CANDIDATE_EVIDENCE_SCHEMA_VERSION,
        binding,
        pending_preparation: None,
        applied_risk: None,
        last_frame: None,
        last_assembled_evidence: None,
        last_evidence_sequence: None,
        fenced: false,
        state_digest: String::new(),
    }
}

fn validate_paths(
    config: &ScalpingCandidateEvidenceConfig,
) -> Result<(), ScalpingCandidateEvidenceError> {
    for path in [
        &config.calibration_artifact_path,
        &config.core_quote_receipt_path,
        &config.evidence_journal_path,
        &config.checkpoint_path,
    ] {
        if path.as_os_str().is_empty() {
            return Err(ScalpingCandidateEvidenceError::Config);
        }
    }
    Ok(())
}

fn validate_checkpoint(
    checkpoint: &ScalpingCandidateEvidenceCheckpoint,
    binding: &StrategyBinding,
    params: &ScalpingParams,
) -> Result<(), ScalpingCandidateEvidenceError> {
    if checkpoint.schema_version != SCALPING_CANDIDATE_EVIDENCE_SCHEMA_VERSION
        || checkpoint.binding != *binding
        || checkpoint.state_digest != checkpoint_digest(checkpoint)?
    {
        return Err(ScalpingCandidateEvidenceError::Checkpoint);
    }
    if let Some(frame) = &checkpoint.last_frame {
        if frame.generation == 0 || frame.watermark_ms == 0 {
            return Err(ScalpingCandidateEvidenceError::Checkpoint);
        }
    }
    if checkpoint.last_assembled_evidence.is_some() && checkpoint.last_frame.is_none() {
        return Err(ScalpingCandidateEvidenceError::Checkpoint);
    }
    if checkpoint.last_evidence_sequence == Some(0) {
        return Err(ScalpingCandidateEvidenceError::Checkpoint);
    }
    if let Some(preparation) = &checkpoint.pending_preparation {
        if preparation.preparation_id.trim().is_empty()
            || preparation.binding_digest != binding.digest()
            || preparation.frame_generation == 0
            || preparation.watermark_ms == 0
            || checkpoint.last_frame.as_ref().is_none_or(|frame| {
                frame.generation != preparation.frame_generation
                    || frame.watermark_ms != preparation.watermark_ms
            })
        {
            return Err(ScalpingCandidateEvidenceError::Checkpoint);
        }
    }
    if let Some(applied) = &checkpoint.applied_risk {
        if !valid_applied_risk(binding, params, &applied.bound(), &applied.receipt)? {
            return Err(ScalpingCandidateEvidenceError::Checkpoint);
        }
    }
    Ok(())
}

fn valid_applied_risk(
    binding: &StrategyBinding,
    params: &ScalpingParams,
    applied: &BoundRiskRevaluation,
    receipt: &AppliedRiskReceipt,
) -> Result<bool, ScalpingCandidateEvidenceError> {
    let digest = risk_revaluation_digest(&applied.proof)
        .map_err(ScalpingCandidateEvidenceError::Strategy)?;
    let source = &applied.binding;
    Ok(receipt.binding == *binding
        && receipt.proof_id == applied.proof.proof_id
        && receipt.cursor_sequence == applied.cursor_sequence
        && receipt.cursor_sequence > 0
        && receipt.risk_revaluation_digest == digest
        && receipt.target_generation == applied.proof.target_generation
        && receipt.valuation_generation == source.valuation_generation
        && source.exchange == binding.exchange
        && source.account == binding.account
        && source.owner_scope == binding.owner_scope
        && source.strategy_instance_id == binding.strategy_instance_id
        && source.run_id == binding.run_id
        && source.parameter_release_id == binding.parameter_release_id
        && source.symbol == binding.symbol
        && source.risk_unit == params.risk_per_episode.unit
        && source.valuation_generation == applied.proof.target_generation
        && applied.proof.target_generation > 0
        && applied.proof.risk_unit == params.risk_per_episode.unit)
}

fn evidence_identity(
    owner: &str,
    preparation: &CandidatePreparation,
    candidate: &SemanticIntent,
    producer_generation: u64,
    release_digest: &str,
    valid_until_ms: u64,
    source_id: &str,
) -> EvidenceIdentity {
    let mut digest = Sha256::new();
    for field in [
        owner.as_bytes(),
        source_id.as_bytes(),
        preparation.preparation_id.as_bytes(),
        candidate.intent_id.as_bytes(),
        preparation.binding_digest.as_bytes(),
        release_digest.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    for value in [
        preparation.frame_generation,
        preparation.watermark_ms,
        producer_generation,
        valid_until_ms,
    ] {
        digest.update(value.to_be_bytes());
    }
    EvidenceIdentity {
        schema_version: crate::runtime::SCALPING_ENTRY_EVIDENCE_SCHEMA_VERSION,
        evidence_id: format!("scalping-{owner}-{:x}", digest.finalize()),
        candidate_id: candidate.intent_id.clone(),
        preparation_id: preparation.preparation_id.clone(),
        binding_digest: preparation.binding_digest.clone(),
        frame_generation: preparation.frame_generation,
        watermark_ms: preparation.watermark_ms,
        producer_generation,
        release_digest: release_digest.to_owned(),
        valid_until_ms,
    }
}

fn checkpoint_digest(
    checkpoint: &ScalpingCandidateEvidenceCheckpoint,
) -> Result<String, ScalpingCandidateEvidenceError> {
    let mut unsigned = checkpoint.clone();
    unsigned.state_digest.clear();
    let encoded = serde_json::to_vec(&unsigned).map_err(ScalpingCandidateEvidenceError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingCandidateEvidenceError {
    #[error("candidate evidence configuration is invalid")]
    Config,
    #[error("candidate evidence binding or strategy input is invalid: {0}")]
    Strategy(#[from] ScalpingError),
    #[error("candidate evidence checkpoint is invalid or tampered")]
    Checkpoint,
    #[error("candidate evidence identity is invalid")]
    Identity,
    #[error("candidate evidence risk receipt identity is invalid")]
    RiskIdentity,
    #[error("candidate evidence coordinator is fenced")]
    Fenced,
    #[error("Core quote receipt source failed: {0}")]
    Quote(#[from] ScalpingCoreQuoteReceiptError),
    #[error("candidate evidence journal failed: {0}")]
    Journal(#[from] ScalpingEvidenceError),
    #[error("candidate evidence projection failed: {0}")]
    Projection(#[from] ScalpingEntryEvidenceError),
    #[error("candidate evidence source failed: {0}")]
    Source(#[from] ScalpingEvidenceSourceError),
    #[error("candidate evidence assembler failed: {0}")]
    Assembler(#[from] ScalpingMarketEvidenceAssemblerError),
    #[error("candidate evidence storage failed: {0}")]
    Storage(#[from] StorageError),
    #[error("candidate evidence I/O failed for {path}: {source}", path = path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("candidate evidence encoding failed: {0}")]
    Encode(serde_json::Error),
}
