use std::path::PathBuf;

use crate::{indicator::FeatureFrame, strategy::scalping::CandidatePreparation};

use super::{
    BoundRiskRevaluation, ScalpingEvidenceSource, ScalpingEvidenceSourceError,
    ScalpingResidentMarket,
};

/// An explicit reason why a prior candidate preparation can no longer cross the resident
/// boundary. The caller owns the relevant control and private-fact interpretation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalpingMarketEvidenceFence {
    ControlStopped,
    PrivateFenced,
}

/// Read-only two-frame assembly of resident market input.
///
/// A preparation is recorded only after the host durably reports it. Its producing frame always
/// receives no evidence. The next strictly advancing frame may query the immutable evidence
/// source against the last successfully applied logical-risk proof. This type deliberately does
/// not derive calibration, costs, risk, authorization, or a market decision.
#[derive(Debug)]
pub struct ScalpingMarketEvidenceAssembler {
    source: ScalpingEvidenceSource,
    applied_risk: Option<BoundRiskRevaluation>,
    pending: Option<CandidatePreparation>,
    last_frame: Option<FrameCursor>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FrameCursor {
    generation: u64,
    watermark_ms: u64,
}

impl ScalpingMarketEvidenceAssembler {
    #[must_use]
    pub fn new(source: ScalpingEvidenceSource) -> Self {
        Self {
            source,
            applied_risk: None,
            pending: None,
            last_frame: None,
        }
    }

    /// Replaces the immutable journal snapshot without changing the active proof, frame cursor,
    /// or pending preparation. Callers must construct the replacement before this atomic swap.
    pub fn replace_evidence_source(&mut self, source: ScalpingEvidenceSource) {
        self.source = source;
    }

    /// Restores the last durably consumed frame before a pending preparation is replayed.
    /// Recovery is only valid on a fresh assembler; pending/proof state is restored separately.
    pub fn restore_last_frame(
        &mut self,
        generation: u64,
        watermark_ms: u64,
    ) -> Result<(), ScalpingMarketEvidenceAssemblerError> {
        if generation == 0 || watermark_ms == 0 || self.last_frame.is_some() {
            return Err(ScalpingMarketEvidenceAssemblerError::FrameOrder);
        }
        self.last_frame = Some(FrameCursor {
            generation,
            watermark_ms,
        });
        Ok(())
    }

    /// Opens and validates a fresh immutable journal snapshot before replacing the current one.
    /// A failed refresh leaves every assembler state field unchanged.
    pub fn refresh_evidence_source(
        &mut self,
        path: impl Into<PathBuf>,
    ) -> Result<(), ScalpingMarketEvidenceAssemblerError> {
        let source = ScalpingEvidenceSource::open(path)?;
        self.replace_evidence_source(source);
        Ok(())
    }

    /// Records a proof only after its owner has successfully applied it. Exact repeats are
    /// idempotent. A changed proof must have a strictly newer cursor and identity; otherwise it
    /// is a contradictory replacement of the active logical-risk view.
    pub fn record_applied_risk(
        &mut self,
        applied: BoundRiskRevaluation,
    ) -> Result<(), ScalpingMarketEvidenceAssemblerError> {
        if applied.cursor_sequence == 0
            || applied.proof.proof_id.trim().is_empty()
            || applied.proof.target_generation == 0
            || applied.proof.risk_unit.as_str().is_empty()
        {
            return Err(ScalpingMarketEvidenceAssemblerError::RiskProof);
        }
        if let Some(previous) = &self.applied_risk {
            if applied.cursor_sequence < previous.cursor_sequence
                || (applied.cursor_sequence == previous.cursor_sequence && applied != *previous)
                || (applied.cursor_sequence > previous.cursor_sequence
                    && applied.proof.proof_id == previous.proof.proof_id)
            {
                return Err(ScalpingMarketEvidenceAssemblerError::RiskProof);
            }
            if applied == *previous {
                return Ok(());
            }
            self.pending = None;
        }
        self.applied_risk = Some(applied);
        Ok(())
    }

    /// Records the preparation from the preceding durably reported host turn. The caller must
    /// pass `ScalpingCoordinatorOutput::preparation` only after its checkpoint save succeeded.
    /// A preparation must be bound to the immediately preceding public frame.
    pub fn record_preparation(
        &mut self,
        preparation: Option<CandidatePreparation>,
    ) -> Result<(), ScalpingMarketEvidenceAssemblerError> {
        let Some(preparation) = preparation else {
            self.pending = None;
            return Ok(());
        };
        let Some(last_frame) = self.last_frame else {
            return Err(ScalpingMarketEvidenceAssemblerError::Preparation);
        };
        if preparation.frame_generation != last_frame.generation
            || preparation.watermark_ms != last_frame.watermark_ms
        {
            return Err(ScalpingMarketEvidenceAssemblerError::Preparation);
        }
        self.pending = Some(preparation);
        Ok(())
    }

    /// Clears only the pending preparation. A later, freshly reported preparation may reuse the
    /// current applied proof; it cannot reuse a candidate produced before the fence.
    pub fn fence(&mut self, _reason: ScalpingMarketEvidenceFence) {
        self.pending = None;
    }

    /// Validates the next frame without consuming pending state or changing the cursor.
    pub fn validate_next_input(
        &self,
        frame: &FeatureFrame,
        decision_at_ms: u64,
    ) -> Result<(), ScalpingMarketEvidenceAssemblerError> {
        if decision_at_ms == 0 {
            return Err(ScalpingMarketEvidenceAssemblerError::DecisionTime);
        }
        let cursor = FrameCursor {
            generation: frame.generation,
            watermark_ms: frame.watermark_ms,
        };
        if cursor.generation == 0
            || cursor.watermark_ms == 0
            || self
                .last_frame
                .is_some_and(|previous| !strictly_after(cursor, previous))
        {
            return Err(ScalpingMarketEvidenceAssemblerError::FrameOrder);
        }
        Ok(())
    }

    #[must_use]
    pub fn is_retryable_error(error: &ScalpingMarketEvidenceAssemblerError) -> bool {
        matches!(
            error,
            ScalpingMarketEvidenceAssemblerError::Evidence(source)
                if ScalpingEvidenceSource::is_retryable_error(source)
        )
    }

    /// Produces exactly one resident market input. Missing proof or journal evidence is a normal
    /// fail-closed result with an empty evidence list; invalid or ambiguous evidence is an error.
    pub fn assemble(
        &mut self,
        frame: FeatureFrame,
        decision_at_ms: u64,
    ) -> Result<ScalpingResidentMarket, ScalpingMarketEvidenceAssemblerError> {
        self.validate_next_input(&frame, decision_at_ms)?;
        let cursor = FrameCursor {
            generation: frame.generation,
            watermark_ms: frame.watermark_ms,
        };

        if self
            .last_frame
            .is_some_and(|previous| cursor.generation != previous.generation)
        {
            self.pending = None;
        }
        // Keep the pending preparation and frame cursor untouched until the join succeeds. A
        // journal/source error must be retryable with the same strictly advancing frame; taking
        // it before `join` would silently turn a recoverable storage fault into empty evidence.
        let pending = self
            .pending
            .clone()
            .filter(|preparation| decision_at_ms <= preparation.valid_until_ms);
        let evidence = match (pending.as_ref(), self.applied_risk.as_ref()) {
            (Some(preparation), Some(applied)) => {
                self.source
                    .join(preparation, &applied.proof, decision_at_ms)?
            }
            _ => Vec::new(),
        };
        self.pending = None;
        self.last_frame = Some(cursor);
        Ok(ScalpingResidentMarket {
            frame,
            decision_at_ms,
            evidence,
            direct_admission: false,
        })
    }

    #[must_use]
    pub fn has_pending_preparation(&self) -> bool {
        self.pending.is_some()
    }
}

fn strictly_after(current: FrameCursor, previous: FrameCursor) -> bool {
    current.generation >= previous.generation && current.watermark_ms > previous.watermark_ms
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingMarketEvidenceAssemblerError {
    #[error("resident market frame did not strictly advance")]
    FrameOrder,
    #[error("resident market decision time is invalid")]
    DecisionTime,
    #[error("applied bound risk proof is incomplete")]
    RiskProof,
    #[error("candidate preparation is not bound to the preceding public frame")]
    Preparation,
    #[error("resident evidence source rejected the journal bundle: {0}")]
    Evidence(#[from] ScalpingEvidenceSourceError),
}
