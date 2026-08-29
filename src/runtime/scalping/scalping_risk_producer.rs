use std::path::PathBuf;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    storage::{
        ScalpingRiskBinding, ScalpingRiskCursor, ScalpingRiskError, ScalpingRiskFact,
        ScalpingRiskJournal, ScalpingRiskReplay,
    },
    strategy::scalping::{RiskFact, RiskRevaluation, RiskUnit, StrategyBinding},
};

pub const MAX_RISK_REPLAY_PAGES: usize = 128;
pub const MAX_RISK_FACTS_PER_PAGE: usize = 512;

/// A complete logical-risk proof together with the binding and durable cursor that produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundRiskRevaluation {
    pub binding: ScalpingRiskBinding,
    pub proof: RiskRevaluation,
    pub cursor_sequence: u64,
}

/// The explicit bounded-time policy for accepting a complete risk replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskProofClock {
    pub now_ms: u64,
    pub max_stale_ms: u64,
}

/// Owns no pricing or account projection. It only persists upstream logical risk facts, then
/// converts a committed replay into the strategy's existing revaluation proof type.
#[derive(Debug)]
pub struct RiskRevaluationProducer {
    expected_binding: StrategyBinding,
    expected_risk_unit: RiskUnit,
    journal: ScalpingRiskJournal,
}

impl RiskRevaluationProducer {
    pub fn open(
        path: impl Into<PathBuf>,
        expected_binding: &StrategyBinding,
        expected_risk_unit: RiskUnit,
    ) -> Result<Self, RiskRevaluationProducerError> {
        if expected_binding.validate().is_err() || expected_risk_unit.as_str().is_empty() {
            return Err(RiskRevaluationProducerError::Binding);
        }
        let journal = ScalpingRiskJournal::open(path)?;
        let producer = Self {
            expected_binding: expected_binding.clone(),
            expected_risk_unit,
            journal,
        };
        producer.validate_recovered_bindings()?;
        Ok(producer)
    }

    /// Durably commits one source page. Intermediate pages return `None`; only a fresh terminal
    /// page that covers every prior intermediate page returns `Some(proof)`.
    pub fn commit_page(
        &mut self,
        clock: RiskProofClock,
        facts: Vec<ScalpingRiskFact>,
        cursor: ScalpingRiskCursor,
    ) -> Result<Option<BoundRiskRevaluation>, RiskRevaluationProducerError> {
        self.validate_binding(&cursor.binding)?;
        self.validate_new_page_bounds(&facts, &cursor)?;
        if facts
            .iter()
            .any(|fact| fact.binding != cursor.binding || !self.matches_binding(&fact.binding))
        {
            return Err(RiskRevaluationProducerError::Binding);
        }
        let commit = self.journal.append_page(facts, cursor.clone())?;
        if cursor.has_more {
            return Ok(None);
        }
        let replay = complete_replays(self.journal.committed_replays()?)?
            .into_iter()
            .find(|replay| replay.cursor.cursor_id == cursor.cursor_id)
            .ok_or(RiskRevaluationProducerError::CommittedCursorMissing)?;
        let proof = self.bound_proof(replay, commit.cursor_sequence)?;
        self.ensure_fresh(clock, &proof)?;
        Ok(Some(proof))
    }

    /// Restores the latest complete proof after the caller's durable application point. An
    /// unknown non-empty id is a fence: the producer must not guess whether a proof was applied.
    pub fn recover_complete(
        &self,
        clock: RiskProofClock,
        last_applied_proof_id: Option<&str>,
    ) -> Result<Option<BoundRiskRevaluation>, RiskRevaluationProducerError> {
        let replays = complete_replays(self.journal.recover_committed_replays()?)?;
        let proofs = replays
            .into_iter()
            .map(|replay| {
                let cursor_sequence = replay.cursor_sequence;
                self.bound_proof(replay, cursor_sequence)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let Some(last_applied_proof_id) = last_applied_proof_id.filter(|value| !value.is_empty())
        else {
            let candidate = proofs.into_iter().last();
            if let Some(candidate) = &candidate {
                self.ensure_fresh(clock, candidate)?;
            }
            return Ok(candidate);
        };
        let Some(applied_index) = proofs
            .iter()
            .position(|proof| proof.proof.proof_id == last_applied_proof_id)
        else {
            return Err(RiskRevaluationProducerError::UnknownAppliedProof);
        };
        if applied_index + 1 == proofs.len() {
            Ok(None)
        } else {
            let candidate = proofs.last().cloned();
            if let Some(candidate) = &candidate {
                self.ensure_fresh(clock, candidate)?;
            }
            Ok(candidate)
        }
    }

    /// Returns the last cursor that is fully durable, including an intermediate cursor whose
    /// terminal manifest has not arrived yet. A source resumes from this point; orphan facts are
    /// deliberately invisible here.
    pub fn recover_durable_cursor(
        &self,
    ) -> Result<Option<ScalpingRiskCursor>, RiskRevaluationProducerError> {
        let replays = self.journal.recover_committed_replays()?;
        validate_replay_bounds(&replays)?;
        for replay in &replays {
            self.validate_binding(&replay.cursor.binding)?;
        }
        Ok(replays.last().map(|replay| replay.cursor.clone()))
    }

    fn validate_recovered_bindings(&self) -> Result<(), RiskRevaluationProducerError> {
        for record in self.journal.recover()?.records {
            match record.entry {
                crate::storage::ScalpingRiskEntry::Fact(fact) => {
                    self.validate_binding(&fact.binding)?;
                }
                crate::storage::ScalpingRiskEntry::Cursor(cursor) => {
                    self.validate_binding(&cursor.binding)?;
                }
            }
        }
        validate_replay_bounds(&self.journal.recover_committed_replays()?)?;
        Ok(())
    }

    fn validate_new_page_bounds(
        &self,
        facts: &[ScalpingRiskFact],
        cursor: &ScalpingRiskCursor,
    ) -> Result<(), RiskRevaluationProducerError> {
        if facts.len() > MAX_RISK_FACTS_PER_PAGE {
            return Err(RiskRevaluationProducerError::ReplayBound);
        }
        let replays = self.journal.committed_replays()?;
        if replays
            .iter()
            .any(|replay| replay.cursor.cursor_id == cursor.cursor_id)
        {
            return Ok(());
        }
        let pending_pages = replays
            .iter()
            .rev()
            .take_while(|replay| replay.cursor.has_more)
            .count();
        let next_pages = pending_pages.saturating_add(1);
        if next_pages > MAX_RISK_REPLAY_PAGES
            || (next_pages == MAX_RISK_REPLAY_PAGES && cursor.has_more)
        {
            return Err(RiskRevaluationProducerError::ReplayBound);
        }
        Ok(())
    }

    fn bound_proof(
        &self,
        replay: ScalpingRiskReplay,
        cursor_sequence: u64,
    ) -> Result<BoundRiskRevaluation, RiskRevaluationProducerError> {
        self.validate_binding(&replay.cursor.binding)?;
        if replay.cursor.has_more
            || replay.facts.iter().any(|fact| {
                fact.binding != replay.cursor.binding
                    || fact.fact.event_time_ms < replay.cursor.complete_from_ms
                    || fact.fact.event_time_ms > replay.cursor.observed_through_ms
            })
        {
            return Err(RiskRevaluationProducerError::Replay);
        }
        if replay
            .facts
            .iter()
            .map(|fact| fact.fact.fact_id.as_str())
            .ne(replay.cursor.source_fact_ids.iter().map(String::as_str))
        {
            return Err(RiskRevaluationProducerError::Replay);
        }
        let mut revalued_facts = replay.facts.into_iter().enumerate().collect::<Vec<_>>();
        revalued_facts.sort_by_key(|(source_index, fact)| (fact.fact.event_time_ms, *source_index));
        let mut proof = RiskRevaluation {
            proof_id: String::new(),
            target_generation: replay.cursor.binding.valuation_generation,
            risk_unit: replay.cursor.binding.risk_unit.clone(),
            window_start_ms: replay.cursor.complete_from_ms,
            complete_through_ms: replay.cursor.observed_through_ms,
            source_fact_ids: replay.cursor.source_fact_ids.clone(),
            revalued_facts: revalued_facts
                .into_iter()
                .map(|(_, fact)| fact.fact)
                .collect(),
        };
        proof.proof_id = proof_id(&replay.cursor, &proof)?;
        Ok(BoundRiskRevaluation {
            binding: replay.cursor.binding,
            proof,
            cursor_sequence,
        })
    }

    fn ensure_fresh(
        &self,
        clock: RiskProofClock,
        proof: &BoundRiskRevaluation,
    ) -> Result<(), RiskRevaluationProducerError> {
        if clock.now_ms.abs_diff(proof.proof.complete_through_ms) > clock.max_stale_ms {
            return Err(RiskRevaluationProducerError::Replay);
        }
        Ok(())
    }

    fn validate_binding(
        &self,
        binding: &ScalpingRiskBinding,
    ) -> Result<(), RiskRevaluationProducerError> {
        if self.matches_binding(binding) {
            Ok(())
        } else {
            Err(RiskRevaluationProducerError::Binding)
        }
    }

    fn matches_binding(&self, binding: &ScalpingRiskBinding) -> bool {
        binding.exchange == self.expected_binding.exchange
            && binding.account == self.expected_binding.account
            && binding.owner_scope == self.expected_binding.owner_scope
            && binding.strategy_instance_id == self.expected_binding.strategy_instance_id
            && binding.run_id == self.expected_binding.run_id
            && binding.parameter_release_id == self.expected_binding.parameter_release_id
            && binding.symbol == self.expected_binding.symbol
            && binding.risk_unit == self.expected_risk_unit
    }
}

fn complete_replays(
    replays: Vec<ScalpingRiskReplay>,
) -> Result<Vec<ScalpingRiskReplay>, RiskRevaluationProducerError> {
    let mut pending_fact_ids = std::collections::BTreeSet::new();
    let mut complete = Vec::new();
    for replay in replays {
        if replay.cursor.has_more {
            pending_fact_ids.extend(replay.cursor.source_fact_ids.iter().cloned());
            continue;
        }
        let terminal_ids = replay
            .cursor
            .source_fact_ids
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if !pending_fact_ids.is_subset(&terminal_ids) {
            return Err(RiskRevaluationProducerError::Replay);
        }
        pending_fact_ids.clear();
        complete.push(replay);
    }
    Ok(complete)
}

fn validate_replay_bounds(
    replays: &[ScalpingRiskReplay],
) -> Result<(), RiskRevaluationProducerError> {
    let mut pages_in_request = 0_usize;
    for replay in replays {
        if replay.facts.len() > MAX_RISK_FACTS_PER_PAGE {
            return Err(RiskRevaluationProducerError::ReplayBound);
        }
        pages_in_request = pages_in_request.saturating_add(1);
        if pages_in_request > MAX_RISK_REPLAY_PAGES {
            return Err(RiskRevaluationProducerError::ReplayBound);
        }
        if !replay.cursor.has_more {
            pages_in_request = 0;
        }
    }
    if pages_in_request >= MAX_RISK_REPLAY_PAGES {
        return Err(RiskRevaluationProducerError::ReplayBound);
    }
    Ok(())
}

#[derive(Serialize)]
struct ProofIdentity<'a> {
    binding: &'a ScalpingRiskBinding,
    cursor_id: &'a str,
    source_sequence: u64,
    target_generation: u64,
    risk_unit: &'a RiskUnit,
    window_start_ms: u64,
    complete_through_ms: u64,
    source_fact_ids: &'a [String],
    revalued_facts: &'a [RiskFact],
}

fn proof_id(
    cursor: &ScalpingRiskCursor,
    proof: &RiskRevaluation,
) -> Result<String, RiskRevaluationProducerError> {
    let identity = ProofIdentity {
        binding: &cursor.binding,
        cursor_id: &cursor.cursor_id,
        source_sequence: cursor.source_sequence,
        target_generation: proof.target_generation,
        risk_unit: &proof.risk_unit,
        window_start_ms: proof.window_start_ms,
        complete_through_ms: proof.complete_through_ms,
        source_fact_ids: &proof.source_fact_ids,
        revalued_facts: &proof.revalued_facts,
    };
    let bytes = serde_json::to_vec(&identity).map_err(RiskRevaluationProducerError::Encode)?;
    Ok(format!("risk-revaluation-{:x}", Sha256::digest(bytes)))
}

#[derive(Debug, thiserror::Error)]
pub enum RiskRevaluationProducerError {
    #[error(
        "risk producer binding differs from its fixed owner, instance, run, release, symbol, or unit"
    )]
    Binding,
    #[error("risk producer replay is incomplete or incompatible")]
    Replay,
    #[error("risk producer replay exceeds the legacy bounded page or fact count")]
    ReplayBound,
    #[error("risk producer committed cursor was not recoverable")]
    CommittedCursorMissing,
    #[error("risk producer cannot find the caller's last applied proof")]
    UnknownAppliedProof,
    #[error("risk producer journal failed: {0}")]
    Journal(#[from] ScalpingRiskError),
    #[error("risk producer proof identity encoding failed: {0}")]
    Encode(serde_json::Error),
}
