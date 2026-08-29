use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{Expert, ScalpingError, SemanticIntent};

pub const MAX_TRACKED_CANDIDATES: usize = 4_096;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SeenCandidate {
    pub candidate_id: String,
    pub opportunity_key: String,
    pub expert: Expert,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BreakoutCursor {
    pub feature_generation: u64,
    pub boundary_sequence: u64,
    pub compression_cycle_sequence: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateMemoryState {
    pub candidates: Vec<SeenCandidate>,
    pub breakout_cursor: Option<BreakoutCursor>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateMemoryRejection {
    Duplicate,
    Capacity,
}

#[derive(Clone, Debug, Default)]
pub struct CandidateMemory {
    candidates: BTreeMap<String, SeenCandidate>,
    breakout_cursor: Option<BreakoutCursor>,
}

impl CandidateMemory {
    pub fn check_and_record(
        &mut self,
        candidate: &SemanticIntent,
        watermark_ms: u64,
    ) -> Result<(), CandidateMemoryRejection> {
        self.prune(watermark_ms);
        if self.candidates.contains_key(&candidate.intent_id)
            || self.candidates.values().any(|seen| {
                seen.expert == candidate.expert && seen.opportunity_key == candidate.opportunity_key
            })
        {
            return Err(CandidateMemoryRejection::Duplicate);
        }
        if let Some(next) = &candidate.breakout_cursor
            && !is_new_breakout(self.breakout_cursor.as_ref(), next)
        {
            return Err(CandidateMemoryRejection::Duplicate);
        }
        if self.candidates.len() >= MAX_TRACKED_CANDIDATES {
            return Err(CandidateMemoryRejection::Capacity);
        }
        self.candidates.insert(
            candidate.intent_id.clone(),
            SeenCandidate {
                candidate_id: candidate.intent_id.clone(),
                opportunity_key: candidate.opportunity_key.clone(),
                expert: candidate.expert,
                expires_at_ms: candidate.valid_until_ms,
            },
        );
        if candidate.breakout_cursor.is_some() {
            self.breakout_cursor.clone_from(&candidate.breakout_cursor);
        }
        Ok(())
    }

    pub fn export_state(&self) -> CandidateMemoryState {
        CandidateMemoryState {
            candidates: self.candidates.values().cloned().collect(),
            breakout_cursor: self.breakout_cursor.clone(),
        }
    }

    pub fn restore_state(&mut self, state: CandidateMemoryState) -> Result<(), ScalpingError> {
        if state.candidates.len() > MAX_TRACKED_CANDIDATES
            || state.candidates.iter().any(|seen| {
                seen.candidate_id.trim().is_empty()
                    || seen.opportunity_key.trim().is_empty()
                    || seen.expires_at_ms == 0
            })
            || state.candidates.iter().enumerate().any(|(index, seen)| {
                state.candidates.iter().skip(index + 1).any(|other| {
                    seen.expert == other.expert && seen.opportunity_key == other.opportunity_key
                })
            })
        {
            return Err(ScalpingError::Checkpoint);
        }
        let expected_len = state.candidates.len();
        let candidates = state
            .candidates
            .into_iter()
            .map(|seen| (seen.candidate_id.clone(), seen))
            .collect::<BTreeMap<_, _>>();
        if candidates.len() != expected_len {
            return Err(ScalpingError::Checkpoint);
        }
        if state.breakout_cursor.as_ref().is_some_and(|cursor| {
            cursor.feature_generation == 0
                || cursor.boundary_sequence == 0
                || cursor.compression_cycle_sequence == 0
        }) {
            return Err(ScalpingError::Checkpoint);
        }
        self.candidates = candidates;
        self.breakout_cursor = state.breakout_cursor;
        Ok(())
    }

    fn prune(&mut self, watermark_ms: u64) {
        self.candidates
            .retain(|_, seen| seen.expires_at_ms >= watermark_ms);
    }
}

fn is_new_breakout(previous: Option<&BreakoutCursor>, next: &BreakoutCursor) -> bool {
    previous.is_none_or(|previous| {
        next.feature_generation > previous.feature_generation
            || (next.feature_generation == previous.feature_generation
                && next.boundary_sequence > previous.boundary_sequence
                && next.compression_cycle_sequence > previous.compression_cycle_sequence)
    })
}
