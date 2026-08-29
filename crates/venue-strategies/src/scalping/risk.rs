use std::collections::{BTreeSet, VecDeque};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{RiskUnit, ScalpingError, ScalpingParams};

pub const MAX_RISK_FACTS: usize = 65_536;
pub const MAX_PENDING_REVALUATION_FACTS: usize = 4_096;

/// One owner-scoped, valuation-bound PnL fact. It is a logical risk amount and never an account,
/// position, fill, or venue projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RiskFact {
    pub fact_id: String,
    pub event_time_ms: u64,
    pub valuation_generation: u64,
    pub risk_unit: RiskUnit,
    #[serde(with = "rust_decimal::serde::str")]
    pub realized_pnl: Decimal,
}

impl venue_domain::RiskFactValue<RiskUnit> for RiskFact {
    fn fact_id(&self) -> &str {
        &self.fact_id
    }

    fn event_time_ms(&self) -> u64 {
        self.event_time_ms
    }

    fn valuation_generation(&self) -> u64 {
        self.valuation_generation
    }

    fn risk_unit(&self) -> &RiskUnit {
        &self.risk_unit
    }
}

/// Complete revaluation proof for the active windows. Strategy never converts between valuation
/// generations itself.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskRevaluation {
    pub proof_id: String,
    pub target_generation: u64,
    pub risk_unit: RiskUnit,
    pub window_start_ms: u64,
    pub complete_through_ms: u64,
    pub source_fact_ids: Vec<String>,
    pub revalued_facts: Vec<RiskFact>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskGate {
    Open,
    LossWindow,
    Drawdown,
    LossStreak,
    Cooldown,
    GenerationMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RiskSnapshot {
    pub gate: RiskGate,
    pub valuation_generation: Option<u64>,
    pub risk_unit: RiskUnit,
    #[serde(with = "rust_decimal::serde::str")]
    pub rolling_loss: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub drawdown: Decimal,
    pub loss_streak: u32,
    pub cooldown_until_ms: Option<u64>,
}

/// Durable logical risk projection only. It contains no balance, order, fill, or position fact.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RiskLedgerState {
    pub facts: Vec<RiskFact>,
    pub seen_fact_ids: Vec<String>,
    #[serde(default)]
    pub pending_revaluation_fact_ids: Vec<String>,
    pub last_event_time_ms: Option<u64>,
    pub valuation_generation: Option<u64>,
    #[serde(default)]
    pub generation_mismatch: bool,
    #[serde(default)]
    pub cooldown_until_ms: Option<u64>,
    #[serde(default)]
    pub last_revaluation_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RiskLedger {
    facts: VecDeque<RiskFact>,
    seen: BTreeSet<String>,
    pending_revaluation: BTreeSet<String>,
    last_event_time: Option<u64>,
    generation: Option<u64>,
    generation_mismatch: bool,
    cooldown_until: Option<u64>,
    last_revaluation_id: Option<String>,
    risk_unit: RiskUnit,
    loss_window_ms: u64,
    drawdown_window_ms: u64,
    loss_window_limit: Decimal,
    drawdown_limit: Decimal,
    max_loss_streak: u32,
    loss_cooldown_ms: u64,
}

impl RiskLedger {
    pub fn new(params: &ScalpingParams) -> Self {
        Self {
            facts: VecDeque::new(),
            seen: BTreeSet::new(),
            pending_revaluation: BTreeSet::new(),
            last_event_time: None,
            generation: None,
            generation_mismatch: false,
            cooldown_until: None,
            last_revaluation_id: None,
            risk_unit: params.risk_per_episode.unit.clone(),
            loss_window_ms: params.loss_window_ms,
            drawdown_window_ms: params.drawdown_window_ms,
            loss_window_limit: params.loss_window_limit.value,
            drawdown_limit: params.drawdown_limit.value,
            max_loss_streak: params.max_loss_streak,
            loss_cooldown_ms: params.loss_cooldown_ms,
        }
    }

    pub fn is_pristine(&self) -> bool {
        self.facts.is_empty()
            && self.pending_revaluation.is_empty()
            && self.last_event_time.is_none()
            && self.generation.is_none()
            && !self.generation_mismatch
    }

    pub fn record(&mut self, fact: RiskFact) -> Result<RiskSnapshot, ScalpingError> {
        if self.seen.contains(&fact.fact_id) || self.pending_revaluation.contains(&fact.fact_id) {
            return Ok(self.snapshot(self.last_event_time.unwrap_or(fact.event_time_ms)));
        }
        if fact.fact_id.trim().is_empty()
            || fact.event_time_ms == 0
            || fact.valuation_generation == 0
            || fact.risk_unit != self.risk_unit
            || self
                .last_event_time
                .is_some_and(|last| fact.event_time_ms < last)
        {
            return Err(ScalpingError::Risk);
        }
        if self.generation_mismatch {
            if self.pending_revaluation.len() >= MAX_PENDING_REVALUATION_FACTS {
                return Err(ScalpingError::Risk);
            }
            self.last_event_time = Some(fact.event_time_ms);
            self.pending_revaluation.insert(fact.fact_id);
            return Ok(self.snapshot(fact.event_time_ms));
        }
        if self
            .generation
            .is_some_and(|generation| generation != fact.valuation_generation)
        {
            self.generation = None;
            self.generation_mismatch = true;
            self.last_event_time = Some(fact.event_time_ms);
            self.pending_revaluation.insert(fact.fact_id);
            return Ok(self.snapshot(fact.event_time_ms));
        }
        if self.facts.len() >= MAX_RISK_FACTS {
            self.trim_at(fact.event_time_ms);
            if self.facts.len() >= MAX_RISK_FACTS {
                return Err(ScalpingError::Risk);
            }
        }
        self.generation = Some(fact.valuation_generation);
        self.last_event_time = Some(fact.event_time_ms);
        self.seen.insert(fact.fact_id.clone());
        self.facts.push_back(fact);
        self.trim();
        let now_ms = self.last_event_time.unwrap_or_default();
        if matches!(
            self.snapshot(now_ms).gate,
            RiskGate::LossWindow | RiskGate::Drawdown | RiskGate::LossStreak
        ) {
            self.cooldown_until = Some(
                self.cooldown_until
                    .unwrap_or_default()
                    .max(now_ms.saturating_add(self.loss_cooldown_ms)),
            );
        }
        Ok(self.snapshot(now_ms))
    }

    pub fn require_revaluation(
        &mut self,
        observed_at_ms: u64,
    ) -> Result<RiskSnapshot, ScalpingError> {
        if observed_at_ms == 0
            || self
                .last_event_time
                .is_some_and(|last| observed_at_ms < last)
        {
            return Err(ScalpingError::Risk);
        }
        self.last_event_time = Some(observed_at_ms);
        self.generation = None;
        self.generation_mismatch = true;
        Ok(self.snapshot(observed_at_ms))
    }

    pub fn export_state(&self) -> RiskLedgerState {
        RiskLedgerState {
            facts: self.facts.iter().cloned().collect(),
            seen_fact_ids: self.seen.iter().cloned().collect(),
            pending_revaluation_fact_ids: self.pending_revaluation.iter().cloned().collect(),
            last_event_time_ms: self.last_event_time,
            valuation_generation: self.generation,
            generation_mismatch: self.generation_mismatch,
            cooldown_until_ms: self.cooldown_until,
            last_revaluation_id: self.last_revaluation_id.clone(),
        }
    }

    pub fn restore_state(&mut self, state: RiskLedgerState) -> Result<(), ScalpingError> {
        if state
            .facts
            .windows(2)
            .any(|pair| pair[0].event_time_ms > pair[1].event_time_ms)
            || state.facts.len() > MAX_RISK_FACTS
            || state.pending_revaluation_fact_ids.len() > MAX_PENDING_REVALUATION_FACTS
            || state.facts.iter().any(|fact| {
                fact.fact_id.trim().is_empty()
                    || fact.risk_unit != self.risk_unit
                    || fact.event_time_ms == 0
                    || fact.valuation_generation == 0
            })
            || (state.generation_mismatch && state.valuation_generation.is_some())
            || (!state.generation_mismatch
                && !state.facts.is_empty()
                && state.valuation_generation.is_none())
            || state.facts.iter().any(|fact| {
                state
                    .valuation_generation
                    .is_some_and(|generation| generation != fact.valuation_generation)
            })
        {
            return Err(ScalpingError::Checkpoint);
        }
        let pending_count = state.pending_revaluation_fact_ids.len();
        self.facts = state.facts.into();
        self.seen = state.seen_fact_ids.into_iter().collect();
        self.pending_revaluation = state.pending_revaluation_fact_ids.into_iter().collect();
        self.last_event_time = state.last_event_time_ms;
        self.generation = state.valuation_generation;
        self.generation_mismatch = state.generation_mismatch;
        self.cooldown_until = state.cooldown_until_ms;
        self.last_revaluation_id = state.last_revaluation_id;
        if self.seen.len() != self.facts.len()
            || self.pending_revaluation.len() != pending_count
            || self
                .seen
                .iter()
                .any(|fact_id| self.pending_revaluation.contains(fact_id))
            || self
                .facts
                .iter()
                .any(|fact| !self.seen.contains(&fact.fact_id))
            || (!self.generation_mismatch && !self.pending_revaluation.is_empty())
        {
            return Err(ScalpingError::Checkpoint);
        }
        self.trim();
        Ok(())
    }

    pub fn apply_revaluation(
        &mut self,
        proof: RiskRevaluation,
    ) -> Result<RiskSnapshot, ScalpingError> {
        if self.last_revaluation_id.as_deref() == Some(proof.proof_id.as_str()) {
            return Ok(self.snapshot(proof.complete_through_ms));
        }
        let required_start = proof
            .complete_through_ms
            .saturating_sub(self.drawdown_window_ms.max(self.loss_window_ms));
        let source_ids = proof.source_fact_ids.iter().collect::<BTreeSet<_>>();
        let required_ids = self
            .facts
            .iter()
            .filter(|fact| fact.event_time_ms >= proof.window_start_ms)
            .map(|fact| &fact.fact_id)
            .chain(self.pending_revaluation.iter())
            .collect::<BTreeSet<_>>();
        let revalued_ids = proof
            .revalued_facts
            .iter()
            .map(|fact| &fact.fact_id)
            .collect::<BTreeSet<_>>();
        if (!self.generation_mismatch && (self.generation.is_some() || !self.facts.is_empty()))
            || proof.proof_id.trim().is_empty()
            || proof.target_generation == 0
            || proof.risk_unit != self.risk_unit
            || proof.complete_through_ms == 0
            || proof.window_start_ms > required_start
            || self
                .last_event_time
                .is_some_and(|last| proof.complete_through_ms < last)
            || proof.revalued_facts.len() > MAX_RISK_FACTS
            || source_ids.len() != proof.source_fact_ids.len()
            || proof
                .source_fact_ids
                .iter()
                .any(|fact_id| fact_id.trim().is_empty())
            || !required_ids.is_subset(&source_ids)
            || revalued_ids.len() != proof.revalued_facts.len()
            || proof
                .revalued_facts
                .windows(2)
                .any(|pair| pair[0].event_time_ms > pair[1].event_time_ms)
            || proof.revalued_facts.iter().any(|fact| {
                fact.fact_id.trim().is_empty()
                    || fact.risk_unit != self.risk_unit
                    || fact.valuation_generation != proof.target_generation
                    || fact.event_time_ms < proof.window_start_ms
                    || fact.event_time_ms > proof.complete_through_ms
            })
        {
            return Err(ScalpingError::Risk);
        }
        self.facts = proof.revalued_facts.into();
        self.seen = self.facts.iter().map(|fact| fact.fact_id.clone()).collect();
        self.pending_revaluation.clear();
        self.last_event_time = Some(proof.complete_through_ms);
        self.generation = Some(proof.target_generation);
        self.generation_mismatch = false;
        self.last_revaluation_id = Some(proof.proof_id);
        self.trim();
        Ok(self.snapshot(proof.complete_through_ms))
    }

    pub fn snapshot(&self, now_ms: u64) -> RiskSnapshot {
        let Some(generation) = self.generation else {
            return RiskSnapshot {
                gate: RiskGate::GenerationMismatch,
                valuation_generation: None,
                risk_unit: self.risk_unit.clone(),
                rolling_loss: Decimal::ZERO,
                drawdown: Decimal::ZERO,
                loss_streak: 0,
                cooldown_until_ms: None,
            };
        };
        let loss_start = now_ms.saturating_sub(self.loss_window_ms);
        let drawdown_start = now_ms.saturating_sub(self.drawdown_window_ms);
        let rolling = self
            .facts
            .iter()
            .filter(|fact| fact.event_time_ms >= loss_start)
            .map(|fact| fact.realized_pnl)
            .sum::<Decimal>();
        let rolling_loss = (-rolling).max(Decimal::ZERO);
        let (drawdown, loss_streak) = drawdown_and_streak(
            self.facts
                .iter()
                .filter(|fact| fact.event_time_ms >= drawdown_start),
        );
        let risk_gate = if rolling_loss >= self.loss_window_limit {
            RiskGate::LossWindow
        } else if drawdown >= self.drawdown_limit {
            RiskGate::Drawdown
        } else if loss_streak >= self.max_loss_streak {
            RiskGate::LossStreak
        } else {
            RiskGate::Open
        };
        let cooldown_until_ms = self.cooldown_until.filter(|until| *until > now_ms);
        let gate = if risk_gate == RiskGate::Open && cooldown_until_ms.is_some() {
            RiskGate::Cooldown
        } else {
            risk_gate
        };
        RiskSnapshot {
            gate,
            valuation_generation: Some(generation),
            risk_unit: self.risk_unit.clone(),
            rolling_loss,
            drawdown,
            loss_streak,
            cooldown_until_ms,
        }
    }

    fn trim(&mut self) {
        if let Some(latest) = self.last_event_time {
            self.trim_at(latest);
        }
    }

    fn trim_at(&mut self, latest: u64) {
        let earliest = latest.saturating_sub(self.drawdown_window_ms.max(self.loss_window_ms));
        while self
            .facts
            .front()
            .is_some_and(|fact| fact.event_time_ms < earliest)
        {
            if let Some(fact) = self.facts.pop_front() {
                self.seen.remove(&fact.fact_id);
            }
        }
    }
}

fn drawdown_and_streak<'a>(facts: impl Iterator<Item = &'a RiskFact>) -> (Decimal, u32) {
    let mut equity = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut drawdown = Decimal::ZERO;
    let mut streak = 0;
    for fact in facts {
        equity += fact.realized_pnl;
        peak = peak.max(equity);
        drawdown = drawdown.max(peak - equity);
        if fact.realized_pnl < Decimal::ZERO {
            streak += 1;
        } else {
            streak = 0;
        }
    }
    (drawdown, streak)
}
