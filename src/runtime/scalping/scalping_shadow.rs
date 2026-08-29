use std::num::NonZeroUsize;

use crate::{
    controller::EntryAuthorization,
    domain::MarketEvent,
    exchange::binance,
    indicator::{FeatureBuildError, FeatureState, ScalpingFeatureBuilder},
    market::{BookError, OrderBook, RawMarketRecord},
    storage::{ScalpingEvidenceError, ScalpingEvidenceJournal},
    strategy::scalping::{
        CandidateEvidenceBundle, CandidatePreparation, RiskRevaluation, SafetyProjection,
        ScalpingDecision, ScalpingError, ScalpingParams, ScalpingStrategy, SemanticIntent,
        StrategyBinding, join_candidate_evidence, risk_revaluation_digest,
    },
};

use super::{PrivateFacts, ScalpingInput, ScalpingShadowCoordinator};

/// Result of a deterministic, recorded-data Shadow run. The result contains semantic proposals
/// only; this module never imports execution, risk mutation, private credentials, or a venue
/// mutation client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShadowReplayResult {
    pub processed_records: u64,
    pub preparations: Vec<CandidatePreparation>,
    pub intents: Vec<SemanticIntent>,
}

/// Shadow never derives account authority from public capture ordering. This context is supplied
/// by the caller from authoritative private reconciliation and the controller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShadowReplayContext {
    pub private: PrivateFacts,
    pub authorization: EntryAuthorization,
}

/// Replays normalized public captures through the first scalping strategy in Shadow. Each raw
/// record is parsed by the exchange boundary, features are built by indicator, then the strategy
/// sees only a versioned frame and anonymous authority projection.
pub fn replay_scalping_shadow(
    records: &[RawMarketRecord],
    expected_native_symbol: &str,
    binding: StrategyBinding,
    params: ScalpingParams,
    _safety: SafetyProjection,
) -> Result<ShadowReplayResult, ShadowReplayError> {
    replay_scalping_shadow_inner(
        records,
        expected_native_symbol,
        binding,
        params,
        &[],
        None,
        None,
    )
}

/// The evidence tape is a read-only replay input from independent calibration, cost, and risk
/// owners. A separate complete risk revaluation is required before replay may prepare or admit
/// any candidate; this compatibility entry point therefore always remains fail-closed.
pub fn replay_scalping_shadow_with_evidence(
    records: &[RawMarketRecord],
    expected_native_symbol: &str,
    binding: StrategyBinding,
    params: ScalpingParams,
    _safety: SafetyProjection,
    evidence_tape: &[CandidateEvidenceBundle],
) -> Result<ShadowReplayResult, ShadowReplayError> {
    replay_scalping_shadow_inner(
        records,
        expected_native_symbol,
        binding,
        params,
        evidence_tape,
        None,
        None,
    )
}

/// Replays Shadow input with a complete, externally produced risk revaluation. The replay first
/// forces the strategy ledger into GenerationMismatch and then accepts only this exact proof, so
/// an empty local ledger can never be treated as an implicit open-risk snapshot.
pub fn replay_scalping_shadow_with_evidence_and_risk_revaluation(
    records: &[RawMarketRecord],
    expected_native_symbol: &str,
    binding: StrategyBinding,
    params: ScalpingParams,
    _safety: SafetyProjection,
    evidence_tape: &[CandidateEvidenceBundle],
    risk_revaluation: &RiskRevaluation,
) -> Result<ShadowReplayResult, ShadowReplayError> {
    replay_scalping_shadow_inner(
        records,
        expected_native_symbol,
        binding,
        params,
        evidence_tape,
        Some(risk_revaluation),
        None,
    )
}

pub fn replay_scalping_shadow_with_context(
    records: &[RawMarketRecord],
    expected_native_symbol: &str,
    binding: StrategyBinding,
    params: ScalpingParams,
    evidence_tape: &[CandidateEvidenceBundle],
    risk_revaluation: &RiskRevaluation,
    context: &ShadowReplayContext,
) -> Result<ShadowReplayResult, ShadowReplayError> {
    replay_scalping_shadow_inner(
        records,
        expected_native_symbol,
        binding,
        params,
        evidence_tape,
        Some(risk_revaluation),
        Some(context),
    )
}

fn replay_scalping_shadow_inner(
    records: &[RawMarketRecord],
    expected_native_symbol: &str,
    binding: StrategyBinding,
    params: ScalpingParams,
    evidence_tape: &[CandidateEvidenceBundle],
    risk_revaluation: Option<&RiskRevaluation>,
    context: Option<&ShadowReplayContext>,
) -> Result<ShadowReplayResult, ShadowReplayError> {
    let capacity = NonZeroUsize::new(2_048).ok_or(ShadowReplayError::Capacity)?;
    let mut features = ScalpingFeatureBuilder::new(
        params.feature_profile.clone(),
        params.feature_digest.clone(),
        params.max_data_age_ms,
        capacity,
    )?;
    let mut strategy = ScalpingStrategy::new(binding, params)?;
    let risk_proof_version = risk_revaluation.map(risk_revaluation_digest).transpose()?;
    if let Some(proof) = risk_revaluation {
        strategy.require_risk_revaluation(proof.complete_through_ms)?;
        strategy.apply_risk_revaluation(proof.clone())?;
    }
    let mut coordinator = ScalpingShadowCoordinator::new(strategy);
    if let Some(context) = context {
        coordinator.process(vec![ScalpingInput::Private(context.private.clone())])?;
    }
    let mut book = OrderBook::default();
    let mut processed_records = 0_u64;
    let mut previous_sequence = 0_u64;
    let mut preparations = Vec::new();
    let mut intents = Vec::new();
    let mut pending_preparation = None;

    for record in records {
        if record.capture_sequence != previous_sequence.saturating_add(1) {
            return Err(ShadowReplayError::Sequence(record.capture_sequence));
        }
        previous_sequence = record.capture_sequence;
        let event = binance::normalize(record, expected_native_symbol).map_err(|source| {
            ShadowReplayError::Normalize {
                sequence: record.capture_sequence,
                source,
            }
        })?;
        match event {
            MarketEvent::Snapshot(snapshot) => {
                book.apply_snapshot(snapshot);
                features.ingest_book(&book, record.received_at_ms)?;
            }
            MarketEvent::Delta(delta) => {
                book.apply_delta(delta)
                    .map_err(|source| ShadowReplayError::Book {
                        sequence: record.capture_sequence,
                        source,
                    })?;
                features.ingest_book(&book, record.received_at_ms)?;
            }
            MarketEvent::Trade(trade) => {
                features.ingest_trade(&trade)?;
                let frame = features.frame(record.received_at_ms)?;
                if frame.state == FeatureState::Ready
                    && risk_proof_version.is_some()
                    && context.is_some()
                {
                    let context = context.ok_or(ShadowReplayError::PrivateContext)?;
                    let evidence = pending_preparation.as_ref().map_or_else(
                        || Ok(Vec::new()),
                        |preparation| {
                            joined_evidence(
                                preparation,
                                evidence_tape,
                                risk_proof_version.as_deref().unwrap_or_default(),
                                risk_revaluation,
                                frame.watermark_ms,
                            )
                        },
                    )?;
                    let output = coordinator
                        .process(vec![ScalpingInput::Market {
                            frame: Box::new(frame),
                            decision_at_ms: record.received_at_ms,
                            authorization: context.authorization.clone(),
                            evidence,
                        }])?
                        .into_iter()
                        .next()
                        .ok_or(ShadowReplayError::CoordinatorOutput)?;
                    if let Some(preparation) = output.preparation {
                        preparations.push(preparation.clone());
                        pending_preparation = Some(preparation);
                    }
                    if let Some(ScalpingDecision::Intent(intent)) = output.decision {
                        pending_preparation = None;
                        intents.push(*intent);
                    }
                }
            }
            MarketEvent::Bar(bar) => features.ingest_bar(&bar)?,
            MarketEvent::Ticker(_) | MarketEvent::MarkFunding(_) => {}
        }
        processed_records = processed_records.saturating_add(1);
    }
    Ok(ShadowReplayResult {
        processed_records,
        preparations,
        intents,
    })
}

fn joined_evidence(
    preparation: &CandidatePreparation,
    evidence_tape: &[CandidateEvidenceBundle],
    risk_proof_version: &str,
    risk_revaluation: Option<&RiskRevaluation>,
    observed_at_ms: u64,
) -> Result<Vec<crate::strategy::scalping::CandidateEvidence>, ShadowReplayError> {
    let mut joined = Vec::new();
    for bundle in evidence_tape.iter().filter(|bundle| {
        bundle.calibration.identity.preparation_id == preparation.preparation_id
            && risk_evidence_matches_revaluation(bundle, risk_proof_version, risk_revaluation)
    }) {
        let candidate = preparation
            .candidates
            .iter()
            .find(|candidate| candidate.intent_id == bundle.calibration.identity.candidate_id)
            .ok_or(ScalpingError::Evidence)?;
        joined.push(join_candidate_evidence(
            preparation,
            candidate,
            bundle,
            observed_at_ms,
        )?);
    }
    Ok(joined)
}

/// The risk proof version is durable-input identity, not an execution credential. It binds the
/// immutable proof content to the risk projection carried by each journaled evidence bundle.
fn risk_evidence_matches_revaluation(
    bundle: &CandidateEvidenceBundle,
    proof_version: &str,
    proof: Option<&RiskRevaluation>,
) -> bool {
    proof.is_some_and(|proof| {
        bundle.risk.identity.release_digest == proof_version
            && bundle.risk.identity.producer_generation == proof.target_generation
    })
}

/// Recovers a durable, read-only Shadow evidence tape before replaying it. Recovery failures are
/// fatal because an incomplete tape cannot prove the provenance of a proposed intent.
pub fn replay_scalping_shadow_with_journal(
    records: &[RawMarketRecord],
    expected_native_symbol: &str,
    binding: StrategyBinding,
    params: ScalpingParams,
    safety: SafetyProjection,
    journal: &ScalpingEvidenceJournal,
) -> Result<ShadowReplayResult, ShadowReplayError> {
    let evidence_records = journal.recover()?;
    let tape: Vec<_> = evidence_records
        .into_iter()
        .map(|record| record.bundle)
        .collect();
    replay_scalping_shadow_with_evidence(
        records,
        expected_native_symbol,
        binding,
        params,
        safety,
        &tape,
    )
}

/// Recovers a journaled evidence tape only when its risk projection explicitly names the same
/// complete revaluation proof used to arm the local Shadow ledger.
pub fn replay_scalping_shadow_with_journal_and_risk_revaluation(
    records: &[RawMarketRecord],
    expected_native_symbol: &str,
    binding: StrategyBinding,
    params: ScalpingParams,
    safety: SafetyProjection,
    journal: &ScalpingEvidenceJournal,
    risk_revaluation: &RiskRevaluation,
) -> Result<ShadowReplayResult, ShadowReplayError> {
    let evidence_records = journal.recover()?;
    let tape: Vec<_> = evidence_records
        .into_iter()
        .map(|record| record.bundle)
        .collect();
    replay_scalping_shadow_with_evidence_and_risk_revaluation(
        records,
        expected_native_symbol,
        binding,
        params,
        safety,
        &tape,
        risk_revaluation,
    )
}

#[derive(Debug, thiserror::Error)]
pub enum ShadowReplayError {
    #[error("shadow replay input has a non-contiguous capture sequence at {0}")]
    Sequence(u64),
    #[error("shadow replay internal capacity is invalid")]
    Capacity,
    #[error("shadow replay could not normalize capture {sequence}: {source}")]
    Normalize {
        sequence: u64,
        source: binance::BinanceError,
    },
    #[error("shadow replay order book rejected capture {sequence}: {source}")]
    Book { sequence: u64, source: BookError },
    #[error("shadow replay feature construction failed: {0}")]
    Feature(#[from] FeatureBuildError),
    #[error("shadow replay strategy evaluation failed: {0}")]
    Strategy(#[from] ScalpingError),
    #[error("shadow replay evidence recovery failed: {0}")]
    EvidenceStorage(#[from] ScalpingEvidenceError),
    #[error("shadow replay coordinator rejected its private or market context: {0}")]
    Coordinator(#[from] super::ScalpingCoordinatorError),
    #[error("shadow replay coordinator produced no output")]
    CoordinatorOutput,
    #[error("shadow replay requires explicit private reconciliation context")]
    PrivateContext,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::{
        controller::{
            CONTROL_SCHEMA_VERSION, ControlAuthority, ControlTarget, InstanceControlRecord,
        },
        domain::{Amount, Asset},
        market::{RawMarketRecord, RawSource},
        runtime::{CustodyStatus, PrivateFacts, ScalpingCoordinatorError},
        storage::ScalpingEvidenceJournal,
        strategy::scalping::{
            CalibrationEvidence, CandidateEvidenceBundle, CandidatePreparation, CostEvidence,
            EvidenceIdentity, ExposureState, ProtectionState, RiskEvidence,
        },
    };

    use super::*;

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding {
            strategy_kind: crate::strategy::scalping::StrategyKind::Scalping,
            strategy_instance_id: "scalping_primary".to_owned(),
            run_id: "shadow_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "primary".to_owned(),
            symbol: "BTC/USDT".parse()?,
            parameter_release_id: "scalping-shadow-v1".to_owned(),
            owner_scope: "scalping_primary:shadow_1".to_owned(),
            risk_budget: Amount::new("USDT".parse::<Asset>()?, Decimal::new(5, 0)),
        })
    }

    fn safety() -> SafetyProjection {
        SafetyProjection {
            private_snapshot_ready: true,
            exposure: ExposureState::Flat,
            execution_unknown: false,
            protection: ProtectionState::Complete,
            owner_conflict: false,
            risk_budget_available: true,
        }
    }

    fn context(
        binding: &StrategyBinding,
        private_generation: u64,
        authorization_generation: u64,
    ) -> ShadowReplayContext {
        let private = PrivateFacts {
            generation: private_generation,
            observed_at_ms: 1,
            root_cause_fact_id: format!("shadow-replay-private:{private_generation}:1"),
            safety: safety(),
            custody: CustodyStatus::Complete,
        };
        let record = InstanceControlRecord {
            schema_version: CONTROL_SCHEMA_VERSION,
            binding: binding.clone(),
            target: ControlTarget::Running,
            command_id: "shadow-replay-control".to_owned(),
            idempotency_key: "shadow-replay-control-1".to_owned(),
            safety_deadline_ms: None,
            revision: 1,
        };
        let authorization = record.authorize(
            &ControlAuthority {
                generation: authorization_generation,
                parameter_release_id: binding.parameter_release_id.clone(),
                private_snapshot_ready: true,
                execution_unknown: false,
                protection_complete: true,
                owner_conflict: false,
            },
            private.observed_at_ms,
        );
        ShadowReplayContext {
            private,
            authorization,
        }
    }

    fn records() -> Result<Vec<RawMarketRecord>, Box<dyn std::error::Error>> {
        let symbol: crate::domain::Symbol = "BTC/USDT".parse()?;
        let mut records = Vec::new();
        let mut capture_sequence = 1_u64;
        let mut snapshot = RawMarketRecord::new(
            RawSource::RestSnapshot,
            symbol.clone(),
            1,
            100,
            r#"{"lastUpdateId":10,"bids":[["99.997","101"]],"asks":[["100.003","1"]]}"#.to_owned(),
        )?;
        snapshot.capture_sequence = capture_sequence;
        records.push(snapshot);

        let mut last_close_time_ms = 0_u64;
        for index in 1_u64..=21 {
            let open_time_ms = index.saturating_mul(60_000);
            let close_time_ms = open_time_ms.saturating_add(59_999);
            last_close_time_ms = close_time_ms;
            capture_sequence = capture_sequence.saturating_add(1);
            let mut bar = RawMarketRecord::new(
                RawSource::WebSocketKline,
                symbol.clone(),
                1,
                close_time_ms,
                format!(
                    r#"{{"e":"kline","E":{close_time_ms},"s":"BTCUSDT","st":1,"k":{{"t":{open_time_ms},"T":{close_time_ms},"s":"BTCUSDT","i":"1m","o":"100","h":"101","l":"99","c":"100","x":true}}}}"#
                ),
            )?;
            bar.capture_sequence = capture_sequence;
            records.push(bar);
        }

        capture_sequence = capture_sequence.saturating_add(1);
        let mut current_book = RawMarketRecord::new(
            RawSource::RestSnapshot,
            symbol.clone(),
            1,
            last_close_time_ms.saturating_add(1),
            r#"{"lastUpdateId":11,"bids":[["99.997","100"]],"asks":[["100.003","1"]]}"#.to_owned(),
        )?;
        current_book.capture_sequence = capture_sequence;
        records.push(current_book);

        // The extra post-warmup observation lets admission prove feature progress after the
        // candidate's preparation frame.
        for trade_id in 1_u64..=65 {
            capture_sequence = capture_sequence.saturating_add(1);
            let received_at_ms = last_close_time_ms
                .saturating_add(trade_id)
                .saturating_add(1);
            let mut trade = RawMarketRecord::new(
                RawSource::WebSocketTrade,
                symbol.clone(),
                1,
                received_at_ms,
                format!(
                    r#"{{"e":"aggTrade","E":{received_at_ms},"s":"BTCUSDT","a":{trade_id},"p":"99.999","q":"1","nq":"99.999","f":{trade_id},"l":{trade_id},"T":{received_at_ms},"m":true,"st":1}}"#
                ),
            )?;
            trade.capture_sequence = capture_sequence;
            records.push(trade);
        }
        Ok(records)
    }

    fn identity(kind: &str, preparation: &CandidatePreparation) -> EvidenceIdentity {
        EvidenceIdentity {
            schema_version: 1,
            evidence_id: format!("{kind}-shadow-1"),
            candidate_id: preparation.candidates[0].intent_id.clone(),
            preparation_id: preparation.preparation_id.clone(),
            binding_digest: preparation.binding_digest.clone(),
            frame_generation: preparation.frame_generation,
            watermark_ms: preparation.watermark_ms,
            producer_generation: 1,
            release_digest: if kind == "calibration" {
                "0".repeat(64)
            } else {
                "a".repeat(64)
            },
            valid_until_ms: preparation.valid_until_ms,
        }
    }

    fn risk_revaluation() -> RiskRevaluation {
        RiskRevaluation {
            proof_id: "shadow-risk-revaluation-1".to_owned(),
            target_generation: 7,
            risk_unit: crate::strategy::scalping::RiskUnit::shadow(),
            window_start_ms: 0,
            complete_through_ms: 2_000_000,
            source_fact_ids: Vec::new(),
            revalued_facts: Vec::new(),
        }
    }

    fn evidence(
        preparation: &CandidatePreparation,
        revaluation: &RiskRevaluation,
    ) -> Result<CandidateEvidenceBundle, ShadowReplayError> {
        let mut risk_identity = identity("risk", preparation);
        risk_identity.producer_generation = revaluation.target_generation;
        risk_identity.release_digest = risk_revaluation_digest(revaluation)?;
        Ok(CandidateEvidenceBundle {
            calibration: CalibrationEvidence {
                identity: identity("calibration", preparation),
                model_version: "scalping-shadow-calibration-v1".to_owned(),
                fill_distribution: vec![crate::strategy::scalping::FillSlice {
                    fill_ratio: Decimal::ONE,
                    probability: Decimal::ONE,
                }],
                outcomes: crate::strategy::scalping::OutcomeProbabilities {
                    target: Decimal::ONE,
                    stop: Decimal::ZERO,
                    other: Decimal::ZERO,
                },
                target_pnl_bps: Decimal::new(10, 0),
                stop_pnl_bps: -Decimal::ONE,
                other_pnl_bps: Decimal::ZERO,
                uncertainty_bps: Decimal::ONE,
            },
            costs: CostEvidence {
                identity: identity("cost", preparation),
                entry_cost_bps: Decimal::ONE,
                exit_cost_bps: Decimal::ONE,
                funding_cost_bps: Decimal::ZERO,
                nonfill_cost_bps: Decimal::ZERO,
                opportunity_cost_bps: Decimal::ZERO,
            },
            risk: RiskEvidence {
                identity: risk_identity,
                policy_digest: "b".repeat(64),
                worst_loss: preparation.candidates[0].risk_plan.risk_per_episode.clone(),
                admissible: true,
            },
        })
    }

    #[test]
    fn replay_without_risk_revaluation_fails_closed_before_preparation()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let params = ScalpingParams::shadow(binding.risk_budget.clone());
        let first = replay_scalping_shadow(
            &records()?,
            "BTCUSDT",
            binding.clone(),
            params.clone(),
            safety(),
        )?;
        let second = replay_scalping_shadow(&records()?, "BTCUSDT", binding, params, safety())?;
        assert_eq!(first, second);
        assert_eq!(first.processed_records, 88);
        assert!(first.preparations.is_empty());
        assert!(first.intents.is_empty());
        Ok(())
    }

    #[test]
    fn replay_with_complete_risk_revaluation_prepares_and_admits_matching_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let params = ScalpingParams::shadow(binding.risk_budget.clone());
        let revaluation = risk_revaluation();
        let context = context(&binding, 7, 7);
        let prepared = replay_scalping_shadow_with_context(
            &records()?,
            "BTCUSDT",
            binding.clone(),
            params.clone(),
            &[],
            &revaluation,
            &context,
        )?;
        assert_eq!(prepared.preparations.len(), 1);
        let tape = vec![evidence(&prepared.preparations[0], &revaluation)?];
        let unproved = replay_scalping_shadow_with_evidence(
            &records()?,
            "BTCUSDT",
            binding.clone(),
            params.clone(),
            safety(),
            &tape,
        )?;
        assert!(unproved.preparations.is_empty());
        assert!(unproved.intents.is_empty());
        let replay = replay_scalping_shadow_with_context(
            &records()?,
            "BTCUSDT",
            binding,
            params,
            &tape,
            &revaluation,
            &context,
        )?;
        assert_eq!(replay.intents.len(), 1);
        assert_eq!(replay.intents[0].target_quote.value, Decimal::new(5, 0));
        Ok(())
    }

    #[test]
    fn durable_evidence_journal_requires_the_explicit_risk_proof_version()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let params = ScalpingParams::shadow(binding.risk_budget.clone());
        let revaluation = risk_revaluation();
        let context = context(&binding, 7, 7);
        let prepared = replay_scalping_shadow_with_context(
            &records()?,
            "BTCUSDT",
            binding.clone(),
            params.clone(),
            &[],
            &revaluation,
            &context,
        )?;
        let directory = tempdir()?;
        let mut journal = ScalpingEvidenceJournal::open(directory.path().join("evidence.jsonl"))?;
        let _ = journal.append(evidence(&prepared.preparations[0], &revaluation)?)?;
        let tape: Vec<_> = journal
            .recover()?
            .into_iter()
            .map(|record| record.bundle)
            .collect();
        let replay = replay_scalping_shadow_with_context(
            &records()?,
            "BTCUSDT",
            binding,
            params,
            &tape,
            &revaluation,
            &context,
        )?;
        assert_eq!(replay.intents.len(), 1);
        Ok(())
    }

    #[test]
    fn durable_evidence_journal_does_not_accept_a_different_risk_proof_version()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let params = ScalpingParams::shadow(binding.risk_budget.clone());
        let revaluation = risk_revaluation();
        let context = context(&binding, 7, 7);
        let prepared = replay_scalping_shadow_with_context(
            &records()?,
            "BTCUSDT",
            binding.clone(),
            params.clone(),
            &[],
            &revaluation,
            &context,
        )?;
        let directory = tempdir()?;
        let mut journal = ScalpingEvidenceJournal::open(directory.path().join("evidence.jsonl"))?;
        let _ = journal.append(evidence(&prepared.preparations[0], &revaluation)?)?;
        let different_revaluation = RiskRevaluation {
            proof_id: "shadow-risk-revaluation-2".to_owned(),
            ..revaluation
        };
        let tape: Vec<_> = journal
            .recover()?
            .into_iter()
            .map(|record| record.bundle)
            .collect();
        let replay = replay_scalping_shadow_with_context(
            &records()?,
            "BTCUSDT",
            binding,
            params,
            &tape,
            &different_revaluation,
            &context,
        )?;
        assert_eq!(replay.preparations.len(), 1);
        assert!(replay.intents.is_empty());
        Ok(())
    }

    #[test]
    fn complete_risk_proof_without_private_context_stays_fenced()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let params = ScalpingParams::shadow(binding.risk_budget.clone());
        let revaluation = risk_revaluation();
        let replay = replay_scalping_shadow_with_evidence_and_risk_revaluation(
            &records()?,
            "BTCUSDT",
            binding,
            params,
            safety(),
            &[],
            &revaluation,
        )?;
        assert!(replay.preparations.is_empty());
        assert!(replay.intents.is_empty());
        Ok(())
    }

    #[test]
    fn explicit_private_generation_must_match_controller_authorization()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let params = ScalpingParams::shadow(binding.risk_budget.clone());
        let revaluation = risk_revaluation();
        let result = replay_scalping_shadow_with_context(
            &records()?,
            "BTCUSDT",
            binding.clone(),
            params,
            &[],
            &revaluation,
            &context(&binding, 7, 8),
        );
        assert!(matches!(
            result,
            Err(ShadowReplayError::Coordinator(
                ScalpingCoordinatorError::Generation
            ))
        ));
        Ok(())
    }
}
