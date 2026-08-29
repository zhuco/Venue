#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rust_decimal::Decimal;
    use tempfile::tempdir;

    use crate::{
        controller::{ControlAuthority, ControlTarget, InstanceControlRecord},
        domain::{Amount, Asset, Price},
        indicator::{
            BARS_SOURCE, BOOK_SOURCE, FeatureFrame, FeatureState, FeatureValues, SourceCursor,
            TRADES_SOURCE,
        },
        strategy::scalping::{
            BlockingReason, CandidateCosts, CandidateEvidence, CandidatePreparation, Direction,
            EntryStyle, EpisodeState, Expert, ExposureState, FillSlice, MarketRegime, NoopReason,
            OutcomeProbabilities, ProtectionState, SafetyProjection, ScalpingCheckpointStore,
            ScalpingDecision, ScalpingParams, ScalpingState, StrategyBinding, StrategyKind,
        },
    };

    use super::{ScalpingStrategy, classify_regime};

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding {
            strategy_kind: StrategyKind::Scalping,
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

    fn strategy() -> Result<ScalpingStrategy, Box<dyn std::error::Error>> {
        let binding = binding()?;
        let params = ScalpingParams::shadow(binding.risk_budget.clone());
        Ok(ScalpingStrategy::new(binding, params)?)
    }

    fn frame(watermark_ms: u64) -> Result<FeatureFrame, Box<dyn std::error::Error>> {
        Ok(FeatureFrame {
            symbol: "BTC/USDT".parse()?,
            schema_version: 1,
            generation: 1,
            watermark_ms,
            state: FeatureState::Ready,
            cursors: [BOOK_SOURCE, TRADES_SOURCE, BARS_SOURCE]
                .into_iter()
                .enumerate()
                .map(|(index, source)| {
                    (
                        source.to_owned(),
                        SourceCursor {
                            generation: 1,
                            sequence: watermark_ms + index as u64,
                            event_time_ms: watermark_ms,
                            fresh: true,
                        },
                    )
                })
                .collect(),
            feature_versions: BTreeMap::from([
                (BOOK_SOURCE.to_owned(), "book-v1".to_owned()),
                (TRADES_SOURCE.to_owned(), "trades-v1".to_owned()),
                (BARS_SOURCE.to_owned(), "bars-v1".to_owned()),
                (
                    "_feature_profile".to_owned(),
                    "scalping-shadow-v1".to_owned(),
                ),
                ("_feature_profile_digest".to_owned(), "0".repeat(64)),
            ]),
            values: FeatureValues {
                mid_price: Price::new(Decimal::new(99, 0))?,
                fair_price: Price::new(Decimal::new(100, 0))?,
                spread_bps: Decimal::new(5, 1),
                depth_quote: Decimal::new(1_000, 0),
                book_imbalance: Decimal::ONE,
                trade_imbalance: Decimal::ONE,
                short_return_bps: Decimal::ZERO,
                trend_efficiency: Decimal::ZERO,
                bandwidth_expansion: Decimal::ZERO,
                expected_move_bps: Decimal::ZERO,
                toxicity: Decimal::ZERO,
            },
            breakout: None,
        })
    }

    fn safe() -> SafetyProjection {
        SafetyProjection {
            private_snapshot_ready: true,
            exposure: ExposureState::Flat,
            execution_unknown: false,
            protection: ProtectionState::Complete,
            owner_conflict: false,
            risk_budget_available: true,
        }
    }

    fn authorization(
        projection: &SafetyProjection,
        generation: u64,
    ) -> Result<crate::controller::EntryAuthorization, Box<dyn std::error::Error>> {
        let binding = binding()?;
        let record = InstanceControlRecord {
            schema_version: 1,
            binding: binding.clone(),
            target: ControlTarget::Running,
            command_id: "start_shadow".to_owned(),
            idempotency_key: "start_shadow_1".to_owned(),
            safety_deadline_ms: None,
            revision: 1,
        };
        Ok(record.authorize(
            &ControlAuthority {
                generation,
                parameter_release_id: binding.parameter_release_id,
                private_snapshot_ready: projection.private_snapshot_ready,
                execution_unknown: projection.execution_unknown,
                protection_complete: projection.protection == ProtectionState::Complete,
                owner_conflict: projection.owner_conflict,
            },
            1,
        ))
    }

    fn evidence(preparation: &CandidatePreparation) -> CandidateEvidence {
        CandidateEvidence {
            candidate_id: preparation.candidates[0].intent_id.clone(),
            preparation_id: preparation.preparation_id.clone(),
            binding_digest: preparation.binding_digest.clone(),
            frame_generation: preparation.frame_generation,
            watermark_ms: preparation.watermark_ms,
            valid_until_ms: preparation.valid_until_ms,
            calibration_model_version: "scalping-shadow-calibration-v1".to_owned(),
            calibration_digest: "0".repeat(64),
            cost_digest: "b".repeat(64),
            risk_digest: "c".repeat(64),
            worst_loss: preparation.candidates[0].risk_plan.risk_per_episode.clone(),
            fill_probability: Decimal::ONE,
            fill_distribution: vec![FillSlice {
                fill_ratio: Decimal::ONE,
                probability: Decimal::ONE,
            }],
            outcomes: OutcomeProbabilities {
                target: Decimal::ONE,
                stop: Decimal::ZERO,
                other: Decimal::ZERO,
            },
            costs: CandidateCosts {
                entry_cost_bps: Decimal::ZERO,
                exit_cost_bps: Decimal::ZERO,
                funding_cost_bps: Decimal::ZERO,
                nonfill_cost_bps: Decimal::ZERO,
                opportunity_cost_bps: Decimal::ZERO,
            },
            target_pnl_bps: Decimal::ONE,
            stop_pnl_bps: -Decimal::ONE,
            other_pnl_bps: Decimal::ZERO,
            outcome_expected_value_bps: Decimal::ONE,
            net_expected_value_bps: Decimal::ONE,
            uncertainty_bps: Decimal::ZERO,
            admissible: true,
        }
    }

    #[test]
    fn shadow_produces_only_semantic_intent_and_needs_ack_before_next_entry()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        let projection = safe();
        let initial_authorization = authorization(&projection, 1)?;
        let first = strategy.evaluate(&frame(100)?, &projection, &initial_authorization)?;
        let _preparation = match first {
            ScalpingDecision::Prepared(preparation) => preparation,
            ScalpingDecision::Noop(reason) => {
                return Err(format!("unexpected noop: {reason:?}").into());
            }
            ScalpingDecision::Intent(_) => return Err("intent bypassed evidence admission".into()),
        };
        assert!(matches!(
            strategy.admit(&[], 100)?,
            ScalpingDecision::Noop(NoopReason::EvidenceUnavailable)
        ));
        let preparation =
            match strategy.evaluate(&frame(101)?, &projection, &initial_authorization)? {
                ScalpingDecision::Prepared(preparation) => preparation,
                decision => return Err(format!("unexpected decision: {decision:?}").into()),
            };
        let intent = match strategy.admit(&[evidence(&preparation)], 101)? {
            ScalpingDecision::Intent(intent) => intent,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        assert_eq!(intent.symbol.to_string(), "BTC/USDT");
        assert_eq!(intent.target_quote.value, Decimal::new(5, 0));
        assert_eq!(intent.risk_plan.quote_cap.value, Decimal::new(100, 0));
        assert_eq!(intent.risk_plan.risk_per_episode.value, Decimal::ONE);
        assert_eq!(intent.risk_plan.max_episode_loss.value, Decimal::ONE);
        assert_eq!(intent.risk_plan.risk_per_episode.unit.as_str(), "risk");
        assert!(intent.requires_server_protection);
        assert!(matches!(
            strategy.evaluate(&frame(102)?, &projection, &initial_authorization)?,
            ScalpingDecision::Noop(NoopReason::ActiveEpisode)
        ));
        strategy.acknowledge_shadow_intent(&intent.intent_id, 102)?;
        assert!(matches!(
            strategy.evaluate(&frame(103)?, &projection, &initial_authorization)?,
            ScalpingDecision::Noop(NoopReason::Cooldown)
        ));
        Ok(())
    }

    #[test]
    fn direct_admission_needs_no_quote_risk_calibration_or_bundle()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        let projection = safe();
        let authorization = authorization(&projection, 1)?;
        assert!(matches!(
            strategy.evaluate(&frame(100)?, &projection, &authorization)?,
            ScalpingDecision::Prepared(_)
        ));

        let intent = match strategy.admit_direct(100)? {
            ScalpingDecision::Intent(intent) => intent,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        assert!(
            strategy
                .episode()
                .is_some_and(|episode| episode.frozen_evidence.is_none())
        );
        assert_eq!(intent.symbol.to_string(), "BTC/USDT");
        strategy.confirm_live_entry(&intent.intent_id, 101)?;
        strategy.confirm_live_entry(&intent.intent_id, 102)?;
        assert!(
            strategy
                .episode()
                .is_some_and(|episode| episode.state == EpisodeState::Open)
        );
        assert!(matches!(
            strategy.evaluate(&frame(102)?, &projection, &authorization)?,
            ScalpingDecision::Noop(NoopReason::ActiveEpisode)
        ));
        Ok(())
    }

    #[test]
    fn reconciled_no_fill_replay_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        let projection = safe();
        let authorization = authorization(&projection, 1)?;
        let _ = strategy.evaluate(&frame(100)?, &projection, &authorization)?;
        let intent = match strategy.admit_direct(100)? {
            ScalpingDecision::Intent(intent) => intent,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };

        strategy.reject_reserved_entry(&intent.intent_id, 101)?;
        strategy.reject_reserved_entry(&intent.intent_id, 102)?;
        assert!(matches!(strategy.state(), ScalpingState::Cooldown { .. }));
        Ok(())
    }

    #[test]
    fn unsafe_private_projection_fails_closed_without_an_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        let mut unsafe_projection = safe();
        unsafe_projection.execution_unknown = true;
        let authorization = authorization(&safe(), 1)?;
        assert_eq!(
            strategy.evaluate(&frame(100)?, &unsafe_projection, &authorization)?,
            ScalpingDecision::Noop(NoopReason::Blocked(BlockingReason::ExecutionUnknown))
        );
        Ok(())
    }

    #[test]
    fn stale_rejected_or_duplicate_evidence_never_reserves_a_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let projection = safe();
        let authorization = authorization(&projection, 1)?;

        let mut duplicate_case = strategy()?;
        let preparation =
            match duplicate_case.evaluate(&frame(100)?, &projection, &authorization)? {
                ScalpingDecision::Prepared(preparation) => preparation,
                decision => return Err(format!("unexpected decision: {decision:?}").into()),
            };
        let item = evidence(&preparation);
        assert!(matches!(
            duplicate_case.admit(&[item.clone(), item], 100),
            Err(crate::strategy::scalping::ScalpingError::Evidence)
        ));

        let mut stale_case = strategy()?;
        let preparation = match stale_case.evaluate(&frame(100)?, &projection, &authorization)? {
            ScalpingDecision::Prepared(preparation) => preparation,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        let mut stale = evidence(&preparation);
        stale.valid_until_ms = 100;
        assert!(matches!(
            stale_case.admit(&[stale], 101)?,
            ScalpingDecision::Noop(NoopReason::EvidenceUnavailable)
        ));

        let mut rejected_case = strategy()?;
        let preparation = match rejected_case.evaluate(&frame(100)?, &projection, &authorization)? {
            ScalpingDecision::Prepared(preparation) => preparation,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        let mut rejected = evidence(&preparation);
        rejected.admissible = false;
        assert!(matches!(
            rejected_case.admit(&[rejected], 100)?,
            ScalpingDecision::Noop(NoopReason::EvidenceUnavailable)
        ));
        Ok(())
    }

    #[test]
    fn trend_expert_requires_regime_dwell_before_preparing_a_candidate()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        let projection = safe();
        let authorization = authorization(&projection, 1)?;
        let mut first = frame(100)?;
        first.values.short_return_bps = Decimal::new(-10, 0);
        first.values.trend_efficiency = Decimal::ONE;
        assert!(matches!(
            strategy.evaluate(&first, &projection, &authorization)?,
            ScalpingDecision::Noop(NoopReason::NoSignal)
        ));
        let mut settled = frame(2_100)?;
        settled.values.short_return_bps = Decimal::new(-10, 0);
        settled.values.trend_efficiency = Decimal::ONE;
        let preparation = match strategy.evaluate(&settled, &projection, &authorization)? {
            ScalpingDecision::Prepared(preparation) => preparation,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        assert_eq!(preparation.candidates.len(), 2);
        assert!(
            preparation
                .candidates
                .iter()
                .all(|candidate| candidate.expert == Expert::TrendPullback)
        );
        assert!(
            preparation
                .candidates
                .iter()
                .any(|candidate| { candidate.entry_style == EntryStyle::PassiveMaker })
        );
        assert!(
            preparation
                .candidates
                .iter()
                .any(|candidate| { candidate.entry_style == EntryStyle::MarketableLimit })
        );
        Ok(())
    }

    #[test]
    fn delayed_or_ambiguous_regime_frames_cannot_prepare_candidates()
    -> Result<(), Box<dyn std::error::Error>> {
        let projection = safe();
        let authorization = authorization(&projection, 1)?;

        let mut delayed = strategy()?;
        assert_eq!(
            delayed.evaluate_at(&frame(100)?, &projection, &authorization, 351)?,
            ScalpingDecision::Noop(NoopReason::DecisionExpired)
        );

        let binding = binding()?;
        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        params.regime_confidence_margin = Decimal::new(3, 1);
        let mut ambiguous = ScalpingStrategy::new(binding, params)?;
        let mut low_range_confidence = frame(100)?;
        low_range_confidence.values.trend_efficiency = Decimal::new(5, 1);
        assert_eq!(
            ambiguous.evaluate(&low_range_confidence, &projection, &authorization)?,
            ScalpingDecision::Noop(NoopReason::RegimeAmbiguous)
        );
        assert!(
            ambiguous
                .select_candidates(&frame(100)?, Decimal::ZERO, MarketRegime::RegimeUnknown)
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn regime_confidence_is_reachable_and_regime_specific() -> Result<(), Box<dyn std::error::Error>>
    {
        let range = strategy()?;
        let mut frame = frame(100)?;
        frame.values.trend_efficiency = Decimal::new(5, 1);
        assert!(!range.regime_is_admissible(&frame, MarketRegime::Range));
        frame.values.trend_efficiency = Decimal::new(4, 1);
        assert!(range.regime_is_admissible(&frame, MarketRegime::Range));

        let binding = binding()?;
        let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
        params.trend_threshold = Decimal::new(2, 1);
        let trend_params = params.clone();
        let trend = ScalpingStrategy::new(binding, params)?;
        frame.values.trend_efficiency = Decimal::new(5, 1);
        assert_eq!(
            classify_regime(&frame, &trend_params),
            MarketRegime::TrendUp
        );
        assert!(!trend.regime_is_admissible(&frame, MarketRegime::TrendUp));
        frame.values.trend_efficiency = Decimal::new(6, 1);
        assert!(trend.regime_is_admissible(&frame, MarketRegime::TrendUp));
        assert!(!trend.regime_is_admissible(&frame, MarketRegime::Shock));
        assert!(!trend.regime_is_admissible(&frame, MarketRegime::RegimeUnknown));
        Ok(())
    }

    #[test]
    fn phase8_shock_requires_confirmed_reversal_while_shadow_still_abstains()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut current = frame(2_100)?;
        current.values.short_return_bps = Decimal::new(-31, 0);
        current.values.book_imbalance = -Decimal::ONE;
        current.values.trade_imbalance = -Decimal::ONE;

        let shadow = strategy()?;
        assert!(
            shadow
                .select_candidates(&current, Decimal::new(20, 0), MarketRegime::Shock)
                .is_empty()
        );

        let mut phase8_binding = binding()?;
        phase8_binding.parameter_release_id =
            crate::strategy::scalping::PHASE8_ATR14_PARAMETER_RELEASE_ID.to_owned();
        let params = ScalpingParams::for_binding(&phase8_binding);
        let mut phase8 = ScalpingStrategy::new(phase8_binding, params)?;
        phase8.regime_entered_at_ms = Some(0);
        assert!(phase8.regime_is_admissible(&current, MarketRegime::Shock));
        assert_eq!(
            phase8.select_candidates(&current, Decimal::new(20, 0), MarketRegime::Shock),
            vec![(
                Direction::Short,
                Expert::RangeFade,
                EntryStyle::PassiveMaker,
                crate::strategy::scalping::ExitTemplate::FairValue,
            )]
        );
        current.values.short_return_bps = Decimal::new(31, 0);
        assert!(
            phase8
                .select_candidates(&current, Decimal::new(20, 0), MarketRegime::Shock)
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn legacy_regime_sign_and_direction_gates_are_preserved()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        strategy.regime_entered_at_ms = Some(0);
        let params = ScalpingParams::shadow(binding()?.risk_budget);
        let mut current = frame(2_100)?;
        current.values.trend_efficiency = Decimal::new(6, 1);
        current.values.bandwidth_expansion = Decimal::new(4, 1);
        assert_eq!(classify_regime(&current, &params), MarketRegime::TrendUp);
        current.values.trend_efficiency = Decimal::new(-6, 1);
        assert_eq!(classify_regime(&current, &params), MarketRegime::TrendDown);
        current.values.bandwidth_expansion = Decimal::new(5, 1);
        assert_eq!(
            classify_regime(&current, &params),
            MarketRegime::ExpansionDown
        );
        current.values.trend_efficiency = Decimal::new(6, 1);
        assert_eq!(
            classify_regime(&current, &params),
            MarketRegime::ExpansionUp
        );

        current.values.bandwidth_expansion = Decimal::new(4, 1);
        current.values.short_return_bps = Decimal::ZERO;
        assert!(
            strategy
                .select_candidates(&current, Decimal::ZERO, MarketRegime::TrendUp)
                .is_empty()
        );
        current.values.short_return_bps = -Decimal::ONE;
        let up = strategy.select_candidates(&current, Decimal::ZERO, MarketRegime::TrendUp);
        assert!(!up.is_empty() && up.iter().all(|candidate| candidate.0 == Direction::Long));
        current.values.short_return_bps = Decimal::ONE;
        let down = strategy.select_candidates(&current, Decimal::ZERO, MarketRegime::TrendDown);
        assert!(!down.is_empty() && down.iter().all(|candidate| candidate.0 == Direction::Short));
        Ok(())
    }

    #[test]
    fn range_fade_accepts_the_minimum_deviation_boundary() -> Result<(), Box<dyn std::error::Error>>
    {
        let strategy = strategy()?;
        let current = frame(100)?;
        assert_eq!(
            strategy
                .select_candidates(&current, Decimal::new(-2, 1), MarketRegime::Range)
                .len(),
            1
        );
        assert!(
            strategy
                .select_candidates(&current, Decimal::new(-199, 3), MarketRegime::Range)
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn expansion_without_a_matching_breakout_abstains() -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        strategy.regime_entered_at_ms = Some(0);
        let mut current = frame(2_100)?;
        assert!(
            strategy
                .select_candidates(&current, Decimal::ZERO, MarketRegime::ExpansionUp)
                .is_empty()
        );
        current.breakout = Some(crate::indicator::BreakoutOpportunity {
            schema_version: 1,
            generation: current.generation,
            feature_version: "pulse-breakout-opportunity-v1".to_owned(),
            direction: crate::indicator::BreakoutDirection::Short,
            boundary_id: "boundary".to_owned(),
            boundary_sequence: 1,
            compression_cycle_id: "compression".to_owned(),
            compression_cycle_sequence: 1,
            detected_at_ms: current.watermark_ms,
            valid_until_ms: current.watermark_ms.saturating_add(1_000),
        });
        assert!(
            strategy
                .select_candidates(&current, Decimal::ZERO, MarketRegime::ExpansionUp)
                .is_empty()
        );
        current
            .breakout
            .as_mut()
            .ok_or("missing breakout")?
            .direction = crate::indicator::BreakoutDirection::Long;
        let matching =
            strategy.select_candidates(&current, Decimal::ZERO, MarketRegime::ExpansionUp);
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].0, Direction::Long);
        assert_eq!(matching[0].1, Expert::BreakoutContinuation);
        Ok(())
    }

    #[test]
    fn admission_uses_stable_robust_value_order_across_entry_styles()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        let projection = safe();
        let authorization = authorization(&projection, 1)?;
        let mut first = frame(100)?;
        first.values.short_return_bps = Decimal::new(-10, 0);
        first.values.trend_efficiency = Decimal::ONE;
        let _ = strategy.evaluate(&first, &projection, &authorization)?;
        let mut settled = frame(2_100)?;
        settled.values.short_return_bps = Decimal::new(-10, 0);
        settled.values.trend_efficiency = Decimal::ONE;
        let preparation = match strategy.evaluate(&settled, &projection, &authorization)? {
            ScalpingDecision::Prepared(preparation) => preparation,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        let mut passive = evidence(&preparation);
        passive.candidate_id = preparation.candidates[0].intent_id.clone();
        passive.net_expected_value_bps = Decimal::ONE;
        let mut marketable = evidence(&preparation);
        marketable.candidate_id = preparation.candidates[1].intent_id.clone();
        marketable.target_pnl_bps = Decimal::new(2, 0);
        marketable.outcome_expected_value_bps = Decimal::new(2, 0);
        marketable.net_expected_value_bps = Decimal::new(2, 0);
        let selected = match strategy.admit(&[passive, marketable], 2_100)? {
            ScalpingDecision::Intent(intent) => intent,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        assert_eq!(
            selected.intent_id, preparation.candidates[1].intent_id,
            "higher robust value must win before expert/style ties"
        );
        Ok(())
    }

    #[test]
    fn minimum_net_ev_gate_rejects_below_and_accepts_at_the_threshold()
    -> Result<(), Box<dyn std::error::Error>> {
        for (value, admitted) in [(Decimal::new(199, 3), false), (Decimal::new(2, 1), true)] {
            let binding = binding()?;
            let mut params = ScalpingParams::shadow(binding.risk_budget.clone());
            params.min_net_ev_bps = Decimal::new(2, 1);
            let mut strategy = ScalpingStrategy::new(binding, params)?;
            let projection = safe();
            let authorization = authorization(&projection, 1)?;
            let mut first = frame(100)?;
            first.values.short_return_bps = Decimal::new(-10, 0);
            first.values.trend_efficiency = Decimal::ONE;
            let _ = strategy.evaluate(&first, &projection, &authorization)?;
            let mut settled = frame(2_100)?;
            settled.values.short_return_bps = Decimal::new(-10, 0);
            settled.values.trend_efficiency = Decimal::ONE;
            let preparation = match strategy.evaluate(&settled, &projection, &authorization)? {
                ScalpingDecision::Prepared(preparation) => preparation,
                decision => return Err(format!("unexpected decision: {decision:?}").into()),
            };
            let mut candidate = evidence(&preparation);
            candidate.target_pnl_bps = value;
            candidate.outcome_expected_value_bps = value;
            candidate.net_expected_value_bps = value;

            let decision = strategy.admit(&[candidate], 2_100)?;
            assert_eq!(matches!(decision, ScalpingDecision::Intent(_)), admitted);
        }
        Ok(())
    }

    #[test]
    fn near_equal_opposite_candidates_fail_closed_on_conflict_margin()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        let projection = safe();
        let authorization = authorization(&projection, 1)?;
        let preparation = match strategy.evaluate(&frame(100)?, &projection, &authorization)? {
            ScalpingDecision::Prepared(preparation) => preparation,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        let mut conflicting = (*preparation).clone();
        let mut reverse = conflicting.candidates[0].clone();
        reverse.intent_id = "opposite-candidate".to_owned();
        reverse.direction = Direction::Short;
        reverse.opportunity_key = "opposite".to_owned();
        conflicting.candidates.push(reverse.clone());
        strategy.state = ScalpingState::CandidatePending(Box::new(conflicting.clone()));
        let mut first = evidence(&conflicting);
        first.net_expected_value_bps = Decimal::ONE;
        let mut opposite = evidence(&conflicting);
        opposite.candidate_id = reverse.intent_id;
        opposite.net_expected_value_bps = Decimal::ONE;
        assert!(matches!(
            strategy.admit(&[first, opposite], 100)?,
            ScalpingDecision::Noop(NoopReason::EvidenceUnavailable)
        ));
        Ok(())
    }

    #[test]
    fn opposite_candidates_at_the_conflict_margin_are_not_blocked()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        let projection = safe();
        let authorization = authorization(&projection, 1)?;
        let preparation = match strategy.evaluate(&frame(100)?, &projection, &authorization)? {
            ScalpingDecision::Prepared(preparation) => preparation,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        let mut conflicting = (*preparation).clone();
        let mut reverse = conflicting.candidates[0].clone();
        reverse.intent_id = "opposite-candidate-at-margin".to_owned();
        reverse.direction = Direction::Short;
        reverse.opportunity_key = "opposite-at-margin".to_owned();
        conflicting.candidates.push(reverse.clone());
        strategy.state = ScalpingState::CandidatePending(Box::new(conflicting.clone()));

        let mut winner = evidence(&conflicting);
        winner.target_pnl_bps = Decimal::new(11, 1);
        winner.outcome_expected_value_bps = Decimal::new(11, 1);
        winner.net_expected_value_bps = Decimal::new(11, 1);
        let mut opposite = evidence(&conflicting);
        opposite.candidate_id = reverse.intent_id;
        assert!(matches!(
            strategy.admit(&[winner, opposite], 100)?,
            ScalpingDecision::Intent(_)
        ));
        Ok(())
    }

    #[test]
    fn stopped_controller_overrides_an_otherwise_safe_strategy_input()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = strategy()?;
        let projection = safe();
        let binding = binding()?;
        let control = InstanceControlRecord {
            schema_version: 1,
            binding: binding.clone(),
            target: ControlTarget::StopAndProtect,
            command_id: "stop_shadow".to_owned(),
            idempotency_key: "stop_shadow_1".to_owned(),
            safety_deadline_ms: None,
            revision: 1,
        };
        let authorization = control.authorize(
            &ControlAuthority {
                generation: 1,
                parameter_release_id: binding.parameter_release_id,
                private_snapshot_ready: true,
                execution_unknown: false,
                protection_complete: true,
                owner_conflict: false,
            },
            1,
        );
        assert_eq!(
            strategy.evaluate(&frame(100)?, &projection, &authorization)?,
            ScalpingDecision::Noop(NoopReason::Blocked(BlockingReason::ControlStopped))
        );
        Ok(())
    }

    #[test]
    fn restart_requires_new_authority_generation_and_feature_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut first = strategy()?;
        let projection = safe();
        let initial_authorization = authorization(&projection, 1)?;
        let initial = frame(100)?;
        let _ = first.evaluate(&initial, &projection, &initial_authorization)?;
        let checkpoint = first.checkpoint();
        let directory = tempdir()?;
        let store = ScalpingCheckpointStore::new(directory.path().join("scalping.json"));
        store.save(&checkpoint)?;
        let persisted = store.load()?.ok_or("checkpoint was not persisted")?;
        let binding = binding()?;
        let params = ScalpingParams::shadow(binding.risk_budget.clone());
        let mut restored = ScalpingStrategy::restore(binding, params, persisted)?;
        assert!(matches!(
            restored.evaluate(&initial, &projection, &initial_authorization),
            Err(crate::strategy::scalping::ScalpingError::FeatureProgress)
        ));
        assert!(matches!(
            restored.evaluate(&frame(101)?, &projection, &initial_authorization)?,
            ScalpingDecision::Noop(NoopReason::Blocked(BlockingReason::RecoveryAuthorization))
        ));
        let fresh_authorization = authorization(&projection, 2)?;
        assert!(matches!(
            restored.evaluate(&frame(102)?, &projection, &fresh_authorization)?,
            ScalpingDecision::Noop(NoopReason::RecoveryWarmup)
        ));
        assert!(matches!(
            restored.evaluate(&frame(2_102)?, &projection, &fresh_authorization)?,
            ScalpingDecision::Prepared(_)
        ));
        Ok(())
    }

    #[test]
    fn checkpoint_rejects_incompatible_identity_and_preserves_cooldown_safely()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut first = strategy()?;
        let projection = safe();
        let initial_authorization = authorization(&projection, 1)?;
        let preparation = match first.evaluate(&frame(100)?, &projection, &initial_authorization)? {
            ScalpingDecision::Prepared(preparation) => preparation,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        let intent = match first.admit(&[evidence(&preparation)], 100)? {
            ScalpingDecision::Intent(intent) => intent,
            decision => return Err(format!("unexpected decision: {decision:?}").into()),
        };
        first.acknowledge_shadow_intent(&intent.intent_id, 100)?;
        let checkpoint = first.checkpoint();
        let binding = binding()?;
        let params = ScalpingParams::shadow(binding.risk_budget.clone());

        let mut wrong_schema = checkpoint.clone();
        wrong_schema.schema_version = 0;
        assert!(matches!(
            ScalpingStrategy::restore(binding.clone(), params.clone(), wrong_schema),
            Err(crate::strategy::scalping::ScalpingError::Checkpoint)
        ));
        let mut wrong_binding = checkpoint.clone();
        wrong_binding.binding_digest = "x".repeat(64);
        assert!(matches!(
            ScalpingStrategy::restore(binding.clone(), params.clone(), wrong_binding),
            Err(crate::strategy::scalping::ScalpingError::Checkpoint)
        ));
        let mut wrong_params = checkpoint.clone();
        wrong_params.params_digest = "x".repeat(64);
        assert!(matches!(
            ScalpingStrategy::restore(binding.clone(), params.clone(), wrong_params),
            Err(crate::strategy::scalping::ScalpingError::Checkpoint)
        ));

        let mut restored = ScalpingStrategy::restore(binding, params, checkpoint)?;
        let fresh_authorization = authorization(&projection, 2)?;
        assert!(matches!(
            restored.evaluate(&frame(101)?, &projection, &fresh_authorization)?,
            ScalpingDecision::Noop(NoopReason::RecoveryWarmup)
        ));
        assert!(matches!(
            restored.evaluate(&frame(2_101)?, &projection, &fresh_authorization)?,
            ScalpingDecision::Prepared(_)
        ));
        Ok(())
    }
}
