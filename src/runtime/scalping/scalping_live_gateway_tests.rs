#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{Asset, MarketKind},
        runtime::{
            ExecutionProjection, OwnerProjection, PrivateFactsReadiness, PrivateProjection,
            ProtectionProjection, RiskBudgetProjection,
        },
        strategy::scalping::{ExitTemplate, Expert, RiskLimit, RiskPlan, RiskUnit, StrategyKind},
    };

    fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        Ok(StrategyBinding {
            strategy_kind: StrategyKind::Scalping,
            strategy_instance_id: "scalping-test".to_owned(),
            run_id: "live-test".to_owned(),
            exchange: EXCHANGE.to_owned(),
            account: TEST_ACCOUNT_ID.to_owned(),
            symbol: "SOL/USDT".parse()?,
            parameter_release_id: "direct-v1".to_owned(),
            owner_scope: "scalping-test:live-test:SOL/USDT".to_owned(),
            risk_budget: Amount::new("USDT".parse()?, Decimal::new(5, 0)),
        })
    }

    fn projections(generation: u64, observed_at_ms: u64) -> PrivateFactsProjectionInput {
        PrivateFactsProjectionInput {
            execution: PrivateProjection {
                generation,
                observed_at_ms,
                value: ExecutionProjection::Known,
            },
            owner: PrivateProjection {
                generation,
                observed_at_ms,
                value: OwnerProjection::Clear,
            },
            protection: PrivateProjection {
                generation,
                observed_at_ms,
                value: ProtectionProjection::Complete,
            },
            risk_budget: PrivateProjection {
                generation,
                observed_at_ms,
                value: RiskBudgetProjection::Available,
            },
        }
    }

    fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
        Ok(Instrument {
            symbol: "SOL/USDT".parse()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some("USDT".parse::<Asset>()?),
            generation: 1,
            price_tick: Price::new(Decimal::new(1, 2))?,
            quantity_step: Decimal::new(1, 2),
            minimum_notional: Amount::new("USDT".parse()?, Decimal::new(5, 0)),
        })
    }

    fn intent() -> Result<SemanticIntent, Box<dyn std::error::Error>> {
        let unit = RiskUnit::new("risk")?;
        Ok(SemanticIntent {
            intent_id: "live-intent-1".to_owned(),
            symbol: "SOL/USDT".parse()?,
            direction: Direction::Long,
            purpose: crate::strategy::scalping::SemanticPurpose::Entry,
            expert: Expert::BreakoutContinuation,
            entry_style: EntryStyle::MarketableLimit,
            exit_template: ExitTemplate::Breakout,
            attempt_cap: 1,
            max_reprices: 0,
            risk_plan: RiskPlan {
                risk_per_episode: RiskLimit::new(unit.clone(), Decimal::ONE),
                quote_cap: Amount::new("USDT".parse()?, Decimal::new(5, 0)),
                max_episode_loss: RiskLimit::new(unit, Decimal::ONE),
            },
            target_quote: Amount::new("USDT".parse()?, Decimal::new(5, 0)),
            reference_price: Price::new(Decimal::new(100, 0))?,
            max_slippage_bps: Decimal::new(100, 0),
            valid_until_ms: 2_000,
            entry_ttl_ms: 1_000,
            hard_stop_distance_bps: Decimal::new(100, 0),
            target_distance_bps: Decimal::ONE,
            max_hold_ms: 1_000,
            max_unprotected_ms: 1_000,
            requires_server_protection: true,
            opportunity_key: "opportunity-1".to_owned(),
            breakout_cursor: None,
            idempotency_seed: "seed-1".to_owned(),
        })
    }

    #[test]
    fn absent_unknown_recovery_is_a_noop_without_command_debt()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        assert!(!recover_absent_unknown_scalping_entry(
            directory.path(),
            &binding()?,
            BinanceAccountBinding::PortfolioMarginUm,
        )?);
        Ok(())
    }

    #[test]
    fn exact_five_usdt_quantity_is_preserved_when_already_step_aligned()
    -> Result<(), Box<dyn std::error::Error>> {
        let quantity = quantity_for_intent(
            &intent()?,
            &instrument()?,
            Price::new(Decimal::new(100, 0))?,
        )?;
        assert_eq!(quantity, Decimal::new(5, 2));
        assert_eq!(quantity * Decimal::new(100, 0), live_entry_target_usdt());
        Ok(())
    }

    #[test]
    fn five_usdt_quantity_uses_the_smallest_upward_step() -> Result<(), Box<dyn std::error::Error>>
    {
        let price = Decimal::new(8_723, 2);
        let quantity = quantity_for_intent(&intent()?, &instrument()?, Price::new(price)?)?;
        assert_eq!(quantity, Decimal::new(6, 2));
        assert!(quantity * price >= live_entry_target_usdt());
        assert!((quantity - instrument()?.quantity_step) * price < live_entry_target_usdt());
        Ok(())
    }

    #[test]
    fn live_quantity_rejects_any_non_five_usdt_target() -> Result<(), Box<dyn std::error::Error>> {
        let mut intent = intent()?;
        intent.target_quote.value = Decimal::new(499, 2);
        assert!(
            quantity_for_intent(&intent, &instrument()?, Price::new(Decimal::new(100, 0))?,)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn stop_is_rounded_away_from_the_current_price() -> Result<(), Box<dyn std::error::Error>> {
        let stop = stop_price(
            PositionSide::Long,
            Price::new(Decimal::new(100, 0))?,
            Price::new(Decimal::new(101, 0))?,
            Decimal::new(100, 0),
            Price::new(Decimal::new(1, 2))?,
        )?;
        assert_eq!(stop, Price::new(Decimal::new(99, 0))?);
        Ok(())
    }

    #[test]
    fn phase8_passive_entry_uses_atr_target_distance_with_tick_floor()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut intent = intent()?;
        intent.entry_style = EntryStyle::PassiveMaker;
        intent.reference_price = Price::new(Decimal::new(100_003, 3))?;
        intent.target_distance_bps = Decimal::new(80, 0);
        let tick = Price::new(Decimal::new(1, 2))?;
        let long = entry_price(
            &intent,
            Price::new(Decimal::new(9_999, 2))?,
            Price::new(Decimal::new(10_000, 2))?,
            tick,
        )?;
        assert!(long.value() <= intent.reference_price.value() * Decimal::new(992, 3));

        intent.direction = Direction::Short;
        let short = entry_price(
            &intent,
            Price::new(Decimal::new(10_000, 2))?,
            Price::new(Decimal::new(10_001, 2))?,
            tick,
        )?;
        assert!(short.value() >= intent.reference_price.value() * Decimal::new(1008, 3));
        Ok(())
    }

    #[test]
    fn newer_complete_private_readback_retires_the_one_shot_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let binding = binding()?;
        let authority = WriterLeaseAuthority::open(
            directory.path().join("writer.json"),
            writer_scope(&binding),
        )?;
        let _writer = authority.register_initial(100, 1)?;
        let readiness = PrivateFactsReadiness {
            generation: 2,
            observed_at_ms: 200,
            root_cause_fact_id: "private-readback:2:200:1".to_owned(),
            exposure: PrivateExposure::Flat,
            ordinary_order_debt: false,
            algo_order_debt: false,
        };

        assert_eq!(
            reconcile_scalping_writer(
                directory.path().to_path_buf(),
                &binding,
                &readiness,
                projections(2, 200)
            )?,
            ScalpingWriterReconciliation::RetiredFlat
        );
        assert!(authority.active_session()?.is_none());
        Ok(())
    }

    #[test]
    fn newer_complete_protected_readback_keeps_only_the_predecessor()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let binding = binding()?;
        let authority = WriterLeaseAuthority::open(
            directory.path().join("writer.json"),
            writer_scope(&binding),
        )?;
        let writer = authority.register_initial(100, 1)?;
        let readiness = PrivateFactsReadiness {
            generation: 2,
            observed_at_ms: 200,
            root_cause_fact_id: "private-readback:2:200:2".to_owned(),
            exposure: PrivateExposure::Open,
            ordinary_order_debt: false,
            algo_order_debt: true,
        };

        assert_eq!(
            reconcile_scalping_writer(
                directory.path().to_path_buf(),
                &binding,
                &readiness,
                projections(2, 200)
            )?,
            ScalpingWriterReconciliation::ProtectionOnly
        );
        assert_eq!(
            authority
                .active_session()?
                .map(|active| active.readback_generation),
            Some(2)
        );
        assert!(authority.renew(&writer, 201).is_err());
        Ok(())
    }

    #[test]
    fn settlement_replays_no_fill_until_the_host_acknowledges_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let binding = binding()?;
        let authority = WriterLeaseAuthority::open(
            directory.path().join("writer.json"),
            writer_scope(&binding),
        )?;
        authority.register_initial(100, 1)?;
        let mut settlement =
            ScalpingLiveSettlement::open(directory.path().to_path_buf(), binding.clone())?;
        settlement.begin_entry("live-intent-1", "settlement-no-fill")?;
        settlement.record_entry_outcome(
            "live-intent-1",
            &ScalpingLiveEntryOutcome::NoFill {
                command_id: command_id("ent", "live-intent-1")?,
            },
        )?;
        let readiness = PrivateFactsReadiness {
            generation: 2,
            observed_at_ms: 200,
            root_cause_fact_id: "private-readback:2:200:no-fill".to_owned(),
            exposure: PrivateExposure::Flat,
            ordinary_order_debt: false,
            algo_order_debt: false,
        };
        let action = settlement
            .reconcile(&readiness, projections(2, 200))?
            .ok_or(ScalpingLiveGatewayError::Settlement)?;
        assert_eq!(
            action,
            ScalpingLiveSettlementAction::RejectNoFill {
                intent_id: "live-intent-1".to_owned(),
            }
        );
        assert!(authority.active_session()?.is_none());

        let mut recovered = ScalpingLiveSettlement::open(directory.path().to_path_buf(), binding)?;
        assert_eq!(
            recovered.reconcile(&readiness, projections(2, 200))?,
            Some(action.clone())
        );
        recovered.acknowledge(&action)?;
        assert_eq!(recovered.reconcile(&readiness, projections(2, 200))?, None);
        Ok(())
    }

    #[test]
    fn submitting_entry_without_a_writer_recovers_as_no_fill_from_complete_flat()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let binding = binding()?;
        let mut settlement = ScalpingLiveSettlement::open(directory.path().to_path_buf(), binding)?;
        settlement.begin_entry("live-intent-submitting-flat", "submitting-flat")?;

        let readiness = PrivateFactsReadiness {
            generation: 1,
            observed_at_ms: 100,
            root_cause_fact_id: "private-readback:1:100:flat".to_owned(),
            exposure: PrivateExposure::Flat,
            ordinary_order_debt: false,
            algo_order_debt: false,
        };
        assert_eq!(
            settlement.reconcile(&readiness, projections(1, 100))?,
            Some(ScalpingLiveSettlementAction::RejectNoFill {
                intent_id: "live-intent-submitting-flat".to_owned(),
            })
        );
        Ok(())
    }

    #[test]
    fn submitting_entry_with_a_newer_protected_writer_recovers_confirmation()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let binding = binding()?;
        let authority = WriterLeaseAuthority::open(
            directory.path().join("writer.json"),
            writer_scope(&binding),
        )?;
        authority.register_initial(100, 1)?;
        let idempotency_seed = "submitting-protected";
        let client_algo_id = CommandId::new(format!(
            "vsa_{}",
            &sha256_hex(idempotency_seed.as_bytes())[..28]
        ))?;
        let mut settlement = ScalpingLiveSettlement::open(directory.path().to_path_buf(), binding)?;
        settlement.begin_entry("live-intent-submitting-protected", idempotency_seed)?;

        let readiness = PrivateFactsReadiness {
            generation: 2,
            observed_at_ms: 200,
            root_cause_fact_id: "private-readback:2:200:protected".to_owned(),
            exposure: PrivateExposure::Open,
            ordinary_order_debt: false,
            algo_order_debt: true,
        };
        assert_eq!(
            settlement.reconcile(&readiness, projections(2, 200))?,
            Some(ScalpingLiveSettlementAction::ConfirmProtected {
                intent_id: "live-intent-submitting-protected".to_owned(),
                client_algo_id,
            })
        );
        Ok(())
    }

    #[test]
    fn entry_outcome_must_match_the_persisted_protection_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let mut settlement =
            ScalpingLiveSettlement::open(directory.path().to_path_buf(), binding()?)?;
        settlement.begin_entry("live-intent-mismatch", "correct-seed")?;

        assert!(matches!(
            settlement.record_entry_outcome(
                "live-intent-mismatch",
                &ScalpingLiveEntryOutcome::Protected {
                    command_id: command_id("ent", "mismatch")?,
                    position_side: PositionSide::Long,
                    quantity: Decimal::ONE,
                    protection_strategy_id: "12345".to_owned(),
                    protection_client_algo_id: command_id("vsa", "wrong-seed")?,
                },
            ),
            Err(ScalpingLiveGatewayError::Settlement)
        ));
        Ok(())
    }

    #[test]
    fn settlement_replays_protected_confirmation_after_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let binding = binding()?;
        let authority = WriterLeaseAuthority::open(
            directory.path().join("writer.json"),
            writer_scope(&binding),
        )?;
        authority.register_initial(100, 1)?;
        let idempotency_seed = "settlement-protected";
        let algo_id = CommandId::new(format!(
            "vsa_{}",
            &sha256_hex(idempotency_seed.as_bytes())[..28]
        ))?;
        let mut settlement =
            ScalpingLiveSettlement::open(directory.path().to_path_buf(), binding.clone())?;
        settlement.begin_entry("live-intent-1", idempotency_seed)?;
        settlement.record_entry_outcome(
            "live-intent-1",
            &ScalpingLiveEntryOutcome::Protected {
                command_id: command_id("ent", "settlement-protected")?,
                position_side: PositionSide::Long,
                quantity: Decimal::new(1, 1),
                protection_strategy_id: "12345".to_owned(),
                protection_client_algo_id: algo_id.clone(),
            },
        )?;
        let readiness = PrivateFactsReadiness {
            generation: 2,
            observed_at_ms: 200,
            root_cause_fact_id: "private-readback:2:200:protected".to_owned(),
            exposure: PrivateExposure::Open,
            ordinary_order_debt: false,
            algo_order_debt: true,
        };
        let action = settlement
            .reconcile(&readiness, projections(2, 200))?
            .ok_or(ScalpingLiveGatewayError::Settlement)?;
        assert_eq!(
            action,
            ScalpingLiveSettlementAction::ConfirmProtected {
                intent_id: "live-intent-1".to_owned(),
                client_algo_id: algo_id.clone(),
            }
        );
        let protected = authority
            .active_session()?
            .ok_or(ScalpingLiveGatewayError::Settlement)?;
        assert_eq!(protected.readback_generation, 2);
        assert!(authority.renew(&protected, 201).is_err());

        let mut recovered = ScalpingLiveSettlement::open(directory.path().to_path_buf(), binding)?;
        assert_eq!(
            recovered.reconcile(&readiness, projections(2, 200))?,
            Some(action.clone())
        );
        recovered.acknowledge(&action)?;
        assert_eq!(recovered.reconcile(&readiness, projections(2, 200))?, None);
        assert_eq!(recovered.active_protection_client_algo_id(), Some(&algo_id));
        let flat = PrivateFactsReadiness {
            generation: 3,
            observed_at_ms: 300,
            root_cause_fact_id: "private-readback:3:300:flat".to_owned(),
            exposure: PrivateExposure::Flat,
            ordinary_order_debt: false,
            algo_order_debt: false,
        };
        recovered.reconcile_flat_exit(&flat, projections(3, 300))?;
        assert!(authority.active_session()?.is_none());
        assert_eq!(recovered.active_protection_client_algo_id(), None);
        Ok(())
    }
}
