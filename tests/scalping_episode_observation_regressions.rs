use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{FieldState, MarkFunding, MarketEvent, Price},
    runtime::{
        CustodyStatus, EpisodeObservationInput, EpisodeObservationSourceConfig,
        EpisodeObservationSourceTurn, PrivateFacts, ScalpingEpisodeObservationSource,
        ScalpingEpisodeObservationSourceError, episode_observation_fact_id,
    },
    storage::ProjectionStore,
    strategy::scalping::{ExposureState, ProtectionState, SafetyProjection, StrategyBinding},
};

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding {
        strategy_kind: venue::strategy::scalping::StrategyKind::Scalping,
        strategy_instance_id: "episode-observation-test".to_owned(),
        run_id: "run-1".to_owned(),
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse()?,
        parameter_release_id: "params-v1".to_owned(),
        owner_scope: "episode-observation-test:run-1".to_owned(),
        risk_budget: venue::domain::Amount::new("USDT".parse()?, Decimal::new(5, 0)),
    })
}

fn private(generation: u64, observed_at_ms: u64) -> PrivateFacts {
    PrivateFacts {
        generation,
        observed_at_ms,
        root_cause_fact_id: format!("private-root:{generation}:{observed_at_ms}"),
        safety: SafetyProjection {
            private_snapshot_ready: true,
            exposure: ExposureState::Flat,
            execution_unknown: false,
            protection: ProtectionState::Complete,
            owner_conflict: false,
            risk_budget_available: true,
        },
        custody: CustodyStatus::Complete,
    }
}

fn mark(
    symbol: &str,
    generation: u64,
    received_at_ms: u64,
    exchange_time_ms: u64,
    price: Decimal,
) -> Result<MarketEvent, Box<dyn std::error::Error>> {
    Ok(MarketEvent::MarkFunding(MarkFunding {
        symbol: symbol.parse()?,
        generation,
        received_at_ms,
        exchange_time_ms,
        next_funding_time_ms: received_at_ms + 1_000,
        mark_price: Price::new(price)?,
        index_price: Price::new(price)?,
        funding_rate: Decimal::ZERO,
        estimated_settle_price: FieldState::Missing,
        predicted_funding_rate: FieldState::Missing,
        unknown_reason: None,
    }))
}

fn config(binding: StrategyBinding) -> EpisodeObservationSourceConfig {
    EpisodeObservationSourceConfig {
        binding,
        active_episode_id: "episode-1".to_owned(),
        mark_stale_after_ms: 50,
    }
}

fn input(
    generation: u64,
    observed_at_ms: u64,
    mark_generation: u64,
    mark_received_at_ms: u64,
    mark_exchange_time_ms: u64,
    price: Decimal,
) -> Result<EpisodeObservationInput, Box<dyn std::error::Error>> {
    Ok(EpisodeObservationInput {
        private: private(generation, observed_at_ms),
        market_event: mark(
            "BTC/USDT",
            mark_generation,
            mark_received_at_ms,
            mark_exchange_time_ms,
            price,
        )?,
    })
}

#[test]
fn mark_observation_is_independent_and_duplicate_survives_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("episode-observation.json");
    let binding = binding()?;
    let source_config = config(binding.clone());
    let mut source =
        ScalpingEpisodeObservationSource::open_or_restore(&path, source_config.clone())?;
    let event = input(1, 200, 3, 190, 189, Decimal::new(101, 0))?;
    let observation = match source.turn(event.clone())? {
        EpisodeObservationSourceTurn::Applied(observation) => observation,
        EpisodeObservationSourceTurn::Duplicate(_) => return Err("first turn was duplicate".into()),
    };
    assert_eq!(observation.mark_price.value(), Decimal::new(101, 0));
    assert_ne!(
        observation.private_root_cause_fact_id,
        observation.observation_fact_id
    );
    assert_eq!(
        source.turn(event)?,
        EpisodeObservationSourceTurn::Duplicate(observation.clone())
    );
    drop(source);

    let mut restored = ScalpingEpisodeObservationSource::open_or_restore(&path, source_config)?;
    assert_eq!(
        restored.turn(input(1, 200, 3, 190, 189, Decimal::new(101, 0))?)?,
        EpisodeObservationSourceTurn::Duplicate(observation)
    );
    Ok(())
}

#[test]
fn source_rejects_non_mark_symbol_stale_and_same_watermark_tamper()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("episode-observation.json");
    let source_config = config(binding()?);
    let mut source = ScalpingEpisodeObservationSource::open_or_restore(&path, source_config)?;

    let mut wrong_symbol = input(1, 200, 1, 190, 190, Decimal::ONE)?;
    wrong_symbol.market_event = mark("ETH/USDT", 1, 190, 190, Decimal::ONE)?;
    assert!(matches!(
        source.turn(wrong_symbol),
        Err(ScalpingEpisodeObservationSourceError::StaleOrInvalidMark)
    ));
    assert!(matches!(
        source.turn(input(1, 200, 1, 149, 149, Decimal::ONE)?),
        Err(ScalpingEpisodeObservationSourceError::StaleOrInvalidMark)
    ));
    assert!(matches!(
        source.turn(EpisodeObservationInput {
            private: private(1, 200),
            market_event: MarketEvent::Bar(venue::domain::PublicBar {
                symbol: "BTC/USDT".parse()?,
                generation: 1,
                received_at_ms: 190,
                sequence: 1,
                open_time_ms: 100,
                close_time_ms: 199,
                interval_ms: 100,
                open: Price::new(Decimal::ONE)?,
                high: Price::new(Decimal::new(2, 0))?,
                low: Price::new(Decimal::ONE)?,
                close: Price::new(Decimal::ONE)?,
            }),
        }),
        Err(ScalpingEpisodeObservationSourceError::MarkSource)
    ));

    source.turn(input(1, 200, 1, 190, 189, Decimal::ONE)?)?;
    assert!(matches!(
        source.turn(input(1, 200, 1, 190, 189, Decimal::new(2, 0))?),
        Err(ScalpingEpisodeObservationSourceError::Equivocation)
    ));
    Ok(())
}

#[test]
fn checkpoint_rechecks_ttl_and_cross_generation_time_watermarks()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("episode-observation.json");
    let source_config = config(binding()?);
    let mut source =
        ScalpingEpisodeObservationSource::open_or_restore(&path, source_config.clone())?;
    source.turn(input(1, 200, 1, 190, 189, Decimal::ONE)?)?;
    let mut checkpoint = source.checkpoint();
    let receipt = checkpoint.receipt.as_mut().ok_or("missing receipt")?;
    receipt.observation.mark_received_at_ms = 201;
    receipt.observation.mark_exchange_time_ms = 201;
    receipt.observation.observation_fact_id = episode_observation_fact_id(&receipt.observation)?;
    let cursor = checkpoint.cursor.as_mut().ok_or("missing cursor")?;
    cursor.mark_received_at_ms = 201;
    cursor.mark_exchange_time_ms = 201;
    cursor.observation_fact_id = checkpoint
        .receipt
        .as_ref()
        .ok_or("missing receipt")?
        .observation
        .observation_fact_id
        .clone();
    ProjectionStore::new(&path).save(&checkpoint)?;
    assert!(matches!(
        ScalpingEpisodeObservationSource::open_or_restore(&path, source_config.clone()),
        Err(ScalpingEpisodeObservationSourceError::Checkpoint)
    ));

    let mut source = ScalpingEpisodeObservationSource::open_or_restore(
        directory.path().join("watermarks.json"),
        source_config,
    )?;
    source.turn(input(1, 200, 1, 190, 189, Decimal::ONE)?)?;
    assert!(matches!(
        source.turn(input(1, 220, 1, 200, 180, Decimal::ONE)?),
        Err(ScalpingEpisodeObservationSourceError::Regressed)
    ));
    assert!(matches!(
        source.turn(input(2, 199, 2, 198, 197, Decimal::ONE)?),
        Err(ScalpingEpisodeObservationSourceError::Regressed)
    ));
    assert!(matches!(
        source.turn(input(2, 220, 2, 180, 179, Decimal::ONE)?),
        Err(ScalpingEpisodeObservationSourceError::Regressed)
    ));
    Ok(())
}
