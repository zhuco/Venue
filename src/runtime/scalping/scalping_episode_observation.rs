use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{
    domain::{MarkFunding, MarketEvent},
    runtime::{
        EpisodeObservation, PrivateFacts, SCALPING_COORDINATOR_SCHEMA_VERSION,
        episode_observation_fact_id,
    },
    storage::{ProjectionStore, StorageError},
    strategy::scalping::StrategyBinding,
};

pub const SCALPING_EPISODE_OBSERVATION_SCHEMA_VERSION: u16 = 1;

/// The only configuration needed by the local episode mark source. The TTL is explicit because
/// this source has no clock and must compare a mark only with the private watermark in its turn.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpisodeObservationSourceConfig {
    pub binding: StrategyBinding,
    pub active_episode_id: String,
    pub mark_stale_after_ms: u64,
}

/// One normalized input turn. `MarketEvent::MarkFunding` is deliberately kept as the mark source;
/// a public FeatureFrame mid or fair price cannot satisfy this contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EpisodeObservationInput {
    pub private: PrivateFacts,
    pub market_event: MarketEvent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeObservationCursor {
    pub private_generation: u64,
    pub private_observed_at_ms: u64,
    pub private_root_cause_fact_id: String,
    pub mark_generation: u64,
    pub mark_received_at_ms: u64,
    pub mark_exchange_time_ms: u64,
    pub observation_fact_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeObservationSourceReceipt {
    pub observation: EpisodeObservation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EpisodeObservationSourceCheckpoint {
    pub schema_version: u16,
    pub coordinator_schema_version: u16,
    pub binding_digest: String,
    pub active_episode_id: String,
    pub mark_stale_after_ms: u64,
    pub cursor: Option<EpisodeObservationCursor>,
    pub receipt: Option<EpisodeObservationSourceReceipt>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpisodeObservationSourceTurn {
    Applied(EpisodeObservation),
    Duplicate(EpisodeObservation),
}

/// Durable, local-only source for one active episode's private snapshot plus normalized mark.
/// It owns no socket, clock, task, writer, authorization, or exchange mutation capability.
#[derive(Debug)]
pub struct ScalpingEpisodeObservationSource {
    store: ProjectionStore,
    config: EpisodeObservationSourceConfig,
    checkpoint: EpisodeObservationSourceCheckpoint,
}

impl ScalpingEpisodeObservationSource {
    pub fn open_or_restore(
        path: impl AsRef<Path>,
        config: EpisodeObservationSourceConfig,
    ) -> Result<Self, ScalpingEpisodeObservationSourceError> {
        validate_config(&config)?;
        let store = ProjectionStore::new(path.as_ref().to_path_buf());
        let checkpoint = match store.load::<EpisodeObservationSourceCheckpoint>()? {
            Some(checkpoint) => {
                validate_checkpoint(&checkpoint, &config)?;
                checkpoint
            }
            None => empty_checkpoint(&config),
        };
        Ok(Self {
            store,
            config,
            checkpoint,
        })
    }

    pub fn turn(
        &mut self,
        input: EpisodeObservationInput,
    ) -> Result<EpisodeObservationSourceTurn, ScalpingEpisodeObservationSourceError> {
        let mark = match input.market_event {
            MarketEvent::MarkFunding(mark) => mark,
            _ => return Err(ScalpingEpisodeObservationSourceError::MarkSource),
        };
        validate_private(&input.private)?;
        validate_mark(&self.config, &input.private, &mark)?;
        let observation = make_observation(&self.config, &input.private, &mark)?;

        if let Some(receipt) = &self.checkpoint.receipt {
            let prior = &receipt.observation;
            let same_private_watermark = prior.generation == observation.generation
                && prior.observed_at_ms == observation.observed_at_ms;
            let same_mark_watermark = prior.mark_generation == observation.mark_generation
                && prior.mark_received_at_ms == observation.mark_received_at_ms
                && prior.mark_exchange_time_ms == observation.mark_exchange_time_ms;
            if private_cursor_less(&observation, prior) {
                return Err(ScalpingEpisodeObservationSourceError::Regressed);
            }
            if mark_cursor_less(&observation, prior) {
                return Err(ScalpingEpisodeObservationSourceError::Regressed);
            }
            if same_private_watermark
                && prior.private_root_cause_fact_id != observation.private_root_cause_fact_id
            {
                return Err(ScalpingEpisodeObservationSourceError::Equivocation);
            }
            if same_private_watermark && same_mark_watermark {
                if prior == &observation {
                    return Ok(EpisodeObservationSourceTurn::Duplicate(observation));
                }
                return Err(ScalpingEpisodeObservationSourceError::Equivocation);
            }
        }

        let next = EpisodeObservationSourceCheckpoint {
            schema_version: SCALPING_EPISODE_OBSERVATION_SCHEMA_VERSION,
            coordinator_schema_version: SCALPING_COORDINATOR_SCHEMA_VERSION,
            binding_digest: self.config.binding.digest(),
            active_episode_id: self.config.active_episode_id.clone(),
            mark_stale_after_ms: self.config.mark_stale_after_ms,
            cursor: Some(cursor_for(&observation)),
            receipt: Some(EpisodeObservationSourceReceipt {
                observation: observation.clone(),
            }),
        };
        validate_checkpoint(&next, &self.config)?;
        self.store.save(&next)?;
        self.checkpoint = next;
        Ok(EpisodeObservationSourceTurn::Applied(observation))
    }

    #[must_use]
    pub fn checkpoint(&self) -> EpisodeObservationSourceCheckpoint {
        self.checkpoint.clone()
    }
}

fn validate_config(
    config: &EpisodeObservationSourceConfig,
) -> Result<(), ScalpingEpisodeObservationSourceError> {
    config
        .binding
        .validate()
        .map_err(|_| ScalpingEpisodeObservationSourceError::Binding)?;
    if config.active_episode_id.trim().is_empty() || config.mark_stale_after_ms == 0 {
        return Err(ScalpingEpisodeObservationSourceError::Binding);
    }
    Ok(())
}

fn empty_checkpoint(config: &EpisodeObservationSourceConfig) -> EpisodeObservationSourceCheckpoint {
    EpisodeObservationSourceCheckpoint {
        schema_version: SCALPING_EPISODE_OBSERVATION_SCHEMA_VERSION,
        coordinator_schema_version: SCALPING_COORDINATOR_SCHEMA_VERSION,
        binding_digest: config.binding.digest(),
        active_episode_id: config.active_episode_id.clone(),
        mark_stale_after_ms: config.mark_stale_after_ms,
        cursor: None,
        receipt: None,
    }
}

fn validate_checkpoint(
    checkpoint: &EpisodeObservationSourceCheckpoint,
    config: &EpisodeObservationSourceConfig,
) -> Result<(), ScalpingEpisodeObservationSourceError> {
    if checkpoint.schema_version != SCALPING_EPISODE_OBSERVATION_SCHEMA_VERSION
        || checkpoint.coordinator_schema_version != SCALPING_COORDINATOR_SCHEMA_VERSION
        || checkpoint.binding_digest != config.binding.digest()
        || checkpoint.active_episode_id != config.active_episode_id
        || checkpoint.mark_stale_after_ms != config.mark_stale_after_ms
        || checkpoint.receipt.is_some() != checkpoint.cursor.is_some()
    {
        return Err(ScalpingEpisodeObservationSourceError::Checkpoint);
    }
    if let (Some(cursor), Some(receipt)) = (&checkpoint.cursor, &checkpoint.receipt) {
        let observation = &receipt.observation;
        if observation.binding_digest != checkpoint.binding_digest
            || observation.episode_id != checkpoint.active_episode_id
            || cursor.private_generation != observation.generation
            || cursor.private_observed_at_ms != observation.observed_at_ms
            || cursor.private_root_cause_fact_id != observation.private_root_cause_fact_id
            || cursor.mark_generation != observation.mark_generation
            || cursor.mark_received_at_ms != observation.mark_received_at_ms
            || cursor.mark_exchange_time_ms != observation.mark_exchange_time_ms
            || cursor.observation_fact_id != observation.observation_fact_id
            || observation.mark_symbol != config.binding.symbol
            || episode_observation_fact_id(observation)
                .map_err(|_| ScalpingEpisodeObservationSourceError::Checkpoint)?
                != observation.observation_fact_id
        {
            return Err(ScalpingEpisodeObservationSourceError::Checkpoint);
        }
        validate_observation_shape(observation, config)?;
    }
    Ok(())
}

fn validate_private(private: &PrivateFacts) -> Result<(), ScalpingEpisodeObservationSourceError> {
    if private.generation == 0
        || private.observed_at_ms == 0
        || private.root_cause_fact_id.trim().is_empty()
    {
        return Err(ScalpingEpisodeObservationSourceError::PrivateFacts);
    }
    Ok(())
}

fn validate_mark(
    config: &EpisodeObservationSourceConfig,
    private: &PrivateFacts,
    mark: &MarkFunding,
) -> Result<(), ScalpingEpisodeObservationSourceError> {
    if mark.symbol != config.binding.symbol
        || mark.generation == 0
        || mark.received_at_ms == 0
        || mark.exchange_time_ms == 0
        || mark.exchange_time_ms > mark.received_at_ms
        || mark.received_at_ms > private.observed_at_ms
        || private.observed_at_ms.saturating_sub(mark.received_at_ms) > config.mark_stale_after_ms
    {
        return Err(ScalpingEpisodeObservationSourceError::StaleOrInvalidMark);
    }
    Ok(())
}

fn make_observation(
    config: &EpisodeObservationSourceConfig,
    private: &PrivateFacts,
    mark: &MarkFunding,
) -> Result<EpisodeObservation, ScalpingEpisodeObservationSourceError> {
    let mut observation = EpisodeObservation {
        binding_digest: config.binding.digest(),
        episode_id: config.active_episode_id.clone(),
        generation: private.generation,
        observed_at_ms: private.observed_at_ms,
        private_root_cause_fact_id: private.root_cause_fact_id.clone(),
        observation_fact_id: String::new(),
        mark_symbol: mark.symbol.clone(),
        mark_generation: mark.generation,
        mark_received_at_ms: mark.received_at_ms,
        mark_exchange_time_ms: mark.exchange_time_ms,
        mark_price: mark.mark_price,
    };
    observation.observation_fact_id = episode_observation_fact_id(&observation)
        .map_err(|_| ScalpingEpisodeObservationSourceError::ObservationIdentity)?;
    Ok(observation)
}

fn cursor_for(observation: &EpisodeObservation) -> EpisodeObservationCursor {
    EpisodeObservationCursor {
        private_generation: observation.generation,
        private_observed_at_ms: observation.observed_at_ms,
        private_root_cause_fact_id: observation.private_root_cause_fact_id.clone(),
        mark_generation: observation.mark_generation,
        mark_received_at_ms: observation.mark_received_at_ms,
        mark_exchange_time_ms: observation.mark_exchange_time_ms,
        observation_fact_id: observation.observation_fact_id.clone(),
    }
}

fn private_cursor_less(current: &EpisodeObservation, prior: &EpisodeObservation) -> bool {
    current.generation < prior.generation
        || (current.generation == prior.generation && current.observed_at_ms < prior.observed_at_ms)
        || (current.generation > prior.generation && current.observed_at_ms < prior.observed_at_ms)
}

fn mark_cursor_less(current: &EpisodeObservation, prior: &EpisodeObservation) -> bool {
    current.mark_generation < prior.mark_generation
        || current.mark_received_at_ms < prior.mark_received_at_ms
        || current.mark_exchange_time_ms < prior.mark_exchange_time_ms
}

fn validate_observation_shape(
    observation: &EpisodeObservation,
    config: &EpisodeObservationSourceConfig,
) -> Result<(), ScalpingEpisodeObservationSourceError> {
    if observation.binding_digest.trim().is_empty()
        || observation.episode_id.trim().is_empty()
        || observation.generation == 0
        || observation.observed_at_ms == 0
        || observation.private_root_cause_fact_id.trim().is_empty()
        || observation.observation_fact_id.trim().is_empty()
        || observation.mark_generation == 0
        || observation.mark_received_at_ms == 0
        || observation.mark_exchange_time_ms == 0
        || observation.mark_exchange_time_ms > observation.mark_received_at_ms
        || observation.mark_received_at_ms > observation.observed_at_ms
        || observation
            .observed_at_ms
            .saturating_sub(observation.mark_received_at_ms)
            > config.mark_stale_after_ms
    {
        return Err(ScalpingEpisodeObservationSourceError::Checkpoint);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ScalpingEpisodeObservationSourceError {
    #[error("episode observation source binding or stale TTL is invalid")]
    Binding,
    #[error("episode observation source checkpoint is incompatible or tampered")]
    Checkpoint,
    #[error("episode observation source storage failed: {0}")]
    Storage(#[from] StorageError),
    #[error("private facts lack a usable generation, watermark, or root identity")]
    PrivateFacts,
    #[error("normalized mark source is not MarkFunding")]
    MarkSource,
    #[error("normalized mark is stale, future-dated, cross-symbol, or invalid")]
    StaleOrInvalidMark,
    #[error("episode observation cursor regressed")]
    Regressed,
    #[error("episode observation watermark is equivocal or fact content changed")]
    Equivocation,
    #[error("episode observation content identity could not be built")]
    ObservationIdentity,
}
