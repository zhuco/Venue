use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use venue_domain::domain::{
    AccountRiskSnapshot, Asset, LegRiskSnapshot, PositionSide, RiskSnapshotError,
    validate_risk_snapshot_pair,
};

use super::{GridPosition, HedgedGridBinding};

pub const EXPOSURE_GUARD_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExposureGuardParams {
    pub enabled: bool,
    #[serde(with = "rust_decimal::serde::str")]
    pub position_equity_multiple: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl_equity_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub reduce_ratio: Decimal,
    pub max_snapshot_age_ms: u64,
    pub rearm_clear_generations: u8,
}

impl ExposureGuardParams {
    pub fn fixed_release() -> Self {
        Self {
            enabled: true,
            position_equity_multiple: Decimal::new(3, 0),
            unrealized_pnl_equity_ratio: Decimal::new(5, 2),
            reduce_ratio: Decimal::new(30, 2),
            max_snapshot_age_ms: 3_000,
            rearm_clear_generations: 2,
        }
    }

    pub fn validate(&self) -> Result<(), ExposureGuardError> {
        let normal_release = self.position_equity_multiple == Decimal::new(3, 0)
            && self.unrealized_pnl_equity_ratio == Decimal::new(5, 2)
            && self.reduce_ratio == Decimal::new(30, 2);
        let minimum_reduce_validation_release = self.position_equity_multiple == Decimal::new(3, 0)
            && self.unrealized_pnl_equity_ratio == Decimal::new(1, 3)
            && self.reduce_ratio == Decimal::new(14, 3);
        if !(normal_release || minimum_reduce_validation_release)
            || self.max_snapshot_age_ms == 0
            || self.rearm_clear_generations != 2
        {
            return Err(ExposureGuardError::Params);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReduceProfitableExposure {
    pub risk_episode_id: String,
    pub position: GridPosition,
    pub trigger_generation: u64,
    #[serde(with = "rust_decimal::serde::str")]
    pub position_quantity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub position_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub account_equity: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub unrealized_pnl: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub reduce_ratio: Decimal,
    pub risk_currency: Asset,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExposureGuardDecision {
    Noop,
    ReduceProfitableExposure(ReduceProfitableExposure),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ExposureEpisodeState {
    Armed,
    TriggerPersisted { risk_episode_id: String },
    Reducing { risk_episode_id: String },
    Reconciling { risk_episode_id: String },
    Latched { risk_episode_id: String },
    ShadowLatched { risk_episode_id: String },
}

impl ExposureEpisodeState {
    fn is_active(&self) -> bool {
        matches!(
            self,
            Self::TriggerPersisted { .. } | Self::Reducing { .. } | Self::Reconciling { .. }
        )
    }

    fn episode_id(&self) -> Option<&str> {
        match self {
            Self::Armed => None,
            Self::TriggerPersisted { risk_episode_id }
            | Self::Reducing { risk_episode_id }
            | Self::Reconciling { risk_episode_id }
            | Self::Latched { risk_episode_id }
            | Self::ShadowLatched { risk_episode_id } => Some(risk_episode_id),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExposureLegGuard {
    pub state: ExposureEpisodeState,
    pub last_evaluated_generation: u64,
    pub clear_generations: u8,
}

impl Default for ExposureLegGuard {
    fn default() -> Self {
        Self {
            state: ExposureEpisodeState::Armed,
            last_evaluated_generation: 0,
            clear_generations: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExposureGuardState {
    pub schema_version: u16,
    pub binding: HedgedGridBinding,
    pub params: ExposureGuardParams,
    pub long: ExposureLegGuard,
    pub short: ExposureLegGuard,
    next_episode_sequence: u64,
}

impl ExposureGuardState {
    pub fn new(
        binding: HedgedGridBinding,
        params: ExposureGuardParams,
    ) -> Result<Self, ExposureGuardError> {
        binding
            .validate()
            .map_err(|_| ExposureGuardError::Binding)?;
        params.validate()?;
        Ok(Self {
            schema_version: EXPOSURE_GUARD_SCHEMA_VERSION,
            binding,
            params,
            long: ExposureLegGuard::default(),
            short: ExposureLegGuard::default(),
            next_episode_sequence: 0,
        })
    }

    pub fn migrate_release_params(
        &mut self,
        params: ExposureGuardParams,
    ) -> Result<(), ExposureGuardError> {
        self.validate_checkpoint()?;
        params.validate()?;
        if self.long.state.is_active() || self.short.state.is_active() {
            return Err(ExposureGuardError::Episode);
        }
        self.params = params;
        self.long = ExposureLegGuard::default();
        self.short = ExposureLegGuard::default();
        Ok(())
    }

    pub fn evaluate(
        &mut self,
        account: &AccountRiskSnapshot,
        leg: &LegRiskSnapshot,
        now_ms: u64,
    ) -> Result<ExposureGuardDecision, ExposureGuardError> {
        self.validate_checkpoint()?;
        validate_risk_snapshot_pair(account, leg, now_ms, self.params.max_snapshot_age_ms)?;
        if account.exchange != self.binding.exchange
            || account.account != self.binding.account
            || leg.symbol != self.binding.symbol
        {
            return Err(ExposureGuardError::Binding);
        }
        let position = match leg.position_side {
            PositionSide::Long => GridPosition::Long,
            PositionSide::Short => GridPosition::Short,
            PositionSide::Net => return Err(ExposureGuardError::Binding),
        };
        let breach = self.breaches(account, leg)?;
        let other_active = self.leg(other(position)).state.is_active();
        let enabled = self.params.enabled;
        let rearm_clear_generations = self.params.rearm_clear_generations;
        if account.private_generation <= self.leg(position).last_evaluated_generation {
            return Ok(ExposureGuardDecision::Noop);
        }
        self.leg_mut(position).last_evaluated_generation = account.private_generation;
        let state = self.leg(position).state.clone();

        match state {
            ExposureEpisodeState::Armed if breach && !other_active && enabled => {
                self.next_episode_sequence = self
                    .next_episode_sequence
                    .checked_add(1)
                    .ok_or(ExposureGuardError::Episode)?;
                let risk_episode_id = format!(
                    "etp-{}-{:016x}",
                    match position {
                        GridPosition::Long => "l",
                        GridPosition::Short => "s",
                    },
                    self.next_episode_sequence
                );
                self.leg_mut(position).state = ExposureEpisodeState::TriggerPersisted {
                    risk_episode_id: risk_episode_id.clone(),
                };
                Ok(ExposureGuardDecision::ReduceProfitableExposure(
                    ReduceProfitableExposure {
                        risk_episode_id,
                        position,
                        trigger_generation: account.private_generation,
                        position_quantity: leg.quantity,
                        position_notional: leg.notional,
                        account_equity: account.account_equity,
                        unrealized_pnl: leg.unrealized_pnl,
                        reduce_ratio: self.params.reduce_ratio,
                        risk_currency: account.risk_currency.clone(),
                    },
                ))
            }
            ExposureEpisodeState::Latched { .. } | ExposureEpisodeState::ShadowLatched { .. }
                if !breach =>
            {
                let guard = self.leg_mut(position);
                guard.clear_generations = guard
                    .clear_generations
                    .saturating_add(1)
                    .min(rearm_clear_generations);
                if guard.clear_generations >= rearm_clear_generations {
                    guard.state = ExposureEpisodeState::Armed;
                    guard.clear_generations = 0;
                }
                Ok(ExposureGuardDecision::Noop)
            }
            ExposureEpisodeState::Latched { .. } | ExposureEpisodeState::ShadowLatched { .. } => {
                self.leg_mut(position).clear_generations = 0;
                Ok(ExposureGuardDecision::Noop)
            }
            _ => Ok(ExposureGuardDecision::Noop),
        }
    }

    /// Advances a proven-flat Hedge side without manufacturing a zero-valued leg snapshot.
    /// Active episodes remain latched to their command lifecycle; only a settled latch may rearm.
    pub fn observe_flat(
        &mut self,
        position: GridPosition,
        private_generation: u64,
    ) -> Result<(), ExposureGuardError> {
        self.validate_checkpoint()?;
        if private_generation == 0
            || private_generation <= self.leg(position).last_evaluated_generation
        {
            return Ok(());
        }
        let rearm_clear_generations = self.params.rearm_clear_generations;
        let guard = self.leg_mut(position);
        guard.last_evaluated_generation = private_generation;
        if matches!(
            guard.state,
            ExposureEpisodeState::Latched { .. } | ExposureEpisodeState::ShadowLatched { .. }
        ) {
            guard.clear_generations = guard
                .clear_generations
                .saturating_add(1)
                .min(rearm_clear_generations);
            if guard.clear_generations >= rearm_clear_generations {
                guard.state = ExposureEpisodeState::Armed;
                guard.clear_generations = 0;
            }
        }
        Ok(())
    }

    pub fn mark_reducing(
        &mut self,
        position: GridPosition,
        risk_episode_id: &str,
    ) -> Result<(), ExposureGuardError> {
        self.transition(
            position,
            risk_episode_id,
            |state| matches!(state, ExposureEpisodeState::TriggerPersisted { .. }),
            ExposureEpisodeState::Reducing {
                risk_episode_id: risk_episode_id.to_owned(),
            },
        )
    }

    pub fn mark_shadow_latched(
        &mut self,
        position: GridPosition,
        risk_episode_id: &str,
    ) -> Result<(), ExposureGuardError> {
        self.transition(
            position,
            risk_episode_id,
            |state| matches!(state, ExposureEpisodeState::TriggerPersisted { .. }),
            ExposureEpisodeState::ShadowLatched {
                risk_episode_id: risk_episode_id.to_owned(),
            },
        )
    }

    /// Shadow observations suppress duplicate logs but cannot block the first live evaluation.
    pub fn release_shadow_latches(&mut self) -> bool {
        let mut changed = false;
        for position in [GridPosition::Long, GridPosition::Short] {
            let guard = self.leg_mut(position);
            if matches!(guard.state, ExposureEpisodeState::ShadowLatched { .. }) {
                guard.state = ExposureEpisodeState::Armed;
                guard.clear_generations = 0;
                changed = true;
            }
        }
        changed
    }

    /// Returns a persisted command plan to its trigger boundary only when the runtime has proved
    /// that no WAL record exists, and therefore no exchange request could have started.
    pub fn recover_unprepared_trigger(
        &mut self,
        position: GridPosition,
        risk_episode_id: &str,
    ) -> Result<(), ExposureGuardError> {
        self.transition(
            position,
            risk_episode_id,
            |state| matches!(state, ExposureEpisodeState::Reducing { .. }),
            ExposureEpisodeState::TriggerPersisted {
                risk_episode_id: risk_episode_id.to_owned(),
            },
        )
    }

    pub fn mark_reconciling(
        &mut self,
        position: GridPosition,
        risk_episode_id: &str,
    ) -> Result<(), ExposureGuardError> {
        self.transition(
            position,
            risk_episode_id,
            |state| matches!(state, ExposureEpisodeState::Reducing { .. }),
            ExposureEpisodeState::Reconciling {
                risk_episode_id: risk_episode_id.to_owned(),
            },
        )
    }

    pub fn mark_latched(
        &mut self,
        position: GridPosition,
        risk_episode_id: &str,
        settled_generation: u64,
    ) -> Result<(), ExposureGuardError> {
        if settled_generation == 0 {
            return Err(ExposureGuardError::Episode);
        }
        let current = &self.leg(position).state;
        if current.episode_id() != Some(risk_episode_id)
            || !matches!(
                current,
                ExposureEpisodeState::TriggerPersisted { .. }
                    | ExposureEpisodeState::Reconciling { .. }
            )
        {
            return Err(ExposureGuardError::Episode);
        }
        let guard = self.leg_mut(position);
        guard.state = ExposureEpisodeState::Latched {
            risk_episode_id: risk_episode_id.to_owned(),
        };
        guard.clear_generations = 0;
        guard.last_evaluated_generation = guard.last_evaluated_generation.max(settled_generation);
        Ok(())
    }

    fn transition(
        &mut self,
        position: GridPosition,
        risk_episode_id: &str,
        accepts: impl FnOnce(&ExposureEpisodeState) -> bool,
        next: ExposureEpisodeState,
    ) -> Result<(), ExposureGuardError> {
        let current = &self.leg(position).state;
        if current.episode_id() != Some(risk_episode_id) || !accepts(current) {
            return Err(ExposureGuardError::Episode);
        }
        self.leg_mut(position).state = next;
        Ok(())
    }

    fn breaches(
        &self,
        account: &AccountRiskSnapshot,
        leg: &LegRiskSnapshot,
    ) -> Result<bool, ExposureGuardError> {
        let exposure_boundary = account
            .account_equity
            .checked_mul(self.params.position_equity_multiple)
            .ok_or(ExposureGuardError::Arithmetic)?;
        let pnl_boundary = account
            .account_equity
            .checked_mul(self.params.unrealized_pnl_equity_ratio)
            .ok_or(ExposureGuardError::Arithmetic)?;
        Ok(leg.notional >= exposure_boundary
            && leg.unrealized_pnl > Decimal::ZERO
            && leg.unrealized_pnl > pnl_boundary)
    }

    pub fn validate_checkpoint(&self) -> Result<(), ExposureGuardError> {
        if self.schema_version != EXPOSURE_GUARD_SCHEMA_VERSION {
            return Err(ExposureGuardError::Checkpoint);
        }
        self.binding
            .validate()
            .map_err(|_| ExposureGuardError::Checkpoint)?;
        self.params.validate()?;
        if self.long.state.is_active() && self.short.state.is_active() {
            return Err(ExposureGuardError::Checkpoint);
        }
        self.validate_leg_checkpoint(GridPosition::Long, &self.long)?;
        self.validate_leg_checkpoint(GridPosition::Short, &self.short)?;
        Ok(())
    }

    fn validate_leg_checkpoint(
        &self,
        position: GridPosition,
        guard: &ExposureLegGuard,
    ) -> Result<(), ExposureGuardError> {
        let clear_count_is_valid = match guard.state {
            ExposureEpisodeState::Armed
            | ExposureEpisodeState::TriggerPersisted { .. }
            | ExposureEpisodeState::Reducing { .. }
            | ExposureEpisodeState::Reconciling { .. } => guard.clear_generations == 0,
            ExposureEpisodeState::Latched { .. } | ExposureEpisodeState::ShadowLatched { .. } => {
                guard.clear_generations < self.params.rearm_clear_generations
            }
        };
        if !clear_count_is_valid {
            return Err(ExposureGuardError::Checkpoint);
        }
        let Some(episode_id) = guard.state.episode_id() else {
            return Ok(());
        };
        if guard.last_evaluated_generation == 0 {
            return Err(ExposureGuardError::Checkpoint);
        }
        let lane = match position {
            GridPosition::Long => "l",
            GridPosition::Short => "s",
        };
        let prefix = format!("etp-{lane}-");
        let Some(encoded_sequence) = episode_id.strip_prefix(&prefix) else {
            return Err(ExposureGuardError::Checkpoint);
        };
        if encoded_sequence.len() != 16
            || !encoded_sequence
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ExposureGuardError::Checkpoint);
        }
        let sequence = u64::from_str_radix(encoded_sequence, 16)
            .map_err(|_| ExposureGuardError::Checkpoint)?;
        if sequence == 0
            || sequence > self.next_episode_sequence
            || episode_id != format!("etp-{lane}-{sequence:016x}")
        {
            return Err(ExposureGuardError::Checkpoint);
        }
        Ok(())
    }

    fn leg(&self, position: GridPosition) -> &ExposureLegGuard {
        match position {
            GridPosition::Long => &self.long,
            GridPosition::Short => &self.short,
        }
    }

    fn leg_mut(&mut self, position: GridPosition) -> &mut ExposureLegGuard {
        match position {
            GridPosition::Long => &mut self.long,
            GridPosition::Short => &mut self.short,
        }
    }
}

const fn other(position: GridPosition) -> GridPosition {
    match position {
        GridPosition::Long => GridPosition::Short,
        GridPosition::Short => GridPosition::Long,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExposureGuardError {
    #[error("exposure guard parameters differ from the approved release")]
    Params,
    #[error("risk snapshot does not match the hedged-grid binding")]
    Binding,
    #[error("risk snapshot is not authoritative: {0}")]
    Snapshot(#[from] RiskSnapshotError),
    #[error("exposure guard arithmetic overflowed")]
    Arithmetic,
    #[error("risk episode transition is invalid")]
    Episode,
    #[error("exposure guard checkpoint schema is unsupported")]
    Checkpoint,
}

#[cfg(test)]
mod tests {
    use venue_domain::domain::{Price, RiskSourceStatus};

    use super::*;

    fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
        Ok(HedgedGridBinding {
            strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "bitget".to_owned(),
            account: "uta_usdt".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            config_version: "risk-v1".to_owned(),
            owner_scope: "hedged_grid_doge_usdt".to_owned(),
        })
    }

    fn snapshots(
        generation: u64,
        side: PositionSide,
        notional: Decimal,
        pnl: Decimal,
    ) -> Result<(AccountRiskSnapshot, LegRiskSnapshot), Box<dyn std::error::Error>> {
        let currency: Asset = "USDT".parse()?;
        Ok((
            AccountRiskSnapshot {
                exchange: "bitget".to_owned(),
                account: "uta_usdt".to_owned(),
                risk_currency: currency.clone(),
                account_equity: Decimal::new(20, 0),
                private_generation: generation,
                observed_at_ms: 1_000 + generation,
                source_status: RiskSourceStatus::Complete,
            },
            LegRiskSnapshot {
                symbol: "DOGE/USDT".parse()?,
                position_side: side,
                quantity: notional * Decimal::new(10, 0),
                mark_price: Price::new(Decimal::new(1, 1))?,
                contract_multiplier: Decimal::ONE,
                notional,
                unrealized_pnl: pnl,
                risk_currency: currency,
                private_generation: generation,
                observed_at_ms: 1_000 + generation,
            },
        ))
    }

    #[test]
    fn three_times_is_inclusive_but_five_percent_is_strict()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = ExposureGuardState::new(binding()?, ExposureGuardParams::fixed_release())?;
        let (account, equal_pnl) = snapshots(
            1,
            PositionSide::Long,
            Decimal::new(60, 0),
            Decimal::new(1, 0),
        )?;
        assert_eq!(
            state.evaluate(&account, &equal_pnl, 1_001)?,
            ExposureGuardDecision::Noop
        );

        let (account, above_pnl) = snapshots(
            2,
            PositionSide::Long,
            Decimal::new(60, 0),
            Decimal::new(101, 2),
        )?;
        let ExposureGuardDecision::ReduceProfitableExposure(action) =
            state.evaluate(&account, &above_pnl, 1_002)?
        else {
            return Err("expected exposure reduction".into());
        };
        assert_eq!(action.reduce_ratio, Decimal::new(30, 2));
        Ok(())
    }

    #[test]
    fn continuous_breach_triggers_once_and_two_clear_generations_rearm()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = ExposureGuardState::new(binding()?, ExposureGuardParams::fixed_release())?;
        let (account, leg) = snapshots(
            1,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        let ExposureGuardDecision::ReduceProfitableExposure(action) =
            state.evaluate(&account, &leg, 1_001)?
        else {
            return Err("expected trigger".into());
        };
        state.mark_reducing(GridPosition::Long, &action.risk_episode_id)?;
        state.mark_reconciling(GridPosition::Long, &action.risk_episode_id)?;
        state.mark_latched(GridPosition::Long, &action.risk_episode_id, 2)?;

        let (account, leg) = snapshots(
            3,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        assert_eq!(
            state.evaluate(&account, &leg, 1_003)?,
            ExposureGuardDecision::Noop
        );
        for generation in [4, 5] {
            let (account, leg) = snapshots(
                generation,
                PositionSide::Long,
                Decimal::new(59, 0),
                Decimal::new(2, 0),
            )?;
            assert_eq!(
                state.evaluate(&account, &leg, 1_000 + generation)?,
                ExposureGuardDecision::Noop
            );
        }
        assert_eq!(state.long.state, ExposureEpisodeState::Armed);
        let (account, leg) = snapshots(
            6,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        assert!(matches!(
            state.evaluate(&account, &leg, 1_006)?,
            ExposureGuardDecision::ReduceProfitableExposure(_)
        ));
        Ok(())
    }

    #[test]
    fn long_and_short_are_independent_but_active_episodes_are_serialized()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = ExposureGuardState::new(binding()?, ExposureGuardParams::fixed_release())?;
        let (account, long) = snapshots(
            1,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        let ExposureGuardDecision::ReduceProfitableExposure(long_action) =
            state.evaluate(&account, &long, 1_001)?
        else {
            return Err("expected long trigger".into());
        };
        let (account, short) = snapshots(
            1,
            PositionSide::Short,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        assert_eq!(
            state.evaluate(&account, &short, 1_001)?,
            ExposureGuardDecision::Noop
        );
        state.mark_latched(GridPosition::Long, &long_action.risk_episode_id, 1)?;
        let (account, short) = snapshots(
            2,
            PositionSide::Short,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        assert!(matches!(
            state.evaluate(&account, &short, 1_002)?,
            ExposureGuardDecision::ReduceProfitableExposure(_)
        ));
        Ok(())
    }

    #[test]
    fn checkpoint_round_trip_preserves_latch_and_episode_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = ExposureGuardState::new(binding()?, ExposureGuardParams::fixed_release())?;
        let (account, leg) = snapshots(
            1,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        let _ = state.evaluate(&account, &leg, 1_001)?;
        let restored: ExposureGuardState = serde_json::from_slice(&serde_json::to_vec(&state)?)?;
        assert_eq!(restored, state);
        Ok(())
    }

    #[test]
    fn flat_generations_rearm_only_a_settled_latch() -> Result<(), Box<dyn std::error::Error>> {
        let mut state = ExposureGuardState::new(binding()?, ExposureGuardParams::fixed_release())?;
        let (account, leg) = snapshots(
            1,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        let ExposureGuardDecision::ReduceProfitableExposure(action) =
            state.evaluate(&account, &leg, 1_001)?
        else {
            return Err("expected trigger".into());
        };
        state.mark_latched(GridPosition::Long, &action.risk_episode_id, 1)?;

        state.observe_flat(GridPosition::Long, 2)?;
        assert!(matches!(
            state.long.state,
            ExposureEpisodeState::Latched { .. }
        ));
        state.observe_flat(GridPosition::Long, 3)?;
        assert_eq!(state.long.state, ExposureEpisodeState::Armed);
        Ok(())
    }

    #[test]
    fn unprepared_recovery_returns_to_the_same_persisted_episode()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = ExposureGuardState::new(binding()?, ExposureGuardParams::fixed_release())?;
        let (account, leg) = snapshots(
            1,
            PositionSide::Short,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        let ExposureGuardDecision::ReduceProfitableExposure(action) =
            state.evaluate(&account, &leg, 1_001)?
        else {
            return Err("expected trigger".into());
        };
        state.mark_reducing(GridPosition::Short, &action.risk_episode_id)?;
        state.recover_unprepared_trigger(GridPosition::Short, &action.risk_episode_id)?;

        assert_eq!(
            state.short.state,
            ExposureEpisodeState::TriggerPersisted {
                risk_episode_id: action.risk_episode_id
            }
        );
        Ok(())
    }

    #[test]
    fn shadow_latch_deduplicates_shadow_but_releases_for_live()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = ExposureGuardState::new(binding()?, ExposureGuardParams::fixed_release())?;
        let (account, leg) = snapshots(
            1,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        let ExposureGuardDecision::ReduceProfitableExposure(action) =
            state.evaluate(&account, &leg, 1_001)?
        else {
            return Err("expected trigger".into());
        };
        state.mark_shadow_latched(GridPosition::Long, &action.risk_episode_id)?;
        let (account, leg) = snapshots(
            2,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        assert_eq!(
            state.evaluate(&account, &leg, 1_002)?,
            ExposureGuardDecision::Noop
        );
        assert!(state.release_shadow_latches());
        let (account, leg) = snapshots(
            3,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        assert!(matches!(
            state.evaluate(&account, &leg, 1_003)?,
            ExposureGuardDecision::ReduceProfitableExposure(_)
        ));
        Ok(())
    }

    #[test]
    fn checkpoint_rejects_conflicting_episodes_bad_identity_and_clear_counts()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = ExposureGuardState::new(binding()?, ExposureGuardParams::fixed_release())?;
        let (account, leg) = snapshots(
            1,
            PositionSide::Long,
            Decimal::new(61, 0),
            Decimal::new(2, 0),
        )?;
        let ExposureGuardDecision::ReduceProfitableExposure(action) =
            state.evaluate(&account, &leg, 1_001)?
        else {
            return Err("expected trigger".into());
        };
        state.validate_checkpoint()?;

        let mut conflicting = state.clone();
        conflicting.short = ExposureLegGuard {
            state: ExposureEpisodeState::Reducing {
                risk_episode_id: "etp-s-0000000000000001".to_owned(),
            },
            last_evaluated_generation: 1,
            clear_generations: 0,
        };
        assert_eq!(
            conflicting.validate_checkpoint(),
            Err(ExposureGuardError::Checkpoint)
        );

        let mut wrong_lane = state.clone();
        wrong_lane.long.state = ExposureEpisodeState::TriggerPersisted {
            risk_episode_id: "etp-s-0000000000000001".to_owned(),
        };
        assert_eq!(
            wrong_lane.validate_checkpoint(),
            Err(ExposureGuardError::Checkpoint)
        );

        let mut future_sequence = state.clone();
        future_sequence.long.state = ExposureEpisodeState::TriggerPersisted {
            risk_episode_id: "etp-l-0000000000000002".to_owned(),
        };
        assert_eq!(
            future_sequence.validate_checkpoint(),
            Err(ExposureGuardError::Checkpoint)
        );

        let mut illegal_clear = state;
        illegal_clear.mark_latched(GridPosition::Long, &action.risk_episode_id, 1)?;
        illegal_clear.long.clear_generations = 2;
        assert_eq!(
            illegal_clear.validate_checkpoint(),
            Err(ExposureGuardError::Checkpoint)
        );
        Ok(())
    }
}
