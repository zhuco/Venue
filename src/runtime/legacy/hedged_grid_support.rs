use super::*;

/// The first release has one fixed deployment identity. A later symbol gets a separate root and
/// binding rather than sharing this writer or checkpoint.
pub(in crate::runtime) fn phase_one_binding_for_account(
    trading_account_id: &str,
) -> Result<HedgedGridBinding, HedgedGridLiveError> {
    if !crate::domain::is_canonical_trading_account_id(trading_account_id) {
        return Err(HedgedGridLiveError::Binding);
    }
    Ok(HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdc".to_owned(),
        run_id: "primary".to_owned(),
        exchange: EXCHANGE.to_owned(),
        account: trading_account_id.to_owned(),
        symbol: "SOL/USDC"
            .parse()
            .map_err(|_| HedgedGridLiveError::Binding)?,
        config_version: "phase1".to_owned(),
        owner_scope: "hedged_grid_sol_usdc_primary".to_owned(),
    })
}

#[cfg(test)]
pub(in crate::runtime) fn phase_one_binding() -> Result<HedgedGridBinding, HedgedGridLiveError> {
    phase_one_binding_for_account("00000000-0000-4000-8000-000000000001")
}

pub(in crate::runtime) fn legacy_exposure_is_settled(
    artifacts_root: &Path,
    binding: &HedgedGridBinding,
) -> Result<bool, HedgedGridLiveError> {
    binance_exposure::legacy_checkpoint_is_settled(artifacts_root, binding)
}

/// Binance enforces a per-order notional floor. A shared epoch quantity is sized at its anchor,
/// but lower-priced LONG openings can otherwise fall below that floor. Only opening orders may
/// grow; reduce-only exits remain bounded by authoritative inventory.
pub(in crate::runtime) fn grid_order_quantity(
    binding: &HedgedGridBinding,
    instrument: &crate::domain::Instrument,
    order: &GridOrderIntent,
) -> Result<Decimal, HedgedGridLiveError> {
    if order.reduce_only {
        return Ok(order.quantity);
    }
    if instrument.minimum_notional.asset.as_str() != binding.symbol.quote() {
        return Err(HedgedGridLiveError::Instrument);
    }
    let minimum = instrument.minimum_notional.value;
    if minimum <= Decimal::ZERO || order.quantity * order.price.value() >= minimum {
        return Ok(order.quantity);
    }
    let required = minimum
        .checked_div(order.price.value())
        .ok_or(HedgedGridLiveError::Instrument)?;
    align_up(required, instrument.quantity_step)
}

pub(in crate::runtime) fn load_state(
    store: &ProjectionStore,
    binding: &HedgedGridBinding,
    release_params: &HedgedGridParams,
) -> Result<HedgedGridState, HedgedGridLiveError> {
    match store.load::<HedgedGridCheckpoint>()? {
        Some(mut checkpoint)
            if checkpoint.schema_version == 1 && checkpoint.state.binding == *binding =>
        {
            checkpoint.state.migrate_checkpoint()?;
            checkpoint.state.reconcile_order_sequences();
            Ok(checkpoint.state)
        }
        Some(_) => Err(HedgedGridLiveError::Checkpoint),
        None => HedgedGridState::new_with_params(binding.clone(), release_params.clone())
            .map_err(Into::into),
    }
}

pub(in crate::runtime) fn apply_release_params(
    state: &mut HedgedGridState,
    release_params: HedgedGridParams,
    reset_on_start: bool,
) -> Result<bool, HedgedGridLiveError> {
    if state.params == release_params {
        return Ok(false);
    }
    if !reset_on_start {
        return Err(HedgedGridLiveError::ParameterChangeRequiresReset);
    }
    if state.phase != GridPhase::ResettingGrid {
        let _ = state.request_reset(GridResetReason::Manual)?;
    }
    state.params = release_params;
    Ok(true)
}

pub(in crate::runtime) fn save_state(
    store: &ProjectionStore,
    state: &HedgedGridState,
) -> Result<(), HedgedGridLiveError> {
    store
        .save(&HedgedGridCheckpoint {
            schema_version: 1,
            state: state.clone(),
        })
        .map_err(Into::into)
}

pub(in crate::runtime) fn read_control(
    store: &ProjectionStore,
    binding: &HedgedGridBinding,
) -> Result<HedgedGridControlTarget, HedgedGridLiveError> {
    match store.load::<HedgedGridControl>()? {
        Some(control) => {
            validate_control(&control, binding)?;
            Ok(control.target)
        }
        None => Ok(HedgedGridControlTarget::Stop),
    }
}

pub(in crate::runtime) fn resume_stopping_state_if_requested(
    state: &mut HedgedGridState,
    target: HedgedGridControlTarget,
) -> Result<bool, HedgedGridLiveError> {
    if state.phase != GridPhase::Stopping {
        return Ok(false);
    }
    if target != HedgedGridControlTarget::Reset {
        return Err(HedgedGridLiveError::Stopped);
    }
    state.resume_after_stop()?;
    Ok(true)
}

pub(super) fn validate_control(
    control: &HedgedGridControl,
    binding: &HedgedGridBinding,
) -> Result<(), HedgedGridLiveError> {
    if control.schema_version != 1 || control.binding != *binding {
        return Err(HedgedGridLiveError::Binding);
    }
    Ok(())
}

pub(in crate::runtime) fn align_up(
    value: Decimal,
    step: Decimal,
) -> Result<Decimal, HedgedGridLiveError> {
    if value <= Decimal::ZERO || step <= Decimal::ZERO {
        return Err(HedgedGridLiveError::Instrument);
    }
    Ok((value / step).ceil() * step)
}

pub(in crate::runtime) fn wall_clock_ms() -> Result<u64, HedgedGridLiveError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| HedgedGridLiveError::Clock)
        .map(|duration| {
            let millis = duration.as_millis();
            if millis > u128::from(u64::MAX) {
                u64::MAX
            } else {
                millis as u64
            }
        })
}

#[derive(Debug, thiserror::Error)]
pub enum HedgedGridLiveError {
    #[error("hedged-grid mainnet mutations require explicit confirmation")]
    Confirmation,
    #[error("hedged-grid artifact root must be absolute")]
    ArtifactsRoot,
    #[error("hedged-grid binding or deployment symbol is invalid")]
    Binding,
    #[error("hedged-grid grid-start requires [hedged_grid].grid_count in the selected config")]
    GridConfigRequired,
    #[error(
        "hedged-grid grid_count changed; restart with --reset-on-start to rebuild owned orders"
    )]
    ParameterChangeRequiresReset,
    #[error("hedged-grid control is stopped")]
    Stopped,
    #[error("hedged-grid clock is unavailable")]
    Clock,
    #[error("hedged-grid instrument is incompatible with fixed USDC parameters")]
    Instrument,
    #[error("hedged-grid private inventory is incomplete or not fresh")]
    Inventory,
    #[error("hedged-grid command journal has an unresolved mutation")]
    Unresolved,
    #[error("hedged-grid startup found ordinary orders not owned by this symbol instance")]
    ForeignOrders,
    #[error("Binance rejected a hedged-grid mutation")]
    Rejected,
    #[error("Binance reported a post-only hedged-grid order as a taker fill")]
    PostOnlyFillBecameTaker,
    #[error("Binance owned grid fill has no authoritative maker/taker evidence")]
    FillLiquidityUnknown,
    #[error("hedged-grid worker snapshot is unavailable")]
    Snapshot,
    #[error("hedged-grid checkpoint is incompatible")]
    Checkpoint,
    #[error("hedged-grid command identity is invalid")]
    Identifier,
    #[error("hedged-grid exposure runtime failed closed: {reason}")]
    Exposure { reason: String },
    #[error("hedged-grid concurrent dispatcher terminated unexpectedly")]
    Dispatch,
    #[error("hedged-grid I/O failed for {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("hedged-grid strategy transition failed: {0}")]
    Strategy(#[from] HedgedGridError),
    #[error("hedged-grid private worker failed: {0}")]
    Worker(#[from] PrivateFactsWorkerError),
    #[error("hedged-grid command journal failed: {0}")]
    Journal(#[from] CommandJournalError),
    #[error("hedged-grid writer lease failed: {0}")]
    Writer(#[from] WriterLeaseError),
    #[error("hedged-grid account writer registry failed closed: {reason}")]
    WriterRegistry { reason: String },
    #[error("hedged-grid projection storage failed: {0}")]
    Storage(#[from] StorageError),
    #[error("hedged-grid public Binance request failed: {0}")]
    Public(#[from] crate::exchange::binance::PublicError),
    #[error("hedged-grid Binance payload failed: {0}")]
    Binance(#[from] BinanceError),
    #[error("hedged-grid private Binance request failed: {0}")]
    Private(#[from] PrivateError),
    #[error("hedged-grid private Binance payload failed: {0}")]
    PrivatePayload(#[from] crate::exchange::binance_private::PrivateParseError),
}
