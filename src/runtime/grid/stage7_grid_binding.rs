use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn gate_binding(cfg: &Config) -> Result<HedgedGridBinding, Stage7GridError> {
    let gate = cfg.gate.as_ref().ok_or(Stage7GridError::Binding)?;
    if cfg.binance.is_some()
        || cfg.bitget.is_some()
        || gate.account_binding != GateAccountBinding::UsdtFuturesDual
        || cfg.symbol.quote() != "USDT"
    {
        return Err(Stage7GridError::Binding);
    }
    let base = cfg.symbol.base().to_ascii_lowercase();
    let quote = cfg.symbol.quote().to_ascii_lowercase();
    let strategy_instance_id = format!("hedged_grid_{base}_{quote}");
    Ok(HedgedGridBinding {
        owner_scope: format!("{strategy_instance_id}_primary"),
        strategy_instance_id,
        run_id: "primary".to_owned(),
        exchange: "gate".to_owned(),
        account: cfg.trading_account_id.clone(),
        symbol: cfg.symbol.clone(),
        config_version: "stage7".to_owned(),
    })
}

pub(super) fn bitget_binding(cfg: &Config) -> Result<HedgedGridBinding, Stage7GridError> {
    let bitget = cfg.bitget.as_ref().ok_or(Stage7GridError::Binding)?;
    if cfg.binance.is_some()
        || cfg.gate.is_some()
        || bitget.account_binding != BitgetAccountBinding::UtaUsdtFuturesHedge
        || cfg.symbol.quote() != "USDT"
    {
        return Err(Stage7GridError::Binding);
    }
    let base = cfg.symbol.base().to_ascii_lowercase();
    let quote = cfg.symbol.quote().to_ascii_lowercase();
    let strategy_instance_id = format!("hedged_grid_{base}_{quote}");
    Ok(HedgedGridBinding {
        owner_scope: format!("{strategy_instance_id}_primary"),
        strategy_instance_id,
        run_id: "primary".to_owned(),
        exchange: "bitget".to_owned(),
        account: cfg.trading_account_id.clone(),
        symbol: cfg.symbol.clone(),
        config_version: "stage7".to_owned(),
    })
}

pub(super) fn binance_binding(cfg: &Config) -> Result<HedgedGridBinding, Stage7GridError> {
    let binance = cfg.binance.as_ref().ok_or(Stage7GridError::Binding)?;
    if cfg.gate.is_some()
        || cfg.bitget.is_some()
        || binance.account_binding != crate::config::BinanceAccountBinding::PortfolioMarginUm
    {
        return Err(Stage7GridError::Binding);
    }
    let base = cfg.symbol.base().to_ascii_lowercase();
    let quote = cfg.symbol.quote().to_ascii_lowercase();
    let strategy_instance_id = format!("hedged_grid_{base}_{quote}");
    Ok(HedgedGridBinding {
        owner_scope: format!("{strategy_instance_id}_primary"),
        strategy_instance_id,
        run_id: "primary".to_owned(),
        exchange: "binance".to_owned(),
        account: cfg.trading_account_id.clone(),
        symbol: cfg.symbol.clone(),
        config_version: "shared-grid-v1".to_owned(),
    })
}

pub(super) fn stage7_binding(cfg: &Config) -> Result<HedgedGridBinding, Stage7GridError> {
    match (
        cfg.binance.is_some(),
        cfg.gate.is_some(),
        cfg.bitget.is_some(),
    ) {
        (true, false, false) => binance_binding(cfg),
        (false, true, false) => gate_binding(cfg),
        (false, false, true) => bitget_binding(cfg),
        _ => Err(Stage7GridError::Binding),
    }
}

pub(super) fn stage7_writer_scope(binding: &HedgedGridBinding) -> WriterScope {
    WriterScope {
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        owner_scope: binding.owner_scope.clone(),
    }
}

pub(super) fn acquire_stage7_writer_root(
    scope: &WriterScope,
    artifacts_root: &Path,
) -> Result<stage7_writer_registry::Stage7CanonicalRootGuard, Stage7GridError> {
    stage7_writer_registry::acquire(scope, artifacts_root).map_err(|error| {
        Stage7GridError::WriterRegistry {
            reason: error.to_string(),
        }
    })
}

pub(super) fn validate_command_binding(
    command: &ExecutionCommand,
    binding: &HedgedGridBinding,
) -> Result<(), Stage7GridError> {
    let owner = command.owner().ok_or(Stage7GridError::JournalScope)?;
    validate_owner_binding(owner, binding)
}

pub(super) fn validate_owner_binding(
    owner: &crate::domain::OrderOwner,
    binding: &HedgedGridBinding,
) -> Result<(), Stage7GridError> {
    if owner.strategy_instance_id != binding.strategy_instance_id
        || owner.run_id != binding.run_id
        || owner.exchange != binding.exchange
        || owner.account != binding.account
        || owner.symbol != binding.symbol
    {
        return Err(Stage7GridError::JournalScope);
    }
    Ok(())
}

pub(super) fn wall_clock_ms() -> Result<u64, Stage7GridError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Stage7GridError::Clock)?;
    u64::try_from(duration.as_millis()).map_err(|_| Stage7GridError::Clock)
}
