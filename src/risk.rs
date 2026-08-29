use rust_decimal::Decimal;

use crate::domain::{
    AccountBalance, AccountRiskSnapshot, Amount, Instrument, LegRiskSnapshot, MarketKind,
    MarketReduceCommand, OrderCommand, OrderPurpose, OrderSide, Position, PositionSide,
    StopMarketCloseAllCommand, StopMarketFullPositionCommand, validate_risk_snapshot_pair,
};

/// Account-level limits are evaluated before a command is journaled or sent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HardRiskLimits {
    pub max_entry_notional: Amount,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountRiskView {
    pub available_margin: Amount,
    pub unresolved_commands: u32,
}

impl AccountRiskView {
    pub fn from_balance(
        balance: &AccountBalance,
        unresolved_commands: u32,
    ) -> Result<Self, RiskError> {
        balance.validate().map_err(RiskError::Account)?;
        Ok(Self {
            available_margin: Amount::new(balance.asset.clone(), balance.available_balance),
            unresolved_commands,
        })
    }
}

/// Evidence that a single entry command was within current instrument and account bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskApproval {
    pub notional: Amount,
}

/// Evidence frozen into the WAL boundary for one exposure-profit market reduction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketReduceApproval {
    pub notional: Amount,
    pub private_generation: u64,
}

pub fn authorize_entry(
    command: &OrderCommand,
    instrument: &Instrument,
    account: &AccountRiskView,
    limits: &HardRiskLimits,
) -> Result<RiskApproval, RiskError> {
    command.validate().map_err(RiskError::Command)?;
    instrument.validate().map_err(RiskError::Instrument)?;
    if command.owner.symbol != instrument.symbol {
        return Err(RiskError::Symbol);
    }
    if command.reduce_only {
        return Err(RiskError::NotEntry);
    }
    if account.unresolved_commands > 0 {
        return Err(RiskError::Unresolved);
    }
    let asset = instrument
        .settlement_asset
        .clone()
        .ok_or(RiskError::Settlement)?;
    if account.available_margin.asset != asset || limits.max_entry_notional.asset != asset {
        return Err(RiskError::Asset);
    }
    if !aligned(command.quantity, instrument.quantity_step) {
        return Err(RiskError::QuantityStep);
    }
    if !aligned(command.limit_price.value(), instrument.price_tick.value()) {
        return Err(RiskError::PriceTick);
    }
    let notional = Amount::new(asset, command.quantity * command.limit_price.value());
    if notional.value < instrument.minimum_notional.value {
        return Err(RiskError::MinimumNotional);
    }
    if notional.value > limits.max_entry_notional.value {
        return Err(RiskError::Limit);
    }
    if notional.value > account.available_margin.value {
        return Err(RiskError::Margin);
    }
    Ok(RiskApproval { notional })
}

/// A reduction may never enlarge exposure: it must name and oppose one non-flat normalized
/// position side and fit its quantity step. It intentionally has no margin-budget branch because
/// it lowers risk.
pub fn authorize_reduction(
    command: &OrderCommand,
    instrument: &Instrument,
    position: &Position,
) -> Result<(), RiskError> {
    command.validate().map_err(RiskError::Command)?;
    instrument.validate().map_err(RiskError::Instrument)?;
    if command.owner.symbol != instrument.symbol || position.symbol != instrument.symbol {
        return Err(RiskError::Symbol);
    }
    if !command.reduce_only
        || !matches!(
            command.owner.purpose,
            OrderPurpose::Protection | OrderPurpose::Reduce
        )
    {
        return Err(RiskError::NotReduction);
    }
    if !aligned(command.quantity, instrument.quantity_step) {
        return Err(RiskError::QuantityStep);
    }
    if !aligned(command.limit_price.value(), instrument.price_tick.value()) {
        return Err(RiskError::PriceTick);
    }
    if command.quantity > position.quantity || position.quantity.is_zero() {
        return Err(RiskError::Position);
    }
    if command.position_side != position.side {
        return Err(RiskError::Position);
    }
    let expected_side = match position.side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net => return Err(RiskError::Position),
    };
    if command.side != expected_side {
        return Err(RiskError::Side);
    }
    Ok(())
}

/// Rechecks a dedicated exposure-profit market reduction against one complete signed snapshot.
/// The strategy owns the trigger; this gate only proves that the command can reduce, never open.
pub fn authorize_market_reduction(
    command: &MarketReduceCommand,
    instrument: &Instrument,
    account: &AccountRiskSnapshot,
    leg: &LegRiskSnapshot,
    now_ms: u64,
    max_snapshot_age_ms: u64,
) -> Result<MarketReduceApproval, RiskError> {
    command.validate().map_err(RiskError::Command)?;
    instrument.validate().map_err(RiskError::Instrument)?;
    validate_risk_snapshot_pair(account, leg, now_ms, max_snapshot_age_ms)
        .map_err(RiskError::RiskSnapshot)?;
    if instrument.market != MarketKind::LinearPerpetual {
        return Err(RiskError::Market);
    }
    if command.owner.exchange != account.exchange || command.owner.account != account.account {
        return Err(RiskError::AccountBinding);
    }
    if command.owner.symbol != instrument.symbol
        || leg.symbol != instrument.symbol
        || command.owner.symbol != leg.symbol
    {
        return Err(RiskError::Symbol);
    }
    if command.position_side != leg.position_side
        || command.position_generation != leg.private_generation
    {
        return Err(RiskError::PositionGeneration);
    }
    if !aligned(command.quantity, instrument.quantity_step) {
        return Err(RiskError::QuantityStep);
    }
    if command.quantity > leg.quantity {
        return Err(RiskError::Position);
    }
    let expected_side = match leg.position_side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net => return Err(RiskError::Position),
    };
    if command.side != expected_side {
        return Err(RiskError::Side);
    }
    // Venue minimums remain in the settlement quote. The leg multiplier may also contain
    // quote-to-risk-currency conversion, so it must not be applied to this physical minimum.
    let physical_notional = command
        .quantity
        .checked_mul(leg.mark_price.value())
        .ok_or(RiskError::Arithmetic)?;
    if physical_notional < instrument.minimum_notional.value {
        return Err(RiskError::MinimumNotional);
    }
    let risk_notional = leg
        .notional
        .checked_mul(command.quantity)
        .and_then(|value| value.checked_div(leg.quantity))
        .ok_or(RiskError::Arithmetic)?;
    Ok(MarketReduceApproval {
        notional: Amount::new(account.risk_currency.clone(), risk_notional),
        private_generation: leg.private_generation,
    })
}

/// Validates the semantic safety of a hedge-side close-all stop. Account permissions, readback
/// freshness, and position-generation fencing are execution gate responsibilities.
pub fn authorize_stop_market_close_all(
    command: &StopMarketCloseAllCommand,
    instrument: &Instrument,
    position: &Position,
) -> Result<(), RiskError> {
    command.validate().map_err(RiskError::Command)?;
    instrument.validate().map_err(RiskError::Instrument)?;
    if command.owner.symbol != instrument.symbol || position.symbol != instrument.symbol {
        return Err(RiskError::Symbol);
    }
    if !aligned(command.stop_price.value(), instrument.price_tick.value()) {
        return Err(RiskError::PriceTick);
    }
    if position.quantity.is_zero() {
        return Err(RiskError::Position);
    }
    if command.position_side != position.side {
        return Err(RiskError::Position);
    }
    let expected_side = match position.side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net => return Err(RiskError::Position),
    };
    if command.side != expected_side {
        return Err(RiskError::Side);
    }
    Ok(())
}

/// Validates the quantity-bound replacement used by the current PAPI UM Algo endpoint. The
/// command must cover exactly one authoritative Hedge leg; a partial or oversized stop fails.
pub fn authorize_stop_market_full_position(
    command: &StopMarketFullPositionCommand,
    instrument: &Instrument,
    position: &Position,
) -> Result<(), RiskError> {
    command.validate().map_err(RiskError::Command)?;
    instrument.validate().map_err(RiskError::Instrument)?;
    if command.owner.symbol != instrument.symbol || position.symbol != instrument.symbol {
        return Err(RiskError::Symbol);
    }
    if !aligned(command.quantity, instrument.quantity_step) {
        return Err(RiskError::QuantityStep);
    }
    if !aligned(command.trigger_price.value(), instrument.price_tick.value()) {
        return Err(RiskError::PriceTick);
    }
    if position.quantity.is_zero()
        || command.quantity != position.quantity
        || command.position_side != position.side
    {
        return Err(RiskError::Position);
    }
    let expected_side = match position.side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net => return Err(RiskError::Position),
    };
    if command.side != expected_side {
        return Err(RiskError::Side);
    }
    Ok(())
}

fn aligned(value: Decimal, step: Decimal) -> bool {
    !step.is_zero() && (value % step).is_zero()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RiskError {
    #[error("account balance is invalid: {0}")]
    Account(crate::domain::AccountError),
    #[error("command is invalid: {0}")]
    Command(crate::domain::CommandError),
    #[error("instrument is invalid: {0}")]
    Instrument(crate::domain::InstrumentError),
    #[error("risk snapshot is not mutation-authoritative: {0}")]
    RiskSnapshot(crate::domain::RiskSnapshotError),
    #[error("command symbol does not match instrument")]
    Symbol,
    #[error("risk snapshot account binding does not match the command owner")]
    AccountBinding,
    #[error("market exposure reduction requires a linear perpetual instrument")]
    Market,
    #[error("entry authorization cannot approve reduce-only commands")]
    NotEntry,
    #[error("a prior command is unresolved; new entry risk is blocked")]
    Unresolved,
    #[error("linear instrument has no settlement asset")]
    Settlement,
    #[error("account, instrument, and hard limit assets must match")]
    Asset,
    #[error("command quantity is not aligned to the instrument step")]
    QuantityStep,
    #[error("command price is not aligned to the instrument tick")]
    PriceTick,
    #[error("command notional is below the instrument minimum")]
    MinimumNotional,
    #[error("risk notional arithmetic overflowed")]
    Arithmetic,
    #[error("command notional exceeds the hard entry limit")]
    Limit,
    #[error("command notional exceeds available margin")]
    Margin,
    #[error("command is not a reduce-only protection or reduction")]
    NotReduction,
    #[error("the authoritative position is flat or smaller than the requested reduction")]
    Position,
    #[error("market reduction position generation does not match the signed target leg")]
    PositionGeneration,
    #[error("reduce-only order side does not oppose the authoritative position")]
    Side,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::domain::{
        AccountRiskSnapshot, Amount, Asset, CommandId, Instrument, LegRiskSnapshot, MarketKind,
        MarketReduceCommand, OrderCommand, OrderOwner, OrderPurpose, OrderSide, Position,
        PositionSide, Price, RiskSourceStatus,
    };

    use super::*;

    #[test]
    fn five_usdt_limit_admits_only_a_aligned_minimum_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let asset: Asset = "USDT".parse()?;
        let instrument = Instrument {
            symbol: "DOGE/USDT".parse()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(asset.clone()),
            generation: 1,
            price_tick: Price::new(Decimal::new(1, 5))?,
            quantity_step: Decimal::ONE,
            minimum_notional: Amount::new(asset.clone(), Decimal::new(5, 0)),
        };
        let command = OrderCommand {
            command_id: CommandId::new("canary_1")?,
            client_order_id: CommandId::new("venue_canary_1")?,
            owner: OrderOwner {
                strategy_instance_id: "scalping_1".to_owned(),
                run_id: "canary_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: instrument.symbol.clone(),
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(50, 0),
            limit_price: Price::new(Decimal::new(1, 1))?,
            reduce_only: false,
        };
        let account = AccountRiskView {
            available_margin: Amount::new(asset.clone(), Decimal::new(5, 0)),
            unresolved_commands: 0,
        };
        let limits = HardRiskLimits {
            max_entry_notional: Amount::new(asset, Decimal::new(5, 0)),
        };

        assert_eq!(
            authorize_entry(&command, &instrument, &account, &limits)?
                .notional
                .value,
            Decimal::new(5, 0)
        );
        Ok(())
    }

    #[test]
    fn reduction_must_oppose_and_fit_the_authoritative_position()
    -> Result<(), Box<dyn std::error::Error>> {
        let asset: Asset = "USDT".parse()?;
        let instrument = Instrument {
            symbol: "DOGE/USDT".parse()?,
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(asset.clone()),
            generation: 1,
            price_tick: Price::new(Decimal::new(1, 5))?,
            quantity_step: Decimal::ONE,
            minimum_notional: Amount::new(asset, Decimal::new(5, 0)),
        };
        let mut command = OrderCommand {
            command_id: CommandId::new("protect_1")?,
            client_order_id: CommandId::new("venue_protect_1")?,
            owner: OrderOwner {
                strategy_instance_id: "scalping_1".to_owned(),
                run_id: "run_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "primary".to_owned(),
                symbol: instrument.symbol.clone(),
                purpose: OrderPurpose::Protection,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::new(50, 0),
            limit_price: Price::new(Decimal::new(9, 2))?,
            reduce_only: true,
        };
        let position = Position {
            symbol: instrument.symbol.clone(),
            side: PositionSide::Long,
            quantity: Decimal::new(50, 0),
            entry_price: Some(Price::new(Decimal::new(1, 1))?),
            mark_price: Some(Price::new(Decimal::new(9, 2))?),
        };
        authorize_reduction(&command, &instrument, &position)?;
        command.side = OrderSide::Buy;
        command.position_side = PositionSide::Short;
        assert!(matches!(
            authorize_reduction(&command, &instrument, &position),
            Err(RiskError::Position)
        ));
        Ok(())
    }

    fn market_reduce_fixture() -> Result<
        (
            MarketReduceCommand,
            Instrument,
            AccountRiskSnapshot,
            LegRiskSnapshot,
        ),
        Box<dyn std::error::Error>,
    > {
        let currency: Asset = "USDT".parse()?;
        let symbol: crate::domain::Symbol = "DOGE/USDT".parse()?;
        Ok((
            MarketReduceCommand {
                command_id: CommandId::new("risk_reduce_1")?,
                client_order_id: CommandId::new("venue_risk_reduce_1")?,
                owner: OrderOwner {
                    strategy_instance_id: "hedged_grid_1".to_owned(),
                    run_id: "run_1".to_owned(),
                    exchange: "bitget".to_owned(),
                    account: "uta_usdt".to_owned(),
                    symbol: symbol.clone(),
                    purpose: OrderPurpose::ExposureTakeProfit,
                },
                side: OrderSide::Sell,
                position_side: PositionSide::Long,
                quantity: Decimal::new(180, 0),
                risk_episode_id: CommandId::new("risk_episode_1")?,
                position_generation: 7,
            },
            Instrument {
                symbol: symbol.clone(),
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(currency.clone()),
                generation: 3,
                price_tick: Price::new(Decimal::new(1, 4))?,
                quantity_step: Decimal::ONE,
                minimum_notional: Amount::new(currency.clone(), Decimal::new(5, 0)),
            },
            AccountRiskSnapshot {
                exchange: "bitget".to_owned(),
                account: "uta_usdt".to_owned(),
                risk_currency: currency.clone(),
                account_equity: Decimal::new(20, 0),
                private_generation: 7,
                observed_at_ms: 1_000,
                source_status: RiskSourceStatus::Complete,
            },
            LegRiskSnapshot {
                symbol,
                position_side: PositionSide::Long,
                quantity: Decimal::new(600, 0),
                mark_price: Price::new(Decimal::new(1, 1))?,
                contract_multiplier: Decimal::ONE,
                notional: Decimal::new(60, 0),
                unrealized_pnl: Decimal::new(101, 2),
                risk_currency: currency,
                private_generation: 7,
                observed_at_ms: 1_000,
            },
        ))
    }

    #[test]
    fn exposure_market_reduce_is_quantity_generation_and_account_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut command, instrument, account, leg) = market_reduce_fixture()?;
        let approval =
            authorize_market_reduction(&command, &instrument, &account, &leg, 4_000, 3_000)?;
        assert_eq!(approval.notional.value, Decimal::new(18, 0));
        assert_eq!(approval.private_generation, 7);

        command.quantity = Decimal::new(601, 0);
        assert_eq!(
            authorize_market_reduction(&command, &instrument, &account, &leg, 4_000, 3_000),
            Err(RiskError::Position)
        );
        command.quantity = Decimal::new(180, 0);
        command.position_generation = 8;
        assert_eq!(
            authorize_market_reduction(&command, &instrument, &account, &leg, 4_000, 3_000),
            Err(RiskError::PositionGeneration)
        );
        command.position_generation = 7;
        command.owner.account = "other".to_owned();
        assert_eq!(
            authorize_market_reduction(&command, &instrument, &account, &leg, 4_000, 3_000),
            Err(RiskError::AccountBinding)
        );
        Ok(())
    }

    #[test]
    fn exposure_market_reduce_fails_closed_on_stale_mixed_or_below_minimum()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut command, instrument, account, mut leg) = market_reduce_fixture()?;
        assert!(matches!(
            authorize_market_reduction(&command, &instrument, &account, &leg, 4_001, 3_000),
            Err(RiskError::RiskSnapshot(
                crate::domain::RiskSnapshotError::Stale
            ))
        ));
        leg.risk_currency = "USD".parse()?;
        assert!(matches!(
            authorize_market_reduction(&command, &instrument, &account, &leg, 4_000, 3_000),
            Err(RiskError::RiskSnapshot(
                crate::domain::RiskSnapshotError::Currency
            ))
        ));
        leg.risk_currency = "USDT".parse()?;
        command.quantity = Decimal::new(49, 0);
        assert_eq!(
            authorize_market_reduction(&command, &instrument, &account, &leg, 4_000, 3_000),
            Err(RiskError::MinimumNotional)
        );
        command.quantity = Decimal::new(1815, 1);
        assert_eq!(
            authorize_market_reduction(&command, &instrument, &account, &leg, 4_000, 3_000),
            Err(RiskError::QuantityStep)
        );
        Ok(())
    }
}
