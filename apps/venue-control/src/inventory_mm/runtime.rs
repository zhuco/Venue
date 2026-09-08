//! A two-quote coordinator. Only the shared PostgreSQL dispatcher can send physical orders.
use super::{InventoryMmStore, InventoryMmStoreError, MmCommandIntent, MmCommandRecord};
use crate::{
    executor_runtime::CommandWake, executor_secret::ExecutorSecretProvider,
    private_projection::BinancePrivateProjectionStore,
};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, BTreeSet};
use venue_control_protocol::{
    inventory_mm::{InventoryMmInstance, InventoryMmState},
    kol::{ExecutorCommandState, TerminalAccountProjection, TerminalPositionMode},
};
use venue_domain::domain::{AccountRiskSnapshot, Asset, Price, RiskSourceStatus};
use venue_gateway_binance::{
    BinanceGridMarketReader, BinanceTransportLimits, GatewayBinding, GatewayMode, VenueId,
};
use venue_strategies::inventory_mm::{MmAction, MmControl, MmInput, MmReason, MmVolatility, plan};
#[path = "facts.rs"]
mod facts;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InventoryMmRuntimeError {
    #[error("inventory MM store unavailable")]
    Store,
    #[error("inventory MM generation changed")]
    Superseded,
    #[error("inventory MM signed facts unavailable")]
    Facts,
    #[error("inventory MM public market unavailable")]
    Market,
    #[error("inventory MM planner rejected facts")]
    Planner,
}
impl From<InventoryMmStoreError> for InventoryMmRuntimeError {
    fn from(error: InventoryMmStoreError) -> Self {
        if error == InventoryMmStoreError::Conflict {
            Self::Superseded
        } else {
            Self::Store
        }
    }
}
type Result<T> = std::result::Result<T, InventoryMmRuntimeError>;

struct MarketState {
    reader: BinanceGridMarketReader,
    volatility: MmVolatility,
    last_sample_ms: u64,
    estimate: Option<Decimal>,
}
pub struct InventoryMmRuntime {
    store: InventoryMmStore,
    projections: BinancePrivateProjectionStore,
    secrets: ExecutorSecretProvider,
    limits: BinanceTransportLimits,
    wake: CommandWake,
    markets: BTreeMap<String, MarketState>,
}
impl InventoryMmRuntime {
    pub fn new(
        store: InventoryMmStore,
        projections: BinancePrivateProjectionStore,
        secrets: ExecutorSecretProvider,
        limits: BinanceTransportLimits,
        wake: CommandWake,
    ) -> Self {
        Self {
            store,
            projections,
            secrets,
            limits,
            wake,
            markets: BTreeMap::new(),
        }
    }
    pub async fn run_until_shutdown(
        mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                _ = interval.tick() => {
                    if *shutdown.borrow() { return Ok(()); }
                    if let Err(error) = self.run_once().await {
                        tracing::warn!(%error, "inventory MM discovery failed");
                    }
                }
            }
        }
    }
    pub async fn run_once(&mut self) -> Result<usize> {
        let records = self.store.active().await?;
        let active: BTreeSet<_> = records.iter().map(|r| r.instance_id.clone()).collect();
        self.markets.retain(|id, _| active.contains(id));
        let mut processed = 0;
        for record in records {
            match self.process(&record).await {
                Ok(()) => processed += 1,
                Err(InventoryMmRuntimeError::Superseded) => {}
                Err(error) => {
                    tracing::warn!(instance_id=%record.instance_id, %error, "inventory MM turn failed closed");
                    if let Err(error) = self
                        .latch_and_cancel(&record, "facts_or_planner_unavailable")
                        .await
                    {
                        tracing::warn!(instance_id=%record.instance_id, %error, "inventory MM halt/cancel awaits authoritative recovery");
                    }
                }
            }
        }
        Ok(processed)
    }
    async fn process(&mut self, record: &InventoryMmInstance) -> Result<()> {
        let commands = self.store.commands(&record.instance_id).await?;
        let projection = self
            .projections
            .load_owned(&record.owner_user_id, &record.credential_id)
            .await
            .map_err(|_| InventoryMmRuntimeError::Facts)?
            .ok_or(InventoryMmRuntimeError::Facts)?;
        if projection.trading_account_id != record.trading_account_id
            || projection.credential_id != record.credential_id
        {
            return Err(InventoryMmRuntimeError::Facts);
        }
        if commands
            .iter()
            .any(|c| c.state == ExecutorCommandState::ReconcileRequired)
        {
            self.latch_and_cancel(record, "command_reconcile_required")
                .await?;
            return Ok(());
        }
        if commands.iter().any(|c| nonterminal(c.state)) {
            return Ok(());
        }
        // Reconciled means a signed command readback, not that an older account snapshot sees it.
        // Wait for a strictly newer complete projection before interpreting an absent quote as filled.
        if !projection_covers_ledger(&commands, projection.observed_ms) {
            return Ok(());
        }
        let owned = owned_orders(record, &projection, &commands)?;
        if matches!(
            record.state,
            InventoryMmState::StopPending | InventoryMmState::NeedsAttention
        ) {
            self.cancel(record, &projection, &owned).await?;
            if owned.is_empty()
                && record.state == InventoryMmState::StopPending
                && self
                    .projections
                    .load_healthy_owned(&record.owner_user_id, &record.credential_id)
                    .await
                    .map_err(|_| InventoryMmRuntimeError::Facts)?
                    .is_some_and(|p| {
                        p.private_generation == projection.private_generation
                            && p.observed_ms == projection.observed_ms
                    })
                && facts::fresh(projection.observed_ms, now()?, facts::PRIVATE_MAX_AGE_MS)
            {
                self.store.finish_stop(record, now()?).await?;
            }
            return Ok(());
        }
        let healthy = self
            .projections
            .load_healthy_owned(&record.owner_user_id, &record.credential_id)
            .await
            .map_err(|_| InventoryMmRuntimeError::Facts)?;
        let now_ms = now()?;
        // A concurrently newer authenticated stream observation is a retry, not a broken-stream halt.
        if healthy.as_ref().is_some_and(|p| {
            p.private_generation != projection.private_generation
                || p.observed_ms != projection.observed_ms
        }) {
            return Ok(());
        }
        if healthy.is_none()
            || projection.position_mode != TerminalPositionMode::Hedge
            || !facts::fresh(projection.observed_ms, now_ms, facts::PRIVATE_MAX_AGE_MS)
            || projection
                .conditional_orders
                .iter()
                .any(|o| o.symbol == record.config.symbol)
        {
            self.latch_and_cancel(record, "private_facts_unavailable")
                .await?;
            return Ok(());
        }
        if !self.markets.contains_key(&record.instance_id) {
            let binding = GatewayBinding {
                venue: VenueId::Binance,
                mode: GatewayMode::Live,
                trading_account_id: record.trading_account_id.clone(),
                symbol: record.config.symbol.clone(),
            };
            let reader = BinanceGridMarketReader::new(binding, self.limits)
                .map_err(|_| InventoryMmRuntimeError::Market)?;
            self.markets.insert(
                record.instance_id.clone(),
                MarketState {
                    reader,
                    volatility: MmVolatility::new(20)
                        .map_err(|_| InventoryMmRuntimeError::Planner)?,
                    last_sample_ms: 0,
                    estimate: None,
                },
            );
        }
        let market = self
            .markets
            .get_mut(&record.instance_id)
            .ok_or(InventoryMmRuntimeError::Market)?;
        let bbo = market
            .reader
            .refresh(now_ms)
            .await
            .map_err(|_| InventoryMmRuntimeError::Market)?;
        let midpoint = bbo
            .bid
            .value()
            .checked_add(bbo.ask.value())
            .and_then(|v| v.checked_div(Decimal::from(2)))
            .and_then(|v| Price::new(v).ok())
            .ok_or(InventoryMmRuntimeError::Facts)?;
        if bbo.observed_at_ms > market.last_sample_ms {
            if market.last_sample_ms != 0
                && bbo.observed_at_ms - market.last_sample_ms > facts::MARKET_MAX_AGE_MS
            {
                market.volatility =
                    MmVolatility::new(20).map_err(|_| InventoryMmRuntimeError::Planner)?;
            }
            market.estimate = market
                .volatility
                .update(bbo.observed_at_ms, midpoint)
                .map_err(|_| InventoryMmRuntimeError::Facts)?;
            market.last_sample_ms = bbo.observed_at_ms;
        }
        let (long_quantity, short_quantity) = facts::positions(&projection, &record.config.symbol)?;
        let reference = market
            .reader
            .refresh_reference(None, now()?)
            .await
            .map_err(|_| InventoryMmRuntimeError::Market)?;
        let credentials = self
            .secrets
            .load(&record.credential_id, &record.owner_user_id)
            .await
            .map_err(|_| InventoryMmRuntimeError::Facts)?;
        let (leverage, (margin, margin_at), conversion) = tokio::try_join!(
            market
                .reader
                .symbol_leverage(&credentials, projection.private_generation),
            market
                .reader
                .account_margin(&credentials, projection.private_generation),
            market
                .reader
                .quote_usd_evidence(projection.private_generation, facts::MARKET_MAX_AGE_MS),
        )
        .map_err(|_| InventoryMmRuntimeError::Facts)?;
        let now_ms = now()?;
        if leverage.0 != record.config.required_leverage
            || !facts::fresh(leverage.1, now_ms, facts::MARKET_MAX_AGE_MS)
            || !facts::fresh(margin_at, now_ms, facts::MARKET_MAX_AGE_MS)
            || !facts::fresh(reference.observed_at_ms, now_ms, facts::MARKET_MAX_AGE_MS)
            || !facts::fresh(bbo.observed_at_ms, now_ms, facts::MARKET_MAX_AGE_MS)
            || !facts::fresh(conversion.observed_at_ms, now_ms, facts::MARKET_MAX_AGE_MS)
            || !facts::fresh(conversion.source_time_ms, now_ms, facts::MARKET_MAX_AGE_MS)
            || conversion.private_generation != projection.private_generation
            || conversion.usd_per_asset <= Decimal::ZERO
            || conversion.asset.as_str() != record.config.symbol.quote()
        {
            return Err(InventoryMmRuntimeError::Facts);
        }
        let available = margin
            .available_balance
            .checked_sub(record.config.min_available_margin)
            .ok_or(InventoryMmRuntimeError::Facts)?
            .max(Decimal::ZERO)
            .checked_mul(Decimal::from(leverage.0))
            .and_then(|v| v.checked_div(conversion.usd_per_asset))
            .ok_or(InventoryMmRuntimeError::Facts)?;
        let baseline = record.baseline_equity.unwrap_or(margin.wallet_balance);
        let peak = record
            .peak_equity
            .unwrap_or(baseline)
            .max(margin.wallet_balance);
        if record.state == InventoryMmState::StartPending {
            // First admission records the account baseline before warming up. Restart never resets a loss history.
            self.store
                .mark_running(record, margin.wallet_balance, now_ms)
                .await?;
            return Ok(());
        }
        let Some(volatility_bps) = market.estimate else {
            // A recovered instance may already hold inventory. Do not substitute a fictitious volatility
            // estimate to authorize opens; remove its existing quotes while the fixed-cadence signal warms.
            self.store
                .observe(record, Some(baseline), Some(peak), None, now_ms)
                .await?;
            let current = self
                .store
                .get(&record.owner_user_id, &record.instance_id)
                .await?;
            self.cancel(&current, &projection, &owned).await?;
            return Ok(());
        };
        let orders = projection
            .open_orders
            .iter()
            .filter(|o| o.symbol == record.config.symbol)
            .map(facts::order)
            .collect::<Result<Vec<_>>>()?;
        let owned_order_ids = owned
            .iter()
            .filter_map(|o| o.native_order_id.clone())
            .collect();
        let input = MmInput {
            config: facts::config(&record.config)?,
            instrument: bbo
                .rules
                .metadata()
                .map_err(|_| InventoryMmRuntimeError::Facts)?,
            maximum_quantity: bbo.rules.maximum_quantity,
            maximum_price: Price::new(bbo.rules.maximum_price)
                .map_err(|_| InventoryMmRuntimeError::Facts)?,
            best_bid: bbo.bid,
            best_ask: bbo.ask,
            mark_price: reference.price,
            market_observed_at_ms: bbo.observed_at_ms.min(reference.observed_at_ms),
            long_quantity,
            short_quantity,
            live_orders: orders,
            owned_order_ids,
            pending_quotes: vec![],
            unknown_results: false,
            account: AccountRiskSnapshot {
                exchange: "binance".into(),
                account: record.trading_account_id.clone(),
                risk_currency: Asset::new("USD").map_err(|_| InventoryMmRuntimeError::Facts)?,
                account_equity: margin.wallet_balance,
                private_generation: projection.private_generation,
                observed_at_ms: margin_at.min(projection.observed_ms),
                source_status: RiskSourceStatus::Complete,
            },
            equity_baseline: facts::amount(baseline, "USD")?,
            equity_peak: facts::amount(peak, "USD")?,
            available_open_notional: facts::amount(available, record.config.symbol.quote())?,
            volatility_bps,
            previous_quotes_at_ms: record.last_quote_ms,
            now_ms,
            control: MmControl::Run,
        };
        let planned = plan(&input).map_err(|_| InventoryMmRuntimeError::Planner)?;
        self.store
            .observe(record, Some(baseline), Some(peak), None, now_ms)
            .await?;
        let current = self
            .store
            .get(&record.owner_user_id, &record.instance_id)
            .await?;
        if current.state != record.state {
            return Err(InventoryMmRuntimeError::Superseded);
        }
        match planned.action {
            MmAction::Keep { .. } => {}
            MmAction::Quote { quotes, .. } => {
                let intents: Vec<_> = quotes
                    .into_iter()
                    .map(|q| MmCommandIntent::Limit {
                        side: q.side,
                        position_side: q.position_side,
                        quantity: q.quantity,
                        price: q.price.value(),
                        reducing: q.reduce_only,
                    })
                    .collect();
                self.store
                    .enqueue(
                        &current,
                        &intents,
                        projection.private_generation,
                        projection.observed_ms,
                        now()?,
                    )
                    .await?;
                self.wake.wake();
            }
            MmAction::CancelThenReplan { order_ids, .. } => {
                let targets: Vec<_> = owned
                    .into_iter()
                    .filter(|o| {
                        o.native_order_id
                            .as_ref()
                            .is_some_and(|id| order_ids.contains(id))
                    })
                    .collect();
                self.cancel(&current, &projection, &targets).await?;
            }
            MmAction::Halt { reason, .. } => {
                let reason = match reason {
                    MmReason::LossLimit => "account_loss_limit",
                    MmReason::DrawdownLimit => "account_drawdown_limit",
                    _ => "planner_halted",
                };
                self.latch_and_cancel(&current, reason).await?;
            }
        }
        Ok(())
    }
    async fn latch_and_cancel(&self, expected: &InventoryMmInstance, reason: &str) -> Result<()> {
        let current = self
            .store
            .get(&expected.owner_user_id, &expected.instance_id)
            .await?;
        if current.state == InventoryMmState::Stopped {
            return Ok(());
        }
        if current.attention.is_none() {
            self.store
                .observe(&current, None, None, Some(reason), now()?)
                .await?;
        }
        let current = self
            .store
            .get(&expected.owner_user_id, &expected.instance_id)
            .await?;
        let commands = self.store.commands(&expected.instance_id).await?;
        if commands.iter().any(|c| nonterminal(c.state)) {
            return Ok(());
        }
        let projection = self
            .projections
            .load_owned(&current.owner_user_id, &current.credential_id)
            .await
            .map_err(|_| InventoryMmRuntimeError::Facts)?
            .ok_or(InventoryMmRuntimeError::Facts)?;
        if projection.trading_account_id != current.trading_account_id
            || projection.credential_id != current.credential_id
        {
            return Err(InventoryMmRuntimeError::Facts);
        }
        if !projection_covers_ledger(&commands, projection.observed_ms) {
            return Ok(());
        }
        let owned = owned_orders(&current, &projection, &commands)?;
        self.cancel(&current, &projection, &owned).await
    }
    async fn cancel(
        &self,
        record: &InventoryMmInstance,
        projection: &TerminalAccountProjection,
        orders: &[&venue_control_protocol::kol::TerminalOpenOrder],
    ) -> Result<()> {
        if orders.is_empty() {
            return Ok(());
        }
        // A corrupt/old surface can contain more than two owned orders. Drain bounded batches
        // through the same serial ledger rather than making cancellation fail on the quote-pair limit.
        let intents: Vec<_> = orders
            .iter()
            .take(2)
            .map(|o| MmCommandIntent::Cancel {
                target_client_order_id: o.client_order_id.clone(),
                native_order_id: o.native_order_id.clone(),
            })
            .collect();
        self.store
            .enqueue(
                record,
                &intents,
                projection.private_generation,
                projection.observed_ms,
                now()?,
            )
            .await?;
        self.wake.wake();
        Ok(())
    }
}
fn owned_orders<'a>(
    record: &InventoryMmInstance,
    projection: &'a TerminalAccountProjection,
    commands: &[MmCommandRecord],
) -> Result<Vec<&'a venue_control_protocol::kol::TerminalOpenOrder>> {
    if projection.credential_id != record.credential_id
        || projection.trading_account_id != record.trading_account_id
    {
        return Err(InventoryMmRuntimeError::Facts);
    }
    let mut clients = BTreeMap::new();
    for command in commands
        .iter()
        .filter(|c| matches!(c.intent, MmCommandIntent::Limit { .. }))
    {
        if clients.insert(&command.client_order_id, command).is_some() {
            return Err(InventoryMmRuntimeError::Facts);
        }
    }
    let mut owned = Vec::new();
    for order in projection
        .open_orders
        .iter()
        .filter(|o| o.symbol == record.config.symbol)
    {
        let Some(command) = clients.get(&order.client_order_id) else {
            continue;
        };
        if command.state != ExecutorCommandState::Reconciled
            || command.native_order_id.is_none()
            || command.native_order_id != order.native_order_id
        {
            return Err(InventoryMmRuntimeError::Facts);
        }
        owned.push(order);
    }
    Ok(owned)
}
fn projection_covers_ledger(commands: &[MmCommandRecord], observed_ms: u64) -> bool {
    observed_ms > 0
        && !commands.iter().any(|c| {
            nonterminal(c.state)
                || (c.state == ExecutorCommandState::Reconciled && c.updated_ms >= observed_ms)
        })
}
fn nonterminal(state: ExecutorCommandState) -> bool {
    matches!(
        state,
        ExecutorCommandState::Pending
            | ExecutorCommandState::Sending
            | ExecutorCommandState::Accepted
            | ExecutorCommandState::ReconcileRequired
    )
}
fn now() -> Result<u64> {
    crate::multi_venue_runtime::now_ms().map_err(|_| InventoryMmRuntimeError::Facts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::{
        inventory_mm::InventoryMmConfig,
        kol::{TERMINAL_PROJECTION_SCHEMA_VERSION, TerminalOpenOrder, TerminalOrderState},
    };
    use venue_domain::{OrderSide, PositionSide};

    fn record() -> std::result::Result<InventoryMmInstance, Box<dyn std::error::Error>> {
        Ok(InventoryMmInstance {
            instance_id: "00000000-0000-4000-8000-000000000003".into(),
            owner_user_id: "owner".into(),
            credential_id: "00000000-0000-4000-8000-000000000001".into(),
            trading_account_id: "00000000-0000-4000-8000-000000000002".into(),
            config: InventoryMmConfig {
                symbol: "XRP/USDC".parse()?,
                order_notional: Decimal::from(5),
                base_half_spread_bps: Decimal::from(5),
                volatility_multiplier: Decimal::from(2),
                inventory_skew_bps: Decimal::from(10),
                max_leg_notional: Decimal::from(400),
                max_gross_notional: Decimal::from(800),
                max_net_notional: Decimal::from(20),
                max_loss_quote: Decimal::from(5),
                max_drawdown_quote: Decimal::from(5),
                min_available_margin: Decimal::from(20),
                required_leverage: 20,
                quote_refresh_ms: 2_000,
            },
            state: InventoryMmState::Running,
            revision: 1,
            baseline_equity: Some(Decimal::from(100)),
            peak_equity: Some(Decimal::from(105)),
            attention: None,
            updated_ms: 100,
            last_quote_ms: None,
        })
    }

    fn command() -> MmCommandRecord {
        MmCommandRecord {
            command_id: "command".into(),
            client_order_id: "owned".into(),
            target_client_order_id: None,
            native_order_id: Some("123".into()),
            state: ExecutorCommandState::Reconciled,
            intent: MmCommandIntent::Limit {
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                quantity: Decimal::from(3),
                price: Decimal::from(2),
                reducing: false,
            },
            created_ms: 80,
            updated_ms: 99,
        }
    }

    fn projection(record: &InventoryMmInstance) -> TerminalAccountProjection {
        TerminalAccountProjection {
            schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
            credential_id: record.credential_id.clone(),
            trading_account_id: record.trading_account_id.clone(),
            observed_ms: 100,
            persisted_ms: 100,
            private_generation: 1,
            position_mode: TerminalPositionMode::Hedge,
            positions: vec![],
            position_history: vec![],
            open_orders: vec![],
            conditional_orders: vec![],
            fills: vec![],
            assets: vec![],
        }
    }

    #[test]
    fn mm_cancel_ownership_requires_exact_instance_ledger_native_identity()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let record = record()?;
        let mut p = projection(&record);
        let q = TerminalOpenOrder {
            client_order_id: "owned".into(),
            native_order_id: Some("123".into()),
            symbol: record.config.symbol.clone(),
            order_side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::from(3),
            filled_quantity: Some(Decimal::ZERO),
            limit_price: Some(Decimal::from(2)),
            time_in_force: None,
            post_only: true,
            reduce_only: false,
            state: TerminalOrderState::New,
            created_ms: Some(80),
        };
        p.open_orders.push(q.clone());
        let commands = vec![command()];
        assert_eq!(owned_orders(&record, &p, &commands)?.len(), 1);
        let mut manual = q;
        manual.client_order_id = "mm-lookalike-manual".into();
        manual.native_order_id = Some("456".into());
        p.open_orders.push(manual);
        assert_eq!(owned_orders(&record, &p, &commands)?.len(), 1);
        p.open_orders[0].native_order_id = Some("789".into());
        assert_eq!(
            owned_orders(&record, &p, &commands),
            Err(InventoryMmRuntimeError::Facts)
        );
        p.open_orders[0].native_order_id = Some("123".into());
        p.credential_id = "00000000-0000-4000-8000-000000000099".into();
        assert_eq!(
            owned_orders(&record, &p, &commands),
            Err(InventoryMmRuntimeError::Facts)
        );
        Ok(())
    }

    #[test]
    fn mm_projection_barrier_waits_for_all_unsettled_and_strictly_newer_readback() {
        let mut commands = vec![command()];
        assert!(!projection_covers_ledger(&commands, 99));
        assert!(projection_covers_ledger(&commands, 100));
        for state in [
            ExecutorCommandState::Pending,
            ExecutorCommandState::Sending,
            ExecutorCommandState::Accepted,
            ExecutorCommandState::ReconcileRequired,
        ] {
            commands[0].state = state;
            assert!(!projection_covers_ledger(&commands, 1_000));
        }
        commands[0].state = ExecutorCommandState::Cancelled;
        assert!(projection_covers_ledger(&commands, 100));
    }
}
