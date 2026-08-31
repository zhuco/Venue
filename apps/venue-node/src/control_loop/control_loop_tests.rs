use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryClaim, AccountDeliveryLease,
    AccountDeliveryPayload, AccountDeliveryPurpose, CONTROL_SCHEMA_VERSION, ControlAction,
    ControlCommandRequest, CopyLifecyclePolicy, CopyRelationBinding, CopyRelationConfig,
    CopyRelationRecord, CopyRiskPolicy,
};
use venue_copy::{AuthoritativePositionSnapshot, DeliveryBinding, RelationCommitment};
use venue_domain::{
    Asset, Fill, InstrumentIdentity, MarketKind, NativeOrderFamily, OrderOwner, OrderSide,
    OrderState, PositionSide, Price,
};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
use venue_runtime::{
    AccountGatewayResult, AccountHostValidationError, AccountInstrumentIdentity,
    AccountRecoveryOutcome, AccountRecoveryReport, AccountRecoveryRequest, AccountRiskEvidence,
    SignedAccountBalance, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot, StrategyKind,
};
use venue_strategies::hedged_grid::HedgedGridParams;

use super::*;

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

struct Gateway {
    binding: GatewayBinding,
    private_generation: Arc<AtomicU64>,
    shutdown: Option<Arc<std::sync::Mutex<ShutdownGatewayState>>>,
}

struct ShutdownGatewayState {
    open_order: bool,
    long_quantity: rust_decimal::Decimal,
    dispatched: Vec<venue_domain::ExecutionCommand>,
}

impl AccountPhysicalGateway for Gateway {
    type Error = io::Error;

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        AccountRecoveryReport::new(
            self.binding.clone(),
            1,
            request
                .unresolved()
                .iter()
                .map(|command| AccountRecoveryOutcome::still_unknown(command.command_id().clone()))
                .collect(),
        )
        .map_err(io::Error::other)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        AccountRiskEvidence::complete(
            self.binding.clone(),
            now_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?,
            1,
            Vec::new(),
            Vec::new(),
        )
    }

    fn signed_account_snapshot(
        &mut self,
        _request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        // Real adapters own a synchronous wrapper around async signed reads.  This test
        // proves the resident calls that wrapper outside the loopback HTTP runtime.
        let adapter_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        adapter_runtime.block_on(async { tokio::task::yield_now().await });
        let observed_ms = now_ms().map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let private_generation = self.private_generation.fetch_add(1, Ordering::AcqRel) + 1;
        let symbol: venue_domain::Symbol = "DOGE/USDT"
            .parse()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let price = Price::new(rust_decimal::Decimal::new(12, 1))
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let shutdown = self
            .shutdown
            .as_ref()
            .map(|state| {
                state
                    .lock()
                    .map(|state| (state.open_order, state.long_quantity))
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)
            })
            .transpose()?;
        let order = SignedAccountOrderFact {
            time_in_force: Some(Default::default()),
            client_order_id: "client-e2e".to_owned(),
            venue_order_id: Some("venue-e2e".to_owned()),
            symbol: symbol.clone(),
            family: NativeOrderFamily::UmOrder,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: rust_decimal::Decimal::new(2, 0),
            limit_price: Some(price.value()),
            reduce_only: false,
            owner: Some(OrderOwner {
                strategy_instance_id: "grid-doge".to_owned(),
                run_id: "run-e2e".to_owned(),
                exchange: self.binding.venue.as_str().to_owned(),
                account: self.binding.trading_account_id.clone(),
                symbol: symbol.clone(),
                purpose: venue_domain::OrderPurpose::Entry,
            }),
            external: false,
            state: Some(OrderState::PartiallyFilled),
            filled_quantity: Some(rust_decimal::Decimal::ONE),
        };
        let fill = Fill {
            fill_id: "fill-e2e".to_owned(),
            execution_sequence: FieldState::Known(7),
            order_id: "venue-e2e".to_owned(),
            symbol: symbol.clone(),
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: rust_decimal::Decimal::ONE,
            price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Missing,
            exchange_time_ms: Some(
                now_ms().map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            ),
        };
        let mut external_order = order.clone();
        external_order.client_order_id = "external-e2e".to_owned();
        external_order.venue_order_id = Some("external-venue-e2e".to_owned());
        external_order.owner = None;
        external_order.external = true;
        let (open_orders, fills, long_quantity) = match shutdown {
            Some((open_order, long_quantity)) => (
                open_order.then_some(order).into_iter().collect(),
                Vec::new(),
                long_quantity,
            ),
            None => (
                Some(order)
                    .into_iter()
                    .chain(std::iter::once(external_order))
                    .collect(),
                vec![fill],
                rust_decimal::Decimal::new(2, 0),
            ),
        };
        SignedAccountSnapshot::complete_with_fills(
            self.binding.clone(),
            observed_ms,
            1,
            private_generation,
            1,
            SignedAccountPositionMode::Hedge,
            open_orders,
            vec![
                SignedAccountPositionFact {
                    symbol: symbol.clone(),
                    position_side: PositionSide::Long,
                    quantity: long_quantity,
                    entry_price: Some(rust_decimal::Decimal::ONE),
                    mark_price: Some(price.value()),
                },
                SignedAccountPositionFact {
                    symbol,
                    position_side: PositionSide::Short,
                    quantity: rust_decimal::Decimal::ZERO,
                    entry_price: None,
                    mark_price: None,
                },
            ],
            fills,
            "fills:0".to_owned(),
            Vec::new(),
        )?
        .with_balances(vec![SignedAccountBalance {
            asset: Asset::new("USDT").map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            equity: rust_decimal::Decimal::new(12, 0),
            available_margin: Some(rust_decimal::Decimal::new(9, 0)),
        }])
    }

    fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        let asset = Asset::new(self.binding.symbol.quote())
            .map_err(|_| AccountHostValidationError::Instrument)?;
        Ok(AccountInstrumentIdentity {
            identity: InstrumentIdentity {
                symbol: self.binding.symbol.clone(),
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(asset),
            },
            rules_generation: 1,
        })
    }

    fn dispatch(&mut self, permit: venue_runtime::AccountDispatchPermit) -> AccountGatewayResult {
        if let Some(state) = &self.shutdown {
            let command = permit.command().clone();
            let mut state = match state.lock() {
                Ok(state) => state,
                Err(_) => return AccountGatewayResult::Unknown,
            };
            match command {
                venue_domain::ExecutionCommand::Cancel(_) => state.open_order = false,
                venue_domain::ExecutionCommand::MarketReduce(_) => {
                    state.long_quantity = rust_decimal::Decimal::ZERO;
                }
                _ => return AccountGatewayResult::Unknown,
            }
            state.dispatched.push(command);
        }
        AccountGatewayResult::Accepted {
            venue_order_id: "test-order".to_owned(),
        }
    }
}

#[test]
fn signed_private_refresh_enters_runtime_without_a_control_poll()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        ACCOUNT,
        "DOGE/USDT".parse()?,
    )?;
    let launch = NodeLaunch::try_parse_from(
        VenueId::Bybit,
        [
            "venue-node-bybit",
            "--mode",
            "LIVE",
            "--trading-account-id",
            ACCOUNT,
            "--symbol",
            "DOGE/USDT",
            "--artifacts-base",
            temporary.path().to_str().ok_or("non-utf8 temporary path")?,
        ],
    )?;
    let strategy = crate::NodeRuntimeStrategy {
        strategy_kind: StrategyKind::Copy,
        instance_id: "copy-doge".to_owned(),
        run_id: "run-private-pump".to_owned(),
        config_digest: "private-pump-config".to_owned(),
        config_epoch: 1,
        symbol: binding.symbol.clone(),
        grid: None,
        copy_leader_capital: None,
    };
    let config = NodeRuntimeConfig {
        version: crate::NODE_RUNTIME_CONFIG_VERSION,
        mode: GatewayMode::Live,
        venue: VenueId::Bybit,
        trading_account_id: ACCOUNT.to_owned(),
        node_id: "node-private-pump".to_owned(),
        control: crate::NodeControlLoopConfig {
            loopback_origin: "http://127.0.0.1:8080/".to_owned(),
            poll_interval_ms: 10,
            projection_interval_ms: 10,
            lease_duration_ms: 1_000,
            claim_limit: 1,
        },
        strategies: vec![strategy],
    };
    config.validate(&binding)?;
    let private_generation = Arc::new(AtomicU64::new(0));
    let resident = ProductionResident::open(
        &launch,
        Gateway {
            binding,
            private_generation: Arc::clone(&private_generation),
            shutdown: None,
        },
    )?;
    // Opening consumes generation 1 for signed recovery. A subsequent resident refresh is a
    // complete Host-routed private observation; no Control HTTP operation has occurred here.
    assert_eq!(private_generation.load(Ordering::Acquire), 1);
    let mut loopback = ControlResidentLoop::open(&launch, &config, resident)?;
    let snapshot = loopback.resident.refresh_signed_snapshot()?;
    assert_eq!(snapshot.private_generation(), 2);
    assert_eq!(private_generation.load(Ordering::Acquire), 2);
    Ok(())
}

#[tokio::test]
async fn resident_tick_polls_claims_applies_and_receipts_without_cross_scope_mixup()
-> Result<(), Box<dyn std::error::Error>> {
    resident_control_delivery_roundtrip(ControlAction::Pause).await
}

#[tokio::test]
async fn manual_trade_is_rejected_without_resuming_actor_or_writing_command_wal()
-> Result<(), Box<dyn std::error::Error>> {
    resident_control_delivery_roundtrip(ControlAction::Trade).await
}

async fn resident_control_delivery_roundtrip(
    action: ControlAction,
) -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        ACCOUNT,
        "DOGE/USDT".parse()?,
    )?;
    let now = now_ms().map_err(|_| "clock")?;
    let command = ControlCommandRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: "control-e2e".to_owned(),
        venue: VenueId::Bybit,
        mode: GatewayMode::Live,
        trading_account_id: ACCOUNT.to_owned(),
        instance_id: "grid-doge".to_owned(),
        symbol: "DOGE/USDT".parse()?,
        action,
        trade: (action == ControlAction::Trade).then_some(venue_control_protocol::TradeIntent {
            action: venue_control_protocol::TradingAction::OpenLong,
            quote_asset: "USDT".to_owned(),
            order_type: venue_control_protocol::TradingOrderType::Limit,
            time_in_force: venue_control_protocol::TradingTimeInForce::Gtc,
            post_only: true,
            reduce_only: false,
            selected_price: Some(rust_decimal::Decimal::ONE),
            quote_notional: Some(rust_decimal::Decimal::ONE),
            close_quantity_cap: None,
            selected_order_id: None,
        }),
        expected_config_epoch: 1,
        confirmation: None,
    };
    let claim = AccountDeliveryClaim {
        lease: AccountDeliveryLease {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            delivery_id: "delivery-e2e".to_owned(),
            binding: AccountDeliveryBinding {
                venue: VenueId::Bybit,
                mode: GatewayMode::Live,
                trading_account_id: ACCOUNT.to_owned(),
                symbol: "DOGE/USDT".parse()?,
                instance_id: "grid-doge".to_owned(),
                config_epoch: 1,
            },
            node_id: "node-e2e".to_owned(),
            lease_epoch: 1,
            leased_at_ms: now.saturating_sub(10),
            expires_at_ms: now.saturating_add(990),
            purpose: AccountDeliveryPurpose::Install,
        },
        payload: AccountDeliveryPayload::ControlCommand(command),
    };
    let (origin, server) = server(serde_json::to_vec(&vec![claim])?, 6, Vec::new()).await?;
    let launch = NodeLaunch::try_parse_from(
        VenueId::Bybit,
        [
            "venue-node-bybit",
            "--mode",
            "LIVE",
            "--trading-account-id",
            ACCOUNT,
            "--symbol",
            "DOGE/USDT",
            "--artifacts-base",
            temporary.path().to_str().ok_or("non-utf8 temporary path")?,
        ],
    )?;
    let strategy = crate::NodeRuntimeStrategy {
        strategy_kind: StrategyKind::HedgedGrid,
        instance_id: "grid-doge".to_owned(),
        run_id: "run-e2e".to_owned(),
        config_digest: "config-e2e".to_owned(),
        config_epoch: 1,
        symbol: "DOGE/USDT".parse()?,
        copy_leader_capital: None,
        grid: Some(crate::NodeGridRuntimeConfig {
            params: HedgedGridParams::fixed_release(Asset::new("USDT")?, 1)?,
            recovery: crate::NodeGridRecoveryPolicy::BootstrapWhenAbsent,
            skip_inventory_replenishment_until_recovered: false,
        }),
    };
    let config = NodeRuntimeConfig {
        version: crate::NODE_RUNTIME_CONFIG_VERSION,
        mode: GatewayMode::Live,
        venue: VenueId::Bybit,
        trading_account_id: ACCOUNT.to_owned(),
        node_id: "node-e2e".to_owned(),
        control: crate::NodeControlLoopConfig {
            loopback_origin: origin,
            poll_interval_ms: 10,
            projection_interval_ms: 10,
            lease_duration_ms: 1_000,
            claim_limit: 1,
        },
        strategies: vec![strategy.clone()],
    };
    config.validate(&binding)?;
    // This is a recovered owned order, not an adapter-provided ownership claim. Production
    // recovers the identical route from Accepted records in its sole account WAL.
    fs::create_dir_all(launch.artifacts_root())?;
    let mut wal =
        venue_runtime::CommandJournal::open(launch.artifacts_root().join("commands.jsonl"))?;
    let create = venue_domain::CommandId::new("recovered-e2e")?;
    wal.prepare(venue_domain::ExecutionCommand::PlaceLimit(
        venue_domain::OrderCommand {
            time_in_force: Default::default(),
            command_id: create.clone(),
            client_order_id: venue_domain::CommandId::new("client-e2e")?,
            owner: OrderOwner {
                strategy_instance_id: "grid-doge".to_owned(),
                run_id: "run-e2e".to_owned(),
                exchange: "bybit".to_owned(),
                account: ACCOUNT.to_owned(),
                symbol: "DOGE/USDT".parse()?,
                purpose: venue_domain::OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: 2.into(),
            limit_price: Price::new(rust_decimal::Decimal::new(12, 1))?,
            reduce_only: false,
        },
    ))?;
    wal.transition(&create, venue_runtime::CommandState::Submitted)?;
    wal.transition(
        &create,
        venue_runtime::CommandState::Accepted {
            venue_order_id: "venue-e2e".to_owned(),
        },
    )?;
    drop(wal);
    let loopback = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| ControlResidentLoopError::Signal)?;
        let mut resident = ProductionResident::open(
            &launch,
            Gateway {
                binding: binding.clone(),
                private_generation: Arc::new(AtomicU64::new(0)),
                shutdown: None,
            },
        )
        .map_err(ControlResidentLoopError::Resident)?;
        resident
            .register_actor(
                config
                    .binding_for(&strategy)
                    .map_err(|_| ControlResidentLoopError::Config)?,
            )
            .map_err(ControlResidentLoopError::Resident)?;
        if resident.strategy_lifecycle(
            &config
                .binding_for(&strategy)
                .map_err(|_| ControlResidentLoopError::Config)?,
        ) != Some(InstanceLifecycle::Paused)
        {
            return Err(ControlResidentLoopError::Config);
        }
        let wal_path = launch.artifacts_root().join("commands.jsonl");
        let wal_before = fs::read(&wal_path).map_err(|_| ControlResidentLoopError::Config)?;
        let mut loopback = ControlResidentLoop::open(&launch, &config, resident)?;
        loopback.tick(&runtime, now)?;
        loopback.tick(&runtime, now.saturating_add(20))?;
        if action == ControlAction::Trade {
            let actor = config
                .binding_for(&strategy)
                .map_err(|_| ControlResidentLoopError::Config)?;
            assert_eq!(
                loopback.resident.strategy_lifecycle(&actor),
                Some(InstanceLifecycle::Paused)
            );
            assert!(
                loopback
                    .resident
                    .apply_control_action(&actor, action)
                    .is_err()
            );
            assert_eq!(
                fs::read(wal_path).map_err(|_| ControlResidentLoopError::Config)?,
                wal_before
            );
        }
        Ok::<_, ControlResidentLoopError>(loopback)
    })
    .await??;
    let requests = server.await??;
    let receipt_body = requests
        .iter()
        .find(|(path, _)| path == "/v2/account-node/deliveries/receipts")
        .map(|(_, body)| body)
        .ok_or("receipt missing")?;
    let receipt: venue_control_protocol::AccountDeliveryReceipt =
        serde_json::from_slice(receipt_body)?;
    assert_eq!(
        receipt.state,
        if action == ControlAction::Trade {
            venue_control_protocol::AccountDeliveryReceiptState::Rejected
        } else {
            venue_control_protocol::AccountDeliveryReceiptState::Applied
        }
    );
    assert_eq!(
        requests.iter().map(|(path, _)| path).collect::<Vec<_>>(),
        vec![
            "/v2/account-node/deliveries/claim",
            "/v2/account-node/deliveries/ack",
            "/v2/account-node/deliveries/receipts",
            "/v2/account-node/projection",
            "/v2/account-node/deliveries/claim",
            "/v2/account-node/projection",
        ]
    );
    let projection_body = requests
        .iter()
        .find(|(path, _)| path == "/v2/account-node/projection")
        .map(|(_, body)| body)
        .ok_or("projection request missing")?;
    let envelope: NodeProjectionEnvelope = serde_json::from_slice(projection_body)?;
    assert_eq!(envelope.snapshot.accounts[0].equity, None);
    assert_eq!(envelope.snapshot.accounts[0].balances.len(), 1);
    assert_eq!(
        envelope.snapshot.accounts[0].balances[0].available_margin,
        Some(rust_decimal::Decimal::new(9, 0))
    );
    assert_eq!(envelope.facts.orders.len(), 1);
    assert_eq!(envelope.facts.orders[0].order_id, "venue-e2e");
    assert_eq!(envelope.facts.positions.len(), 2);
    assert_eq!(
        envelope.facts.positions[0].quantity,
        rust_decimal::Decimal::new(2, 0)
    );
    assert_eq!(envelope.facts.fills.len(), 1);
    assert_eq!(envelope.facts.fills[0].execution_sequence, Some(7));
    assert_eq!(
        envelope.facts.fills[0].position_side,
        Some(PositionSide::Long)
    );
    // The snapshot also contains an unowned same-symbol order. Flatten may be semantically
    // accepted, but no cancellation can be admitted until an operator resolves that scope
    // conflict; it must never infer ownership from symbol or position alone.
    let loopback = tokio::task::spawn_blocking(move || {
        let mut loopback = loopback;
        let binding = loopback
            .bindings
            .get("grid-doge")
            .cloned()
            .ok_or(ControlResidentLoopError::Config)?;
        loopback
            .resident
            .apply_control_action(&binding, ControlAction::Flatten)
            .map_err(ControlResidentLoopError::Resident)?;
        loopback
            .shutdowns
            .get_mut("grid-doge")
            .ok_or(ControlResidentLoopError::Config)?
            .begin(ControlAction::Flatten)
            .map_err(|_| ControlResidentLoopError::ControlShutdown)?;
        loopback.advance_control_shutdown("grid-doge")?;
        Ok::<_, ControlResidentLoopError>(loopback)
    })
    .await??;
    assert_eq!(
        loopback
            .shutdowns
            .get("grid-doge")
            .and_then(ControlShutdownJournal::operation)
            .ok_or("shutdown operation missing")?
            .phase,
        ShutdownPhase::NeedsAttention
    );
    assert_eq!(
        loopback
            .drivers
            .get("grid-doge")
            .ok_or("driver missing")?
            .inbox()
            .pending_receipts(now)
            .len(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn publisher_uploads_only_fresh_signed_leader_planning_fact()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        ACCOUNT,
        "DOGE/USDT".parse()?,
    )?;
    let relation = CopyRelationRecord {
        revision: 1,
        relation: CopyRelationConfig {
            relation_id: "00000000-0000-4000-8000-000000000071".to_owned(),
            leader: CopyRelationBinding {
                venue: VenueId::Bybit,
                mode: GatewayMode::Live,
                trading_account_id: ACCOUNT.to_owned(),
                instance_id: "copy-leader".to_owned(),
                symbol: "DOGE/USDT".parse()?,
            },
            follower: CopyRelationBinding {
                venue: VenueId::Bybit,
                mode: GatewayMode::Live,
                trading_account_id: "00000000-0000-4000-8000-000000000002".to_owned(),
                instance_id: "copy-follower".to_owned(),
                symbol: "DOGE/USDT".parse()?,
            },
            allocated_capital: rust_decimal::Decimal::TEN,
            multiplier: rust_decimal::Decimal::ONE,
            safety_reserve_rate: rust_decimal::Decimal::new(1, 1),
            risk: CopyRiskPolicy {
                max_total_notional: rust_decimal::Decimal::from(10),
                max_order_notional: rust_decimal::Decimal::from(5),
                max_leverage: rust_decimal::Decimal::ONE,
            },
            lifecycle: CopyLifecyclePolicy::Active,
        },
    };
    relation.validate()?;
    let (origin, server) = server(
        serde_json::to_vec(&Vec::<AccountDeliveryClaim>::new())?,
        3,
        vec![relation.clone()],
    )
    .await?;
    let launch = NodeLaunch::try_parse_from(
        VenueId::Bybit,
        [
            "venue-node-bybit",
            "--mode",
            "LIVE",
            "--trading-account-id",
            ACCOUNT,
            "--symbol",
            "DOGE/USDT",
            "--artifacts-base",
            temporary.path().to_str().ok_or("non-utf8 temporary path")?,
        ],
    )?;
    let strategy = crate::NodeRuntimeStrategy {
        strategy_kind: StrategyKind::Copy,
        instance_id: "copy-leader".to_owned(),
        run_id: "run-leader".to_owned(),
        config_digest: "leader-digest".to_owned(),
        config_epoch: 1,
        symbol: "DOGE/USDT".parse()?,
        copy_leader_capital: Some(venue_domain::Amount::new(
            Asset::new("USDT")?,
            rust_decimal::Decimal::from(7),
        )),
        grid: None,
    };
    let config = NodeRuntimeConfig {
        version: crate::NODE_RUNTIME_CONFIG_VERSION,
        mode: GatewayMode::Live,
        venue: VenueId::Bybit,
        trading_account_id: ACCOUNT.to_owned(),
        node_id: "leader-node".to_owned(),
        control: crate::NodeControlLoopConfig {
            loopback_origin: origin,
            poll_interval_ms: 10,
            projection_interval_ms: 10,
            lease_duration_ms: 1_000,
            claim_limit: 1,
        },
        strategies: vec![strategy.clone()],
    };
    config.validate(&binding)?;
    let now = now_ms().map_err(|_| "clock")?;
    let loopback = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| ControlResidentLoopError::Signal)?;
        let mut resident = ProductionResident::open(
            &launch,
            Gateway {
                binding: binding.clone(),
                private_generation: Arc::new(AtomicU64::new(0)),
                shutdown: Some(Arc::new(std::sync::Mutex::new(ShutdownGatewayState {
                    open_order: false,
                    long_quantity: rust_decimal::Decimal::ZERO,
                    dispatched: Vec::new(),
                }))),
            },
        )
        .map_err(ControlResidentLoopError::Resident)?;
        let strategy_binding = config
            .binding_for(&strategy)
            .map_err(|_| ControlResidentLoopError::Config)?;
        resident
            .register_actor(strategy_binding.clone())
            .map_err(ControlResidentLoopError::Resident)?;
        if resident.strategy_lifecycle(&strategy_binding) != Some(InstanceLifecycle::Running) {
            return Err(ControlResidentLoopError::Config);
        }
        let mut loopback = ControlResidentLoop::open(&launch, &config, resident)?;
        loopback.tick(&runtime, now)?;
        Ok::<_, ControlResidentLoopError>(loopback)
    })
    .await??;
    let requests = server.await??;
    assert_eq!(
        requests.iter().map(|(path, _)| path).collect::<Vec<_>>(),
        vec![
            "/v2/account-node/deliveries/claim",
            "/v2/copy/relations",
            "/v2/account-node/projection",
        ]
    );
    let projection_body = requests
        .iter()
        .find(|(path, _)| path == "/v2/account-node/projection")
        .map(|(_, body)| body)
        .ok_or("projection request missing")?;
    let envelope: NodeProjectionEnvelope = serde_json::from_slice(projection_body)?;
    assert_eq!(envelope.copy_planning_facts.len(), 1);
    let fact = &envelope.copy_planning_facts[0];
    assert_eq!(
        fact.role,
        venue_control_protocol::CopyPlanningFactRole::Leader
    );
    assert_eq!(fact.relation_id, relation.relation.relation_id);
    assert_eq!(fact.relation_revision, relation.revision);
    assert_eq!(fact.policy_digest, relation.relation.policy_digest());
    assert_eq!(fact.binding.instance_id, "copy-leader");
    assert_eq!(fact.private_generation, 2);
    assert_eq!(fact.rules_generation, 1);
    assert_eq!(fact.quote_net_exposure.value, rust_decimal::Decimal::ZERO);
    assert_eq!(
        fact.leader_configured_capital
            .as_ref()
            .map(|amount| amount.value),
        Some(rust_decimal::Decimal::from(7))
    );
    assert!(fact.follower_available_margin.is_none());
    assert_eq!(fact.validate(), Ok(()));
    drop(loopback);
    Ok(())
}

#[tokio::test]
async fn fresh_copy_follower_bootstrap_publishes_planning_without_a_control_job()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        ACCOUNT,
        "DOGE/USDT".parse()?,
    )?;
    let relation = CopyRelationRecord {
        revision: 1,
        relation: CopyRelationConfig {
            relation_id: "00000000-0000-4000-8000-000000000072".to_owned(),
            leader: CopyRelationBinding {
                venue: VenueId::Bybit,
                mode: GatewayMode::Live,
                trading_account_id: "00000000-0000-4000-8000-000000000002".to_owned(),
                instance_id: "copy-leader".to_owned(),
                symbol: "DOGE/USDT".parse()?,
            },
            follower: CopyRelationBinding {
                venue: VenueId::Bybit,
                mode: GatewayMode::Live,
                trading_account_id: ACCOUNT.to_owned(),
                instance_id: "copy-follower".to_owned(),
                symbol: "DOGE/USDT".parse()?,
            },
            allocated_capital: rust_decimal::Decimal::TEN,
            multiplier: rust_decimal::Decimal::ONE,
            safety_reserve_rate: rust_decimal::Decimal::new(1, 1),
            risk: CopyRiskPolicy {
                max_total_notional: rust_decimal::Decimal::from(10),
                max_order_notional: rust_decimal::Decimal::from(5),
                max_leverage: rust_decimal::Decimal::ONE,
            },
            lifecycle: CopyLifecyclePolicy::Active,
        },
    };
    relation.validate()?;
    let (origin, server) = server(
        serde_json::to_vec(&Vec::<AccountDeliveryClaim>::new())?,
        3,
        vec![relation.clone()],
    )
    .await?;
    let launch = NodeLaunch::try_parse_from(
        VenueId::Bybit,
        [
            "venue-node-bybit",
            "--mode",
            "LIVE",
            "--trading-account-id",
            ACCOUNT,
            "--symbol",
            "DOGE/USDT",
            "--artifacts-base",
            temporary.path().to_str().ok_or("non-utf8 temporary path")?,
        ],
    )?;
    let strategy = crate::NodeRuntimeStrategy {
        strategy_kind: StrategyKind::Copy,
        instance_id: "copy-follower".to_owned(),
        run_id: "run-follower".to_owned(),
        config_digest: "follower-digest".to_owned(),
        config_epoch: 1,
        symbol: "DOGE/USDT".parse()?,
        copy_leader_capital: None,
        grid: None,
    };
    let config = NodeRuntimeConfig {
        version: crate::NODE_RUNTIME_CONFIG_VERSION,
        mode: GatewayMode::Live,
        venue: VenueId::Bybit,
        trading_account_id: ACCOUNT.to_owned(),
        node_id: "follower-node".to_owned(),
        control: crate::NodeControlLoopConfig {
            loopback_origin: origin,
            poll_interval_ms: 10,
            projection_interval_ms: 10,
            lease_duration_ms: 1_000,
            claim_limit: 1,
        },
        strategies: vec![strategy.clone()],
    };
    config.validate(&binding)?;
    let now = now_ms().map_err(|_| "clock")?;
    let loopback = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| ControlResidentLoopError::Signal)?;
        let mut resident = ProductionResident::open(
            &launch,
            Gateway {
                binding: binding.clone(),
                private_generation: Arc::new(AtomicU64::new(0)),
                // The signed account page is an exact zero surface with a USDT available-margin
                // fact. No Control delivery is installed before the first planning publication.
                shutdown: Some(Arc::new(std::sync::Mutex::new(ShutdownGatewayState {
                    open_order: false,
                    long_quantity: rust_decimal::Decimal::ZERO,
                    dispatched: Vec::new(),
                }))),
            },
        )
        .map_err(ControlResidentLoopError::Resident)?;
        let strategy_binding = config
            .binding_for(&strategy)
            .map_err(|_| ControlResidentLoopError::Config)?;
        resident
            .register_actor(strategy_binding.clone())
            .map_err(ControlResidentLoopError::Resident)?;
        if resident.strategy_lifecycle(&strategy_binding) != Some(InstanceLifecycle::Running) {
            return Err(ControlResidentLoopError::Config);
        }
        let mut loopback = ControlResidentLoop::open(&launch, &config, resident)?;
        if !loopback
            .copy_jobs
            .get("copy-follower")
            .ok_or(ControlResidentLoopError::Config)?
            .jobs()
            .is_empty()
        {
            return Err(ControlResidentLoopError::Config);
        }
        loopback.tick(&runtime, now)?;
        Ok::<_, ControlResidentLoopError>(loopback)
    })
    .await??;
    let requests = server.await??;
    let projection_body = requests
        .iter()
        .find(|(path, _)| path == "/v2/account-node/projection")
        .map(|(_, body)| body)
        .ok_or("projection request missing")?;
    let envelope: NodeProjectionEnvelope = serde_json::from_slice(projection_body)?;
    assert_eq!(envelope.copy_planning_facts.len(), 1);
    let fact = &envelope.copy_planning_facts[0];
    assert_eq!(
        fact.role,
        venue_control_protocol::CopyPlanningFactRole::Follower
    );
    assert_eq!(fact.binding.instance_id, "copy-follower");
    assert_eq!(fact.relation_id, relation.relation.relation_id);
    assert_eq!(fact.private_generation, 2);
    assert_eq!(fact.rules_generation, 1);
    assert_eq!(fact.quote_net_exposure.value, rust_decimal::Decimal::ZERO);
    assert_eq!(
        fact.follower_available_margin
            .as_ref()
            .map(|amount| amount.value),
        Some(rust_decimal::Decimal::new(9, 0))
    );
    assert!(fact.leader_configured_capital.is_none());
    assert_eq!(fact.validate(), Ok(()));
    drop(loopback);
    Ok(())
}

#[test]
fn flatten_physically_cancels_then_reduces_only_after_signed_zero()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = GatewayBinding::new(
        VenueId::Bybit,
        GatewayMode::Live,
        ACCOUNT,
        "DOGE/USDT".parse()?,
    )?;
    let launch = NodeLaunch::try_parse_from(
        VenueId::Bybit,
        [
            "venue-node-bybit",
            "--mode",
            "LIVE",
            "--trading-account-id",
            ACCOUNT,
            "--symbol",
            "DOGE/USDT",
            "--artifacts-base",
            temporary.path().to_str().ok_or("non-utf8 temporary path")?,
        ],
    )?;
    let strategy = crate::NodeRuntimeStrategy {
        strategy_kind: StrategyKind::HedgedGrid,
        instance_id: "grid-doge".to_owned(),
        run_id: "run-e2e".to_owned(),
        config_digest: "config-e2e".to_owned(),
        config_epoch: 1,
        symbol: "DOGE/USDT".parse()?,
        copy_leader_capital: None,
        grid: Some(crate::NodeGridRuntimeConfig {
            params: HedgedGridParams::fixed_release(Asset::new("USDT")?, 10)?,
            recovery: crate::NodeGridRecoveryPolicy::BootstrapWhenAbsent,
            skip_inventory_replenishment_until_recovered: false,
        }),
    };
    let config = NodeRuntimeConfig {
        version: crate::NODE_RUNTIME_CONFIG_VERSION,
        mode: GatewayMode::Live,
        venue: VenueId::Bybit,
        trading_account_id: ACCOUNT.to_owned(),
        node_id: "node-e2e".to_owned(),
        control: crate::NodeControlLoopConfig {
            loopback_origin: "http://127.0.0.1:9/".to_owned(),
            poll_interval_ms: 10,
            projection_interval_ms: 10,
            lease_duration_ms: 1_000,
            claim_limit: 1,
        },
        strategies: vec![strategy.clone()],
    };
    config.validate(&binding)?;
    let actor = config.binding_for(&strategy)?;

    // The adapter emits a native open order without trusting its own Owner field. The exact
    // accepted account-WAL identity below is what lets Host enrich it as this Actor's order.
    fs::create_dir_all(launch.artifacts_root())?;
    let mut wal =
        venue_runtime::CommandJournal::open(launch.artifacts_root().join("commands.jsonl"))?;
    let create = venue_domain::CommandId::new("recovered-shutdown-order")?;
    wal.prepare(venue_domain::ExecutionCommand::PlaceLimit(
        venue_domain::OrderCommand {
            time_in_force: Default::default(),
            command_id: create.clone(),
            client_order_id: venue_domain::CommandId::new("client-e2e")?,
            owner: OrderOwner {
                strategy_instance_id: actor.key.instance_id.clone(),
                run_id: actor.run_id.clone(),
                exchange: "bybit".to_owned(),
                account: ACCOUNT.to_owned(),
                symbol: actor.key.symbol.clone(),
                purpose: venue_domain::OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: 2.into(),
            limit_price: Price::new(rust_decimal::Decimal::new(12, 1))?,
            reduce_only: false,
        },
    ))?;
    wal.transition(&create, venue_runtime::CommandState::Submitted)?;
    wal.transition(
        &create,
        venue_runtime::CommandState::Accepted {
            venue_order_id: "venue-e2e".to_owned(),
        },
    )?;
    drop(wal);

    let state = Arc::new(std::sync::Mutex::new(ShutdownGatewayState {
        open_order: true,
        long_quantity: rust_decimal::Decimal::new(2, 0),
        dispatched: Vec::new(),
    }));
    let mut resident = ProductionResident::open(
        &launch,
        Gateway {
            binding: binding.clone(),
            private_generation: Arc::new(AtomicU64::new(0)),
            shutdown: Some(state.clone()),
        },
    )?;
    resident.register_actor(actor.clone())?;
    let mut loopback = ControlResidentLoop::open(&launch, &config, resident)?;
    loopback
        .resident
        .apply_control_action(&actor, ControlAction::Flatten)?;
    loopback
        .shutdowns
        .get_mut(&strategy.instance_id)
        .ok_or("shutdown journal missing")?
        .begin(ControlAction::Flatten)?;

    // Every stage consumes a fresh complete signed observation. The first host-WAL command
    // removes only the recovered self-owned order; the second is reduce-only for the signed
    // Long leg; only the third zero-order/zero-position observation is terminal.
    loopback.advance_control_shutdown(&strategy.instance_id)?;
    loopback.advance_control_shutdown(&strategy.instance_id)?;
    loopback.advance_control_shutdown(&strategy.instance_id)?;

    let state = state.lock().map_err(|_| "shutdown state poisoned")?;
    assert!(!state.open_order);
    assert!(state.long_quantity.is_zero());
    assert_eq!(state.dispatched.len(), 2);
    assert!(matches!(
        state.dispatched[0],
        venue_domain::ExecutionCommand::Cancel(_)
    ));
    assert!(matches!(
        state.dispatched[1],
        venue_domain::ExecutionCommand::MarketReduce(_)
    ));
    assert_eq!(
        loopback
            .shutdowns
            .get(&strategy.instance_id)
            .and_then(ControlShutdownJournal::operation)
            .ok_or("shutdown operation missing")?
            .phase,
        ShutdownPhase::Reconciled
    );
    Ok(())
}

#[test]
fn projection_keeps_owned_orders_when_status_or_cumulative_fill_is_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = StrategyBinding::new(
        venue_runtime::StrategyInstanceKey::new(
            venue_runtime::AccountKey::new(VenueId::Bybit, ACCOUNT.to_owned())?,
            StrategyKind::Copy,
            "copy-doge",
            "DOGE/USDT".parse()?,
        )?,
        "run-copy",
        "config-copy",
    )?;
    let snapshot = SignedAccountSnapshot::complete_with_fills(
        GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            ACCOUNT,
            "DOGE/USDT".parse()?,
        )?,
        1000,
        1,
        1,
        1,
        SignedAccountPositionMode::Net,
        vec![SignedAccountOrderFact {
            time_in_force: Some(Default::default()),
            client_order_id: "copy-open".to_owned(),
            venue_order_id: Some("native-open".to_owned()),
            symbol: binding.key.symbol.clone(),
            family: NativeOrderFamily::UmOrder,
            side: OrderSide::Buy,
            position_side: PositionSide::Net,
            quantity: 2.into(),
            limit_price: Some(1.into()),
            reduce_only: false,
            owner: Some(OrderOwner {
                strategy_instance_id: binding.key.instance_id.clone(),
                run_id: binding.run_id.clone(),
                exchange: "bybit".to_owned(),
                account: ACCOUNT.to_owned(),
                symbol: binding.key.symbol.clone(),
                purpose: venue_domain::OrderPurpose::Entry,
            }),
            external: false,
            state: None,
            filled_quantity: None,
        }],
        vec![SignedAccountPositionFact {
            symbol: binding.key.symbol.clone(),
            position_side: PositionSide::Net,
            quantity: 0.into(),
            entry_price: None,
            mark_price: Some(1.into()),
        }],
        Vec::new(),
        "cursor".to_owned(),
        Vec::new(),
    )?;
    let directory = tempfile::tempdir()?;
    let jobs = crate::CopyDeliveryJournal::recover(
        directory.path().join("copy.jsonl"),
        delivery_binding(&binding),
    )?;
    let (summary, facts) = projection_from_signed(
        &snapshot,
        &BTreeMap::new(),
        &binding,
        Some(InstanceLifecycle::Running),
        AccountHealth::Ready,
        false,
        1000,
        &jobs,
    )?;
    facts.validate()?;
    assert_eq!(summary.strategies[0].open_orders, 1);
    assert_eq!(facts.orders.len(), 1);
    assert_eq!(facts.orders[0].state, None);
    assert_eq!(facts.orders[0].filled_quantity, None);
    let json = serde_json::to_value(&facts)?;
    assert!(json["orders"][0]["state"].is_null());
    assert!(json["orders"][0]["filled_quantity"].is_null());
    let mut invalid = facts;
    invalid.orders[0].filled_quantity = Some(3.into());
    assert!(invalid.validate().is_err());
    Ok(())
}

#[test]
fn reconciled_copy_fills_share_signed_identity_and_keep_each_completed_phase()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: venue_domain::Symbol = "DOGE/USDT".parse()?;
    let binding = ExecutionFactBinding {
        venue: VenueId::Bybit,
        mode: GatewayMode::Live,
        trading_account_id: ACCOUNT.to_owned(),
        symbol: symbol.clone(),
        instance_id: "copy-doge".to_owned(),
        config_epoch: 1,
    };
    let position = AuthoritativePositionSnapshot {
        binding: DeliveryBinding {
            relation: RelationCommitment {
                relation_id: venue_copy::CopyId::parse("00000000-0000-4000-8000-000000000010")?,
                revision: 1,
                policy_digest: [1; 32],
            },
            leader_id: venue_copy::CopyId::parse("00000000-0000-4000-8000-000000000011")?,
            follower_id: venue_copy::CopyId::parse("00000000-0000-4000-8000-000000000012")?,
            follower_binding_id: venue_copy::CopyId::parse("00000000-0000-4000-8000-000000000013")?,
            follower_instance_id: binding.instance_id.clone(),
            account_id: ACCOUNT.to_owned(),
            instrument: InstrumentIdentity {
                symbol: symbol.clone(),
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(Asset::new("USDT")?),
            },
            policy_id: venue_copy::CopyId::parse("00000000-0000-4000-8000-000000000014")?,
        },
        generation: 9,
        observed_at_ms: 100,
        expires_at_ms: 200,
        exposure: venue_domain::Amount::new(Asset::new("USDT")?, 1.into()),
        fact_digest: [2; 32],
    };
    let fill = Fill {
        fill_id: "copy-fill-a".to_owned(),
        execution_sequence: FieldState::Known(7),
        order_id: "native-a".to_owned(),
        symbol: symbol.clone(),
        side: OrderSide::Buy,
        position_side: FieldState::Known(PositionSide::Long),
        quantity: 1.into(),
        price: Price::new(1.into())?,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Missing,
        exchange_time_ms: Some(100),
    };
    let direct = SignedFillFact {
        binding: binding.clone(),
        fill_id: fill.fill_id.clone(),
        order_id: fill.order_id.clone(),
        side: fill.side,
        position_side: Some(PositionSide::Long),
        quantity: fill.quantity,
        price: fill.price.value(),
        execution_sequence: Some(7),
        occurred_ms: 100,
        signed_generation: position.generation,
        fact_digest: projection_digest_for("fill", &fill)?,
    };
    let mut facts = vec![direct.clone()];
    let mut known = BTreeMap::from([(direct.fill_id.clone(), direct)]);
    let reconciled = venue_copy::CopyExecutionResult {
        request: venue_copy::CopyExecutionRequest {
            job_id: venue_copy::CopyId::parse("00000000-0000-4000-8000-000000000015")?,
            delivery_digest: [3; 32],
            binding: position.binding.clone(),
            target_generation: 1,
            position_generation: 8,
            target_exposure: position.exposure.clone(),
            current_exposure: position.exposure.clone(),
            requested_delta_exposure: position.exposure.clone(),
            phase: venue_copy::CopyExecutionPhase::Adjust,
        },
        state: venue_copy::CopyExecutionState::Reconciled,
        command_id: Some("copy-command".to_owned()),
        fact_digest: [4; 32],
        reconciled_position: Some(position.clone()),
        observed_at_ms: 100,
    };
    append_reconciled_copy_fill_set(
        &mut facts,
        &mut known,
        &reconciled,
        &position,
        std::slice::from_ref(&fill),
        &binding,
        100,
    )?;
    assert_eq!(facts.len(), 1);

    let mut prior_only = fill;
    prior_only.fill_id = "copy-fill-prior".to_owned();
    prior_only.order_id = "native-prior".to_owned();
    append_reconciled_copy_fill_set(
        &mut facts,
        &mut known,
        &reconciled,
        &position,
        std::slice::from_ref(&prior_only),
        &binding,
        100,
    )?;
    assert_eq!(facts.len(), 2);
    assert_eq!(
        facts[1].fact_digest,
        projection_digest_for("fill", &prior_only)?
    );
    Ok(())
}

async fn server(
    claim: Vec<u8>,
    count: usize,
    relations: Vec<CopyRelationRecord>,
) -> Result<
    (
        String,
        tokio::task::JoinHandle<Result<Vec<(String, Vec<u8>)>, io::Error>>,
    ),
    io::Error,
> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let mut paths = Vec::new();
        for index in 0..count {
            let (mut stream, _) = listener.accept().await?;
            let (path, body) = request(&mut stream).await?;
            let response = if path.ends_with("/claim") {
                if index == 0 {
                    claim.clone()
                } else {
                    b"[]".to_vec()
                }
            } else if path == "/v2/copy/relations" {
                serde_json::to_vec(&relations).map_err(io::Error::other)?
            } else {
                body.clone()
            };
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).as_bytes()).await?;
            stream.write_all(&response).await?;
            stream.shutdown().await?;
            paths.push((path, body));
        }
        Ok(paths)
    });
    Ok((format!("http://{address}/"), handle))
}

async fn request(stream: &mut tokio::net::TcpStream) -> Result<(String, Vec<u8>), io::Error> {
    let mut encoded = Vec::new();
    let header_end = loop {
        if let Some(index) = encoded.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await?;
        if read == 0 || encoded.len() > 128 * 1024 {
            return Err(io::Error::other("invalid test request"));
        }
        encoded.extend_from_slice(&chunk[..read]);
    };
    let headers = std::str::from_utf8(&encoded[..header_end])
        .map_err(|_| io::Error::other("invalid headers"))?;
    let path = headers
        .split("\r\n")
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .ok_or_else(|| io::Error::other("missing path"))?
        .to_owned();
    let length = headers
        .lines()
        .find_map(|line| {
            line.split_once(':')
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .and_then(|(_, value)| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    while encoded.len() < header_end.saturating_add(length) {
        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(io::Error::other("incomplete body"));
        }
        encoded.extend_from_slice(&chunk[..read]);
    }
    Ok((path, encoded[header_end..header_end + length].to_vec()))
}
