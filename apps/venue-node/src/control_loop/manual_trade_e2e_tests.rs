//! These fixtures traverse the durable Control inbox, resident checkpoint, account WAL and lane.
//! Only exchange reads and dispatch are faked.

use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryBinding, AccountDeliveryClaim,
    AccountDeliveryLease, AccountDeliveryPayload, AccountDeliveryPurpose, CONTROL_SCHEMA_VERSION,
    ControlAction, ControlCommandRequest, TradeIntent, TradingAction, TradingOrderType,
    TradingTimeInForce,
};
use venue_domain::{
    Asset, ExecutionCommand, InstrumentIdentity, MarketKind, NativeOrderFamily, OrderCommand,
    OrderState, PositionSide,
};
use venue_gateway_api::{GatewayBinding, GatewayMode, VenueId};
use venue_runtime::{
    AccountGatewayResult, AccountHostValidationError, AccountInstrumentIdentity,
    AccountPhysicalGateway, AccountPricedLimitIntent, AccountRecoveryOutcome,
    AccountRecoveryReport, AccountRecoveryRequest, AccountRiskEvidence, SignedAccountOrderFact,
    SignedAccountPositionFact, SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact,
    SignedUnknownResult, StrategyBinding, StrategyInstanceKey, StrategyKind,
};

use super::*;
use crate::{ActorDeliveryTurn, ClaimAcceptance};

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";
const REQUEST_ID: &str = "manual-e2e-request";
const INSTANCE_ID: &str = "manual-e2e";

struct GatewayState {
    private_generation: u64,
    dispatches: usize,
    return_unknown: bool,
    accepted_order: Option<OrderCommand>,
}

struct ManualGateway {
    binding: GatewayBinding,
    state: Arc<Mutex<GatewayState>>,
}

impl AccountPhysicalGateway for ManualGateway {
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
            now_ms()?,
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
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        state.private_generation = state.private_generation.saturating_add(1);
        let private_generation = state.private_generation;
        let open_orders = state
            .accepted_order
            .as_ref()
            .map(|order| SignedAccountOrderFact {
                created_at_ms: Some(1),
                time_in_force: Some(order.time_in_force),
                client_order_id: order.client_order_id.as_str().to_owned(),
                venue_order_id: Some("manual-venue-order".to_owned()),
                symbol: order.owner.symbol.clone(),
                family: NativeOrderFamily::UmOrder,
                side: order.side,
                position_side: order.position_side,
                quantity: order.quantity,
                limit_price: Some(order.limit_price.value()),
                reduce_only: order.reduce_only,
                owner: Some(order.owner.clone()),
                external: false,
                state: Some(OrderState::New),
                filled_quantity: Some(Decimal::ZERO),
            })
            .into_iter()
            .collect();
        SignedAccountSnapshot::complete(
            self.binding.clone(),
            now_ms().map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            1,
            private_generation,
            1,
            SignedAccountPositionMode::Hedge,
            open_orders,
            vec![
                SignedAccountPositionFact {
                    symbol: self.binding.symbol.clone(),
                    position_side: PositionSide::Long,
                    quantity: Decimal::ZERO,
                    entry_price: None,
                    mark_price: Some(Decimal::ONE),
                },
                SignedAccountPositionFact {
                    symbol: self.binding.symbol.clone(),
                    position_side: PositionSide::Short,
                    quantity: Decimal::ZERO,
                    entry_price: None,
                    mark_price: Some(Decimal::ONE),
                },
            ],
            format!("manual-e2e-fills:{private_generation}"),
            request
                .unresolved()
                .iter()
                .map(|command| SignedUnknownFact {
                    command_id: command.command_id().clone(),
                    result: SignedUnknownResult::Unknown,
                })
                .collect(),
        )
    }

    fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        Ok(AccountInstrumentIdentity {
            identity: InstrumentIdentity {
                symbol: self.binding.symbol.clone(),
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(
                    Asset::new("USDT").map_err(|_| AccountHostValidationError::Instrument)?,
                ),
            },
            rules_generation: 1,
        })
    }

    fn normalize_priced_limit_intent(
        &mut self,
        intent: &AccountPricedLimitIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        let quantity = intent
            .intent
            .quote_delta
            .checked_div(intent.limit_price.value())
            .ok_or(AccountHostValidationError::Command)?;
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            time_in_force: intent.time_in_force,
            command_id: intent.intent.command_id.clone(),
            client_order_id: intent.intent.client_order_id.clone(),
            owner: intent.intent.owner.clone(),
            side: intent.intent.side,
            position_side: intent.intent.position_side,
            quantity,
            limit_price: intent.limit_price,
            reduce_only: intent.intent.reduce_only,
        }))
    }

    fn dispatch(&mut self, permit: venue_runtime::AccountDispatchPermit) -> AccountGatewayResult {
        let Ok(mut state) = self.state.lock() else {
            return AccountGatewayResult::Unknown;
        };
        state.dispatches = state.dispatches.saturating_add(1);
        if state.return_unknown {
            return AccountGatewayResult::Unknown;
        }
        let ExecutionCommand::PlaceLimit(order) = permit.command() else {
            return AccountGatewayResult::Unknown;
        };
        state.accepted_order = Some(order.clone());
        AccountGatewayResult::Accepted {
            venue_order_id: "manual-venue-order".to_owned(),
        }
    }
}

fn now_ms() -> Result<u64, io::Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_millis()
        .try_into()
        .map_err(io::Error::other)
}

fn launch(root: &Path) -> Result<NodeLaunch, Box<dyn std::error::Error>> {
    Ok(NodeLaunch::try_parse_from(
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
            root.to_str().ok_or("non-utf8 test root")?,
        ],
    )?)
}

fn binding() -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    Ok(StrategyBinding::new(
        StrategyInstanceKey::new(
            venue_runtime::AccountKey::new(VenueId::Bybit, ACCOUNT.to_owned())?,
            StrategyKind::HedgedGrid,
            INSTANCE_ID,
            "DOGE/USDT".parse()?,
        )?,
        "manual-e2e-run",
        "manual-e2e-config",
    )?)
}

fn open_resident(
    launch: &NodeLaunch,
    state: Arc<Mutex<GatewayState>>,
) -> Result<ProductionResident<ManualGateway>, Box<dyn std::error::Error>> {
    Ok(ProductionResident::open(
        launch,
        ManualGateway {
            binding: launch.binding().clone(),
            state,
        },
    )?)
}

fn initialized_resident(
    root: &Path,
    return_unknown: bool,
) -> Result<
    (
        ProductionResident<ManualGateway>,
        Arc<Mutex<GatewayState>>,
        StrategyBinding,
        NodeLaunch,
    ),
    Box<dyn std::error::Error>,
> {
    let launch = launch(root)?;
    let state = Arc::new(Mutex::new(GatewayState {
        private_generation: 0,
        dispatches: 0,
        return_unknown,
        accepted_order: None,
    }));
    let strategy = binding()?;
    let mut seed = open_resident(&launch, state.clone())?;
    seed.register_actor(strategy.clone())?;
    // A Pause is a normal durable Grid actor turn. Reopening through the resident bootstrap is
    // what promotes the exact recovered binding back to Running; calling Resume alone only
    // correctly enters Recovering and therefore cannot authorize a manual opening order.
    seed.apply_control_action(&strategy, ControlAction::Pause)
        .map_err(|error| io::Error::other(format!("grid pause: {error}")))?;
    drop(seed);
    let mut resident = open_resident(&launch, state.clone())?;
    resident.register_actor(strategy.clone())?;
    Ok((resident, state, strategy, launch))
}

fn actor_turn(
    root: &Path,
    delivery_id: &str,
) -> Result<ActorDeliveryTurn, Box<dyn std::error::Error>> {
    let observed_ms = now_ms()?;
    let command = ControlCommandRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: REQUEST_ID.to_owned(),
        venue: VenueId::Bybit,
        mode: GatewayMode::Live,
        trading_account_id: ACCOUNT.to_owned(),
        instance_id: INSTANCE_ID.to_owned(),
        symbol: "DOGE/USDT".parse()?,
        action: ControlAction::Trade,
        trade: Some(TradeIntent {
            action: TradingAction::OpenLong,
            quote_asset: "USDT".to_owned(),
            order_type: TradingOrderType::Limit,
            time_in_force: TradingTimeInForce::Gtc,
            post_only: false,
            reduce_only: false,
            selected_price: Some(Decimal::ONE),
            quote_notional: Some(Decimal::ONE),
            close_quantity_cap: None,
            selected_order_id: None,
        }),
        expected_config_epoch: 1,
        confirmation: None,
    };
    let delivery_binding = AccountDeliveryBinding {
        venue: VenueId::Bybit,
        mode: GatewayMode::Live,
        trading_account_id: ACCOUNT.to_owned(),
        symbol: "DOGE/USDT".parse()?,
        instance_id: INSTANCE_ID.to_owned(),
        config_epoch: 1,
    };
    let journal = OpaqueControlDeliveryJournal::open(root.join(format!("{delivery_id}.jsonl")))?;
    let mut inbox = ControlDeliveryInbox::recover(journal, delivery_binding, "manual-e2e-node")?;
    let claim = AccountDeliveryClaim {
        lease: AccountDeliveryLease {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            delivery_id: delivery_id.to_owned(),
            binding: inbox.binding().clone(),
            node_id: "manual-e2e-node".to_owned(),
            lease_epoch: 1,
            leased_at_ms: observed_ms.saturating_sub(1),
            expires_at_ms: observed_ms.saturating_add(10_000),
            purpose: AccountDeliveryPurpose::Install,
        },
        payload: AccountDeliveryPayload::ControlCommand(command),
    };
    let ClaimAcceptance::Install(acknowledgement) = inbox.accept_claim(claim, observed_ms)? else {
        return Err("manual install became a reconcile claim".into());
    };
    inbox.confirm_acknowledgement(acknowledgement.value(), observed_ms)?;
    inbox
        .pending_actor_turns(observed_ms)?
        .pop()
        .ok_or_else(|| "manual actor turn is missing".into())
}

fn is_applied(outcome: crate::production_resident::manual::ManualTradeOutcome) -> bool {
    matches!(
        outcome,
        crate::production_resident::manual::ManualTradeOutcome::Applied(_)
    )
}

#[test]
fn manual_trade_redelivery_and_restart_never_redispatches() -> Result<(), Box<dyn std::error::Error>>
{
    let root = tempfile::tempdir()?;
    let (mut resident, state, strategy, launch) = initialized_resident(root.path(), false)?;

    assert!(is_applied(resident.apply_manual_trade(
        &strategy,
        &actor_turn(root.path(), "first")?
    )?));
    assert!(is_applied(resident.apply_manual_trade(
        &strategy,
        &actor_turn(root.path(), "redelivery")?
    )?));
    assert_eq!(
        state
            .lock()
            .map_err(|_| "gateway mutex poisoned")?
            .dispatches,
        1
    );

    drop(resident);
    let mut reopened = open_resident(&launch, state.clone())?;
    reopened.register_actor(strategy.clone())?;
    assert!(is_applied(reopened.apply_manual_trade(
        &strategy,
        &actor_turn(root.path(), "after-restart")?
    )?));
    assert_eq!(
        state
            .lock()
            .map_err(|_| "gateway mutex poisoned")?
            .dispatches,
        1
    );
    Ok(())
}

#[test]
fn manual_unknown_survives_restart_without_redispatch() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let (mut resident, state, strategy, launch) = initialized_resident(root.path(), true)?;

    assert!(matches!(
        resident.apply_manual_trade(&strategy, &actor_turn(root.path(), "unknown-first")?)?,
        crate::production_resident::manual::ManualTradeOutcome::Unknown { .. }
    ));
    assert!(matches!(
        resident.apply_manual_trade(&strategy, &actor_turn(root.path(), "unknown-redelivery")?)?,
        crate::production_resident::manual::ManualTradeOutcome::Unknown { .. }
    ));
    assert_eq!(
        state
            .lock()
            .map_err(|_| "gateway mutex poisoned")?
            .dispatches,
        1
    );

    drop(resident);
    let mut reopened = open_resident(&launch, state.clone())?;
    reopened.register_actor(strategy.clone())?;
    assert!(matches!(
        reopened.apply_manual_trade(
            &strategy,
            &actor_turn(root.path(), "unknown-after-restart")?
        )?,
        crate::production_resident::manual::ManualTradeOutcome::Unknown { .. }
    ));
    assert_eq!(
        state
            .lock()
            .map_err(|_| "gateway mutex poisoned")?
            .dispatches,
        1
    );
    Ok(())
}
