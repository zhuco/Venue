use std::{io, io::Write};

use rust_decimal::Decimal;
use tempfile::TempDir;
use venue_domain::domain::{
    CancelCommand, FieldState, Fill, LimitTimeInForce, MarketReduceCommand, Order, OrderCommand,
    OrderOwner, OrderPurpose, OrderSide, OrderState, PositionSide, Price,
};
use venue_gateway_api::{GatewayMode, VenueId};
use venue_storage::DurableWalHead;

use super::*;

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

#[derive(Debug)]
struct Gateway {
    binding: GatewayBinding,
    result: AccountGatewayResult,
    dispatches: usize,
}

#[derive(Debug)]
struct LegacySnapshotGateway {
    binding: GatewayBinding,
    snapshot: SignedAccountSnapshot,
}

impl AccountPhysicalGateway for LegacySnapshotGateway {
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

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        if request.binding() != &self.binding {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        Ok(self.snapshot.clone())
    }

    fn signed_client_order_id_matches(&self, canonical: &CommandId, signed: &str) -> bool {
        canonical.as_str() == signed
    }

    fn dispatch(&mut self, _: AccountDispatchPermit) -> AccountGatewayResult {
        AccountGatewayResult::Unknown
    }
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
        let outcomes = request
            .unresolved()
            .iter()
            .map(|command| AccountRecoveryOutcome::still_unknown(command.command_id().clone()))
            .collect();
        AccountRecoveryReport::new(self.binding.clone(), 1, outcomes).map_err(io::Error::other)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        risk_evidence(self.binding.clone(), Vec::new(), Vec::new())
    }

    fn dispatch(&mut self, _permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatches += 1;
        self.result.clone()
    }
}

#[derive(Debug)]
struct RecoveringGateway {
    binding: GatewayBinding,
    dispatches: usize,
}

#[derive(Debug)]
struct NetGateway {
    binding: GatewayBinding,
    quantity: Decimal,
    private_generation: u64,
    fills: Vec<Fill>,
    result: AccountGatewayResult,
    settle_unknown_to_rejected: bool,
    dispatches: usize,
}

impl AccountPhysicalGateway for NetGateway {
    type Error = io::Error;

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        let outcomes = request
            .unresolved()
            .iter()
            .map(|command| AccountRecoveryOutcome::still_unknown(command.command_id().clone()))
            .collect();
        AccountRecoveryReport::new(self.binding.clone(), 1, outcomes).map_err(io::Error::other)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        risk_evidence(self.binding.clone(), Vec::new(), Vec::new())
    }

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        SignedAccountSnapshot::complete_with_fills(
            self.binding.clone(),
            now_ms()?,
            1,
            self.private_generation,
            1,
            SignedAccountPositionMode::Net,
            Vec::new(),
            vec![SignedAccountPositionFact {
                symbol: self.binding.symbol.clone(),
                position_side: PositionSide::Net,
                quantity: self.quantity,
                entry_price: Some(Decimal::ONE),
                mark_price: Some(Decimal::ONE),
            }],
            self.fills.clone(),
            "net-fills:0".to_owned(),
            request
                .unresolved()
                .iter()
                .map(|command| SignedUnknownFact {
                    command_id: command.command_id().clone(),
                    result: if self.settle_unknown_to_rejected {
                        SignedUnknownResult::Rejected {
                            reason: "signed-terminal-rejection".to_owned(),
                        }
                    } else {
                        SignedUnknownResult::Unknown
                    },
                })
                .collect(),
        )
    }

    fn dispatch(&mut self, _permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatches = self.dispatches.saturating_add(1);
        self.result.clone()
    }
}

impl AccountPhysicalGateway for RecoveringGateway {
    type Error = io::Error;

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        let outcomes = request
            .unresolved()
            .iter()
            .map(|command| {
                AccountRecoveryOutcome::accepted(
                    command.command_id().clone(),
                    "recovered-order".to_owned(),
                )
            })
            .collect();
        AccountRecoveryReport::new(self.binding.clone(), 2, outcomes).map_err(io::Error::other)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        risk_evidence(self.binding.clone(), Vec::new(), Vec::new())
    }

    fn dispatch(&mut self, _permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatches += 1;
        AccountGatewayResult::Unknown
    }
}

fn binding() -> Result<GatewayBinding, Box<dyn std::error::Error>> {
    Ok(GatewayBinding::new(
        VenueId::Okx,
        GatewayMode::Live,
        ACCOUNT,
        "DOGE/USDT".parse()?,
    )?)
}

fn root(temp: &TempDir) -> PathBuf {
    temp.path().join("okx").join("LIVE").join(ACCOUNT)
}

fn command(notional: Decimal) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
    let identity = notional.normalize().to_string().replace('.', "-");
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        time_in_force: Default::default(),
        command_id: CommandId::new(format!("cmd-{identity}"))?,
        client_order_id: CommandId::new(format!("client-{identity}"))?,
        owner: owner()?,
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(notional)?,
        reduce_only: false,
    }))
}

fn indexed_command(index: usize) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        time_in_force: Default::default(),
        command_id: CommandId::new(format!("cmd-segment-{index}"))?,
        client_order_id: CommandId::new(format!("client-segment-{index}"))?,
        owner: owner()?,
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::ONE)?,
        reduce_only: false,
    }))
}

#[test]
fn signed_order_cannot_confirm_a_different_or_missing_limit_policy()
-> Result<(), Box<dyn std::error::Error>> {
    use venue_domain::LimitTimeInForce;
    let mut execution = command(Decimal::ONE)?;
    let ExecutionCommand::PlaceLimit(place) = &mut execution else {
        return Err("limit required".into());
    };
    place.time_in_force = LimitTimeInForce::Gtc;
    let mut fact = SignedAccountOrderFact {
        created_at_ms: None,
        client_order_id: place.client_order_id.as_str().to_owned(),
        venue_order_id: Some("native-policy-1".to_owned()),
        symbol: place.owner.symbol.clone(),
        family: venue_domain::NativeOrderFamily::UmOrder,
        side: place.side,
        position_side: place.position_side,
        quantity: place.quantity,
        limit_price: Some(place.limit_price.value()),
        time_in_force: None,
        reduce_only: place.reduce_only,
        owner: Some(place.owner.clone()),
        external: false,
        state: Some(venue_domain::OrderState::New),
        filled_quantity: Some(Decimal::ZERO),
    };
    assert!(!command_matches_signed_order(&execution, &fact));
    let legacy = serde_json::to_value(&fact)?;
    assert!(legacy.get("time_in_force").is_none());
    assert!(legacy.get("created_at_ms").is_none());
    let recovered: SignedAccountOrderFact = serde_json::from_value(legacy.clone())?;
    assert_eq!(recovered.time_in_force, None);
    assert_eq!(recovered.created_at_ms, None);
    assert_eq!(serde_json::to_value(&recovered)?, legacy);
    assert!(!command_matches_signed_order(&execution, &recovered));
    fact.time_in_force = Some(LimitTimeInForce::PostOnly);
    assert!(!command_matches_signed_order(&execution, &fact));
    fact.time_in_force = Some(LimitTimeInForce::Gtc);
    assert!(command_matches_signed_order(&execution, &fact));
    fact.created_at_ms = Some(1_700_000_000_125);
    let recovered: SignedAccountOrderFact = serde_json::from_slice(&serde_json::to_vec(&fact)?)?;
    assert_eq!(recovered.created_at_ms, fact.created_at_ms);
    assert!(command_matches_signed_order(&execution, &recovered));
    Ok(())
}

#[test]
fn legacy_custody_routes_require_the_frozen_owner_wal_and_signed_open_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = GatewayBinding::new(
        VenueId::Gate,
        GatewayMode::Live,
        ACCOUNT,
        "DOGE/USDT".parse()?,
    )?;
    let artifacts = temporary.path().join("gate").join("LIVE").join(ACCOUNT);
    std::fs::create_dir_all(&artifacts)?;
    let owner = OrderOwner {
        strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "gate".to_owned(),
        account: "usdt_futures".to_owned(),
        symbol: binding.symbol.clone(),
        purpose: OrderPurpose::Entry,
    };
    let command = ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: CommandId::new("legacy-create")?,
        client_order_id: CommandId::new("legacy-client")?,
        owner: owner.clone(),
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::ONE)?,
        time_in_force: LimitTimeInForce::PostOnly,
        reduce_only: false,
    });
    let mut journal = CommandJournal::open(artifacts.join("commands.jsonl"))?;
    journal.prepare(command.clone())?;
    journal.transition(command.command_id(), CommandState::Submitted)?;
    journal.transition(
        command.command_id(),
        CommandState::Accepted {
            venue_order_id: "legacy-native".to_owned(),
        },
    )?;
    drop(journal);
    let ExecutionCommand::PlaceLimit(order) = &command else {
        return Err("limit command required".into());
    };
    let snapshot = SignedAccountSnapshot::complete(
        binding.clone(),
        now_ms()?,
        1,
        1,
        1,
        SignedAccountPositionMode::Hedge,
        vec![SignedAccountOrderFact {
            client_order_id: order.client_order_id.as_str().to_owned(),
            venue_order_id: Some("legacy-native".to_owned()),
            symbol: order.owner.symbol.clone(),
            family: command.native_order_family().ok_or("family missing")?,
            side: order.side,
            position_side: order.position_side,
            quantity: order.quantity,
            limit_price: Some(order.limit_price.value()),
            time_in_force: Some(order.time_in_force),
            created_at_ms: Some(1),
            reduce_only: order.reduce_only,
            owner: None,
            external: true,
            state: Some(OrderState::New),
            filled_quantity: Some(Decimal::ZERO),
        }],
        Vec::new(),
        "legacy-fills:0".to_owned(),
        Vec::new(),
    )?;
    let gateway = LegacySnapshotGateway {
        binding: binding.clone(),
        snapshot,
    };
    let mut host = AccountMutationHost::open(artifacts, binding, Decimal::TEN, gateway)?;
    host.legacy_v1_predecessor = Some(LegacyV1WriterPredecessor {
        exchange: VenueId::Gate,
        successor_trading_account_id: ACCOUNT.to_owned(),
        legacy_product_account: owner.account.clone(),
        legacy_symbol: owner.symbol.clone(),
        legacy_owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
        legacy_strategy_instance_id: owner.strategy_instance_id.clone(),
        legacy_run_id: owner.run_id.clone(),
        legacy_artifacts_root: temporary.path().to_path_buf(),
        legacy_lock_sha256: "0".repeat(64),
        legacy_lock_path: temporary.path().join("legacy.lock"),
        handoff_sha256: "0".repeat(64),
    });

    let routes = host.refresh_legacy_v1_custody_routes()?;
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].owner, owner);
    assert_eq!(routes[0].client_order_id.as_str(), "legacy-client");
    assert_eq!(routes[0].venue_order_id, "legacy-native");
    assert_eq!(host.gateway.snapshot.open_orders()[0].owner, None);
    Ok(())
}

#[test]
fn legacy_custody_routes_reject_terminal_or_fully_filled_signed_orders()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let binding = GatewayBinding::new(
        VenueId::Gate,
        GatewayMode::Live,
        ACCOUNT,
        "DOGE/USDT".parse()?,
    )?;
    let artifacts = temporary.path().join("gate").join("LIVE").join(ACCOUNT);
    std::fs::create_dir_all(&artifacts)?;
    let owner = OrderOwner {
        strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "gate".to_owned(),
        account: "usdt_futures".to_owned(),
        symbol: binding.symbol.clone(),
        purpose: OrderPurpose::Entry,
    };
    let command = ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: CommandId::new("legacy-terminal-create")?,
        client_order_id: CommandId::new("legacy-terminal-client")?,
        owner: owner.clone(),
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::ONE)?,
        time_in_force: LimitTimeInForce::PostOnly,
        reduce_only: false,
    });
    let mut journal = CommandJournal::open(artifacts.join("commands.jsonl"))?;
    journal.prepare(command.clone())?;
    journal.transition(command.command_id(), CommandState::Submitted)?;
    journal.transition(
        command.command_id(),
        CommandState::Accepted {
            venue_order_id: "legacy-terminal-native".to_owned(),
        },
    )?;
    drop(journal);
    let ExecutionCommand::PlaceLimit(order) = &command else {
        return Err("limit command required".into());
    };
    let snapshot = SignedAccountSnapshot::complete(
        binding.clone(),
        now_ms()?,
        1,
        1,
        1,
        SignedAccountPositionMode::Hedge,
        vec![SignedAccountOrderFact {
            client_order_id: order.client_order_id.as_str().to_owned(),
            venue_order_id: Some("legacy-terminal-native".to_owned()),
            symbol: order.owner.symbol.clone(),
            family: command.native_order_family().ok_or("family missing")?,
            side: order.side,
            position_side: order.position_side,
            quantity: order.quantity,
            limit_price: Some(order.limit_price.value()),
            time_in_force: Some(order.time_in_force),
            created_at_ms: Some(1),
            reduce_only: order.reduce_only,
            owner: None,
            external: true,
            state: Some(OrderState::Filled),
            filled_quantity: Some(Decimal::ONE),
        }],
        Vec::new(),
        "legacy-fills:terminal".to_owned(),
        Vec::new(),
    )?;
    let mut host = AccountMutationHost::open(
        artifacts,
        binding.clone(),
        Decimal::TEN,
        LegacySnapshotGateway {
            binding: binding.clone(),
            snapshot,
        },
    )?;
    host.legacy_v1_predecessor = Some(LegacyV1WriterPredecessor {
        exchange: VenueId::Gate,
        successor_trading_account_id: ACCOUNT.to_owned(),
        legacy_product_account: owner.account.clone(),
        legacy_symbol: owner.symbol.clone(),
        legacy_owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
        legacy_strategy_instance_id: owner.strategy_instance_id.clone(),
        legacy_run_id: owner.run_id.clone(),
        legacy_artifacts_root: temporary.path().to_path_buf(),
        legacy_lock_sha256: "0".repeat(64),
        legacy_lock_path: temporary.path().join("legacy.lock"),
        handoff_sha256: "0".repeat(64),
    });
    assert!(matches!(
        host.refresh_legacy_v1_custody_routes(),
        Err(AccountHostError::Validation(
            AccountHostValidationError::LegacyPredecessor
        ))
    ));
    Ok(())
}

#[test]
fn unknown_readback_requires_complete_order_semantics_and_terminal_cancel()
-> Result<(), Box<dyn std::error::Error>> {
    let command = command(Decimal::ONE)?;
    let ExecutionCommand::PlaceLimit(place) = &command else {
        return Err("limit required".into());
    };
    let mut order = Order {
        order_id: "native-readback-1".to_owned(),
        client_order_id: FieldState::Known(place.client_order_id.as_str().to_owned()),
        symbol: place.owner.symbol.clone(),
        side: place.side,
        position_side: FieldState::Known(place.position_side),
        purpose: FieldState::Missing,
        state: OrderState::New,
        quantity: place.quantity,
        filled_quantity: Decimal::ZERO,
        limit_price: Some(place.limit_price),
        time_in_force: FieldState::Known(LimitTimeInForce::PostOnly),
        average_price: FieldState::Missing,
        reduce_only: place.reduce_only,
    };
    assert!(command_matches_readback_order(&command, &order));
    order.limit_price = Some(Price::new(Decimal::from(2))?);
    assert!(!command_matches_readback_order(&command, &order));

    let cancel = ExecutionCommand::Cancel(CancelCommand {
        command_id: CommandId::new("cancel-readback-1")?,
        owner: place.owner.clone(),
        target_client_order_id: place.client_order_id.clone(),
    });
    order.limit_price = Some(place.limit_price);
    assert!(!command_matches_readback_order(&cancel, &order));
    order.state = OrderState::Cancelled;
    assert!(command_matches_readback_order(&cancel, &order));
    Ok(())
}

fn owner() -> Result<OrderOwner, Box<dyn std::error::Error>> {
    Ok(OrderOwner {
        strategy_instance_id: "canary".to_owned(),
        run_id: "run-1".to_owned(),
        exchange: "okx".to_owned(),
        account: ACCOUNT.to_owned(),
        symbol: "DOGE/USDT".parse()?,
        purpose: OrderPurpose::Entry,
    })
}

fn net_reduce(
    command_id: &str,
    quantity: Decimal,
    position_generation: u64,
) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
    Ok(ExecutionCommand::MarketReduce(MarketReduceCommand {
        command_id: CommandId::new(command_id)?,
        client_order_id: CommandId::new(format!("client-{command_id}"))?,
        owner: OrderOwner {
            purpose: OrderPurpose::ExposureTakeProfit,
            ..owner()?
        },
        side: OrderSide::Sell,
        position_side: PositionSide::Net,
        quantity,
        risk_episode_id: CommandId::new(format!("episode-{command_id}"))?,
        position_generation,
    }))
}

fn net_fill(
    fill_id: &str,
    venue_order_id: &str,
    quantity: Decimal,
) -> Result<Fill, Box<dyn std::error::Error>> {
    Ok(Fill {
        fill_id: fill_id.to_owned(),
        execution_sequence: FieldState::Known(1),
        order_id: venue_order_id.to_owned(),
        symbol: "DOGE/USDT".parse()?,
        side: OrderSide::Sell,
        position_side: FieldState::Known(PositionSide::Net),
        quantity,
        price: Price::new(Decimal::ONE)?,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Missing,
        exchange_time_ms: Some(now_ms()?),
    })
}

fn risk_evidence(
    binding: GatewayBinding,
    signed_position_notionals: Vec<Decimal>,
    open_entry_order_notionals: Vec<Decimal>,
) -> Result<AccountRiskEvidence, AccountHostValidationError> {
    AccountRiskEvidence::complete(
        binding,
        now_ms()?,
        1,
        signed_position_notionals,
        open_entry_order_notionals,
    )
}

#[test]
fn host_persists_submitted_before_one_dispatch_and_records_unknown()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    assert_eq!(
        host.dispatch(command(Decimal::TEN)?)?,
        AccountDispatchOutcome::Unknown
    );
    assert!(host.has_unresolved());
    assert_eq!(host.gateway.dispatches, 1);
    Ok(())
}

#[test]
fn command_snapshot_clones_only_the_durable_wal_command() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    let command = command(Decimal::TEN)?;
    let command_id = command.command_id().clone();

    assert_eq!(host.command_snapshot(&command_id), None);
    let _prepared = host.prepare_for_lane(command.clone())?;
    assert_eq!(host.command_snapshot(&command_id), Some(command));
    assert_eq!(host.gateway.dispatches, 0);
    Ok(())
}

#[test]
fn restart_keeps_the_pre_dispatch_wal_prefix_and_rejects_a_forged_tail()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let checkpoint_head;
    {
        let gateway = Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Accepted {
                venue_order_id: "accepted-1".to_owned(),
            },
            dispatches: 0,
        };
        let mut host =
            AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, gateway)?;
        checkpoint_head = host.runtime_wal_head()?;
        assert!(matches!(
            host.dispatch(command(Decimal::TEN)?)?,
            AccountDispatchOutcome::Accepted { .. }
        ));
        assert_eq!(host.gateway.dispatches, 1);
    }

    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let reopened = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    assert!(reopened.validates_historical_wal_head(checkpoint_head));
    let current = reopened.runtime_wal_head()?;
    let forged = DurableWalHead::new(
        current.root_sha256(),
        current
            .tail_sequence()
            .checked_add(1)
            .ok_or("tail overflow")?,
        current.record_count(),
    )?;
    assert!(!reopened.validates_historical_wal_head(forged));
    Ok(())
}

#[test]
fn prepared_capability_is_idempotent_and_cannot_be_replayed_after_submit()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Accepted {
            venue_order_id: "prepared-1".to_owned(),
        },
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    let command = command(Decimal::TEN)?;
    let first = host.prepare_for_lane(command.clone())?;
    let replay = host.prepare_for_lane(command)?;
    assert_eq!(first.command_id(), replay.command_id());
    assert!(matches!(
        host.dispatch_prepared(first)?,
        AccountDispatchOutcome::Accepted { .. }
    ));
    assert!(matches!(
        host.dispatch_prepared(replay),
        Err(AccountHostError::Validation(
            AccountHostValidationError::PreparedCommand
        ))
    ));
    assert_eq!(host.gateway.dispatches, 1);
    Ok(())
}

#[test]
fn net_reductions_require_fresh_signed_position_generation_and_reserve_pending_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = NetGateway {
        binding: binding.clone(),
        quantity: Decimal::ONE,
        private_generation: 7,
        fills: Vec::new(),
        result: AccountGatewayResult::Accepted {
            venue_order_id: "net-reduce".to_owned(),
        },
        settle_unknown_to_rejected: false,
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;

    // A WAL shape alone never authorizes Net semantics; no signed cache means fail closed.
    assert!(matches!(
        host.prepare_for_lane(net_reduce("net-without-signed", Decimal::ONE, 7)?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::SignedSnapshot
        ))
    ));
    let snapshot = host.refresh_signed_snapshot()?;
    assert_eq!(snapshot.private_generation(), 7);
    let first = host.prepare_for_lane(net_reduce("net-first", Decimal::ONE, 7)?)?;

    // The exact signed 1-unit position is already fully reserved by the Prepared reduction.
    assert!(matches!(
        host.prepare_for_lane(net_reduce("net-over-reduce", Decimal::ONE, 7)?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::Command
        ))
    ));
    assert!(matches!(
        host.prepare_for_lane(net_reduce("net-old-generation", Decimal::ONE, 6)?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::SignedSnapshot
        ))
    ));
    host.latest_signed_snapshot
        .as_mut()
        .ok_or("signed snapshot missing")?
        .expire_for_test();
    assert!(matches!(
        host.dispatch_prepared(first),
        Err(AccountHostError::Validation(
            AccountHostValidationError::SignedSnapshot
        ))
    ));
    assert!(matches!(
        host.command_status(&CommandId::new("net-first")?)?
            .ok_or("status missing")?
            .state(),
        CommandState::Rejected { reason } if reason == "dispatch_signed_position_recheck_failed"
    ));
    assert_eq!(host.gateway.dispatches, 0);
    Ok(())
}

#[test]
fn unknown_net_reduce_reservation_survives_later_signed_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = NetGateway {
        binding: binding.clone(),
        quantity: Decimal::ONE,
        private_generation: 7,
        fills: Vec::new(),
        result: AccountGatewayResult::Unknown,
        settle_unknown_to_rejected: false,
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;

    host.refresh_signed_snapshot()?;
    let prepared = host.prepare_for_lane(net_reduce("net-unknown-old", Decimal::ONE, 7)?)?;
    assert!(matches!(
        host.dispatch_prepared(prepared)?,
        AccountDispatchOutcome::Unknown
    ));

    // This represents a later adapter attempt, not a Host-side rewrite of a repeated fact.
    host.gateway.private_generation = 8;
    let refreshed = host.refresh_signed_snapshot()?;
    assert_eq!(refreshed.private_generation(), 8);

    // The earlier Unknown has not been signed away, so a newer fact cannot admit a second
    // reduction against the same one-unit Net position.
    assert!(matches!(
        host.prepare_for_lane(net_reduce("net-after-unknown", Decimal::ONE, 8)?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::Command
        ))
    ));
    assert_eq!(host.gateway.dispatches, 1);
    Ok(())
}

#[test]
fn exact_signed_rejection_releases_an_unknown_net_reduce_reservation()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = NetGateway {
        binding: binding.clone(),
        quantity: Decimal::ONE,
        private_generation: 7,
        fills: Vec::new(),
        result: AccountGatewayResult::Unknown,
        settle_unknown_to_rejected: false,
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;

    host.refresh_signed_snapshot()?;
    let old_command = CommandId::new("net-unknown-settle")?;
    let prepared = host.prepare_for_lane(net_reduce(old_command.as_str(), Decimal::ONE, 7)?)?;
    assert!(matches!(
        host.dispatch_prepared(prepared)?,
        AccountDispatchOutcome::Unknown
    ));

    // Only the signed reply for this exact WAL identity releases its conservative reservation.
    host.gateway.private_generation = 8;
    host.gateway.settle_unknown_to_rejected = true;
    assert!(matches!(
        host.reconcile_command_status(&old_command)?
            .ok_or("missing settled status")?
            .state(),
        CommandState::Rejected { reason } if reason == "signed-terminal-rejection"
    ));

    let replacement = host.prepare_for_lane(net_reduce("net-after-settle", Decimal::ONE, 8)?)?;
    assert_eq!(replacement.command_id().as_str(), "net-after-settle");
    assert_eq!(host.gateway.dispatches, 1);
    Ok(())
}

#[test]
fn accepted_net_reduce_stays_reserved_when_later_snapshot_has_no_exact_fill()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = NetGateway {
        binding: binding.clone(),
        quantity: Decimal::ONE,
        private_generation: 7,
        fills: Vec::new(),
        result: AccountGatewayResult::Accepted {
            venue_order_id: "accepted-but-unproven".to_owned(),
        },
        settle_unknown_to_rejected: false,
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    host.refresh_signed_snapshot()?;
    let first = host.prepare_for_lane(net_reduce("net-accepted-unproven", Decimal::ONE, 7)?)?;
    assert!(matches!(
        host.dispatch_prepared(first)?,
        AccountDispatchOutcome::Accepted { .. }
    ));

    // A new private generation and an absent open order are not a completion proof.  No exact
    // signed fill means this Accepted command must still fence the whole one-unit position.
    host.gateway.private_generation = 8;
    host.refresh_signed_snapshot()?;
    assert!(matches!(
        host.prepare_for_lane(net_reduce("net-after-unproven-accepted", Decimal::ONE, 8)?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::Command
        ))
    ));
    Ok(())
}

#[test]
fn accepted_fully_filled_net_reduce_releases_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    {
        let gateway = NetGateway {
            binding: binding.clone(),
            quantity: Decimal::from(2_u8),
            private_generation: 7,
            fills: Vec::new(),
            result: AccountGatewayResult::Accepted {
                venue_order_id: "accepted-full-net-reduce".to_owned(),
            },
            settle_unknown_to_rejected: false,
            dispatches: 0,
        };
        let mut host =
            AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, gateway)?;
        host.refresh_signed_snapshot()?;
        let first = host.prepare_for_lane(net_reduce("net-accepted-full", Decimal::ONE, 7)?)?;
        assert!(matches!(
            host.dispatch_prepared(first)?,
            AccountDispatchOutcome::Accepted { .. }
        ));

        // This is the only release path: exact native fill, expected Net semantics, a later
        // complete position generation, and no remaining native/client order.
        host.gateway.private_generation = 8;
        host.gateway.quantity = Decimal::ONE;
        host.gateway.fills = vec![net_fill(
            "net-fill-accepted-full",
            "accepted-full-net-reduce",
            Decimal::ONE,
        )?];
        host.refresh_signed_snapshot()?;
    }

    let gateway = NetGateway {
        binding: binding.clone(),
        quantity: Decimal::ONE,
        private_generation: 9,
        fills: Vec::new(),
        result: AccountGatewayResult::Unknown,
        settle_unknown_to_rejected: false,
        dispatches: 0,
    };
    let mut reopened = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    let snapshot = reopened.refresh_signed_snapshot()?;
    assert_eq!(snapshot.private_generation(), 9);
    let next = reopened.prepare_for_lane(net_reduce("net-after-full-restart", Decimal::ONE, 9)?)?;
    assert_eq!(next.command_id().as_str(), "net-after-full-restart");
    Ok(())
}

#[test]
fn checkpoint_wrapper_cannot_omit_net_reduce_settlement_state()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    {
        let gateway = NetGateway {
            binding: binding.clone(),
            quantity: Decimal::ONE,
            private_generation: 7,
            fills: Vec::new(),
            result: AccountGatewayResult::Unknown,
            settle_unknown_to_rejected: false,
            dispatches: 0,
        };
        let mut host =
            AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, gateway)?;
        host.refresh_signed_snapshot()?;
    }
    let checkpoint_path = root(&temp).join(RUNTIME_BOOTSTRAP_FILE);
    let mut checkpoint: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&checkpoint_path)?)?;
    checkpoint
        .as_object_mut()
        .ok_or("checkpoint object")?
        .remove("net_reduce_settlements");
    std::fs::write(&checkpoint_path, serde_json::to_vec(&checkpoint)?)?;

    let gateway = NetGateway {
        binding: binding.clone(),
        quantity: Decimal::ONE,
        private_generation: 8,
        fills: Vec::new(),
        result: AccountGatewayResult::Unknown,
        settle_unknown_to_rejected: false,
        dispatches: 0,
    };
    assert!(matches!(
        AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway),
        Err(AccountHostError::Validation(
            AccountHostValidationError::SignedSnapshot
        ))
    ));
    Ok(())
}

#[test]
fn net_settlement_tracking_does_not_reject_hedge_bootstrap()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, gateway)?;
    let positions =
        [PositionSide::Long, PositionSide::Short].map(|position_side| SignedAccountPositionFact {
            symbol: binding.symbol.clone(),
            position_side,
            quantity: Decimal::ZERO,
            entry_price: None,
            mark_price: Some(Decimal::ONE),
        });
    let signed = SignedAccountSnapshot::complete_with_fills(
        binding,
        now_ms()?,
        1,
        1,
        1,
        SignedAccountPositionMode::Hedge,
        Vec::new(),
        positions.to_vec(),
        Vec::new(),
        "hedge:0".to_owned(),
        Vec::new(),
    )?;
    host.persist_signed_snapshot(&signed)?;
    assert!(host.net_reduce_settlements.is_empty());
    assert_eq!(host.latest_signed_snapshot.as_ref(), Some(&signed));
    Ok(())
}

#[test]
fn one_host_accepts_two_symbols_only_when_the_signed_hedge_legs_cover_both()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let sol: venue_domain::domain::Symbol = "SOL/USDT".parse()?;
    let symbols = AccountSymbolSet::new(&binding, [binding.symbol.clone(), sol.clone()])?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let host = AccountMutationHost::open_with_symbols(
        root(&temp),
        binding.clone(),
        symbols.clone(),
        Decimal::TEN,
        gateway,
    )?;
    assert!(host.configured_symbols().contains(&binding.symbol));
    assert!(host.configured_symbols().contains(&sol));
    let positions = [binding.symbol.clone(), sol.clone()]
        .into_iter()
        .flat_map(|symbol| {
            [PositionSide::Long, PositionSide::Short].map(move |position_side| {
                SignedAccountPositionFact {
                    symbol: symbol.clone(),
                    position_side,
                    quantity: Decimal::ZERO,
                    entry_price: None,
                    mark_price: Some(Decimal::ONE),
                }
            })
        })
        .collect::<Vec<_>>();
    let signed = SignedAccountSnapshot::complete_with_fills(
        binding,
        now_ms()?,
        1,
        1,
        1,
        SignedAccountPositionMode::Hedge,
        Vec::new(),
        positions,
        Vec::new(),
        "two-symbols:0".to_owned(),
        Vec::new(),
    )?;
    assert!(snapshot_covers_configured_symbols(&signed, &symbols));
    let incomplete = SignedAccountSnapshot::complete_with_fills(
        signed.binding().clone(),
        now_ms()?,
        1,
        2,
        1,
        SignedAccountPositionMode::Hedge,
        Vec::new(),
        signed.positions()[..2].to_vec(),
        Vec::new(),
        "two-symbols:1".to_owned(),
        Vec::new(),
    )?;
    assert!(!snapshot_covers_configured_symbols(&incomplete, &symbols));
    Ok(())
}

#[test]
fn failed_checkpoint_write_cannot_release_an_accepted_net_reduce()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = NetGateway {
        binding: binding.clone(),
        quantity: Decimal::from(2),
        private_generation: 7,
        fills: Vec::new(),
        result: AccountGatewayResult::Accepted {
            venue_order_id: "net-fsync-test".to_owned(),
        },
        settle_unknown_to_rejected: false,
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    host.refresh_signed_snapshot()?;
    let prepared = host.prepare_for_lane(net_reduce("net-fsync-command", Decimal::ONE, 7)?)?;
    host.dispatch_prepared(prepared)?;
    host.gateway.private_generation = 8;
    host.gateway.quantity = Decimal::ONE;
    host.gateway.fills = vec![net_fill("net-fsync-fill", "net-fsync-test", Decimal::ONE)?];
    let blocked_temporary = root(&temp)
        .join(RUNTIME_BOOTSTRAP_FILE)
        .with_extension("tmp");
    std::fs::create_dir(&blocked_temporary)?;
    assert!(host.refresh_signed_snapshot().is_err());
    assert!(host.net_reduce_settlements.is_empty());
    assert_eq!(
        host.pending_net_reduce_quantity(&CommandId::new("next")?, &"DOGE/USDT".parse()?)?,
        Decimal::ONE
    );
    std::fs::remove_dir(&blocked_temporary)?;
    host.gateway.private_generation = 9;
    host.refresh_signed_snapshot()?;
    assert_eq!(host.net_reduce_settlements.len(), 1);
    Ok(())
}

#[test]
fn resident_can_durably_reject_an_undispatched_prepared_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Accepted {
            venue_order_id: "must-not-dispatch".to_owned(),
        },
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    let command = command(Decimal::ONE)?;
    let command_id = command.command_id().clone();
    let prepared = host.prepare_for_lane(command)?;

    host.reject_prepared_without_dispatch(&prepared, "resident_queue_invalidated")?;

    assert!(matches!(
        host.command_status(&command_id)?.ok_or("status missing")?.state(),
        CommandState::Rejected { reason } if reason == "resident_queue_invalidated"
    ));
    assert!(matches!(
        host.dispatch_prepared(prepared),
        Err(AccountHostError::Validation(
            AccountHostValidationError::PreparedCommand
        ))
    ));
    assert_eq!(host.gateway.dispatches, 0);
    Ok(())
}

#[test]
fn prepared_capability_rejects_tampering_before_gateway_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    let mut prepared = host.prepare_for_lane(command(Decimal::TEN)?)?;
    prepared.receipt_sequence = prepared.receipt_sequence.saturating_add(1);
    assert!(matches!(
        host.dispatch_prepared(prepared),
        Err(AccountHostError::Validation(
            AccountHostValidationError::PreparedCommand
        ))
    ));
    assert_eq!(host.gateway.dispatches, 0);
    Ok(())
}

#[test]
fn prepared_capability_is_scoped_to_its_account_host() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let first_gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let mut first = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, first_gateway)?;
    let prepared = first.prepare_for_lane(command(Decimal::TEN)?)?;
    drop(first);

    let other_account = "00000000-0000-4000-8000-000000000002";
    let other_binding = GatewayBinding::new(
        VenueId::Okx,
        GatewayMode::Live,
        other_account,
        "DOGE/USDT".parse()?,
    )?;
    let other_root = temp.path().join("okx").join("LIVE").join(other_account);
    let other_gateway = Gateway {
        binding: other_binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let mut other =
        AccountMutationHost::open(other_root, other_binding, Decimal::TEN, other_gateway)?;
    assert!(matches!(
        other.dispatch_prepared(prepared),
        Err(AccountHostError::Validation(
            AccountHostValidationError::PreparedCommand
        ))
    ));
    assert_eq!(other.gateway.dispatches, 0);
    Ok(())
}

#[test]
fn restart_fences_an_undispatched_prepared_record_without_redispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    {
        let gateway = Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Unknown,
            dispatches: 0,
        };
        let mut host =
            AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, gateway)?;
        let _prepared = host.prepare_for_lane(command(Decimal::TEN)?)?;
        assert_eq!(host.gateway.dispatches, 0);
    }
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let mut reopened = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    assert!(matches!(
        reopened.prepare_for_lane(command(Decimal::TEN)?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::PreparedCommand
        ))
    ));
    assert_eq!(reopened.gateway.dispatches, 0);
    Ok(())
}

#[test]
fn restart_resolves_unknown_by_readback_without_redispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    {
        let gateway = Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Unknown,
            dispatches: 0,
        };
        let mut host =
            AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, gateway)?;
        assert_eq!(
            host.dispatch(command(Decimal::TEN)?)?,
            AccountDispatchOutcome::Unknown
        );
        assert_eq!(host.gateway.dispatches, 1);
    }

    let gateway = RecoveringGateway {
        binding: binding.clone(),
        dispatches: 0,
    };
    let mut reopened = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    assert!(!reopened.has_unresolved());
    assert_eq!(reopened.gateway.dispatches, 0);
    assert!(matches!(
        reopened.dispatch(command(Decimal::new(9, 0))?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::OpenEntryFence
        ))
    ));
    assert_eq!(reopened.gateway.dispatches, 0);
    Ok(())
}

#[test]
fn host_rejects_more_than_ten_usdt_before_gateway_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Accepted {
            venue_order_id: "1".to_owned(),
        },
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    assert!(matches!(
        host.dispatch(command(Decimal::new(1001, 2))?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::AccountRiskLimit
        ))
    ));
    assert_eq!(host.gateway.dispatches, 0);
    Ok(())
}

#[test]
fn host_requires_an_accepted_cancel_before_another_entry() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Accepted {
            venue_order_id: "1".to_owned(),
        },
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    assert!(matches!(
        host.dispatch(command(Decimal::TEN)?)?,
        AccountDispatchOutcome::Accepted { .. }
    ));
    assert!(matches!(
        host.dispatch(command(Decimal::new(9, 0))?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::OpenEntryFence
        ))
    ));
    assert_eq!(host.gateway.dispatches, 1);
    Ok(())
}

#[test]
fn host_requires_complete_signed_account_risk_evidence_before_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    struct MissingEvidenceGateway(Gateway);
    impl AccountPhysicalGateway for MissingEvidenceGateway {
        type Error = io::Error;

        fn binding(&self) -> &GatewayBinding {
            self.0.binding()
        }

        fn reconcile(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<AccountRecoveryReport, Self::Error> {
            self.0.reconcile(request)
        }

        fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
            self.0.dispatch(permit)
        }
    }
    let gateway = MissingEvidenceGateway(Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Accepted {
            venue_order_id: "1".to_owned(),
        },
        dispatches: 0,
    });
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    assert!(matches!(
        host.dispatch(command(Decimal::ONE)?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::RiskEvidence
        ))
    ));
    assert_eq!(host.gateway.0.dispatches, 0);
    Ok(())
}

#[test]
fn host_sums_signed_open_and_wal_reservations_before_new_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    struct AggregateGateway {
        base: Gateway,
        evidence: AccountRiskEvidence,
    }
    impl AccountPhysicalGateway for AggregateGateway {
        type Error = io::Error;

        fn binding(&self) -> &GatewayBinding {
            self.base.binding()
        }

        fn reconcile(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<AccountRecoveryReport, Self::Error> {
            self.base.reconcile(request)
        }

        fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
            Ok(self.evidence.clone())
        }

        fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
            self.base.dispatch(permit)
        }
    }
    let gateway = AggregateGateway {
        base: Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Accepted {
                venue_order_id: "1".to_owned(),
            },
            dispatches: 0,
        },
        evidence: risk_evidence(
            binding.clone(),
            vec![Decimal::from(3)],
            vec![Decimal::from(2)],
        )?,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    assert!(matches!(
        host.dispatch(command(Decimal::from(6))?),
        Err(AccountHostError::Validation(
            AccountHostValidationError::AccountRiskLimit
        ))
    ));
    assert_eq!(host.gateway.base.dispatches, 0);
    Ok(())
}

#[test]
fn prepared_entry_rechecks_risk_at_dispatch_before_submitting()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    struct RecheckGateway {
        base: Gateway,
        risk_reads: usize,
    }
    impl AccountPhysicalGateway for RecheckGateway {
        type Error = io::Error;

        fn binding(&self) -> &GatewayBinding {
            self.base.binding()
        }

        fn reconcile(
            &mut self,
            request: &AccountRecoveryRequest,
        ) -> Result<AccountRecoveryReport, Self::Error> {
            self.base.reconcile(request)
        }

        fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
            self.risk_reads = self.risk_reads.saturating_add(1);
            if self.risk_reads == 1 {
                risk_evidence(self.base.binding.clone(), Vec::new(), Vec::new())
            } else {
                Err(AccountHostValidationError::RiskEvidence)
            }
        }

        fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
            self.base.dispatch(permit)
        }
    }
    let gateway = RecheckGateway {
        base: Gateway {
            binding: binding.clone(),
            result: AccountGatewayResult::Accepted {
                venue_order_id: "would-not-dispatch".to_owned(),
            },
            dispatches: 0,
        },
        risk_reads: 0,
    };
    let mut host = AccountMutationHost::open(root(&temp), binding, Decimal::TEN, gateway)?;
    let prepared = host.prepare_for_lane(command(Decimal::ONE)?)?;
    assert!(matches!(
        host.dispatch_prepared(prepared),
        Err(AccountHostError::Validation(
            AccountHostValidationError::RiskEvidence
        ))
    ));
    assert_eq!(host.gateway.risk_reads, 2);
    assert_eq!(host.gateway.base.dispatches, 0);
    assert!(matches!(
        host.command_status(command(Decimal::ONE)?.command_id())?
            .ok_or("status missing")?
            .state(),
        CommandState::Rejected { reason } if reason == "dispatch_risk_recheck_failed"
    ));
    Ok(())
}

#[test]
fn second_host_cannot_acquire_the_same_account_lock() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let binding = binding()?;
    let make_gateway = || Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Unknown,
        dispatches: 0,
    };
    let _first =
        AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, make_gateway())?;
    assert!(
        AccountMutationHost::open(root(&temp), binding.clone(), Decimal::TEN, make_gateway())
            .is_err()
    );
    Ok(())
}

#[test]
fn five_mib_journal_stops_before_any_new_physical_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let account_root = root(&temp);
    fs::create_dir_all(&account_root)?;
    let journal_path = account_root.join("commands.jsonl");
    let mut journal = File::create(&journal_path)?;
    journal.write_all(&vec![b' '; COMMAND_JOURNAL_ROTATE_BYTES as usize])?;
    journal.sync_all()?;

    assert!(matches!(
        require_append_budget(&journal_path, &command(Decimal::TEN)?),
        Err(AccountHostValidationError::RotationRequired)
    ));
    assert!(fs::metadata(journal_path)?.len() <= COMMAND_JOURNAL_HARD_LIMIT_BYTES);
    Ok(())
}

#[test]
fn clean_five_mib_segment_rotates_and_retains_cancel_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let account_root = root(&temp);
    fs::create_dir_all(&account_root)?;
    let journal_path = account_root.join("commands.jsonl");
    let mut bytes = Vec::new();
    let mut sequence = 1_u64;
    let mut index = 0_usize;
    while bytes.len() < COMMAND_JOURNAL_ROTATE_BYTES as usize {
        let command = indexed_command(index)?;
        let hash = crate::execution_command_sha256(&command)?
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        for state in [
            CommandState::Prepared,
            CommandState::Submitted,
            CommandState::Accepted {
                venue_order_id: format!("venue-{index}"),
            },
        ] {
            serde_json::to_writer(
                &mut bytes,
                &crate::CommandReceipt {
                    sequence,
                    command: command.clone(),
                    command_sha256: hash.clone(),
                    state,
                },
            )?;
            bytes.push(b'\n');
            sequence = sequence.checked_add(1).ok_or("sequence overflow")?;
        }
        index = index.checked_add(1).ok_or("index overflow")?;
    }
    let mut file = File::create(&journal_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;

    let binding = binding()?;
    let gateway = Gateway {
        binding: binding.clone(),
        result: AccountGatewayResult::Accepted {
            venue_order_id: "cancel-1".to_owned(),
        },
        dispatches: 0,
    };
    let mut host = AccountMutationHost::open(account_root.clone(), binding, Decimal::TEN, gateway)?;
    assert!(account_root.join("commands-000001.jsonl").is_file());
    assert_eq!(fs::metadata(&journal_path)?.len(), 0);

    let cancel = ExecutionCommand::Cancel(CancelCommand {
        command_id: CommandId::new("cancel-segment-0")?,
        owner: owner()?,
        target_client_order_id: CommandId::new("client-segment-0")?,
    });
    assert!(matches!(
        host.dispatch(cancel)?,
        AccountDispatchOutcome::Accepted { .. }
    ));
    assert_eq!(host.gateway.dispatches, 1);
    Ok(())
}

#[test]
fn frozen_legacy_journal_is_segmented_without_mutating_the_source()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let legacy_root = temporary.path().join("legacy");
    let destination = root(&temporary);
    fs::create_dir_all(&legacy_root)?;
    let source = legacy_root.join("commands.jsonl");
    let command = command(Decimal::ONE)?;
    let mut legacy = CommandJournal::open(&source)?;
    legacy.prepare(command.clone())?;
    legacy.transition(command.command_id(), CommandState::Submitted)?;
    legacy.transition(
        command.command_id(),
        CommandState::Accepted {
            venue_order_id: "legacy-native-1".to_owned(),
        },
    )?;
    let before = fs::read(&source)?;

    import_legacy_v1_journal_if_needed(&legacy_root, &destination)?;

    assert_eq!(fs::read(&source)?, before);
    assert!(destination.join(LEGACY_V1_IMPORT_FILE).is_file());
    let segments = journal_segment_paths(&destination)?;
    assert_eq!(segments.len(), 1);
    assert!(fs::metadata(&segments[0])?.len() <= COMMAND_JOURNAL_ROTATE_BYTES);
    let recovered = CommandJournal::open_segmented(destination.join("commands.jsonl"), &segments)?;
    assert!(!recovered.has_unresolved());
    assert_eq!(recovered.native_order_routes().len(), 1);
    import_legacy_v1_journal_if_needed(&legacy_root, &destination)?;
    Ok(())
}

#[test]
fn frozen_legacy_journal_rejects_a_mixed_owner_before_copying()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let legacy_root = temporary.path().join("legacy");
    let destination = root(&temporary);
    fs::create_dir_all(&legacy_root)?;
    let source = legacy_root.join("commands.jsonl");
    let command = command(Decimal::ONE)?;
    let owner = command.mutation_owner().clone();
    let mut foreign = command.clone();
    let ExecutionCommand::PlaceLimit(order) = &mut foreign else {
        return Err("limit command required".into());
    };
    order.command_id = CommandId::new("foreign-legacy-command")?;
    order.client_order_id = CommandId::new("foreign-legacy-client")?;
    order.owner.run_id = "other-run".to_owned();
    let mut legacy = CommandJournal::open(&source)?;
    for (entry, venue_order_id) in [
        (&command, "legacy-native-owner"),
        (&foreign, "legacy-native-foreign"),
    ] {
        legacy.prepare(entry.clone())?;
        legacy.transition(entry.command_id(), CommandState::Submitted)?;
        legacy.transition(
            entry.command_id(),
            CommandState::Accepted {
                venue_order_id: venue_order_id.to_owned(),
            },
        )?;
    }
    let predecessor = LegacyV1WriterPredecessor {
        exchange: VenueId::Okx,
        successor_trading_account_id: ACCOUNT.to_owned(),
        legacy_product_account: owner.account.clone(),
        legacy_symbol: owner.symbol.clone(),
        legacy_owner_scope: "legacy-owner-scope".to_owned(),
        legacy_strategy_instance_id: owner.strategy_instance_id.clone(),
        legacy_run_id: owner.run_id.clone(),
        legacy_artifacts_root: fs::canonicalize(&legacy_root)?,
        legacy_lock_sha256: "0".repeat(64),
        legacy_lock_path: temporary.path().join("legacy.lock"),
        handoff_sha256: "0".repeat(64),
    };
    assert!(matches!(
        import_legacy_v1_journal_for_predecessor_if_needed(&predecessor, &destination),
        Err(AccountHostValidationError::LegacyPredecessor)
    ));
    assert!(!destination.exists());
    Ok(())
}

#[test]
fn frozen_legacy_journal_marker_allows_later_clean_rotation()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let legacy_root = temporary.path().join("legacy");
    let destination = root(&temporary);
    fs::create_dir_all(&legacy_root)?;
    let source = legacy_root.join("commands.jsonl");
    let command = command(Decimal::ONE)?;
    let mut legacy = CommandJournal::open(&source)?;
    legacy.prepare(command.clone())?;
    legacy.transition(command.command_id(), CommandState::Submitted)?;
    legacy.transition(
        command.command_id(),
        CommandState::Accepted {
            venue_order_id: "legacy-native-2".to_owned(),
        },
    )?;

    import_legacy_v1_journal_if_needed(&legacy_root, &destination)?;
    fs::write(legacy_import_segment_path(&destination, 2)?, [])?;

    import_legacy_v1_journal_if_needed(&legacy_root, &destination)?;
    Ok(())
}

#[test]
fn frozen_legacy_journal_marker_rejects_a_tampered_import_segment()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let legacy_root = temporary.path().join("legacy");
    let destination = root(&temporary);
    fs::create_dir_all(&legacy_root)?;
    let source = legacy_root.join("commands.jsonl");
    let command = command(Decimal::ONE)?;
    let mut legacy = CommandJournal::open(&source)?;
    legacy.prepare(command.clone())?;
    legacy.transition(command.command_id(), CommandState::Submitted)?;
    legacy.transition(
        command.command_id(),
        CommandState::Accepted {
            venue_order_id: "legacy-native-3".to_owned(),
        },
    )?;

    import_legacy_v1_journal_if_needed(&legacy_root, &destination)?;
    fs::write(legacy_import_segment_path(&destination, 1)?, b"tampered\n")?;

    assert!(matches!(
        import_legacy_v1_journal_if_needed(&legacy_root, &destination),
        Err(AccountHostValidationError::LegacyPredecessor)
    ));
    Ok(())
}

#[test]
fn frozen_legacy_journal_with_unknown_is_not_imported() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let legacy_root = temporary.path().join("legacy");
    let destination = root(&temporary);
    fs::create_dir_all(&legacy_root)?;
    let source = legacy_root.join("commands.jsonl");
    let command = command(Decimal::ONE)?;
    let mut legacy = CommandJournal::open(&source)?;
    legacy.prepare(command.clone())?;
    legacy.transition(command.command_id(), CommandState::Submitted)?;
    legacy.transition(
        command.command_id(),
        CommandState::Unknown {
            reason: "ambiguous".to_owned(),
        },
    )?;

    assert!(matches!(
        import_legacy_v1_journal_if_needed(&legacy_root, &destination),
        Err(AccountHostValidationError::LegacyPredecessor)
    ));
    assert!(!destination.exists());
    Ok(())
}
