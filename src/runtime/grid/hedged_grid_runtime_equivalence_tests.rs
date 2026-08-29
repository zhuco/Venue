use std::{fs, path::Path, sync::Arc};

use rust_decimal::Decimal;
use serde_json::json;

use super::*;
use crate::{
    domain::{
        AccountBalance, Amount, Asset, CommandId, ExecutionCommand, Fill, Instrument, MarketKind,
        MarketReduceCommand, OrderCommand, OrderPurpose,
    },
    exchange::{
        binance::PrivateError,
        grid::{GridOrderFamilyReadback, GridVenueFill, HedgedGridMutationClient},
    },
    execution::CommandReceipt,
    runtime::hedged_grid_live::{
        GridMutation, LegacyGridMutationEndpoint, handle_dispatch_transactions_with_endpoint,
        reserve_confirmed_fills,
    },
    strategy::hedged_grid::{
        GridEpoch, GridInventory, GridOrderIntent, GridOrderRole, HedgedGridParams, OwnedGridFill,
    },
};

struct AcceptedLegacyEndpoint;

impl LegacyGridMutationEndpoint for AcceptedLegacyEndpoint {
    fn submit(&self, mutation: &GridMutation) -> Result<String, PrivateError> {
        Ok(match mutation {
            GridMutation::Place(command) => legacy_order_payload(
                command.command_id.as_str(),
                command.client_order_id.as_str(),
                &command.owner.symbol,
                command.side,
                command.position_side,
                command.quantity,
                command.limit_price,
                command.reduce_only,
                "NEW",
            ),
            GridMutation::Cancel(command) => legacy_order_payload(
                command.command_id.as_str(),
                command.target_client_order_id.as_str(),
                &command.owner.symbol,
                OrderSide::Sell,
                PositionSide::Long,
                Decimal::ONE,
                Price::new(Decimal::ONE).map_err(|_| PrivateError::Clock)?,
                true,
                "CANCELED",
            ),
            GridMutation::Market(command) => legacy_order_payload(
                command.command_id.as_str(),
                command.client_order_id.as_str(),
                &command.owner.symbol,
                command.side,
                command.position_side,
                command.quantity,
                Price::new(Decimal::ONE).map_err(|_| PrivateError::Clock)?,
                command.reduce_only,
                "NEW",
            ),
            GridMutation::Reduce(command) => legacy_order_payload(
                command.command_id.as_str(),
                command.client_order_id.as_str(),
                &command.owner.symbol,
                command.side,
                command.position_side,
                command.quantity,
                Price::new(Decimal::ONE).map_err(|_| PrivateError::Clock)?,
                true,
                "NEW",
            ),
        })
    }
}

#[derive(Clone)]
struct AcceptedSharedClient;

impl HedgedGridMutationClient for AcceptedSharedClient {
    fn place_limit_post_only(&self, command: &OrderCommand) -> Result<String, GridVenueError> {
        Ok(format!("venue-{}", command.command_id.as_str()))
    }

    fn place_market(&self, command: &MarketOrderCommand) -> Result<String, GridVenueError> {
        Ok(format!("venue-{}", command.command_id.as_str()))
    }

    fn place_market_reduce(&self, command: &MarketReduceCommand) -> Result<String, GridVenueError> {
        Ok(format!("venue-{}", command.command_id.as_str()))
    }

    fn cancel_by_client_id(&self, command: &CancelCommand) -> Result<String, GridVenueError> {
        Ok(format!("venue-{}", command.command_id.as_str()))
    }
}

struct SharedEquivalenceVenue {
    instrument: Instrument,
    terminal_source: Order,
}

impl HedgedGridVenue for SharedEquivalenceVenue {
    fn exchange(&self) -> &'static str {
        "binance"
    }

    fn instrument(&self) -> &Instrument {
        &self.instrument
    }

    fn minimum_quantity(&self) -> Decimal {
        Decimal::new(1, 2)
    }

    fn best_bid_ask(&self, _now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        Ok((
            Price::new(Decimal::new(999, 1)).map_err(|_| GridVenueError::PublicPayload)?,
            Price::new(Decimal::new(1001, 1)).map_err(|_| GridVenueError::PublicPayload)?,
        ))
    }

    fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }

    fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
        Ok(())
    }

    fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
        Ok(None)
    }

    fn reset_private_stream(&mut self) {}

    fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
        Arc::new(AcceptedSharedClient)
    }

    fn order_by_client_id(&mut self, client_order_id: &str) -> Result<Order, GridVenueError> {
        if self.terminal_source.client_order_id == FieldState::Known(client_order_id.to_owned()) {
            Ok(self.terminal_source.clone())
        } else {
            Err(GridVenueError::PrivateReadbackRequired)
        }
    }

    fn verify_post_only_order(&mut self, _client_order_id: &str) -> Result<(), GridVenueError> {
        Ok(())
    }
}

#[test]
fn legacy_and_shared_normal_maker_roll_have_equivalent_desired_wal_and_state()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let legacy_root = temporary.path().join("legacy");
    let shared_root = temporary.path().join("shared");
    fs::create_dir_all(&legacy_root)?;
    fs::create_dir_all(&shared_root)?;
    let (initial, source, owned_fill) = fixture()?;
    let binding = initial.binding.clone();
    let instrument = instrument()?;

    let legacy_store = ProjectionStore::new(legacy_root.join("hedged_grid_state.json"));
    let legacy_wal_path = legacy_root.join("commands.jsonl");
    let mut legacy_commands = CommandJournal::open(&legacy_wal_path)?;
    seed_initial_ladder(&mut legacy_commands, &binding, &instrument, &initial)?;
    let mut legacy_state = initial.clone();
    let legacy_actions =
        reserve_confirmed_fills(&mut legacy_state, &legacy_store, vec![owned_fill.clone()])?;
    let legacy_transactions = transactions(legacy_actions)?;
    let legacy_authority = WriterLeaseAuthority::open(
        legacy_root.join("writer.json"),
        stage7_writer_scope(&binding),
    )?;
    let legacy_writer = legacy_authority.register_initial(1, 2)?;
    assert!(handle_dispatch_transactions_with_endpoint(
        &mut legacy_state,
        &legacy_store,
        &mut legacy_commands,
        &legacy_authority,
        &legacy_writer,
        &binding,
        &instrument,
        &AcceptedLegacyEndpoint,
        legacy_transactions,
        2,
    )?);

    let shared_store = ProjectionStore::new(shared_root.join("hedged_grid_state.json"));
    let shared_wal_path = shared_root.join("commands.jsonl");
    let mut shared_commands = CommandJournal::open(&shared_wal_path)?;
    seed_initial_ladder(&mut shared_commands, &binding, &instrument, &initial)?;
    let mut shared_checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state: initial,
        private_generation: 2,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let source_client_id = client_order_id(&source.key)?;
    let readback = shared_readback(&binding, &source, &owned_fill, &source_client_id)?;
    let shared_authority = WriterLeaseAuthority::open(
        shared_root.join("writer.json"),
        stage7_writer_scope(&binding),
    )?;
    let shared_writer = shared_authority.register_initial(1, 2)?;
    let mut venue = SharedEquivalenceVenue {
        instrument,
        terminal_source: terminal_source_order(&binding, &source, &source_client_id)?,
    };
    assert_eq!(
        process_complete_owned_fills(
            &mut shared_checkpoint,
            &mut shared_commands,
            &mut venue,
            &shared_authority,
            &shared_writer,
            &binding,
            &readback,
            &shared_store,
        )?,
        FillDriveOutcome::dispatched()
    );

    // These are independent physical ingress/dispatch paths. Equality therefore proves more
    // than the common reducer alone: both shells projected and settled the same semantic ladder.
    assert_eq!(
        legacy_state.owned_orders,
        shared_checkpoint.state.owned_orders
    );
    assert_eq!(legacy_state, shared_checkpoint.state);
    let legacy_wal = prepared_and_accepted_wal(&legacy_wal_path)?;
    let shared_wal = prepared_and_accepted_wal(&shared_wal_path)?;
    assert_eq!(legacy_wal, shared_wal);
    assert_eq!(
        legacy_wal
            .iter()
            .filter(|(state, _)| state == "prepared")
            .count(),
        15
    );
    assert_eq!(
        legacy_wal
            .iter()
            .filter(|(state, _)| state == "accepted")
            .count(),
        15
    );
    Ok(())
}

fn fixture() -> Result<(HedgedGridState, GridOrderIntent, OwnedGridFill), Box<dyn std::error::Error>>
{
    let binding = HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdc".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDC".parse()?,
        config_version: "equivalence_v1".to_owned(),
        owner_scope: "hedged_grid_sol_usdc_primary".to_owned(),
    };
    let mut state = HedgedGridState::new_with_params(
        binding,
        HedgedGridParams::fixed_release(Asset::new("USDC")?, 3)?,
    )?;
    let _ = state.observe_inventory(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(15, 2),
        short_quantity: Decimal::new(15, 2),
    })?;
    let _ = state.install_epoch(GridEpoch {
        epoch: 1,
        anchor_price: Price::new(Decimal::new(100, 0))?,
        step: Price::new(Decimal::new(2, 1))?,
        grid_quantity: Decimal::new(5, 2),
        passive_book_fallback: None,
    })?;
    let source = state
        .owned_orders
        .values()
        .find(|intent| {
            intent.key.position == GridPosition::Long
                && intent.key.role == GridOrderRole::Close
                && intent.key.level == 1
        })
        .cloned()
        .ok_or("missing source order")?;
    let fill = OwnedGridFill {
        fill_id: "equivalence-maker-fill-2".to_owned(),
        private_generation: 2,
        source_order: source.key.clone(),
        fill_price: source.price,
        complete: true,
        maker: FieldState::Known(true),
    };
    Ok((state, source, fill))
}

fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
    Ok(Instrument {
        symbol: "SOL/USDC".parse()?,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(Asset::new("USDC")?),
        generation: 1,
        price_tick: Price::new(Decimal::new(1, 2))?,
        quantity_step: Decimal::new(1, 2),
        minimum_notional: Amount::new(Asset::new("USDC")?, Decimal::ZERO),
    })
}

fn seed_initial_ladder(
    commands: &mut CommandJournal,
    binding: &HedgedGridBinding,
    instrument: &Instrument,
    state: &HedgedGridState,
) -> Result<(), Box<dyn std::error::Error>> {
    for intent in state.owned_orders.values() {
        let GridMutation::Place(command) = place_command(binding, instrument, intent)? else {
            return Err("grid intent did not map to a place command".into());
        };
        let command_id = command.command_id.clone();
        let venue_order_id = if intent.key.role == GridOrderRole::Close
            && intent.key.position == GridPosition::Long
            && intent.key.level == 1
        {
            "venue-source-order".to_owned()
        } else {
            format!("venue-{}", command.client_order_id.as_str())
        };
        commands.prepare_place(command)?;
        commands.transition(&command_id, CommandState::Submitted)?;
        commands.transition(&command_id, CommandState::Accepted { venue_order_id })?;
    }
    Ok(())
}

fn transactions(
    actions: Vec<GridAction>,
) -> Result<Vec<crate::strategy::hedged_grid::GridTransaction>, Box<dyn std::error::Error>> {
    actions
        .into_iter()
        .map(|action| match action {
            GridAction::Dispatch(transaction) => Ok(transaction),
            _ => Err("legacy fill emitted a non-dispatch action".into()),
        })
        .collect()
}

fn shared_readback(
    binding: &HedgedGridBinding,
    source: &GridOrderIntent,
    owned_fill: &OwnedGridFill,
    client_order_id: &CommandId,
) -> Result<GridVenueReadback, Box<dyn std::error::Error>> {
    Ok(GridVenueReadback {
        raw_private_payloads: vec!["{\"fills\":\"complete\"}".to_owned()],
        order_family_readback: Some(GridOrderFamilyReadback::regular_only_adapter_profile(
            Vec::new(),
            vec!["[]".to_owned()],
        )?),
        balance: AccountBalance {
            asset: Asset::new("USDC")?,
            wallet_balance: Decimal::new(100, 0),
            available_balance: Decimal::new(100, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        },
        hedge_position: true,
        positions: Vec::new(),
        orders: Vec::new(),
        fills: vec![GridVenueFill {
            fill: Fill {
                execution_sequence: FieldState::Known(2),
                fill_id: owned_fill.fill_id.clone(),
                order_id: "venue-source-order".to_owned(),
                symbol: binding.symbol.clone(),
                side: source.side,
                position_side: FieldState::Known(PositionSide::Long),
                quantity: source.quantity,
                price: owned_fill.fill_price,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(true),
                exchange_time_ms: Some(200),
            },
            client_order_id: FieldState::Known(client_order_id.as_str().to_owned()),
        }],
    })
}

fn terminal_source_order(
    binding: &HedgedGridBinding,
    source: &GridOrderIntent,
    client_order_id: &CommandId,
) -> Result<Order, Box<dyn std::error::Error>> {
    Ok(Order {
        order_id: "venue-source-order".to_owned(),
        client_order_id: FieldState::Known(client_order_id.as_str().to_owned()),
        symbol: binding.symbol.clone(),
        side: source.side,
        position_side: FieldState::Known(PositionSide::Long),
        purpose: FieldState::Known(OrderPurpose::Reduce),
        state: OrderState::Filled,
        quantity: source.quantity,
        filled_quantity: source.quantity,
        limit_price: Some(source.price),
        average_price: FieldState::Known(source.price),
        reduce_only: true,
    })
}

fn prepared_and_accepted_wal(
    path: &Path,
) -> Result<Vec<(String, ExecutionCommand)>, Box<dyn std::error::Error>> {
    fs::read_to_string(path)?
        .lines()
        .map(serde_json::from_str::<CommandReceipt>)
        .collect::<Result<Vec<_>, _>>()
        .map(|receipts| {
            receipts
                .into_iter()
                .filter_map(|receipt| match receipt.state {
                    CommandState::Prepared => Some(("prepared".to_owned(), receipt.command)),
                    CommandState::Accepted { .. } => Some(("accepted".to_owned(), receipt.command)),
                    CommandState::Submitted
                    | CommandState::Rejected { .. }
                    | CommandState::Unknown { .. } => None,
                })
                .collect()
        })
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
fn legacy_order_payload(
    order_id: &str,
    client_order_id: &str,
    symbol: &crate::domain::Symbol,
    side: OrderSide,
    position_side: PositionSide,
    quantity: Decimal,
    price: Price,
    reduce_only: bool,
    status: &str,
) -> String {
    json!({
        "symbol": format!("{}{}", symbol.base(), symbol.quote()),
        "orderId": order_id,
        "clientOrderId": client_order_id,
        "status": status,
        "side": match side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
        "positionSide": match position_side {
            PositionSide::Long => "LONG",
            PositionSide::Short => "SHORT",
            PositionSide::Net => "BOTH",
        },
        "origQty": quantity.to_string(),
        "executedQty": if status == "CANCELED" { quantity.to_string() } else { "0".to_owned() },
        "price": price.value().to_string(),
        "avgPrice": "0",
        "reduceOnly": reduce_only,
    })
    .to_string()
}
