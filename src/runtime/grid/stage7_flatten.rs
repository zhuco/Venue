use std::{path::PathBuf, thread, time::Duration};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    config::Config,
    domain::{
        CancelCommand, CommandId, FieldState, MarketReduceCommand, Order, OrderOwner, OrderPurpose,
        OrderSide, OrderState, PositionSide,
    },
    exchange::grid::{BinanceGridVenue, BitgetGridVenue, GateGridVenue, GridVenueReadback},
    execution::{
        CommandJournal, CommandState, FlatReceipt, WriterLeaseAuthority, WriterSession, sha256_hex,
    },
    storage::{PrivateEvidenceJournal, ProjectionStore},
    strategy::hedged_grid::{GridInventory, GridPhase, HedgedGridBinding},
};

use super::{
    CHECKPOINT_FILE, COMMAND_FILE, CONTROL_FILE, Stage7CanaryVenue, Stage7GridCheckpoint,
    Stage7GridControl, Stage7GridError, Stage7Mutation, WRITER_FILE, acquire_stage7_writer_root,
    binance_binding, bitget_binding, canary_cleanup_readback, checkpoint_quantity_matches,
    execute_mutations, gate_binding, open_stage7_private_evidence, recover_unresolved,
    reduce_canary_market, stage7_writer_scope, validate_owner_binding,
};
use crate::runtime::HedgedGridControlTarget;

const FLATTEN_FILE: &str = "hedged_grid_flatten.json";
const PRIVATE_SETTLEMENT_ATTEMPTS: u8 = 120;
const PRIVATE_SETTLEMENT_INTERVAL_MS: u64 = 250;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7FlattenRequest {
    pub artifacts_root: PathBuf,
    pub confirm_mainnet_grid_mutations: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7FlattenReport {
    pub exchange: String,
    pub symbol: String,
    pub private_generation: u64,
    pub writer_generation: u64,
    pub recovered_after_retirement: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FlattenStatus {
    Verified,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stage7FlattenState {
    schema_version: u16,
    binding: HedgedGridBinding,
    writer_generation: u64,
    private_generation: u64,
    status: FlattenStatus,
    summary_sha256: String,
}

pub fn run_gate_stage7_flatten(
    cfg: &Config,
    request: Stage7FlattenRequest,
) -> Result<Stage7FlattenReport, Stage7GridError> {
    validate_request(&request)?;
    let binding = gate_binding(cfg)?;
    let mut venue = GateGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_flatten(request, binding, &mut venue)
}

pub fn run_bitget_stage7_flatten(
    cfg: &Config,
    request: Stage7FlattenRequest,
) -> Result<Stage7FlattenReport, Stage7GridError> {
    validate_request(&request)?;
    let binding = bitget_binding(cfg)?;
    let mut venue = BitgetGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_flatten(request, binding, &mut venue)
}

pub fn run_binance_stage7_flatten(
    cfg: &Config,
    request: Stage7FlattenRequest,
) -> Result<Stage7FlattenReport, Stage7GridError> {
    validate_request(&request)?;
    let binding = binance_binding(cfg)?;
    let mut venue = BinanceGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_flatten(request, binding, &mut venue)
}

fn validate_request(request: &Stage7FlattenRequest) -> Result<(), Stage7GridError> {
    if !request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    Ok(())
}

fn run_stage7_flatten<V: Stage7CanaryVenue>(
    request: Stage7FlattenRequest,
    binding: HedgedGridBinding,
    venue: &mut V,
) -> Result<Stage7FlattenReport, Stage7GridError> {
    let writer_scope = stage7_writer_scope(&binding);
    let _canonical_root_guard = acquire_stage7_writer_root(&writer_scope, &request.artifacts_root)?;
    let checkpoint_store = ProjectionStore::new(request.artifacts_root.join(CHECKPOINT_FILE));
    let mut checkpoint = load_bound_checkpoint(&checkpoint_store, &binding)?;
    venue.set_fill_history_start_ms(checkpoint.fill_history_start_ms);

    // Persist Stop before any private mutation. A failed or interrupted flatten must never leave
    // the root configured to re-enter the ordinary grid on its next resident start.
    ProjectionStore::new(request.artifacts_root.join(CONTROL_FILE)).save(&Stage7GridControl {
        schema_version: 1,
        binding: binding.clone(),
        target: HedgedGridControlTarget::Stop,
    })?;

    let mut commands = CommandJournal::open(request.artifacts_root.join(COMMAND_FILE))?;
    validate_journal_scope(&commands, &binding)?;
    let mut evidence = open_stage7_private_evidence(&request.artifacts_root, &binding)?;
    let evidence_generation = evidence.last_generation();
    let flatten_store = ProjectionStore::new(request.artifacts_root.join(FLATTEN_FILE));
    let previous_flatten = flatten_store.load::<Stage7FlattenState>()?;
    if let Some(state) = &previous_flatten {
        validate_flatten_state(state, &binding)?;
    }

    let authority = WriterLeaseAuthority::open(
        request.artifacts_root.join(WRITER_FILE),
        writer_scope.clone(),
    )?;
    let Some(writer) = authority.active_session()? else {
        return recover_completed_retirement(
            previous_flatten.as_ref(),
            &flatten_store,
            &checkpoint,
            &commands,
            evidence_generation,
            &binding,
        );
    };
    let mut generation = evidence_generation
        .max(checkpoint.private_generation)
        .max(writer.readback_generation);

    // Prepared proves no gateway call occurred and is durably rejected. Submitted is ambiguous
    // and becomes UNKNOWN; it is resolved by its exact client identity below and never resent.
    commands.fence_interrupted_dispatches()?;
    let (readback, _) = signed_readback(venue, &mut evidence, &mut generation, &binding)?;
    recover_unresolved(
        &mut commands,
        venue,
        &authority,
        &writer,
        &binding,
        &readback,
        false,
    )?;
    require_resolved(&commands)?;

    // Resolve-old-WAL and classify ownership from different signed generations. This prevents a
    // terminal exact-order query from lending authority to a stale open-orders projection.
    let (readback, _) = signed_readback(venue, &mut evidence, &mut generation, &binding)?;
    let cancels = owned_cancel_commands(&commands, &readback, &binding, generation)?;
    execute_mutations(
        &mut commands,
        venue,
        &authority,
        &writer,
        cancels.into_iter().map(Stage7Mutation::Cancel).collect(),
        true,
    )?;
    require_resolved(&commands)?;

    let (_, mut inventory) =
        wait_for_orders_empty(venue, &mut evidence, &mut generation, &commands, &binding)?;
    flatten_leg(
        venue,
        &mut generation,
        &mut commands,
        &authority,
        &writer,
        &binding,
        PositionSide::Long,
        inventory.long_quantity,
    )?;
    inventory = wait_for_leg_flat(
        venue,
        &mut evidence,
        &mut generation,
        &commands,
        &binding,
        PositionSide::Long,
    )?;
    flatten_leg(
        venue,
        &mut generation,
        &mut commands,
        &authority,
        &writer,
        &binding,
        PositionSide::Short,
        inventory.short_quantity,
    )?;
    let _ = wait_for_leg_flat(
        venue,
        &mut evidence,
        &mut generation,
        &commands,
        &binding,
        PositionSide::Short,
    )?;

    // Two independent newer signed observations close the retirement boundary. The second view
    // must still be flat, order-free and WAL-resolved; a one-frame transient cannot retire writer.
    let (first_final_readback, first_final_inventory) =
        signed_readback(venue, &mut evidence, &mut generation, &binding)?;
    require_final_flat(&commands, &first_final_readback, &first_final_inventory)?;
    let (final_readback, final_inventory) =
        signed_readback(venue, &mut evidence, &mut generation, &binding)?;
    require_final_flat(&commands, &final_readback, &final_inventory)?;
    if generation <= writer.readback_generation {
        return Err(Stage7GridError::Flatten);
    }

    checkpoint.private_generation = generation;
    checkpoint.state.phase = GridPhase::Stopping;
    checkpoint.state.inventory = Some(final_inventory);
    checkpoint.state.owned_orders.clear();
    checkpoint.state.pending_transactions.clear();
    checkpoint.state.pending_replenishments.clear();
    checkpoint_store.save(&checkpoint)?;

    let summary_sha256 = flatten_summary(&binding, writer.generation, generation)?;
    let mut flatten_state = Stage7FlattenState {
        schema_version: 1,
        binding: binding.clone(),
        writer_generation: writer.generation,
        private_generation: generation,
        status: FlattenStatus::Verified,
        summary_sha256: summary_sha256.clone(),
    };
    flatten_store.save(&flatten_state)?;
    authority.retire_flat(&FlatReceipt {
        receipt_id: flatten_receipt_id(&summary_sha256)?,
        predecessor: writer.clone(),
        scope: writer_scope,
        readback_generation: generation,
        summary_sha256,
    })?;
    flatten_state.status = FlattenStatus::Retired;
    flatten_store.save(&flatten_state)?;

    Ok(Stage7FlattenReport {
        exchange: binding.exchange,
        symbol: binding.symbol.to_string(),
        private_generation: generation,
        writer_generation: writer.generation,
        recovered_after_retirement: false,
    })
}

fn load_bound_checkpoint(
    store: &ProjectionStore,
    binding: &HedgedGridBinding,
) -> Result<Stage7GridCheckpoint, Stage7GridError> {
    match store.load::<Stage7GridCheckpoint>()? {
        Some(checkpoint)
            if checkpoint.schema_version == 1
                && checkpoint.binding == *binding
                && checkpoint.state.binding == *binding
                && checkpoint.fill_history_start_ms != 0 =>
        {
            Ok(checkpoint)
        }
        Some(_) | None => Err(Stage7GridError::Checkpoint),
    }
}

fn validate_journal_scope(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
) -> Result<(), Stage7GridError> {
    for (_, owner, _, _) in commands.recovery_identities() {
        validate_owner_binding(&owner, binding)?;
    }
    Ok(())
}

fn signed_readback<V: Stage7CanaryVenue>(
    venue: &mut V,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
) -> Result<(GridVenueReadback, GridInventory), Stage7GridError> {
    let output = canary_cleanup_readback(venue, evidence, generation, binding)?;
    if output.0.raw_private_payloads.is_empty()
        || output.0.positions.iter().any(|position| {
            position.symbol != binding.symbol
                || !matches!(position.side, PositionSide::Long | PositionSide::Short)
                || position.quantity.is_sign_negative()
        })
        || output
            .0
            .orders
            .iter()
            .any(|order| order.symbol != binding.symbol)
    {
        return Err(Stage7GridError::Inventory);
    }
    Ok(output)
}

fn owned_cancel_commands(
    commands: &CommandJournal,
    readback: &GridVenueReadback,
    binding: &HedgedGridBinding,
    generation: u64,
) -> Result<Vec<CancelCommand>, Stage7GridError> {
    readback
        .orders
        .iter()
        .filter(|order| matches!(order.state, OrderState::New | OrderState::PartiallyFilled))
        .map(|order| owned_cancel_command(commands, order, binding, generation))
        .collect()
}

fn owned_cancel_command(
    commands: &CommandJournal,
    order: &Order,
    binding: &HedgedGridBinding,
    generation: u64,
) -> Result<CancelCommand, Stage7GridError> {
    let FieldState::Known(client_order_id) = &order.client_order_id else {
        return Err(Stage7GridError::ForeignOrders);
    };
    let client_order_id =
        CommandId::new(client_order_id.clone()).map_err(|_| Stage7GridError::ForeignOrders)?;
    let command_id = commands
        .command_id_by_client_id(&client_order_id)
        .ok_or(Stage7GridError::ForeignOrders)?;
    match commands.receipt(command_id).map(|receipt| &receipt.state) {
        Some(CommandState::Accepted { venue_order_id }) if venue_order_id == &order.order_id => {}
        Some(CommandState::Accepted { .. }) => return Err(Stage7GridError::ForeignOrders),
        _ => return Err(Stage7GridError::Unresolved),
    }
    let command = commands
        .place_by_client_id(&client_order_id)
        .ok_or(Stage7GridError::ForeignOrders)?;
    validate_owner_binding(&command.owner, binding).map_err(|_| Stage7GridError::ForeignOrders)?;
    if command.client_order_id != client_order_id
        || command.side != order.side
        || !matches!(
            order.position_side,
            FieldState::Known(position_side) if position_side == command.position_side
        )
        || !matches!(order.purpose, FieldState::Known(purpose) if purpose == command.owner.purpose)
        || command.reduce_only != order.reduce_only
        || !checkpoint_quantity_matches(
            command.quantity,
            order.quantity,
            command.reduce_only,
            order.state,
            order.filled_quantity,
        )
        || order.limit_price != Some(command.limit_price)
    {
        return Err(Stage7GridError::ForeignOrders);
    }
    Ok(CancelCommand {
        command_id: hashed_command_id("hgf_c_", &format!("{client_order_id:?}:{generation}"))?,
        owner: command.owner.clone(),
        target_client_order_id: client_order_id,
    })
}

#[allow(clippy::too_many_arguments)]
fn flatten_leg<V: Stage7CanaryVenue>(
    venue: &mut V,
    generation: &mut u64,
    commands: &mut CommandJournal,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    position_side: PositionSide,
    quantity: Decimal,
) -> Result<(), Stage7GridError> {
    if quantity.is_zero() {
        return Ok(());
    }
    require_resolved(commands)?;
    let command = reduce_command(binding, writer, *generation, position_side, quantity)?;
    reduce_canary_market(commands, venue, authority, writer, command)?;
    require_resolved(commands)?;
    Ok(())
}

fn reduce_command(
    binding: &HedgedGridBinding,
    writer: &WriterSession,
    generation: u64,
    position_side: PositionSide,
    quantity: Decimal,
) -> Result<MarketReduceCommand, Stage7GridError> {
    let side_name = match position_side {
        PositionSide::Long => "l",
        PositionSide::Short => "s",
        PositionSide::Net => return Err(Stage7GridError::Flatten),
    };
    let seed = format!(
        "{}:{side_name}:{}:{generation}",
        writer.generation, quantity
    );
    let command = MarketReduceCommand {
        command_id: hashed_command_id("hgf_r_", &seed)?,
        client_order_id: hashed_command_id("hgf_", &seed)?,
        owner: OrderOwner {
            strategy_instance_id: binding.strategy_instance_id.clone(),
            run_id: binding.run_id.clone(),
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
            purpose: OrderPurpose::ExposureTakeProfit,
        },
        side: match position_side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
            PositionSide::Net => return Err(Stage7GridError::Flatten),
        },
        position_side,
        quantity,
        risk_episode_id: hashed_command_id("hgf_e_", &seed)?,
        position_generation: generation,
    };
    command.validate().map_err(|_| Stage7GridError::Flatten)?;
    Ok(command)
}

fn wait_for_orders_empty<V: Stage7CanaryVenue>(
    venue: &mut V,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
) -> Result<(GridVenueReadback, GridInventory), Stage7GridError> {
    for attempt in 0..PRIVATE_SETTLEMENT_ATTEMPTS {
        let output = signed_readback(venue, evidence, generation, binding)?;
        let active = owned_cancel_commands(commands, &output.0, binding, *generation)?;
        if active.is_empty() {
            return Ok(output);
        }
        if attempt + 1 < PRIVATE_SETTLEMENT_ATTEMPTS {
            thread::sleep(Duration::from_millis(PRIVATE_SETTLEMENT_INTERVAL_MS));
        }
    }
    Err(Stage7GridError::Flatten)
}

fn wait_for_leg_flat<V: Stage7CanaryVenue>(
    venue: &mut V,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    side: PositionSide,
) -> Result<GridInventory, Stage7GridError> {
    for attempt in 0..PRIVATE_SETTLEMENT_ATTEMPTS {
        let (readback, inventory) = signed_readback(venue, evidence, generation, binding)?;
        if !owned_cancel_commands(commands, &readback, binding, *generation)?.is_empty() {
            return Err(Stage7GridError::ForeignOrders);
        }
        let quantity = match side {
            PositionSide::Long => inventory.long_quantity,
            PositionSide::Short => inventory.short_quantity,
            PositionSide::Net => return Err(Stage7GridError::Flatten),
        };
        if quantity.is_zero() {
            return Ok(inventory);
        }
        if attempt + 1 < PRIVATE_SETTLEMENT_ATTEMPTS {
            thread::sleep(Duration::from_millis(PRIVATE_SETTLEMENT_INTERVAL_MS));
        }
    }
    Err(Stage7GridError::Flatten)
}

fn require_resolved(commands: &CommandJournal) -> Result<(), Stage7GridError> {
    if commands.has_unresolved() {
        Err(Stage7GridError::Unresolved)
    } else {
        Ok(())
    }
}

fn require_final_flat(
    commands: &CommandJournal,
    readback: &GridVenueReadback,
    inventory: &GridInventory,
) -> Result<(), Stage7GridError> {
    require_resolved(commands)?;
    // A risk-reducing flatten must remain available even when free margin is exhausted. Opening
    // Canary admission requires positive available margin; retirement needs only signed zero
    // orders and zero quantities on both hedge legs.
    if readback
        .all_order_families_empty()
        .map_err(|_| Stage7GridError::OrderFamily)?
        && inventory.long_quantity.is_zero()
        && inventory.short_quantity.is_zero()
    {
        Ok(())
    } else {
        Err(Stage7GridError::Flatten)
    }
}

fn flatten_summary(
    binding: &HedgedGridBinding,
    writer_generation: u64,
    private_generation: u64,
) -> Result<String, Stage7GridError> {
    serde_json::to_vec(&(
        binding,
        writer_generation,
        private_generation,
        "signed_flat_v1",
    ))
    .map(sha256_hex)
    .map_err(|_| Stage7GridError::Flatten)
}

fn flatten_receipt_id(summary_sha256: &str) -> Result<String, Stage7GridError> {
    let prefix = summary_sha256.get(..24).ok_or(Stage7GridError::Flatten)?;
    Ok(format!("hgf_flat_{prefix}"))
}

fn hashed_command_id(prefix: &str, seed: &str) -> Result<CommandId, Stage7GridError> {
    let digest = sha256_hex(seed);
    // Gate reserves its own `t-` prefix and permits at most 28 strategy bytes. Shared Stage-7
    // identities use that stricter limit so the same durable command is valid on both venues.
    let suffix_length = 28usize
        .checked_sub(prefix.len())
        .ok_or(Stage7GridError::Command)?;
    let suffix = digest
        .get(..suffix_length)
        .ok_or(Stage7GridError::Command)?;
    CommandId::new(format!("{prefix}{suffix}")).map_err(|_| Stage7GridError::Command)
}

fn validate_flatten_state(
    state: &Stage7FlattenState,
    binding: &HedgedGridBinding,
) -> Result<(), Stage7GridError> {
    if state.schema_version != 1
        || state.binding != *binding
        || state.writer_generation == 0
        || state.private_generation == 0
        || state.summary_sha256
            != flatten_summary(binding, state.writer_generation, state.private_generation)?
    {
        return Err(Stage7GridError::Flatten);
    }
    Ok(())
}

fn recover_completed_retirement(
    previous: Option<&Stage7FlattenState>,
    store: &ProjectionStore,
    checkpoint: &Stage7GridCheckpoint,
    commands: &CommandJournal,
    evidence_generation: u64,
    binding: &HedgedGridBinding,
) -> Result<Stage7FlattenReport, Stage7GridError> {
    let state = previous.ok_or(Stage7GridError::Writer)?;
    validate_flatten_state(state, binding)?;
    let inventory = checkpoint
        .state
        .inventory
        .as_ref()
        .ok_or(Stage7GridError::Flatten)?;
    if checkpoint.private_generation < state.private_generation
        || evidence_generation < state.private_generation
        || checkpoint.state.phase != GridPhase::Stopping
        || !checkpoint.state.owned_orders.is_empty()
        || !checkpoint.state.pending_transactions.is_empty()
        || !checkpoint.state.pending_replenishments.is_empty()
        || !inventory.long_quantity.is_zero()
        || !inventory.short_quantity.is_zero()
        || commands.has_unresolved()
    {
        return Err(Stage7GridError::Flatten);
    }
    if state.status == FlattenStatus::Verified {
        let mut retired = state.clone();
        retired.status = FlattenStatus::Retired;
        store.save(&retired)?;
    }
    Ok(Stage7FlattenReport {
        exchange: binding.exchange.clone(),
        symbol: binding.symbol.to_string(),
        private_generation: state.private_generation,
        writer_generation: state.writer_generation,
        recovered_after_retirement: true,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::super::legacy_gate_flatten_identity_never_dispatched;
    use super::*;
    use crate::{
        domain::{
            AccountBalance, Amount, Asset, ExecutionCommand, Instrument, MarketKind, OrderCommand,
            Position, Price, Symbol,
        },
        exchange::grid::{
            GridOrderFamilyReadback, GridPrivateEvent, GridVenueError, HedgedGridMutationClient,
            HedgedGridVenue,
        },
        execution::{CapabilityBinding, WriterScope},
        strategy::hedged_grid::{HedgedGridBinding, HedgedGridParams, HedgedGridState},
    };

    fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
        Ok(HedgedGridBinding {
            owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
            strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "gate".to_owned(),
            account: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "DOGE/USDT".parse::<Symbol>()?,
            config_version: "stage7".to_owned(),
        })
    }

    fn writer(binding: &HedgedGridBinding) -> WriterSession {
        WriterSession {
            scope: WriterScope {
                exchange: binding.exchange.clone(),
                account: binding.account.clone(),
                symbol: binding.symbol.clone(),
                owner_scope: binding.owner_scope.clone(),
            },
            token: "test_token".to_owned(),
            generation: 7,
            revision: 1,
            readback_generation: 10,
            valid_until_ms: 100,
        }
    }

    #[derive(Debug)]
    struct FakeVenueState {
        orders: Vec<Order>,
        positions: Vec<Position>,
        reductions: Vec<MarketReduceCommand>,
        query_count: u64,
        exact_absence: bool,
    }

    #[derive(Clone)]
    struct FakeMutationClient {
        state: Arc<Mutex<FakeVenueState>>,
    }

    impl HedgedGridMutationClient for FakeMutationClient {
        fn place_limit_post_only(&self, _command: &OrderCommand) -> Result<String, GridVenueError> {
            Err(GridVenueError::PrivateReadbackRequired)
        }

        fn place_market(
            &self,
            _command: &crate::domain::MarketOrderCommand,
        ) -> Result<String, GridVenueError> {
            Err(GridVenueError::PrivateReadbackRequired)
        }

        fn cancel_by_client_id(&self, command: &CancelCommand) -> Result<String, GridVenueError> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| GridVenueError::PrivateReadbackRequired)?;
            let index = state
                .orders
                .iter()
                .position(|order| {
                    matches!(
                        &order.client_order_id,
                        FieldState::Known(client_id)
                            if client_id == command.target_client_order_id.as_str()
                    )
                })
                .ok_or(GridVenueError::PrivateReadbackRequired)?;
            Ok(state.orders.remove(index).order_id)
        }
    }

    struct FakeVenue {
        instrument: Instrument,
        balance: AccountBalance,
        state: Arc<Mutex<FakeVenueState>>,
    }

    impl HedgedGridVenue for FakeVenue {
        fn exchange(&self) -> &'static str {
            "gate"
        }

        fn instrument(&self) -> &Instrument {
            &self.instrument
        }

        fn minimum_quantity(&self) -> Decimal {
            Decimal::ONE
        }

        fn best_bid_ask(&self, _now_ms: u64) -> Result<(Price, Price), GridVenueError> {
            Ok((
                Price::new(Decimal::ONE).map_err(|_| GridVenueError::PrivateReadbackRequired)?,
                Price::new(Decimal::ONE).map_err(|_| GridVenueError::PrivateReadbackRequired)?,
            ))
        }

        fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
            let state = self
                .state
                .lock()
                .map_err(|_| GridVenueError::PrivateReadbackRequired)?;
            Ok(GridVenueReadback {
                raw_private_payloads: vec!["{\"signed\":true}".to_owned()],
                order_family_readback: Some(GridOrderFamilyReadback::regular_only_adapter_profile(
                    state.orders.clone(),
                    vec!["[]".to_owned()],
                )?),
                balance: self.balance.clone(),
                hedge_position: true,
                positions: state.positions.clone(),
                orders: state.orders.clone(),
                fills: Vec::new(),
            })
        }

        fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
            Ok(())
        }

        fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
            Ok(None)
        }

        fn reset_private_stream(&mut self) {}

        fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
            Arc::new(FakeMutationClient {
                state: Arc::clone(&self.state),
            })
        }

        fn validate_client_order_id(&self, client_order_id: &str) -> Result<(), GridVenueError> {
            if crate::exchange::gate::client_order_id_is_valid(client_order_id) {
                Ok(())
            } else {
                Err(crate::exchange::gate::GateError::ClientOrderId.into())
            }
        }

        fn proves_never_dispatched(
            &self,
            command: &ExecutionCommand,
            unknown_reason: &str,
        ) -> bool {
            unknown_reason == crate::exchange::gate::GateError::ClientOrderId.to_string()
                && matches!(
                    command,
                    ExecutionCommand::PlaceLimit(command)
                        if !crate::exchange::gate::client_order_id_is_valid(
                            command.client_order_id.as_str()
                        )
                )
        }

        fn order_by_client_id(&mut self, client_order_id: &str) -> Result<Order, GridVenueError> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| GridVenueError::PrivateReadbackRequired)?;
            state.query_count = state.query_count.saturating_add(1);
            let order = state
                .orders
                .iter()
                .find(|order| {
                    matches!(&order.client_order_id, FieldState::Known(value) if value == client_order_id)
                })
                .cloned();
            if let Some(order) = order {
                Ok(order)
            } else if state.exact_absence {
                Err(crate::exchange::gate::GateError::OrderAbsent.into())
            } else {
                Err(GridVenueError::PrivateReadbackRequired)
            }
        }

        fn verify_post_only_order(&mut self, _client_order_id: &str) -> Result<(), GridVenueError> {
            Err(GridVenueError::PrivateReadbackRequired)
        }
    }

    impl Stage7CanaryVenue for FakeVenue {
        fn capability_binding(&self) -> CapabilityBinding {
            CapabilityBinding {
                exchange: "gate".to_owned(),
                account_binding: "usdt_futures_dual".to_owned(),
                symbol: self.instrument.symbol.to_string(),
                api_key_sha256: "a".repeat(64),
            }
        }

        fn place_market_reduce(
            &mut self,
            command: &MarketReduceCommand,
        ) -> Result<String, GridVenueError> {
            if command.owner.purpose != OrderPurpose::ExposureTakeProfit {
                return Err(GridVenueError::PrivateReadbackRequired);
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| GridVenueError::PrivateReadbackRequired)?;
            let position = state
                .positions
                .iter_mut()
                .find(|position| position.side == command.position_side)
                .ok_or(GridVenueError::PrivateReadbackRequired)?;
            if position.quantity != command.quantity {
                return Err(GridVenueError::PrivateReadbackRequired);
            }
            position.quantity = Decimal::ZERO;
            state.reductions.push(command.clone());
            Ok(format!("reduce-{:?}", command.position_side))
        }
    }

    fn owned_limit(
        binding: &HedgedGridBinding,
        name: &str,
        side: OrderSide,
        position_side: PositionSide,
        quantity: Decimal,
    ) -> Result<(OrderCommand, Order), Box<dyn std::error::Error>> {
        let command = OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new(format!("{name}_cmd"))?,
            client_order_id: CommandId::new(format!("{name}_client"))?,
            owner: OrderOwner {
                strategy_instance_id: binding.strategy_instance_id.clone(),
                run_id: binding.run_id.clone(),
                exchange: binding.exchange.clone(),
                account: binding.account.clone(),
                symbol: binding.symbol.clone(),
                purpose: OrderPurpose::Entry,
            },
            side,
            position_side,
            quantity,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        };
        let order = Order {
            time_in_force: venue_domain::FieldState::Known(Default::default()),
            order_id: format!("{name}_venue"),
            client_order_id: FieldState::Known(command.client_order_id.as_str().to_owned()),
            symbol: binding.symbol.clone(),
            side,
            position_side: FieldState::Known(position_side),
            purpose: FieldState::Known(OrderPurpose::Entry),
            state: OrderState::New,
            quantity,
            filled_quantity: Decimal::ZERO,
            limit_price: Some(command.limit_price),
            average_price: FieldState::Missing,
            reduce_only: false,
        };
        Ok((command, order))
    }

    #[test]
    fn flatten_reduce_is_exact_hedge_side_market_reduce_and_never_opening()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let long = reduce_command(
            &binding,
            &writer(&binding),
            11,
            PositionSide::Long,
            Decimal::new(5, 0),
        )?;
        assert_eq!(long.owner.purpose, OrderPurpose::ExposureTakeProfit);
        assert_eq!(long.side, OrderSide::Sell);
        assert_eq!(long.quantity, Decimal::new(5, 0));
        assert_eq!(long.position_generation, 11);
        assert_ne!(long.risk_episode_id, long.command_id);
        assert!(long.validate().is_ok());
        assert!(long.client_order_id.as_str().len() <= 28);
        assert!(crate::exchange::gate::client_order_id_is_valid(
            long.client_order_id.as_str()
        ));

        let short = reduce_command(
            &binding,
            &writer(&binding),
            12,
            PositionSide::Short,
            Decimal::new(3, 0),
        )?;
        assert_eq!(short.owner.purpose, OrderPurpose::ExposureTakeProfit);
        assert_eq!(short.side, OrderSide::Buy);
        assert_eq!(short.position_generation, 12);
        Ok(())
    }

    #[test]
    fn flatten_command_identity_is_stable_for_same_signed_fact()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = binding()?;
        let first = reduce_command(
            &binding,
            &writer(&binding),
            11,
            PositionSide::Long,
            Decimal::ONE,
        )?;
        let same = reduce_command(
            &binding,
            &writer(&binding),
            11,
            PositionSide::Long,
            Decimal::ONE,
        )?;
        let newer = reduce_command(
            &binding,
            &writer(&binding),
            12,
            PositionSide::Long,
            Decimal::ONE,
        )?;
        assert_eq!(first, same);
        assert_ne!(first.command_id, newer.command_id);
        Ok(())
    }

    #[test]
    fn prepared_opening_is_rejected_instead_of_resent() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let binding = binding()?;
        let opening = OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new("opening_cmd")?,
            client_order_id: CommandId::new("opening_client")?,
            owner: OrderOwner {
                strategy_instance_id: binding.strategy_instance_id,
                run_id: binding.run_id,
                exchange: binding.exchange,
                account: binding.account,
                symbol: binding.symbol,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        };
        let mut journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
        journal.prepare_place(opening.clone())?;
        journal.fence_interrupted_dispatches()?;
        assert!(matches!(
            journal
                .receipt(&opening.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Rejected { .. })
        ));
        assert!(!journal.has_unresolved());
        Ok(())
    }

    #[test]
    fn submitted_opening_becomes_unknown_and_keeps_flatten_mutations_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let binding = binding()?;
        let opening = OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new("submitted_opening_cmd")?,
            client_order_id: CommandId::new("submitted_opening_client")?,
            owner: OrderOwner {
                strategy_instance_id: binding.strategy_instance_id,
                run_id: binding.run_id,
                exchange: binding.exchange,
                account: binding.account,
                symbol: binding.symbol,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: false,
        };
        let mut journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
        journal.prepare_place(opening.clone())?;
        journal.transition(&opening.command_id, CommandState::Submitted)?;
        journal.fence_interrupted_dispatches()?;
        assert!(matches!(
            journal
                .receipt(&opening.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Unknown { .. })
        ));
        assert!(matches!(
            require_resolved(&journal),
            Err(Stage7GridError::Unresolved)
        ));
        Ok(())
    }

    #[test]
    fn legacy_gate_flatten_identity_is_rejected_without_query_or_dispatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let binding = binding()?;
        let command = OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new(format!("hgf_r_{}", "b".repeat(24)))?,
            client_order_id: CommandId::new(format!("hgf_m_{}", "a".repeat(24)))?,
            owner: OrderOwner {
                strategy_instance_id: binding.strategy_instance_id.clone(),
                run_id: binding.run_id.clone(),
                exchange: binding.exchange.clone(),
                account: binding.account.clone(),
                symbol: binding.symbol.clone(),
                purpose: OrderPurpose::Reduce,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: true,
        };
        let mut journal = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
        journal.prepare_place(command.clone())?;
        journal.transition(&command.command_id, CommandState::Submitted)?;
        journal.transition(
            &command.command_id,
            CommandState::Unknown {
                reason: crate::exchange::gate::GateError::ClientOrderId.to_string(),
            },
        )?;

        let receipt = journal
            .receipt(&command.command_id)
            .cloned()
            .ok_or("missing legacy receipt")?;

        let state = Arc::new(Mutex::new(FakeVenueState {
            orders: Vec::new(),
            positions: Vec::new(),
            reductions: Vec::new(),
            query_count: 0,
            exact_absence: false,
        }));
        let mut venue = FakeVenue {
            instrument: Instrument {
                symbol: binding.symbol.clone(),
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(Asset::new("USDT")?),
                generation: 1,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::ONE,
                minimum_notional: Amount::new(Asset::new("USDT")?, Decimal::ZERO),
            },
            balance: AccountBalance {
                asset: Asset::new("USDT")?,
                wallet_balance: Decimal::ONE,
                available_balance: Decimal::ONE,
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            },
            state: Arc::clone(&state),
        };
        assert!(legacy_gate_flatten_identity_never_dispatched(
            &receipt, &binding, &venue
        ));
        let mut wrong_reason = receipt.clone();
        wrong_reason.state = CommandState::Unknown {
            reason: crate::exchange::gate::GateError::Http.to_string(),
        };
        assert!(!legacy_gate_flatten_identity_never_dispatched(
            &wrong_reason,
            &binding,
            &venue
        ));
        let mut wrong_purpose = receipt.clone();
        if let ExecutionCommand::PlaceLimit(command) = &mut wrong_purpose.command {
            command.owner.purpose = OrderPurpose::Entry;
        }
        assert!(!legacy_gate_flatten_identity_never_dispatched(
            &wrong_purpose,
            &binding,
            &venue
        ));
        let mut non_hex_identity = receipt.clone();
        if let ExecutionCommand::PlaceLimit(command) = &mut non_hex_identity.command {
            command.client_order_id = CommandId::new(format!("hgf_m_{}", "g".repeat(24)))?;
        }
        assert!(!legacy_gate_flatten_identity_never_dispatched(
            &non_hex_identity,
            &binding,
            &venue
        ));
        let mut bitget_binding = binding.clone();
        bitget_binding.exchange = "bitget".to_owned();
        assert!(!legacy_gate_flatten_identity_never_dispatched(
            &receipt,
            &bitget_binding,
            &venue
        ));
        let authority = WriterLeaseAuthority::open(
            temporary.path().join(WRITER_FILE),
            stage7_writer_scope(&binding),
        )?;
        let active_writer = authority.register_initial(1, 1)?;
        let readback = venue.readback()?;
        recover_unresolved(
            &mut journal,
            &mut venue,
            &authority,
            &active_writer,
            &binding,
            &readback,
            false,
        )?;

        assert!(matches!(
            journal
                .receipt(&command.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Rejected { reason })
                if reason == "legacy_gate_flatten_client_id_proved_never_dispatched"
        ));
        let state = state.lock().map_err(|_| "fake venue state poisoned")?;
        assert_eq!(state.query_count, 0);
        assert!(state.reductions.is_empty());
        Ok(())
    }

    #[test]
    fn ordinary_unknown_stays_unresolved_even_after_exact_absence()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let binding = binding()?;
        let command = OrderCommand {
            time_in_force: Default::default(),
            command_id: CommandId::new("ordinary_unknown_command")?,
            client_order_id: CommandId::new("ordinary_unknown_client")?,
            owner: OrderOwner {
                strategy_instance_id: binding.strategy_instance_id.clone(),
                run_id: binding.run_id.clone(),
                exchange: binding.exchange.clone(),
                account: binding.account.clone(),
                symbol: binding.symbol.clone(),
                purpose: OrderPurpose::Reduce,
            },
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::ONE)?,
            reduce_only: true,
        };
        let mut journal = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
        journal.prepare_place(command.clone())?;
        journal.transition(&command.command_id, CommandState::Submitted)?;
        journal.transition(
            &command.command_id,
            CommandState::Unknown {
                reason: crate::exchange::gate::GateError::Http.to_string(),
            },
        )?;
        let state = Arc::new(Mutex::new(FakeVenueState {
            orders: Vec::new(),
            positions: Vec::new(),
            reductions: Vec::new(),
            query_count: 0,
            exact_absence: true,
        }));
        let mut venue = FakeVenue {
            instrument: Instrument {
                symbol: binding.symbol.clone(),
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(Asset::new("USDT")?),
                generation: 1,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::ONE,
                minimum_notional: Amount::new(Asset::new("USDT")?, Decimal::ZERO),
            },
            balance: AccountBalance {
                asset: Asset::new("USDT")?,
                wallet_balance: Decimal::ONE,
                available_balance: Decimal::ONE,
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            },
            state: Arc::clone(&state),
        };
        let authority = WriterLeaseAuthority::open(
            temporary.path().join(WRITER_FILE),
            stage7_writer_scope(&binding),
        )?;
        let active_writer = authority.register_initial(1, 1)?;
        let readback = venue.readback()?;
        recover_unresolved(
            &mut journal,
            &mut venue,
            &authority,
            &active_writer,
            &binding,
            &readback,
            false,
        )?;
        assert!(matches!(
            journal
                .receipt(&command.command_id)
                .map(|receipt| &receipt.state),
            Some(CommandState::Unknown { .. })
        ));
        assert!(journal.has_unresolved());
        assert_eq!(
            state
                .lock()
                .map_err(|_| "fake venue state poisoned")?
                .query_count,
            1
        );
        Ok(())
    }

    #[test]
    fn foreign_or_incomplete_visible_identity_is_refused() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
        let binding = binding()?;
        let readback = GridVenueReadback {
            raw_private_payloads: vec!["{}".to_owned()],
            order_family_readback: Some(GridOrderFamilyReadback::regular_only_adapter_profile(
                Vec::new(),
                vec!["[]".to_owned()],
            )?),
            balance: crate::domain::AccountBalance {
                asset: Asset::new("USDT")?,
                wallet_balance: Decimal::ONE,
                available_balance: Decimal::ONE,
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            },
            hedge_position: true,
            positions: Vec::new(),
            orders: vec![Order {
                time_in_force: venue_domain::FieldState::Known(Default::default()),
                order_id: "foreign".to_owned(),
                client_order_id: FieldState::Missing,
                symbol: binding.symbol.clone(),
                side: OrderSide::Buy,
                position_side: FieldState::Known(PositionSide::Long),
                purpose: FieldState::Known(OrderPurpose::Entry),
                state: OrderState::New,
                quantity: Decimal::ONE,
                filled_quantity: Decimal::ZERO,
                limit_price: Some(Price::new(Decimal::ONE)?),
                average_price: FieldState::Missing,
                reduce_only: false,
            }],
            fills: Vec::new(),
        };
        assert!(matches!(
            owned_cancel_commands(&journal, &readback, &binding, 1),
            Err(Stage7GridError::ForeignOrders)
        ));
        Ok(())
    }

    #[test]
    fn live_flatten_cancels_owned_then_reduces_each_leg_and_retires_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let artifacts_root = temporary.path().join("flatten-root");
        std::fs::create_dir_all(&artifacts_root)?;
        let mut binding = binding()?;
        let unique = sha256_hex(artifacts_root.to_string_lossy().as_bytes());
        let suffix = unique.get(..10).ok_or("missing digest prefix")?;
        binding.owner_scope = format!("hgf_{suffix}_primary");
        binding.strategy_instance_id = format!("hgf_{suffix}");

        let (long_command, long_order) = owned_limit(
            &binding,
            "long_open",
            OrderSide::Buy,
            PositionSide::Long,
            Decimal::new(5, 0),
        )?;
        let (short_command, short_order) = owned_limit(
            &binding,
            "short_open",
            OrderSide::Sell,
            PositionSide::Short,
            Decimal::new(4, 0),
        )?;
        let mut commands = CommandJournal::open(artifacts_root.join(COMMAND_FILE))?;
        for (command, venue_order_id) in [
            (long_command, long_order.order_id.clone()),
            (short_command, short_order.order_id.clone()),
        ] {
            commands.prepare_place(command.clone())?;
            commands.transition(&command.command_id, CommandState::Submitted)?;
            commands.transition(
                &command.command_id,
                CommandState::Accepted { venue_order_id },
            )?;
        }

        let params = HedgedGridParams::fixed_release(Asset::new("USDT")?, 3)?;
        let mut grid_state = HedgedGridState::new_with_params(binding.clone(), params)?;
        grid_state.phase = GridPhase::Running;
        grid_state.inventory = Some(GridInventory {
            private_generation: 1,
            private_observed_at_ms: 1,
            mark_price: Price::new(Decimal::ONE)?,
            long_quantity: Decimal::new(5, 0),
            short_quantity: Decimal::new(4, 0),
        });
        ProjectionStore::new(artifacts_root.join(CHECKPOINT_FILE)).save(&Stage7GridCheckpoint {
            schema_version: 1,
            binding: binding.clone(),
            state: grid_state,
            private_generation: 1,
            exposure_guard: None,
            pending_exposure_reduction: None,
            fill_history_start_ms: 1,
            order_health_fenced: false,
            last_order_health_checked_at_ms: 0,
        })?;
        let authority = WriterLeaseAuthority::open(
            artifacts_root.join(WRITER_FILE),
            stage7_writer_scope(&binding),
        )?;
        let original_writer = authority.register_initial(1, 1)?;
        let state = Arc::new(Mutex::new(FakeVenueState {
            orders: vec![long_order, short_order],
            positions: vec![
                Position {
                    symbol: binding.symbol.clone(),
                    side: PositionSide::Long,
                    quantity: Decimal::new(5, 0),
                    entry_price: Some(Price::new(Decimal::ONE)?),
                    mark_price: Some(Price::new(Decimal::ONE)?),
                },
                Position {
                    symbol: binding.symbol.clone(),
                    side: PositionSide::Short,
                    quantity: Decimal::new(4, 0),
                    entry_price: Some(Price::new(Decimal::ONE)?),
                    mark_price: Some(Price::new(Decimal::ONE)?),
                },
            ],
            reductions: Vec::new(),
            query_count: 0,
            exact_absence: false,
        }));
        let mut venue = FakeVenue {
            instrument: Instrument {
                symbol: binding.symbol.clone(),
                market: MarketKind::LinearPerpetual,
                settlement_asset: Some(Asset::new("USDT")?),
                generation: 1,
                price_tick: Price::new(Decimal::new(1, 2))?,
                quantity_step: Decimal::ONE,
                minimum_notional: Amount::new(Asset::new("USDT")?, Decimal::ONE),
            },
            balance: AccountBalance {
                asset: Asset::new("USDT")?,
                wallet_balance: Decimal::new(100, 0),
                available_balance: Decimal::new(100, 0),
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            },
            state: Arc::clone(&state),
        };

        let report = run_stage7_flatten(
            Stage7FlattenRequest {
                artifacts_root: artifacts_root.clone(),
                confirm_mainnet_grid_mutations: true,
            },
            binding,
            &mut venue,
        )?;
        assert_eq!(report.writer_generation, original_writer.generation);
        assert!(!report.recovered_after_retirement);
        assert!(authority.active_session()?.is_none());
        let state = state
            .lock()
            .map_err(|_| "fake venue state lock was poisoned")?;
        assert!(state.orders.is_empty());
        assert_eq!(state.reductions.len(), 2);
        assert_eq!(state.reductions[0].position_side, PositionSide::Long);
        assert_eq!(state.reductions[1].position_side, PositionSide::Short);
        assert!(state.reductions.iter().all(|command| {
            command.owner.purpose == OrderPurpose::ExposureTakeProfit
                && command.position_generation > 0
        }));
        assert!(!CommandJournal::open(artifacts_root.join(COMMAND_FILE))?.has_unresolved());
        Ok(())
    }

    #[test]
    fn flatten_receipt_identity_fits_native_command_constraints()
    -> Result<(), Box<dyn std::error::Error>> {
        let summary = flatten_summary(&binding()?, 1, 2)?;
        let id = flatten_receipt_id(&summary)?;
        assert!(CommandId::new(id).is_ok());
        Ok(())
    }

    #[test]
    fn verified_flatten_recovers_idempotently_after_writer_retirement()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let binding = binding()?;
        let mut state = HedgedGridState::new_with_params(
            binding.clone(),
            HedgedGridParams::fixed_release(Asset::new("USDT")?, 3)?,
        )?;
        state.phase = GridPhase::Stopping;
        state.inventory = Some(GridInventory {
            private_generation: 20,
            private_observed_at_ms: 1,
            mark_price: Price::new(Decimal::ONE)?,
            long_quantity: Decimal::ZERO,
            short_quantity: Decimal::ZERO,
        });
        let checkpoint = Stage7GridCheckpoint {
            schema_version: 1,
            binding: binding.clone(),
            state,
            private_generation: 20,
            exposure_guard: None,
            pending_exposure_reduction: None,
            fill_history_start_ms: 1,
            order_health_fenced: false,
            last_order_health_checked_at_ms: 0,
        };
        let flatten_store = ProjectionStore::new(temporary.path().join(FLATTEN_FILE));
        let verified = Stage7FlattenState {
            schema_version: 1,
            binding: binding.clone(),
            writer_generation: 7,
            private_generation: 20,
            status: FlattenStatus::Verified,
            summary_sha256: flatten_summary(&binding, 7, 20)?,
        };
        flatten_store.save(&verified)?;
        let commands = CommandJournal::open(temporary.path().join("commands.jsonl"))?;

        let report = recover_completed_retirement(
            Some(&verified),
            &flatten_store,
            &checkpoint,
            &commands,
            20,
            &binding,
        )?;
        assert!(report.recovered_after_retirement);
        assert_eq!(
            flatten_store
                .load::<Stage7FlattenState>()?
                .map(|state| state.status),
            Some(FlattenStatus::Retired)
        );
        Ok(())
    }
}
