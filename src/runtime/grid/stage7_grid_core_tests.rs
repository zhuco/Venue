use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, mpsc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use super::fill_sequence_tests::intent;
use super::stage7_canary_support::{STAGE7_LIVE_ADMISSION_FILE, Stage7LiveAdmissionEvidence};
use super::*;
use crate::{
    config::{
        BitgetAccountBinding, BitgetConfig, ExposureTakeProfitConfig, GateConfig, HedgedGridConfig,
        LogLevel,
    },
    domain::{
        AccountBalance, Amount, Asset, FieldState, Fill, Instrument, MarketKind, OrderPurpose,
        OrderState, Position, PositionSide,
    },
    exchange::grid::{
        GridOrderFamilyReadback, GridPrivateEvent, GridPublicPayload, GridPublicPayloadSource,
        GridRiskReadback, GridVenueFill, HedgedGridMutationClient,
        HedgedGridRiskReadbackClient,
    },
    execution::CapabilityBinding,
    runtime::hedged_grid,
    strategy::hedged_grid::{
        GridEpoch, GridInventory, GridOrderIntent, GridOrderKey, GridOrderRole, GridPhase,
        GridPosition,
    },
};

const TEST_PUBLIC_FRESHNESS_MS: u64 = 5_000;

struct UnavailableMutationClient;

struct GatedRiskClient {
    gate: Mutex<mpsc::Receiver<()>>,
}

impl HedgedGridRiskReadbackClient for GatedRiskClient {
    fn risk_readback(
        &self,
        _account: &str,
        _private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError> {
        self.gate
            .lock()
            .map_err(|_| GridVenueError::RiskReadbackUnsupported)?
            .recv()
            .map_err(|_| GridVenueError::RiskReadbackUnsupported)?;
        Err(GridVenueError::RiskReadbackUnsupported)
    }
}

impl HedgedGridMutationClient for UnavailableMutationClient {
    fn place_limit_post_only(
        &self,
        _command: &crate::domain::OrderCommand,
    ) -> Result<String, GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }

    fn place_market(&self, _command: &MarketOrderCommand) -> Result<String, GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }

    fn cancel_by_client_id(&self, _command: &CancelCommand) -> Result<String, GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }
}

#[derive(Clone)]
struct RecordingMutationClient {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl HedgedGridMutationClient for RecordingMutationClient {
    fn place_limit_post_only(
        &self,
        command: &crate::domain::OrderCommand,
    ) -> Result<String, GridVenueError> {
        self.calls
            .lock()
            .map_err(|_| GridVenueError::PrivateReadbackRequired)?
            .push("place");
        Ok(command.client_order_id.as_str().to_owned())
    }

    fn place_market(&self, _command: &MarketOrderCommand) -> Result<String, GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }

    fn cancel_by_client_id(&self, command: &CancelCommand) -> Result<String, GridVenueError> {
        self.calls
            .lock()
            .map_err(|_| GridVenueError::PrivateReadbackRequired)?
            .push("cancel");
        Ok(command.target_client_order_id.as_str().to_owned())
    }
}

struct StreamFillVenue {
    instrument: Instrument,
    client: RecordingMutationClient,
    readback_calls: Arc<AtomicUsize>,
    book_reads: Arc<AtomicUsize>,
    readbacks: VecDeque<Result<GridVenueReadback, GridVenueError>>,
    private_events: VecDeque<GridPrivateEvent>,
    private_empty_polls: usize,
    risk_client: Option<Arc<dyn HedgedGridRiskReadbackClient>>,
    exact_order_outcomes: VecDeque<Result<Order, GridVenueError>>,
    book: (Price, Price),
}

impl HedgedGridVenue for StreamFillVenue {
    fn exchange(&self) -> &'static str {
        "gate"
    }

    fn instrument(&self) -> &Instrument {
        &self.instrument
    }

    fn minimum_quantity(&self) -> Decimal {
        self.instrument.quantity_step
    }

    fn verify_current_instrument_rules(&mut self) -> Result<(), GridVenueError> {
        Ok(())
    }

    fn best_bid_ask(&self, _now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        self.book_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.book)
    }

    fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
        self.readback_calls.fetch_add(1, Ordering::SeqCst);
        self.readbacks
            .pop_front()
            .unwrap_or(Err(GridVenueError::PrivateReadbackRequired))
    }

    fn risk_readback_client(&self) -> Option<Arc<dyn HedgedGridRiskReadbackClient>> {
        self.risk_client.clone()
    }

    fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
        Ok(())
    }

    fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
        if self.private_empty_polls > 0 {
            self.private_empty_polls -= 1;
            return Ok(None);
        }
        Ok(self.private_events.pop_front())
    }

    fn reset_private_stream(&mut self) {}

    fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
        Arc::new(self.client.clone())
    }

    fn order_by_client_id(&mut self, _client_order_id: &str) -> Result<Order, GridVenueError> {
        self.exact_order_outcomes
            .pop_front()
            .unwrap_or(Err(GridVenueError::PrivateReadbackRequired))
    }

    fn verify_post_only_order(&mut self, _client_order_id: &str) -> Result<(), GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }
}

struct ShadowVenue {
    instrument: Instrument,
    readbacks: VecDeque<GridVenueReadback>,
    minimum_quantity: Decimal,
    stream_polls_before_error: Option<u8>,
    stream_resets: u8,
    public_payloads: VecDeque<GridPublicPayload>,
    accepted_public_at_ms: Option<u64>,
    public_resets: u8,
    exact_order_outcomes: VecDeque<Result<Order, GridVenueError>>,
}

impl HedgedGridVenue for ShadowVenue {
    fn exchange(&self) -> &'static str {
        "gate"
    }
    fn instrument(&self) -> &Instrument {
        &self.instrument
    }

    fn minimum_quantity(&self) -> Decimal {
        self.minimum_quantity
    }

    fn verify_current_instrument_rules(&mut self) -> Result<(), GridVenueError> {
        if self.public_resets == u8::MAX - 1 {
            Err(GridVenueError::InstrumentRulesDrift)
        } else if self.public_resets == u8::MAX - 2 {
            self.public_resets = 0;
            Err(GridVenueError::InstrumentRulesUnavailable)
        } else {
            Ok(())
        }
    }

    fn connect_public_stream(&mut self) -> Result<(), GridVenueError> {
        Ok(())
    }

    fn next_public_payload(&mut self) -> Result<Option<GridPublicPayload>, GridVenueError> {
        Ok(self.public_payloads.pop_front())
    }

    fn accept_public_payload(&mut self, payload: GridPublicPayload) -> Result<(), GridVenueError> {
        self.accepted_public_at_ms = Some(payload.received_at_ms);
        Ok(())
    }

    fn reset_public_stream(&mut self) {
        self.public_resets = self.public_resets.saturating_add(1);
        self.accepted_public_at_ms = None;
    }

    fn best_bid_ask(&self, now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        if self.public_resets == u8::MAX {
            return Err(GridVenueError::PublicNotReady);
        }
        if self.public_resets == u8::MAX - 3
            && self.accepted_public_at_ms.is_some_and(|received_at_ms| {
                now_ms.saturating_sub(received_at_ms) > TEST_PUBLIC_FRESHNESS_MS
            })
        {
            return Err(GridVenueError::PublicNotReady);
        }
        if self
            .accepted_public_at_ms
            .is_some_and(|received_at_ms| now_ms < received_at_ms)
        {
            return Err(GridVenueError::PublicNotReady);
        }
        Ok((
            Price::new(Decimal::new(1, 1)).map_err(|_| GridVenueError::PrivateReadbackRequired)?,
            Price::new(Decimal::new(11, 2)).map_err(|_| GridVenueError::PrivateReadbackRequired)?,
        ))
    }

    fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
        self.readbacks
            .pop_front()
            .ok_or(GridVenueError::PrivateReadbackRequired)
    }

    fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
        Ok(())
    }
    fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
        if let Some(polls) = self.stream_polls_before_error.as_mut() {
            if *polls == 0 {
                self.stream_polls_before_error = None;
                return Err(GridVenueError::PrivateReadbackRequired);
            }
            *polls = polls.saturating_sub(1);
        }
        Ok(None)
    }
    fn reset_private_stream(&mut self) {
        self.stream_resets = self.stream_resets.saturating_add(1);
    }
    fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
        Arc::new(UnavailableMutationClient)
    }

    fn order_by_client_id(&mut self, _client_order_id: &str) -> Result<Order, GridVenueError> {
        self.exact_order_outcomes
            .pop_front()
            .unwrap_or(Err(GridVenueError::PrivateReadbackRequired))
    }

    fn verify_post_only_order(&mut self, _client_order_id: &str) -> Result<(), GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }
}

impl Stage7CanaryVenue for ShadowVenue {
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
        _command: &crate::domain::MarketReduceCommand,
    ) -> Result<String, GridVenueError> {
        Err(GridVenueError::PrivateReadbackRequired)
    }
}

fn config(grid_count: u8) -> Result<Config, Box<dyn std::error::Error>> {
    Ok(Config {
        log: LogLevel::Info,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        symbol: "DOGE/USDT".parse()?,
        binance: None,
        gate: Some(GateConfig {
            account_binding: GateAccountBinding::UsdtFuturesDual,
            private_custody_max_stale_ms: 5_000,
        }),
        bitget: None,
        hedged_grid: Some(HedgedGridConfig {
            grid_count,
            exposure_take_profit: None,
        }),
    })
}

fn shadow_readback() -> Result<GridVenueReadback, Box<dyn std::error::Error>> {
    Ok(GridVenueReadback {
        raw_private_payloads: vec![
            "{\"account\":true}".to_owned(),
            "{\"mode\":\"dual\"}".to_owned(),
            "[]".to_owned(),
            "[]".to_owned(),
            "[]".to_owned(),
        ],
        order_family_readback: Some(GridOrderFamilyReadback::regular_only_adapter_profile(
            Vec::new(),
            vec!["[]".to_owned()],
        )?),
        balance: AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::new(100, 0),
            available_balance: Decimal::new(100, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        },
        hedge_position: true,
        positions: Vec::new(),
        orders: Vec::new(),
        fills: Vec::<GridVenueFill>::new(),
    })
}

#[test]
fn stage7_rejects_missing_signed_order_family_coverage() -> Result<(), Box<dyn std::error::Error>> {
    let mut readback = shadow_readback()?;
    readback.order_family_readback = None;

    assert!(matches!(
        require_complete_order_family_readback(&readback),
        Err(Stage7GridError::OrderFamily)
    ));
    Ok(())
}

#[test]
fn stage7_rejects_nonregular_rows_without_a_wal_owner() -> Result<(), Box<dyn std::error::Error>> {
    let mut readback = shadow_readback()?;
    let conditional = Order {
        time_in_force: venue_domain::FieldState::Known(Default::default()),
        order_id: "conditional-7".to_owned(),
        client_order_id: FieldState::Known("external-conditional".to_owned()),
        symbol: "DOGE/USDT".parse()?,
        side: OrderSide::Sell,
        position_side: FieldState::Known(PositionSide::Long),
        purpose: FieldState::Missing,
        state: OrderState::New,
        quantity: Decimal::ONE,
        filled_quantity: Decimal::ZERO,
        limit_price: Some(Price::new(Decimal::ONE)?),
        average_price: FieldState::Missing,
        reduce_only: true,
    };
    readback.order_family_readback = Some(GridOrderFamilyReadback::complete_adapter_profile(
        Vec::new(),
        vec!["[]".to_owned()],
        vec![conditional],
        vec!["[\"conditional-7\"]".to_owned()],
        Vec::new(),
        vec!["[]".to_owned()],
    )?);

    assert!(matches!(
        require_no_unmanaged_order_family_rows(&readback),
        Err(Stage7GridError::ForeignOrders)
    ));
    Ok(())
}

fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
    Ok(Instrument {
        symbol: "DOGE/USDT".parse()?,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(Asset::new("USDT")?),
        generation: 1,
        price_tick: Price::new(Decimal::new(1, 5))?,
        quantity_step: Decimal::ONE,
        minimum_notional: Amount::new(Asset::new("USDT")?, Decimal::ZERO),
    })
}

#[test]
fn resident_fill_hot_path_dispatches_while_risk_worker_is_blocked()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let mut cfg = config(3)?;
    let exposure_config = ExposureTakeProfitConfig {
        enabled: true,
        shadow: true,
        position_equity_multiple: Decimal::new(3, 0),
        unrealized_pnl_equity_ratio: Decimal::new(5, 2),
        reduce_ratio: Decimal::new(30, 2),
        snapshot_interval_ms: 120_000,
        max_snapshot_age_ms: 3_000,
        rearm_clear_generations: 2,
    };
    cfg.hedged_grid
        .as_mut()
        .ok_or("missing grid config")?
        .exposure_take_profit = Some(exposure_config);
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    let _ = state.observe_inventory(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(20, 2),
        short_quantity: Decimal::new(20, 2),
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
        .find(|order| {
            order.key.position == GridPosition::Long
                && order.key.role == GridOrderRole::Open
                && order.key.level == 1
        })
        .cloned()
        .ok_or("missing stream-fill source")?;
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    let mut original_venue_order_id = None;
    let mut signed_orders = Vec::new();
    for owned in state.owned_orders.values() {
        let GridMutation::Place(original) = place_command(&binding, &instrument()?, owned)? else {
            return Err("owned intent did not create a place command".into());
        };
        let command_id = original.command_id.clone();
        let venue_order_id = original.client_order_id.as_str().to_owned();
        if owned.key == source.key {
            original_venue_order_id = Some(venue_order_id.clone());
        }
        signed_orders.push(Order {
            time_in_force: venue_domain::FieldState::Known(Default::default()),
            order_id: venue_order_id.clone(),
            client_order_id: FieldState::Known(venue_order_id.clone()),
            symbol: binding.symbol.clone(),
            side: original.side,
            position_side: FieldState::Known(original.position_side),
            purpose: FieldState::Known(if owned.reduce_only {
                OrderPurpose::Reduce
            } else {
                OrderPurpose::Entry
            }),
            state: OrderState::New,
            quantity: original.quantity,
            filled_quantity: Decimal::ZERO,
            limit_price: Some(original.limit_price),
            average_price: FieldState::Missing,
            reduce_only: original.reduce_only,
        });
        commands.prepare_place(original)?;
        commands.transition(&command_id, CommandState::Submitted)?;
        commands.transition(&command_id, CommandState::Accepted { venue_order_id })?;
    }
    let original_command_ids = commands
        .commands()
        .map(|command| command.command_id().clone())
        .collect::<Vec<_>>();
    let original_venue_order_id = original_venue_order_id.ok_or("missing source venue id")?;
    let checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE)).save(&checkpoint)?;
    set_stage7_grid_control(
        &cfg,
        temporary.path(),
        HedgedGridControlTarget::Running,
    )?;
    let admission = Stage7LiveAdmissionEvidence::new_with_exposure(
        CapabilityBinding {
            exchange: binding.exchange.clone(),
            account_binding: "usdt_futures_dual".to_owned(),
            symbol: binding.symbol.to_string(),
            api_key_sha256: "a".repeat(64),
        },
        binding.clone(),
        checkpoint.state.params.clone(),
        instrument()?,
        Decimal::ONE,
        Some(exposure_config),
        "b".repeat(64),
        10,
        100,
        1,
        1,
    )?;
    ProjectionStore::new(temporary.path().join(STAGE7_LIVE_ADMISSION_FILE)).save(&admission)?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let readback_calls = Arc::new(AtomicUsize::new(0));
    let book_reads = Arc::new(AtomicUsize::new(0));
    let mut readback = shadow_readback()?;
    readback.positions = vec![
        Position {
            symbol: binding.symbol.clone(),
            side: PositionSide::Long,
            quantity: Decimal::new(20, 2),
            entry_price: Some(Price::new(Decimal::new(100, 0))?),
            mark_price: Some(Price::new(Decimal::new(100, 0))?),
        },
        Position {
            symbol: binding.symbol.clone(),
            side: PositionSide::Short,
            quantity: Decimal::new(20, 2),
            entry_price: Some(Price::new(Decimal::new(100, 0))?),
            mark_price: Some(Price::new(Decimal::new(100, 0))?),
        },
    ];
    readback.orders = signed_orders.clone();
    readback.order_family_readback = Some(GridOrderFamilyReadback::regular_only_adapter_profile(
        signed_orders,
        vec!["signed resident order surface".to_owned()],
    )?);
    let private_event = GridPrivateEvent::Fill {
        fill: Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "resident-stream-fill-1".to_owned(),
            order_id: original_venue_order_id,
            symbol: binding.symbol.clone(),
            side: source.side,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: source.quantity,
            price: source.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(150),
        },
        client_order_id: FieldState::Known(client_order_id(&source.key)?.as_str().to_owned()),
        raw_payload: "resident private complete fill".to_owned(),
    };
    let (release_risk, risk_gate) = mpsc::channel();
    let venue = StreamFillVenue {
        instrument: instrument()?,
        client: RecordingMutationClient {
            calls: Arc::clone(&calls),
        },
        readback_calls: Arc::clone(&readback_calls),
        book_reads: Arc::clone(&book_reads),
        readbacks: VecDeque::from([
            Ok(readback),
            Err(GridVenueError::Gate(
                crate::exchange::gate::GateError::Http,
            )),
        ]),
        private_events: VecDeque::from([private_event]),
        private_empty_polls: 1,
        risk_client: Some(Arc::new(GatedRiskClient {
            gate: Mutex::new(risk_gate),
        })),
        exact_order_outcomes: VecDeque::new(),
        book: (
            Price::new(Decimal::new(200, 0))?,
            Price::new(Decimal::new(201, 0))?,
        ),
    };
    let wal_path = temporary.path().join(COMMAND_FILE);
    let wal_len_before_dispatch = std::fs::metadata(&wal_path)?.len();
    drop(commands);
    let artifacts_root = temporary.path().to_path_buf();
    let (done_tx, done_rx) = mpsc::channel();
    let resident = thread::spawn(move || {
        let mut venue = venue;
        let result = run_stage7_grid(
            &cfg,
            Stage7GridRequest {
                artifacts_root,
                max_turns: Some(2),
                reset_on_start: false,
                skip_inventory_replenishment_until_recovered: false,
                confirm_mainnet_grid_mutations: true,
                shadow_only: false,
                stop_after_first_owned_fill: false,
                wall_clock_deadline_ms: None,
                force_order_health_check: false,
            },
            binding,
            &mut venue,
        )
        .map_err(|error| error.to_string());
        let _ = done_tx.send(result);
    });
    let report = match done_rx.recv_timeout(Duration::from_secs(1)) {
        Ok(result) => result.map_err(std::io::Error::other)?,
        Err(_) => {
            let _ = release_risk.send(());
            let _ = resident.join();
            return Err("resident blocked behind the in-flight risk worker".into());
        }
    };
    assert_eq!(report.turns, 2);
    assert!(std::fs::metadata(wal_path)?.len() > wal_len_before_dispatch);
    assert_eq!(readback_calls.load(Ordering::SeqCst), 2);
    assert!(book_reads.load(Ordering::SeqCst) > 0);
    let mut recorded = calls
        .lock()
        .map_err(|_| "recording client lock poisoned")?
        .clone();
    recorded.sort_unstable();
    assert_eq!(recorded, ["cancel", "place", "place"]);
    let commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    let dispatched = commands
        .commands()
        .filter(|command| !original_command_ids.contains(command.command_id()))
        .collect::<Vec<_>>();
    assert_eq!(dispatched.len(), 3);
    assert_eq!(
        dispatched
            .iter()
            .filter(|command| matches!(command, ExecutionCommand::PlaceLimit(_)))
            .count(),
        2
    );
    assert_eq!(
        dispatched
            .iter()
            .filter(|command| matches!(command, ExecutionCommand::Cancel(_)))
            .count(),
        1
    );
    assert!(dispatched.iter().all(|command| {
        commands.receipt(command.command_id()).is_some_and(|receipt| {
            matches!(receipt.state, CommandState::Accepted { .. })
        })
    }));
    assert!(!commands.has_unresolved());
    release_risk.send(())?;
    resident
        .join()
        .map_err(|_| "resident test thread panicked")?;
    Ok(())
}

#[test]
fn complete_stream_fill_dispatches_without_signed_readback_or_bbo_gate()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    let _ = state.observe_inventory(GridInventory {
        private_generation: 1,
        private_observed_at_ms: 100,
        mark_price: Price::new(Decimal::new(100, 0))?,
        long_quantity: Decimal::new(20, 2),
        short_quantity: Decimal::new(20, 2),
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
        .find(|order| {
            order.key.position == GridPosition::Long
                && order.key.role == GridOrderRole::Open
                && order.key.level == 1
        })
        .cloned()
        .ok_or("missing stream-fill source")?;
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    let mut original_venue_order_id = None;
    for owned in state.owned_orders.values() {
        let GridMutation::Place(original) = place_command(&binding, &instrument()?, owned)? else {
            return Err("owned intent did not create a place command".into());
        };
        let command_id = original.command_id.clone();
        let venue_order_id = original.client_order_id.as_str().to_owned();
        if owned.key == source.key {
            original_venue_order_id = Some(venue_order_id.clone());
        }
        commands.prepare_place(original)?;
        commands.transition(&command_id, CommandState::Submitted)?;
        commands.transition(&command_id, CommandState::Accepted { venue_order_id })?;
    }
    let mut checkpoint = Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    };
    let authority = WriterLeaseAuthority::open(
        temporary.path().join(WRITER_FILE),
        stage7_writer_scope(&binding),
    )?;
    let writer = authority.register_initial(1, 1)?;
    let calls = Arc::new(Mutex::new(Vec::new()));
    let readback_calls = Arc::new(AtomicUsize::new(0));
    let book_reads = Arc::new(AtomicUsize::new(0));
    let mut venue = StreamFillVenue {
        instrument: instrument()?,
        client: RecordingMutationClient {
            calls: Arc::clone(&calls),
        },
        readback_calls: Arc::clone(&readback_calls),
        book_reads: Arc::clone(&book_reads),
        readbacks: VecDeque::new(),
        private_events: VecDeque::new(),
        private_empty_polls: 0,
        risk_client: None,
        exact_order_outcomes: VecDeque::new(),
        book: (
            Price::new(Decimal::new(200, 0))?,
            Price::new(Decimal::new(201, 0))?,
        ),
    };
    let mut accumulator = Stage7StreamFillAccumulator::default();
    let store = ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE));
    let wal_path = temporary.path().join(COMMAND_FILE);
    let wal_len_before_dispatch = std::fs::metadata(&wal_path)?.len();
    let fill = GridVenueFill {
        fill: Fill {
            execution_sequence: FieldState::Known(1),
            fill_id: "stream-fill-zero-rest".to_owned(),
            order_id: original_venue_order_id.ok_or("missing source venue id")?,
            symbol: binding.symbol.clone(),
            side: source.side,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: source.quantity,
            price: source.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(150),
        },
        client_order_id: FieldState::Known(client_order_id(&source.key)?.as_str().to_owned()),
    };

    assert_eq!(
        process_stream_grid_fill(
            &mut checkpoint,
            &store,
            &mut commands,
            &mut venue,
            &authority,
            &writer,
            &binding,
            &mut accumulator,
            fill,
            2,
            200,
        )?,
        FillDriveOutcome::dispatched()
    );
    assert!(std::fs::metadata(wal_path)?.len() > wal_len_before_dispatch);
    assert_eq!(readback_calls.load(Ordering::SeqCst), 0);
    assert_eq!(book_reads.load(Ordering::SeqCst), 0);
    let mut recorded = calls
        .lock()
        .map_err(|_| "recording client lock poisoned")?
        .clone();
    recorded.sort_unstable();
    assert_eq!(recorded, ["cancel", "place", "place"]);
    assert!(!commands.has_unresolved());
    Ok(())
}

#[test]
fn shared_resident_uses_the_configured_opening_depth() -> Result<(), Box<dyn std::error::Error>> {
    let binding = gate_binding(&config(3)?)?;
    assert_eq!(release_params(&config(3)?, &binding)?.grid_count, 3);
    assert_eq!(release_params(&config(4)?, &binding)?.grid_count, 4);
    Ok(())
}

#[test]
fn canary_position_wait_distinguishes_zero_from_a_held_leg() {
    assert!(stage7_canary_support::position_presence_matches(
        Decimal::ZERO,
        false
    ));
    assert!(!stage7_canary_support::position_presence_matches(
        Decimal::ZERO,
        true
    ));
    assert!(stage7_canary_support::position_presence_matches(
        Decimal::ONE,
        true
    ));
    assert!(!stage7_canary_support::position_presence_matches(
        Decimal::ONE,
        false
    ));
}

#[test]
fn transient_private_readbacks_remain_fenced_for_both_stage7_exchanges() {
    assert!(stage7_retry::is_transient_readback_error(
        &GridVenueError::Gate(crate::exchange::gate::GateError::Http)
    ));
    assert!(stage7_retry::is_transient_readback_error(
        &GridVenueError::Bitget(crate::exchange::bitget::BitgetError::RateLimited)
    ));
    assert!(!stage7_retry::is_transient_readback_error(
        &GridVenueError::Gate(crate::exchange::gate::GateError::Rejected {
            label: "POC_FILL_IMMEDIATELY".to_owned(),
        })
    ));
    assert!(stage7_retry::is_transient_instrument_rule_error(
        &GridVenueError::InstrumentRulesUnavailable
    ));
    assert!(!stage7_retry::is_transient_instrument_rule_error(
        &GridVenueError::InstrumentRulesDrift
    ));
    assert!(stage7_retry::is_transient_venue_startup_error(
        &GridVenueError::Gate(crate::exchange::gate::GateError::Http)
    ));
    assert!(stage7_retry::is_transient_venue_startup_error(
        &GridVenueError::InstrumentRulesUnavailable
    ));
    assert!(!stage7_retry::is_transient_venue_startup_error(
        &GridVenueError::InstrumentRulesDrift
    ));
}

#[test]
fn bitget_fill_history_window_advances_only_with_a_five_minute_overlap() {
    assert_eq!(
        next_fill_history_start_ms("bitget", 1, 600_001),
        Some(300_001)
    );
    assert_eq!(next_fill_history_start_ms("bitget", 300_001, 600_001), None);
    assert_eq!(next_fill_history_start_ms("gate", 1, 600_001), None);
}

#[test]
fn expired_grid_writer_recovers_only_after_the_new_signed_readback()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let scope = WriterScope {
        exchange: "gate".to_owned(),
        account: "usdt_futures".to_owned(),
        symbol: "DOGE/USDT".parse()?,
        owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
    };
    let authority = WriterLeaseAuthority::open(temporary.path().join("writer.json"), scope)?;
    let initial = authority.register_initial(1, 1)?;

    let recovered = active_writer(
        &authority,
        Some(initial.clone()),
        crate::execution::WRITER_LEASE_TTL_MS + 1,
        2,
    )?;
    assert_eq!(recovered.generation, initial.generation);
    assert_eq!(recovered.readback_generation, 2);
    assert!(recovered.valid_until_ms > crate::execution::WRITER_LEASE_TTL_MS + 1);
    Ok(())
}

#[test]
fn public_runtime_uses_the_last_durable_frame_time_within_a_busy_turn()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let artifacts_root = temporary.path().join("public-runtime");
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::from([GridPublicPayload {
            generation: 1,
            source: GridPublicPayloadSource::WebSocketBbo,
            received_at_ms: 11,
            payload: "{}".to_owned(),
        }]),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let mut runtime = Stage7PublicRuntime::open(&artifacts_root, &binding)?;

    assert!(runtime.drive(&mut venue, 10)?);
    assert_eq!(venue.accepted_public_at_ms, Some(11));
    assert_eq!(venue.public_resets, 0);
    let recorded = std::fs::read_to_string(artifacts_root.join("public_market.jsonl"))?;
    assert!(recorded.contains("web_socket_bbo"));
    Ok(())
}

#[test]
fn public_runtime_drains_more_than_the_legacy_128_frame_limit_in_one_turn()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let artifacts_root = temporary.path().join("public-runtime-large-batch");
    let public_payloads = (1_u64..=512)
        .map(|received_at_ms| GridPublicPayload {
            generation: 1,
            source: GridPublicPayloadSource::WebSocketBbo,
            received_at_ms,
            payload: format!("{{\"sequence\":{received_at_ms}}}"),
        })
        .collect();
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads,
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let mut runtime = Stage7PublicRuntime::open(&artifacts_root, &binding)?;

    assert!(runtime.drive(&mut venue, 1)?);
    assert!(venue.public_payloads.is_empty());
    assert_eq!(venue.accepted_public_at_ms, Some(512));
    let recorded = std::fs::read_to_string(artifacts_root.join("public_market.jsonl"))?;
    assert_eq!(
        recorded.lines().filter(|line| !line.is_empty()).count(),
        512
    );
    Ok(())
}

#[test]
fn second_public_drain_refreshes_frames_after_private_checks_outlive_freshness()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let artifacts_root = temporary.path().join("public-runtime-post-private-refresh");
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::from([GridPublicPayload {
            generation: 1,
            source: GridPublicPayloadSource::WebSocketBbo,
            received_at_ms: 10,
            payload: "{\"sequence\":1}".to_owned(),
        }]),
        accepted_public_at_ms: None,
        public_resets: u8::MAX - 3,
        exact_order_outcomes: VecDeque::new(),
    };
    let mut runtime = Stage7PublicRuntime::open(&artifacts_root, &binding)?;

    assert!(runtime.drive(&mut venue, 10)?);
    assert!(matches!(
        venue.best_bid_ask(10 + TEST_PUBLIC_FRESHNESS_MS + 1),
        Err(GridVenueError::PublicNotReady)
    ));

    venue.public_payloads.push_back(GridPublicPayload {
        generation: 1,
        source: GridPublicPayloadSource::WebSocketBbo,
        received_at_ms: 10 + TEST_PUBLIC_FRESHNESS_MS + 1,
        payload: "{\"sequence\":2}".to_owned(),
    });
    assert!(runtime.drive(&mut venue, 10 + TEST_PUBLIC_FRESHNESS_MS + 1)?);
    assert_eq!(
        venue.accepted_public_at_ms,
        Some(10 + TEST_PUBLIC_FRESHNESS_MS + 1)
    );
    Ok(())
}

#[test]
fn stage7_shadow_reads_signed_private_payloads_without_writing_artifacts()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let private_path = temporary.path().join(PRIVATE_EVIDENCE_FILE);
    let mut persisted = PrivateEvidenceJournal::open(&private_path)?;
    persisted.append(PrivateEvidence::new(
        7,
        100,
        "existing private evidence".to_owned(),
    )?)?;
    let before = std::fs::read(&private_path)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: false,
            shadow_only: true,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;
    assert!(report.shadow_only);
    assert_eq!(report.private_generation, 8);
    assert_eq!(std::fs::read(&private_path)?, before);
    assert_eq!(
        PrivateEvidenceJournal::open(&private_path)?
            .recover()?
            .len(),
        1
    );
    assert!(!temporary.path().join(CHECKPOINT_FILE).exists());
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    assert!(!temporary.path().join("public_market.jsonl").exists());
    assert!(
        !temporary
            .path()
            .join(hedged_grid::EXPOSURE_SHADOW_EVIDENCE_FILE)
            .exists()
    );
    Ok(())
}

#[test]
fn stage7_shadow_observes_predecessor_orders_without_adopting_or_mutating_them()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let mut readback = shadow_readback()?;
    readback.orders.push(Order {
        time_in_force: venue_domain::FieldState::Known(Default::default()),
        order_id: "predecessor-order".to_owned(),
        client_order_id: FieldState::Known("legacy-owner-order".to_owned()),
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
    });
    readback.order_family_readback = Some(GridOrderFamilyReadback::regular_only_adapter_profile(
        readback.orders.clone(),
        vec!["[\"predecessor-order\"]".to_owned()],
    )?);
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([readback]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };

    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: false,
            shadow_only: true,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;

    assert!(report.shadow_only);
    assert_eq!(report.private_generation, 1);
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    assert!(!temporary.path().join(CHECKPOINT_FILE).exists());
    assert!(!temporary.path().join("public_market.jsonl").exists());
    Ok(())
}

#[test]
fn stage7_shadow_leaves_a_predecessor_reset_control_unconsumed()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    set_stage7_grid_control(&cfg, temporary.path(), HedgedGridControlTarget::Reset)?;
    let control_path = temporary.path().join(CONTROL_FILE);
    let before = std::fs::read(&control_path)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };

    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: false,
            shadow_only: true,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;

    assert!(report.shadow_only);
    assert_eq!(std::fs::read(&control_path)?, before);
    assert!(!temporary.path().join(CHECKPOINT_FILE).exists());
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    Ok(())
}

#[test]
fn stage7_shadow_never_persists_risk_shadow_state() -> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let mut cfg = config(3)?;
    cfg.hedged_grid
        .as_mut()
        .ok_or("missing grid config")?
        .exposure_take_profit = Some(ExposureTakeProfitConfig {
        enabled: true,
        shadow: true,
        position_equity_multiple: Decimal::new(3, 0),
        unrealized_pnl_equity_ratio: Decimal::new(5, 2),
        reduce_ratio: Decimal::new(30, 2),
        snapshot_interval_ms: 120_000,
        max_snapshot_age_ms: 3_000,
        rearm_clear_generations: 2,
    });
    let binding = gate_binding(&cfg)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };

    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: false,
            shadow_only: true,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;

    assert!(report.shadow_only);
    assert_eq!(report.private_generation, 1);
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    assert!(!temporary.path().join(CHECKPOINT_FILE).exists());
    assert!(
        !temporary
            .path()
            .join(hedged_grid::EXPOSURE_SHADOW_EVIDENCE_FILE)
            .exists()
    );
    Ok(())
}

#[test]
fn wall_clock_deadline_ends_a_canary_phase_before_any_new_turn()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: false,
            shadow_only: true,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: Some(0),
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;
    assert_eq!(report.turns, 0);
    assert_eq!(report.private_generation, 0);
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    Ok(())
}

#[test]
fn stage7_shadow_fences_a_failed_private_stream_and_recovers_by_signed_readback()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?, shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: Some(1),
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(2),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: false,
            shadow_only: true,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;
    assert_eq!(venue.stream_resets, 1);
    assert_eq!(report.private_generation, 2);
    assert!(report.shadow_only);
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    Ok(())
}

#[test]
fn interrupted_stop_waits_for_signed_empty_orders_before_a_requested_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Stopping;
    let stale = intent(1, GridPosition::Long, 1)?;
    state.owned_orders.insert(stale.key.clone(), stale);
    ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE)).save(&Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    })?;
    set_stage7_grid_control(&cfg, temporary.path(), HedgedGridControlTarget::Reset)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::from([GridPublicPayload {
            generation: 1,
            source: GridPublicPayloadSource::WebSocketBbo,
            received_at_ms: 1,
            payload: "{}".to_owned(),
        }]),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: true,
            shadow_only: false,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding.clone(),
        &mut venue,
    )?;
    assert_eq!(report.phase, GridPhase::ResettingGrid);
    let control = ProjectionStore::new(temporary.path().join(CONTROL_FILE))
        .load::<Stage7GridControl>()?
        .ok_or("missing control")?;
    assert_eq!(control.target, HedgedGridControlTarget::Running);
    let restored = ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE))
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing checkpoint")?;
    assert!(restored.state.owned_orders.is_empty());
    Ok(())
}

#[test]
fn missing_control_keeps_an_existing_stopping_checkpoint_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let params = release_params(&cfg, &binding)?;
    let mut state = HedgedGridState::new_with_params(binding.clone(), params)?;
    state.phase = GridPhase::Stopping;
    ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE)).save(&Stage7GridCheckpoint {
        schema_version: 1,
        binding: binding.clone(),
        state,
        private_generation: 1,
        exposure_guard: None,
        pending_exposure_reduction: None,
        fill_history_start_ms: 1,
        order_health_fenced: false,
        last_order_health_checked_at_ms: 0,
    })?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };

    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: true,
            shadow_only: false,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;
    assert!(report.stopped);
    let restored = ProjectionStore::new(temporary.path().join(CHECKPOINT_FILE))
        .load::<Stage7GridCheckpoint>()?
        .ok_or("missing checkpoint")?;
    assert_eq!(restored.state.phase, GridPhase::Stopping);
    assert!(!temporary.path().join(CONTROL_FILE).exists());
    Ok(())
}

#[test]
fn grid_stop_reaches_signed_private_cleanup_during_a_public_outage()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    set_stage7_grid_control(&cfg, temporary.path(), HedgedGridControlTarget::Stop)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: u8::MAX,
        exact_order_outcomes: VecDeque::new(),
    };
    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(1),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: true,
            shadow_only: false,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;
    assert!(report.stopped);
    assert_eq!(report.private_generation, 1);
    Ok(())
}

#[test]
fn instrument_rule_drift_converts_live_into_a_signed_stop_before_new_risk()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?, shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: u8::MAX - 1,
        exact_order_outcomes: VecDeque::new(),
    };
    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(2),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: true,
            shadow_only: false,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;
    assert!(report.stopped);
    let control = ProjectionStore::new(temporary.path().join(CONTROL_FILE))
        .load::<Stage7GridControl>()?
        .ok_or("missing control")?;
    assert_eq!(control.target, HedgedGridControlTarget::Stop);
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    Ok(())
}

#[test]
fn unavailable_instrument_rules_keep_live_fenced_without_requesting_stop()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cfg = config(3)?;
    let binding = gate_binding(&cfg)?;
    let mut venue = ShadowVenue {
        instrument: instrument()?,
        readbacks: VecDeque::from([shadow_readback()?, shadow_readback()?]),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: u8::MAX - 2,
        exact_order_outcomes: VecDeque::new(),
    };
    let report = run_stage7_grid(
        &cfg,
        Stage7GridRequest {
            artifacts_root: temporary.path().to_path_buf(),
            max_turns: Some(2),
            reset_on_start: false,
            skip_inventory_replenishment_until_recovered: false,
            confirm_mainnet_grid_mutations: true,
            shadow_only: false,
            stop_after_first_owned_fill: false,
            wall_clock_deadline_ms: None,
            force_order_health_check: false,
        },
        binding,
        &mut venue,
    )?;
    assert!(!report.stopped);
    assert_eq!(report.private_generation, 2);
    let control = ProjectionStore::new(temporary.path().join(CONTROL_FILE))
        .load::<Stage7GridControl>()?
        .ok_or("missing control")?;
    assert_eq!(control.target, HedgedGridControlTarget::Running);
    assert!(!temporary.path().join(COMMAND_FILE).exists());
    Ok(())
}

#[test]
fn stage7_quantity_uses_exchange_minimums_without_expanding_past_the_hard_cap()
-> Result<(), Box<dyn std::error::Error>> {
    let mut instrument = instrument()?;
    instrument.quantity_step = Decimal::new(5, 1);
    instrument.minimum_notional = Amount::new(Asset::new("USDT")?, Decimal::new(55, 1));
    let venue = ShadowVenue {
        instrument,
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::new(4, 0),
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let quantity = stage7_quantity(
        &venue,
        Decimal::new(5, 0),
        Decimal::ONE,
        Price::new(Decimal::ONE)?,
    )?;
    assert_eq!(quantity, Decimal::new(55, 1));
    assert!(assert_order_notional(quantity, Price::new(Decimal::ONE)?, venue.instrument()).is_ok());

    let cap_venue = ShadowVenue {
        instrument: venue.instrument.clone(),
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::new(70, 0),
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let rejected = stage7_quantity(
        &cap_venue,
        Decimal::new(5, 0),
        Decimal::ONE,
        Price::new(Decimal::new(1, 1))?,
    )?;
    assert!(matches!(
        assert_order_notional(
            rejected,
            Price::new(Decimal::new(1, 1))?,
            cap_venue.instrument()
        ),
        Err(Stage7GridError::OrderNotional { .. })
    ));
    Ok(())
}

#[test]
fn canary_short_quantity_uses_the_executable_bid_for_the_exchange_minimum()
-> Result<(), Box<dyn std::error::Error>> {
    let mut instrument = instrument()?;
    instrument.minimum_notional = Amount::new(Asset::new("USDT")?, Decimal::new(5, 0));
    let venue = ShadowVenue {
        instrument,
        readbacks: VecDeque::new(),
        minimum_quantity: Decimal::ONE,
        stream_polls_before_error: None,
        stream_resets: 0,
        public_payloads: VecDeque::new(),
        accepted_public_at_ms: None,
        public_resets: 0,
        exact_order_outcomes: VecDeque::new(),
    };
    let bid = Price::new(Decimal::new(9, 2))?;
    let ask = Price::new(Decimal::new(1, 1))?;
    let quantity = canary_quantity(&venue, bid, ask, bid)?;
    assert!(physical_notional(quantity, bid) >= venue.instrument.minimum_notional.value);
    assert!(physical_notional(quantity, bid) <= SINGLE_ORDER_MAX_NOTIONAL);
    Ok(())
}

#[test]
fn basic_canary_market_entry_uses_six_usdt_cap_not_replenishment_cap()
-> Result<(), Box<dyn std::error::Error>> {
    let instrument = instrument()?;
    let bid = Price::new(Decimal::new(1, 1))?;
    let ask = Price::new(Decimal::new(11, 2))?;
    let command = MarketOrderCommand {
        command_id: CommandId::new("canary-market-command")?,
        client_order_id: CommandId::new("canary-market-order")?,
        owner: OrderOwner {
            strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "gate".to_owned(),
            account: "usdt_futures".to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose: OrderPurpose::Entry,
        },
        position_side: PositionSide::Long,
        side: OrderSide::Buy,
        quantity: Decimal::new(60, 0),
        reduce_only: false,
    };
    let mutation = Stage7Mutation::Market(command);
    assert!(assert_market_notional(&mutation, bid, ask, &instrument).is_ok());
    assert!(matches!(
        assert_single_market_notional(&mutation, bid, ask, &instrument),
        Err(Stage7GridError::OrderNotional { .. })
    ));
    Ok(())
}

#[test]
fn legacy_binance_ten_level_grid_has_a_bounded_rounding_compatibility_cap()
-> Result<(), Box<dyn std::error::Error>> {
    let mut instrument = instrument()?;
    instrument.minimum_notional = Amount::new(Asset::new("USDC")?, Decimal::new(5, 0));
    let price = Price::new(Decimal::new(1002100, 4))?;
    let quantity = Decimal::new(6, 2);
    let binance = HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdc".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDC".parse()?,
        config_version: "shared-grid-v1".to_owned(),
        owner_scope: "hedged_grid_sol_usdc_primary".to_owned(),
    };
    assert!(assert_order_notional(quantity, price, &instrument).is_err());
    assert!(assert_grid_order_notional(quantity, price, &instrument, &binance, 10).is_ok());

    let mut gate = binance;
    gate.exchange = "gate".to_owned();
    gate.account = "usdt_futures".to_owned();
    gate.config_version = "stage7".to_owned();
    assert!(assert_grid_order_notional(quantity, price, &instrument, &gate, 3).is_err());
    Ok(())
}

#[test]
fn migrated_grid_uses_an_epoch_above_every_durable_client_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdc".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDC".parse()?,
        config_version: "shared-grid-v1".to_owned(),
        owner_scope: "hedged_grid_sol_usdc_primary".to_owned(),
    };
    let old_intent = intent(960, GridPosition::Long, 1)?;
    let mut binance_instrument = instrument()?;
    binance_instrument.symbol = binding.symbol.clone();
    binance_instrument.settlement_asset = Some(Asset::new("USDC")?);
    binance_instrument.minimum_notional = Amount::new(Asset::new("USDC")?, Decimal::ZERO);
    let GridMutation::Place(old_command) =
        place_command(&binding, &binance_instrument, &old_intent)?
    else {
        unreachable!();
    };
    let temporary = tempfile::tempdir()?;
    let mut commands = CommandJournal::open(temporary.path().join(COMMAND_FILE))?;
    commands.prepare_place(old_command)?;

    assert_eq!(next_unused_grid_epoch(&commands, &binding)?, 961);
    let mut state = HedgedGridState::new_with_params(
        binding.clone(),
        HedgedGridParams::fixed_release(Asset::new("USDC")?, 10)?,
    )?;
    state
        .owned_orders
        .insert(old_intent.key.clone(), old_intent);
    assert!(checkpoint_projection_is_wal_bound(
        &state,
        &commands,
        &binding,
        &binance_instrument,
    )?);
    let replacement = intent(960, GridPosition::Long, 2)?;
    state.owned_orders.clear();
    state
        .owned_orders
        .insert(replacement.key.clone(), replacement);
    assert!(!checkpoint_projection_is_wal_bound(
        &state,
        &commands,
        &binding,
        &binance_instrument,
    )?);
    let mut another_binding = binding.clone();
    another_binding.run_id = "other".to_owned();
    assert_eq!(next_unused_grid_epoch(&commands, &another_binding)?, 1);
    Ok(())
}

#[test]
fn legacy_binance_raw_ack_is_bound_to_both_signed_order_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let binding = HedgedGridBinding {
        strategy_instance_id: "hedged_grid_sol_usdc".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "binance".to_owned(),
        account: "portfolio_margin_um".to_owned(),
        symbol: "SOL/USDC".parse()?,
        config_version: "shared-grid-v1".to_owned(),
        owner_scope: "hedged_grid_sol_usdc_primary".to_owned(),
    };
    let client_id = CommandId::new("hgo_e961_long_close_l1")?;
    let raw = r#"{"orderId":13677753681,"clientOrderId":"hgo_e961_long_close_l1"}"#;
    assert!(accepted_venue_order_id_matches(
        raw,
        "13677753681",
        &client_id,
        &binding,
    ));
    assert!(!accepted_venue_order_id_matches(
        raw,
        "13677753682",
        &client_id,
        &binding,
    ));
    let other = CommandId::new("hgo_e961_long_close_l2")?;
    assert!(!accepted_venue_order_id_matches(
        raw,
        "13677753681",
        &other,
        &binding,
    ));
    Ok(())
}
