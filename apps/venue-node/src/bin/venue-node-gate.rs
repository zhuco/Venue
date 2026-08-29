use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    process::ExitCode,
};

use clap::Parser;
use sha2::{Digest, Sha256};
use venue_domain::domain::{
    CommandId, ExecutionCommand, FieldState, NativeOrderFamily, OrderOwner, OrderPurpose,
    OrderState, PositionSide,
};
use venue_gateway_api::{CapabilitySnapshot, GatewayBinding, GatewayMode, VenueId};
use venue_gateway_gate::{
    GATE_STAGE7_ORDER_PROFILE_VERSION, GateCancelIntent, GateConfig, GateContractRules,
    GateFillsCursor, GateGatewayBinding, GateMutationKind, GateMutationSettlement,
    GatePreparedMutation, GatePrivateReadbackCandidate, GateSettlementFinality, prepare_cancel,
    prepare_limit_post_only, prepare_reduce_once,
};
use venue_node::{
    AdapterIsolation, DispatchPermit, FamilyReadbackCoverage, GatewayAcknowledgement,
    GatewayDispatchResult, GatewayRecoveryPermit, NodeError, NodeLaunch, PhysicalGateway,
    SignedCommandReadback, SignedOwnedOrder, SignedReadbackReceipt, SignedReadbackRequest,
    reject_unintegrated_legacy_test_runtime, report_result,
};

const PROGRAM: &str = "venue-node-gate";

/// Synchronous candidate seam between the account-node safety host and Gate's async transport.
///
/// The production binary deliberately does not install an implementation yet: constructing a
/// network implementation requires the shared runtime/dependency work owned by another goal and
/// an executable handoff from the existing Stage 7 writer. Tests inject a deterministic backend
/// to prove the adapter mapping without granting LIVE authority.
pub trait GatePhysicalIo {
    fn connect_after_recovery(
        &mut self,
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
        permit: &GatewayRecoveryPermit,
    ) -> Result<(), GatePhysicalGatewayError>;

    fn signed_account_readback(
        &mut self,
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
        request: &SignedReadbackRequest,
        fills_cursor: &GateFillsCursor,
    ) -> Result<GatePhysicalReadback, GatePhysicalGatewayError>;

    /// Returns `Acknowledged` only after Gate's ACK has converged through the exact signed order
    /// readback. An ACK disconnect or unresolved exact lookup must return `Unknown`.
    fn dispatch_with_exact_readback(
        &mut self,
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
        mutation: GatePreparedMutation,
    ) -> Result<GateExactDispatch, GatePhysicalGatewayError>;
}

/// One already adapter-validated account attempt plus the Owner/exact-command facts that the
/// current shared `SignedReadbackRequest` cannot itself provide to Gate's transport.
pub struct GatePhysicalReadback {
    pub connection_generation: u64,
    pub private_generation: u64,
    pub candidate: GatePrivateReadbackCandidate,
    pub owned_open_client_ids: BTreeSet<String>,
    pub command_results: Vec<SignedCommandReadback>,
}

pub enum GateExactDispatch {
    /// The enclosed settlement is downstream of `settle_exact_readback`, never the bare ACK.
    Acknowledged(GateMutationSettlement),
    Rejected,
    Unknown,
}

/// Gate-local `PhysicalGateway` candidate. It is intentionally not constructed by `run()` until
/// handoff, capability, Owner/WAL recovery and an async-to-sync runtime owner are available.
pub struct GatePhysicalGateway<I> {
    binding: GateGatewayBinding,
    rules: GateContractRules,
    owner: OrderOwner,
    capability: CapabilitySnapshot,
    fills_cursor: GateFillsCursor,
    owned_open_orders: BTreeMap<String, String>,
    attempted_reduce_episodes: BTreeSet<(String, u64)>,
    last_receipt: Option<SignedReadbackReceipt>,
    connected: bool,
    io: I,
}

impl<I> GatePhysicalGateway<I> {
    pub fn new(
        binding: GateGatewayBinding,
        rules: GateContractRules,
        owner: OrderOwner,
        capability: CapabilitySnapshot,
        fills_cursor: GateFillsCursor,
        io: I,
    ) -> Result<Self, GatePhysicalGatewayError> {
        owner
            .validate()
            .map_err(|_| GatePhysicalGatewayError::Owner)?;
        let gateway_binding = binding.gateway_binding();
        if owner.exchange != VenueId::Gate.as_str()
            || owner.account != gateway_binding.trading_account_id
            || owner.symbol != gateway_binding.symbol
            || capability.binding != *gateway_binding
            || rules.instrument.symbol != gateway_binding.symbol
            || rules.instrument.generation == 0
            || rules.instrument.validate().is_err()
            || rules.native_symbol.trim().is_empty()
        {
            return Err(GatePhysicalGatewayError::Binding);
        }
        Ok(Self {
            binding,
            rules,
            owner,
            capability,
            fills_cursor,
            owned_open_orders: BTreeMap::new(),
            attempted_reduce_episodes: BTreeSet::new(),
            last_receipt: None,
            connected: false,
            io,
        })
    }

    fn validate_candidate(
        &self,
        candidate: &GatePrivateReadbackCandidate,
    ) -> Result<(), GatePhysicalGatewayError> {
        if candidate.binding != *self.binding.gateway_binding()
            || candidate.generation != self.rules.instrument.generation
            || candidate.order_families.scope().profile_version != GATE_STAGE7_ORDER_PROFILE_VERSION
            || candidate.positions[0].side != PositionSide::Long
            || candidate.positions[1].side != PositionSide::Short
            || candidate
                .positions
                .iter()
                .any(|position| position.symbol != self.binding.gateway_binding().symbol)
            || candidate.fills_cursor_before != self.fills_cursor
        {
            Err(GatePhysicalGatewayError::Readback)
        } else {
            Ok(())
        }
    }

    fn family_coverage() -> Vec<FamilyReadbackCoverage> {
        vec![
            FamilyReadbackCoverage::complete(NativeOrderFamily::UmOrder),
            FamilyReadbackCoverage::unsupported(NativeOrderFamily::UmConditional),
            FamilyReadbackCoverage::unsupported(NativeOrderFamily::UmAlgo),
        ]
    }

    fn adapt_readback(
        &mut self,
        request: &SignedReadbackRequest,
        readback: GatePhysicalReadback,
    ) -> Result<SignedReadbackReceipt, GatePhysicalGatewayError> {
        if readback.connection_generation == 0
            || readback.private_generation == 0
            || readback.connection_generation < request.after_connection_generation()
            || readback.private_generation <= request.after_private_generation()
        {
            return Err(GatePhysicalGatewayError::Readback);
        }
        self.validate_candidate(&readback.candidate)?;

        let expected_commands = request.commands().iter().collect::<BTreeSet<_>>();
        let actual_commands = readback
            .command_results
            .iter()
            .map(SignedCommandReadback::key)
            .collect::<BTreeSet<_>>();
        if expected_commands != actual_commands
            || actual_commands.len() != readback.command_results.len()
        {
            return Err(GatePhysicalGatewayError::CommandReadback);
        }

        let mut owned_open_orders = Vec::new();
        let mut native_by_client = BTreeMap::new();
        for order in &readback.candidate.order_families.regular().orders {
            let FieldState::Known(client_id) = &order.client_order_id else {
                continue;
            };
            if !readback.owned_open_client_ids.contains(client_id) {
                continue;
            }
            if native_by_client
                .insert(client_id.clone(), order.order_id.clone())
                .is_some()
            {
                return Err(GatePhysicalGatewayError::Owner);
            }
            let mut owner = self.owner.clone();
            owner.purpose = if order.reduce_only {
                OrderPurpose::TakeProfit
            } else {
                OrderPurpose::Entry
            };
            owned_open_orders.push(
                SignedOwnedOrder::new(
                    owner,
                    NativeOrderFamily::UmOrder,
                    CommandId::new(client_id.clone())
                        .map_err(|_| GatePhysicalGatewayError::Owner)?,
                    order.order_id.clone(),
                )
                .map_err(|_| GatePhysicalGatewayError::Owner)?,
            );
        }
        if native_by_client.len() != readback.owned_open_client_ids.len() {
            return Err(GatePhysicalGatewayError::Owner);
        }

        let nonzero_position_symbols = readback
            .candidate
            .positions
            .iter()
            .filter(|position| !position.quantity.is_zero())
            .map(|position| position.symbol.clone())
            .collect::<BTreeSet<_>>();
        let commitment = readback_commitment(
            &readback.candidate,
            readback.connection_generation,
            readback.private_generation,
        );
        let receipt = SignedReadbackReceipt::new(
            self.binding.gateway_binding().clone(),
            readback.connection_generation,
            readback.private_generation,
            readback.candidate.observed_at_ms,
            commitment,
            Self::family_coverage(),
            owned_open_orders,
            nonzero_position_symbols,
            readback.command_results,
        )
        .map_err(|_| GatePhysicalGatewayError::Readback)?;
        self.fills_cursor = readback.candidate.fills_cursor_after;
        self.owned_open_orders = native_by_client;
        self.last_receipt = Some(receipt.clone());
        Ok(receipt)
    }

    fn prepare_mutation(
        &mut self,
        command: &ExecutionCommand,
    ) -> Result<GatePreparedMutation, GatePhysicalGatewayError> {
        if !same_owner_scope(command.mutation_owner(), &self.owner) {
            return Err(GatePhysicalGatewayError::Owner);
        }
        match command {
            ExecutionCommand::PlaceLimit(command) => {
                prepare_limit_post_only(&self.binding, &self.rules, command)
                    .map_err(|_| GatePhysicalGatewayError::Mutation)
            }
            ExecutionCommand::MarketReduce(command) => {
                let episode = (
                    command.risk_episode_id.as_str().to_owned(),
                    command.position_generation,
                );
                if !self.attempted_reduce_episodes.insert(episode) {
                    return Err(GatePhysicalGatewayError::ReduceReplay);
                }
                prepare_reduce_once(&self.binding, &self.rules, command)
                    .map_err(|_| GatePhysicalGatewayError::Mutation)
            }
            ExecutionCommand::Cancel(command) => {
                let venue_order_id = self
                    .owned_open_orders
                    .get(command.target_client_order_id.as_str())
                    .cloned()
                    .ok_or(GatePhysicalGatewayError::CancelTarget)?;
                prepare_cancel(
                    &self.binding,
                    &self.rules,
                    &GateCancelIntent {
                        command: command.clone(),
                        venue_order_id,
                    },
                )
                .map_err(|_| GatePhysicalGatewayError::Mutation)
            }
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                Err(GatePhysicalGatewayError::UnsupportedMutation)
            }
        }
    }

    fn accept_exact_settlement(
        &mut self,
        command: &ExecutionCommand,
        expected_kind: GateMutationKind,
        settlement: GateMutationSettlement,
    ) -> Result<GatewayAcknowledgement, GatePhysicalGatewayError> {
        if settlement.kind != expected_kind || settlement.order.order_id.trim().is_empty() {
            return Err(GatePhysicalGatewayError::ExactReadback);
        }
        let expected_client_id = match command {
            ExecutionCommand::PlaceLimit(command) => command.client_order_id.as_str(),
            ExecutionCommand::MarketReduce(command) => command.client_order_id.as_str(),
            ExecutionCommand::Cancel(command) => command.target_client_order_id.as_str(),
            _ => return Err(GatePhysicalGatewayError::UnsupportedMutation),
        };
        if !matches!(
            &settlement.order.client_order_id,
            FieldState::Known(actual) if actual == expected_client_id
        ) || expected_kind == GateMutationKind::Cancel
            && settlement.finality != GateSettlementFinality::Terminal
        {
            return Err(GatePhysicalGatewayError::ExactReadback);
        }
        match command {
            ExecutionCommand::PlaceLimit(command) => {
                if matches!(
                    settlement.order.state,
                    OrderState::New | OrderState::PartiallyFilled
                ) {
                    self.owned_open_orders.insert(
                        command.client_order_id.as_str().to_owned(),
                        settlement.order.order_id.clone(),
                    );
                }
            }
            ExecutionCommand::Cancel(command) => {
                self.owned_open_orders
                    .remove(command.target_client_order_id.as_str());
            }
            ExecutionCommand::MarketReduce(_) => {}
            _ => return Err(GatePhysicalGatewayError::UnsupportedMutation),
        }
        GatewayAcknowledgement::new(settlement.order.order_id)
            .map_err(|_| GatePhysicalGatewayError::ExactReadback)
    }
}

impl<I: GatePhysicalIo> GatePhysicalGateway<I> {
    fn dispatch_authorized_command(&mut self, command: ExecutionCommand) -> GatewayDispatchResult {
        let mutation = match self.prepare_mutation(&command) {
            Ok(mutation) => mutation,
            Err(error) => {
                return GatewayDispatchResult::Rejected {
                    reason_code: error.reason_code().to_owned(),
                };
            }
        };
        let kind = mutation.kind();
        match self
            .io
            .dispatch_with_exact_readback(&self.binding, &self.rules, mutation)
        {
            Ok(GateExactDispatch::Acknowledged(settlement)) => self
                .accept_exact_settlement(&command, kind, settlement)
                .map_or(
                    GatewayDispatchResult::Unknown,
                    GatewayDispatchResult::Acknowledged,
                ),
            Ok(GateExactDispatch::Rejected) => GatewayDispatchResult::Rejected {
                reason_code: "gate_venue_rejected".to_owned(),
            },
            Ok(GateExactDispatch::Unknown) | Err(_) => GatewayDispatchResult::Unknown,
        }
    }
}

impl<I: GatePhysicalIo> PhysicalGateway for GatePhysicalGateway<I> {
    type Error = GatePhysicalGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.binding.gateway_binding()
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.capability.clone()
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        if self.connected
            || permit.binding() != self.binding.gateway_binding()
            || permit.config_epoch() == 0
        {
            return Err(GatePhysicalGatewayError::Recovery);
        }
        self.io
            .connect_after_recovery(&self.binding, &self.rules, &permit)?;
        self.connected = true;
        Ok(())
    }

    fn signed_readback(
        &mut self,
        request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        if !self.connected || request.binding() != self.binding.gateway_binding() {
            return Err(GatePhysicalGatewayError::Recovery);
        }
        let readback = self.io.signed_account_readback(
            &self.binding,
            &self.rules,
            request,
            &self.fills_cursor,
        )?;
        self.adapt_readback(request, readback)
    }

    fn verify_signed_readback(&self, receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        if self.last_receipt.as_ref() == Some(receipt) {
            Ok(())
        } else {
            Err(GatePhysicalGatewayError::Readback)
        }
    }

    fn dispatch(&mut self, permit: DispatchPermit) -> GatewayDispatchResult {
        if !self.connected
            || permit.binding() != self.binding.gateway_binding()
            || permit.writer_generation() == 0
            || permit.writer_revision() == 0
            || self
                .last_receipt
                .as_ref()
                .is_none_or(|receipt| receipt.private_generation() != permit.readback_generation())
            || matches!(
                permit.command(),
                ExecutionCommand::PlaceLimit(command)
                    if command.owner.purpose == OrderPurpose::Entry
                        && permit.canary_sha256().is_none()
            )
        {
            return GatewayDispatchResult::Rejected {
                reason_code: "gate_physical_authority_rejected".to_owned(),
            };
        }
        self.dispatch_authorized_command(permit.command().clone())
    }
}

fn same_owner_scope(actual: &OrderOwner, expected: &OrderOwner) -> bool {
    actual.strategy_instance_id == expected.strategy_instance_id
        && actual.run_id == expected.run_id
        && actual.exchange == expected.exchange
        && actual.account == expected.account
        && actual.symbol == expected.symbol
}

fn readback_commitment(
    candidate: &GatePrivateReadbackCandidate,
    connection_generation: u64,
    private_generation: u64,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"venue-node-gate-readback-v1\0");
    digest.update(candidate.binding.mode.as_str().as_bytes());
    digest.update(candidate.binding.trading_account_id.as_bytes());
    digest.update(candidate.binding.symbol.to_string().as_bytes());
    digest.update(connection_generation.to_be_bytes());
    digest.update(private_generation.to_be_bytes());
    digest.update(candidate.generation.to_be_bytes());
    digest.update(candidate.attempt.to_be_bytes());
    digest.update(candidate.observed_at_ms.to_be_bytes());
    for raw_digest in &candidate.raw_payload_digests {
        digest.update(raw_digest);
    }
    digest.update(candidate.order_families.regular_payload_digest());
    if let Some(cursor) = candidate.fills_cursor_before.last_native_id() {
        digest.update(cursor.as_bytes());
    }
    digest.update([0]);
    if let Some(cursor) = candidate.fills_cursor_after.last_native_id() {
        digest.update(cursor.as_bytes());
    }
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GatePhysicalGatewayError {
    #[error("Gate physical gateway binding or contract rules do not match")]
    Binding,
    #[error("Gate physical gateway Owner evidence is missing or inconsistent")]
    Owner,
    #[error("Gate physical gateway recovery permit is missing or stale")]
    Recovery,
    #[error("Gate signed account readback is incomplete or stale")]
    Readback,
    #[error("Gate UNKNOWN command readback is incomplete or ambiguous")]
    CommandReadback,
    #[error("Gate mutation cannot be prepared from the durable command")]
    Mutation,
    #[error("Gate cancel target lacks an exact signed native order identity")]
    CancelTarget,
    #[error("Gate regular-only profile does not support this mutation family")]
    UnsupportedMutation,
    #[error("Gate reduce episode was already physically attempted")]
    ReduceReplay,
    #[error("Gate ACK did not converge through exact signed order readback")]
    ExactReadback,
}

impl GatePhysicalGatewayError {
    const fn reason_code(self) -> &'static str {
        match self {
            Self::Binding => "gate_binding_rejected",
            Self::Owner => "gate_owner_rejected",
            Self::Recovery => "gate_recovery_rejected",
            Self::Readback => "gate_readback_rejected",
            Self::CommandReadback => "gate_command_readback_rejected",
            Self::Mutation => "gate_mutation_rejected",
            Self::CancelTarget => "gate_cancel_target_unproven",
            Self::UnsupportedMutation => "gate_order_family_unsupported",
            Self::ReduceReplay => "gate_reduce_episode_already_attempted",
            Self::ExactReadback => "gate_exact_readback_rejected",
        }
    }
}

fn main() -> ExitCode {
    report_result(PROGRAM, run())
}

fn run() -> Result<(), NodeError> {
    let launch = NodeLaunch::from_environment(VenueId::Gate)?;
    let arguments = launch.legacy_runtime_arguments(PROGRAM)?;
    let cli = venue::Cli::try_parse_from(arguments)?;
    let config =
        venue::config::Config::load(&cli.config).map_err(|error| NodeError::ExistingRuntime {
            venue: VenueId::Gate,
            message: error.to_string(),
        })?;
    launch.validate_runtime_scope(&config.trading_account_id, &config.symbol)?;
    let account_binding = config.gate.ok_or(NodeError::RuntimeScope)?.account_binding;
    let _binding = GateGatewayBinding::new(launch.binding().clone())
        .map_err(|_| NodeError::AdapterIsolation(VenueId::Gate))?;
    let adapter = GateConfig::for_mode(launch.binding().mode);
    let account_binding = match account_binding {
        venue::config::GateAccountBinding::UsdtFuturesDual => "usdt_futures_dual",
    };
    AdapterIsolation {
        venue: VenueId::Gate,
        mode: adapter.mode(),
        endpoints: &[adapter.rest_origin(), adapter.usdt_futures_ws()],
        credential_environment: &["GATEIO_API_KEY", "GATEIO_API_SECRET"],
        credential_prefix: "GATEIO_",
        account_binding,
    }
    .validate(launch.binding())?;
    if launch.binding().mode == GatewayMode::Test {
        return reject_unintegrated_legacy_test_runtime(VenueId::Gate);
    }
    venue::start_hedged_grid_gate_deployment(cli).map_err(|error| NodeError::ExistingRuntime {
        venue: VenueId::Gate,
        message: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::VecDeque, rc::Rc};

    use rust_decimal::Decimal;
    use venue_domain::domain::{
        Amount, CancelCommand, Instrument, MarketKind, MarketReduceCommand, Order, OrderCommand,
        OrderSide, Price, Symbol,
    };
    use venue_gateway_api::{CapabilityFlags, GatewayMode};
    use venue_gateway_gate::{
        GatePrivateReadSource, GateRawPrivateResponse, GateStage7UnsupportedOrderFamily,
        prepare_private_read, validate_private_readback,
    };

    use super::*;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000028";

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct CapturedMutation {
        kind: GateMutationKind,
        endpoint: String,
        body: String,
        reduce_episode_id: Option<String>,
        position_generation: Option<u64>,
    }

    struct ScriptedIo {
        dispatches: VecDeque<GateExactDispatch>,
        captured: Rc<RefCell<Vec<CapturedMutation>>>,
    }

    impl ScriptedIo {
        fn new(
            dispatches: Vec<GateExactDispatch>,
            captured: Rc<RefCell<Vec<CapturedMutation>>>,
        ) -> Self {
            Self {
                dispatches: dispatches.into(),
                captured,
            }
        }
    }

    impl GatePhysicalIo for ScriptedIo {
        fn connect_after_recovery(
            &mut self,
            _binding: &GateGatewayBinding,
            _rules: &GateContractRules,
            _permit: &GatewayRecoveryPermit,
        ) -> Result<(), GatePhysicalGatewayError> {
            Ok(())
        }

        fn signed_account_readback(
            &mut self,
            _binding: &GateGatewayBinding,
            _rules: &GateContractRules,
            _request: &SignedReadbackRequest,
            _fills_cursor: &GateFillsCursor,
        ) -> Result<GatePhysicalReadback, GatePhysicalGatewayError> {
            Err(GatePhysicalGatewayError::Recovery)
        }

        fn dispatch_with_exact_readback(
            &mut self,
            _binding: &GateGatewayBinding,
            _rules: &GateContractRules,
            mutation: GatePreparedMutation,
        ) -> Result<GateExactDispatch, GatePhysicalGatewayError> {
            let body = std::str::from_utf8(mutation.body())
                .map_err(|_| GatePhysicalGatewayError::Mutation)?
                .to_owned();
            self.captured.borrow_mut().push(CapturedMutation {
                kind: mutation.kind(),
                endpoint: mutation.endpoint().to_owned(),
                body,
                reduce_episode_id: mutation.reduce_episode_id().map(str::to_owned),
                position_generation: mutation.position_generation(),
            });
            self.dispatches
                .pop_front()
                .ok_or(GatePhysicalGatewayError::Mutation)
        }
    }

    fn binding(mode: GatewayMode) -> Result<GateGatewayBinding, Box<dyn std::error::Error>> {
        Ok(GateGatewayBinding::new(GatewayBinding::new(
            VenueId::Gate,
            mode,
            ACCOUNT,
            "DOGE/USDT".parse()?,
        )?)?)
    }

    fn rules() -> Result<GateContractRules, Box<dyn std::error::Error>> {
        let symbol: Symbol = "DOGE/USDT".parse()?;
        Ok(GateContractRules {
            native_symbol: "DOGE_USDT".to_owned(),
            instrument: Instrument {
                settlement_asset: Some("USDT".parse()?),
                minimum_notional: Amount::new("USDT".parse()?, Decimal::ZERO),
                symbol,
                market: MarketKind::LinearPerpetual,
                generation: 7,
                price_tick: Price::new(Decimal::new(1, 5))?,
                quantity_step: Decimal::new(1, 1),
            },
            quanto_multiplier: Decimal::new(1, 1),
            minimum_contracts: Decimal::ONE,
            decimal_contracts: false,
        })
    }

    fn owner(purpose: OrderPurpose) -> Result<OrderOwner, Box<dyn std::error::Error>> {
        Ok(OrderOwner {
            strategy_instance_id: "grid_gate_primary".to_owned(),
            run_id: "run_28".to_owned(),
            exchange: "gate".to_owned(),
            account: ACCOUNT.to_owned(),
            symbol: "DOGE/USDT".parse()?,
            purpose,
        })
    }

    fn capability(binding: &GateGatewayBinding) -> CapabilitySnapshot {
        CapabilitySnapshot {
            binding: binding.gateway_binding().clone(),
            version: 28,
            observed_ms: 1_000,
            expires_ms: 10_000,
            flags: CapabilityFlags::READ_ACCOUNT
                | CapabilityFlags::READ_ORDERS
                | CapabilityFlags::READ_FILLS
                | CapabilityFlags::PRIVATE_STREAM
                | CapabilityFlags::TRADE
                | CapabilityFlags::PLACE_LIMIT
                | CapabilityFlags::PLACE_MARKET
                | CapabilityFlags::CANCEL
                | CapabilityFlags::HEDGE_POSITION,
        }
    }

    fn gateway(
        mode: GatewayMode,
        cursor: Option<&str>,
        dispatches: Vec<GateExactDispatch>,
        captured: Rc<RefCell<Vec<CapturedMutation>>>,
    ) -> Result<GatePhysicalGateway<ScriptedIo>, Box<dyn std::error::Error>> {
        let binding = binding(mode)?;
        Ok(GatePhysicalGateway::new(
            binding.clone(),
            rules()?,
            owner(OrderPurpose::Entry)?,
            capability(&binding),
            GateFillsCursor::new(cursor.map(str::to_owned))?,
            ScriptedIo::new(dispatches, captured),
        )?)
    }

    fn raw(
        binding: &GateGatewayBinding,
        rules: &GateContractRules,
        source: GatePrivateReadSource,
        cursor: Option<&str>,
        payload: &str,
    ) -> Result<GateRawPrivateResponse, Box<dyn std::error::Error>> {
        let request = prepare_private_read(
            binding,
            rules,
            7,
            28,
            source,
            GateFillsCursor::new(cursor.map(str::to_owned))?,
        )?;
        Ok(GateRawPrivateResponse::from_response(
            binding,
            rules,
            &request,
            1_000,
            1_100,
            payload.to_owned(),
        )?)
    }

    fn private_candidate() -> Result<GatePrivateReadbackCandidate, Box<dyn std::error::Error>> {
        let binding = binding(GatewayMode::Test)?;
        let rules = rules()?;
        Ok(validate_private_readback(
            &binding,
            &rules,
            GATE_STAGE7_ORDER_PROFILE_VERSION,
            2_000,
            1_200,
            [
                raw(
                    &binding,
                    &rules,
                    GatePrivateReadSource::Account,
                    None,
                    r#"{"position_mode":"dual","total":"10","available":"9"}"#,
                )?,
                raw(
                    &binding,
                    &rules,
                    GatePrivateReadSource::DualPositions,
                    None,
                    r#"[{"user":42,"contract":"DOGE_USDT","mode":"dual_long","size":"0","entry_price":"0","mark_price":"0"},{"user":42,"contract":"DOGE_USDT","mode":"dual_short","size":"2","entry_price":"0.1","mark_price":"0.11"}]"#,
                )?,
                raw(
                    &binding,
                    &rules,
                    GatePrivateReadSource::RegularOrders,
                    None,
                    include_str!(
                        "../../../../crates/venue-gateway-gate/tests/fixtures/regular_orders.json"
                    ),
                )?,
                raw(
                    &binding,
                    &rules,
                    GatePrivateReadSource::Fills,
                    Some("227262265"),
                    include_str!("../../../../crates/venue-gateway-gate/tests/fixtures/fills.json"),
                )?,
            ],
        )?)
    }

    struct OrderFixture<'a> {
        order_id: &'a str,
        client_id: &'a str,
        side: OrderSide,
        position_side: PositionSide,
        quantity: Decimal,
        price: Option<Price>,
        reduce_only: bool,
        state: OrderState,
    }

    fn order(fixture: OrderFixture<'_>) -> Result<Order, Box<dyn std::error::Error>> {
        Ok(Order {
            order_id: fixture.order_id.to_owned(),
            client_order_id: FieldState::Known(fixture.client_id.to_owned()),
            symbol: "DOGE/USDT".parse()?,
            side: fixture.side,
            position_side: FieldState::Known(fixture.position_side),
            purpose: FieldState::Missing,
            state: fixture.state,
            quantity: fixture.quantity,
            filled_quantity: Decimal::ZERO,
            limit_price: fixture.price,
            average_price: FieldState::Missing,
            reduce_only: fixture.reduce_only,
        })
    }

    fn settlement(
        kind: GateMutationKind,
        order: Order,
        finality: GateSettlementFinality,
    ) -> GateExactDispatch {
        GateExactDispatch::Acknowledged(GateMutationSettlement {
            kind,
            order,
            finality,
            settled_at_ms: 1_500,
        })
    }

    fn limit_command() -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::PlaceLimit(OrderCommand {
            command_id: CommandId::new("place_tp_1")?,
            client_order_id: CommandId::new("tp_limit_1")?,
            owner: owner(OrderPurpose::TakeProfit)?,
            side: OrderSide::Sell,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            limit_price: Price::new(Decimal::new(1, 1))?,
            reduce_only: true,
        }))
    }

    fn cancel_command() -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::Cancel(CancelCommand {
            command_id: CommandId::new("cancel_tp_1")?,
            owner: owner(OrderPurpose::TakeProfit)?,
            target_client_order_id: CommandId::new("tp_limit_1")?,
        }))
    }

    fn reduce_command(
        command_id: &str,
        client_id: &str,
    ) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
        Ok(ExecutionCommand::MarketReduce(MarketReduceCommand {
            command_id: CommandId::new(command_id)?,
            client_order_id: CommandId::new(client_id)?,
            owner: owner(OrderPurpose::ExposureTakeProfit)?,
            position_side: PositionSide::Long,
            side: OrderSide::Sell,
            quantity: Decimal::ONE,
            risk_episode_id: CommandId::new("risk_episode_28")?,
            position_generation: 9,
        }))
    }

    #[test]
    fn candidate_binds_exact_test_live_origins_without_connecting()
    -> Result<(), Box<dyn std::error::Error>> {
        let captured = Rc::new(RefCell::new(Vec::new()));
        for (mode, rest, websocket) in [
            (
                GatewayMode::Test,
                "https://api-testnet.gateapi.io/api/v4",
                "wss://ws-testnet.gate.com/v4/ws/futures/usdt",
            ),
            (
                GatewayMode::Live,
                "https://api.gateio.ws/api/v4",
                "wss://fx-ws.gateio.ws/v4/ws/usdt",
            ),
        ] {
            let gateway = gateway(mode, None, Vec::new(), Rc::clone(&captured))?;
            assert_eq!(gateway.binding.config().mode(), mode);
            assert_eq!(gateway.binding.config().rest_origin(), rest);
            assert_eq!(gateway.binding.config().usdt_futures_ws(), websocket);
            assert!(!gateway.connected);
        }
        Ok(())
    }

    #[test]
    fn candidate_requires_hedge_legs_regular_profile_and_exact_fill_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let captured = Rc::new(RefCell::new(Vec::new()));
        let gateway = gateway(GatewayMode::Test, Some("227262265"), Vec::new(), captured)?;
        let candidate = private_candidate()?;
        gateway.validate_candidate(&candidate)?;
        assert_eq!(candidate.positions[0].side, PositionSide::Long);
        assert_eq!(candidate.positions[1].side, PositionSide::Short);
        assert_eq!(
            candidate.fills_cursor_after.last_native_id(),
            Some("227262267")
        );
        assert_eq!(
            candidate.order_families.conditional().family,
            GateStage7UnsupportedOrderFamily::Conditional
        );
        assert_eq!(
            candidate.order_families.algo().family,
            GateStage7UnsupportedOrderFamily::Algo
        );
        assert_eq!(
            GatePhysicalGateway::<ScriptedIo>::family_coverage(),
            vec![
                FamilyReadbackCoverage::complete(NativeOrderFamily::UmOrder),
                FamilyReadbackCoverage::unsupported(NativeOrderFamily::UmConditional),
                FamilyReadbackCoverage::unsupported(NativeOrderFamily::UmAlgo),
            ]
        );
        assert_eq!(readback_commitment(&candidate, 1, 1).len(), 64);
        Ok(())
    }

    #[test]
    fn post_only_exact_cancel_and_reduce_once_cross_the_candidate_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let captured = Rc::new(RefCell::new(Vec::new()));
        let price = Price::new(Decimal::new(1, 1))?;
        let dispatches = vec![
            settlement(
                GateMutationKind::PlacePostOnly,
                order(OrderFixture {
                    order_id: "9100",
                    client_id: "tp_limit_1",
                    side: OrderSide::Sell,
                    position_side: PositionSide::Long,
                    quantity: Decimal::ONE,
                    price: Some(price),
                    reduce_only: true,
                    state: OrderState::New,
                })?,
                GateSettlementFinality::Working,
            ),
            settlement(
                GateMutationKind::Cancel,
                order(OrderFixture {
                    order_id: "9100",
                    client_id: "tp_limit_1",
                    side: OrderSide::Sell,
                    position_side: PositionSide::Long,
                    quantity: Decimal::ONE,
                    price: Some(price),
                    reduce_only: true,
                    state: OrderState::Cancelled,
                })?,
                GateSettlementFinality::Terminal,
            ),
            settlement(
                GateMutationKind::ReduceOnce,
                order(OrderFixture {
                    order_id: "9200",
                    client_id: "risk_reduce_1",
                    side: OrderSide::Sell,
                    position_side: PositionSide::Long,
                    quantity: Decimal::ONE,
                    price: None,
                    reduce_only: true,
                    state: OrderState::Filled,
                })?,
                GateSettlementFinality::Terminal,
            ),
        ];
        let mut gateway = gateway(GatewayMode::Test, None, dispatches, Rc::clone(&captured))?;

        assert!(matches!(
            gateway.dispatch_authorized_command(limit_command()?),
            GatewayDispatchResult::Acknowledged(ref ack) if ack.venue_order_id() == "9100"
        ));
        assert!(matches!(
            gateway.dispatch_authorized_command(cancel_command()?),
            GatewayDispatchResult::Acknowledged(ref ack) if ack.venue_order_id() == "9100"
        ));
        assert!(matches!(
            gateway.dispatch_authorized_command(reduce_command("reduce_1", "risk_reduce_1")?),
            GatewayDispatchResult::Acknowledged(ref ack) if ack.venue_order_id() == "9200"
        ));
        assert!(matches!(
            gateway.dispatch_authorized_command(reduce_command("reduce_2", "risk_reduce_2")?),
            GatewayDispatchResult::Rejected { ref reason_code }
                if reason_code == "gate_reduce_episode_already_attempted"
        ));

        let captured = captured.borrow();
        assert_eq!(captured.len(), 3);
        assert!(captured[0].body.contains(r#""tif":"poc""#));
        assert!(captured[0].body.contains(r#""reduce_only":true"#));
        assert_eq!(captured[1].kind, GateMutationKind::Cancel);
        assert!(captured[1].endpoint.ends_with("/9100"));
        assert!(captured[1].body.is_empty());
        assert_eq!(captured[2].kind, GateMutationKind::ReduceOnce);
        assert!(captured[2].body.contains(r#""tif":"ioc""#));
        assert!(captured[2].body.contains(r#""reduce_only":true"#));
        assert_eq!(
            captured[2].reduce_episode_id.as_deref(),
            Some("risk_episode_28")
        );
        assert_eq!(captured[2].position_generation, Some(9));
        Ok(())
    }

    #[test]
    fn ack_unknown_never_becomes_ack_or_retries_inside_the_wrapper()
    -> Result<(), Box<dyn std::error::Error>> {
        let captured = Rc::new(RefCell::new(Vec::new()));
        let mut gateway = gateway(
            GatewayMode::Live,
            None,
            vec![GateExactDispatch::Unknown],
            Rc::clone(&captured),
        )?;
        assert_eq!(
            gateway.dispatch_authorized_command(reduce_command("reduce_1", "risk_reduce_1")?),
            GatewayDispatchResult::Unknown
        );
        assert!(matches!(
            gateway.dispatch_authorized_command(reduce_command("reduce_2", "risk_reduce_2")?),
            GatewayDispatchResult::Rejected { ref reason_code }
                if reason_code == "gate_reduce_episode_already_attempted"
        ));
        assert_eq!(captured.borrow().len(), 1);
        Ok(())
    }

    #[test]
    fn wrapper_type_conforms_to_the_shared_physical_gateway_trait() {
        fn assert_gateway<G: PhysicalGateway<Error = GatePhysicalGatewayError>>() {}
        assert_gateway::<GatePhysicalGateway<ScriptedIo>>();
    }
}
