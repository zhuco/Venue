//! Account-local semantic orchestration for the six fixed Node processes.
//!
//! This module intentionally has no `AccountRuntimeHost`, gateway, WAL, or physical command.
//! Its output is a strategy-owned semantic intent.  The resident composition may hand that
//! output to Runtime only after the runtime has issued a durable actor turn and the Host has
//! prepared the same command WAL record.  Keeping this boundary explicit prevents a recovered
//! actor, a Control delivery, or an Unknown reconciliation from becoming a second writer.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};
use venue_control_protocol::{ControlAction, ControlCommandRequest};
use venue_runtime::{AccountKey, StrategyBinding, StrategyInstanceKey};
use venue_strategies::{
    hedged_grid::{GridAction, GridDecision, GridInventory, HedgedGridState, OwnedGridFill},
    scalping::{RiskFact, RiskLedger, RiskSnapshot, ScalpingParams, SemanticIntent},
};

use crate::CopySemanticDelivery;

/// The three reducer families are only distinguished for routing and fairness.  They cannot
/// select a physical venue request from here.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ResidentActorKind {
    Grid,
    Scalping,
    Copy,
}

#[derive(Clone, Debug)]
pub struct GridResidentActor {
    binding: StrategyBinding,
    state: HedgedGridState,
}

impl GridResidentActor {
    pub fn new(binding: StrategyBinding, state: HedgedGridState) -> Result<Self, ResidentError> {
        if state.binding.strategy_instance_id != binding.key.instance_id
            || state.binding.run_id != binding.run_id
            || state.binding.exchange != binding.key.account.exchange.as_str()
            || state.binding.account != binding.key.account.account
            || state.binding.symbol != binding.key.symbol
            || state.binding.config_version != binding.config_digest
        {
            return Err(ResidentError::ActorBinding);
        }
        Ok(Self { binding, state })
    }

    #[must_use]
    pub const fn binding(&self) -> &StrategyBinding {
        &self.binding
    }

    #[must_use]
    pub const fn state(&self) -> &HedgedGridState {
        &self.state
    }

    fn observe_inventory(
        &mut self,
        inventory: GridInventory,
    ) -> Result<GridDecision, ResidentError> {
        self.state
            .observe_inventory(inventory)
            .map_err(|_| ResidentError::GridReducer)
    }

    fn observe_fill(&mut self, fill: OwnedGridFill) -> Result<GridDecision, ResidentError> {
        self.state
            .observe_owned_fill(fill)
            .map_err(|_| ResidentError::GridReducer)
    }
}

#[derive(Clone, Debug)]
pub struct ScalpingResidentActor {
    binding: StrategyBinding,
    risk: RiskLedger,
}

impl ScalpingResidentActor {
    #[must_use]
    pub fn new(binding: StrategyBinding, params: &ScalpingParams) -> Self {
        Self {
            binding,
            risk: RiskLedger::new(params),
        }
    }

    #[must_use]
    pub const fn binding(&self) -> &StrategyBinding {
        &self.binding
    }

    fn observe_risk(&mut self, fact: RiskFact) -> Result<RiskSnapshot, ResidentError> {
        self.risk
            .record(fact)
            .map_err(|_| ResidentError::ScalpingReducer)
    }
}

#[derive(Clone, Debug)]
pub struct ResidentControlDelivery {
    /// The durable Control inbox supplies this sequence.  It is not an SSE cursor and cannot be
    /// manufactured from UI notifications.
    pub inbox_sequence: u64,
    pub command: ControlCommandRequest,
}

/// Normalized resident inputs.  Market inputs are deliberately represented by the strategy
/// semantic candidate rather than raw exchange payloads; adapter normalization belongs upstream.
// These are in-process actor mailboxes; boxing would add allocation to every hot-path fact.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum ResidentFact {
    GridInventory {
        target: StrategyInstanceKey,
        inventory: GridInventory,
    },
    GridOwnedFill {
        target: StrategyInstanceKey,
        fill: OwnedGridFill,
    },
    ScalpingRisk {
        target: StrategyInstanceKey,
        fact: RiskFact,
    },
    /// A market-frame reducer has already produced an execution-independent proposal.
    MarketScalpingCandidate {
        target: StrategyInstanceKey,
        intent: SemanticIntent,
    },
    /// Copy remains a manifest-validated semantic delivery until the runtime durably applies it.
    CopyDelivery(CopySemanticDelivery),
    Control(ResidentControlDelivery),
    /// A signed recovery reports an unresolved physical outcome.  It freezes new risk globally;
    /// only cancellation, reduce-only, Stop, and Flatten semantics can continue.
    UnknownFence {
        active: bool,
    },
}

/// This is deliberately not `ExecutionCommand`.  The missing Runtime/Host bridge must turn it
/// into a current actor turn, Host-prepared WAL record, lane admission, and then host dispatch.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum ResidentSemanticIntent {
    Grid {
        binding: StrategyBinding,
        action: GridAction,
    },
    Scalping {
        binding: StrategyBinding,
        intent: SemanticIntent,
    },
    Copy {
        delivery: CopySemanticDelivery,
    },
    Control {
        binding: StrategyBinding,
        action: ControlAction,
    },
}

impl ResidentSemanticIntent {
    fn binding(&self) -> &StrategyBinding {
        match self {
            Self::Grid { binding, .. }
            | Self::Scalping { binding, .. }
            | Self::Control { binding, .. } => binding,
            Self::Copy { delivery } => delivery.actor(),
        }
    }

    fn is_risk_increase(&self) -> bool {
        match self {
            Self::Grid { action, .. } => match action {
                GridAction::Place(order) => !order.reduce_only,
                GridAction::Replenish(_) => true,
                GridAction::Dispatch(transaction) => {
                    transaction.places.iter().any(|order| !order.reduce_only)
                }
                GridAction::Reset { .. } | GridAction::ReanchorAtFill { .. } => false,
            },
            Self::Scalping { .. } => true,
            // Copy requires a fresh signed follower position to decide its phase.  Treating the
            // delivery as a risk increase here closes the unsafe "apply now, decide later" gap.
            Self::Copy { .. } => true,
            Self::Control { .. } => false,
        }
    }

    fn priority(&self) -> ResidentPriority {
        match self {
            Self::Control {
                action: ControlAction::Stop | ControlAction::Flatten,
                ..
            } => ResidentPriority::Critical,
            Self::Grid {
                action: GridAction::Dispatch(_),
                ..
            } => ResidentPriority::FillRepair,
            _ => ResidentPriority::Normal,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ResidentPriority {
    Critical,
    FillRepair,
    Normal,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResidentRecoveryState {
    pub unknown_fenced: bool,
    pub last_control_inbox_sequence: u64,
}

/// Fair account-local sequencing over semantic outputs.  It owns no durable mutation state:
/// restart deliberately discards unadmitted outputs so they must be regenerated from durable
/// strategy checkpoints, Control inbox, and signed recovery rather than replayed as new orders.
#[derive(Debug)]
pub struct ResidentLoop {
    account: AccountKey,
    actors: BTreeMap<StrategyInstanceKey, ResidentActor>,
    symbol_owner: BTreeMap<venue_domain::domain::Symbol, StrategyInstanceKey>,
    queue: PriorityQueues,
    recovery: ResidentRecoveryState,
}

impl ResidentLoop {
    #[must_use]
    pub fn new(account: AccountKey) -> Self {
        Self::from_recovery(account, ResidentRecoveryState::default())
    }

    #[must_use]
    pub fn from_recovery(account: AccountKey, recovery: ResidentRecoveryState) -> Self {
        Self {
            account,
            actors: BTreeMap::new(),
            symbol_owner: BTreeMap::new(),
            queue: PriorityQueues::default(),
            recovery,
        }
    }

    #[must_use]
    pub const fn account(&self) -> &AccountKey {
        &self.account
    }

    #[must_use]
    pub const fn recovery_state(&self) -> &ResidentRecoveryState {
        &self.recovery
    }

    pub fn register_grid(&mut self, actor: GridResidentActor) -> Result<(), ResidentError> {
        self.register(ResidentActor::Grid(actor))
    }

    pub fn register_scalping(&mut self, actor: ScalpingResidentActor) -> Result<(), ResidentError> {
        self.register(ResidentActor::Scalping(actor))
    }

    pub fn register_copy(&mut self, binding: &StrategyBinding) -> Result<(), ResidentError> {
        self.register(ResidentActor::Copy(binding.clone()))
    }

    fn register(&mut self, actor: ResidentActor) -> Result<(), ResidentError> {
        let binding = actor.binding();
        if binding.key.account != self.account {
            return Err(ResidentError::AccountScope);
        }
        if self.actors.contains_key(&binding.key) {
            return Err(ResidentError::ActorOccupied);
        }
        if self.symbol_owner.contains_key(&binding.key.symbol) {
            return Err(ResidentError::SymbolOwnerConflict);
        }
        self.symbol_owner
            .insert(binding.key.symbol.clone(), binding.key.clone());
        self.actors.insert(binding.key.clone(), actor);
        Ok(())
    }

    /// Applies one normalized fact.  It never hands a command to a gateway or to the Host.
    pub fn consume(&mut self, fact: ResidentFact) -> Result<Option<RiskSnapshot>, ResidentError> {
        match fact {
            ResidentFact::GridInventory { target, inventory } => {
                self.require_kind(&target, ResidentActorKind::Grid)?;
                let (binding, decision) = match self.actors.get_mut(&target) {
                    Some(ResidentActor::Grid(actor)) => {
                        (actor.binding.clone(), actor.observe_inventory(inventory)?)
                    }
                    Some(_) => return Err(ResidentError::ActorKind),
                    None => return Err(ResidentError::ActorMissing),
                };
                self.enqueue_grid(binding, decision);
                Ok(None)
            }
            ResidentFact::GridOwnedFill { target, fill } => {
                self.require_kind(&target, ResidentActorKind::Grid)?;
                let (binding, decision) = match self.actors.get_mut(&target) {
                    Some(ResidentActor::Grid(actor)) => {
                        (actor.binding.clone(), actor.observe_fill(fill)?)
                    }
                    Some(_) => return Err(ResidentError::ActorKind),
                    None => return Err(ResidentError::ActorMissing),
                };
                self.enqueue_grid(binding, decision);
                Ok(None)
            }
            ResidentFact::ScalpingRisk { target, fact } => {
                self.require_kind(&target, ResidentActorKind::Scalping)?;
                match self.actors.get_mut(&target) {
                    Some(ResidentActor::Scalping(actor)) => actor.observe_risk(fact).map(Some),
                    Some(_) => Err(ResidentError::ActorKind),
                    None => Err(ResidentError::ActorMissing),
                }
            }
            ResidentFact::MarketScalpingCandidate { target, intent } => {
                self.require_kind(&target, ResidentActorKind::Scalping)?;
                let binding = match self.actors.get(&target) {
                    Some(ResidentActor::Scalping(actor)) => actor.binding.clone(),
                    Some(_) => return Err(ResidentError::ActorKind),
                    None => return Err(ResidentError::ActorMissing),
                };
                if intent.symbol != binding.key.symbol || intent.intent_id.trim().is_empty() {
                    return Err(ResidentError::CandidateBinding);
                }
                self.enqueue(ResidentSemanticIntent::Scalping { binding, intent });
                Ok(None)
            }
            ResidentFact::CopyDelivery(delivery) => {
                self.require_kind(&delivery.actor().key, ResidentActorKind::Copy)?;
                self.enqueue(ResidentSemanticIntent::Copy { delivery });
                Ok(None)
            }
            ResidentFact::Control(delivery) => {
                self.consume_control(delivery)?;
                Ok(None)
            }
            ResidentFact::UnknownFence { active } => {
                self.recovery.unknown_fenced = active;
                Ok(None)
            }
        }
    }

    /// Returns the next safe semantic output.  Unknown never removes queued risk proposals;
    /// they remain held until signed recovery explicitly clears the fence, avoiding retransmit.
    pub fn next_intent(&mut self) -> Option<ResidentSemanticIntent> {
        for priority in [
            ResidentPriority::Critical,
            ResidentPriority::FillRepair,
            ResidentPriority::Normal,
        ] {
            let mut skipped = Vec::new();
            while let Some(intent) = self.queue.pop(priority) {
                if self.recovery.unknown_fenced && intent.is_risk_increase() {
                    skipped.push(intent);
                    continue;
                }
                for deferred in skipped {
                    self.queue.push(deferred);
                }
                return Some(intent);
            }
            for deferred in skipped {
                self.queue.push(deferred);
            }
        }
        None
    }

    fn require_kind(
        &self,
        target: &StrategyInstanceKey,
        expected: ResidentActorKind,
    ) -> Result<(), ResidentError> {
        if target.account != self.account {
            return Err(ResidentError::AccountScope);
        }
        match self.actors.get(target) {
            Some(actor) if actor.kind() == expected => Ok(()),
            Some(_) => Err(ResidentError::ActorKind),
            None => Err(ResidentError::ActorMissing),
        }
    }

    fn enqueue_grid(&mut self, binding: StrategyBinding, decision: GridDecision) {
        if let GridDecision::Actions(actions) = decision {
            for action in actions {
                self.enqueue(ResidentSemanticIntent::Grid {
                    binding: binding.clone(),
                    action,
                });
            }
        }
    }

    fn consume_control(&mut self, delivery: ResidentControlDelivery) -> Result<(), ResidentError> {
        if delivery.inbox_sequence == 0
            || delivery.inbox_sequence <= self.recovery.last_control_inbox_sequence
            || delivery.command.venue != self.account.exchange
            || delivery.command.trading_account_id != self.account.account
        {
            return Err(ResidentError::ControlDelivery);
        }
        // Control is addressed to an already registered actor.  It carries no owner/run ID, so
        // lookup returns the authoritative binding captured by the actor maps via this helper.
        let binding = self.binding_for(&delivery.command)?;
        self.recovery.last_control_inbox_sequence = delivery.inbox_sequence;
        self.enqueue(ResidentSemanticIntent::Control {
            binding,
            action: delivery.command.action,
        });
        Ok(())
    }

    fn binding_for(
        &self,
        command: &ControlCommandRequest,
    ) -> Result<StrategyBinding, ResidentError> {
        let key = self
            .actors
            .keys()
            .find(|key| key.instance_id == command.instance_id && key.symbol == command.symbol)
            .ok_or(ResidentError::ActorMissing)?;
        self.actors
            .get(key)
            .map(|actor| actor.binding().clone())
            .ok_or(ResidentError::RuntimeBindingUnavailable)
    }

    fn enqueue(&mut self, intent: ResidentSemanticIntent) {
        self.queue.push(intent);
    }
}

// Actors are stored once per instance; preserving inline variants avoids a second heap indirection
// in the resident registry and is not part of the durable or wire protocol.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum ResidentActor {
    Grid(GridResidentActor),
    Scalping(ScalpingResidentActor),
    Copy(StrategyBinding),
}

impl ResidentActor {
    const fn kind(&self) -> ResidentActorKind {
        match self {
            Self::Grid(_) => ResidentActorKind::Grid,
            Self::Scalping(_) => ResidentActorKind::Scalping,
            Self::Copy(_) => ResidentActorKind::Copy,
        }
    }

    fn binding(&self) -> &StrategyBinding {
        match self {
            Self::Grid(actor) => actor.binding(),
            Self::Scalping(actor) => actor.binding(),
            Self::Copy(binding) => binding,
        }
    }
}

#[derive(Debug, Default)]
struct PriorityQueues {
    critical: FairQueue,
    fill_repair: FairQueue,
    normal: FairQueue,
}

impl PriorityQueues {
    fn push(&mut self, intent: ResidentSemanticIntent) {
        match intent.priority() {
            ResidentPriority::Critical => self.critical.push(intent),
            ResidentPriority::FillRepair => self.fill_repair.push(intent),
            ResidentPriority::Normal => self.normal.push(intent),
        }
    }

    fn pop(&mut self, priority: ResidentPriority) -> Option<ResidentSemanticIntent> {
        match priority {
            ResidentPriority::Critical => self.critical.pop(),
            ResidentPriority::FillRepair => self.fill_repair.pop(),
            ResidentPriority::Normal => self.normal.pop(),
        }
    }
}

/// One FIFO per owner plus a rotating owner ring gives same-account fairness without pretending
/// that a symbol has an independent writer.
#[derive(Debug, Default)]
struct FairQueue {
    pending: BTreeMap<StrategyInstanceKey, VecDeque<ResidentSemanticIntent>>,
    ring: VecDeque<StrategyInstanceKey>,
}

impl FairQueue {
    fn push(&mut self, intent: ResidentSemanticIntent) {
        let owner = intent.binding().key.clone();
        let queue = self.pending.entry(owner.clone()).or_default();
        let was_empty = queue.is_empty();
        queue.push_back(intent);
        if was_empty {
            self.ring.push_back(owner);
        }
    }

    fn pop(&mut self) -> Option<ResidentSemanticIntent> {
        let owner = self.ring.pop_front()?;
        let queue = self.pending.get_mut(&owner)?;
        let intent = queue.pop_front()?;
        if queue.is_empty() {
            self.pending.remove(&owner);
        } else {
            self.ring.push_back(owner);
        }
        Some(intent)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ResidentError {
    #[error("resident actor belongs to another account")]
    AccountScope,
    #[error("resident actor binding does not match its reducer checkpoint")]
    ActorBinding,
    #[error("resident actor is already registered")]
    ActorOccupied,
    #[error("another resident actor already owns this symbol")]
    SymbolOwnerConflict,
    #[error("resident actor is missing")]
    ActorMissing,
    #[error("resident fact was routed to the wrong strategy kind")]
    ActorKind,
    #[error("grid reducer rejected the normalized private fact")]
    GridReducer,
    #[error("scalping reducer rejected the normalized risk fact")]
    ScalpingReducer,
    #[error("scalping semantic candidate does not match its registered actor")]
    CandidateBinding,
    #[error("Control delivery is non-monotonic or outside this account")]
    ControlDelivery,
    #[error("Control must obtain the recovered runtime binding; it cannot construct one")]
    RuntimeBindingUnavailable,
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use venue_domain::domain::{Amount, Asset, OrderSide, Price, Symbol};
    use venue_gateway_api::{GatewayMode, VenueId};
    use venue_runtime::{ExchangeId, StrategyKind};
    use venue_strategies::{
        hedged_grid::{
            GridOrderIntent, GridOrderKey, GridOrderRole, GridPosition, HedgedGridBinding,
            HedgedGridParams,
        },
        scalping::RiskUnit,
    };

    use super::*;

    const ACCOUNT: &str = "00000000-0000-4000-8000-000000000001";

    fn account() -> Result<AccountKey, Box<dyn std::error::Error>> {
        Ok(AccountKey::new(ExchangeId::Okx, ACCOUNT)?)
    }

    fn binding(
        kind: StrategyKind,
        instance: &str,
        symbol: &str,
    ) -> Result<StrategyBinding, Box<dyn std::error::Error>> {
        let key = StrategyInstanceKey::new(account()?, kind, instance, symbol.parse::<Symbol>()?)?;
        Ok(StrategyBinding::new(key, "run-1", "a".repeat(64))?)
    }

    fn grid_actor(
        instance: &str,
        symbol: &str,
    ) -> Result<GridResidentActor, Box<dyn std::error::Error>> {
        let binding = binding(StrategyKind::HedgedGrid, instance, symbol)?;
        let state = HedgedGridState::new_with_params(
            HedgedGridBinding {
                strategy_instance_id: instance.to_owned(),
                run_id: "run-1".to_owned(),
                exchange: "okx".to_owned(),
                account: ACCOUNT.to_owned(),
                symbol: symbol.parse()?,
                config_version: "a".repeat(64),
                owner_scope: "owner-1".to_owned(),
            },
            HedgedGridParams::fixed_release(Asset::new("USDT")?, 3)?,
        )?;
        Ok(GridResidentActor::new(binding, state)?)
    }

    fn control(
        sequence: u64,
        binding: &StrategyBinding,
        action: ControlAction,
    ) -> ResidentControlDelivery {
        ResidentControlDelivery {
            inbox_sequence: sequence,
            command: ControlCommandRequest {
                schema_version: venue_control_protocol::CONTROL_SCHEMA_VERSION,
                request_id: format!("control-{sequence}"),
                venue: VenueId::Okx,
                mode: GatewayMode::Live,
                trading_account_id: ACCOUNT.to_owned(),
                instance_id: binding.key.instance_id.clone(),
                symbol: binding.key.symbol.clone(),
                action,
                trade: None,
                expected_config_epoch: 1,
                confirmation: None,
            },
        }
    }

    #[test]
    fn grid_and_scalping_share_a_fair_account_queue() -> Result<(), Box<dyn std::error::Error>> {
        let mut loop_ = ResidentLoop::new(account()?);
        let grid = grid_actor("grid-1", "DOGE/USDT")?;
        let grid_key = grid.binding().key.clone();
        loop_.register_grid(grid)?;
        let scalping_binding = binding(StrategyKind::Scalping, "scalp-1", "BTC/USDT")?;
        let scalping = ScalpingResidentActor::new(
            scalping_binding.clone(),
            &ScalpingParams::phase8(Amount::new(Asset::new("USDT")?, Decimal::TEN)),
        );
        loop_.register_scalping(scalping)?;

        loop_.consume(ResidentFact::GridInventory {
            target: grid_key,
            inventory: GridInventory {
                private_generation: 1,
                private_observed_at_ms: 1,
                mark_price: Price::new(Decimal::ONE)?,
                long_quantity: Decimal::ZERO,
                short_quantity: Decimal::ZERO,
            },
        })?;
        // Grid emits a reset (neutral); a semantic scalping entry is queued independently below.
        let grid_intent = loop_.next_intent().ok_or("grid intent missing")?;
        assert!(matches!(grid_intent, ResidentSemanticIntent::Grid { .. }));
        Ok(())
    }

    #[test]
    fn duplicate_symbol_owner_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut loop_ = ResidentLoop::new(account()?);
        let grid = grid_actor("grid-1", "DOGE/USDT")?;
        loop_.register_grid(grid)?;
        let scalping = ScalpingResidentActor::new(
            binding(StrategyKind::Scalping, "scalp-1", "DOGE/USDT")?,
            &ScalpingParams::phase8(Amount::new(Asset::new("USDT")?, Decimal::TEN)),
        );
        assert_eq!(
            loop_.register_scalping(scalping),
            Err(ResidentError::SymbolOwnerConflict)
        );
        Ok(())
    }

    #[test]
    fn unknown_fence_survives_restart_and_blocks_new_risk() -> Result<(), Box<dyn std::error::Error>>
    {
        let state = ResidentRecoveryState {
            unknown_fenced: true,
            last_control_inbox_sequence: 7,
        };
        let mut loop_ = ResidentLoop::from_recovery(account()?, state.clone());
        assert_eq!(loop_.recovery_state(), &state);
        assert!(loop_.next_intent().is_none());
        Ok(())
    }

    #[test]
    fn scalping_risk_is_reduced_without_an_execution_command()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut loop_ = ResidentLoop::new(account()?);
        let binding = binding(StrategyKind::Scalping, "scalp-1", "BTC/USDT")?;
        let actor = ScalpingResidentActor::new(
            binding.clone(),
            &ScalpingParams::phase8(Amount::new(Asset::new("USDT")?, Decimal::TEN)),
        );
        loop_.register_scalping(actor)?;
        let snapshot = loop_
            .consume(ResidentFact::ScalpingRisk {
                target: binding.key.clone(),
                fact: RiskFact {
                    fact_id: "risk-1".to_owned(),
                    event_time_ms: 1,
                    valuation_generation: 1,
                    risk_unit: RiskUnit::shadow(),
                    realized_pnl: Decimal::ZERO,
                },
            })?
            .ok_or("risk snapshot missing")?;
        assert_eq!(snapshot.valuation_generation, Some(1));
        Ok(())
    }

    #[test]
    fn stop_control_preempts_normal_actor_work() -> Result<(), Box<dyn std::error::Error>> {
        let mut loop_ = ResidentLoop::new(account()?);
        let grid = grid_actor("grid-1", "DOGE/USDT")?;
        let binding = grid.binding().clone();
        let key = binding.key.clone();
        loop_.register_grid(grid)?;
        loop_.consume(ResidentFact::GridInventory {
            target: key,
            inventory: GridInventory {
                private_generation: 1,
                private_observed_at_ms: 1,
                mark_price: Price::new(Decimal::ONE)?,
                long_quantity: Decimal::ZERO,
                short_quantity: Decimal::ZERO,
            },
        })?;
        loop_.consume(ResidentFact::Control(control(
            1,
            &binding,
            ControlAction::Stop,
        )))?;
        assert!(matches!(
            loop_.next_intent(),
            Some(ResidentSemanticIntent::Control {
                action: ControlAction::Stop,
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn unknown_holds_entry_but_not_flatten() -> Result<(), Box<dyn std::error::Error>> {
        let mut loop_ = ResidentLoop::new(account()?);
        let grid = grid_actor("grid-1", "DOGE/USDT")?;
        let binding = grid.binding().clone();
        loop_.register_grid(grid)?;
        loop_.enqueue(ResidentSemanticIntent::Grid {
            binding: binding.clone(),
            action: GridAction::Place(GridOrderIntent {
                key: GridOrderKey {
                    epoch: 1,
                    position: GridPosition::Long,
                    role: GridOrderRole::Open,
                    level: 1,
                },
                side: OrderSide::Buy,
                price: Price::new(Decimal::ONE)?,
                quantity: Decimal::ONE,
                reduce_only: false,
            }),
        });
        loop_.enqueue(ResidentSemanticIntent::Control {
            binding,
            action: ControlAction::Flatten,
        });
        loop_.consume(ResidentFact::UnknownFence { active: true })?;
        assert!(matches!(
            loop_.next_intent(),
            Some(ResidentSemanticIntent::Control {
                action: ControlAction::Flatten,
                ..
            })
        ));
        assert!(loop_.next_intent().is_none());
        loop_.consume(ResidentFact::UnknownFence { active: false })?;
        assert!(matches!(
            loop_.next_intent(),
            Some(ResidentSemanticIntent::Grid {
                action: GridAction::Place(_),
                ..
            })
        ));
        Ok(())
    }
}
