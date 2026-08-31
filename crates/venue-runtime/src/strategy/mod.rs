use std::collections::{BTreeMap, VecDeque};

use sha2::{Digest, Sha256};
mod actor_applied;

pub(crate) use actor_applied::{ActorAppliedTurnStore, AppliedPrivateDelivery};

use crate::{
    domain::{
        DomainEvent, EventSource, FactRecord, MarketEvent, NativeOrderFamily, StrategyBinding,
        StrategyInstanceKey, StrategyTurnToken, Symbol,
    },
    storage::PersistedPrivateEvidence,
};

const MAX_FACTS_PER_PRIVATE_EVIDENCE: u32 = 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateEvidenceRef {
    sequence: u64,
    generation: u64,
    received_at_ms: u64,
    payload_sha256: String,
}

impl PrivateEvidenceRef {
    fn from_persisted(evidence: &PersistedPrivateEvidence) -> Result<Self, StrategyHostError> {
        if evidence.sequence() == 0
            || evidence.generation() == 0
            || evidence.payload_sha256().is_empty()
        {
            return Err(StrategyHostError::PrivateEvidence);
        }
        Ok(Self {
            sequence: evidence.sequence(),
            generation: evidence.generation(),
            received_at_ms: evidence.received_at_ms(),
            payload_sha256: evidence.payload_sha256().to_owned(),
        })
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn received_at_ms(&self) -> u64 {
        self.received_at_ms
    }

    #[must_use]
    pub fn payload_sha256(&self) -> &str {
        &self.payload_sha256
    }
}

/// A normalized private fact can only be constructed with a reference to an already appended raw
/// evidence record. The router therefore has no API that accepts an unpersisted user-stream fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedPrivateFact {
    evidence: PrivateEvidenceRef,
    record: FactRecord,
    order_family: Option<NativeOrderFamily>,
    fact_index: u32,
    fact_count: u32,
}

impl PersistedPrivateFact {
    pub(crate) fn new(
        evidence: &PersistedPrivateEvidence,
        order_family: Option<NativeOrderFamily>,
        record: FactRecord,
    ) -> Result<Self, StrategyHostError> {
        Self::new_indexed(evidence, order_family, 0, 1, record)
    }

    pub(crate) fn new_indexed(
        evidence: &PersistedPrivateEvidence,
        order_family: Option<NativeOrderFamily>,
        fact_index: u32,
        fact_count: u32,
        record: FactRecord,
    ) -> Result<Self, StrategyHostError> {
        let evidence = PrivateEvidenceRef::from_persisted(evidence)?;
        record
            .header
            .validate()
            .map_err(|_| StrategyHostError::PrivateFact)?;
        if fact_count == 0
            || fact_count > MAX_FACTS_PER_PRIVATE_EVIDENCE
            || fact_index >= fact_count
            || record.header.source != EventSource::PrivateAccount
            || record.header.generation != evidence.generation
            || record.header.received_at_ms != evidence.received_at_ms
            || record.header.source_sequence != Some(evidence.sequence)
            || matches!(record.event, DomainEvent::Instrument(_))
            || (matches!(record.event, DomainEvent::Order(_) | DomainEvent::Fill(_))
                != order_family.is_some())
        {
            return Err(StrategyHostError::PrivateFact);
        }
        validate_private_domain_event(&record.event)?;
        Ok(Self {
            evidence,
            record,
            order_family,
            fact_index,
            fact_count,
        })
    }

    /// The production account ingress writes the normalized fact to the shared facts journal
    /// before constructing this value.  Its sequence is therefore the fsynced journal sequence,
    /// not a caller assertion or an independent raw-payload journal.
    pub(crate) fn from_persisted_fact_record(
        sequence: u64,
        order_family: Option<NativeOrderFamily>,
        record: FactRecord,
    ) -> Result<Self, StrategyHostError> {
        record
            .header
            .validate()
            .map_err(|_| StrategyHostError::PrivateFact)?;
        if sequence == 0
            || record.header.source != EventSource::PrivateAccount
            || record.header.source_sequence != Some(sequence)
            || matches!(record.event, DomainEvent::Instrument(_))
            || (matches!(record.event, DomainEvent::Order(_) | DomainEvent::Fill(_))
                != order_family.is_some())
        {
            return Err(StrategyHostError::PrivateFact);
        }
        validate_private_domain_event(&record.event)?;
        let encoded = serde_json::to_vec(&record).map_err(|_| StrategyHostError::PrivateFact)?;
        let evidence = PrivateEvidenceRef {
            sequence,
            generation: record.header.generation,
            received_at_ms: record.header.received_at_ms,
            payload_sha256: format!("{:x}", Sha256::digest(encoded)),
        };
        Ok(Self {
            evidence,
            record,
            order_family,
            fact_index: 0,
            fact_count: 1,
        })
    }

    #[must_use]
    pub const fn fact_index(&self) -> u32 {
        self.fact_index
    }

    #[must_use]
    pub const fn fact_count(&self) -> u32 {
        self.fact_count
    }

    /// Canonical endpoint family is mandatory for order/fill facts because native identifiers may
    /// overlap between regular, conditional and algo endpoints.
    #[must_use]
    pub const fn order_family(&self) -> Option<NativeOrderFamily> {
        self.order_family
    }

    #[must_use]
    pub(crate) const fn evidence(&self) -> &PrivateEvidenceRef {
        &self.evidence
    }

    #[must_use]
    pub const fn record(&self) -> &FactRecord {
        &self.record
    }

    #[must_use]
    pub fn symbol(&self) -> Option<&Symbol> {
        match &self.record.event {
            DomainEvent::Order(order) => Some(&order.symbol),
            DomainEvent::Fill(fill) => Some(&fill.symbol),
            DomainEvent::Position(position) => Some(&position.symbol),
            DomainEvent::Instrument(instrument) => Some(&instrument.symbol),
            DomainEvent::Balance(_) | DomainEvent::Funding(_) => None,
        }
    }

    #[must_use]
    pub const fn requires_exact_order_owner(&self) -> bool {
        matches!(
            self.record.event,
            DomainEvent::Order(_) | DomainEvent::Fill(_)
        )
    }
}

pub(crate) fn validate_private_domain_event(event: &DomainEvent) -> Result<(), StrategyHostError> {
    let valid = match event {
        DomainEvent::Order(order) => order.validate().is_ok(),
        DomainEvent::Fill(fill) => fill.validate().is_ok(),
        DomainEvent::Position(position) => {
            // Hedge-mode legs are unsigned magnitudes. Net-mode adapters may preserve signed
            // quantity, so its direction remains encoded in the value rather than a fake leg.
            !matches!(
                position.side,
                crate::domain::PositionSide::Long | crate::domain::PositionSide::Short
            ) || !position.quantity.is_sign_negative()
        }
        DomainEvent::Balance(balance) => balance.validate().is_ok(),
        // Funding is a signed account adjustment. Asset validity is guaranteed by the typed Asset
        // constructor and Decimal has no NaN/infinity representation.
        DomainEvent::Funding(_) => true,
        DomainEvent::Instrument(_) => false,
    };
    valid.then_some(()).ok_or(StrategyHostError::PrivateFact)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MarketEventKind {
    Snapshot,
    Delta,
    Trade,
    Bar,
    Ticker,
    MarkFunding,
}

impl MarketEventKind {
    #[must_use]
    const fn coalescible(self) -> bool {
        matches!(self, Self::Snapshot | Self::Ticker | Self::MarkFunding)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountMarketEvent {
    pub received_at_ms: u64,
    pub event: MarketEvent,
}

impl AccountMarketEvent {
    pub fn new(received_at_ms: u64, event: MarketEvent) -> Result<Self, StrategyHostError> {
        if received_at_ms == 0
            || event_generation(&event) == 0
            || event_sequence(&event) == 0
            || embedded_received_at(&event).is_some_and(|embedded| embedded != received_at_ms)
            || matches!(&event, MarketEvent::Bar(bar) if !bar.is_valid())
        {
            return Err(StrategyHostError::MarketFact);
        }
        Ok(Self {
            received_at_ms,
            event,
        })
    }

    #[must_use]
    pub fn symbol(&self) -> &Symbol {
        event_symbol(&self.event)
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        event_generation(&self.event)
    }

    #[must_use]
    pub fn sequence(&self) -> u64 {
        event_sequence(&self.event)
    }

    #[must_use]
    pub const fn kind(&self) -> MarketEventKind {
        match self.event {
            MarketEvent::Snapshot(_) => MarketEventKind::Snapshot,
            MarketEvent::Delta(_) => MarketEventKind::Delta,
            MarketEvent::Trade(_) => MarketEventKind::Trade,
            MarketEvent::Bar(_) => MarketEventKind::Bar,
            MarketEvent::Ticker(_) => MarketEventKind::Ticker,
            MarketEvent::MarkFunding(_) => MarketEventKind::MarkFunding,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationNotice {
    pub private_generation: u64,
    pub desired_open_orders: usize,
    pub actual_open_orders: usize,
    pub missing_client_order_ids: Vec<String>,
    pub unexpected_client_order_ids: Vec<String>,
    pub mismatched_client_order_ids: Vec<String>,
}

impl ReconciliationNotice {
    #[must_use]
    pub fn exact(&self) -> bool {
        self.missing_client_order_ids.is_empty()
            && self.unexpected_client_order_ids.is_empty()
            && self.mismatched_client_order_ids.is_empty()
    }

    #[must_use]
    pub const fn signed_owned_orders_are_zero(&self) -> bool {
        self.actual_open_orders == 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StrategyControl {
    Pause,
    Resume,
    Stop,
    Flatten,
    ParametersChanged {
        config_digest: String,
        config_epoch: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StrategyInput {
    Private(PersistedPrivateFact),
    Reconciliation(ReconciliationNotice),
    Control(StrategyControl),
    Market(AccountMarketEvent),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StrategyTurn {
    token: StrategyTurnToken,
    input: StrategyInput,
}

impl StrategyTurn {
    pub(crate) fn issued(token: StrategyTurnToken, input: StrategyInput) -> Self {
        Self { token, input }
    }

    #[must_use]
    pub const fn token(&self) -> &StrategyTurnToken {
        &self.token
    }

    #[must_use]
    pub const fn input(&self) -> &StrategyInput {
        &self.input
    }

    pub(crate) fn into_parts(self) -> (StrategyTurnToken, StrategyInput) {
        (self.token, self.input)
    }
}

// Queue exhaustion is a fail-closed scheduling signal; the pure host never drops lossless facts.
const MAX_PRIVATE_MAILBOX_DEPTH: usize = 4_096;
const MAX_LOSSLESS_MARKET_MAILBOX_DEPTH: usize = 4_096;
const MAX_RECONCILIATION_MAILBOX_DEPTH: usize = 1_024;
const MAX_CONTROL_MAILBOX_DEPTH: usize = 1_024;
const PRIVATE_INPUT_BURST_LIMIT: usize = 64;

#[derive(Clone, Debug)]
enum MarketMailboxEntry {
    Lossless(Box<AccountMarketEvent>),
    Coalesced(MarketEventKind),
}

/// One host is mutably driven by one account-runtime turn. Private facts are lossless and receive
/// bounded priority over reconciliation, control and public market updates.
#[derive(Clone, Debug)]
pub struct StrategyActorHost {
    binding: StrategyBinding,
    config_epoch: u64,
    private: VecDeque<PersistedPrivateFact>,
    reconciliations: VecDeque<ReconciliationNotice>,
    controls: VecDeque<StrategyControl>,
    market_order: VecDeque<MarketMailboxEntry>,
    latest_market: BTreeMap<MarketEventKind, AccountMarketEvent>,
    lossless_market_depth: usize,
    market_generation: Option<u64>,
    consecutive_private_inputs: usize,
    prefer_control: bool,
}

impl StrategyActorHost {
    #[must_use]
    pub fn new(binding: StrategyBinding) -> Self {
        Self {
            binding,
            config_epoch: 1,
            private: VecDeque::new(),
            reconciliations: VecDeque::new(),
            controls: VecDeque::new(),
            market_order: VecDeque::new(),
            latest_market: BTreeMap::new(),
            lossless_market_depth: 0,
            market_generation: None,
            consecutive_private_inputs: 0,
            prefer_control: false,
        }
    }

    #[must_use]
    pub const fn binding(&self) -> &StrategyBinding {
        &self.binding
    }

    #[must_use]
    pub const fn config_epoch(&self) -> u64 {
        self.config_epoch
    }

    pub(crate) fn install_configuration(
        &mut self,
        binding: StrategyBinding,
        config_epoch: u64,
    ) -> Result<(), StrategyHostError> {
        if binding.key != self.binding.key
            || binding.run_id != self.binding.run_id
            || binding.config_digest == self.binding.config_digest
            || config_epoch <= self.config_epoch
        {
            return Err(StrategyHostError::Configuration);
        }
        if self.controls.len() >= MAX_CONTROL_MAILBOX_DEPTH {
            return Err(StrategyHostError::ControlMailboxFull);
        }
        let control = StrategyControl::ParametersChanged {
            config_digest: binding.config_digest.clone(),
            config_epoch,
        };
        self.binding = binding;
        self.config_epoch = config_epoch;
        self.controls.push_back(control);
        Ok(())
    }

    pub(crate) fn restore_configuration(
        &mut self,
        binding: StrategyBinding,
        config_epoch: u64,
    ) -> Result<(), StrategyHostError> {
        if binding.key != self.binding.key
            || binding.run_id != self.binding.run_id
            || binding.config_digest != self.binding.config_digest
            || config_epoch == 0
        {
            return Err(StrategyHostError::Configuration);
        }
        self.binding = binding;
        self.config_epoch = config_epoch;
        self.clear_transient_inputs();
        Ok(())
    }

    pub(crate) fn push_private(
        &mut self,
        fact: PersistedPrivateFact,
    ) -> Result<(), StrategyHostError> {
        if fact
            .symbol()
            .is_some_and(|symbol| symbol != &self.binding.key.symbol)
        {
            return Err(StrategyHostError::WrongInstance);
        }
        if self.private.len() >= MAX_PRIVATE_MAILBOX_DEPTH {
            return Err(StrategyHostError::PrivateMailboxFull);
        }
        self.private.push_back(fact);
        Ok(())
    }

    pub(crate) fn push_reconciliation(
        &mut self,
        notice: ReconciliationNotice,
    ) -> Result<(), StrategyHostError> {
        if notice.private_generation == 0 {
            return Err(StrategyHostError::Reconciliation);
        }
        if self.reconciliations.len() >= MAX_RECONCILIATION_MAILBOX_DEPTH {
            return Err(StrategyHostError::ReconciliationMailboxFull);
        }
        self.reconciliations.push_back(notice);
        Ok(())
    }

    pub(crate) fn push_control(
        &mut self,
        control: StrategyControl,
    ) -> Result<(), StrategyHostError> {
        if self.controls.len() >= MAX_CONTROL_MAILBOX_DEPTH {
            return Err(StrategyHostError::ControlMailboxFull);
        }
        self.controls.push_back(control);
        Ok(())
    }

    pub(crate) fn push_market(
        &mut self,
        event: AccountMarketEvent,
    ) -> Result<(), StrategyHostError> {
        if event.symbol() != &self.binding.key.symbol {
            return Err(StrategyHostError::WrongInstance);
        }
        match self.market_generation {
            Some(generation) if event.generation() < generation => {
                return Err(StrategyHostError::StaleMarketGeneration);
            }
            Some(generation) if event.generation() > generation => {
                self.market_order.clear();
                self.latest_market.clear();
                self.lossless_market_depth = 0;
                self.market_generation = Some(event.generation());
            }
            None => self.market_generation = Some(event.generation()),
            Some(_) => {}
        }
        let kind = event.kind();
        if kind.coalescible() {
            if self.latest_market.insert(kind, event).is_some() {
                self.market_order.retain(|entry| {
                    !matches!(entry, MarketMailboxEntry::Coalesced(existing) if *existing == kind)
                });
            }
            self.market_order
                .push_back(MarketMailboxEntry::Coalesced(kind));
            return Ok(());
        }
        if self.lossless_market_depth >= MAX_LOSSLESS_MARKET_MAILBOX_DEPTH {
            return Err(StrategyHostError::LosslessMarketMailboxFull);
        }
        self.market_order
            .push_back(MarketMailboxEntry::Lossless(Box::new(event)));
        self.lossless_market_depth += 1;
        Ok(())
    }

    pub fn pop_next(&mut self) -> Option<StrategyInput> {
        let maintenance_pending = !self.reconciliations.is_empty() || !self.controls.is_empty();
        if (self.consecutive_private_inputs < PRIVATE_INPUT_BURST_LIMIT || !maintenance_pending)
            && let Some(fact) = self.private.pop_front()
        {
            self.consecutive_private_inputs = self
                .consecutive_private_inputs
                .saturating_add(1)
                .min(PRIVATE_INPUT_BURST_LIMIT);
            return Some(StrategyInput::Private(fact));
        }
        if let Some(input) = self.pop_maintenance() {
            self.consecutive_private_inputs = 0;
            return Some(input);
        }
        if let Some(fact) = self.private.pop_front() {
            self.consecutive_private_inputs = 1;
            return Some(StrategyInput::Private(fact));
        }
        self.consecutive_private_inputs = 0;
        while let Some(entry) = self.market_order.pop_front() {
            match entry {
                MarketMailboxEntry::Lossless(event) => {
                    self.lossless_market_depth = self.lossless_market_depth.saturating_sub(1);
                    return Some(StrategyInput::Market(*event));
                }
                MarketMailboxEntry::Coalesced(kind) => {
                    if let Some(event) = self.latest_market.remove(&kind) {
                        return Some(StrategyInput::Market(event));
                    }
                }
            }
        }
        None
    }

    #[must_use]
    pub fn pending_private(&self) -> usize {
        self.private.len()
    }

    #[must_use]
    pub fn pending_market_kinds(&self) -> usize {
        self.latest_market.len()
    }

    #[must_use]
    pub const fn pending_lossless_market(&self) -> usize {
        self.lossless_market_depth
    }

    /// Reconnect/config fences invalidate every queued transient input. Durable lifecycle and actor
    /// checkpoint state are restored separately; old inputs are never replayed under new authority.
    pub(crate) fn clear_transient_inputs(&mut self) {
        self.private.clear();
        self.reconciliations.clear();
        self.controls.clear();
        self.market_order.clear();
        self.latest_market.clear();
        self.lossless_market_depth = 0;
        self.consecutive_private_inputs = 0;
        self.prefer_control = false;
    }

    fn pop_maintenance(&mut self) -> Option<StrategyInput> {
        if self.prefer_control {
            if let Some(control) = self.controls.pop_front() {
                self.prefer_control = false;
                return Some(StrategyInput::Control(control));
            }
            if let Some(notice) = self.reconciliations.pop_front() {
                return Some(StrategyInput::Reconciliation(notice));
            }
        } else {
            if let Some(notice) = self.reconciliations.pop_front() {
                self.prefer_control = true;
                return Some(StrategyInput::Reconciliation(notice));
            }
            if let Some(control) = self.controls.pop_front() {
                return Some(StrategyInput::Control(control));
            }
        }
        None
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StrategyHostError {
    #[error("private evidence reference is invalid")]
    PrivateEvidence,
    #[error("normalized private fact does not match its durable evidence boundary")]
    PrivateFact,
    #[error("market fact lacks a positive generation, sequence, or receive timestamp")]
    MarketFact,
    #[error("fact belongs to another strategy instance")]
    WrongInstance,
    #[error("reconciliation generation is invalid")]
    Reconciliation,
    #[error("private mailbox reached its lossless capacity")]
    PrivateMailboxFull,
    #[error("lossless public-market mailbox reached its capacity")]
    LosslessMarketMailboxFull,
    #[error("public-market event belongs to an older symbol generation")]
    StaleMarketGeneration,
    #[error("reconciliation mailbox reached its bounded capacity")]
    ReconciliationMailboxFull,
    #[error("control mailbox reached its bounded capacity")]
    ControlMailboxFull,
    #[error("strategy configuration binding or epoch transition is invalid")]
    Configuration,
}

fn event_symbol(event: &MarketEvent) -> &Symbol {
    match event {
        MarketEvent::Snapshot(value) => &value.symbol,
        MarketEvent::Delta(value) => &value.symbol,
        MarketEvent::Trade(value) => &value.symbol,
        MarketEvent::Bar(value) => &value.symbol,
        MarketEvent::Ticker(value) => &value.symbol,
        MarketEvent::MarkFunding(value) => &value.symbol,
    }
}

fn event_generation(event: &MarketEvent) -> u64 {
    match event {
        MarketEvent::Snapshot(value) => value.generation,
        MarketEvent::Delta(value) => value.generation,
        MarketEvent::Trade(value) => value.generation,
        MarketEvent::Bar(value) => value.generation,
        MarketEvent::Ticker(value) => value.generation,
        MarketEvent::MarkFunding(value) => value.generation,
    }
}

fn event_sequence(event: &MarketEvent) -> u64 {
    match event {
        MarketEvent::Snapshot(value) => value.sequence,
        MarketEvent::Delta(value) => value.sequence,
        MarketEvent::Trade(value) => value.last_trade_id,
        MarketEvent::Bar(value) => value.sequence,
        MarketEvent::Ticker(value) => value.update_id,
        MarketEvent::MarkFunding(value) => value.exchange_time_ms,
    }
}

fn embedded_received_at(event: &MarketEvent) -> Option<u64> {
    match event {
        MarketEvent::Snapshot(_) | MarketEvent::Delta(_) => None,
        MarketEvent::Trade(value) => Some(value.received_at_ms),
        MarketEvent::Bar(value) => Some(value.received_at_ms),
        MarketEvent::Ticker(value) => Some(value.received_at_ms),
        MarketEvent::MarkFunding(value) => Some(value.received_at_ms),
    }
}

#[must_use]
pub fn instance_key(host: &StrategyActorHost) -> &StrategyInstanceKey {
    &host.binding.key
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        domain::{
            AccountBalance, Amount, EventHeader, EventId, FieldState, Fill, Order, OrderPurpose,
            OrderSide, OrderState, Position, PositionSide, Price, PublicBar, PublicTicker,
        },
        runtime::account::{AccountKey, ExchangeId, StrategyKind},
        storage::{PersistedPrivateEvidence, PrivateEvidence, PrivateEvidenceJournal},
    };

    fn binding() -> Result<StrategyBinding, Box<dyn Error>> {
        let account = AccountKey::new(ExchangeId::Binance, "portfolio")?;
        let key = StrategyInstanceKey::new(
            account,
            StrategyKind::HedgedGrid,
            "grid_sol",
            Symbol::new("SOL", "USDT")?,
        )?;
        Ok(StrategyBinding::new(key, "run_1", "config_1")?)
    }

    fn price(value: i64) -> Result<Price, Box<dyn Error>> {
        Ok(Price::new(Decimal::new(value, 0))?)
    }

    fn bar(binding: &StrategyBinding, sequence: u64) -> Result<AccountMarketEvent, Box<dyn Error>> {
        let received_at_ms = 1_000 + sequence;
        Ok(AccountMarketEvent::new(
            received_at_ms,
            MarketEvent::Bar(PublicBar {
                symbol: binding.key.symbol.clone(),
                generation: 1,
                received_at_ms,
                sequence,
                open_time_ms: sequence * 60,
                close_time_ms: sequence * 60 + 59,
                interval_ms: 60,
                open: price(10)?,
                high: price(11)?,
                low: price(9)?,
                close: price(10)?,
                base_volume: FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
                quote_volume: FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
                trade_count: FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
                taker_buy_base_volume: FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
                taker_buy_quote_volume: FieldState::Unavailable {
                    reason: crate::domain::UnknownReason::SourceOmitted,
                },
            }),
        )?)
    }

    fn ticker(
        binding: &StrategyBinding,
        sequence: u64,
    ) -> Result<AccountMarketEvent, Box<dyn Error>> {
        ticker_generation(binding, 1, sequence)
    }

    fn ticker_generation(
        binding: &StrategyBinding,
        generation: u64,
        sequence: u64,
    ) -> Result<AccountMarketEvent, Box<dyn Error>> {
        let received_at_ms = 2_000 + sequence;
        Ok(AccountMarketEvent::new(
            received_at_ms,
            MarketEvent::Ticker(PublicTicker {
                symbol: binding.key.symbol.clone(),
                generation,
                received_at_ms,
                exchange_time_ms: 1_900 + sequence,
                transaction_time_ms: 1_900 + sequence,
                update_id: sequence,
                bid_price: price(9)?,
                bid_quantity: Decimal::ONE,
                ask_price: price(10)?,
                ask_quantity: Decimal::ONE,
            }),
        )?)
    }

    fn private_fact(binding: &StrategyBinding) -> Result<PersistedPrivateFact, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let mut journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        let receipt = journal.append_persisted(PrivateEvidence::new(
            1,
            100,
            "position payload".to_owned(),
        )?)?;
        let record = FactRecord {
            header: EventHeader {
                schema_version: 1,
                event_id: EventId::new("private_1")?,
                source: EventSource::PrivateAccount,
                source_sequence: Some(receipt.sequence()),
                received_at_ms: receipt.received_at_ms(),
                generation: receipt.generation(),
            },
            event: DomainEvent::Position(Position {
                symbol: binding.key.symbol.clone(),
                side: PositionSide::Long,
                quantity: Decimal::ZERO,
                entry_price: None,
                mark_price: None,
            }),
        };
        Ok(PersistedPrivateFact::new(&receipt, None, record)?)
    }

    fn evidence_receipt() -> Result<(tempfile::TempDir, PersistedPrivateEvidence), Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let mut journal = PrivateEvidenceJournal::open(directory.path().join("private.jsonl"))?;
        let receipt = journal.append_persisted(PrivateEvidence::new(
            1,
            100,
            "private domain payload".to_owned(),
        )?)?;
        Ok((directory, receipt))
    }

    fn private_record(
        receipt: &PersistedPrivateEvidence,
        event_id: &str,
        event: DomainEvent,
    ) -> Result<FactRecord, Box<dyn Error>> {
        Ok(FactRecord {
            header: EventHeader {
                schema_version: 1,
                event_id: EventId::new(event_id)?,
                source: EventSource::PrivateAccount,
                source_sequence: Some(receipt.sequence()),
                received_at_ms: receipt.received_at_ms(),
                generation: receipt.generation(),
            },
            event,
        })
    }

    fn valid_order(binding: &StrategyBinding) -> Result<Order, Box<dyn Error>> {
        Ok(Order {
            order_id: "venue_1".to_owned(),
            client_order_id: FieldState::Known("client_1".to_owned()),
            symbol: binding.key.symbol.clone(),
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            purpose: FieldState::Known(OrderPurpose::Entry),
            state: OrderState::New,
            quantity: Decimal::ONE,
            filled_quantity: Decimal::ZERO,
            limit_price: Some(price(10)?),
            average_price: FieldState::Missing,
            reduce_only: false,
        })
    }

    fn valid_fill(binding: &StrategyBinding) -> Result<Fill, Box<dyn Error>> {
        Ok(Fill {
            fill_id: "fill_1".to_owned(),
            execution_sequence: FieldState::Known(1),
            order_id: "venue_1".to_owned(),
            symbol: binding.key.symbol.clone(),
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            quantity: Decimal::ONE,
            price: price(10)?,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(90),
        })
    }

    #[test]
    fn private_fact_token_requires_valid_order_fill_balance_and_hedge_position()
    -> Result<(), Box<dyn Error>> {
        let binding = binding()?;
        let (_directory, receipt) = evidence_receipt()?;

        let mut invalid_order = valid_order(&binding)?;
        invalid_order.quantity = Decimal::ZERO;
        assert_eq!(
            PersistedPrivateFact::new(
                &receipt,
                Some(NativeOrderFamily::UmOrder),
                private_record(&receipt, "invalid_order", DomainEvent::Order(invalid_order))?,
            ),
            Err(StrategyHostError::PrivateFact)
        );

        let mut invalid_fill = valid_fill(&binding)?;
        invalid_fill.quantity = Decimal::ZERO;
        assert_eq!(
            PersistedPrivateFact::new(
                &receipt,
                Some(NativeOrderFamily::UmOrder),
                private_record(&receipt, "invalid_fill", DomainEvent::Fill(invalid_fill))?,
            ),
            Err(StrategyHostError::PrivateFact)
        );

        let invalid_balance = AccountBalance {
            asset: "USDT".parse()?,
            wallet_balance: Decimal::ONE,
            available_balance: Decimal::new(-1, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        };
        assert_eq!(
            PersistedPrivateFact::new(
                &receipt,
                None,
                private_record(
                    &receipt,
                    "invalid_balance",
                    DomainEvent::Balance(invalid_balance),
                )?,
            ),
            Err(StrategyHostError::PrivateFact)
        );

        let invalid_hedge_position = Position {
            symbol: binding.key.symbol.clone(),
            side: PositionSide::Long,
            quantity: Decimal::new(-1, 0),
            entry_price: None,
            mark_price: None,
        };
        assert_eq!(
            PersistedPrivateFact::new(
                &receipt,
                None,
                private_record(
                    &receipt,
                    "invalid_position",
                    DomainEvent::Position(invalid_hedge_position),
                )?,
            ),
            Err(StrategyHostError::PrivateFact)
        );
        Ok(())
    }

    #[test]
    fn private_fact_token_accepts_valid_events_and_signed_net_or_funding_values()
    -> Result<(), Box<dyn Error>> {
        let binding = binding()?;
        let (_directory, receipt) = evidence_receipt()?;
        let events = [
            DomainEvent::Order(valid_order(&binding)?),
            DomainEvent::Fill(valid_fill(&binding)?),
            DomainEvent::Balance(AccountBalance {
                asset: "USDT".parse()?,
                wallet_balance: Decimal::ONE,
                available_balance: Decimal::ONE,
                initial_margin: Decimal::ZERO,
                maintenance_margin: Decimal::ZERO,
            }),
            DomainEvent::Position(Position {
                symbol: binding.key.symbol.clone(),
                side: PositionSide::Net,
                quantity: Decimal::new(-1, 0),
                entry_price: None,
                mark_price: None,
            }),
            DomainEvent::Funding(Amount::new("USDT".parse()?, Decimal::new(-1, 0))),
        ];

        for (index, event) in events.into_iter().enumerate() {
            let order_family = matches!(event, DomainEvent::Order(_) | DomainEvent::Fill(_))
                .then_some(NativeOrderFamily::UmOrder);
            let token = PersistedPrivateFact::new(
                &receipt,
                order_family,
                private_record(&receipt, &format!("valid_{index}"), event)?,
            )?;
            assert_eq!(
                token.record().header.event_id.to_string(),
                format!("valid_{index}")
            );
        }
        Ok(())
    }

    fn pop_market(host: &mut StrategyActorHost) -> Result<AccountMarketEvent, Box<dyn Error>> {
        let Some(StrategyInput::Market(event)) = host.pop_next() else {
            return Err("market input missing".into());
        };
        Ok(event)
    }

    #[test]
    fn only_explicit_state_events_are_coalescible() {
        assert!(MarketEventKind::Snapshot.coalescible());
        assert!(MarketEventKind::Ticker.coalescible());
        assert!(MarketEventKind::MarkFunding.coalescible());
        assert!(!MarketEventKind::Delta.coalescible());
        assert!(!MarketEventKind::Trade.coalescible());
        assert!(!MarketEventKind::Bar.coalescible());
    }

    #[test]
    fn actor_mailbox_drops_all_old_generation_market_state() -> Result<(), Box<dyn Error>> {
        let binding = binding()?;
        let mut host = StrategyActorHost::new(binding.clone());
        host.push_market(ticker_generation(&binding, 1, 10)?)?;
        host.push_market(ticker_generation(&binding, 2, 1)?)?;

        let event = pop_market(&mut host)?;
        assert_eq!(event.generation(), 2);
        assert_eq!(event.sequence(), 1);
        assert_eq!(
            host.push_market(ticker_generation(&binding, 1, 11)?),
            Err(StrategyHostError::StaleMarketGeneration)
        );
        assert!(host.pop_next().is_none());
        Ok(())
    }

    #[test]
    fn continuous_market_events_are_lossless_while_tickers_coalesce() -> Result<(), Box<dyn Error>>
    {
        let binding = binding()?;
        let mut host = StrategyActorHost::new(binding.clone());
        host.push_market(ticker(&binding, 10)?)?;
        host.push_market(bar(&binding, 1)?)?;
        host.push_market(ticker(&binding, 11)?)?;
        host.push_market(bar(&binding, 2)?)?;

        assert_eq!(host.pending_lossless_market(), 2);
        assert_eq!(host.pending_market_kinds(), 1);
        let first = pop_market(&mut host)?;
        let second = pop_market(&mut host)?;
        let third = pop_market(&mut host)?;
        assert_eq!((first.kind(), first.sequence()), (MarketEventKind::Bar, 1));
        assert_eq!(
            (second.kind(), second.sequence()),
            (MarketEventKind::Ticker, 11)
        );
        assert_eq!((third.kind(), third.sequence()), (MarketEventKind::Bar, 2));
        assert!(host.pop_next().is_none());
        Ok(())
    }

    #[test]
    fn lossless_mailboxes_fail_explicitly_at_capacity() -> Result<(), Box<dyn Error>> {
        let binding = binding()?;
        let fact = private_fact(&binding)?;
        let mut private_host = StrategyActorHost::new(binding.clone());
        for _ in 0..MAX_PRIVATE_MAILBOX_DEPTH {
            private_host.push_private(fact.clone())?;
        }
        assert_eq!(
            private_host.push_private(fact),
            Err(StrategyHostError::PrivateMailboxFull)
        );

        let mut market_host = StrategyActorHost::new(binding.clone());
        for sequence in 1..=MAX_LOSSLESS_MARKET_MAILBOX_DEPTH as u64 {
            market_host.push_market(bar(&binding, sequence)?)?;
        }
        assert_eq!(
            market_host.push_market(bar(&binding, MAX_LOSSLESS_MARKET_MAILBOX_DEPTH as u64 + 1)?),
            Err(StrategyHostError::LosslessMarketMailboxFull)
        );
        assert_eq!(
            market_host.pending_lossless_market(),
            MAX_LOSSLESS_MARKET_MAILBOX_DEPTH
        );
        Ok(())
    }

    #[test]
    fn private_burst_yields_to_reconciliation_and_control() -> Result<(), Box<dyn Error>> {
        let binding = binding()?;
        let fact = private_fact(&binding)?;
        let mut host = StrategyActorHost::new(binding);
        for _ in 0..(PRIVATE_INPUT_BURST_LIMIT * 2 + 1) {
            host.push_private(fact.clone())?;
        }
        host.push_reconciliation(ReconciliationNotice {
            private_generation: 2,
            desired_open_orders: 0,
            actual_open_orders: 0,
            missing_client_order_ids: Vec::new(),
            unexpected_client_order_ids: Vec::new(),
            mismatched_client_order_ids: Vec::new(),
        })?;
        host.push_control(StrategyControl::Pause)?;

        for _ in 0..PRIVATE_INPUT_BURST_LIMIT {
            assert!(matches!(host.pop_next(), Some(StrategyInput::Private(_))));
        }
        assert!(matches!(
            host.pop_next(),
            Some(StrategyInput::Reconciliation(_))
        ));
        for _ in 0..PRIVATE_INPUT_BURST_LIMIT {
            assert!(matches!(host.pop_next(), Some(StrategyInput::Private(_))));
        }
        assert_eq!(
            host.pop_next(),
            Some(StrategyInput::Control(StrategyControl::Pause))
        );
        assert!(matches!(host.pop_next(), Some(StrategyInput::Private(_))));
        Ok(())
    }
}
