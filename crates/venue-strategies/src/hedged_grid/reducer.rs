use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

use venue_domain::domain::{FieldState, OrderSide, Price};

use super::{
    GridAction, GridDecision, GridEpoch, GridInventory, GridOrderIntent, GridOrderKey,
    GridOrderRole, GridPhase, GridPosition, GridReplenishment, GridResetReason, GridTransaction,
    HedgedGridBinding, HedgedGridError, HedgedGridParams, InventoryDeficiency,
    InventoryRecoveryState, OwnedGridFill, OwnedGridFillRecord,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HedgedGridState {
    pub schema_version: u16,
    pub binding: HedgedGridBinding,
    pub params: HedgedGridParams,
    pub phase: GridPhase,
    pub epoch: Option<GridEpoch>,
    pub inventory: Option<GridInventory>,
    #[serde(with = "grid_order_map")]
    pub owned_orders: BTreeMap<GridOrderKey, GridOrderIntent>,
    pub pending_transactions: BTreeMap<String, GridTransaction>,
    #[serde(with = "grid_replenishment_map")]
    pub pending_replenishments: BTreeMap<GridPosition, GridReplenishment>,
    pub seen_fill_ids: BTreeMap<String, GridOrderKey>,
    /// Complete owned executions, including taker and unresolved maker evidence. Unlike
    /// `seen_fill_ids`, these records need not have emitted a grid action.
    #[serde(default)]
    pub owned_fill_records: BTreeMap<String, OwnedGridFillRecord>,
    #[serde(default)]
    pub inventory_recovery: InventoryRecoveryState,
    pub reset_reason: Option<GridResetReason>,
    replenish_round: u64,
    #[serde(default)]
    pub suppress_replenishment_until_inventory_recovers: bool,
    /// A rejected/indeterminate physical batch must remain fenced long enough for late exchange
    /// results to settle before the runtime replaces the whole grid. The timestamp is supplied by
    /// the runtime's synchronized Binance clock and is durable across a process restart.
    #[serde(default)]
    blocked_reconciliation_not_before_ms: Option<u64>,
    #[serde(default)]
    order_sequences: GridOrderSequences,
    #[serde(default)]
    stream_inventory_adjustments: StreamInventoryAdjustments,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct GridOrderSequences {
    epoch: u64,
    long_open: u64,
    long_close: u64,
    short_open: u64,
    short_close: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct StreamInventoryAdjustments {
    #[serde(with = "rust_decimal::serde::str")]
    long: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    short: Decimal,
}

impl GridOrderSequences {
    fn current(&self, position: GridPosition, role: GridOrderRole) -> u64 {
        match (position, role) {
            (GridPosition::Long, GridOrderRole::Open) => self.long_open,
            (GridPosition::Long, GridOrderRole::Close) => self.long_close,
            (GridPosition::Short, GridOrderRole::Open) => self.short_open,
            (GridPosition::Short, GridOrderRole::Close) => self.short_close,
        }
    }

    fn observe(&mut self, key: &GridOrderKey) {
        if self.epoch != key.epoch {
            *self = Self {
                epoch: key.epoch,
                ..Self::default()
            };
        }
        let slot = match (key.position, key.role) {
            (GridPosition::Long, GridOrderRole::Open) => &mut self.long_open,
            (GridPosition::Long, GridOrderRole::Close) => &mut self.long_close,
            (GridPosition::Short, GridOrderRole::Open) => &mut self.short_open,
            (GridPosition::Short, GridOrderRole::Close) => &mut self.short_close,
        };
        *slot = (*slot).max(key.level);
    }
}

// JSON object keys must be strings. These are strategy projections, so retain the typed map in
// memory and persist its entries as an ordered sequence instead of inventing lossy key strings.
mod grid_order_map {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    use super::{GridOrderIntent, GridOrderKey};

    pub fn serialize<S>(
        map: &BTreeMap<GridOrderKey, GridOrderIntent>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<GridOrderKey, GridOrderIntent>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Stored {
            Entries(Vec<(GridOrderKey, GridOrderIntent)>),
            LegacyObject(BTreeMap<String, GridOrderIntent>),
        }

        let entries = match Stored::deserialize(deserializer)? {
            Stored::Entries(entries) => entries,
            // Version 1 briefly wrote an empty JSON object before this map had a stable
            // sequence representation. A non-empty object cannot be reconstructed safely.
            Stored::LegacyObject(entries) if entries.is_empty() => Vec::new(),
            Stored::LegacyObject(_) => {
                return Err(D::Error::custom(
                    "non-empty legacy hedged-grid order map cannot be recovered",
                ));
            }
        };
        let mut map = BTreeMap::new();
        for (key, value) in entries {
            if map.insert(key, value).is_some() {
                return Err(D::Error::custom("duplicate hedged-grid order key"));
            }
        }
        Ok(map)
    }
}

mod grid_replenishment_map {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    use super::{GridPosition, GridReplenishment};

    pub fn serialize<S>(
        map: &BTreeMap<GridPosition, GridReplenishment>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<GridPosition, GridReplenishment>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Stored {
            Entries(Vec<(GridPosition, GridReplenishment)>),
            LegacyObject(BTreeMap<String, GridReplenishment>),
        }

        let entries = match Stored::deserialize(deserializer)? {
            Stored::Entries(entries) => entries,
            Stored::LegacyObject(entries) if entries.is_empty() => Vec::new(),
            Stored::LegacyObject(_) => {
                return Err(D::Error::custom(
                    "non-empty legacy hedged-grid replenishment map cannot be recovered",
                ));
            }
        };
        let mut map = BTreeMap::new();
        for (key, value) in entries {
            if map.insert(key, value).is_some() {
                return Err(D::Error::custom(
                    "duplicate hedged-grid replenishment position",
                ));
            }
        }
        Ok(map)
    }
}

impl HedgedGridState {
    pub fn new_with_params(
        binding: HedgedGridBinding,
        params: HedgedGridParams,
    ) -> Result<Self, HedgedGridError> {
        binding.validate()?;
        params.validate()?;
        if params.order_notional.asset.as_str() != binding.symbol.quote() {
            return Err(HedgedGridError::Params);
        }
        Ok(Self {
            schema_version: super::HEDGED_GRID_SCHEMA_VERSION,
            binding,
            params,
            phase: GridPhase::Recovering,
            epoch: None,
            inventory: None,
            owned_orders: BTreeMap::new(),
            pending_transactions: BTreeMap::new(),
            pending_replenishments: BTreeMap::new(),
            seen_fill_ids: BTreeMap::new(),
            owned_fill_records: BTreeMap::new(),
            inventory_recovery: InventoryRecoveryState::Inactive,
            reset_reason: None,
            replenish_round: 0,
            suppress_replenishment_until_inventory_recovers: false,
            blocked_reconciliation_not_before_ms: None,
            order_sequences: GridOrderSequences::default(),
            stream_inventory_adjustments: StreamInventoryAdjustments::default(),
        })
    }

    /// Upgrades a deserialized checkpoint before it is admitted by a runtime. Schema 1 had no
    /// recovery sub-state. Schema 2 could persist a late maker fact after a fully drained Stop but
    /// before emitting its action; only that debt-free terminal shape is retired as a tombstone.
    pub fn migrate_checkpoint(&mut self) -> Result<(), HedgedGridError> {
        match self.schema_version {
            super::HEDGED_GRID_SCHEMA_VERSION => {
                // Early schema-3 residents could stop after persisting a proven maker fill but
                // before retiring its now-suppressed grid action. Repair only the same fully
                // drained Stop shape accepted by the schema-2 migration.
                self.retire_drained_stop_maker_facts()?;
                self.retire_superseded_maker_facts()?;
                self.validate_checkpoint_structure()
            }
            2 => self.migrate_schema_two_checkpoint(),
            1 => self.migrate_schema_one_checkpoint(),
            _ => Err(HedgedGridError::Checkpoint),
        }
    }

    fn migrate_schema_one_checkpoint(&mut self) -> Result<(), HedgedGridError> {
        if self.inventory_recovery != InventoryRecoveryState::Inactive
            || !self.owned_fill_records.is_empty()
        {
            return Err(HedgedGridError::Checkpoint);
        }
        if self.phase == GridPhase::Running
            && let (Some(epoch), Some(inventory)) = (&self.epoch, &self.inventory)
        {
            let legs = Self::capacity_deficiency(epoch, inventory, self.params.grid_count)?;
            if legs.any() {
                self.inventory_recovery = InventoryRecoveryState::Deficient {
                    legs,
                    first_seen_generation: inventory.private_generation,
                };
            }
        }
        self.schema_version = super::HEDGED_GRID_SCHEMA_VERSION;
        self.reconcile_order_sequences();
        self.validate_checkpoint_structure()
    }

    fn migrate_schema_two_checkpoint(&mut self) -> Result<(), HedgedGridError> {
        if self
            .owned_fill_records
            .values()
            .any(|record| record.retired_without_action)
        {
            return Err(HedgedGridError::Checkpoint);
        }
        self.retire_drained_stop_maker_facts()?;
        self.schema_version = super::HEDGED_GRID_SCHEMA_VERSION;
        self.validate_checkpoint_structure()
    }

    fn retire_drained_stop_maker_facts(&mut self) -> Result<(), HedgedGridError> {
        let retire = self
            .owned_fill_records
            .iter()
            .filter_map(|(fill_id, record)| {
                (record.maker == Some(true)
                    && !record.grid_action_emitted
                    && !record.retired_without_action)
                    .then_some((fill_id.clone(), record.source_order.key.clone()))
            })
            .collect::<Vec<_>>();
        if !retire.is_empty() {
            if self.phase != GridPhase::Stopping
                || !self.owned_orders.is_empty()
                || !self.pending_transactions.is_empty()
                || !self.pending_replenishments.is_empty()
            {
                return Err(HedgedGridError::Checkpoint);
            }
            if retire
                .iter()
                .any(|(fill_id, _)| self.seen_fill_ids.contains_key(fill_id))
            {
                return Err(HedgedGridError::Checkpoint);
            }
            for (fill_id, source) in &retire {
                self.seen_fill_ids.insert(fill_id.clone(), source.clone());
                self.owned_fill_records
                    .get_mut(fill_id)
                    .ok_or(HedgedGridError::Checkpoint)?
                    .retired_without_action = true;
            }
        }
        Ok(())
    }

    fn retire_superseded_maker_facts(&mut self) -> Result<(), HedgedGridError> {
        let Some(current_epoch) = self.epoch.as_ref().map(|epoch| epoch.epoch) else {
            return Ok(());
        };
        if !self.pending_transactions.is_empty() {
            return Ok(());
        }
        let active_recovery_fill = match &self.inventory_recovery {
            InventoryRecoveryState::ReanchorPending { fill_id, .. }
            | InventoryRecoveryState::Rebuilding { fill_id, .. } => Some(fill_id.as_str()),
            InventoryRecoveryState::Inactive
            | InventoryRecoveryState::Deficient { .. }
            | InventoryRecoveryState::AwaitingNextOwnedFill { .. } => None,
        };
        let retire = self
            .owned_fill_records
            .iter()
            .filter(|(fill_id, record)| {
                record.source_order.key.epoch < current_epoch
                    && record.maker == Some(true)
                    && !record.grid_action_emitted
                    && !record.retired_without_action
                    && active_recovery_fill != Some(fill_id.as_str())
            })
            .map(|(fill_id, record)| (fill_id.clone(), record.source_order.key.clone()))
            .collect::<Vec<_>>();
        if retire
            .iter()
            .any(|(fill_id, _)| self.seen_fill_ids.contains_key(fill_id))
        {
            return Err(HedgedGridError::Checkpoint);
        }
        for (fill_id, source) in retire {
            self.seen_fill_ids.insert(fill_id.clone(), source);
            self.owned_fill_records
                .get_mut(&fill_id)
                .ok_or(HedgedGridError::Checkpoint)?
                .retired_without_action = true;
        }
        Ok(())
    }

    fn validate_checkpoint_structure(&self) -> Result<(), HedgedGridError> {
        self.binding
            .validate()
            .map_err(|_| HedgedGridError::Checkpoint)?;
        self.params
            .validate()
            .map_err(|_| HedgedGridError::Checkpoint)?;
        if self.params.order_notional.asset.as_str() != self.binding.symbol.quote() {
            return Err(HedgedGridError::Checkpoint);
        }
        if let Some(epoch) = &self.epoch {
            epoch
                .validate(self.params.grid_count)
                .map_err(|_| HedgedGridError::Checkpoint)?;
        }
        if let Some(inventory) = &self.inventory {
            inventory
                .validate()
                .map_err(|_| HedgedGridError::Checkpoint)?;
        }
        if self.phase == GridPhase::Running && (self.epoch.is_none() || self.inventory.is_none()) {
            return Err(HedgedGridError::Checkpoint);
        }
        if self.epoch.is_none()
            && ((!self.owned_orders.is_empty() && self.phase != GridPhase::Stopping)
                || !self.pending_transactions.is_empty()
                || !self.seen_fill_ids.is_empty()
                || !self.owned_fill_records.is_empty()
                || self.inventory_recovery != InventoryRecoveryState::Inactive)
        {
            return Err(HedgedGridError::Checkpoint);
        }

        let current_epoch = self.epoch.as_ref().map(|epoch| epoch.epoch);
        for (key, order) in &self.owned_orders {
            if key != &order.key
                || current_epoch.is_some_and(|epoch| key.epoch != epoch)
                || (current_epoch.is_none() && self.phase != GridPhase::Stopping)
            {
                return Err(HedgedGridError::Checkpoint);
            }
            order.validate().map_err(|_| HedgedGridError::Checkpoint)?;
        }
        for (fill_id, source) in &self.seen_fill_ids {
            if fill_id.is_empty() || current_epoch.is_none_or(|epoch| source.epoch > epoch) {
                return Err(HedgedGridError::Checkpoint);
            }
            source.validate().map_err(|_| HedgedGridError::Checkpoint)?;
        }
        for (fill_id, record) in &self.owned_fill_records {
            if fill_id.is_empty()
                || current_epoch.is_none_or(|epoch| record.source_order.key.epoch > epoch)
            {
                return Err(HedgedGridError::Checkpoint);
            }
            record
                .source_order
                .validate()
                .map_err(|_| HedgedGridError::Checkpoint)?;
            let seen_source = self.seen_fill_ids.get(fill_id);
            if record.retired_without_action {
                if record.maker != Some(true)
                    || record.grid_action_emitted
                    || seen_source != Some(&record.source_order.key)
                {
                    return Err(HedgedGridError::Checkpoint);
                }
            } else if record.grid_action_emitted {
                if record.maker != Some(true) {
                    return Err(HedgedGridError::Checkpoint);
                }
                if seen_source != Some(&record.source_order.key) {
                    return Err(HedgedGridError::Checkpoint);
                }
            } else if record.maker == Some(true) || seen_source.is_some() {
                return Err(HedgedGridError::Checkpoint);
            }
        }

        self.validate_recovery_checkpoint()?;
        self.validate_sequence_checkpoint()?;
        Ok(())
    }

    fn validate_recovery_checkpoint(&self) -> Result<(), HedgedGridError> {
        let epoch = self.epoch.as_ref();
        let inventory = self.inventory.as_ref();
        match &self.inventory_recovery {
            InventoryRecoveryState::Inactive => Ok(()),
            InventoryRecoveryState::Deficient {
                legs,
                first_seen_generation,
            } => {
                let (Some(epoch), Some(inventory)) = (epoch, inventory) else {
                    return Err(HedgedGridError::Checkpoint);
                };
                if !legs.any()
                    || *first_seen_generation == 0
                    || *first_seen_generation > inventory.private_generation
                {
                    return Err(HedgedGridError::Checkpoint);
                }
                if self.phase == GridPhase::Running {
                    let current =
                        Self::capacity_deficiency(epoch, inventory, self.params.grid_count)
                            .map_err(|_| HedgedGridError::Checkpoint)?;
                    if &current != legs {
                        return Err(HedgedGridError::Checkpoint);
                    }
                }
                Ok(())
            }
            InventoryRecoveryState::AwaitingNextOwnedFill { armed_generation } => {
                let (Some(epoch), Some(inventory)) = (epoch, inventory) else {
                    return Err(HedgedGridError::Checkpoint);
                };
                if *armed_generation == 0 || *armed_generation > inventory.private_generation {
                    return Err(HedgedGridError::Checkpoint);
                }
                if self.phase == GridPhase::Running
                    && Self::capacity_deficiency(epoch, inventory, self.params.grid_count)
                        .map_err(|_| HedgedGridError::Checkpoint)?
                        .any()
                {
                    return Err(HedgedGridError::Checkpoint);
                }
                Ok(())
            }
            InventoryRecoveryState::ReanchorPending {
                fill_id,
                fill_price,
            } => {
                let (Some(epoch), Some(inventory)) = (epoch, inventory) else {
                    return Err(HedgedGridError::Checkpoint);
                };
                if !matches!(self.phase, GridPhase::Running | GridPhase::Stopping)
                    || !self.pending_transactions.is_empty()
                {
                    return Err(HedgedGridError::Checkpoint);
                }
                let record = self.validate_reanchor_record(fill_id, fill_price)?;
                if record.source_order.key.epoch != epoch.epoch
                    || record.private_generation > inventory.private_generation
                {
                    return Err(HedgedGridError::Checkpoint);
                }
                Ok(())
            }
            InventoryRecoveryState::Rebuilding {
                fill_id,
                fill_price,
            } => {
                let Some(epoch) = epoch else {
                    return Err(HedgedGridError::Checkpoint);
                };
                if inventory.is_none()
                    || !matches!(
                        self.phase,
                        GridPhase::ResettingGrid | GridPhase::Running | GridPhase::Stopping
                    )
                {
                    return Err(HedgedGridError::Checkpoint);
                }
                let record = self.validate_reanchor_record(fill_id, fill_price)?;
                let accepted_anchor = epoch.anchor_price == *fill_price
                    || epoch
                        .passive_book_fallback
                        .as_ref()
                        .is_some_and(|fallback| {
                            fallback.matches_fill(fill_id, *fill_price)
                                && fallback.anchor_price == epoch.anchor_price
                        });
                if record.source_order.key.epoch > epoch.epoch
                    || (self.phase == GridPhase::Running && !accepted_anchor)
                    || (self.phase == GridPhase::ResettingGrid
                        && self.reset_reason != Some(GridResetReason::InventoryReplenished))
                {
                    return Err(HedgedGridError::Checkpoint);
                }
                Ok(())
            }
        }
    }

    fn validate_reanchor_record(
        &self,
        fill_id: &str,
        fill_price: &Price,
    ) -> Result<&OwnedGridFillRecord, HedgedGridError> {
        let record = self
            .owned_fill_records
            .get(fill_id)
            .ok_or(HedgedGridError::Checkpoint)?;
        if fill_id.is_empty()
            || record.private_generation == 0
            || record.fill_price != *fill_price
            || record.maker != Some(true)
            || !record.grid_action_emitted
            || self.seen_fill_ids.get(fill_id) != Some(&record.source_order.key)
        {
            return Err(HedgedGridError::Checkpoint);
        }
        Ok(record)
    }

    fn validate_sequence_checkpoint(&self) -> Result<(), HedgedGridError> {
        let Some(epoch) = self.epoch.as_ref().map(|epoch| epoch.epoch) else {
            return if self.order_sequences == GridOrderSequences::default() {
                Ok(())
            } else {
                Err(HedgedGridError::Checkpoint)
            };
        };
        if self.order_sequences.epoch != epoch {
            return Err(HedgedGridError::Checkpoint);
        }
        let mut minimum = GridOrderSequences {
            epoch,
            ..GridOrderSequences::default()
        };
        for key in self
            .owned_orders
            .keys()
            .chain(self.seen_fill_ids.values())
            .filter(|key| key.epoch == epoch)
        {
            minimum.observe(key);
        }
        for transaction in self
            .pending_transactions
            .values()
            .filter(|transaction| transaction.source_order.epoch == epoch)
        {
            minimum.observe(&transaction.source_order);
            minimum.observe(&transaction.cancel);
            for order in &transaction.places {
                minimum.observe(&order.key);
            }
        }
        for position in [GridPosition::Long, GridPosition::Short] {
            for role in [GridOrderRole::Open, GridOrderRole::Close] {
                if self.order_sequences.current(position, role) < minimum.current(position, role) {
                    return Err(HedgedGridError::Checkpoint);
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn new(binding: HedgedGridBinding) -> Result<Self, HedgedGridError> {
        Self::new_with_params(binding, HedgedGridParams::phase_one(10)?)
    }

    pub fn begin_inventory_check(&mut self) -> Result<(), HedgedGridError> {
        if matches!(self.phase, GridPhase::BlockedUnknown | GridPhase::Stopping) {
            return Err(HedgedGridError::Phase);
        }
        self.phase = GridPhase::CheckingInventory;
        Ok(())
    }

    /// A symbol-scoped operator reset freezes only this grid's owned orders. Runtime must settle
    /// those cancellations and obtain a new private inventory snapshot before installing an epoch.
    pub fn request_reset(
        &mut self,
        reason: GridResetReason,
    ) -> Result<GridDecision, HedgedGridError> {
        if matches!(self.phase, GridPhase::BlockedUnknown | GridPhase::Stopping) {
            return Err(HedgedGridError::Phase);
        }
        self.phase = GridPhase::ResettingGrid;
        self.reset_reason = Some(reason);
        self.blocked_reconciliation_not_before_ms = None;
        Ok(GridDecision::Actions(vec![GridAction::Reset { reason }]))
    }

    /// Runtime calls this after durably checkpointing `ReanchorPending` and obtaining the unique
    /// writer. The trigger identity/price remain unchanged throughout cancellation and rebuild.
    pub fn begin_reanchor_rebuild(&mut self) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::Running || !self.pending_transactions.is_empty() {
            return Err(HedgedGridError::Phase);
        }
        let InventoryRecoveryState::ReanchorPending {
            fill_id,
            fill_price,
        } = self.inventory_recovery.clone()
        else {
            return Err(HedgedGridError::Phase);
        };
        self.inventory_recovery = InventoryRecoveryState::Rebuilding {
            fill_id,
            fill_price,
        };
        self.phase = GridPhase::ResettingGrid;
        self.reset_reason = Some(GridResetReason::InventoryReplenished);
        self.blocked_reconciliation_not_before_ms = None;
        Ok(())
    }

    /// Completes recovery only after the newly installed epoch and a signed inventory fact prove
    /// both legs can still cover every configured closing level.
    pub fn complete_reanchor_rebuild(&mut self) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::Running
            || !matches!(
                self.inventory_recovery,
                InventoryRecoveryState::Rebuilding { .. }
            )
        {
            return Err(HedgedGridError::Phase);
        }
        let epoch = self.epoch.as_ref().ok_or(HedgedGridError::Epoch)?;
        let inventory = self.inventory.clone().ok_or(HedgedGridError::Inventory)?;
        let legs = Self::capacity_deficiency(epoch, &inventory, self.params.grid_count)?;
        if legs.any() {
            self.inventory_recovery = InventoryRecoveryState::Deficient {
                legs,
                first_seen_generation: inventory.private_generation,
            };
            return Err(HedgedGridError::Inventory);
        }
        self.inventory_recovery = InventoryRecoveryState::Inactive;
        Ok(())
    }

    /// Records a complete private observation. Low inventory starts a controlled reset; the
    /// runtime must settle owned orders first, then invoke `begin_replenishment`.
    pub fn observe_inventory(
        &mut self,
        inventory: GridInventory,
    ) -> Result<GridDecision, HedgedGridError> {
        inventory.validate()?;
        if let Some(previous) = &self.inventory {
            if inventory.private_generation < previous.private_generation
                || (inventory.private_generation == previous.private_generation
                    && inventory.private_observed_at_ms < previous.private_observed_at_ms)
            {
                return Err(HedgedGridError::InventoryGeneration);
            }
            if inventory.private_generation == previous.private_generation
                && inventory.private_observed_at_ms == previous.private_observed_at_ms
                && inventory != *previous
            {
                return Err(HedgedGridError::InventoryGeneration);
            }
            if inventory.private_generation == previous.private_generation
                && inventory.private_observed_at_ms == previous.private_observed_at_ms
            {
                return Ok(GridDecision::Noop);
            }
        }
        self.inventory = Some(inventory.clone());
        self.stream_inventory_adjustments = StreamInventoryAdjustments::default();
        self.update_inventory_recovery_from_authoritative(&inventory)?;

        let inventory_low = self.is_low(&inventory, GridPosition::Long)
            || self.is_low(&inventory, GridPosition::Short);
        if !inventory_low {
            self.suppress_replenishment_until_inventory_recovers = false;
        }
        if inventory_low && !self.suppress_replenishment_until_inventory_recovers {
            self.phase = GridPhase::ResettingGrid;
            self.reset_reason = Some(GridResetReason::InventoryLow);
            return Ok(GridDecision::Actions(vec![GridAction::Reset {
                reason: GridResetReason::InventoryLow,
            }]));
        }
        if matches!(
            self.phase,
            GridPhase::Recovering | GridPhase::CheckingInventory
        ) {
            self.phase = GridPhase::ResettingGrid;
            self.reset_reason = Some(GridResetReason::Startup);
            return Ok(GridDecision::Actions(vec![GridAction::Reset {
                reason: GridResetReason::Startup,
            }]));
        }
        Ok(GridDecision::Noop)
    }

    /// Must be called only after reset orchestration has cancelled/settled this instance's owned
    /// grid orders. Each low Hedge leg gets exactly one 15-quote-asset semantic market
    /// replenishment.
    pub fn reconcile_replenishment_round(
        &mut self,
        highest_durable_round: u64,
    ) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::ResettingGrid
            || self.reset_reason != Some(GridResetReason::InventoryLow)
            || !self.pending_replenishments.is_empty()
        {
            return Err(HedgedGridError::Phase);
        }
        self.replenish_round = self.replenish_round.max(highest_durable_round);
        Ok(())
    }

    pub fn begin_replenishment(&mut self) -> Result<GridDecision, HedgedGridError> {
        if self.phase != GridPhase::ResettingGrid
            || self.reset_reason != Some(GridResetReason::InventoryLow)
        {
            return Err(HedgedGridError::Phase);
        }
        let inventory = self.inventory.clone().ok_or(HedgedGridError::Inventory)?;
        self.replenish_round = self
            .replenish_round
            .checked_add(1)
            .ok_or(HedgedGridError::Phase)?;
        self.pending_replenishments.clear();
        let mut actions = Vec::new();
        for position in [GridPosition::Long, GridPosition::Short] {
            if self.is_low(&inventory, position) {
                let replenishment = GridReplenishment {
                    round: self.replenish_round,
                    private_generation: inventory.private_generation,
                    position,
                    target_notional: self.params.replenish_notional(),
                };
                self.pending_replenishments
                    .insert(position, replenishment.clone());
                actions.push(GridAction::Replenish(replenishment));
            }
        }
        if actions.is_empty() {
            return Err(HedgedGridError::Phase);
        }
        self.phase = GridPhase::ReplenishingInventory;
        Ok(GridDecision::Actions(actions))
    }

    /// Operator-directed recovery mode: rebuild from the current Hedge inventory without a market
    /// top-up. Normal replenishment is restored automatically after both legs recover to one grid.
    pub fn request_restart_without_replenishment(&mut self) -> Result<(), HedgedGridError> {
        if matches!(self.phase, GridPhase::BlockedUnknown | GridPhase::Stopping)
            || !self.pending_transactions.is_empty()
        {
            return Err(HedgedGridError::Phase);
        }
        self.pending_replenishments.clear();
        self.phase = GridPhase::ResettingGrid;
        self.reset_reason = Some(GridResetReason::Manual);
        self.blocked_reconciliation_not_before_ms = None;
        self.suppress_replenishment_until_inventory_recovers = true;
        Ok(())
    }

    /// Reopens an instance only after its symbol-scoped stop has durably removed every owned
    /// order. Any unfinished replenishment intent is discarded; a fresh private observation
    /// decides whether the restarted instance must replenish again.
    pub fn resume_after_stop(&mut self) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::Stopping
            || !self.owned_orders.is_empty()
            || !self.pending_transactions.is_empty()
        {
            return Err(HedgedGridError::Phase);
        }
        self.pending_replenishments.clear();
        // A settled stop removes the old grid surface. Restart installs a fresh midpoint epoch,
        // so an inventory-recovery latch tied to the retired epoch must not cross that boundary.
        // The first authoritative inventory after resume arms a new episode if a leg is deficient.
        self.inventory_recovery = InventoryRecoveryState::Inactive;
        self.phase = GridPhase::Recovering;
        self.reset_reason = None;
        self.blocked_reconciliation_not_before_ms = None;
        self.stream_inventory_adjustments = StreamInventoryAdjustments::default();
        Ok(())
    }

    /// Runtime calls this only after every currently owned ordinary grid order has an exact
    /// cancellation/readback settlement. It deliberately leaves hedge inventory untouched:
    /// reset means rebuild the grid, not flatten the account.
    pub fn reset_orders_settled(&mut self) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::ResettingGrid || !self.pending_transactions.is_empty() {
            return Err(HedgedGridError::Phase);
        }
        self.owned_orders.clear();
        Ok(())
    }

    /// A successful market order alone is not enough: `replenishment_settled` waits for a newer
    /// private inventory generation before allowing a new epoch.
    pub fn replenishment_settled(
        &mut self,
        position: GridPosition,
    ) -> Result<GridDecision, HedgedGridError> {
        if self.phase != GridPhase::ReplenishingInventory
            || self.pending_replenishments.remove(&position).is_none()
        {
            return Err(HedgedGridError::Phase);
        }
        if !self.pending_replenishments.is_empty() {
            return Ok(GridDecision::Noop);
        }
        self.phase = GridPhase::ResettingGrid;
        self.reset_reason = Some(GridResetReason::InventoryReplenished);
        Ok(GridDecision::Actions(vec![GridAction::Reset {
            reason: GridResetReason::InventoryReplenished,
        }]))
    }

    /// Settles exactly the directions that were durably recorded as pending. A one-sided
    /// replenishment must not manufacture a settlement for the other Hedge leg.
    pub fn settle_pending_replenishments(&mut self) -> Result<GridDecision, HedgedGridError> {
        if self.phase != GridPhase::ReplenishingInventory || self.pending_replenishments.is_empty()
        {
            return Err(HedgedGridError::Phase);
        }
        let positions = self
            .pending_replenishments
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut decision = GridDecision::Noop;
        for position in positions {
            decision = self.replenishment_settled(position)?;
        }
        Ok(decision)
    }

    /// Updates the private inventory cursor while a durable market replenishment is pending.
    /// It intentionally does not re-run the ordinary low-inventory transition: a delayed venue
    /// snapshot must wait for the original market command rather than create another one.
    pub fn observe_replenishment_inventory(
        &mut self,
        inventory: GridInventory,
    ) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::ReplenishingInventory {
            return Err(HedgedGridError::Phase);
        }
        inventory.validate()?;
        if let Some(previous) = &self.inventory
            && (inventory.private_generation < previous.private_generation
                || (inventory.private_generation == previous.private_generation
                    && inventory.private_observed_at_ms < previous.private_observed_at_ms)
                || (inventory.private_generation == previous.private_generation
                    && inventory.private_observed_at_ms == previous.private_observed_at_ms
                    && inventory != *previous))
        {
            return Err(HedgedGridError::InventoryGeneration);
        }
        self.inventory = Some(inventory);
        self.stream_inventory_adjustments = StreamInventoryAdjustments::default();
        Ok(())
    }

    /// Installs exchange-normalized epoch values and yields the complete desired grid. Runtime
    /// persists/maps these semantic orders before it creates any native client-order identities.
    pub fn install_epoch(&mut self, epoch: GridEpoch) -> Result<GridDecision, HedgedGridError> {
        if self.phase != GridPhase::ResettingGrid || !self.pending_transactions.is_empty() {
            return Err(HedgedGridError::Phase);
        }
        epoch.validate(self.params.grid_count)?;
        if self
            .epoch
            .as_ref()
            .is_some_and(|previous| epoch.epoch <= previous.epoch)
        {
            return Err(HedgedGridError::Epoch);
        }
        let inventory = self.inventory.clone().ok_or(HedgedGridError::Inventory)?;
        if !self.suppress_replenishment_until_inventory_recovers
            && (self.is_low(&inventory, GridPosition::Long)
                || self.is_low(&inventory, GridPosition::Short))
        {
            return Err(HedgedGridError::Inventory);
        }

        let epoch_number = epoch.epoch;
        let orders = desired_orders(&epoch, &inventory, self.params.grid_count)?;
        self.epoch = Some(epoch);
        self.owned_orders = orders
            .iter()
            .cloned()
            .map(|order| (order.key.clone(), order))
            .collect();
        self.order_sequences = GridOrderSequences {
            epoch: epoch_number,
            ..GridOrderSequences::default()
        };
        for key in self.owned_orders.keys() {
            self.order_sequences.observe(key);
        }
        self.phase = GridPhase::Running;
        self.reset_reason = None;
        self.retire_superseded_maker_facts()?;
        self.update_inventory_recovery_from_authoritative(&inventory)?;
        Ok(GridDecision::Actions(
            orders.into_iter().map(GridAction::Place).collect(),
        ))
    }

    /// Legacy callers do not carry authoritative execution price and maker evidence. They must
    /// fail closed and migrate to `observe_owned_fill`; using the order limit as a fill price is
    /// explicitly forbidden.
    pub fn observe_full_fill(
        &mut self,
        _fill_id: String,
        _source: GridOrderKey,
        _complete: bool,
    ) -> Result<GridDecision, HedgedGridError> {
        Err(HedgedGridError::FillEvidence)
    }

    /// Accepts one complete owned execution from signed reconciliation. Taker and unresolved
    /// maker evidence consume/update the owned inventory fact but never emit grid actions.
    pub fn observe_owned_fill(
        &mut self,
        fill: OwnedGridFill,
    ) -> Result<GridDecision, HedgedGridError> {
        self.observe_owned_fill_inner(fill, false)
    }

    /// The raw private event must already be durable. Its quantity effect is projected once while
    /// the signed inventory readback catches up; replay with richer maker evidence cannot apply it
    /// twice.
    pub fn observe_stream_owned_fill(
        &mut self,
        fill: OwnedGridFill,
    ) -> Result<GridDecision, HedgedGridError> {
        self.observe_owned_fill_inner(fill, true)
    }

    /// Consumes a complete owned execution that races a signed startup cancellation. The old
    /// epoch is already frozen, so the fill retires its exact route without creating another
    /// rolling transaction; inventory deltas and immutable fill evidence remain durable.
    pub fn retire_owned_fill_during_reset(
        &mut self,
        fill: OwnedGridFill,
    ) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::ResettingGrid
            || !self.pending_transactions.is_empty()
            || !fill.complete
            || fill.fill_id.trim().is_empty()
            || fill.private_generation == 0
        {
            return Err(HedgedGridError::Phase);
        }
        let maker = match fill.maker {
            FieldState::Known(value) => Some(value),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => None,
        };
        if let Some(record) = self.owned_fill_records.get(&fill.fill_id) {
            if record.source_order.key != fill.source_order
                || record.fill_price != fill.fill_price
                || (record.maker.is_some() && maker.is_some() && record.maker != maker)
            {
                return Err(HedgedGridError::FillConflict);
            }
            return Ok(());
        }
        let epoch = self.epoch.as_ref().ok_or(HedgedGridError::Phase)?;
        if fill.source_order.epoch != epoch.epoch {
            return Err(HedgedGridError::UnknownFill);
        }
        let source_order = self
            .owned_orders
            .remove(&fill.source_order)
            .ok_or(HedgedGridError::UnknownFill)?;
        let adjustment = match source_order.key.position {
            GridPosition::Long => &mut self.stream_inventory_adjustments.long,
            GridPosition::Short => &mut self.stream_inventory_adjustments.short,
        };
        *adjustment = match source_order.key.role {
            GridOrderRole::Open => adjustment
                .checked_add(source_order.quantity)
                .ok_or(HedgedGridError::Rolling)?,
            GridOrderRole::Close => adjustment
                .checked_sub(source_order.quantity)
                .ok_or(HedgedGridError::Rolling)?,
        };
        let retired_without_action = maker == Some(true);
        self.owned_fill_records.insert(
            fill.fill_id.clone(),
            OwnedGridFillRecord {
                source_order: source_order.clone(),
                fill_price: fill.fill_price,
                private_generation: fill.private_generation,
                maker,
                grid_action_emitted: false,
                retired_without_action,
            },
        );
        if retired_without_action {
            self.seen_fill_ids
                .insert(fill.fill_id, source_order.key.clone());
        }
        self.reconcile_order_sequences();
        Ok(())
    }

    fn observe_owned_fill_inner(
        &mut self,
        fill: OwnedGridFill,
        adjust_stream_inventory: bool,
    ) -> Result<GridDecision, HedgedGridError> {
        if fill.fill_id.trim().is_empty() {
            return Err(HedgedGridError::FillEvidence);
        }
        if fill.private_generation == 0 {
            return Err(HedgedGridError::FillEvidence);
        }
        fill.source_order.validate()?;
        if !fill.complete {
            return Ok(GridDecision::Noop);
        }
        if let Some(previous) = self.seen_fill_ids.get(&fill.fill_id) {
            if previous != &fill.source_order
                || self
                    .owned_fill_records
                    .get(&fill.fill_id)
                    .is_some_and(|record| record.fill_price != fill.fill_price)
            {
                return Err(HedgedGridError::FillConflict);
            }
            return Ok(GridDecision::Noop);
        }
        let maker = match fill.maker {
            FieldState::Known(value) => Some(value),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => None,
        };

        if let Some(record) = self.owned_fill_records.get(&fill.fill_id) {
            if record.source_order.key != fill.source_order || record.fill_price != fill.fill_price
            {
                return Err(HedgedGridError::FillConflict);
            }
            if record.maker.is_some() && maker.is_some() && record.maker != maker {
                return Err(HedgedGridError::FillConflict);
            }
            if record.grid_action_emitted || maker != Some(true) {
                if record.maker.is_none()
                    && maker.is_some()
                    && let Some(record) = self.owned_fill_records.get_mut(&fill.fill_id)
                {
                    record.maker = maker;
                }
                return Ok(GridDecision::Noop);
            }
            return self.emit_maker_fill_action(&fill.fill_id);
        }

        if self.phase != GridPhase::Running {
            return Err(HedgedGridError::Phase);
        }
        let Some(epoch) = self.epoch.clone() else {
            return Err(HedgedGridError::Phase);
        };
        if fill.source_order.epoch != epoch.epoch {
            return Err(HedgedGridError::UnknownFill);
        }
        let Some(source_order) = self.owned_orders.remove(&fill.source_order) else {
            return Err(HedgedGridError::UnknownFill);
        };

        if adjust_stream_inventory {
            let adjustment = match source_order.key.position {
                GridPosition::Long => &mut self.stream_inventory_adjustments.long,
                GridPosition::Short => &mut self.stream_inventory_adjustments.short,
            };
            *adjustment = match source_order.key.role {
                GridOrderRole::Open => adjustment
                    .checked_add(source_order.quantity)
                    .ok_or(HedgedGridError::Rolling)?,
                GridOrderRole::Close => adjustment
                    .checked_sub(source_order.quantity)
                    .ok_or(HedgedGridError::Rolling)?,
            };
        }

        self.owned_fill_records.insert(
            fill.fill_id.clone(),
            OwnedGridFillRecord {
                source_order,
                fill_price: fill.fill_price,
                private_generation: fill.private_generation,
                maker,
                grid_action_emitted: false,
                retired_without_action: false,
            },
        );
        if maker == Some(true) {
            self.emit_maker_fill_action(&fill.fill_id)
        } else {
            Ok(GridDecision::Noop)
        }
    }

    fn emit_maker_fill_action(&mut self, fill_id: &str) -> Result<GridDecision, HedgedGridError> {
        if self.phase != GridPhase::Running {
            return Err(HedgedGridError::Phase);
        }
        let record = self
            .owned_fill_records
            .get(fill_id)
            .cloned()
            .ok_or(HedgedGridError::UnknownFill)?;
        if record.maker == Some(false) || record.grid_action_emitted {
            return Ok(GridDecision::Noop);
        }
        let source = record.source_order.clone();
        let reanchor_after_generation = matches!(
            self.inventory_recovery,
            InventoryRecoveryState::AwaitingNextOwnedFill { armed_generation }
                if record.private_generation > armed_generation
        );
        let decision = if reanchor_after_generation {
            self.inventory_recovery = InventoryRecoveryState::ReanchorPending {
                fill_id: fill_id.to_owned(),
                fill_price: record.fill_price,
            };
            GridDecision::Actions(vec![GridAction::ReanchorAtFill {
                fill_id: fill_id.to_owned(),
                fill_price: record.fill_price,
            }])
        } else {
            let epoch = self.epoch.clone().ok_or(HedgedGridError::Phase)?;
            let transaction = self.reserve_rolling_transaction(&epoch, fill_id, &source)?;
            for order in &transaction.places {
                self.order_sequences.observe(&order.key);
            }
            self.owned_orders.remove(&transaction.cancel);
            for order in &transaction.places {
                self.owned_orders.insert(order.key.clone(), order.clone());
            }
            self.pending_transactions
                .insert(transaction.id.clone(), transaction.clone());
            GridDecision::Actions(vec![GridAction::Dispatch(transaction)])
        };
        if let Some(record) = self.owned_fill_records.get_mut(fill_id) {
            record.maker = Some(true);
            record.grid_action_emitted = true;
        }
        self.seen_fill_ids
            .insert(fill_id.to_owned(), source.key.clone());
        Ok(decision)
    }

    pub fn settle_transaction(
        &mut self,
        transaction_id: &str,
        success: bool,
    ) -> Result<GridDecision, HedgedGridError> {
        if !self.pending_transactions.contains_key(transaction_id) {
            return Err(HedgedGridError::Rolling);
        }
        if !success {
            self.phase = GridPhase::BlockedUnknown;
            return Ok(GridDecision::Blocked);
        }
        self.pending_transactions.remove(transaction_id);
        Ok(GridDecision::Noop)
    }

    fn reserve_rolling_transaction(
        &self,
        epoch: &GridEpoch,
        fill_id: &str,
        source: &GridOrderIntent,
    ) -> Result<GridTransaction, HedgedGridError> {
        let position = source.key.position;
        let cancel_role = match source.key.role {
            GridOrderRole::Open => GridOrderRole::Close,
            GridOrderRole::Close => GridOrderRole::Open,
        };
        let cancel_candidates = self.owned_orders.iter().filter(|(key, _)| {
            key.epoch == epoch.epoch
                && key.position == position
                && key.role == cancel_role
                && !self
                    .pending_transactions
                    .values()
                    .any(|transaction| transaction.places.iter().any(|order| &order.key == *key))
        });
        let (cancel, cancelled_order) = match source.side {
            // A sell fill moves the active ladder upward, so retire the lowest trailing order.
            OrderSide::Sell => cancel_candidates
                .min_by_key(|(_, order)| order.price.value())
                .map(|(key, order)| (key.clone(), order.clone())),
            // A buy fill moves the active ladder downward, so retire the highest trailing order.
            OrderSide::Buy => cancel_candidates
                .max_by_key(|(_, order)| order.price.value())
                .map(|(key, order)| (key.clone(), order.clone())),
        }
        .ok_or(HedgedGridError::Rolling)?;
        let open = self.rolling_order(epoch, source, GridOrderRole::Open, &cancel)?;
        let close = self.rolling_order(epoch, source, GridOrderRole::Close, &cancel)?;
        let id = format!(
            "roll-{}-{}-{:?}-{:?}-{}",
            epoch.epoch, fill_id, position, source.key.role, source.key.level
        );
        Ok(GridTransaction {
            id,
            source_fill_id: fill_id.to_owned(),
            source_order: source.key.clone(),
            places: [open, close],
            cancel,
            cancelled_order: Some(cancelled_order),
        })
    }

    /// Releases only transactions that have been reserved locally but have not reached the
    /// exchange. The confirmed fill remains consumed; the still-live cancel targets are restored
    /// so the runtime can cancel the exact signed order set and rebuild at current precision.
    pub fn abandon_unsubmitted_transactions_for_reconciliation(
        &mut self,
        transaction_ids: &[String],
    ) -> Result<GridDecision, HedgedGridError> {
        if self.phase != GridPhase::Running || transaction_ids.is_empty() {
            return Err(HedgedGridError::Phase);
        }
        let expected = transaction_ids.iter().collect::<BTreeSet<_>>();
        if expected.len() != transaction_ids.len()
            || self.pending_transactions.len() != expected.len()
            || !self
                .pending_transactions
                .keys()
                .all(|id| expected.contains(id))
        {
            return Err(HedgedGridError::Rolling);
        }
        let transactions = self
            .pending_transactions
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for transaction in &transactions {
            let Some(cancelled_order) = transaction.cancelled_order.clone() else {
                // Checkpoints created before this recovery contract cannot prove the old order
                // intent, so they retain the existing fail-closed behavior.
                return Err(HedgedGridError::Rolling);
            };
            if cancelled_order.key != transaction.cancel {
                return Err(HedgedGridError::Order);
            }
            for replacement in &transaction.places {
                self.owned_orders.remove(&replacement.key);
            }
            self.owned_orders
                .insert(cancelled_order.key.clone(), cancelled_order);
        }
        self.pending_transactions.clear();
        self.reconcile_order_sequences();
        self.request_reset(GridResetReason::Reconciliation)
    }

    fn next_level(
        &self,
        epoch: u64,
        position: GridPosition,
        role: GridOrderRole,
    ) -> Result<u64, HedgedGridError> {
        if self.order_sequences.epoch != epoch {
            return Err(HedgedGridError::Rolling);
        }
        let highest = self.order_sequences.current(position, role);
        highest.checked_add(1).ok_or(HedgedGridError::Rolling)
    }

    pub fn reconcile_order_sequences(&mut self) {
        let Some(epoch) = self.epoch.as_ref().map(|epoch| epoch.epoch) else {
            self.order_sequences = GridOrderSequences::default();
            return;
        };
        let mut sequences = GridOrderSequences {
            epoch,
            ..GridOrderSequences::default()
        };
        for key in self
            .owned_orders
            .keys()
            .chain(self.seen_fill_ids.values())
            .filter(|key| key.epoch == epoch)
        {
            sequences.observe(key);
        }
        for transaction in self
            .pending_transactions
            .values()
            .filter(|transaction| transaction.source_order.epoch == epoch)
        {
            sequences.observe(&transaction.source_order);
            sequences.observe(&transaction.cancel);
            for order in &transaction.places {
                sequences.observe(&order.key);
            }
        }
        self.order_sequences = sequences;
    }

    pub fn begin_reconciliation_reset(
        &mut self,
        owned_orders: BTreeMap<GridOrderKey, GridOrderIntent>,
    ) -> Result<(), HedgedGridError> {
        if !self.pending_transactions.is_empty() {
            return Err(HedgedGridError::Rolling);
        }
        for (key, order) in &owned_orders {
            if key != &order.key {
                return Err(HedgedGridError::Order);
            }
            key.validate()?;
            order.validate()?;
        }
        self.owned_orders = owned_orders;
        self.phase = GridPhase::ResettingGrid;
        self.reset_reason = Some(GridResetReason::Reconciliation);
        self.blocked_reconciliation_not_before_ms = None;
        self.reconcile_order_sequences();
        Ok(())
    }

    pub fn block_for_order_reconciliation(&mut self) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::Running {
            return Err(HedgedGridError::Phase);
        }
        self.phase = GridPhase::BlockedUnknown;
        Ok(())
    }

    pub fn defer_blocked_reconciliation_until(
        &mut self,
        not_before_ms: u64,
    ) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::BlockedUnknown || not_before_ms == 0 {
            return Err(HedgedGridError::Phase);
        }
        self.blocked_reconciliation_not_before_ms = Some(not_before_ms);
        Ok(())
    }

    pub fn blocked_reconciliation_is_due(&self, now_ms: u64) -> bool {
        self.phase == GridPhase::BlockedUnknown
            && self
                .blocked_reconciliation_not_before_ms
                .is_none_or(|not_before_ms| now_ms >= not_before_ms)
    }

    pub fn blocked_reconciliation_not_before_ms(&self) -> Option<u64> {
        self.blocked_reconciliation_not_before_ms
    }

    pub fn reconcile_blocked_orders(
        &mut self,
        owned_orders: BTreeMap<GridOrderKey, GridOrderIntent>,
    ) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::BlockedUnknown {
            return Err(HedgedGridError::Phase);
        }
        for (key, order) in &owned_orders {
            if key != &order.key {
                return Err(HedgedGridError::Order);
            }
            key.validate()?;
            order.validate()?;
        }
        self.pending_transactions.clear();
        self.blocked_reconciliation_not_before_ms = None;
        if owned_orders == self.owned_orders {
            self.phase = GridPhase::Running;
            self.reset_reason = None;
        } else {
            self.owned_orders = owned_orders;
            self.phase = GridPhase::ResettingGrid;
            self.reset_reason = Some(GridResetReason::Reconciliation);
        }
        self.reconcile_order_sequences();
        Ok(())
    }

    /// Replaces an optimistic ladder with a complete signed open-order projection while an
    /// operator stop is active. Every supplied order has already been proved against this
    /// binding's accepted WAL by the runtime. Pending rolling transactions are local planning
    /// state and can be discarded because this transition may only cancel visible orders.
    pub fn reconcile_stopping_orders(
        &mut self,
        owned_orders: BTreeMap<GridOrderKey, GridOrderIntent>,
    ) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::Stopping {
            return Err(HedgedGridError::Phase);
        }
        for (key, order) in &owned_orders {
            if key != &order.key {
                return Err(HedgedGridError::Order);
            }
            key.validate()?;
            order.validate()?;
        }
        self.owned_orders = owned_orders;
        self.pending_transactions.clear();
        self.pending_replenishments.clear();
        self.blocked_reconciliation_not_before_ms = None;
        self.stream_inventory_adjustments = StreamInventoryAdjustments::default();
        self.reconcile_order_sequences();
        Ok(())
    }

    fn rolling_order(
        &self,
        epoch: &GridEpoch,
        source: &GridOrderIntent,
        role: GridOrderRole,
        cancel: &GridOrderKey,
    ) -> Result<GridOrderIntent, HedgedGridError> {
        let position = source.key.position;
        let level = self.next_level(epoch.epoch, position, role)?;
        let same_lane = role == source.key.role;
        let price_value = if same_lane {
            let prices = self
                .owned_orders
                .values()
                .filter(|order| {
                    order.key.epoch == epoch.epoch
                        && order.key.position == position
                        && order.key.role == role
                })
                .map(|order| order.price.value());
            match source.side {
                OrderSide::Sell => prices
                    .max()
                    .ok_or(HedgedGridError::Rolling)?
                    .checked_add(epoch.step.value())
                    .ok_or(HedgedGridError::Rolling)?,
                OrderSide::Buy => prices
                    .min()
                    .ok_or(HedgedGridError::Rolling)?
                    .checked_sub(epoch.step.value())
                    .ok_or(HedgedGridError::Rolling)?,
            }
        } else {
            match source.side {
                OrderSide::Sell => source
                    .price
                    .value()
                    .checked_sub(epoch.step.value())
                    .ok_or(HedgedGridError::Rolling)?,
                OrderSide::Buy => source
                    .price
                    .value()
                    .checked_add(epoch.step.value())
                    .ok_or(HedgedGridError::Rolling)?,
            }
        };
        let price = Price::new(price_value).map_err(|_| HedgedGridError::Rolling)?;
        if self.owned_orders.values().any(|order| {
            order.key.epoch == epoch.epoch
                && order.key.position == position
                && order.key.role == role
                && order.price == price
        }) {
            return Err(HedgedGridError::Rolling);
        }
        let quantity = if role == GridOrderRole::Close {
            let inventory = self.inventory.as_ref().ok_or(HedgedGridError::Inventory)?;
            let available = match position {
                GridPosition::Long => {
                    inventory.long_quantity + self.stream_inventory_adjustments.long
                }
                GridPosition::Short => {
                    inventory.short_quantity + self.stream_inventory_adjustments.short
                }
            };
            let committed = self
                .owned_orders
                .values()
                .filter(|order| {
                    order.key.epoch == epoch.epoch
                        && order.key.position == position
                        && order.key.role == GridOrderRole::Close
                        && order.key != *cancel
                })
                .map(|order| order.quantity)
                .sum::<Decimal>();
            epoch.grid_quantity.min(
                available
                    .checked_sub(committed)
                    .ok_or(HedgedGridError::Rolling)?,
            )
        } else {
            epoch.grid_quantity
        };
        if quantity <= Decimal::ZERO {
            return Err(HedgedGridError::Rolling);
        }
        order_at_price(epoch.epoch, position, role, level, price, quantity)
    }

    fn is_low(&self, inventory: &GridInventory, position: GridPosition) -> bool {
        inventory.notional(position) < self.params.order_notional.value
    }

    fn capacity_deficiency(
        epoch: &GridEpoch,
        inventory: &GridInventory,
        grid_count: u8,
    ) -> Result<InventoryDeficiency, HedgedGridError> {
        let required = epoch
            .grid_quantity
            .checked_mul(Decimal::from(grid_count))
            .ok_or(HedgedGridError::Inventory)?;
        Ok(InventoryDeficiency {
            long: inventory.long_quantity < required,
            short: inventory.short_quantity < required,
        })
    }

    fn update_inventory_recovery_from_authoritative(
        &mut self,
        inventory: &GridInventory,
    ) -> Result<(), HedgedGridError> {
        if self.phase != GridPhase::Running {
            return Ok(());
        }
        let Some(epoch) = self.epoch.as_ref() else {
            return Ok(());
        };
        let legs = Self::capacity_deficiency(epoch, inventory, self.params.grid_count)?;
        self.inventory_recovery = match &self.inventory_recovery {
            InventoryRecoveryState::Inactive if legs.any() => InventoryRecoveryState::Deficient {
                legs,
                first_seen_generation: inventory.private_generation,
            },
            InventoryRecoveryState::Inactive => InventoryRecoveryState::Inactive,
            InventoryRecoveryState::Deficient {
                first_seen_generation,
                ..
            } if legs.any() => InventoryRecoveryState::Deficient {
                legs,
                first_seen_generation: *first_seen_generation,
            },
            InventoryRecoveryState::Deficient { .. } => {
                InventoryRecoveryState::AwaitingNextOwnedFill {
                    armed_generation: inventory.private_generation,
                }
            }
            InventoryRecoveryState::AwaitingNextOwnedFill { .. } if legs.any() => {
                InventoryRecoveryState::Deficient {
                    legs,
                    first_seen_generation: inventory.private_generation,
                }
            }
            InventoryRecoveryState::AwaitingNextOwnedFill { armed_generation } => {
                InventoryRecoveryState::AwaitingNextOwnedFill {
                    armed_generation: *armed_generation,
                }
            }
            // Pending/rebuilding identity is durable until explicit runtime settlement.
            pending @ (InventoryRecoveryState::ReanchorPending { .. }
            | InventoryRecoveryState::Rebuilding { .. }) => pending.clone(),
        };
        Ok(())
    }
}

pub fn desired_orders(
    epoch: &GridEpoch,
    inventory: &GridInventory,
    grid_count: u8,
) -> Result<Vec<GridOrderIntent>, HedgedGridError> {
    epoch.validate(grid_count)?;
    inventory.validate()?;
    let mut orders = Vec::with_capacity(usize::from(grid_count) * 4);
    for position in [GridPosition::Long, GridPosition::Short] {
        for level in 1..=grid_count {
            orders.push(order_at(epoch, position, GridOrderRole::Open, level)?);
        }
        let available = match position {
            GridPosition::Long => inventory.long_quantity,
            GridPosition::Short => inventory.short_quantity,
        };
        let close_count = grid_count_from_inventory(available, epoch.grid_quantity, grid_count);
        for level in 1..=close_count {
            orders.push(order_at(epoch, position, GridOrderRole::Close, level)?);
        }
    }
    Ok(orders)
}

fn grid_count_from_inventory(quantity: Decimal, grid_quantity: Decimal, max_grid_count: u8) -> u8 {
    if quantity <= Decimal::ZERO || grid_quantity <= Decimal::ZERO {
        return 0;
    }
    let count = (quantity / grid_quantity).floor();
    count.to_u8().unwrap_or(max_grid_count).min(max_grid_count)
}

fn order_at(
    epoch: &GridEpoch,
    position: GridPosition,
    role: GridOrderRole,
    level: u8,
) -> Result<GridOrderIntent, HedgedGridError> {
    order_at_price(
        epoch.epoch,
        position,
        role,
        u64::from(level),
        epoch.price(position, role, level)?,
        epoch.grid_quantity,
    )
}

fn order_at_price(
    epoch: u64,
    position: GridPosition,
    role: GridOrderRole,
    level: u64,
    price: Price,
    quantity: Decimal,
) -> Result<GridOrderIntent, HedgedGridError> {
    let key = GridOrderKey {
        epoch,
        position,
        role,
        level,
    };
    let order = GridOrderIntent {
        side: match role {
            GridOrderRole::Open => position.opening_side(),
            GridOrderRole::Close => position.closing_side(),
        },
        price,
        quantity,
        reduce_only: role == GridOrderRole::Close,
        key,
    };
    order.validate()?;
    Ok(order)
}

#[cfg(test)]
#[path = "reducer_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod recovery_tests;
