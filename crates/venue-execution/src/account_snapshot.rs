use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{
    Asset, CommandId, Fill, NativeOrderFamily, OrderOwner, OrderSide, OrderState, PositionSide,
    Symbol,
};
use venue_gateway_api::GatewayBinding;

use super::{AccountHostValidationError, MAX_RISK_EVIDENCE_AGE_MS, sum_notional};

/// Canonical position-mode fact returned by a complete signed account collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SignedAccountPositionMode {
    Net,
    Hedge,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedAccountOrderFact {
    pub client_order_id: String,
    pub venue_order_id: Option<String>,
    pub symbol: Symbol,
    pub family: NativeOrderFamily,
    pub side: OrderSide,
    pub position_side: PositionSide,
    /// Original order quantity, not leaves quantity. Risk reservations separately use remaining.
    pub quantity: Decimal,
    pub limit_price: Option<Decimal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<venue_domain::LimitTimeInForce>,
    /// Native creation time, never a local receive time or last-fill/update time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at_ms: Option<u64>,
    pub reduce_only: bool,
    pub owner: Option<OrderOwner>,
    pub external: bool,
    #[serde(default)]
    pub state: Option<OrderState>,
    #[serde(default)]
    pub filled_quantity: Option<Decimal>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedAccountBalance {
    pub asset: Asset,
    #[serde(with = "rust_decimal::serde::str")]
    pub equity: Decimal,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub available_margin: Option<Decimal>,
}

/// Native-currency amount that must be converted using a same-generation signed rate before it
/// can participate in the account's fixed USDT risk limit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountRiskAmount {
    pub asset: Asset,
    pub value: Decimal,
}

/// Adapter-proved quote-asset price in USDT. USDT itself is represented only by an exact 1 rate;
/// no stablecoin is implicitly treated as parity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountQuoteToUsdtRate {
    pub asset: Asset,
    pub usdt_per_asset: Decimal,
    pub observed_at_ms: u64,
    pub private_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UsdtRate {
    value: Decimal,
    observed_at_ms: u64,
    private_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedAccountPositionFact {
    pub symbol: Symbol,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub entry_price: Option<Decimal>,
    pub mark_price: Option<Decimal>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SignedUnknownResult {
    Accepted { venue_order_id: String },
    Rejected { reason: String },
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedUnknownFact {
    pub command_id: CommandId,
    pub result: SignedUnknownResult,
}

/// Complete, normalized, adapter-signed account observation. It grants no writer or dispatch
/// capability; the Host persists it only as a restart checkpoint before Runtime consumes it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedAccountSnapshot {
    binding: GatewayBinding,
    observed_at_ms: u64,
    connection_generation: u64,
    private_generation: u64,
    rules_generation: u64,
    position_mode: SignedAccountPositionMode,
    open_orders: Vec<SignedAccountOrderFact>,
    positions: Vec<SignedAccountPositionFact>,
    fills: Vec<Fill>,
    fills_cursor: String,
    #[serde(default)]
    balances: Vec<SignedAccountBalance>,
    unknown_results: Vec<SignedUnknownFact>,
    /// Set only for a live read model derived from uninterrupted authenticated stream updates.
    /// Unchanged account balances retain this REST observation time, not the newer socket time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream_rest_baseline_ms: Option<u64>,
}

impl SignedAccountSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub fn complete(
        binding: GatewayBinding,
        observed_at_ms: u64,
        connection_generation: u64,
        private_generation: u64,
        rules_generation: u64,
        position_mode: SignedAccountPositionMode,
        open_orders: Vec<SignedAccountOrderFact>,
        positions: Vec<SignedAccountPositionFact>,
        fills_cursor: String,
        unknown_results: Vec<SignedUnknownFact>,
    ) -> Result<Self, AccountHostValidationError> {
        Self::complete_with_fills(
            binding,
            observed_at_ms,
            connection_generation,
            private_generation,
            rules_generation,
            position_mode,
            open_orders,
            positions,
            Vec::new(),
            fills_cursor,
            unknown_results,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn complete_with_fills(
        binding: GatewayBinding,
        observed_at_ms: u64,
        connection_generation: u64,
        private_generation: u64,
        rules_generation: u64,
        position_mode: SignedAccountPositionMode,
        open_orders: Vec<SignedAccountOrderFact>,
        positions: Vec<SignedAccountPositionFact>,
        fills: Vec<Fill>,
        fills_cursor: String,
        unknown_results: Vec<SignedUnknownFact>,
    ) -> Result<Self, AccountHostValidationError> {
        if binding.mode != venue_gateway_api::GatewayMode::Live
            || observed_at_ms == 0
            || connection_generation == 0
            || private_generation == 0
            || rules_generation == 0
            || fills_cursor.trim().is_empty()
            || open_orders.iter().any(|fact| {
                fact.client_order_id.trim().is_empty()
                    || fact.created_at_ms == Some(0)
                    || fact
                        .venue_order_id
                        .as_deref()
                        .is_some_and(|id| id.trim().is_empty())
                    || !fact.quantity.is_sign_positive()
                    || fact
                        .limit_price
                        .is_some_and(|price| !price.is_sign_positive())
                    || fact
                        .owner
                        .as_ref()
                        .is_some_and(|owner| owner.symbol != fact.symbol)
                    || fact.filled_quantity.is_some_and(|quantity| {
                        quantity < Decimal::ZERO
                            || quantity == Decimal::MAX
                            || quantity == Decimal::MIN
                    })
            })
            || positions.iter().any(|fact| {
                fact.quantity == Decimal::MAX
                    || fact.quantity == Decimal::MIN
                    || fact
                        .entry_price
                        .is_some_and(|price| !price.is_sign_positive())
                    || fact
                        .mark_price
                        .is_some_and(|price| !price.is_sign_positive())
            })
            || fills.iter().any(|fill| fill.validate().is_err())
            || unknown_results.iter().any(|fact| match &fact.result {
                SignedUnknownResult::Accepted { venue_order_id } => {
                    venue_order_id.trim().is_empty()
                }
                SignedUnknownResult::Rejected { reason } => reason.trim().is_empty(),
                SignedUnknownResult::Unknown => false,
            })
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let mut identities = BTreeMap::new();
        for fact in &unknown_results {
            if identities.insert(fact.command_id.clone(), ()).is_some() {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
        }
        Ok(Self {
            binding,
            observed_at_ms,
            connection_generation,
            private_generation,
            rules_generation,
            position_mode,
            open_orders,
            positions,
            fills,
            fills_cursor,
            balances: Vec::new(),
            unknown_results,
            stream_rest_baseline_ms: None,
        })
    }

    pub fn with_stream_origin(
        mut self,
        rest_baseline_ms: u64,
    ) -> Result<Self, AccountHostValidationError> {
        if rest_baseline_ms == 0 || rest_baseline_ms > self.observed_at_ms {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        self.stream_rest_baseline_ms = Some(rest_baseline_ms);
        Ok(self)
    }

    pub fn balance_observed_at_ms(&self) -> u64 {
        self.stream_rest_baseline_ms.unwrap_or(self.observed_at_ms)
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn observed_at_ms(&self) -> u64 {
        self.observed_at_ms
    }

    #[must_use]
    pub const fn connection_generation(&self) -> u64 {
        self.connection_generation
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn rules_generation(&self) -> u64 {
        self.rules_generation
    }

    #[must_use]
    pub const fn position_mode(&self) -> SignedAccountPositionMode {
        self.position_mode
    }

    #[must_use]
    pub fn unknown_results(&self) -> &[SignedUnknownFact] {
        &self.unknown_results
    }

    #[must_use]
    pub fn open_orders(&self) -> &[SignedAccountOrderFact] {
        &self.open_orders
    }

    /// Host-only normalization of signed adapter facts. Gateway-provided ownership is never
    /// trusted; only the same account WAL may attach an owner after exact native identity checks.
    pub(crate) fn open_orders_mut(&mut self) -> &mut [SignedAccountOrderFact] {
        &mut self.open_orders
    }

    /// A gateway's attempt counter can restart with a fresh process. The account Host alone
    /// ratchets that freshly collected signed observation above its fsynced checkpoint before it
    /// becomes a Runtime generation; adapters and callers cannot rewrite this watermark.
    pub(crate) fn rebase_private_generation(
        &mut self,
        private_generation: u64,
    ) -> Result<(), AccountHostValidationError> {
        if private_generation <= self.private_generation {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        self.private_generation = private_generation;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn expire_for_test(&mut self) {
        self.observed_at_ms = self
            .observed_at_ms
            .saturating_sub(MAX_RISK_EVIDENCE_AGE_MS.saturating_add(1));
    }

    #[must_use]
    pub fn positions(&self) -> &[SignedAccountPositionFact] {
        &self.positions
    }

    #[must_use]
    pub fn fills(&self) -> &[Fill] {
        &self.fills
    }

    /// Carries adapter-signed fills across later account refreshes until the resident Actor has
    /// durably consumed them. Existing order is retained because reducer application order is
    /// part of the inventory transition; an identity with different bytes fails closed.
    pub(crate) fn prepend_unacknowledged_fills(
        &mut self,
        previous: &[Fill],
    ) -> Result<(), AccountHostValidationError> {
        let mut identities = BTreeMap::<String, Fill>::new();
        let mut merged = Vec::with_capacity(previous.len().saturating_add(self.fills.len()));
        for fill in previous.iter().chain(self.fills.iter()) {
            if let Some(existing) = identities.get(&fill.fill_id) {
                if existing != fill {
                    return Err(AccountHostValidationError::SignedSnapshot);
                }
                continue;
            }
            identities.insert(fill.fill_id.clone(), fill.clone());
            merged.push(fill.clone());
        }
        self.fills = merged;
        Ok(())
    }

    pub(crate) fn acknowledge_fills(&mut self, fill_ids: &BTreeMap<String, ()>) {
        self.fills
            .retain(|fill| !fill_ids.contains_key(&fill.fill_id));
    }

    #[must_use]
    pub fn fills_cursor(&self) -> &str {
        &self.fills_cursor
    }

    #[must_use]
    pub fn balances(&self) -> &[SignedAccountBalance] {
        &self.balances
    }

    pub fn with_balances(
        mut self,
        balances: Vec<SignedAccountBalance>,
    ) -> Result<Self, AccountHostValidationError> {
        validate_balances(&balances)?;
        self.balances = balances;
        Ok(self)
    }
}

fn validate_balances(balances: &[SignedAccountBalance]) -> Result<(), AccountHostValidationError> {
    let mut assets = BTreeMap::new();
    for balance in balances {
        if balance.equity == Decimal::MAX
            || balance.equity == Decimal::MIN
            || balance
                .available_margin
                .is_some_and(|value| value == Decimal::MAX || value == Decimal::MIN)
            || assets.insert(balance.asset.clone(), ()).is_some()
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
    }
    Ok(())
}

/// Opaque Host-issued durable checkpoint receipt. Callers can inspect scope but cannot forge a
/// snapshot or turn it into a physical dispatch permit.
#[derive(Clone, Debug)]
pub struct RuntimeBootstrapReceipt {
    pub(super) snapshot: SignedAccountSnapshot,
    pub(super) risk_fenced: bool,
    pub(super) wal_head: venue_storage::DurableWalHead,
}

impl RuntimeBootstrapReceipt {
    #[must_use]
    pub const fn snapshot(&self) -> &SignedAccountSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub const fn risk_fenced(&self) -> bool {
        self.risk_fenced
    }

    #[must_use]
    pub const fn wal_head(&self) -> venue_storage::DurableWalHead {
        self.wal_head
    }
}

/// Complete signed account-risk evidence used only to admit a new entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountRiskEvidence {
    binding: GatewayBinding,
    observed_at_ms: u64,
    private_generation: u64,
    signed_position_notionals: Vec<Decimal>,
    open_entry_order_notionals: Vec<Decimal>,
    usdt_per_asset: BTreeMap<Asset, UsdtRate>,
}

impl AccountRiskEvidence {
    pub fn complete(
        binding: GatewayBinding,
        observed_at_ms: u64,
        private_generation: u64,
        signed_position_notionals: Vec<Decimal>,
        open_entry_order_notionals: Vec<Decimal>,
    ) -> Result<Self, AccountHostValidationError> {
        if binding.symbol.quote() != "USDT" {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        let usdt = Asset::new("USDT").map_err(|_| AccountHostValidationError::RiskEvidence)?;
        Self::complete_with_usdt_valuation(
            binding,
            observed_at_ms,
            private_generation,
            signed_position_notionals
                .into_iter()
                .map(|value| AccountRiskAmount {
                    asset: usdt.clone(),
                    value,
                })
                .collect(),
            open_entry_order_notionals
                .into_iter()
                .map(|value| AccountRiskAmount {
                    asset: usdt.clone(),
                    value,
                })
                .collect(),
            vec![AccountQuoteToUsdtRate {
                asset: usdt,
                usdt_per_asset: Decimal::ONE,
                observed_at_ms,
                private_generation,
            }],
        )
    }

    pub fn complete_with_usdt_valuation(
        binding: GatewayBinding,
        observed_at_ms: u64,
        private_generation: u64,
        signed_positions: Vec<AccountRiskAmount>,
        open_entry_orders: Vec<AccountRiskAmount>,
        rates: Vec<AccountQuoteToUsdtRate>,
    ) -> Result<Self, AccountHostValidationError> {
        if observed_at_ms == 0 || private_generation == 0 {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        let usdt = Asset::new("USDT").map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let mut usdt_per_asset = BTreeMap::new();
        for rate in rates {
            if rate.observed_at_ms == 0
                || rate.observed_at_ms > observed_at_ms
                || observed_at_ms.saturating_sub(rate.observed_at_ms) > MAX_RISK_EVIDENCE_AGE_MS
                || rate.private_generation != private_generation
                || rate.usdt_per_asset <= Decimal::ZERO
                || rate.usdt_per_asset == Decimal::MAX
                || rate.usdt_per_asset == Decimal::MIN
                || (rate.asset == usdt && rate.usdt_per_asset != Decimal::ONE)
                || usdt_per_asset
                    .insert(
                        rate.asset,
                        UsdtRate {
                            value: rate.usdt_per_asset,
                            observed_at_ms: rate.observed_at_ms,
                            private_generation: rate.private_generation,
                        },
                    )
                    .is_some()
            {
                return Err(AccountHostValidationError::RiskEvidence);
            }
        }
        let convert = |amounts: Vec<AccountRiskAmount>| {
            amounts
                .into_iter()
                .map(|amount| {
                    if amount.value < Decimal::ZERO
                        || amount.value == Decimal::MAX
                        || amount.value == Decimal::MIN
                    {
                        return Err(AccountHostValidationError::RiskEvidence);
                    }
                    let rate = if amount.asset == usdt {
                        Decimal::ONE
                    } else {
                        usdt_per_asset
                            .get(&amount.asset)
                            .ok_or(AccountHostValidationError::RiskEvidence)?
                            .value
                    };
                    amount
                        .value
                        .checked_mul(rate)
                        .ok_or(AccountHostValidationError::RiskEvidence)
                })
                .collect::<Result<Vec<_>, _>>()
        };
        let signed_position_notionals = convert(signed_positions)?;
        let open_entry_order_notionals = convert(open_entry_orders)?;
        Ok(Self {
            binding,
            observed_at_ms,
            private_generation,
            signed_position_notionals,
            open_entry_order_notionals,
            usdt_per_asset,
        })
    }

    pub fn validate_for(
        &self,
        binding: &GatewayBinding,
        now_ms: u64,
    ) -> Result<(), AccountHostValidationError> {
        if self.binding.venue != binding.venue
            || self.binding.mode != binding.mode
            || self.binding.trading_account_id != binding.trading_account_id
            || self.private_generation == 0
            || self.observed_at_ms == 0
            || now_ms < self.observed_at_ms
            || now_ms.saturating_sub(self.observed_at_ms) > MAX_RISK_EVIDENCE_AGE_MS
            || self
                .signed_position_notionals
                .iter()
                .chain(self.open_entry_order_notionals.iter())
                .any(|notional| *notional < Decimal::ZERO)
            || self.usdt_per_asset.values().any(|rate| {
                rate.private_generation != self.private_generation
                    || rate.observed_at_ms == 0
                    || rate.observed_at_ms > now_ms
                    || now_ms.saturating_sub(rate.observed_at_ms) > MAX_RISK_EVIDENCE_AGE_MS
            })
        {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        Ok(())
    }

    /// A multi-request collection is only as fresh as its oldest account/order observation.
    /// Later FX or pagination responses must not renew the lifetime of earlier signed facts.
    /// Rates retain their own timestamps and are independently checked again at dispatch.
    pub fn with_earliest_observation(
        mut self,
        oldest_at_ms: u64,
    ) -> Result<Self, AccountHostValidationError> {
        if oldest_at_ms == 0
            || oldest_at_ms > self.observed_at_ms
            || self.observed_at_ms.saturating_sub(oldest_at_ms) > MAX_RISK_EVIDENCE_AGE_MS
        {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        self.observed_at_ms = oldest_at_ms;
        Ok(self)
    }

    pub fn signed_position_total(&self) -> Result<Decimal, AccountHostValidationError> {
        sum_notional(&self.signed_position_notionals)
    }

    pub fn open_entry_order_total(&self) -> Result<Decimal, AccountHostValidationError> {
        sum_notional(&self.open_entry_order_notionals)
    }

    pub fn value_in_usdt(
        &self,
        asset: &Asset,
        value: Decimal,
    ) -> Result<Decimal, AccountHostValidationError> {
        if value < Decimal::ZERO || value == Decimal::MAX || value == Decimal::MIN {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        let usdt = Asset::new("USDT").map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let rate = if *asset == usdt {
            Decimal::ONE
        } else {
            self.usdt_per_asset
                .get(asset)
                .ok_or(AccountHostValidationError::RiskEvidence)?
                .value
        };
        value
            .checked_mul(rate)
            .ok_or(AccountHostValidationError::RiskEvidence)
    }
}

#[cfg(test)]
mod timing_tests {
    use super::*;
    use venue_gateway_api::{GatewayMode, VenueId};

    #[test]
    fn later_fx_response_does_not_freshen_earlier_signed_account_facts()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = GatewayBinding::new(
            VenueId::Hyperliquid,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "DOGE/USDC".parse()?,
        )?;
        let evidence = AccountRiskEvidence::complete_with_usdt_valuation(
            binding.clone(),
            61_000,
            2,
            Vec::new(),
            Vec::new(),
            vec![AccountQuoteToUsdtRate {
                asset: Asset::new("USDC")?,
                usdt_per_asset: Decimal::ONE,
                observed_at_ms: 60_500,
                private_generation: 2,
            }],
        )?
        .with_earliest_observation(1_000)?;
        assert!(evidence.validate_for(&binding, 61_000).is_ok());
        assert!(evidence.validate_for(&binding, 61_001).is_err());
        assert!(evidence.clone().with_earliest_observation(0).is_err());
        Ok(())
    }
}
