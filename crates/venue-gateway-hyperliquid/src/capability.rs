use std::collections::{BTreeMap, BTreeSet};

use k256::ecdsa::{Signature, signature::hazmat::PrehashVerifier};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use venue_domain::domain::{FieldState, Symbol};
use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding, VenueId};

use crate::{
    HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT, HyperliquidAccountSnapshot, HyperliquidCredentials,
    HyperliquidError, HyperliquidFill, HyperliquidFillCoverage, HyperliquidFillCursor,
    HyperliquidFillPage, HyperliquidFillQuery, HyperliquidGatewayBinding,
    HyperliquidOpenOrdersSnapshot, HyperliquidOrderFamily, HyperliquidOrderLookup,
    HyperliquidOrderStatus, HyperliquidOrderStatusUnknownReason, HyperliquidPerpMeta,
    HyperliquidPrivateStreamBinding, HyperliquidReadBinding, parse_clearinghouse_snapshot,
    parse_frontend_open_orders_snapshot, parse_order_status, parse_perp_meta,
    parse_user_fills_page, validate_frontend_open_orders_snapshot,
};

pub const HYPERLIQUID_CAPABILITY_PROBE_SCHEMA: u16 = 2;
pub const HYPERLIQUID_CAPABILITY_PROBE_MAX_TTL_MS: u64 = 60_000;
const HYPERLIQUID_RECOVERY_PROFILE_VERSION: u64 = 1;
type RawOrderIdentity = (HyperliquidOrderFamily, Symbol, u64, Option<String>);
type RawOrderIdentityMap = BTreeMap<u64, (RawOrderIdentity, serde_json::Value)>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidProbeAuthorityRoots {
    owner_keccak256: String,
    wal_keccak256: String,
    unknown_keccak256: String,
}

impl HyperliquidProbeAuthorityRoots {
    #[must_use]
    pub fn owner_keccak256(&self) -> &str {
        &self.owner_keccak256
    }

    pub fn for_snapshots(
        owner: &HyperliquidOwnerSnapshot,
        unknown: &HyperliquidUnknownSnapshot,
        wal: [u8; 32],
    ) -> Result<Self, HyperliquidError> {
        if wal == [0; 32] {
            return Err(HyperliquidError::CapabilityProbe);
        }
        Ok(Self {
            owner_keccak256: owner.commitment_keccak256.clone(),
            wal_keccak256: hex_digest(wal),
            unknown_keccak256: unknown.commitment_keccak256.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidOwnerRoute {
    family: HyperliquidOrderFamily,
    symbol: Symbol,
    order_id: u64,
    client_order_id: Option<String>,
    owner_id: String,
}

impl HyperliquidOwnerRoute {
    pub fn new(
        family: HyperliquidOrderFamily,
        symbol: Symbol,
        order_id: u64,
        client_order_id: Option<String>,
        owner_id: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        let owner_id = owner_id.into();
        let client_order_id = client_order_id
            .map(HyperliquidOrderLookup::client_order_id)
            .transpose()?
            .map(|lookup| lookup.native_identity());
        if order_id == 0
            || owner_id.is_empty()
            || owner_id.len() > 128
            || !owner_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        Ok(Self {
            family,
            symbol,
            order_id,
            client_order_id,
            owner_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidOwnerSnapshot {
    routes: Vec<HyperliquidOwnerRoute>,
    commitment_keccak256: String,
}

impl HyperliquidOwnerSnapshot {
    pub fn new(mut routes: Vec<HyperliquidOwnerRoute>) -> Result<Self, HyperliquidError> {
        routes.sort();
        if routes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(HyperliquidError::CapabilityProbe);
        }
        let commitment_keccak256 = owner_commitment(&routes)?;
        Ok(Self {
            routes,
            commitment_keccak256,
        })
    }

    #[must_use]
    pub fn commitment_keccak256(&self) -> &str {
        &self.commitment_keccak256
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidUnresolvedOrder {
    family: HyperliquidOrderFamily,
    lookup: HyperliquidOrderLookup,
    native_identity: String,
    reason: String,
}

impl HyperliquidUnresolvedOrder {
    pub fn new(
        family: HyperliquidOrderFamily,
        lookup: HyperliquidOrderLookup,
        reason: impl Into<String>,
    ) -> Result<Self, HyperliquidError> {
        let native_identity = lookup.native_identity();
        let reason = reason.into();
        if reason.is_empty()
            || reason.len() > 128
            || reason.chars().any(char::is_control)
            || reason.trim() != reason
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        Ok(Self {
            family,
            lookup,
            native_identity,
            reason,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidUnknownSnapshot {
    unresolved: Vec<HyperliquidUnresolvedOrder>,
    commitment_keccak256: String,
}

impl HyperliquidUnknownSnapshot {
    pub fn new(mut unresolved: Vec<HyperliquidUnresolvedOrder>) -> Result<Self, HyperliquidError> {
        unresolved.sort();
        if unresolved.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(HyperliquidError::CapabilityProbe);
        }
        let commitment_keccak256 = unknown_commitment(&unresolved)?;
        Ok(Self {
            unresolved,
            commitment_keccak256,
        })
    }

    #[must_use]
    pub fn commitment_keccak256(&self) -> &str {
        &self.commitment_keccak256
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidProbeCollectionScope {
    binding: GatewayBinding,
    master_address: String,
    user_address: String,
    vault_address: Option<String>,
    native_coin: String,
    config_digest: String,
    config_epoch: u64,
    symbol_universe: Vec<Symbol>,
    attempt_id: u64,
    connection_generation: u64,
    private_generation: u64,
    recovered_private_generation: u64,
    authority_roots: HyperliquidProbeAuthorityRoots,
    started_ms: u64,
    deadline_ms: u64,
    expires_ms: u64,
}

impl HyperliquidProbeCollectionScope {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        meta: &HyperliquidPerpMeta,
        credentials: &HyperliquidCredentials,
        private: &HyperliquidPrivateStreamBinding,
        config_digest: impl Into<String>,
        config_epoch: u64,
        mut symbol_universe: Vec<Symbol>,
        attempt_id: u64,
        connection_generation: u64,
        recovered_private_generation: u64,
        authority_roots: HyperliquidProbeAuthorityRoots,
        started_ms: u64,
        deadline_ms: u64,
        expires_ms: u64,
    ) -> Result<Self, HyperliquidError> {
        let config_digest = config_digest.into();
        symbol_universe.sort();
        let ttl = expires_ms.checked_sub(started_ms);
        if private.scope() != &meta.scope
            || private.generation() <= recovered_private_generation
            || config_epoch == 0
            || !valid_config_digest(&config_digest)
            || symbol_universe.is_empty()
            || symbol_universe.windows(2).any(|pair| pair[0] == pair[1])
            || symbol_universe.binary_search(meta.scope.symbol()).is_err()
            || attempt_id == 0
            || connection_generation == 0
            || started_ms == 0
            || deadline_ms < started_ms
            || deadline_ms > expires_ms
            || ttl.is_none_or(|value| value == 0 || value > HYPERLIQUID_CAPABILITY_PROBE_MAX_TTL_MS)
            || credentials.user_address() != meta.scope.user_address()
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        Ok(Self {
            binding: meta.scope.binding().gateway().gateway_binding().clone(),
            master_address: credentials.master_address().to_owned(),
            user_address: credentials.user_address().to_owned(),
            vault_address: credentials.vault_address().map(str::to_owned),
            native_coin: meta.scope.native_coin().to_owned(),
            config_digest,
            config_epoch,
            symbol_universe,
            attempt_id,
            connection_generation,
            private_generation: private.generation(),
            recovered_private_generation,
            authority_roots,
            started_ms,
            deadline_ms,
            expires_ms,
        })
    }

    #[must_use]
    pub const fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub fn symbol_universe(&self) -> &[Symbol] {
        &self.symbol_universe
    }

    #[must_use]
    pub const fn authority_roots(&self) -> &HyperliquidProbeAuthorityRoots {
        &self.authority_roots
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum HyperliquidRecoverySurface {
    Account,
    Positions,
    UmOrder,
    UmConditional,
    UmAlgo,
    FillsCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum HyperliquidRecoveryCoverage {
    Complete {
        raw_commitment_keccak256: String,
        record_count: u64,
    },
    Unsupported {
        evidence_keccak256: String,
        profile_version: u64,
    },
    BlockedUnknown {
        raw_commitment_keccak256: String,
        visible_record_count: u64,
        unresolved_commitment_keccak256: String,
        unresolved_count: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidRecoveryFace {
    surface: HyperliquidRecoverySurface,
    attempt_id: u64,
    private_generation: u64,
    scope_commitment_keccak256: String,
    coverage: HyperliquidRecoveryCoverage,
}

impl HyperliquidRecoveryFace {
    #[must_use]
    pub const fn surface(&self) -> HyperliquidRecoverySurface {
        self.surface
    }

    #[must_use]
    pub const fn coverage(&self) -> &HyperliquidRecoveryCoverage {
        &self.coverage
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidUnknownOrderEvidence {
    family: HyperliquidOrderFamily,
    lookup: HyperliquidOrderLookup,
    native_identity: String,
    unresolved_reason: String,
    reason: HyperliquidOrderStatusUnknownReason,
    observed_ms: u64,
    raw_payload: Vec<u8>,
}

impl HyperliquidUnknownOrderEvidence {
    #[must_use]
    pub fn native_identity(&self) -> &str {
        &self.native_identity
    }

    #[must_use]
    pub const fn reason(&self) -> HyperliquidOrderStatusUnknownReason {
        self.reason
    }

    #[must_use]
    pub fn unresolved_reason(&self) -> &str {
        &self.unresolved_reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFillPageEvidence {
    limit: usize,
    observed_ms: u64,
    raw_payload: Vec<u8>,
}

/// Successful subscription ACK coverage from one connected private stream. Construction is kept
/// crate-private and is exposed by the live WebSocket transport only after all three official
/// private subscriptions have been acknowledged.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HyperliquidPrivateStreamProbeEvidence {
    gateway_binding: GatewayBinding,
    user_address: String,
    native_coin: String,
    private_generation: u64,
    observed_ms: u64,
    order_updates: bool,
    user_fills: bool,
    user_events: bool,
}

impl HyperliquidPrivateStreamProbeEvidence {
    pub(crate) fn from_connected(
        binding: &HyperliquidPrivateStreamBinding,
        observed_ms: u64,
    ) -> Result<Self, HyperliquidError> {
        if observed_ms == 0 || binding.generation() == 0 {
            return Err(HyperliquidError::CapabilityProbe);
        }
        Ok(Self {
            gateway_binding: binding
                .scope()
                .binding()
                .gateway()
                .gateway_binding()
                .clone(),
            user_address: binding.scope().user_address().to_owned(),
            native_coin: binding.scope().native_coin().to_owned(),
            private_generation: binding.generation(),
            observed_ms,
            order_updates: true,
            user_fills: true,
            user_events: true,
        })
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn observed_ms(&self) -> u64 {
        self.observed_ms
    }
}

/// In-memory accumulator for one bounded recent-fill window. REST pages must be consumed in exact
/// cursor order. Private fills are deduplicated by the adapter's composite fill identity and must
/// also appear in the exhausted REST window before the probe can be sealed.
pub struct HyperliquidFillWindowProbe {
    binding: HyperliquidPrivateStreamBinding,
    begin_ms: u64,
    end_ms: u64,
    next_cursor: Option<HyperliquidFillCursor>,
    fills: BTreeMap<String, HyperliquidFill>,
    rest_ids: BTreeSet<String>,
    private_ids: BTreeSet<String>,
    complete: bool,
}

impl HyperliquidFillWindowProbe {
    pub fn new(
        binding: &HyperliquidPrivateStreamBinding,
        begin_ms: u64,
        end_ms: u64,
    ) -> Result<Self, HyperliquidError> {
        if binding.generation() == 0 || begin_ms == 0 || end_ms < begin_ms {
            return Err(HyperliquidError::CapabilityProbe);
        }
        Ok(Self {
            binding: binding.clone(),
            begin_ms,
            end_ms,
            next_cursor: None,
            fills: BTreeMap::new(),
            rest_ids: BTreeSet::new(),
            private_ids: BTreeSet::new(),
            complete: false,
        })
    }

    pub fn ingest_page(
        &mut self,
        query: &HyperliquidFillQuery,
        page: &HyperliquidFillPage,
    ) -> Result<(), HyperliquidError> {
        if self.complete
            || query.scope() != self.binding.scope()
            || page.scope != *self.binding.scope()
            || query.begin_ms() != self.begin_ms
            || query.end_ms() != self.end_ms
            || query.after() != self.next_cursor.as_ref()
            || (page.complete && page.next_cursor.is_some())
            || (!page.complete && page.next_cursor.is_none())
            || !matches!(
                (page.complete, page.coverage),
                (
                    true,
                    HyperliquidFillCoverage::VenueVisibleWindowExhausted {
                        maximum_retained_fills: HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT
                    }
                ) | (false, HyperliquidFillCoverage::MorePages)
            )
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        for fill in &page.fills {
            self.insert_fill(fill, FillSource::Rest)?;
        }
        self.next_cursor = page.next_cursor.clone();
        self.complete = page.complete;
        Ok(())
    }

    pub fn ingest_private(
        &mut self,
        update: &crate::HyperliquidFillUpdate,
    ) -> Result<(), HyperliquidError> {
        if update.binding != self.binding {
            return Err(HyperliquidError::CapabilityProbe);
        }
        self.insert_fill(&update.fill, FillSource::Private)
    }

    pub fn finish(self) -> Result<HyperliquidFillWindowEvidence, HyperliquidError> {
        if !self.complete || !self.private_ids.is_subset(&self.rest_ids) {
            return Err(HyperliquidError::CapabilityProbe);
        }
        let fill_commitment_keccak256 = fill_commitment(self.fills.values())?;
        Ok(HyperliquidFillWindowEvidence {
            gateway_binding: self
                .binding
                .scope()
                .binding()
                .gateway()
                .gateway_binding()
                .clone(),
            user_address: self.binding.scope().user_address().to_owned(),
            native_coin: self.binding.scope().native_coin().to_owned(),
            private_generation: self.binding.generation(),
            begin_ms: self.begin_ms,
            end_ms: self.end_ms,
            fill_count: self.fills.len(),
            private_overlap_count: self.private_ids.len(),
            maximum_retained_fills: HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT,
            complete: true,
            fill_commitment_keccak256,
        })
    }

    fn insert_fill(
        &mut self,
        fill: &HyperliquidFill,
        source: FillSource,
    ) -> Result<(), HyperliquidError> {
        let time_ms = fill
            .fill
            .exchange_time_ms
            .ok_or(HyperliquidError::CapabilityProbe)?;
        if time_ms < self.begin_ms
            || time_ms > self.end_ms
            || fill.fill.symbol != *self.binding.scope().symbol()
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        let fill_id = fill.fill.fill_id.clone();
        if self
            .fills
            .get(&fill_id)
            .is_some_and(|existing| existing != fill)
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        self.fills
            .entry(fill_id.clone())
            .or_insert_with(|| fill.clone());
        match source {
            FillSource::Rest => self.rest_ids.insert(fill_id),
            FillSource::Private => self.private_ids.insert(fill_id),
        };
        Ok(())
    }
}

enum FillSource {
    Rest,
    Private,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HyperliquidFillWindowEvidence {
    gateway_binding: GatewayBinding,
    user_address: String,
    native_coin: String,
    private_generation: u64,
    begin_ms: u64,
    end_ms: u64,
    fill_count: usize,
    private_overlap_count: usize,
    maximum_retained_fills: usize,
    complete: bool,
    fill_commitment_keccak256: String,
}

impl HyperliquidFillWindowEvidence {
    #[must_use]
    pub const fn begin_ms(&self) -> u64 {
        self.begin_ms
    }

    #[must_use]
    pub const fn end_ms(&self) -> u64 {
        self.end_ms
    }

    #[must_use]
    pub const fn fill_count(&self) -> usize {
        self.fill_count
    }
}

/// Linear, in-memory collector for one fresh recovery turn. It cannot be serialized or cloned,
/// and every accepted surface is parsed under the scope fixed at `start`.
pub struct HyperliquidFreshProbeCollector {
    scope: HyperliquidProbeCollectionScope,
    meta: HyperliquidPerpMeta,
    meta_raw_payload: Vec<u8>,
    owner_snapshot: HyperliquidOwnerSnapshot,
    unknown_snapshot: HyperliquidUnknownSnapshot,
    fill_probe: HyperliquidFillWindowProbe,
    fill_pages: Vec<RawFillPageEvidence>,
    account: Option<(HyperliquidAccountSnapshot, Vec<u8>, u64)>,
    orders: Option<HyperliquidOpenOrdersSnapshot>,
    unknown_orders: Vec<HyperliquidUnknownOrderEvidence>,
}

impl HyperliquidFreshProbeCollector {
    #[cfg(not(test))]
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        _scope: HyperliquidProbeCollectionScope,
        _meta: &HyperliquidPerpMeta,
        _meta_raw_payload: &[u8],
        _private: &HyperliquidPrivateStreamBinding,
        _owner_snapshot: HyperliquidOwnerSnapshot,
        _unknown_snapshot: HyperliquidUnknownSnapshot,
        _fill_begin_ms: u64,
        _fill_end_ms: u64,
    ) -> Result<Self, HyperliquidError> {
        Err(HyperliquidError::RecoveryIntegrationUnavailable)
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        scope: HyperliquidProbeCollectionScope,
        meta: &HyperliquidPerpMeta,
        meta_raw_payload: &[u8],
        private: &HyperliquidPrivateStreamBinding,
        owner_snapshot: HyperliquidOwnerSnapshot,
        unknown_snapshot: HyperliquidUnknownSnapshot,
        fill_begin_ms: u64,
        fill_end_ms: u64,
    ) -> Result<Self, HyperliquidError> {
        if scope.binding != *meta.scope.binding().gateway().gateway_binding()
            || scope.user_address != meta.scope.user_address()
            || scope.native_coin != meta.scope.native_coin()
            || scope.private_generation != private.generation()
            || private.scope() != &meta.scope
            || owner_snapshot.commitment_keccak256 != scope.authority_roots.owner_keccak256
            || unknown_snapshot.commitment_keccak256 != scope.authority_roots.unknown_keccak256
            || parse_perp_meta(meta_raw_payload, meta.scope.binding())? != *meta
            || !metadata_for_universe(&scope, meta_raw_payload)?
                .iter()
                .any(|candidate| candidate == meta)
            || fill_end_ms > scope.deadline_ms
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        Ok(Self {
            scope,
            meta: meta.clone(),
            meta_raw_payload: meta_raw_payload.to_vec(),
            owner_snapshot,
            unknown_snapshot,
            fill_probe: HyperliquidFillWindowProbe::new(private, fill_begin_ms, fill_end_ms)?,
            fill_pages: Vec::new(),
            account: None,
            orders: None,
            unknown_orders: Vec::new(),
        })
    }

    pub fn ingest_account(
        &mut self,
        raw_payload: &[u8],
        observed_ms: u64,
    ) -> Result<(), HyperliquidError> {
        self.validate_observed(observed_ms)?;
        if self.account.is_some() {
            return Err(HyperliquidError::CapabilityProbe);
        }
        let account = parse_clearinghouse_snapshot(raw_payload, &self.meta)?;
        for meta in metadata_for_universe(&self.scope, &self.meta_raw_payload)? {
            parse_clearinghouse_snapshot(raw_payload, &meta)?;
        }
        if account.exchange_time_ms > observed_ms {
            return Err(HyperliquidError::CapabilityProbe);
        }
        self.account = Some((account, raw_payload.to_vec(), observed_ms));
        Ok(())
    }

    pub fn ingest_orders(
        &mut self,
        raw_payload: &[u8],
        observed_ms: u64,
    ) -> Result<(), HyperliquidError> {
        self.validate_observed(observed_ms)?;
        if self.orders.is_some() {
            return Err(HyperliquidError::CapabilityProbe);
        }
        let orders = parse_frontend_open_orders_snapshot(raw_payload, &self.meta, observed_ms)?;
        for meta in metadata_for_universe(&self.scope, &self.meta_raw_payload)? {
            parse_frontend_open_orders_snapshot(raw_payload, &meta, observed_ms)?;
        }
        validate_raw_exact_owners(raw_payload, &self.scope, &self.owner_snapshot)?;
        validate_exact_owners(&orders, &self.owner_snapshot)?;
        self.orders = Some(orders);
        Ok(())
    }

    pub fn ingest_fill_page(
        &mut self,
        query: &HyperliquidFillQuery,
        raw_payload: &[u8],
        observed_ms: u64,
    ) -> Result<(), HyperliquidError> {
        self.validate_observed(observed_ms)?;
        let page = parse_user_fills_page(raw_payload, &self.meta, query)?;
        self.fill_probe.ingest_page(query, &page)?;
        self.fill_pages.push(RawFillPageEvidence {
            limit: query.limit(),
            observed_ms,
            raw_payload: raw_payload.to_vec(),
        });
        Ok(())
    }

    pub fn ingest_private_fill(
        &mut self,
        update: &crate::HyperliquidFillUpdate,
    ) -> Result<(), HyperliquidError> {
        self.fill_probe.ingest_private(update)
    }

    pub fn ingest_unknown_order_status(
        &mut self,
        lookup: &HyperliquidOrderLookup,
        raw_payload: &[u8],
        observed_ms: u64,
    ) -> Result<(), HyperliquidError> {
        self.validate_observed(observed_ms)?;
        let status = parse_order_status(raw_payload, &self.meta, lookup)?;
        let HyperliquidOrderStatus::Unknown {
            lookup,
            native_identity,
            reason,
            ..
        } = status
        else {
            return Err(HyperliquidError::CapabilityProbe);
        };
        let unresolved = self
            .unknown_snapshot
            .unresolved
            .iter()
            .find(|unresolved| unresolved.lookup == lookup)
            .ok_or(HyperliquidError::CapabilityProbe)?;
        if self
            .unknown_orders
            .iter()
            .any(|existing| existing.lookup == lookup)
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        self.unknown_orders.push(HyperliquidUnknownOrderEvidence {
            family: unresolved.family,
            lookup,
            native_identity,
            unresolved_reason: unresolved.reason.clone(),
            reason,
            observed_ms,
            raw_payload: raw_payload.to_vec(),
        });
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish(
        self,
        credentials: &HyperliquidCredentials,
        private_stream: HyperliquidPrivateStreamProbeEvidence,
        version: u64,
        observed_ms: u64,
    ) -> Result<HyperliquidCapabilityProbeEvidence, HyperliquidError> {
        self.validate_observed(observed_ms)?;
        if private_stream.observed_ms < self.scope.started_ms
            || private_stream.observed_ms > self.scope.deadline_ms
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        let (account, account_raw_payload, account_observed_ms) =
            self.account.ok_or(HyperliquidError::CapabilityProbe)?;
        let orders = self.orders.ok_or(HyperliquidError::CapabilityProbe)?;
        let fill_window = self.fill_probe.finish()?;
        validate_unknown_correspondence(&self.unknown_snapshot, &self.unknown_orders)?;
        HyperliquidCapabilityProbeEvidence::issue_collected(
            self.scope,
            &self.meta,
            credentials,
            &account,
            account_raw_payload,
            account_observed_ms,
            &orders,
            self.meta_raw_payload,
            self.fill_pages,
            fill_window,
            private_stream,
            self.owner_snapshot,
            self.unknown_snapshot,
            self.unknown_orders,
            version,
            observed_ms,
        )
    }

    fn validate_observed(&self, observed_ms: u64) -> Result<(), HyperliquidError> {
        if observed_ms < self.scope.started_ms || observed_ms > self.scope.deadline_ms {
            Err(HyperliquidError::CapabilityProbe)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CapabilityProbePayload {
    schema_version: u16,
    collection_scope: HyperliquidProbeCollectionScope,
    binding: GatewayBinding,
    version: u64,
    observed_ms: u64,
    expires_ms: u64,
    connection_generation: u64,
    private_generation: u64,
    master_address: String,
    user_address: String,
    vault_address: Option<String>,
    agent_address: String,
    agent_name: String,
    native_coin: String,
    asset_index: u32,
    account_exchange_time_ms: u64,
    account_observed_ms: u64,
    orders_observed_ms: u64,
    meta_commitment_keccak256: String,
    account_commitment_keccak256: String,
    orders_commitment_keccak256: String,
    meta_raw_payload: Vec<u8>,
    account_raw_payload: Vec<u8>,
    orders_raw_payload: Vec<u8>,
    fill_raw_pages: Vec<RawFillPageEvidence>,
    fill_window: HyperliquidFillWindowEvidence,
    private_stream: HyperliquidPrivateStreamProbeEvidence,
    owner_snapshot: HyperliquidOwnerSnapshot,
    unknown_snapshot: HyperliquidUnknownSnapshot,
    recovery_faces: Vec<HyperliquidRecoveryFace>,
    unknown_orders: Vec<HyperliquidUnknownOrderEvidence>,
    withdrawals_permitted: bool,
}

/// Immutable, serializable commitment to a complete active adapter probe. It is candidate
/// capability evidence only: it does not acquire a writer, create a WAL, or change the crate's
/// static empty capability set.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HyperliquidCapabilityProbeEvidence {
    payload: CapabilityProbePayload,
    commitment_keccak256: String,
    authority_signature: String,
}

impl HyperliquidCapabilityProbeEvidence {
    #[allow(clippy::too_many_arguments)]
    fn issue_collected(
        collection_scope: HyperliquidProbeCollectionScope,
        meta: &HyperliquidPerpMeta,
        credentials: &HyperliquidCredentials,
        account: &HyperliquidAccountSnapshot,
        account_raw_payload: Vec<u8>,
        account_observed_ms: u64,
        orders: &HyperliquidOpenOrdersSnapshot,
        meta_raw_payload: Vec<u8>,
        fill_raw_pages: Vec<RawFillPageEvidence>,
        fill_window: HyperliquidFillWindowEvidence,
        private_stream: HyperliquidPrivateStreamProbeEvidence,
        owner_snapshot: HyperliquidOwnerSnapshot,
        unknown_snapshot: HyperliquidUnknownSnapshot,
        unknown_orders: Vec<HyperliquidUnknownOrderEvidence>,
        version: u64,
        observed_ms: u64,
    ) -> Result<Self, HyperliquidError> {
        validate_frontend_open_orders_snapshot(orders, meta)?;
        let recovery_faces = recovery_faces(
            &collection_scope,
            &account_raw_payload,
            orders,
            &fill_raw_pages,
            &unknown_snapshot,
        )?;
        let payload = CapabilityProbePayload {
            schema_version: HYPERLIQUID_CAPABILITY_PROBE_SCHEMA,
            collection_scope: collection_scope.clone(),
            binding: meta.scope.binding().gateway().gateway_binding().clone(),
            version,
            observed_ms,
            expires_ms: collection_scope.expires_ms,
            connection_generation: collection_scope.connection_generation,
            private_generation: private_stream.private_generation,
            master_address: credentials.master_address().to_owned(),
            user_address: credentials.user_address().to_owned(),
            vault_address: credentials.vault_address().map(str::to_owned),
            agent_address: credentials.agent_address().to_owned(),
            agent_name: credentials.agent_name().to_owned(),
            native_coin: meta.scope.native_coin().to_owned(),
            asset_index: meta.asset_index,
            account_exchange_time_ms: account.exchange_time_ms,
            account_observed_ms,
            orders_observed_ms: orders.observed_at_ms,
            meta_commitment_keccak256: meta_commitment(meta)?,
            account_commitment_keccak256: hex_digest(
                Keccak256::digest(&account_raw_payload).into(),
            ),
            orders_commitment_keccak256: hex_digest(Keccak256::digest(&orders.raw_payload).into()),
            meta_raw_payload,
            account_raw_payload,
            orders_raw_payload: orders.raw_payload.clone(),
            fill_raw_pages,
            fill_window,
            private_stream,
            owner_snapshot,
            unknown_snapshot,
            recovery_faces,
            unknown_orders,
            withdrawals_permitted: false,
        };
        if account_observed_ms < collection_scope.started_ms
            || account_observed_ms > collection_scope.deadline_ms
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        validate_probe_payload(&payload, meta, account, orders)?;
        replay_raw_payloads(&payload, credentials)?;
        let commitment_keccak256 = commitment(&payload)?;
        let authority_signature = sign_commitment(credentials, &commitment_keccak256)?;
        Ok(Self {
            payload,
            commitment_keccak256,
            authority_signature,
        })
    }

    pub fn verify(&self, credentials: &HyperliquidCredentials) -> Result<(), HyperliquidError> {
        validate_persisted_payload(&self.payload)?;
        if commitment(&self.payload)? != self.commitment_keccak256 {
            return Err(HyperliquidError::CapabilityProbe);
        }
        verify_commitment_signature(
            credentials,
            &self.commitment_keccak256,
            &self.authority_signature,
        )?;
        replay_raw_payloads(&self.payload, credentials)?;
        Ok(())
    }

    /// Produces read-only candidate evidence. No persisted probe or ordinary caller can grant
    /// mutation capability; writer/WAL/host authority remains unavailable.
    pub fn candidate_capability_snapshot(
        &self,
        expected_binding: &GatewayBinding,
        credentials: &HyperliquidCredentials,
        now_ms: u64,
    ) -> Result<CapabilitySnapshot, HyperliquidError> {
        self.verify(credentials)?;
        if &self.payload.binding != expected_binding
            || now_ms < self.payload.observed_ms
            || now_ms >= self.payload.expires_ms
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        Ok(CapabilitySnapshot {
            binding: self.payload.binding.clone(),
            version: self.payload.version,
            observed_ms: self.payload.observed_ms,
            expires_ms: self.payload.expires_ms,
            flags: CapabilityFlags::READ_ACCOUNT
                | CapabilityFlags::READ_ORDERS
                | CapabilityFlags::READ_FILLS
                | CapabilityFlags::PRIVATE_STREAM,
        })
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.payload.private_generation
    }

    #[must_use]
    pub fn commitment_keccak256(&self) -> &str {
        &self.commitment_keccak256
    }

    #[must_use]
    pub fn collection_scope(&self) -> &HyperliquidProbeCollectionScope {
        &self.payload.collection_scope
    }

    #[must_use]
    pub fn recovery_faces(&self) -> &[HyperliquidRecoveryFace] {
        &self.payload.recovery_faces
    }

    #[must_use]
    pub fn unknown_orders(&self) -> &[HyperliquidUnknownOrderEvidence] {
        &self.payload.unknown_orders
    }
}

fn validate_probe_payload(
    payload: &CapabilityProbePayload,
    meta: &HyperliquidPerpMeta,
    account: &HyperliquidAccountSnapshot,
    orders: &HyperliquidOpenOrdersSnapshot,
) -> Result<(), HyperliquidError> {
    if account.scope != meta.scope
        || orders.scope != meta.scope
        || payload.binding != *meta.scope.binding().gateway().gateway_binding()
        || payload.user_address != meta.scope.user_address()
        || payload.native_coin != meta.scope.native_coin()
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    validate_persisted_payload(payload)
}

fn validate_persisted_payload(payload: &CapabilityProbePayload) -> Result<(), HyperliquidError> {
    let ttl = payload.expires_ms.checked_sub(payload.observed_ms);
    let scope = &payload.collection_scope;
    if payload.schema_version != HYPERLIQUID_CAPABILITY_PROBE_SCHEMA
        || payload.binding.venue != VenueId::Hyperliquid
        || payload.binding.validate().is_err()
        || payload.binding.symbol.quote() != "USDC"
        || payload.version == 0
        || payload.connection_generation == 0
        || payload.private_generation == 0
        || ttl.is_none_or(|value| value == 0 || value > HYPERLIQUID_CAPABILITY_PROBE_MAX_TTL_MS)
        || payload.binding != scope.binding
        || payload.master_address != scope.master_address
        || payload.user_address != scope.user_address
        || payload.vault_address != scope.vault_address
        || payload.native_coin != scope.native_coin
        || payload.connection_generation != scope.connection_generation
        || payload.private_generation != scope.private_generation
        || payload.expires_ms != scope.expires_ms
        || payload.observed_ms < scope.started_ms
        || payload.observed_ms > scope.deadline_ms
        || scope.private_generation <= scope.recovered_private_generation
        || scope.config_epoch == 0
        || !valid_config_digest(&scope.config_digest)
        || scope.symbol_universe.is_empty()
        || scope
            .symbol_universe
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || scope
            .symbol_universe
            .binary_search(&scope.binding.symbol)
            .is_err()
        || scope.attempt_id == 0
        || scope.connection_generation == 0
        || scope.started_ms == 0
        || scope.deadline_ms < scope.started_ms
        || scope.deadline_ms > scope.expires_ms
        || scope
            .expires_ms
            .checked_sub(scope.started_ms)
            .is_none_or(|value| value == 0 || value > HYPERLIQUID_CAPABILITY_PROBE_MAX_TTL_MS)
        || !valid_authority_roots(&scope.authority_roots)
        || payload.account_exchange_time_ms == 0
        || payload.account_exchange_time_ms > payload.observed_ms
        || payload.account_observed_ms < scope.started_ms
        || payload.account_observed_ms > scope.deadline_ms
        || payload.orders_observed_ms == 0
        || payload.orders_observed_ms > payload.observed_ms
        || payload.fill_window.end_ms > payload.observed_ms
        || payload.private_stream.observed_ms > payload.observed_ms
        || payload.withdrawals_permitted
        || payload.master_address.is_empty()
        || payload.agent_address.is_empty()
        || payload.agent_name.is_empty()
        || payload.native_coin.is_empty()
        || !valid_hex_digest(&payload.meta_commitment_keccak256)
        || !valid_hex_digest(&payload.account_commitment_keccak256)
        || !valid_hex_digest(&payload.orders_commitment_keccak256)
        || payload.meta_raw_payload.is_empty()
        || payload.account_raw_payload.is_empty()
        || payload.orders_raw_payload.is_empty()
        || payload.fill_raw_pages.is_empty()
        || payload.fill_raw_pages.iter().any(|page| {
            page.limit == 0
                || page.raw_payload.is_empty()
                || page.observed_ms < scope.started_ms
                || page.observed_ms > scope.deadline_ms
        })
        || owner_commitment(&payload.owner_snapshot.routes)?
            != payload.owner_snapshot.commitment_keccak256
        || payload.owner_snapshot.commitment_keccak256 != scope.authority_roots.owner_keccak256
        || unknown_commitment(&payload.unknown_snapshot.unresolved)?
            != payload.unknown_snapshot.commitment_keccak256
        || payload.unknown_snapshot.commitment_keccak256 != scope.authority_roots.unknown_keccak256
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    let expected_user = payload
        .vault_address
        .as_deref()
        .unwrap_or(&payload.master_address);
    if !crate::credentials::valid_address(&payload.master_address)
        || !crate::credentials::valid_address(&payload.user_address)
        || !crate::credentials::valid_address(&payload.agent_address)
        || payload
            .vault_address
            .as_deref()
            .is_some_and(|value| !crate::credentials::valid_address(value))
        || payload.user_address != expected_user
        || payload.agent_address == payload.master_address
        || payload.agent_address == payload.user_address
        || payload
            .vault_address
            .as_ref()
            .is_some_and(|vault| vault == &payload.agent_address)
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    validate_embedded_scope(payload)
}

fn validate_embedded_scope(payload: &CapabilityProbePayload) -> Result<(), HyperliquidError> {
    let fill = &payload.fill_window;
    let private = &payload.private_stream;
    if fill.gateway_binding != payload.binding
        || private.gateway_binding != payload.binding
        || fill.user_address != payload.user_address
        || private.user_address != payload.user_address
        || fill.native_coin != payload.native_coin
        || private.native_coin != payload.native_coin
        || fill.private_generation != payload.private_generation
        || private.private_generation != payload.private_generation
        || fill.begin_ms == 0
        || fill.end_ms < fill.begin_ms
        || !fill.complete
        || fill.maximum_retained_fills != HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT
        || fill.private_overlap_count > fill.fill_count
        || !valid_hex_digest(&fill.fill_commitment_keccak256)
        || !private.order_updates
        || !private.user_fills
        || !private.user_events
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    Ok(())
}

fn validate_exact_owners(
    orders: &HyperliquidOpenOrdersSnapshot,
    owners: &HyperliquidOwnerSnapshot,
) -> Result<(), HyperliquidError> {
    for order in &orders.orders {
        let order_id = order
            .order
            .order_id
            .parse::<u64>()
            .map_err(|_| HyperliquidError::CapabilityProbe)?;
        let client_order_id = match &order.order.client_order_id {
            FieldState::Known(value) => Some(value.as_str()),
            FieldState::Missing => None,
            FieldState::Null | FieldState::Unavailable { .. } | FieldState::NotApplicable => {
                return Err(HyperliquidError::CapabilityProbe);
            }
        };
        let matches = owners
            .routes
            .iter()
            .filter(|route| {
                route.family == order.family
                    && route.symbol == order.order.symbol
                    && route.order_id == order_id
                    && route.client_order_id.as_deref() == client_order_id
            })
            .count();
        if matches != 1 {
            return Err(HyperliquidError::CapabilityProbe);
        }
    }
    Ok(())
}

fn validate_raw_exact_owners(
    raw_payload: &[u8],
    scope: &HyperliquidProbeCollectionScope,
    owners: &HyperliquidOwnerSnapshot,
) -> Result<(), HyperliquidError> {
    for (family, symbol, order_id, client_order_id) in raw_order_identities(raw_payload, scope)? {
        let matches = owners
            .routes
            .iter()
            .filter(|route| {
                route.family == family
                    && route.symbol == symbol
                    && route.order_id == order_id
                    && route.client_order_id == client_order_id
            })
            .count();
        if matches != 1 {
            return Err(HyperliquidError::CapabilityProbe);
        }
    }
    Ok(())
}

fn raw_order_identities(
    raw_payload: &[u8],
    scope: &HyperliquidProbeCollectionScope,
) -> Result<Vec<RawOrderIdentity>, HyperliquidError> {
    let rows: Vec<serde_json::Value> =
        serde_json::from_slice(raw_payload).map_err(|_| HyperliquidError::CapabilityProbe)?;
    let mut by_order_id = RawOrderIdentityMap::new();
    for row in &rows {
        collect_raw_order_identity(row, scope, &mut by_order_id)?;
    }
    Ok(by_order_id
        .into_values()
        .map(|(identity, _)| identity)
        .collect())
}

fn collect_raw_order_identity(
    row: &serde_json::Value,
    scope: &HyperliquidProbeCollectionScope,
    identities: &mut RawOrderIdentityMap,
) -> Result<(), HyperliquidError> {
    let object = row.as_object().ok_or(HyperliquidError::CapabilityProbe)?;
    let coin = object
        .get("coin")
        .and_then(serde_json::Value::as_str)
        .ok_or(HyperliquidError::CapabilityProbe)?;
    let symbol = exact_symbol_for_coin(scope, coin)?;
    let order_id = object
        .get("oid")
        .and_then(serde_json::Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or(HyperliquidError::CapabilityProbe)?;
    let is_trigger = object
        .get("isTrigger")
        .and_then(serde_json::Value::as_bool)
        .ok_or(HyperliquidError::CapabilityProbe)?;
    let client_order_id = match object.get("cloid") {
        Some(serde_json::Value::String(value)) => {
            Some(HyperliquidOrderLookup::client_order_id(value.clone())?.native_identity())
        }
        Some(serde_json::Value::Null) => None,
        _ => return Err(HyperliquidError::CapabilityProbe),
    };
    let family = if is_trigger {
        HyperliquidOrderFamily::Conditional
    } else {
        HyperliquidOrderFamily::Regular
    };
    let identity = (family, symbol, order_id, client_order_id);
    if let Some((existing_identity, existing_raw)) = identities.get(&order_id) {
        if existing_identity != &identity || existing_raw != row {
            return Err(HyperliquidError::CapabilityProbe);
        }
    } else {
        identities.insert(order_id, (identity, row.clone()));
    }
    let children = object
        .get("children")
        .and_then(serde_json::Value::as_array)
        .ok_or(HyperliquidError::CapabilityProbe)?;
    for child in children {
        collect_raw_order_identity(child, scope, identities)?;
    }
    Ok(())
}

fn raw_position_count(
    raw_payload: &[u8],
    scope: &HyperliquidProbeCollectionScope,
) -> Result<u64, HyperliquidError> {
    let value: serde_json::Value =
        serde_json::from_slice(raw_payload).map_err(|_| HyperliquidError::CapabilityProbe)?;
    let rows = value
        .get("assetPositions")
        .and_then(serde_json::Value::as_array)
        .ok_or(HyperliquidError::CapabilityProbe)?;
    let mut symbols = BTreeSet::new();
    for row in rows {
        let coin = row
            .get("position")
            .and_then(|position| position.get("coin"))
            .and_then(serde_json::Value::as_str)
            .ok_or(HyperliquidError::CapabilityProbe)?;
        let symbol = exact_symbol_for_coin(scope, coin)?;
        if !symbols.insert(symbol) {
            return Err(HyperliquidError::CapabilityProbe);
        }
    }
    u64::try_from(rows.len()).map_err(|_| HyperliquidError::CapabilityProbe)
}

fn exact_symbol_for_coin(
    scope: &HyperliquidProbeCollectionScope,
    native_coin: &str,
) -> Result<Symbol, HyperliquidError> {
    let mut matches = scope
        .symbol_universe
        .iter()
        .filter(|symbol| symbol.base() == native_coin);
    let symbol = matches
        .next()
        .cloned()
        .ok_or(HyperliquidError::CapabilityProbe)?;
    if matches.next().is_some() || symbol.quote() != "USDC" {
        return Err(HyperliquidError::CapabilityProbe);
    }
    Ok(symbol)
}

fn owner_commitment(routes: &[HyperliquidOwnerRoute]) -> Result<String, HyperliquidError> {
    let encoded = serde_json::to_vec(routes).map_err(|_| HyperliquidError::CapabilityProbe)?;
    Ok(hex_digest(Keccak256::digest(encoded).into()))
}

fn unknown_commitment(
    unresolved: &[HyperliquidUnresolvedOrder],
) -> Result<String, HyperliquidError> {
    let encoded = serde_json::to_vec(unresolved).map_err(|_| HyperliquidError::CapabilityProbe)?;
    Ok(hex_digest(Keccak256::digest(encoded).into()))
}

fn validate_unknown_correspondence(
    snapshot: &HyperliquidUnknownSnapshot,
    observed: &[HyperliquidUnknownOrderEvidence],
) -> Result<(), HyperliquidError> {
    if snapshot.unresolved.len() != observed.len() {
        return Err(HyperliquidError::CapabilityProbe);
    }
    for unresolved in &snapshot.unresolved {
        let matches = observed
            .iter()
            .filter(|evidence| {
                evidence.family == unresolved.family
                    && evidence.lookup == unresolved.lookup
                    && evidence.native_identity == unresolved.native_identity
                    && evidence.unresolved_reason == unresolved.reason
            })
            .count();
        if matches != 1 {
            return Err(HyperliquidError::CapabilityProbe);
        }
    }
    Ok(())
}

fn valid_authority_roots(roots: &HyperliquidProbeAuthorityRoots) -> bool {
    [
        &roots.owner_keccak256,
        &roots.wal_keccak256,
        &roots.unknown_keccak256,
    ]
    .into_iter()
    .all(|value| valid_hex_digest(value) && value.as_bytes().iter().any(|byte| *byte != b'0'))
}

fn recovery_faces(
    scope: &HyperliquidProbeCollectionScope,
    account_raw: &[u8],
    orders: &HyperliquidOpenOrdersSnapshot,
    fill_pages: &[RawFillPageEvidence],
    unknown_snapshot: &HyperliquidUnknownSnapshot,
) -> Result<Vec<HyperliquidRecoveryFace>, HyperliquidError> {
    let scope_commitment_keccak256 = scope_commitment(scope)?;
    let complete = |surface, raw_commitment_keccak256, record_count| HyperliquidRecoveryFace {
        surface,
        attempt_id: scope.attempt_id,
        private_generation: scope.private_generation,
        scope_commitment_keccak256: scope_commitment_keccak256.clone(),
        coverage: HyperliquidRecoveryCoverage::Complete {
            raw_commitment_keccak256,
            record_count,
        },
    };
    let account_commitment = raw_surface_commitment(
        HyperliquidRecoverySurface::Account,
        std::iter::once(account_raw),
    )?;
    let position_commitment = raw_surface_commitment(
        HyperliquidRecoverySurface::Positions,
        std::iter::once(account_raw),
    )?;
    let regular_commitment = raw_surface_commitment(
        HyperliquidRecoverySurface::UmOrder,
        std::iter::once(orders.raw_payload.as_slice()),
    )?;
    let conditional_commitment = raw_surface_commitment(
        HyperliquidRecoverySurface::UmConditional,
        std::iter::once(orders.raw_payload.as_slice()),
    )?;
    let fill_commitment = raw_surface_commitment(
        HyperliquidRecoverySurface::FillsCursor,
        fill_pages.iter().map(|page| page.raw_payload.as_slice()),
    )?;
    let unsupported = serde_json::to_vec(&(
        scope,
        HyperliquidRecoverySurface::UmAlgo,
        HYPERLIQUID_RECOVERY_PROFILE_VERSION,
        "no_algorithmic_action_surface",
    ))
    .map_err(|_| HyperliquidError::CapabilityProbe)?;
    let raw_orders = raw_order_identities(&orders.raw_payload, scope)?;
    let position_count = raw_position_count(account_raw, scope)?;
    let order_face = |surface, family, raw_commitment_keccak256, visible_record_count| {
        let unresolved_count = unknown_snapshot
            .unresolved
            .iter()
            .filter(|unknown| unknown.family == family)
            .count() as u64;
        HyperliquidRecoveryFace {
            surface,
            attempt_id: scope.attempt_id,
            private_generation: scope.private_generation,
            scope_commitment_keccak256: scope_commitment_keccak256.clone(),
            coverage: if unresolved_count == 0 {
                HyperliquidRecoveryCoverage::Complete {
                    raw_commitment_keccak256,
                    record_count: visible_record_count,
                }
            } else {
                HyperliquidRecoveryCoverage::BlockedUnknown {
                    raw_commitment_keccak256,
                    visible_record_count,
                    unresolved_commitment_keccak256: unknown_snapshot.commitment_keccak256.clone(),
                    unresolved_count,
                }
            },
        }
    };
    Ok(vec![
        complete(HyperliquidRecoverySurface::Account, account_commitment, 1),
        complete(
            HyperliquidRecoverySurface::Positions,
            position_commitment,
            position_count,
        ),
        order_face(
            HyperliquidRecoverySurface::UmOrder,
            HyperliquidOrderFamily::Regular,
            regular_commitment,
            raw_orders
                .iter()
                .filter(|order| order.0 == HyperliquidOrderFamily::Regular)
                .count() as u64,
        ),
        order_face(
            HyperliquidRecoverySurface::UmConditional,
            HyperliquidOrderFamily::Conditional,
            conditional_commitment,
            raw_orders
                .iter()
                .filter(|order| order.0 == HyperliquidOrderFamily::Conditional)
                .count() as u64,
        ),
        HyperliquidRecoveryFace {
            surface: HyperliquidRecoverySurface::UmAlgo,
            attempt_id: scope.attempt_id,
            private_generation: scope.private_generation,
            scope_commitment_keccak256: scope_commitment_keccak256.clone(),
            coverage: HyperliquidRecoveryCoverage::Unsupported {
                evidence_keccak256: hex_digest(Keccak256::digest(unsupported).into()),
                profile_version: HYPERLIQUID_RECOVERY_PROFILE_VERSION,
            },
        },
        complete(
            HyperliquidRecoverySurface::FillsCursor,
            fill_commitment,
            fill_pages.len() as u64,
        ),
    ])
}

fn scope_commitment(scope: &HyperliquidProbeCollectionScope) -> Result<String, HyperliquidError> {
    let encoded = serde_json::to_vec(scope).map_err(|_| HyperliquidError::CapabilityProbe)?;
    Ok(hex_digest(Keccak256::digest(encoded).into()))
}

fn raw_surface_commitment<'a>(
    surface: HyperliquidRecoverySurface,
    chunks: impl Iterator<Item = &'a [u8]>,
) -> Result<String, HyperliquidError> {
    let prefix = serde_json::to_vec(&surface).map_err(|_| HyperliquidError::CapabilityProbe)?;
    let mut digest = Keccak256::new();
    digest.update((prefix.len() as u64).to_be_bytes());
    digest.update(prefix);
    for chunk in chunks {
        digest.update((chunk.len() as u64).to_be_bytes());
        digest.update(chunk);
    }
    Ok(hex_digest(digest.finalize().into()))
}

fn metadata_for_universe(
    scope: &HyperliquidProbeCollectionScope,
    raw_payload: &[u8],
) -> Result<Vec<HyperliquidPerpMeta>, HyperliquidError> {
    scope
        .symbol_universe
        .iter()
        .map(|symbol| {
            let binding = GatewayBinding::new(
                VenueId::Hyperliquid,
                scope.binding.mode,
                scope.binding.trading_account_id.clone(),
                symbol.clone(),
            )
            .map_err(|_| HyperliquidError::CapabilityProbe)?;
            let read = HyperliquidReadBinding::new(
                HyperliquidGatewayBinding::new(binding)
                    .map_err(|_| HyperliquidError::CapabilityProbe)?,
                scope.user_address.clone(),
            )?;
            parse_perp_meta(raw_payload, &read)
        })
        .collect()
}

fn replay_fill_pages(
    meta: &HyperliquidPerpMeta,
    payload: &CapabilityProbePayload,
) -> Result<HyperliquidFillWindowEvidence, HyperliquidError> {
    let private_binding = HyperliquidPrivateStreamBinding::new(meta, payload.private_generation)?;
    let mut fill_probe = HyperliquidFillWindowProbe::new(
        &private_binding,
        payload.fill_window.begin_ms,
        payload.fill_window.end_ms,
    )?;
    let mut cursor = None;
    for raw in &payload.fill_raw_pages {
        let query = HyperliquidFillQuery::new(
            meta,
            payload.fill_window.begin_ms,
            payload.fill_window.end_ms,
            raw.limit,
            cursor,
        )?;
        let page = parse_user_fills_page(&raw.raw_payload, meta, &query)?;
        cursor = page.next_cursor.clone();
        fill_probe.ingest_page(&query, &page)?;
    }
    fill_probe.finish()
}

fn replay_raw_payloads(
    payload: &CapabilityProbePayload,
    credentials: &HyperliquidCredentials,
) -> Result<(), HyperliquidError> {
    if credentials.master_address() != payload.master_address
        || credentials.user_address() != payload.user_address
        || credentials.vault_address() != payload.vault_address.as_deref()
        || credentials.agent_address() != payload.agent_address
        || credentials.agent_name() != payload.agent_name
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    let read = HyperliquidReadBinding::new(
        HyperliquidGatewayBinding::new(payload.binding.clone())
            .map_err(|_| HyperliquidError::CapabilityProbe)?,
        payload.user_address.clone(),
    )?;
    let meta = parse_perp_meta(&payload.meta_raw_payload, &read)?;
    let universe_meta =
        metadata_for_universe(&payload.collection_scope, &payload.meta_raw_payload)?;
    if meta.scope.native_coin() != payload.native_coin
        || meta.asset_index != payload.asset_index
        || meta_commitment(&meta)? != payload.meta_commitment_keccak256
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    let account = parse_clearinghouse_snapshot(&payload.account_raw_payload, &meta)?;
    for candidate in &universe_meta {
        parse_clearinghouse_snapshot(&payload.account_raw_payload, candidate)?;
    }
    if account.exchange_time_ms != payload.account_exchange_time_ms
        || hex_digest(Keccak256::digest(&payload.account_raw_payload).into())
            != payload.account_commitment_keccak256
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    let orders = parse_frontend_open_orders_snapshot(
        &payload.orders_raw_payload,
        &meta,
        payload.orders_observed_ms,
    )?;
    for candidate in &universe_meta {
        parse_frontend_open_orders_snapshot(
            &payload.orders_raw_payload,
            candidate,
            payload.orders_observed_ms,
        )?;
    }
    validate_raw_exact_owners(
        &payload.orders_raw_payload,
        &payload.collection_scope,
        &payload.owner_snapshot,
    )?;
    validate_exact_owners(&orders, &payload.owner_snapshot)?;
    if hex_digest(Keccak256::digest(&payload.orders_raw_payload).into())
        != payload.orders_commitment_keccak256
    {
        return Err(HyperliquidError::CapabilityProbe);
    }

    let replayed_fill = replay_fill_pages(&meta, payload)?;
    for candidate in &universe_meta {
        replay_fill_pages(candidate, payload)?;
    }
    if replayed_fill.fill_count != payload.fill_window.fill_count
        || replayed_fill.fill_commitment_keccak256 != payload.fill_window.fill_commitment_keccak256
        || replayed_fill.begin_ms != payload.fill_window.begin_ms
        || replayed_fill.end_ms != payload.fill_window.end_ms
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    for unknown in &payload.unknown_orders {
        if unknown.observed_ms < payload.collection_scope.started_ms
            || unknown.observed_ms > payload.collection_scope.deadline_ms
        {
            return Err(HyperliquidError::CapabilityProbe);
        }
        match parse_order_status(&unknown.raw_payload, &meta, &unknown.lookup)? {
            HyperliquidOrderStatus::Unknown {
                lookup,
                native_identity,
                reason,
                ..
            } if lookup == unknown.lookup
                && native_identity == unknown.native_identity
                && reason == unknown.reason => {}
            HyperliquidOrderStatus::Unknown { .. } | HyperliquidOrderStatus::Known { .. } => {
                return Err(HyperliquidError::CapabilityProbe);
            }
        }
    }
    validate_unknown_correspondence(&payload.unknown_snapshot, &payload.unknown_orders)?;
    if recovery_faces(
        &payload.collection_scope,
        &payload.account_raw_payload,
        &orders,
        &payload.fill_raw_pages,
        &payload.unknown_snapshot,
    )? != payload.recovery_faces
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    Ok(())
}

fn sign_commitment(
    credentials: &HyperliquidCredentials,
    commitment: &str,
) -> Result<String, HyperliquidError> {
    let digest = Keccak256::digest(commitment.as_bytes());
    let (signature, _) = credentials
        .signing_key()?
        .sign_prehash_recoverable(&digest)
        .map_err(|_| HyperliquidError::CapabilityProbe)?;
    Ok(hex_bytes(&signature.to_bytes()))
}

fn verify_commitment_signature(
    credentials: &HyperliquidCredentials,
    commitment: &str,
    encoded_signature: &str,
) -> Result<(), HyperliquidError> {
    let bytes = decode_hex::<64>(encoded_signature)?;
    let signature = Signature::from_slice(&bytes).map_err(|_| HyperliquidError::CapabilityProbe)?;
    credentials
        .signing_key()?
        .verifying_key()
        .verify_prehash(&Keccak256::digest(commitment.as_bytes()), &signature)
        .map_err(|_| HyperliquidError::CapabilityProbe)
}

fn fill_commitment<'a>(
    fills: impl Iterator<Item = &'a HyperliquidFill>,
) -> Result<String, HyperliquidError> {
    #[derive(Serialize)]
    struct FillCommitment<'a> {
        fill: &'a venue_domain::domain::Fill,
        client_order_id: &'a venue_domain::domain::FieldState<String>,
    }
    let mut hasher = Keccak256::new();
    for fill in fills {
        let encoded = serde_json::to_vec(&FillCommitment {
            fill: &fill.fill,
            client_order_id: &fill.client_order_id,
        })
        .map_err(|_| HyperliquidError::CapabilityProbe)?;
        hasher.update((encoded.len() as u64).to_be_bytes());
        hasher.update(encoded);
    }
    Ok(hex_digest(hasher.finalize().into()))
}

fn commitment(payload: &CapabilityProbePayload) -> Result<String, HyperliquidError> {
    let encoded = serde_json::to_vec(payload).map_err(|_| HyperliquidError::CapabilityProbe)?;
    Ok(hex_digest(Keccak256::digest(encoded).into()))
}

fn meta_commitment(meta: &HyperliquidPerpMeta) -> Result<String, HyperliquidError> {
    let gateway = meta.scope.binding().gateway().gateway_binding();
    let encoded = serde_json::to_vec(&(
        gateway,
        meta.scope.user_address(),
        meta.scope.native_coin(),
        meta.asset_index,
        meta.size_decimals,
        meta.max_leverage,
        meta.trading_enabled,
    ))
    .map_err(|_| HyperliquidError::CapabilityProbe)?;
    Ok(hex_digest(Keccak256::digest(encoded).into()))
}

fn hex_digest(bytes: [u8; 32]) -> String {
    hex_bytes(&bytes)
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(*byte >> 4)]));
        output.push(char::from(HEX[usize::from(*byte & 0x0f)]));
    }
    output
}

fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N], HyperliquidError> {
    if value.len() != N * 2 {
        return Err(HyperliquidError::CapabilityProbe);
    }
    let mut decoded = [0; N];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let high = hex_nibble(pair[0]).ok_or(HyperliquidError::CapabilityProbe)?;
        let low = hex_nibble(pair[1]).ok_or(HyperliquidError::CapabilityProbe)?;
        decoded[index] = (high << 4) | low;
    }
    Ok(decoded)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn valid_config_digest(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn valid_hex_digest(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
}
