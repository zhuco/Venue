use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use venue_domain::domain::OrderState;
use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding, VenueId};

use crate::{
    HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT, HyperliquidAccountSnapshot, HyperliquidActionKind,
    HyperliquidCredentials, HyperliquidError, HyperliquidFill, HyperliquidFillCoverage,
    HyperliquidFillCursor, HyperliquidFillPage, HyperliquidFillQuery,
    HyperliquidOpenOrdersSnapshot, HyperliquidPerpMeta, HyperliquidPrivateStreamBinding,
    HyperliquidProbeActionReceipt, validate_frontend_open_orders_snapshot,
};

pub const HYPERLIQUID_CAPABILITY_PROBE_SCHEMA: u16 = 1;
pub const HYPERLIQUID_CAPABILITY_PROBE_MAX_TTL_MS: u64 = 60_000;

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CapabilityProbePayload {
    schema_version: u16,
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
    orders_observed_ms: u64,
    meta_commitment_keccak256: String,
    account_commitment_keccak256: String,
    orders_commitment_keccak256: String,
    fill_window: HyperliquidFillWindowEvidence,
    private_stream: HyperliquidPrivateStreamProbeEvidence,
    actions: [HyperliquidProbeActionReceipt; 3],
    withdrawals_permitted: bool,
}

/// Immutable, serializable commitment to a complete active adapter probe. It is candidate
/// capability evidence only: it does not acquire a writer, create a WAL, or change the crate's
/// static empty capability set.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HyperliquidCapabilityProbeEvidence {
    payload: CapabilityProbePayload,
    commitment_keccak256: String,
}

impl HyperliquidCapabilityProbeEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn issue(
        meta: &HyperliquidPerpMeta,
        credentials: &HyperliquidCredentials,
        account: &HyperliquidAccountSnapshot,
        orders: &HyperliquidOpenOrdersSnapshot,
        fill_window: HyperliquidFillWindowEvidence,
        private_stream: HyperliquidPrivateStreamProbeEvidence,
        connection_generation: u64,
        version: u64,
        observed_ms: u64,
        expires_ms: u64,
        actions: [HyperliquidProbeActionReceipt; 3],
    ) -> Result<Self, HyperliquidError> {
        validate_frontend_open_orders_snapshot(orders, meta)?;
        let payload = CapabilityProbePayload {
            schema_version: HYPERLIQUID_CAPABILITY_PROBE_SCHEMA,
            binding: meta.scope.binding().gateway().gateway_binding().clone(),
            version,
            observed_ms,
            expires_ms,
            connection_generation,
            private_generation: private_stream.private_generation,
            master_address: credentials.master_address().to_owned(),
            user_address: credentials.user_address().to_owned(),
            vault_address: credentials.vault_address().map(str::to_owned),
            agent_address: credentials.agent_address().to_owned(),
            agent_name: credentials.agent_name().to_owned(),
            native_coin: meta.scope.native_coin().to_owned(),
            asset_index: meta.asset_index,
            account_exchange_time_ms: account.exchange_time_ms,
            orders_observed_ms: orders.observed_at_ms,
            meta_commitment_keccak256: meta_commitment(meta)?,
            account_commitment_keccak256: account_commitment(account)?,
            orders_commitment_keccak256: hex_digest(Keccak256::digest(&orders.raw_payload).into()),
            fill_window,
            private_stream,
            actions: canonical_actions(actions)?,
            withdrawals_permitted: false,
        };
        validate_probe_payload(&payload, meta, account, orders)?;
        let commitment_keccak256 = commitment(&payload)?;
        Ok(Self {
            payload,
            commitment_keccak256,
        })
    }

    pub fn verify(&self) -> Result<(), HyperliquidError> {
        validate_persisted_payload(&self.payload)?;
        if commitment(&self.payload)? != self.commitment_keccak256 {
            return Err(HyperliquidError::CapabilityProbe);
        }
        Ok(())
    }

    /// Produces only a candidate common snapshot. `PLACE_MARKET` is intentionally absent because
    /// the shared API cannot yet distinguish exposure-increasing market orders from the proven IOC
    /// reduce-only surface. `WITHDRAW` is always absent.
    pub fn candidate_capability_snapshot(
        &self,
        expected_binding: &GatewayBinding,
        now_ms: u64,
    ) -> Result<CapabilitySnapshot, HyperliquidError> {
        self.verify()?;
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
                | CapabilityFlags::PRIVATE_STREAM
                | CapabilityFlags::TRADE
                | CapabilityFlags::PLACE_LIMIT
                | CapabilityFlags::CANCEL,
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
}

fn canonical_actions(
    actions: [HyperliquidProbeActionReceipt; 3],
) -> Result<[HyperliquidProbeActionReceipt; 3], HyperliquidError> {
    let find = |kind| {
        actions
            .iter()
            .find(|receipt| receipt.kind() == kind)
            .cloned()
            .ok_or(HyperliquidError::CapabilityProbe)
    };
    let result = [
        find(HyperliquidActionKind::AloPlace)?,
        find(HyperliquidActionKind::Cancel)?,
        find(HyperliquidActionKind::IocReduceOnly)?,
    ];
    if actions.iter().any(|left| {
        actions
            .iter()
            .filter(|right| right.kind() == left.kind())
            .count()
            != 1
    }) {
        return Err(HyperliquidError::CapabilityProbe);
    }
    Ok(result)
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
    if payload.schema_version != HYPERLIQUID_CAPABILITY_PROBE_SCHEMA
        || payload.binding.venue != VenueId::Hyperliquid
        || payload.binding.validate().is_err()
        || payload.binding.symbol.quote() != "USDC"
        || payload.version == 0
        || payload.connection_generation == 0
        || payload.private_generation == 0
        || ttl.is_none_or(|value| value == 0 || value > HYPERLIQUID_CAPABILITY_PROBE_MAX_TTL_MS)
        || payload.account_exchange_time_ms == 0
        || payload.account_exchange_time_ms > payload.observed_ms
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
    validate_embedded_scope(payload)?;
    validate_actions(payload)
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

fn validate_actions(payload: &CapabilityProbePayload) -> Result<(), HyperliquidError> {
    let [alo, cancel, ioc] = &payload.actions;
    let common = |receipt: &HyperliquidProbeActionReceipt| {
        receipt.gateway_binding() == &payload.binding
            && receipt.user_address() == payload.user_address
            && receipt.vault_address() == payload.vault_address.as_deref()
            && receipt.native_coin() == payload.native_coin
            && receipt.private_generation() == payload.private_generation
            && receipt.nonce() > 0
            && receipt.connection_id() != [0; 32]
            && receipt.order_id() > 0
            && receipt.exchange_time_ms() > 0
            && receipt.exchange_time_ms() <= payload.observed_ms
    };
    if !common(alo)
        || !common(cancel)
        || !common(ioc)
        || alo.kind() != HyperliquidActionKind::AloPlace
        || cancel.kind() != HyperliquidActionKind::Cancel
        || ioc.kind() != HyperliquidActionKind::IocReduceOnly
        || !matches!(alo.state(), OrderState::New | OrderState::PartiallyFilled)
        || cancel.state() != OrderState::Cancelled
        || ioc.state() != OrderState::Filled
        || cancel.order_id() != alo.order_id()
        || !(alo.nonce() < cancel.nonce() && cancel.nonce() < ioc.nonce())
        || alo.connection_id() == cancel.connection_id()
        || alo.connection_id() == ioc.connection_id()
        || cancel.connection_id() == ioc.connection_id()
    {
        return Err(HyperliquidError::CapabilityProbe);
    }
    Ok(())
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

fn account_commitment(account: &HyperliquidAccountSnapshot) -> Result<String, HyperliquidError> {
    let encoded = serde_json::to_vec(&(
        account.scope.binding().gateway().gateway_binding(),
        account.scope.user_address(),
        account.scope.native_coin(),
        account.exchange_time_ms,
        &account.balance,
        &account.position,
    ))
    .map_err(|_| HyperliquidError::CapabilityProbe)?;
    Ok(hex_digest(Keccak256::digest(encoded).into()))
}

fn hex_digest(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn valid_hex_digest(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
}
