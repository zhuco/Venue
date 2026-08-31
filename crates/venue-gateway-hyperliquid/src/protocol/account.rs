//! Account-wide reads reuse the instrument parsers but never treat the selected coin as the
//! account universe. Fill restart cursors retain an overlap anchor to detect retention gaps.

use super::*;
use venue_execution::{SignedAccountBalance, SignedAccountPositionFact};

#[cfg(test)]
mod tests;

pub(crate) type PerpUniverse = BTreeMap<String, HyperliquidPerpMeta>;

pub(crate) fn parse_universe(
    payload: &[u8],
    binding: &HyperliquidReadBinding,
) -> Result<PerpUniverse, HyperliquidError> {
    let response: PerpMetaResponse =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    let mut universe = BTreeMap::new();
    for (index, row) in response.universe.into_iter().enumerate() {
        if row.name.contains([':', '/', '@']) || row.max_leverage == 0 || row.sz_decimals > 18 {
            return Err(HyperliquidError::Payload);
        }
        let mut gateway = binding.gateway().gateway_binding().clone();
        gateway.symbol = Symbol::new(&row.name, "USDC").map_err(|_| HyperliquidError::Binding)?;
        let read = HyperliquidReadBinding::new(
            crate::HyperliquidGatewayBinding::new(gateway)
                .map_err(|_| HyperliquidError::Binding)?,
            binding.user_address(),
        )?;
        let meta = HyperliquidPerpMeta {
            scope: HyperliquidPayloadScope {
                binding: read,
                native_coin: row.name.clone(),
            },
            asset_index: u32::try_from(index).map_err(|_| HyperliquidError::Payload)?,
            size_decimals: row.sz_decimals,
            max_leverage: row.max_leverage,
            trading_enabled: !row.is_delisted,
        };
        if universe.insert(row.name, meta).is_some() {
            return Err(HyperliquidError::Payload);
        }
    }
    if !universe.contains_key(binding.gateway().gateway_binding().symbol.base()) {
        return Err(HyperliquidError::Binding);
    }
    Ok(universe)
}

pub(super) fn perp_scope<'a>(
    coin: &str,
    universe: &'a PerpUniverse,
) -> Result<Option<&'a HyperliquidPayloadScope>, HyperliquidError> {
    if coin.starts_with('@') || coin.contains('/') {
        return Ok(None);
    }
    universe
        .get(coin)
        .map(|meta| Some(&meta.scope))
        .ok_or(HyperliquidError::Binding)
}

pub(crate) struct AccountState {
    pub exchange_time_ms: u64,
    pub balance: SignedAccountBalance,
    pub positions: Vec<SignedAccountPositionFact>,
}

pub(crate) fn parse_account_twap_fills(
    payload: &[u8],
    universe: &PerpUniverse,
) -> Result<Vec<Fill>, HyperliquidError> {
    let rows: Vec<UserTwapSliceFillRow> =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if rows.len() >= HYPERLIQUID_FILL_RESPONSE_LIMIT {
        return Err(HyperliquidError::Payload);
    }
    let mut fills = BTreeMap::new();
    for row in rows {
        if row.twap_id == 0 {
            return Err(HyperliquidError::Payload);
        }
        let Some(scope) = perp_scope(&row.fill.coin, universe)? else {
            continue;
        };
        let fill = normalize_fill(row.fill, scope)?.fill;
        if fills.insert(fill.fill_id.clone(), fill).is_some() {
            return Err(HyperliquidError::Payload);
        }
    }
    Ok(fills.into_values().collect())
}

pub(crate) fn parse_account_state(
    payload: &[u8],
    universe: &PerpUniverse,
    selected: &HyperliquidPerpMeta,
) -> Result<AccountState, HyperliquidError> {
    let state: ClearinghouseState =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if state.time == 0 {
        return Err(HyperliquidError::Payload);
    }
    let balance = SignedAccountBalance {
        asset: Asset::new("USDC").map_err(|_| HyperliquidError::Payload)?,
        equity: decimal(&state.margin_summary.account_value)?,
        available_margin: Some(decimal(&state.withdrawable)?),
    };
    let mut positions = BTreeMap::new();
    for row in state.asset_positions {
        let meta = universe
            .get(&row.position.coin)
            .ok_or(HyperliquidError::Binding)?;
        let coin = row.position.coin.clone();
        if positions.contains_key(&coin) {
            return Err(HyperliquidError::Payload);
        }
        let position = normalize_position(row, meta)?;
        let (quantity, entry_price, mark_price) = match position {
            Some(position) => (
                if position.side == PositionSide::Short {
                    -position.quantity
                } else {
                    position.quantity
                },
                position.entry_price.map(|price| price.value()),
                position.mark_price.map(|price| price.value()),
            ),
            None => (Decimal::ZERO, None, None),
        };
        positions.insert(
            coin,
            SignedAccountPositionFact {
                symbol: meta.scope.symbol().clone(),
                position_side: PositionSide::Net,
                quantity,
                entry_price,
                mark_price,
            },
        );
    }
    // clearinghouseState lists the complete nonzero position set; an absent selected coin is a
    // proven flat Net leg, not an empty response guessed to mean zero.
    positions
        .entry(selected.scope.native_coin().to_owned())
        .or_insert_with(|| SignedAccountPositionFact {
            symbol: selected.scope.symbol().clone(),
            position_side: PositionSide::Net,
            quantity: Decimal::ZERO,
            entry_price: None,
            mark_price: None,
        });
    Ok(AccountState {
        exchange_time_ms: state.time,
        balance,
        positions: positions.into_values().collect(),
    })
}

pub(crate) fn parse_account_orders(
    payload: &[u8],
    universe: &PerpUniverse,
    observed_ms: u64,
) -> Result<Vec<HyperliquidOpenOrder>, HyperliquidError> {
    let rows: Vec<FrontendOrderRow> =
        serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
    if rows.len() >= MAX_FRONTEND_ORDERS {
        return Err(HyperliquidError::OrderFamily);
    }
    let mut orders = Vec::new();
    let mut client_ids = BTreeMap::new();
    for row in flatten_frontend_rows(rows)? {
        let Some(scope) = perp_scope(&row.coin, universe)? else {
            continue;
        };
        if row.timestamp == 0 || row.timestamp > observed_ms {
            return Err(HyperliquidError::Payload);
        }
        let order = normalize_open_order(row, scope.symbol())?;
        if let FieldState::Known(id) = &order.order.client_order_id
            && client_ids.insert(id.to_ascii_lowercase(), ()).is_some()
        {
            return Err(HyperliquidError::Payload);
        }
        orders.push(order);
    }
    Ok(orders)
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FillKey {
    time_ms: u64,
    coin: String,
    trade_id: u64,
}

impl FillKey {
    fn row(row: &UserFillRow) -> Result<Self, HyperliquidError> {
        if row.time == 0 || row.tid == 0 || row.coin.is_empty() {
            return Err(HyperliquidError::Payload);
        }
        Ok(Self {
            time_ms: row.time,
            coin: row.coin.clone(),
            trade_id: row.tid,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumeCursor {
    version: u8,
    account: String,
    user: String,
    through_ms: u64,
    anchor: Option<FillKey>,
}

pub(crate) struct FillCollection {
    previous: ResumeCursor,
    end_ms: u64,
    anchor_seen: bool,
    seen: BTreeMap<FillKey, Vec<u8>>,
    fills: BTreeMap<String, Fill>,
    after: Option<HyperliquidFillCursor>,
    complete: bool,
}

impl FillCollection {
    pub fn resume(
        previous: Option<&str>,
        meta: &HyperliquidPerpMeta,
        end_ms: u64,
    ) -> Result<Self, HyperliquidError> {
        if previous.is_some_and(|cursor| cursor.len() > 4096) {
            return Err(HyperliquidError::Payload);
        }
        let account = &meta
            .scope
            .binding()
            .gateway()
            .gateway_binding()
            .trading_account_id;
        let cursor = match previous {
            None => ResumeCursor {
                version: 2,
                account: account.clone(),
                user: meta.scope.user_address().to_owned(),
                through_ms: 1,
                anchor: None,
            },
            Some(value) if value.starts_with("hl-account-fills-v2:") => serde_json::from_str(
                value
                    .strip_prefix("hl-account-fills-v2:")
                    .ok_or(HyperliquidError::Payload)?,
            )
            .map_err(|_| HyperliquidError::Payload)?,
            // The old cursor covered only the selected coin. A one-time full visible replay is
            // necessary before claiming account-wide coverage; do not silently skip siblings.
            Some(value) if value.starts_with("hl-visible-fills-v1:") => {
                let rest = value
                    .strip_prefix("hl-visible-fills-v1:")
                    .ok_or(HyperliquidError::Payload)?;
                let (time, _) = rest.split_once(':').ok_or(HyperliquidError::Payload)?;
                let through_ms = time.parse::<u64>().map_err(|_| HyperliquidError::Payload)?;
                ResumeCursor {
                    version: 2,
                    account: account.clone(),
                    user: meta.scope.user_address().to_owned(),
                    through_ms,
                    anchor: None,
                }
            }
            Some(_) => return Err(HyperliquidError::Payload),
        };
        if cursor.version != 2
            || cursor.account != *account
            || cursor.user != meta.scope.user_address()
            || cursor.through_ms == 0
            || cursor.through_ms > end_ms
            || cursor.anchor.as_ref().is_some_and(|anchor| {
                anchor.time_ms == 0
                    || anchor.time_ms > cursor.through_ms
                    || anchor.trade_id == 0
                    || anchor.coin.is_empty()
            })
        {
            return Err(HyperliquidError::Binding);
        }
        Ok(Self {
            anchor_seen: cursor.anchor.is_none(),
            previous: cursor,
            end_ms,
            seen: BTreeMap::new(),
            fills: BTreeMap::new(),
            after: None,
            complete: false,
        })
    }

    pub fn query(
        &self,
        meta: &HyperliquidPerpMeta,
    ) -> Result<HyperliquidFillQuery, HyperliquidError> {
        HyperliquidFillQuery::new(
            meta,
            self.previous.anchor.as_ref().map_or(1, |key| key.time_ms),
            self.end_ms,
            HYPERLIQUID_FILL_RESPONSE_LIMIT,
            self.after.clone(),
        )
    }

    pub fn ingest(
        &mut self,
        payload: &[u8],
        meta: &HyperliquidPerpMeta,
        universe: &PerpUniverse,
    ) -> Result<bool, HyperliquidError> {
        if self.complete {
            return Err(HyperliquidError::Payload);
        }
        let query = self.query(meta)?;
        let page = parse_user_fills_page_scoped(payload, meta, &query, Some(universe))?;
        let rows: Vec<UserFillRow> =
            serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
        let raw: Vec<serde_json::Value> =
            serde_json::from_slice(payload).map_err(|_| HyperliquidError::Payload)?;
        for (row, raw) in rows.iter().zip(raw) {
            let key = FillKey::row(row)?;
            let bytes = serde_json::to_vec(&raw).map_err(|_| HyperliquidError::Payload)?;
            if let Some(prior) = self.seen.insert(key.clone(), bytes.clone())
                && prior != bytes
            {
                return Err(HyperliquidError::Payload);
            }
            self.anchor_seen |= self.previous.anchor.as_ref() == Some(&key);
        }
        // Reaching retention makes all-time/bootstrap coverage unprovable, even if the next
        // request returns empty. With a retained overlap anchor continuity is still provable.
        if self.seen.len() >= HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT
            && self.previous.anchor.is_none()
        {
            return Err(HyperliquidError::Payload);
        }
        for item in page.fills {
            if let Some(prior) = self
                .fills
                .insert(item.fill.fill_id.clone(), item.fill.clone())
                && prior != item.fill
            {
                return Err(HyperliquidError::Payload);
            }
        }
        self.after = page.next_cursor;
        self.complete = page.complete;
        if self.complete && !self.anchor_seen {
            return Err(HyperliquidError::Payload);
        }
        Ok(self.complete)
    }

    pub fn finish(self) -> Result<(Vec<Fill>, String), HyperliquidError> {
        if !self.complete || !self.anchor_seen {
            return Err(HyperliquidError::Payload);
        }
        let cursor = ResumeCursor {
            through_ms: self.end_ms,
            anchor: self
                .seen
                .last_key_value()
                .map(|(key, _)| key.clone())
                .or(self.previous.anchor),
            ..self.previous
        };
        let encoded = serde_json::to_string(&cursor).map_err(|_| HyperliquidError::Payload)?;
        Ok((
            self.fills.into_values().collect(),
            format!("hl-account-fills-v2:{encoded}"),
        ))
    }
}
