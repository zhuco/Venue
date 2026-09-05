use std::collections::BTreeSet;

use serde_json::Value;
use venue_domain::domain::Order;
use venue_execution::AccountHostValidationError;

use super::{
    GateAccountGatewayError, fetch_private_page, now_ms, page_cursor, snapshot_id, snapshot_read,
};
use crate::{
    GATE_PRIVATE_MAX_PAGES, GATE_PRIVATE_PAGE_LIMIT, GateContractRules, GateCredentials,
    GateFillsCursor, GateGatewayBinding, GateHttpTransport, GatePrivateReadSource,
    collect_regular_order_pages, endpoints, parse_account_balance, parse_dual_position_mode,
};

const SNAPSHOT_FILLS_CURSOR_PREFIX: &str = "gate-fills-v2|";
const SNAPSHOT_FILLS_OVERLAP_SECS: u64 = 60;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GateAccountPrivateState {
    pub generation: u64,
    pub attempt: u64,
    pub user_id: String,
    pub regular_orders: Vec<Order>,
}

/// Collects only the current signed surfaces needed before a mutation or websocket reconnect.
/// Historical fills belong to the durable account snapshot and must not make this preflight
/// unbounded for accounts with a long trading history.
pub(super) async fn fetch_private_state(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    attempt: u64,
) -> Result<GateAccountPrivateState, GateAccountGatewayError> {
    if attempt == 0 {
        return Err(GateAccountGatewayError::Readback);
    }
    let started_at_ms = now_ms()?;
    let deadline = started_at_ms
        .checked_add(3_000)
        .ok_or(GateAccountGatewayError::Clock)?;
    let account = fetch_private_page(
        transport,
        binding,
        credentials,
        rules,
        attempt,
        GatePrivateReadSource::Account,
        GateFillsCursor::default(),
    )
    .await?;
    let positions = fetch_private_page(
        transport,
        binding,
        credentials,
        rules,
        attempt,
        GatePrivateReadSource::DualPositions,
        GateFillsCursor::default(),
    )
    .await?;
    let mut order_pages = Vec::new();
    let mut cursor = GateFillsCursor::default();
    let mut terminal = false;
    for _ in 0..GATE_PRIVATE_MAX_PAGES {
        let response = fetch_private_page(
            transport,
            binding,
            credentials,
            rules,
            attempt,
            GatePrivateReadSource::RegularOrders,
            cursor,
        )
        .await?;
        let (next, complete) = page_cursor(&response.payload)?;
        order_pages.push(response);
        if complete {
            terminal = true;
            break;
        }
        cursor = GateFillsCursor::new(next).map_err(|_| GateAccountGatewayError::Readback)?;
    }
    let validated_at_ms = now_ms()?;
    if !terminal
        || validated_at_ms >= deadline
        || [&account, &positions]
            .into_iter()
            .chain(order_pages.iter())
            .any(|raw| {
                raw.binding != *binding.gateway_binding()
                    || raw.generation != rules.instrument.generation
                    || raw.attempt != attempt
                    || raw.requested_at_ms < started_at_ms
                    || raw.received_at_ms < raw.requested_at_ms
                    || raw.received_at_ms > validated_at_ms
            })
    {
        return Err(GateAccountGatewayError::Readback);
    }
    let account_value: Value =
        serde_json::from_str(&account.payload).map_err(|_| GateAccountGatewayError::Readback)?;
    if parse_dual_position_mode(&account_value) != Ok(true)
        || parse_account_balance(&account_value).is_err()
    {
        return Err(GateAccountGatewayError::Readback);
    }
    let (user_id, _) = crate::private_surface::parse_two_legs(&positions.payload, rules)
        .map_err(|_| GateAccountGatewayError::Readback)?;
    let regular = collect_regular_order_pages(
        order_pages.iter().map(|page| page.payload.as_str()),
        &rules.instrument.symbol,
        rules,
    )
    .map_err(|_| GateAccountGatewayError::Readback)?;
    Ok(GateAccountPrivateState {
        generation: rules.instrument.generation,
        attempt,
        user_id,
        regular_orders: regular.orders,
    })
}

/// Gate recommends its time-range endpoint for iterating personal futures fills. The cursor is a
/// signed-snapshot time watermark, and each read overlaps it so late-arriving fills cannot fall
/// between snapshots. The initial read intentionally establishes a recent tail rather than
/// replaying an account's entire history; unresolved durable commands are reconciled separately.
pub(super) async fn fetch_snapshot_fills(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    observed_at_ms: u64,
    previous_cursor: Option<&str>,
) -> Result<(Vec<Value>, String), AccountHostValidationError> {
    if observed_at_ms == 0 {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let previous = parse_snapshot_fills_cursor(previous_cursor)?;
    if previous.is_some_and(|value| value > observed_at_ms) {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let anchor_ms = previous.unwrap_or(observed_at_ms);
    let from_sec = anchor_ms
        .checked_div(1_000)
        .unwrap_or_default()
        .saturating_sub(SNAPSHOT_FILLS_OVERLAP_SECS);
    let to_sec = observed_at_ms
        .checked_div(1_000)
        .and_then(|value| value.checked_add(1))
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    let mut rows = Vec::new();
    let mut seen_ids = BTreeSet::new();
    for page in 0..GATE_PRIVATE_MAX_PAGES {
        let offset = page
            .checked_mul(GATE_PRIVATE_PAGE_LIMIT)
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        let query = snapshot_fills_query(from_sec, to_sec, offset)?;
        let payload = snapshot_read(
            transport,
            binding,
            credentials,
            rules,
            endpoints::FUTURES_FILLS_TIMERANGE,
            &query,
        )
        .await?;
        let normalized = normalize_timerange_page(&payload, &mut seen_ids)?;
        let terminal = normalized.len() < GATE_PRIVATE_PAGE_LIMIT;
        rows.extend(normalized);
        if terminal {
            return Ok((rows, snapshot_fills_cursor(observed_at_ms)?));
        }
    }
    Err(AccountHostValidationError::SignedSnapshot)
}

fn snapshot_fills_query(
    from_sec: u64,
    to_sec: u64,
    offset: usize,
) -> Result<String, AccountHostValidationError> {
    if to_sec <= from_sec {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    Ok(format!(
        "from={from_sec}&to={to_sec}&limit={GATE_PRIVATE_PAGE_LIMIT}&offset={offset}"
    ))
}

fn normalize_timerange_page(
    payload: &str,
    seen_ids: &mut BTreeSet<String>,
) -> Result<Vec<Value>, AccountHostValidationError> {
    let page: Vec<Value> =
        serde_json::from_str(payload).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    if page.len() > GATE_PRIVATE_PAGE_LIMIT {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    page.into_iter()
        .map(|row| {
            let mut item = row
                .as_object()
                .cloned()
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            if item.contains_key("id") {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            let id = snapshot_id(item.get("trade_id"))?;
            if !seen_ids.insert(id.clone()) {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            item.insert("id".to_owned(), Value::String(id));
            Ok(Value::Object(item))
        })
        .collect()
}

pub(super) fn snapshot_fills_cursor(
    observed_at_ms: u64,
) -> Result<String, AccountHostValidationError> {
    if observed_at_ms == 0 {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    Ok(format!("{SNAPSHOT_FILLS_CURSOR_PREFIX}{observed_at_ms}"))
}

pub(super) fn parse_snapshot_fills_cursor(
    value: Option<&str>,
) -> Result<Option<u64>, AccountHostValidationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let watermark = value
        .strip_prefix(SNAPSHOT_FILLS_CURSOR_PREFIX)
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or(AccountHostValidationError::SignedSnapshot)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    Ok(Some(watermark))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_cursor_rejects_legacy_or_future_ambiguous_values() {
        assert_eq!(
            parse_snapshot_fills_cursor(Some("gate-fills-v2|1700000000123")),
            Ok(Some(1_700_000_000_123))
        );
        assert!(parse_snapshot_fills_cursor(Some("gate-fills-v1|123")).is_err());
        assert!(parse_snapshot_fills_cursor(Some("gate-fills-v2|0")).is_err());
    }

    #[test]
    fn timerange_page_normalizes_trade_id_and_rejects_overlap()
    -> Result<(), AccountHostValidationError> {
        let mut seen = BTreeSet::new();
        let rows =
            normalize_timerange_page(r#"[{"trade_id":"124","contract":"DOGE_USDT"}]"#, &mut seen)?;
        assert_eq!(rows[0].get("id").and_then(Value::as_str), Some("124"));
        assert!(
            normalize_timerange_page(r#"[{"trade_id":"124","contract":"DOGE_USDT"}]"#, &mut seen)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn timerange_query_is_fixed_and_bounded() {
        assert_eq!(
            snapshot_fills_query(1_699_999_940, 1_700_000_001, 200),
            Ok("from=1699999940&to=1700000001&limit=100&offset=200".to_owned())
        );
        assert_eq!(
            snapshot_fills_cursor(1_700_000_000_123),
            Ok("gate-fills-v2|1700000000123".to_owned())
        );
    }
}
