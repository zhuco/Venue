use serde_json::Value;
use thiserror::Error;

pub const USER_TRADES_PAGE_LIMIT: u16 = 1_000;
pub(crate) const USER_TRADES_MAX_PAGES: u32 = 10_000;
pub(crate) const USER_TRADES_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecentFillsCursor {
    pub observed_through_ms: u64,
    pub last_trade_id: Option<u64>,
    pub last_event_time_ms: Option<u64>,
}

/// One bounded request issued by the cursor paginator. It is public so a
/// local fixture or a credential-owning gateway can inspect the exact query
/// contract without constructing a network client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecentFillsPageRequest {
    pub start_time_ms: u64,
    pub end_time_ms: u64,
    pub from_id: Option<u64>,
    pub limit: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecentFillsReadback {
    pub payload: String,
    pub cursor: RecentFillsCursor,
    pub pages: u32,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RecentFillsPaginationError {
    #[error("Binance PAPI user-trades page is invalid or regressed")]
    FillPage,
    #[error("Binance PAPI user-trades pagination exceeded its bounded page budget")]
    FillPageLimit,
}

/// Applies the frozen MAS user-trades cursor contract to a page source. The
/// returned payload is one JSON array, so the caller can persist/publish it as
/// one batch before committing `readback.cursor`.
pub fn paginate_recent_fills<F, E>(
    initial_cursor: RecentFillsCursor,
    target_through_ms: u64,
    mut fetch: F,
) -> Result<RecentFillsReadback, E>
where
    F: FnMut(RecentFillsPageRequest) -> Result<String, E>,
    E: From<RecentFillsPaginationError>,
{
    validate_recent_fills_range(initial_cursor, target_through_ms)?;

    let mut cursor = initial_cursor;
    let mut values = Vec::new();
    let mut pages = 0_u32;
    let mut window_start = cursor.observed_through_ms;

    while window_start < target_through_ms {
        let window_end = window_start
            .saturating_add(USER_TRADES_WINDOW_MS)
            .min(target_through_ms);
        loop {
            pages = pages
                .checked_add(1)
                .ok_or(RecentFillsPaginationError::FillPageLimit)?;
            if pages > USER_TRADES_MAX_PAGES {
                return Err(RecentFillsPaginationError::FillPageLimit.into());
            }
            let from_id = match cursor.last_trade_id {
                Some(value) => Some(
                    value
                        .checked_add(1)
                        .ok_or(RecentFillsPaginationError::FillPage)?,
                ),
                None => None,
            };
            let request = RecentFillsPageRequest {
                start_time_ms: window_start,
                end_time_ms: window_end,
                from_id,
                limit: USER_TRADES_PAGE_LIMIT,
            };
            let payload = fetch(request)?;
            let (page, page_is_terminal) =
                advance_recent_fills_page(&mut cursor, request, &payload)?;
            values.extend(page);
            if page_is_terminal {
                cursor.observed_through_ms = window_end;
                break;
            }
        }
        window_start = window_end;
    }

    let payload =
        serde_json::to_string(&values).map_err(|_| RecentFillsPaginationError::FillPage)?;
    Ok(RecentFillsReadback {
        payload,
        cursor,
        pages,
    })
}

pub(crate) fn validate_recent_fills_range<E>(
    initial_cursor: RecentFillsCursor,
    target_through_ms: u64,
) -> Result<(), E>
where
    E: From<RecentFillsPaginationError>,
{
    if initial_cursor.observed_through_ms == 0
        || initial_cursor.last_trade_id.is_some() != initial_cursor.last_event_time_ms.is_some()
        || target_through_ms < initial_cursor.observed_through_ms
    {
        return Err(RecentFillsPaginationError::FillPage.into());
    }
    Ok(())
}

pub(crate) fn advance_recent_fills_page<E>(
    cursor: &mut RecentFillsCursor,
    request: RecentFillsPageRequest,
    payload: &str,
) -> Result<(Vec<Value>, bool), E>
where
    E: From<RecentFillsPaginationError>,
{
    let page = serde_json::from_str::<Value>(payload)
        .map_err(|_| RecentFillsPaginationError::FillPage)?
        .as_array()
        .cloned()
        .ok_or(RecentFillsPaginationError::FillPage)?;
    if page.len() > usize::from(USER_TRADES_PAGE_LIMIT) {
        return Err(RecentFillsPaginationError::FillPage.into());
    }
    let page_is_terminal = page.len() < usize::from(USER_TRADES_PAGE_LIMIT);
    let mut last_trade_id = cursor.last_trade_id;
    let mut last_event_time_ms = cursor.last_event_time_ms;
    for value in &page {
        let object = value
            .as_object()
            .ok_or(RecentFillsPaginationError::FillPage)?;
        let trade_id = user_trades_u64(object.get("id"))?;
        let event_time_ms = user_trades_u64(object.get("time"))?;
        if last_trade_id.is_some_and(|previous| trade_id <= previous)
            || last_event_time_ms.is_some_and(|previous| event_time_ms < previous)
            || event_time_ms < request.start_time_ms
            || event_time_ms > request.end_time_ms
        {
            return Err(RecentFillsPaginationError::FillPage.into());
        }
        last_trade_id = Some(trade_id);
        last_event_time_ms = Some(event_time_ms);
    }
    cursor.last_trade_id = last_trade_id;
    cursor.last_event_time_ms = last_event_time_ms;
    Ok((page, page_is_terminal))
}

fn user_trades_u64<E>(value: Option<&Value>) -> Result<u64, E>
where
    E: From<RecentFillsPaginationError>,
{
    match value {
        Some(Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| RecentFillsPaginationError::FillPage.into()),
        Some(Value::String(value)) => value
            .parse()
            .map_err(|_| RecentFillsPaginationError::FillPage.into()),
        _ => Err(RecentFillsPaginationError::FillPage.into()),
    }
}
