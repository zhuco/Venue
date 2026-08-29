use super::*;
/// Applies the frozen MAS user-trades cursor contract to a page source. The
/// returned payload is one JSON array, so the caller can persist/publish it as
/// one batch before committing `readback.cursor`.
pub fn paginate_recent_fills<F>(
    initial_cursor: RecentFillsCursor,
    target_through_ms: u64,
    mut fetch: F,
) -> Result<RecentFillsReadback, PrivateError>
where
    F: FnMut(RecentFillsPageRequest) -> Result<String, PrivateError>,
{
    if initial_cursor.observed_through_ms == 0
        || initial_cursor.last_trade_id.is_some() != initial_cursor.last_event_time_ms.is_some()
        || target_through_ms < initial_cursor.observed_through_ms
    {
        return Err(PrivateError::FillPage);
    }

    let mut cursor = initial_cursor;
    let mut values = Vec::new();
    let mut pages = 0_u32;
    let mut window_start = cursor.observed_through_ms;

    while window_start < target_through_ms {
        let window_end = window_start
            .saturating_add(USER_TRADES_WINDOW_MS)
            .min(target_through_ms);
        loop {
            pages = pages.checked_add(1).ok_or(PrivateError::FillPageLimit)?;
            if pages > USER_TRADES_MAX_PAGES {
                return Err(PrivateError::FillPageLimit);
            }
            let from_id = match cursor.last_trade_id {
                Some(value) => Some(value.checked_add(1).ok_or(PrivateError::FillPage)?),
                None => None,
            };
            let request = RecentFillsPageRequest {
                start_time_ms: window_start,
                end_time_ms: window_end,
                from_id,
                limit: USER_TRADES_PAGE_LIMIT,
            };
            let payload = fetch(request)?;
            let page = serde_json::from_str::<Value>(&payload)
                .map_err(|_| PrivateError::FillPage)?
                .as_array()
                .cloned()
                .ok_or(PrivateError::FillPage)?;
            if page.len() > usize::from(USER_TRADES_PAGE_LIMIT) {
                return Err(PrivateError::FillPage);
            }
            let page_is_terminal = page.len() < usize::from(USER_TRADES_PAGE_LIMIT);

            let mut last_trade_id = cursor.last_trade_id;
            let mut last_event_time_ms = cursor.last_event_time_ms;
            for value in &page {
                let object = value.as_object().ok_or(PrivateError::FillPage)?;
                let trade_id = user_trades_u64(object.get("id"))?;
                let event_time_ms = user_trades_u64(object.get("time"))?;
                if last_trade_id.is_some_and(|previous| trade_id <= previous)
                    || last_event_time_ms.is_some_and(|previous| event_time_ms < previous)
                    || event_time_ms < window_start
                    || event_time_ms > window_end
                {
                    return Err(PrivateError::FillPage);
                }
                last_trade_id = Some(trade_id);
                last_event_time_ms = Some(event_time_ms);
            }
            values.extend(page);
            cursor.last_trade_id = last_trade_id;
            cursor.last_event_time_ms = last_event_time_ms;
            if page_is_terminal {
                cursor.observed_through_ms = window_end;
                break;
            }
        }
        window_start = window_end;
    }

    let payload = serde_json::to_string(&values).map_err(|_| PrivateError::FillPage)?;
    Ok(RecentFillsReadback {
        payload,
        cursor,
        pages,
    })
}

fn user_trades_u64(value: Option<&Value>) -> Result<u64, PrivateError> {
    match value {
        Some(Value::Number(value)) => value.as_u64().ok_or(PrivateError::FillPage),
        Some(Value::String(value)) => value.parse().map_err(|_| PrivateError::FillPage),
        _ => Err(PrivateError::FillPage),
    }
}
