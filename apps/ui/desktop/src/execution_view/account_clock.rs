//! Display-only server time; never adjusts order timestamps or signed facts.

#[derive(Clone, Debug)]
pub(crate) struct AccountClock {
    server_ms: u64,
    local_ms: u64,
    #[cfg(not(target_arch = "wasm32"))]
    received: std::time::Instant,
}

impl AccountClock {
    pub(crate) fn from_response(
        headers: &reqwest::header::HeaderMap,
        elapsed_ms: u64,
    ) -> Option<Self> {
        // HTTP Date has one-second precision. Bound network uncertainty so it cannot
        // turn a slow response into a reliable clock calibration.
        if elapsed_ms > 2_000 || headers.get(reqwest::header::AGE).is_some() {
            return None;
        }
        let server_ms = parse_date(headers.get(reqwest::header::DATE)?.to_str().ok()?)?
            .checked_add(500 + elapsed_ms / 2)?;
        Some(Self {
            server_ms,
            local_ms: crate::account_center::now_ms(),
            #[cfg(not(target_arch = "wasm32"))]
            received: std::time::Instant::now(),
        })
    }

    pub(crate) fn now_ms(&self, local_now: u64) -> Option<u64> {
        #[cfg(not(target_arch = "wasm32"))]
        let elapsed = {
            let _ = (local_now, self.local_ms);
            u64::try_from(self.received.elapsed().as_millis()).ok()?
        };
        #[cfg(target_arch = "wasm32")]
        let elapsed = local_now.checked_sub(self.local_ms)?;
        // The stream reconnects every five minutes; an old session is not a clock source.
        (elapsed <= 600_000).then(|| self.server_ms.saturating_add(elapsed))
    }
}

fn parse_date(value: &str) -> Option<u64> {
    let parts: Vec<_> = value.split_ascii_whitespace().collect();
    if parts.len() != 6 || parts[5] != "GMT" || !parts[0].ends_with(',') {
        return None;
    }
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|month| *month == parts[2])?;
    let date = time::Date::from_calendar_date(
        parts[3].parse().ok()?,
        time::Month::try_from(u8::try_from(month + 1).ok()?).ok()?,
        parts[1].parse().ok()?,
    )
    .ok()?;
    let mut clock = parts[4].split(':');
    let time = time::Time::from_hms(
        clock.next()?.parse().ok()?,
        clock.next()?.parse().ok()?,
        clock.next()?.parse().ok()?,
    )
    .ok()?;
    if clock.next().is_some() {
        return None;
    }
    u64::try_from(date.with_time(time).assume_utc().unix_timestamp())
        .ok()?
        .checked_mul(1_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_clock_ignores_desktop_wall_clock_and_rejects_bad_samples() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::DATE,
            reqwest::header::HeaderValue::from_static("Fri, 11 Sep 2026 01:00:00 GMT"),
        );
        let sample = AccountClock::from_response(&headers, 200);
        assert!(sample.is_some());
        if let Some(sample) = sample {
            let expected = 1_789_088_400_600;
            assert_eq!(sample.server_ms, expected);
            assert!(
                sample
                    .now_ms(1)
                    .is_some_and(|now| now >= expected && now < expected + 1_000)
            );
        }
        assert!(AccountClock::from_response(&headers, 2_001).is_none());
        headers.insert(
            reqwest::header::AGE,
            reqwest::header::HeaderValue::from_static("10"),
        );
        assert!(AccountClock::from_response(&headers, 100).is_none());
        assert!(parse_date("Fri, 32 Sep 2026 01:00:00 GMT").is_none());
        assert!(parse_date("garbage").is_none());
    }
}
