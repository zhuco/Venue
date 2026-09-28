//! UTC previous-session levels prepared from completed daily public bars.

use rust_decimal::Decimal;
use venue_domain::{FieldState, PublicBar, UnknownReason};

use crate::catalog::{BarIndicator, price::{PivotOutput, PivotPoints}};

const DAY_MS: u64 = 86_400_000;
const WEEK_MS: u64 = 7 * DAY_MS;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionPeriod { Daily, Weekly }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreviousSession {
    pub start_ms: u64,
    pub end_ms: u64,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SessionLevels {
    pub previous: PreviousSession,
    pub pivot: PivotOutput,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SessionError {
    #[error("daily bars are invalid, unordered, or from mixed scopes")]
    InvalidBars,
    #[error("session timestamp overflow")]
    Timestamp,
}

/// Uses only completed UTC daily bars strictly before the queried session.
/// Missing any day of a previous week returns None; no partial H/L is substituted.
pub fn previous_levels(
    daily_bars: &[PublicBar],
    at_ms: u64,
    period: SessionPeriod,
) -> Result<Option<SessionLevels>, SessionError> {
    let start = session_start(at_ms, period).ok_or(SessionError::Timestamp)?;
    let duration = match period { SessionPeriod::Daily => DAY_MS, SessionPeriod::Weekly => WEEK_MS };
    let previous_start = start.checked_sub(duration).ok_or(SessionError::Timestamp)?;
    let mut previous_open = None;
    let mut scope = None;
    for bar in daily_bars {
        if !bar.is_valid() || bar.interval_ms != DAY_MS || bar.open_time_ms % DAY_MS != 0
            || previous_open.is_some_and(|time| bar.open_time_ms <= time)
        {
            return Err(SessionError::InvalidBars);
        }
        let this_scope = (&bar.symbol, bar.generation);
        if scope.is_some_and(|other| other != this_scope) {
            return Err(SessionError::InvalidBars);
        }
        scope = Some(this_scope);
        previous_open = Some(bar.open_time_ms);
    }
    let Some(first) = daily_bars.binary_search_by_key(&previous_start, |bar| bar.open_time_ms).ok() else {
        return Ok(None);
    };
    let count = (duration / DAY_MS) as usize;
    let Some(slice) = daily_bars.get(first..first + count) else { return Ok(None) };
    for (index, bar) in slice.iter().enumerate() {
        if bar.open_time_ms != previous_start + index as u64 * DAY_MS {
            return Ok(None);
        }
    }
    let Some(last) = slice.last() else { return Ok(None) };
    let high = slice.iter().map(|bar| bar.high.value()).max().ok_or(SessionError::InvalidBars)?;
    let low = slice.iter().map(|bar| bar.low.value()).min().ok_or(SessionError::InvalidBars)?;
    let previous = PreviousSession {
        start_ms: previous_start,
        end_ms: start,
        high,
        low,
        close: last.close.value(),
    };
    let mut synthetic = last.clone();
    synthetic.open_time_ms = previous_start;
    synthetic.close_time_ms = start.checked_sub(1).ok_or(SessionError::Timestamp)?;
    synthetic.interval_ms = duration;
    synthetic.open = slice[0].open;
    synthetic.high = venue_domain::Price::new(high).map_err(|_| SessionError::InvalidBars)?;
    synthetic.low = venue_domain::Price::new(low).map_err(|_| SessionError::InvalidBars)?;
    synthetic.base_volume = FieldState::Unavailable { reason: UnknownReason::SourceOmitted };
    synthetic.quote_volume = FieldState::Unavailable { reason: UnknownReason::SourceOmitted };
    synthetic.trade_count = FieldState::Unavailable { reason: UnknownReason::SourceOmitted };
    synthetic.taker_buy_base_volume = FieldState::Unavailable { reason: UnknownReason::SourceOmitted };
    synthetic.taker_buy_quote_volume = FieldState::Unavailable { reason: UnknownReason::SourceOmitted };
    let pivot = PivotPoints::new().update(&synthetic).map_err(|_| SessionError::InvalidBars)?
        .ok_or(SessionError::InvalidBars)?;
    Ok(Some(SessionLevels { previous, pivot }))
}

pub fn session_start(at_ms: u64, period: SessionPeriod) -> Option<u64> {
    let day = at_ms / DAY_MS;
    match period {
        SessionPeriod::Daily => day.checked_mul(DAY_MS),
        // Unix epoch was Thursday. The first representable Monday starts on day 4.
        SessionPeriod::Weekly => day.checked_sub((day + 3) % 7)?.checked_mul(DAY_MS),
    }
}

/// The first completed 1m bar supplies today's or this week's open. No later bar substitutes it.
pub fn current_open(minute_bars: &[PublicBar], at_ms: u64, period: SessionPeriod) -> Option<Decimal> {
    let start = session_start(at_ms, period)?;
    minute_bars.binary_search_by_key(&start, |bar| bar.open_time_ms).ok()
        .and_then(|index| minute_bars.get(index))
        .filter(|bar| bar.is_valid() && bar.interval_ms == 60_000)
        .map(|bar| bar.open.value())
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_domain::Price;

    fn day_bar(day: u64, low: i64, high: i64) -> Result<PublicBar, Box<dyn std::error::Error>> {
        let start = day * DAY_MS;
        let missing = || FieldState::Unavailable { reason: UnknownReason::SourceOmitted };
        Ok(PublicBar {
            symbol: "DOGE/USDC".parse()?,
            generation: 1,
            received_at_ms: start + DAY_MS,
            sequence: day + 1,
            open_time_ms: start,
            close_time_ms: start + DAY_MS - 1,
            interval_ms: DAY_MS,
            open: Price::new(Decimal::from(low))?,
            high: Price::new(Decimal::from(high))?,
            low: Price::new(Decimal::from(low))?,
            close: Price::new(Decimal::from(high))?,
            base_volume: missing(), quote_volume: missing(),
            trade_count: FieldState::Unavailable { reason: UnknownReason::SourceOmitted },
            taker_buy_base_volume: missing(), taker_buy_quote_volume: missing(),
        })
    }

    #[test]
    fn weekly_levels_require_all_seven_complete_days_and_never_use_current_day() -> Result<(), Box<dyn std::error::Error>> {
        let mut days = (4..=10).map(|day| day_bar(day, day as i64, day as i64 + 10))
            .collect::<Result<Vec<_>, _>>()?;
        days.push(day_bar(11, 1, 999)?);
        let at = 11 * DAY_MS + 3_600_000;
        let result = previous_levels(&days, at, SessionPeriod::Weekly)?.ok_or("week")?;
        assert_eq!(result.previous.start_ms, 4 * DAY_MS);
        assert_eq!(result.previous.end_ms, 11 * DAY_MS);
        assert_eq!(result.previous.high, Decimal::from(20));
        assert_eq!(result.previous.low, Decimal::from(4));
        assert_eq!(result.previous.close, Decimal::from(20));
        assert!(result.pivot.resistance_1 < 999.0);
        days.remove(2);
        assert_eq!(previous_levels(&days, at, SessionPeriod::Weekly)?, None);
        Ok(())
    }

    #[test]
    fn daily_levels_follow_the_queried_date() -> Result<(), Box<dyn std::error::Error>> {
        let days = [day_bar(9, 10, 20)?, day_bar(10, 30, 40)?, day_bar(11, 1, 999)?];
        let result = previous_levels(&days, 11 * DAY_MS, SessionPeriod::Daily)?.ok_or("day")?;
        assert_eq!(result.previous.high, Decimal::from(40));
        assert_eq!(result.previous.low, Decimal::from(30));
        assert_eq!(result.previous.close, Decimal::from(40));
        Ok(())
    }
}
