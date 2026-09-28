use rust_decimal::Decimal;
use venue_control_protocol::kol::TerminalPosition;

#[derive(Debug, Default)]
pub(super) struct FrameValuations {
    frame: Option<u64>,
    rows: Vec<(TerminalPosition, Option<Decimal>)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn same_frame_reuses_value_and_next_frame_rechecks_expiry()
    -> Result<(), Box<dyn std::error::Error>> {
        let position = TerminalPosition {
            symbol: "BTC/USDC".parse()?,
            position_side: venue_domain::PositionSide::Long,
            quantity: 1.into(),
            entry_price: Some(10.into()),
            mark_price: Some(11.into()),
        };
        let mut cache = FrameValuations::default();
        let calls = std::cell::Cell::new(0);
        cache.begin(1);
        for _ in 0..3 {
            assert_eq!(
                cache.value(&position, || {
                    calls.set(calls.get() + 1);
                    Some(1.into())
                }),
                Some(1.into())
            );
        }
        assert_eq!(calls.get(), 1);
        cache.begin(2);
        assert_eq!(cache.value(&position, || None), None);
        Ok(())
    }
}

impl FrameValuations {
    pub(super) fn begin(&mut self, frame: u64) {
        if self.frame != Some(frame) {
            self.rows.clear();
            self.frame = Some(frame);
        }
    }

    pub(super) fn value(
        &mut self,
        position: &TerminalPosition,
        calculate: impl FnOnce() -> Option<Decimal>,
    ) -> Option<Decimal> {
        if self.frame.is_none() {
            return calculate();
        }
        if let Some((_, value)) = self.rows.iter().find(|(old, _)| old == position) {
            return *value;
        }
        let value = calculate();
        self.rows.push((position.clone(), value));
        value
    }
}
