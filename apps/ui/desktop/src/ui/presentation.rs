use std::{sync::Arc, time::Duration};

use eframe::egui;

use crate::trading::DisplayCadence;

#[derive(Clone)]
struct Sample<K, T> {
    scope: K,
    cadence: DisplayCadence,
    sampled_at: f64,
    value: Arc<T>,
}

impl<K: PartialEq, T> Sample<K, T> {
    fn due(&self, scope: &K, cadence: DisplayCadence, now: f64) -> bool {
        self.scope != *scope
            || self.cadence != cadence
            || now < self.sampled_at
            || now - self.sampled_at + 0.000_001 >= cadence.millis() as f64 / 1000.0
    }
}

// One bounded snapshot per pane/surface, never a queue of skipped frames. Scope changes
// invalidate immediately; collectors and interactive paint are never throttled here.
pub(super) fn sample<K, T>(
    ui: &egui::Ui,
    surface: impl std::hash::Hash + std::fmt::Debug,
    scope: K,
    cadence: DisplayCadence,
    latest: impl FnOnce() -> T,
) -> Arc<T>
where
    K: Clone + PartialEq + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    let now = ui.input(|input| input.time);
    let id = ui.make_persistent_id(surface);
    let (value, remaining) = ui.data_mut(|data| {
        let existing = data.get_temp::<Sample<K, T>>(id);
        let snapshot = match existing {
            Some(snapshot) if !snapshot.due(&scope, cadence, now) => snapshot,
            _ => Sample {
                scope,
                cadence,
                sampled_at: now,
                value: Arc::new(latest()),
            },
        };
        let remaining = (cadence.millis() as f64 / 1000.0 - (now - snapshot.sampled_at)).max(0.0);
        let value = Arc::clone(&snapshot.value);
        data.insert_temp(id, snapshot);
        (value, remaining)
    });
    ui.ctx()
        .request_repaint_after(Duration::from_secs_f64(remaining));
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interactive_frames_keep_last_sample_and_scope_switch_is_immediate() {
        let context = egui::Context::default();
        for (time, scope, latest, expected) in [
            (1.0, 1, 10, 10),
            (1.1, 1, 11, 10),
            (1.25, 1, 12, 12),
            (1.26, 2, 13, 13),
        ] {
            let mut output = context.run_ui(
                egui::RawInput {
                    time: Some(time),
                    ..Default::default()
                },
                |ui| {
                    assert_eq!(
                        *sample(ui, "test-market", scope, DisplayCadence::Ms250, || latest),
                        expected
                    );
                },
            );
            output.textures_delta.clear();
        }
    }

    #[test]
    fn sampling_is_bounded_and_invalidates_on_scope_or_rate_change() {
        let state = Sample {
            scope: ("BTC/USDC", 1),
            cadence: DisplayCadence::Ms250,
            sampled_at: 1.0,
            value: Arc::new(17),
        };
        assert!(!state.due(&("BTC/USDC", 1), DisplayCadence::Ms250, 1.1));
        assert!(state.due(&("BTC/USDC", 1), DisplayCadence::Ms250, 1.25));
        assert!(state.due(&("ETH/USDC", 1), DisplayCadence::Ms250, 1.1));
        assert!(state.due(&("BTC/USDC", 2), DisplayCadence::Ms250, 1.1));
        assert!(state.due(&("BTC/USDC", 1), DisplayCadence::Ms100, 1.1));
        assert!(state.due(&("BTC/USDC", 1), DisplayCadence::Ms250, 0.1));
    }

    #[test]
    fn old_preferences_get_readable_defaults_and_persist_choices() -> Result<(), serde_json::Error>
    {
        let mut settings: crate::trading::TradingSettings = serde_json::from_str("{}")?;
        assert_eq!(settings.book_cadence.millis(), 250);
        assert_eq!(settings.tape_cadence.millis(), 500);
        assert_eq!(settings.chart_cadence.millis(), 250);
        assert_eq!(settings.price_validity_seconds, 10);
        settings.price_validity_seconds = 3;
        settings.tape_cadence = DisplayCadence::Ms1000;
        let restored = serde_json::from_str::<crate::trading::TradingSettings>(
            &serde_json::to_string(&settings)?,
        )?;
        assert_eq!(restored, settings);
        Ok(())
    }
}
