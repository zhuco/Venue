use std::{sync::Arc, time::Duration};

use eframe::egui;

use crate::trading::DisplayCadence;

pub(super) fn funding_display(funding: &venue_domain::MarkFunding, now_ms: u64,
    language: crate::i18n::Language) -> (String, String) {
    let zh = language == crate::i18n::Language::SimplifiedChinese;
    let percent = |rate: rust_decimal::Decimal| rate.checked_mul(rust_decimal::Decimal::from(100))
        .map(|value| format!("{}%", crate::model::format_decimal(value, 4)))
        .unwrap_or_else(|| "—".to_owned());
    let rate = percent(funding.funding_rate);
    let predicted = match &funding.predicted_funding_rate {
        venue_domain::FieldState::Known(value) => percent(*value),
        _ => "—".to_owned(),
    };
    let settlement = match funding.next_funding_time_ms {
        Some(next) if next > now_ms => {
            let minutes = next.saturating_sub(now_ms).saturating_add(59_999) / 60_000;
            format!("{}h {}m", minutes / 60, minutes % 60)
        }
        Some(_) if zh => "待更新".to_owned(),
        Some(_) => "awaiting update".to_owned(),
        None => "—".to_owned(),
    };
    let stale = now_ms < funding.received_at_ms
        || now_ms.saturating_sub(funding.received_at_ms) > 45_000;
    let label = format!("Funding {rate} · {settlement}{}", if stale { " · stale" } else { "" });
    let source = if funding.time_source == venue_domain::MarketTimeSource::Exchange {
        if zh { "交易所" } else { "exchange" }
    } else if zh { "本机观察" } else { "local observation" };
    let tooltip = if zh {
        format!("当前费率：{rate}\n预测费率：{predicted}\n数据时间：{} ms UTC（{source}）\n下一结算：{}\n结算周期：当前公共来源未提供；按下一结算时间显示倒计时",
            funding.exchange_time_ms,
            funding.next_funding_time_ms.map_or_else(|| "—".to_owned(), |value| format!("{value} ms UTC")))
    } else {
        format!("Current rate: {rate}\nPredicted rate: {predicted}\nData time: {} ms UTC ({source})\nNext settlement: {}\nSettlement interval: unavailable from this public source; countdown uses the next settlement timestamp",
            funding.exchange_time_ms,
            funding.next_funding_time_ms.map_or_else(|| "—".to_owned(), |value| format!("{value} ms UTC")))
    };
    (label, tooltip)
}

pub(super) fn open_interest_display(interest: &venue_domain::OpenInterestSample, now_ms: u64,
    language: crate::i18n::Language) -> (String, String) {
    let zh = language == crate::i18n::Language::SimplifiedChinese;
    let stale = now_ms < interest.received_at_ms
        || now_ms.saturating_sub(interest.received_at_ms) > 45_000;
    let (value, reason) = match &interest.base_quantity {
        venue_domain::FieldState::Known(quantity) => {
            let decimals = if *quantity >= rust_decimal::Decimal::from(1_000) { 2 }
                else if *quantity >= rust_decimal::Decimal::ONE { 4 } else { 8 };
            (format!("{} {}", quantity.round_dp(decimals).normalize(), interest.symbol.base()), None)
        }
        venue_domain::FieldState::Missing =>
            ("—".to_owned(), Some(if zh { "来源未提供" } else { "source omitted" })),
        venue_domain::FieldState::Null =>
            ("—".to_owned(), Some(if zh { "来源返回空值" } else { "source returned null" })),
        venue_domain::FieldState::Unavailable { reason } =>
            ("—".to_owned(), Some(match reason {
                venue_domain::UnknownReason::SourceOmitted => if zh { "来源未提供" } else { "source omitted" },
                venue_domain::UnknownReason::PermissionDenied => if zh { "无读取权限" } else { "permission denied" },
                venue_domain::UnknownReason::VenueUnavailable => if zh { "交易所暂不可用" } else { "venue unavailable" },
                venue_domain::UnknownReason::ParseFailure => if zh { "来源数值无法解析" } else { "source value could not be parsed" },
                venue_domain::UnknownReason::Ambiguous => if zh { "合约单位不明确" } else { "contract unit ambiguous" },
                venue_domain::UnknownReason::NotYetObserved => if zh { "尚未观察到" } else { "not yet observed" },
            })),
        venue_domain::FieldState::NotApplicable =>
            ("—".to_owned(), Some(if zh { "不适用" } else { "not applicable" })),
    };
    let source = if interest.time_source == venue_domain::MarketTimeSource::Exchange {
        if zh { "交易所" } else { "exchange" }
    } else if zh { "本机观察" } else { "local observation" };
    let unit = match &interest.native_unit {
        venue_domain::OpenInterestUnit::BaseAsset => "base asset",
        venue_domain::OpenInterestUnit::Contracts { .. } => "contracts",
    };
    let label = format!("OI {value}{}", if stale { " · stale" } else { "" });
    let tooltip = if zh {
        format!("当前 OI 时间：{} ms UTC（{source}）\n原生数量：{} {unit}{}",
            interest.exchange_time_ms, interest.native_quantity,
            reason.map_or_else(String::new, |value| format!("\n基础币数量不可用：{value}")))
    } else {
        format!("Current OI time: {} ms UTC ({source})\nNative: {} {unit}{}",
            interest.exchange_time_ms, interest.native_quantity,
            reason.map_or_else(String::new, |value| format!("\nBase quantity unavailable: {value}")))
    };
    (label, tooltip)
}

pub(super) fn open_interest_history_stale(
    last: Option<&venue_domain::OpenInterestSample>, now_ms: u64,
) -> bool {
    last.is_some_and(|sample| now_ms < sample.exchange_time_ms
        || now_ms.saturating_sub(sample.exchange_time_ms) > 900_000)
}

#[derive(Clone)]
struct Sample<K, R, T> {
    revision: R,
    scope: K,
    cadence: DisplayCadence,
    sampled_at: f64,
    value: Arc<T>,
}

#[derive(Clone)]
struct Shared<K, R, T> {
    scope: K,
    revision: R,
    value: Arc<T>,
}

impl<K: PartialEq, R, T> Sample<K, R, T> {
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
    sample_revision(ui, surface, scope, None::<u64>, cadence, latest)
}

// A revision is content, never identity: arriving packets cannot bypass cadence.
// None supports older transports without revisions; they retain periodic collection.
pub(super) fn sample_revision<K, R, T>(
    ui: &egui::Ui,
    surface: impl std::hash::Hash + std::fmt::Debug,
    scope: K,
    revision: Option<R>,
    cadence: DisplayCadence,
    latest: impl FnOnce() -> T,
) -> Arc<T>
where
    K: Clone + PartialEq + Send + Sync + 'static,
    R: Clone + PartialEq + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    let now = ui.input(|input| input.time);
    let id = ui.make_persistent_id(surface);
    let (value, remaining) = ui.data_mut(|data| {
        // Pane IDs survive closure in egui temp memory; bound retained surface snapshots too.
        let registry_id = egui::Id::new((
            "market-sample-surfaces",
            std::any::TypeId::of::<Sample<K, Option<R>, T>>(),
        ));
        let mut surfaces = data
            .get_temp::<Vec<egui::Id>>(registry_id)
            .unwrap_or_default();
        surfaces.retain(|surface| *surface != id);
        surfaces.push(id);
        if surfaces.len() > 64 {
            data.remove::<Sample<K, Option<R>, T>>(surfaces.remove(0));
        }
        data.insert_temp(registry_id, surfaces);
        let existing = data.get_temp::<Sample<K, Option<R>, T>>(id);
        let snapshot = match existing {
            Some(snapshot) if !snapshot.due(&scope, cadence, now) => snapshot,
            Some(mut snapshot)
                if snapshot.scope == scope
                    && snapshot.cadence == cadence
                    && now >= snapshot.sampled_at
                    && revision.is_some()
                    && snapshot.revision == revision =>
            {
                snapshot.sampled_at = now;
                snapshot
            }
            _ => {
                let value = if let Some(version) = &revision {
                    let shared_id = egui::Id::new("shared-market-presentation");
                    let mut shared = data
                        .get_temp::<Vec<Shared<K, R, T>>>(shared_id)
                        .unwrap_or_default();
                    let value = shared
                        .iter()
                        .find(|entry| entry.scope == scope && &entry.revision == version)
                        .map(|entry| Arc::clone(&entry.value));
                    let value = value.unwrap_or_else(|| Arc::new(latest()));
                    shared.retain(|entry| entry.scope != scope);
                    shared.push(Shared {
                        scope: scope.clone(),
                        revision: version.clone(),
                        value: Arc::clone(&value),
                    });
                    if shared.len() > 8 {
                        shared.remove(0);
                    }
                    data.insert_temp(shared_id, shared);
                    value
                } else {
                    Arc::new(latest())
                };
                Sample {
                    scope,
                    revision,
                    cadence,
                    sampled_at: now,
                    value,
                }
            }
        };
        let remaining = (cadence.millis() as f64 / 1000.0 - (now - snapshot.sampled_at)).max(0.0);
        let value = Arc::clone(&snapshot.value);
        data.insert_temp(id, snapshot);
        (value, remaining)
    });
    // Retain wakeups even without packets, for price expiration and live interaction.
    ui.ctx()
        .request_repaint_after(Duration::from_secs_f64(remaining));
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_surface_snapshots_are_bounded() {
        let context = egui::Context::default();
        let mut output = context.run_ui(
            egui::RawInput {
                time: Some(1.0),
                ..Default::default()
            },
            |ui| {
                for index in 0..80_u64 {
                    sample_revision(
                        ui,
                        ("bounded", index),
                        index,
                        Some(0_u64),
                        DisplayCadence::Ms250,
                        || index,
                    );
                }
                let recreated = sample_revision(
                    ui,
                    ("bounded", 0_u64),
                    0_u64,
                    Some(0_u64),
                    DisplayCadence::Ms250,
                    || 99_u64,
                );
                assert_eq!(*recreated, 99);
            },
        );
        output.textures_delta.clear();
    }

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
            revision: 1,
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
    fn unchanged_revision_skips_collection_but_changed_revision_waits_for_cadence() {
        let context = egui::Context::default();
        let calls = std::cell::Cell::new(0);
        for (time, scope, revision, expected, expected_calls) in [
            (1.0, 1, 1, 1, 1),
            (1.1, 1, 2, 1, 1),
            (1.25, 1, 2, 2, 2),
            (1.5, 1, 2, 2, 2),
            (1.6, 2, 2, 3, 3),
        ] {
            let mut output = context.run_ui(
                egui::RawInput {
                    time: Some(time),
                    ..Default::default()
                },
                |ui| {
                    let value = sample_revision(
                        ui,
                        "revision-test",
                        scope,
                        Some(revision),
                        DisplayCadence::Ms250,
                        || {
                            calls.set(calls.get() + 1);
                            calls.get()
                        },
                    );
                    assert_eq!(*value, expected);
                    assert_eq!(calls.get(), expected_calls);
                },
            );
            output.textures_delta.clear();
        }
    }

    #[test]
    fn identical_surfaces_share_read_only_content_without_sharing_cadence() {
        let context = egui::Context::default();
        let mut output = context.run_ui(
            egui::RawInput {
                time: Some(1.0),
                ..Default::default()
            },
            |ui| {
                let first =
                    sample_revision(ui, "pane-a", "BTC", Some(1), DisplayCadence::Ms250, || {
                        vec![10]
                    });
                let second =
                    sample_revision(ui, "pane-b", "BTC", Some(1), DisplayCadence::Ms1000, || {
                        vec![20]
                    });
                assert!(Arc::ptr_eq(&first, &second));
                assert_eq!(*second, vec![10]);
            },
        );
        output.textures_delta.clear();
    }

    #[test]
    fn old_preferences_get_readable_defaults_and_persist_choices() -> Result<(), serde_json::Error>
    {
        let mut settings: crate::trading::TradingSettings = serde_json::from_str("{}")?;
        assert_eq!(settings.book_cadence.millis(), 33);
        assert_eq!(settings.tape_cadence.millis(), 33);
        assert_eq!(settings.chart_cadence.millis(), 33);
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
