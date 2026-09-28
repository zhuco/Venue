use crate::{
    chart::ChartStudyPoint, chart_settings::ChartDisplaySettings, model::format_decimal, theme,
};
use eframe::egui::{self, FontId};

pub(super) fn job(
    settings: &ChartDisplaySettings,
    point: Option<&ChartStudyPoint>,
    close: Option<rust_decimal::Decimal>,
    scale: usize,
    width: f32,
) -> egui::text::LayoutJob {
    let mut job = egui::text::LayoutJob::default();
    job.wrap.max_width = width.max(1.0);
    let empty = ChartStudyPoint::default();
    let point = point.unwrap_or(&empty);
    let base = egui::TextFormat {
        font_id: FontId::proportional(f32::from(settings.chart_text_size)),
        color: theme::TEXT_SECONDARY,
        ..Default::default()
    };
    for (name, periods, style, values) in [
        (
            "MA",
            settings.ma_periods,
            settings.ma,
            [point.sma, point.sma_second, point.sma_third],
        ),
        (
            "EMA",
            settings.ema_periods,
            settings.ema,
            [point.ema, point.ema_second, point.ema_third],
        ),
        (
            "WMA",
            settings.wma_periods,
            settings.wma,
            [point.wma, point.wma_second, point.wma_third],
        ),
    ] {
        if !style.enabled {
            continue;
        }
        for (index, color) in [
            style.color(),
            style.secondary_color(),
            style.tertiary_color(),
        ]
        .into_iter()
        .enumerate()
        {
            if !style.line_enabled[index] {
                continue;
            }
            job.append(&format!("{name}{} ", periods[index]), 0.0, base.clone());
            job.append(
                &format!(
                    "{}  ",
                    values[index].map_or_else(|| "—".into(), |v| format_decimal(v, scale))
                ),
                0.0,
                egui::TextFormat {
                    color,
                    ..base.clone()
                },
            );
        }
    }
    for (name, style, values, labels) in [
        (
            format!("BOLL({})", settings.bollinger_period),
            settings.bollinger,
            [
                point.bollinger_upper,
                point.bollinger_middle,
                point.bollinger_lower,
            ],
            ["UP", "MID", "LOW"],
        ),
        (
            "VWAP".into(),
            settings.vwap,
            [point.vwap, None, None],
            ["", "", ""],
        ),
        (
            "AVL".into(),
            settings.avl,
            [point.avl, None, None],
            ["", "", ""],
        ),
        (
            format!("TRIX({})", settings.trix_period),
            settings.trix,
            [point.trix, None, None],
            ["", "", ""],
        ),
        (
            "SAR".into(),
            settings.sar,
            [point.sar, None, None],
            ["", "", ""],
        ),
        (
            format!("SUPER({})", settings.supertrend_period),
            settings.supertrend,
            [point.supertrend, None, None],
            ["", "", ""],
        ),
    ] {
        if !style.enabled {
            continue;
        }
        let count = if name.starts_with("BOLL") { 3 } else { 1 };
        for index in 0..count {
            if !style.line_enabled[index] {
                continue;
            }
            let color = if index == 1 {
                style.secondary_color()
            } else {
                style.color()
            };
            job.append(&format!("{name}{} ", labels[index]), 0.0, base.clone());
            job.append(
                &format!(
                    "{}  ",
                    values[index].map_or_else(|| "—".into(), |v| format_decimal(v, scale))
                ),
                0.0,
                egui::TextFormat {
                    color,
                    ..base.clone()
                },
            );
        }
    }
    if settings.atr_value_readout {
        job.append(&format!("ATR({}) ", settings.atr_period), 0.0, base.clone());
        job.append(
            &format!(
                "{}  ",
                point
                    .atr
                    .map_or_else(|| "—".into(), |v| format_decimal(v, scale))
            ),
            0.0,
            egui::TextFormat {
                color: settings.atr.color(),
                ..base.clone()
            },
        );
    }
    if settings.atr_percent_readout {
        let percent = point
            .atr
            .zip(close.filter(|price| *price > rust_decimal::Decimal::ZERO))
            .and_then(|(atr, price)| atr.checked_div(price))
            .and_then(|ratio| ratio.checked_mul(rust_decimal::Decimal::from(100)));
        job.append(
            &format!("ATR%({}) ", settings.atr_period),
            0.0,
            base.clone(),
        );
        job.append(
            &format!(
                "{}  ",
                percent.map_or_else(|| "—".into(), |v| format!("{}%", format_decimal(v, 2)))
            ),
            0.0,
            egui::TextFormat {
                color: settings.atr.color(),
                ..base.clone()
            },
        );
    }
    if settings.microstructure.show_delta || settings.microstructure.show_cvd {
        let flow = point.order_flow;
        for (name, value) in [
            ("Buy", flow.buy),
            ("Sell", flow.sell),
            ("Delta", flow.delta),
        ] {
            job.append(
                &format!(
                    "{name} {}  ",
                    value.map_or_else(|| "—".into(), |v| v.to_string())
                ),
                0.0,
                base.clone(),
            );
        }
        if settings.microstructure.show_cvd {
            let origin = flow
                .cumulative_start_ms
                .and_then(|ms| time::OffsetDateTime::from_unix_timestamp((ms / 1_000) as i64).ok())
                .map(|time| format!("{} {} UTC", time.date(), time.time()))
                .unwrap_or_else(|| "—".into());
            let coverage = if settings.microstructure.cvd_reset_mode
                == venue_indicators::chart::CvdResetMode::UtcDaily
                && !flow.complete_day
            {
                " · since gap"
            } else {
                ""
            };
            job.append(
                &format!(
                    "CVD {} · from {origin}{coverage}  ",
                    flow.cumulative
                        .map_or_else(|| "—".into(), |v| v.to_string())
                ),
                0.0,
                base,
            );
        }
    }
    job
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn values_follow_the_selected_point_and_missing_values_are_not_reused() {
        let mut settings = ChartDisplaySettings::default();
        settings.ma.enabled = true;
        settings.ema.enabled = true;
        settings.bollinger.enabled = true;
        let mut point = ChartStudyPoint {
            sma: Some(Decimal::new(8949, 5)),
            ema: Some(Decimal::new(8951, 5)),
            bollinger_upper: Some(Decimal::new(9000, 5)),
            ..Default::default()
        };
        let first = job(
            &settings,
            Some(&point),
            Some(Decimal::new(9000, 5)),
            5,
            500.0,
        );
        assert!(first.text.contains("MA7 0.08949"));
        assert!(first.text.contains("EMA7 0.08951"));
        assert!(first.text.contains("UP 0.09000"));
        point.sma = Some(Decimal::new(8911, 5));
        let next = job(
            &settings,
            Some(&point),
            Some(Decimal::new(9000, 5)),
            5,
            500.0,
        );
        assert!(next.text.contains("MA7 0.08911"));
        assert!(!next.text.contains("0.08949"));
        assert!(job(&settings, None, None, 5, 500.0).text.contains("MA7 —"));
        settings.ma.enabled = false;
        settings.ema.enabled = false;
        settings.bollinger.enabled = false;
        settings.atr_percent_readout = false;
        assert!(job(&settings, Some(&point), None, 5, 500.0).text.is_empty());
    }

    #[test]
    fn atr_percent_uses_hovered_close_and_is_blank_for_zero_price() {
        let settings = ChartDisplaySettings::default();
        let point = ChartStudyPoint {
            atr: Some(Decimal::from(2)),
            ..Default::default()
        };
        let historical = job(&settings, Some(&point), Some(Decimal::from(100)), 2, 500.0);
        assert!(historical.text.contains("2.00%"));
        let current = job(&settings, Some(&point), Some(Decimal::from(50)), 2, 500.0);
        assert!(current.text.contains("4.00%"));
        assert!(
            job(&settings, Some(&point), Some(Decimal::ZERO), 2, 500.0)
                .text
                .contains("ATR%(14) —")
        );
    }
}
