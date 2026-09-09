use super::*;

#[derive(Clone, Default)]
struct Panel {
    open: bool,
    calibration: Calibration,
    message: String,
}
#[derive(Serialize)]
pub(super) struct Distribution {
    pub(super) eligible: usize,
    pub(super) excluded: usize,
    pub(super) p50_ms: Option<u64>,
    pub(super) p95_ms: Option<u64>,
    target_ms: u64,
    result: &'static str,
}
pub(super) fn distribution(samples: &VecDeque<Sample>, kind: &str, target_ms: u64) -> Distribution {
    let total = samples.iter().filter(|s| s.kind == kind).count();
    let mut values: Vec<_> = samples
        .iter()
        .filter(|s| s.kind == kind)
        .filter_map(|s| s.upper_bound_ms)
        .collect();
    values.sort_unstable();
    let percentile = |p: usize| {
        values
            .get((values.len() * p).div_ceil(100).saturating_sub(1))
            .copied()
    };
    let p95 = percentile(95);
    Distribution {
        eligible: values.len(),
        excluded: total - values.len(),
        p50_ms: percentile(50),
        p95_ms: p95,
        target_ms,
        result: if p95.is_none() {
            "NOT_MEASURED"
        } else if p95.is_some_and(|v| v > target_ms) {
            "INSTRUMENTED_UPPER_BOUND_EXCEEDS_TARGET"
        } else {
            "INSTRUMENTED_UPPER_BOUND_WITHIN_TARGET_NOT_LIVE_ACCEPTANCE"
        },
    }
}
pub(super) fn export(c: &Capture) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&serde_json::json!({
        "schema_version": 1, "capture_start": c.anchor, "window_limit_ms": WINDOW_MS,
        "sample_limit": LIMIT, "pending_limit": PENDING_LIMIT, "market_sampling_interval_ms": 250,
        "render_boundary": "egui framebuffer screenshot reply; includes GPU readback and event delivery; not monitor scan-out",
        "clock_offset_sign": "local UTC minus exchange UTC", "calibration": c.calibration,
        "control_clock": "uncalibrated; command/projection server timestamps are raw only",
        "exchange_confirmation": "signed open-order fact; created_ms is exchange order creation, NOT ACK or exact confirmation time",
        "click_boundary": "accepted terminal action handler entry, including local validation; not hardware input time",
        "scope": "native desktop last-price line and ordinary terminal limit-order action path; no cancel/replace/position-action timing",
        "live_acceptance": "NOT_RUN", "dropped": c.dropped, "incomplete": c.incomplete,
        "unfinished_samples": c.unfinished, "pending": c.pending.len(), "frame_in_flight": c.flight.is_some(),
        "market": distribution(&c.samples, "market_last_price", 300),
        "orders": distribution(&c.samples, "terminal_order_line", 1000), "samples": c.samples
    }))
}
pub(crate) fn show(ctx: &egui::Context) {
    let id = egui::Id::new("venueflow-latency-panel");
    let mut panel = ctx.data_mut(|d| d.get_temp::<Panel>(id).unwrap_or_default());
    if ctx.input(|i| i.modifiers.ctrl && i.modifiers.shift && i.key_pressed(egui::Key::L)) {
        panel.open = !panel.open;
    }
    if panel.open {
        egui::Window::new("延迟证据 / Latency evidence")
            .open(&mut panel.open)
            .show(ctx, |ui| {
                ui.label("仅观测，120秒自动停止；最多512样本、32个待确认请求。");
                ui.label("帧缓冲读回包含额外开销；不会下单或保存截图。");
                let mut c = capture().lock();
                ui.add_enabled_ui(!c.active, |ui| {
                    ui.checkbox(
                        &mut panel.calibration.verified,
                        "已独立核验交易所时钟偏差与误差界限",
                    );
                    ui.horizontal(|ui| {
                        ui.label("交易所 (Binance/Bybit/...)");
                        ui.add(
                            egui::TextEdit::singleline(&mut panel.calibration.venue).char_limit(32),
                        );
                    });
                    ui.horizontal(|ui| {
                        ui.label("本机减交易所 ms");
                        ui.add(
                            egui::DragValue::new(&mut panel.calibration.local_minus_exchange_ms)
                                .range(-86_400_000..=86_400_000),
                        );
                    });
                    ui.horizontal(|ui| {
                        ui.label("误差上界 ms（含120秒漂移）");
                        ui.add(
                            egui::DragValue::new(&mut panel.calibration.uncertainty_ms)
                                .range(0..=86_400_000),
                        );
                    });
                    ui.label("校时来源、测量UTC与方法（不填则只导出原始年龄）");
                    ui.add(
                        egui::TextEdit::singleline(&mut panel.calibration.source).char_limit(256),
                    );
                    if ui.button("开始新的有界采集").clicked() {
                        c.start(panel.calibration.clone(), now());
                        panel.message.clear();
                    }
                });
                if c.active && ui.button("停止采集").clicked() {
                    c.stop();
                }
                ui.label(format!(
                    "样本 {} / 待确认 {} / 丢弃 {} / 未完成 {}",
                    c.samples.len(),
                    c.pending.len(),
                    c.dropped,
                    c.incomplete
                ));
                for (kind, target) in [("market_last_price", 300), ("terminal_order_line", 1000)] {
                    let d = distribution(&c.samples, kind, target);
                    ui.label(format!(
                        "{kind}: n={} excluded={} P50={:?} P95={:?} ms",
                        d.eligible, d.excluded, d.p50_ms, d.p95_ms
                    ));
                }
                if ui
                    .add_enabled(
                        !c.active && c.flight.is_none(),
                        egui::Button::new("导出 JSON 到剪贴板"),
                    )
                    .clicked()
                {
                    match export(&c) {
                        Ok(json) => {
                            ctx.copy_text(json);
                            panel.message = "已复制有界报告（不含凭证、账户编号或截图）".into();
                        }
                        Err(_) => panel.message = "导出失败".into(),
                    }
                }
                ui.label(&panel.message);
            });
    }
    ctx.data_mut(|d| d.insert_temp(id, panel));
}
