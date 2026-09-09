//! Opt-in, memory-only diagnostics. Never a trading authority or recovery journal.
use crate::account_scope::AccountScope;
use eframe::egui;
use parking_lot::Mutex;
use serde::Serialize;
use std::{collections::VecDeque, sync::OnceLock, time::Instant};
use venue_control_protocol::kol::{
    ExecutorCommandState, ExecutorCommandSummary, TerminalAccountProjection,
};

mod panel;
#[cfg(test)]
mod tests;
const LIMIT: usize = 512;
const PENDING_LIMIT: usize = 32;
const WINDOW_MS: u64 = 120_000;

#[derive(Clone, Copy, Debug, Serialize)]
pub(super) struct Stamp {
    utc_ms: u64,
    mono_ms: u64,
}
pub(crate) fn now() -> Stamp {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    Stamp {
        utc_ms: crate::account_center::now_ms(),
        mono_ms: ORIGIN.get_or_init(Instant::now).elapsed().as_millis() as u64,
    }
}
#[derive(Clone, Debug, Default, Serialize)]
pub(super) struct Calibration {
    pub verified: bool,
    // Local UTC minus exchange UTC; uncertainty includes drift over this capture window.
    pub local_minus_exchange_ms: i64,
    pub uncertainty_ms: u64,
    pub venue: String,
    pub source: String,
}
#[derive(Clone, Debug, Serialize)]
struct Sample {
    kind: &'static str,
    key: String,
    event_ms: Option<u64>,
    network_received_ms: Option<u64>,
    click: Option<Stamp>,
    queued: Option<Stamp>,
    http_started: Option<Stamp>,
    http_completed: Option<Stamp>,
    command_state: Option<ExecutorCommandState>,
    command_created_ms: Option<u64>,
    command_updated_ms: Option<u64>,
    exchange_order_created_ms: Option<u64>,
    projection_observed_ms: Option<u64>,
    projection_persisted_ms: Option<u64>,
    projection_received: Option<Stamp>,
    paint: Stamp,
    framebuffer_received: Option<Stamp>,
    raw_event_to_framebuffer_ms: Option<i128>,
    corrected_event_to_framebuffer_ms: Option<i128>,
    upper_bound_ms: Option<u64>,
    clock_valid: bool,
}
impl Sample {
    fn empty(kind: &'static str, key: String) -> Self {
        Self {
            kind,
            key,
            event_ms: None,
            network_received_ms: None,
            click: None,
            queued: None,
            http_started: None,
            http_completed: None,
            command_state: None,
            command_created_ms: None,
            command_updated_ms: None,
            exchange_order_created_ms: None,
            projection_observed_ms: None,
            projection_persisted_ms: None,
            projection_received: None,
            paint: now(),
            framebuffer_received: None,
            raw_event_to_framebuffer_ms: None,
            corrected_event_to_framebuffer_ms: None,
            upper_bound_ms: None,
            clock_valid: true,
        }
    }
    fn finish(&mut self, at: Stamp, calibration: &Calibration, anchor: Stamp) {
        self.upper_bound_ms = None;
        self.corrected_event_to_framebuffer_ms = None;
        self.framebuffer_received = Some(at);
        let wall = at.utc_ms as i128 - anchor.utc_ms as i128;
        let mono = at.mono_ms as i128 - anchor.mono_ms as i128;
        self.clock_valid = (wall - mono).abs() <= 20;
        if let Some(click) = self.click {
            self.upper_bound_ms = at.mono_ms.checked_sub(click.mono_ms);
        } else if let Some(event) = self.event_ms {
            let raw = at.utc_ms as i128 - event as i128;
            self.raw_event_to_framebuffer_ms = Some(raw);
            if calibration.verified && self.clock_valid && !calibration.source.trim().is_empty() {
                let corrected = raw - calibration.local_minus_exchange_ms as i128;
                self.corrected_event_to_framebuffer_ms = Some(corrected);
                // Negative intervals remain invalid, never saturate them into a passing zero.
                if corrected >= calibration.uncertainty_ms as i128 {
                    self.upper_bound_ms =
                        u64::try_from(corrected + calibration.uncertainty_ms as i128).ok();
                }
            }
        }
    }
}
struct Pending {
    scope: AccountScope,
    symbol: String,
    sample: Sample,
    native: Option<String>,
    uncertain: bool,
}
struct ProjectionReceipt {
    scope: AccountScope,
    observed: u64,
    persisted: u64,
    generation: u64,
    received: Stamp,
}
#[derive(Clone, Copy)]
struct FrameToken(u64);
struct Flight {
    token: u64,
    sent: Stamp,
    samples: Vec<Sample>,
}
#[derive(Default)]
struct Capture {
    active: bool,
    anchor: Option<Stamp>,
    calibration: Calibration,
    samples: VecDeque<Sample>,
    unfinished: VecDeque<Sample>,
    pending: Vec<Pending>,
    receipts: VecDeque<ProjectionReceipt>,
    staged: Vec<Sample>,
    candidate: Option<(Sample, rust_decimal::Decimal)>,
    flight: Option<Flight>,
    seen: VecDeque<String>,
    sequence: u64,
    last_frame_ms: u64,
    dropped: u64,
    incomplete: u64,
}
impl Capture {
    fn start(&mut self, calibration: Calibration, at: Stamp) {
        let sequence = self.sequence.wrapping_add(1);
        *self = Self {
            active: true,
            anchor: Some(at),
            calibration,
            sequence,
            ..Self::default()
        };
    }
    fn tick(&mut self, at: Stamp) {
        if self.active
            && self
                .anchor
                .is_some_and(|s| at.mono_ms.saturating_sub(s.mono_ms) >= WINDOW_MS)
        {
            self.stop();
        }
        if self
            .flight
            .as_ref()
            .is_some_and(|f| at.mono_ms.saturating_sub(f.sent.mono_ms) > 5_000)
        {
            if let Some(f) = self.flight.take() {
                for sample in f.samples {
                    self.unfinished(sample);
                }
            }
        }
    }
    fn stop(&mut self) {
        self.active = false;
        let pending = std::mem::take(&mut self.pending);
        for p in pending {
            self.unfinished(p.sample);
        }
        self.candidate = None;
    }
    fn unfinished(&mut self, sample: Sample) {
        self.incomplete += 1;
        if self.unfinished.len() == PENDING_LIMIT {
            self.unfinished.pop_front();
            self.dropped += 1;
        }
        self.unfinished.push_back(sample);
    }
    fn stage(&mut self, sample: Sample) {
        if !self.active
            || self.seen.contains(&sample.key)
            || self.staged.iter().any(|s| s.key == sample.key)
        {
            return;
        }
        if self.staged.len() < PENDING_LIMIT {
            self.staged.push(sample);
        } else {
            self.dropped += 1;
        }
    }
    fn finish_frame(&mut self, token: u64, at: Stamp) {
        if self.flight.as_ref().is_none_or(|f| f.token != token) {
            return;
        }
        let Some(flight) = self.flight.take() else {
            return;
        };
        let Some(anchor) = self.anchor else {
            return;
        };
        for mut sample in flight.samples {
            sample.finish(at, &self.calibration, anchor);
            if self.samples.len() == LIMIT {
                self.samples.pop_front();
                self.dropped += 1;
            }
            self.samples.push_back(sample);
        }
    }
}
fn capture() -> &'static Mutex<Capture> {
    static CAPTURE: OnceLock<Mutex<Capture>> = OnceLock::new();
    CAPTURE.get_or_init(|| Mutex::new(Capture::default()))
}

pub(crate) fn click(scope: Option<AccountScope>, request_id: &str, symbol: &str, at: Stamp) {
    let Some(scope) = scope else {
        return;
    };
    let mut c = capture().lock();
    c.tick(now());
    if !c.active || c.pending.iter().any(|p| p.sample.key == request_id) {
        return;
    }
    if c.pending.len() == PENDING_LIMIT {
        c.dropped += 1;
        return;
    }
    let mut sample = Sample::empty("terminal_order_line", request_id.into());
    sample.click = Some(at);
    c.pending.push(Pending {
        scope,
        symbol: symbol.into(),
        sample,
        native: None,
        uncertain: false,
    });
}
pub(crate) fn submission(request_id: &str, stage: &'static str) {
    let mut c = capture().lock();
    c.tick(now());
    if let Some(p) = c.pending.iter_mut().find(|p| p.sample.key == request_id) {
        match stage {
            "queued" => p.sample.queued = Some(now()),
            "http_started" => p.sample.http_started = Some(now()),
            "http_completed" => p.sample.http_completed = Some(now()),
            _ => {}
        }
    }
}
pub(crate) fn summary(scope: &AccountScope, row: &ExecutorCommandSummary) {
    let mut c = capture().lock();
    if !c.active || row.validate().is_err() || row.trading_account_id != scope.trading_account_id {
        return;
    }
    if let Some(p) = c.pending.iter_mut().find(|p| {
        p.scope == *scope
            && row.request_id.as_deref() == Some(&p.sample.key)
            && row.symbol.to_string() == p.symbol
    }) {
        p.sample.command_state = Some(row.state);
        p.sample.command_created_ms = Some(row.created_ms);
        p.sample.command_updated_ms = Some(row.updated_ms);
        p.uncertain = !matches!(
            row.state,
            ExecutorCommandState::Accepted | ExecutorCommandState::Reconciled
        );
        p.native = row.native_order_id.clone();
    }
}
pub(crate) fn projection_received(scope: &AccountScope, projection: &TerminalAccountProjection) {
    let mut c = capture().lock();
    c.tick(now());
    if !c.active
        || projection.validate().is_err()
        || projection.trading_account_id != scope.trading_account_id
        || projection.credential_id != scope.credential_id
    {
        return;
    }
    if c.receipts.iter().any(|r| {
        r.scope == *scope
            && r.observed == projection.observed_ms
            && r.persisted == projection.persisted_ms
            && r.generation == projection.private_generation
    }) {
        return;
    }
    if c.receipts.len() == PENDING_LIMIT {
        c.receipts.pop_front();
    }
    c.receipts.push_back(ProjectionReceipt {
        scope: scope.clone(),
        observed: projection.observed_ms,
        persisted: projection.persisted_ms,
        generation: projection.private_generation,
        received: now(),
    });
}
pub(crate) fn bind_orders(model: &crate::model::AppModel, projection: &TerminalAccountProjection) {
    if !capture().lock().active {
        return;
    }
    let Some(scope) = model.confirmed_account_scope() else {
        return;
    };
    for row in &model.execution.terminal_executions {
        summary(&scope, row);
    }
    let mut c = capture().lock();
    for p in &mut c.pending {
        p.sample.projection_received = None;
    }
    let received = c
        .receipts
        .iter()
        .find(|r| {
            r.scope == scope
                && r.observed == projection.observed_ms
                && r.persisted == projection.persisted_ms
                && r.generation == projection.private_generation
        })
        .map(|r| r.received);
    let Some(received) = received else {
        return;
    };
    for p in &mut c.pending {
        p.sample.projection_received = None;
        if p.scope != scope || p.uncertain {
            continue;
        }
        if let Some(order) = projection.open_orders.iter().find(|o| {
            o.native_order_id.is_some()
                && o.native_order_id == p.native
                && o.symbol.to_string() == p.symbol
        }) {
            p.sample.exchange_order_created_ms = order.created_ms;
            p.sample.projection_observed_ms = Some(projection.observed_ms);
            p.sample.projection_persisted_ms = Some(projection.persisted_ms);
            p.sample.projection_received = Some(received);
        }
    }
}
pub(crate) fn order_painted(selection: &crate::trading::TerminalOrderSelection) {
    let mut c = capture().lock();
    let index = c.pending.iter().position(|p| {
        !p.uncertain
            && p.scope.trading_account_id == selection.trading_account_id
            && p.scope.credential_id == selection.credential_id
            && p.symbol == selection.symbol.to_string()
            && p.native.as_ref() == Some(&selection.native_order_id)
            && p.sample.projection_received.is_some()
    });
    if let Some(index) = index {
        let mut sample = c.pending[index].sample.clone();
        sample.paint = now();
        c.stage(sample);
    }
}
pub(crate) fn prepare_market(
    venue: &str,
    generation: u64,
    symbol: &str,
    event: Option<u64>,
    received: Option<u64>,
    price: Option<rust_decimal::Decimal>,
) {
    let mut c = capture().lock();
    c.candidate = None;
    if !c.active || c.flight.is_some() || now().mono_ms.saturating_sub(c.last_frame_ms) < 250 {
        return;
    }
    let (Some(event), Some(received)) = (event, received) else {
        return;
    };
    if c.calibration.verified && c.calibration.venue != venue {
        return;
    }
    let mut sample = Sample::empty(
        "market_last_price",
        format!("{venue}:{generation}:{symbol}:{event}:{received}"),
    );
    sample.event_ms = Some(event);
    sample.network_received_ms = Some(received);
    c.candidate = price.map(|price| (sample, price));
}
pub(crate) fn market_painted(price: rust_decimal::Decimal) {
    let mut c = capture().lock();
    if c.candidate
        .as_ref()
        .is_some_and(|(_, candidate)| *candidate == price)
        && let Some((mut sample, _)) = c.candidate.take()
    {
        sample.paint = now();
        c.stage(sample);
    }
}
pub(crate) fn clear_market() {
    capture().lock().candidate = None;
}

pub(crate) fn begin_pass(context: &egui::Context, scope: Option<AccountScope>) {
    let mut c = capture().lock();
    let at = now();
    c.tick(at);
    context.input(|i| {
        for event in &i.events {
            if let egui::Event::Screenshot {
                user_data,
                viewport_id,
                image,
            } = event
                && image.size[0] > 0
                && image.size[1] > 0
                && *viewport_id == egui::ViewportId::ROOT
                && let Some(token) = user_data
                    .data
                    .as_ref()
                    .and_then(|d| d.downcast_ref::<FrameToken>())
            {
                c.finish_frame(token.0, at);
            }
        }
    });
    let pending = std::mem::take(&mut c.pending);
    for p in pending {
        if Some(&p.scope) == scope.as_ref() {
            c.pending.push(p);
        } else {
            c.unfinished(p.sample);
        }
    }
    if c.active || c.flight.is_some() {
        context.request_repaint_after(std::time::Duration::from_millis(250));
    }
    c.staged.clear();
    c.candidate = None;
}
pub(crate) fn end_pass(context: &egui::Context) {
    let mut c = capture().lock();
    c.tick(now());
    if context.will_discard() || !c.active || c.flight.is_some() || c.staged.is_empty() {
        return;
    }
    // Readback is an instrumented framebuffer upper bound, not monitor scan-out time.
    c.sequence = c.sequence.wrapping_add(1);
    let token = c.sequence;
    let sent = now();
    let samples = std::mem::take(&mut c.staged);
    for s in &samples {
        if c.seen.len() == LIMIT {
            c.seen.pop_front();
        }
        c.seen.push_back(s.key.clone());
        if s.kind == "terminal_order_line" {
            c.pending.retain(|p| p.sample.key != s.key);
        }
    }
    c.last_frame_ms = sent.mono_ms;
    c.flight = Some(Flight {
        token,
        sent,
        samples,
    });
    context.send_viewport_cmd(egui::ViewportCommand::Screenshot(egui::UserData::new(
        FrameToken(token),
    )));
    context.request_repaint();
}
pub(crate) use panel::show;
