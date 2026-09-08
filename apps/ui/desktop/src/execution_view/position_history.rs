use std::collections::{BTreeMap, BTreeSet};

use eframe::egui;
use rust_decimal::Decimal;
use venue_control_protocol::kol::{TerminalAccountProjection, TerminalFill};
use venue_domain::{OrderSide, PositionSide, Symbol};

use crate::{i18n::Language, model::AppModel};

#[derive(Debug)]
pub(super) struct Cycle {
    symbol: Symbol,
    side: PositionSide,
    opened: Option<u64>,
    closed: Option<u64>,
    last: u64,
    opened_qty: Decimal,
    opened_value: Decimal,
    closed_qty: Decimal,
    closed_value: Decimal,
    remaining: Decimal,
    cost: Decimal,
    pnl: Decimal,
    known_start: bool,
}

impl Cycle {
    fn new(fill: &TerminalFill, known_start: bool) -> Self {
        Self {
            symbol: fill.symbol.clone(),
            side: fill.position_side,
            opened: None,
            closed: None,
            last: 0,
            opened_qty: Decimal::ZERO,
            opened_value: Decimal::ZERO,
            closed_qty: Decimal::ZERO,
            closed_value: Decimal::ZERO,
            remaining: Decimal::ZERO,
            cost: Decimal::ZERO,
            pnl: Decimal::ZERO,
            known_start,
        }
    }

    fn apply(&mut self, fill: &TerminalFill) -> Option<()> {
        let time = fill.occurred_ms?;
        self.last = time;
        let value = fill.quantity.checked_mul(fill.price)?;
        let opening = matches!(
            (fill.order_side, fill.position_side),
            (OrderSide::Buy, PositionSide::Long) | (OrderSide::Sell, PositionSide::Short)
        );
        if opening {
            if self.known_start && self.opened.is_none() {
                self.opened = Some(time);
            }
            self.opened_qty = self.opened_qty.checked_add(fill.quantity)?;
            self.opened_value = self.opened_value.checked_add(value)?;
            self.remaining = self.remaining.checked_add(fill.quantity)?;
            self.cost = self.cost.checked_add(value)?;
        } else {
            self.closed_qty = self.closed_qty.checked_add(fill.quantity)?;
            self.closed_value = self.closed_value.checked_add(value)?;
            if fill.quantity > self.remaining || self.remaining.is_zero() {
                self.known_start = false;
                self.opened = None;
                self.remaining = Decimal::ZERO;
                self.cost = Decimal::ZERO;
            } else {
                let cost = if fill.quantity == self.remaining {
                    self.cost
                } else {
                    self.cost
                        .checked_div(self.remaining)?
                        .checked_mul(fill.quantity)?
                };
                let pnl = if self.side == PositionSide::Long {
                    value.checked_sub(cost)?
                } else {
                    cost.checked_sub(value)?
                };
                self.pnl = self.pnl.checked_add(pnl)?;
                self.remaining = self.remaining.checked_sub(fill.quantity)?;
                self.cost = self.cost.checked_sub(cost)?;
            }
        }
        Some(())
    }
}

// This is a view of bounded observed fills, never an accounting or execution input.
// A zero inventory observation is required before estimating a cycle's cost basis.
pub(super) fn rebuild(projection: &TerminalAccountProjection) -> Vec<Cycle> {
    let mut groups: BTreeMap<(Symbol, PositionSide), Vec<&TerminalFill>> = BTreeMap::new();
    for fill in &projection.fills {
        groups
            .entry((fill.symbol.clone(), fill.position_side))
            .or_default()
            .push(fill);
    }
    let mut result = Vec::new();
    for ((symbol, side), mut fills) in groups {
        // Net fills lack startPosition in the current normalized contract.
        if side == PositionSide::Net {
            continue;
        }
        let mut seen = BTreeMap::new();
        let mut invalid = false;
        fills.retain(|fill| {
            if fill
                .occurred_ms
                .is_none_or(|time| time == 0 || time > projection.observed_ms)
                || fill.quantity <= Decimal::ZERO
                || fill.price <= Decimal::ZERO
            {
                invalid = true;
                return false;
            }
            match seen.insert(fill.native_trade_id.as_str(), *fill) {
                Some(previous) => {
                    invalid |= previous != *fill;
                    false
                }
                None => true,
            }
        });
        // Missing timestamps or contradictory duplicate identities prevent safe ordering.
        if invalid {
            continue;
        }
        fills.sort_by_key(|fill| (fill.occurred_ms, &fill.native_trade_id));
        if fills.windows(2).any(|pair| {
            pair[0].occurred_ms == pair[1].occurred_ms && pair[0].order_side != pair[1].order_side
        }) {
            continue;
        }
        let mut zeros: BTreeSet<u64> = projection
            .position_history
            .iter()
            .filter(|entry| {
                entry.position.symbol == symbol
                    && entry.position.position_side == side
                    && entry.position.quantity.is_zero()
                    && entry.observed_ms <= projection.observed_ms
            })
            .map(|entry| entry.observed_ms)
            .collect();
        // Require an explicit zero row; absence from a filtered projection is not proof of flatness.
        if projection
            .positions
            .iter()
            .any(|row| row.symbol == symbol && row.position_side == side && row.quantity.is_zero())
        {
            zeros.insert(projection.observed_ms);
        }
        let mut current: Option<Cycle> = None;
        let mut rows = Vec::new();
        let mut failed = false;
        for fill in fills {
            let Some(time) = fill.occurred_ms else {
                continue;
            };
            let last = current.as_ref().map_or(0, |row| row.last);
            let boundary = if current.is_none() {
                projection
                    .position_history
                    .iter()
                    .filter(|entry| {
                        entry.position.symbol == symbol
                            && entry.position.position_side == side
                            && entry.observed_ms < time
                    })
                    .max_by_key(|entry| entry.observed_ms)
                    .filter(|entry| entry.position.quantity.is_zero())
                    .map(|entry| entry.observed_ms)
            } else {
                zeros.range(last..time).next_back().copied()
            };
            if boundary.is_some()
                && let Some(mut row) = current.take()
            {
                if row.known_start && row.remaining.is_zero() && row.closed_qty > Decimal::ZERO {
                    row.closed = Some(row.last);
                }
                rows.push(row);
            }
            let row = current.get_or_insert_with(|| Cycle::new(fill, boundary.is_some()));
            if row.apply(fill).is_none() {
                failed = true;
                break;
            }
        }
        if failed {
            continue;
        }
        if let Some(mut row) = current {
            if row.known_start
                && row.remaining.is_zero()
                && row.closed_qty > Decimal::ZERO
                && zeros.range(row.last..).next().is_some()
            {
                row.closed = Some(row.last);
            }
            rows.push(row);
        }
        result.extend(rows);
    }
    result.sort_by_key(|row| std::cmp::Reverse(row.last));
    result
}

fn tr(language: Language, zh: &'static str, en: &'static str) -> &'static str {
    match language {
        Language::SimplifiedChinese => zh,
        Language::English => en,
    }
}

pub(super) fn show(ui: &mut egui::Ui, model: &AppModel) {
    let language = model.preferences.language;
    let rows: Vec<_> = model
        .execution
        .position_cycles
        .iter()
        .filter(|row| {
            !model.execution.current_symbol
                || row.symbol.to_string() == model.preferences.selected_symbol
        })
        .collect();
    if rows.is_empty() {
        ui.weak(tr(
            language,
            "暂无可重建成交片段；可查看历史成交或仓位变更。",
            "No reconstructable fill segments; see trades or position changes.",
        ));
        return;
    }
    super::history_table_scroll(
        ui,
        super::Tab::PositionHistory,
        rows.len(),
        ui.spacing().interact_size.y,
        true,
        |ui, visible| {
            egui::Grid::new("position-cycles")
                .striped(true)
                .start_row(visible.start)
                .spacing([18.0, 8.0])
                .show(ui, |ui| {
                    if visible.start == 0 {
                        for (zh, en) in [
                            ("交易对", "Symbol"),
                            ("方向", "Side"),
                            ("开仓时间（估算）", "Opened (est.)"),
                            ("平仓时间（估算）", "Closed (est.)"),
                            ("开仓均价（估算）", "Entry (est.)"),
                            ("平仓均价", "Exit"),
                            ("已知平仓数量", "Observed closed size"),
                            ("毛 PnL（估算）", "Gross PnL (est.)"),
                            ("手续费", "Fees"),
                            ("资金费", "Funding"),
                            ("净 PnL", "Net PnL"),
                            ("覆盖", "Coverage"),
                        ] {
                            ui.weak(tr(language, zh, en));
                        }
                        ui.end_row();
                    }
                    for row in rows.iter().skip(visible.start.saturating_sub(1)).take(
                        visible
                            .end
                            .saturating_sub(1)
                            .saturating_sub(visible.start.saturating_sub(1)),
                    ) {
                        ui.label(row.symbol.to_string());
                        ui.label(if row.side == PositionSide::Long {
                            tr(language, "多", "Long")
                        } else {
                            tr(language, "空", "Short")
                        });
                        ui.label(row.opened.map_or_else(|| "—".into(), history_time));
                        ui.label(row.closed.map_or_else(|| "—".into(), history_time));
                        super::market_price(
                            ui,
                            model,
                            &row.symbol,
                            row.known_start
                                .then(|| row.opened_value.checked_div(row.opened_qty))
                                .flatten(),
                        );
                        super::market_price(
                            ui,
                            model,
                            &row.symbol,
                            row.closed_value.checked_div(row.closed_qty),
                        );
                        super::market_quantity(ui, model, &row.symbol, Some(row.closed_qty));
                        if row.known_start && row.closed_qty > Decimal::ZERO {
                            ui.colored_label(
                                super::pnl_color(row.pnl),
                                format!("{:.4} {}", row.pnl, row.symbol.quote()),
                            );
                        } else {
                            ui.label("—");
                        }
                        for _ in 0..3 {
                            ui.label("—");
                        }
                        ui.weak(tr(language, "历史不完整", "Incomplete history"));
                        ui.end_row();
                    }
                });
        },
    );
}

fn history_time(ms: u64) -> String {
    let Ok(date) = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
    else {
        return "—".into();
    };
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03} UTC",
        date.year(),
        u8::from(date.month()),
        date.day(),
        date.hour(),
        date.minute(),
        date.second(),
        date.millisecond()
    )
}

#[cfg(test)]
mod tests;
