use eframe::egui::{self, Align2, FontId, Pos2, Rect, Sense, Stroke};
use rust_decimal::Decimal;

use super::{ChartOverlay, label};
use crate::{chart::PriceRange, model::format_decimal, theme, trading::TerminalOrderSelection};

#[cfg(test)]
#[path = "order_tags_tests.rs"]
mod tests;

#[derive(Clone, Debug)]
pub(crate) struct TradingBadge {
    pub language: crate::i18n::Language,
    pub quantity: Option<String>,
    pub stale: bool,
    pub pending: bool,
    pub provisional: bool,
    pub pnl: Option<Decimal>,
    pub selection: Option<TerminalOrderSelection>,
}

#[derive(Debug, Default)]
// Ephemeral presentation only: never mutate signed projections or execute/retry commands.
pub(crate) struct OrderTagState {
    pending: Vec<(TerminalOrderSelection, String)>,
    uncertain_cancels: std::collections::HashSet<String>,
    submitted_orders: Vec<SubmittedOrder>,
}

#[derive(Debug)]
struct SubmittedOrder {
    account: String,
    request: venue_control_protocol::kol::TerminalOrderRequest,
    receipt: Option<venue_control_protocol::kol::ExecutorCommandSummary>,
}

impl OrderTagState {
    pub(crate) fn submitted_cancel(
        &mut self,
        target: TerminalOrderSelection,
        request_id: String,
        context: &egui::Context,
    ) {
        if !self.is_pending(&target) {
            self.pending.push((target, request_id));
        }
        context.request_repaint();
    }

    pub(crate) fn submitted_order(
        &mut self,
        account: String,
        request: venue_control_protocol::kol::TerminalOrderRequest,
        context: &egui::Context,
    ) {
        if request.limit_price.is_some()
            && !self
                .submitted_orders
                .iter()
                .any(|pending| pending.request.request_id == request.request_id)
        {
            self.submitted_orders.push(SubmittedOrder {
                account,
                request,
                receipt: None,
            });
        }
        context.request_repaint();
    }

    pub(crate) fn hidden(&self, selection: &TerminalOrderSelection) -> bool {
        self.pending
            .iter()
            .any(|(target, id)| target == selection && !self.uncertain_cancels.contains(id))
    }

    pub(crate) fn is_pending(&self, selection: &TerminalOrderSelection) -> bool {
        self.pending.iter().any(|(target, _)| target == selection)
    }

    pub(crate) fn submission_failed(&mut self, request_id: &str, definitive: bool) {
        if definitive {
            self.pending.retain(|(_, id)| id != request_id);
            self.uncertain_cancels.remove(request_id);
            self.submitted_orders
                .retain(|pending| pending.request.request_id != request_id);
        } else if self.pending.iter().any(|(_, id)| id == request_id) {
            self.uncertain_cancels.insert(request_id.into());
        }
    }

    pub(crate) fn completed(
        &mut self,
        rows: &[venue_control_protocol::kol::ExecutorCommandSummary],
    ) {
        for pending in &mut self.submitted_orders {
            if let Some(row) = rows.iter().find(|row| {
                row.origin == venue_control_protocol::kol::ExecutorCommandOrigin::Terminal
                    && row.request_id.as_ref() == Some(&pending.request.request_id)
                    && row.trading_account_id == pending.account
                    && row.symbol == pending.request.symbol
            }) && pending
                .receipt
                .as_ref()
                .is_none_or(|old| old.updated_ms <= row.updated_ms)
            {
                pending.receipt = Some(row.clone());
            }
        }
        self.submitted_orders.retain(|pending| {
            pending.receipt.as_ref().is_none_or(|row| {
                !matches!(
                    row.state,
                    venue_control_protocol::kol::ExecutorCommandState::Rejected
                        | venue_control_protocol::kol::ExecutorCommandState::Cancelled
                )
            })
        });
        for (target, id) in &self.pending {
            if let Some(row) = rows.iter().find(|row| {
                row.request_id.as_ref() == Some(id)
                    && row.trading_account_id == target.trading_account_id
                    && row.symbol == target.symbol
            }) {
                if row.state == venue_control_protocol::kol::ExecutorCommandState::ReconcileRequired
                {
                    self.uncertain_cancels.insert(id.clone());
                } else if row.state == venue_control_protocol::kol::ExecutorCommandState::Reconciled
                {
                    self.uncertain_cancels.remove(id);
                }
            }
        }
        self.pending.retain(|(_, id)| {
            !rows.iter().any(|row| {
                row.request_id.as_ref() == Some(id)
                    && matches!(
                        row.state,
                        venue_control_protocol::kol::ExecutorCommandState::Rejected
                            | venue_control_protocol::kol::ExecutorCommandState::Cancelled
                    )
            })
        });
        self.uncertain_cancels
            .retain(|id| self.pending.iter().any(|(_, pending)| pending == id));
    }

    pub(crate) fn observe(
        &mut self,
        projection: &venue_control_protocol::kol::TerminalAccountProjection,
    ) {
        self.submitted_orders.retain(|pending| {
            if pending.account != projection.trading_account_id
                || pending.request.credential_id != projection.credential_id
            {
                return true;
            }
            let Some(receipt) = &pending.receipt else {
                return true;
            };
            let matched = receipt.native_order_id.as_ref().is_some_and(|id| {
                projection.open_orders.iter().any(|order| {
                    order.symbol == pending.request.symbol
                        && order.native_order_id.as_ref() == Some(id)
                })
            });
            let refreshed_after_resolution = receipt.state
                == venue_control_protocol::kol::ExecutorCommandState::Reconciled
                && projection.observed_ms >= receipt.updated_ms;
            !matched && !refreshed_after_resolution
        });
        self.pending.retain(|(target, _)| {
            target.trading_account_id != projection.trading_account_id
                || target.credential_id != projection.credential_id
                || projection.open_orders.iter().any(|order| {
                    order.symbol == target.symbol
                        && order.native_order_id.as_ref() == Some(&target.native_order_id)
                })
        });
        self.uncertain_cancels
            .retain(|id| self.pending.iter().any(|(_, pending)| pending == id));
    }

    pub(crate) fn append_submitted(
        &self,
        model: &crate::model::AppModel,
        symbol: &str,
        settings: &super::ChartTradingSettings,
        overlays: &mut Vec<ChartOverlay>,
    ) {
        let language = model.preferences.language;
        let selected_credential = model
            .account_overview
            .as_ref()
            .and_then(|overview| overview.selected_credential_id.as_deref());
        for pending in &self.submitted_orders {
            if model.preferences.execution_account_id.as_deref() != Some(pending.account.as_str())
                || selected_credential != Some(pending.request.credential_id.as_str())
                || pending.request.symbol.to_string() != symbol
            {
                continue;
            }
            let Some(price) = pending.request.limit_price else {
                continue;
            };
            let buy = matches!(
                pending.request.action,
                venue_control_protocol::kol::TerminalAction::OpenLong
                    | venue_control_protocol::kol::TerminalAction::CloseShort
            );
            let quote = symbol.split_once('/').map_or("", |(_, quote)| quote);
            overlays.push(ChartOverlay {
                price,
                label: label(language, "只做Maker", "Maker only").into(),
                color: if buy { theme::BUY } else { theme::SELL },
                time_ms: None,
                line: settings.order_lines,
                tick: false,
                badge: Some(TradingBadge {
                    language,
                    quantity: settings
                        .order_quantity
                        .then(|| format!("{} {quote}", pending.request.quote_notional.normalize())),
                    stale: false,
                    pending: false,
                    provisional: true,
                    pnl: None,
                    selection: None,
                }),
            });
        }
    }
}

#[derive(Clone, Debug)]
enum Interaction {
    Cancel(TerminalOrderSelection),
    Preview(TerminalOrderSelection, Decimal, Decimal),
}

fn action_id() -> egui::Id {
    egui::Id::new("chart-order-tag-action")
}

fn order_id(selection: &TerminalOrderSelection) -> egui::Id {
    egui::Id::new((
        "chart-order-tag",
        &selection.credential_id,
        &selection.trading_account_id,
        selection.symbol.to_string(),
        &selection.native_order_id,
    ))
}

// The drag only previews a price: no replace command exists in the current wire contract.
// A release must never turn into an unrelated open order or a cancel-and-forget sequence.
#[allow(clippy::too_many_arguments)]
pub(super) fn draw(
    ui: &egui::Ui,
    painter: &egui::Painter,
    plot: Rect,
    range: PriceRange,
    overlay: &ChartOverlay,
    badge: &TradingBadge,
    price_y: f32,
    label_y: f32,
    scale: usize,
) {
    if plot.width() < 120.0 || plot.height() < 24.0 {
        return;
    }
    let language = badge.language;
    let color = overlay.color;
    let font = FontId::proportional(11.0);
    let title = badge.pnl.map_or_else(
        || format!("{} {}", overlay.label, format_decimal(overlay.price, scale)),
        |pnl| {
            format!(
                "{} {} {:+.2}",
                overlay.label,
                label(language, "盈亏", "PnL"),
                pnl
            )
        },
    );
    let title_galley = painter.layout_no_wrap(title, font.clone(), theme::TEXT_PRIMARY);
    let qty_galley = badge
        .quantity
        .as_ref()
        .map(|quantity| painter.layout_no_wrap(quantity.clone(), FontId::monospace(11.0), color));
    let grip_width = if badge.selection.is_some() { 16.0 } else { 0.0 };
    let cancel_width = if badge.selection.is_some() { 24.0 } else { 0.0 };
    let qty_width = qty_galley
        .as_ref()
        .map_or(0.0, |galley| galley.size().x + 16.0)
        .min((plot.width() * 0.3).min(104.0));
    let title_width = (title_galley.size().x + 14.0)
        .min((plot.width() - grip_width - qty_width - cancel_width - 12.0).max(24.0));
    let total = grip_width + title_width + qty_width + cancel_width;
    let rect = Rect::from_min_size(
        Pos2::new(plot.left() + 5.0, label_y - 11.0),
        egui::vec2(total, 22.0),
    );
    let title_rect = Rect::from_min_max(
        rect.min + egui::vec2(grip_width, 0.0),
        Pos2::new(rect.left() + grip_width + title_width, rect.bottom()),
    );
    let cancel_rect =
        Rect::from_min_max(Pos2::new(rect.right() - cancel_width, rect.top()), rect.max);
    let body = Rect::from_min_max(rect.min, Pos2::new(cancel_rect.left(), rect.bottom()));
    let mut hovered_cancel = false;
    if let Some(selection) = &badge.selection {
        let id = order_id(selection);
        let pending = badge.pending;
        let enabled = !badge.stale && !pending;
        let response = ui.interact(
            body.intersect(plot),
            ui.id().with(id).with("drag"),
            if enabled {
                Sense::click_and_drag()
            } else {
                Sense::click()
            },
        );
        response.clone().on_hover_text(if pending {
            label(
                language,
                "撤单结果待确认，请勿重复提交",
                "Cancellation awaiting confirmation; do not resubmit",
            )
        } else if badge.stale {
            label(
                language,
                "账户数据待刷新，暂不可操作",
                "Account data is stale; actions disabled",
            )
        } else {
            label(
                language,
                "上下拖动预览新价格；尚未接通安全改单，不会自动提交",
                "Drag to preview a price; safe amendment is not connected and no order is sent",
            )
        });
        if enabled && response.hovered() {
            ui.ctx().set_cursor_icon(egui::CursorIcon::Grab);
        }
        if response.dragged() || response.drag_stopped() {
            ui.ctx().set_cursor_icon(egui::CursorIcon::Grabbing);
            let escaped = ui.input(|input| input.key_pressed(egui::Key::Escape));
            if escaped {
                ui.ctx()
                    .data_mut(|data| data.insert_temp(id.with("aborted"), true));
            }
            let aborted = ui
                .ctx()
                .data(|data| data.get_temp::<bool>(id.with("aborted")))
                .unwrap_or(false);
            if !aborted
                && let Some(pointer) = ui.input(|input| input.pointer.interact_pos())
                && plot.contains(pointer)
                && let Some(price) = preview_price(range, plot, pointer.y, scale)
            {
                painter.extend(egui::Shape::dashed_line(
                    &[
                        Pos2::new(plot.left(), pointer.y),
                        Pos2::new(plot.right(), pointer.y),
                    ],
                    Stroke::new(1.0, theme::WARNING),
                    5.0,
                    4.0,
                ));
                let preview = format!(
                    "{} {}",
                    label(language, "预览·未提交", "Preview · not sent"),
                    format_decimal(price, scale)
                );
                let galley = painter.layout_no_wrap(preview, font.clone(), theme::WARNING);
                let preview_rect = Rect::from_min_size(
                    Pos2::new(rect.left(), (pointer.y - 25.0).max(plot.top())),
                    galley.size() + egui::vec2(10.0, 6.0),
                );
                painter.rect_filled(preview_rect, 3, theme::BG_SECONDARY);
                painter.galley(
                    preview_rect.min + egui::vec2(5.0, 3.0),
                    galley,
                    theme::WARNING,
                );
                if response.drag_stopped() && price != overlay.price {
                    ui.ctx().data_mut(|data| {
                        data.insert_temp(
                            action_id(),
                            Interaction::Preview(selection.clone(), overlay.price, price),
                        )
                    });
                }
            }
            if response.drag_stopped() {
                ui.ctx()
                    .data_mut(|data| data.remove::<bool>(id.with("aborted")));
            }
        }
        let cancel = ui.interact(
            cancel_rect.intersect(plot),
            ui.id().with(id).with("cancel"),
            Sense::click(),
        );
        hovered_cancel = enabled && cancel.hovered();
        cancel.clone().on_hover_text(if enabled {
            label(
                language,
                "撤销此笔委托（不停止运行策略）",
                "Cancel this order (running strategies are not stopped)",
            )
        } else {
            label(
                language,
                "等待账户刷新或撤单确认",
                "Awaiting fresh account data or cancellation confirmation",
            )
        });
        if enabled && cancel.clicked() {
            ui.ctx().data_mut(|data| {
                data.insert_temp(action_id(), Interaction::Cancel(selection.clone()))
            });
        }
    }
    if (price_y - label_y).abs() > 1.0 {
        painter.line_segment(
            [Pos2::new(rect.left() - 3.0, price_y), rect.left_center()],
            Stroke::new(1.0, color.gamma_multiply(0.6)),
        );
    }
    painter.rect_filled(rect, 4, theme::BG_SECONDARY);
    let head_color = badge.pnl.map_or(color, |pnl| {
        if pnl < Decimal::ZERO {
            theme::SELL
        } else {
            theme::BUY
        }
    });
    painter.rect_filled(
        title_rect,
        0,
        head_color.gamma_multiply(if badge.stale || badge.provisional {
            0.35
        } else {
            0.8
        }),
    );
    painter.with_clip_rect(title_rect.intersect(plot)).galley(
        title_rect.left_center() + egui::vec2(7.0, -title_galley.size().y * 0.5),
        title_galley,
        theme::TEXT_PRIMARY,
    );
    if let Some(galley) = qty_galley {
        painter
            .with_clip_rect(
                Rect::from_min_max(title_rect.right_top(), cancel_rect.left_bottom())
                    .intersect(plot),
            )
            .galley(
                Pos2::new(
                    title_rect.right() + 8.0,
                    rect.center().y - galley.size().y * 0.5,
                ),
                galley,
                color,
            );
    }
    if badge.selection.is_some() {
        for offset in [-3.0, 0.0, 3.0] {
            painter.line_segment(
                [
                    Pos2::new(rect.left() + 5.0, label_y + offset),
                    Pos2::new(rect.left() + 11.0, label_y + offset),
                ],
                Stroke::new(1.0, color),
            );
        }
        if hovered_cancel {
            painter.rect_filled(cancel_rect.shrink(1.0), 3, color.gamma_multiply(0.25));
        }
        painter.line_segment(
            [cancel_rect.left_top(), cancel_rect.left_bottom()],
            Stroke::new(1.0, color),
        );
        let center = cancel_rect.center();
        if badge.pending {
            painter.text(
                center,
                Align2::CENTER_CENTER,
                "…",
                FontId::proportional(12.0),
                color,
            );
        } else {
            for direction in [-1.0, 1.0] {
                painter.line_segment(
                    [
                        center + egui::vec2(-3.0, -3.0 * direction),
                        center + egui::vec2(3.0, 3.0 * direction),
                    ],
                    Stroke::new(1.2, color),
                );
            }
        }
    }
    if badge.stale {
        painter.circle_filled(rect.left_top() + egui::vec2(3.0, 3.0), 2.5, theme::WARNING);
    }
    let detail = format!(
        "{} · {} · {}",
        overlay.label,
        format_decimal(overlay.price, scale),
        if badge.provisional {
            label(
                language,
                "价格与金额来自本次请求，以实际委托回报为准",
                "Price and amount come from this request; exchange confirmation is authoritative",
            )
        } else if badge.stale {
            label(language, "账户数据待刷新", "Account data is stale")
        } else {
            label(language, "已观察委托/持仓", "Observed order/position")
        }
    );
    if badge.selection.is_none() {
        ui.interact(
            rect,
            ui.id().with((
                "position-badge",
                overlay.label.as_str(),
                overlay.price.to_string(),
            )),
            Sense::hover(),
        )
        .on_hover_text(detail);
    }
    if overlay.tick && rect.right() + 90.0 < plot.right() {
        painter.text(
            Pos2::new(plot.right() - 10.0, price_y),
            Align2::RIGHT_CENTER,
            format_decimal(overlay.price, scale),
            FontId::monospace(11.0),
            color,
        );
        painter.line_segment(
            [
                Pos2::new(plot.right() - 8.0, price_y),
                Pos2::new(plot.right(), price_y),
            ],
            Stroke::new(2.0, color),
        );
    }
    if !badge.provisional {
        painter.rect_stroke(
            rect,
            4,
            Stroke::new(
                1.0,
                color.gamma_multiply(if badge.stale { 0.55 } else { 1.0 }),
            ),
            egui::StrokeKind::Inside,
        );
    } else {
        painter.extend(egui::Shape::dashed_line(
            &[
                rect.left_top(),
                rect.right_top(),
                rect.right_bottom(),
                rect.left_bottom(),
                rect.left_top(),
            ],
            Stroke::new(1.0, color),
            3.0,
            3.0,
        ));
    }
}

fn preview_price(range: PriceRange, plot: Rect, y: f32, scale: usize) -> Option<Decimal> {
    let value = range.y_to_price(plot.top(), plot.height(), y)?;
    Decimal::from_f64_retain(value)
        .map(|price| price.round_dp(scale.min(28) as u32))
        .filter(|price| *price > Decimal::ZERO)
}

pub(crate) fn apply_interaction(
    model: &mut crate::model::AppModel,
    client: &crate::client::ControlClient,
    context: &egui::Context,
) {
    if !model.execution.chart_orders.pending.is_empty()
        || !model.execution.chart_orders.submitted_orders.is_empty()
    {
        context.request_repaint_after(std::time::Duration::from_millis(100));
    }
    let action = context.data_mut(|data| {
        let action = data.get_temp::<Interaction>(action_id());
        data.remove::<Interaction>(action_id());
        action
    });
    if let Some(action) = action {
        let selection = match &action {
            Interaction::Cancel(selection) | Interaction::Preview(selection, _, _) => selection,
        };
        if !target_is_current(model, selection) {
            return;
        }
        match action {
            Interaction::Cancel(selection) => {
                if model.execution.chart_orders.is_pending(&selection) {
                    return;
                }
                model.select_symbol(selection.symbol.to_string());
                model.trade_dock.select_terminal_order(selection.clone());
                crate::trade_dock::apply_action(
                    model,
                    client,
                    venue_control_protocol::TradingAction::CancelSelectedOrder,
                    context,
                );
            }
            preview @ Interaction::Preview(..) => {
                context.data_mut(|data| data.insert_temp(action_id().with("preview"), preview));
            }
        }
    }
    let preview_id = action_id().with("preview");
    if let Some(Interaction::Preview(selection, old, new)) =
        context.data(|data| data.get_temp::<Interaction>(preview_id))
    {
        if !target_is_current(model, &selection) {
            context.data_mut(|data| data.remove::<Interaction>(preview_id));
            return;
        }
        let language = model.preferences.language;
        let mut open = true;
        egui::Window::new(label(
            language,
            "改单预览 · 未提交",
            "Amendment preview · not submitted",
        ))
        .id(preview_id)
        .open(&mut open)
        .collapsible(false)
        .resizable(false)
        .show(context, |ui| {
            ui.label(format!(
                "{}   {} → {}",
                selection.symbol,
                old.normalize(),
                new.normalize()
            ));
            ui.colored_label(
                theme::WARNING,
                label(
                    language,
                    "原委托未改变；当前后端尚不支持安全改单。",
                    "Original order unchanged; safe amendment is not supported by the backend yet.",
                ),
            );
            ui.label(label(
                language,
                "本次拖动没有撤单、下单或修改下单面板。",
                "This drag did not cancel, place, or change the order form.",
            ));
        });
        if !open {
            context.data_mut(|data| data.remove::<Interaction>(preview_id));
        }
    }
}

fn target_is_current(model: &crate::model::AppModel, selection: &TerminalOrderSelection) -> bool {
    model.preferences.execution_account_id.as_ref() == Some(&selection.trading_account_id)
        && model.account_overview.as_ref().is_some_and(|overview| {
            overview.selected_credential_id.as_ref() == Some(&selection.credential_id)
        })
        && model.execution.private_ready(
            Some(&selection.trading_account_id),
            crate::account_center::now_ms(),
        )
        && model
            .execution
            .private_projection_for(Some(&selection.trading_account_id))
            .is_some_and(|projection| {
                projection.credential_id == selection.credential_id
                    && projection.open_orders.iter().any(|order| {
                        order.symbol == selection.symbol
                            && order.native_order_id.as_ref() == Some(&selection.native_order_id)
                    })
            })
}
