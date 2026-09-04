use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Stroke};
use rust_decimal::Decimal;
use venue_control_protocol::UiBar;
use venue_domain::{OrderSide, PositionSide};

use super::{ChartTradingSettings, label};
use crate::{
    chart::{ChartInterval, PriceRange, bar_center_x},
    model::{AppModel, decimal_to_f64, format_decimal},
    theme,
};

#[derive(Clone, Debug)]
pub(crate) struct ChartOverlay {
    pub price: Decimal,
    pub label: String,
    pub color: Color32,
    pub time_ms: Option<u64>,
    pub line: bool,
    pub tick: bool,
    pub badge: Option<super::order_tags::TradingBadge>,
}

pub(crate) fn collect(
    model: &AppModel,
    symbol: &str,
    settings: &ChartTradingSettings,
) -> Vec<ChartOverlay> {
    let language = model.preferences.language;
    let mut result = Vec::new();
    if settings.current_orders {
        model
            .execution
            .chart_orders
            .append_submitted(model, symbol, settings, &mut result);
    }
    let tick_enabled = settings.price_lines && settings.price_labels && settings.ticks;
    if let Some(projection) = model
        .execution
        .private_projection_for(model.preferences.execution_account_id.as_deref())
    {
        let fresh = model.execution.private_ready(
            model.preferences.execution_account_id.as_deref(),
            crate::account_center::now_ms(),
        );
        let suffix = if fresh {
            ""
        } else {
            label(language, " · 待刷新", " · stale")
        };
        if settings.current_orders {
            for order in projection
                .open_orders
                .iter()
                .filter(|order| order.symbol.to_string() == symbol)
            {
                let Some(price) = order.limit_price.filter(|price| *price > Decimal::ZERO) else {
                    continue;
                };
                let quantity = settings.order_quantity.then(|| {
                    order.filled_quantity.map_or_else(
                        || "—".into(),
                        |filled| {
                            (order.quantity - filled)
                                .max(Decimal::ZERO)
                                .normalize()
                                .to_string()
                        },
                    )
                });
                let selection = order.native_order_id.as_ref().map(|id| {
                    crate::trading::TerminalOrderSelection {
                        credential_id: projection.credential_id.clone(),
                        trading_account_id: projection.trading_account_id.clone(),
                        symbol: order.symbol.clone(),
                        native_order_id: id.clone(),
                    }
                });
                if selection
                    .as_ref()
                    .is_some_and(|selection| model.execution.chart_orders.hidden(selection))
                {
                    continue;
                }
                result.push(ChartOverlay {
                    price,
                    label: if order.post_only {
                        label(language, "只做Maker", "Maker only")
                    } else {
                        label(language, "限价委托", "Limit order")
                    }
                    .into(),
                    color: side_color(order.order_side),
                    time_ms: None,
                    line: settings.order_lines,
                    tick: tick_enabled && settings.tick_orders,
                    badge: Some(super::order_tags::TradingBadge {
                        language,
                        quantity,
                        stale: !fresh,
                        pending: selection.as_ref().is_some_and(|selection| {
                            model.execution.chart_orders.is_pending(selection)
                        }),
                        provisional: false,
                        pnl: None,
                        selection,
                    }),
                });
            }
        }
        if settings.positions {
            for position in projection.positions.iter().filter(|position| {
                position.symbol.to_string() == symbol && !position.quantity.is_zero()
            }) {
                let Some(price) = position.entry_price.filter(|price| *price > Decimal::ZERO)
                else {
                    continue;
                };
                let long = position.position_side == PositionSide::Long;
                result.push(ChartOverlay {
                    price,
                    label: if long {
                        label(language, "多仓", "Long")
                    } else {
                        label(language, "空仓", "Short")
                    }
                    .into(),
                    color: if long { theme::BUY } else { theme::SELL },
                    time_ms: None,
                    line: true,
                    tick: tick_enabled && settings.tick_positions,
                    badge: Some(super::order_tags::TradingBadge {
                        language,
                        quantity: Some(position.quantity.normalize().to_string()),
                        stale: !fresh,
                        pending: false,
                        provisional: false,
                        selection: None,
                        pnl: crate::execution_view::position_pnl_value(position),
                    }),
                });
            }
        }
        if settings.history {
            for fill in projection
                .fills
                .iter()
                .filter(|fill| fill.symbol.to_string() == symbol)
            {
                if let Some(time_ms) = fill.occurred_ms {
                    result.push(ChartOverlay {
                        price: fill.price,
                        label: format!(
                            "{} {} @ {}",
                            if fill.order_side == OrderSide::Buy {
                                label(language, "买入成交", "Buy fill")
                            } else {
                                label(language, "卖出成交", "Sell fill")
                            },
                            fill.quantity.normalize(),
                            fill.price.normalize()
                        ),
                        color: side_color(fill.order_side),
                        time_ms: Some(time_ms),
                        line: false,
                        tick: false,
                        badge: None,
                    });
                }
            }
        }
        if settings.price_lines
            && settings.mark_price
            && let Some(price) = projection
                .positions
                .iter()
                .find(|position| position.symbol.to_string() == symbol)
                .and_then(|position| position.mark_price)
                .filter(|price| *price > Decimal::ZERO)
        {
            result.push(ChartOverlay {
                price,
                label: format!("{}{}", label(language, "标记价格", "Mark price"), suffix),
                color: theme::WARNING,
                time_ms: None,
                line: true,
                tick: tick_enabled && settings.tick_prices,
                badge: None,
            });
        }
    }
    if settings.alerts {
        for alert in model
            .preferences
            .chart_alerts
            .items
            .iter()
            .filter(|alert| alert.active && alert.symbol == symbol)
        {
            result.push(ChartOverlay {
                price: alert.price,
                label: label(language, "价格提醒", "Price alert").into(),
                color: theme::BRAND,
                time_ms: None,
                line: true,
                tick: false,
                badge: None,
            });
        }
    }
    if settings.order_preview
        && symbol == model.preferences.selected_symbol
        && let Some(price) = model.trade_dock.selected_price
    {
        result.push(ChartOverlay {
            price,
            label: label(
                language,
                "订单预览 · 未提交",
                "Order preview · not submitted",
            )
            .into(),
            color: theme::TEXT_SECONDARY,
            time_ms: None,
            line: true,
            tick: false,
            badge: None,
        });
    }
    result
}

fn side_color(side: OrderSide) -> Color32 {
    if side == OrderSide::Buy {
        theme::BUY
    } else {
        theme::SELL
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn draw(
    ui: &egui::Ui,
    painter: &egui::Painter,
    rect: Rect,
    bars: &[UiBar],
    slots: usize,
    interval: ChartInterval,
    range: PriceRange,
    overlays: &[ChartOverlay],
    price_scale: usize,
    settings: &ChartTradingSettings,
) {
    let painter = painter.with_clip_rect(rect);
    let mut label_rows = Vec::<f32>::new();
    let mut sorted = overlays.iter().enumerate().collect::<Vec<_>>();
    sorted.sort_by(|(_, a), (_, b)| {
        b.badge
            .as_ref()
            .is_some_and(|badge| badge.provisional)
            .cmp(&a.badge.as_ref().is_some_and(|badge| badge.provisional))
            .then_with(|| b.price.cmp(&a.price))
    });
    for (index, overlay) in sorted {
        let Some(raw_y) =
            range.price_to_y(rect.top(), rect.height(), decimal_to_f64(overlay.price))
        else {
            continue;
        };
        let off_scale = raw_y < rect.top() || raw_y > rect.bottom();
        if off_scale
            && !overlay
                .badge
                .as_ref()
                .is_some_and(|badge| badge.provisional)
        {
            continue;
        }
        let y = raw_y.clamp(rect.top(), rect.bottom());
        if let Some(time) = overlay.time_ms {
            let Some(bar_index) = bars.iter().position(|bar| {
                time >= bar.open_time_ms
                    && time < bar.open_time_ms.saturating_add(interval.duration_ms())
            }) else {
                continue;
            };
            let Some(x) = bar_center_x(rect.left(), rect.width(), slots, bar_index) else {
                continue;
            };
            let direction = if overlay.color == theme::BUY {
                1.0
            } else {
                -1.0
            };
            let center = Pos2::new(x, y);
            painter.add(egui::Shape::convex_polygon(
                vec![
                    center,
                    center + egui::vec2(-4.0, direction * 8.0),
                    center + egui::vec2(4.0, direction * 8.0),
                ],
                overlay.color,
                Stroke::NONE,
            ));
            ui.interact(
                Rect::from_center_size(center, egui::vec2(12.0, 18.0)),
                ui.id().with(("chart-fill", index, time)),
                egui::Sense::hover(),
            )
            .on_hover_text(&overlay.label);
            continue;
        }
        if overlay.line && !off_scale {
            painter.extend(egui::Shape::dashed_line(
                &[Pos2::new(rect.left(), y), Pos2::new(rect.right(), y)],
                Stroke::new(1.0, overlay.color),
                4.0,
                3.0,
            ));
        }
        if overlay.label.is_empty() {
            continue;
        }
        let mut label_y = y.clamp(rect.top() + 12.0, rect.bottom() - 12.0);
        for _ in 0..label_rows.len() {
            if label_rows
                .iter()
                .all(|other| (label_y - other).abs() >= 24.0)
            {
                break;
            }
            label_y += 24.0;
        }
        if label_y > rect.bottom() - 12.0 {
            continue;
        }
        label_rows.push(label_y);
        if let Some(badge) = &overlay.badge {
            super::order_tags::draw(
                ui,
                &painter,
                rect,
                range,
                overlay,
                badge,
                y,
                label_y,
                price_scale,
            );
            continue;
        }
        let text = format!(
            "{}  {}",
            overlay.label,
            format_decimal(overlay.price, price_scale)
        );
        let galley = painter.layout_no_wrap(text, FontId::proportional(11.0), overlay.color);
        let label_rect = Rect::from_min_size(
            Pos2::new(rect.left() + 5.0, label_y - 8.0),
            galley.size() + egui::vec2(8.0, 4.0),
        );
        painter.rect_filled(label_rect, 2, theme::BG_SECONDARY);
        painter.galley(label_rect.min + egui::vec2(4.0, 2.0), galley, overlay.color);
        if settings.price_labels && overlay.tick {
            painter.line_segment(
                [Pos2::new(rect.right() - 8.0, y), Pos2::new(rect.right(), y)],
                Stroke::new(2.0, overlay.color),
            );
            painter.text(
                Pos2::new(rect.right() - 10.0, y),
                Align2::RIGHT_CENTER,
                format_decimal(overlay.price, price_scale),
                FontId::monospace(11.0),
                overlay.color,
            );
        }
    }
}
