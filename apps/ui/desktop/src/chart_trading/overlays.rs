use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Stroke};
use rust_decimal::Decimal;
use venue_control_protocol::UiBar;
use venue_domain::{OrderSide, PositionSide};
mod fills;

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
    let tick_enabled = settings.price_lines && settings.price_labels && settings.ticks;
    if let Some(projection) = model
        .execution
        .private_projection_for(model.preferences.execution_account_id.as_deref())
        .filter(|projection| {
            model.selected_execution_credential().is_some_and(|c| {
                c.venue == model.preferences.market_server.venue()
                    && c.credential_id == projection.credential_id
                    && c.trading_account_id.as_ref() == Some(&projection.trading_account_id)
            })
        })
    {
        let fresh = model.execution.private_ready(
            model.preferences.execution_account_id.as_deref(),
            crate::account_center::now_ms(),
        );
        #[cfg(not(target_arch = "wasm32"))]
        if fresh {
            crate::latency_evidence::bind_orders(model, projection);
        }
        let suffix = if fresh {
            ""
        } else {
            label(language, " · 待刷新", " · stale")
        };
        if settings.current_orders {
            for order in projection.open_orders.iter().filter(|order| {
                order.symbol.to_string() == symbol
                    && order
                        .filled_quantity
                        .is_none_or(|filled| filled < order.quantity)
            }) {
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
                    label: format!(
                        "{} · {}",
                        order_intent(
                            language,
                            order.order_side,
                            order.position_side,
                            order.reduce_only
                        ),
                        if order.post_only {
                            label(language, "只做Maker", "Maker only")
                        } else {
                            label(language, "限价委托", "Limit order")
                        }
                    ),
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
                        pnl: None,
                        position: None,
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
                    color: theme::POSITION_LINE,
                    time_ms: None,
                    line: true,
                    tick: tick_enabled && settings.tick_positions,
                    badge: Some(super::order_tags::TradingBadge {
                        language,
                        quantity: Some(position.quantity.normalize().to_string()),
                        stale: !fresh,
                        pending: false,
                        selection: None,
                        pnl: crate::execution_view::live_position_pnl_value(model, position),
                        position: crate::execution_view::chart_position_draft(
                            model, projection, position,
                        ),
                    }),
                });
            }
        }
        if settings.history {
            for fill in fills::by_order(&projection.fills, symbol) {
                if let Some(time_ms) = fill.occurred_ms {
                    result.push(ChartOverlay {
                        price: fill.price,
                        label: format!(
                            "{} {} @ {}\nID: {}",
                            if fill.order_side == OrderSide::Buy {
                                label(language, "买入成交", "Buy fill")
                            } else {
                                label(language, "卖出成交", "Sell fill")
                            },
                            fill.quantity.normalize(),
                            format_decimal(fill.price, model.market_scales(symbol).0),
                            fill.native_order_id
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

pub(crate) fn order_intent(
    language: crate::i18n::Language,
    side: OrderSide,
    position: PositionSide,
    reduce_only: bool,
) -> &'static str {
    match (side, position, reduce_only) {
        (OrderSide::Buy, PositionSide::Long, _) => label(language, "开多", "Open long"),
        (OrderSide::Sell, PositionSide::Long, _) => label(language, "平多", "Close long"),
        (OrderSide::Sell, PositionSide::Short, _) => label(language, "开空", "Open short"),
        (OrderSide::Buy, PositionSide::Short, _) => label(language, "平空", "Close short"),
        (OrderSide::Buy, PositionSide::Net, true) => label(language, "平空", "Close short"),
        (OrderSide::Sell, PositionSide::Net, true) => label(language, "平多", "Close long"),
        // A non-reduce-only Net order may both close and open. Do not infer intent from price.
        (OrderSide::Buy, PositionSide::Net, false) => label(language, "买入", "Buy"),
        (OrderSide::Sell, PositionSide::Net, false) => label(language, "卖出", "Sell"),
    }
}

#[cfg(test)]
mod intent_tests {
    use super::*;
    use crate::i18n::Language;

    #[test]
    fn hedge_intent_uses_both_sides_even_without_reduce_only() {
        for (side, position, zh, en) in [
            (OrderSide::Buy, PositionSide::Long, "开多", "Open long"),
            (OrderSide::Sell, PositionSide::Long, "平多", "Close long"),
            (OrderSide::Buy, PositionSide::Short, "平空", "Close short"),
            (OrderSide::Sell, PositionSide::Short, "开空", "Open short"),
        ] {
            for reduce in [false, true] {
                assert_eq!(
                    order_intent(Language::SimplifiedChinese, side, position, reduce),
                    zh
                );
                assert_eq!(order_intent(Language::English, side, position, reduce), en);
            }
        }
        assert_eq!(
            order_intent(Language::English, OrderSide::Buy, PositionSide::Net, false),
            "Buy"
        );
        assert_eq!(
            order_intent(Language::English, OrderSide::Sell, PositionSide::Net, false),
            "Sell"
        );
        assert_eq!(
            order_intent(Language::English, OrderSide::Buy, PositionSide::Net, true),
            "Close short"
        );
        assert_eq!(
            order_intent(Language::English, OrderSide::Sell, PositionSide::Net, true),
            "Close long"
        );
    }
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
    let mut occupied = Vec::<Rect>::new();
    let mut price_lines = Vec::new();
    let mut fill_stacks = std::collections::HashMap::<(usize, bool), usize>::new();
    let mut sorted = overlays.iter().enumerate().collect::<Vec<_>>();
    sorted.sort_by(|(_, a), (_, b)| b.price.cmp(&a.price));
    for (index, overlay) in sorted {
        let Some(raw_y) =
            range.price_to_y(rect.top(), rect.height(), decimal_to_f64(overlay.price))
        else {
            continue;
        };
        let off_scale = raw_y < rect.top() || raw_y > rect.bottom();
        if off_scale {
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
            let buy = overlay.color == theme::BUY;
            let direction = if buy { 1.0 } else { -1.0 };
            let candle = &bars[bar_index];
            let anchor_price = if buy { candle.low } else { candle.high };
            let anchor_y = range
                .price_to_y(rect.top(), rect.height(), decimal_to_f64(anchor_price))
                .unwrap_or(y);
            let stack = fill_stacks.entry((bar_index, buy)).or_default();
            let center = Pos2::new(x, anchor_y + direction * (13.0 + *stack as f32 * 20.0));
            *stack += 1;
            let marker = Rect::from_center_size(center, egui::vec2(18.0, 18.0));
            painter.rect_filled(marker, 5, overlay.color);
            painter.text(
                center,
                Align2::CENTER_CENTER,
                if buy { "B" } else { "S" },
                FontId::proportional(13.0),
                Color32::WHITE,
            );
            let response = ui.interact(
                marker.intersect(rect),
                ui.id().with(("chart-fill", index, time)),
                egui::Sense::hover(),
            );
            if response.hovered() {
                egui::Tooltip::for_widget(&response).show(|ui| {
                    ui.label(&overlay.label);
                });
            }
            continue;
        }
        if overlay.label.is_empty() {
            if overlay.line {
                price_lines.push((
                    y,
                    overlay.color,
                    rect.left() - 2.0,
                    None,
                    Some(overlay.price),
                ));
            }
            continue;
        }
        let label_y = y;
        if let Some(badge) = &overlay.badge {
            let badge_rect =
                super::order_tags::draw(ui, &painter, rect, range, overlay, badge, y, price_scale);
            if badge_rect.is_positive() {
                occupied.push(badge_rect);
            }
            if overlay.line && badge_rect.is_positive() {
                price_lines.push((
                    y,
                    overlay.color,
                    badge_rect.right(),
                    (!badge.stale && !badge.pending)
                        .then_some(badge.selection.as_ref())
                        .flatten(),
                    None,
                ));
            }
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
        occupied.push(label_rect);
        if overlay.line {
            price_lines.push((y, overlay.color, label_rect.right(), None, None));
        }
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
                FontId::proportional(11.0),
                overlay.color,
            );
        }
    }
    for (y, color, start, order_evidence, market_evidence) in price_lines {
        let mut gaps = occupied
            .iter()
            .filter(|rect| rect.top() <= y && rect.bottom() >= y)
            .map(|rect| (rect.left() - 2.0, rect.right() + 2.0))
            .collect::<Vec<_>>();
        gaps.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut left = start + 2.0;
        for (gap_left, gap_right) in gaps {
            let end = gap_left.min(rect.right());
            draw_dash(&painter, left, end, y, color);
            #[cfg(not(target_arch = "wasm32"))]
            evidence_segment(ui, &painter, left, end, y, order_evidence, market_evidence);
            left = left.max(gap_right);
        }
        draw_dash(&painter, left, rect.right(), y, color);
        #[cfg(not(target_arch = "wasm32"))]
        evidence_segment(
            ui,
            &painter,
            left,
            rect.right(),
            y,
            order_evidence,
            market_evidence,
        );
        #[cfg(target_arch = "wasm32")]
        let _ = (order_evidence, market_evidence);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn evidence_segment(
    ui: &egui::Ui,
    painter: &egui::Painter,
    left: f32,
    right: f32,
    y: f32,
    order: Option<&crate::trading::TerminalOrderSelection>,
    market: Option<Decimal>,
) {
    // Probe inside the first painted dash, after subtracting every overlapping label.
    let point = Pos2::new(left + 1.0 / painter.ctx().pixels_per_point(), y);
    if right - left >= 2.0 / painter.ctx().pixels_per_point()
        && painter.clip_rect().contains(point)
        && ui.ctx().layer_id_at(point) == Some(ui.layer_id())
    {
        if let Some(order) = order {
            crate::latency_evidence::order_painted(order);
        }
        if let Some(price) = market {
            crate::latency_evidence::market_painted(price);
        }
    }
}

fn draw_dash(painter: &egui::Painter, left: f32, right: f32, y: f32, color: Color32) {
    if right > left {
        // egui rounds both ends to pixel centers. A one-point dash can collapse to
        // zero length, so retain at least two physical pixels at every UI scale.
        let minimum_dash = 2.0 / painter.ctx().pixels_per_point();
        let dash = if color == theme::TEXT_SECONDARY {
            4.0_f32
        } else {
            3.0_f32
        };
        painter.extend(egui::Shape::dashed_line(
            &[Pos2::new(left, y), Pos2::new(right, y)],
            Stroke::new(1.25, color),
            dash.max(minimum_dash),
            3.0,
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_updates_bypass_one_second_market_cadence() {
        let mut model = crate::account_scope::tests::model();
        model.preferences.trading.chart_cadence = crate::trading::DisplayCadence::Ms1000;
        let mut projection = crate::account_scope::tests::projection(1);
        let settings = ChartTradingSettings::default();
        model
            .execution
            .apply_private(Some(projection.clone()), &mut model.trade_dock);
        assert!(collect(&model, "BTC/USDC", &settings).is_empty());
        projection
            .open_orders
            .push(venue_control_protocol::kol::TerminalOpenOrder {
                client_order_id: "order-fixture".into(),
                native_order_id: Some("123456".into()),
                symbol: "BTC/USDC".parse().unwrap(),
                order_side: venue_domain::OrderSide::Buy,
                position_side: venue_domain::PositionSide::Long,
                quantity: Decimal::ONE,
                filled_quantity: Some(Decimal::ZERO),
                limit_price: Some(100.into()),
                post_only: true,
                time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
                reduce_only: false,
                state: venue_control_protocol::kol::TerminalOrderState::New,
                created_ms: Some(projection.observed_ms),
            });
        model
            .execution
            .apply_private(Some(projection.clone()), &mut model.trade_dock);
        assert!(
            collect(&model, "BTC/USDC", &settings)
                .iter()
                .any(|o| o.price == Decimal::from(100))
        );
        projection.open_orders.clear();
        model
            .execution
            .apply_private(Some(projection), &mut model.trade_dock);
        assert!(collect(&model, "BTC/USDC", &settings).is_empty());
    }

    #[test]
    fn price_line_mesh_survives_pixel_rounding_at_multiple_scales() {
        for scale in [0.75, 1.0, 1.25, 1.5, 2.0] {
            for offset in [0.0, 0.25, 0.5, 0.75] {
                for color in [
                    theme::BUY,
                    theme::SELL,
                    theme::POSITION_LINE,
                    theme::TEXT_SECONDARY,
                ] {
                    let context = egui::Context::default();
                    context.set_pixels_per_point(scale);
                    let mut output = context.run_ui(
                        egui::RawInput {
                            screen_rect: Some(Rect::from_min_size(
                                Pos2::ZERO,
                                egui::vec2(400.0, 200.0),
                            )),
                            ..Default::default()
                        },
                        |ui| {
                            draw_dash(ui.painter(), 100.0 + offset, 300.0, 80.0, color);
                        },
                    );
                    output.textures_delta.clear();
                    let shapes = output.shapes.into_iter().filter(|shape| matches!(
                        &shape.shape, egui::Shape::LineSegment { stroke, .. } if stroke.color == color
                    )).collect();
                    let meshes = context.tessellate(shapes, scale);
                    let mut visible_area = 0.0;
                    let mut rightmost = 0.0_f32;
                    for primitive in meshes {
                        if let egui::epaint::Primitive::Mesh(mesh) = primitive.primitive {
                            for triangle in mesh.indices.chunks_exact(3) {
                                let a = mesh.vertices[triangle[0] as usize];
                                let b = mesh.vertices[triangle[1] as usize];
                                let c = mesh.vertices[triangle[2] as usize];
                                if ![a, b, c].iter().any(|v| {
                                    v.color.a() > 0
                                        && v.color.r() == color.r()
                                        && v.color.g() == color.g()
                                        && v.color.b() == color.b()
                                }) {
                                    continue;
                                }
                                let ab = b.pos - a.pos;
                                let ac = c.pos - a.pos;
                                let area = (ab.x * ac.y - ab.y * ac.x).abs() * 0.5;
                                visible_area += area;
                                if area > 0.0 {
                                    rightmost = rightmost.max(a.pos.x.max(b.pos.x).max(c.pos.x));
                                }
                            }
                        }
                    }
                    assert!(
                        visible_area > 20.0,
                        "invisible dashes: scale={scale}, offset={offset}, color={color:?}, area={visible_area}"
                    );
                    assert!(
                        rightmost > 290.0,
                        "line did not reach the right edge: {rightmost}"
                    );
                }
            }
        }
    }
}
