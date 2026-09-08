use eframe::egui::{self, Align, Align2, Color32, FontId, Pos2, Rect, RichText, Sense};
use rust_decimal::Decimal;
use venue_control_protocol::{AggressorSide, UiBookLevel, UiTrade};

use crate::{
    i18n::{Language, TextKey, text},
    model::{AppModel, decimal_to_f64},
    theme,
};

const BOOK_ROWS_PER_SIDE: usize = 6;
const SINGLE_SIDE_ROWS: usize = 15;
const BOOK_ROW_HEIGHT: f32 = 18.0;
const TRADE_ROW_HEIGHT: f32 = 17.0;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum BookMode {
    #[default]
    Both,
    Bids,
    Asks,
}

impl BookMode {
    const fn shows_bids(self) -> bool {
        matches!(self, Self::Both | Self::Bids)
    }

    const fn shows_asks(self) -> bool {
        matches!(self, Self::Both | Self::Asks)
    }
}

#[allow(clippy::too_many_arguments)]
pub fn show(
    ui: &mut egui::Ui,
    instance: u32,
    asks: &[UiBookLevel],
    bids: &[UiBookLevel],
    trades: &[UiTrade],
    last: Option<Decimal>,
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    language: Language,
    model: &AppModel,
    symbol: &str,
) -> Option<Decimal> {
    let mut selected = None;
    section_title(ui, text(language, TextKey::OrderBook));
    let mode_id = ui.make_persistent_id(("venueflow-book-mode", instance));
    let mut mode = ui.data(|data| data.get_temp::<BookMode>(mode_id).unwrap_or_default());
    ui.horizontal(|ui| {
        mode_button(ui, &mut mode, BookMode::Both, theme::BRAND);
        mode_button(ui, &mut mode, BookMode::Bids, theme::BUY);
        mode_button(ui, &mut mode, BookMode::Asks, theme::SELL);
        ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
            let precision = inferred_price_step(asks, bids)
                .map(|step| model.format_market_price(symbol, step))
                .unwrap_or_else(|| "—".to_owned());
            ui.label(
                RichText::new(format!("{precision} ▾"))
                    .family(egui::FontFamily::Proportional)
                    .size(11.0),
            )
            .on_hover_text(text(language, TextKey::PricePrecision));
        });
    });
    let (base, quote) = symbol.split_once('/').unwrap_or((symbol, ""));
    book_header(ui, language, base, quote);
    let limit = if mode == BookMode::Both {
        BOOK_ROWS_PER_SIDE
    } else {
        SINGLE_SIDE_ROWS
    };
    let ask_rows = cumulative_rows(asks, limit);
    let bid_rows = cumulative_rows(bids, limit);
    let (ask_marks, bid_marks) = own_order_marks(model, symbol, asks, bids, limit);
    let max_total = ask_rows
        .last()
        .map(|(_, total)| *total)
        .unwrap_or(Decimal::ZERO)
        .max(
            bid_rows
                .last()
                .map(|(_, total)| *total)
                .unwrap_or(Decimal::ZERO),
        );
    if mode.shows_asks() {
        for (index, (level, cumulative)) in ask_rows.iter().enumerate().rev() {
            if book_row(
                ui,
                level,
                *cumulative,
                max_total,
                theme::SELL,
                model,
                symbol,
                ask_marks[index],
            ) {
                selected = Some(level.price);
            }
        }
    }
    price_mid_row(ui, trades, last, bid, ask, model, symbol);
    if mode.shows_bids() {
        for (index, (level, cumulative)) in bid_rows.iter().enumerate() {
            if book_row(
                ui,
                level,
                *cumulative,
                max_total,
                theme::BUY,
                model,
                symbol,
                bid_marks[index],
            ) {
                selected = Some(level.price);
            }
        }
    }
    ui.add_space(4.0);
    ui.separator();
    section_title(ui, text(language, TextKey::RecentTrades));
    trade_header(ui, language, base, quote);
    trade_rows(ui, instance, trades, language, model, symbol);
    ui.data_mut(|data| data.insert_temp(mode_id, mode));
    selected
}

fn section_title(ui: &mut egui::Ui, title: &str) {
    ui.horizontal(|ui| {
        ui.label(RichText::new(title).strong().size(14.0));
        ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
            ui.colored_label(theme::TEXT_SECONDARY, "•••");
        });
    });
    ui.separator();
}

fn mode_button(ui: &mut egui::Ui, mode: &mut BookMode, candidate: BookMode, color: Color32) {
    let response = ui.add(
        egui::Button::new("")
            .min_size(egui::vec2(28.0, 24.0))
            .selected(*mode == candidate),
    );
    let label = match candidate {
        BookMode::Both => "买卖盘口 / Both sides",
        BookMode::Bids => "买盘 / Bids",
        BookMode::Asks => "卖盘 / Asks",
    };
    let response = response.on_hover_text(label);
    response.widget_info(|| {
        egui::WidgetInfo::selected(
            egui::WidgetType::Button,
            ui.is_enabled(),
            *mode == candidate,
            label,
        )
    });
    let origin = response.rect.center() - egui::vec2(7.0, 6.0);
    for row in 0..3 {
        let tint = if candidate == BookMode::Both {
            if row == 0 { theme::SELL } else { theme::BUY }
        } else {
            color
        };
        let y = origin.y + row as f32 * 5.0;
        for (offset, width) in [(0.0, 3.0), (5.0, 9.0)] {
            ui.painter().rect_filled(
                Rect::from_min_size(Pos2::new(origin.x + offset, y), egui::vec2(width, 3.0)),
                0.75,
                tint,
            );
        }
    }
    if response.clicked() {
        *mode = candidate;
    }
}

fn book_header(ui: &mut egui::Ui, language: Language, base: &str, quote: &str) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(ui.available_width(), 20.0), Sense::hover());
    let painter = ui.painter();
    let font = FontId::proportional(11.0);
    painter.text(
        rect.left_center(),
        Align2::LEFT_CENTER,
        format!("{} ({quote})", text(language, TextKey::Price)),
        font.clone(),
        theme::TEXT_SECONDARY,
    );
    painter.text(
        Pos2::new(rect.left() + rect.width() * 0.68, rect.center().y),
        Align2::RIGHT_CENTER,
        format!("{} ({base})", text(language, TextKey::Quantity)),
        font.clone(),
        theme::TEXT_SECONDARY,
    );
    painter.text(
        rect.right_center(),
        Align2::RIGHT_CENTER,
        format!("{} ({base})", text(language, TextKey::Total)),
        font,
        theme::TEXT_SECONDARY,
    );
}

fn cumulative_rows(levels: &[UiBookLevel], limit: usize) -> Vec<(&UiBookLevel, Decimal)> {
    let mut cumulative = Decimal::ZERO;
    levels
        .iter()
        .take(limit)
        .map(|level| {
            cumulative += level.quantity;
            (level, cumulative)
        })
        .collect()
}

fn own_order_marks(
    model: &AppModel,
    symbol: &str,
    asks: &[UiBookLevel],
    bids: &[UiBookLevel],
    limit: usize,
) -> ([bool; SINGLE_SIDE_ROWS], [bool; SINGLE_SIDE_ROWS]) {
    let mut marks = ([false; SINGLE_SIDE_ROWS], [false; SINGLE_SIDE_ROWS]);
    let Some(credential) = model.selected_execution_credential() else {
        return marks;
    };
    let Some(projection) = model
        .execution
        .private_projection_for(model.preferences.execution_account_id.as_deref())
        .filter(|projection| {
            credential.venue == model.preferences.market_server.venue()
                && credential.credential_id == projection.credential_id
                && credential.trading_account_id.as_ref() == Some(&projection.trading_account_id)
        })
    else {
        return marks;
    };
    let Some((base, quote)) = symbol.split_once('/') else {
        return marks;
    };
    // Scan the existing projection once; only visible price levels need storage.
    for order in &projection.open_orders {
        if order.symbol.base() != base
            || order.symbol.quote() != quote
            || order.quantity <= Decimal::ZERO
            || order
                .filled_quantity
                .is_some_and(|filled| filled >= order.quantity)
        {
            continue;
        }
        let Some(price) = order.limit_price.filter(|price| *price > Decimal::ZERO) else {
            continue;
        };
        let (levels, side_marks) = match order.order_side {
            venue_domain::OrderSide::Buy => (bids, &mut marks.1),
            venue_domain::OrderSide::Sell => (asks, &mut marks.0),
        };
        if let Some(index) = levels
            .iter()
            .take(limit.min(SINGLE_SIDE_ROWS))
            .position(|level| level.price == price)
        {
            side_marks[index] = true;
        }
    }
    marks
}

#[allow(clippy::too_many_arguments)]
fn book_row(
    ui: &mut egui::Ui,
    level: &UiBookLevel,
    cumulative: Decimal,
    max_total: Decimal,
    color: Color32,
    model: &AppModel,
    symbol: &str,
    own_order: bool,
) -> bool {
    let (rect, response) = ui.allocate_exact_size(
        egui::vec2(ui.available_width(), BOOK_ROW_HEIGHT),
        Sense::click(),
    );
    let painter = ui.painter();
    let depth_ratio = if max_total.is_zero() {
        0.0
    } else {
        decimal_to_f64(cumulative / max_total).clamp(0.0, 1.0) as f32
    };
    painter.rect_filled(
        Rect::from_min_max(
            Pos2::new(rect.right() - rect.width() * depth_ratio, rect.top()),
            rect.right_bottom(),
        ),
        0.0,
        Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), 24),
    );
    if response.hovered() {
        painter.rect_filled(rect, 0.0, Color32::from_white_alpha(10));
    }
    if model.trade_dock.selected_price == Some(level.price) {
        painter.rect_stroke(
            rect,
            0.0,
            egui::Stroke::new(1.0, theme::WARNING),
            egui::StrokeKind::Inside,
        );
    }
    let font = FontId::proportional(12.0);
    if own_order {
        painter.circle_filled(
            Pos2::new(rect.left() + 3.0, rect.center().y),
            1.5,
            theme::WARNING,
        );
    }
    painter.text(
        Pos2::new(rect.left() + 9.0, rect.center().y),
        Align2::LEFT_CENTER,
        model.format_market_price(symbol, level.price),
        font.clone(),
        color,
    );
    painter.text(
        Pos2::new(rect.left() + rect.width() * 0.68, rect.center().y),
        Align2::RIGHT_CENTER,
        compact_quantity(
            level.quantity,
            model.format_market_quantity(symbol, level.quantity),
        ),
        font.clone(),
        theme::TEXT_PRIMARY,
    );
    painter.text(
        rect.right_center(),
        Align2::RIGHT_CENTER,
        compact_quantity(cumulative, model.format_market_quantity(symbol, cumulative)),
        font,
        theme::TEXT_PRIMARY,
    );
    response.clicked()
}

fn price_mid_row(
    ui: &mut egui::Ui,
    trades: &[UiTrade],
    last: Option<Decimal>,
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    model: &AppModel,
    symbol: &str,
) {
    let midpoint = bid.zip(ask).map(|(bid, ask)| (bid + ask) / Decimal::TWO);
    let latest = trades.last().map(|trade| trade.price).or(last).or(midpoint);
    let rising = trade_direction(trades, latest, midpoint);
    let color = if rising { theme::BUY } else { theme::SELL };
    let arrow = if rising { "↑" } else { "↓" };
    let (rect, _) = ui.allocate_exact_size(egui::vec2(ui.available_width(), 38.0), Sense::hover());
    if let Some(price) = latest {
        ui.painter().text(
            rect.left_center(),
            Align2::LEFT_CENTER,
            format!("{} {arrow}", model.format_market_price(symbol, price)),
            FontId::proportional(20.0),
            color,
        );
    }
    if let Some(midpoint) = midpoint {
        ui.painter().text(
            Pos2::new(rect.left() + rect.width() * 0.46, rect.center().y),
            Align2::LEFT_CENTER,
            model.format_market_price(symbol, midpoint),
            FontId::proportional(11.0),
            theme::TEXT_SECONDARY,
        );
    }
}

fn trade_direction(trades: &[UiTrade], latest: Option<Decimal>, midpoint: Option<Decimal>) -> bool {
    let Some(current) = trades.last() else {
        return latest
            .zip(midpoint)
            .is_none_or(|(latest, mid)| latest >= mid);
    };
    if let Some(previous) = trades.iter().rev().nth(1)
        && current.price != previous.price
    {
        return current.price > previous.price;
    }
    !matches!(current.aggressor, AggressorSide::Sell)
}

fn trade_header(ui: &mut egui::Ui, language: Language, base: &str, quote: &str) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(ui.available_width(), 20.0), Sense::hover());
    let font = FontId::proportional(11.0);
    ui.painter().text(
        rect.left_center(),
        Align2::LEFT_CENTER,
        format!("{} ({quote})", text(language, TextKey::Price)),
        font.clone(),
        theme::TEXT_SECONDARY,
    );
    ui.painter().text(
        Pos2::new(rect.left() + rect.width() * 0.70, rect.center().y),
        Align2::RIGHT_CENTER,
        format!("{} ({base})", text(language, TextKey::Quantity)),
        font.clone(),
        theme::TEXT_SECONDARY,
    );
    ui.painter().text(
        rect.right_center(),
        Align2::RIGHT_CENTER,
        text(language, TextKey::Time),
        font,
        theme::TEXT_SECONDARY,
    );
}

fn trade_rows(
    ui: &mut egui::Ui,
    instance: u32,
    trades: &[UiTrade],
    language: Language,
    model: &AppModel,
    symbol: &str,
) {
    if trades.is_empty() {
        ui.centered_and_justified(|ui| {
            ui.colored_label(theme::TEXT_SECONDARY, text(language, TextKey::NoTrades));
        });
        return;
    }
    egui::ScrollArea::vertical()
        .id_salt(("combined-trades", instance))
        .auto_shrink([false, false])
        .show(ui, |ui| {
            for trade in trades.iter().rev().take(80) {
                trade_row(ui, trade, model, symbol);
            }
        });
}

fn trade_row(ui: &mut egui::Ui, trade: &UiTrade, model: &AppModel, symbol: &str) {
    let color = match trade.aggressor {
        AggressorSide::Buy => theme::BUY,
        AggressorSide::Sell => theme::SELL,
        AggressorSide::Unknown => theme::TEXT_SECONDARY,
    };
    let (rect, response) = ui.allocate_exact_size(
        egui::vec2(ui.available_width(), TRADE_ROW_HEIGHT),
        Sense::hover(),
    );
    if response.hovered() {
        ui.painter()
            .rect_filled(rect, 0.0, Color32::from_white_alpha(10));
    }
    let font = FontId::proportional(11.0);
    ui.painter().text(
        rect.left_center(),
        Align2::LEFT_CENTER,
        model.format_market_price(symbol, trade.price),
        font.clone(),
        color,
    );
    ui.painter().text(
        Pos2::new(rect.left() + rect.width() * 0.70, rect.center().y),
        Align2::RIGHT_CENTER,
        compact_quantity(
            trade.quantity,
            model.format_market_quantity(symbol, trade.quantity),
        ),
        font.clone(),
        theme::TEXT_PRIMARY,
    );
    ui.painter().text(
        rect.right_center(),
        Align2::RIGHT_CENTER,
        clock_time(trade.occurred_ms),
        font,
        theme::TEXT_SECONDARY,
    );
}

fn inferred_price_step(asks: &[UiBookLevel], bids: &[UiBookLevel]) -> Option<Decimal> {
    asks.windows(2)
        .chain(bids.windows(2))
        .filter_map(|pair| positive_distance(pair[0].price, pair[1].price))
        .chain(
            asks.first()
                .zip(bids.first())
                .and_then(|(ask, bid)| positive_distance(ask.price, bid.price)),
        )
        .min()
}

fn positive_distance(left: Decimal, right: Decimal) -> Option<Decimal> {
    let distance = if left >= right {
        left - right
    } else {
        right - left
    };
    (!distance.is_zero()).then_some(distance)
}

fn compact_quantity(value: Decimal, fallback: String) -> String {
    for (threshold, suffix) in [
        (1_000_000_000_i64, "B"),
        (1_000_000_i64, "M"),
        (1_000_i64, "K"),
    ] {
        let divisor = Decimal::from(threshold);
        if value.abs() >= divisor {
            return format!("{}{}", (value / divisor).round_dp(2).normalize(), suffix);
        }
    }
    fallback
}

fn clock_time(timestamp_ms: u64) -> String {
    let seconds = timestamp_ms / 1_000 % 86_400;
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3_600,
        seconds / 60 % 60,
        seconds % 60
    )
}

#[cfg(test)]
mod tests {
    use super::{clock_time, compact_quantity, cumulative_rows};
    use rust_decimal::Decimal;
    use venue_control_protocol::UiBookLevel;

    #[test]
    fn depth_rows_accumulate_from_the_best_price_outward() {
        let levels = [
            UiBookLevel {
                price: Decimal::new(100, 0),
                quantity: Decimal::new(2, 0),
            },
            UiBookLevel {
                price: Decimal::new(99, 0),
                quantity: Decimal::new(3, 0),
            },
        ];
        let rows = cumulative_rows(&levels, 10);
        assert_eq!(rows[0].1, Decimal::new(2, 0));
        assert_eq!(rows[1].1, Decimal::new(5, 0));
    }

    #[test]
    fn large_book_quantities_use_compact_exchange_style_units() {
        assert_eq!(
            compact_quantity(Decimal::new(117_580, 0), String::new()),
            "117.58K"
        );
        assert_eq!(clock_time(82_247_000), "22:50:47");
    }
}
