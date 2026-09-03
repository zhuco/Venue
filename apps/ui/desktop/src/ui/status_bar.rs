use eframe::egui::{self, RichText};
use venue_control_protocol::{ConnectionState, kol::TerminalAsset};

use crate::{
    i18n::{TextKey, text},
    model::AppModel,
    theme,
};

fn selected_quote_asset<'a>(
    assets: &'a [TerminalAsset],
    selected_symbol: &str,
) -> Option<&'a TerminalAsset> {
    let quote = selected_symbol.split_once('/')?.1;
    assets.iter().find(|asset| asset.asset == quote)
}

pub(super) fn show(ui: &mut egui::Ui, model: &AppModel) {
    let language = model.preferences.language;
    let account_id = model.preferences.execution_account_id.as_deref();
    let now = super::now_ms();
    let private_projection = model.execution.private_projection_for(account_id);
    let private_current = model.execution.private_ready(account_id, now);
    let node_key = if account_id.is_none() {
        TextKey::NoAccountShort
    } else if private_projection.is_none() {
        TextKey::AwaitingNode
    } else if private_current {
        TextKey::Healthy
    } else {
        TextKey::Stale
    };
    let mut node_hint = match language {
        crate::i18n::Language::SimplifiedChinese => {
            "交易连接与账户资金统一依据选中账户的 Binance Executor 签名私有投影。"
                .to_owned()
        }
        crate::i18n::Language::English => {
            "Trading connectivity and account funds share the selected account's signed Binance Executor projection."
                .to_owned()
        }
    };
    if let Some(error) = &model.last_error {
        node_hint.push_str(&format!("\n{error}"));
    }
    if let Some(receipt) = model.last_terminal_receipt() {
        node_hint.push_str(&format!(
            "\n{}: {:?} · {}",
            text(language, TextKey::Receipt),
            receipt.state,
            receipt.receipt_id
        ));
    }
    let account_id = account_id.unwrap_or("—");
    let asset = private_projection.and_then(|projection| {
        selected_quote_asset(&projection.assets, &model.preferences.selected_symbol)
    });
    let funds_hint = format!(
        "{}: {account_id}\n{}",
        text(language, TextKey::Account),
        match language {
            crate::i18n::Language::SimplifiedChinese => {
                "资金与交易连接使用同一份 Binance Executor 签名私有投影。"
            }
            crate::i18n::Language::English => {
                "Funds and trading connectivity use the same signed Binance Executor projection."
            }
        }
    );
    egui::Frame::new()
        .fill(theme::BG_SECONDARY)
        .inner_margin(egui::Margin::symmetric(10, 3))
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.spacing_mut().item_spacing = egui::vec2(8.0, 0.0);
            ui.spacing_mut().interact_size.y = 18.0;
            let total_width = ui.available_width();
            egui::containers::Sides::new()
                .shrink_left()
                .height(18.0)
                .show(
                    ui,
                    |ui| {
                        let width = ui.available_width();
                        #[cfg(not(target_arch = "wasm32"))]
                        {
                            use crate::market::MarketStatus;
                            let market = model
                                .local_markets
                                .view_for_symbol(&model.preferences.selected_symbol);
                            let live = market.is_some_and(|m| m.status == MarketStatus::Live);
                            let status = match market.map(|m| m.status) {
                                Some(MarketStatus::Live) => TextKey::Online,
                                Some(MarketStatus::LoadingHistory) => TextKey::LoadingHistory,
                                Some(MarketStatus::Connecting) => TextKey::Connecting,
                                Some(MarketStatus::Resyncing) => TextKey::Resyncing,
                                Some(MarketStatus::Stale) => TextKey::Stale,
                                _ => TextKey::Offline,
                            };
                            let delay = market
                                .filter(|_| live)
                                .and_then(|m| m.latency_ms)
                                .map_or_else(|| "—".to_owned(), |ms| format!("{ms} ms"));
                            status_text(
                                ui,
                                width * 0.40,
                                format!("● Binance {} · {delay}", text(language, status)),
                                if live { theme::BUY } else { theme::WARNING },
                                text(language, TextKey::MarketDelayHint),
                            );
                        }
                        #[cfg(target_arch = "wasm32")]
                        status_text(
                            ui,
                            width * 0.40,
                            text(language, TextKey::ControlFallback).to_owned(),
                            theme::TEXT_SECONDARY,
                            text(language, TextKey::WebControlOnly),
                        );
                        let (control_label, color) = match (
                            model.snapshot_online,
                            model.event_stream_online,
                            model.connection,
                        ) {
                            (true, true, _) => (
                                format!("Control {}", text(language, TextKey::Online)),
                                theme::BUY,
                            ),
                            (true, false, _) => (
                                match language {
                                    crate::i18n::Language::SimplifiedChinese => {
                                        "Control 在线 · SSE 重连".to_owned()
                                    }
                                    crate::i18n::Language::English => {
                                        "Control online · SSE reconnecting".to_owned()
                                    }
                                },
                                theme::WARNING,
                            ),
                            (false, _, ConnectionState::Connecting) => (
                                format!("Control {}", text(language, TextKey::Connecting)),
                                theme::WARNING,
                            ),
                            (false, _, ConnectionState::Offline) => (
                                format!("Control {}", text(language, TextKey::Offline)),
                                theme::SELL,
                            ),
                            _ => (
                                format!("Control {}", text(language, TextKey::Degraded)),
                                theme::WARNING,
                            ),
                        };
                        let mut control_hint =
                            super::endpoint_label(&model.preferences.endpoint).to_owned();
                        if model.snapshot_online && !model.event_stream_online {
                            control_hint.push_str(match language {
                                crate::i18n::Language::SimplifiedChinese => {
                                    "\n快照接口可用；SSE 事件流正在重连。"
                                }
                                crate::i18n::Language::English => {
                                    "\nSnapshot API is available; the SSE stream is reconnecting."
                                }
                            });
                        }
                        status_text(ui, width * 0.22, control_label, color, &control_hint);
                        status_text(
                            ui,
                            (width * 0.38 - 24.0).max(50.0),
                            format!(
                                "{}: {}",
                                text(language, TextKey::TradeConnection),
                                text(language, node_key)
                            ),
                            if node_key == TextKey::Healthy {
                                theme::BUY
                            } else {
                                theme::WARNING
                            },
                            &node_hint,
                        );
                    },
                    |ui| {
                        let color = if private_current {
                            theme::TEXT_PRIMARY
                        } else {
                            theme::TEXT_SECONDARY
                        };
                        // Numeric fields are never rounded to fit; ellipsis exposes the exact value on hover.
                        for (key, value) in [
                            (TextKey::Equity, asset.map(|a| a.equity)),
                            (TextKey::MarginShort, asset.and_then(|a| a.available_margin)),
                        ] {
                            let amount =
                                value.map_or_else(|| "—".to_owned(), |v| v.normalize().to_string());
                            let asset_label = asset.map_or("", |asset| asset.asset.as_str());
                            let full = format!("{} {amount} {asset_label}", text(language, key));
                            status_text(
                                ui,
                                if total_width < 1000.0 { 135.0 } else { 180.0 },
                                full.clone(),
                                color,
                                &format!("{full}\n{funds_hint}"),
                            );
                        }
                        if total_width >= 1150.0 {
                            status_text(
                                ui,
                                110.0,
                                format!("{}: {account_id}", text(language, TextKey::Account)),
                                theme::TEXT_SECONDARY,
                                &funds_hint,
                            );
                        }
                    },
                );
        });
}

fn status_text(ui: &mut egui::Ui, width: f32, value: String, color: egui::Color32, tooltip: &str) {
    ui.add_sized(
        [width.max(20.0), 18.0],
        egui::Label::new(RichText::new(value).size(11.0).color(color))
            .truncate()
            .halign(egui::Align::Min),
    )
    .on_hover_text(tooltip);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn funds_follow_the_selected_symbol_quote_asset() {
        let assets = vec![
            TerminalAsset {
                asset: "USDT".into(),
                equity: rust_decimal::Decimal::new(1, 0),
                available_margin: None,
            },
            TerminalAsset {
                asset: "USDC".into(),
                equity: rust_decimal::Decimal::new(2, 0),
                available_margin: Some(rust_decimal::Decimal::new(15, 1)),
            },
        ];
        assert_eq!(
            selected_quote_asset(&assets, "SOL/USDC").map(|asset| asset.equity),
            Some(rust_decimal::Decimal::new(2, 0))
        );
        assert!(selected_quote_asset(&assets, "SOL/FDUSD").is_none());
        assert!(selected_quote_asset(&assets, "INVALID").is_none());
    }
}
