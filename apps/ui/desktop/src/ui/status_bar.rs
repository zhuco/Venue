use eframe::egui::{self, RichText};
use venue_control_protocol::{ConnectionState, kol::TerminalAsset};

use crate::{
    i18n::{TextKey, text},
    model::AppModel,
    theme,
};

fn selected_account_asset<'a>(
    assets: &'a [TerminalAsset],
    selected_symbol: &str,
) -> Option<&'a TerminalAsset> {
    let quote = selected_symbol.split_once('/')?.1;
    assets
        .iter()
        .find(|asset| asset.asset == quote)
        .or_else(|| assets.iter().find(|asset| asset.asset == "USD"))
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
    let node_status = if account_id.is_some() && model.execution.private_error.is_some() {
        match language {
            crate::i18n::Language::SimplifiedChinese => "账户刷新失败",
            crate::i18n::Language::English => "Account refresh failed",
        }
    } else if private_projection
        .is_some_and(|projection| projection.observed_ms > now.saturating_add(2_000))
    {
        match language {
            crate::i18n::Language::SimplifiedChinese => "本机与服务器时钟不一致",
            crate::i18n::Language::English => "Client/server clock mismatch",
        }
    } else {
        text(language, node_key)
    };
    let mut node_hint = match language {
        crate::i18n::Language::SimplifiedChinese => {
            "账户状态与资金依据选中交易所的签名私有投影；账户切换后重新读取。"
                .to_owned()
        }
        crate::i18n::Language::English => {
            "Account status and funds use signed private facts from the selected exchange. Switching accounts refreshes those facts."
                .to_owned()
        }
    };
    if let Some(error) = &model.last_error {
        node_hint.push_str(&format!("\n{error}"));
    }
    if let Some(error) = &model.execution.private_error {
        node_hint.push_str(&format!("\n{error}"));
    }
    if let Some(projection) = private_projection {
        node_hint.push_str(&match language {
            crate::i18n::Language::SimplifiedChinese => format!(
                "\n服务器账户事实距今 {:.1} 秒；桌面上次接收距今 {:.1} 秒。超过 15 秒视为过期。",
                now.saturating_sub(projection.observed_ms) as f64 / 1000.0,
                now.saturating_sub(model.execution.private_received_ms()) as f64 / 1000.0,
            ),
            crate::i18n::Language::English => format!(
                "\nServer facts age: {:.1}s; last desktop receipt: {:.1}s ago. Stale after 15s.",
                now.saturating_sub(projection.observed_ms) as f64 / 1000.0,
                now.saturating_sub(model.execution.private_received_ms()) as f64 / 1000.0,
            ),
        });
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
    let account_label = model
        .selected_execution_credential()
        .map_or("—", |credential| credential.label.as_str());
    let asset = private_projection.and_then(|projection| {
        selected_account_asset(&projection.assets, &model.preferences.selected_symbol)
    });
    let funds_hint = format!(
        "{}: {account_id}\n{}",
        text(language, TextKey::Account),
        match language {
            crate::i18n::Language::SimplifiedChinese => {
                "资金来自同一账户最近的签名资产快照；USD 为统一账户计价，不转换为当前交易对报价币。资产不随仓位事件实时刷新。"
            }
            crate::i18n::Language::English => {
                "Latest signed asset snapshot for the same account. USD is portfolio valuation, not converted to the symbol quote. Position events do not refresh balances."
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
            ui.horizontal(|ui| {
                let widths = footer_widths(total_width);
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
                        widths[0],
                        format!(
                            "● {} {} · {delay}",
                            model.preferences.market_server.label(),
                            text(language, status)
                        ),
                        if live { theme::BUY } else { theme::WARNING },
                        &market
                            .and_then(|view| view.status_detail.as_deref())
                            .map_or_else(
                                || text(language, TextKey::MarketDelayHint).to_owned(),
                                |detail| {
                                    format!(
                                        "{}\n{detail}",
                                        text(language, TextKey::MarketDelayHint)
                                    )
                                },
                            ),
                    );
                }
                #[cfg(target_arch = "wasm32")]
                status_text(
                    ui,
                    widths[0],
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
                    (false, true, _) => (
                        match language {
                            crate::i18n::Language::SimplifiedChinese => {
                                "Control 快照重连 · SSE 在线".to_owned()
                            }
                            crate::i18n::Language::English => {
                                "Control snapshot retry · SSE online".to_owned()
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
                if let Some(error) = &model.last_error {
                    control_hint.push_str(&format!("\n{error}"));
                }
                status_text(ui, widths[1], control_label, color, &control_hint);
                let feedback = crate::terminal_feedback::message(model);
                status_text(
                    ui,
                    widths[2],
                    feedback.clone().unwrap_or_else(|| {
                        format!(
                            "{}: {}",
                            text(language, TextKey::TradeConnection),
                            node_status
                        )
                    }),
                    if feedback.is_none() && node_key == TextKey::Healthy {
                        theme::BUY
                    } else {
                        theme::WARNING
                    },
                    &feedback.map_or(node_hint.clone(), |message| {
                        format!("{message}\n{node_hint}")
                    }),
                );
                status_text(
                    ui,
                    widths[3],
                    format!(
                        "{}: {account_label}",
                        text(language, TextKey::ExecutionAccount)
                    ),
                    theme::TEXT_SECONDARY,
                    &funds_hint,
                );
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
                        widths[4],
                        full.clone(),
                        color,
                        &format!("{full}\n{funds_hint}"),
                    );
                }
            });
        });
}

fn footer_widths(width: f32) -> [f32; 6] {
    let usable = (width - 5.0 * 8.0).max(0.0);
    // Reserve predictable columns for connection/account/funds; messages absorb extra space.
    let scale = (usable / 960.0).min(1.0);
    let fixed = [160.0, 140.0, 0.0, 140.0, 170.0, 170.0];
    let mut widths = fixed.map(|value| value * scale);
    widths[2] = (usable - widths.iter().sum::<f32>()).max(0.0);
    widths
}

fn status_text(ui: &mut egui::Ui, width: f32, value: String, color: egui::Color32, tooltip: &str) {
    let response = ui
        .add_sized(
            [width.max(20.0), 18.0],
            egui::Label::new(RichText::new(value).size(11.0).color(color))
                .truncate()
                .halign(egui::Align::Min),
        )
        .on_hover_text(tooltip);
    let x = response.rect.right() + 4.0;
    if x < ui.clip_rect().right() - 8.0 {
        ui.painter().line_segment(
            [
                egui::pos2(x, response.rect.top() + 3.0),
                egui::pos2(x, response.rect.bottom() - 3.0),
            ],
            egui::Stroke::new(1.0, theme::TEXT_SECONDARY.gamma_multiply(0.2)),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_error_is_in_footer_and_absent_from_order_controls() {
        fn texts(shape: &egui::Shape, values: &mut String) {
            match shape {
                egui::Shape::Text(text) => values.push_str(&text.galley.job.text),
                egui::Shape::Vec(shapes) => shapes.iter().for_each(|shape| texts(shape, values)),
                _ => (),
            }
        }
        let mut model = AppModel::new(Default::default());
        model.execution.terminal_submission_error =
            Some("[binance_-1111] precision rejected".into());
        let context = egui::Context::default();
        for footer in [false, true] {
            let mut output = context.run_ui(
                egui::RawInput {
                    screen_rect: Some(egui::Rect::from_min_size(
                        egui::Pos2::ZERO,
                        egui::vec2(1280.0, 700.0),
                    )),
                    ..Default::default()
                },
                |ui| {
                    if footer {
                        show(ui, &model);
                    } else {
                        crate::trade_dock::controls(ui, &mut model);
                    }
                },
            );
            let mut rendered = String::new();
            for shape in &output.shapes {
                texts(&shape.shape, &mut rendered);
            }
            assert_eq!(rendered.contains("binance_-1111"), footer);
            output.textures_delta.clear();
        }
    }

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
            selected_account_asset(&assets, "SOL/USDC").map(|asset| asset.equity),
            Some(rust_decimal::Decimal::new(2, 0))
        );
        assert!(selected_account_asset(&assets, "SOL/FDUSD").is_none());
        assert!(selected_account_asset(&assets, "INVALID").is_none());
    }

    #[test]
    fn portfolio_usd_valuation_is_visible_without_relabeling_it_as_usdc() {
        let assets = vec![TerminalAsset {
            asset: "USD".into(),
            equity: rust_decimal::Decimal::new(15397, 2),
            available_margin: Some(rust_decimal::Decimal::new(14499, 2)),
        }];
        for symbol in ["SOL/USDC", "BTC/USDT"] {
            let selected = selected_account_asset(&assets, symbol);
            assert_eq!(selected.map(|asset| asset.asset.as_str()), Some("USD"));
            assert_eq!(selected.map(|asset| asset.equity), Some(assets[0].equity));
            assert_eq!(
                selected.and_then(|asset| asset.available_margin),
                assets[0].available_margin
            );
        }
    }
}
