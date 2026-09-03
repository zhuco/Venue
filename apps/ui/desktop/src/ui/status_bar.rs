use eframe::egui::{self, RichText};
use venue_control_protocol::{AccountSummary, ConnectionState, ControlSnapshot, HealthState};

use crate::{
    i18n::{TextKey, text},
    model::{AppModel, freshness_age_ms},
    theme,
};

const ACCOUNT_FRESH_MS: u64 = 15_000;

fn selected_account<'a>(
    snapshot: Option<&'a ControlSnapshot>,
    id: Option<&str>,
) -> Option<&'a AccountSummary> {
    let id = id?;
    snapshot?.accounts.iter().find(|account| {
        account.trading_account_id == id
            && account.venue == venue_control_protocol::VenueId::Binance
    })
}

fn fresh(snapshot: Option<&ControlSnapshot>, account: Option<&AccountSummary>, now: u64) -> bool {
    let (Some(snapshot), Some(account)) = (snapshot, account) else {
        return false;
    };
    [snapshot.generated_ms, account.last_reconciled_ms]
        .into_iter()
        .all(|observed| freshness_age_ms(now, observed).is_some_and(|age| age <= ACCOUNT_FRESH_MS))
}

pub(super) fn show(ui: &mut egui::Ui, model: &AppModel) {
    let language = model.preferences.language;
    let snapshot = model.snapshot.as_ref();
    let account = selected_account(snapshot, model.preferences.execution_account_id.as_deref());
    let now = super::now_ms();
    let account_current = fresh(snapshot, account, now) && model.snapshot_online;
    let private_projection = model
        .execution
        .private_projection
        .as_ref()
        .filter(|projection| {
            model.preferences.execution_account_id.as_deref()
                == Some(projection.trading_account_id.as_str())
        });
    let private_current = private_projection.is_some() && model.execution.private_fresh(now);
    let node_key = if model.preferences.execution_account_id.is_none() {
        TextKey::NoAccountShort
    } else if private_projection.is_some() {
        if private_current {
            TextKey::Healthy
        } else {
            TextKey::Stale
        }
    } else if account.is_none() {
        TextKey::AwaitingNode
    } else if !account_current || model.control_connection != Some(ConnectionState::Live) {
        TextKey::Stale
    } else if account.is_some_and(|a| a.private_generation == 0 || a.writer_generation == 0) {
        TextKey::Pending
    } else {
        match account.map(|a| a.health) {
            Some(HealthState::Healthy) => TextKey::Healthy,
            Some(HealthState::Recovering) => TextKey::Recovering,
            Some(HealthState::NeedsAttention) => TextKey::NeedsAttention,
            Some(HealthState::Stopped) => TextKey::Stopped,
            _ => TextKey::Unknown,
        }
    };
    let mut node_hint = text(language, TextKey::NodeStatusHint).to_owned();
    if private_projection.is_some() {
        node_hint = match language {
            crate::i18n::Language::SimplifiedChinese => {
                "交易连接依据 Binance Executor 的签名私有账户投影；旧交易节点快照不再覆盖此状态。"
                    .to_owned()
            }
            crate::i18n::Language::English => {
                "Trading connectivity follows the Binance Executor signed private projection; a legacy node snapshot cannot override it."
                    .to_owned()
            }
        };
    }
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
    let account_id = model
        .preferences
        .execution_account_id
        .as_deref()
        .unwrap_or("—");
    let funds_hint = format!(
        "{}: {account_id}\n{}",
        text(language, TextKey::Account),
        text(language, TextKey::FundsHint)
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
                        let color = if account_current {
                            theme::TEXT_PRIMARY
                        } else {
                            theme::TEXT_SECONDARY
                        };
                        if account.is_some() && !account_current {
                            status_text(
                                ui,
                                52.0,
                                text(language, TextKey::Stale).to_owned(),
                                theme::WARNING,
                                &funds_hint,
                            );
                        }
                        // Numeric fields are never rounded to fit; ellipsis exposes the exact value on hover.
                        for (key, value) in [
                            (TextKey::Equity, account.and_then(|a| a.equity)),
                            (
                                TextKey::MarginShort,
                                account.and_then(|a| a.available_margin),
                            ),
                        ] {
                            let amount =
                                value.map_or_else(|| "—".to_owned(), |v| v.normalize().to_string());
                            let marker = if account.is_some() { "*" } else { "" };
                            let full = format!("{} {amount}{marker}", text(language, key));
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

    fn snapshot() -> ControlSnapshot {
        ControlSnapshot {
            schema_version: venue_control_protocol::CONTROL_SCHEMA_VERSION,
            generated_ms: 50_000,
            connection: ConnectionState::Live,
            accounts: vec![AccountSummary {
                venue: venue_control_protocol::VenueId::Binance,
                mode: venue_control_protocol::GatewayMode::Live,
                trading_account_id: "binance-account-1".into(),
                health: HealthState::Healthy,
                equity: Some(rust_decimal::Decimal::new(123456, 2)),
                available_margin: Some(rust_decimal::Decimal::new(3456, 2)),
                unrealized_pnl: Some(rust_decimal::Decimal::ZERO),
                balances: vec![],
                private_generation: 1,
                writer_generation: 1,
                last_reconciled_ms: 49_000,
            }],
            strategies: vec![],
            copy_relations: vec![],
            markets: vec![],
            ledger: vec![],
        }
    }

    #[test]
    fn funds_never_fall_back_to_another_account_or_venue() {
        let mut snapshot = snapshot();
        assert!(selected_account(Some(&snapshot), None).is_none());
        assert!(selected_account(Some(&snapshot), Some("other-account")).is_none());
        assert_eq!(
            selected_account(Some(&snapshot), Some("binance-account-1")).and_then(|a| a.equity),
            Some(rust_decimal::Decimal::new(123456, 2))
        );
        snapshot.accounts[0].venue = venue_control_protocol::VenueId::Okx;
        assert!(selected_account(Some(&snapshot), Some("binance-account-1")).is_none());
    }

    #[test]
    fn fresh_control_cannot_hide_stale_account_and_vice_versa() {
        let mut snapshot = snapshot();
        assert!(fresh(Some(&snapshot), snapshot.accounts.first(), 50_000));
        assert!(!fresh(Some(&snapshot), snapshot.accounts.first(), 65_000));
        snapshot.accounts[0].last_reconciled_ms = 0;
        assert!(!fresh(Some(&snapshot), snapshot.accounts.first(), 50_000));
        snapshot.accounts[0].last_reconciled_ms = 49_000;
        snapshot.generated_ms = 1;
        assert!(!fresh(Some(&snapshot), snapshot.accounts.first(), 50_000));
    }

    #[test]
    fn missing_or_future_observation_is_never_fresh() {
        assert!(!fresh(None, None, 50_000));
        assert_eq!(freshness_age_ms(50_000, 0), None);
        assert_eq!(freshness_age_ms(50_000, 50_001), None);
    }
}
