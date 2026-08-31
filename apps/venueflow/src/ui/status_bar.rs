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
    let current = fresh(snapshot, account, super::now_ms());
    egui::Frame::new()
        .fill(theme::BG_SECONDARY)
        .inner_margin(egui::Margin::symmetric(10, 4))
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.spacing_mut().item_spacing = egui::vec2(8.0, 4.0);
            ui.spacing_mut().interact_size.y = 18.0;
            ui.horizontal(|ui| {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    use crate::market::MarketStatus;
                    let market = model
                        .local_markets
                        .view_for_symbol(&model.preferences.selected_symbol);
                    let status = market.map(|market| market.status);
                    let live = status == Some(MarketStatus::Live);
                    let key = match status {
                        Some(MarketStatus::Live) => TextKey::Online,
                        Some(MarketStatus::LoadingHistory) => TextKey::LoadingHistory,
                        Some(MarketStatus::Connecting) => TextKey::Connecting,
                        Some(MarketStatus::Resyncing) => TextKey::Resyncing,
                        Some(MarketStatus::Stale) => TextKey::Stale,
                        _ => TextKey::Offline,
                    };
                    let delay = market
                        .filter(|_| live)
                        .and_then(|market| market.latency_ms)
                        .map_or_else(|| "—".to_owned(), |ms| format!("{ms} ms"));
                    ui.label(
                        RichText::new(format!(
                            "● Binance {} · {} {delay}",
                            text(language, key),
                            text(language, TextKey::MarketDelay)
                        ))
                        .size(12.0)
                        .color(if live {
                            theme::BUY
                        } else {
                            theme::WARNING
                        }),
                    )
                    .on_hover_text(text(language, TextKey::MarketDelayHint));
                }
                #[cfg(target_arch = "wasm32")]
                ui.small(text(language, TextKey::WebControlOnly));
                ui.separator();
                ui.small("Control")
                    .on_hover_text(super::endpoint_label(&model.preferences.endpoint));
                super::connection_badge(ui, model.connection, language);
                ui.separator();
                let node_key = if model.preferences.execution_account_id.is_none() {
                    TextKey::NoExecutionAccount
                } else if account.is_none() {
                    TextKey::AwaitingNode
                } else if !current
                    || !model.snapshot_online
                    || model.control_connection != Some(ConnectionState::Live)
                {
                    TextKey::Stale
                } else if account
                    .is_some_and(|a| a.private_generation == 0 || a.writer_generation == 0)
                {
                    TextKey::Pending
                } else {
                    match account.map(|account| account.health) {
                        Some(HealthState::Healthy) => TextKey::Healthy,
                        Some(HealthState::Recovering) => TextKey::Recovering,
                        Some(HealthState::NeedsAttention) => TextKey::NeedsAttention,
                        Some(HealthState::Stopped) => TextKey::Stopped,
                        _ => TextKey::Unknown,
                    }
                };
                ui.label(
                    RichText::new(format!(
                        "{}: {}",
                        text(language, TextKey::TradeConnection),
                        text(language, node_key)
                    ))
                    .size(12.0)
                    .color(if node_key == TextKey::Healthy {
                        theme::BUY
                    } else {
                        theme::WARNING
                    }),
                )
                .on_hover_text(text(language, TextKey::NodeStatusHint));
                if let Some(error) = &model.last_error {
                    ui.colored_label(theme::SELL, "ⓘ").on_hover_text(error);
                }
            });
            ui.horizontal(|ui| {
                let account_label = model
                    .preferences
                    .execution_account_id
                    .as_deref()
                    .map(|id| id.chars().take(24).collect::<String>())
                    .unwrap_or_else(|| text(language, TextKey::None).to_owned());
                ui.small(format!(
                    "{}: {account_label}",
                    text(language, TextKey::Account)
                ));
                ui.separator();
                for (key, value) in [
                    (TextKey::Equity, account.map(|a| a.equity)),
                    (
                        TextKey::AvailableMargin,
                        account.map(|a| a.available_margin),
                    ),
                ] {
                    let amount =
                        value.map_or_else(|| "—".to_owned(), |value| value.normalize().to_string());
                    ui.label(
                        RichText::new(format!("{} {amount}", text(language, key)))
                            .size(12.0)
                            .color(if current && model.snapshot_online {
                                theme::TEXT_PRIMARY
                            } else {
                                theme::TEXT_SECONDARY
                            }),
                    )
                    .on_hover_text(text(language, TextKey::FundsHint));
                }
                if account.is_some() && (!current || !model.snapshot_online) {
                    ui.colored_label(theme::WARNING, text(language, TextKey::Stale));
                }
                if account.is_some() {
                    ui.small(text(language, TextKey::ValuationCurrencyMissing))
                        .on_hover_text(text(language, TextKey::FundsHint));
                }
                if let Some(receipt) = model.last_terminal_receipt() {
                    ui.small(format!(
                        "{}: {:?}",
                        text(language, TextKey::Receipt),
                        receipt.state
                    ))
                    .on_hover_text(&receipt.receipt_id);
                }
            });
        });
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
                equity: rust_decimal::Decimal::new(123456, 2),
                available_margin: rust_decimal::Decimal::new(3456, 2),
                unrealized_pnl: rust_decimal::Decimal::ZERO,
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
            selected_account(Some(&snapshot), Some("binance-account-1")).map(|a| a.equity),
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
