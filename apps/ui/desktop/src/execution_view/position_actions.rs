use super::*;
use rust_decimal::Decimal;
use venue_control_protocol::{
    kol::{ExecutorCommandState, TerminalPosition},
    terminal_position::{
        PositionAction, TERMINAL_POSITION_ACTION_SCHEMA, TerminalPositionActionRequest,
    },
};
use venue_domain::{PositionSide, Symbol};

#[derive(Clone, Debug)]
pub(super) struct PositionActionDraft {
    credential_id: String,
    trading_account_id: String,
    symbol: Symbol,
    side: PositionSide,
    quantity: Decimal,
    action: PositionAction,
}

#[derive(Debug, Default)]
pub(super) struct PositionActions {
    draft: Option<PositionActionDraft>,
    pending: Option<(String, PositionActionDraft)>,
}

impl PositionActions {
    pub(super) fn completed(&mut self, rows: &[ExecutorCommandSummary]) {
        if self.pending.as_ref().is_some_and(|(id, _)| {
            rows.iter().any(|row| {
                row.request_id.as_ref() == Some(id)
                    && matches!(
                        row.state,
                        ExecutorCommandState::Reconciled
                            | ExecutorCommandState::Rejected
                            | ExecutorCommandState::Cancelled
                    )
            })
        }) {
            self.pending = None;
        }
    }
    pub(super) fn submission_failed(&mut self, id: &str, definitive: bool) {
        if definitive
            && self
                .pending
                .as_ref()
                .is_some_and(|(pending, _)| pending == id)
        {
            self.pending = None;
        }
    }
}

pub(super) fn row_buttons(
    ui: &mut egui::Ui,
    model: &AppModel,
    projection: &TerminalAccountProjection,
    row: &TerminalPosition,
) -> Option<PositionActionDraft> {
    let language = model.preferences.language;
    let busy = model.execution.position_actions.pending.is_some();
    let enabled = !busy
        && matches!(row.position_side, PositionSide::Long | PositionSide::Short)
        && row.quantity > Decimal::ZERO;
    let mut action = None;
    ui.horizontal(|ui| {
        for (kind, key) in [
            (PositionAction::Close, Key::Close),
            (PositionAction::Reverse, Key::Reverse),
        ] {
            if ui
                .add_enabled(enabled, egui::Button::new(text(language, key)))
                .clicked()
            {
                action = Some(PositionActionDraft {
                    credential_id: projection.credential_id.clone(),
                    trading_account_id: projection.trading_account_id.clone(),
                    symbol: row.symbol.clone(),
                    side: row.position_side,
                    quantity: row.quantity,
                    action: kind,
                });
            }
        }
    });
    action
}

pub(super) fn show_confirmation(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    requested: Option<PositionActionDraft>,
) {
    if requested.is_some() {
        model.execution.position_actions.draft = requested;
    }
    let Some(draft) = model.execution.position_actions.draft.clone() else {
        return;
    };
    let matches_account = selected_account_matches(model, &draft);
    let language = model.preferences.language;
    let mut confirm = false;
    let mut cancel = false;
    egui::Window::new(text(
        language,
        if draft.action == PositionAction::Close {
            Key::CloseConfirm
        } else {
            Key::ReverseConfirm
        },
    ))
    .id(egui::Id::new("position-action-confirmation"))
    .collapsible(false)
    .resizable(false)
    .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
    .show(ui.ctx(), |ui| {
        ui.label(format!(
            "{} · {} · {}",
            draft.symbol,
            side_label(draft.side, language),
            draft.quantity.normalize()
        ));
        ui.label(text(
            language,
            if draft.action == PositionAction::Close {
                Key::CloseExplain
            } else {
                Key::ReverseExplain
            },
        ));
        ui.colored_label(theme::WARNING, text(language, Key::MarketWarning));
        ui.small(text(language, Key::StrategyWarning));
        if !matches_account {
            ui.colored_label(theme::WARNING, text(language, Key::AccountChanged));
        }
        ui.horizontal(|ui| {
            confirm = ui
                .add_enabled(
                    matches_account && model.execution.position_actions.pending.is_none(),
                    egui::Button::new(text(language, Key::Confirm)),
                )
                .clicked();
            cancel = ui.button(text(language, Key::Cancel)).clicked();
        });
    });
    if cancel || confirm {
        model.execution.position_actions.draft = None;
    }
    if confirm {
        submit(model, client, draft);
    }
}

fn selected_account_matches(model: &AppModel, draft: &PositionActionDraft) -> bool {
    model.preferences.execution_account_id.as_ref() == Some(&draft.trading_account_id)
        && model.account_overview.as_ref().is_some_and(|overview| {
            overview.selected_credential_id.as_ref() == Some(&draft.credential_id)
        })
}

fn request(draft: &PositionActionDraft, request_id: String) -> TerminalPositionActionRequest {
    TerminalPositionActionRequest {
        schema_version: TERMINAL_POSITION_ACTION_SCHEMA,
        request_id,
        credential_id: draft.credential_id.clone(),
        symbol: draft.symbol.clone(),
        position_side: draft.side,
        quantity: draft.quantity,
        action: draft.action,
        market_risk_confirmed: true,
    }
}

fn submit(model: &mut AppModel, client: &ControlClient, draft: PositionActionDraft) {
    let id = model.next_terminal_request_id();
    match client.send_position_action(request(&draft, id.clone())) {
        Ok(()) => {
            model.execution.position_actions.pending = Some((id.clone(), draft));
            model.execution.begin_terminal_submission(id);
        }
        Err(error) => {
            model.execution.terminal_submission_error = Some(format!("持仓操作未提交：{error}"));
        }
    }
}

pub(crate) fn submit_confirmed_close(
    model: &mut AppModel,
    client: &ControlClient,
    side: PositionSide,
) {
    if model.execution.position_actions.pending.is_some() {
        return;
    }
    let Some(projection) = model.execution.private_projection.clone() else {
        return;
    };
    let Some(row) = projection.positions.iter().find(|row| {
        row.symbol.to_string() == model.preferences.selected_symbol
            && row.position_side == side
            && row.quantity > Decimal::ZERO
    }) else {
        return;
    };
    let draft = PositionActionDraft {
        credential_id: projection.credential_id.clone(),
        trading_account_id: projection.trading_account_id.clone(),
        symbol: row.symbol.clone(),
        side,
        quantity: row.quantity,
        action: PositionAction::Close,
    };
    if selected_account_matches(model, &draft) {
        submit(model, client, draft);
    }
}

fn side_label(side: PositionSide, language: crate::i18n::Language) -> &'static str {
    match (side, language) {
        (PositionSide::Long, crate::i18n::Language::SimplifiedChinese) => "多仓",
        (PositionSide::Short, crate::i18n::Language::SimplifiedChinese) => "空仓",
        (PositionSide::Long, _) => "Long",
        (PositionSide::Short, _) => "Short",
        _ => "—",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn row_request_uses_clicked_symbol_side_and_quantity_not_chart()
    -> Result<(), Box<dyn std::error::Error>> {
        for side in [PositionSide::Long, PositionSide::Short] {
            for action in [PositionAction::Close, PositionAction::Reverse] {
                let draft = PositionActionDraft {
                    credential_id: "00000000-0000-4000-8000-000000000001".into(),
                    trading_account_id: "00000000-0000-4000-8000-000000000002".into(),
                    symbol: "SOL/USDC".parse()?,
                    side,
                    quantity: Decimal::new(276, 2),
                    action,
                };
                let request = request(&draft, "00000000-0000-4000-8000-000000000003".into());
                request.validate()?;
                assert_eq!(request.symbol, draft.symbol);
                assert_eq!(request.position_side, side);
                assert_eq!(request.quantity, Decimal::new(276, 2));
                assert_eq!(request.action, action);
            }
        }
        Ok(())
    }
}
