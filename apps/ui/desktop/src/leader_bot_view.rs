use crate::{
    client::{ControlClient, GridMutation},
    model::AppModel,
};
use eframe::egui;
use venue_control_protocol::{accounts::CredentialSummary, leader_bot::*};

#[derive(Debug, Default)]
pub struct LeaderBotView {
    pub access: Option<LeaderBotAccess>,
    pub error: Option<String>,
    pub fresh: bool,
    pub pending: Option<GridMutation>,
    pub create_credential_id: Option<String>,
    confirmed: bool,
}

pub fn show(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
) {
    let state = &model.execution.leader_bot;
    let Some(access) = state.access.clone() else {
        if let Some(error) = &state.error {
            ui.group(|ui| {
                ui.strong("KOL 带单机器人 · 尚未就绪");
                ui.colored_label(crate::theme::WARNING, error);
            });
        }
        return;
    };
    if !access.can_use && access.bot.is_none() {
        return;
    }
    let mut action = None;
    let mut retry = false;
    ui.group(|ui| {
        ui.strong(if access.can_use {
            "带单机器人"
        } else {
            "带单实例 · 权限已撤销"
        });
        ui.label(
            "同步主账户启用后的限价委托；子账户独立成交，不追补仓位。停止只撤镜像挂单，不平仓。",
        );
        if let Some(error) = &model.execution.leader_bot.error {
            ui.colored_label(egui::Color32::YELLOW, error);
        }
        if model.execution.leader_bot.pending.is_some() {
            ui.label("操作尚未确认");
            retry = ui.button("重试原请求").clicked();
            return;
        }
        if let Some(bot) = &access.bot {
            ui.label(format!(
                "状态：{:?} · 跟随账户：{} · 未结束订单：{}",
                bot.state, bot.active_followers, bot.pending_orders
            ));
            if bot.credential_id != credential.credential_id {
                ui.label("此实例绑定其他账户；请切换至其绑定账户管理。");
                return;
            }
            if let Some(code) = &bot.attention_code {
                ui.label(format!("需处理：{code}"));
            }
            if bot.state == LeaderBotState::Stopped && access.can_use {
                ui.checkbox(
                    &mut model.execution.leader_bot.confirmed,
                    "确认开启带单，跟随者仍须自行授权启用",
                );
                if ui
                    .add_enabled(
                        model.execution.leader_bot.fresh && model.execution.leader_bot.confirmed,
                        egui::Button::new("启动带单"),
                    )
                    .clicked()
                {
                    action = Some(LeaderBotAction::Start);
                }
            } else if bot.state != LeaderBotState::Stopped && bot.state != LeaderBotState::Draining
            {
                if ui.button("停止同步并撤销镜像挂单").clicked() {
                    action = Some(LeaderBotAction::Stop);
                }
            }
        } else if ui
            .add_enabled(
                access.can_use && model.execution.leader_bot.fresh,
                egui::Button::new("使用当前KOL主账户创建带单机器人"),
            )
            .clicked()
        {
            model.execution.leader_bot.create_credential_id =
                Some(credential.credential_id.clone());
        }
    });
    if let Some(draft_credential) = model.execution.leader_bot.create_credential_id.clone() {
        let pending = model.execution.leader_bot.pending.is_some();
        let current = draft_credential == credential.credential_id;
        let response = egui::Modal::new(egui::Id::new("leader-bot-create-dialog"))
            .frame(
                egui::Frame::new()
                    .fill(crate::theme::BG_SECONDARY)
                    .stroke(egui::Stroke::new(1.0, crate::theme::DIVIDER))
                    .corner_radius(10)
                    .inner_margin(24),
            )
            .show(ui.ctx(), |ui| {
                ui.set_width(460.0_f32.min((ui.ctx().content_rect().width() - 64.0).max(280.0)));
                ui.heading("新建 KOL 带单机器人");
                ui.label(format!("主账户：{} · Binance", credential.label));
                ui.label("创建后为停止状态。确认启动后，才会向已授权的跟随者同步新限价委托。");
                if !current {
                    ui.colored_label(crate::theme::WARNING, "账户已切换，请关闭后重新创建。");
                }
                if let Some(error) = &model.execution.leader_bot.error {
                    ui.colored_label(crate::theme::WARNING, error);
                }
                ui.separator();
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(
                            !pending
                                && current
                                && access.can_use
                                && access.bot.is_none()
                                && model.execution.leader_bot.fresh,
                            egui::Button::new("创建机器人"),
                        )
                        .clicked()
                    {
                        action = Some(LeaderBotAction::Start);
                    }
                    if ui
                        .add_enabled(!pending, egui::Button::new("取消"))
                        .clicked()
                    {
                        model.execution.leader_bot.create_credential_id = None;
                    }
                    if pending {
                        ui.label("等待 Control 确认");
                        retry = ui.button("重试原请求").clicked();
                    }
                });
            });
        if response.should_close() && !pending && action.is_none() {
            model.execution.leader_bot.create_credential_id = None;
        }
    }
    let mutation = if retry {
        model.execution.leader_bot.pending.clone()
    } else if let Some(action) = action {
        let request_id = model.next_terminal_request_id();
        Some(match access.bot {
            Some(bot) => GridMutation::LeaderLifecycle(LeaderBotLifecycleRequest {
                schema_version: LEADER_BOT_SCHEMA_VERSION,
                request_id,
                bot_id: bot.bot_id,
                expected_revision: bot.revision,
                action,
                risk_confirmed: action == LeaderBotAction::Start,
            }),
            None => GridMutation::LeaderCreate(LeaderBotCreateRequest {
                schema_version: LEADER_BOT_SCHEMA_VERSION,
                request_id,
                credential_id: credential.credential_id.clone(),
            }),
        })
    } else {
        None
    };
    if let Some(mutation) = mutation {
        match client.send_grid(mutation.clone()) {
            Ok(()) => {
                model.execution.leader_bot.pending = Some(mutation);
                model.execution.leader_bot.error = None;
            }
            Err(_) => model.execution.leader_bot.error = Some("请求未进入发送队列".into()),
        }
    }
}
