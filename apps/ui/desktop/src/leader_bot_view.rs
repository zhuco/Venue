use crate::{
    client::{ControlClient, GridMutation},
    model::AppModel,
    theme,
};
use eframe::egui;
use rust_decimal::Decimal;
use venue_control_protocol::{
    accounts::CredentialSummary, grid::GridInstanceSummary, kol::KolProfileState, leader_bot::*,
};

#[derive(Clone, Debug)]
enum EditorMode {
    Create,
    Update { bot_id: String, revision: u64 },
}

#[derive(Clone, Debug)]
struct LeaderEditor {
    mode: EditorMode,
    credential_id: String,
    name: String,
    description: String,
    strategy_capital: String,
}

impl LeaderEditor {
    fn create(credential_id: &str, capital: Decimal) -> Self {
        Self {
            mode: EditorMode::Create,
            credential_id: credential_id.to_owned(),
            name: "KOL 带单".to_owned(),
            description: "同步主账户符合条件的新限价挂单".to_owned(),
            strategy_capital: capital.normalize().to_string(),
        }
    }

    fn update(bot: &LeaderBotListItem) -> Self {
        Self {
            mode: EditorMode::Update {
                bot_id: bot.bot_id.clone(),
                revision: bot.revision,
            },
            credential_id: bot.credential_id.clone(),
            name: bot.config.name.clone(),
            description: bot.config.description.clone(),
            strategy_capital: bot.config.strategy_capital.normalize().to_string(),
        }
    }

    fn config(&self) -> Result<LeaderBotConfig, String> {
        let config = LeaderBotConfig {
            name: self.name.trim().to_owned(),
            description: self.description.trim().to_owned(),
            strategy_capital: self
                .strategy_capital
                .trim()
                .parse()
                .map_err(|_| "策略资金必须是十进制数".to_owned())?,
        };
        config
            .valid()
            .then_some(config)
            .ok_or_else(|| "名称、说明或策略资金不合法".to_owned())
    }
}

#[derive(Debug, Default)]
pub struct LeaderBotView {
    pub access: Option<LeaderBotsAccess>,
    pub error: Option<String>,
    pub fresh: bool,
    pub pending: Option<GridMutation>,
    selected_bot_id: Option<String>,
    editor: Option<LeaderEditor>,
    confirmed: Option<(String, u64)>,
}

#[derive(Clone)]
enum BotRow {
    Grid(GridInstanceSummary),
    Leader(LeaderBotListItem),
}

impl BotRow {
    fn updated_ms(&self) -> u64 {
        match self {
            Self::Grid(instance) => instance.updated_ms,
            Self::Leader(bot) => bot.updated_ms,
        }
    }

    fn id(&self) -> &str {
        match self {
            Self::Grid(instance) => &instance.instance_id,
            Self::Leader(bot) => &bot.bot_id,
        }
    }
}

enum UiAction {
    Create(LeaderBotConfig),
    Update {
        bot_id: String,
        revision: u64,
        config: LeaderBotConfig,
    },
    Lifecycle {
        bot_id: String,
        revision: u64,
        action: LeaderBotAction,
    },
}

pub fn show(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    account_id: &str,
) {
    let access = model.execution.leader_bot.access.clone();
    let grids = crate::grid_view::visible_instances(model, account_id);
    let leaders = access
        .as_ref()
        .map(|access| {
            access
                .bots
                .iter()
                .filter(|bot| bot.trading_account_id == account_id)
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut rows = grids
        .iter()
        .cloned()
        .map(BotRow::Grid)
        .chain(leaders.iter().cloned().map(BotRow::Leader))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .updated_ms()
            .cmp(&left.updated_ms())
            .then_with(|| left.id().cmp(right.id()))
    });

    let credential_ready = credential.selectable(crate::account_center::now_ms());
    toolbar(
        ui,
        model,
        client,
        credential,
        access.as_ref(),
        credential_ready,
    );
    if let Some(error) = &model.execution.leader_bot.error {
        ui.colored_label(theme::WARNING, error);
    }
    robot_table(ui, model, access.as_ref(), credential, &rows);

    let mut action = None;
    leader_management(
        ui,
        model,
        access.as_ref(),
        credential,
        credential_ready,
        &mut action,
    );
    leader_editor(
        ui,
        model,
        access.as_ref(),
        credential,
        credential_ready,
        &mut action,
    );
    if let Some(action) = action {
        dispatch(model, client, credential, action);
    }
    crate::grid_view::show_management(ui, model, client, credential, &grids);
}

fn toolbar(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    access: Option<&LeaderBotsAccess>,
    credential_ready: bool,
) {
    let leader_pending = model.execution.leader_bot.pending.is_some();
    let grid_pending = crate::grid_view::pending(model);
    ui.horizontal_wrapped(|ui| {
        ui.strong("交易机器人");
        ui.weak("当前账户的私有内置策略；不包含策略广场。");
        ui.menu_button("新建机器人", |ui| {
            if ui
                .add_enabled(
                    credential_ready && !grid_pending,
                    egui::Button::new("Binance 对冲网格"),
                )
                .on_disabled_hover_text("需要已验证且可用的 Binance 双向持仓账户")
                .clicked()
            {
                model.execution.leader_bot.editor = None;
                crate::grid_view::open_create(model, credential);
                ui.close();
            }
            let can_create_leader = access.is_some_and(|access| {
                access.can_use
                    && access.bots.len() < MAX_LEADER_BOTS_PER_KOL as usize
                    && model.execution.leader_bot.fresh
            }) && credential_ready
                && !leader_pending;
            if ui
                .add_enabled(can_create_leader, egui::Button::new("KOL 挂单同步"))
                .on_disabled_hover_text("需要已启用的 KOL 资料、带单授权和已验证主账户")
                .clicked()
            {
                crate::grid_view::close_editor(model);
                let capital = access
                    .and_then(|access| access.bots.first())
                    .map_or_else(|| Decimal::from(100), |bot| bot.config.strategy_capital);
                model.execution.leader_bot.editor =
                    Some(LeaderEditor::create(&credential.credential_id, capital));
                ui.close();
            }
        });
        if leader_pending {
            ui.spinner();
            ui.colored_label(theme::WARNING, "带单操作待确认");
            if ui.button("重试原请求").clicked()
                && let Some(mutation) = model.execution.leader_bot.pending.clone()
            {
                send_mutation(model, client, mutation, false);
            }
        }
        if grid_pending {
            ui.spinner();
            ui.weak("Grid 操作待确认");
        }
    });
}

fn robot_table(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    access: Option<&LeaderBotsAccess>,
    credential: &CredentialSummary,
    rows: &[BotRow],
) {
    egui::ScrollArea::horizontal()
        .id_salt("built-in-robot-list")
        .show(ui, |ui| {
            egui::Grid::new("built-in-robot-table")
                .striped(true)
                .spacing([14.0, 7.0])
                .show(ui, |ui| {
                    for heading in [
                        "名称",
                        "策略",
                        "账户",
                        "范围",
                        "状态",
                        "运行数据",
                        "版本",
                        "PnL / 更新 / 注意",
                        "操作",
                    ] {
                        ui.weak(heading);
                    }
                    ui.end_row();
                    for row in rows {
                        match row {
                            BotRow::Grid(instance) => grid_row(ui, model, instance),
                            BotRow::Leader(bot) => leader_row(ui, model, access, credential, bot),
                        }
                        ui.end_row();
                    }
                    if rows.is_empty()
                        && access.is_some_and(|access| access.profile_state.is_some())
                    {
                        empty_leader_row(ui, access, credential);
                        ui.end_row();
                    }
                });
        });
    if rows.is_empty() && access.is_none_or(|access| access.profile_state.is_none()) {
        ui.weak("当前账户暂无机器人，可从“新建机器人”选择内置策略。");
    }
}

fn grid_row(ui: &mut egui::Ui, model: &mut AppModel, instance: &GridInstanceSummary) {
    let selected = crate::grid_view::is_selected(model, &instance.instance_id);
    if ui
        .selectable_label(selected, format!("Grid · {}", instance.symbol))
        .clicked()
    {
        model.execution.leader_bot.selected_bot_id = None;
        crate::grid_view::select(model, &instance.instance_id);
    }
    ui.label("Binance 对冲网格");
    ui.label(credential_label(model, &instance.credential_id))
        .on_hover_text(&instance.trading_account_id);
    if ui.link(instance.symbol.to_string()).clicked() {
        model.select_symbol(instance.symbol.to_string());
        model.follow_latest_requested = true;
    }
    let (state, color) = crate::grid_view::state_presentation(instance.state);
    ui.colored_label(color, state);
    ui.monospace(format!("失败 {}", instance.consecutive_failures));
    ui.monospace(format!(
        "{} / {} / {}",
        instance.revision, instance.config_revision, instance.plan_revision
    ));
    if let Some(code) = &instance.attention_code {
        crate::grid_view::attention_label(ui, model.preferences.language, code);
    } else if let Some(pnl) = crate::grid_view::grid_pnl(model, instance) {
        crate::grid_view::pnl_label(ui, pnl)
            .on_hover_text("来自签名账户投影的该交易对双腿未实现盈亏；不表示单个 Grid 的独立归因");
    } else {
        ui.weak(timestamp(instance.updated_ms));
    }
    ui.horizontal(|ui| {
        if ui.small_button("管理").clicked() {
            model.execution.leader_bot.selected_bot_id = None;
            crate::grid_view::select(model, &instance.instance_id);
        }
        if ui
            .add_enabled(
                crate::grid_view::editable(instance.state) && !crate::grid_view::pending(model),
                egui::Button::new("编辑"),
            )
            .clicked()
        {
            model.execution.leader_bot.editor = None;
            model.execution.leader_bot.selected_bot_id = None;
            crate::grid_view::open_update(model, instance);
        }
    });
}

fn leader_row(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    access: Option<&LeaderBotsAccess>,
    credential: &CredentialSummary,
    bot: &LeaderBotListItem,
) {
    let selected = model.execution.leader_bot.selected_bot_id.as_deref() == Some(&bot.bot_id);
    if ui.selectable_label(selected, &bot.config.name).clicked() {
        crate::grid_view::clear_selection(model);
        model.execution.leader_bot.selected_bot_id = Some(bot.bot_id.clone());
    }
    ui.label("KOL 挂单同步");
    ui.label(credential_label_or_current(
        model,
        credential,
        &bot.credential_id,
    ))
    .on_hover_text(&bot.trading_account_id);
    ui.weak("符合条件的新限价挂单");
    let (state, color) = leader_state(bot.state, model.execution.leader_bot.fresh);
    ui.colored_label(color, state);
    ui.monospace(format!(
        "跟随 {} · 挂单 {}",
        bot.active_followers, bot.pending_orders
    ));
    ui.monospace(format!(
        "{} / {} / {}",
        bot.revision,
        bot.config_revision,
        access.map_or(0, |access| access.permission_revision)
    ));
    if let Some(code) = &bot.attention_code {
        ui.colored_label(theme::WARNING, attention_text(code));
    } else if access.is_some_and(|access| !access.can_use) {
        ui.colored_label(theme::WARNING, "授权不可用");
    } else {
        ui.weak(timestamp(bot.updated_ms));
    }
    ui.horizontal(|ui| {
        if ui.small_button("管理").clicked() {
            crate::grid_view::clear_selection(model);
            model.execution.leader_bot.selected_bot_id = Some(bot.bot_id.clone());
        }
        let editable = bot.state == LeaderBotState::Stopped
            && bot.credential_id == credential.credential_id
            && model.execution.leader_bot.fresh
            && model.execution.leader_bot.pending.is_none();
        if ui
            .add_enabled(editable, egui::Button::new("编辑"))
            .on_disabled_hover_text("仅可编辑当前凭证绑定且已停用的机器人")
            .clicked()
        {
            crate::grid_view::close_editor(model);
            crate::grid_view::clear_selection(model);
            model.execution.leader_bot.selected_bot_id = Some(bot.bot_id.clone());
            model.execution.leader_bot.editor = Some(LeaderEditor::update(bot));
        }
    });
}

fn empty_leader_row(
    ui: &mut egui::Ui,
    access: Option<&LeaderBotsAccess>,
    credential: &CredentialSummary,
) {
    let Some(access) = access else {
        return;
    };
    ui.weak("尚未创建");
    ui.label("KOL 挂单同步");
    ui.label(&credential.label);
    ui.weak("符合条件的新限价挂单");
    let (label, color) = availability_state(access);
    ui.colored_label(color, label);
    ui.weak("—");
    ui.monospace(format!("— / — / {}", access.permission_revision));
    ui.weak(availability_note(access));
    ui.weak("从上方新建");
}

fn leader_management(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    access: Option<&LeaderBotsAccess>,
    credential: &CredentialSummary,
    credential_ready: bool,
    action: &mut Option<UiAction>,
) {
    let Some(access) = access else {
        return;
    };
    let Some(bot) = model
        .execution
        .leader_bot
        .selected_bot_id
        .as_deref()
        .and_then(|id| access.bots.iter().find(|bot| bot.bot_id == id))
        .cloned()
    else {
        return;
    };
    let scope_current = bot.credential_id == credential.credential_id;
    let pending = model.execution.leader_bot.pending.is_some();
    let fresh = model.execution.leader_bot.fresh;
    let active_sibling = access
        .bots
        .iter()
        .any(|other| other.bot_id != bot.bot_id && other.state != LeaderBotState::Stopped);
    ui.add_space(8.0);
    ui.group(|ui| {
        ui.horizontal_wrapped(|ui| {
            ui.strong(&bot.config.name);
            ui.weak(format!(
                "策略资金 {} · 实例 {}",
                bot.config.strategy_capital.normalize(),
                short_id(&bot.bot_id)
            ));
        });
        if !bot.config.description.is_empty() {
            ui.label(&bot.config.description);
        }
        if bot.state == LeaderBotState::Stopped && access.can_use {
            let mut confirmed = confirmed(model, &bot);
            if ui
                .add_enabled(
                    scope_current && credential_ready && fresh && !pending && !active_sibling,
                    egui::Checkbox::new(
                        &mut confirmed,
                        "确认启用后同步符合条件的新限价挂单；跟随账户仍须各自启用",
                    ),
                )
                .changed()
            {
                model.execution.leader_bot.confirmed =
                    confirmed.then(|| (bot.bot_id.clone(), bot.revision));
            }
        }
        ui.horizontal_wrapped(|ui| match bot.state {
            LeaderBotState::Stopped => {
                if ui
                    .add_enabled(
                        access.can_use
                            && scope_current
                            && credential_ready
                            && fresh
                            && !pending
                            && !active_sibling
                            && confirmed(model, &bot),
                        egui::Button::new("启用"),
                    )
                    .on_disabled_hover_text(
                        "需有效授权、当前已验证凭证、无其他活动带单机器人并完成风险确认",
                    )
                    .clicked()
                {
                    *action = Some(UiAction::Lifecycle {
                        bot_id: bot.bot_id.clone(),
                        revision: bot.revision,
                        action: LeaderBotAction::Start,
                    });
                }
                if ui
                    .add_enabled(
                        scope_current && fresh && !pending,
                        egui::Button::new("编辑配置"),
                    )
                    .clicked()
                {
                    model.execution.leader_bot.editor = Some(LeaderEditor::update(&bot));
                }
            }
            LeaderBotState::Running | LeaderBotState::NeedsAttention => {
                if ui
                    .add_enabled(
                        scope_current && fresh && !pending,
                        egui::Button::new("停用"),
                    )
                    .on_disabled_hover_text("停用会撤销程序子单，但不会自动平仓")
                    .clicked()
                {
                    *action = Some(UiAction::Lifecycle {
                        bot_id: bot.bot_id.clone(),
                        revision: bot.revision,
                        action: LeaderBotAction::Stop,
                    });
                }
            }
            LeaderBotState::Draining => {
                ui.spinner();
                ui.weak("正在撤销程序子单并等待对账，不会自动平仓");
                if ui
                    .add_enabled(
                        scope_current && fresh && !pending,
                        egui::Button::new("重试停用"),
                    )
                    .clicked()
                {
                    *action = Some(UiAction::Lifecycle {
                        bot_id: bot.bot_id.clone(),
                        revision: bot.revision,
                        action: LeaderBotAction::Stop,
                    });
                }
            }
        });
        if active_sibling && bot.state == LeaderBotState::Stopped {
            ui.colored_label(
                theme::WARNING,
                "同一 KOL 当前已有活动带单机器人；停用完成后才能切换。",
            );
        } else if !scope_current {
            ui.colored_label(theme::WARNING, "请切换到该机器人绑定的凭证后管理。");
        } else if !credential_ready {
            ui.colored_label(theme::WARNING, "主账户凭证验证已失效，请先重新验证。");
        } else if !access.can_use && bot.state == LeaderBotState::Stopped {
            ui.colored_label(theme::WARNING, "带单授权未启用，当前不能启用机器人。");
        }
        ui.weak("停用只撤销程序创建的同步挂单；已有仓位不会自动平仓。");
    });
}

fn leader_editor(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    access: Option<&LeaderBotsAccess>,
    credential: &CredentialSummary,
    credential_ready: bool,
    action: &mut Option<UiAction>,
) {
    let Some(mut draft) = model.execution.leader_bot.editor.clone() else {
        return;
    };
    let pending = model.execution.leader_bot.pending.is_some();
    let scope_current = draft.credential_id == credential.credential_id;
    let current = editor_is_current(&draft, access);
    let viewport = ui.ctx().content_rect().size();
    let response = egui::Modal::new(egui::Id::new("leader-bot-config-dialog"))
        .frame(
            egui::Frame::new()
                .fill(theme::BG_SECONDARY)
                .stroke(egui::Stroke::new(1.0, theme::DIVIDER))
                .corner_radius(10)
                .inner_margin(24),
        )
        .show(ui.ctx(), |ui| {
            ui.set_width(500.0_f32.min((viewport.x - 64.0).max(280.0)));
            ui.horizontal(|ui| {
                ui.heading(match &draft.mode {
                    EditorMode::Create => "新建 KOL 挂单同步机器人",
                    EditorMode::Update { .. } => "编辑 KOL 挂单同步机器人",
                });
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.add_enabled(!pending, egui::Button::new("×")).clicked() {
                        model.execution.leader_bot.editor = None;
                    }
                });
            });
            ui.label(format!("主账户：{} · Binance", credential.label));
            ui.weak("同一主账户可保存多条配置；当前只允许一条带单机器人处于活动状态。");
            ui.separator();
            ui.add_enabled_ui(!pending && scope_current, |ui| {
                egui::Grid::new("leader-bot-config-editor")
                    .num_columns(2)
                    .spacing([10.0, 8.0])
                    .show(ui, |ui| {
                        ui.label("名称");
                        ui.add_sized(
                            [280.0, 24.0],
                            egui::TextEdit::singleline(&mut draft.name).char_limit(64),
                        );
                        ui.end_row();
                        ui.label("策略资金");
                        ui.add_sized(
                            [160.0, 24.0],
                            egui::TextEdit::singleline(&mut draft.strategy_capital),
                        );
                        ui.end_row();
                        ui.label("说明");
                        ui.add_sized(
                            [320.0, 24.0],
                            egui::TextEdit::singleline(&mut draft.description).char_limit(500),
                        );
                        ui.end_row();
                    });
            });
            if let Some(error) = &model.execution.leader_bot.error {
                ui.colored_label(theme::WARNING, error);
            }
            if !scope_current {
                ui.colored_label(theme::WARNING, "账户已切换，请关闭后从绑定凭证重新打开。");
            } else if !current {
                ui.colored_label(theme::WARNING, "机器人状态或版本已刷新，请取消后重新编辑。");
            }
            ui.separator();
            ui.horizontal(|ui| {
                let can_save = !pending
                    && scope_current
                    && current
                    && access.is_some_and(|access| access.can_use)
                    && (credential_ready || matches!(&draft.mode, EditorMode::Update { .. }));
                if ui
                    .add_enabled(can_save, egui::Button::new("保存配置"))
                    .clicked()
                {
                    match draft.config() {
                        Ok(config) => {
                            *action = Some(match &draft.mode {
                                EditorMode::Create => UiAction::Create(config),
                                EditorMode::Update { bot_id, revision } => UiAction::Update {
                                    bot_id: bot_id.clone(),
                                    revision: *revision,
                                    config,
                                },
                            });
                        }
                        Err(error) => model.execution.leader_bot.error = Some(error),
                    }
                }
                if ui
                    .add_enabled(!pending, egui::Button::new("取消"))
                    .clicked()
                {
                    model.execution.leader_bot.editor = None;
                }
                if pending {
                    ui.spinner();
                    ui.weak("等待 Control 确认");
                }
            });
        });
    if response.should_close() && !pending && action.is_none() {
        model.execution.leader_bot.editor = None;
    } else if model.execution.leader_bot.editor.is_some() {
        model.execution.leader_bot.editor = Some(draft);
    }
}

fn editor_is_current(editor: &LeaderEditor, access: Option<&LeaderBotsAccess>) -> bool {
    match &editor.mode {
        EditorMode::Create => {
            access.is_some_and(|access| access.bots.len() < MAX_LEADER_BOTS_PER_KOL as usize)
        }
        EditorMode::Update { bot_id, revision } => access.is_some_and(|access| {
            access.bots.iter().any(|bot| {
                bot.bot_id == *bot_id
                    && bot.revision == *revision
                    && bot.state == LeaderBotState::Stopped
            })
        }),
    }
}

fn dispatch(
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    action: UiAction,
) {
    let request_id = model.next_terminal_request_id();
    let (mutation, clear_confirmation) = match action {
        UiAction::Create(config) => (
            GridMutation::LeaderCreate(LeaderBotConfiguredCreateRequest {
                schema_version: LEADER_BOTS_SCHEMA_VERSION,
                request_id,
                credential_id: credential.credential_id.clone(),
                config,
            }),
            false,
        ),
        UiAction::Update {
            bot_id,
            revision,
            config,
        } => (
            GridMutation::LeaderUpdate(LeaderBotUpdateRequest {
                schema_version: LEADER_BOTS_SCHEMA_VERSION,
                request_id,
                bot_id,
                expected_revision: revision,
                credential_id: credential.credential_id.clone(),
                config,
            }),
            false,
        ),
        UiAction::Lifecycle {
            bot_id,
            revision,
            action,
        } => (
            GridMutation::LeaderLifecycle(LeaderBotLifecycleRequest {
                schema_version: LEADER_BOT_SCHEMA_VERSION,
                request_id,
                bot_id,
                expected_revision: revision,
                action,
                risk_confirmed: action == LeaderBotAction::Start,
            }),
            action == LeaderBotAction::Start,
        ),
    };
    send_mutation(model, client, mutation, clear_confirmation);
}

fn send_mutation(
    model: &mut AppModel,
    client: &ControlClient,
    mutation: GridMutation,
    clear_confirmation: bool,
) {
    match client.send_grid(mutation.clone()) {
        Ok(()) => {
            if clear_confirmation {
                model.execution.leader_bot.confirmed = None;
            }
            if matches!(
                &mutation,
                GridMutation::LeaderCreate(_) | GridMutation::LeaderUpdate(_)
            ) {
                model.execution.leader_bot.editor = None;
            }
            model.execution.leader_bot.pending = Some(mutation);
            model.execution.leader_bot.error = None;
        }
        Err(_) => model.execution.leader_bot.error = Some("请求未进入发送队列".to_owned()),
    }
}

fn confirmed(model: &AppModel, bot: &LeaderBotListItem) -> bool {
    model
        .execution
        .leader_bot
        .confirmed
        .as_ref()
        .is_some_and(|(bot_id, revision)| bot_id == &bot.bot_id && *revision == bot.revision)
}

fn leader_state(state: LeaderBotState, fresh: bool) -> (&'static str, egui::Color32) {
    if !fresh {
        return ("状态未刷新", theme::WARNING);
    }
    match state {
        LeaderBotState::Stopped => ("已停用", theme::TEXT_SECONDARY),
        LeaderBotState::Running => ("运行中", theme::BUY),
        LeaderBotState::Draining => ("正在停用", theme::WARNING),
        LeaderBotState::NeedsAttention => ("需要处理", theme::SELL),
    }
}

fn availability_state(access: &LeaderBotsAccess) -> (&'static str, egui::Color32) {
    if access.can_use {
        ("可新建", theme::TEXT_SECONDARY)
    } else {
        match access.profile_state {
            Some(KolProfileState::Enabled) => ("待带单授权", theme::WARNING),
            Some(KolProfileState::Draft) => ("KOL 资料草稿", theme::WARNING),
            Some(KolProfileState::Disabled) | None => ("KOL 已停用", theme::TEXT_SECONDARY),
        }
    }
}

fn availability_note(access: &LeaderBotsAccess) -> &'static str {
    if access.can_use {
        "授权有效，可创建"
    } else {
        match access.profile_state {
            Some(KolProfileState::Enabled) => "等待管理员开启带单授权",
            Some(KolProfileState::Draft) => "先启用 KOL 资料",
            Some(KolProfileState::Disabled) | None => "当前不可创建或启用",
        }
    }
}

fn attention_text(code: &str) -> String {
    match code {
        "permission_changed" => "授权已变化，正在停用".to_owned(),
        "cancel_retry_exhausted" => "撤单重试已达上限".to_owned(),
        _ => format!("需处理：{code}"),
    }
}

fn credential_label(model: &AppModel, credential_id: &str) -> String {
    model
        .account_overview
        .as_ref()
        .and_then(|overview| {
            overview
                .credentials
                .iter()
                .find(|credential| credential.credential_id == credential_id)
        })
        .map(|credential| credential.label.clone())
        .unwrap_or_else(|| "凭证已移除".to_owned())
}

fn credential_label_or_current(
    model: &AppModel,
    current: &CredentialSummary,
    credential_id: &str,
) -> String {
    if current.credential_id == credential_id {
        current.label.clone()
    } else {
        credential_label(model, credential_id)
    }
}

fn short_id(value: &str) -> &str {
    value.get(..8).unwrap_or(value)
}

fn timestamp(ms: u64) -> String {
    crate::chart::format_timeline_label(ms, crate::chart::ChartInterval::OneHour)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_text(shape: &egui::Shape, texts: &mut Vec<String>) {
        match shape {
            egui::Shape::Text(text) => texts.push(text.galley.job.text.clone()),
            egui::Shape::Vec(shapes) => {
                shapes.iter().for_each(|shape| collect_text(shape, texts));
            }
            _ => {}
        }
    }

    fn bot(id: &str, state: LeaderBotState, updated_ms: u64) -> LeaderBotListItem {
        LeaderBotListItem {
            bot_id: id.to_owned(),
            trading_account_id: "00000000-0000-4000-8000-000000000020".to_owned(),
            credential_id: "00000000-0000-4000-8000-000000000002".to_owned(),
            config: LeaderBotConfig {
                name: format!("带单 {}", short_id(id)),
                description: String::new(),
                strategy_capital: Decimal::from(100),
            },
            state,
            revision: 1,
            config_revision: 1,
            active_followers: 0,
            pending_orders: 0,
            attention_code: None,
            created_ms: 1,
            updated_ms,
        }
    }

    fn access(bots: Vec<LeaderBotListItem>) -> LeaderBotsAccess {
        LeaderBotsAccess {
            schema_version: LEADER_BOTS_SCHEMA_VERSION,
            profile_state: Some(KolProfileState::Enabled),
            can_use: true,
            permission_revision: 1,
            bots,
        }
    }

    #[test]
    fn multiple_saved_bots_are_valid_but_only_one_may_be_active() {
        let first = bot(
            "00000000-0000-4000-8000-000000000001",
            LeaderBotState::Running,
            2,
        );
        let second = bot(
            "00000000-0000-4000-8000-000000000003",
            LeaderBotState::Stopped,
            1,
        );
        assert!(access(vec![first.clone(), second.clone()]).valid());
        assert!(
            !access(vec![
                first,
                LeaderBotListItem {
                    state: LeaderBotState::Draining,
                    ..second
                },
            ])
            .valid()
        );
    }

    #[test]
    fn stopped_editor_requires_the_same_revision() {
        let bot = bot(
            "00000000-0000-4000-8000-000000000001",
            LeaderBotState::Stopped,
            1,
        );
        let editor = LeaderEditor::update(&bot);
        assert!(editor_is_current(&editor, Some(&access(vec![bot.clone()]))));
        let changed = LeaderBotListItem { revision: 2, ..bot };
        assert!(!editor_is_current(&editor, Some(&access(vec![changed]))));
    }

    #[test]
    fn list_rows_sort_across_built_in_strategy_types() {
        let mut rows = [
            BotRow::Leader(bot(
                "00000000-0000-4000-8000-000000000001",
                LeaderBotState::Stopped,
                1,
            )),
            BotRow::Leader(bot(
                "00000000-0000-4000-8000-000000000003",
                LeaderBotState::Stopped,
                3,
            )),
        ];
        rows.sort_by(|left, right| right.updated_ms().cmp(&left.updated_ms()));
        assert_eq!(rows[0].updated_ms(), 3);
    }

    #[test]
    fn multiple_leader_bots_render_as_bounded_list_rows() {
        let context = egui::Context::default();
        crate::theme::apply(&context);
        let screen = egui::Rect::from_min_size(egui::Pos2::ZERO, egui::vec2(390.0, 240.0));
        let mut model = AppModel::new(Default::default());
        model.execution.leader_bot.fresh = true;
        let credential = CredentialSummary {
            credential_id: "00000000-0000-4000-8000-000000000002".to_owned(),
            label: "zhu".to_owned(),
            venue: venue_control_protocol::VenueId::Binance,
            masked_key: "****1234".to_owned(),
            trading_account_id: Some("00000000-0000-4000-8000-000000000020".to_owned()),
            verification: venue_control_protocol::accounts::ApiVerificationState::Verified,
            verified_ms: Some(crate::account_center::now_ms()),
            expires_ms: None,
            api_reachable: true,
            dual_position: true,
            account_mode: Some("portfolio_margin_um".to_owned()),
            has_exposure: Some(false),
        };
        let access = access(vec![
            bot(
                "00000000-0000-4000-8000-000000000001",
                LeaderBotState::Running,
                3,
            ),
            bot(
                "00000000-0000-4000-8000-000000000003",
                LeaderBotState::Stopped,
                2,
            ),
        ]);
        let rows = access
            .bots
            .iter()
            .cloned()
            .map(BotRow::Leader)
            .collect::<Vec<_>>();
        let mut texts = Vec::new();
        for _ in 0..3 {
            let mut output = context.run_ui(
                egui::RawInput {
                    screen_rect: Some(screen),
                    ..Default::default()
                },
                |ui| robot_table(ui, &mut model, Some(&access), &credential, &rows),
            );
            output.textures_delta.clear();
            texts.clear();
            for shape in output.shapes {
                collect_text(&shape.shape, &mut texts);
            }
        }
        for expected in ["名称", "策略", "KOL 挂单同步", "zhu"] {
            assert!(
                texts.iter().any(|text| text == expected),
                "missing {expected}"
            );
        }
        assert!(context.globally_used_rect().max.x <= screen.max.x);
    }
}
