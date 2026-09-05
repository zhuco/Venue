use eframe::egui;
use rust_decimal::Decimal;
use std::str::FromStr as _;
use venue_control_protocol::{
    accounts::CredentialSummary,
    grid::{
        GRID_SCHEMA_VERSION, GridConfig, GridConfigUpdateRequest, GridInstanceCreateRequest,
        GridInstanceState, GridInstanceSummary, GridInventoryReplenishment, GridLifecycleAction,
        GridLifecycleRequest, GridProfitReduction, GridResetPolicy,
    },
};
use venue_domain::PositionSide;

use crate::{
    client::{ControlClient, GridMutation},
    i18n::Language,
    model::AppModel,
    theme,
};

#[derive(Clone, Debug)]
enum EditorMode {
    Create,
    Update { instance_id: String, revision: u64 },
}

#[derive(Clone, Debug)]
struct GridEditor {
    mode: EditorMode,
    credential_id: String,
    symbol: String,
    order_notional: String,
    spacing_rate: String,
    grid_levels: String,
    max_total_notional: String,
    replenish_enabled: bool,
    minimum_inventory_notional: String,
    target_inventory_notional: String,
    max_single_replenishment_notional: String,
    reduce_enabled: bool,
    inventory_equity_multiple: String,
    minimum_unrealized_profit_rate: String,
    reduction_fraction: String,
    max_single_reduce_notional: String,
    stale_market_ms: String,
    stale_private_ms: String,
    convergence_timeout_ms: String,
    max_consecutive_failures: String,
}

impl GridEditor {
    fn create(symbol: &str, credential_id: &str) -> Self {
        Self {
            mode: EditorMode::Create,
            credential_id: credential_id.to_owned(),
            symbol: symbol.to_owned(),
            order_notional: "10".to_owned(),
            spacing_rate: "0.002".to_owned(),
            grid_levels: "10".to_owned(),
            max_total_notional: "1000".to_owned(),
            replenish_enabled: false,
            minimum_inventory_notional: "10".to_owned(),
            target_inventory_notional: "20".to_owned(),
            max_single_replenishment_notional: "10".to_owned(),
            reduce_enabled: false,
            inventory_equity_multiple: "3".to_owned(),
            minimum_unrealized_profit_rate: "0.05".to_owned(),
            reduction_fraction: "0.3".to_owned(),
            max_single_reduce_notional: "100".to_owned(),
            stale_market_ms: "5000".to_owned(),
            stale_private_ms: "15000".to_owned(),
            convergence_timeout_ms: "30000".to_owned(),
            max_consecutive_failures: "3".to_owned(),
        }
    }

    fn update(instance: &GridInstanceSummary) -> Self {
        let config = &instance.config;
        Self {
            mode: EditorMode::Update {
                instance_id: instance.instance_id.clone(),
                revision: instance.revision,
            },
            credential_id: instance.credential_id.clone(),
            symbol: instance.symbol.to_string(),
            order_notional: number(config.order_notional),
            spacing_rate: number(config.spacing_rate),
            grid_levels: config.grid_levels.to_string(),
            max_total_notional: number(config.max_total_notional),
            replenish_enabled: config.inventory_replenishment.enabled,
            minimum_inventory_notional: number(
                config.inventory_replenishment.minimum_inventory_notional,
            ),
            target_inventory_notional: number(
                config.inventory_replenishment.target_inventory_notional,
            ),
            max_single_replenishment_notional: number(
                config
                    .inventory_replenishment
                    .max_single_replenishment_notional,
            ),
            reduce_enabled: config.profit_reduction.enabled,
            inventory_equity_multiple: number(config.profit_reduction.inventory_equity_multiple),
            minimum_unrealized_profit_rate: number(
                config.profit_reduction.minimum_unrealized_profit_rate,
            ),
            reduction_fraction: number(config.profit_reduction.reduction_fraction),
            max_single_reduce_notional: number(config.profit_reduction.max_single_reduce_notional),
            stale_market_ms: config.reset_policy.stale_market_ms.to_string(),
            stale_private_ms: config.reset_policy.stale_private_ms.to_string(),
            convergence_timeout_ms: config.reset_policy.convergence_timeout_ms.to_string(),
            max_consecutive_failures: config.reset_policy.max_consecutive_failures.to_string(),
        }
    }

    fn config(&self) -> Result<GridConfig, String> {
        let config = GridConfig {
            order_notional: decimal(&self.order_notional, "单层名义价值")?,
            spacing_rate: decimal(&self.spacing_rate, "网格间距")?,
            grid_levels: integer(&self.grid_levels, "网格层数")?,
            max_total_notional: decimal(&self.max_total_notional, "最大总名义价值")?,
            inventory_replenishment: GridInventoryReplenishment {
                enabled: self.replenish_enabled,
                minimum_inventory_notional: decimal(&self.minimum_inventory_notional, "最低库存")?,
                target_inventory_notional: decimal(&self.target_inventory_notional, "目标库存")?,
                max_single_replenishment_notional: decimal(
                    &self.max_single_replenishment_notional,
                    "单次补库存上限",
                )?,
            },
            profit_reduction: GridProfitReduction {
                enabled: self.reduce_enabled,
                inventory_equity_multiple: decimal(
                    &self.inventory_equity_multiple,
                    "库存权益倍数",
                )?,
                minimum_unrealized_profit_rate: decimal(
                    &self.minimum_unrealized_profit_rate,
                    "最低浮盈率",
                )?,
                reduction_fraction: decimal(&self.reduction_fraction, "减仓比例")?,
                max_single_reduce_notional: decimal(
                    &self.max_single_reduce_notional,
                    "单次减仓上限",
                )?,
            },
            reset_policy: GridResetPolicy {
                stale_market_ms: integer(&self.stale_market_ms, "行情过期毫秒")?,
                stale_private_ms: integer(&self.stale_private_ms, "私有事实过期毫秒")?,
                convergence_timeout_ms: integer(&self.convergence_timeout_ms, "收敛超时毫秒")?,
                max_consecutive_failures: integer(&self.max_consecutive_failures, "连续失败次数")?,
            },
        };
        config
            .validate()
            .map_err(|error| format!("网格配置不合法：{error}"))?;
        Ok(config)
    }
}

#[derive(Debug, Default)]
pub struct GridViewState {
    instances: Vec<GridInstanceSummary>,
    selected_instance_id: Option<String>,
    editor: Option<GridEditor>,
    refresh_error: Option<String>,
    mutation_error: Option<String>,
    pending: bool,
    risk_confirmed: bool,
    positions_remain_acknowledged: bool,
}

impl GridViewState {
    pub fn apply_instances(&mut self, mut instances: Vec<GridInstanceSummary>) {
        if instances
            .iter()
            .any(|instance| instance.validate().is_err())
        {
            self.refresh_error = Some("Grid 实例投影校验失败".to_owned());
            return;
        }
        for current in &self.instances {
            match instances
                .iter()
                .position(|incoming| incoming.instance_id == current.instance_id)
            {
                Some(index)
                    if instances[index].revision < current.revision
                        || (instances[index].revision == current.revision
                            && instances[index].updated_ms < current.updated_ms) =>
                {
                    instances[index] = current.clone();
                }
                Some(_) => {}
                None => instances.push(current.clone()),
            }
        }
        instances.sort_by(|left, right| {
            right
                .updated_ms
                .cmp(&left.updated_ms)
                .then_with(|| left.instance_id.cmp(&right.instance_id))
        });
        if self.selected_instance_id.as_ref().is_some_and(|selected| {
            !instances
                .iter()
                .any(|instance| &instance.instance_id == selected)
        }) {
            self.selected_instance_id = None;
        }
        self.instances = instances;
        self.refresh_error = None;
    }

    pub fn apply_summary(&mut self, summary: GridInstanceSummary) {
        if summary.validate().is_err() {
            self.mutation_error = Some("Grid 操作响应校验失败".to_owned());
            self.pending = false;
            return;
        }
        self.instances
            .retain(|instance| instance.instance_id != summary.instance_id);
        self.selected_instance_id = Some(summary.instance_id.clone());
        self.instances.insert(0, summary);
        self.editor = None;
        self.pending = false;
        self.risk_confirmed = false;
        self.positions_remain_acknowledged = false;
        self.refresh_error = None;
        self.mutation_error = None;
    }

    pub fn list_unavailable(&mut self, message: String) {
        self.refresh_error = Some(message);
    }

    pub fn mutation_unavailable(&mut self, message: String) {
        self.mutation_error = Some(message);
        self.pending = false;
    }
}

pub(crate) fn visible_instances(model: &AppModel, account_id: &str) -> Vec<GridInstanceSummary> {
    let current_symbol = model
        .execution
        .current_symbol
        .then(|| model.preferences.selected_symbol.clone());
    model
        .execution
        .grid
        .instances
        .iter()
        .filter(|instance| {
            instance.trading_account_id == account_id
                && current_symbol
                    .as_ref()
                    .is_none_or(|symbol| instance.symbol.to_string() == *symbol)
        })
        .cloned()
        .collect()
}

pub(crate) fn open_create(model: &mut AppModel, credential: &CredentialSummary) {
    model.execution.grid.mutation_error = None;
    model.execution.grid.editor = Some(GridEditor::create(
        &model.preferences.selected_symbol,
        &credential.credential_id,
    ));
}

pub(crate) fn open_update(model: &mut AppModel, instance: &GridInstanceSummary) {
    if config_editable(instance.state) && !model.execution.grid.pending {
        model.execution.grid.mutation_error = None;
        model.execution.grid.selected_instance_id = Some(instance.instance_id.clone());
        model.execution.grid.editor = Some(GridEditor::update(instance));
    }
}

pub(crate) fn select(model: &mut AppModel, instance_id: &str) {
    model.execution.grid.selected_instance_id = Some(instance_id.to_owned());
}

pub(crate) fn clear_selection(model: &mut AppModel) {
    model.execution.grid.selected_instance_id = None;
}

pub(crate) fn close_editor(model: &mut AppModel) {
    model.execution.grid.editor = None;
}

pub(crate) fn is_selected(model: &AppModel, instance_id: &str) -> bool {
    model.execution.grid.selected_instance_id.as_deref() == Some(instance_id)
}

pub(crate) fn pending(model: &AppModel) -> bool {
    model.execution.grid.pending
}

pub(crate) fn editable(state: GridInstanceState) -> bool {
    config_editable(state)
}

pub(crate) fn state_presentation(state: GridInstanceState) -> (&'static str, egui::Color32) {
    match state {
        GridInstanceState::Draft => ("草稿", theme::TEXT_SECONDARY),
        GridInstanceState::StartPending => ("正在启动", theme::WARNING),
        GridInstanceState::Running => ("运行中", theme::BUY),
        GridInstanceState::Paused => ("已暂停", theme::TEXT_SECONDARY),
        GridInstanceState::StopPending => ("正在停止", theme::SELL),
        GridInstanceState::Stopped => ("已停止", theme::TEXT_SECONDARY),
        GridInstanceState::Blocked => ("已阻塞", theme::WARNING),
        GridInstanceState::ResetRequired => ("需要重置", theme::WARNING),
        GridInstanceState::NeedsAttention => ("需要处理", theme::WARNING),
    }
}

pub(crate) fn show_management(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    visible: &[GridInstanceSummary],
) {
    let credential_ready = credential.selectable(crate::account_center::now_ms());
    let mut action = None;
    if let Some(error) = &model.execution.grid.mutation_error {
        ui.colored_label(theme::WARNING, error);
    }
    if let Some(error) = &model.execution.grid.refresh_error {
        ui.colored_label(theme::WARNING, error);
    }
    lifecycle_controls(ui, model, visible, credential_ready, &mut action);
    editor(ui, model, credential, credential_ready, &mut action);
    if let Some(action) = action {
        dispatch(model, client, credential, action);
    }
}

enum UiAction {
    Create {
        symbol: String,
        config: GridConfig,
    },
    Update {
        instance_id: String,
        revision: u64,
        config: GridConfig,
    },
    Lifecycle {
        instance_id: String,
        revision: u64,
        action: GridLifecycleAction,
    },
}

fn instance_credential_label(model: &AppModel, credential_id: &str) -> String {
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
        .unwrap_or_else(|| {
            tr(
                model.preferences.language,
                "凭证已移除",
                "Credential removed",
            )
            .to_owned()
        })
}

fn lifecycle_controls(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    visible: &[GridInstanceSummary],
    credential_ready: bool,
    action: &mut Option<UiAction>,
) {
    let language = model.preferences.language;
    let selected = model
        .execution
        .grid
        .selected_instance_id
        .as_deref()
        .and_then(|id| visible.iter().find(|instance| instance.instance_id == id))
        .cloned();
    let Some(instance) = selected else {
        return;
    };
    ui.add_space(8.0);
    ui.horizontal_wrapped(|ui| {
        ui.checkbox(
            &mut model.execution.grid.risk_confirmed,
            tr(language, "确认启动交易风险", "Confirm trading risk"),
        );
        ui.checkbox(
            &mut model.execution.grid.positions_remain_acknowledged,
            tr(
                language,
                "确认 Stop / Reset 不会平仓",
                "Acknowledge Stop / Reset keeps positions",
            ),
        );
    });
    ui.horizontal_wrapped(|ui| {
        let pending = model.execution.grid.pending;
        for (kind, label) in [
            (GridLifecycleAction::Start, tr(language, "启动", "Start")),
            (GridLifecycleAction::Pause, tr(language, "暂停", "Pause")),
            (GridLifecycleAction::Resume, tr(language, "继续", "Resume")),
            (
                GridLifecycleAction::Reset,
                tr(language, "异常重置", "Reset"),
            ),
            (GridLifecycleAction::Stop, tr(language, "停止", "Stop")),
        ] {
            let enabled = lifecycle_enabled(
                instance.state,
                kind,
                credential_ready,
                model.execution.grid.risk_confirmed,
                model.execution.grid.positions_remain_acknowledged,
            );
            if ui
                .add_enabled(enabled && !pending, egui::Button::new(label))
                .clicked()
            {
                *action = Some(UiAction::Lifecycle {
                    instance_id: instance.instance_id.clone(),
                    revision: instance.revision,
                    action: kind,
                });
            }
        }
    });
}

fn editor(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    credential: &CredentialSummary,
    credential_ready: bool,
    action: &mut Option<UiAction>,
) {
    let Some(mut draft) = model.execution.grid.editor.clone() else {
        return;
    };
    let language = model.preferences.language;
    let pending = model.execution.grid.pending;
    let scope_current = draft.credential_id == credential.credential_id;
    let viewport = ui.ctx().content_rect().size();
    let response = egui::Modal::new(egui::Id::new("grid-config-dialog"))
        .frame(
            egui::Frame::new()
                .fill(theme::BG_SECONDARY)
                .stroke(egui::Stroke::new(1.0, theme::DIVIDER))
                .corner_radius(10)
                .inner_margin(24),
        )
        .show(ui.ctx(), |ui| {
            ui.set_width(520.0_f32.min((viewport.x - 64.0).max(280.0)));
            ui.spacing_mut().item_spacing = egui::vec2(10.0, 10.0);
            ui.horizontal(|ui| {
                let title = match draft.mode {
                    EditorMode::Create => tr(language, "新建 Grid 机器人", "New Grid robot"),
                    EditorMode::Update { .. } => {
                        tr(language, "编辑 Grid 配置", "Edit Grid configuration")
                    }
                };
                ui.heading(title);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.add_enabled(!pending, egui::Button::new("×")).clicked() {
                        model.execution.grid.editor = None;
                    }
                });
            });
            ui.label(format!(
                "{} · Binance",
                instance_credential_label(model, &draft.credential_id)
            ));
            ui.separator();
            egui::ScrollArea::vertical()
                .id_salt("grid-config-dialog-scroll")
                .max_height((viewport.y - 280.0).clamp(120.0, 620.0))
                .show(ui, |ui| {
                    ui.add_enabled_ui(!pending && scope_current, |ui| {
                        egui::Grid::new("grid-config-editor")
                            .num_columns(2)
                            .spacing([10.0, 6.0])
                            .show(ui, |ui| {
                                ui.label(tr(language, "交易对", "Symbol"));
                                ui.add_enabled(
                                    matches!(&draft.mode, EditorMode::Create),
                                    egui::TextEdit::singleline(&mut draft.symbol)
                                        .desired_width(120.0),
                                );
                                ui.end_row();
                                ui.label(tr(language, "委托策略", "Order policy"));
                                ui.strong(tr(language, "仅 Maker（固定）", "Maker only (fixed)"));
                                ui.end_row();
                                input(
                                    ui,
                                    tr(language, "单层名义价值", "Order notional"),
                                    &mut draft.order_notional,
                                );
                                input(
                                    ui,
                                    tr(language, "间距率", "Spacing rate"),
                                    &mut draft.spacing_rate,
                                );
                                input(ui, tr(language, "层数", "Levels"), &mut draft.grid_levels);
                                input(
                                    ui,
                                    tr(language, "总名义上限", "Total notional cap"),
                                    &mut draft.max_total_notional,
                                );
                                ui.checkbox(
                                    &mut draft.replenish_enabled,
                                    tr(language, "低库存自动补充", "Inventory replenishment"),
                                );
                                ui.end_row();
                                input(
                                    ui,
                                    tr(language, "最低库存", "Minimum inventory"),
                                    &mut draft.minimum_inventory_notional,
                                );
                                input(
                                    ui,
                                    tr(language, "目标库存", "Target inventory"),
                                    &mut draft.target_inventory_notional,
                                );
                                input(
                                    ui,
                                    tr(language, "单次补充上限", "Replenishment cap"),
                                    &mut draft.max_single_replenishment_notional,
                                );
                                ui.checkbox(
                                    &mut draft.reduce_enabled,
                                    tr(
                                        language,
                                        "库存过多且盈利时减仓",
                                        "Profitable excess reduction",
                                    ),
                                );
                                ui.end_row();
                                input(
                                    ui,
                                    tr(language, "库存权益倍数", "Inventory/equity multiple"),
                                    &mut draft.inventory_equity_multiple,
                                );
                                input(
                                    ui,
                                    tr(language, "最低浮盈率", "Minimum profit rate"),
                                    &mut draft.minimum_unrealized_profit_rate,
                                );
                                input(
                                    ui,
                                    tr(language, "减仓比例", "Reduction fraction"),
                                    &mut draft.reduction_fraction,
                                );
                                input(
                                    ui,
                                    tr(language, "单次减仓上限", "Reduction cap"),
                                    &mut draft.max_single_reduce_notional,
                                );
                                input(
                                    ui,
                                    tr(language, "行情过期 ms", "Market stale ms"),
                                    &mut draft.stale_market_ms,
                                );
                                input(
                                    ui,
                                    tr(language, "私有事实过期 ms", "Private stale ms"),
                                    &mut draft.stale_private_ms,
                                );
                                input(
                                    ui,
                                    tr(language, "收敛超时 ms", "Convergence timeout ms"),
                                    &mut draft.convergence_timeout_ms,
                                );
                                input(
                                    ui,
                                    tr(language, "连续失败阈值", "Failure threshold"),
                                    &mut draft.max_consecutive_failures,
                                );
                            });
                    });
                });
            ui.separator();
            if let Some(error) = &model.execution.grid.mutation_error {
                ui.colored_label(theme::WARNING, error);
            }
            ui.horizontal(|ui| {
                let editor_current = editor_is_current(&draft, &model.execution.grid.instances);
                let save = ui
                    .add_enabled(
                        !pending
                            && scope_current
                            && editor_current
                            && (credential_ready
                                || matches!(&draft.mode, EditorMode::Update { .. })),
                        egui::Button::new(tr(language, "保存配置", "Save")),
                    )
                    .clicked();
                if ui
                    .add_enabled(!pending, egui::Button::new(tr(language, "取消", "Cancel")))
                    .clicked()
                {
                    model.execution.grid.editor = None;
                }
                if pending {
                    ui.spinner();
                    ui.weak(tr(language, "等待 Control 确认", "Waiting for Control"));
                }
                if save {
                    match draft.config() {
                        Ok(config) => {
                            *action = Some(match &draft.mode {
                                EditorMode::Create => UiAction::Create {
                                    symbol: draft.symbol.trim().to_owned(),
                                    config,
                                },
                                EditorMode::Update {
                                    instance_id,
                                    revision,
                                } => UiAction::Update {
                                    instance_id: instance_id.clone(),
                                    revision: *revision,
                                    config,
                                },
                            });
                        }
                        Err(error) => model.execution.grid.mutation_error = Some(error),
                    }
                }
            });
            if !scope_current {
                ui.colored_label(
                    theme::WARNING,
                    tr(
                        language,
                        "账户已切换，请关闭后重新打开配置。",
                        "The account changed; close and reopen the configuration.",
                    ),
                );
            } else if !editor_is_current(&draft, &model.execution.grid.instances) {
                ui.colored_label(
                    theme::WARNING,
                    tr(
                        language,
                        "实例状态或版本已刷新，请取消并重新打开编辑器。",
                        "The instance state or revision changed; cancel and reopen the editor.",
                    ),
                );
            }
        });
    if response.should_close() && !pending && action.is_none() {
        model.execution.grid.editor = None;
    } else if model.execution.grid.editor.is_some() {
        model.execution.grid.editor = Some(draft);
    }
}

fn dispatch(
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    action: UiAction,
) {
    let request_id = model.next_terminal_request_id();
    let mutation = match action {
        UiAction::Create { symbol, config } => {
            let symbol = match symbol.parse() {
                Ok(symbol) => symbol,
                Err(_) => {
                    model.execution.grid.mutation_error =
                        Some("交易对格式无效，应为 BASE/QUOTE".to_owned());
                    return;
                }
            };
            GridMutation::Create(GridInstanceCreateRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id,
                credential_id: credential.credential_id.clone(),
                symbol,
                config,
            })
        }
        UiAction::Update {
            instance_id,
            revision,
            config,
        } => GridMutation::Update(GridConfigUpdateRequest {
            schema_version: GRID_SCHEMA_VERSION,
            request_id,
            instance_id,
            expected_revision: revision,
            config,
        }),
        UiAction::Lifecycle {
            instance_id,
            revision,
            action,
        } => GridMutation::Lifecycle(GridLifecycleRequest {
            schema_version: GRID_SCHEMA_VERSION,
            request_id,
            instance_id,
            expected_revision: revision,
            action,
            risk_confirmed: matches!(
                action,
                GridLifecycleAction::Start | GridLifecycleAction::Resume
            ),
            positions_remain_acknowledged: matches!(
                action,
                GridLifecycleAction::Stop | GridLifecycleAction::Reset
            ),
        }),
    };
    match client.send_grid(mutation) {
        Ok(()) => {
            model.execution.grid.pending = true;
            model.execution.grid.mutation_error = None;
        }
        Err(error) => model.execution.grid.mutation_error = Some(error.to_string()),
    }
}

#[cfg(test)]
fn selected_visible_instance<'a>(
    state: &GridViewState,
    visible: &'a [GridInstanceSummary],
) -> Option<&'a GridInstanceSummary> {
    let id = state.selected_instance_id.as_deref()?;
    visible.iter().find(|instance| instance.instance_id == id)
}

fn config_editable(state: GridInstanceState) -> bool {
    !matches!(
        state,
        GridInstanceState::StartPending | GridInstanceState::StopPending
    )
}

fn editor_is_current(editor: &GridEditor, instances: &[GridInstanceSummary]) -> bool {
    match &editor.mode {
        EditorMode::Create => true,
        EditorMode::Update {
            instance_id,
            revision,
        } => instances.iter().any(|instance| {
            instance.instance_id == *instance_id
                && instance.revision == *revision
                && config_editable(instance.state)
        }),
    }
}

fn lifecycle_enabled(
    state: GridInstanceState,
    action: GridLifecycleAction,
    credential_ready: bool,
    risk_confirmed: bool,
    positions_acknowledged: bool,
) -> bool {
    match action {
        GridLifecycleAction::Start => {
            credential_ready
                && risk_confirmed
                && matches!(state, GridInstanceState::Draft | GridInstanceState::Stopped)
        }
        GridLifecycleAction::Resume => {
            credential_ready && risk_confirmed && state == GridInstanceState::Paused
        }
        GridLifecycleAction::Pause => matches!(
            state,
            GridInstanceState::Running
                | GridInstanceState::StartPending
                | GridInstanceState::Blocked
                | GridInstanceState::ResetRequired
                | GridInstanceState::NeedsAttention
        ),
        GridLifecycleAction::Stop => {
            positions_acknowledged
                && matches!(
                    state,
                    GridInstanceState::Draft
                        | GridInstanceState::Running
                        | GridInstanceState::StartPending
                        | GridInstanceState::Paused
                        | GridInstanceState::Blocked
                        | GridInstanceState::ResetRequired
                        | GridInstanceState::NeedsAttention
                )
        }
        GridLifecycleAction::Reset => {
            positions_acknowledged
                && matches!(
                    state,
                    GridInstanceState::Running
                        | GridInstanceState::Paused
                        | GridInstanceState::Blocked
                        | GridInstanceState::NeedsAttention
                )
        }
    }
}

pub(crate) fn grid_pnl(model: &AppModel, instance: &GridInstanceSummary) -> Option<Decimal> {
    if !model.execution.private_ready(
        Some(&instance.trading_account_id),
        crate::account_center::now_ms(),
    ) {
        return None;
    }
    let projection = model
        .execution
        .private_projection_for(Some(&instance.trading_account_id))?;
    account_symbol_pnl(projection, &instance.symbol)
}

fn account_symbol_pnl(
    projection: &venue_control_protocol::kol::TerminalAccountProjection,
    symbol: &venue_domain::Symbol,
) -> Option<Decimal> {
    let mut result = Decimal::ZERO;
    for position in projection
        .positions
        .iter()
        .filter(|position| &position.symbol == symbol)
    {
        if position.quantity.is_zero() {
            continue;
        }
        let (Some(entry), Some(mark)) = (position.entry_price, position.mark_price) else {
            return None;
        };
        let movement = match position.position_side {
            PositionSide::Long => mark.checked_sub(entry),
            PositionSide::Short => entry.checked_sub(mark),
            PositionSide::Net => continue,
        }?;
        let pnl = movement.checked_mul(position.quantity)?;
        result = result.checked_add(pnl)?;
    }
    Some(result)
}

pub(crate) fn pnl_label(ui: &mut egui::Ui, value: Decimal) -> egui::Response {
    let color = if value.is_sign_negative() {
        theme::SELL
    } else {
        theme::BUY
    };
    ui.colored_label(color, crate::model::format_decimal(value, 4))
}

pub(crate) fn attention_label(ui: &mut egui::Ui, language: Language, code: &str) {
    let label = match code {
        "market_unavailable" | "market_stale" | "market_invalid" => {
            tr(language, "行情事实不可用", "Market facts unavailable")
        }
        "private_unavailable" | "private_missing" | "private_stale" | "private_invalid" => {
            tr(language, "账户事实不可用", "Private facts unavailable")
        }
        "convergence_timeout" | "stop_convergence_timeout" | "reset_convergence_timeout" => {
            tr(language, "收敛超时", "Convergence timed out")
        }
        "manual_reset" => tr(language, "等待异常重置", "Manual reset requested"),
        "failure_threshold" => tr(language, "连续失败过多", "Failure threshold reached"),
        "surface_conflict"
        | "desired_facts_changed"
        | "revision_mismatch"
        | "instrument_changed"
        | "owned_order_invalid"
        | "owned_order_duplicate"
        | "surface_incomplete"
        | "fill_order_conflict"
        | "fill_conflict"
        | "rolling_conflict"
        | "price_cross"
        | "planner_invalid"
        | "facts_invalid"
        | "risk_missing"
        | "risk_invalid"
        | "reduction_below_minimum" => {
            tr(language, "需要检查订单面", "Order surface needs attention")
        }
        _ => tr(language, "需要人工检查", "Needs manual attention"),
    };
    ui.colored_label(theme::WARNING, label);
}

fn input(ui: &mut egui::Ui, label: &str, value: &mut String) {
    ui.label(label);
    ui.add_sized([120.0, 24.0], egui::TextEdit::singleline(value));
    ui.end_row();
}

fn decimal(value: &str, label: &str) -> Result<Decimal, String> {
    Decimal::from_str(value.trim()).map_err(|_| format!("{label}必须是十进制数"))
}

fn integer<T: std::str::FromStr>(value: &str, label: &str) -> Result<T, String> {
    value
        .trim()
        .parse()
        .map_err(|_| format!("{label}必须是整数"))
}

fn number(value: Decimal) -> String {
    value.normalize().to_string()
}

fn tr<'a>(language: Language, chinese: &'a str, english: &'a str) -> &'a str {
    match language {
        Language::SimplifiedChinese => chinese,
        Language::English => english,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(
        instance_id: &str,
        account_id: &str,
        state: GridInstanceState,
        revision: u64,
    ) -> Result<GridInstanceSummary, Box<dyn std::error::Error>> {
        let config =
            GridEditor::create("BTC/USDT", "00000000-0000-4000-8000-000000000010").config()?;
        let attention_code = matches!(
            state,
            GridInstanceState::Blocked
                | GridInstanceState::ResetRequired
                | GridInstanceState::NeedsAttention
        )
        .then(|| "market_stale".to_owned());
        Ok(GridInstanceSummary {
            schema_version: GRID_SCHEMA_VERSION,
            instance_id: instance_id.to_owned(),
            credential_id: "00000000-0000-4000-8000-000000000010".to_owned(),
            trading_account_id: account_id.to_owned(),
            symbol: "BTC/USDT".parse()?,
            state,
            revision,
            config_revision: 1,
            plan_revision: 1,
            config,
            anchor: None,
            desired_digest: None,
            dirty: false,
            convergence_started_ms: None,
            consecutive_failures: 0,
            last_facts_ms: None,
            attention_code,
            created_ms: 100,
            updated_ms: 200_u64.saturating_add(revision),
        })
    }

    #[test]
    fn editor_builds_explicit_inventory_profit_and_reset_policies() {
        let editor = GridEditor::create("BTC/USDT", "00000000-0000-4000-8000-000000000010");
        let config = editor.config();
        assert!(config.is_ok());
        let Ok(config) = config else {
            return;
        };
        assert!(!config.inventory_replenishment.enabled);
        assert!(!config.profit_reduction.enabled);
        assert_eq!(config.reset_policy.max_consecutive_failures, 3);
    }

    #[test]
    fn configuration_dialog_keeps_save_visible_and_fences_stale_or_switched_accounts()
    -> Result<(), Box<dyn std::error::Error>> {
        let credential = CredentialSummary {
            credential_id: "00000000-0000-4000-8000-000000000010".into(),
            label: "Dialog test account".into(),
            venue: venue_control_protocol::VenueId::Binance,
            masked_key: "masked".into(),
            trading_account_id: Some("00000000-0000-4000-8000-000000000101".into()),
            verification: venue_control_protocol::accounts::ApiVerificationState::Verified,
            verified_ms: Some(100),
            expires_ms: None,
            api_reachable: true,
            dual_position: true,
            account_mode: Some("portfolio_margin_um".into()),
            has_exposure: Some(false),
        };
        for case in [
            "create", "update", "pending", "switched", "stale", "invalid",
        ] {
            let context = egui::Context::default();
            crate::theme::apply(&context);
            let mut model = AppModel::new(crate::model::Preferences {
                language: Language::English,
                ..Default::default()
            });
            let instance = summary(
                "00000000-0000-4000-8000-000000000201",
                "00000000-0000-4000-8000-000000000101",
                GridInstanceState::Running,
                1,
            )?;
            let mut draft = if matches!(case, "update" | "stale") {
                GridEditor::update(&instance)
            } else {
                GridEditor::create("BTC/USDT", &credential.credential_id)
            };
            model.execution.grid.instances = vec![instance];
            if case == "switched" {
                draft.credential_id = "another-account".into();
            }
            if case == "stale" {
                model.execution.grid.instances[0].revision = 2;
            }
            if case == "invalid" {
                draft.order_notional = "invalid".into();
            }
            model.execution.grid.pending = case == "pending";
            model.execution.grid.editor = Some(draft);
            let screen = egui::Rect::from_min_size(egui::Pos2::ZERO, egui::vec2(1100.0, 700.0));
            let mut action = None;
            let mut save_rect = None;
            for _ in 0..3 {
                let mut output = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(screen),
                        ..Default::default()
                    },
                    |ui| {
                        editor(ui, &mut model, &credential, true, &mut action);
                    },
                );
                output.textures_delta.clear();
                for shape in output.shapes {
                    if let egui::Shape::Text(text) = shape.shape
                        && text.galley.job.text == "Save"
                    {
                        save_rect = Some(egui::Rect::from_min_size(text.pos, text.galley.size()));
                    }
                }
            }
            assert!(
                context
                    .memory(|m| m.area_rect(egui::Id::new("grid-config-dialog")))
                    .is_some_and(|rect| screen.contains_rect(rect)),
                "{case}: dialog must fit the minimum window"
            );
            let rect = save_rect.ok_or("Save must remain visible below the scrolling form")?;
            assert!(screen.contains_rect(rect));
            for pressed in [true, false] {
                let mut output = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(screen),
                        events: vec![egui::Event::PointerButton {
                            pos: rect.center(),
                            button: egui::PointerButton::Primary,
                            pressed,
                            modifiers: egui::Modifiers::NONE,
                        }],
                        ..Default::default()
                    },
                    |ui| editor(ui, &mut model, &credential, true, &mut action),
                );
                output.textures_delta.clear();
            }
            match case {
                "create" => assert!(matches!(action, Some(UiAction::Create { .. }))),
                "update" => assert!(matches!(action, Some(UiAction::Update { revision: 1, .. }))),
                "invalid" => {
                    assert!(action.is_none());
                    assert!(model.execution.grid.mutation_error.is_some());
                }
                _ => assert!(action.is_none(), "{case}: must not submit"),
            }
            assert!(
                model.execution.grid.editor.is_some(),
                "retain draft until a confirmed result"
            );
        }
        Ok(())
    }

    #[test]
    fn lifecycle_flags_match_protocol() {
        for action in [
            GridLifecycleAction::Start,
            GridLifecycleAction::Pause,
            GridLifecycleAction::Resume,
            GridLifecycleAction::Reset,
            GridLifecycleAction::Stop,
        ] {
            let request = GridLifecycleRequest {
                schema_version: GRID_SCHEMA_VERSION,
                request_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                instance_id: "00000000-0000-4000-8000-000000000002".to_owned(),
                expected_revision: 1,
                action,
                risk_confirmed: matches!(
                    action,
                    GridLifecycleAction::Start | GridLifecycleAction::Resume
                ),
                positions_remain_acknowledged: matches!(
                    action,
                    GridLifecycleAction::Stop | GridLifecycleAction::Reset
                ),
            };
            assert_eq!(request.validate(), Ok(()));
        }
    }

    #[test]
    fn selected_instance_is_scoped_to_the_visible_account() -> Result<(), Box<dyn std::error::Error>>
    {
        let account_a = "00000000-0000-4000-8000-000000000101";
        let account_b = "00000000-0000-4000-8000-000000000102";
        let selected = "00000000-0000-4000-8000-000000000201";
        let mut state = GridViewState {
            selected_instance_id: Some(selected.to_owned()),
            ..GridViewState::default()
        };
        state.instances = vec![
            summary(selected, account_a, GridInstanceState::Running, 1)?,
            summary(
                "00000000-0000-4000-8000-000000000202",
                account_b,
                GridInstanceState::Running,
                1,
            )?,
        ];
        let visible_b = state
            .instances
            .iter()
            .filter(|instance| instance.trading_account_id == account_b)
            .cloned()
            .collect::<Vec<_>>();
        assert!(selected_visible_instance(&state, &visible_b).is_none());
        Ok(())
    }

    #[test]
    fn instance_account_uses_its_credential_remark() {
        let mut model = AppModel::new(crate::model::Preferences::default());
        model.account_overview = Some(venue_control_protocol::accounts::AccountOverview {
            user: venue_control_protocol::accounts::UserSummary {
                user_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                username: "alice".to_owned(),
            },
            credentials: vec![CredentialSummary {
                credential_id: "00000000-0000-4000-8000-000000000010".to_owned(),
                label: "主策略账户".to_owned(),
                venue: venue_control_protocol::VenueId::Binance,
                masked_key: "••••1234".to_owned(),
                trading_account_id: Some("00000000-0000-4000-8000-000000000101".to_owned()),
                verification: venue_control_protocol::accounts::ApiVerificationState::Verified,
                verified_ms: Some(100),
                expires_ms: None,
                api_reachable: true,
                dual_position: true,
                account_mode: Some("portfolio_margin_um".to_owned()),
                has_exposure: Some(false),
            }],
            selected_credential_id: None,
        });
        assert_eq!(
            instance_credential_label(&model, "00000000-0000-4000-8000-000000000010"),
            "主策略账户"
        );
        assert_eq!(
            instance_credential_label(&model, "00000000-0000-4000-8000-000000000099"),
            "凭证已移除"
        );
    }

    #[test]
    fn lifecycle_buttons_match_the_durable_state_machine() {
        assert!(lifecycle_enabled(
            GridInstanceState::Draft,
            GridLifecycleAction::Start,
            true,
            true,
            false,
        ));
        assert!(!lifecycle_enabled(
            GridInstanceState::Draft,
            GridLifecycleAction::Start,
            false,
            true,
            false,
        ));
        assert!(lifecycle_enabled(
            GridInstanceState::StartPending,
            GridLifecycleAction::Pause,
            false,
            false,
            false,
        ));
        assert!(lifecycle_enabled(
            GridInstanceState::Draft,
            GridLifecycleAction::Stop,
            false,
            false,
            true,
        ));
        assert!(lifecycle_enabled(
            GridInstanceState::NeedsAttention,
            GridLifecycleAction::Reset,
            false,
            false,
            true,
        ));
        assert!(!lifecycle_enabled(
            GridInstanceState::ResetRequired,
            GridLifecycleAction::Reset,
            true,
            false,
            true,
        ));
        assert!(!config_editable(GridInstanceState::StartPending));
        assert!(!config_editable(GridInstanceState::StopPending));
        assert!(config_editable(GridInstanceState::Running));
    }

    #[test]
    fn refresh_success_does_not_erase_a_mutation_diagnostic() {
        let mut state = GridViewState::default();
        state.mutation_unavailable("state_or_revision_conflict".to_owned());
        state.list_unavailable("refresh_failed".to_owned());
        state.apply_instances(Vec::new());
        assert_eq!(
            state.mutation_error.as_deref(),
            Some("state_or_revision_conflict")
        );
        assert!(state.refresh_error.is_none());
    }

    #[test]
    fn higher_durable_revision_wins_even_if_wall_clock_moves_back()
    -> Result<(), Box<dyn std::error::Error>> {
        let instance_id = "00000000-0000-4000-8000-000000000201";
        let account_id = "00000000-0000-4000-8000-000000000101";
        let mut current = summary(instance_id, account_id, GridInstanceState::Draft, 1)?;
        current.updated_ms = 1_000;
        let mut incoming = summary(instance_id, account_id, GridInstanceState::Running, 2)?;
        incoming.updated_ms = 500;
        let mut state = GridViewState {
            instances: vec![current],
            ..GridViewState::default()
        };
        state.apply_instances(vec![incoming]);
        assert_eq!(state.instances[0].revision, 2);
        assert_eq!(state.instances[0].state, GridInstanceState::Running);
        Ok(())
    }

    #[test]
    fn account_symbol_pnl_combines_both_hedge_legs() -> Result<(), Box<dyn std::error::Error>> {
        let symbol: venue_domain::Symbol = "BTC/USDT".parse()?;
        let position =
            |position_side, entry, mark, quantity| venue_control_protocol::kol::TerminalPosition {
                symbol: symbol.clone(),
                position_side,
                quantity,
                entry_price: Some(entry),
                mark_price: Some(mark),
            };
        let projection = venue_control_protocol::kol::TerminalAccountProjection {
            schema_version: venue_control_protocol::kol::TERMINAL_PROJECTION_SCHEMA_VERSION,
            credential_id: "00000000-0000-4000-8000-000000000010".to_owned(),
            trading_account_id: "00000000-0000-4000-8000-000000000101".to_owned(),
            observed_ms: 100,
            persisted_ms: 100,
            private_generation: 1,
            position_mode: venue_control_protocol::kol::TerminalPositionMode::Hedge,
            positions: vec![
                position(
                    PositionSide::Long,
                    Decimal::new(100, 0),
                    Decimal::new(103, 0),
                    Decimal::new(2, 0),
                ),
                position(
                    PositionSide::Short,
                    Decimal::new(105, 0),
                    Decimal::new(103, 0),
                    Decimal::new(1, 0),
                ),
            ],
            position_history: Vec::new(),
            open_orders: Vec::new(),
            fills: Vec::new(),
            assets: Vec::new(),
        };
        assert_eq!(
            account_symbol_pnl(&projection, &symbol),
            Some(Decimal::new(8, 0))
        );
        Ok(())
    }
}
