use std::collections::BTreeMap;

use crate::{
    client::{
        ControlClient, GridMutation, SupportMartingaleConfig, SupportMartingaleCreateRequest,
        SupportMartingaleLifecycleRequest, SupportMartingaleListItem,
        SupportMartingalePreflightRequest,
    },
    model::AppModel,
    theme,
};
use eframe::egui;
use venue_control_protocol::{
    accounts::CredentialSummary,
    support_martingale::{
        MartingaleEntryMode, MartingaleStopLoss, MartingaleSymbolParameters,
        SupportMartingaleAction, SupportMartingaleHealth, SupportMartingaleLifecycle,
        SupportMartingalePreflightCheckCode as CheckCode,
        SupportMartingalePreflightCheckStatus as CheckStatus, SupportMartingalePreflightResponse,
    },
};
use venue_gateway_api::VenueId;

const PREFLIGHT_DISPLAY_TTL_MS: u64 = 60_000;

#[derive(Clone, Debug)]
struct Editor {
    credential_id: String,
    trading_account_id: String,
    symbols: String,
    total_budget: String,
    first_order_notional: String,
    max_entries: String,
    size_multiplier: String,
    target_profit_rate: String,
    minimum_profit_quote: String,
    max_active_positions: String,
    entry_mode: MartingaleEntryMode,
    symbol_parameters: BTreeMap<String, SymbolEditor>,
}

#[derive(Clone, Debug)]
struct SymbolEditor {
    entry_price: String,
    add_drop_percent: String,
    stop_enabled: bool,
    stop_fixed: bool,
    stop_value: String,
}
impl Default for SymbolEditor {
    fn default() -> Self {
        Self {
            entry_price: String::new(),
            add_drop_percent: "2".into(),
            stop_enabled: false,
            stop_fixed: false,
            stop_value: "10".into(),
        }
    }
}

impl Editor {
    fn create(credential: &CredentialSummary, account_id: &str) -> Self {
        Self {
            credential_id: credential.credential_id.clone(),
            trading_account_id: account_id.to_owned(),
            symbols: "SOL/USDT,DOGE/USDT".into(),
            total_budget: "30".into(),
            first_order_notional: "5".into(),
            max_entries: "4".into(),
            size_multiplier: "1.0".into(),
            target_profit_rate: "0.005".into(),
            minimum_profit_quote: "0.02".into(),
            max_active_positions: "2".into(),
            entry_mode: MartingaleEntryMode::Support,
            symbol_parameters: BTreeMap::new(),
        }
    }

    fn is_current(&self, credential: &CredentialSummary, account_id: &str) -> bool {
        self.credential_id == credential.credential_id && self.trading_account_id == account_id
    }

    fn config(&self) -> Result<SupportMartingaleConfig, String> {
        let symbols = self
            .symbols
            .split(',')
            .map(|symbol| symbol.trim().to_ascii_uppercase())
            .filter(|symbol| !symbol.is_empty())
            .map(|symbol| symbol.parse())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "交易对必须是 BASE/QUOTE".to_owned())?;
        let decimal = |value: &str| {
            value
                .trim()
                .parse()
                .map_err(|_| "金额和比率必须是十进制数".to_owned())
        };
        let config = SupportMartingaleConfig {
            entry_mode: self.entry_mode,
            symbol_parameters: symbols
                .iter()
                .map(|symbol: &venue_domain::Symbol| {
                    let value = self
                        .symbol_parameters
                        .get(&symbol.to_string())
                        .cloned()
                        .unwrap_or_default();
                    let entry_price = if self.entry_mode == MartingaleEntryMode::FixedPrice {
                        Some(decimal(&value.entry_price)?)
                    } else {
                        None
                    };
                    let stop_loss = if !value.stop_enabled {
                        None
                    } else if value.stop_fixed {
                        Some(MartingaleStopLoss::FixedPrice {
                            price: decimal(&value.stop_value)?,
                        })
                    } else {
                        Some(MartingaleStopLoss::AveragePricePercent {
                            rate: decimal(&value.stop_value)? / rust_decimal::Decimal::from(100),
                        })
                    };
                    Ok(MartingaleSymbolParameters {
                        symbol: symbol.clone(),
                        entry_price,
                        add_drop_rate: decimal(&value.add_drop_percent)?
                            / rust_decimal::Decimal::from(100),
                        stop_loss,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            reference_venue: VenueId::Binance,
            execution_venue: VenueId::Bybit,
            symbols,
            total_budget: decimal(&self.total_budget)?,
            first_order_notional: decimal(&self.first_order_notional)?,
            max_entries: self
                .max_entries
                .trim()
                .parse()
                .map_err(|_| "最大层数必须是整数".to_owned())?,
            size_multiplier: decimal(&self.size_multiplier)?,
            target_profit_rate: decimal(&self.target_profit_rate)?,
            minimum_profit_quote: decimal(&self.minimum_profit_quote)?,
            max_active_positions: self
                .max_active_positions
                .trim()
                .parse()
                .map_err(|_| "最大活动币数必须是整数".to_owned())?,
        };
        config.validate().map_err(|_| {
            "配置无效：需 1–30 个同报价交易对、1–10 层、1–10 个活动币，预算和名义金额为正数"
                .to_owned()
        })?;
        Ok(config)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingOperation {
    Save,
    Preflight,
    Lifecycle(SupportMartingaleAction),
}

#[derive(Debug, Default)]
pub(crate) struct SupportMartingaleViewState {
    pub instances: Vec<SupportMartingaleListItem>,
    pub fresh: bool,
    load_error: Option<String>,
    operation_error: Option<String>,
    notice: Option<String>,
    pending: Option<PendingOperation>,
    editor: Option<Editor>,
    preflights: BTreeMap<String, SupportMartingalePreflightResponse>,
    start_confirmation: Option<String>,
    selected_instance_id: Option<String>,
}

impl SupportMartingaleViewState {
    pub fn apply_instances(&mut self, instances: Vec<SupportMartingaleListItem>) {
        self.instances = instances;
        self.fresh = true;
        self.load_error = None;
        self.preflights.retain(|instance_id, result| {
            self.instances.iter().any(|instance| {
                instance.instance_id == *instance_id && instance.revision == result.revision
            })
        });
    }

    pub fn apply_summary(&mut self, summary: SupportMartingaleListItem) {
        let operation = self.pending.take();
        self.preflights.remove(&summary.instance_id);
        if let Some(old) = self
            .instances
            .iter_mut()
            .find(|item| item.instance_id == summary.instance_id)
        {
            *old = summary.clone();
        } else {
            self.instances.push(summary);
        }
        self.fresh = true;
        self.operation_error = None;
        self.notice = Some(match operation {
            Some(PendingOperation::Save) => {
                self.editor = None;
                "配置已保存为停止态；尚未预检或启动".to_owned()
            }
            Some(PendingOperation::Lifecycle(action)) => format!(
                "生命周期请求已确认：{}；这不代表订单成交",
                action_label(action)
            ),
            _ => "实例状态已刷新".to_owned(),
        });
    }

    pub fn apply_preflight(&mut self, result: SupportMartingalePreflightResponse) {
        self.pending = None;
        self.operation_error = None;
        self.notice = Some(if result.ready {
            "只读预检通过；启动时服务端仍会重新读取签名事实".into()
        } else {
            "预检未通过；未启动，也未发送交易请求".into()
        });
        self.preflights.insert(result.instance_id.clone(), result);
    }

    pub fn unavailable(&mut self, message: String, mutation: bool) {
        self.fresh = false;
        if mutation {
            self.pending = None;
            self.operation_error = Some(message);
        } else {
            self.load_error = Some(message);
        }
    }

    fn is_pending(&self) -> bool {
        self.pending.is_some()
    }
}

pub(crate) fn can_create(model: &AppModel, credential: &CredentialSummary) -> bool {
    credential.venue == VenueId::Bybit
        && credential.trading_account_id.is_some()
        && credential.selectable(crate::account_center::now_ms())
        && !model.execution.support_martingale.is_pending()
}

pub(crate) fn open_create(model: &mut AppModel, credential: &CredentialSummary) {
    if let Some(account) = &credential.trading_account_id {
        model.execution.support_martingale.editor = Some(Editor::create(credential, account));
        model.execution.support_martingale.selected_instance_id = None;
    }
}

pub(crate) fn clear_selection(model: &mut AppModel) {
    model.execution.support_martingale.selected_instance_id = None;
}

pub(crate) fn list_row(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    credential: &CredentialSummary,
    item: &SupportMartingaleListItem,
) {
    let selected = model
        .execution
        .support_martingale
        .selected_instance_id
        .as_deref()
        == Some(&item.instance_id);
    if ui
        .selectable_label(
            selected,
            format!(
                "马丁 · {}",
                item.instance_id.chars().take(8).collect::<String>()
            ),
        )
        .clicked()
    {
        crate::grid_view::clear_selection(model);
        crate::leader_bot_view::clear_selection(model);
        model.execution.support_martingale.selected_instance_id = Some(item.instance_id.clone());
    }
    ui.label("马丁做多");
    ui.label(&credential.label);
    ui.label(
        item.config
            .symbols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", "),
    );
    ui.label(lifecycle_label(item.lifecycle));
    ui.label(format!("预留 {}", item.reserved_budget));
    ui.label(item.revision.to_string());
    ui.label(health_label(item.health)).on_hover_text(
        item.health_reason
            .as_deref()
            .map(health_reason_label)
            .unwrap_or(""),
    );
    if ui.button("管理").clicked() {
        crate::grid_view::clear_selection(model);
        crate::leader_bot_view::clear_selection(model);
        model.execution.support_martingale.selected_instance_id = Some(item.instance_id.clone());
    }
}

pub(crate) fn show(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    account_id: &str,
) {
    let selected = model
        .execution
        .support_martingale
        .selected_instance_id
        .clone();
    if let Some(item) = model
        .execution
        .support_martingale
        .instances
        .iter()
        .find(|item| {
            Some(&item.instance_id) == selected.as_ref() && item.trading_account_id == account_id
        })
        .cloned()
    {
        let mut open = true;
        egui::Window::new("马丁做多管理")
            .open(&mut open)
            .collapsible(false)
            .resizable(true)
            .vscroll(true)
            .show(ui.ctx(), |ui| {
                ui.label(format!(
                    "账户：{} · {}",
                    credential.label,
                    item.execution_venue.as_str()
                ));
                ui.label(format!(
                    "交易对：{}",
                    item.config
                        .symbols
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                ui.label(format!(
                    "总预算 {} · 首仓 {} · 层数 {} · 倍率 {}",
                    item.config.total_budget,
                    item.config.first_order_notional,
                    item.config.max_entries,
                    item.config.size_multiplier
                ));
                ui.label(format!(
                    "止盈率 {} · 最低止盈 {}",
                    item.config.target_profit_rate, item.config.minimum_profit_quote
                ));
                configuration_summary(ui, &item.config);
                ui.separator();
                ui.vertical(|ui| row(ui, model, client, &item));
                for message in [
                    model
                        .execution
                        .support_martingale
                        .operation_error
                        .as_deref(),
                    model.execution.support_martingale.load_error.as_deref(),
                ]
                .into_iter()
                .flatten()
                {
                    ui.colored_label(theme::WARNING, message);
                }
                if let Some(notice) = &model.execution.support_martingale.notice {
                    ui.weak(notice);
                }
            });
        if !open {
            model.execution.support_martingale.selected_instance_id = None;
        }
    }
    start_confirmation(ui, model, client);
    editor(ui, model, client, credential, account_id);
}

fn configuration_summary(ui: &mut egui::Ui, config: &SupportMartingaleConfig) {
    ui.label(match config.entry_mode {
        MartingaleEntryMode::Support => "入场：支撑位做多",
        MartingaleEntryMode::FixedPrice => "入场：固定价格做多",
    });
    for parameters in &config.symbol_parameters {
        if let Some(price) = parameters.entry_price {
            ui.label(format!(
                "{} 首仓价 {} · 补仓跌幅 {}%",
                parameters.symbol,
                price,
                parameters.add_drop_rate * rust_decimal::Decimal::from(100)
            ));
        }
        let stop = match parameters.stop_loss {
            None => "止损未启用".to_owned(),
            Some(MartingaleStopLoss::FixedPrice { price }) => format!("固定止损价 {price}"),
            Some(MartingaleStopLoss::AveragePricePercent { rate }) => {
                format!("均价下跌 {}% 止损", rate * rust_decimal::Decimal::from(100))
            }
        };
        ui.label(format!("{} · {stop}", parameters.symbol));
    }
}

fn row(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    item: &SupportMartingaleListItem,
) {
    ui.label(item.execution_venue.as_str());
    ui.label(format!(
        "{} · {}",
        lifecycle_label(item.lifecycle),
        health_label(item.health)
    ))
    .on_hover_text(
        item.health_reason
            .as_deref()
            .map(health_reason_label)
            .unwrap_or("生命周期与健康状态相互独立"),
    );
    ui.label(format!(
        "{}/{}",
        item.symbol_count, item.config.max_active_positions
    ));
    ui.label(item.reserved_budget.to_string());
    ui.vertical(|ui| {
        ui.horizontal_wrapped(|ui| {
            let stopped = item.lifecycle == SupportMartingaleLifecycle::Stopped;
            if ui
                .add_enabled(
                    stopped && !model.execution.support_martingale.is_pending(),
                    egui::Button::new("启动预检"),
                )
                .clicked()
            {
                dispatch(
                    model,
                    client,
                    GridMutation::SupportMartingalePreflight(SupportMartingalePreflightRequest {
                        schema_version: crate::client::schema_version(),
                        instance_id: item.instance_id.clone(),
                        expected_revision: item.revision,
                    }),
                    PendingOperation::Preflight,
                );
            }
            let preflight_ready = model
                .execution
                .support_martingale
                .preflights
                .get(&item.instance_id)
                .is_some_and(|result| preflight_is_current(result, item.revision));
            if ui
                .add_enabled(
                    stopped && preflight_ready && !model.execution.support_martingale.is_pending(),
                    egui::Button::new("启动"),
                )
                .on_disabled_hover_text("先完成当前 revision 的只读预检")
                .clicked()
            {
                model.execution.support_martingale.start_confirmation =
                    Some(item.instance_id.clone());
            }
            for (label, action, enabled) in [
                (
                    "暂停首仓",
                    SupportMartingaleAction::PauseEntry,
                    item.lifecycle == SupportMartingaleLifecycle::Running,
                ),
                (
                    "暂停增险",
                    SupportMartingaleAction::PauseIncrease,
                    matches!(
                        item.lifecycle,
                        SupportMartingaleLifecycle::Running
                            | SupportMartingaleLifecycle::EntryPaused
                    ),
                ),
                (
                    "停止等待止盈",
                    SupportMartingaleAction::Drain,
                    item.lifecycle != SupportMartingaleLifecycle::Stopped,
                ),
                (
                    "恢复",
                    SupportMartingaleAction::Resume,
                    matches!(
                        item.lifecycle,
                        SupportMartingaleLifecycle::EntryPaused
                            | SupportMartingaleLifecycle::IncreasePaused
                    ),
                ),
            ] {
                if ui
                    .add_enabled(
                        enabled && !model.execution.support_martingale.is_pending(),
                        egui::Button::new(label),
                    )
                    .clicked()
                {
                    lifecycle(model, client, item, action);
                }
            }
        });
        if let Some(result) = model
            .execution
            .support_martingale
            .preflights
            .get(&item.instance_id)
        {
            show_preflight(ui, result, item.revision);
        }
    });
}

fn start_confirmation(ui: &mut egui::Ui, model: &mut AppModel, client: &ControlClient) {
    let Some(instance_id) = model
        .execution
        .support_martingale
        .start_confirmation
        .clone()
    else {
        return;
    };
    let item = model
        .execution
        .support_martingale
        .instances
        .iter()
        .find(|item| item.instance_id == instance_id)
        .cloned();
    let mut close = item.is_none();
    let mut confirm = false;
    if let Some(item) = item.as_ref() {
        egui::Window::new("确认启动马丁做多")
            .collapsible(false)
            .resizable(false)
            .show(ui.ctx(), |ui| {
                ui.label(format!("账户：{}", item.trading_account_id));
                ui.label(format!("交易对：{}", item.config.symbols.len()));
                ui.label(format!("总名义预算：{}", item.config.total_budget));
                ui.label(format!(
                    "首仓 {}，最多 {} 层，补仓倍率 {}",
                    item.config.first_order_notional,
                    item.config.max_entries,
                    item.config.size_multiplier
                ));
                configuration_summary(ui, &item.config);
                ui.colored_label(
                    theme::WARNING,
                    "下跌中已有仓位仍可能补仓；止损仅对已启用的币种生效，由服务器监控，不限制持仓时间。",
                );
                ui.weak("启动只允许未来新信号。服务端会再次核对 LIVE、账户排他和签名事实。");
                ui.horizontal(|ui| {
                    if ui.button("取消").clicked() {
                        close = true;
                    }
                    if ui.button("确认启动").clicked() {
                        confirm = true;
                    }
                });
            });
        if confirm {
            lifecycle(model, client, item, SupportMartingaleAction::Start);
            close = true;
        }
    }
    if close {
        model.execution.support_martingale.start_confirmation = None;
    }
}

fn lifecycle(
    model: &mut AppModel,
    client: &ControlClient,
    item: &SupportMartingaleListItem,
    action: SupportMartingaleAction,
) {
    let request = SupportMartingaleLifecycleRequest {
        schema_version: crate::client::schema_version(),
        request_id: format!(
            "desktop-{}-{}",
            item.instance_id,
            crate::account_center::now_ms()
        ),
        instance_id: item.instance_id.clone(),
        expected_revision: item.revision,
        action,
    };
    dispatch(
        model,
        client,
        GridMutation::SupportMartingaleLifecycle(request),
        PendingOperation::Lifecycle(action),
    );
}

fn editor(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    account_id: &str,
) {
    let Some(mut draft) = model.execution.support_martingale.editor.clone() else {
        return;
    };
    let pending = model.execution.support_martingale.is_pending();
    let current = draft.is_current(credential, account_id);
    let mut close = false;
    let mut submit = false;
    egui::Window::new("新建机器人 · 马丁做多")
        .default_width((ui.ctx().content_rect().width() - 64.0).clamp(240.0, 460.0))
        .collapsible(false)
        .resizable(true)
        .show(ui.ctx(), |ui| {
            egui::ScrollArea::vertical()
                .max_height((ui.ctx().content_rect().height() - 160.0).clamp(120.0, 420.0))
                .show(ui, |ui| {
                    ui.label("参考行情固定为 Binance USD-M；当前执行所为 Bybit LIVE。");
                    for (label, value) in [
                        ("交易对（逗号分隔）", &mut draft.symbols),
                        ("总名义预算", &mut draft.total_budget),
                        ("首仓名义", &mut draft.first_order_notional),
                        ("最大层数", &mut draft.max_entries),
                        ("补仓倍率", &mut draft.size_multiplier),
                        ("止盈率", &mut draft.target_profit_rate),
                        ("最低止盈金额", &mut draft.minimum_profit_quote),
                        ("最大活动币数", &mut draft.max_active_positions),
                    ] {
                        ui.label(label);
                        ui.text_edit_singleline(value);
                    }
                    ui.horizontal(|ui| {
                        ui.label("入场方式");
                        ui.selectable_value(&mut draft.entry_mode, MartingaleEntryMode::Support, "支撑位做多");
                        ui.selectable_value(&mut draft.entry_mode, MartingaleEntryMode::FixedPrice, "固定价格做多");
                    });
                    for symbol in draft.symbols.split(',').map(|s| s.trim().to_ascii_uppercase()).filter(|s| !s.is_empty()) {
                        let parameters = draft.symbol_parameters.entry(symbol.clone()).or_default();
                        ui.push_id(&symbol, |ui| ui.group(|ui| {
                            ui.strong(&symbol);
                            if draft.entry_mode == MartingaleEntryMode::FixedPrice {
                                ui.label("首仓触发价（现价不高于此价时市价入场）");
                                ui.text_edit_singleline(&mut parameters.entry_price);
                                ui.label("每层相对上一层触发价下跌（%）");
                                ui.text_edit_singleline(&mut parameters.add_drop_percent);
                            }
                            ui.checkbox(&mut parameters.stop_enabled, "启用止损");
                            if parameters.stop_enabled {
                                ui.horizontal(|ui| {
                                    ui.selectable_value(&mut parameters.stop_fixed, false, "均价下跌百分比");
                                    ui.selectable_value(&mut parameters.stop_fixed, true, "固定止损价格");
                                });
                                ui.label(if parameters.stop_fixed { "止损价格" } else { "均价下跌（%）" });
                                ui.text_edit_singleline(&mut parameters.stop_value);
                            }
                        }));
                    }
                    ui.weak("保存只建立停止态实例，不执行预检、不启动、不下单。");
                    ui.colored_label(
                        theme::WARNING,
                        "止损由服务器按签名标记价监控，触发后先撤止盈再市价减仓；可能滑点，离线期间无法触发。未启用止损的币种仍可能持续持仓。",
                    );
                    if !current {
                        ui.colored_label(
                            theme::WARNING,
                            "账户或凭证已切换，请取消后重新创建配置。",
                        );
                    }
                });
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(!pending, egui::Button::new("取消"))
                    .clicked()
                {
                    close = true;
                }
                if ui
                    .add_enabled(current && !pending, egui::Button::new("保存（不启动）"))
                    .clicked()
                {
                    submit = true;
                }
                if pending {
                    ui.spinner();
                    ui.weak("等待 Control 确认保存结果");
                }
            });
        });
    if submit {
        match draft.config() {
            Ok(config) => {
                let request = SupportMartingaleCreateRequest {
                    schema_version: crate::client::schema_version(),
                    request_id: format!("desktop-create-{}", crate::account_center::now_ms()),
                    credential_id: draft.credential_id.clone(),
                    config,
                };
                dispatch(
                    model,
                    client,
                    GridMutation::SupportMartingaleCreate(request),
                    PendingOperation::Save,
                );
            }
            Err(error) => model.execution.support_martingale.operation_error = Some(error),
        }
    }
    model.execution.support_martingale.editor = if close { None } else { Some(draft) };
}

fn dispatch(
    model: &mut AppModel,
    client: &ControlClient,
    mutation: GridMutation,
    operation: PendingOperation,
) {
    if client.send_grid(mutation).is_ok() {
        model.execution.support_martingale.pending = Some(operation);
        model.execution.support_martingale.operation_error = None;
        model.execution.support_martingale.notice = None;
    } else {
        model.execution.support_martingale.operation_error = Some("请求未进入发送队列".into());
    }
}

fn show_preflight(ui: &mut egui::Ui, result: &SupportMartingalePreflightResponse, revision: u64) {
    let current = preflight_is_current(result, revision);
    let color = if current { theme::BUY } else { theme::WARNING };
    ui.colored_label(
        color,
        if current {
            "预检通过（启动时重检）"
        } else if result.ready {
            "预检已过期或 revision 已变化，请重新预检"
        } else {
            "预检未通过"
        },
    );
    for check in &result.checks {
        if check.status != CheckStatus::Passed {
            ui.weak(format!(
                "{}：{}",
                check_label(check.code),
                match check.status {
                    CheckStatus::Passed => "通过",
                    CheckStatus::Failed => "失败",
                    CheckStatus::Skipped => "因前置失败未执行",
                }
            ));
        }
    }
}

fn preflight_is_current(result: &SupportMartingalePreflightResponse, revision: u64) -> bool {
    let now_ms = crate::account_center::now_ms();
    result.ready
        && result.revision == revision
        && result.checked_at_ms <= now_ms
        && now_ms.saturating_sub(result.checked_at_ms) <= PREFLIGHT_DISPLAY_TTL_MS
}

fn lifecycle_label(value: SupportMartingaleLifecycle) -> &'static str {
    match value {
        SupportMartingaleLifecycle::Running => "运行中",
        SupportMartingaleLifecycle::EntryPaused => "暂停首仓（仍可补仓）",
        SupportMartingaleLifecycle::IncreasePaused => "暂停增险",
        SupportMartingaleLifecycle::Draining => "停止中（等待止盈）",
        SupportMartingaleLifecycle::Stopped => "已停止",
    }
}

fn health_label(value: SupportMartingaleHealth) -> &'static str {
    match value {
        SupportMartingaleHealth::Healthy => "健康",
        SupportMartingaleHealth::NeedsAttention => "需处理",
        SupportMartingaleHealth::Unavailable => "事实不可用",
    }
}

fn action_label(value: SupportMartingaleAction) -> &'static str {
    match value {
        SupportMartingaleAction::Start => "启动",
        SupportMartingaleAction::PauseEntry => "暂停首仓",
        SupportMartingaleAction::PauseIncrease => "暂停增险",
        SupportMartingaleAction::Drain => "停止并等待止盈",
        SupportMartingaleAction::Resume => "恢复",
    }
}

fn check_label(value: CheckCode) -> &'static str {
    match value {
        CheckCode::LiveOnly => "LIVE 与账户身份",
        CheckCode::CredentialVerified => "当前凭证验证",
        CheckCode::AccountExclusive => "账户排他与未决命令",
        CheckCode::SignedFactsFresh => "签名事实新鲜度",
        CheckCode::PositionMode => "持仓模式",
        CheckCode::NoPositions => "账户空仓",
        CheckCode::NoOpenOrders => "无普通/条件挂单",
        CheckCode::NoUnknownResults => "无未知结果",
        CheckCode::BudgetAvailable => "可用保证金覆盖预算",
    }
}

fn health_reason_label(value: &str) -> &str {
    match value {
        "external_open_order" => "检测到实例外开放订单，已停止增险",
        "unexpected_position" => "持仓方向或腿位与策略不符",
        "stop_loss_rejected" => "止损请求被拒绝；已暂停增险，需人工处理",
        "external_position" => "检测到未归属本实例的持仓",
        "symbol_state_missing" => "逐币持久状态缺失",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::accounts::ApiVerificationState;

    fn credential() -> CredentialSummary {
        CredentialSummary {
            credential_id: "credential".into(),
            label: "bybit".into(),
            venue: VenueId::Bybit,
            masked_key: "••••".into(),
            trading_account_id: Some("account".into()),
            verification: ApiVerificationState::Verified,
            verified_ms: Some(1),
            expires_ms: None,
            api_reachable: true,
            dual_position: true,
            account_mode: Some("hedge".into()),
            has_exposure: Some(false),
            equity: None,
            available_margin: None,
            balance_observed_ms: None,
        }
    }

    #[test]
    fn default_config_accepts_bound_two_symbol_account() {
        assert!(Editor::create(&credential(), "account").config().is_ok());
    }

    #[test]
    fn fixed_editor_serializes_each_symbols_entry_and_stop()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut editor = Editor::create(&credential(), "account");
        editor.symbols = "SOL/USDT".into();
        editor.entry_mode = MartingaleEntryMode::FixedPrice;
        editor.max_active_positions = "1".into();
        editor.symbol_parameters.insert(
            "SOL/USDT".into(),
            SymbolEditor {
                entry_price: "100".into(),
                add_drop_percent: "2.5".into(),
                stop_enabled: true,
                stop_fixed: false,
                stop_value: "10".into(),
            },
        );
        let config = editor.config()?;
        let parameters = &config.symbol_parameters[0];
        assert_eq!(
            parameters.entry_price,
            Some(rust_decimal::Decimal::from(100))
        );
        assert_eq!(parameters.add_drop_rate, rust_decimal::Decimal::new(25, 3));
        assert_eq!(
            parameters
                .stop_loss
                .and_then(|sl| sl.trigger_price(rust_decimal::Decimal::from(80))),
            Some(rust_decimal::Decimal::from(72))
        );
        Ok(())
    }

    #[test]
    fn editor_rejects_account_switch_and_mixed_quotes() {
        let mut editor = Editor::create(&credential(), "account");
        assert!(!editor.is_current(&credential(), "other"));
        editor.symbols = "SOL/USDT,ETH/USDC".into();
        assert!(editor.config().is_err());
    }

    #[test]
    fn load_refresh_does_not_erase_operation_failure() {
        let mut state = SupportMartingaleViewState::default();
        state.unavailable("保存失败".into(), true);
        state.apply_instances(Vec::new());
        assert_eq!(state.operation_error.as_deref(), Some("保存失败"));
        assert!(state.fresh);
    }
}
