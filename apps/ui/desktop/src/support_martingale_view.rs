use crate::{
    client::{
        ControlClient, GridMutation, SupportMartingaleConfig, SupportMartingaleCreateRequest,
        SupportMartingaleLifecycleRequest, SupportMartingaleListItem,
    },
    model::AppModel,
    theme,
};
use eframe::egui;
use venue_control_protocol::{
    accounts::CredentialSummary,
    support_martingale::{SupportMartingaleAction, SupportMartingaleLifecycle},
};

#[derive(Clone, Debug)]
struct Editor {
    symbols: String,
    total_budget: String,
    first_order_notional: String,
    max_entries: String,
    size_multiplier: String,
    target_profit_rate: String,
    minimum_profit_quote: String,
    max_active_positions: String,
}
impl Editor {
    fn create() -> Self {
        Self {
            symbols: "SOL/USDT,DOGE/USDT".into(),
            total_budget: "30".into(),
            first_order_notional: "5".into(),
            max_entries: "4".into(),
            size_multiplier: "1.0".into(),
            target_profit_rate: "0.005".into(),
            minimum_profit_quote: "0.02".into(),
            max_active_positions: "2".into(),
        }
    }
    fn config(&self) -> Result<SupportMartingaleConfig, String> {
        let symbols = self
            .symbols
            .split(',')
            .map(|s| s.trim().to_ascii_uppercase())
            .filter(|s| !s.is_empty())
            .map(|s| s.parse())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "交易对必须是 BASE/QUOTE".to_owned())?;
        let decimal = |v: &str| {
            v.trim()
                .parse()
                .map_err(|_| "金额必须是十进制数".to_owned())
        };
        let config = SupportMartingaleConfig {
            reference_venue: venue_gateway_api::VenueId::Binance,
            execution_venue: venue_gateway_api::VenueId::Bybit,
            symbols,
            total_budget: decimal(&self.total_budget)?,
            first_order_notional: decimal(&self.first_order_notional)?,
            max_entries: self
                .max_entries
                .trim()
                .parse()
                .map_err(|_| "最大层数必须是整数")?,
            size_multiplier: decimal(&self.size_multiplier)?,
            target_profit_rate: decimal(&self.target_profit_rate)?,
            minimum_profit_quote: decimal(&self.minimum_profit_quote)?,
            max_active_positions: self
                .max_active_positions
                .trim()
                .parse()
                .map_err(|_| "最大活动币数必须是整数")?,
        };
        config
            .validate()
            .map_err(|_| "配置无效：预算、名义金额、补仓次数和止盈必须为正数".to_owned())
            .map(|_| config)
    }
}

#[derive(Debug, Default)]
pub(crate) struct SupportMartingaleViewState {
    pub instances: Vec<SupportMartingaleListItem>,
    pub error: Option<String>,
    pub fresh: bool,
    pub pending: bool,
    editor: Option<Editor>,
}
impl SupportMartingaleViewState {
    pub fn apply_instances(&mut self, instances: Vec<SupportMartingaleListItem>) {
        self.instances = instances;
        self.fresh = true;
        if !self.pending {
            self.error = None;
        }
    }
    pub fn apply_summary(&mut self, summary: SupportMartingaleListItem) {
        if let Some(old) = self
            .instances
            .iter_mut()
            .find(|item| item.instance_id == summary.instance_id)
        {
            *old = summary;
        } else {
            self.instances.push(summary);
        }
        self.pending = false;
        self.fresh = true;
        self.error = None;
    }
    pub fn unavailable(&mut self, message: String, mutation: bool) {
        self.error = Some(message);
        self.fresh = false;
        if mutation {
            self.pending = false;
        }
    }
}

pub(crate) fn show(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    account_id: &str,
) {
    let pending = model.execution.support_martingale.pending;
    let fresh = model.execution.support_martingale.fresh;
    let error = model.execution.support_martingale.error.clone();
    let instances = model.execution.support_martingale.instances.clone();
    ui.horizontal_wrapped(|ui| {
        ui.strong("支撑分批做多");
        ui.weak("Binance USD-M 参考行情 · Bybit 执行");
        if ui
            .add_enabled(!pending, egui::Button::new("新建策略"))
            .clicked()
        {
            model.execution.support_martingale.editor = Some(Editor::create());
        }
    });
    if let Some(error) = error {
        ui.colored_label(theme::WARNING, error);
    }
    if !fresh {
        ui.weak("正在读取策略实例；接口未接入时不会产生交易请求。");
    }
    if instances.is_empty() {
        ui.weak("当前账户暂无支撑分批实例");
    } else {
        egui::Grid::new("support-martingale-list")
            .striped(true)
            .show(ui, |ui| {
                ui.strong("执行所");
                ui.strong("状态");
                ui.strong("交易对数");
                ui.strong("预留");
                ui.strong("操作");
                ui.end_row();
                for item in &instances {
                    if item.trading_account_id == account_id {
                        row(ui, model, client, item);
                        ui.end_row();
                    }
                }
            });
    }
    editor(ui, model, client, credential);
}

fn row(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    item: &SupportMartingaleListItem,
) {
    ui.label(item.execution_venue.as_str());
    ui.label(lifecycle_label(item.lifecycle));
    ui.label(item.symbol_count.to_string());
    ui.label(item.reserved_budget.to_string());
    ui.horizontal(|ui| {
        for (label, action, enabled) in [
            (
                "启动",
                SupportMartingaleAction::Start,
                matches!(item.lifecycle, SupportMartingaleLifecycle::Stopped),
            ),
            (
                "暂停首仓",
                SupportMartingaleAction::PauseEntry,
                item.lifecycle == SupportMartingaleLifecycle::Running,
            ),
            (
                "暂停增险",
                SupportMartingaleAction::PauseIncrease,
                item.lifecycle == SupportMartingaleLifecycle::Running,
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
                    enabled && !model.execution.support_martingale.pending,
                    egui::Button::new(label),
                )
                .clicked()
            {
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
                );
            }
        }
    });
}
fn lifecycle_label(value: SupportMartingaleLifecycle) -> &'static str {
    match value {
        SupportMartingaleLifecycle::Running => "运行中",
        SupportMartingaleLifecycle::EntryPaused => "暂停首仓",
        SupportMartingaleLifecycle::IncreasePaused => "暂停增险",
        SupportMartingaleLifecycle::Draining => "等待止盈",
        SupportMartingaleLifecycle::Stopped => "已停止",
    }
}

fn editor(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
) {
    let Some(mut draft) = model.execution.support_martingale.editor.clone() else {
        return;
    };
    let mut close = false;
    let mut submit = false;
    egui::Window::new("支撑分批做多配置").collapsible(false).resizable(true).show(ui.ctx(), |ui| { egui::ScrollArea::vertical().max_height(420.0).show(ui, |ui| { ui.label("参考行情固定为 Binance USD-M；当前执行所为 Bybit。"); for (label, value) in [("交易对（逗号分隔）", &mut draft.symbols), ("总预算", &mut draft.total_budget), ("首仓名义", &mut draft.first_order_notional), ("最大层数", &mut draft.max_entries), ("补仓倍率", &mut draft.size_multiplier), ("止盈率", &mut draft.target_profit_rate), ("最低止盈金额", &mut draft.minimum_profit_quote), ("最大活动币数", &mut draft.max_active_positions)] { ui.label(label); ui.text_edit_singleline(value); } ui.weak("大周期下跌时仅已有仓位允许新支撑补仓；无自动止损、无持仓超时。保存不会启动。"); }); ui.horizontal(|ui| { if ui.button("取消").clicked() { close = true; } if ui.button("保存（不启动）").clicked() { submit = true; } }); });
    if submit {
        match draft.config() {
            Ok(config) => {
                let request = SupportMartingaleCreateRequest {
                    schema_version: crate::client::schema_version(),
                    request_id: format!("desktop-create-{}", crate::account_center::now_ms()),
                    credential_id: credential.credential_id.clone(),
                    config,
                };
                dispatch(
                    model,
                    client,
                    GridMutation::SupportMartingaleCreate(request),
                );
                close = true;
            }
            Err(error) => model.execution.support_martingale.error = Some(error),
        }
    }
    model.execution.support_martingale.editor = if close { None } else { Some(draft) };
}
fn dispatch(model: &mut AppModel, client: &ControlClient, mutation: GridMutation) {
    if client.send_grid(mutation).is_ok() {
        model.execution.support_martingale.pending = true;
        model.execution.support_martingale.error = None;
    } else {
        model.execution.support_martingale.error = Some("请求未进入发送队列".into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_config_accepts_two_symbols() {
        assert!(Editor::create().config().is_ok());
    }
    #[test]
    fn invalid_config_is_rejected() {
        let mut editor = Editor::create();
        editor.total_budget = "0".into();
        assert!(editor.config().is_err());
    }
}
