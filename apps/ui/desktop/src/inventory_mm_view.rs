use crate::{
    client::{
        ControlClient, GridMutation,
        inventory_mm::{Event, Mutation},
    },
    model::AppModel,
    theme,
};
use eframe::egui;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use venue_control_protocol::{VenueId, accounts::CredentialSummary, inventory_mm::*};

const FIELDS: [&str; 12] = [
    "每单名义额（报价币）",
    "每侧基础半价差（bp）",
    "波动价差倍数",
    "净库存报价偏移（bp）",
    "单腿库存上限",
    "双腿总库存上限",
    "最大净敞口",
    "账户亏损上限（USD）",
    "账户权益回撤（USD）",
    "账户保证金缓冲（USD）",
    "要求币安杠杆",
    "报价刷新间隔（ms）",
];
#[derive(Clone, Debug)]
struct Editor {
    credential_id: String,
    account_id: String,
    symbol: String,
    values: [String; 12],
}
impl Editor {
    fn new(credential: &CredentialSummary, account: &str) -> Self {
        Self {
            credential_id: credential.credential_id.clone(),
            account_id: account.into(),
            symbol: "XRP/USDC".into(),
            values: [
                "5", "5", "1", "5", "400", "800", "20", "5", "5", "20", "20", "3000",
            ]
            .map(str::to_owned),
        }
    }
    fn config(&self) -> Result<InventoryMmConfig, String> {
        let decimal = |index: usize| {
            self.values[index]
                .trim()
                .parse::<Decimal>()
                .map_err(|_| format!("{}必须是十进制数", FIELDS[index]))
        };
        let config = InventoryMmConfig {
            symbol: self
                .symbol
                .trim()
                .to_ascii_uppercase()
                .parse()
                .map_err(|_| "交易对必须是 BASE/QUOTE".to_owned())?,
            order_notional: decimal(0)?,
            base_half_spread_bps: decimal(1)?,
            volatility_multiplier: decimal(2)?,
            inventory_skew_bps: decimal(3)?,
            max_leg_notional: decimal(4)?,
            max_gross_notional: decimal(5)?,
            max_net_notional: decimal(6)?,
            max_loss_quote: decimal(7)?,
            max_drawdown_quote: decimal(8)?,
            min_available_margin: decimal(9)?,
            required_leverage: self.values[10]
                .trim()
                .parse()
                .map_err(|_| "杠杆必须为整数".to_owned())?,
            quote_refresh_ms: self.values[11]
                .trim()
                .parse()
                .map_err(|_| "刷新间隔必须为整数".to_owned())?,
        };
        config.validate().map_err(|_| {
            "风控参数不合法：金额须为正数、净上限不大于单腿、杠杆1–20、刷新1000–60000ms".to_owned()
        })?;
        Ok(config)
    }
}
#[derive(Debug, Default)]
pub(crate) struct InventoryMmViewState {
    pub instances: Vec<InventoryMmInstance>,
    loaded_ms: Option<u64>,
    selected: Option<String>,
    editor: Option<Editor>,
    pending: Option<Mutation>,
    uncertain: bool,
    error: Option<String>,
    notice: Option<String>,
    preflights: BTreeMap<String, InventoryMmPreflight>,
    confirmed: Option<(String, u64)>,
}
impl InventoryMmViewState {
    pub fn apply(&mut self, event: Event) {
        match event {
            Event::Instances(items) => {
                self.instances = items;
                self.loaded_ms = Some(crate::account_center::now_ms());
                self.preflights.retain(|id, result| {
                    self.instances
                        .iter()
                        .any(|item| &item.instance_id == id && item.revision == result.revision)
                });
            }
            Event::Applied(item) => {
                if matches!(&self.pending, Some(Mutation::Create(request)) if request.credential_id == item.credential_id && request.config == item.config)
                {
                    self.editor = None;
                }
                self.pending = None;
                self.uncertain = false;
                self.error = None;
                self.confirmed = None;
                self.preflights.remove(&item.instance_id);
                self.notice = Some(
                    "控制请求已确认；保存为停止态不代表启动，生命周期回执不代表订单成交".into(),
                );
                self.instances
                    .retain(|old| old.instance_id != item.instance_id);
                self.selected = Some(item.instance_id.clone());
                self.instances.push(*item);
            }
            Event::Preflight(result) => {
                self.pending = None;
                self.error = None;
                self.uncertain = false;
                self.preflights.insert(result.instance_id.clone(), result);
                self.confirmed = None;
            }
            Event::Error {
                mutation,
                uncertain,
                message,
            } => {
                if mutation {
                    self.uncertain = uncertain;
                    if !uncertain {
                        self.pending = None;
                    }
                } else {
                    self.loaded_ms = None;
                }
                self.error = Some(message);
            }
        }
    }
    fn fresh(&self, now: u64) -> bool {
        self.loaded_ms
            .is_some_and(|at| at <= now && now - at <= 30_000)
    }
}
pub(crate) fn can_create(model: &AppModel, credential: &CredentialSummary) -> bool {
    credential.venue == VenueId::Binance
        && credential.selectable(crate::account_center::now_ms())
        && credential.trading_account_id.is_some()
        && model.execution.inventory_mm.pending.is_none()
}
pub(crate) fn open_create(model: &mut AppModel, credential: &CredentialSummary) {
    if let Some(account) = &credential.trading_account_id {
        model.execution.inventory_mm.editor = Some(Editor::new(credential, account));
        model.execution.inventory_mm.selected = None;
        model.execution.inventory_mm.error = None;
    }
}
pub(crate) fn list_row(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    credential: &CredentialSummary,
    item: &InventoryMmInstance,
) {
    ui.label(format!("MM · {}", item.config.symbol));
    ui.label("Binance 库存做市");
    ui.label(&credential.label);
    ui.label(item.config.symbol.to_string());
    ui.label(state_label(item.state));
    ui.label(format!(
        "净上限 {} / 总上限 {}",
        item.config.max_net_notional, item.config.max_gross_notional
    ));
    ui.label(item.revision.to_string());
    ui.label(item.attention.as_deref().unwrap_or("—"));
    if ui.button("管理").clicked() {
        crate::grid_view::clear_selection(model);
        crate::leader_bot_view::clear_selection(model);
        crate::support_martingale_view::clear_selection(model);
        model.execution.inventory_mm.selected = Some(item.instance_id.clone());
    }
}
fn state_label(state: InventoryMmState) -> &'static str {
    match state {
        InventoryMmState::Stopped => "已停止",
        InventoryMmState::StartPending => "启动待确认",
        InventoryMmState::Running => "运行中",
        InventoryMmState::StopPending => "撤单停止中",
        InventoryMmState::NeedsAttention => "需要处理",
    }
}
fn preflight_ready(
    result: Option<&InventoryMmPreflight>,
    item: &InventoryMmInstance,
    now: u64,
) -> bool {
    result.is_some_and(|result| {
        result.ready
            && result.blockers.is_empty()
            && result.instance_id == item.instance_id
            && result.revision == item.revision
            && result.checked_ms <= now
            && now - result.checked_ms <= 60_000
    })
}
pub(crate) fn show(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    account: &str,
) {
    let selected = model.execution.inventory_mm.selected.clone();
    let item = model
        .execution
        .inventory_mm
        .instances
        .iter()
        .find(|item| {
            Some(&item.instance_id) == selected.as_ref()
                && item.trading_account_id == account
                && item.credential_id == credential.credential_id
        })
        .cloned();
    if let Some(item) = item {
        let mut open = true;
        egui::Window::new("独立库存做市管理").open(&mut open).resizable(true).vscroll(true).show(ui.ctx(), |ui| {
            ui.label(format!("{} · {} · {}", credential.label, item.config.symbol, state_label(item.state)));
            ui.weak("双向库存做市：中间价、波动率价差和净库存偏移；不是网格。仅 Binance LIVE。");
            configuration(ui, &item.config);
            ui.label(format!("权益基线 {:?} / 权益峰值 {:?}", item.baseline_equity, item.peak_equity));
            ui.weak("权益含账户其他交易影响；这些数值不是本策略独立收益归因。");
            let now = crate::account_center::now_ms();
            let state = &model.execution.inventory_mm;
            let ready = preflight_ready(state.preflights.get(&item.instance_id), &item, now);
            let enabled = state.pending.is_none() && state.fresh(now) && credential.selectable(now);
            let stopped = item.state == InventoryMmState::Stopped;
            if let Some(result) = state.preflights.get(&item.instance_id) {
                ui.label(if ready { "当前版本只读预检通过" } else { "预检未通过、过期或版本已变化" });
                for blocker in &result.blockers { ui.colored_label(theme::WARNING, blocker); }
            }
            ui.horizontal(|ui| {
                if ui.add_enabled(enabled && stopped, egui::Button::new("只读签名预检")).clicked() {
                    dispatch(model, client, Mutation::Preflight(InventoryMmPreflightRequest { instance_id: item.instance_id.clone(), expected_revision: item.revision }));
                }
                let mut confirmed = model.execution.inventory_mm.confirmed.as_ref() == Some(&(item.instance_id.clone(), item.revision));
                if ui.add_enabled(enabled && stopped && ready, egui::Checkbox::new(&mut confirmed, "确认实盘风险及以上参数")).changed() {
                    model.execution.inventory_mm.confirmed = confirmed.then_some((item.instance_id.clone(), item.revision));
                }
                if ui.add_enabled(enabled && stopped && ready && confirmed, egui::Button::new("启动实盘做市")).clicked() {
                    let request = lifecycle(&item, InventoryMmAction::Start, model.next_terminal_request_id());
                    dispatch(model, client, request);
                }
                if ui.add_enabled(model.execution.inventory_mm.pending.is_none() && item.state != InventoryMmState::Stopped,
                    egui::Button::new("停止并撤本策略挂单")).clicked() {
                    let request = lifecycle(&item, InventoryMmAction::Stop, model.next_terminal_request_id());
                    dispatch(model, client, request);
                }
            });
            ui.weak("停止保留已有多空仓位；服务端签名确认撤单终态后才停止。启动会重新预检，不保证盈利。");
            messages(ui, &model.execution.inventory_mm);
        });
        if !open {
            model.execution.inventory_mm.selected = None;
        }
    }
    editor(ui, model, client, credential, account);
    if model.execution.inventory_mm.selected.is_none()
        && model.execution.inventory_mm.editor.is_none()
    {
        messages(ui, &model.execution.inventory_mm);
    }
}
fn configuration(ui: &mut egui::Ui, config: &InventoryMmConfig) {
    ui.label(format!(
        "单笔 {} · 每侧基础半价差 {} bp（基础全价差 {} bp） · 要求 {}×",
        config.order_notional,
        config.base_half_spread_bps,
        config.base_half_spread_bps * Decimal::from(2),
        config.required_leverage
    ));
    ui.label(format!(
        "单腿 {} / 双腿总额 {} / 净敞口 {} · 波动倍数 {} / 库存偏移 {} bp",
        config.max_leg_notional,
        config.max_gross_notional,
        config.max_net_notional,
        config.volatility_multiplier,
        config.inventory_skew_bps
    ));
    ui.label(format!(
        "最大亏损 {} / 最大回撤 {} / 最低可用保证金 {} / 刷新 {} ms",
        config.max_loss_quote,
        config.max_drawdown_quote,
        config.min_available_margin,
        config.quote_refresh_ms
    ));
}
fn messages(ui: &mut egui::Ui, state: &InventoryMmViewState) {
    if let Some(error) = &state.error {
        ui.colored_label(theme::WARNING, error);
    }
    if let Some(notice) = &state.notice {
        ui.weak(notice);
    }
    if state.pending.is_some() {
        ui.weak(if state.uncertain {
            "请求结果不确定；禁止新请求，请管理员按原请求身份核对。"
        } else {
            "控制请求待确认…"
        });
    }
}
fn lifecycle(
    item: &InventoryMmInstance,
    action: InventoryMmAction,
    request_id: String,
) -> Mutation {
    Mutation::Lifecycle(InventoryMmLifecycleRequest {
        schema_version: INVENTORY_MM_SCHEMA_VERSION,
        request_id,
        instance_id: item.instance_id.clone(),
        expected_revision: item.revision,
        action,
    })
}
fn dispatch(model: &mut AppModel, client: &ControlClient, mutation: Mutation) {
    if model.execution.inventory_mm.pending.is_some() {
        return;
    }
    if client
        .send_grid(GridMutation::InventoryMm(mutation.clone()))
        .is_ok()
    {
        model.execution.inventory_mm.pending = Some(mutation);
        model.execution.inventory_mm.error = None;
        model.execution.inventory_mm.notice = None;
        model.execution.inventory_mm.uncertain = false;
    } else {
        model.execution.inventory_mm.error = Some("请求未进入发送队列".into());
    }
}
fn editor(
    ui: &mut egui::Ui,
    model: &mut AppModel,
    client: &ControlClient,
    credential: &CredentialSummary,
    account: &str,
) {
    let Some(mut draft) = model.execution.inventory_mm.editor.clone() else {
        return;
    };
    let current = draft.credential_id == credential.credential_id && draft.account_id == account;
    let mut close = false;
    let mut save = false;
    egui::Modal::new(egui::Id::new("inventory-mm-create")).show(ui.ctx(), |ui| {
        ui.set_width(490.0); ui.heading("新建独立库存做市策略");
        ui.weak("仅 Binance LIVE；保存为停止态，预检后才能启动。参数不影响已有对冲网格。");
        if !current { ui.colored_label(theme::WARNING, "当前账户已变化，不能提交旧账户草稿"); }
        egui::ScrollArea::vertical().max_height(420.0).show(ui, |ui| {
            egui::Grid::new("inventory-mm-fields").num_columns(2).show(ui, |ui| {
                ui.label("交易对"); ui.text_edit_singleline(&mut draft.symbol); ui.end_row();
                for (label, value) in FIELDS.iter().zip(draft.values.iter_mut()) {
                    ui.label(*label); ui.text_edit_singleline(value); ui.end_row();
                }
            });
            ui.weak("5 bp 是每侧半价差，基础双边价差为10 bp；波动调整可加宽。开仓最低额按交易所规则处理，平仓按可减数量裁剪。");
        });
        messages(ui, &model.execution.inventory_mm);
        ui.horizontal(|ui| {
            save = ui.add_enabled(current && can_create(model, credential), egui::Button::new("保存为停止态")).clicked();
            close = ui.add_enabled(model.execution.inventory_mm.pending.is_none(), egui::Button::new("取消")).clicked();
        });
    });
    if save {
        match draft.config() {
            Ok(config) => {
                let request_id = model.next_terminal_request_id();
                dispatch(
                    model,
                    client,
                    Mutation::Create(InventoryMmCreateRequest {
                        schema_version: INVENTORY_MM_SCHEMA_VERSION,
                        request_id,
                        credential_id: draft.credential_id.clone(),
                        config,
                    }),
                );
            }
            Err(error) => model.execution.inventory_mm.error = Some(error),
        }
    }
    model.execution.inventory_mm.editor = if close { None } else { Some(draft) };
}

#[cfg(test)]
mod tests {
    use super::*;
    fn draft() -> Editor {
        Editor {
            credential_id: "00000000-0000-4000-8000-000000000001".into(),
            account_id: "00000000-0000-4000-8000-000000000002".into(),
            symbol: "XRP/USDC".into(),
            values: [
                "5", "5", "1", "5", "400", "800", "20", "5", "5", "20", "20", "3000",
            ]
            .map(str::to_owned),
        }
    }
    #[test]
    fn inventory_mm_editor_uses_independent_user_defaults() -> Result<(), String> {
        let config = draft().config()?;
        assert_eq!(config.symbol.to_string(), "XRP/USDC");
        assert_eq!(config.order_notional, Decimal::from(5));
        assert_eq!(config.base_half_spread_bps, Decimal::from(5));
        assert_eq!(config.max_leg_notional, Decimal::from(400));
        assert_eq!(config.max_gross_notional, Decimal::from(800));
        assert_eq!(config.required_leverage, 20);
        Ok(())
    }
    #[test]
    fn inventory_mm_editor_rejects_unbounded_and_invalid_parameters() {
        let mut editor = draft();
        editor.values[6] = "401".into();
        assert!(editor.config().is_err());
        editor = draft();
        editor.values[10] = "21".into();
        assert!(editor.config().is_err());
        editor = draft();
        editor.values[7] = "0".into();
        assert!(editor.config().is_err());
        editor = draft();
        editor.values[3] = "1000".into();
        assert!(editor.config().is_err());
    }
    #[test]
    fn inventory_mm_uncertain_mutation_preserves_original_identity() -> Result<(), String> {
        let request = Mutation::Create(InventoryMmCreateRequest {
            schema_version: INVENTORY_MM_SCHEMA_VERSION,
            request_id: "00000000-0000-4000-8000-000000000003".into(),
            credential_id: draft().credential_id,
            config: draft().config()?,
        });
        let mut state = InventoryMmViewState {
            pending: Some(request),
            ..Default::default()
        };
        state.apply(Event::Error {
            mutation: true,
            uncertain: true,
            message: "unknown".into(),
        });
        state.apply(Event::Instances(Vec::new()));
        assert!(state.uncertain);
        assert!(
            matches!(&state.pending, Some(Mutation::Create(value)) if value.request_id == "00000000-0000-4000-8000-000000000003")
        );
        Ok(())
    }
    #[test]
    fn inventory_mm_preflight_is_revision_and_time_bound() -> Result<(), String> {
        let item = InventoryMmInstance {
            instance_id: "00000000-0000-4000-8000-000000000003".into(),
            owner_user_id: draft().account_id,
            trading_account_id: draft().account_id,
            credential_id: draft().credential_id,
            config: draft().config()?,
            state: InventoryMmState::Stopped,
            revision: 2,
            baseline_equity: None,
            peak_equity: None,
            attention: None,
            updated_ms: 1,
            last_quote_ms: None,
        };
        let mut result = InventoryMmPreflight {
            instance_id: item.instance_id.clone(),
            revision: 2,
            checked_ms: 10,
            ready: true,
            blockers: Vec::new(),
        };
        assert!(preflight_ready(Some(&result), &item, 11));
        assert!(!preflight_ready(Some(&result), &item, 9));
        assert!(!preflight_ready(Some(&result), &item, 60_011));
        result.revision = 1;
        assert!(!preflight_ready(Some(&result), &item, 11));
        Ok(())
    }
}
