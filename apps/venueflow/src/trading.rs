use eframe::egui;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_control_protocol::{
    StrategySummary, TradeIntent, TradingAction, TradingOrderType, TradingTimeInForce,
};

pub const SIZE_PRESET_COUNT: usize = 5;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum DisplayCadence {
    Ms100,
    #[default]
    Ms250,
    Ms500,
    Ms1000,
}

impl DisplayCadence {
    pub const ALL: [Self; 4] = [Self::Ms100, Self::Ms250, Self::Ms500, Self::Ms1000];

    pub const fn millis(self) -> u64 {
        match self {
            Self::Ms100 => 100,
            Self::Ms250 => 250,
            Self::Ms500 => 500,
            Self::Ms1000 => 1000,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum TradingKey {
    A,
    B,
    C,
    D,
    E,
    F,
    G,
    H,
    I,
    J,
    K,
    L,
    M,
    N,
    O,
    P,
    Q,
    R,
    S,
    T,
    U,
    V,
    W,
    X,
    Y,
    Z,
    Num1,
    Num2,
    Num3,
    Num4,
    Num5,
    Escape,
    Space,
}

impl TradingKey {
    pub const ALL: [Self; 33] = [
        Self::A,
        Self::B,
        Self::C,
        Self::D,
        Self::E,
        Self::F,
        Self::G,
        Self::H,
        Self::I,
        Self::J,
        Self::K,
        Self::L,
        Self::M,
        Self::N,
        Self::O,
        Self::P,
        Self::Q,
        Self::R,
        Self::S,
        Self::T,
        Self::U,
        Self::V,
        Self::W,
        Self::X,
        Self::Y,
        Self::Z,
        Self::Num1,
        Self::Num2,
        Self::Num3,
        Self::Num4,
        Self::Num5,
        Self::Escape,
        Self::Space,
    ];

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
            Self::C => "C",
            Self::D => "D",
            Self::E => "E",
            Self::F => "F",
            Self::G => "G",
            Self::H => "H",
            Self::I => "I",
            Self::J => "J",
            Self::K => "K",
            Self::L => "L",
            Self::M => "M",
            Self::N => "N",
            Self::O => "O",
            Self::P => "P",
            Self::Q => "Q",
            Self::R => "R",
            Self::S => "S",
            Self::T => "T",
            Self::U => "U",
            Self::V => "V",
            Self::W => "W",
            Self::X => "X",
            Self::Y => "Y",
            Self::Z => "Z",
            Self::Num1 => "1",
            Self::Num2 => "2",
            Self::Num3 => "3",
            Self::Num4 => "4",
            Self::Num5 => "5",
            Self::Escape => "Esc",
            Self::Space => "Space",
        }
    }

    fn from_egui(key: egui::Key) -> Option<Self> {
        Some(match key {
            egui::Key::A => Self::A,
            egui::Key::B => Self::B,
            egui::Key::C => Self::C,
            egui::Key::D => Self::D,
            egui::Key::E => Self::E,
            egui::Key::F => Self::F,
            egui::Key::G => Self::G,
            egui::Key::H => Self::H,
            egui::Key::I => Self::I,
            egui::Key::J => Self::J,
            egui::Key::K => Self::K,
            egui::Key::L => Self::L,
            egui::Key::M => Self::M,
            egui::Key::N => Self::N,
            egui::Key::O => Self::O,
            egui::Key::P => Self::P,
            egui::Key::Q => Self::Q,
            egui::Key::R => Self::R,
            egui::Key::S => Self::S,
            egui::Key::T => Self::T,
            egui::Key::U => Self::U,
            egui::Key::V => Self::V,
            egui::Key::W => Self::W,
            egui::Key::X => Self::X,
            egui::Key::Y => Self::Y,
            egui::Key::Z => Self::Z,
            egui::Key::Num1 => Self::Num1,
            egui::Key::Num2 => Self::Num2,
            egui::Key::Num3 => Self::Num3,
            egui::Key::Num4 => Self::Num4,
            egui::Key::Num5 => Self::Num5,
            egui::Key::Escape => Self::Escape,
            egui::Key::Space => Self::Space,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct HotkeyMapping {
    pub open_long: TradingKey,
    pub close_long: TradingKey,
    pub close_short: TradingKey,
    pub open_short: TradingKey,
    pub cancel_selected: TradingKey,
    pub cancel_all: TradingKey,
    pub size_presets: [TradingKey; SIZE_PRESET_COUNT],
    pub clear: TradingKey,
    pub center_market: TradingKey,
}

impl Default for HotkeyMapping {
    fn default() -> Self {
        Self {
            open_long: TradingKey::A,
            close_long: TradingKey::S,
            close_short: TradingKey::D,
            open_short: TradingKey::F,
            cancel_selected: TradingKey::Q,
            cancel_all: TradingKey::E,
            size_presets: [
                TradingKey::Num1,
                TradingKey::Num2,
                TradingKey::Num3,
                TradingKey::Num4,
                TradingKey::Num5,
            ],
            clear: TradingKey::Escape,
            center_market: TradingKey::Space,
        }
    }
}

impl HotkeyMapping {
    #[must_use]
    pub fn key_for(&self, action: TradingAction) -> Option<TradingKey> {
        Some(match action {
            TradingAction::OpenLong => self.open_long,
            TradingAction::CloseLong => self.close_long,
            TradingAction::CloseShort => self.close_short,
            TradingAction::OpenShort => self.open_short,
            TradingAction::CancelSelectedOrder => self.cancel_selected,
            TradingAction::CancelAllOrders => self.cancel_all,
            TradingAction::SelectSizePreset(index) => *self.size_presets.get(index)?,
            TradingAction::ClearSelection => self.clear,
            TradingAction::CenterMarket => self.center_market,
        })
    }

    #[must_use]
    pub fn action_for(&self, key: TradingKey) -> Option<TradingAction> {
        executable_actions()
            .into_iter()
            .find(|action| self.key_for(*action) == Some(key))
    }

    pub fn assign(&mut self, action: TradingAction, key: TradingKey) {
        let Some(old_key) = self.key_for(action) else {
            return;
        };
        if old_key == key {
            return;
        }
        if let Some(conflicting) = self.action_for(key) {
            self.set_key(conflicting, old_key);
        }
        self.set_key(action, key);
    }

    fn set_key(&mut self, action: TradingAction, key: TradingKey) {
        match action {
            TradingAction::OpenLong => self.open_long = key,
            TradingAction::CloseLong => self.close_long = key,
            TradingAction::CloseShort => self.close_short = key,
            TradingAction::OpenShort => self.open_short = key,
            TradingAction::CancelSelectedOrder => self.cancel_selected = key,
            TradingAction::CancelAllOrders => self.cancel_all = key,
            TradingAction::SelectSizePreset(index) => {
                if let Some(binding) = self.size_presets.get_mut(index) {
                    *binding = key;
                }
            }
            TradingAction::ClearSelection => self.clear = key,
            TradingAction::CenterMarket => self.center_market = key,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TradingSettings {
    pub post_only: bool,
    pub size_presets: [Decimal; SIZE_PRESET_COUNT],
    pub hotkeys: HotkeyMapping,
    pub hotkeys_enabled: bool,
    pub book_cadence: DisplayCadence,
    pub tape_cadence: DisplayCadence,
    pub chart_cadence: DisplayCadence,
}

impl Default for TradingSettings {
    fn default() -> Self {
        Self {
            post_only: false,
            size_presets: [
                Decimal::new(25, 0),
                Decimal::new(50, 0),
                Decimal::new(100, 0),
                Decimal::new(200, 0),
                Decimal::new(500, 0),
            ],
            hotkeys: HotkeyMapping::default(),
            hotkeys_enabled: true,
            book_cadence: DisplayCadence::Ms250,
            tape_cadence: DisplayCadence::Ms500,
            chart_cadence: DisplayCadence::Ms250,
        }
    }
}

impl TradingSettings {
    #[must_use]
    pub fn validate(&self) -> bool {
        self.size_presets.iter().all(|value| *value > Decimal::ZERO)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradingScope {
    pub venue: String,
    pub trading_account_id: String,
    pub symbol: String,
}

#[derive(Clone, Debug, Default)]
pub struct TradeDockState {
    pub selected_price: Option<Decimal>,
    pub selected_order_id: Option<String>,
    pub armed_action: Option<TradingAction>,
    pub selected_size_preset: usize,
    scope: Option<TradingScope>,
}

impl TradeDockState {
    pub fn observe_scope(&mut self, scope: Option<TradingScope>) {
        if self.scope != scope {
            self.clear_selection();
            self.scope = scope;
        }
    }

    pub fn select_price(&mut self, price: Decimal) -> Result<(), TradePlanError> {
        if price <= Decimal::ZERO {
            return Err(TradePlanError::InvalidPrice);
        }
        self.selected_price = Some(price);
        self.armed_action = None;
        Ok(())
    }

    pub fn clear_selection(&mut self) {
        self.selected_price = None;
        self.selected_order_id = None;
        self.armed_action = None;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TradePlanError {
    #[error("select a chart or order-book price before placing an order")]
    MissingPrice,
    #[error("selected price is invalid")]
    InvalidPrice,
    #[error("selected size preset is invalid")]
    InvalidSize,
    #[error("the corresponding position is zero")]
    NoPosition,
    #[error("UI-only trading actions cannot be submitted to Control")]
    UiOnlyAction,
}

pub fn build_trade_intent(
    strategy: &StrategySummary,
    settings: &TradingSettings,
    state: &TradeDockState,
    action: TradingAction,
) -> Result<TradeIntent, TradePlanError> {
    if action.is_ui_only() {
        return Err(TradePlanError::UiOnlyAction);
    }
    if matches!(
        action,
        TradingAction::CancelSelectedOrder | TradingAction::CancelAllOrders
    ) {
        return Ok(TradeIntent {
            action,
            quote_asset: strategy.symbol.quote().to_owned(),
            order_type: TradingOrderType::Limit,
            time_in_force: TradingTimeInForce::Gtc,
            post_only: settings.post_only,
            reduce_only: false,
            selected_price: None,
            quote_notional: None,
            close_quantity_cap: None,
            selected_order_id: (action == TradingAction::CancelSelectedOrder)
                .then(|| state.selected_order_id.clone())
                .flatten(),
        });
    }
    let price = state.selected_price.ok_or(TradePlanError::MissingPrice)?;
    if price <= Decimal::ZERO {
        return Err(TradePlanError::InvalidPrice);
    }
    let notional = settings
        .size_presets
        .get(state.selected_size_preset)
        .copied()
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(TradePlanError::InvalidSize)?;
    let close_quantity_cap = match action {
        TradingAction::CloseLong => Some(strategy.long_quantity),
        TradingAction::CloseShort => Some(strategy.short_quantity),
        _ => None,
    }
    .map(|position| position.min(notional / price))
    .filter(|quantity| *quantity > Decimal::ZERO);
    if action.is_close_action() && close_quantity_cap.is_none() {
        return Err(TradePlanError::NoPosition);
    }
    Ok(TradeIntent {
        action,
        quote_asset: strategy.symbol.quote().to_owned(),
        order_type: TradingOrderType::Limit,
        time_in_force: TradingTimeInForce::Gtc,
        post_only: settings.post_only,
        reduce_only: action.is_close_action(),
        selected_price: Some(price),
        quote_notional: Some(notional),
        close_quantity_cap,
        selected_order_id: None,
    })
}

#[must_use]
pub fn hotkey_action(event: &egui::Event, settings: &TradingSettings) -> Option<TradingAction> {
    if !settings.hotkeys_enabled {
        return None;
    }
    let egui::Event::Key {
        key,
        pressed: true,
        repeat: false,
        modifiers,
        ..
    } = event
    else {
        return None;
    };
    if modifiers.alt || modifiers.ctrl || modifiers.command || modifiers.shift {
        return None;
    }
    settings.hotkeys.action_for(TradingKey::from_egui(*key)?)
}

#[must_use]
pub fn executable_actions() -> [TradingAction; 13] {
    [
        TradingAction::OpenLong,
        TradingAction::CloseLong,
        TradingAction::CloseShort,
        TradingAction::OpenShort,
        TradingAction::CancelSelectedOrder,
        TradingAction::CancelAllOrders,
        TradingAction::SelectSizePreset(0),
        TradingAction::SelectSizePreset(1),
        TradingAction::SelectSizePreset(2),
        TradingAction::SelectSizePreset(3),
        TradingAction::SelectSizePreset(4),
        TradingAction::ClearSelection,
        TradingAction::CenterMarket,
    ]
}

pub fn show_settings(context: &egui::Context, open: &mut bool, model: &mut crate::model::AppModel) {
    if !*open {
        return;
    }
    let language = model.preferences.language;
    let mut window_open = true;
    let mut close_requested = false;
    let viewport = context.input(|input| input.viewport_rect().size());
    let window_size = egui::vec2(
        (viewport.x - 40.0).clamp(320.0, 820.0),
        (viewport.y - 40.0).clamp(180.0, 740.0),
    );
    egui::Window::new("trading-settings")
        .open(&mut window_open)
        .title_bar(false)
        .resizable(false)
        .collapsible(false)
        .anchor(egui::Align2::CENTER_CENTER, egui::Vec2::ZERO)
        .fixed_size(window_size)
        .frame(
            egui::Frame::new()
                .fill(egui::Color32::from_rgb(31, 38, 50))
                .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(54, 64, 79)))
                .inner_margin(egui::Margin::ZERO)
                .corner_radius(egui::CornerRadius::same(8)),
        )
        .show(context, |ui| {
            trading_settings_header(ui, language, &mut close_requested);
            ui.separator();
            egui::ScrollArea::both().id_salt("trading-settings-body")
                .auto_shrink([false, false]).max_height((window_size.y - 115.0).max(40.0))
                .show(ui, |ui| {
            display_settings(ui, model);
            ui.add_space(10.0);
            ui.horizontal(|ui| {
                ui.allocate_ui_with_layout(
                    egui::vec2(300.0, 475.0),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        settings_card_frame(ui, |ui| {
                            settings_card_title(
                                ui,
                                settings_label(language, "订单参数", "Order parameters"),
                                settings_label(
                                    language,
                                    "下单时默认使用的执行参数",
                                    "Defaults used when placing an order",
                                ),
                            );
                            egui::Grid::new("trading-core-settings")
                                .num_columns(2)
                                .spacing([18.0, 12.0])
                                .show(ui, |ui| {
                                    settings_row(
                                        ui,
                                        settings_label(language, "订单类型", "Order type"),
                                        "Limit",
                                    );
                                    ui.label(settings_label(language, "Post Only", "Post only"));
                                    ui.checkbox(&mut model.preferences.trading.post_only, "ON");
                                    ui.end_row();
                                    settings_row(ui, "TIF", "GTC");
                                    settings_row(
                                        ui,
                                        settings_label(language, "数量单位", "Size unit"),
                                        model.preferences.selected_symbol.split_once('/').map_or("—", |(_, quote)| quote),
                                    );
                                });
                        });
                        ui.add_space(12.0);
                        settings_card_frame(ui, |ui| {
                            settings_card_title(
                                ui,
                                settings_label(language, "数量预设", "Size presets"),
                                settings_label(
                                    language,
                                    "快捷键 1–5 直接选择下单金额",
                                    "Keys 1–5 select a quote amount",
                                ),
                            );
                            egui::Grid::new("trading-size-presets")
                                .num_columns(2)
                                .spacing([12.0, 10.0])
                                .show(ui, |ui| {
                                    for index in 0..SIZE_PRESET_COUNT {
                                        settings_preset_cell(ui, model, index);
                                        if index % 2 == 1 {
                                            ui.end_row();
                                        }
                                    }
                                    if !SIZE_PRESET_COUNT.is_multiple_of(2) {
                                        ui.end_row();
                                    }
                                });
                        });
                    },
                );
                ui.separator();
                ui.allocate_ui_with_layout(
                    egui::vec2(450.0, 475.0),
                    egui::Layout::top_down(egui::Align::Min),
                    |ui| {
                        settings_card_frame(ui, |ui| {
                            settings_card_title(
                                ui,
                                settings_label(language, "交易快捷键", "Trading hotkeys"),
                                settings_label(
                                    language,
                                    "可按个人习惯重新绑定，冲突按最后一次选择覆盖",
                                    "Rebind keys to fit your workflow; the latest choice wins conflicts",
                                ),
                            );
                            egui::ScrollArea::vertical()
                                .id_salt("trading-hotkeys-scroll")
                                .auto_shrink([false, false])
                                .max_height(325.0)
                                .show(ui, |ui| {
                                    egui::Grid::new("trading-hotkeys-grid")
                                        .num_columns(2)
                                        .spacing([14.0, 9.0])
                                        .show(ui, |ui| {
                                            for (index, (action, chinese, english)) in
                                                settings_hotkey_rows().into_iter().enumerate()
                                            {
                                                settings_hotkey_editor(
                                                    ui,
                                                    model,
                                                    action,
                                                    settings_label(language, chinese, english),
                                                );
                                                if index % 2 == 1 {
                                                    ui.end_row();
                                                }
                                            }
                                            if !settings_hotkey_rows().len().is_multiple_of(2) {
                                                ui.end_row();
                                            }
                                        });
                                });
                            ui.separator();
                            ui.checkbox(
                                &mut model.preferences.trading.hotkeys_enabled,
                                settings_label(
                                    language,
                                    "启用交易快捷键",
                                    "Trading hotkeys enabled",
                                ),
                            );
                            if !model.preferences.trading.validate() {
                                ui.colored_label(
                                    crate::theme::SELL,
                                    settings_label(
                                        language,
                                        "所有数量预设必须大于 0",
                                        "Every size preset must be greater than zero",
                                    ),
                                );
                            }
                        });
                    },
                );
            });
            });
            ui.separator();
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new(settings_label(
                    language,
                    "设置会即时应用到当前交易面板",
                    "Changes apply immediately to the trading panel",
                ))
                .size(11.0)
                .color(crate::theme::TEXT_SECONDARY));
                ui.with_layout(
                    egui::Layout::right_to_left(egui::Align::Center),
                    |ui| {
                        if ui
                            .add(
                                egui::Button::new(
                                    egui::RichText::new(settings_label(
                                        language,
                                        "完成",
                                        "Done",
                                    ))
                                    .strong(),
                                )
                                .fill(crate::theme::BRAND)
                                .min_size(egui::vec2(88.0, 30.0)),
                            )
                            .clicked()
                        {
                            close_requested = true;
                        }
                    },
                );
            });
        });
    if close_requested {
        window_open = false;
    }
    if !window_open {
        *open = false;
    }
}

fn trading_settings_header(
    ui: &mut egui::Ui,
    language: crate::i18n::Language,
    close_requested: &mut bool,
) {
    ui.allocate_ui_with_layout(
        egui::vec2(ui.available_width(), 58.0),
        egui::Layout::left_to_right(egui::Align::Center),
        |ui| {
            ui.add_space(18.0);
            ui.vertical(|ui| {
                ui.label(
                    egui::RichText::new(settings_label(language, "交易设置", "Trading settings"))
                        .size(16.0)
                        .strong()
                        .color(crate::theme::TEXT_PRIMARY),
                );
                ui.label(
                    egui::RichText::new(settings_label(
                        language,
                        "订单参数 · 数量预设 · 快捷键",
                        "Order parameters · presets · hotkeys",
                    ))
                    .size(11.0)
                    .color(crate::theme::TEXT_SECONDARY),
                );
            });
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add(egui::Button::new(egui::RichText::new("×").size(28.0)).frame(false))
                    .clicked()
                {
                    *close_requested = true;
                }
            });
        },
    );
}

fn settings_card_frame(ui: &mut egui::Ui, add_contents: impl FnOnce(&mut egui::Ui)) {
    egui::Frame::new()
        .fill(egui::Color32::from_rgb(37, 45, 58))
        .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(54, 64, 79)))
        .inner_margin(egui::Margin::symmetric(14, 12))
        .corner_radius(egui::CornerRadius::same(6))
        .show(ui, add_contents);
}

fn settings_card_title(ui: &mut egui::Ui, title: &str, description: &str) {
    ui.label(
        egui::RichText::new(title)
            .size(14.0)
            .strong()
            .color(crate::theme::TEXT_PRIMARY),
    );
    ui.label(
        egui::RichText::new(description)
            .size(11.0)
            .color(crate::theme::TEXT_SECONDARY),
    );
    ui.add_space(10.0);
}

fn settings_preset_cell(ui: &mut egui::Ui, model: &mut crate::model::AppModel, index: usize) {
    let quote = model
        .preferences
        .selected_symbol
        .split_once('/')
        .map_or("—", |(_, quote)| quote)
        .to_owned();
    ui.horizontal(|ui| {
        ui.label(
            egui::RichText::new(format!("{}", index + 1))
                .size(12.0)
                .strong()
                .color(crate::theme::TEXT_SECONDARY),
        );
        if let Some(value) = model.preferences.trading.size_presets.get_mut(index) {
            let mut numeric = value.to_string().parse::<f64>().unwrap_or(1.0);
            if ui
                .add(
                    egui::DragValue::new(&mut numeric)
                        .range(0.01..=1_000_000_000.0)
                        .speed(0.1)
                        .suffix(format!(" {quote}")),
                )
                .changed()
                && let Some(decimal) = Decimal::from_f64_retain(numeric)
            {
                *value = decimal;
            }
        }
    });
}

fn display_settings(ui: &mut egui::Ui, model: &mut crate::model::AppModel) {
    use crate::i18n::{TextKey, text};
    let language = model.preferences.language;
    settings_card_frame(ui, |ui| {
        settings_card_title(
            ui,
            text(language, TextKey::DisplayCadence),
            text(language, TextKey::DisplayCadenceHint),
        );
        ui.horizontal_wrapped(|ui| {
            let settings = &mut model.preferences.trading;
            for (key, cadence) in [
                (TextKey::OrderBook, &mut settings.book_cadence),
                (TextKey::RecentTrades, &mut settings.tape_cadence),
                (TextKey::CandleCadence, &mut settings.chart_cadence),
            ] {
                ui.label(text(language, key));
                egui::ComboBox::from_id_salt(format!("display-{key:?}"))
                    .width(82.0)
                    .selected_text(format!("{} ms", cadence.millis()))
                    .show_ui(ui, |ui| {
                        for candidate in DisplayCadence::ALL {
                            ui.selectable_value(
                                cadence,
                                candidate,
                                format!("{} ms", candidate.millis()),
                            );
                        }
                    });
            }
        });
    });
}

fn settings_row(ui: &mut egui::Ui, key: &str, value: &str) {
    ui.label(key);
    ui.monospace(value);
    ui.end_row();
}

fn settings_hotkey_editor(
    ui: &mut egui::Ui,
    model: &mut crate::model::AppModel,
    action: TradingAction,
    title: &str,
) {
    let Some(current) = model.preferences.trading.hotkeys.key_for(action) else {
        return;
    };
    let mut selected = current;
    ui.horizontal(|ui| {
        ui.set_min_width(205.0);
        ui.label(
            egui::RichText::new(title)
                .size(12.0)
                .color(crate::theme::TEXT_PRIMARY),
        );
        egui::ComboBox::from_id_salt(("trading-hotkey", action))
            .width(76.0)
            .selected_text(current.label())
            .show_ui(ui, |ui| {
                for key in TradingKey::ALL {
                    ui.selectable_value(&mut selected, key, key.label());
                }
            });
    });
    if selected != current {
        model.preferences.trading.hotkeys.assign(action, selected);
    }
}

fn settings_hotkey_rows() -> [(TradingAction, &'static str, &'static str); 13] {
    [
        (TradingAction::OpenLong, "开多", "Open Long"),
        (TradingAction::CloseLong, "平多", "Close Long"),
        (TradingAction::CloseShort, "平空", "Close Short"),
        (TradingAction::OpenShort, "开空", "Open Short"),
        (
            TradingAction::CancelSelectedOrder,
            "撤当前",
            "Cancel Current",
        ),
        (TradingAction::CancelAllOrders, "撤全部", "Cancel All"),
        (
            TradingAction::SelectSizePreset(0),
            "数量预设 1",
            "Size Preset 1",
        ),
        (
            TradingAction::SelectSizePreset(1),
            "数量预设 2",
            "Size Preset 2",
        ),
        (
            TradingAction::SelectSizePreset(2),
            "数量预设 3",
            "Size Preset 3",
        ),
        (
            TradingAction::SelectSizePreset(3),
            "数量预设 4",
            "Size Preset 4",
        ),
        (
            TradingAction::SelectSizePreset(4),
            "数量预设 5",
            "Size Preset 5",
        ),
        (TradingAction::ClearSelection, "清除", "Clear"),
        (TradingAction::CenterMarket, "回到市场", "Center Market"),
    ]
}

const fn settings_label<'a>(
    language: crate::i18n::Language,
    chinese: &'a str,
    english: &'a str,
) -> &'a str {
    match language {
        crate::i18n::Language::SimplifiedChinese => chinese,
        crate::i18n::Language::English => english,
    }
}

#[cfg(test)]
mod tests {
    use eframe::egui;
    use rust_decimal::Decimal;
    use venue_control_protocol::{
        GatewayMode, StrategyKind, StrategyLifecycle, StrategySummary, TradingAction, VenueId,
    };

    use super::{
        HotkeyMapping, TradeDockState, TradingKey, TradingScope, TradingSettings,
        build_trade_intent, hotkey_action,
    };

    fn strategy(long: Decimal, short: Decimal) -> StrategySummary {
        StrategySummary {
            instance_id: "manual-btc".to_owned(),
            kind: StrategyKind::Scalping,
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse().unwrap_or_else(|_| unreachable!()),
            lifecycle: StrategyLifecycle::Running,
            config_epoch: 1,
            open_orders: 0,
            long_quantity: long,
            short_quantity: short,
            realized_pnl: Some(Decimal::ZERO),
            unrealized_pnl: Some(Decimal::ZERO),
            last_receipt_ms: 1,
            attention: None,
        }
    }

    #[test]
    fn close_is_reduce_only_and_clamped_to_the_corresponding_position() {
        let settings = TradingSettings::default();
        let mut state = TradeDockState {
            selected_size_preset: 2,
            ..TradeDockState::default()
        };
        state
            .select_price(Decimal::new(20, 0))
            .unwrap_or_else(|_| unreachable!());
        let intent = build_trade_intent(
            &strategy(Decimal::new(3, 0), Decimal::new(9, 0)),
            &settings,
            &state,
            TradingAction::CloseLong,
        )
        .unwrap_or_else(|_| unreachable!());
        assert!(intent.reduce_only());
        assert_eq!(intent.close_quantity_cap, Some(Decimal::new(3, 0)));
    }

    #[test]
    fn order_actions_require_a_selected_price() {
        assert!(
            build_trade_intent(
                &strategy(Decimal::ONE, Decimal::ONE),
                &TradingSettings::default(),
                &TradeDockState::default(),
                TradingAction::OpenLong,
            )
            .is_err()
        );
    }

    #[test]
    fn cancel_current_without_selection_requests_scoped_recent_working_fallback() {
        let intent = build_trade_intent(
            &strategy(Decimal::ZERO, Decimal::ZERO),
            &TradingSettings::default(),
            &TradeDockState::default(),
            TradingAction::CancelSelectedOrder,
        )
        .unwrap_or_else(|_| unreachable!());
        assert_eq!(intent.selected_order_id, None);
        assert_eq!(intent.selected_price, None);
    }

    #[test]
    fn rebinding_swaps_conflicts_and_button_labels_follow_the_mapping() {
        let mut mapping = HotkeyMapping::default();
        mapping.assign(TradingAction::OpenLong, TradingKey::Z);
        assert_eq!(
            mapping.key_for(TradingAction::OpenLong),
            Some(TradingKey::Z)
        );
        assert_eq!(mapping.action_for(TradingKey::A), None);
        mapping.assign(TradingAction::OpenLong, TradingKey::S);
        assert_eq!(
            mapping.key_for(TradingAction::CloseLong),
            Some(TradingKey::Z)
        );
    }

    #[test]
    fn keyboard_repeat_and_modified_keys_do_not_emit_actions() {
        let settings = TradingSettings::default();
        let event = egui::Event::Key {
            key: egui::Key::A,
            physical_key: None,
            pressed: true,
            repeat: true,
            modifiers: egui::Modifiers::NONE,
        };
        assert_eq!(hotkey_action(&event, &settings), None);
        let mut modified = event;
        if let egui::Event::Key {
            repeat, modifiers, ..
        } = &mut modified
        {
            *repeat = false;
            modifiers.ctrl = true;
        }
        assert_eq!(hotkey_action(&modified, &settings), None);
    }

    #[test]
    fn account_or_symbol_scope_change_clears_price_order_and_armed_action() {
        let mut state = TradeDockState::default();
        state.observe_scope(Some(TradingScope {
            venue: "Binance".to_owned(),
            trading_account_id: "account-a".to_owned(),
            symbol: "BTC/USDT".to_owned(),
        }));
        state
            .select_price(Decimal::new(100, 0))
            .unwrap_or_else(|_| unreachable!());
        state.selected_order_id = Some("order-1".to_owned());
        state.armed_action = Some(TradingAction::OpenLong);
        state.observe_scope(Some(TradingScope {
            venue: "Binance".to_owned(),
            trading_account_id: "account-a".to_owned(),
            symbol: "ETH/USDT".to_owned(),
        }));
        assert_eq!(state.selected_price, None);
        assert_eq!(state.selected_order_id, None);
        assert_eq!(state.armed_action, None);
    }
}
