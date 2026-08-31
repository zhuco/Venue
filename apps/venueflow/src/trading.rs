use eframe::egui;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_control_protocol::{
    StrategySummary, TradeIntent, TradingAction, TradingOrderType, TradingTimeInForce,
};

pub const SIZE_PRESET_COUNT: usize = 5;
const PRICE_HIGHLIGHT_DURATION: std::time::Duration = std::time::Duration::from_secs(2);
pub const DEFAULT_PRICE_VALIDITY_SECONDS: u16 = 10;
const MAX_PRICE_VALIDITY_SECONDS: u16 = 300;

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
    pub price_validity_seconds: u16,
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
            price_validity_seconds: DEFAULT_PRICE_VALIDITY_SECONDS,
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
            && (1..=MAX_PRICE_VALIDITY_SECONDS).contains(&self.price_validity_seconds)
    }

    pub fn normalize_price_validity(&mut self) {
        if !(1..=MAX_PRICE_VALIDITY_SECONDS).contains(&self.price_validity_seconds) {
            self.price_validity_seconds = DEFAULT_PRICE_VALIDITY_SECONDS;
        }
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
    price_selected_at: Option<f64>,
    display_symbol: String,
    scope: Option<TradingScope>,
}

impl TradeDockState {
    pub fn observe_scope(&mut self, symbol: &str, scope: Option<TradingScope>) {
        // The displayed symbol can change even when both trading scopes are unavailable.
        if self.display_symbol != symbol || self.scope != scope {
            self.clear_selection();
            self.display_symbol = symbol.to_owned();
            self.scope = scope;
        }
    }

    pub fn select_price(&mut self, price: Decimal, now: f64) -> Result<(), TradePlanError> {
        if price <= Decimal::ZERO {
            return Err(TradePlanError::InvalidPrice);
        }
        self.selected_price = Some(price);
        self.price_selected_at = Some(now);
        self.armed_action = None;
        Ok(())
    }

    pub fn price_remaining_seconds(&self, now: f64, validity_seconds: u16) -> Option<f64> {
        self.selected_price?;
        let elapsed = now - self.price_selected_at?;
        let duration = f64::from(validity_seconds);
        if !(1..=MAX_PRICE_VALIDITY_SECONDS).contains(&validity_seconds)
            || !(0.0..duration).contains(&elapsed)
        {
            return None;
        }
        Some(duration - elapsed)
    }

    pub fn expire_price(&mut self, now: f64, validity_seconds: u16) {
        if self.selected_price.is_some()
            && self
                .price_remaining_seconds(now, validity_seconds)
                .is_none()
        {
            self.clear_price();
        }
    }

    // Brief chart feedback does not extend the selected limit price's validity.
    pub fn highlighted_price(&self, context: &egui::Context) -> Option<Decimal> {
        let now = context.input(|input| input.time);
        let elapsed = now - self.price_selected_at?;
        let duration = PRICE_HIGHLIGHT_DURATION.as_secs_f64();
        if !(0.0..duration).contains(&elapsed) {
            return None;
        }
        // Re-arm on every visible frame so an earlier input repaint cannot consume expiry.
        context.request_repaint_after(std::time::Duration::from_secs_f64(duration - elapsed));
        self.selected_price
    }

    fn clear_price(&mut self) {
        self.selected_price = None;
        self.price_selected_at = None;
        self.armed_action = None;
    }

    pub fn clear_selection(&mut self) {
        self.clear_price();
        self.selected_order_id = None;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TradePlanError {
    #[error("select a chart or order-book price before placing an order")]
    MissingPrice,
    #[error("selected price has expired; select a new chart or order-book price")]
    ExpiredPrice,
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
    now: f64,
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
    if state
        .price_remaining_seconds(now, settings.price_validity_seconds)
        .is_none()
    {
        return Err(TradePlanError::ExpiredPrice);
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
pub fn executable_actions() -> [TradingAction; 11] {
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
    ]
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum SettingsPage {
    #[default]
    Orders,
    Hotkeys,
    Market,
}

pub fn show_settings(context: &egui::Context, open: &mut bool, model: &mut crate::model::AppModel) {
    use crate::{
        i18n::{TextKey, text},
        theme,
    };
    if !*open {
        return;
    }
    let language = model.preferences.language;
    let page_id = egui::Id::new("trading-settings-page");
    let mut page = context.data(|data| data.get_temp::<SettingsPage>(page_id).unwrap_or_default());
    let viewport = context.content_rect().size();
    let width = (viewport.x - 80.0).clamp(320.0, 720.0);
    let mut close = false;
    let modal = egui::Modal::new(egui::Id::new("trading-settings"))
        .frame(
            egui::Frame::new()
                .fill(theme::BG_SECONDARY)
                .stroke(egui::Stroke::new(1.0, theme::DIVIDER))
                .corner_radius(10)
                .inner_margin(20),
        )
        .show(context, |ui| {
            let content_top = ui.cursor().top();
            ui.set_width(width);
            ui.spacing_mut().item_spacing = egui::vec2(12.0, 10.0);
            ui.spacing_mut().interact_size.y = 30.0;
            ui.visuals_mut().widgets.inactive.weak_bg_fill = theme::DIVIDER;
            ui.visuals_mut().widgets.inactive.bg_fill = theme::DIVIDER;
            ui.horizontal(|ui| {
                ui.label(
                    egui::RichText::new(text(language, TextKey::TradingSettings))
                        .size(19.0)
                        .strong(),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    close = ui
                        .add(egui::Button::new(egui::RichText::new("×").size(23.0)).frame(false))
                        .clicked();
                });
            });
            ui.horizontal(|ui| {
                for (candidate, key) in [
                    (SettingsPage::Orders, TextKey::OrderParameters),
                    (SettingsPage::Hotkeys, TextKey::TradingHotkeys),
                    (SettingsPage::Market, TextKey::DisplayCadence),
                ] {
                    let selected = page == candidate;
                    let response = ui.add_sized(
                        [((width - 24.0) / 3.0).min(180.0), 34.0],
                        egui::Button::new(
                            egui::RichText::new(text(language, key)).size(14.0).color(
                                if selected {
                                    theme::BRAND
                                } else {
                                    theme::TEXT_SECONDARY
                                },
                            ),
                        )
                        .frame(false),
                    );
                    if selected {
                        ui.painter().line_segment(
                            [response.rect.left_bottom(), response.rect.right_bottom()],
                            egui::Stroke::new(2.0, theme::BRAND),
                        );
                    }
                    if response.clicked() {
                        page = candidate;
                    }
                }
            });
            ui.separator();
            let body_height =
                (viewport.y - 144.0 - (ui.cursor().top() - content_top)).clamp(80.0, 326.0);
            egui::ScrollArea::vertical()
                .id_salt(("trade-settings-page", format!("{page:?}")))
                .auto_shrink([false, false])
                .max_height(body_height)
                .show(ui, |ui| {
                    ui.set_min_height(body_height);
                    match page {
                        SettingsPage::Orders => order_settings(ui, model),
                        SettingsPage::Hotkeys => hotkey_settings(ui, model),
                        SettingsPage::Market => market_display_settings(ui, model),
                    }
                });
            ui.separator();
            egui::containers::Sides::new()
                .shrink_left()
                .height(34.0)
                .show(
                    ui,
                    |ui| {
                        ui.label(
                            egui::RichText::new(text(language, TextKey::SettingsImmediate))
                                .size(11.0)
                                .color(theme::TEXT_SECONDARY),
                        );
                    },
                    |ui| {
                        if ui
                            .add_sized(
                                [100.0, 34.0],
                                egui::Button::new(
                                    egui::RichText::new(text(language, TextKey::Done))
                                        .strong()
                                        .color(theme::BG_PRIMARY),
                                )
                                .fill(theme::BRAND)
                                .corner_radius(5),
                            )
                            .clicked()
                        {
                            close = true;
                        }
                    },
                );
        });
    context.data_mut(|data| data.insert_temp(page_id, page));
    if close || modal.should_close() {
        *open = false;
    }
}

fn form_row(ui: &mut egui::Ui, title: &str, add: impl FnOnce(&mut egui::Ui)) {
    ui.horizontal(|ui| {
        ui.add_sized(
            [(ui.available_width() * 0.45).min(205.0), 30.0],
            egui::Label::new(title).halign(egui::Align::Min),
        );
        add(ui);
    });
}

fn section_label(ui: &mut egui::Ui, title: &str) {
    ui.label(egui::RichText::new(title).strong().size(13.0));
}

fn order_settings(ui: &mut egui::Ui, model: &mut crate::model::AppModel) {
    use crate::{
        i18n::{TextKey, text},
        theme,
    };
    let language = model.preferences.language;
    if !model.preferences.trading.validate() {
        ui.colored_label(theme::SELL, text(language, TextKey::InvalidSizePreset));
    }
    let quote = model
        .preferences
        .selected_symbol
        .split_once('/')
        .map_or("—", |(_, quote)| quote)
        .to_owned();
    ui.columns(2, |columns| {
        section_label(&mut columns[0], text(language, TextKey::OrderParameters));
        form_row(&mut columns[0], text(language, TextKey::OrderType), |ui| {
            ui.label(
                egui::RichText::new(text(language, TextKey::LimitOrder)).color(theme::TEXT_PRIMARY),
            );
        });
        form_row(&mut columns[0], "Time in force", |ui| {
            ui.label("GTC");
        });
        form_row(&mut columns[0], "Post Only", |ui| {
            ui.checkbox(
                &mut model.preferences.trading.post_only,
                text(language, TextKey::Enable),
            );
        });
        form_row(&mut columns[0], text(language, TextKey::SizeUnit), |ui| {
            ui.monospace(&quote);
        });
        form_row(
            &mut columns[0],
            text(language, TextKey::PriceValidity),
            |ui| {
                if ui
                    .add(
                        egui::DragValue::new(&mut model.preferences.trading.price_validity_seconds)
                            .range(1..=MAX_PRICE_VALIDITY_SECONDS)
                            .speed(1.0)
                            .suffix(" s"),
                    )
                    .on_hover_text(text(language, TextKey::PriceValidityHint))
                    .changed()
                {
                    model.trade_dock.clear_price();
                }
            },
        );
        columns[0].add_space(12.0);
        columns[0].label(
            egui::RichText::new(text(language, TextKey::OrderSettingsHint))
                .size(11.0)
                .color(theme::TEXT_SECONDARY),
        );
        section_label(&mut columns[1], text(language, TextKey::SizePresets));
        for index in 0..SIZE_PRESET_COUNT {
            let title = format!("{} {}", text(language, TextKey::Preset), index + 1);
            form_row(&mut columns[1], &title, |ui| {
                let value = &mut model.preferences.trading.size_presets[index];
                let mut numeric = value.to_string().parse::<f64>().unwrap_or(25.0);
                if ui
                    .add(
                        egui::DragValue::new(&mut numeric)
                            .range(0.01..=1_000_000_000.0)
                            .speed(1.0)
                            .max_decimals(2)
                            .suffix(format!(" {quote}")),
                    )
                    .changed()
                    && let Some(decimal) = Decimal::from_f64_retain(numeric)
                {
                    *value = decimal;
                }
            });
        }
        columns[1].add_space(12.0);
        columns[1].label(
            egui::RichText::new(text(language, TextKey::PresetHint))
                .size(11.0)
                .color(theme::TEXT_SECONDARY),
        );
    });
}

fn hotkey_settings(ui: &mut egui::Ui, model: &mut crate::model::AppModel) {
    use crate::i18n::{TextKey, text};
    let language = model.preferences.language;
    ui.checkbox(
        &mut model.preferences.trading.hotkeys_enabled,
        text(language, TextKey::EnableHotkeys),
    );
    ui.add_space(4.0);
    let actions = executable_actions();
    ui.columns(2, |columns| {
        for (index, action) in actions.into_iter().enumerate() {
            let column = usize::from(index >= 6);
            let name = crate::trade_dock::action_name(language, action);
            let title = match action {
                TradingAction::SelectSizePreset(index) => format!("{name} {}", index + 1),
                _ => name.to_owned(),
            };
            form_row(&mut columns[column], &title, |ui| {
                if let Some(current) = model.preferences.trading.hotkeys.key_for(action) {
                    let mut selected = current;
                    egui::ComboBox::from_id_salt(("trading-hotkey", action))
                        .width(85.0)
                        .selected_text(current.label())
                        .show_ui(ui, |ui| {
                            for key in TradingKey::ALL {
                                ui.selectable_value(&mut selected, key, key.label());
                            }
                        });
                    if selected != current {
                        model.preferences.trading.hotkeys.assign(action, selected);
                    }
                }
            });
        }
    });
}

fn market_display_settings(ui: &mut egui::Ui, model: &mut crate::model::AppModel) {
    use crate::{
        i18n::{TextKey, text},
        theme,
    };
    let language = model.preferences.language;
    section_label(ui, text(language, TextKey::DisplayCadence));
    ui.label(
        egui::RichText::new(text(language, TextKey::DisplayCadenceHint))
            .size(12.0)
            .color(theme::TEXT_SECONDARY),
    );
    ui.add_space(12.0);
    let settings = &mut model.preferences.trading;
    for (key, cadence) in [
        (TextKey::OrderBook, &mut settings.book_cadence),
        (TextKey::RecentTrades, &mut settings.tape_cadence),
        (TextKey::CandleCadence, &mut settings.chart_cadence),
    ] {
        form_row(ui, text(language, key), |ui| {
            egui::ComboBox::from_id_salt(format!("display-{key:?}"))
                .width(120.0)
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
        });
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
            .select_price(Decimal::new(20, 0), 1.0)
            .unwrap_or_else(|_| unreachable!());
        let intent = build_trade_intent(
            &strategy(Decimal::new(3, 0), Decimal::new(9, 0)),
            &settings,
            &state,
            TradingAction::CloseLong,
            2.0,
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
                2.0,
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
            2.0,
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
        assert_eq!(settings.hotkeys.action_for(TradingKey::Escape), None);
        assert_eq!(settings.hotkeys.action_for(TradingKey::Space), None);
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
        state.observe_scope(
            "BTC/USDT",
            Some(TradingScope {
                venue: "Binance".to_owned(),
                trading_account_id: "account-a".to_owned(),
                symbol: "BTC/USDT".to_owned(),
            }),
        );
        state
            .select_price(Decimal::new(100, 0), 1.0)
            .unwrap_or_else(|_| unreachable!());
        state.selected_order_id = Some("order-1".to_owned());
        state.armed_action = Some(TradingAction::OpenLong);
        state.observe_scope(
            "ETH/USDT",
            Some(TradingScope {
                venue: "Binance".to_owned(),
                trading_account_id: "account-a".to_owned(),
                symbol: "ETH/USDT".to_owned(),
            }),
        );
        assert_eq!(state.selected_price, None);
        assert_eq!(state.selected_order_id, None);
        assert_eq!(state.armed_action, None);
    }

    #[test]
    fn switching_symbols_without_a_strategy_requires_a_new_price_for_every_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut state = TradeDockState::default();
        state.observe_scope("BTC/USDT", None);
        state.select_price(Decimal::from(100), 1.0)?;
        state.observe_scope("ETH/USDT", None);
        let mut target = strategy(Decimal::ONE, Decimal::ONE);
        target.symbol = "ETH/USDT".parse()?;
        for action in [
            TradingAction::OpenLong,
            TradingAction::OpenShort,
            TradingAction::CloseLong,
            TradingAction::CloseShort,
        ] {
            assert_eq!(
                build_trade_intent(&target, &TradingSettings::default(), &state, action, 2.0),
                Err(super::TradePlanError::MissingPrice),
            );
        }
        state.select_price(Decimal::from(20), 2.0)?;
        let intent = build_trade_intent(
            &target,
            &TradingSettings::default(),
            &state,
            TradingAction::OpenLong,
            2.0,
        )?;
        assert_eq!(intent.selected_price, Some(Decimal::from(20)));
        Ok(())
    }

    #[test]
    fn expired_prices_cannot_build_orders_even_before_the_next_ui_refresh()
    -> Result<(), Box<dyn std::error::Error>> {
        let settings = TradingSettings {
            price_validity_seconds: 3,
            ..Default::default()
        };
        let mut state = TradeDockState::default();
        state.select_price(Decimal::from(20), 1.0)?;
        let target = strategy(Decimal::ONE, Decimal::ONE);
        for action in [
            TradingAction::OpenLong,
            TradingAction::OpenShort,
            TradingAction::CloseLong,
            TradingAction::CloseShort,
        ] {
            assert!(build_trade_intent(&target, &settings, &state, action, 3.999).is_ok());
            for now in [4.0, 500.0, 0.0, f64::NAN, f64::INFINITY] {
                assert_eq!(
                    build_trade_intent(&target, &settings, &state, action, now),
                    Err(super::TradePlanError::ExpiredPrice),
                );
            }
        }
        state.selected_order_id = Some("working-order".to_owned());
        state.armed_action = Some(TradingAction::OpenLong);
        state.expire_price(4.0, settings.price_validity_seconds);
        assert_eq!(state.selected_price, None);
        assert_eq!(state.armed_action, None);
        let cancel = build_trade_intent(
            &target,
            &settings,
            &state,
            TradingAction::CancelSelectedOrder,
            4.0,
        )?;
        assert_eq!(cancel.selected_order_id.as_deref(), Some("working-order"));
        state.select_price(Decimal::from(20), 5.0)?;
        assert!(
            build_trade_intent(&target, &settings, &state, TradingAction::OpenLong, 7.999).is_ok()
        );
        state.expire_price(8.0, settings.price_validity_seconds);
        state.expire_price(8.0, 300);
        assert_eq!(state.selected_price, None);
        Ok(())
    }
}
