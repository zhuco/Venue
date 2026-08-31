use crate::{
    account_client::{AccountAction, AccountClient, AccountResult},
    i18n::Language,
    model::AppModel,
    theme,
};
use eframe::egui::{self, RichText};
use venue_control_protocol::accounts::*;
use zeroize::{Zeroize, Zeroizing};

#[cfg(test)]
mod tests;

#[derive(Default)]
pub(crate) struct AccountCenter {
    pub session: Option<SessionResponse>,
    client: AccountClient,
    busy: bool,
    registering: bool,
    adding: bool,
    username: String,
    password: Zeroizing<String>,
    confirmation: Zeroizing<String>,
    label: String,
    api_key: Zeroizing<String>,
    api_secret: Zeroizing<String>,
    deleting: Option<String>,
    error: Option<AccountErrorCode>,
    next_refresh_ms: u64,
}

impl AccountCenter {
    pub fn clear(&mut self, model: &mut AppModel) {
        *self = Self::default();
        model.clear_account_session();
    }

    pub fn poll(&mut self, model: &mut AppModel, context: &egui::Context) -> bool {
        let events = self.client.drain().collect::<Vec<_>>();
        let mut reconnect = false;
        for result in events {
            self.busy = false;
            match result {
                Ok(AccountResult::Session(session, overview)) => {
                    self.session = Some(session);
                    model.apply_account_overview(overview);
                    self.error = None;
                    reconnect = true;
                }
                Ok(AccountResult::Overview(overview)) => {
                    let old = model.preferences.execution_account_id.clone();
                    model.apply_account_overview(overview);
                    self.error = None;
                    reconnect |= old != model.preferences.execution_account_id;
                }
                Ok(AccountResult::LoggedOut) => {
                    self.clear(model);
                    reconnect = true;
                }
                Err(code) => {
                    if code == AccountErrorCode::Unauthorized {
                        self.clear(model);
                        reconnect = true;
                    }
                    self.error = Some(code);
                }
            }
        }
        let now = now_ms();
        if !self.busy
            && let Some(id) = model.account_selection_requested.take()
        {
            self.submit(AccountAction::Select(id), model, context);
        }
        if self.session.as_ref().is_some_and(|s| s.expires_ms <= now) {
            self.clear(model);
            reconnect = true;
        }
        if self.session.is_some() && !self.busy && now >= self.next_refresh_ms {
            self.submit(AccountAction::Refresh, model, context);
        }
        reconnect
    }

    fn submit(&mut self, action: AccountAction, model: &AppModel, context: &egui::Context) {
        if self.busy {
            return;
        }
        self.busy = true;
        self.error = None;
        self.next_refresh_ms = now_ms().saturating_add(30_000);
        self.client.submit(
            model.preferences.endpoint.clone(),
            self.session.as_ref().map(|s| s.token.clone()),
            action,
            context.clone(),
        );
    }

    fn clear_form_secrets(&mut self) {
        self.password.zeroize();
        self.confirmation.zeroize();
        self.api_key.zeroize();
        self.api_secret.zeroize();
    }
}

pub(crate) fn show(
    context: &egui::Context,
    open: &mut bool,
    state: &mut AccountCenter,
    model: &mut AppModel,
) {
    if !*open {
        return;
    }
    let language = model.preferences.language;
    let mut visible = true;
    egui::Window::new(tr(language,"账户中心","Account center")).open(&mut visible)
        .resizable(false).collapsible(false).default_width(650.0).show(context,|ui|{
            ui.set_min_width(600.0);ui.spacing_mut().item_spacing=egui::vec2(10.0,10.0);
            ui.label(RichText::new(tr(language,"行情无需登录。交易账户和 API 绑定仅属于当前登录用户。","Market data needs no login. Trading accounts and API bindings belong to your user.")).color(theme::TEXT_SECONDARY));
            ui.separator();
            ui.add_enabled_ui(!state.busy,|ui|{if state.session.is_none(){show_login(ui,state,model);}else{show_accounts(ui,state,model);}});
            if state.busy{ui.horizontal(|ui|{ui.spinner();ui.label(tr(language,"正在处理，请稍候…","Working…"));});}
            if let Some(code)=state.error{ui.colored_label(theme::SELL,error_text(language,code));}
        });
    if !visible {
        state.clear_form_secrets();
        state.adding = false;
        state.deleting = None;
    }
    *open = visible;
}

fn show_login(ui: &mut egui::Ui, state: &mut AccountCenter, model: &AppModel) {
    let l = model.preferences.language;
    ui.horizontal(|ui| {
        if ui
            .selectable_label(!state.registering, tr(l, "登录", "Log in"))
            .clicked()
        {
            state.registering = false;
            state.clear_form_secrets();
        }
        if ui
            .selectable_label(state.registering, tr(l, "注册", "Register"))
            .clicked()
        {
            state.registering = true;
            state.clear_form_secrets();
        }
    });
    field(
        ui,
        tr(l, "用户名", "Username"),
        &mut state.username,
        false,
        64,
    );
    ui.small(tr(
        l,
        "3–64 位字母、数字或 . _ - @，不区分大小写",
        "3–64 letters, numbers or . _ - @; case-insensitive",
    ));
    field(
        ui,
        tr(l, "密码", "Password"),
        &mut state.password,
        true,
        128,
    );
    if state.registering {
        field(
            ui,
            tr(l, "确认密码", "Confirm password"),
            &mut state.confirmation,
            true,
            128,
        );
        ui.small(tr(
            l,
            "密码至少 15 个字符，可使用长口令。",
            "Use a passphrase of at least 15 characters.",
        ));
    }
    let enabled = !state.username.trim().is_empty()
        && !state.password.is_empty()
        && (!state.registering
            || (state.password.chars().count() >= 15 && *state.password == *state.confirmation));
    let caption = if state.registering {
        tr(l, "注册并登录", "Register and log in")
    } else {
        tr(l, "登录", "Log in")
    };
    if ui
        .add_enabled(enabled, egui::Button::new(caption))
        .clicked()
    {
        let request = LoginRequest {
            username: state.username.clone(),
            password: SecretValue::new(std::mem::take(&mut *state.password)),
        };
        state.confirmation.zeroize();
        state.submit(
            if state.registering {
                AccountAction::Register(request)
            } else {
                AccountAction::Login(request)
            },
            model,
            ui.ctx(),
        );
    }
}

fn show_accounts(ui: &mut egui::Ui, state: &mut AccountCenter, model: &AppModel) {
    let l = model.preferences.language;
    ui.horizontal(|ui| {
        if let Some(session) = &state.session {
            ui.strong(&session.user.username);
        }
        if ui.button(tr(l, "刷新", "Refresh")).clicked() {
            state.submit(AccountAction::Refresh, model, ui.ctx());
        }
        if ui.button(tr(l, "退出登录", "Log out")).clicked() {
            state.submit(AccountAction::Logout, model, ui.ctx());
        }
    });
    ui.horizontal(|ui| {
        ui.heading(tr(l, "交易所 API 管理", "Exchange API management"));
        if ui.button(tr(l, "＋ 添加 API", "+ Add API")).clicked() {
            state.adding = true;
            state.deleting = None;
            state.clear_form_secrets();
        }
    });
    if state.adding {
        show_add(ui, state, model);
        ui.separator();
    }
    let credentials = model
        .account_overview
        .as_ref()
        .map(|v| v.credentials.as_slice())
        .unwrap_or_default();
    if credentials.is_empty() {
        ui.label(tr(
            l,
            "尚未绑定 API。添加币安 API 后，验证并选择执行账户。",
            "No API is bound. Add a Binance API, verify it, then select the execution account.",
        ));
    }
    egui::ScrollArea::vertical().max_height(350.0).show(ui,|ui|{
        for credential in credentials{
            egui::Frame::group(ui.style()).show(ui,|ui|{
                ui.set_min_width(550.0);
                ui.horizontal(|ui|{ui.strong(&credential.label);ui.label("Binance");ui.monospace(&credential.masked_key);});
                ui.horizontal(|ui|{
                    let ready=credential.selectable(now_ms());
                    let status=if credential.verification==ApiVerificationState::Verified&&!ready{tr(l,"验证已过期","Verification expired")}else{verification_text(l,&credential.verification)};
                    ui.colored_label(if ready{theme::BUY}else{theme::WARNING},status);
                    ui.label(if ready{tr(l,"API 可访问 · 双向持仓","API reachable · Hedge mode")}else{tr(l,"API / 账户模式待验证","API / account mode requires verification")});
                });
                ui.small(node_status(l,model,credential,now_ms()));
                if let Some(mode)=&credential.account_mode{ui.small(mode);}
                if let Some(account)=&credential.trading_account_id{ui.small(format!("{}: {account}",tr(l,"交易账户","Trading account")));}
                ui.horizontal(|ui|{
                    if ui.button(tr(l,"验证 API","Verify API")).clicked(){state.submit(AccountAction::Verify(credential.credential_id.clone()),model,ui.ctx());}
                    let selected=model.account_overview.as_ref().and_then(|v|v.selected_credential_id.as_deref())==Some(credential.credential_id.as_str());
                    if ui.add_enabled(credential.selectable(now_ms())&&!selected,egui::Button::new(if selected{tr(l,"当前执行账户","Current execution account")}else{tr(l,"设为执行账户","Use for execution")})).clicked(){state.submit(AccountAction::Select(credential.credential_id.clone()),model,ui.ctx());}
                    if ui.button(tr(l,"删除绑定","Remove binding")).clicked(){state.deleting=Some(credential.credential_id.clone());state.password.zeroize();}
                });
                if state.deleting.as_deref()==Some(credential.credential_id.as_str()){
                    ui.colored_label(theme::WARNING,tr(l,"仅删除 Venue 绑定，不撤销币安 Key。存在持仓、挂单或运行账户时会拒绝删除。","Removes only the Venue binding, not the Binance key. Exposure, orders or a running account block removal."));
                    field(ui,tr(l,"确认登录密码","Confirm login password"),&mut state.password,true,128);
                    ui.horizontal(|ui|{
                        if ui.add_enabled(!state.password.is_empty(),egui::Button::new(tr(l,"确认删除","Confirm removal"))).clicked(){
                            let request=DeleteCredentialRequest{credential_id:credential.credential_id.clone(),password:SecretValue::new(std::mem::take(&mut *state.password))};
                            state.submit(AccountAction::Delete(request),model,ui.ctx());state.deleting=None;
                        }
                        if ui.button(tr(l,"取消","Cancel")).clicked(){state.deleting=None;state.password.zeroize();}
                    });
                }
            });
        }
    });
    ui.small(tr(l,"验证只读取交易所，不下单、不修改账户模式。选择账户不会启动策略。","Verification only reads exchange state; it never trades or changes account mode. Selecting an account does not start a strategy."));
}

fn show_add(ui: &mut egui::Ui, state: &mut AccountCenter, model: &AppModel) {
    let l = model.preferences.language;
    ui.strong(tr(
        l,
        "添加已有 Binance API · Portfolio Margin UM",
        "Add existing Binance API · Portfolio Margin UM",
    ));
    ui.small(tr(
        l,
        "需要读取和统一账户交易权限、双向持仓；请关闭提现权限。",
        "Requires reading, Portfolio Margin trading and Hedge mode; disable withdrawals.",
    ));
    field(ui, tr(l, "备注名称", "Label"), &mut state.label, false, 64);
    field(ui, "API Key", &mut state.api_key, true, 256);
    field(ui, "API Secret", &mut state.api_secret, true, 256);
    ui.small(tr(
        l,
        "密钥仅用于加密绑定，不保存在本机界面配置。",
        "Keys are submitted for encrypted storage, never saved in local UI preferences.",
    ));
    ui.horizontal(|ui| {
        let valid = !state.label.trim().is_empty()
            && state.api_key.len() >= 16
            && state.api_secret.len() >= 16;
        if ui
            .add_enabled(valid, egui::Button::new(tr(l, "保存绑定", "Save binding")))
            .clicked()
        {
            let request = BindCredentialRequest {
                label: std::mem::take(&mut state.label),
                api_key: SecretValue::new(std::mem::take(&mut *state.api_key)),
                api_secret: SecretValue::new(std::mem::take(&mut *state.api_secret)),
            };
            state.adding = false;
            state.submit(AccountAction::Bind(request), model, ui.ctx());
        }
        if ui.button(tr(l, "取消", "Cancel")).clicked() {
            state.adding = false;
            state.clear_form_secrets();
        }
    });
}

fn field(ui: &mut egui::Ui, label: &str, value: &mut String, password: bool, limit: usize) {
    ui.horizontal(|ui| {
        ui.add_sized([120.0, 30.0], egui::Label::new(label));
        let response = ui.add_sized(
            [410.0, 32.0],
            egui::TextEdit::singleline(value)
                .password(password)
                .char_limit(limit)
                .horizontal_align(egui::Align::LEFT)
                .vertical_align(egui::Align::Center),
        );
        // egui keeps undo text even for password fields. Do not retain secrets
        // in widget history after the frame or after the form is cleared.
        if password && let Some(mut state) = egui::TextEdit::load_state(ui.ctx(), response.id) {
            state.clear_undoer();
            state.store(ui.ctx(), response.id);
        }
    });
}
fn node_status(
    l: Language,
    model: &AppModel,
    credential: &CredentialSummary,
    now: u64,
) -> &'static str {
    use venue_control_protocol::HealthState;
    let snapshot = model.snapshot.as_ref().filter(|s| {
        model.snapshot_online
            && now
                .checked_sub(s.generated_ms)
                .is_some_and(|age| age <= 15_000)
    });
    let account = snapshot.and_then(|s| {
        s.accounts.iter().find(|a| {
            credential.trading_account_id.as_deref() == Some(a.trading_account_id.as_str())
        })
    });
    match account.map(|a| a.health) {
        Some(HealthState::Healthy) => {
            tr(l, "执行节点：报告正常", "Execution node: reports healthy")
        }
        Some(HealthState::Recovering) => tr(l, "执行节点：恢复中", "Execution node: recovering"),
        Some(HealthState::NeedsAttention) => {
            tr(l, "执行节点：需要处理", "Execution node: needs attention")
        }
        Some(HealthState::Stopped) => tr(l, "执行节点：已停止", "Execution node: stopped"),
        Some(HealthState::Unknown) | None => tr(
            l,
            "执行节点：未连接或暂无新鲜状态",
            "Execution node: disconnected or no fresh status",
        ),
    }
}
fn tr<'a>(language: Language, zh: &'a str, en: &'a str) -> &'a str {
    if language == Language::SimplifiedChinese {
        zh
    } else {
        en
    }
}
pub(crate) fn now_ms() -> u64 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
    #[cfg(target_arch = "wasm32")]
    {
        js_sys::Date::now() as u64
    }
}
fn verification_text(l: Language, state: &ApiVerificationState) -> &'static str {
    match state {
        ApiVerificationState::Unverified => tr(l, "未验证", "Unverified"),
        ApiVerificationState::Verified => tr(l, "验证通过", "Verified"),
        ApiVerificationState::InvalidCredentials => {
            tr(l, "密钥或 IP 授权无效", "Invalid key or IP authorization")
        }
        ApiVerificationState::PermissionDenied => {
            tr(l, "权限不符合要求", "Permissions do not meet requirements")
        }
        ApiVerificationState::ModeMismatch => tr(
            l,
            "账户或持仓模式不符合要求",
            "Account or position mode mismatch",
        ),
        ApiVerificationState::NetworkUnavailable => {
            tr(l, "无法完成验证", "Verification unavailable")
        }
        ApiVerificationState::AccountConflict => tr(l, "账户绑定冲突", "Account binding conflict"),
    }
}
fn error_text(l: Language, code: AccountErrorCode) -> &'static str {
    match code {
        AccountErrorCode::InvalidInput => tr(
            l,
            "输入不符合要求，或服务地址不是 HTTPS/本机地址。",
            "Invalid input or an insecure service address.",
        ),
        AccountErrorCode::InvalidLogin => {
            tr(l, "用户名或密码不正确。", "Incorrect username or password.")
        }
        AccountErrorCode::UsernameUnavailable => {
            tr(l, "用户名不可用。", "Username is unavailable.")
        }
        AccountErrorCode::Unauthorized => tr(
            l,
            "登录已失效，请重新登录。",
            "Session expired. Please log in again.",
        ),
        AccountErrorCode::Forbidden => tr(
            l,
            "没有此账户的访问权限。",
            "Access to this account is denied.",
        ),
        AccountErrorCode::NotFound => tr(
            l,
            "绑定不存在，请刷新。",
            "Binding not found. Refresh the list.",
        ),
        AccountErrorCode::Conflict => tr(
            l,
            "API 已绑定，或状态已变化，请刷新。",
            "API already bound, or state changed. Refresh the list.",
        ),
        AccountErrorCode::VerificationRequired => tr(
            l,
            "请先重新验证 API 和账户模式。",
            "Reverify the API and account mode first.",
        ),
        AccountErrorCode::AccountInUse => tr(
            l,
            "账户仍有风险、正在运行或无法确认安全状态，不能删除。",
            "Account has exposure, is running, or cannot be confirmed safe to remove.",
        ),
        AccountErrorCode::RateLimited => tr(
            l,
            "操作过于频繁，请稍后再试。",
            "Too many attempts. Try again later.",
        ),
        AccountErrorCode::Unavailable => tr(
            l,
            "账户服务暂不可用，请检查连接和服务配置。",
            "Account service unavailable. Check its connection and configuration.",
        ),
    }
}
