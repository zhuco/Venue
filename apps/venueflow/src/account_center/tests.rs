use super::*;
use crate::model::Preferences;

fn credential() -> CredentialSummary {
    CredentialSummary {
        credential_id: "fixture-binding".into(),
        label: "主账户".into(),
        venue: venue_control_protocol::VenueId::Binance,
        masked_key: "••••1234".into(),
        trading_account_id: Some("00000000-0000-4000-8000-000000000001".into()),
        verification: ApiVerificationState::Verified,
        verified_ms: Some(now_ms()),
        expires_ms: Some(now_ms() + 300_000),
        api_reachable: true,
        dual_position: true,
        account_mode: Some("Portfolio Margin · UM".into()),
        has_exposure: Some(false),
    }
}
fn overview() -> AccountOverview {
    AccountOverview {
        user: UserSummary {
            user_id: "fixture-user".into(),
            username: "alice".into(),
        },
        credentials: vec![credential()],
        selected_credential_id: Some("fixture-binding".into()),
    }
}
fn session() -> SessionResponse {
    SessionResponse {
        user: overview().user,
        token: SecretValue::new("fixture-session-must-not-render".into()),
        expires_ms: now_ms() + 3_600_000,
    }
}

#[test]
fn clear_discards_old_replies_selection_secrets_and_keeps_public_preferences() {
    let mut model = AppModel::new(Preferences::default());
    let market = model.preferences.market_server;
    let symbol = model.preferences.selected_symbol.clone();
    model.apply_account_overview(overview());
    let mut state = AccountCenter {
        session: Some(session()),
        password: Zeroizing::new("private password".into()),
        api_secret: Zeroizing::new("private API secret".into()),
        ..Default::default()
    };
    let old_sender = state.client.test_sender();
    state.clear(&mut model);
    assert!(state.session.is_none() && state.password.is_empty() && state.api_secret.is_empty());
    assert!(
        old_sender
            .send(Ok(AccountResult::Session(session(), overview())))
            .is_err()
    );
    assert!(model.account_overview.is_none() && model.preferences.execution_account_id.is_none());
    assert_eq!(model.preferences.market_server, market);
    assert_eq!(model.preferences.selected_symbol, symbol);
}

#[test]
fn saved_selection_is_not_authentication_and_api_success_is_not_node_online() {
    let preferences = Preferences {
        execution_account_id: credential().trading_account_id,
        selected_instance: Some("old-strategy".into()),
        ..Default::default()
    };
    let mut model = AppModel::new(preferences);
    assert!(
        model.preferences.execution_account_id.is_none()
            && model.preferences.selected_instance.is_none()
    );
    model.apply_account_overview(overview());
    assert!(credential().selectable(now_ms()));
    assert_eq!(
        node_status(Language::SimplifiedChinese, &model, &credential(), now_ms()),
        "执行节点：未连接或暂无新鲜状态"
    );
    assert!(model.selected_trading_strategy().is_none());
}

fn collect_text(shape: &egui::Shape, output: &mut String) {
    match shape {
        egui::Shape::Text(text) => {
            output.push_str(&text.galley.job.text);
            output.push('\n');
        }
        egui::Shape::Vec(shapes) => {
            for shape in shapes {
                collect_text(shape, output);
            }
        }
        _ => (),
    }
}

#[test]
fn login_registration_binding_and_management_render_without_exposing_secrets() {
    for language in [Language::SimplifiedChinese, Language::English] {
        for page in 0..4 {
            let context = egui::Context::default();
            crate::theme::apply(&context);
            let mut model = AppModel::new(Preferences {
                language,
                ..Default::default()
            });
            let mut state = AccountCenter {
                registering: page == 1,
                password: Zeroizing::new("do-not-render-password".into()),
                api_key: Zeroizing::new("do-not-render-api-key".into()),
                api_secret: Zeroizing::new("do-not-render-api-secret".into()),
                ..Default::default()
            };
            if page >= 2 {
                state.session = Some(session());
                model.apply_account_overview(overview());
                state.adding = page == 3;
            }
            let mut open = true;
            let mut rendered = String::new();
            for _ in 0..2 {
                let mut output = context.run_ui(
                    egui::RawInput {
                        screen_rect: Some(egui::Rect::from_min_size(
                            egui::Pos2::ZERO,
                            egui::vec2(1100.0, 700.0),
                        )),
                        ..Default::default()
                    },
                    |ui| show(ui.ctx(), &mut open, &mut state, &mut model),
                );
                rendered.clear();
                // This headless test inspects shapes, without a renderer to consume textures.
                output.textures_delta.clear();
                for clipped in output.shapes {
                    collect_text(&clipped.shape, &mut rendered);
                }
            }
            for secret in [
                "do-not-render-password",
                "do-not-render-api-key",
                "do-not-render-api-secret",
                "fixture-session-must-not-render",
            ] {
                assert!(!rendered.contains(secret));
            }
            assert!(
                rendered.contains(if language == Language::SimplifiedChinese {
                    if page < 2 {
                        "账户登录"
                    } else {
                        "账户中心"
                    }
                } else {
                    if page < 2 {
                        "Account login"
                    } else {
                        "Account center"
                    }
                })
            );
            assert!(rendered.contains(match (page, language) {
                (0, Language::SimplifiedChinese) => "登录",
                (1, Language::SimplifiedChinese) => "确认密码",
                (2, Language::SimplifiedChinese) => "交易所 API 管理",
                (3, Language::SimplifiedChinese) => "保存绑定",
                (0, _) => "Log in",
                (1, _) => "Confirm password",
                (2, _) => "Exchange API management",
                _ => "Save binding",
            }));
        }
    }
}

#[test]
fn login_and_registration_keep_identical_centered_bounds() {
    for language in Language::ALL {
        for size in [egui::vec2(1100.0, 700.0), egui::vec2(850.0, 520.0)] {
            let context = egui::Context::default();
            crate::theme::apply(&context);
            let screen = egui::Rect::from_min_size(egui::Pos2::ZERO, size);
            let mut model = AppModel::new(Preferences {
                language,
                ..Default::default()
            });
            let mut state = AccountCenter::default();
            let mut bounds = Vec::new();
            for registering in [false, true, false] {
                state.registering = registering;
                let mut text = String::new();
                for _ in 0..3 {
                    let mut output = context.run_ui(
                        egui::RawInput {
                            screen_rect: Some(screen),
                            ..Default::default()
                        },
                        |ui| show(ui.ctx(), &mut true, &mut state, &mut model),
                    );
                    output.textures_delta.clear();
                    text.clear();
                    for shape in output.shapes {
                        collect_text(&shape.shape, &mut text);
                    }
                }
                let rect = context.memory(|m| m.area_rect(egui::Id::new("account-center")));
                assert!(rect.is_some());
                if let Some(rect) = rect {
                    assert!(screen.contains_rect(rect), "{rect:?} outside {screen:?}");
                    assert!((rect.center() - screen.center()).length() <= 2.0);
                    bounds.push(rect);
                }
                assert!(!text.contains("Market data needs no login"));
                assert!(!text.contains("行情无需登录"));
            }
            assert!(
                bounds
                    .windows(2)
                    .all(|pair| (pair[0].size() - pair[1].size()).length() <= 1.0)
            );
        }
    }
}

fn saved_login() -> LoginRequest {
    LoginRequest {
        username: "alice".into(),
        password: SecretValue::new("offline-fixture-password".into()),
    }
}

#[test]
fn restored_session_requires_server_identity_and_does_not_authorize_offline() {
    let context = egui::Context::default();
    let mut model = AppModel::new(Preferences::default());
    let mut state = AccountCenter {
        restoring: Some(session()),
        next_refresh_ms: u64::MAX,
        ..Default::default()
    };
    assert!(state.session.is_none() && model.account_overview.is_none());
    assert!(
        state
            .client
            .test_sender()
            .send(Err(AccountErrorCode::Unavailable))
            .is_ok()
    );
    assert!(!state.poll(&mut model, &context));
    assert!(state.session.is_none() && model.account_overview.is_none());
    assert!(state.restoring.is_some());
    assert!(
        state
            .client
            .test_sender()
            .send(Ok(AccountResult::Overview(overview())))
            .is_ok()
    );
    assert!(state.poll(&mut model, &context));
    assert!(state.session.is_some() && state.restoring.is_none());
    assert!(model.account_overview.is_some());
    assert!(model.selected_trading_strategy().is_none());
}

#[test]
fn wrong_user_or_expired_session_cannot_restore_an_account() {
    for mismatch in [false, true] {
        let mut candidate = session();
        if mismatch {
            candidate.user.user_id = "other-user".into();
        } else {
            candidate.expires_ms = 1;
        }
        let mut state = AccountCenter {
            restoring: Some(candidate),
            next_refresh_ms: u64::MAX,
            ..Default::default()
        };
        let mut model = AppModel::new(Preferences::default());
        assert!(
            state
                .client
                .test_sender()
                .send(Ok(AccountResult::Overview(overview())))
                .is_ok()
        );
        assert!(state.poll(&mut model, &egui::Context::default()));
        assert!(state.session.is_none() && state.restoring.is_none());
        assert!(model.account_overview.is_none());
    }
}

#[test]
fn failed_login_is_never_promoted_to_saved_password() {
    let mut state = AccountCenter {
        pending_login: Some(saved_login()),
        ..Default::default()
    };
    let mut model = AppModel::new(Preferences::default());
    assert!(
        state
            .client
            .test_sender()
            .send(Err(AccountErrorCode::InvalidLogin))
            .is_ok()
    );
    state.poll(&mut model, &egui::Context::default());
    assert!(state.saved_login.is_none() && state.pending_login.is_none());
}

#[cfg(target_os = "windows")]
#[test]
fn system_vault_mock_roundtrip_forget_password_and_invalidate_session() {
    let mut state = AccountCenter {
        vault: Some(Vault::fixture("https://control.example.com")),
        pending_login: Some(saved_login()),
        remember_password: true,
        next_refresh_ms: u64::MAX,
        ..Default::default()
    };
    let mut model = AppModel::new(Preferences::default());
    assert!(
        state
            .client
            .test_sender()
            .send(Ok(AccountResult::Session(session(), overview())))
            .is_ok()
    );
    assert!(state.poll(&mut model, &egui::Context::default()));
    assert!(!state.storage_error);
    let vault = state.vault.as_ref();
    let stored = vault.and_then(|v| v.load(now_ms()).ok());
    assert!(stored.is_some_and(|v| v.login.is_some() && v.session.is_some()));
    state.remember_password = false;
    state.remember_changed();
    let stored = state.vault.as_ref().and_then(|v| v.load(now_ms()).ok());
    assert!(stored.is_some_and(|v| v.login.is_none() && v.session.is_some()));
    state.clear(&mut model);
    let stored = state.vault.as_ref().and_then(|v| v.load(now_ms()).ok());
    assert!(stored.is_some_and(|v| v.login.is_none() && v.session.is_none()));
}

#[cfg(target_os = "windows")]
#[test]
fn expiry_and_logout_keep_remembered_password_but_never_auto_login() {
    let mut state = AccountCenter {
        vault: Some(Vault::fixture("https://control.example.com")),
        saved_login: Some(saved_login()),
        session: Some(session()),
        remember_password: true,
        ..Default::default()
    };
    state.persist();
    let expired = state.vault.as_ref().and_then(|v| v.load(u64::MAX).ok());
    assert!(expired.is_some_and(|v| v.login.is_some() && v.session.is_none()));
    let mut model = AppModel::new(Preferences::default());
    state.clear(&mut model);
    let stored = state.vault.as_ref().and_then(|v| v.load(now_ms()).ok());
    assert!(stored.is_some_and(|v| v.login.is_some() && v.session.is_none()));
    assert!(state.session.is_none() && state.restoring.is_none());
    assert!(!state.password.is_empty());
    assert!(!state.poll(&mut model, &egui::Context::default()));
    assert!(!state.busy);
}
