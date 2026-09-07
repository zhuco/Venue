use super::*;
use venue_control_protocol::{accounts::*, kol::*};

pub(crate) fn overview(selected: u64) -> AccountOverview {
    AccountOverview {
        user: UserSummary {
            user_id: id(90),
            username: "fixture".into(),
        },
        selected_credential_id: Some(id(selected)),
        credentials: (1..=3)
            .map(|i| CredentialSummary {
                credential_id: id(i),
                label: format!("account {i}"),
                venue: VenueId::Binance,
                masked_key: "***".into(),
                trading_account_id: Some(id(i + 10)),
                verification: ApiVerificationState::Verified,
                verified_ms: Some(1),
                expires_ms: None,
                api_reachable: true,
                dual_position: true,
                account_mode: None,
                has_exposure: Some(false),
                equity: None,
                available_margin: None,
                balance_observed_ms: None,
            })
            .collect(),
    }
}

pub(crate) fn id(i: u64) -> String {
    format!("00000000-0000-4000-8000-{i:012}")
}

pub(crate) fn model() -> AppModel {
    let mut model = AppModel::new(Default::default());
    model.apply_account_overview(overview(1));
    model
}

pub(crate) fn projection(i: u64) -> TerminalAccountProjection {
    let now = crate::account_center::now_ms();
    TerminalAccountProjection {
        schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
        credential_id: id(i),
        trading_account_id: id(i + 10),
        observed_ms: now,
        persisted_ms: now,
        private_generation: 1,
        position_mode: TerminalPositionMode::Hedge,
        positions: vec![],
        position_history: vec![],
        open_orders: vec![],
        conditional_orders: vec![],
        fills: vec![],
        assets: vec![],
    }
}

pub(crate) fn receipt(i: u64) -> ExecutorCommandSummary {
    ExecutorCommandSummary {
        command_id: id(80),
        request_id: Some(id(81)),
        origin: ExecutorCommandOrigin::Terminal,
        phase: ExecutorCommandPhase::Open,
        trading_account_id: id(i + 10),
        symbol: "BTC/USDC".parse().unwrap(),
        position_side: Some(venue_domain::PositionSide::Long),
        order_side: Some(venue_domain::OrderSide::Buy),
        order_kind: ExecutorOrderKind::LimitPostOnly,
        requested_quantity: Some(1.into()),
        limit_price: Some(100.into()),
        state: ExecutorCommandState::Rejected,
        native_order_id: None,
        created_ms: 1,
        updated_ms: 1,
        sanitized_error_code: Some("binance_-2019".into()),
    }
}

fn late_result(event: ClientEvent, return_to_a: bool) {
    let mut model = model();
    let old = model.confirmed_account_scope().unwrap();
    model.begin_account_selection(id(2));
    model.apply_account_overview(overview(2));
    if return_to_a {
        model.begin_account_selection(id(1));
        model.apply_account_overview(overview(1));
    }
    let current = model.confirmed_account_scope().unwrap();
    let selected = if return_to_a { 1 } else { 2 };
    model
        .execution
        .apply_private(Some(projection(selected)), &mut model.trade_dock);
    model.execution.terminal_request_id = Some("current".into());
    let before = format!("{:?}", model.execution);
    assert!(!model.accept_account_event(&old, &event));
    assert!(!model.apply_account_event(&old, event));
    assert_eq!(before, format!("{:?}", model.execution));
    assert!(model.notices.is_empty());
    assert_eq!(model.confirmed_account_scope(), Some(current));
}

#[test]
fn selection_late_projection_success() {
    late_result(
        ClientEvent::TerminalAccountProjection {
            credential_id: id(1),
            projection: Some(projection(1)),
        },
        false,
    );
    late_result(
        ClientEvent::TerminalAccountProjection {
            credential_id: id(1),
            projection: Some(projection(1)),
        },
        true,
    );
}
#[test]
fn selection_late_unavailable_and_empty() {
    for event in [
        ClientEvent::TerminalAccountUnavailable {
            credential_id: id(1),
            message: "old error".into(),
        },
        ClientEvent::TerminalAccountProjection {
            credential_id: id(1),
            projection: None,
        },
    ] {
        late_result(event.clone(), false);
        late_result(event, true);
    }
}
#[test]
fn selection_late_401_and_a_b_a() {
    late_result(ClientEvent::SessionExpired, false);
    late_result(ClientEvent::SessionExpired, true);
    late_result(
        ClientEvent::TerminalAccountProjection {
            credential_id: id(1),
            projection: Some(projection(1)),
        },
        true,
    );
    late_result(
        ClientEvent::TerminalAccountProjection {
            credential_id: id(1),
            projection: None,
        },
        true,
    );
    late_result(
        ClientEvent::TerminalAccountUnavailable {
            credential_id: id(1),
            message: "first A".into(),
        },
        true,
    );
}
#[test]
fn selection_late_history_receipt_and_errors() {
    for again in [false, true] {
        for event in [
            ClientEvent::TerminalExecutions(vec![]),
            ClientEvent::TerminalExecutions(vec![receipt(1)]),
            ClientEvent::TerminalExecutionUpdated(receipt(1)),
            ClientEvent::TerminalExecutionsUnavailable("old history".into()),
            ClientEvent::TerminalSubmissionUnavailable {
                request_id: "current".into(),
                message: "old receipt".into(),
                definitely_not_submitted: true,
            },
        ] {
            late_result(event, again);
        }
    }
}

#[test]
fn selection_pending_failed_and_rapid_choices_stay_closed() {
    let mut model = model();
    model.trade_dock.select_price(100.into(), 1.0).unwrap();
    model.trade_dock.armed_action = Some(venue_control_protocol::TradingAction::OpenLong);
    model.trade_dock.selected_order_id = Some("old".into());
    model.begin_account_selection(id(2));
    assert!(model.confirmed_account_scope().is_none());
    assert!(model.trade_dock.selected_price.is_none());
    assert!(model.trade_dock.price_input.is_empty());
    assert!(model.trade_dock.armed_action.is_none());
    assert!(model.trade_dock.selected_order_id.is_none());
    // Failed selection followed by a refresh of the old server choice cannot re-arm A.
    model.apply_account_overview(overview(1));
    assert!(model.confirmed_account_scope().is_none());
    model.begin_account_selection(id(3));
    model.apply_account_overview(overview(2));
    assert!(model.confirmed_account_scope().is_none());
    let mut wrong_account = overview(3);
    wrong_account.credentials[2].trading_account_id = Some(id(99));
    model.apply_account_overview(wrong_account);
    assert!(model.confirmed_account_scope().is_none());
    model.apply_account_overview(overview(3));
    assert_eq!(
        model.confirmed_account_scope().unwrap().credential_id,
        id(3)
    );
    assert!(!model.execution.private_ready(
        model.preferences.execution_account_id.as_deref(),
        crate::account_center::now_ms()
    ));
    assert!(model.trade_dock.armed_action.is_none());
}

#[test]
fn selection_requires_all_four_scope_dimensions() {
    let mut model = model();
    let scope = model.confirmed_account_scope().unwrap();
    for dimension in 0..4 {
        let mut wrong = scope.clone();
        match dimension {
            0 => wrong.generation += 1,
            1 => wrong.credential_id = id(2),
            2 => wrong.trading_account_id = id(12),
            _ => wrong.venue = VenueId::Bybit,
        }
        assert!(!model.apply_account_event(&wrong, ClientEvent::SessionExpired));
    }
    let mut wrong = projection(1);
    wrong.trading_account_id = id(12);
    model.apply_account_event(
        &scope,
        ClientEvent::TerminalAccountProjection {
            credential_id: id(1),
            projection: Some(wrong),
        },
    );
    assert!(model.execution.private_projection.is_none());
    assert!(model.apply_account_event(&scope, ClientEvent::SessionExpired));
}
