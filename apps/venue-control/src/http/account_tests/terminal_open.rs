use super::*;
use venue_control_protocol::kol::{
    ExecutorCommandOrigin, ExecutorCommandState, ExecutorCommandSummary, KOL_TERMINAL_ORDER_PATH,
    TERMINAL_SCHEMA_VERSION, TerminalAction, TerminalOrderKind, TerminalOrderRequest,
};

#[tokio::test]
async fn manual_opens_need_verified_ownership_not_private_projection() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let server = Server::start(&f).await?;
    let alice = f.service.register(login("manualalice"), now()).await?;
    let bob = f.service.register(login("manualbob"), now()).await?;
    let principal = f.service.authenticate(alice.token.expose(), now()).await?;
    let mut credential = f
        .service
        .bind_credential(&principal, binding(), now())
        .await?;
    let account = "00000000-0000-4000-8000-000000000721";
    sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)")
        .bind(account).bind(&alice.user.user_id).bind([21_u8;32].as_slice()).execute(&f.pool).await?;
    credential.trading_account_id = Some(account.into());
    credential.verification = ApiVerificationState::Verified;
    credential.verified_ms = Some(now());
    credential.dual_position = true;
    credential.api_reachable = true;
    sqlx::query("UPDATE venue_api_credentials SET trading_account_id=$1,verification_json=$2 WHERE credential_id=$3")
        .bind(account).bind(serde_json::to_value(&credential)?).bind(&credential.credential_id).execute(&f.pool).await?;
    let mut request = TerminalOrderRequest {
        schema_version: TERMINAL_SCHEMA_VERSION,
        request_id: "00000000-0000-4000-8000-000000000722".into(),
        credential_id: credential.credential_id.clone(),
        symbol: "DOGE/USDC".parse()?,
        action: TerminalAction::OpenLong,
        order_kind: TerminalOrderKind::LimitPostOnly,
        quote_notional: rust_decimal::Decimal::from(25),
        limit_price: Some(rust_decimal::Decimal::new(8663, 5)),
        close_quantity_cap: None,
        market_risk_confirmed: false,
    };
    let created = server
        .post(KOL_TERMINAL_ORDER_PATH, Some(&alice), &request)
        .send()
        .await?
        .error_for_status()?
        .json::<ExecutorCommandSummary>()
        .await?;
    assert_eq!(created.state, ExecutorCommandState::Pending);
    let replay = server
        .post(KOL_TERMINAL_ORDER_PATH, Some(&alice), &request)
        .send()
        .await?
        .error_for_status()?
        .json::<ExecutorCommandSummary>()
        .await?;
    assert_eq!(created.command_id, replay.command_id);
    code(
        server
            .post(KOL_TERMINAL_ORDER_PATH, Some(&bob), &request)
            .send()
            .await?,
        409,
        AccountErrorCode::VerificationRequired,
    )
    .await?;
    code(
        server
            .post(KOL_TERMINAL_ORDER_PATH, None, &request)
            .send()
            .await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    request.request_id = "00000000-0000-4000-8000-000000000723".into();
    request.action = TerminalAction::OpenShort;
    server
        .post(KOL_TERMINAL_ORDER_PATH, Some(&alice), &request)
        .send()
        .await?
        .error_for_status()?;
    request.action = TerminalAction::CloseLong;
    request.close_quantity_cap = Some(rust_decimal::Decimal::ONE);
    code(
        server
            .post(KOL_TERMINAL_ORDER_PATH, Some(&alice), &request)
            .send()
            .await?,
        409,
        AccountErrorCode::VerificationRequired,
    )
    .await?;
    let old = now() - 60_000;
    let projection = serde_json::json!({"fills_cursor":"fixture", "stream_healthy":true, "projection":{
        "schema_version":venue_control_protocol::kol::TERMINAL_PROJECTION_SCHEMA_VERSION,
        "credential_id":credential.credential_id,"trading_account_id":account,
        "observed_ms":old,"persisted_ms":old,"private_generation":1,"position_mode":"hedge",
        "positions":[],"open_orders":[],"fills":[],"assets":[],"position_history":[]
    }});
    sqlx::query("INSERT INTO venue_binance_account_projections (credential_id,owner_user_id,trading_account_id,observed_ms,persisted_ms,private_generation,projection_json) VALUES ($1,$2,$3,$4,$4,1,$5)")
        .bind(&credential.credential_id).bind(&alice.user.user_id).bind(account).bind(i64::try_from(old)?)
        .bind(projection).execute(&f.pool).await?;
    code(
        server
            .post(KOL_TERMINAL_ORDER_PATH, Some(&alice), &request)
            .send()
            .await?,
        409,
        AccountErrorCode::VerificationRequired,
    )
    .await?;
    request.action = TerminalAction::OpenLong;
    request.close_quantity_cap = None;
    request.request_id = "00000000-0000-4000-8000-000000000724".into();
    server
        .post(KOL_TERMINAL_ORDER_PATH, Some(&alice), &request)
        .send()
        .await?
        .error_for_status()?;
    let ledger = crate::kol_executor::BinanceCommandLedger::new(f.pool.clone());
    let command = ledger
        .claim_next(account, now())
        .await?
        .ok_or("command absent")?;
    assert_eq!(command.origin, ExecutorCommandOrigin::Terminal);
    let store = crate::executor_store::PgExecutorStore::new(f.pool.clone());
    assert!(store.terminal_open_credential_verified(&command).await?);
    sqlx::query("UPDATE venue_api_credentials SET verification_json=jsonb_set(verification_json,'{verification}','\"unverified\"'::jsonb) WHERE credential_id=$1")
        .bind(&credential.credential_id).execute(&f.pool).await?;
    assert!(!store.terminal_open_credential_verified(&command).await?);
    code(
        server
            .post(KOL_TERMINAL_ORDER_PATH, Some(&alice), &request)
            .send()
            .await?,
        409,
        AccountErrorCode::VerificationRequired,
    )
    .await?;
    server.stop().await?;
    f.cleanup().await
}
