use super::*;
use std::sync::{Arc, Mutex};
use venue_control::executor_exchange::*;
use venue_control_protocol::kol::TerminalCancelRequest;
use venue_gateway_binance::BinanceCredentials;

#[derive(Clone)]
struct ReplaceExchange {
    calls: Arc<Mutex<Vec<(bool, ExecutionRequest)>>>,
    mode: u8,
}
impl ReplaceExchange {
    fn result(
        &self,
        request: &ExecutionRequest,
        read: bool,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        self.calls
            .lock()
            .map_err(|_| BinanceExecutionError::Unavailable)?
            .push((read, request.clone()));
        let cancel = matches!(request.order_kind, ExecutionOrderKind::CancelExact { .. });
        let state = if cancel && self.mode == 1 && !read {
            ExecutionReadback::Unknown
        } else if cancel && self.mode == 2 || !cancel && self.mode == 5 {
            ExecutionReadback::Rejected
        } else {
            ExecutionReadback::Reconciled
        };
        Ok(ExecutionOutcome {
            state,
            native_order_id: Some(if cancel { "old-order" } else { "new-order" }.into()),
            market_settlement: None,
            exchange_error_code: None,
            order_fact: (cancel && state == ExecutionReadback::Reconciled && self.mode != 4)
                .then_some(ExactOrderFact {
                    quantity: Decimal::from(10),
                    filled_quantity: Decimal::from(if self.mode == 3 { 10 } else { 4 }),
                    terminal: true,
                }),
        })
    }
}
impl BinanceExecution for ReplaceExchange {
    fn submit<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        _: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(async move { self.result(request, false) })
    }
    fn readback<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        _: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(async move { self.result(request, true) })
    }
    fn submit_grid_batch<'a>(
        &'a mut self,
        _: &'a GridBatchExecutionContext,
        _: &'a [ExecutionRequest],
        _: BinanceCredentials,
    ) -> BinanceGridBatchFuture<'a> {
        Box::pin(async {
            Err(GridBatchSubmitError::DefinitelyNotDispatched(
                BinanceExecutionError::Invalid,
            ))
        })
    }
}
impl BinanceActivationBaseline for ReplaceExchange {
    async fn activation_baseline(
        &mut self,
        _: &str,
        _: &std::collections::BTreeSet<venue_domain::Symbol>,
        _: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError> {
        Err(BinanceExecutionError::Invalid)
    }
}

#[tokio::test]
async fn terminal_replace_preserves_type_and_waits_for_final_cancel_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    for reducing in [false, true] {
        for tif in [
            venue_domain::LimitTimeInForce::PostOnly,
            venue_domain::LimitTimeInForce::Gtc,
        ] {
            for mode in 0..6 {
                eprintln!("replace case: {tif:?}, reducing={reducing}, mode={mode}");
                let fixture = Fixture::create(&url).await?;
                fixture.migrate_twice().await?;
                let service = AccountService::new_with_node_token(
                    fixture.pool.clone(),
                    CredentialCipher::from_key(&[9; 32])?,
                    None,
                )?;
                let session = service
                    .register(
                        LoginRequest {
                            username: "replace-owner".into(),
                            password: SecretValue::new("safe terminal password".into()),
                        },
                        test_now_ms()?,
                    )
                    .await?;
                let principal = service
                    .authenticate(session.token.expose(), test_now_ms()?)
                    .await?;
                let account = id(8972);
                let credential = id(8973);
                sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,'binance',$3)")
            .bind(&account).bind(&principal.user.user_id).bind(vec![72_u8;32]).execute(&fixture.pool).await?;
                sqlx::query("INSERT INTO venue_api_credentials(credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,created_ms) VALUES($1,$2,'replace',$3,'***',decode('00','hex'),$4,'{\"verification\":\"verified\"}'::jsonb,1)")
            .bind(&credential).bind(&principal.user.user_id).bind(vec![73_u8;32]).bind(&account).execute(&fixture.pool).await?;
                let symbol: venue_domain::Symbol = "BTC/USDT".parse()?;
                let now = test_now_ms()?;
                let source = ActiveProjectionSource {
                    kol_user_id: None,
                    owner_user_id: principal.user.user_id.clone(),
                    credential_id: credential.clone(),
                    trading_account_id: account.clone(),
                    symbols: [symbol.clone()].into_iter().collect(),
                    previous_fills_cursor: None,
                };
                let snapshot = SignedAccountSnapshot::complete_with_fills(
                    GatewayBinding::new(
                        VenueId::Binance,
                        GatewayMode::Live,
                        account.clone(),
                        symbol.clone(),
                    )?,
                    now,
                    1,
                    1,
                    1,
                    SignedAccountPositionMode::Hedge,
                    vec![venue_execution::SignedAccountOrderFact {
                        client_order_id: "old-client".into(),
                        venue_order_id: Some("old-order".into()),
                        symbol: symbol.clone(),
                        family: venue_domain::NativeOrderFamily::UmOrder,
                        side: if reducing {
                            OrderSide::Sell
                        } else {
                            OrderSide::Buy
                        },
                        position_side: PositionSide::Long,
                        quantity: Decimal::from(10),
                        limit_price: Some(Decimal::from(50000)),
                        time_in_force: Some(tif),
                        created_at_ms: Some(now),
                        reduce_only: false,
                        owner: None,
                        external: true,
                        state: Some(OrderState::PartiallyFilled),
                        filled_quantity: Some(Decimal::from(2)),
                    }],
                    vec![SignedAccountPositionFact {
                        symbol: symbol.clone(),
                        position_side: PositionSide::Long,
                        quantity: Decimal::from(10),
                        entry_price: Some(Decimal::from(50000)),
                        mark_price: Some(Decimal::from(50000)),
                    }],
                    Vec::new(),
                    "replace-cursor".into(),
                    Vec::new(),
                )?;
                BinancePrivateProjectionStore::new(fixture.pool.clone())
                    .persist(&source, &snapshot, now)
                    .await?;
                let request = TerminalCancelRequest {
                    schema_version: TERMINAL_SCHEMA_VERSION,
                    request_id: id(8974),
                    credential_id: credential.clone(),
                    symbol,
                    native_order_id: "old-order".into(),
                    replacement_price: Some(Decimal::from(49900)),
                };
                let response = service
                    .enqueue_terminal_cancel(&principal, request.clone(), test_now_ms()?)
                    .await?;
                assert_eq!(
                    response.order_kind,
                    if tif == venue_domain::LimitTimeInForce::PostOnly {
                        ExecutorOrderKind::LimitPostOnly
                    } else {
                        ExecutorOrderKind::LimitGtc
                    }
                );
                assert_eq!(
                    service
                        .enqueue_terminal_cancel(&principal, request.clone(), test_now_ms()?)
                        .await?
                        .command_id,
                    response.command_id
                );
                let mut changed = request.clone();
                changed.replacement_price = Some(Decimal::from(49800));
                assert_eq!(
                    service
                        .enqueue_terminal_cancel(&principal, changed, test_now_ms()?)
                        .await
                        .err()
                        .ok_or("changed replay accepted")?
                        .code,
                    AccountErrorCode::Conflict
                );
                let mut duplicate = request.clone();
                duplicate.request_id = id(8975);
                assert_eq!(
                    service
                        .enqueue_terminal_cancel(&principal, duplicate, test_now_ms()?)
                        .await
                        .err()
                        .ok_or("duplicate accepted")?
                        .code,
                    AccountErrorCode::Conflict
                );
                let calls = Arc::new(Mutex::new(Vec::new()));
                let make_runtime = || {
                    BinanceExecutorRuntime::new(
                        PgExecutorStore::new(fixture.pool.clone()),
                        ReplaceExchange {
                            calls: calls.clone(),
                            mode,
                        },
                        super::terminal_positions::FixtureSecrets,
                    )
                };
                let mut runtime = make_runtime();
                runtime.recover_once().await?;
                if mode == 1 {
                    assert_eq!(calls.lock().map_err(|_| "poison")?.len(), 1);
                    assert_eq!(
                        command_state(&fixture.pool, &response.command_id).await?,
                        "pending"
                    );
                    drop(runtime);
                    runtime = make_runtime();
                    sqlx::query("UPDATE venue_binance_commands SET next_reconcile_ms=1 WHERE command_state='reconcile_required'").execute(&fixture.pool).await?;
                    runtime.recover_once().await?;
                    runtime.recover_once().await?;
                }
                let seen = calls.lock().map_err(|_| "poison")?.clone();
                assert_eq!(
                    seen.iter()
                        .filter(|(read, r)| !read
                            && matches!(r.order_kind, ExecutionOrderKind::CancelExact { .. }))
                        .count(),
                    1
                );
                let opens: Vec<_> = seen
                    .iter()
                    .filter(|(_, r)| matches!(r.order_kind, ExecutionOrderKind::Limit { .. }))
                    .collect();
                if matches!(mode, 0 | 1 | 5) {
                    assert_eq!(opens.len(), 1);
                    assert!(
                        matches!(opens[0].1.order_kind, ExecutionOrderKind::Limit { quantity, price, time_in_force, side, position_side: PositionSide::Long, reducing: actual_reducing } if actual_reducing == reducing && side == if reducing {OrderSide::Sell} else {OrderSide::Buy} && quantity == Decimal::from(6) && price == Decimal::from(49900) && time_in_force == tif)
                    );
                    assert_eq!(
                        command_state(&fixture.pool, &response.command_id).await?,
                        if mode == 5 { "rejected" } else { "reconciled" }
                    );
                } else {
                    assert!(opens.is_empty());
                    assert_eq!(
                        command_state(&fixture.pool, &response.command_id).await?,
                        if mode == 4 { "pending" } else { "cancelled" }
                    );
                }
                if mode != 2 {
                    let mut stale_repeat = request;
                    stale_repeat.request_id = id(8976);
                    assert_eq!(
                        service
                            .enqueue_terminal_cancel(&principal, stale_repeat, test_now_ms()?)
                            .await
                            .err()
                            .ok_or("stale repeat accepted")?
                            .code,
                        AccountErrorCode::Conflict
                    );
                }
                fixture.cleanup().await?;
            }
        }
    }
    Ok(())
}
