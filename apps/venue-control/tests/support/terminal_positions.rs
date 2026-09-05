use super::*;
use std::sync::{Arc, Mutex};
use venue_control::executor_exchange::*;
use venue_control::executor_runtime::ExecutorCredentials;
use venue_control_protocol::{kol::TerminalPosition, terminal_position::*};
use venue_gateway_binance::BinanceCredentials;

#[derive(Clone, Copy, Debug)]
enum ResultMode {
    Filled,
    Unknown,
    Rejected,
    Partial,
    NotFlat,
    Revoked,
    Stale,
    UnpreparedRestart,
    OpenRejected,
}

#[derive(Clone)]
struct PositionExchange {
    mode: ResultMode,
    calls: Arc<Mutex<Vec<(bool, bool, Decimal, String)>>>,
}
impl BinanceExecution for PositionExchange {
    fn submit<'a>(
        &'a mut self,
        _: &'a ExecutionRequest,
        _: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(async { Err(BinanceExecutionError::Invalid) })
    }
    fn readback<'a>(
        &'a mut self,
        _: &'a ExecutionRequest,
        _: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(async { Err(BinanceExecutionError::Invalid) })
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
    fn terminal_market<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        context: &'a TerminalMarketContext,
        _: BinanceCredentials,
        read_only: bool,
    ) -> TerminalMarketFuture<'a> {
        Box::pin(async move {
            let ExecutionOrderKind::Market {
                reducing,
                position_side,
                side,
                ..
            } = request.order_kind
            else {
                return Err(BinanceExecutionError::Invalid);
            };
            assert_eq!(
                (position_side, side),
                if reducing {
                    (PositionSide::Long, OrderSide::Sell)
                } else {
                    (PositionSide::Short, OrderSide::Sell)
                }
            );
            self.calls
                .lock()
                .map_err(|_| BinanceExecutionError::Unavailable)?
                .push((
                    read_only,
                    reducing,
                    context.quantity,
                    request.client_order_id.clone(),
                ));
            let state = match (self.mode, read_only) {
                (ResultMode::OpenRejected, _) if !reducing => ExecutionReadback::Rejected,
                (ResultMode::Unknown, false) if reducing => ExecutionReadback::Unknown,
                (ResultMode::Rejected | ResultMode::Partial, _) if reducing => {
                    ExecutionReadback::Rejected
                }
                _ => ExecutionReadback::Reconciled,
            };
            let positions = [PositionSide::Long, PositionSide::Short]
                .into_iter()
                .map(|side| TerminalPosition {
                    symbol: request.symbol.clone(),
                    position_side: side,
                    quantity: if side == PositionSide::Long {
                        if matches!(self.mode, ResultMode::NotFlat | ResultMode::Partial) {
                            Decimal::new(1, 3)
                        } else {
                            Decimal::ZERO
                        }
                    } else if reducing {
                        Decimal::new(2, 3)
                    } else {
                        Decimal::new(2, 3) + context.quantity
                    },
                    entry_price: None,
                    mark_price: None,
                })
                .collect();
            Ok(TerminalMarketResult {
                outcome: ExecutionOutcome {
                    market_settlement: None,
                    order_fact: None,
                    exchange_error_code: (matches!(self.mode, ResultMode::Rejected))
                        .then_some(-2022),
                    state,
                    native_order_id: (state != ExecutionReadback::Rejected
                        || matches!(self.mode, ResultMode::Partial))
                    .then(|| "fixture-order".into()),
                },
                settlement: (state != ExecutionReadback::Unknown).then_some(
                    TerminalPositionSettlement {
                        executed_quantity: if matches!(self.mode, ResultMode::Partial) {
                            Decimal::new(4, 3)
                        } else {
                            context.quantity
                        },
                        positions,
                        observed_ms: test_now_ms()
                            .map_err(|_| BinanceExecutionError::Unavailable)?
                            + 1,
                    },
                ),
                failure_code: matches!(self.mode, ResultMode::Partial)
                    .then_some("market_partial_fill"),
            })
        })
    }
}
impl BinanceActivationBaseline for PositionExchange {
    async fn activation_baseline(
        &mut self,
        _: &str,
        _: &std::collections::BTreeSet<venue_domain::Symbol>,
        _: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError> {
        Err(BinanceExecutionError::Invalid)
    }
}
struct FixtureSecrets;
impl ExecutorCredentials for FixtureSecrets {
    fn credentials<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<BinanceCredentials, ExecutorSecretError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            BinanceCredentials::from_secrets(
                secrecy::SecretString::from("fixture-key"),
                secrecy::SecretString::from("fixture-secret"),
            )
            .map_err(|_| ExecutorSecretError::Unavailable)
        })
    }
}

#[tokio::test]
async fn terminal_position_reverse_is_durable_and_never_opens_before_confirmed_close()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    for mode in [
        ResultMode::Filled,
        ResultMode::Unknown,
        ResultMode::Rejected,
        ResultMode::Partial,
        ResultMode::NotFlat,
        ResultMode::Revoked,
        ResultMode::Stale,
        ResultMode::UnpreparedRestart,
        ResultMode::OpenRejected,
    ] {
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
                    username: "row-owner".into(),
                    password: SecretValue::new("safe terminal password".into()),
                },
                test_now_ms()?,
            )
            .await?;
        let principal = service
            .authenticate(session.token.expose(), test_now_ms()?)
            .await?;
        let account = id(9972);
        let credential = id(9973);
        sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)")
            .bind(&account).bind(&principal.user.user_id).bind(vec![72_u8;32]).execute(&fixture.pool).await?;
        sqlx::query("INSERT INTO venue_api_credentials (credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,created_ms) VALUES ($1,$2,'row',$3,'***',decode('00','hex'),$4,'{\"verification\":\"verified\"}'::jsonb,1)")
            .bind(&credential).bind(&principal.user.user_id).bind(vec![73_u8;32]).bind(&account).execute(&fixture.pool).await?;
        let source = ActiveProjectionSource {
            kol_user_id: None,
            owner_user_id: principal.user.user_id.clone(),
            credential_id: credential.clone(),
            trading_account_id: account.clone(),
            symbols: ["BTC/USDT".parse()?].into_iter().collect(),
            previous_fills_cursor: None,
        };
        let projections = BinancePrivateProjectionStore::new(fixture.pool.clone());
        projections
            .persist(
                &source,
                &projection_snapshot(
                    account.clone(),
                    "BTC/USDT".parse()?,
                    test_now_ms()?,
                    1,
                    "row-cursor",
                    Decimal::new(5, 3),
                    Decimal::from(50100),
                    false,
                )?,
                test_now_ms()?,
            )
            .await?;
        let request = TerminalPositionActionRequest {
            schema_version: TERMINAL_POSITION_ACTION_SCHEMA,
            request_id: id(9974),
            credential_id: credential.clone(),
            symbol: "BTC/USDT".parse()?,
            position_side: PositionSide::Long,
            quantity: Decimal::new(7, 3),
            action: PositionAction::Reverse,
            market_risk_confirmed: true,
        };
        let response = service
            .enqueue_position_action(&principal, request.clone(), test_now_ms()?)
            .await?;
        assert_eq!(response.requested_quantity, Some(Decimal::new(5, 3)));
        assert_eq!(
            service
                .enqueue_position_action(&principal, request.clone(), test_now_ms()?)
                .await?
                .command_id,
            response.command_id
        );
        let mut changed = request.clone();
        changed.quantity = Decimal::new(1, 3);
        assert_eq!(
            service
                .enqueue_position_action(&principal, changed, test_now_ms()?)
                .await
                .err()
                .ok_or("changed replay accepted")?
                .code,
            AccountErrorCode::Conflict
        );
        let mut duplicate = request.clone();
        duplicate.request_id = id(9975);
        assert_eq!(
            service
                .enqueue_position_action(&principal, duplicate, test_now_ms()?)
                .await
                .err()
                .ok_or("duplicate accepted")?
                .code,
            AccountErrorCode::Conflict
        );
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_binance_commands")
            .fetch_one(&fixture.pool)
            .await?;
        assert_eq!(count, 2);
        match mode {
            ResultMode::Revoked => {
                sqlx::query("UPDATE venue_api_credentials SET verification_json='{}'::jsonb WHERE credential_id=$1")
                    .bind(&credential).execute(&fixture.pool).await?;
            }
            ResultMode::Stale => {
                sqlx::query("UPDATE venue_binance_account_projections SET projection_json=jsonb_set(projection_json,'{projection,observed_ms}',to_jsonb($1::bigint)) WHERE credential_id=$2")
                    .bind(i64::try_from(test_now_ms()?.saturating_sub(30_000))?).bind(&credential).execute(&fixture.pool).await?;
            }
            ResultMode::UnpreparedRestart => {
                PgExecutorStore::new(fixture.pool.clone())
                    .claim_next_command(&account, test_now_ms()?)
                    .await?
                    .ok_or("close claim missing")?;
            }
            _ => (),
        }
        let calls = Arc::new(Mutex::new(Vec::new()));
        let exchange = PositionExchange {
            mode,
            calls: calls.clone(),
        };
        let mut runtime = BinanceExecutorRuntime::new(
            PgExecutorStore::new(fixture.pool.clone()),
            exchange,
            FixtureSecrets,
        );
        runtime.recover_once().await?;
        if matches!(
            mode,
            ResultMode::Revoked | ResultMode::Stale | ResultMode::UnpreparedRestart
        ) {
            if matches!(mode, ResultMode::UnpreparedRestart) {
                sqlx::query("UPDATE venue_binance_commands SET next_reconcile_ms=1 WHERE command_state='reconcile_required'").execute(&fixture.pool).await?;
                runtime.recover_once().await?;
            }
            assert!(calls.lock().map_err(|_| "poison")?.is_empty());
            assert_eq!(
                command_state(&fixture.pool, &response.command_id).await?,
                "cancelled"
            );
            fixture.cleanup().await?;
            continue;
        }
        if matches!(mode, ResultMode::OpenRejected) {
            assert_eq!(
                command_state(&fixture.pool, &response.command_id).await?,
                "rejected"
            );
            let close_state: String = sqlx::query_scalar(
                "SELECT command_state FROM venue_binance_commands WHERE command_phase='close'",
            )
            .fetch_one(&fixture.pool)
            .await?;
            assert_eq!(close_state, "reconciled");
            assert_eq!(calls.lock().map_err(|_| "poison")?.len(), 2);
            fixture.cleanup().await?;
            continue;
        }
        if matches!(mode, ResultMode::Unknown) {
            let summaries = service.terminal_executions(&principal).await?;
            assert_eq!(
                summaries
                    .iter()
                    .find(|row| row.command_id == response.command_id)
                    .ok_or("root summary missing")?
                    .sanitized_error_code
                    .as_deref(),
                Some("dispatch_unknown")
            );
            assert_eq!(calls.lock().map_err(|_| "poison")?.len(), 1);
            assert_eq!(
                command_state(&fixture.pool, &response.command_id).await?,
                "pending"
            );
            sqlx::query("UPDATE venue_binance_commands SET next_reconcile_ms=1 WHERE command_state='reconcile_required'").execute(&fixture.pool).await?;
            runtime.recover_once().await?;
            runtime.recover_once().await?;
        }
        let recorded = calls.lock().map_err(|_| "poison")?.clone();
        assert_eq!(
            recorded
                .iter()
                .filter(|(read, reducing, _, _)| !read && *reducing)
                .count(),
            1
        );
        if matches!(mode, ResultMode::Filled | ResultMode::Unknown) {
            assert_eq!(
                command_state(&fixture.pool, &response.command_id).await?,
                "reconciled"
            );
            assert_eq!(
                recorded
                    .iter()
                    .filter(|(read, reducing, _, _)| !read && !reducing)
                    .count(),
                1
            );
            assert!(
                recorded
                    .iter()
                    .all(|(_, _, quantity, _)| *quantity == Decimal::new(5, 3))
            );
            if matches!(mode, ResultMode::Unknown) {
                assert_eq!(recorded[0].3, recorded[1].3);
                assert!(recorded[1].0);
            }
            let projection = projections
                .load_owned(&principal.user.user_id, &credential)
                .await?
                .ok_or("missing refreshed positions")?;
            assert_eq!(
                projection
                    .positions
                    .iter()
                    .find(|p| p.position_side == PositionSide::Short)
                    .ok_or("short absent")?
                    .quantity,
                Decimal::new(7, 3)
            );
            assert!(
                projections
                    .load_owned(&id(9999), &credential)
                    .await?
                    .is_none()
            );
        } else {
            assert_eq!(
                command_state(&fixture.pool, &response.command_id).await?,
                "cancelled"
            );
            assert_eq!(recorded.len(), 1, "a failed/non-flat close must not open");
        }
        fixture.cleanup().await?;
    }
    Ok(())
}
