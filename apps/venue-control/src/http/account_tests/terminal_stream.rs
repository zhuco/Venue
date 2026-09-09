use super::*;
use venue_control_protocol::{
    kol::{KOL_TERMINAL_ACCOUNT_STREAM_PATH, TerminalAccountProjection, TerminalProjectionRequest},
    terminal_account_stream::{COMPACT_QUERY, TerminalAccountStreamEvent},
};

pub(super) async fn verify(
    fixture: &Fixture,
    server: &Server,
    alice: &SessionResponse,
    bob: &SessionResponse,
    request: &TerminalProjectionRequest,
    expected: &TerminalAccountProjection,
) -> TestResult {
    let compact = format!("{KOL_TERMINAL_ACCOUNT_STREAM_PATH}?{COMPACT_QUERY}");
    for route in [KOL_TERMINAL_ACCOUNT_STREAM_PATH, compact.as_str()] {
        code(
            server.post(route, None, request).send().await?,
            401,
            AccountErrorCode::Unauthorized,
        )
        .await?;
        code(
            server.post(route, Some(bob), request).send().await?,
            409,
            AccountErrorCode::VerificationRequired,
        )
        .await?;
    }
    code(
        server
            .post(
                &format!("{KOL_TERMINAL_ACCOUNT_STREAM_PATH}?compact=2"),
                Some(alice),
                request,
            )
            .send()
            .await?,
        400,
        AccountErrorCode::InvalidInput,
    )
    .await?;
    let mut legacy = server
        .post(KOL_TERMINAL_ACCOUNT_STREAM_PATH, Some(alice), request)
        .send()
        .await?
        .error_for_status()?;
    let mut buffer = Vec::new();
    let legacy_frame = frame(&mut legacy, &mut buffer).await?;
    assert_eq!(
        serde_json::from_str::<Option<TerminalAccountProjection>>(&legacy_frame)?,
        Some(expected.clone())
    );
    drop(legacy);

    let mut response = server
        .post(&compact, Some(alice), request)
        .send()
        .await?
        .error_for_status()?;
    buffer.clear();
    let first: TerminalAccountStreamEvent =
        serde_json::from_str(&frame(&mut response, &mut buffer).await?)?;
    assert!(matches!(
        &first,
        TerminalAccountStreamEvent::Snapshot(Some(_))
    ));
    let mut received = None;
    first.apply(&mut received)?;
    assert_eq!(received.as_ref(), Some(expected));

    let mut current = expected.clone();
    current.observed_ms += 1;
    current.persisted_ms += 1;
    current.positions[0].mark_price = Some(50200.into());
    sqlx::query("UPDATE venue_binance_account_projections SET observed_ms=$1,persisted_ms=$1,projection_json=$2 WHERE credential_id=$3")
        .bind(i64::try_from(current.observed_ms)?)
        .bind(serde_json::json!({"fills_cursor":"fixture-cursor","stream_healthy":true,"projection":current}))
        .bind(&request.credential_id).execute(&fixture.pool).await?;
    for _ in 0..4 {
        let next: TerminalAccountStreamEvent =
            serde_json::from_str(&frame(&mut response, &mut buffer).await?)?;
        assert!(matches!(
            &next,
            TerminalAccountStreamEvent::Update {
                fills: None,
                position_history: None,
                ..
            }
        ));
        next.apply(&mut received)?;
        if received.as_ref() == Some(&current) {
            break;
        }
    }
    assert_eq!(received, Some(current));
    drop(response);
    verify_history_budget(fixture, server, alice, bob, request, expected).await?;
    Ok(())
}

async fn verify_history_budget(
    fixture: &Fixture,
    server: &Server,
    owner: &SessionResponse,
    other: &SessionResponse,
    request: &TerminalProjectionRequest,
    expected: &TerminalAccountProjection,
) -> TestResult {
    let account = &expected.trading_account_id;
    let user = &owner.user.user_id;
    let fill = serde_json::json!({
        "native_order_id":"fixture", "native_trade_id":"fixture", "symbol":"BTC/USDT",
        "order_side":"buy", "position_side":"long", "quantity":"1", "price":"50000"
    });
    sqlx::query("INSERT INTO venue_binance_account_fills (trading_account_id,owner_user_id,native_trade_id,symbol,observed_ms,fill_json) SELECT $1,$2,i::text,'BTC/USDT',i,jsonb_set($3,'{native_trade_id}',to_jsonb(i::text)) FROM generate_series(1,150) i")
        .bind(account).bind(user).bind(fill).execute(&fixture.pool).await?;
    sqlx::query("INSERT INTO venue_binance_position_history (trading_account_id,owner_user_id,symbol,position_side,observed_ms,position_json) SELECT $1,$2,'BTC/USDT','long',i,jsonb_set($3,'{quantity}',to_jsonb(i::text)) FROM generate_series(1,150) i")
        .bind(account).bind(user).bind(serde_json::to_value(&expected.positions[0])?).execute(&fixture.pool).await?;
    let display = server
        .post(
            venue_control_protocol::kol::KOL_TERMINAL_ACCOUNT_PATH,
            Some(owner),
            request,
        )
        .send()
        .await?
        .error_for_status()?
        .json::<Option<TerminalAccountProjection>>()
        .await?
        .ok_or("display missing")?;
    assert_eq!(display.fills.len(), 100);
    assert_eq!(display.position_history.len(), 100);
    assert_eq!(display.fills[0].native_trade_id, "150");
    let store = crate::private_projection::BinancePrivateProjectionStore::new(fixture.pool.clone());
    let execution = store
        .load_owned(user, &request.credential_id)
        .await?
        .ok_or("execution missing")?;
    assert_eq!(execution.fills.len(), 150);
    assert_eq!(execution.position_history.len(), 151);
    // A cached display update must not query either history table, while account ownership
    // and generation changes must still prevent history from crossing its original scope.
    sqlx::raw_sql("ALTER TABLE venue_binance_account_fills RENAME TO fixture_history_fills; ALTER TABLE venue_binance_position_history RENAME TO fixture_history_positions")
        .execute(&fixture.pool).await?;
    let cached = store
        .load_owned_for_display(user, &request.credential_id, Some(&display))
        .await?;
    assert_eq!(cached, Some(display.clone()));
    let current = store
        .load_owned_current(user, &request.credential_id)
        .await?
        .ok_or("current facts missing")?;
    assert_eq!(current.positions, display.positions);
    assert_eq!(current.open_orders, display.open_orders);
    assert!(current.fills.is_empty() && current.position_history.is_empty());
    assert!(
        store
            .load_owned_for_display(&other.user.user_id, &request.credential_id, Some(&display))
            .await?
            .is_none()
    );
    let mut wrong_generation = display;
    wrong_generation.private_generation += 1;
    assert!(
        store
            .load_owned_for_display(user, &request.credential_id, Some(&wrong_generation))
            .await
            .is_err()
    );
    sqlx::raw_sql("ALTER TABLE fixture_history_fills RENAME TO venue_binance_account_fills; ALTER TABLE fixture_history_positions RENAME TO venue_binance_position_history")
        .execute(&fixture.pool).await?;
    Ok(())
}

async fn frame(
    response: &mut reqwest::Response,
    buffer: &mut Vec<u8>,
) -> Result<String, Box<dyn std::error::Error>> {
    time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(end) = buffer.windows(2).position(|bytes| bytes == b"\n\n") {
                let text = std::str::from_utf8(&buffer[..end])?;
                let data = text
                    .lines()
                    .find_map(|line| line.strip_prefix("data: "))
                    .ok_or("missing account data")?
                    .to_owned();
                buffer.drain(..end + 2);
                return Ok::<_, Box<dyn std::error::Error>>(data);
            }
            buffer.extend_from_slice(&response.chunk().await?.ok_or("stream closed")?);
            if buffer.len() > 4 * 1024 * 1024 {
                return Err("frame too large".into());
            }
        }
    })
    .await?
}
