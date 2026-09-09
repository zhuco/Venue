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
                retain_fills: true,
                retain_position_history: true,
                ..
            }
        ));
        next.apply(&mut received)?;
        if received.as_ref() == Some(&current) {
            break;
        }
    }
    assert_eq!(received, Some(current));
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
