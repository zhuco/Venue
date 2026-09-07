use super::{
    EventCursor, MAX_SSE_BUFFER_BYTES, RECONNECT_INITIAL, RECONNECT_MAX, ReconnectBackoff,
    SseDecoder, StreamGates, event_stream_url, parse_sse_frame, path, sse_boundary,
    validate_invalidation_frame,
};
use venue_control_protocol::{
    CONTROL_SCHEMA_VERSION, GatewayMode, UiAccountScope, UiEventEnvelope, UiEventKind, VenueId,
};

fn scope() -> UiAccountScope {
    UiAccountScope {
        venue: VenueId::Binance,
        mode: GatewayMode::Live,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
    }
}

#[test]
fn api_paths_preserve_the_control_v2_route() {
    assert_eq!(
        path("http://control:39180/", "/v2/ui/snapshot"),
        "http://control:39180/v2/ui/snapshot"
    );
    assert_eq!(path("", "/v2/ui/events"), "/v2/ui/events");
    assert_eq!(
        event_stream_url("http://control:39180", &scope(), Some(EventCursor(42))),
        "http://control:39180/v2/ui/events?venue=binance&mode=LIVE&trading_account_id=00000000-0000-4000-8000-000000000001&after=42"
    );
    assert_eq!(
        event_stream_url("http://control:39180", &scope(), None),
        "http://control:39180/v2/ui/events?venue=binance&mode=LIVE&trading_account_id=00000000-0000-4000-8000-000000000001&after=0"
    );
}

#[test]
fn sse_frame_retains_cursor_for_reconnect_and_joins_multiline_data() -> Result<(), String> {
    let frame = parse_sse_frame("id: 42\nevent: control\ndata: {\"type\":\ndata: \"snapshot\"}")?;
    assert_eq!(frame.cursor, Some(EventCursor(42)));
    assert_eq!(frame.payload.as_deref(), Some("{\"type\":\n\"snapshot\"}"));
    Ok(())
}

#[test]
fn heartbeat_does_not_advance_a_scoped_cursor() -> Result<(), String> {
    let frame = parse_sse_frame("id: 43\n: heartbeat")?;
    assert_eq!(frame.cursor, Some(EventCursor(43)));
    assert_eq!(frame.payload, None);
    assert!(validate_invalidation_frame(&frame, &scope(), Some(EventCursor(42))).is_err());
    assert_eq!(sse_boundary(b"id: x\r\n\r\nnext"), Some((5, 4)));
    assert_eq!(sse_boundary(b"id: x\n\nnext"), Some((5, 2)));
    Ok(())
}

#[test]
fn schema_two_invalidation_requires_exact_scope_and_cursor_chain() -> Result<(), String> {
    let envelope = UiEventEnvelope {
        schema_version: CONTROL_SCHEMA_VERSION,
        cursor: 43,
        previous_cursor: 42,
        event_type: UiEventKind::Snapshot,
        scope: scope(),
    };
    let frame = parse_sse_frame(&format!(
        "id: 43\nevent: control\ndata: {}",
        serde_json::to_string(&envelope).map_err(|error| error.to_string())?
    ))?;
    assert_eq!(
        validate_invalidation_frame(&frame, &scope(), Some(EventCursor(42)))?,
        Some(EventCursor(43))
    );
    assert!(validate_invalidation_frame(&frame, &scope(), Some(EventCursor(41))).is_err());
    let mut another_scope = scope();
    another_scope.trading_account_id = "00000000-0000-4000-8000-000000000002".to_owned();
    assert!(validate_invalidation_frame(&frame, &another_scope, Some(EventCursor(42))).is_err());
    Ok(())
}

#[test]
fn write_gate_is_open_only_for_its_healthy_scope() {
    let gates = StreamGates::default();
    let scope = scope();
    let mut another_scope = scope.clone();
    another_scope.trading_account_id = "00000000-0000-4000-8000-000000000002".to_owned();
    gates.reconcile([scope.clone(), another_scope.clone()].into_iter().collect());
    assert!(gates.try_start(&scope));
    gates.opened(&scope);
    assert!(gates.is_open(&scope));
    assert!(!gates.is_open(&another_scope));
    gates.closed(&scope);
    assert!(!gates.is_open(&scope));
    gates.reconcile([another_scope.clone()].into_iter().collect());
    assert!(!gates.is_open(&scope));
    assert!(!gates.is_open(&another_scope));
}

#[test]
fn invalid_or_negative_last_event_id_is_rejected() {
    assert!(parse_sse_frame("id: cursor-43\ndata: {}").is_err());
    assert!(parse_sse_frame("id: -1\ndata: {}").is_err());
}

#[test]
fn fragmented_sse_is_decoded_without_loss() -> Result<(), String> {
    let mut decoder = SseDecoder::default();
    assert!(decoder.push(b"id: 44\r\ndata: {\"type\":")?.is_empty());
    let frames = decoder.push(b"\"notice\"}\r\n\r\n")?;
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].cursor, Some(EventCursor(44)));
    assert_eq!(frames[0].payload.as_deref(), Some("{\"type\":\"notice\"}"));
    Ok(())
}

#[test]
fn sse_receive_buffer_fails_closed_at_its_bound() {
    let mut decoder = SseDecoder::default();
    let oversized = vec![b'x'; MAX_SSE_BUFFER_BYTES + 1];
    assert!(decoder.push(&oversized).is_err());
}

#[test]
fn reconnect_backoff_is_capped_and_resets_after_progress() {
    let mut backoff = ReconnectBackoff::default();
    assert_eq!(backoff.next_delay(), RECONNECT_INITIAL);
    let mut last = RECONNECT_INITIAL;
    for _ in 0..16 {
        last = backoff.next_delay();
    }
    assert_eq!(last, RECONNECT_MAX);
    backoff.reset();
    assert_eq!(backoff.next_delay(), RECONNECT_INITIAL);
}
