use std::num::NonZeroUsize;

use tempfile::tempdir;
use venue::{
    domain::{MarketEvent, Symbol},
    exchange::binance::{self, PublicStream},
    indicator::{
        FeatureState, PublicMarketSourceError, RecordedPublicEvent, ScalpingPublicMarketSource,
    },
    market::{MarketSession, RawMarketRecorder, SessionState},
    runtime::{
        PublicCaptureCompletion, PublicCaptureEffect, PublicCaptureFault,
        PublicCaptureTransportError, ScalpingPublicMarketWorker, transport_error_completion,
    },
};

const SNAPSHOT: &str = r#"{"lastUpdateId":10,"bids":[["100.0","10.0"]],"asks":[["101.0","10.0"]]}"#;
const SNAPSHOT_12: &str =
    r#"{"lastUpdateId":12,"bids":[["100.0","10.0"]],"asks":[["101.0","10.0"]]}"#;
const BRIDGE_11: &str = r#"{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":11,"u":11,"pu":11,"st":1,"b":[["100.0","11.0"]],"a":[["101.0","9.0"]]}"#;
const STALE_11: &str = r#"{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":11,"u":11,"pu":10,"st":1,"b":[["100.0","11.0"]],"a":[["101.0","9.0"]]}"#;
const BRIDGE_13: &str = r#"{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":13,"u":13,"pu":99,"st":1,"b":[["100.0","11.0"]],"a":[["101.0","9.0"]]}"#;
const GAP_12: &str = r#"{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":12,"u":12,"pu":11,"st":1,"b":[["100.0","11.0"]],"a":[["101.0","9.0"]]}"#;
const MARK_FUNDING: &str = r#"{"e":"markPriceUpdate","E":3,"s":"BTCUSDT","p":"100.5","i":"100.4","r":"0.0001","T":4,"st":1}"#;

fn worker() -> Result<(tempfile::TempDir, ScalpingPublicMarketWorker), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let recorder = RawMarketRecorder::open(directory.path().join("public.ndjson"))?;
    let symbol: Symbol = "BTC/USDT".parse()?;
    let session = MarketSession::new(symbol.clone(), recorder);
    let source = ScalpingPublicMarketSource::new(
        symbol,
        "scalping-shadow-v1",
        "0".repeat(64),
        65_000,
        NonZeroUsize::new(2_048).ok_or("history")?,
    )?;
    Ok((directory, ScalpingPublicMarketWorker::new(session, source)))
}

fn connect_depth_and_snapshot(
    worker: &mut ScalpingPublicMarketWorker,
    payload: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        worker.next_effect(0),
        Some(PublicCaptureEffect::Connect {
            stream: PublicStream::DiffDepth
        })
    );
    worker.complete(PublicCaptureCompletion::StreamConnected {
        stream: PublicStream::DiffDepth,
    })?;
    assert_eq!(
        worker.next_effect(0),
        Some(PublicCaptureEffect::FetchDepthSnapshot { limit: 1_000 })
    );
    worker.complete(PublicCaptureCompletion::DepthSnapshot {
        received_at_ms: 1,
        payload: payload.to_owned(),
    })?;
    Ok(())
}

fn complete_kline_bootstrap(
    worker: &mut ScalpingPublicMarketWorker,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        worker.next_effect(0),
        Some(PublicCaptureEffect::FetchClosedKlineBootstrap)
    );
    let rows = (1_u64..=22)
        .map(|sequence| {
            serde_json::json!([
                (sequence - 1) * 60_000,
                "100",
                "101",
                "99",
                "100",
                "1",
                sequence * 60_000 - 1
            ])
        })
        .collect::<Vec<_>>();
    worker.complete(PublicCaptureCompletion::ClosedKlineBootstrap {
        received_at_ms: 23 * 60_000,
        payload: serde_json::to_string(&rows)?,
    })?;
    Ok(())
}

fn connect_remaining(
    worker: &mut ScalpingPublicMarketWorker,
) -> Result<(), Box<dyn std::error::Error>> {
    for stream in [
        PublicStream::AggTrade,
        PublicStream::Kline1m,
        PublicStream::MarkFunding,
    ] {
        assert_eq!(
            worker.next_effect(0),
            Some(PublicCaptureEffect::Connect { stream })
        );
        worker.complete(PublicCaptureCompletion::StreamConnected { stream })?;
    }
    Ok(())
}

fn connect_and_snapshot(
    worker: &mut ScalpingPublicMarketWorker,
    payload: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    connect_depth_and_snapshot(worker, payload)
}

fn bridge_depth_and_connect_remaining(
    worker: &mut ScalpingPublicMarketWorker,
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        worker.next_effect(2),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    worker.complete(PublicCaptureCompletion::StreamFrame {
        stream: PublicStream::DiffDepth,
        received_at_ms: 2,
        payload: BRIDGE_11.to_owned(),
    })?;
    complete_kline_bootstrap(worker)?;
    connect_remaining(worker)
}

#[test]
fn scheduler_is_one_step_and_round_robin_without_socket_loops()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    assert_eq!(
        worker.next_effect(0),
        Some(PublicCaptureEffect::Connect {
            stream: PublicStream::DiffDepth
        })
    );
    assert_eq!(worker.next_effect(0), None);
    worker.complete(PublicCaptureCompletion::StreamConnected {
        stream: PublicStream::DiffDepth,
    })?;
    assert_eq!(
        worker.next_effect(0),
        Some(PublicCaptureEffect::FetchDepthSnapshot { limit: 1_000 })
    );
    assert_eq!(worker.next_effect(0), None);

    let output = worker.complete(PublicCaptureCompletion::DepthSnapshot {
        received_at_ms: 1,
        payload: SNAPSHOT.to_owned(),
    })?;
    let output = output.ok_or("snapshot output")?;
    assert_eq!(output.event.capture_sequence, 1);
    assert_eq!(output.generation, 1);
    assert_eq!(output.state, FeatureState::Warmup);
    assert_eq!(worker.recorder().recover()?.records.len(), 1);

    assert_eq!(
        worker.next_effect(0),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    worker.complete(PublicCaptureCompletion::StreamFrame {
        stream: PublicStream::DiffDepth,
        received_at_ms: 2,
        payload: BRIDGE_11.to_owned(),
    })?;
    complete_kline_bootstrap(&mut worker)?;
    assert_eq!(worker.recorder().recover()?.records.len(), 23);
    connect_remaining(&mut worker)?;
    for stream in [
        PublicStream::DiffDepth,
        PublicStream::AggTrade,
        PublicStream::DiffDepth,
        PublicStream::AggTrade,
        PublicStream::DiffDepth,
        PublicStream::Kline1m,
        PublicStream::DiffDepth,
        PublicStream::MarkFunding,
    ] {
        assert_eq!(
            worker.next_effect(0),
            Some(PublicCaptureEffect::Read { stream })
        );
        worker.complete(PublicCaptureCompletion::StreamReady { stream })?;
    }
    assert_eq!(
        worker.next_effect(0),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    Ok(())
}

#[test]
fn aggregate_trade_batch_preserves_order_and_samples_latest_output()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    connect_and_snapshot(&mut worker, SNAPSHOT)?;
    bridge_depth_and_connect_remaining(&mut worker)?;
    assert_eq!(
        worker.next_effect(23 * 60_000),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    worker.complete(PublicCaptureCompletion::StreamFrame {
        stream: PublicStream::DiffDepth,
        received_at_ms: 23 * 60_000,
        payload: GAP_12.to_owned(),
    })?;
    assert_eq!(
        worker.next_effect(23 * 60_000),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::AggTrade
        })
    );
    let payloads = (1_u64..=64)
        .map(|trade_id| {
            format!(
                r#"{{"e":"aggTrade","E":1380000,"s":"BTCUSDT","a":{trade_id},"p":"100.5","q":"1","nq":"100.5","f":{trade_id},"l":{trade_id},"T":1380000,"m":true,"st":1}}"#
            )
        })
        .collect();
    let output = worker
        .complete(PublicCaptureCompletion::StreamFrames {
            stream: PublicStream::AggTrade,
            received_at_ms: 23 * 60_000,
            payloads,
        })?
        .ok_or("batch output")?;

    assert_eq!(output.event.capture_sequence, 88);
    assert_eq!(worker.recorder().recover()?.records.len(), 88);
    assert!(matches!(output.event.event, MarketEvent::Trade(_)));
    assert!(output.frame.is_some());

    assert_eq!(
        worker.next_effect(23 * 60_000 + 65_001),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    let stale = worker
        .complete(PublicCaptureCompletion::StreamFrame {
            stream: PublicStream::DiffDepth,
            received_at_ms: 23 * 60_000 + 65_001,
            payload: r#"{"e":"depthUpdate","E":1445001,"T":1445001,"s":"BTCUSDT","U":13,"u":13,"pu":12,"st":1,"b":[["100.0","12.0"]],"a":[["101.0","8.0"]]}"#.to_owned(),
        })?
        .ok_or("stale output")?;
    assert_eq!(stale.state, FeatureState::Stale);
    assert_eq!(worker.session_state(), SessionState::Backoff);
    Ok(())
}

#[test]
fn diff_depth_batch_preserves_sequence_and_journals_every_update()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    connect_and_snapshot(&mut worker, SNAPSHOT)?;
    bridge_depth_and_connect_remaining(&mut worker)?;
    assert_eq!(
        worker.next_effect(23 * 60_000),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    let payloads = (12_u64..=75)
        .map(|sequence| {
            let event_time_ms = 23 * 60_000 + sequence;
            format!(
                r#"{{"e":"depthUpdate","E":{event_time_ms},"T":{event_time_ms},"s":"BTCUSDT","U":{sequence},"u":{sequence},"pu":{},"st":1,"b":[["100.0","12.0"]],"a":[["101.0","8.0"]]}}"#,
                sequence - 1
            )
        })
        .collect();
    let output = worker
        .complete(PublicCaptureCompletion::StreamFrames {
            stream: PublicStream::DiffDepth,
            received_at_ms: 23 * 60_000 + 75,
            payloads,
        })?
        .ok_or("batch output")?;

    assert_eq!(output.event.capture_sequence, 87);
    assert_eq!(worker.recorder().recover()?.records.len(), 87);
    assert!(matches!(output.event.event, MarketEvent::Delta(_)));
    assert_eq!(worker.session_state(), SessionState::Ready);
    Ok(())
}

#[test]
fn mark_funding_is_recorded_without_becoming_book_trade_or_bar_readiness()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    connect_and_snapshot(&mut worker, SNAPSHOT)?;
    bridge_depth_and_connect_remaining(&mut worker)?;
    for stream in [
        PublicStream::DiffDepth,
        PublicStream::AggTrade,
        PublicStream::DiffDepth,
        PublicStream::AggTrade,
        PublicStream::DiffDepth,
        PublicStream::Kline1m,
        PublicStream::DiffDepth,
    ] {
        assert_eq!(
            worker.next_effect(2),
            Some(PublicCaptureEffect::Read { stream })
        );
        worker.complete(PublicCaptureCompletion::StreamReady { stream })?;
    }
    assert_eq!(
        worker.next_effect(2),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::MarkFunding
        })
    );
    let output = worker.complete(PublicCaptureCompletion::StreamFrame {
        stream: PublicStream::MarkFunding,
        received_at_ms: 3,
        payload: MARK_FUNDING.to_owned(),
    })?;
    let output = output.ok_or("mark output")?;
    assert!(matches!(
        output.event.event,
        venue::domain::MarketEvent::MarkFunding(mark)
            if mark.mark_price.value() == rust_decimal::Decimal::new(1005, 1)
    ));
    assert_eq!(output.state, FeatureState::Warmup);
    assert!(output.frame.is_none());
    assert_eq!(worker.recorder().pending_sync_count(), 0);
    let records = worker.recorder().recover()?.records;
    assert_eq!(records.len(), 24);
    assert_eq!(
        records[23].source,
        venue::market::RawSource::WebSocketMarkFunding
    );
    Ok(())
}

#[test]
fn mark_parse_fault_fences_the_whole_session_and_restarts_at_a_new_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    connect_and_snapshot(&mut worker, SNAPSHOT)?;
    bridge_depth_and_connect_remaining(&mut worker)?;
    for stream in [
        PublicStream::DiffDepth,
        PublicStream::AggTrade,
        PublicStream::DiffDepth,
        PublicStream::AggTrade,
        PublicStream::DiffDepth,
        PublicStream::Kline1m,
        PublicStream::DiffDepth,
    ] {
        assert_eq!(
            worker.next_effect(2),
            Some(PublicCaptureEffect::Read { stream })
        );
        worker.complete(PublicCaptureCompletion::StreamReady { stream })?;
    }
    assert_eq!(
        worker.next_effect(2),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::MarkFunding
        })
    );
    assert!(matches!(
        worker.complete(PublicCaptureCompletion::StreamFrame {
            stream: PublicStream::MarkFunding,
            received_at_ms: 3,
            payload: "not-json".to_owned(),
        }),
        Err(venue::runtime::PublicCaptureWorkerError::Session(_))
    ));
    assert_eq!(worker.session_state(), SessionState::Backoff);
    assert_eq!(worker.feature_state(), FeatureState::DataGap);
    assert_eq!(worker.generation(), 2);
    let retry_at = worker
        .session_retry_at_ms()
        .ok_or("missing public retry deadline")?;
    assert!((503..=1_003).contains(&retry_at));
    assert_eq!(worker.next_effect(retry_at.saturating_sub(1)), None);
    assert_eq!(
        worker.next_effect(retry_at),
        Some(PublicCaptureEffect::Connect {
            stream: PublicStream::DiffDepth
        })
    );
    Ok(())
}

#[test]
fn snapshot_then_first_depth_bridge_is_accepted_without_snapshot_pu_equality()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    connect_and_snapshot(&mut worker, SNAPSHOT)?;
    assert_eq!(
        worker.next_effect(2),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    let output = worker.complete(PublicCaptureCompletion::StreamFrame {
        stream: PublicStream::DiffDepth,
        received_at_ms: 2,
        payload: BRIDGE_11.to_owned(),
    })?;
    let output = output.ok_or("bridge output")?;
    assert!(matches!(
        output.event.event,
        venue::domain::MarketEvent::Delta(delta) if delta.sequence == 11
    ));
    assert_eq!(worker.session_state(), SessionState::Ready);
    assert_eq!(worker.generation(), 1);
    Ok(())
}

#[test]
fn queued_stale_delta_is_ignored_before_a_later_first_bridge()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    connect_and_snapshot(&mut worker, SNAPSHOT_12)?;
    assert_eq!(
        worker.next_effect(2),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    assert!(
        worker
            .complete(PublicCaptureCompletion::StreamFrame {
                stream: PublicStream::DiffDepth,
                received_at_ms: 2,
                payload: STALE_11.to_owned(),
            })?
            .is_none()
    );
    assert_eq!(
        worker.next_effect(0),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    let output = worker.complete(PublicCaptureCompletion::StreamFrame {
        stream: PublicStream::DiffDepth,
        received_at_ms: 3,
        payload: BRIDGE_13.to_owned(),
    })?;
    assert!(output.is_some());
    assert_eq!(worker.session_state(), SessionState::Ready);
    complete_kline_bootstrap(&mut worker)?;
    connect_remaining(&mut worker)?;
    Ok(())
}

#[test]
fn first_depth_frame_with_a_real_bridge_gap_fences_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    connect_and_snapshot(&mut worker, SNAPSHOT)?;
    assert_eq!(
        worker.next_effect(2),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    assert!(matches!(
        worker.complete(PublicCaptureCompletion::StreamFrame {
            stream: PublicStream::DiffDepth,
            received_at_ms: 2,
            payload: GAP_12.to_owned(),
        }),
        Err(venue::runtime::PublicCaptureWorkerError::Session(_))
    ));
    assert_eq!(worker.session_state(), SessionState::Backoff);
    assert_eq!(worker.feature_state(), FeatureState::DataGap);
    Ok(())
}

#[test]
fn transport_fault_fences_until_backoff_and_new_snapshot_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    connect_depth_and_snapshot(&mut worker, SNAPSHOT)?;
    assert_eq!(
        worker.next_effect(100),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );

    worker.complete(PublicCaptureCompletion::Fault {
        stream: Some(PublicStream::DiffDepth),
        fault: PublicCaptureFault::Disconnected,
        now_ms: 100,
    })?;
    assert_eq!(worker.session_state(), SessionState::Backoff);
    assert_eq!(worker.generation(), 2);
    assert_eq!(worker.feature_state(), FeatureState::DataGap);
    let retry_at = worker
        .session_retry_at_ms()
        .ok_or("missing public retry deadline")?;
    assert!((600..=1_100).contains(&retry_at));
    assert_eq!(worker.next_effect(retry_at.saturating_sub(1)), None);
    assert_eq!(
        worker.next_effect(retry_at),
        Some(PublicCaptureEffect::Connect {
            stream: PublicStream::DiffDepth
        })
    );
    worker.complete(PublicCaptureCompletion::StreamConnected {
        stream: PublicStream::DiffDepth,
    })?;
    assert_eq!(
        worker.next_effect(retry_at),
        Some(PublicCaptureEffect::FetchDepthSnapshot { limit: 1_000 })
    );

    let output = worker.complete(PublicCaptureCompletion::DepthSnapshot {
        received_at_ms: retry_at.saturating_add(1),
        payload: SNAPSHOT.to_owned(),
    })?;
    let output = output.ok_or("recovery snapshot output")?;
    assert_eq!(output.event.capture_sequence, 2);
    assert_eq!(output.generation, 2);
    assert_eq!(worker.feature_state(), FeatureState::Warmup);
    assert_eq!(worker.session_retry_at_ms(), None);
    assert_eq!(worker.recorder().recover()?.records.len(), 2);

    assert_eq!(
        worker.next_effect(1_200),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    worker.complete(PublicCaptureCompletion::Fault {
        stream: Some(PublicStream::DiffDepth),
        fault: PublicCaptureFault::Disconnected,
        now_ms: 1_200,
    })?;
    assert_eq!(worker.session_state(), SessionState::Backoff);
    assert_eq!(worker.generation(), 3);
    let second_retry_at = worker
        .session_retry_at_ms()
        .ok_or("missing second public retry deadline")?;
    assert!((2_200..=3_200).contains(&second_retry_at));
    assert_eq!(worker.next_effect(second_retry_at.saturating_sub(1)), None);
    assert_eq!(
        worker.next_effect(second_retry_at),
        Some(PublicCaptureEffect::Connect {
            stream: PublicStream::DiffDepth
        })
    );
    worker.complete(PublicCaptureCompletion::StreamConnected {
        stream: PublicStream::DiffDepth,
    })?;
    assert_eq!(
        worker.next_effect(second_retry_at),
        Some(PublicCaptureEffect::FetchDepthSnapshot { limit: 1_000 })
    );
    let output = worker.complete(PublicCaptureCompletion::DepthSnapshot {
        received_at_ms: second_retry_at.saturating_add(1),
        payload: SNAPSHOT.to_owned(),
    })?;
    assert_eq!(output.ok_or("second recovery snapshot")?.generation, 3);
    Ok(())
}

#[test]
fn wrong_stream_fault_is_rejected_and_effect_remains_pending()
-> Result<(), Box<dyn std::error::Error>> {
    let (_directory, mut worker) = worker()?;
    assert_eq!(
        worker.next_effect(0),
        Some(PublicCaptureEffect::Connect {
            stream: PublicStream::DiffDepth
        })
    );
    assert!(matches!(
        worker.complete(PublicCaptureCompletion::Fault {
            stream: Some(PublicStream::AggTrade),
            fault: PublicCaptureFault::Disconnected,
            now_ms: 10,
        }),
        Err(venue::runtime::PublicCaptureWorkerError::UnexpectedCompletion)
    ));
    assert_eq!(worker.next_effect(0), None);

    worker.complete(PublicCaptureCompletion::StreamConnected {
        stream: PublicStream::DiffDepth,
    })?;
    assert_eq!(
        worker.next_effect(1),
        Some(PublicCaptureEffect::FetchDepthSnapshot { limit: 1_000 })
    );
    assert!(matches!(
        worker.complete(PublicCaptureCompletion::Fault {
            stream: Some(PublicStream::DiffDepth),
            fault: PublicCaptureFault::Disconnected,
            now_ms: 10,
        }),
        Err(venue::runtime::PublicCaptureWorkerError::UnexpectedCompletion)
    ));
    assert_eq!(worker.next_effect(1), None);
    worker.complete(PublicCaptureCompletion::DepthSnapshot {
        received_at_ms: 1,
        payload: SNAPSHOT.to_owned(),
    })?;
    assert_eq!(
        worker.next_effect(1),
        Some(PublicCaptureEffect::Read {
            stream: PublicStream::DiffDepth
        })
    );
    assert!(matches!(
        worker.complete(PublicCaptureCompletion::Fault {
            stream: Some(PublicStream::AggTrade),
            fault: PublicCaptureFault::Disconnected,
            now_ms: 10,
        }),
        Err(venue::runtime::PublicCaptureWorkerError::UnexpectedCompletion)
    ));
    assert_eq!(worker.next_effect(1), None);
    worker.complete(PublicCaptureCompletion::Fault {
        stream: Some(PublicStream::DiffDepth),
        fault: PublicCaptureFault::Disconnected,
        now_ms: 10,
    })?;
    assert_eq!(worker.session_state(), SessionState::Backoff);
    Ok(())
}

#[test]
fn transport_error_mapping_preserves_pending_effect_stream_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let error = PublicCaptureTransportError::NotConnected;
    assert_eq!(
        transport_error_completion(
            PublicCaptureEffect::FetchDepthSnapshot { limit: 1_000 },
            &error,
            10,
        ),
        PublicCaptureCompletion::Fault {
            stream: None,
            fault: PublicCaptureFault::Disconnected,
            now_ms: 10,
        }
    );
    assert_eq!(
        transport_error_completion(
            PublicCaptureEffect::Read {
                stream: PublicStream::AggTrade,
            },
            &error,
            11,
        ),
        PublicCaptureCompletion::Fault {
            stream: Some(PublicStream::AggTrade),
            fault: PublicCaptureFault::Disconnected,
            now_ms: 11,
        }
    );
    Ok(())
}

#[test]
fn recovered_worker_advances_generation_and_capture_without_reusing_readiness()
-> Result<(), Box<dyn std::error::Error>> {
    let (directory, mut first) = worker()?;
    connect_and_snapshot(&mut first, SNAPSHOT)?;
    assert_eq!(first.generation(), 1);
    drop(first);

    let symbol: Symbol = "BTC/USDT".parse()?;
    let source = ScalpingPublicMarketSource::new(
        symbol.clone(),
        "scalping-shadow-v1",
        "0".repeat(64),
        65_000,
        NonZeroUsize::new(2_048).ok_or("history")?,
    )?;
    let mut second = ScalpingPublicMarketWorker::open_recovered(
        symbol,
        directory.path().join("public.ndjson"),
        source,
    )?;
    assert_eq!(second.generation(), 2);
    assert_eq!(second.feature_state(), FeatureState::Warmup);
    assert_eq!(
        second.next_effect(0),
        Some(PublicCaptureEffect::Connect {
            stream: PublicStream::DiffDepth
        })
    );
    second.complete(PublicCaptureCompletion::StreamConnected {
        stream: PublicStream::DiffDepth,
    })?;
    assert_eq!(
        second.next_effect(0),
        Some(PublicCaptureEffect::FetchDepthSnapshot { limit: 1_000 })
    );
    let output = second.complete(PublicCaptureCompletion::DepthSnapshot {
        received_at_ms: 2,
        payload: SNAPSHOT.to_owned(),
    })?;
    assert_eq!(
        output.ok_or("recovered snapshot")?.event.capture_sequence,
        2
    );
    assert_eq!(second.generation(), 2);
    Ok(())
}

#[test]
fn public_source_rejects_a_frame_from_the_previous_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "BTC/USDT".parse()?;
    let mut source = ScalpingPublicMarketSource::new(
        symbol.clone(),
        "scalping-shadow-v1",
        "0".repeat(64),
        65_000,
        NonZeroUsize::new(2_048).ok_or("history")?,
    )?;
    let record_one = venue::market::RawMarketRecord::new(
        venue::market::RawSource::RestSnapshot,
        symbol.clone(),
        1,
        1,
        SNAPSHOT.to_owned(),
    )?;
    let event_one = binance::normalize(&record_one, "BTCUSDT")?;
    let MarketEvent::Snapshot(snapshot_one) = event_one.clone() else {
        return Err("expected first snapshot".into());
    };
    let mut book = venue::market::OrderBook::default();
    book.apply_snapshot(snapshot_one);
    source.consume(
        RecordedPublicEvent {
            capture_sequence: 1,
            received_at_ms: 1,
            event: event_one.clone(),
        },
        &book,
        1,
    )?;

    let record_two = venue::market::RawMarketRecord::new(
        venue::market::RawSource::RestSnapshot,
        symbol,
        2,
        2,
        SNAPSHOT.to_owned(),
    )?;
    let event_two = binance::normalize(&record_two, "BTCUSDT")?;
    let MarketEvent::Snapshot(snapshot_two) = event_two.clone() else {
        return Err("expected second snapshot".into());
    };
    book.apply_snapshot(snapshot_two);
    source.consume(
        RecordedPublicEvent {
            capture_sequence: 2,
            received_at_ms: 2,
            event: event_two,
        },
        &book,
        2,
    )?;

    assert!(matches!(
        source.consume(
            RecordedPublicEvent {
                capture_sequence: 3,
                received_at_ms: 3,
                event: event_one,
            },
            &book,
            3,
        ),
        Err(PublicMarketSourceError::Generation)
    ));
    Ok(())
}

#[test]
fn recovered_worker_rejects_a_generation_segment_without_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("public.ndjson");
    let symbol: Symbol = "BTC/USDT".parse()?;
    let mut recorder = RawMarketRecorder::open(&path)?;
    recorder.append(venue::market::RawMarketRecord::new(
        venue::market::RawSource::WebSocketTrade,
        symbol.clone(),
        1,
        1,
        "{}".to_owned(),
    )?)?;
    let source = ScalpingPublicMarketSource::new(
        symbol,
        "scalping-shadow-v1",
        "0".repeat(64),
        65_000,
        NonZeroUsize::new(2_048).ok_or("history")?,
    )?;
    assert!(matches!(
        ScalpingPublicMarketWorker::open_recovered("BTC/USDT".parse()?, path, source,),
        Err(venue::runtime::PublicCaptureWorkerError::RecoveredJournal)
    ));
    Ok(())
}
