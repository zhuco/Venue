use std::{fs, fs::OpenOptions, io::Write};

use tempfile::tempdir;
use venue::{
    domain::{MarketEvent, Symbol},
    market::{
        MarketSession, OrderBook, RawError, RawMarketRecord, RawMarketRecorder, RawSource,
        SessionState, TransportFault, replay_binance,
    },
};

fn snapshot(symbol: Symbol) -> Result<RawMarketRecord, Box<dyn std::error::Error>> {
    RawMarketRecord::new(
        RawSource::RestSnapshot,
        symbol,
        1,
        1,
        r#"{"lastUpdateId":10,"bids":[["100.0","1.0"]],"asks":[["101.0","2.0"]]}"#.to_owned(),
    )
    .map_err(Into::into)
}

fn delta(
    symbol: Symbol,
    previous: u64,
    sequence: u64,
) -> Result<RawMarketRecord, Box<dyn std::error::Error>> {
    RawMarketRecord::new(
        RawSource::WebSocketDelta,
        symbol,
        1,
        2,
        format!(
            r#"{{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":{},"u":{},"pu":{},"st":1,"b":[["100.0","0"]],"a":[["101.0","1.5"]]}}"#,
            previous + 1,
            sequence,
            previous
        ),
    )
    .map_err(Into::into)
}

#[test]
fn raw_capture_replays_deterministically_and_applies_a_connected_delta()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("market.jsonl");
    let symbol: Symbol = "BTC/USDT".parse()?;
    let mut recorder = RawMarketRecorder::open(&path)?;
    recorder.append(snapshot(symbol.clone())?)?;
    recorder.append(delta(symbol, 10, 11)?)?;
    let records = recorder.recover()?.records;

    let first = replay_binance(&records, "BTCUSDT")?;
    let second = replay_binance(&records, "BTCUSDT")?;
    assert_eq!(first, second);
    assert_eq!(first.final_sequence, Some(11));
    Ok(())
}

#[test]
fn raw_capture_syncs_on_explicit_and_bounded_batch_boundaries()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("bounded-sync.jsonl");
    let symbol: Symbol = "BTC/USDT".parse()?;
    let mut recorder = RawMarketRecorder::open_for_symbol(&path, symbol.clone())?;
    recorder.append(snapshot(symbol.clone())?)?;
    assert_eq!(recorder.pending_sync_count(), 1);
    recorder.sync_pending()?;
    assert_eq!(recorder.pending_sync_count(), 0);

    for _ in 0..16 {
        recorder.append(snapshot(symbol.clone())?)?;
    }
    assert_eq!(recorder.pending_sync_count(), 0);
    assert_eq!(recorder.recover()?.records.len(), 17);
    Ok(())
}

#[test]
fn a_gap_or_wrong_generation_clears_book_readiness() -> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "BTC/USDT".parse()?;
    let snapshot_record = snapshot(symbol.clone())?;
    let mut book = OrderBook::default();
    let venue::domain::MarketEvent::Snapshot(snapshot) =
        venue::exchange::binance::normalize(&snapshot_record, "BTCUSDT")?
    else {
        return Err("expected snapshot".into());
    };
    book.apply_snapshot(snapshot);
    let gap = delta(symbol, 11, 12)?;
    let venue::domain::MarketEvent::Delta(delta) =
        venue::exchange::binance::normalize(&gap, "BTCUSDT")?
    else {
        return Err("expected delta".into());
    };

    assert!(book.apply_delta(delta).is_err());
    assert!(!book.synchronized());
    Ok(())
}

#[test]
fn futures_previous_update_id_proves_continuity_when_first_id_jumps()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "BTC/USDT".parse()?;
    let mut book = OrderBook::default();
    let venue::domain::MarketEvent::Snapshot(snapshot) =
        venue::exchange::binance::normalize(&snapshot(symbol.clone())?, "BTCUSDT")?
    else {
        return Err("expected snapshot".into());
    };
    book.apply_snapshot(snapshot);
    let bridge = RawMarketRecord::new(
        RawSource::WebSocketDelta,
        symbol.clone(),
        1,
        2,
        r#"{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":20,"u":25,"pu":10,"st":1,"b":[],"a":[]}"#.to_owned(),
    )?;
    let venue::domain::MarketEvent::Delta(bridge) =
        venue::exchange::binance::normalize(&bridge, "BTCUSDT")?
    else {
        return Err("expected delta".into());
    };
    assert!(book.apply_delta_if_fresh(bridge)?);
    assert_eq!(book.sequence(), Some(25));

    let next = RawMarketRecord::new(
        RawSource::WebSocketDelta,
        symbol,
        1,
        3,
        r#"{"e":"depthUpdate","E":3,"T":3,"s":"BTCUSDT","U":40,"u":45,"pu":25,"st":1,"b":[],"a":[]}"#.to_owned(),
    )?;
    let venue::domain::MarketEvent::Delta(next) =
        venue::exchange::binance::normalize(&next, "BTCUSDT")?
    else {
        return Err("expected delta".into());
    };
    assert!(book.apply_delta_if_fresh(next)?);
    assert_eq!(book.sequence(), Some(45));
    Ok(())
}

#[test]
fn truncated_raw_journal_never_becomes_replay_input() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("market.jsonl");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    file.write_all(br#"{"capture_sequence":1"#)?;
    file.sync_data()?;

    assert!(matches!(
        RawMarketRecorder::open(path),
        Err(RawError::Truncated)
    ));
    Ok(())
}

#[test]
fn session_revokes_readiness_and_advances_generation_on_a_gap()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let symbol: Symbol = "BTC/USDT".parse()?;
    let recorder = RawMarketRecorder::open(directory.path().join("market.jsonl"))?;
    let mut session = MarketSession::new(symbol, recorder);
    session.ingest_snapshot(1, snapshot("BTC/USDT".parse()?)?.payload)?;
    assert!(session.ready());

    assert!(
        session
            .ingest_delta(2, delta("BTC/USDT".parse()?, 11, 12)?.payload)
            .is_err()
    );
    assert!(!session.ready());
    assert_eq!(session.generation(), 2);
    assert_eq!(session.recorder().recover()?.records.len(), 2);
    Ok(())
}

#[test]
fn session_bridges_buffered_delta_and_uses_backoff_for_faults()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let symbol: Symbol = "BTC/USDT".parse()?;
    let recorder = RawMarketRecorder::open(directory.path().join("market.jsonl"))?;
    let mut session = MarketSession::new(symbol, recorder);
    let buffered = session.ingest_delta_captured(1, delta("BTC/USDT".parse()?, 10, 11)?.payload)?;
    assert!(!buffered.applied);
    assert_eq!(session.state(), SessionState::Snapshotting);
    session.ingest_snapshot(2, snapshot("BTC/USDT".parse()?)?.payload)?;
    assert!(session.ready());

    assert!(
        session
            .on_transport_fault(100, TransportFault::RateLimited)
            .is_err()
    );
    assert_eq!(session.state(), SessionState::Backoff);
    let retry_at = session.retry_at_ms().ok_or("missing retry deadline")?;
    assert!((600..=1_100).contains(&retry_at));
    assert!(!session.begin_retry(retry_at.saturating_sub(1)));
    assert!(session.begin_retry(retry_at));
    assert_eq!(session.state(), SessionState::Snapshotting);
    session.ingest_snapshot(
        retry_at.saturating_add(1),
        snapshot("BTC/USDT".parse()?)?.payload,
    )?;
    assert!(session.ready());
    Ok(())
}

#[test]
fn session_captures_each_auxiliary_stream_without_affecting_book_state()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let symbol: Symbol = "DOGE/USDT".parse()?;
    let recorder = RawMarketRecorder::open(directory.path().join("market.jsonl"))?;
    let mut session = MarketSession::new(symbol, recorder);

    let trade = session.ingest_auxiliary(
        RawSource::WebSocketTrade,
        1,
        r#"{"e":"aggTrade","E":1,"s":"DOGEUSDT","a":2,"p":"0.1","q":"50","nq":"5","f":3,"l":4,"T":1,"m":false,"st":1}"#.to_owned(),
    )?;
    let ticker = session.ingest_auxiliary(
        RawSource::WebSocketTicker,
        2,
        r#"{"e":"bookTicker","u":2,"E":2,"T":2,"s":"DOGEUSDT","b":"0.1","B":"50","a":"0.2","A":"25","st":1}"#.to_owned(),
    )?;
    let bar = session.ingest_auxiliary(
        RawSource::WebSocketKline,
        119_999,
        r#"{"e":"kline","E":119999,"s":"DOGEUSDT","st":1,"k":{"t":60000,"T":119999,"s":"DOGEUSDT","i":"1m","o":"0.1","h":"0.2","l":"0.09","c":"0.15","x":true}}"#.to_owned(),
    )?;
    let mark = session.ingest_auxiliary(
        RawSource::WebSocketMarkFunding,
        120_000,
        r#"{"e":"markPriceUpdate","E":120000,"s":"DOGEUSDT","p":"0.1","i":"0.1","r":"0.0001","T":180000,"st":1}"#.to_owned(),
    )?;

    assert!(matches!(trade, MarketEvent::Trade(_)));
    assert!(matches!(ticker, MarketEvent::Ticker(_)));
    assert!(matches!(bar, MarketEvent::Bar(_)));
    assert!(matches!(mark, MarketEvent::MarkFunding(_)));
    assert_eq!(session.state(), SessionState::Snapshotting);
    assert_eq!(session.recorder().recover()?.records.len(), 4);
    Ok(())
}

#[test]
fn session_revokes_readiness_when_market_data_becomes_stale()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let symbol: Symbol = "BTC/USDT".parse()?;
    let recorder = RawMarketRecorder::open(directory.path().join("market.jsonl"))?;
    let mut session = MarketSession::new(symbol, recorder);
    session.ingest_snapshot(1, snapshot("BTC/USDT".parse()?)?.payload)?;

    assert!(session.ensure_fresh(5_001).is_ok());
    assert!(session.ensure_fresh(5_002).is_err());
    assert_eq!(session.state(), SessionState::Backoff);
    assert!(!session.ready());
    Ok(())
}

#[test]
fn scoped_recovery_rejects_cross_symbol_and_generation_regression()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let symbol: Symbol = "BTC/USDT".parse()?;
    let path = directory.path().join("scoped.jsonl");
    let mut recorder = RawMarketRecorder::open(&path)?;
    recorder.append(snapshot(symbol.clone())?)?;
    let mut other = snapshot("ETH/USDT".parse()?)?;
    other.received_at_ms = 2;
    recorder.append(other)?;
    assert!(matches!(
        RawMarketRecorder::open_for_symbol(&path, symbol.clone()),
        Err(RawError::Symbol)
    ));

    let generations = directory.path().join("generations.jsonl");
    let mut first = snapshot(symbol.clone())?;
    first.capture_sequence = 1;
    first.generation = 2;
    let mut second = snapshot(symbol)?;
    second.capture_sequence = 2;
    second.generation = 1;
    second.received_at_ms = 2;
    write_records(&generations, &[first, second])?;
    assert!(matches!(
        RawMarketRecorder::open_for_symbol(&generations, "BTC/USDT".parse()?),
        Err(RawError::Generation)
    ));
    Ok(())
}

#[test]
fn scoped_recovery_rejects_schema_parser_and_zero_received_records()
-> Result<(), Box<dyn std::error::Error>> {
    let symbol: Symbol = "BTC/USDT".parse()?;
    let cases: [CorruptCase; 3] = [
        (
            "schema",
            |value| value["schema_version"] = 2.into(),
            RawError::Schema,
        ),
        (
            "parser",
            |value| value["parser_schema_version"] = 2.into(),
            RawError::ParserSchema,
        ),
        (
            "received",
            |value| value["received_at_ms"] = 0.into(),
            RawError::Invalid,
        ),
    ];
    for (name, mutate, expected) in cases {
        let directory = tempdir()?;
        let path = directory.path().join(format!("{name}.jsonl"));
        let mut value = serde_json::to_value(snapshot(symbol.clone())?)?;
        mutate(&mut value);
        fs::write(&path, format!("{}\n", serde_json::to_string(&value)?))?;
        let actual = match RawMarketRecorder::open_for_symbol(&path, symbol.clone()) {
            Ok(_) => return Err("corrupt record was accepted".into()),
            Err(error) => error,
        };
        assert!(same_raw_error(actual, expected));
    }
    Ok(())
}

#[test]
fn same_received_timestamp_across_public_streams_is_valid() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempdir()?;
    let path = directory.path().join("same-time.jsonl");
    let symbol: Symbol = "BTC/USDT".parse()?;
    let mut recorder = RawMarketRecorder::open_for_symbol(&path, symbol.clone())?;
    recorder.append(snapshot(symbol.clone())?)?;
    recorder.append(RawMarketRecord::new(
        RawSource::WebSocketTrade,
        symbol,
        1,
        1,
        "{}".to_owned(),
    )?)?;
    assert_eq!(recorder.next_capture_sequence(), 3);
    Ok(())
}

#[test]
fn recovered_generation_overflow_is_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("overflow.jsonl");
    let symbol: Symbol = "BTC/USDT".parse()?;
    let mut record = snapshot(symbol.clone())?;
    record.capture_sequence = 1;
    record.generation = u64::MAX;
    write_records(&path, &[record])?;
    let recorder = RawMarketRecorder::open_for_symbol(&path, symbol.clone())?;
    assert!(matches!(
        MarketSession::recover(symbol, recorder),
        Err(venue::market::SessionError::Generation)
    ));
    Ok(())
}

fn write_records(
    path: &std::path::Path,
    records: &[RawMarketRecord],
) -> Result<(), Box<dyn std::error::Error>> {
    let contents = records
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    fs::write(path, format!("{contents}\n"))?;
    Ok(())
}

fn same_raw_error(actual: RawError, expected: RawError) -> bool {
    matches!(
        (actual, expected),
        (RawError::Schema, RawError::Schema)
            | (RawError::ParserSchema, RawError::ParserSchema)
            | (RawError::Invalid, RawError::Invalid)
    )
}

type CorruptCase = (&'static str, fn(&mut serde_json::Value), RawError);
