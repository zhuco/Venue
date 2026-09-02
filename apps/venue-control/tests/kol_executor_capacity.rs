//! Deterministic offline fan-out harness. It measures central scheduling only; Binance network
//! time and a 2-core/4-GiB host remain separate deployment evidence.

use std::time::Instant;

use venue_control::{AccountSerialScheduler, MAX_ENABLED_FOLLOWERS, MAX_ENABLED_KOLS};

#[test]
fn five_kols_fan_out_to_two_hundred_followers_without_account_serial_blocking()
-> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut scheduler = AccountSerialScheduler::new(32);
    for kol in 0..MAX_ENABLED_KOLS {
        for follower in 0..(MAX_ENABLED_FOLLOWERS / MAX_ENABLED_KOLS) {
            scheduler.enqueue(format!("follower-{kol:02}-{follower:03}"), kol)?;
        }
    }
    let mut send_starts = Vec::with_capacity(MAX_ENABLED_FOLLOWERS);
    while send_starts.len() < MAX_ENABLED_FOLLOWERS {
        while let Some(claimed) = scheduler.claim_next() {
            send_starts.push(started.elapsed());
            scheduler.settle(&claimed.trading_account_id)?;
        }
    }
    send_starts.sort_unstable();
    let p50 = send_starts[send_starts.len() / 2];
    let p95 = send_starts[send_starts.len() * 95 / 100];
    let all_started = *send_starts.last().ok_or("no commands scheduled")?;
    assert!(p50 <= std::time::Duration::from_millis(100));
    assert!(p95 <= std::time::Duration::from_millis(500));
    assert!(all_started <= std::time::Duration::from_millis(1_500));
    assert_eq!(scheduler.in_flight(), 0);
    eprintln!(
        "offline scheduler fixture: kols={MAX_ENABLED_KOLS} followers={MAX_ENABLED_FOLLOWERS} p50_ms={:.3} p95_ms={:.3} all_started_ms={:.3} queue_depth={} global_in_flight=32",
        p50.as_secs_f64() * 1_000.0,
        p95.as_secs_f64() * 1_000.0,
        all_started.as_secs_f64() * 1_000.0,
        16,
    );
    Ok(())
}
