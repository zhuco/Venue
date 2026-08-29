use std::fs::{self, OpenOptions};

use rust_decimal::Decimal;
use tempfile::tempdir;
use venue::{
    domain::{AccountBalance, Asset, DomainEvent, EventHeader, EventId, EventSource, FactRecord},
    storage::{AcceptOutcome, Checkpoint, CheckpointStore, Journal, TradingFacts},
};

fn balance(id: &str, sequence: Option<u64>) -> Result<FactRecord, Box<dyn std::error::Error>> {
    Ok(FactRecord {
        header: EventHeader {
            schema_version: 1,
            event_id: EventId::new(id)?,
            source: EventSource::PrivateAccount,
            source_sequence: sequence,
            received_at_ms: 1,
            generation: 1,
        },
        event: DomainEvent::Balance(AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::new(5, 0),
            available_balance: Decimal::new(5, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        }),
    })
}

#[test]
fn journal_recovers_complete_entries_and_ignores_a_partial_tail()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("journal.jsonl");
    let mut journal = Journal::open(&path)?;
    assert_eq!(journal.append(balance("event-1", Some(1))?)?, 1);
    let mut file = OpenOptions::new().append(true).open(&path)?;
    use std::io::Write as _;
    file.write_all(br#"{"sequence":2"#)?;
    file.sync_data()?;

    let recovered = journal.recover()?;
    assert!(recovered.truncated_tail);
    assert_eq!(recovered.entries.len(), 1);
    Ok(())
}

#[test]
fn journal_reopen_repairs_a_partial_tail_before_the_next_append()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("journal.jsonl");
    let mut journal = Journal::open(&path)?;
    assert_eq!(journal.append(balance("event-1", Some(1))?)?, 1);
    let mut file = OpenOptions::new().append(true).open(&path)?;
    use std::io::Write as _;
    file.write_all(br#"{"sequence":2"#)?;
    file.sync_all()?;
    drop(file);
    drop(journal);

    let mut reopened = Journal::open(&path)?;
    assert_eq!(reopened.append(balance("event-2", Some(2))?)?, 2);
    let recovered = reopened.recover()?;
    assert!(!recovered.truncated_tail);
    assert_eq!(
        recovered
            .entries
            .iter()
            .map(|entry| entry.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    Ok(())
}

#[test]
fn facts_deduplicate_and_checkpoint_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let mut facts = TradingFacts::default();
    let first = balance("event-1", Some(2))?;
    assert_eq!(facts.accept(first.clone()), AcceptOutcome::Accepted);
    assert_eq!(facts.accept(first), AcceptOutcome::Duplicate);
    assert_eq!(
        facts.accept(balance("event-0", Some(1))?),
        AcceptOutcome::Late
    );

    let store = CheckpointStore::new(directory.path().join("checkpoint.json"));
    store.save(&Checkpoint {
        journal_sequence: 2,
        facts: facts.clone(),
    })?;
    assert_eq!(
        store.load()?,
        Some(Checkpoint {
            journal_sequence: 2,
            facts
        })
    );
    store.save(&Checkpoint {
        journal_sequence: 3,
        facts: TradingFacts::default(),
    })?;
    assert_eq!(
        store.load()?,
        Some(Checkpoint {
            journal_sequence: 3,
            facts: TradingFacts::default(),
        })
    );
    assert_eq!(
        fs::read_dir(directory.path())?
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains(".tmp."))
            .count(),
        0
    );
    Ok(())
}

#[test]
fn checkpoint_replays_only_facts_after_its_journal_watermark()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let journal_path = directory.path().join("journal.jsonl");
    let checkpoint_path = directory.path().join("checkpoint.json");
    let first = balance("event-1", Some(1))?;
    let second = balance("event-2", Some(2))?;
    let mut journal = Journal::open(&journal_path)?;
    let first_sequence = journal.append(first.clone())?;
    let mut before_restart = TradingFacts::default();
    assert_eq!(before_restart.accept(first), AcceptOutcome::Accepted);
    CheckpointStore::new(&checkpoint_path).save(&Checkpoint {
        journal_sequence: first_sequence,
        facts: before_restart,
    })?;
    journal.append(second)?;

    let checkpoint = CheckpointStore::new(&checkpoint_path)
        .load()?
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "checkpoint was not persisted")
        })?;
    let mut restored = checkpoint.facts;
    for entry in journal
        .recover()?
        .entries
        .into_iter()
        .filter(|entry| entry.sequence > checkpoint.journal_sequence)
    {
        assert_eq!(restored.accept(entry.record), AcceptOutcome::Accepted);
    }

    assert_eq!(restored.records().len(), 2);
    assert_eq!(restored.source_watermark(), 2);
    Ok(())
}
