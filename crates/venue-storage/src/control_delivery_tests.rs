use std::{fs, fs::OpenOptions, io::Write};

use tempfile::tempdir;

use super::*;

#[test]
fn compact_replaces_only_the_exact_fenced_history_and_preserves_append_sequence()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("projection.jsonl");
    let mut journal = OpaqueJournal::open(&path)?;
    journal.append(1, b"old-a")?;
    journal.append(2, b"old-b")?;

    assert!(matches!(
        journal.compact(2, &[b"root".to_vec(), b"cursor".to_vec()]),
        Err(OpaqueJournalError::SequenceConflict { actual: 3, .. })
    ));
    journal.compact(3, &[b"root".to_vec(), b"cursor".to_vec()])?;
    assert_eq!(
        journal.recover()?,
        vec![
            OpaqueJournalRecord {
                sequence: 1,
                payload: b"root".to_vec(),
            },
            OpaqueJournalRecord {
                sequence: 2,
                payload: b"cursor".to_vec(),
            },
        ]
    );
    assert_eq!(journal.append(3, b"next")?, 3);
    drop(journal);

    let mut recovered = OpaqueJournal::open(path)?;
    assert_eq!(
        recovered
            .recover()?
            .into_iter()
            .map(|record| record.payload)
            .collect::<Vec<_>>(),
        vec![b"root".to_vec(), b"cursor".to_vec(), b"next".to_vec()]
    );
    Ok(())
}

#[test]
fn bounded_append_and_compaction_reject_before_replacing_durable_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("projection.jsonl");
    let mut journal = OpaqueJournal::open(&path)?;
    assert!(matches!(
        journal.append_bounded(1, b"root", 1),
        Err(OpaqueJournalError::FileLimitExceeded)
    ));
    assert_eq!(fs::metadata(&path)?.len(), 0);

    journal.append(1, b"root")?;
    let durable = fs::read(&path)?;
    assert!(matches!(
        journal.compact_bounded(2, &[vec![255; 1024]], 1),
        Err(OpaqueJournalError::FileLimitExceeded)
    ));
    assert_eq!(fs::read(&path)?, durable);
    assert!(!compaction_path(&path, "next").exists());
    assert!(!compaction_path(&path, "previous").exists());
    Ok(())
}

#[test]
fn interrupted_compaction_recovers_old_before_swap_and_new_after_swap()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("projection.jsonl");
    let previous = compaction_path(&path, "previous");
    let next = compaction_path(&path, "next");
    let mut old = OpaqueJournal::open(&path)?;
    old.append(1, b"old")?;
    drop(old);

    fs::rename(&path, &previous)?;
    fs::write(&next, b"partial replacement")?;
    let mut rolled_back = OpaqueJournal::open(&path)?;
    assert_eq!(rolled_back.recover()?[0].payload, b"old");
    assert!(!previous.exists());
    assert!(!next.exists());
    drop(rolled_back);

    fs::rename(&path, &previous)?;
    let replacement_path = directory.path().join("replacement.jsonl");
    let mut replacement = OpaqueJournal::open(&replacement_path)?;
    replacement.append(1, b"new")?;
    drop(replacement);
    fs::rename(replacement_path, &path)?;
    let mut committed = OpaqueJournal::open(&path)?;
    assert_eq!(committed.recover()?[0].payload, b"new");
    assert!(!previous.exists());
    Ok(())
}

#[test]
fn crash_tail_is_repaired_before_the_expected_sequence_append()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("control-delivery.jsonl");
    let mut journal = OpaqueJournal::open(&path)?;
    assert_eq!(journal.append(1, b"root")?, 1);
    let durable_prefix = fs::read(&path)?;

    let mut file = OpenOptions::new().append(true).open(&path)?;
    file.write_all(b"{\"schema_version\":1,\"sequence\":2")?;
    file.sync_all()?;
    drop(file);

    assert_eq!(journal.append(2, b"claim")?, 2);
    let repaired = fs::read(&path)?;
    assert_eq!(&repaired[..durable_prefix.len()], &durable_prefix);
    assert_eq!(
        journal.recover()?,
        vec![
            OpaqueJournalRecord {
                sequence: 1,
                payload: b"root".to_vec(),
            },
            OpaqueJournalRecord {
                sequence: 2,
                payload: b"claim".to_vec(),
            },
        ]
    );
    Ok(())
}

#[test]
fn competing_stale_sequence_is_fenced_without_writing() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("control-delivery.jsonl");
    let mut first = OpaqueJournal::open(&path)?;
    let mut stale = OpaqueJournal::open(&path)?;

    assert_eq!(first.append(1, b"first")?, 1);
    let durable = fs::read(&path)?;
    assert!(matches!(
        stale.append(1, b"fork"),
        Err(OpaqueJournalError::SequenceConflict {
            expected: 1,
            actual: 2
        })
    ));
    assert_eq!(fs::read(&path)?, durable);
    assert_eq!(stale.append(2, b"second")?, 2);
    Ok(())
}

#[test]
fn restart_recovers_opaque_payloads_and_continues_the_chain()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("control-delivery.jsonl");
    {
        let mut journal = OpaqueJournal::open(&path)?;
        journal.append(1, &[0, 1, 2, 255])?;
        journal.append(2, b"ack-confirmed")?;
    }

    let mut restarted = OpaqueJournal::open(&path)?;
    assert_eq!(
        restarted.recover()?,
        vec![
            OpaqueJournalRecord {
                sequence: 1,
                payload: vec![0, 1, 2, 255],
            },
            OpaqueJournalRecord {
                sequence: 2,
                payload: b"ack-confirmed".to_vec(),
            },
        ]
    );
    assert_eq!(restarted.append(3, b"receipt")?, 3);
    assert_eq!(restarted.recover()?.len(), 3);
    Ok(())
}

#[test]
fn first_durable_append_is_recovered_after_restart() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("control-delivery.jsonl");
    {
        let mut journal = OpaqueJournal::open(&path)?;
        assert_eq!(journal.append(1, b"root")?, 1);
    }

    let mut restarted = OpaqueJournal::open(path)?;
    assert_eq!(
        restarted.recover()?,
        vec![OpaqueJournalRecord {
            sequence: 1,
            payload: b"root".to_vec(),
        }]
    );
    Ok(())
}

#[test]
fn complete_payload_or_chain_tampering_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    for field in ["payload", "previous_sha256"] {
        let directory = tempdir()?;
        let path = directory.path().join("control-delivery.jsonl");
        let mut journal = OpaqueJournal::open(&path)?;
        journal.append(1, b"root")?;
        journal.append(2, b"claim")?;

        let bytes = fs::read(&path)?;
        let mut lines = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(serde_json::from_slice::<serde_json::Value>)
            .collect::<Result<Vec<_>, _>>()?;
        match field {
            "payload" => lines[1]["payload"][0] = serde_json::json!(0),
            "previous_sha256" => lines[1]["previous_sha256"][0] = serde_json::json!(7),
            _ => return Err("unexpected tamper field".into()),
        }
        let mut tampered = Vec::new();
        for line in lines {
            serde_json::to_writer(&mut tampered, &line)?;
            tampered.push(b'\n');
        }
        fs::write(&path, &tampered)?;

        assert!(matches!(
            OpaqueJournal::open(&path),
            Err(OpaqueJournalError::Corrupt)
        ));
        assert_eq!(fs::read(&path)?, tampered);
    }
    Ok(())
}

#[test]
fn complete_malformed_line_fails_closed_without_tail_repair()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let path = directory.path().join("control-delivery.jsonl");
    let mut journal = OpaqueJournal::open(&path)?;
    journal.append(1, b"root")?;
    let mut file = OpenOptions::new().append(true).open(&path)?;
    file.write_all(b"{not-json}\n")?;
    file.sync_all()?;
    let corrupted = fs::read(&path)?;

    assert!(matches!(
        journal.append(2, b"claim"),
        Err(OpaqueJournalError::Decode(_))
    ));
    assert_eq!(fs::read(&path)?, corrupted);
    Ok(())
}

#[test]
fn open_fails_closed_for_a_missing_or_invalid_parent_directory()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let missing_path = directory.path().join("missing").join("control.jsonl");
    assert!(matches!(
        OpaqueJournal::open(&missing_path),
        Err(OpaqueJournalError::Storage(StorageError::Io { .. }))
    ));
    assert!(!missing_path.exists());

    let not_directory = directory.path().join("not-a-directory");
    fs::write(&not_directory, b"file")?;
    let invalid_path = not_directory.join("control.jsonl");
    assert!(matches!(
        OpaqueJournal::open(&invalid_path),
        Err(OpaqueJournalError::Storage(StorageError::Io { .. }))
    ));
    assert!(!invalid_path.exists());
    Ok(())
}
