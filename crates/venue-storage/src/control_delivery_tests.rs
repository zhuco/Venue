use std::{fs, fs::OpenOptions, io::Write};

use tempfile::tempdir;

use super::*;

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
