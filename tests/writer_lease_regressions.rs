use std::{fs, sync::mpsc, thread};

use tempfile::tempdir;
use venue::{
    domain,
    execution::{
        ExecutableHandoffReceipt, FlatReceipt, ProtectedReceipt, WriterLeaseAuthority,
        WriterLeaseError, WriterScope,
    },
};

fn scope() -> Result<WriterScope, Box<dyn std::error::Error>> {
    Ok(WriterScope {
        exchange: "binance".to_owned(),
        account: "primary".to_owned(),
        symbol: "BTC/USDT".parse::<domain::Symbol>()?,
        owner_scope: "scalping_run_1".to_owned(),
    })
}

fn authority_path(directory: &tempfile::TempDir) -> std::path::PathBuf {
    directory.path().join("authority.json")
}

fn summary() -> String {
    "a".repeat(64)
}

fn executable_handoff(
    predecessor: venue::execution::WriterSession,
    readback_generation: u64,
) -> ExecutableHandoffReceipt {
    ExecutableHandoffReceipt {
        receipt_id: "handoff_1".to_owned(),
        scope: predecessor.scope.clone(),
        predecessor,
        readback_generation,
        handoff_sha256: "b".repeat(64),
        successor_executable_sha256: "c".repeat(64),
    }
}

#[test]
fn expired_ttl_fences_old_writer_and_never_elects_a_replacement()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let authority = WriterLeaseAuthority::open(authority_path(&directory), scope.clone())?;
    let old = authority.register_initial(100, 7)?;

    assert!(matches!(
        authority.dispatch_guard(&old, 10_100),
        Err(WriterLeaseError::Expired)
    ));
    assert!(matches!(
        authority.register_initial(10_101, 8),
        Err(WriterLeaseError::WriterExists)
    ));
    Ok(())
}

#[test]
fn persistent_resident_guard_ignores_ttl_but_keeps_exact_writer_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let authority = WriterLeaseAuthority::open(authority_path(&directory), scope)?;
    let expired = authority.register_initial(100, 7)?;

    let guard = authority.persistent_dispatch_guard(&expired)?;
    drop(guard);
    let renewed = authority.renew(&expired, 101)?;
    assert!(matches!(
        authority.persistent_dispatch_guard(&expired),
        Err(WriterLeaseError::Fenced)
    ));
    authority.persistent_dispatch_guard(&renewed)?;
    Ok(())
}

#[test]
fn dispatch_guard_holds_the_os_lock_across_the_gateway_call()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let path = authority_path(&directory);
    let authority = WriterLeaseAuthority::open(path.clone(), scope.clone())?;
    let session = authority.register_initial(100, 7)?;
    let guard = authority.dispatch_guard(&session, 101)?;
    let (sender, receiver) = mpsc::channel();
    let thread_scope = scope.clone();
    let worker = thread::spawn(move || {
        let authority = WriterLeaseAuthority::open(path, thread_scope);
        let outcome = authority.and_then(|authority| authority.dispatch_guard(&session, 102));
        sender.send(outcome.is_err()).map_err(|_| "channel closed")
    });
    assert!(receiver.recv()?);
    drop(guard);
    let worker_result = worker
        .join()
        .map_err(|_| std::io::Error::other("lock worker panicked"))?;
    worker_result.map_err(std::io::Error::other)?;
    Ok(())
}

#[test]
fn current_backup_recovers_without_rolling_back_a_committed_lease()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let path = authority_path(&directory);
    let authority = WriterLeaseAuthority::open(path.clone(), scope.clone())?;
    let session = authority.register_initial(100, 7)?;
    let renewed = authority.renew(&session, 101)?;
    fs::write(&path, b"corrupt")?;

    let recovered = WriterLeaseAuthority::open(path, scope)?;
    let restored = recovered
        .active_session()?
        .ok_or("missing recovered writer")?;
    assert_eq!(restored.generation, renewed.generation);
    assert_eq!(restored.revision, renewed.revision);
    assert_eq!(restored.readback_generation, renewed.readback_generation);
    Ok(())
}

#[test]
fn every_corrupt_recovery_snapshot_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let path = authority_path(&directory);
    let authority = WriterLeaseAuthority::open(path.clone(), scope.clone())?;
    authority.register_initial(100, 7)?;
    fs::write(&path, b"bad")?;
    fs::write(path.with_file_name("authority.json.backup"), b"bad")?;
    fs::write(path.with_file_name("authority.json.next"), b"bad")?;
    assert!(matches!(
        WriterLeaseAuthority::open(path, scope),
        Err(WriterLeaseError::CorruptAuthority)
    ));
    Ok(())
}

#[test]
fn cross_scope_and_flat_receipt_replay_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let predecessor_scope = scope()?;
    let authority =
        WriterLeaseAuthority::open(authority_path(&directory), predecessor_scope.clone())?;
    let predecessor = authority.register_initial(100, 7)?;
    let mut successor_scope = predecessor_scope.clone();
    successor_scope.owner_scope = "scalping_run_2".to_owned();
    let receipt = FlatReceipt {
        receipt_id: "flat_1".to_owned(),
        predecessor: predecessor.clone(),
        scope: predecessor_scope.clone(),
        readback_generation: predecessor.readback_generation + 1,
        summary_sha256: summary(),
    };
    assert!(matches!(
        authority.consume_flat_receipt(&successor_scope, &receipt, 10_101),
        Err(WriterLeaseError::Scope)
    ));
    let successor = authority.consume_flat_receipt(&predecessor_scope, &receipt, 10_101)?;
    assert_ne!(successor.token, predecessor.token);
    assert!(matches!(
        authority.consume_flat_receipt(&predecessor_scope, &receipt, 10_102),
        Err(WriterLeaseError::ReceiptConsumed)
    ));
    Ok(())
}

#[test]
fn protected_receipt_keeps_predecessor_protection_only_without_activation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let authority = WriterLeaseAuthority::open(authority_path(&directory), scope.clone())?;
    let predecessor = authority.register_initial(100, 7)?;
    authority.retain_protected_predecessor(&ProtectedReceipt {
        predecessor: predecessor.clone(),
        scope: scope.clone(),
        readback_generation: predecessor.readback_generation + 1,
        summary_sha256: summary(),
    })?;
    assert!(matches!(
        authority.dispatch_guard(&predecessor, 101),
        Err(WriterLeaseError::ProtectionOnly)
    ));
    let protection_session = authority
        .active_session()?
        .ok_or("missing protection-only writer")?;
    let protection_guard = authority.protection_dispatch_guard(&protection_session, 101)?;
    drop(protection_guard);
    let flat = FlatReceipt {
        receipt_id: "flat_after_protected".to_owned(),
        predecessor: predecessor.clone(),
        scope: scope.clone(),
        readback_generation: predecessor.readback_generation + 2,
        summary_sha256: summary(),
    };
    assert!(matches!(
        authority.consume_flat_receipt(&scope, &flat, 102),
        Err(WriterLeaseError::Fenced)
    ));
    Ok(())
}

#[test]
fn protected_predecessor_can_renew_after_entry_lease_expiry_without_reopening_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let authority = WriterLeaseAuthority::open(authority_path(&directory), scope.clone())?;
    let predecessor = authority.register_initial(100, 7)?;
    authority.retain_protected_predecessor(&ProtectedReceipt {
        predecessor,
        scope,
        readback_generation: 8,
        summary_sha256: summary(),
    })?;
    let protected = authority
        .active_session()?
        .ok_or("missing protection-only writer")?;

    let renewed = authority.renew_protection(&protected, 10_101)?;
    let guard = authority.protection_dispatch_guard(&renewed, 10_102)?;
    drop(guard);
    assert!(matches!(
        authority.dispatch_guard(&renewed, 10_102),
        Err(WriterLeaseError::ProtectionOnly)
    ));
    Ok(())
}

#[test]
fn flat_retirement_preserves_one_authority_and_advances_the_next_writer_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let authority = WriterLeaseAuthority::open(authority_path(&directory), scope.clone())?;
    let predecessor = authority.register_initial(100, 7)?;
    let receipt = FlatReceipt {
        receipt_id: "flat_retire_1".to_owned(),
        predecessor: predecessor.clone(),
        scope,
        readback_generation: 8,
        summary_sha256: summary(),
    };
    authority.retire_flat(&receipt)?;
    assert!(authority.active_session()?.is_none());
    let successor = authority.register_initial(101, 9)?;
    assert!(successor.generation > predecessor.generation);
    assert!(matches!(
        authority.retire_flat(&receipt),
        Err(WriterLeaseError::ReceiptConsumed)
    ));
    Ok(())
}

#[test]
fn executable_handoff_fences_old_writer_before_successor_activation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let path = authority_path(&directory);
    let authority = WriterLeaseAuthority::open(path.clone(), scope.clone())?;
    let predecessor = authority.register_initial(100, 7)?;
    let receipt = executable_handoff(predecessor.clone(), 8);

    authority.fence_for_executable_handoff(&receipt)?;
    assert!(authority.active_session()?.is_none());
    assert!(matches!(
        authority.renew(&predecessor, 101),
        Err(WriterLeaseError::NoWriter)
    ));
    assert!(matches!(
        authority.recover_same_scope_after_readback(&predecessor, 8, 10_101),
        Err(WriterLeaseError::NoWriter)
    ));
    assert!(matches!(
        authority.persistent_dispatch_guard(&predecessor),
        Err(WriterLeaseError::NoWriter)
    ));
    assert!(matches!(
        authority.register_initial(102, 9),
        Err(WriterLeaseError::HandoffPending)
    ));

    let recovered = WriterLeaseAuthority::open(path, scope)?;
    assert!(matches!(
        recovered.activate_executable_handoff_successor(&receipt, &"d".repeat(64), 103),
        Err(WriterLeaseError::Receipt)
    ));
    let successor = recovered.activate_executable_handoff_successor(
        &receipt,
        &receipt.successor_executable_sha256,
        103,
    )?;
    assert!(successor.generation > predecessor.generation);
    assert!(successor.readback_generation > predecessor.readback_generation);
    recovered.dispatch_guard(&successor, 104)?;
    assert!(matches!(
        recovered.activate_executable_handoff_successor(
            &receipt,
            &receipt.successor_executable_sha256,
            105,
        ),
        Err(WriterLeaseError::HandoffReceiptConsumed)
    ));
    assert!(matches!(
        recovered.fence_for_executable_handoff(&receipt),
        Err(WriterLeaseError::HandoffReceiptConsumed)
    ));
    Ok(())
}

#[test]
fn executable_handoff_accepts_the_exact_final_signed_watermark()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let authority = WriterLeaseAuthority::open(authority_path(&directory), scope.clone())?;
    let predecessor = authority.register_initial(100, 7)?;
    let receipt = executable_handoff(predecessor.clone(), 7);

    authority.fence_for_executable_handoff(&receipt)?;
    let successor = authority.activate_executable_handoff_successor(
        &receipt,
        &receipt.successor_executable_sha256,
        101,
    )?;
    assert!(successor.generation > predecessor.generation);
    assert_eq!(
        successor.readback_generation,
        predecessor.readback_generation
    );
    authority.dispatch_guard(&successor, 102)?;
    Ok(())
}

#[test]
fn executable_handoff_rejects_regressing_or_foreign_receipts()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let scope = scope()?;
    let authority = WriterLeaseAuthority::open(authority_path(&directory), scope.clone())?;
    let predecessor = authority.register_initial(100, 7)?;

    let regressing = executable_handoff(predecessor.clone(), 6);
    assert!(matches!(
        authority.fence_for_executable_handoff(&regressing),
        Err(WriterLeaseError::Receipt)
    ));

    let mut foreign = executable_handoff(predecessor, 8);
    foreign.scope.owner_scope = "different_owner".to_owned();
    assert!(matches!(
        authority.fence_for_executable_handoff(&foreign),
        Err(WriterLeaseError::Receipt)
    ));
    Ok(())
}
