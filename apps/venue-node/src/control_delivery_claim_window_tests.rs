use super::*;
use venue_control_protocol::{AccountDeliveryClaimRequest, CopySemanticJobDelivery};

#[tokio::test]
async fn copy_claim_window_is_exactly_capped_without_renewing_the_job()
-> Result<(), Box<dyn std::error::Error>> {
    for (purpose, job_expiry, lease_expiry, valid) in [
        (AccountDeliveryPurpose::Install, 300, 300, true),
        (AccountDeliveryPurpose::Install, 300, 301, false),
        (AccountDeliveryPurpose::Install, 300, 299, false),
        (AccountDeliveryPurpose::Install, 2_000, 1_100, true),
        (AccountDeliveryPurpose::Install, 2_000, 1_101, false),
        (AccountDeliveryPurpose::Install, 100, 1_100, false),
        (AccountDeliveryPurpose::ReconcileOnly, 100, 1_100, true),
        (AccountDeliveryPurpose::ReconcileOnly, 100, 300, false),
    ] {
        let epoch = if purpose == AccountDeliveryPurpose::Install {
            1
        } else {
            2
        };
        let mut response = claim(epoch, 100, lease_expiry, purpose)?;
        response.payload = AccountDeliveryPayload::CopySemanticJob(CopySemanticJobDelivery {
            job_id: "copy-window-fixture".into(),
            job_digest: [3; 32],
            symbol: response.lease.binding.symbol.clone(),
            // Only transport DTO validation is under test; no Actor or execution consumes these.
            manifest: serde_json::json!({"transport_fixture": true}),
            semantic_job: serde_json::json!({"transport_fixture": true}),
            created_at_ms: 50,
            expires_at_ms: job_expiry,
        });
        let expected = response.clone();
        let (base_url, server) =
            spawn_control_server(serde_json::to_vec(&vec![response])?, 1).await?;
        let client = ControlHttpClient::new(ControlHttpClientConfig::local(base_url))?;
        let result = client
            .claim(&AccountDeliveryClaimRequest {
                schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
                binding: binding()?,
                node_id: NODE.into(),
                lease_duration_ms: 1_000,
                limit: 1,
            })
            .await;
        assert_eq!(
            result.is_ok(),
            valid,
            "{purpose:?}/{job_expiry}/{lease_expiry}"
        );
        if valid {
            assert_eq!(result?, vec![expected]);
        } else {
            assert!(matches!(
                result,
                Err(ControlHttpClientError::InvalidResponse)
            ));
        }
        assert_eq!(server.await??, vec!["/v2/account-node/deliveries/claim"]);
    }
    Ok(())
}

#[tokio::test]
async fn control_command_claim_window_cannot_be_arbitrarily_shortened()
-> Result<(), Box<dyn std::error::Error>> {
    let response = claim(1, 100, 300, AccountDeliveryPurpose::Install)?;
    let (base_url, server) = spawn_control_server(serde_json::to_vec(&vec![response])?, 1).await?;
    let client = ControlHttpClient::new(ControlHttpClientConfig::local(base_url))?;
    assert!(matches!(
        client
            .claim(&AccountDeliveryClaimRequest {
                schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
                binding: binding()?,
                node_id: NODE.into(),
                lease_duration_ms: 1_000,
                limit: 1,
            })
            .await,
        Err(ControlHttpClientError::InvalidResponse)
    ));
    server.await??;
    Ok(())
}

#[test]
fn confirmed_unknown_allows_early_read_only_reconciliation_across_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let journal = MemoryJournal::default();
    let mut inbox = new_inbox(journal.clone())?;
    install_ack(
        &mut inbox,
        claim(1, 100, 300, AccountDeliveryPurpose::Install)?,
        110,
        120,
    )?;
    let turn = inbox
        .actor_turn("command:request-32", 130)?
        .ok_or("actor turn missing")?;
    let unknown = inbox.record_actor_completion(
        turn.unknown(150, [0; 32], "exchange result cannot be proven")?,
        150,
    )?;
    inbox.confirm_receipt(unknown.value(), 160)?;

    let reconciliation = claim(2, 170, 400, AccountDeliveryPurpose::ReconcileOnly)?;
    assert!(matches!(
        inbox.accept_claim(reconciliation, 170)?,
        ClaimAcceptance::Reconcile(_)
    ));

    let recovered = new_inbox(journal)?;
    let turn = recovered
        .reconciliation_turn("command:request-32", 180)?
        .ok_or("reconciliation turn missing after restart")?;
    assert!(!turn.grants_gateway_capability());
    assert!(!turn.grants_writer_lease());
    assert!(!turn.grants_wal_authority());
    assert!(!turn.grants_dispatch_permit());
    Ok(())
}

#[test]
fn early_reconciliation_requires_a_confirmed_unknown_and_its_observed_time()
-> Result<(), Box<dyn std::error::Error>> {
    let early = || claim(2, 170, 400, AccountDeliveryPurpose::ReconcileOnly);

    let mut install_after_unknown = new_inbox(MemoryJournal::default())?;
    install_ack(
        &mut install_after_unknown,
        claim(1, 100, 300, AccountDeliveryPurpose::Install)?,
        110,
        120,
    )?;
    let turn = install_after_unknown
        .actor_turn("command:request-32", 130)?
        .ok_or("install-after-unknown actor turn missing")?;
    let receipt = install_after_unknown.record_actor_completion(
        turn.unknown(150, [0; 32], "exchange result cannot be proven")?,
        150,
    )?;
    install_after_unknown.confirm_receipt(receipt.value(), 160)?;
    assert!(matches!(
        install_after_unknown
            .accept_claim(claim(2, 170, 400, AccountDeliveryPurpose::Install)?, 170,),
        Err(ControlDeliveryError::FailedClosed)
    ));

    let mut applied = new_inbox(MemoryJournal::default())?;
    install_ack(
        &mut applied,
        claim(1, 100, 300, AccountDeliveryPurpose::Install)?,
        110,
        120,
    )?;
    let turn = applied
        .actor_turn("command:request-32", 130)?
        .ok_or("applied actor turn missing")?;
    let receipt = applied.record_actor_completion(
        turn.applied_fixture(150, digest(1), "actor checkpoint durable")?,
        150,
    )?;
    applied.confirm_receipt(receipt.value(), 160)?;
    assert!(matches!(
        applied.accept_claim(early()?, 170),
        Err(ControlDeliveryError::FailedClosed)
    ));

    let mut rejected = new_inbox(MemoryJournal::default())?;
    install_ack(
        &mut rejected,
        claim(1, 100, 300, AccountDeliveryPurpose::Install)?,
        110,
        120,
    )?;
    let turn = rejected
        .actor_turn("command:request-32", 130)?
        .ok_or("rejected actor turn missing")?;
    let receipt = rejected
        .record_actor_completion(turn.rejected(150, [0; 32], "actor rejected command")?, 150)?;
    rejected.confirm_receipt(receipt.value(), 160)?;
    assert!(matches!(
        rejected.accept_claim(early()?, 170),
        Err(ControlDeliveryError::FailedClosed)
    ));

    let mut unconfirmed = new_inbox(MemoryJournal::default())?;
    install_ack(
        &mut unconfirmed,
        claim(1, 100, 300, AccountDeliveryPurpose::Install)?,
        110,
        120,
    )?;
    let turn = unconfirmed
        .actor_turn("command:request-32", 130)?
        .ok_or("unconfirmed actor turn missing")?;
    unconfirmed.record_actor_completion(
        turn.unknown(150, [0; 32], "exchange result cannot be proven")?,
        150,
    )?;
    assert!(matches!(
        unconfirmed.accept_claim(early()?, 170),
        Err(ControlDeliveryError::FailedClosed)
    ));

    let mut too_early = new_inbox(MemoryJournal::default())?;
    install_ack(
        &mut too_early,
        claim(1, 100, 300, AccountDeliveryPurpose::Install)?,
        110,
        120,
    )?;
    let turn = too_early
        .actor_turn("command:request-32", 130)?
        .ok_or("too-early actor turn missing")?;
    let receipt = too_early.record_actor_completion(
        turn.unknown(180, [0; 32], "exchange result cannot be proven")?,
        180,
    )?;
    too_early.confirm_receipt(receipt.value(), 190)?;
    assert!(matches!(
        too_early.accept_claim(early()?, 190),
        Err(ControlDeliveryError::FailedClosed)
    ));
    Ok(())
}
