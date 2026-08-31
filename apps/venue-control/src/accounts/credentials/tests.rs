use super::*;
use crate::accounts::test_support::{Fixture, TestResult, login, now};
use venue_control_protocol::accounts::SecretValue;
use venue_gateway_binance::BinanceCredentialProbe;

fn binding(seed: char) -> BindCredentialRequest {
    BindCredentialRequest {
        label: format!("test-{seed}"),
        api_key: SecretValue::new(seed.to_string().repeat(32)),
        api_secret: SecretValue::new("fixturesecretvalue1234567890123456".into()),
    }
}
fn proof(identity: u8, exposed: bool) -> Result<BinanceCredentialProbe, BinanceProbeError> {
    Ok(BinanceCredentialProbe {
        account_identity_hash: [identity; 32],
        observed_ms: now(),
        has_exposure: exposed,
    })
}

#[tokio::test]
async fn postgres_superseded_probe_cannot_restore_readiness_and_logout_fences_commands()
-> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let session = f.service.register(login("alice"), now()).await?;
    let a = f
        .service
        .authenticate(session.token.expose(), now())
        .await?;
    let credential = f.service.bind_credential(&a, binding('A'), now()).await?;
    let (entered, received) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let older = f
        .service
        .verify_with(&a, &credential.credential_id, now(), |_| async {
            let _ = entered.send(());
            let _ = wait.await;
            proof(1, false)
        });
    let newer = async {
        received.await?;
        let result = f
            .service
            .verify_with(&a, &credential.credential_id, now(), |_| async {
                Err(BinanceProbeError::AccountMode)
            })
            .await?;
        let _ = release.send(());
        Ok::<_, Box<dyn std::error::Error>>(result)
    };
    let (older, newer) = tokio::join!(older, newer);
    assert_eq!(older.err().map(|e| e.code), Some(Code::Conflict));
    assert_eq!(newer?.verification, State::ModeMismatch);
    assert!(!f.service.overview(&a, now()).await?.credentials[0].selectable(now()));
    let verified = f
        .service
        .verify_with(&a, &credential.credential_id, now(), |_| async {
            proof(1, false)
        })
        .await?;
    f.service
        .select_credential(&a, &credential.credential_id, now())
        .await?;
    let principal = f
        .service
        .authenticate(session.token.expose(), now())
        .await?;
    let account = verified
        .trading_account_id
        .as_deref()
        .ok_or("account missing")?;
    let guard = f
        .service
        .authorize_command(&principal, VenueId::Binance, account, now())
        .await?;
    {
        let logout = f.service.logout(&principal);
        tokio::pin!(logout);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut logout)
                .await
                .is_err()
        );
        guard.rollback().await?;
        logout.await?;
    }
    assert!(
        f.service
            .authorize_command(&principal, VenueId::Binance, account, now())
            .await
            .is_err()
    );
    f.cleanup().await
}

#[tokio::test]
async fn postgres_removal_respects_running_nodes_and_another_keys_command_barrier() -> TestResult {
    use crate::{ControlService, PgControlRepository};
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let session = f.service.register(login("alice"), now()).await?;
    let a = f
        .service
        .authenticate(session.token.expose(), now())
        .await?;
    let mut keys = Vec::new();
    for seed in ['A', 'B'] {
        let c = f.service.bind_credential(&a, binding(seed), now()).await?;
        keys.push(
            f.service
                .verify_with(&a, &c.credential_id, now(), |_| async { proof(1, false) })
                .await?,
        );
    }
    let account = keys[0]
        .trading_account_id
        .as_deref()
        .ok_or("account missing")?;
    let removal = || DeleteCredentialRequest {
        credential_id: keys[0].credential_id.clone(),
        password: login("alice").password,
    };
    let mut snapshot = ControlSnapshot {
        schema_version: venue_control_protocol::CONTROL_SCHEMA_VERSION,
        generated_ms: now(),
        connection: venue_control_protocol::ConnectionState::Live,
        accounts: vec![venue_control_protocol::AccountSummary {
            venue: VenueId::Binance,
            mode: venue_control_protocol::GatewayMode::Live,
            trading_account_id: account.into(),
            health: HealthState::Healthy,
            equity: Some(rust_decimal::Decimal::ZERO),
            available_margin: Some(rust_decimal::Decimal::ZERO),
            unrealized_pnl: Some(rust_decimal::Decimal::ZERO),
            balances: Vec::new(),
            private_generation: 1,
            writer_generation: 1,
            last_reconciled_ms: now(),
        }],
        strategies: vec![],
        copy_relations: vec![],
        markets: vec![],
        ledger: vec![],
    };
    let control = ControlService::new(PgControlRepository::new(f.pool.clone()));
    control.publish_snapshot(&snapshot).await?;
    assert_eq!(
        f.service
            .delete_with(&a, removal(), now(), |_| async { proof(1, false) })
            .await
            .err()
            .map(|e| e.code),
        Some(Code::AccountInUse)
    );
    snapshot.generated_ms = now();
    snapshot.accounts[0].health = HealthState::Stopped;
    control.publish_snapshot(&snapshot).await?;
    f.service
        .select_credential(&a, &keys[1].credential_id, now())
        .await?;
    let selected = f
        .service
        .authenticate(session.token.expose(), now())
        .await?;
    let guard = f
        .service
        .authorize_command(&selected, VenueId::Binance, account, now())
        .await?;
    let (entered, received) = tokio::sync::oneshot::channel();
    {
        let deletion = f.service.delete_with(&a, removal(), now(), |_| async {
            let _ = entered.send(());
            proof(1, false)
        });
        tokio::pin!(deletion);
        tokio::select! {
            result=&mut deletion=>return Err(format!("deletion completed before probe barrier: {result:?}").into()),
            result=received=>{result?;}
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut deletion)
                .await
                .is_err()
        );
        guard.rollback().await?;
        deletion.await?;
    }
    assert_eq!(
        f.service
            .overview(&selected, now())
            .await?
            .credentials
            .len(),
        1
    );
    f.cleanup().await
}

#[tokio::test]
async fn postgres_login_sessions_encrypt_bindings_and_isolate_users() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let alice = f.service.register(login("Alice"), now()).await?;
    let bob = f.service.register(login("Bob"), now()).await?;
    assert_eq!(
        f.service
            .register(login("alice"), now())
            .await
            .err()
            .map(|e| e.code),
        Some(Code::UsernameUnavailable)
    );
    let a = f.service.authenticate(alice.token.expose(), now()).await?;
    let b = f.service.authenticate(bob.token.expose(), now()).await?;
    let credential = f.service.bind_credential(&a, binding('A'), now()).await?;
    assert!(f.service.overview(&b, now()).await?.credentials.is_empty());
    assert_eq!(
        f.service
            .select_credential(&b, &credential.credential_id, now())
            .await
            .err()
            .map(|e| e.code),
        Some(Code::NotFound)
    );
    assert_eq!(
        f.service
            .select_credential(&a, &credential.credential_id, now())
            .await
            .err()
            .map(|e| e.code),
        Some(Code::VerificationRequired)
    );
    let encrypted: Vec<u8> =
        sqlx::query_scalar("SELECT encrypted_credentials FROM venue_api_credentials")
            .fetch_one(&f.pool)
            .await?;
    assert!(!encrypted.windows(16).any(|w| w == b"AAAAAAAAAAAAAAAA"));
    let serialized = serde_json::to_string(&f.service.overview(&a, now()).await?)?;
    assert!(!serialized.contains("fixturesecret"));
    assert!(!serialized.contains("AAAAAAAAAAAAAAAA"));
    let restart = AccountService::new(
        f.pool.clone(),
        crate::accounts::CredentialCipher::from_key(&[17; 32])?,
    )?;
    assert_eq!(restart.overview(&a, now()).await?.credentials.len(), 1);
    let relogged = restart.login(login("ALICE"), now()).await?;
    assert_eq!(relogged.user.user_id, a.user.user_id);
    assert_ne!(relogged.token.expose(), alice.token.expose());
    restart.logout(&a).await?;
    assert_eq!(
        restart
            .authenticate(alice.token.expose(), now())
            .await
            .err()
            .map(|e| e.code),
        Some(Code::Unauthorized)
    );
    assert!(
        restart
            .authenticate(relogged.token.expose(), now() + 12 * 60 * 60 * 1000 + 1)
            .await
            .is_err()
    );
    f.cleanup().await
}

#[tokio::test]
async fn postgres_verification_reuses_real_account_and_revokes_old_readiness() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let session = f.service.register(login("alice"), now()).await?;
    let a = f
        .service
        .authenticate(session.token.expose(), now())
        .await?;
    let first = f.service.bind_credential(&a, binding('A'), now()).await?;
    let second = f.service.bind_credential(&a, binding('B'), now()).await?;
    let first = f
        .service
        .verify_with(&a, &first.credential_id, now(), |_| async {
            proof(1, false)
        })
        .await?;
    let second = f
        .service
        .verify_with(&a, &second.credential_id, now(), |_| async {
            proof(1, false)
        })
        .await?;
    assert!(first.selectable(now()));
    assert_eq!(first.trading_account_id, second.trading_account_id);
    f.service
        .select_credential(&a, &first.credential_id, now())
        .await?;
    let a = f
        .service
        .authenticate(session.token.expose(), now())
        .await?;
    let account = first
        .trading_account_id
        .as_deref()
        .ok_or("missing account")?;
    f.service
        .authorize_command(&a, VenueId::Binance, account, now())
        .await?
        .rollback()
        .await?;
    assert!(
        f.service
            .authorize_command(
                &a,
                VenueId::Binance,
                "00000000-0000-4000-8000-000000000099",
                now()
            )
            .await
            .is_err()
    );
    let failed = f
        .service
        .verify_with(&a, &first.credential_id, now(), |_| async {
            Err(BinanceProbeError::AccountMode)
        })
        .await?;
    assert_eq!(failed.verification, State::ModeMismatch);
    assert!(!failed.api_reachable);
    assert!(
        f.service
            .authorize_command(&a, VenueId::Binance, account, now())
            .await
            .is_err()
    );
    let session = f.service.register(login("bob"), now()).await?;
    let b = f
        .service
        .authenticate(session.token.expose(), now())
        .await?;
    let other = f.service.bind_credential(&b, binding('C'), now()).await?;
    let other = f
        .service
        .verify_with(&b, &other.credential_id, now(), |_| async {
            proof(1, false)
        })
        .await?;
    assert_eq!(other.verification, State::AccountConflict);
    assert!(other.trading_account_id.is_none());
    f.cleanup().await
}

#[tokio::test]
async fn postgres_removal_requires_password_fresh_flat_readback_and_no_runtime_custody()
-> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let session = f.service.register(login("alice"), now()).await?;
    let a = f
        .service
        .authenticate(session.token.expose(), now())
        .await?;
    let credential = f.service.bind_credential(&a, binding('A'), now()).await?;
    let credential = f
        .service
        .verify_with(&a, &credential.credential_id, now(), |_| async {
            proof(1, false)
        })
        .await?;
    let removal = || DeleteCredentialRequest {
        credential_id: credential.credential_id.clone(),
        password: login("alice").password,
    };
    let mut wrong = removal();
    wrong.password = SecretValue::new("wrong".into());
    assert_eq!(
        f.service
            .delete_with(&a, wrong, now(), |_| async { proof(1, false) })
            .await
            .err()
            .map(|e| e.code),
        Some(Code::InvalidLogin)
    );
    assert_eq!(
        f.service
            .delete_with(&a, removal(), now(), |_| async { proof(1, true) })
            .await
            .err()
            .map(|e| e.code),
        Some(Code::AccountInUse)
    );
    assert_eq!(
        f.service
            .delete_with(&a, removal(), now(), |_| async {
                Err(BinanceProbeError::Unavailable)
            })
            .await
            .err()
            .map(|e| e.code),
        Some(Code::AccountInUse)
    );
    f.service
        .delete_with(&a, removal(), now(), |_| async { proof(1, false) })
        .await?;
    assert!(f.service.overview(&a, now()).await?.credentials.is_empty());
    let encrypted_count: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_api_credentials")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(encrypted_count, 0);
    let rebound = f.service.bind_credential(&a, binding('A'), now()).await?;
    let rebound = f
        .service
        .verify_with(&a, &rebound.credential_id, now(), |_| async {
            proof(1, false)
        })
        .await?;
    assert_eq!(rebound.trading_account_id, credential.trading_account_id);
    f.cleanup().await
}
