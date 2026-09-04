use super::*;
use crate::accounts::test_support::{Fixture, TestResult, login, now};
use venue_control_protocol::accounts::{ApiVerificationState, BindCredentialRequest, SecretValue};

fn request(id: &str, key: char) -> ManagedFollowerCreateRequest {
    ManagedFollowerCreateRequest {
        request_id: id.into(),
        credential: BindCredentialRequest {
            label: "托管一号".into(),
            api_key: SecretValue::new(key.to_string().repeat(32)),
            api_secret: SecretValue::new("S".repeat(32)),
        },
    }
}

#[tokio::test]
async fn managed_save_is_atomic_scoped_idempotent_and_never_grants_trading() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let now = now();
    let session = f.service.register(login("kol"), now).await?;
    let owner = f.service.authenticate(session.token.expose(), now).await?;
    let session2 = f.service.register(login("stranger"), now).await?;
    let stranger = f.service.authenticate(session2.token.expose(), now).await?;
    let req = request("00000000-0000-4000-8000-000000000601", 'K');
    assert_eq!(
        f.service
            .create_managed_follower(&owner, req.clone(), now)
            .await
            .err()
            .map(|e| e.code),
        Some(Code::Forbidden)
    );
    sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES('00000000-0000-4000-8000-000000000602',$1,'binance',$2)")
        .bind(&owner.user.user_id).bind(vec![91_u8;32]).execute(&f.pool).await?;
    sqlx::query("INSERT INTO venue_kol_profiles(kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES($1,'00000000-0000-4000-8000-000000000602','KOL','Title','','100','enabled',1,$2,$2)")
        .bind(&owner.user.user_id).bind(ms(now)?).execute(&f.pool).await?;
    let (a, b) = tokio::join!(
        f.service.create_managed_follower(&owner, req.clone(), now),
        f.service.create_managed_follower(&owner, req.clone(), now)
    );
    let saved = a?;
    assert_eq!(saved, b?);
    assert_eq!(saved.verification, ApiVerificationState::Unverified);
    assert_eq!(
        f.service
            .create_managed_follower(&owner, request(&req.request_id, 'Z'), now)
            .await
            .err()
            .map(|e| e.code),
        Some(Code::Conflict)
    );
    let users: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_users")
        .fetch_one(&f.pool)
        .await?;
    assert_eq!(
        f.service
            .create_managed_follower(
                &owner,
                request("00000000-0000-4000-8000-000000000603", 'K'),
                now
            )
            .await
            .err()
            .map(|e| e.code),
        Some(Code::Conflict)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_users")
            .fetch_one(&f.pool)
            .await?,
        users
    );
    let own = f.service.managed_followers(&owner).await?;
    assert!(own.can_manage);
    assert_eq!(own.accounts, vec![saved.clone()]);
    assert!(
        f.service
            .managed_followers(&stranger)
            .await?
            .accounts
            .is_empty()
    );
    assert!(
        f.service
            .managed_verification_subject(&stranger, &saved.managed_id, now)
            .await
            .is_err()
    );
    let json = serde_json::to_string(&saved)?;
    assert!(!json.contains(&"K".repeat(32)));
    assert!(!json.contains(&"S".repeat(32)));
    assert!(!json.contains("credential_id"));
    assert!(!json.contains("trading_account_id"));
    let (subject, credential_id) = f
        .service
        .managed_verification_subject(&owner, &saved.managed_id, now)
        .await?;
    assert!(
        f.service
            .verify_with(&owner, &credential_id, now, |_| async {
                Err(venue_gateway_binance::BinanceProbeError::Unavailable)
            })
            .await
            .is_err()
    );
    let encrypted: Vec<u8> = sqlx::query_scalar(
        "SELECT encrypted_credentials FROM venue_api_credentials WHERE credential_id=$1",
    )
    .bind(&credential_id)
    .fetch_one(&f.pool)
    .await?;
    assert!(
        !encrypted
            .windows(32)
            .any(|w| w == "S".repeat(32).as_bytes())
    );
    let clear = f.service.cipher.decrypt(
        &super::super::credential_scope(&subject.user.user_id, &credential_id),
        &encrypted,
    )?;
    let decoded: BindCredentialRequest = serde_json::from_slice(&clear)?;
    assert_eq!(decoded.api_key.expose(), req.credential.api_key.expose());
    // Even a database-created session and a syntactically valid renamed username cannot
    // turn the internal subject into a login-capable customer.
    sqlx::query("UPDATE venue_users SET username='managedfixture' WHERE user_id=$1")
        .bind(&subject.user.user_id)
        .execute(&f.pool)
        .await?;
    assert!(
        f.service
            .login(
                venue_control_protocol::accounts::LoginRequest {
                    username: "managedfixture".into(),
                    password: SecretValue::new("unavailable account dummy password".into())
                },
                now
            )
            .await
            .is_err()
    );
    sqlx::query("INSERT INTO venue_user_sessions(token_hash,user_id,expires_ms) VALUES($1,$2,$3)")
        .bind(crypto::fingerprint("a".repeat(64).as_bytes()))
        .bind(&subject.user.user_id)
        .bind(ms(now + 1000)?)
        .execute(&f.pool)
        .await?;
    assert!(f.service.authenticate(&"a".repeat(64), now).await.is_err());
    let verified = f
        .service
        .verify_with(&subject, &credential_id, now, |_| async move {
            Ok(venue_gateway_binance::BinanceCredentialProbe {
                account_identity_hash: [92; 32],
                observed_ms: now,
                has_exposure: false,
            })
        })
        .await?;
    assert_eq!(verified.verification, ApiVerificationState::Verified);
    assert_eq!(
        f.service.managed_followers(&owner).await?.accounts[0].verification,
        ApiVerificationState::Verified
    );
    let work: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM venue_kol_follow_relations)+(SELECT count(*) FROM venue_binance_commands)+(SELECT count(*) FROM venue_leader_bot_permissions)").fetch_one(&f.pool).await?;
    assert_eq!(work, 0);
    sqlx::query("UPDATE venue_kol_profiles SET profile_state='disabled',active_slot=NULL WHERE kol_user_id=$1").bind(&owner.user.user_id).execute(&f.pool).await?;
    assert!(!f.service.managed_followers(&owner).await?.can_manage);
    assert!(
        f.service
            .managed_verification_subject(&owner, &saved.managed_id, now)
            .await
            .is_err()
    );
    assert_eq!(
        f.service
            .create_managed_follower(&owner, req, now)
            .await
            .err()
            .map(|e| e.code),
        Some(Code::Forbidden)
    );
    f.cleanup().await
}
