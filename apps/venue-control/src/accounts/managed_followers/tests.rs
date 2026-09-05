use super::*;
use crate::accounts::test_support::{Fixture, TestResult, login, now};
use rust_decimal::Decimal;
use venue_control_protocol::accounts::{ApiVerificationState, BindCredentialRequest, SecretValue};
use venue_control_protocol::managed_followers::{
    ManagedFollowRiskSettings, ManagedFollowSettingsUpsertRequest,
};

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
    let follow_request = ManagedFollowSettingsUpsertRequest {
        request_id: "00000000-0000-4000-8000-000000000604".into(),
        managed_id: saved.managed_id.clone(),
        settings: ManagedFollowRiskSettings {
            sizing: venue_control_protocol::follow_sizing::FollowSizing::FixedNotional {
                notional: Decimal::new(55, 1),
            },
            allocated_capital: Decimal::new(100, 0),
            multiplier: Decimal::ONE,
            max_order_notional: Decimal::new(20, 0),
            max_total_notional: Decimal::new(100, 0),
            max_deviation_bps: 100,
            allowed_symbols: vec!["BTC/USDT".parse()?],
        },
        expected_revision: None,
    };
    assert!(
        f.service
            .upsert_managed_follow_settings(&stranger, follow_request.clone(), now)
            .await
            .is_err()
    );
    let verification: serde_json::Value = sqlx::query_scalar(
        "SELECT verification_json FROM venue_api_credentials WHERE credential_id=$1",
    )
    .bind(&credential_id)
    .fetch_one(&f.pool)
    .await?;
    sqlx::query(
        "UPDATE venue_api_credentials SET verification_json='{}'::jsonb WHERE credential_id=$1",
    )
    .bind(&credential_id)
    .execute(&f.pool)
    .await?;
    assert!(
        f.service
            .upsert_managed_follow_settings(&owner, follow_request.clone(), now)
            .await
            .is_err()
    );
    let incomplete: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM venue_user_kol_bindings)+(SELECT count(*) FROM venue_kol_follow_relations)+(SELECT count(*) FROM venue_follow_requests)")
        .fetch_one(&f.pool).await?;
    assert_eq!(incomplete, 0);
    sqlx::query("UPDATE venue_api_credentials SET verification_json=$1 WHERE credential_id=$2")
        .bind(verification)
        .bind(&credential_id)
        .execute(&f.pool)
        .await?;
    let (left, right) = tokio::join!(
        f.service
            .upsert_managed_follow_settings(&owner, follow_request.clone(), now),
        f.service
            .upsert_managed_follow_settings(&owner, follow_request.clone(), now)
    );
    let relation = left?;
    assert_eq!(relation, right?);
    assert_eq!(relation.settings.sizing, follow_request.settings.sizing);
    let mut conflict = follow_request.clone();
    conflict.settings.multiplier = Decimal::from(2);
    assert_eq!(
        f.service
            .upsert_managed_follow_settings(&owner, conflict, now)
            .await
            .err()
            .map(|e| e.code),
        Some(Code::Conflict)
    );
    assert!(
        f.service
            .upsert_follow_settings(
                &subject,
                FollowSettingsUpsertRequest {
                    schema_version: KOL_SCHEMA_VERSION,
                    request_id: "00000000-0000-4000-8000-000000000609".into(),
                    settings: managed_settings(
                        follow_request.settings.clone(),
                        credential_id.clone()
                    ),
                    expected_revision: Some(relation.revision),
                },
                now
            )
            .await
            .is_err()
    );
    assert_eq!(relation.managed_id, saved.managed_id);
    assert_eq!(
        relation.state,
        venue_control_protocol::kol::FollowLifecycleState::Paused
    );
    let relation_json = serde_json::to_string(&relation)?;
    assert!(!relation_json.contains("credential_id"));
    assert!(!relation_json.contains("trading_account_id"));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT follower_user_id FROM venue_kol_follow_relations WHERE relation_id=$1",
        )
        .bind(&relation.relation_id)
        .fetch_one(&f.pool)
        .await?,
        subject.user.user_id
    );
    let work: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM venue_kol_follow_relations)+(SELECT count(*) FROM venue_binance_commands)+(SELECT count(*) FROM venue_leader_bot_permissions)").fetch_one(&f.pool).await?;
    assert_eq!(work, 1);
    sqlx::query("UPDATE venue_kol_profiles SET profile_state='disabled',active_slot=NULL WHERE kol_user_id=$1").bind(&owner.user.user_id).execute(&f.pool).await?;
    let pause = ManagedFollowLifecycleRequest {
        request_id: "00000000-0000-4000-8000-000000000605".into(),
        managed_id: saved.managed_id.clone(),
        relation_id: relation.relation_id.clone(),
        expected_revision: relation.revision,
        action: venue_control_protocol::kol::FollowLifecycleAction::Pause,
        risk_confirmed: false,
    };
    let paused = f
        .service
        .request_managed_follow_lifecycle(&owner, pause.clone(), now)
        .await?;
    assert_eq!(paused.revision, relation.revision + 1);
    assert_eq!(
        paused,
        f.service
            .request_managed_follow_lifecycle(&owner, pause, now)
            .await?
    );
    let mut tx = f.pool.begin().await?;
    assert!(
        sqlx::query("UPDATE venue_user_kol_bindings SET managed_id=NULL WHERE user_id=$1")
            .bind(&subject.user.user_id)
            .execute(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await?;
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

#[tokio::test]
async fn frozen_managed_table_is_preserved_and_nonempty_legacy_fails_closed() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    // DDL touches only Fixture's isolated random schema, never a production table.
    sqlx::raw_sql("ALTER TABLE venue_user_kol_bindings DROP CONSTRAINT venue_binding_managed_owner, DROP CONSTRAINT venue_binding_source;")
        .execute(&f.pool).await?;
    sqlx::raw_sql("DROP TABLE venue_managed_credentials; DROP TABLE venue_kol_managed_followers; CREATE TABLE venue_kol_managed_followers(managed_follower_id TEXT PRIMARY KEY, kol_user_id TEXT NOT NULL, user_id TEXT NOT NULL, credential_id TEXT NOT NULL, label TEXT NOT NULL, managed_state TEXT NOT NULL, created_ms BIGINT NOT NULL, disabled_ms BIGINT);")
        .execute(&f.pool).await?;
    sqlx::query("INSERT INTO venue_kol_managed_followers VALUES('old','kol','subject','credential','label','active',1,NULL)").execute(&f.pool).await?;
    assert!(crate::install_control_schema(&f.pool).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_kol_managed_followers")
            .fetch_one(&f.pool)
            .await?,
        1
    );
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT to_regclass('venue_managed_credentials')::text"
        )
        .fetch_one(&f.pool)
        .await?
        .is_none()
    );
    sqlx::query("DELETE FROM venue_kol_managed_followers WHERE managed_follower_id='old'")
        .execute(&f.pool)
        .await?;
    crate::install_control_schema(&f.pool).await?;
    crate::install_control_schema(&f.pool).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT max(version) FROM venue_control_schema_migrations")
            .fetch_one(&f.pool)
            .await?,
        34
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='venue_kol_managed_followers' AND column_name='managed_follower_id'").fetch_one(&f.pool).await?,1);
    let session = f.service.register(login("freshuser"), now()).await?;
    let principal = f
        .service
        .authenticate(session.token.expose(), now())
        .await?;
    assert!(
        f.service
            .managed_followers(&principal)
            .await?
            .accounts
            .is_empty()
    );
    f.cleanup().await
}
