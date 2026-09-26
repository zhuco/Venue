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
        authorization: Default::default(),
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
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT kol_user_id FROM venue_user_kol_bindings WHERE managed_id=$1"
        )
        .bind(&saved.managed_id)
        .fetch_one(&f.pool)
        .await?,
        owner.user.user_id
    );
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
                equity: Decimal::from(100),
                available_margin: Decimal::from(80),
            })
        })
        .await?;
    assert_eq!(verified.verification, ApiVerificationState::Verified);
    assert_eq!(verified.equity, Some(Decimal::from(100)));
    assert_eq!(verified.available_margin, Some(Decimal::from(80)));
    assert_eq!(verified.balance_observed_ms, Some(now));
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
    // Production upgraded from the frozen managed-follower table and retains this stricter
    // provenance column. The canonical fresh schema deliberately does not require it.
    sqlx::raw_sql("ALTER TABLE venue_user_kol_bindings ADD COLUMN binding_source TEXT NOT NULL DEFAULT 'invite' CHECK(binding_source IN ('invite','kol_managed')); UPDATE venue_user_kol_bindings SET binding_source='kol_managed' WHERE managed_id IS NOT NULL; ALTER TABLE venue_user_kol_bindings ADD CONSTRAINT venue_user_kol_bindings_invite_source_check CHECK((binding_source='invite' AND invite_id IS NOT NULL) OR (binding_source='kol_managed' AND invite_id IS NULL));")
        .execute(&f.pool)
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
    assert_eq!(incomplete, 1);
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
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT binding_source FROM venue_user_kol_bindings WHERE user_id=$1",
        )
        .bind(&subject.user.user_id)
        .fetch_one(&f.pool)
        .await?,
        "kol_managed"
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

fn proof(
    identity: u8,
    exposed: bool,
    observed_ms: u64,
) -> Result<venue_gateway_binance::BinanceCredentialProbe, venue_gateway_binance::BinanceProbeError>
{
    Ok(venue_gateway_binance::BinanceCredentialProbe {
        account_identity_hash: [identity; 32],
        observed_ms,
        has_exposure: exposed,
        equity: Decimal::from(100),
        available_margin: Decimal::from(80),
    })
}

#[tokio::test]
async fn managed_verification_uses_saved_authorization_and_requests_activation() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let timestamp = now();
    let session = f
        .service
        .register(login("kol-auto-follow"), timestamp)
        .await?;
    let owner = f
        .service
        .authenticate(session.token.expose(), timestamp)
        .await?;
    sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES('00000000-0000-4000-8000-000000000631',$1,'binance',$2)")
        .bind(&owner.user.user_id).bind(vec![95_u8;32]).execute(&f.pool).await?;
    sqlx::query("INSERT INTO venue_kol_profiles(kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES($1,'00000000-0000-4000-8000-000000000631','KOL','Title','','100','enabled',1,$2,$2)")
        .bind(&owner.user.user_id).bind(ms(timestamp)?).execute(&f.pool).await?;
    let saved = f
        .service
        .create_managed_follower(
            &owner,
            request("00000000-0000-4000-8000-000000000632", 'A'),
            timestamp,
        )
        .await?;
    let verified = f
        .service
        .verify_managed_follower_with(
            &owner,
            ManagedFollowerVerifyRequest {
                authorization: None,
                managed_id: saved.managed_id.clone(),
            },
            timestamp,
            |_| async { proof(96, false, timestamp + 1) },
        )
        .await?;
    assert_eq!(verified.equity, Some(Decimal::from(100)));
    let relation = f
        .service
        .managed_follow_status(
            &owner,
            ManagedFollowStatusRequest {
                managed_id: saved.managed_id,
            },
            timestamp,
        )
        .await?
        .ok_or("missing auto relation")?;
    assert!(relation.activation_requested);
    assert_eq!(relation.settings.sizing, Default::default());
    assert_eq!(relation.settings.multiplier, Decimal::ONE);
    assert_eq!(relation.settings.allocated_capital, Decimal::from(100));
    assert_eq!(relation.settings.max_total_notional, Decimal::from(100));
    let mut fixed = request("00000000-0000-4000-8000-000000000633", 'B');
    fixed.authorization.sizing =
        venue_control_protocol::follow_sizing::FollowSizing::FixedNotional {
            notional: Decimal::from(600),
        };
    let fixed = f
        .service
        .create_managed_follower(&owner, fixed, timestamp)
        .await?;
    f.service
        .verify_managed_follower_with(
            &owner,
            ManagedFollowerVerifyRequest {
                authorization: None,
                managed_id: fixed.managed_id.clone(),
            },
            timestamp,
            |_| async { proof(97, false, timestamp + 1) },
        )
        .await?;
    let fixed = f
        .service
        .managed_follow_status(
            &owner,
            ManagedFollowStatusRequest {
                managed_id: fixed.managed_id,
            },
            timestamp,
        )
        .await?
        .ok_or("missing fixed relation")?;
    assert!(fixed.activation_requested);
    assert_eq!(fixed.settings.allocated_capital, Decimal::from(100));
    assert_eq!(
        fixed.settings.sizing,
        venue_control_protocol::follow_sizing::FollowSizing::FixedNotional {
            notional: Decimal::from(600)
        }
    );
    f.cleanup().await
}

#[tokio::test]
async fn managed_delete_is_one_click_and_erases_credentials_after_drain() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let timestamp = now();
    let session = f.service.register(login("kol-delete"), timestamp).await?;
    let owner = f
        .service
        .authenticate(session.token.expose(), timestamp)
        .await?;
    sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES('00000000-0000-4000-8000-000000000621',$1,'binance',$2)")
        .bind(&owner.user.user_id).bind(vec![93_u8;32]).execute(&f.pool).await?;
    sqlx::query("INSERT INTO venue_kol_profiles(kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES($1,'00000000-0000-4000-8000-000000000621','KOL','Title','','100','enabled',1,$2,$2)")
        .bind(&owner.user.user_id).bind(ms(timestamp)?).execute(&f.pool).await?;
    let saved = f
        .service
        .create_managed_follower(
            &owner,
            request("00000000-0000-4000-8000-000000000622", 'D'),
            timestamp,
        )
        .await?;
    f.service
        .verify_managed_follower_with(
            &owner,
            ManagedFollowerVerifyRequest {
                authorization: None,
                managed_id: saved.managed_id.clone(),
            },
            timestamp,
            |_| async { proof(94, false, timestamp) },
        )
        .await?;
    let removal = ManagedFollowerDeleteRequest {
        managed_id: saved.managed_id.clone(),
    };
    let overview = f
        .service
        .delete_managed_follower(&owner, removal, timestamp)
        .await?;
    assert!(overview.accounts.is_empty());
    let requested: (Option<i64>, Option<i64>, i32) = sqlx::query_as(
        "SELECT m.delete_requested_ms,c.deleted_ms,octet_length(c.encrypted_credentials) FROM venue_api_credentials c JOIN venue_managed_credentials m USING(credential_id) WHERE m.managed_id=$1",
    )
    .bind(&saved.managed_id)
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(requested.0, Some(ms(timestamp)?));
    assert_eq!(requested.1, None);
    assert!(requested.2 > 0);
    let unresolved = "00000000-0000-4000-8000-000000000623";
    sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,target_revision,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,target_quantity,rule_version,client_order_id,command_state,created_ms,updated_ms,copy_risk) SELECT $1,'copy',r.relation_id,r.revision,1,r.follower_user_id,r.follower_trading_account_id,r.credential_id,'BTC/USDT','long','open','market','buy','0.001','0.001','fixture',$1,'pending',$2,$2,'{\"max_order_notional\":\"20\",\"max_total_notional\":\"100\",\"max_deviation_bps\":100,\"source_price\":\"10000\",\"source_occurred_ms\":1}'::jsonb FROM venue_kol_follow_relations r WHERE r.follower_user_id=(SELECT follower_user_id FROM venue_managed_credentials WHERE managed_id=$3)")
        .bind(unresolved).bind(ms(timestamp)?).bind(&saved.managed_id).execute(&f.pool).await?;
    assert_eq!(finalize_managed_deletions(&f.pool, timestamp + 1).await?, 0);
    sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$1,updated_ms=$1,sanitized_error_code='test_terminal' WHERE command_id=$2")
        .bind(ms(timestamp + 1)?).bind(unresolved).execute(&f.pool).await?;
    assert_eq!(finalize_managed_deletions(&f.pool, timestamp + 2).await?, 1);
    let tombstone: (Option<i64>, i32, String) = sqlx::query_as(
        "SELECT deleted_ms,octet_length(encrypted_credentials),masked_key FROM venue_api_credentials c JOIN venue_managed_credentials m USING(credential_id) WHERE m.managed_id=$1",
    )
    .bind(&saved.managed_id)
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(tombstone.0, Some(ms(timestamp + 2)?));
    assert_eq!(tombstone.1, 0);
    assert_eq!(tombstone.2, "已删除");
    f.cleanup().await
}

#[tokio::test]
async fn frozen_managed_table_is_preserved_and_nonempty_legacy_fails_closed() -> TestResult {
    let Some(f) = Fixture::create_before_managed_credential_migration().await? else {
        return Ok(());
    };
    // DDL touches only Fixture's isolated random schema, never a production table.
    sqlx::raw_sql("DROP TABLE venue_kol_managed_followers; CREATE TABLE venue_kol_managed_followers(managed_follower_id TEXT PRIMARY KEY, kol_user_id TEXT NOT NULL, user_id TEXT NOT NULL, credential_id TEXT NOT NULL, label TEXT NOT NULL, managed_state TEXT NOT NULL, created_ms BIGINT NOT NULL, disabled_ms BIGINT);")
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
        52
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

#[tokio::test]
async fn managed_adoption_requires_same_owner_deleted_flat_drained_and_preserves_history()
-> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let timestamp = now();
    let session = f.service.register(login("adopt-owner"), timestamp).await?;
    let owner = f
        .service
        .authenticate(session.token.expose(), timestamp)
        .await?;
    let stranger_session = f
        .service
        .register(login("adopt-stranger"), timestamp)
        .await?;
    let stranger = f
        .service
        .authenticate(stranger_session.token.expose(), timestamp)
        .await?;
    for (user, identity, slot) in [(&owner, 101_u8, 1_i32), (&stranger, 102_u8, 2_i32)] {
        let account = crypto::opaque_id()?;
        sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,'binance',$3)").bind(&account).bind(&user.user.user_id).bind(vec![identity;32]).execute(&f.pool).await?;
        sqlx::query("INSERT INTO venue_kol_profiles(kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES($1,$2,'KOL','Title','','500','enabled',$3,$4,$4)").bind(&user.user.user_id).bind(&account).bind(slot).bind(ms(timestamp)?).execute(&f.pool).await?;
    }
    let old = f
        .service
        .bind_credential(
            &owner,
            request("00000000-0000-4000-8000-000000000701", 'A').credential,
            timestamp,
        )
        .await?;
    let old = f
        .service
        .verify_with(&owner, &old.credential_id, timestamp, |_| async {
            proof(103, false, timestamp)
        })
        .await?;
    let old_account = old
        .trading_account_id
        .as_deref()
        .ok_or("old account missing")?;
    let managed = f
        .service
        .create_managed_follower(
            &owner,
            request("00000000-0000-4000-8000-000000000702", 'B'),
            timestamp,
        )
        .await?;
    let verify = || ManagedFollowerVerifyRequest {
        authorization: None,
        managed_id: managed.managed_id.clone(),
    };
    let blocked = f
        .service
        .verify_managed_follower_with(&owner, verify(), timestamp, |_| async {
            proof(103, false, timestamp)
        })
        .await?;
    assert_eq!(blocked.verification, ApiVerificationState::AccountConflict);
    sqlx::query("INSERT INTO venue_binance_commands(command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,rule_version,client_order_id,command_state,created_ms,updated_ms) VALUES('adopt-history','terminal','adopt-history',$1,$2,$3,'SOL/USDC','long','open','market','buy','1','fixture','adopt-history','pending',1,1)").bind(&owner.user.user_id).bind(old_account).bind(&old.credential_id).execute(&f.pool).await?;
    // The deletion path has separate signed-readback tests. Retain its credential tombstone.
    sqlx::query("UPDATE venue_api_credentials SET deleted_ms=$1,encrypted_credentials=$2 WHERE credential_id=$3").bind(ms(timestamp)?).bind(Vec::<u8>::new()).bind(&old.credential_id).execute(&f.pool).await?;
    let other = f
        .service
        .create_managed_follower(
            &stranger,
            request("00000000-0000-4000-8000-000000000703", 'C'),
            timestamp,
        )
        .await?;
    let rejected = f
        .service
        .verify_managed_follower_with(
            &stranger,
            ManagedFollowerVerifyRequest {
                authorization: None,
                managed_id: other.managed_id,
            },
            timestamp,
            |_| async { proof(103, false, timestamp) },
        )
        .await?;
    assert_eq!(rejected.verification, ApiVerificationState::AccountConflict);
    assert_eq!(
        f.service
            .verify_managed_follower_with(&owner, verify(), timestamp, |_| async {
                proof(103, true, timestamp)
            })
            .await
            .err()
            .map(|e| e.code),
        Some(Code::AccountInUse)
    );
    assert_eq!(
        f.service
            .verify_managed_follower_with(&owner, verify(), timestamp, |_| async {
                proof(103, false, timestamp)
            })
            .await
            .err()
            .map(|e| e.code),
        Some(Code::AccountInUse)
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=2,updated_ms=2 WHERE command_id='adopt-history'").execute(&f.pool).await?;
    let mut chosen = verify();
    chosen.authorization = Some(FollowAuthorization {
        sizing: venue_control_protocol::follow_sizing::FollowSizing::SourceRatio {
            ratio: Decimal::new(5, 1),
        },
        multiplier: Decimal::ONE,
    });
    let adopted = f
        .service
        .verify_managed_follower_with(&owner, chosen.clone(), timestamp, |_| async {
            proof(103, false, timestamp)
        })
        .await?;
    assert_eq!(adopted.verification, ApiVerificationState::Verified);
    assert_eq!(adopted.equity, Some(Decimal::from(100)));
    let (subject, credential) = f
        .service
        .managed_verification_subject(&owner, &managed.managed_id, timestamp)
        .await?;
    let account: String = sqlx::query_scalar(
        "SELECT trading_account_id FROM venue_api_credentials WHERE credential_id=$1",
    )
    .bind(&credential)
    .fetch_one(&f.pool)
    .await?;
    assert_ne!(account, old_account);
    let sizing: serde_json::Value = sqlx::query_scalar(
        "SELECT sizing_json FROM venue_kol_follow_relations WHERE follower_user_id=$1",
    )
    .bind(&subject.user.user_id)
    .fetch_one(&f.pool)
    .await?;
    assert_eq!(
        sizing,
        serde_json::json!({"mode":"source_ratio","ratio":"0.5"})
    );
    assert_eq!(
        f.service
            .verify_managed_follower_with(&owner, chosen, timestamp, |_| async {
                proof(103, false, timestamp)
            })
            .await
            .err()
            .map(|e| e.code),
        Some(Code::Conflict)
    );
    let history: bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_user_trading_accounts a JOIN venue_binance_commands c USING(trading_account_id) WHERE a.trading_account_id=$1 AND a.user_id=$2 AND a.successor_account_id=$3 AND a.retired_by_managed_id=$4 AND a.retired_ms IS NOT NULL AND c.command_state='cancelled')").bind(old_account).bind(&owner.user.user_id).bind(&account).bind(&managed.managed_id).fetch_one(&f.pool).await?;
    assert!(history);
    let active: i64=sqlx::query_scalar("SELECT count(*) FROM venue_user_trading_accounts WHERE exchange_identity_hash=$1 AND retired_ms IS NULL").bind(vec![103_u8;32]).fetch_one(&f.pool).await?;
    assert_eq!(active, 1);
    let repeat = f
        .service
        .verify_managed_follower_with(&owner, verify(), timestamp, |_| async {
            proof(103, false, timestamp)
        })
        .await?;
    assert_eq!(repeat.verification, ApiVerificationState::Verified);
    let retained: String=sqlx::query_scalar("SELECT trading_account_id FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2").bind(&credential).bind(&subject.user.user_id).fetch_one(&f.pool).await?;
    assert_eq!(retained, account);
    f.cleanup().await
}
