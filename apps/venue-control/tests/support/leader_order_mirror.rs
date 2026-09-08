use super::*;
use venue_control::leader_bot_admin::{LeaderBotAdminError, set_permission};
use venue_control_protocol::leader_bot::*;
#[path = "mirror_drain_fixture.rs"]
mod mirror_drain_fixture;

#[tokio::test]
async fn leader_bot_grant_is_owned_revisioned_and_revocation_drains_without_resurrection()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let service = AccountService::new_with_node_token(
        fixture.pool.clone(),
        CredentialCipher::from_key(&[12; 32])?,
        None,
    )?;
    let now = test_now_ms()?;
    let session = service
        .register(
            LoginRequest {
                username: "leader-permission".into(),
                password: SecretValue::new("leader fixture password".into()),
            },
            now,
        )
        .await?;
    let principal = service
        .authenticate(session.token.expose(), now + 1)
        .await?;
    let user = &principal.user.user_id;
    let account = id(9001);
    let credential = id(9002);
    provision_account(&fixture.pool, user, &account, &credential, 61).await?;
    insert_kol_profile(&fixture.pool, user, &account, 1).await?;
    let denied = service.leader_bot_access(&principal).await?;
    assert_eq!(
        denied.profile_state,
        Some(venue_control_protocol::kol::KolProfileState::Enabled)
    );
    assert!(!denied.can_use);
    assert!(denied.bot.is_none());
    assert_eq!(denied.permission_revision, 0);
    let create = LeaderBotCreateRequest {
        schema_version: 1,
        request_id: id(9003),
        credential_id: credential.clone(),
    };
    assert_eq!(
        service
            .create_leader_bot(&principal, create.clone(), now + 2)
            .await
            .err()
            .ok_or("unauthorized creation admitted")?
            .code,
        AccountErrorCode::Forbidden
    );
    assert_eq!(
        set_permission(&fixture.pool, user, true, 0, "fixture-admin", now + 3).await?,
        1
    );
    assert!(matches!(
        set_permission(&fixture.pool, user, false, 0, "fixture-admin", now + 4).await,
        Err(LeaderBotAdminError::Conflict)
    ));
    let created = service
        .create_leader_bot(&principal, create.clone(), now + 5)
        .await?;
    assert_eq!(
        created,
        service
            .create_leader_bot(&principal, create, now + 6)
            .await?
    );
    let bot = created.bot.ok_or("missing bot")?;
    let start = LeaderBotLifecycleRequest {
        schema_version: 1,
        request_id: id(9004),
        bot_id: bot.bot_id.clone(),
        expected_revision: bot.revision,
        action: LeaderBotAction::Start,
        risk_confirmed: true,
    };
    let running = service
        .request_leader_bot_lifecycle(&principal, start.clone(), now + 7)
        .await?;
    assert_eq!(
        running,
        service
            .request_leader_bot_lifecycle(&principal, start.clone(), now + 8)
            .await?
    );
    assert_eq!(
        set_permission(&fixture.pool, user, false, 1, "fixture-admin", now + 9).await?,
        2
    );
    let revoked = service.leader_bot_access(&principal).await?;
    assert!(!revoked.can_use);
    assert_eq!(
        revoked.bot.as_ref().ok_or("lost revoked bot")?.state,
        LeaderBotState::Draining
    );
    assert_eq!(
        service
            .request_leader_bot_lifecycle(&principal, start, now + 10)
            .await
            .err()
            .ok_or("revoked start replay admitted")?
            .code,
        AccountErrorCode::Forbidden
    );
    let current = revoked.bot.ok_or("missing bot")?;
    service
        .request_leader_bot_lifecycle(
            &principal,
            LeaderBotLifecycleRequest {
                schema_version: 1,
                request_id: id(9005),
                bot_id: bot.bot_id,
                expected_revision: current.revision,
                action: LeaderBotAction::Stop,
                risk_confirmed: false,
            },
            now + 11,
        )
        .await?;
    set_permission(&fixture.pool, user, true, 2, "fixture-admin", now + 12).await?;
    assert_eq!(
        service
            .leader_bot_access(&principal)
            .await?
            .bot
            .ok_or("missing bot")?
            .state,
        LeaderBotState::Draining
    );
    let audit: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_leader_bot_permission_audit")
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(audit, 3);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn creation_waiting_for_admin_lock_observes_committed_revocation()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let service = AccountService::new_with_node_token(
        fixture.pool.clone(),
        CredentialCipher::from_key(&[12; 32])?,
        None,
    )?;
    let now = test_now_ms()?;
    let session = service
        .register(
            LoginRequest {
                username: "leader-revoke-race".into(),
                password: SecretValue::new("leader fixture password".into()),
            },
            now,
        )
        .await?;
    let principal = service
        .authenticate(session.token.expose(), now + 1)
        .await?;
    let user = &principal.user.user_id;
    let account = id(9101);
    let credential = id(9102);
    provision_account(&fixture.pool, user, &account, &credential, 62).await?;
    insert_kol_profile(&fixture.pool, user, &account, 1).await?;
    set_permission(&fixture.pool, user, true, 0, "fixture-admin", now + 2).await?;

    let mut admin = fixture.pool.begin().await?;
    sqlx::query("SELECT kol_user_id FROM venue_kol_profiles WHERE kol_user_id=$1 FOR UPDATE")
        .bind(user)
        .fetch_one(&mut *admin)
        .await?;
    sqlx::query(
        "UPDATE venue_leader_bot_permissions SET enabled=false,revision=2 WHERE kol_user_id=$1",
    )
    .bind(user)
    .execute(&mut *admin)
    .await?;
    let creation = service.create_leader_bot(
        &principal,
        LeaderBotCreateRequest {
            schema_version: 1,
            request_id: id(9103),
            credential_id: credential,
        },
        now + 3,
    );
    tokio::pin!(creation);
    tokio::select! {
        _ = &mut creation => return Err("creation bypassed administrator lock".into()),
        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
    }
    admin.commit().await?;
    assert_eq!(
        creation
            .await
            .err()
            .ok_or("revoked creation admitted")?
            .code,
        AccountErrorCode::Forbidden
    );
    let bots: i64 = sqlx::query_scalar("SELECT count(*) FROM venue_leader_bots")
        .fetch_one(&fixture.pool)
        .await?;
    assert_eq!(bots, 0);
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn order_mirror_plans_once_and_revocation_cancels_only_definitely_unsent_children()
-> Result<(), Box<dyn std::error::Error>> {
    mirror_sizing_and_revocation(false, false, false).await
}

#[tokio::test]
async fn leader_bot_catalog_allows_same_account_presets_and_only_one_active_bot()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    fixture.migrate_twice().await?;
    let service = AccountService::new_with_node_token(
        fixture.pool.clone(),
        CredentialCipher::from_key(&[13; 32])?,
        None,
    )?;
    let now = test_now_ms()?;
    let session = service
        .register(
            LoginRequest {
                username: "leader-catalog".into(),
                password: SecretValue::new("leader catalog fixture password".into()),
            },
            now,
        )
        .await?;
    let principal = service
        .authenticate(session.token.expose(), now + 1)
        .await?;
    let user = &principal.user.user_id;
    let account = id(9051);
    let credential = id(9052);
    provision_account(&fixture.pool, user, &account, &credential, 64).await?;
    insert_kol_profile(&fixture.pool, user, &account, 1).await?;
    set_permission(&fixture.pool, user, true, 0, "fixture-admin", now + 2).await?;

    let first_request = LeaderBotConfiguredCreateRequest {
        schema_version: LEADER_BOTS_SCHEMA_VERSION,
        request_id: id(9053),
        credential_id: credential.clone(),
        config: LeaderBotConfig {
            name: "稳健带单".into(),
            description: "第一套配置".into(),
            strategy_capital: Decimal::from(100),
        },
    };
    service
        .create_configured_leader_bot(&principal, first_request, now + 3)
        .await?;
    let second_request = LeaderBotConfiguredCreateRequest {
        schema_version: LEADER_BOTS_SCHEMA_VERSION,
        request_id: id(9054),
        credential_id: credential.clone(),
        config: LeaderBotConfig {
            name: "积极带单".into(),
            description: "第二套配置".into(),
            strategy_capital: Decimal::from(200),
        },
    };
    let created = service
        .create_configured_leader_bot(&principal, second_request.clone(), now + 4)
        .await?;
    assert_eq!(created.bots.len(), 2);
    assert!(
        created
            .bots
            .iter()
            .all(|bot| bot.trading_account_id == account)
    );
    assert_eq!(
        created,
        service
            .create_configured_leader_bot(&principal, second_request, now + 5)
            .await?
    );

    let second = created
        .bots
        .iter()
        .find(|bot| bot.config.name == "积极带单")
        .ok_or("second configured bot missing")?;
    let update = LeaderBotUpdateRequest {
        schema_version: LEADER_BOTS_SCHEMA_VERSION,
        request_id: id(9055),
        bot_id: second.bot_id.clone(),
        expected_revision: second.revision,
        credential_id: credential.clone(),
        config: LeaderBotConfig {
            name: "积极带单 2".into(),
            description: "已编辑".into(),
            strategy_capital: Decimal::from(250),
        },
    };
    let updated = service
        .update_leader_bot(&principal, update.clone(), now + 6)
        .await?;
    assert!(updated.bots.iter().any(|bot| {
        bot.bot_id == second.bot_id
            && bot.config.name == "积极带单 2"
            && bot.config_revision == second.config_revision + 1
    }));
    let mut stale = update;
    stale.request_id = id(9056);
    assert_eq!(
        service
            .update_leader_bot(&principal, stale, now + 7)
            .await
            .err()
            .ok_or("stale update admitted")?
            .code,
        AccountErrorCode::Conflict
    );

    let first = updated
        .bots
        .iter()
        .find(|bot| bot.config.name == "稳健带单")
        .ok_or("first configured bot missing")?;
    let running = service
        .request_leader_bots_lifecycle(
            &principal,
            LeaderBotLifecycleRequest {
                schema_version: LEADER_BOT_SCHEMA_VERSION,
                request_id: id(9057),
                bot_id: first.bot_id.clone(),
                expected_revision: first.revision,
                action: LeaderBotAction::Start,
                risk_confirmed: true,
            },
            now + 8,
        )
        .await?;
    let second = running
        .bots
        .iter()
        .find(|bot| bot.config.name == "积极带单 2")
        .ok_or("updated second bot missing")?;
    assert_eq!(
        service
            .request_leader_bots_lifecycle(
                &principal,
                LeaderBotLifecycleRequest {
                    schema_version: LEADER_BOT_SCHEMA_VERSION,
                    request_id: id(9058),
                    bot_id: second.bot_id.clone(),
                    expected_revision: second.revision,
                    action: LeaderBotAction::Start,
                    risk_confirmed: true,
                },
                now + 9,
            )
            .await
            .err()
            .ok_or("second active bot admitted")?
            .code,
        AccountErrorCode::AccountInUse
    );
    let first = running
        .bots
        .iter()
        .find(|bot| bot.config.name == "稳健带单")
        .ok_or("running first bot missing")?;
    let draining = service
        .request_leader_bots_lifecycle(
            &principal,
            LeaderBotLifecycleRequest {
                schema_version: LEADER_BOT_SCHEMA_VERSION,
                request_id: id(9059),
                bot_id: first.bot_id.clone(),
                expected_revision: first.revision,
                action: LeaderBotAction::Stop,
                risk_confirmed: false,
            },
            now + 10,
        )
        .await?;
    assert_eq!(
        draining
            .bots
            .iter()
            .find(|bot| bot.bot_id == first.bot_id)
            .ok_or("draining bot missing")?
            .state,
        LeaderBotState::Draining
    );
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn fixed_notional_mirror_uses_persisted_sizing_and_keeps_reconciliation_fences()
-> Result<(), Box<dyn std::error::Error>> {
    mirror_sizing_and_revocation(true, false, false).await
}

#[tokio::test]
async fn mirror_admission_error_before_submit_does_not_leave_uncertain_commands()
-> Result<(), Box<dyn std::error::Error>> {
    mirror_sizing_and_revocation(false, true, false).await
}

#[tokio::test]
async fn stopped_mirror_requires_complete_recent_absence_and_preserves_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    mirror_sizing_and_revocation(false, false, true).await
}

async fn mirror_sizing_and_revocation(
    fixed: bool,
    admission_failure: bool,
    drain_absence: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    venue_control::install_control_schema(&fixture.pool).await?;
    let now = test_now_ms()?;
    let kol = id(9101);
    let leader_account = id(9102);
    let leader_credential = id(9103);
    let follower = id(9111);
    let follower_account = id(9112);
    let follower_credential = id(9113);
    let relation = id(9114);
    let invite = id(9115);
    let bot = id(9104);
    seed_verified_account(&fixture.pool, &kol, &leader_account, &leader_credential, 71).await?;
    seed_verified_account(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        72,
    )
    .await?;
    sqlx::query("UPDATE venue_api_credentials SET verification_json='{\"verification\":\"verified\"}'::jsonb").execute(&fixture.pool).await?;
    insert_kol_profile(&fixture.pool, &kol, &leader_account, 1).await?;
    insert_invite(&fixture.pool, &invite, &kol, 73).await?;
    sqlx::query("INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) VALUES ($1,$2,$3,1)").bind(&follower).bind(&kol).bind(&invite).execute(&fixture.pool).await?;
    insert_follow_relation(
        &fixture.pool,
        &relation,
        &follower,
        &kol,
        &leader_account,
        &follower_account,
        &follower_credential,
        1,
    )
    .await?;
    sqlx::query("UPDATE venue_kol_follow_relations SET baseline_json=jsonb_build_object('target_model',2,'baseline_ms',$1::bigint)").bind(i64::try_from(now-20)?).execute(&fixture.pool).await?;
    if fixed {
        sqlx::query("UPDATE venue_kol_follow_relations SET sizing_json='{\"mode\":\"fixed_notional\",\"notional\":\"5.5\"}'::jsonb")
            .execute(&fixture.pool).await?;
    }
    set_permission(&fixture.pool, &kol, true, 0, "fixture", now).await?;
    sqlx::query("INSERT INTO venue_leader_bots(bot_id,owner_user_id,trading_account_id,credential_id,create_request_id,bot_name,bot_description,strategy_capital,bot_state,revision,permission_revision,started_ms,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$1,'Fixture KOL','','100','running',1,1,$5,$5,$5)").bind(&bot).bind(&kol).bind(&leader_account).bind(&leader_credential).bind(i64::try_from(now-10)?).execute(&fixture.pool).await?;
    let order = serde_json::json!({"client_order_id":"source-client","native_order_id":"123","symbol":"BTC/USDT","order_side":OrderSide::Buy,"position_side":PositionSide::Long,"quantity":"0.001","filled_quantity":"0","limit_price":"50000","post_only":true,"time_in_force":"post_only","reduce_only":false,"state":"new","created_ms":now-1});
    let mut gtc = order.clone();
    gtc["native_order_id"] = "124".into();
    gtc["client_order_id"] = "source-gtc".into();
    gtc["post_only"] = false.into();
    gtc["time_in_force"] = "gtc".into();
    // Open the worker/poller connections before publishing short-lived signed facts.
    let mut connections = Vec::new();
    for _ in 0..4 {
        connections.push(fixture.pool.acquire().await?);
    }
    drop(connections);
    persist_projection(
        &fixture.pool,
        &kol,
        &leader_account,
        &leader_credential,
        vec![order.clone(), gtc.clone()],
        test_now_ms()?,
    )
    .await?;
    persist_projection(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        vec![],
        test_now_ms()?,
    )
    .await?;
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(venue_control::order_mirror::run_order_mirror(
        fixture.pool.clone(),
        venue_control::executor_runtime::CommandWake::new(),
        receiver,
    ));
    wait_count(&fixture.pool, "SELECT count(*) FROM venue_order_mirrors", 2).await?;
    let child: String = sqlx::query_scalar(
        "SELECT child_client_order_id FROM venue_order_mirrors WHERE source_order_id='123'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    if drain_absence {
        shutdown.send(true)?;
        task.await??;
        mirror_drain_fixture::verify(
            &fixture,
            &follower,
            &follower_credential,
            &follower_account,
            &bot,
            &child,
        )
        .await?;
        fixture.cleanup().await?;
        return Ok(());
    }
    if admission_failure {
        shutdown.send(true)?;
        task.await??;
        // A malformed source projection fails admission after the command has been claimed,
        // before any exchange request is allowed. The mock would return Accepted if reached.
        sqlx::query("UPDATE venue_binance_account_projections SET projection_json=jsonb_build_object('stream_healthy',true,'projection','{}'::jsonb) WHERE credential_id=$1")
            .bind(&leader_credential).execute(&fixture.pool).await?;
        let cipher = CredentialCipher::from_key(&[42; 32])?;
        let payload = serde_json::to_vec(&BindCredentialRequest {
            label: "fixture".into(),
            api_key: SecretValue::new("a".repeat(32)),
            api_secret: SecretValue::new("b".repeat(32)),
        })?;
        let encrypted = cipher.encrypt(
            &format!("venue-api-v1:{follower}:{follower_credential}"),
            &payload,
        )?;
        sqlx::query(
            "UPDATE venue_api_credentials SET encrypted_credentials=$1 WHERE credential_id=$2",
        )
        .bind(encrypted)
        .bind(&follower_credential)
        .execute(&fixture.pool)
        .await?;
        let mut runtime = BinanceExecutorRuntime::new(
            PgExecutorStore::new(fixture.pool.clone()),
            MockBinanceExecution::default(),
            ExecutorSecretProvider::new(fixture.pool.clone(), cipher),
        );
        assert_eq!(runtime.recover_once().await?, 2);
        let outcomes: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT command_state,sanitized_error_code,native_order_id FROM venue_binance_commands",
        )
        .fetch_all(&fixture.pool)
        .await?;
        assert_eq!(outcomes.len(), 2);
        for (state, code, native) in outcomes {
            assert_eq!(state, "rejected");
            assert_eq!(code.as_deref(), Some("not_dispatched_ledger_conflict"));
            assert!(native.is_none());
        }
        fixture.cleanup().await?;
        return Ok(());
    }
    let kind: String =
        sqlx::query_scalar("SELECT order_kind FROM venue_binance_commands WHERE command_id=$1")
            .bind(&child)
            .fetch_one(&fixture.pool)
            .await?;
    assert_eq!(kind, "limit_post_only");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_binance_commands WHERE command_phase='open' AND copy_risk->>'round_open_quantity_up'='true' AND copy_risk->>'notional_limit_policy'='exchange_account'"
        )
        .fetch_one(&fixture.pool)
        .await?,
        2
    );
    if fixed {
        let quantities: Vec<String> =
            sqlx::query_scalar("SELECT child_quantity FROM venue_order_mirrors")
                .fetch_all(&fixture.pool)
                .await?;
        assert_eq!(quantities.len(), 2);
        for quantity in quantities {
            assert_eq!(quantity.parse::<Decimal>()?, Decimal::new(11, 5));
        }
    }
    venue_control::install_control_schema(&fixture.pool).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_binance_commands WHERE order_kind='limit_gtc'"
        )
        .fetch_one(&fixture.pool)
        .await?,
        1
    );
    let mut partial = order;
    partial["filled_quantity"] = "0.0005".into();
    partial["state"] = "partially_filled".into();
    persist_projection(
        &fixture.pool,
        &kol,
        &leader_account,
        &leader_credential,
        vec![partial, gtc],
        test_now_ms()?,
    )
    .await?;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM venue_binance_commands")
            .fetch_one(&fixture.pool)
            .await?,
        2
    );
    // Simulate an independently signed accepted GTC order. Revocation must create an exact
    // cancel, retain its uncertain identity, and retry only a definitely rejected cancellation.
    let gtc_child: String = sqlx::query_scalar(
        "SELECT child_client_order_id FROM venue_order_mirrors WHERE source_order_id='124'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',native_order_id='child-124',terminal_ms=$1 WHERE command_id=$2")
        .bind(i64::try_from(test_now_ms()?)?).bind(&gtc_child).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_order_mirrors SET mirror_state='live',child_native_order_id='child-124' WHERE source_order_id='124'").execute(&fixture.pool).await?;
    // RESULT/exact readback can precede the follower stream's NEW event. Even a healthy
    // projection with a newer observation clock is not a request to retire that child.
    persist_projection(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        vec![],
        test_now_ms()?,
    )
    .await?;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_binance_commands WHERE command_phase='cancel'"
        )
        .fetch_one(&fixture.pool)
        .await?,
        0,
        "a lagging follower projection must not cancel a signed live child"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT mirror_state FROM venue_order_mirrors WHERE source_order_id='124'"
        )
        .fetch_one(&fixture.pool)
        .await?,
        "live"
    );
    // A pause may cancel the queued command independently of the planner; the mapping must drain.
    sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$1 WHERE command_id=$2 AND command_state='pending'").bind(i64::try_from(test_now_ms()?)?).bind(&child).execute(&fixture.pool).await?;
    set_permission(&fixture.pool, &kol, false, 1, "fixture", test_now_ms()?).await?;
    wait_count(
        &fixture.pool,
        "SELECT count(*) FROM venue_order_mirrors WHERE mirror_state='blocked'",
        1,
    )
    .await?;
    wait_count(
        &fixture.pool,
        "SELECT count(*) FROM venue_binance_commands WHERE command_phase='cancel'",
        1,
    )
    .await?;
    let (cancel, native, account): (String,String,String) = sqlx::query_as("SELECT command_id,selected_native_order_id,trading_account_id FROM venue_binance_commands WHERE command_phase='cancel'").fetch_one(&fixture.pool).await?;
    assert_eq!(native, "child-124");
    assert_eq!(account, follower_account);
    sqlx::query(
        "UPDATE venue_binance_commands SET command_state='reconcile_required' WHERE command_id=$1",
    )
    .bind(&cancel)
    .execute(&fixture.pool)
    .await?;
    sqlx::query("UPDATE venue_order_mirrors SET updated_ms=$1 WHERE source_order_id='124'")
        .bind(i64::try_from(test_now_ms()? - 31_000)?)
        .execute(&fixture.pool)
        .await?;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_binance_commands WHERE command_phase='cancel'"
        )
        .fetch_one(&fixture.pool)
        .await?,
        1
    );
    assert_eq!(
        command_state(&fixture.pool, &cancel).await?,
        "reconcile_required"
    );
    sqlx::query("UPDATE venue_binance_commands SET command_state='rejected',terminal_ms=$1 WHERE command_id=$2").bind(i64::try_from(test_now_ms()?)?).bind(&cancel).execute(&fixture.pool).await?;
    persist_projection(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        vec![],
        test_now_ms()?,
    )
    .await?;
    wait_count(
        &fixture.pool,
        "SELECT count(*) FROM venue_binance_commands WHERE command_phase='cancel'",
        2,
    )
    .await?;
    let retry:String=sqlx::query_scalar("SELECT command_id FROM venue_binance_commands WHERE command_phase='cancel' AND command_state='pending'").fetch_one(&fixture.pool).await?;
    assert_ne!(cancel, retry);
    // Emulate the next exact signed terminal fact; only this releases the drain.
    sqlx::query("UPDATE venue_order_mirrors SET mirror_state='terminal',filled_quantity=$1 WHERE source_order_id='124'")
        .bind(if fixed { "0.00005" } else { "0.0002" }).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',native_order_id='child-124',terminal_ms=$1 WHERE command_id=$2").bind(i64::try_from(test_now_ms()?)?).bind(&retry).execute(&fixture.pool).await?;
    wait_count(
        &fixture.pool,
        "SELECT count(*) FROM venue_leader_bots WHERE bot_state='stopped'",
        1,
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_binance_commands WHERE order_kind='market'"
        )
        .fetch_one(&fixture.pool)
        .await?,
        0
    );
    shutdown.send(true)?;
    task.await??;
    fixture.cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn market_is_planned_once_and_stop_create_modify_cancel_keep_exact_algo_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&url).await?;
    venue_control::install_control_schema(&fixture.pool).await?;
    let now = test_now_ms()?;
    let kol = id(9201);
    let leader_account = id(9202);
    let leader_credential = id(9203);
    let bot = id(9204);
    let follower = id(9211);
    let follower_account = id(9212);
    let follower_credential = id(9213);
    let relation = id(9214);
    let invite = id(9215);
    seed_verified_account(&fixture.pool, &kol, &leader_account, &leader_credential, 81).await?;
    seed_verified_account(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        82,
    )
    .await?;
    sqlx::query("UPDATE venue_api_credentials SET verification_json='{\"verification\":\"verified\"}'::jsonb")
        .execute(&fixture.pool)
        .await?;
    insert_kol_profile(&fixture.pool, &kol, &leader_account, 1).await?;
    insert_invite(&fixture.pool, &invite, &kol, 83).await?;
    sqlx::query("INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) VALUES ($1,$2,$3,1)")
        .bind(&follower).bind(&kol).bind(&invite).execute(&fixture.pool).await?;
    insert_follow_relation(
        &fixture.pool,
        &relation,
        &follower,
        &kol,
        &leader_account,
        &follower_account,
        &follower_credential,
        1,
    )
    .await?;
    sqlx::query("UPDATE venue_kol_follow_relations SET baseline_json=jsonb_build_object('target_model',2,'baseline_ms',$1::bigint)")
        .bind(i64::try_from(now - 20)?).execute(&fixture.pool).await?;
    set_permission(&fixture.pool, &kol, true, 0, "fixture", now).await?;
    sqlx::query("INSERT INTO venue_leader_bots(bot_id,owner_user_id,trading_account_id,credential_id,create_request_id,bot_name,bot_description,strategy_capital,bot_state,revision,permission_revision,started_ms,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$1,'Fixture KOL','','100','running',1,1,$5,$5,$5)")
        .bind(&bot).bind(&kol).bind(&leader_account).bind(&leader_credential)
        .bind(i64::try_from(now - 10)?).execute(&fixture.pool).await?;

    let store = PgExecutorStore::new(fixture.pool.clone());
    store
        .record_source_market_order(
            &kol,
            &leader_account,
            &venue_execution::SignedMarketOrderFact {
                client_order_id: "source-market".into(),
                venue_order_id: "market-1".into(),
                symbol: "BTC/USDT".parse()?,
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                quantity: Decimal::new(1, 3),
                reference_price: Decimal::from(50_000),
                created_at_ms: now - 1,
            },
            now,
        )
        .await?;
    let source_stop = conditional("source-stop", "stop-1", "49000", now - 1);
    persist_projection_with_conditionals(
        &fixture.pool,
        &kol,
        &leader_account,
        &leader_credential,
        vec![source_stop.clone()],
        now,
    )
    .await?;
    persist_projection_with_conditionals(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        vec![],
        now,
    )
    .await?;
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(venue_control::order_mirror::run_order_mirror(
        fixture.pool.clone(),
        venue_control::executor_runtime::CommandWake::new(),
        receiver,
    ));
    wait_count(
        &fixture.pool,
        "SELECT count(*) FROM venue_order_mirrors WHERE source_kind IN ('market','stop')",
        2,
    )
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_order_mirrors WHERE source_kind='market'"
        )
        .fetch_one(&fixture.pool)
        .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_binance_commands WHERE order_kind='market'"
        )
        .fetch_one(&fixture.pool)
        .await?,
        1
    );

    let stop_place: String = sqlx::query_scalar(
        "SELECT child_client_order_id FROM venue_order_mirrors WHERE source_kind='stop'",
    )
    .fetch_one(&fixture.pool)
    .await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',native_order_id='child-stop-1',terminal_ms=$1 WHERE command_id=$2")
        .bind(i64::try_from(test_now_ms()?)?).bind(&stop_place).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_order_mirrors SET mirror_state='live',child_native_order_id='child-stop-1' WHERE source_kind='stop'")
        .execute(&fixture.pool).await?;
    persist_projection_with_conditionals(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        vec![],
        test_now_ms()?,
    )
    .await?;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM venue_binance_commands WHERE order_kind='cancel_algo_exact'"
        )
        .fetch_one(&fixture.pool)
        .await?,
        0,
        "a missing follower stream event must not cancel a confirmed stop"
    );
    let child_stop = conditional(&stop_place, "child-stop-1", "49000", now - 1);
    let changed_stop = conditional("source-stop", "stop-1", "48000", now - 1);
    let refreshed = test_now_ms()?;
    persist_projection_with_conditionals(
        &fixture.pool,
        &kol,
        &leader_account,
        &leader_credential,
        vec![changed_stop.clone()],
        refreshed,
    )
    .await?;
    persist_projection_with_conditionals(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        vec![child_stop],
        refreshed,
    )
    .await?;
    wait_count(
        &fixture.pool,
        "SELECT count(*) FROM venue_binance_commands WHERE order_kind='cancel_algo_exact'",
        1,
    )
    .await?;
    let (cancel, selected): (String, String) = sqlx::query_as("SELECT command_id,selected_native_order_id FROM venue_binance_commands WHERE order_kind='cancel_algo_exact'")
        .fetch_one(&fixture.pool).await?;
    assert_eq!(selected, "child-stop-1");

    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',native_order_id='child-stop-1',terminal_ms=$1 WHERE command_id=$2")
        .bind(i64::try_from(test_now_ms()?)?).bind(&cancel).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_order_mirrors SET mirror_state='terminal' WHERE source_kind='stop'")
        .execute(&fixture.pool)
        .await?;
    persist_projection_with_conditionals(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        vec![],
        test_now_ms()?,
    )
    .await?;
    wait_count(
        &fixture.pool,
        "SELECT count(*) FROM venue_binance_commands WHERE order_kind='stop_market'",
        2,
    )
    .await?;

    let replacement: String = sqlx::query_scalar("SELECT child_client_order_id FROM venue_order_mirrors WHERE source_kind='stop' ORDER BY child_sequence DESC LIMIT 1")
        .fetch_one(&fixture.pool).await?;
    sqlx::query("UPDATE venue_binance_commands SET command_state='reconciled',native_order_id='child-stop-2',terminal_ms=$1 WHERE command_id=$2")
        .bind(i64::try_from(test_now_ms()?)?).bind(&replacement).execute(&fixture.pool).await?;
    sqlx::query("UPDATE venue_order_mirrors SET mirror_state='live',child_native_order_id='child-stop-2' WHERE child_client_order_id=$1")
        .bind(&replacement).execute(&fixture.pool).await?;
    let refreshed = test_now_ms()?;
    persist_projection_with_conditionals(
        &fixture.pool,
        &kol,
        &leader_account,
        &leader_credential,
        vec![],
        refreshed,
    )
    .await?;
    persist_projection_with_conditionals(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        vec![conditional(&replacement, "child-stop-2", "48000", now - 1)],
        refreshed,
    )
    .await?;
    wait_count(
        &fixture.pool,
        "SELECT count(*) FROM venue_binance_commands WHERE order_kind='cancel_algo_exact'",
        2,
    )
    .await?;

    shutdown.send(true)?;
    task.await??;
    fixture.cleanup().await?;
    Ok(())
}

fn conditional(client: &str, native: &str, trigger: &str, created: u64) -> serde_json::Value {
    serde_json::json!({"client_order_id":client,"native_order_id":native,"symbol":"BTC/USDT","order_side":"buy","position_side":"long","quantity":"0.001","trigger_price":trigger,"working_type":"MARK_PRICE","reduce_only":false,"created_ms":created})
}

async fn provision_account(
    pool: &PgPool,
    user: &str,
    account: &str,
    credential: &str,
    seed: u8,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,'binance',$3)").bind(account).bind(user).bind(vec![seed;32]).execute(pool).await?;
    sqlx::query("INSERT INTO venue_api_credentials(credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,created_ms) VALUES($1,$2,'fixture',$3,'***',decode('00','hex'),$4,'{\"verification\":\"verified\"}'::jsonb,1)").bind(credential).bind(user).bind(vec![seed.wrapping_add(100);32]).bind(account).execute(pool).await?;
    Ok(())
}
async fn persist_projection(
    pool: &PgPool,
    user: &str,
    account: &str,
    credential: &str,
    orders: Vec<serde_json::Value>,
    now: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    persist_projection_full(pool, user, account, credential, orders, vec![], now).await
}

async fn persist_projection_with_conditionals(
    pool: &PgPool,
    user: &str,
    account: &str,
    credential: &str,
    conditional_orders: Vec<serde_json::Value>,
    now: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    persist_projection_full(
        pool,
        user,
        account,
        credential,
        vec![],
        conditional_orders,
        now,
    )
    .await
}

async fn persist_projection_full(
    pool: &PgPool,
    user: &str,
    account: &str,
    credential: &str,
    orders: Vec<serde_json::Value>,
    conditional_orders: Vec<serde_json::Value>,
    now: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let projection = serde_json::json!({"stream_healthy":true,"projection":{"schema_version":1,"credential_id":credential,"trading_account_id":account,"observed_ms":now,"persisted_ms":now,"private_generation":1,"position_mode":"hedge","positions":[],"open_orders":orders,"conditional_orders":conditional_orders,"fills":[],"assets":[]}});
    sqlx::query("INSERT INTO venue_binance_account_projections(credential_id,owner_user_id,trading_account_id,observed_ms,persisted_ms,private_generation,projection_json) VALUES($1,$2,$3,$4,$4,1,$5) ON CONFLICT(credential_id) DO UPDATE SET projection_json=EXCLUDED.projection_json,observed_ms=EXCLUDED.observed_ms,persisted_ms=EXCLUDED.persisted_ms").bind(credential).bind(user).bind(account).bind(i64::try_from(now)?).bind(projection).execute(pool).await?;
    Ok(())
}
async fn wait_count(
    pool: &PgPool,
    query: &str,
    wanted: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let actual: i64 = sqlx::query_scalar(query).fetch_one(pool).await?;
        if actual == wanted {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(
                format!("mirror fixture timed out: expected {wanted}, got {actual}").into(),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
