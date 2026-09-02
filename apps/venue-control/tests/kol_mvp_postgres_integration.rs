use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::{
    BinanceExecutorSingleton, ExecutorSingletonError, MIGRATION_0017, accounts::MIGRATION_0015,
};

#[tokio::test]
async fn kol_mvp_migration_is_idempotent_and_enforces_capacity_and_ownership()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(database_url) = integration_database_url()? else {
        return Ok(());
    };
    let fixture = Fixture::create(&database_url).await?;
    fixture.migrate_twice().await?;
    let singleton = BinanceExecutorSingleton::acquire(&database_url).await?;
    assert!(matches!(
        BinanceExecutorSingleton::acquire(&database_url).await,
        Err(ExecutorSingletonError::AlreadyRunning)
    ));
    singleton.release().await?;

    for slot in 1_i16..=5 {
        let user_id = id(100 + i64::from(slot));
        let account_id = id(200 + i64::from(slot));
        let credential_id = id(300 + i64::from(slot));
        seed_verified_account(
            &fixture.pool,
            &user_id,
            &account_id,
            &credential_id,
            slot as u8,
        )
        .await?;
        insert_kol_profile(&fixture.pool, &user_id, &account_id, slot).await?;
    }

    let sixth_user = id(106);
    let sixth_account = id(206);
    let sixth_credential = id(306);
    seed_verified_account(
        &fixture.pool,
        &sixth_user,
        &sixth_account,
        &sixth_credential,
        6,
    )
    .await?;
    assert!(
        insert_kol_profile(&fixture.pool, &sixth_user, &sixth_account, 6)
            .await
            .is_err()
    );

    let first_kol = id(101);
    let second_kol = id(102);
    let first_invite = id(401);
    let second_invite = id(402);
    insert_invite(&fixture.pool, &first_invite, &first_kol, 41).await?;
    insert_invite(&fixture.pool, &second_invite, &second_kol, 42).await?;
    assert!(
        insert_invite(&fixture.pool, &id(403), &first_kol, 43)
            .await
            .is_err()
    );

    let follower = id(501);
    let follower_account = id(601);
    let follower_credential = id(701);
    seed_verified_account(
        &fixture.pool,
        &follower,
        &follower_account,
        &follower_credential,
        51,
    )
    .await?;
    sqlx::query(
        "INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) \
         VALUES ($1,$2,$3,100)",
    )
    .bind(&follower)
    .bind(&first_kol)
    .bind(&first_invite)
    .execute(&fixture.pool)
    .await?;
    assert!(
        sqlx::query(
            "UPDATE venue_user_kol_bindings SET kol_user_id=$1,invite_id=$2 WHERE user_id=$3",
        )
        .bind(&second_kol)
        .bind(&second_invite)
        .bind(&follower)
        .execute(&fixture.pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM venue_user_kol_bindings WHERE user_id=$1")
            .bind(&follower)
            .execute(&fixture.pool)
            .await
            .is_err()
    );

    insert_follow_relation(
        &fixture.pool,
        &id(801),
        &follower,
        &first_kol,
        &id(201),
        &follower_account,
        &follower_credential,
        1,
    )
    .await?;
    let other_follower = id(502);
    let other_account = id(602);
    let other_credential = id(702);
    seed_verified_account(
        &fixture.pool,
        &other_follower,
        &other_account,
        &other_credential,
        52,
    )
    .await?;
    sqlx::query(
        "INSERT INTO venue_user_kol_bindings (user_id,kol_user_id,invite_id,bound_ms) \
         VALUES ($1,$2,$3,100)",
    )
    .bind(&other_follower)
    .bind(&first_kol)
    .bind(&first_invite)
    .execute(&fixture.pool)
    .await?;
    assert!(
        insert_follow_relation(
            &fixture.pool,
            &id(802),
            &other_follower,
            &first_kol,
            &id(201),
            &other_account,
            &other_credential,
            201,
        )
        .await
        .is_err()
    );

    insert_terminal_command(
        &fixture.pool,
        &id(901),
        &id(902),
        &follower,
        &follower_account,
        &follower_credential,
        "pending",
    )
    .await?;
    assert!(
        insert_terminal_command(
            &fixture.pool,
            &id(903),
            &id(902),
            &follower,
            &follower_account,
            &follower_credential,
            "pending",
        )
        .await
        .is_err()
    );
    assert!(
        insert_terminal_command(
            &fixture.pool,
            &id(904),
            &id(905),
            &follower,
            &follower_account,
            &follower_credential,
            "unknown",
        )
        .await
        .is_err()
    );

    fixture.cleanup().await?;
    Ok(())
}

fn integration_database_url() -> Result<Option<String>, Box<dyn std::error::Error>> {
    match std::env::var("VENUE_CONTROL_TEST_DATABASE_URL") {
        Ok(value) => Ok(Some(value)),
        Err(_)
            if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED")
                .ok()
                .as_deref()
                == Some("1") =>
        {
            Err("KOL MVP PostgreSQL test database is required".into())
        }
        Err(_) => {
            eprintln!("SKIP: KOL MVP PostgreSQL test database is not configured");
            Ok(None)
        }
    }
}

struct Fixture {
    pool: PgPool,
    admin: PgPool,
    schema: String,
}

impl Fixture {
    async fn create(database_url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await?;
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let schema = format!("venue_kol_mvp_{}_{}", std::process::id(), nonce);
        admin
            .execute(format!("CREATE SCHEMA {schema}").as_str())
            .await?;
        let search_path = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .after_connect(move |connection, _| {
                let sql = format!("SET search_path TO {search_path}");
                Box::pin(async move {
                    connection.execute(sql.as_str()).await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            admin,
            schema,
        })
    }

    async fn migrate_twice(&self) -> Result<(), sqlx::Error> {
        for _ in 0..2 {
            sqlx::raw_sql(MIGRATION_0015).execute(&self.pool).await?;
            sqlx::raw_sql(MIGRATION_0017).execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn cleanup(self) -> Result<(), sqlx::Error> {
        self.pool.close().await;
        self.admin
            .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
            .await?;
        self.admin.close().await;
        Ok(())
    }
}

async fn seed_verified_account(
    pool: &PgPool,
    user_id: &str,
    account_id: &str,
    credential_id: &str,
    seed: u8,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_users (user_id,username,password_hash,created_ms) VALUES ($1,$2,'test',1)")
        .bind(user_id)
        .bind(format!("user-{seed}"))
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)")
        .bind(account_id)
        .bind(user_id)
        .bind(vec![seed; 32])
        .execute(pool)
        .await?;
    sqlx::query("INSERT INTO venue_api_credentials (credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,trading_account_id,verification_json,created_ms) VALUES ($1,$2,'test',$3,'***',decode('00','hex'),$4,'{}'::jsonb,1)")
        .bind(credential_id)
        .bind(user_id)
        .bind(vec![seed.wrapping_add(100); 32])
        .bind(account_id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_kol_profile(
    pool: &PgPool,
    user_id: &str,
    account_id: &str,
    slot: i16,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_kol_profiles (kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES ($1,$2,'KOL','title','','1000','enabled',$3,1,1)")
        .bind(user_id)
        .bind(account_id)
        .bind(slot)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_invite(
    pool: &PgPool,
    invite_id: &str,
    kol_user_id: &str,
    seed: u8,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_kol_invites (invite_id,kol_user_id,code_hash,invite_state,created_ms) VALUES ($1,$2,$3,'active',1)")
        .bind(invite_id)
        .bind(kol_user_id)
        .bind(vec![seed; 32])
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_follow_relation(
    pool: &PgPool,
    relation_id: &str,
    follower_user_id: &str,
    kol_user_id: &str,
    leader_account_id: &str,
    account_id: &str,
    credential_id: &str,
    active_slot: i16,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_kol_follow_relations (relation_id,follower_user_id,kol_user_id,leader_trading_account_id,follower_trading_account_id,credential_id,relation_state,active_slot,allocated_capital,multiplier,max_order_notional,max_total_notional,max_deviation_bps,allowed_symbols,revision,created_ms,updated_ms) VALUES ($1,$2,$3,$4,$5,$6,'active',$7,'1000','1','100','1000',100,'[\"BTC/USDT\"]'::jsonb,1,1,1)")
        .bind(relation_id)
        .bind(follower_user_id)
        .bind(kol_user_id)
        .bind(leader_account_id)
        .bind(account_id)
        .bind(credential_id)
        .bind(active_slot)
        .execute(pool)
        .await?;
    Ok(())
}

async fn insert_terminal_command(
    pool: &PgPool,
    command_id: &str,
    request_id: &str,
    owner_user_id: &str,
    account_id: &str,
    credential_id: &str,
    state: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,rule_version,client_order_id,command_state,created_ms,updated_ms) VALUES ($1,'terminal',$2,$3,$4,$5,'BTC/USDT','long','open','market','buy','0.001','fixture',$6,$7,1,1)")
        .bind(command_id)
        .bind(request_id)
        .bind(owner_user_id)
        .bind(account_id)
        .bind(credential_id)
        .bind(command_id)
        .bind(state)
        .execute(pool)
        .await?;
    Ok(())
}

fn id(value: i64) -> String {
    format!("00000000-0000-4000-8000-{value:012}")
}
