use sqlx::{Executor, PgPool, postgres::PgPoolOptions};
use venue_control::accounts::MIGRATION_0015;
use venue_control::{
    BinanceCommandLedger, BinanceCommandLedgerError, MIGRATION_0001, MIGRATION_0017,
    MIGRATION_0018, MIGRATION_0019, MIGRATION_0020, MIGRATION_0021, MIGRATION_0022, MIGRATION_0023,
    MIGRATION_0024,
};
use venue_control_protocol::kol::ExecutorCommandState;

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn reconciled_grid_commands_settle_exact_owners_in_the_same_transaction() -> TestResult {
    let Some(fixture) = Fixture::create().await? else {
        return Ok(());
    };
    let result = exercise(&fixture.pool).await;
    fixture.cleanup().await?;
    result
}

async fn exercise(pool: &PgPool) -> TestResult {
    let scope = seed_grid_scope(pool).await?;
    let ledger = BinanceCommandLedger::new(pool.clone());

    insert_grid_place(
        pool,
        &scope,
        "grid-place-bind",
        "grid-client-bind",
        "long:open:1:1",
        1,
        None,
        None,
        "accepted",
    )
    .await?;
    ledger
        .settle_with_readback(
            "grid-place-bind",
            ExecutorCommandState::Reconciled,
            2_000,
            None,
            Some("native-bind"),
        )
        .await?;
    assert_eq!(
        owner_identity(pool, "grid-client-bind").await?,
        (Some("native-bind".into()), "working".into())
    );
    assert_eq!(
        command_identity(pool, "grid-place-bind").await?,
        ("reconciled".into(), Some("native-bind".into()))
    );
    assert_eq!(
        ledger
            .settle_with_readback(
                "grid-place-bind",
                ExecutorCommandState::Reconciled,
                2_001,
                None,
                Some("native-bind"),
            )
            .await,
        Err(BinanceCommandLedgerError::Conflict)
    );

    insert_grid_place(
        pool,
        &scope,
        "grid-place-conflict",
        "grid-client-conflict",
        "long:open:2:1",
        2,
        None,
        Some("native-existing"),
        "accepted",
    )
    .await?;
    assert_eq!(
        ledger
            .settle_with_readback(
                "grid-place-conflict",
                ExecutorCommandState::Reconciled,
                2_010,
                None,
                Some("native-other"),
            )
            .await,
        Err(BinanceCommandLedgerError::Conflict)
    );
    assert_eq!(
        command_identity(pool, "grid-place-conflict").await?,
        ("accepted".into(), None)
    );
    assert_eq!(
        owner_identity(pool, "grid-client-conflict").await?,
        (Some("native-existing".into()), "working".into())
    );

    insert_grid_place(
        pool,
        &scope,
        "grid-place-missing",
        "grid-client-missing",
        "long:open:3:1",
        3,
        None,
        None,
        "accepted",
    )
    .await?;
    assert_eq!(
        ledger
            .settle_with_readback(
                "grid-place-missing",
                ExecutorCommandState::Reconciled,
                2_020,
                None,
                None,
            )
            .await,
        Err(BinanceCommandLedgerError::Conflict)
    );
    assert_eq!(
        command_identity(pool, "grid-place-missing").await?,
        ("accepted".into(), None)
    );

    insert_grid_cancel(
        pool,
        &scope,
        "grid-cancel-bind",
        "grid-cancel-client-bind",
        "cancel:long:open:1:1",
        "grid-client-bind",
        "native-bind",
        "accepted",
    )
    .await?;
    ledger
        .settle_with_readback(
            "grid-cancel-bind",
            ExecutorCommandState::Reconciled,
            2_030,
            None,
            Some("native-bind"),
        )
        .await?;
    assert_eq!(
        owner_identity(pool, "grid-client-bind").await?,
        (Some("native-bind".into()), "terminal".into())
    );

    insert_grid_place(
        pool,
        &scope,
        "grid-place-keep",
        "grid-client-keep",
        "long:open:4:1",
        4,
        Some("native-keep"),
        Some("native-keep"),
        "reconciled",
    )
    .await?;
    insert_grid_cancel(
        pool,
        &scope,
        "grid-cancel-rejected",
        "grid-cancel-client-rejected",
        "cancel:long:open:4:1",
        "grid-client-keep",
        "native-keep",
        "sending",
    )
    .await?;
    ledger
        .settle_with_readback(
            "grid-cancel-rejected",
            ExecutorCommandState::ReconcileRequired,
            2_040,
            Some("dispatch_unknown"),
            Some("native-keep"),
        )
        .await?;
    assert_eq!(
        owner_identity(pool, "grid-client-keep").await?,
        (Some("native-keep".into()), "working".into())
    );
    ledger
        .settle_with_readback(
            "grid-cancel-rejected",
            ExecutorCommandState::Rejected,
            2_041,
            Some("binance_rejected"),
            Some("native-keep"),
        )
        .await?;
    assert_eq!(
        owner_identity(pool, "grid-client-keep").await?,
        (Some("native-keep".into()), "working".into())
    );

    insert_terminal_command(pool, &scope, "terminal-command", "terminal-client").await?;
    ledger
        .settle_with_readback(
            "terminal-command",
            ExecutorCommandState::Reconciled,
            2_050,
            None,
            Some("native-terminal"),
        )
        .await?;
    assert_eq!(
        command_identity(pool, "terminal-command").await?,
        ("reconciled".into(), Some("native-terminal".into()))
    );
    assert_eq!(
        owner_identity(pool, "grid-client-keep").await?,
        (Some("native-keep".into()), "working".into())
    );
    Ok(())
}

struct GridScope {
    owner_user_id: String,
    trading_account_id: String,
    credential_id: String,
    instance_id: String,
}

async fn seed_grid_scope(pool: &PgPool) -> Result<GridScope, Box<dyn std::error::Error>> {
    let scope = GridScope {
        owner_user_id: id(1),
        trading_account_id: id(2),
        credential_id: id(3),
        instance_id: id(4),
    };
    sqlx::query(
        "INSERT INTO venue_users (user_id,username,password_hash,created_ms) \
         VALUES ($1,'grid-settlement-owner','fixture',1)",
    )
    .bind(&scope.owner_user_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO venue_user_trading_accounts \
         (trading_account_id,user_id,venue,exchange_identity_hash) \
         VALUES ($1,$2,'binance',$3)",
    )
    .bind(&scope.trading_account_id)
    .bind(&scope.owner_user_id)
    .bind(vec![2_u8; 32])
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO venue_api_credentials \
         (credential_id,user_id,label,key_fingerprint,masked_key,encrypted_credentials,\
          trading_account_id,verification_json,revision,created_ms) \
         VALUES ($1,$2,'grid','fixture-fingerprint','masked',$3,$4,\
          '{\"verification\":\"verified\"}'::jsonb,1,1)",
    )
    .bind(&scope.credential_id)
    .bind(&scope.owner_user_id)
    .bind(vec![3_u8; 32])
    .bind(&scope.trading_account_id)
    .execute(pool)
    .await?;

    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO venue_binance_grid_instances \
         (instance_id,owner_user_id,trading_account_id,credential_id,create_request_id,\
          create_request_digest,symbol,instance_state,revision,current_config_revision,\
          plan_revision,dirty,consecutive_failures,created_ms,updated_ms) \
         VALUES ($1,$2,$3,$4,$5,$6,'BTC/USDT','running',1,1,1,false,0,1,1)",
    )
    .bind(&scope.instance_id)
    .bind(&scope.owner_user_id)
    .bind(&scope.trading_account_id)
    .bind(&scope.credential_id)
    .bind(id(5))
    .bind(vec![5_u8; 32])
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO venue_binance_grid_config_revisions \
         (instance_id,config_revision,request_id,config_json,config_digest,created_ms) \
         VALUES ($1,1,$2,'{}'::jsonb,$3,1)",
    )
    .bind(&scope.instance_id)
    .bind(id(6))
    .bind(vec![6_u8; 32])
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(scope)
}

#[allow(clippy::too_many_arguments)]
async fn insert_grid_place(
    pool: &PgPool,
    scope: &GridScope,
    command_id: &str,
    client_order_id: &str,
    semantic_key: &str,
    level: i16,
    command_native_order_id: Option<&str>,
    owner_native_order_id: Option<&str>,
    state: &str,
) -> TestResult {
    insert_batch(pool, scope, command_id, state_time(state)).await?;
    let time = state_time(state);
    let sending_ms = (state != "pending").then_some(time);
    let accepted_ms = matches!(state, "accepted" | "reconciled").then_some(time);
    let terminal_ms = (state == "reconciled").then_some(time);
    sqlx::query(
        "INSERT INTO venue_binance_commands \
         (command_id,command_origin,owner_user_id,trading_account_id,credential_id,symbol,\
          position_side,command_phase,order_kind,order_side,requested_quantity,limit_price,\
          rule_version,native_order_id,client_order_id,command_state,source_digest,created_ms,\
          sending_ms,accepted_ms,terminal_ms,updated_ms,grid_instance_id,grid_config_revision,\
          grid_plan_revision,grid_semantic_key,grid_batch_id,dispatch_sequence) \
         VALUES ($1,'grid',$2,$3,$4,'BTC/USDT','long','open','limit_post_only','buy',\
                 '0.001','50000','fixture',$5,$6,$7,$8,$9,$10,$11,$12,$9,$13,1,1,$14,$1,1)",
    )
    .bind(command_id)
    .bind(&scope.owner_user_id)
    .bind(&scope.trading_account_id)
    .bind(&scope.credential_id)
    .bind(command_native_order_id)
    .bind(client_order_id)
    .bind(state)
    .bind(vec![level as u8; 32])
    .bind(time)
    .bind(sending_ms)
    .bind(accepted_ms)
    .bind(terminal_ms)
    .bind(&scope.instance_id)
    .bind(semantic_key)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO venue_binance_grid_order_owners \
         (trading_account_id,client_order_id,instance_id,config_revision,plan_revision,\
          semantic_key,place_command_id,symbol,position_side,order_role,grid_level,order_sequence,\
          order_side,quantity,filled_quantity,limit_price,native_order_id,ownership_source,\
          order_state,ownership_digest,first_seen_ms,last_seen_ms) \
         VALUES ($1,$2,$3,1,1,$4,$5,'BTC/USDT','long','open',$6,1,'buy','0.001','0',\
                 '50000',$7,'executor','working',$8,$9,$9)",
    )
    .bind(&scope.trading_account_id)
    .bind(client_order_id)
    .bind(&scope.instance_id)
    .bind(semantic_key)
    .bind(command_id)
    .bind(level)
    .bind(owner_native_order_id)
    .bind(vec![level as u8; 32])
    .bind(time)
    .execute(pool)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_grid_cancel(
    pool: &PgPool,
    scope: &GridScope,
    command_id: &str,
    client_order_id: &str,
    semantic_key: &str,
    target_client_order_id: &str,
    selected_native_order_id: &str,
    state: &str,
) -> TestResult {
    insert_batch(pool, scope, command_id, state_time(state)).await?;
    let time = state_time(state);
    let sending_ms = (state != "pending").then_some(time);
    let accepted_ms = (state == "accepted").then_some(time);
    sqlx::query(
        "INSERT INTO venue_binance_commands \
         (command_id,command_origin,owner_user_id,trading_account_id,credential_id,symbol,\
          command_phase,order_kind,rule_version,selected_native_order_id,target_client_order_id,\
          client_order_id,command_state,source_digest,created_ms,sending_ms,accepted_ms,updated_ms,\
          grid_instance_id,grid_config_revision,grid_plan_revision,grid_semantic_key,grid_batch_id,\
          dispatch_sequence) \
         VALUES ($1,'grid',$2,$3,$4,'BTC/USDT','cancel','cancel_exact','fixture',$5,$6,$7,$8,\
                 $9,$10,$11,$12,$10,$13,1,1,$14,$1,1)",
    )
    .bind(command_id)
    .bind(&scope.owner_user_id)
    .bind(&scope.trading_account_id)
    .bind(&scope.credential_id)
    .bind(selected_native_order_id)
    .bind(target_client_order_id)
    .bind(client_order_id)
    .bind(state)
    .bind(vec![9_u8; 32])
    .bind(time)
    .bind(sending_ms)
    .bind(accepted_ms)
    .bind(&scope.instance_id)
    .bind(semantic_key)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_batch(
    pool: &PgPool,
    scope: &GridScope,
    batch_id: &str,
    created_ms: i64,
) -> TestResult {
    sqlx::query(
        "INSERT INTO venue_binance_grid_mutation_batches \
         (batch_id,instance_id,expected_instance_revision,config_revision,plan_revision,\
          desired_digest,batch_digest,command_count,created_ms) \
         VALUES ($1,$2,1,1,1,$3,$4,1,$5)",
    )
    .bind(batch_id)
    .bind(&scope.instance_id)
    .bind(vec![7_u8; 32])
    .bind(vec![8_u8; 32])
    .bind(created_ms)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_terminal_command(
    pool: &PgPool,
    scope: &GridScope,
    command_id: &str,
    client_order_id: &str,
) -> TestResult {
    sqlx::query(
        "INSERT INTO venue_binance_commands \
         (command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,\
          symbol,position_side,command_phase,order_kind,order_side,requested_quantity,rule_version,\
          client_order_id,command_state,created_ms,sending_ms,accepted_ms,updated_ms) \
         VALUES ($1,'terminal',$2,$3,$4,$5,'BTC/USDT','long','open','market','buy','0.001',\
                 'fixture',$6,'accepted',100,100,100,100)",
    )
    .bind(command_id)
    .bind(id(50))
    .bind(&scope.owner_user_id)
    .bind(&scope.trading_account_id)
    .bind(&scope.credential_id)
    .bind(client_order_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn owner_identity(
    pool: &PgPool,
    client_order_id: &str,
) -> TestResultValue<(Option<String>, String)> {
    let row = sqlx::query_as::<_, (Option<String>, String)>(
        "SELECT native_order_id,order_state FROM venue_binance_grid_order_owners \
         WHERE client_order_id=$1",
    )
    .bind(client_order_id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

async fn command_identity(
    pool: &PgPool,
    command_id: &str,
) -> TestResultValue<(String, Option<String>)> {
    let row = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT command_state,native_order_id FROM venue_binance_commands WHERE command_id=$1",
    )
    .bind(command_id)
    .fetch_one(pool)
    .await?;
    Ok(row)
}

type TestResultValue<T> = Result<T, Box<dyn std::error::Error>>;

fn state_time(state: &str) -> i64 {
    match state {
        "sending" => 110,
        "accepted" => 120,
        "reconciled" => 130,
        _ => 100,
    }
}

fn id(value: u64) -> String {
    format!("00000000-0000-4000-8000-{value:012}")
}

struct Fixture {
    pool: PgPool,
    admin: PgPool,
    schema: String,
}

impl Fixture {
    async fn create() -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let Some(url) = std::env::var("VENUE_CONTROL_TEST_DATABASE_URL").ok() else {
            if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED")
                .ok()
                .as_deref()
                == Some("1")
            {
                return Err("Grid owner-settlement PostgreSQL database is required".into());
            }
            eprintln!("SKIP: Grid owner-settlement PostgreSQL database is not configured");
            return Ok(None);
        };
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let schema = format!("venue_grid_owner_{}_{nonce}", std::process::id());
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
            .connect(&url)
            .await?;
        for migration in [
            MIGRATION_0001,
            MIGRATION_0015,
            MIGRATION_0017,
            MIGRATION_0018,
            MIGRATION_0019,
            MIGRATION_0020,
            MIGRATION_0021,
            MIGRATION_0022,
            MIGRATION_0023,
            MIGRATION_0024,
        ] {
            sqlx::raw_sql(migration).execute(&pool).await?;
        }
        Ok(Some(Self {
            pool,
            admin,
            schema,
        }))
    }

    async fn cleanup(self) -> TestResult {
        self.pool.close().await;
        self.admin
            .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
            .await?;
        self.admin.close().await;
        Ok(())
    }
}
