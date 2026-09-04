use super::*;
use crate::{
    PgControlRepository,
    accounts::{
        AccountService, CredentialCipher,
        test_support::{Fixture, TestResult, login, now},
    },
};
use venue_control_protocol::{
    CommandReceipt, CommandState, ControlAction, ControlSnapshot,
    accounts::*,
    grid::{
        GRID_INSTANCES_PATH, GRID_LIFECYCLE_PATH, GRID_SCHEMA_VERSION, GridConfig,
        GridConfigUpdateRequest, GridInstanceCreateRequest, GridInstanceState, GridInstanceSummary,
        GridInventoryReplenishment, GridLifecycleAction, GridLifecycleRequest, GridProfitReduction,
        GridResetPolicy,
    },
};

const INVITE_CODE: &str = "Safe_Kol_Invite_Code_00001";

#[tokio::test]
async fn terminal_registration_is_free_without_weakening_invite_registration() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let s = Server::start(&f).await?;
    code(
        s.post(REGISTER_PATH, None, &login("ordinary"))
            .send()
            .await?,
        400,
        AccountErrorCode::InvalidInput,
    )
    .await?;
    let ordinary = s
        .post(TERMINAL_REGISTER_PATH, None, &login("ordinary"))
        .send()
        .await?
        .error_for_status()?
        .json::<SessionResponse>()
        .await?;
    let overview = s
        .get(SESSION_PATH, Some(&ordinary))
        .send()
        .await?
        .error_for_status()?
        .json::<AccountOverview>()
        .await?;
    assert_eq!(overview.user.user_id, ordinary.user.user_id);
    assert!(overview.credentials.is_empty());
    let bindings: i64 =
        sqlx::query_scalar("SELECT count(*) FROM venue_user_kol_bindings WHERE user_id=$1")
            .bind(&ordinary.user.user_id)
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(bindings, 0);
    let kols: i64 =
        sqlx::query_scalar("SELECT count(*) FROM venue_kol_profiles WHERE kol_user_id=$1")
            .bind(&ordinary.user.user_id)
            .fetch_one(&f.pool)
            .await?;
    assert_eq!(kols, 0);
    let instances = s
        .get(GRID_INSTANCES_PATH, Some(&ordinary))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<GridInstanceSummary>>()
        .await?;
    assert!(instances.is_empty());
    code(
        s.post(TERMINAL_REGISTER_PATH, None, &registration("forged"))
            .send()
            .await?,
        400,
        AccountErrorCode::InvalidInput,
    )
    .await?;
    code(
        s.post(REGISTER_PATH, None, &registration("follower"))
            .send()
            .await?,
        400,
        AccountErrorCode::InvalidInput,
    )
    .await?;
    s.post(LOGIN_PATH, None, &login("ordinary"))
        .send()
        .await?
        .error_for_status()?;
    s.post(LOGOUT_PATH, Some(&ordinary), &())
        .send()
        .await?
        .error_for_status()?;
    code(
        s.get(SESSION_PATH, Some(&ordinary)).send().await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    s.stop().await?;
    f.cleanup().await
}

fn registration(username: &str) -> RegisterRequest {
    RegisterRequest {
        username: username.into(),
        password: login(username).password,
        invite_code: INVITE_CODE.into(),
    }
}

#[tokio::test]
async fn ordinary_terminal_reads_only_its_verified_account_projection() -> TestResult {
    use venue_control_protocol::kol::{
        KOL_TERMINAL_ACCOUNT_PATH, TERMINAL_PROJECTION_SCHEMA_VERSION, TerminalAccountProjection,
        TerminalProjectionRequest,
    };
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let s = Server::start(&f).await?;
    let alice = s
        .post(TERMINAL_REGISTER_PATH, None, &login("alice"))
        .send()
        .await?
        .error_for_status()?
        .json::<SessionResponse>()
        .await?;
    let bob = f.service.register(login("bob"), now()).await?;
    let principal = f.service.authenticate(alice.token.expose(), now()).await?;
    let mut credential = f
        .service
        .bind_credential(&principal, binding(), now())
        .await?;
    let account = "00000000-0000-4000-8000-000000000711";
    let observed = now();
    // Only this isolated fixture supplies signed-readback results; no exchange call is made.
    sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)")
        .bind(account).bind(&alice.user.user_id).bind([11_u8;32].as_slice()).execute(&f.pool).await?;
    credential.trading_account_id = Some(account.into());
    credential.verification = ApiVerificationState::Verified;
    credential.verified_ms = Some(observed);
    credential.api_reachable = true;
    credential.dual_position = true;
    sqlx::query("UPDATE venue_api_credentials SET trading_account_id=$1,verification_json=$2 WHERE credential_id=$3")
        .bind(account).bind(serde_json::to_value(&credential)?).bind(&credential.credential_id).execute(&f.pool).await?;
    let request = TerminalProjectionRequest {
        schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
        credential_id: credential.credential_id.clone(),
        symbols: vec!["BTC/USDT".parse()?],
    };
    let empty = s
        .post(KOL_TERMINAL_ACCOUNT_PATH, Some(&alice), &request)
        .send()
        .await?
        .error_for_status()?
        .json::<Option<TerminalAccountProjection>>()
        .await?;
    assert!(empty.is_none());
    let position = serde_json::json!({
        "symbol":"BTC/USDT", "position_side":"long", "quantity":"0.01",
        "entry_price":"50000", "mark_price":"50100"
    });
    let projection: TerminalAccountProjection = serde_json::from_value(serde_json::json!({
        "schema_version":TERMINAL_PROJECTION_SCHEMA_VERSION,
        "credential_id":credential.credential_id, "trading_account_id":account,
        "observed_ms":observed, "persisted_ms":observed, "private_generation":1,
        "position_mode":"hedge", "positions":[position.clone()], "position_history":[],
        "open_orders":[{
            "client_order_id":"fixture-order", "native_order_id":"123", "symbol":"BTC/USDT",
            "order_side":"buy", "position_side":"long", "quantity":"0.001",
            "filled_quantity":"0", "limit_price":"49900", "post_only":true,
            "reduce_only":false, "state":"new", "created_ms":observed
        }],
        "fills":[], "assets":[{"asset":"USDT", "equity":"100", "available_margin":"90"}]
    }))?;
    projection.validate()?;
    sqlx::query("INSERT INTO venue_binance_account_projections (credential_id,owner_user_id,trading_account_id,observed_ms,persisted_ms,private_generation,projection_json) VALUES ($1,$2,$3,$4,$4,1,$5)")
        .bind(&credential.credential_id).bind(&alice.user.user_id).bind(account).bind(i64::try_from(observed)?)
        .bind(serde_json::json!({"fills_cursor":"fixture-cursor","stream_healthy":true,"projection":projection}))
        .execute(&f.pool).await?;
    sqlx::query("INSERT INTO venue_binance_position_history (trading_account_id,owner_user_id,symbol,position_side,observed_ms,position_json) VALUES ($1,$2,'BTC/USDT','long',$3,$4)")
        .bind(account).bind(&alice.user.user_id).bind(i64::try_from(observed)?).bind(position).execute(&f.pool).await?;
    let response = s
        .post(KOL_TERMINAL_ACCOUNT_PATH, Some(&alice), &request)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok()),
        Some("no-store")
    );
    let owned = response
        .json::<Option<TerminalAccountProjection>>()
        .await?
        .ok_or("projection missing")?;
    assert_eq!(owned.credential_id, credential.credential_id);
    assert_eq!(owned.positions.len(), 1);
    assert_eq!(owned.open_orders.len(), 1);
    assert_eq!(owned.assets.len(), 1);
    assert_eq!(owned.position_history.len(), 1);
    assert_eq!(owned.position_history[0].position, owned.positions[0]);
    code(
        s.post(KOL_TERMINAL_ACCOUNT_PATH, None, &request)
            .send()
            .await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    code(
        s.post(KOL_TERMINAL_ACCOUNT_PATH, Some(&bob), &request)
            .send()
            .await?,
        409,
        AccountErrorCode::VerificationRequired,
    )
    .await?;
    sqlx::query("UPDATE venue_api_credentials SET deleted_ms=$1 WHERE credential_id=$2")
        .bind(i64::try_from(now())?)
        .bind(&credential.credential_id)
        .execute(&f.pool)
        .await?;
    code(
        s.post(KOL_TERMINAL_ACCOUNT_PATH, Some(&alice), &request)
            .send()
            .await?,
        409,
        AccountErrorCode::VerificationRequired,
    )
    .await?;
    s.stop().await?;
    f.cleanup().await
}

async fn seed_enabled_invite(fixture: &Fixture) -> TestResult {
    let kol = fixture.service.register(login("kol"), now()).await?;
    let account_id = "00000000-0000-4000-8000-000000000701";
    sqlx::query("INSERT INTO venue_user_trading_accounts (trading_account_id,user_id,venue,exchange_identity_hash) VALUES ($1,$2,'binance',$3)")
        .bind(account_id)
        .bind(&kol.user.user_id)
        .bind([7_u8; 32].as_slice())
        .execute(&fixture.pool)
        .await?;
    sqlx::query("INSERT INTO venue_kol_profiles (kol_user_id,leader_trading_account_id,public_name,public_title,public_description,strategy_capital,profile_state,active_slot,created_ms,updated_ms) VALUES ($1,$2,'KOL','Title','','100','enabled',1,$3,$3)")
        .bind(&kol.user.user_id)
        .bind(account_id)
        .bind(i64::try_from(now())?)
        .execute(&fixture.pool)
        .await?;
    sqlx::query("INSERT INTO venue_kol_invites (invite_id,kol_user_id,code_hash,invite_state,created_ms) VALUES ('00000000-0000-4000-8000-000000000702',$1,$2,'active',$3)")
        .bind(&kol.user.user_id)
        .bind(ring::digest::digest(&ring::digest::SHA256, INVITE_CODE.as_bytes()).as_ref())
        .bind(i64::try_from(now())?)
        .execute(&fixture.pool)
        .await?;
    Ok(())
}

struct Server {
    endpoint: String,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), HttpServerError>>,
    client: reqwest::Client,
}
impl Server {
    async fn start(f: &Fixture) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (stop, shutdown) = control_shutdown_channel();
        let service = Arc::new(ControlService::new(PgControlRepository::new(
            f.pool.clone(),
        )));
        let accounts = Arc::new(AccountService::new(
            f.pool.clone(),
            CredentialCipher::from_key(&[17; 32])?,
        )?);
        let task = tokio::spawn(serve_local_with_accounts(
            listener,
            service,
            accounts,
            ControlHttpConfig::default(),
            shutdown,
        ));
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            endpoint,
            stop,
            task,
            client,
        })
    }
    fn get(&self, path: &str, session: Option<&SessionResponse>) -> reqwest::RequestBuilder {
        let request = self.client.get(format!("{}{path}", self.endpoint));
        match session {
            Some(s) => request.bearer_auth(s.token.expose()),
            None => request,
        }
    }
    fn post(
        &self,
        path: &str,
        session: Option<&SessionResponse>,
        value: &impl serde::Serialize,
    ) -> reqwest::RequestBuilder {
        let request = self
            .client
            .post(format!("{}{path}", self.endpoint))
            .json(value);
        match session {
            Some(s) => request.bearer_auth(s.token.expose()),
            None => request,
        }
    }
    async fn stop(self) -> TestResult {
        self.stop.send(true)?;
        self.task.await??;
        Ok(())
    }
}

async fn code(response: reqwest::Response, status: u16, expected: AccountErrorCode) -> TestResult {
    assert_eq!(response.status().as_u16(), status);
    assert_eq!(
        response.json::<AccountErrorResponse>().await?.code,
        expected
    );
    Ok(())
}
fn binding() -> BindCredentialRequest {
    BindCredentialRequest {
        label: "Primary account".into(),
        api_key: SecretValue::new("A".repeat(32)),
        api_secret: SecretValue::new("S".repeat(32)),
    }
}

fn grid_config(order_notional: i64) -> GridConfig {
    GridConfig {
        order_notional: rust_decimal::Decimal::new(order_notional, 0),
        spacing_rate: rust_decimal::Decimal::new(2, 3),
        grid_levels: 10,
        max_total_notional: rust_decimal::Decimal::new(1_000, 0),
        inventory_replenishment: GridInventoryReplenishment {
            enabled: false,
            minimum_inventory_notional: rust_decimal::Decimal::new(10, 0),
            target_inventory_notional: rust_decimal::Decimal::new(20, 0),
            max_single_replenishment_notional: rust_decimal::Decimal::new(10, 0),
        },
        profit_reduction: GridProfitReduction {
            enabled: false,
            inventory_equity_multiple: rust_decimal::Decimal::new(3, 0),
            minimum_unrealized_profit_rate: rust_decimal::Decimal::new(5, 2),
            reduction_fraction: rust_decimal::Decimal::new(3, 1),
            max_single_reduce_notional: rust_decimal::Decimal::new(100, 0),
        },
        reset_policy: GridResetPolicy {
            stale_market_ms: 5_000,
            stale_private_ms: 15_000,
            convergence_timeout_ms: 30_000,
            max_consecutive_failures: 3,
        },
    }
}

#[tokio::test]
async fn postgres_http_account_lifecycle_requires_session_json_and_ownership() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    seed_enabled_invite(&f).await?;
    let s = Server::start(&f).await?;
    code(
        s.get(SESSION_PATH, None).send().await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    code(
        s.post(CREDENTIALS_PATH, None, &binding()).send().await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    let anonymous = s
        .get(venue_control_protocol::SNAPSHOT_PATH, None)
        .send()
        .await?;
    assert_eq!(
        anonymous
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok()),
        Some("no-store")
    );
    assert!(
        anonymous
            .json::<ControlSnapshot>()
            .await?
            .accounts
            .is_empty()
    );
    let alice = s
        .post(REGISTER_PATH, None, &registration("alice"))
        .send()
        .await?
        .error_for_status()?
        .json::<SessionResponse>()
        .await?;
    let bob = s
        .post(REGISTER_PATH, None, &registration("bob"))
        .send()
        .await?
        .error_for_status()?
        .json::<SessionResponse>()
        .await?;
    code(
        s.post(REGISTER_PATH, None, &registration("Alice"))
            .send()
            .await?,
        409,
        AccountErrorCode::UsernameUnavailable,
    )
    .await?;
    let response = s
        .post(CREDENTIALS_PATH, Some(&alice), &binding())
        .send()
        .await?
        .error_for_status()?;
    let raw = response.text().await?;
    assert!(!raw.contains(&"A".repeat(32)) && !raw.contains(&"S".repeat(32)));
    let credential: CredentialSummary = serde_json::from_str(&raw)?;
    let target = CredentialRequest {
        credential_id: credential.credential_id.clone(),
    };
    for route in [VERIFY_PATH, SELECT_PATH] {
        code(
            s.post(route, Some(&bob), &target).send().await?,
            404,
            AccountErrorCode::NotFound,
        )
        .await?;
    }
    code(
        s.post(SELECT_PATH, Some(&alice), &target).send().await?,
        409,
        AccountErrorCode::VerificationRequired,
    )
    .await?;
    let overview = s
        .get(SESSION_PATH, Some(&bob))
        .send()
        .await?
        .json::<AccountOverview>()
        .await?;
    assert!(overview.credentials.is_empty());
    let removal = DeleteCredentialRequest {
        credential_id: credential.credential_id,
        password: login("alice").password,
    };
    code(
        s.post(DELETE_PATH, Some(&bob), &removal).send().await?,
        404,
        AccountErrorCode::NotFound,
    )
    .await?;
    let wrong_type = s
        .client
        .post(format!("{}{CREDENTIALS_PATH}", s.endpoint))
        .bearer_auth(alice.token.expose())
        .body(serde_json::to_vec(&binding())?)
        .send()
        .await?;
    code(wrong_type, 400, AccountErrorCode::InvalidInput).await?;
    let overview = s
        .post(DELETE_PATH, Some(&alice), &removal)
        .send()
        .await?
        .error_for_status()?
        .json::<AccountOverview>()
        .await?;
    assert!(overview.credentials.is_empty());
    s.post(LOGOUT_PATH, Some(&alice), &())
        .send()
        .await?
        .error_for_status()?;
    code(
        s.get(SESSION_PATH, Some(&alice)).send().await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    let restored = s
        .post(LOGIN_PATH, None, &login("ALICE"))
        .send()
        .await?
        .error_for_status()?
        .json::<SessionResponse>()
        .await?;
    assert_eq!(restored.user.user_id, alice.user.user_id);
    assert_ne!(restored.token.expose(), alice.token.expose());
    s.stop().await?;
    f.cleanup().await
}

#[tokio::test]
async fn postgres_http_grid_is_user_scoped_and_running_config_is_revisioned() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let alice = f.service.register(login("grid-alice"), now()).await?;
    let bob = f.service.register(login("grid-bob"), now()).await?;
    let principal = f.service.authenticate(alice.token.expose(), now()).await?;
    let mut credential = f
        .service
        .bind_credential(&principal, binding(), now())
        .await?;
    let account_id = "00000000-0000-4000-8000-000000000801";
    sqlx::query(
        "INSERT INTO venue_user_trading_accounts \
         (trading_account_id,user_id,venue,exchange_identity_hash) \
         VALUES ($1,$2,'binance',$3)",
    )
    .bind(account_id)
    .bind(&alice.user.user_id)
    .bind([8_u8; 32].as_slice())
    .execute(&f.pool)
    .await?;
    credential.trading_account_id = Some(account_id.to_owned());
    credential.verification = ApiVerificationState::Verified;
    credential.verified_ms = Some(now());
    credential.api_reachable = true;
    credential.dual_position = true;
    sqlx::query(
        "UPDATE venue_api_credentials SET trading_account_id=$1,verification_json=$2 \
         WHERE credential_id=$3",
    )
    .bind(account_id)
    .bind(serde_json::to_value(&credential)?)
    .bind(&credential.credential_id)
    .execute(&f.pool)
    .await?;

    let server = Server::start(&f).await?;
    code(
        server.get(GRID_INSTANCES_PATH, None).send().await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    assert!(
        server
            .get(GRID_INSTANCES_PATH, Some(&bob))
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<GridInstanceSummary>>()
            .await?
            .is_empty()
    );

    let create = GridInstanceCreateRequest {
        schema_version: GRID_SCHEMA_VERSION,
        request_id: "00000000-0000-4000-8000-000000000802".to_owned(),
        credential_id: credential.credential_id.clone(),
        symbol: "BTC/USDT".parse()?,
        config: grid_config(10),
    };
    let created = server
        .post(GRID_INSTANCES_PATH, Some(&alice), &create)
        .send()
        .await?
        .error_for_status()?
        .json::<GridInstanceSummary>()
        .await?;
    assert_eq!(created.state, GridInstanceState::Draft);
    assert_eq!(created.trading_account_id, account_id);
    assert_eq!(created.credential_id, credential.credential_id);
    assert_eq!(
        server
            .get(GRID_INSTANCES_PATH, Some(&alice))
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<GridInstanceSummary>>()
            .await?
            .len(),
        1
    );

    let foreign_update = GridConfigUpdateRequest {
        schema_version: GRID_SCHEMA_VERSION,
        request_id: "00000000-0000-4000-8000-000000000803".to_owned(),
        instance_id: created.instance_id.clone(),
        expected_revision: created.revision,
        config: grid_config(11),
    };
    code(
        server
            .post(GRID_INSTANCES_PATH, Some(&bob), &foreign_update)
            .send()
            .await?,
        403,
        AccountErrorCode::Forbidden,
    )
    .await?;

    let invalid_start = GridLifecycleRequest {
        schema_version: GRID_SCHEMA_VERSION,
        request_id: "00000000-0000-4000-8000-000000000804".to_owned(),
        instance_id: created.instance_id.clone(),
        expected_revision: created.revision,
        action: GridLifecycleAction::Start,
        risk_confirmed: false,
        positions_remain_acknowledged: false,
    };
    code(
        server
            .post(GRID_LIFECYCLE_PATH, Some(&alice), &invalid_start)
            .send()
            .await?,
        400,
        AccountErrorCode::InvalidInput,
    )
    .await?;
    let start = GridLifecycleRequest {
        risk_confirmed: true,
        ..invalid_start
    };
    credential.api_reachable = false;
    sqlx::query("UPDATE venue_api_credentials SET verification_json=$1 WHERE credential_id=$2")
        .bind(serde_json::to_value(&credential)?)
        .bind(&credential.credential_id)
        .execute(&f.pool)
        .await?;
    code(
        server
            .post(GRID_LIFECYCLE_PATH, Some(&alice), &start)
            .send()
            .await?,
        409,
        AccountErrorCode::VerificationRequired,
    )
    .await?;
    credential.api_reachable = true;
    sqlx::query("UPDATE venue_api_credentials SET verification_json=$1 WHERE credential_id=$2")
        .bind(serde_json::to_value(&credential)?)
        .bind(&credential.credential_id)
        .execute(&f.pool)
        .await?;
    let starting = server
        .post(GRID_LIFECYCLE_PATH, Some(&alice), &start)
        .send()
        .await?
        .error_for_status()?
        .json::<GridInstanceSummary>()
        .await?;
    assert_eq!(starting.state, GridInstanceState::StartPending);

    let settled_ms = now().saturating_add(1);
    sqlx::query(
        "UPDATE venue_binance_grid_instances SET instance_state='running',revision=revision+1,\
         dirty=FALSE,convergence_started_ms=NULL,updated_ms=$1 WHERE instance_id=$2",
    )
    .bind(i64::try_from(settled_ms)?)
    .bind(&created.instance_id)
    .execute(&f.pool)
    .await?;
    let running = server
        .get(GRID_INSTANCES_PATH, Some(&alice))
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<GridInstanceSummary>>()
        .await?
        .into_iter()
        .next()
        .ok_or("missing running Grid")?;
    assert_eq!(running.state, GridInstanceState::Running);
    let update = GridConfigUpdateRequest {
        schema_version: GRID_SCHEMA_VERSION,
        request_id: "00000000-0000-4000-8000-000000000805".to_owned(),
        instance_id: running.instance_id.clone(),
        expected_revision: running.revision,
        config: grid_config(12),
    };
    let updated = server
        .post(GRID_INSTANCES_PATH, Some(&alice), &update)
        .send()
        .await?
        .error_for_status()?
        .json::<GridInstanceSummary>()
        .await?;
    assert_eq!(updated.state, GridInstanceState::Running);
    assert_eq!(updated.config_revision, 2);
    assert_eq!(
        updated.config.order_notional,
        rust_decimal::Decimal::new(12, 0)
    );

    let stop_without_ack = GridLifecycleRequest {
        schema_version: GRID_SCHEMA_VERSION,
        request_id: "00000000-0000-4000-8000-000000000806".to_owned(),
        instance_id: updated.instance_id.clone(),
        expected_revision: updated.revision,
        action: GridLifecycleAction::Stop,
        risk_confirmed: false,
        positions_remain_acknowledged: false,
    };
    code(
        server
            .post(GRID_LIFECYCLE_PATH, Some(&alice), &stop_without_ack)
            .send()
            .await?,
        400,
        AccountErrorCode::InvalidInput,
    )
    .await?;
    let stopped = server
        .post(
            GRID_LIFECYCLE_PATH,
            Some(&alice),
            &GridLifecycleRequest {
                positions_remain_acknowledged: true,
                ..stop_without_ack
            },
        )
        .send()
        .await?
        .error_for_status()?
        .json::<GridInstanceSummary>()
        .await?;
    assert_eq!(stopped.state, GridInstanceState::StopPending);

    server.stop().await?;
    f.cleanup().await
}

#[tokio::test]
async fn postgres_http_private_snapshots_commands_and_sse_are_scoped_and_revocable() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
    let alice = f.service.register(login("alice"), now()).await?;
    let bob = f.service.register(login("bob"), now()).await?;
    let a = f.service.authenticate(alice.token.expose(), now()).await?;
    let mut credential = f.service.bind_credential(&a, binding(), now()).await?;
    let snapshot = super::tests::snapshot()?;
    let account = &snapshot.accounts[0].trading_account_id;
    // Offline fixture evidence: the real probe/state transitions are tested in accounts.
    sqlx::query("INSERT INTO venue_user_trading_accounts(trading_account_id,user_id,venue,exchange_identity_hash) VALUES($1,$2,'binance',$3)")
        .bind(account).bind(&alice.user.user_id).bind([9_u8;32].as_slice()).execute(&f.pool).await?;
    credential.trading_account_id = Some(account.clone());
    credential.verification = ApiVerificationState::Verified;
    credential.verified_ms = Some(now());
    credential.expires_ms = Some(now() + 60_000);
    credential.api_reachable = true;
    credential.dual_position = true;
    sqlx::query("UPDATE venue_api_credentials SET trading_account_id=$1,verification_json=$2 WHERE credential_id=$3")
        .bind(account).bind(serde_json::to_value(&credential)?).bind(&credential.credential_id).execute(&f.pool).await?;
    let service = ControlService::new(PgControlRepository::new(f.pool.clone()));
    service.publish_snapshot(&snapshot).await?;
    let s = Server::start(&f).await?;
    for session in [None, Some(&bob)] {
        let response = s
            .get(venue_control_protocol::SNAPSHOT_PATH, session)
            .send()
            .await?
            .error_for_status()?
            .json::<ControlSnapshot>()
            .await?;
        assert!(response.accounts.is_empty() && response.strategies.is_empty());
    }
    let response = s
        .get(venue_control_protocol::SNAPSHOT_PATH, Some(&alice))
        .send()
        .await?
        .error_for_status()?
        .json::<ControlSnapshot>()
        .await?;
    assert_eq!(response.accounts.len(), 1);
    assert_eq!(response.strategies.len(), 1);
    let command = super::tests::command(ControlAction::Resume)?;
    code(
        s.post(venue_control_protocol::COMMAND_PATH, None, &command)
            .send()
            .await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    code(
        s.post(venue_control_protocol::COMMAND_PATH, Some(&bob), &command)
            .send()
            .await?,
        409,
        AccountErrorCode::VerificationRequired,
    )
    .await?;
    s.post(
        SELECT_PATH,
        Some(&alice),
        &CredentialRequest {
            credential_id: credential.credential_id.clone(),
        },
    )
    .send()
    .await?
    .error_for_status()?;
    let receipt = s
        .post(venue_control_protocol::COMMAND_PATH, Some(&alice), &command)
        .send()
        .await?
        .error_for_status()?
        .json::<CommandReceipt>()
        .await?;
    assert_eq!(receipt.state, CommandState::Accepted);
    let mut foreign = command.clone();
    foreign.trading_account_id = "00000000-0000-4000-8000-000000000099".into();
    code(
        s.post(venue_control_protocol::COMMAND_PATH, Some(&alice), &foreign)
            .send()
            .await?,
        403,
        AccountErrorCode::Forbidden,
    )
    .await?;
    let event_path = format!(
        "{}?venue=binance&mode=LIVE&trading_account_id={account}&after=0",
        venue_control_protocol::EVENT_STREAM_PATH,
    );
    code(
        s.get(&event_path, None).send().await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    code(
        s.get(&event_path, Some(&bob)).send().await?,
        403,
        AccountErrorCode::Forbidden,
    )
    .await?;
    let mut stream = s
        .get(&event_path, Some(&alice))
        .send()
        .await?
        .error_for_status()?;
    let mut frames = String::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !frames.contains("event: control") {
            let chunk = stream.chunk().await?.ok_or("missing private event")?;
            frames.push_str(std::str::from_utf8(&chunk)?);
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await??;
    assert!(frames.contains(account));
    s.post(LOGOUT_PATH, Some(&alice), &())
        .send()
        .await?
        .error_for_status()?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while stream.chunk().await?.is_some() {}
        Ok::<_, reqwest::Error>(())
    })
    .await??;
    code(
        s.get(venue_control_protocol::SNAPSHOT_PATH, Some(&alice))
            .send()
            .await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    code(
        s.post("/v2/account-node/deliveries/claim", Some(&bob), &())
            .send()
            .await?,
        401,
        AccountErrorCode::Unauthorized,
    )
    .await?;
    code(
        s.get(
            &format!(
                "{}?venue=binance&mode=LIVE&trading_account_id={account}&after=bad",
                venue_control_protocol::EVENT_STREAM_PATH,
            ),
            None,
        )
        .send()
        .await?,
        400,
        AccountErrorCode::InvalidInput,
    )
    .await?;
    s.stop().await?;
    f.cleanup().await
}
