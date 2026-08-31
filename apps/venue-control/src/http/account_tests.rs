use super::*;
use crate::{
    PgControlRepository,
    accounts::{
        AccountService, CredentialCipher,
        test_support::{Fixture, TestResult, login, now},
    },
};
use venue_control_protocol::{
    CommandReceipt, CommandState, ControlAction, ControlSnapshot, accounts::*,
};

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

#[tokio::test]
async fn postgres_http_account_lifecycle_requires_session_json_and_ownership() -> TestResult {
    let Some(f) = Fixture::create().await? else {
        return Ok(());
    };
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
        .post(REGISTER_PATH, None, &login("alice"))
        .send()
        .await?
        .error_for_status()?
        .json::<SessionResponse>()
        .await?;
    let bob = s
        .post(REGISTER_PATH, None, &login("bob"))
        .send()
        .await?
        .error_for_status()?
        .json::<SessionResponse>()
        .await?;
    code(
        s.post(REGISTER_PATH, None, &login("Alice")).send().await?,
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
    for session in [None, Some(&bob)] {
        let mut stream = s
            .get(venue_control_protocol::EVENT_STREAM_PATH, session)
            .send()
            .await?
            .error_for_status()?;
        let mut frames = String::new();
        tokio::time::timeout(Duration::from_secs(3), async {
            while !frames.contains(": scoped") {
                let chunk = stream.chunk().await?.ok_or("missing scoped event")?;
                frames.push_str(std::str::from_utf8(&chunk)?);
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await??;
        assert!(
            !frames.contains(account)
                && !frames.contains("grid-btc")
                && !frames.contains(&receipt.receipt_id)
        );
    }
    let mut stream = s
        .get(venue_control_protocol::EVENT_STREAM_PATH, Some(&alice))
        .send()
        .await?
        .error_for_status()?;
    let mut frames = String::new();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !frames.contains(&receipt.receipt_id) {
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
            &format!("{}?after=bad", venue_control_protocol::EVENT_STREAM_PATH),
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
