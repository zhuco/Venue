use super::*;
use crate::accounts::{AccountError, AccountService, Principal};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeSet;
use venue_control_protocol::{ControlEvent, ControlSnapshot, accounts::*};

pub(super) async fn dispatch_authenticated<R>(
    stream: &mut TcpStream,
    state: &HttpState<R>,
    request: HttpRequest,
) -> Result<(), ()>
where
    R: ControlRepository + AccountDeliveryRepository + 'static,
{
    let Some((path, query)) = split_target(&request.target) else {
        return write_error(stream, HttpError::BadRequest)
            .await
            .map_err(|_| ());
    };
    let accounts = match &state.access {
        AccessMode::Accounts(accounts) => Some(accounts.as_ref()),
        _ => None,
    };
    let now = now_ms().map_err(|_| ())?;
    if path.starts_with("/v2/account/") {
        let Some(accounts) = accounts else {
            return account_error(stream, AccountErrorCode::Unavailable).await;
        };
        if query.is_some() || (request.method == Method::Post && !request.json_content) {
            return account_error(stream, AccountErrorCode::InvalidInput).await;
        }
        let result = time::timeout(
            Duration::from_secs(30),
            account_request(accounts, &request, path, now),
        )
        .await;
        return match result {
            Ok(Ok(body)) => write_response(stream, "200 OK", "application/json", "close", &body)
                .await
                .map_err(|_| ()),
            Ok(Err(error)) => account_error(stream, error.code).await,
            Err(_) => account_error(stream, AccountErrorCode::Unavailable).await,
        };
    }
    if AccountNodeRoute::from_path(path).is_some() {
        if !accounts.is_some_and(|a| {
            request
                .bearer
                .as_ref()
                .is_some_and(|t| a.node_authorized(t.expose()))
        }) {
            return account_error(stream, AccountErrorCode::Unauthorized).await;
        }
        return dispatch_control(stream, state, request).await;
    }
    let principal = match principal(accounts, request.bearer.as_ref(), now).await {
        Ok(principal) => principal,
        Err(error) => return account_error(stream, error.code).await,
    };
    match (request.method, path) {
        (Method::Get, venue_control_protocol::SNAPSHOT_PATH) if query.is_none() => {
            let owned = match owned(accounts, principal.as_ref()).await {
                Ok(ids) => ids,
                Err(error) => return account_error(stream, error.code).await,
            };
            let snapshot = match state.service.snapshot().await {
                Ok(snapshot) => snapshot,
                Err(ServiceError::SnapshotUnavailable) => empty_snapshot(now),
                Err(_) => return account_error(stream, AccountErrorCode::Unavailable).await,
            };
            let body = serde_json::to_vec(&filter_snapshot(snapshot, &owned)).map_err(|_| ())?;
            write_response(stream, "200 OK", "application/json", "close", &body)
                .await
                .map_err(|_| ())
        }
        (Method::Get, venue_control_protocol::EVENT_STREAM_PATH) => {
            let cursor = match event_cursor(query, request.last_event_id) {
                Ok(cursor) => cursor,
                Err(_) => return account_error(stream, AccountErrorCode::InvalidInput).await,
            };
            write_sse_headers(stream).await.map_err(|_| ())?;
            stream_scoped_events(stream, state, cursor, request.bearer).await;
            Ok(())
        }
        (Method::Post, venue_control_protocol::COMMAND_PATH)
            if query.is_none() && request.json_content =>
        {
            let (Some(accounts), Some(principal)) = (accounts, principal) else {
                return account_error(stream, AccountErrorCode::Unauthorized).await;
            };
            let command: ControlCommandRequest = match decode(&request.body) {
                Ok(value) => value,
                Err(e) => return account_error(stream, e.code).await,
            };
            let access = match accounts
                .authorize_command(&principal, command.venue, &command.trading_account_id, now)
                .await
            {
                Ok(access) => access,
                Err(error) => return account_error(stream, error.code).await,
            };
            let result = dispatch_control(stream, state, request).await;
            let _ = access.rollback().await;
            result
        }
        // Indicator projections contain no user/account data.
        (Method::Get, INDICATOR_SNAPSHOT_PATH | INDICATOR_EVENT_STREAM_PATH) => {
            dispatch_control(stream, state, request).await
        }
        _ => account_error(stream, AccountErrorCode::InvalidInput).await,
    }
}

async fn account_request(
    accounts: &AccountService,
    request: &HttpRequest,
    path: &str,
    now: u64,
) -> Result<zeroize::Zeroizing<Vec<u8>>, AccountError> {
    match (request.method, path) {
        (Method::Post, REGISTER_PATH) => {
            return encode(&accounts.register(decode(&request.body)?, now).await?);
        }
        (Method::Post, LOGIN_PATH) => {
            return encode(&accounts.login(decode(&request.body)?, now).await?);
        }
        _ => (),
    }
    let token = request.bearer.as_ref().ok_or(AccountError {
        code: AccountErrorCode::Unauthorized,
    })?;
    let principal = accounts.authenticate(token.expose(), now).await?;
    match (request.method, path) {
        (Method::Get, SESSION_PATH | CREDENTIALS_PATH) => {
            encode(&accounts.overview(&principal, now).await?)
        }
        (Method::Post, LOGOUT_PATH) => {
            accounts.logout(&principal).await?;
            encode(&())
        }
        (Method::Post, CREDENTIALS_PATH) => encode(
            &accounts
                .bind_credential(&principal, decode(&request.body)?, now)
                .await?,
        ),
        (Method::Post, VERIFY_PATH) => {
            let target: CredentialRequest = decode(&request.body)?;
            encode(
                &accounts
                    .verify_credential(&principal, &target.credential_id, now)
                    .await?,
            )
        }
        (Method::Post, DELETE_PATH) => {
            accounts
                .delete_credential(&principal, decode(&request.body)?, now)
                .await?;
            let refreshed = accounts.authenticate(token.expose(), clock()?).await?;
            encode(&accounts.overview(&refreshed, clock()?).await?)
        }
        (Method::Post, SELECT_PATH) => {
            let target: CredentialRequest = decode(&request.body)?;
            accounts
                .select_credential(&principal, &target.credential_id, now)
                .await?;
            let refreshed = accounts.authenticate(token.expose(), clock()?).await?;
            encode(&accounts.overview(&refreshed, clock()?).await?)
        }
        _ => Err(AccountError {
            code: AccountErrorCode::InvalidInput,
        }),
    }
}

async fn principal(
    accounts: Option<&AccountService>,
    token: Option<&SecretValue>,
    now: u64,
) -> Result<Option<Principal>, AccountError> {
    match token {
        None => Ok(None),
        Some(token) => accounts
            .ok_or(AccountError {
                code: AccountErrorCode::Unauthorized,
            })?
            .authenticate(token.expose(), now)
            .await
            .map(Some),
    }
}

async fn owned(
    accounts: Option<&AccountService>,
    principal: Option<&Principal>,
) -> Result<BTreeSet<String>, AccountError> {
    match (accounts, principal) {
        (Some(accounts), Some(principal)) => accounts.owned_account_ids(principal).await,
        _ => Ok(BTreeSet::new()),
    }
}

pub(crate) fn filter_snapshot(
    mut snapshot: ControlSnapshot,
    owned: &BTreeSet<String>,
) -> ControlSnapshot {
    snapshot
        .accounts
        .retain(|a| owned.contains(&a.trading_account_id));
    snapshot
        .strategies
        .retain(|s| owned.contains(&s.trading_account_id));
    let instances: BTreeSet<_> = snapshot
        .strategies
        .iter()
        .map(|s| s.instance_id.as_str())
        .collect();
    snapshot
        .copy_relations
        .retain(|r| instances.contains(r.follower_instance_id.as_str()));
    snapshot
        .ledger
        .retain(|r| instances.contains(r.instance_id.as_str()));
    snapshot
}

async fn stream_scoped_events<R>(
    stream: &mut TcpStream,
    state: &HttpState<R>,
    mut cursor: i64,
    token: Option<SecretValue>,
) where
    R: ControlRepository + 'static,
{
    let mut shutdown = state.shutdown.clone();
    let mut poll = time::interval(
        state
            .config
            .event_poll_interval
            .max(Duration::from_millis(500)),
    );
    let mut keep_alive = time::interval(state.config.event_keep_alive);
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            changed=shutdown.changed()=>if changed.is_err() || *shutdown.borrow() { return; },
            _=keep_alive.tick()=>if write_sse(stream,b": keep-alive\n\n",state.config.request_timeout).await.is_err(){return;},
            _=poll.tick()=> {
                let accounts=match &state.access{AccessMode::Accounts(a)=>Some(a.as_ref()),_=>None};
                let Ok(now)=clock() else{return;};
                let Ok(principal)=principal(accounts,token.as_ref(),now).await else{return;};
                let Ok(owned)=owned(accounts,principal.as_ref()).await else{return;};
                let events=match call(state,state.service.events(cursor,state.config.event_page_limit)).await{Ok(v)=>v,Err(_)=>return};
                for stored in events {
                    cursor=stored.sequence;
                    let event=match stored.event {
                        ControlEvent::Snapshot(snapshot)=>Some(ControlEvent::Snapshot(filter_snapshot(snapshot,&owned))),
                        ControlEvent::CommandReceipt(receipt)=>{
                            match (accounts,principal.as_ref()) {
                                (Some(a),Some(p)) if a.owns_receipt(p,&receipt.request_id).await.unwrap_or(false)=>Some(ControlEvent::CommandReceipt(receipt)),
                                _=>None,
                            }
                        }
                        ControlEvent::Notice{..}=>None,
                    };
                    let frame=match event {
                        Some(event)=>match serde_json::to_string(&event){Ok(body)=>format!("id: {cursor}\nevent: control\ndata: {body}\n\n"),Err(_)=>return},
                        None=>format!("id: {cursor}\n: scoped\n\n"),
                    };
                    if write_sse(stream,frame.as_bytes(),state.config.request_timeout).await.is_err(){return;}
                }
            }
        }
    }
}

fn empty_snapshot(now: u64) -> ControlSnapshot {
    ControlSnapshot {
        schema_version: venue_control_protocol::CONTROL_SCHEMA_VERSION,
        generated_ms: now,
        connection: venue_control_protocol::ConnectionState::Offline,
        accounts: Vec::new(),
        strategies: Vec::new(),
        copy_relations: Vec::new(),
        markets: Vec::new(),
        ledger: Vec::new(),
    }
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, AccountError> {
    serde_json::from_slice(bytes).map_err(|_| AccountError {
        code: AccountErrorCode::InvalidInput,
    })
}
fn encode<T: Serialize>(value: &T) -> Result<zeroize::Zeroizing<Vec<u8>>, AccountError> {
    serde_json::to_vec(value)
        .map(zeroize::Zeroizing::new)
        .map_err(|_| AccountError {
            code: AccountErrorCode::Unavailable,
        })
}
fn clock() -> Result<u64, AccountError> {
    now_ms().map_err(|_| AccountError {
        code: AccountErrorCode::Unavailable,
    })
}

async fn account_error(stream: &mut TcpStream, code: AccountErrorCode) -> Result<(), ()> {
    let status = match code {
        AccountErrorCode::InvalidInput => "400 Bad Request",
        AccountErrorCode::InvalidLogin | AccountErrorCode::Unauthorized => "401 Unauthorized",
        AccountErrorCode::Forbidden => "403 Forbidden",
        AccountErrorCode::NotFound => "404 Not Found",
        AccountErrorCode::RateLimited => "429 Too Many Requests",
        AccountErrorCode::Unavailable => "503 Service Unavailable",
        _ => "409 Conflict",
    };
    let body = serde_json::to_vec(&AccountErrorResponse { code }).map_err(|_| ())?;
    write_response(stream, status, "application/json", "close", &body)
        .await
        .map_err(|_| ())
}
