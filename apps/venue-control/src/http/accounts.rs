use super::*;
use crate::accounts::{AccountError, AccountService, Principal};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::BTreeSet;
use venue_control_protocol::{
    ControlSnapshot, CopyRelationUpsertRequest, ExecutionFactsSnapshot,
    accounts::*,
    grid::{
        GRID_INSTANCES_PATH, GRID_LIFECYCLE_PATH, GridConfigUpdateRequest,
        GridInstanceCreateRequest, GridLifecycleRequest,
    },
    kol::{
        FollowLifecycleRequest, FollowSettingsUpsertRequest, KOL_EXECUTION_STATUS_PATH,
        KOL_FOLLOW_LIFECYCLE_PATH, KOL_FOLLOW_SETTINGS_PATH, KOL_PROFILE_PATH,
        KOL_TERMINAL_ACCOUNT_PATH, KOL_TERMINAL_CANCEL_PATH, KOL_TERMINAL_ORDER_PATH,
        KolProfileUpdateRequest, TerminalCancelRequest, TerminalOrderRequest,
        TerminalProjectionRequest,
    },
};

pub(super) async fn dispatch_authenticated<R>(
    stream: &mut TcpStream,
    state: &HttpState<R>,
    request: HttpRequest,
) -> Result<(), ()>
where
    R: ControlRepository + AccountDeliveryRepository + CopyRelationRepository + 'static,
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
    if let Some(invite_code) = path.strip_prefix("/v2/public/kol/invites/") {
        if request.method != Method::Get || query.is_some() {
            return account_error(stream, AccountErrorCode::InvalidInput).await;
        }
        let Some(accounts) = accounts else {
            return account_error(stream, AccountErrorCode::Unavailable).await;
        };
        return match accounts.resolve_invite(invite_code, now).await {
            Ok(resolution) => match encode(&resolution) {
                Ok(body) => write_response(stream, "200 OK", "application/json", "close", &body)
                    .await
                    .map_err(|_| ()),
                Err(error) => account_error(stream, error.code).await,
            },
            Err(error) => account_error(stream, error.code).await,
        };
    }
    if path.starts_with("/v2/account/")
        || matches!(
            path,
            KOL_PROFILE_PATH
                | KOL_FOLLOW_SETTINGS_PATH
                | KOL_FOLLOW_LIFECYCLE_PATH
                | KOL_TERMINAL_ACCOUNT_PATH
                | KOL_TERMINAL_ORDER_PATH
                | KOL_TERMINAL_CANCEL_PATH
                | KOL_EXECUTION_STATUS_PATH
                | GRID_INSTANCES_PATH
                | GRID_LIFECYCLE_PATH
        )
    {
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
            let (scope, cursor) = match event_stream_scope(query, request.last_event_id) {
                Ok(scope) => scope,
                Err(_) => return account_error(stream, AccountErrorCode::InvalidInput).await,
            };
            let (Some(accounts), Some(principal)) = (accounts, principal.as_ref()) else {
                return account_error(stream, AccountErrorCode::Unauthorized).await;
            };
            let owned = match accounts.owned_account_ids(principal).await {
                Ok(ids) => ids,
                Err(error) => return account_error(stream, error.code).await,
            };
            if !owned.contains(&scope.trading_account_id) {
                return account_error(stream, AccountErrorCode::Forbidden).await;
            }
            let Some(token) = request.bearer else {
                return account_error(stream, AccountErrorCode::Unauthorized).await;
            };
            write_sse_headers(stream).await.map_err(|_| ())?;
            stream_account_events(stream, state, accounts, token, scope, cursor).await;
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
        (Method::Get, EXECUTION_FACTS_PATH) if query.is_none() => {
            let (Some(accounts), Some(principal)) = (accounts, principal.as_ref()) else {
                return account_error(stream, AccountErrorCode::Unauthorized).await;
            };
            let owned = match accounts.owned_account_ids(principal).await {
                Ok(ids) => ids,
                Err(error) => return account_error(stream, error.code).await,
            };
            let facts = match state.service.execution_facts().await {
                Ok(facts) => facts,
                Err(ServiceError::SnapshotUnavailable) => {
                    return account_error(stream, AccountErrorCode::NotFound).await;
                }
                Err(_) => return account_error(stream, AccountErrorCode::Unavailable).await,
            };
            let body =
                serde_json::to_vec(&filter_execution_facts(facts, &owned)).map_err(|_| ())?;
            write_response(stream, "200 OK", "application/json", "close", &body)
                .await
                .map_err(|_| ())
        }
        (Method::Get, COPY_RELATION_PATH) if query.is_none() => {
            let (Some(accounts), Some(principal)) = (accounts, principal.as_ref()) else {
                return account_error(stream, AccountErrorCode::Unauthorized).await;
            };
            let owned = match accounts.owned_account_ids(principal).await {
                Ok(ids) => ids,
                Err(error) => return account_error(stream, error.code).await,
            };
            let relations = match state.service.copy_relations().await {
                Ok(relations) => relations,
                Err(_) => return account_error(stream, AccountErrorCode::Unavailable).await,
            };
            let relations: Vec<_> = relations
                .into_iter()
                .filter(|record| owns_relation(&owned, &record.relation))
                .collect();
            let body = serde_json::to_vec(&relations).map_err(|_| ())?;
            write_response(stream, "200 OK", "application/json", "close", &body)
                .await
                .map_err(|_| ())
        }
        (Method::Get, COPY_RELATION_CANDIDATES_PATH) if query.is_none() => {
            let (Some(accounts), Some(principal)) = (accounts, principal.as_ref()) else {
                return account_error(stream, AccountErrorCode::Unauthorized).await;
            };
            let owned = match accounts.owned_account_ids(principal).await {
                Ok(ids) => ids,
                Err(error) => return account_error(stream, error.code).await,
            };
            let candidates = match state.service.copy_relation_candidates().await {
                Ok(candidates) => candidates,
                Err(_) => return account_error(stream, AccountErrorCode::Unavailable).await,
            };
            let candidates: Vec<_> = candidates
                .into_iter()
                .filter(|candidate| owned.contains(&candidate.binding.trading_account_id))
                .collect();
            let body = serde_json::to_vec(&candidates).map_err(|_| ())?;
            write_response(stream, "200 OK", "application/json", "close", &body)
                .await
                .map_err(|_| ())
        }
        (Method::Post, COPY_RELATION_PATH) if query.is_none() && request.json_content => {
            let (Some(accounts), Some(principal)) = (accounts, principal.as_ref()) else {
                return account_error(stream, AccountErrorCode::Unauthorized).await;
            };
            let relation: CopyRelationUpsertRequest = match decode(&request.body) {
                Ok(value) => value,
                Err(error) => return account_error(stream, error.code).await,
            };
            let owned = match accounts.owned_account_ids(principal).await {
                Ok(ids) => ids,
                Err(error) => return account_error(stream, error.code).await,
            };
            if !owns_relation(&owned, &relation.relation) {
                return account_error(stream, AccountErrorCode::Forbidden).await;
            }
            let receipt = match state.service.upsert_copy_relation(&relation, now).await {
                Ok(receipt) => receipt,
                Err(ServiceError::CopyRelationRepository(
                    CopyRelationRepositoryError::Conflict,
                )) => {
                    return account_error(stream, AccountErrorCode::Conflict).await;
                }
                Err(ServiceError::Protocol(_)) => {
                    return account_error(stream, AccountErrorCode::InvalidInput).await;
                }
                Err(_) => return account_error(stream, AccountErrorCode::Unavailable).await,
            };
            let body = serde_json::to_vec(&receipt).map_err(|_| ())?;
            write_response(stream, "200 OK", "application/json", "close", &body)
                .await
                .map_err(|_| ())
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
            return encode(
                &accounts
                    .register_with_invite(decode::<RegisterRequest>(&request.body)?, now)
                    .await?,
            );
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
        (Method::Get, KOL_PROFILE_PATH) => encode(&accounts.own_kol_profile(&principal).await?),
        (Method::Get, KOL_FOLLOW_SETTINGS_PATH) => {
            encode(&accounts.follow_relation(&principal).await?)
        }
        (Method::Post, KOL_PROFILE_PATH) => encode(
            &accounts
                .update_own_kol_profile(
                    &principal,
                    decode::<KolProfileUpdateRequest>(&request.body)?,
                    now,
                )
                .await?,
        ),
        (Method::Post, KOL_FOLLOW_SETTINGS_PATH) => encode(
            &accounts
                .upsert_follow_settings(
                    &principal,
                    decode::<FollowSettingsUpsertRequest>(&request.body)?,
                    now,
                )
                .await?,
        ),
        (Method::Post, KOL_FOLLOW_LIFECYCLE_PATH) => encode(
            &accounts
                .request_follow_lifecycle(
                    &principal,
                    decode::<FollowLifecycleRequest>(&request.body)?,
                    now,
                )
                .await?,
        ),
        (Method::Post, KOL_TERMINAL_ACCOUNT_PATH) => encode(
            &accounts
                .terminal_account_projection(
                    &principal,
                    decode::<TerminalProjectionRequest>(&request.body)?,
                    now,
                )
                .await?,
        ),
        (Method::Post, KOL_TERMINAL_ORDER_PATH) => encode(
            &accounts
                .enqueue_terminal_order(
                    &principal,
                    decode::<TerminalOrderRequest>(&request.body)?,
                    now,
                )
                .await?,
        ),
        (Method::Post, KOL_TERMINAL_CANCEL_PATH) => encode(
            &accounts
                .enqueue_terminal_cancel(
                    &principal,
                    decode::<TerminalCancelRequest>(&request.body)?,
                    now,
                )
                .await?,
        ),
        (Method::Get, KOL_EXECUTION_STATUS_PATH) => {
            encode(&accounts.terminal_executions(&principal).await?)
        }
        (Method::Get, GRID_INSTANCES_PATH) => encode(&accounts.grid_instances(&principal).await?),
        (Method::Post, GRID_INSTANCES_PATH) => {
            let value: serde_json::Value = decode(&request.body)?;
            if value.get("instance_id").is_some() {
                encode(
                    &accounts
                        .update_grid_config(
                            &principal,
                            serde_json::from_value::<GridConfigUpdateRequest>(value).map_err(
                                |_| AccountError {
                                    code: AccountErrorCode::InvalidInput,
                                },
                            )?,
                            now,
                        )
                        .await?,
                )
            } else {
                encode(
                    &accounts
                        .create_grid_instance(
                            &principal,
                            serde_json::from_value::<GridInstanceCreateRequest>(value).map_err(
                                |_| AccountError {
                                    code: AccountErrorCode::InvalidInput,
                                },
                            )?,
                            now,
                        )
                        .await?,
                )
            }
        }
        (Method::Post, GRID_LIFECYCLE_PATH) => encode(
            &accounts
                .request_grid_lifecycle(
                    &principal,
                    decode::<GridLifecycleRequest>(&request.body)?,
                    now,
                )
                .await?,
        ),
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
    // Legacy ledger/relation rows carry instance IDs but no account scope. Build their allow-list
    // from the unfiltered snapshot so a duplicate ID in another account fails closed.
    let uniquely_owned_instances = unique_owned_instance_ids(&snapshot.strategies, owned);
    snapshot
        .accounts
        .retain(|a| owned.contains(&a.trading_account_id));
    snapshot
        .strategies
        .retain(|s| owned.contains(&s.trading_account_id));
    snapshot.copy_relations.retain(|r| {
        uniquely_owned_instances.contains(&r.leader_id)
            && uniquely_owned_instances.contains(&r.follower_instance_id)
    });
    snapshot
        .ledger
        .retain(|r| uniquely_owned_instances.contains(&r.instance_id));
    snapshot
}

fn unique_owned_instance_ids(
    strategies: &[venue_control_protocol::StrategySummary],
    owned: &BTreeSet<String>,
) -> BTreeSet<String> {
    strategies
        .iter()
        .filter(|strategy| owned.contains(&strategy.trading_account_id))
        .filter(|strategy| {
            strategies
                .iter()
                .filter(|other| other.instance_id == strategy.instance_id)
                .count()
                == 1
        })
        .map(|strategy| strategy.instance_id.clone())
        .collect()
}

fn owns_relation(
    owned: &BTreeSet<String>,
    relation: &venue_control_protocol::CopyRelationConfig,
) -> bool {
    owned.contains(&relation.leader.trading_account_id)
        && owned.contains(&relation.follower.trading_account_id)
}

fn filter_execution_facts(
    mut facts: ExecutionFactsSnapshot,
    owned: &BTreeSet<String>,
) -> ExecutionFactsSnapshot {
    facts
        .orders
        .retain(|fact| owned.contains(&fact.binding.trading_account_id));
    facts
        .positions
        .retain(|fact| owned.contains(&fact.binding.trading_account_id));
    facts
        .fills
        .retain(|fact| owned.contains(&fact.binding.trading_account_id));
    facts
        .reconciliation
        .retain(|fact| owned.contains(&fact.binding.trading_account_id));
    facts
        .copy_ledger
        .retain(|fact| owned.contains(&fact.binding.trading_account_id));
    facts
        .drift
        .retain(|fact| owned.contains(&fact.binding.trading_account_id));
    facts
        .execution
        .retain(|fact| owned.contains(&fact.binding.trading_account_id));
    facts
        .risk
        .retain(|fact| owned.contains(&fact.trading_account_id));
    facts
        .health
        .retain(|fact| owned.contains(&fact.trading_account_id));
    facts
}

async fn stream_account_events<R>(
    stream: &mut TcpStream,
    state: &HttpState<R>,
    accounts: &AccountService,
    token: SecretValue,
    scope: UiAccountScope,
    mut cursor: i64,
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
            changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
            _ = keep_alive.tick() => if write_sse(stream, b": keep-alive\n\n", state.config.request_timeout).await.is_err() { return; },
            _ = poll.tick() => {
                let Ok(now) = clock() else { return; };
                let Ok(principal) = accounts.authenticate(token.expose(), now).await else { return; };
                let Ok(owned) = accounts.owned_account_ids(&principal).await else { return; };
                if !owned.contains(&scope.trading_account_id) { return; }
                let events = match call(state, state.service.events(&scope, cursor, state.config.event_page_limit)).await {
                    Ok(events) => events,
                    Err(_) => return,
                };
                for event in events {
                    let payload = match serde_json::to_string(&event.event) {
                        Ok(payload) if payload.len() <= state.config.request_body_limit => payload,
                        _ => return,
                    };
                    let frame = format!("id: {}\nevent: control\ndata: {payload}\n\n", event.sequence);
                    if write_sse(stream, frame.as_bytes(), state.config.request_timeout).await.is_err() { return; }
                    cursor = event.sequence;
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

#[cfg(test)]
mod scope_tests {
    use super::*;
    use rust_decimal::Decimal;
    use venue_control_protocol::{CopyRelationSummary, CopyStatus, LedgerEntry};

    #[test]
    fn legacy_rows_require_unique_owned_leader_and_follower_instances()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut snapshot = super::super::tests::snapshot()?;
        let owned = BTreeSet::from([snapshot.strategies[0].trading_account_id.clone()]);
        let mut leader = snapshot.strategies[0].clone();
        leader.instance_id = "leader-btc".to_owned();
        snapshot.strategies.push(leader);
        snapshot.copy_relations.push(CopyRelationSummary {
            relation_id: "00000000-0000-4000-8000-000000000010".to_owned(),
            revision: 1,
            leader_id: "leader-btc".to_owned(),
            follower_instance_id: "grid-btc".to_owned(),
            symbol: "BTC/USDT".parse()?,
            target_exposure: Decimal::ZERO,
            actual_exposure: Decimal::ZERO,
            drift: Decimal::ZERO,
            status: CopyStatus::Tracking,
            last_applied_job: None,
        });
        snapshot.ledger.push(LedgerEntry {
            receipt_id: "legacy-receipt".to_owned(),
            instance_id: "grid-btc".to_owned(),
            occurred_ms: 100,
            action: "copy".to_owned(),
            state: "reconciled".to_owned(),
            detail: "legacy instance-only row".to_owned(),
        });
        let filtered = filter_snapshot(snapshot.clone(), &owned);
        assert_eq!(filtered.copy_relations.len(), 1);
        assert_eq!(filtered.ledger.len(), 1);

        let mut ambiguous = snapshot;
        let mut foreign = ambiguous.strategies[0].clone();
        foreign.trading_account_id = "00000000-0000-4000-8000-000000000099".to_owned();
        ambiguous.strategies.push(foreign);
        let filtered = filter_snapshot(ambiguous, &owned);
        assert_eq!(filtered.strategies.len(), 2);
        assert!(filtered.copy_relations.is_empty());
        assert!(filtered.ledger.is_empty());
        Ok(())
    }

    #[test]
    fn legacy_relation_without_an_owned_leader_is_excluded()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut snapshot = super::super::tests::snapshot()?;
        let owned = BTreeSet::from([snapshot.strategies[0].trading_account_id.clone()]);
        snapshot.copy_relations.push(CopyRelationSummary {
            relation_id: "00000000-0000-4000-8000-000000000011".to_owned(),
            revision: 1,
            leader_id: "unscoped-leader".to_owned(),
            follower_instance_id: "grid-btc".to_owned(),
            symbol: "BTC/USDT".parse()?,
            target_exposure: Decimal::ZERO,
            actual_exposure: Decimal::ZERO,
            drift: Decimal::ZERO,
            status: CopyStatus::Tracking,
            last_applied_job: None,
        });
        assert!(filter_snapshot(snapshot, &owned).copy_relations.is_empty());
        Ok(())
    }
}
