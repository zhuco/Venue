use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{ExecutionCommand, NativeOrderFamily, OrderState};
use venue_execution::{
    AccountDispatchPermit, AccountGatewayResult, AccountPhysicalGateway, AccountRecoveryOutcome,
    AccountRecoveryReport, AccountRecoveryRequest,
};
use venue_gateway_api::GatewayBinding;

use crate::private::diagnose_position_page;
use crate::transport::unix_ms;
use crate::{
    BybitAccountIdentity, BybitCancelIntent, BybitClosedOrderReadback, BybitCredentials,
    BybitGatewayBinding, BybitHistoryWindow, BybitHttpTransport, BybitLinearInstrumentRules,
    BybitOpenOrderPage, BybitOrderEvidencePage, BybitOrderKind, BybitOrderLookup, BybitPlaceIntent,
    BybitPositionReadback, BybitPreparedPrivateRequest, BybitPrivateSource, BybitTimeInForce,
    BybitTransportError, BybitTransportLimits, parse_account_identity, parse_api_key_evidence,
    parse_linear_instrument, parse_open_order_page, parse_order_history_page, parse_position_page,
    prepare_cancel_request, prepare_place_request, prepare_private_request,
};

const EXACT_READBACK_MAX_PAGES: u32 = 32;
const HISTORY_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

/// Production Bybit adapter for the lightweight account host. It can only POST while consuming
/// the host's linear permit; callers cannot obtain or clone that permit from this crate.
pub struct BybitAccountGateway {
    runtime: Runtime,
    binding: BybitGatewayBinding,
    credentials: BybitCredentials,
    transport: BybitHttpTransport,
    identity: BybitAccountIdentity,
    positions: BybitPositionReadback,
    rules: BybitLinearInstrumentRules,
    next_attempt_id: u64,
}

impl BybitAccountGateway {
    /// Performs public-rule and signed account/permission/position preflight against production.
    /// Construction does not issue any mutation.
    pub fn connect_from_environment(
        binding: GatewayBinding,
        limits: BybitTransportLimits,
    ) -> Result<Self, BybitAccountGatewayError> {
        let credentials = BybitCredentials::from_environment()
            .map_err(|_| BybitAccountGatewayError::Credentials)?;
        Self::connect(binding, credentials, limits)
    }

    fn connect(
        binding: GatewayBinding,
        credentials: BybitCredentials,
        limits: BybitTransportLimits,
    ) -> Result<Self, BybitAccountGatewayError> {
        let binding =
            BybitGatewayBinding::new(binding).map_err(|_| BybitAccountGatewayError::Binding)?;
        let generation = unix_ms().map_err(BybitAccountGatewayError::Transport)?;
        let transport = BybitHttpTransport::new(&binding, generation, limits)
            .map_err(BybitAccountGatewayError::Transport)?;
        Self::connect_with_transport(binding, credentials, transport, generation)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn connect_with_endpoint(
        binding: GatewayBinding,
        credentials: BybitCredentials,
        limits: BybitTransportLimits,
        endpoint: String,
        generation: u64,
    ) -> Result<Self, BybitAccountGatewayError> {
        let binding =
            BybitGatewayBinding::new(binding).map_err(|_| BybitAccountGatewayError::Binding)?;
        let transport = BybitHttpTransport::with_endpoint(&binding, generation, endpoint, limits)
            .map_err(BybitAccountGatewayError::Transport)?;
        Self::connect_with_transport(binding, credentials, transport, generation)
    }

    fn connect_with_transport(
        binding: BybitGatewayBinding,
        credentials: BybitCredentials,
        transport: BybitHttpTransport,
        generation: u64,
    ) -> Result<Self, BybitAccountGatewayError> {
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| BybitAccountGatewayError::Runtime)?;
        let (rules, identity, positions) =
            runtime.block_on(bootstrap(&binding, &credentials, &transport, generation, 1))?;
        Ok(Self {
            runtime,
            binding,
            credentials,
            transport,
            identity,
            positions,
            rules,
            next_attempt_id: 2,
        })
    }

    fn take_attempt_id(&mut self) -> Result<u64, BybitAccountGatewayError> {
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(BybitAccountGatewayError::Attempt)?;
        Ok(attempt_id)
    }

    fn refresh_private(&mut self) -> Result<(), BybitAccountGatewayError> {
        let attempt_id = self.take_attempt_id()?;
        let generation = self.rules.instrument.generation;
        let (identity, positions) = self.runtime.block_on(fetch_private_state(
            &self.binding,
            &self.credentials,
            &self.transport,
            generation,
            attempt_id,
        ))?;
        self.identity = identity;
        self.positions = positions;
        Ok(())
    }

    fn dispatch_permit(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        if permit.binding() != self.binding.gateway_binding() {
            return AccountGatewayResult::Rejected {
                reason: "bybit_permit_binding".to_owned(),
            };
        }
        if self.refresh_private().is_err() {
            return AccountGatewayResult::Rejected {
                reason: "bybit_preflight_failed".to_owned(),
            };
        }
        let now_ms = match unix_ms() {
            Ok(value) => value,
            Err(_) => {
                return AccountGatewayResult::Rejected {
                    reason: "bybit_clock".to_owned(),
                };
            }
        };
        let request = match permit.command() {
            ExecutionCommand::PlaceLimit(command) => {
                if !command.reduce_only
                    && self
                        .positions
                        .positions
                        .iter()
                        .any(|position| !position.position.quantity.is_zero())
                {
                    return AccountGatewayResult::Rejected {
                        reason: "bybit_existing_position".to_owned(),
                    };
                }
                prepare_place_request(
                    &self.binding,
                    &self.identity,
                    &self.rules,
                    &BybitPlaceIntent {
                        client_order_id: command.client_order_id.as_str().to_owned(),
                        side: command.side,
                        position_side: command.position_side,
                        kind: BybitOrderKind::Limit,
                        quantity: command.quantity,
                        limit_price: Some(command.limit_price),
                        time_in_force: BybitTimeInForce::PostOnly,
                        reduce_only: command.reduce_only,
                    },
                    now_ms,
                    None,
                )
            }
            ExecutionCommand::Cancel(command) => prepare_cancel_request(
                &self.binding,
                &self.identity,
                &self.rules,
                &BybitCancelIntent {
                    order_id: None,
                    client_order_id: Some(command.target_client_order_id.as_str().to_owned()),
                },
            ),
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                return AccountGatewayResult::Rejected {
                    reason: "bybit_initial_profile_unsupported_command".to_owned(),
                };
            }
        };
        let request = match request {
            Ok(value) => value,
            Err(_) => {
                return AccountGatewayResult::Rejected {
                    reason: "bybit_intent_rejected".to_owned(),
                };
            }
        };
        match self.runtime.block_on(self.transport.execute_order(
            &self.binding,
            &self.credentials,
            &request,
            now_ms,
        )) {
            Ok(ack) => ack
                .order_id
                .or(ack.client_order_id)
                .map_or(AccountGatewayResult::Unknown, |venue_order_id| {
                    AccountGatewayResult::Accepted { venue_order_id }
                }),
            Err(BybitTransportError::Rejected) => AccountGatewayResult::Rejected {
                reason: "bybit_venue_rejected".to_owned(),
            },
            Err(
                BybitTransportError::Binding
                | BybitTransportError::Signing
                | BybitTransportError::BodyTooLarge
                | BybitTransportError::Limits,
            ) => AccountGatewayResult::Rejected {
                reason: "bybit_pre_send_rejected".to_owned(),
            },
            Err(_) => AccountGatewayResult::Unknown,
        }
    }
}

impl AccountPhysicalGateway for BybitAccountGateway {
    type Error = BybitAccountGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.binding.gateway_binding()
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        if request.binding() != self.binding.gateway_binding() {
            return Err(BybitAccountGatewayError::Binding);
        }
        self.refresh_private()?;
        let observed_at_ms = unix_ms().map_err(BybitAccountGatewayError::Transport)?;
        let mut outcomes = Vec::with_capacity(request.unresolved().len());
        for command in request.unresolved() {
            let lookup_id = match command {
                ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
                _ => command
                    .native_client_id()
                    .ok_or(BybitAccountGatewayError::Readback)?
                    .as_str(),
            };
            let lookup = BybitOrderLookup::by_client_order_id(lookup_id.to_owned())
                .map_err(|_| BybitAccountGatewayError::Readback)?;
            let attempt_id = self.take_attempt_id()?;
            let readback = self.runtime.block_on(fetch_exact_readback(
                &self.binding,
                &self.credentials,
                &self.transport,
                self.rules.instrument.generation,
                attempt_id,
                lookup,
                observed_at_ms,
            ))?;
            let outcome = recovery_outcome(command, &readback)?;
            outcomes.push(outcome);
        }
        AccountRecoveryReport::new(
            self.binding.gateway_binding().clone(),
            observed_at_ms,
            outcomes,
        )
        .map_err(|_| BybitAccountGatewayError::Readback)
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatch_permit(permit)
    }
}

async fn bootstrap(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
) -> Result<
    (
        BybitLinearInstrumentRules,
        BybitAccountIdentity,
        BybitPositionReadback,
    ),
    BybitAccountGatewayError,
> {
    let raw = transport
        .fetch_linear_instrument(binding)
        .await
        .map_err(BybitAccountGatewayError::PublicTransport)?;
    let rules =
        parse_linear_instrument(binding, raw).map_err(|_| BybitAccountGatewayError::Instrument)?;
    let api_key = fetch_one(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::ApiKeyInfo,
        0,
        None,
        None,
    )
    .await?;
    let api_key = parse_api_key_evidence(binding, credentials, &api_key)
        .map_err(|_| BybitAccountGatewayError::Permissions)?;
    if api_key.read_only
        || !api_key.contract_order
        || !api_key.contract_position
        || api_key.withdraw
    {
        return Err(BybitAccountGatewayError::Permissions);
    }
    let (identity, positions) =
        fetch_private_state(binding, credentials, transport, generation, attempt_id).await?;
    Ok((rules, identity, positions))
}

async fn fetch_private_state(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
) -> Result<(BybitAccountIdentity, BybitPositionReadback), BybitAccountGatewayError> {
    let account = fetch_one(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountInfo,
        0,
        None,
        None,
    )
    .await?;
    let identity = parse_account_identity(binding, &account)
        .map_err(|_| BybitAccountGatewayError::AccountIdentity)?;
    if !identity.mode.supports_unified_wallet() {
        return Err(BybitAccountGatewayError::AccountMode);
    }
    let raw = fetch_one(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::Positions,
        0,
        None,
        None,
    )
    .await?;
    if !has_both_hedge_legs(&raw)? {
        return Err(BybitAccountGatewayError::PositionMode);
    }
    let page = parse_position_page(binding, &raw).map_err(|_| {
        BybitAccountGatewayError::PositionPayload(diagnose_position_page(binding, &raw))
    })?;
    let positions = BybitPositionReadback {
        raw_pages: vec![raw.clone()],
        binding: raw.binding.clone(),
        generation: raw.generation,
        attempt_id: raw.attempt_id,
        observed_at_ms: raw.received_at_ms,
        hedge_mode: true,
        positions: page.positions,
    };
    Ok((identity, positions))
}

fn has_both_hedge_legs(
    raw: &crate::BybitRawPrivatePayload,
) -> Result<bool, BybitAccountGatewayError> {
    #[derive(Deserialize)]
    struct Envelope {
        #[serde(rename = "retCode")]
        ret_code: i64,
        result: ResultRows,
    }
    #[derive(Deserialize)]
    struct ResultRows {
        list: Vec<PositionIndex>,
    }
    #[derive(Deserialize)]
    struct PositionIndex {
        #[serde(rename = "positionIdx")]
        position_idx: u8,
    }

    let envelope: Envelope =
        serde_json::from_slice(&raw.payload).map_err(|_| BybitAccountGatewayError::Positions)?;
    if envelope.ret_code != 0 {
        return Err(BybitAccountGatewayError::Positions);
    }
    let indexes = envelope
        .result
        .list
        .into_iter()
        .map(|row| row.position_idx)
        .collect::<std::collections::BTreeSet<_>>();
    Ok(indexes == std::collections::BTreeSet::from([1, 2]))
}

#[allow(clippy::too_many_arguments)]
async fn fetch_one(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
    source: BybitPrivateSource,
    page_index: u32,
    cursor: Option<&str>,
    history_window: Option<BybitHistoryWindow>,
) -> Result<crate::BybitRawPrivatePayload, BybitAccountGatewayError> {
    let request = prepare_private_request(
        binding,
        generation,
        attempt_id,
        page_index,
        source,
        cursor,
        history_window,
        None,
    )
    .map_err(|_| BybitAccountGatewayError::Readback)?;
    execute_private(binding, credentials, transport, &request)
        .await
        .map_err(|error| match source {
            BybitPrivateSource::ApiKeyInfo => BybitAccountGatewayError::ApiKeyTransport(error),
            BybitPrivateSource::AccountInfo | BybitPrivateSource::WalletBalance => {
                BybitAccountGatewayError::AccountTransport(error)
            }
            BybitPrivateSource::Positions => BybitAccountGatewayError::PositionTransport(error),
            BybitPrivateSource::OpenOrders(_)
            | BybitPrivateSource::OrderHistory(_)
            | BybitPrivateSource::Executions => BybitAccountGatewayError::OrderTransport(error),
        })
}

async fn execute_private(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    request: &BybitPreparedPrivateRequest,
) -> Result<crate::BybitRawPrivatePayload, BybitTransportError> {
    let now_ms = unix_ms()?;
    transport
        .execute_private_read(binding, credentials, request, now_ms)
        .await
}

async fn fetch_exact_readback(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
    lookup: BybitOrderLookup,
    now_ms: u64,
) -> Result<BybitClosedOrderReadback, BybitAccountGatewayError> {
    let history_window =
        BybitHistoryWindow::new(now_ms.saturating_sub(HISTORY_WINDOW_MS).max(1), now_ms)
            .map_err(|_| BybitAccountGatewayError::Readback)?;
    let open = fetch_order_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
        None,
        &lookup,
    )
    .await?;
    let history = fetch_order_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::OrderHistory(NativeOrderFamily::UmOrder),
        Some(history_window),
        &lookup,
    )
    .await?;
    let open = open
        .iter()
        .map(|raw| parse_open_order_page(binding, raw))
        .collect::<Result<Vec<BybitOpenOrderPage>, _>>()
        .map_err(|_| BybitAccountGatewayError::Readback)?;
    let history = history
        .iter()
        .map(|raw| parse_order_history_page(binding, raw))
        .collect::<Result<Vec<BybitOrderEvidencePage>, _>>()
        .map_err(|_| BybitAccountGatewayError::Readback)?;
    BybitClosedOrderReadback::from_pages(binding, generation, &open, &history)
        .map_err(|_| BybitAccountGatewayError::Readback)
}

#[allow(clippy::too_many_arguments)]
async fn fetch_order_pages(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
    source: BybitPrivateSource,
    history_window: Option<BybitHistoryWindow>,
    lookup: &BybitOrderLookup,
) -> Result<Vec<crate::BybitRawPrivatePayload>, BybitAccountGatewayError> {
    let mut pages = Vec::new();
    let mut cursor = None;
    for page_index in 0..EXACT_READBACK_MAX_PAGES {
        let request = prepare_private_request(
            binding,
            generation,
            attempt_id,
            page_index,
            source,
            cursor.as_deref(),
            history_window.clone(),
            Some(lookup.clone()),
        )
        .map_err(|_| BybitAccountGatewayError::Readback)?;
        let raw = execute_private(binding, credentials, transport, &request)
            .await
            .map_err(BybitAccountGatewayError::OrderTransport)?;
        cursor = match source {
            BybitPrivateSource::OpenOrders(_) => {
                parse_open_order_page(binding, &raw)
                    .map_err(|_| BybitAccountGatewayError::Readback)?
                    .meta
                    .next_cursor
            }
            BybitPrivateSource::OrderHistory(_) => {
                parse_order_history_page(binding, &raw)
                    .map_err(|_| BybitAccountGatewayError::Readback)?
                    .meta
                    .next_cursor
            }
            _ => return Err(BybitAccountGatewayError::Readback),
        };
        pages.push(raw);
        if cursor.is_none() {
            return Ok(pages);
        }
    }
    Err(BybitAccountGatewayError::Readback)
}

fn recovery_outcome(
    command: &ExecutionCommand,
    readback: &BybitClosedOrderReadback,
) -> Result<AccountRecoveryOutcome, BybitAccountGatewayError> {
    let settlement = readback
        .exact_settlement()
        .map_err(|_| BybitAccountGatewayError::Readback)?;
    let Some(settlement) = settlement else {
        return Ok(AccountRecoveryOutcome::still_unknown(
            command.command_id().clone(),
        ));
    };
    if matches!(command, ExecutionCommand::Cancel(_)) {
        return Ok(if settlement.state == OrderState::Cancelled {
            AccountRecoveryOutcome::accepted(command.command_id().clone(), settlement.order_id)
        } else {
            AccountRecoveryOutcome::still_unknown(command.command_id().clone())
        });
    }
    Ok(if settlement.state == OrderState::Rejected {
        AccountRecoveryOutcome::rejected(
            command.command_id().clone(),
            "bybit_order_rejected".to_owned(),
        )
    } else {
        AccountRecoveryOutcome::accepted(command.command_id().clone(), settlement.order_id)
    })
}

#[derive(Debug, thiserror::Error)]
pub enum BybitAccountGatewayError {
    #[error("Bybit account gateway binding is invalid")]
    Binding,
    #[error("Bybit account gateway credentials are unavailable")]
    Credentials,
    #[error("Bybit account gateway runtime could not be created")]
    Runtime,
    #[error("Bybit account gateway attempt identity overflowed")]
    Attempt,
    #[error("Bybit public instrument rules are unavailable or invalid")]
    Instrument,
    #[error("Bybit API key lacks the required order and position permissions")]
    Permissions,
    #[error("Bybit signed account identity response is invalid")]
    AccountIdentity,
    #[error("Bybit account is not UTA2/UTA2 Pro")]
    AccountMode,
    #[error("Bybit DOGE position response does not prove both hedge legs")]
    Positions,
    #[error("Bybit DOGE position payload failed validation at {0}")]
    PositionPayload(&'static str),
    #[error("Bybit DOGE contract is in one-way mode; hedge mode is required")]
    PositionMode,
    #[error("Bybit exact signed order readback failed")]
    Readback,
    #[error("Bybit transport setup failed")]
    Transport(#[source] BybitTransportError),
    #[error("Bybit public instrument request failed")]
    PublicTransport(#[source] BybitTransportError),
    #[error("Bybit signed API-key request failed")]
    ApiKeyTransport(#[source] BybitTransportError),
    #[error("Bybit signed account request failed")]
    AccountTransport(#[source] BybitTransportError),
    #[error("Bybit signed position request failed")]
    PositionTransport(#[source] BybitTransportError),
    #[error("Bybit signed order readback request failed")]
    OrderTransport(#[source] BybitTransportError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_profile_is_bounded() {
        assert_eq!(EXACT_READBACK_MAX_PAGES, 32);
        assert_eq!(
            HISTORY_WINDOW_MS,
            std::time::Duration::from_secs(7 * 24 * 60 * 60).as_millis() as u64
        );
    }
}
