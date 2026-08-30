use std::time::{Duration, SystemTime};

use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{ExecutionCommand, OrderState};
use venue_execution::{
    AccountDispatchPermit, AccountGatewayResult, AccountPhysicalGateway, AccountRecoveryOutcome,
    AccountRecoveryReport, AccountRecoveryRequest,
};
use venue_gateway_api::GatewayBinding;

use crate::execution::{
    OkxPlaceIntent, build_host_cancel_request, build_host_order_lookup_request,
    build_place_request, parse_host_cancel_ack, parse_host_order_lookup, parse_place_ack,
};
use crate::recovery_collector::okx_timestamp;
use crate::{
    OkxAccountProfile, OkxConfig, OkxCredentials, OkxError, OkxHttpTransport, OkxInstrument,
    OkxPositionMode, OkxPrivateReadScope, OkxTimedPosition, OkxTradeMode, OkxTransportError,
    build_account_config_request, build_positions_request, parse_account_profile, parse_instrument,
    parse_positions,
};

/// Production OKX adapter for the lightweight account host. Base quantities remain canonical in
/// the WAL; `build_place_request` converts them to contracts using ctVal × ctMult exactly once.
pub struct OkxAccountGateway {
    runtime: Runtime,
    config: OkxConfig,
    credentials: OkxCredentials,
    transport: OkxHttpTransport,
    instrument: OkxInstrument,
    profile: OkxAccountProfile,
    positions: Vec<OkxTimedPosition>,
    trade_mode: OkxTradeMode,
    next_attempt_id: u64,
}

impl OkxAccountGateway {
    /// Performs a public instrument read plus signed account-mode, permission, and position reads.
    /// It never issues an order mutation during construction.
    pub fn connect_from_environment(
        binding: GatewayBinding,
        trade_mode: OkxTradeMode,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, OkxAccountGatewayError> {
        let credentials =
            OkxCredentials::from_environment().map_err(|_| OkxAccountGatewayError::Credentials)?;
        Self::connect(
            binding,
            credentials,
            trade_mode,
            operation_timeout,
            max_body_bytes,
        )
    }

    fn connect(
        binding: GatewayBinding,
        credentials: OkxCredentials,
        trade_mode: OkxTradeMode,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, OkxAccountGatewayError> {
        let config =
            OkxConfig::for_binding(binding).map_err(|_| OkxAccountGatewayError::Binding)?;
        let transport = OkxHttpTransport::new(config.clone(), operation_timeout, max_body_bytes)
            .map_err(OkxAccountGatewayError::Transport)?;
        Self::connect_with_transport(config, credentials, transport, trade_mode)
    }

    fn connect_with_transport(
        config: OkxConfig,
        credentials: OkxCredentials,
        transport: OkxHttpTransport,
        trade_mode: OkxTradeMode,
    ) -> Result<Self, OkxAccountGatewayError> {
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| OkxAccountGatewayError::Runtime)?;
        let generation = unix_ms()?;
        let response = runtime
            .block_on(transport.fetch_instrument(generation))
            .map_err(OkxAccountGatewayError::Transport)?;
        let instrument = parse_instrument(&response.body, &config, generation)
            .map_err(|_| OkxAccountGatewayError::Instrument)?;
        let (profile, positions) = runtime.block_on(fetch_private_state(
            &config,
            &credentials,
            &transport,
            &instrument,
            trade_mode,
            1,
        ))?;
        Ok(Self {
            runtime,
            config,
            credentials,
            transport,
            instrument,
            profile,
            positions,
            trade_mode,
            next_attempt_id: 2,
        })
    }

    fn take_attempt_id(&mut self) -> Result<u64, OkxAccountGatewayError> {
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(OkxAccountGatewayError::Attempt)?;
        Ok(attempt_id)
    }

    fn refresh_private(&mut self) -> Result<(), OkxAccountGatewayError> {
        let attempt_id = self.take_attempt_id()?;
        let (profile, positions) = self.runtime.block_on(fetch_private_state(
            &self.config,
            &self.credentials,
            &self.transport,
            &self.instrument,
            self.trade_mode,
            attempt_id,
        ))?;
        self.profile = profile;
        self.positions = positions;
        Ok(())
    }

    fn dispatch_permit(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        if permit.binding() != self.config.gateway_binding() {
            return rejected("okx_permit_binding");
        }
        if self.refresh_private().is_err() {
            return rejected("okx_preflight_failed");
        }
        let timestamp = match okx_timestamp(SystemTime::now()) {
            Ok(value) => value,
            Err(_) => return rejected("okx_clock"),
        };
        match permit.command() {
            ExecutionCommand::PlaceLimit(command) => {
                if !command.reduce_only
                    && self
                        .positions
                        .iter()
                        .any(|position| !position.position.quantity.is_zero())
                {
                    return rejected("okx_existing_position");
                }
                let request = match build_place_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    OkxPlaceIntent::Limit(command),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_intent_rejected"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_place_ack(response.clone(), &request) {
                        Ok(accepted) => AccountGatewayResult::Accepted {
                            venue_order_id: accepted.order_id().to_owned(),
                        },
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::Cancel(command) => {
                let request = match build_host_cancel_request(
                    &self.config,
                    &self.instrument,
                    &self.profile,
                    self.trade_mode,
                    command,
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("okx_cancel_intent_rejected"),
                };
                match self.runtime.block_on(self.transport.execute(
                    &self.credentials,
                    &request,
                    &timestamp,
                )) {
                    Ok(response) => match parse_host_cancel_ack(response.clone(), &request) {
                        Ok(venue_order_id) => AccountGatewayResult::Accepted { venue_order_id },
                        Err(OkxError::Rejected) => rejected_response(&response.body),
                        Err(_) => AccountGatewayResult::Unknown,
                    },
                    Err(error) => map_transport_dispatch(error),
                }
            }
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                rejected("okx_initial_profile_unsupported_command")
            }
        }
    }
}

impl AccountPhysicalGateway for OkxAccountGateway {
    type Error = OkxAccountGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.config.gateway_binding()
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        if request.binding() != self.config.gateway_binding() {
            return Err(OkxAccountGatewayError::Binding);
        }
        self.refresh_private()?;
        let observed_at_ms = unix_ms()?;
        let mut outcomes = Vec::with_capacity(request.unresolved().len());
        for command in request.unresolved() {
            let client_id = match command {
                ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
                _ => command
                    .native_client_id()
                    .ok_or(OkxAccountGatewayError::Readback)?
                    .as_str(),
            };
            let lookup = build_host_order_lookup_request(
                &self.config,
                &self.instrument,
                &self.profile,
                self.trade_mode,
                client_id,
            )
            .map_err(|_| OkxAccountGatewayError::Readback)?;
            let timestamp =
                okx_timestamp(SystemTime::now()).map_err(|_| OkxAccountGatewayError::Clock)?;
            let response = self
                .runtime
                .block_on(
                    self.transport
                        .execute(&self.credentials, &lookup, &timestamp),
                )
                .map_err(OkxAccountGatewayError::Transport)?;
            let found = parse_host_order_lookup(response, &lookup)
                .map_err(|_| OkxAccountGatewayError::Readback)?;
            outcomes.push(okx_recovery_outcome(command, found));
        }
        AccountRecoveryReport::new(
            self.config.gateway_binding().clone(),
            observed_at_ms,
            outcomes,
        )
        .map_err(|_| OkxAccountGatewayError::Readback)
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatch_permit(permit)
    }
}

async fn fetch_private_state(
    config: &OkxConfig,
    credentials: &OkxCredentials,
    transport: &OkxHttpTransport,
    instrument: &OkxInstrument,
    trade_mode: OkxTradeMode,
    attempt_id: u64,
) -> Result<(OkxAccountProfile, Vec<OkxTimedPosition>), OkxAccountGatewayError> {
    let scope = OkxPrivateReadScope::new(
        config,
        instrument,
        OkxPositionMode::LongShort,
        trade_mode,
        attempt_id,
    )
    .map_err(|_| OkxAccountGatewayError::Account)?;
    let account_request =
        build_account_config_request(&scope).map_err(|_| OkxAccountGatewayError::Account)?;
    let timestamp = okx_timestamp(SystemTime::now()).map_err(|_| OkxAccountGatewayError::Clock)?;
    let account = transport
        .execute_read(credentials, &account_request, &timestamp)
        .await
        .map_err(OkxAccountGatewayError::Transport)?;
    let profile = parse_account_profile(&account.body, OkxPositionMode::LongShort)
        .map_err(|_| OkxAccountGatewayError::Account)?;
    if !profile.can_read() || !profile.can_trade() || profile.can_withdraw() {
        return Err(OkxAccountGatewayError::Permissions);
    }
    let positions_request =
        build_positions_request(&scope).map_err(|_| OkxAccountGatewayError::Account)?;
    let timestamp = okx_timestamp(SystemTime::now()).map_err(|_| OkxAccountGatewayError::Clock)?;
    let positions = transport
        .execute_read(credentials, &positions_request, &timestamp)
        .await
        .map_err(OkxAccountGatewayError::Transport)?;
    let positions = parse_positions(&positions.body, config, instrument, &profile)
        .map_err(|_| OkxAccountGatewayError::Account)?;
    Ok((profile, positions))
}

fn okx_recovery_outcome(
    command: &ExecutionCommand,
    found: Option<(String, OrderState)>,
) -> AccountRecoveryOutcome {
    let Some((venue_order_id, state)) = found else {
        return AccountRecoveryOutcome::still_unknown(command.command_id().clone());
    };
    if matches!(command, ExecutionCommand::Cancel(_)) {
        return match state {
            OrderState::Cancelled => {
                AccountRecoveryOutcome::accepted(command.command_id().clone(), venue_order_id)
            }
            OrderState::Filled | OrderState::Expired | OrderState::Rejected => {
                AccountRecoveryOutcome::rejected(
                    command.command_id().clone(),
                    "okx_target_terminal_without_cancel".to_owned(),
                )
            }
            _ => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
        };
    }
    if state == OrderState::Rejected {
        AccountRecoveryOutcome::rejected(
            command.command_id().clone(),
            "okx_order_rejected".to_owned(),
        )
    } else {
        AccountRecoveryOutcome::accepted(command.command_id().clone(), venue_order_id)
    }
}

fn map_transport_dispatch(error: OkxTransportError) -> AccountGatewayResult {
    match error {
        OkxTransportError::Configuration
        | OkxTransportError::Binding
        | OkxTransportError::BodyTooLarge
        | OkxTransportError::Clock => rejected("okx_pre_send_rejected"),
        _ => AccountGatewayResult::Unknown,
    }
}

fn rejected(reason: &str) -> AccountGatewayResult {
    AccountGatewayResult::Rejected {
        reason: reason.to_owned(),
    }
}

fn rejected_response(body: &[u8]) -> AccountGatewayResult {
    let value = serde_json::from_slice::<serde_json::Value>(body).ok();
    let envelope_code = value
        .as_ref()
        .and_then(|value| value.get("code"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| valid_response_code(value));
    let row = value
        .as_ref()
        .and_then(|value| value.get("data"))
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first());
    let code = row
        .and_then(|row| row.get("sCode"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| valid_response_code(value));
    // This helper is reached only after the normal ACK parser failed. A row-level success code
    // therefore proves that a physical order may exist, but does not prove that the ACK identity
    // and timestamp were valid. Persist UNKNOWN so signed order readback, rather than a retry, is
    // the only path that can settle the command.
    if code == Some("0") {
        return AccountGatewayResult::Unknown;
    }
    let authoritative_code = code.or(envelope_code.filter(|value| *value != "0"));
    let Some(authoritative_code) = authoritative_code else {
        return AccountGatewayResult::Unknown;
    };
    let message = row
        .and_then(|row| row.get("sMsg"))
        .and_then(serde_json::Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= 160 && !value.chars().any(char::is_control)
        })
        .unwrap_or("venue rejected request");
    AccountGatewayResult::Rejected {
        reason: format!("okx_{authoritative_code}: {message}"),
    }
}

fn valid_response_code(value: &str) -> bool {
    !value.is_empty() && value.len() <= 16 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn unix_ms() -> Result<u64, OkxAccountGatewayError> {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| OkxAccountGatewayError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| OkxAccountGatewayError::Clock)
}

#[derive(Debug, thiserror::Error)]
pub enum OkxAccountGatewayError {
    #[error("OKX account gateway binding is invalid")]
    Binding,
    #[error("OKX account gateway credentials are unavailable")]
    Credentials,
    #[error("OKX account gateway runtime could not be created")]
    Runtime,
    #[error("OKX account gateway attempt identity overflowed")]
    Attempt,
    #[error("OKX public instrument or contract conversion is invalid")]
    Instrument,
    #[error("OKX API key lacks exact read/trade permissions or permits withdrawal")]
    Permissions,
    #[error("OKX account-mode or signed position preflight failed")]
    Account,
    #[error("OKX exact signed order readback failed")]
    Readback,
    #[error("OKX timestamp clock is invalid")]
    Clock,
    #[error("OKX transport failed")]
    Transport(#[source] OkxTransportError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_ack_parse_with_row_success_stays_unknown() {
        for payload in [
            br#"{"code":"0","msg":"Order placed","data":[{"sCode":"0","sMsg":"Order placed"}]}"#
                .as_slice(),
            br#"{"code":"1","msg":"partial result","data":[{"sCode":"0","sMsg":"Order placed"}]}"#
                .as_slice(),
        ] {
            assert_eq!(rejected_response(payload), AccountGatewayResult::Unknown);
        }
    }

    #[test]
    fn explicit_nonzero_okx_code_is_terminal_rejection() {
        assert_eq!(
            rejected_response(
                br#"{"code":"1","msg":"failed","data":[{"sCode":"51000","sMsg":"parameter error"}]}"#,
            ),
            AccountGatewayResult::Rejected {
                reason: "okx_51000: parameter error".to_owned(),
            }
        );
    }

    #[test]
    fn malformed_ack_failure_stays_unknown() {
        assert_eq!(
            rejected_response(br#"{"code":"0","msg":"","data":[]}"#),
            AccountGatewayResult::Unknown
        );
    }

    #[test]
    fn timestamp_format_is_exact_utc_milliseconds() -> Result<(), Box<dyn std::error::Error>> {
        let timestamp =
            okx_timestamp(SystemTime::UNIX_EPOCH + Duration::from_millis(1_607_418_537_715))?;
        assert_eq!(timestamp, "2020-12-08T09:08:57.715Z");
        Ok(())
    }
}
