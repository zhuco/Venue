use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use sha3::{Digest, Keccak256};
use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{ExecutionCommand, OrderState};
use venue_execution::{
    AccountDispatchPermit, AccountGatewayResult, AccountPhysicalGateway, AccountRecoveryOutcome,
    AccountRecoveryReport, AccountRecoveryRequest,
};
use venue_gateway_api::GatewayBinding;

use crate::action::{
    HyperliquidAloOrder, HyperliquidCancel, HyperliquidExchangeOutcome, build_alo_place_request,
    build_cancel_request, parse_exchange_ack,
};
use crate::{
    HyperliquidCredentials, HyperliquidError, HyperliquidGatewayBinding, HyperliquidHttpTransport,
    HyperliquidNonceStore, HyperliquidOrderLookup, HyperliquidOrderStatus, HyperliquidPerpMeta,
    HyperliquidReadBinding, HyperliquidTransportError, NonceCheckpoint,
    build_clearinghouse_state_request, build_frontend_open_orders_request, build_meta_request,
    build_order_status_request, parse_clearinghouse_snapshot, parse_frontend_open_orders_snapshot,
    parse_order_status, parse_perp_meta, reserve_next_nonce,
};

const NONCE_CHECKPOINT_MAX_BYTES: u64 = 4 * 1024;
const ACTION_EXPIRY_MS: u64 = 30_000;

/// Production Hyperliquid adapter for the lightweight account host. The only mutation method is
/// the host trait's linear-permit consumer; raw signing and `/exchange` POST remain crate-private.
pub struct HyperliquidAccountGateway {
    runtime: Runtime,
    binding: HyperliquidReadBinding,
    credentials: HyperliquidCredentials,
    transport: HyperliquidHttpTransport,
    meta: HyperliquidPerpMeta,
    account_safety: AccountSafety,
    nonce_store: FileNonceStore,
}

impl HyperliquidAccountGateway {
    /// Performs meta, clearinghouse, and complete frontend-open-order reads. Construction does not
    /// sign or send an exchange action.
    pub fn connect_from_environment(
        binding: GatewayBinding,
        nonce_checkpoint_path: impl Into<PathBuf>,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, HyperliquidAccountGatewayError> {
        let gateway = HyperliquidGatewayBinding::new(binding)
            .map_err(|_| HyperliquidAccountGatewayError::Binding)?;
        let credentials = HyperliquidCredentials::from_environment()
            .map_err(|_| HyperliquidAccountGatewayError::Credentials)?;
        let read_binding = HyperliquidReadBinding::new(gateway, credentials.user_address())
            .map_err(|_| HyperliquidAccountGatewayError::Binding)?;
        let nonce_store = FileNonceStore::new(
            nonce_checkpoint_path.into(),
            read_binding.gateway().gateway_binding(),
        )?;
        let transport = HyperliquidHttpTransport::new(operation_timeout, max_body_bytes)
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| HyperliquidAccountGatewayError::Runtime)?;
        let meta = runtime.block_on(fetch_meta(&read_binding, &transport))?;
        let account_safety = runtime.block_on(refresh_account(&meta, &transport))?;
        Ok(Self {
            runtime,
            binding: read_binding,
            credentials,
            transport,
            meta,
            account_safety,
            nonce_store,
        })
    }

    fn refresh(&mut self) -> Result<(), HyperliquidAccountGatewayError> {
        self.account_safety = self
            .runtime
            .block_on(refresh_account(&self.meta, &self.transport))?;
        Ok(())
    }

    fn order_status(
        &mut self,
        lookup: &HyperliquidOrderLookup,
    ) -> Result<HyperliquidOrderStatus, HyperliquidAccountGatewayError> {
        let request = build_order_status_request(&self.meta, lookup)
            .map_err(|_| HyperliquidAccountGatewayError::Readback)?;
        let response = self
            .runtime
            .block_on(self.transport.post_info(&self.binding, &request))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        parse_order_status(&response.body, &self.meta, lookup)
            .map_err(|_| HyperliquidAccountGatewayError::Readback)
    }

    fn reserve_nonce(&mut self) -> Result<crate::PersistedNonce, HyperliquidAccountGatewayError> {
        let now_ms = unix_ms()?;
        reserve_next_nonce(
            &mut self.nonce_store,
            self.credentials.api_wallet_address(),
            now_ms,
        )
        .map_err(|_| HyperliquidAccountGatewayError::Nonce)
    }

    fn dispatch_permit(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        if permit.binding() != self.binding.gateway().gateway_binding() {
            return rejected("hyperliquid_permit_binding");
        }
        if self.refresh().is_err() {
            return rejected("hyperliquid_preflight_failed");
        }
        let now_ms = match unix_ms() {
            Ok(value) => value,
            Err(_) => return rejected("hyperliquid_clock"),
        };
        let expires_after_ms = match now_ms.checked_add(ACTION_EXPIRY_MS) {
            Some(value) => Some(value),
            None => return rejected("hyperliquid_clock"),
        };
        let request = match permit.command() {
            ExecutionCommand::PlaceLimit(command) => {
                if !command.reduce_only
                    && (self.account_safety.has_position || self.account_safety.has_open_orders)
                {
                    return rejected("hyperliquid_existing_account_risk");
                }
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let cloid = command_cloid(command.client_order_id.as_str());
                let order = match HyperliquidAloOrder::new(
                    &self.meta,
                    command.side,
                    command.limit_price.value(),
                    command.quantity,
                    command.reduce_only,
                    cloid,
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_intent_rejected"),
                };
                build_alo_place_request(&self.credentials, nonce, order, expires_after_ms)
            }
            ExecutionCommand::Cancel(command) => {
                let lookup = match HyperliquidOrderLookup::client_order_id(command_cloid(
                    command.target_client_order_id.as_str(),
                )) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_cancel_identity"),
                };
                let order_id = match self.order_status(&lookup) {
                    Ok(HyperliquidOrderStatus::Known { order_id, .. }) => order_id,
                    Ok(HyperliquidOrderStatus::Unknown { .. }) | Err(_) => {
                        return rejected("hyperliquid_cancel_target_unresolved");
                    }
                };
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let cancel = match HyperliquidCancel::new(&self.meta, order_id) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_cancel_identity"),
                };
                build_cancel_request(&self.credentials, nonce, cancel, expires_after_ms)
            }
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::MarketReduce(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                return rejected("hyperliquid_initial_profile_unsupported_command");
            }
        };
        let request = match request {
            Ok(value) => value,
            Err(_) => return rejected("hyperliquid_signing_rejected"),
        };
        match self
            .runtime
            .block_on(self.transport.post_exchange(request.binding(), &request))
        {
            Ok(response) => match parse_exchange_ack(&response.body, &request) {
                Ok(HyperliquidExchangeOutcome::Resting { order_id })
                | Ok(HyperliquidExchangeOutcome::Filled { order_id, .. })
                | Ok(HyperliquidExchangeOutcome::Cancelled { order_id }) => {
                    AccountGatewayResult::Accepted {
                        venue_order_id: order_id.to_string(),
                    }
                }
                Ok(HyperliquidExchangeOutcome::Rejected { reason }) => {
                    AccountGatewayResult::Rejected { reason }
                }
                Err(_) => AccountGatewayResult::Unknown,
            },
            Err(error) => map_transport_dispatch(error),
        }
    }
}

impl AccountPhysicalGateway for HyperliquidAccountGateway {
    type Error = HyperliquidAccountGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.binding.gateway().gateway_binding()
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        if request.binding() != self.binding.gateway().gateway_binding() {
            return Err(HyperliquidAccountGatewayError::Binding);
        }
        self.refresh()?;
        let observed_at_ms = unix_ms()?;
        let mut outcomes = Vec::with_capacity(request.unresolved().len());
        for command in request.unresolved() {
            let client_id = match command {
                ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
                _ => command
                    .native_client_id()
                    .ok_or(HyperliquidAccountGatewayError::Readback)?
                    .as_str(),
            };
            let lookup = HyperliquidOrderLookup::client_order_id(command_cloid(client_id))
                .map_err(|_| HyperliquidAccountGatewayError::Readback)?;
            let status = self.order_status(&lookup)?;
            outcomes.push(hyperliquid_recovery_outcome(command, status));
        }
        AccountRecoveryReport::new(
            self.binding.gateway().gateway_binding().clone(),
            observed_at_ms,
            outcomes,
        )
        .map_err(|_| HyperliquidAccountGatewayError::Readback)
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatch_permit(permit)
    }
}

async fn fetch_meta(
    binding: &HyperliquidReadBinding,
    transport: &HyperliquidHttpTransport,
) -> Result<HyperliquidPerpMeta, HyperliquidAccountGatewayError> {
    let request =
        build_meta_request(binding).map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
    let response = transport
        .post_info(binding, &request)
        .await
        .map_err(HyperliquidAccountGatewayError::Transport)?;
    let meta = parse_perp_meta(&response.body, binding)
        .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
    if !meta.trading_enabled {
        return Err(HyperliquidAccountGatewayError::Instrument);
    }
    Ok(meta)
}

async fn refresh_account(
    meta: &HyperliquidPerpMeta,
    transport: &HyperliquidHttpTransport,
) -> Result<AccountSafety, HyperliquidAccountGatewayError> {
    let account_request = build_clearinghouse_state_request(meta)
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
    let account = transport
        .post_info(meta.scope.binding(), &account_request)
        .await
        .map_err(HyperliquidAccountGatewayError::Transport)?;
    let snapshot = parse_clearinghouse_snapshot(&account.body, meta)
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
    if snapshot.exchange_time_ms > account.received_at_ms {
        return Err(HyperliquidAccountGatewayError::Account);
    }
    let orders_request = build_frontend_open_orders_request(meta)
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
    let orders = transport
        .post_info(meta.scope.binding(), &orders_request)
        .await
        .map_err(HyperliquidAccountGatewayError::Transport)?;
    let orders = parse_frontend_open_orders_snapshot(&orders.body, meta, orders.received_at_ms)
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
    Ok(AccountSafety {
        has_position: snapshot
            .position
            .as_ref()
            .is_some_and(|position| !position.quantity.is_zero()),
        has_open_orders: !orders.orders.is_empty(),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AccountSafety {
    has_position: bool,
    has_open_orders: bool,
}

fn hyperliquid_recovery_outcome(
    command: &ExecutionCommand,
    status: HyperliquidOrderStatus,
) -> AccountRecoveryOutcome {
    match status {
        HyperliquidOrderStatus::Unknown { .. } => {
            AccountRecoveryOutcome::still_unknown(command.command_id().clone())
        }
        HyperliquidOrderStatus::Known {
            order_id, state, ..
        } if matches!(command, ExecutionCommand::Cancel(_)) => match state {
            OrderState::Cancelled => {
                AccountRecoveryOutcome::accepted(command.command_id().clone(), order_id.to_string())
            }
            OrderState::Filled | OrderState::Expired | OrderState::Rejected => {
                AccountRecoveryOutcome::rejected(
                    command.command_id().clone(),
                    "hyperliquid_target_terminal_without_cancel".to_owned(),
                )
            }
            _ => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
        },
        HyperliquidOrderStatus::Known {
            order_id: _,
            state: OrderState::Rejected,
            ..
        } => AccountRecoveryOutcome::rejected(
            command.command_id().clone(),
            "hyperliquid_order_rejected".to_owned(),
        ),
        HyperliquidOrderStatus::Known { order_id, .. } => {
            AccountRecoveryOutcome::accepted(command.command_id().clone(), order_id.to_string())
        }
    }
}

fn command_cloid(command_id: &str) -> String {
    let mut digest = Keccak256::new();
    digest.update(b"venue-hyperliquid-command-cloid-v1");
    digest.update(command_id.as_bytes());
    let digest = digest.finalize();
    let mut value = String::with_capacity(34);
    value.push_str("0x");
    for byte in &digest[..16] {
        value.push_str(&format!("{byte:02x}"));
    }
    value
}

fn map_transport_dispatch(error: HyperliquidTransportError) -> AccountGatewayResult {
    match error {
        HyperliquidTransportError::Configuration
        | HyperliquidTransportError::Binding
        | HyperliquidTransportError::BodyTooLarge
        | HyperliquidTransportError::Protocol
        | HyperliquidTransportError::Clock => rejected("hyperliquid_pre_send_rejected"),
        _ => AccountGatewayResult::Unknown,
    }
}

fn rejected(reason: &str) -> AccountGatewayResult {
    AccountGatewayResult::Rejected {
        reason: reason.to_owned(),
    }
}

fn unix_ms() -> Result<u64, HyperliquidAccountGatewayError> {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| HyperliquidAccountGatewayError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| HyperliquidAccountGatewayError::Clock)
}

struct FileNonceStore {
    path: PathBuf,
}

impl FileNonceStore {
    fn new(
        path: PathBuf,
        binding: &GatewayBinding,
    ) -> Result<Self, HyperliquidAccountGatewayError> {
        if !path.is_absolute()
            || path.file_name().and_then(|value| value.to_str()) != Some("nonce.json")
            || !path.parent().is_some_and(|parent| {
                parent.ends_with(
                    Path::new(binding.venue.as_str())
                        .join(binding.mode.as_str())
                        .join(&binding.trading_account_id),
                )
            })
        {
            return Err(HyperliquidAccountGatewayError::NoncePath);
        }
        Ok(Self { path })
    }
}

impl HyperliquidNonceStore for FileNonceStore {
    fn load(&mut self, _agent_address: &str) -> Result<Option<NonceCheckpoint>, HyperliquidError> {
        let metadata = match fs::metadata(&self.path) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(HyperliquidError::Nonce),
        };
        if metadata.len() == 0 || metadata.len() > NONCE_CHECKPOINT_MAX_BYTES {
            return Err(HyperliquidError::Nonce);
        }
        let mut file = File::open(&self.path).map_err(|_| HyperliquidError::Nonce)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.read_to_end(&mut bytes)
            .map_err(|_| HyperliquidError::Nonce)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| HyperliquidError::Nonce)
    }

    fn persist(&mut self, checkpoint: &NonceCheckpoint) -> Result<(), HyperliquidError> {
        let bytes = serde_json::to_vec(checkpoint).map_err(|_| HyperliquidError::Nonce)?;
        if bytes.is_empty() || bytes.len() as u64 > NONCE_CHECKPOINT_MAX_BYTES {
            return Err(HyperliquidError::Nonce);
        }
        let parent = self.path.parent().ok_or(HyperliquidError::Nonce)?;
        fs::create_dir_all(parent).map_err(|_| HyperliquidError::Nonce)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.path)
            .map_err(|_| HyperliquidError::Nonce)?;
        file.write_all(&bytes)
            .map_err(|_| HyperliquidError::Nonce)?;
        file.sync_all().map_err(|_| HyperliquidError::Nonce)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HyperliquidAccountGatewayError {
    #[error("Hyperliquid account gateway binding is invalid")]
    Binding,
    #[error("Hyperliquid account gateway credentials are unavailable")]
    Credentials,
    #[error("Hyperliquid account gateway runtime could not be created")]
    Runtime,
    #[error("Hyperliquid public perpetual metadata is unavailable or invalid")]
    Instrument,
    #[error("Hyperliquid account/open-order preflight failed")]
    Account,
    #[error("Hyperliquid exact orderStatus readback failed")]
    Readback,
    #[error("Hyperliquid nonce checkpoint path is outside the bound account artifact root")]
    NoncePath,
    #[error("Hyperliquid durable nonce reservation failed")]
    Nonce,
    #[error("Hyperliquid clock is invalid")]
    Clock,
    #[error("Hyperliquid transport failed")]
    Transport(#[source] HyperliquidTransportError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloid_is_stable_lower_hex_and_exact_wire_length() {
        let first = command_cloid("client-order-1");
        assert_eq!(first, command_cloid("client-order-1"));
        assert_ne!(first, command_cloid("client-order-2"));
        assert_eq!(first.len(), 34);
        assert!(first.starts_with("0x"));
        assert!(first[2..].bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
}
