use std::time::Duration;

use venue_domain::domain::{CancelCommand, ExecutionCommand, OrderState, PositionSide};
use venue_gateway_api::{CapabilitySnapshot, GatewayBinding, MutationCapability};

use crate::{
    OkxAcceptedOrder, OkxCancelOnceOutcome, OkxCancelRequest, OkxCapabilityCandidate, OkxConfig,
    OkxCredentials, OkxError, OkxHttpResponse, OkxHttpTransport, OkxInstrument, OkxOrderReadback,
    OkxOrderReadbackRequest, OkxPlaceIntent, OkxPlaceOnceOutcome, OkxPlaceRequest,
    OkxTransportError, OkxUnknownCancelReadbackRequest, OkxUnknownCancelResolution,
    OkxUnknownOrderReadback, OkxUnknownOrderReadbackRequest, PersistedOkxCapabilityProbe,
    build_cancel_order_readback_request, build_cancel_request, build_order_readback_request,
    build_place_request, parse_order_detail, parse_unknown_cancel_readback,
    parse_unknown_order_readback, validate_capability_candidate,
};

/// Secret-free, non-authoritative bridge from durable probe evidence to the physical adapter.
/// It owns neither credentials nor a transport and therefore cannot dispatch by itself.
pub struct OkxPhysicalCandidate {
    config: OkxConfig,
    instrument: OkxInstrument,
    probe: OkxCapabilityCandidate,
    capability: CapabilitySnapshot,
}

impl OkxPhysicalCandidate {
    pub fn from_probe(
        config: OkxConfig,
        instrument: OkxInstrument,
        persisted: &PersistedOkxCapabilityProbe,
        now_ms: u64,
    ) -> Result<Self, OkxPhysicalError> {
        let probe = validate_capability_candidate(&config, &instrument, persisted, now_ms)
            .map_err(|_| OkxPhysicalError::Capability)?;
        if probe.readback.profile.position_mode() != crate::OkxPositionMode::LongShort
            || probe.readback.scope().trade_mode() != probe.scope.trade_mode
            || probe.readback.scope().attempt_id() != probe.scope.read_attempt_id
        {
            return Err(OkxPhysicalError::Scope);
        }
        let capability = CapabilitySnapshot {
            binding: probe.scope.binding.clone(),
            version: probe.scope.capability_version,
            observed_ms: probe.scope.observed_at_ms,
            expires_ms: probe.scope.expires_at_ms,
            flags: probe.candidate_flags,
        };
        Ok(Self {
            config,
            instrument,
            probe,
            capability,
        })
    }

    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        self.config.gateway_binding()
    }

    #[must_use]
    pub fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.capability.clone()
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.probe.scope.private_generation
    }

    #[must_use]
    pub fn probe_sha256(&self) -> &str {
        &self.probe.evidence_sha256
    }

    #[must_use]
    pub const fn readback(&self) -> &crate::OkxPrivateReadbackCandidate {
        &self.probe.readback
    }

    pub fn prepare_place_once(
        &self,
        command: &ExecutionCommand,
        now_ms: u64,
    ) -> Result<OkxOneShotMutation, OkxPhysicalError> {
        let (intent, capability, reduce_once) = match command {
            ExecutionCommand::PlaceLimit(command) => (
                OkxPlaceIntent::Limit(command),
                MutationCapability::PlaceLimit,
                false,
            ),
            ExecutionCommand::PlaceMarket(command) => (
                OkxPlaceIntent::Market(command),
                MutationCapability::PlaceMarket,
                false,
            ),
            ExecutionCommand::MarketReduce(command) => {
                let signed_quantity = self
                    .probe
                    .readback
                    .positions
                    .iter()
                    .find(|position| position.position.side == command.position_side)
                    .map(|position| position.position.quantity)
                    .ok_or(OkxPhysicalError::Intent)?;
                if !matches!(
                    command.position_side,
                    PositionSide::Long | PositionSide::Short
                ) || command.quantity > signed_quantity
                {
                    return Err(OkxPhysicalError::Intent);
                }
                (
                    OkxPlaceIntent::MarketReduce(command),
                    MutationCapability::PlaceMarket,
                    true,
                )
            }
            ExecutionCommand::Cancel(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                return Err(OkxPhysicalError::UnsupportedCommand);
            }
        };
        self.authorize(now_ms, capability)?;
        let request = build_place_request(
            &self.config,
            &self.instrument,
            &self.probe.readback.profile,
            self.probe.scope.trade_mode,
            intent,
        )?;
        if request.is_reduce_once() != reduce_once {
            return Err(OkxPhysicalError::Intent);
        }
        Ok(OkxOneShotMutation {
            binding: self.binding().clone(),
            capability_version: self.capability.version,
            capability,
            probe_sha256: self.probe.evidence_sha256.clone(),
            prepared_at_ms: now_ms,
            request: OkxPhysicalRequest::Place {
                request: Box::new(request),
                reduce_once,
            },
        })
    }

    pub fn prepare_cancel_once(
        &self,
        command: &CancelCommand,
        accepted_order: &OkxAcceptedOrder,
        now_ms: u64,
    ) -> Result<OkxOneShotMutation, OkxPhysicalError> {
        self.authorize(now_ms, MutationCapability::Cancel)?;
        let request = build_cancel_request(
            &self.config,
            &self.instrument,
            &self.probe.readback.profile,
            command,
            accepted_order,
        )?;
        Ok(OkxOneShotMutation {
            binding: self.binding().clone(),
            capability_version: self.capability.version,
            capability: MutationCapability::Cancel,
            probe_sha256: self.probe.evidence_sha256.clone(),
            prepared_at_ms: now_ms,
            request: OkxPhysicalRequest::Cancel {
                request: Box::new(request),
                accepted_order: Box::new(accepted_order.clone()),
            },
        })
    }

    pub fn into_session(
        self,
        credentials: OkxCredentials,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<OkxPhysicalSession, OkxPhysicalError> {
        let transport =
            OkxHttpTransport::new(self.config.clone(), operation_timeout, max_body_bytes)?;
        Ok(OkxPhysicalSession {
            candidate: self,
            credentials,
            transport,
        })
    }

    #[cfg(test)]
    pub(crate) fn into_session_with_origin(
        self,
        credentials: OkxCredentials,
        origin: &str,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<OkxPhysicalSession, OkxPhysicalError> {
        let transport = OkxHttpTransport::with_origin(
            self.config.clone(),
            origin,
            operation_timeout,
            max_body_bytes,
        )?;
        Ok(OkxPhysicalSession {
            candidate: self,
            credentials,
            transport,
        })
    }

    fn authorize(&self, now_ms: u64, mutation: MutationCapability) -> Result<(), OkxPhysicalError> {
        self.capability
            .authorize(self.binding(), self.capability.version, now_ms, mutation)
            .map_err(|_| OkxPhysicalError::Capability)
    }
}

/// A real one-attempt OKX transport paired with one validated candidate. It still owns no
/// Owner map, WAL, writer lease, Control applied receipt, reconciliation loop, or Canary permit.
pub struct OkxPhysicalSession {
    candidate: OkxPhysicalCandidate,
    credentials: OkxCredentials,
    transport: OkxHttpTransport,
}

impl OkxPhysicalSession {
    #[must_use]
    pub const fn candidate(&self) -> &OkxPhysicalCandidate {
        &self.candidate
    }

    pub async fn dispatch_once(
        &self,
        mutation: OkxOneShotMutation,
        timestamp: &str,
        now_ms: u64,
    ) -> Result<OkxDispatchOnceResult, OkxPhysicalError> {
        mutation.validate(&self.candidate, now_ms)?;
        let result = match mutation.request {
            OkxPhysicalRequest::Place {
                request,
                reduce_once,
            } => {
                let outcome = if reduce_once {
                    self.transport
                        .reduce_once(
                            &self.credentials,
                            &self.candidate.instrument,
                            &self.candidate.probe.readback.profile,
                            *request,
                            timestamp,
                        )
                        .await?
                } else {
                    self.transport
                        .place_once(
                            &self.credentials,
                            &self.candidate.instrument,
                            &self.candidate.probe.readback.profile,
                            *request,
                            timestamp,
                        )
                        .await?
                };
                match outcome {
                    OkxPlaceOnceOutcome::Acknowledged(accepted) => {
                        let request = build_order_readback_request(
                            &self.candidate.config,
                            &self.candidate.instrument,
                            &self.candidate.probe.readback.profile,
                            &accepted,
                        )?;
                        OkxDispatchOnceResult::PendingReadback(OkxPendingMutation {
                            binding: self.candidate.binding().clone(),
                            capability_version: self.candidate.capability.version,
                            probe_sha256: self.candidate.probe.evidence_sha256.clone(),
                            request: OkxPendingRequest::OrderAck(request),
                        })
                    }
                    OkxPlaceOnceOutcome::Unknown { readback, .. } => {
                        OkxDispatchOnceResult::PendingReadback(OkxPendingMutation {
                            binding: self.candidate.binding().clone(),
                            capability_version: self.candidate.capability.version,
                            probe_sha256: self.candidate.probe.evidence_sha256.clone(),
                            request: OkxPendingRequest::OrderUnknown(*readback),
                        })
                    }
                }
            }
            OkxPhysicalRequest::Cancel {
                request,
                accepted_order,
            } => match self
                .transport
                .cancel_once(
                    &self.credentials,
                    &self.candidate.instrument,
                    &self.candidate.probe.readback.profile,
                    &accepted_order,
                    *request,
                    timestamp,
                )
                .await?
            {
                OkxCancelOnceOutcome::Acknowledged(accepted_cancel) => {
                    let request = build_cancel_order_readback_request(
                        &self.candidate.config,
                        &self.candidate.instrument,
                        &self.candidate.probe.readback.profile,
                        &accepted_order,
                        &accepted_cancel,
                    )?;
                    OkxDispatchOnceResult::PendingReadback(OkxPendingMutation {
                        binding: self.candidate.binding().clone(),
                        capability_version: self.candidate.capability.version,
                        probe_sha256: self.candidate.probe.evidence_sha256.clone(),
                        request: OkxPendingRequest::CancelAck(request),
                    })
                }
                OkxCancelOnceOutcome::Unknown { readback, .. } => {
                    OkxDispatchOnceResult::PendingReadback(OkxPendingMutation {
                        binding: self.candidate.binding().clone(),
                        capability_version: self.candidate.capability.version,
                        probe_sha256: self.candidate.probe.evidence_sha256.clone(),
                        request: OkxPendingRequest::CancelUnknown(*readback),
                    })
                }
            },
        };
        Ok(result)
    }

    pub async fn readback_pending(
        &self,
        pending: OkxPendingMutation,
        timestamp: &str,
    ) -> Result<OkxPhysicalReadbackResult, OkxPhysicalError> {
        pending.validate(&self.candidate)?;
        let response = match &pending.request {
            OkxPendingRequest::OrderAck(request) | OkxPendingRequest::CancelAck(request) => {
                self.transport
                    .execute(&self.credentials, request, timestamp)
                    .await
            }
            OkxPendingRequest::OrderUnknown(request) => {
                self.transport
                    .execute(&self.credentials, request, timestamp)
                    .await
            }
            OkxPendingRequest::CancelUnknown(request) => {
                self.transport
                    .execute(&self.credentials, request, timestamp)
                    .await
            }
        };
        match response {
            Ok(response) => pending.converge(response),
            Err(_) => Ok(OkxPhysicalReadbackResult::PendingUnknown(Box::new(pending))),
        }
    }
}

/// Linear dispatch value. It is consumed by `dispatch_once` and intentionally is not `Clone`.
pub struct OkxOneShotMutation {
    binding: GatewayBinding,
    capability_version: u64,
    capability: MutationCapability,
    probe_sha256: String,
    prepared_at_ms: u64,
    request: OkxPhysicalRequest,
}

impl OkxOneShotMutation {
    fn validate(
        &self,
        candidate: &OkxPhysicalCandidate,
        now_ms: u64,
    ) -> Result<(), OkxPhysicalError> {
        if self.binding != *candidate.binding()
            || self.capability_version != candidate.capability.version
            || self.probe_sha256 != candidate.probe.evidence_sha256
            || now_ms < self.prepared_at_ms
        {
            return Err(OkxPhysicalError::Scope);
        }
        match &self.request {
            OkxPhysicalRequest::Place {
                request,
                reduce_once,
            } => {
                if request.is_reduce_once() != *reduce_once {
                    return Err(OkxPhysicalError::Intent);
                }
            }
            OkxPhysicalRequest::Cancel { .. } => {
                if self.capability != MutationCapability::Cancel {
                    return Err(OkxPhysicalError::Scope);
                }
            }
        }
        candidate.authorize(now_ms, self.capability)
    }
}

enum OkxPhysicalRequest {
    Place {
        request: Box<OkxPlaceRequest>,
        reduce_once: bool,
    },
    Cancel {
        request: Box<OkxCancelRequest>,
        accepted_order: Box<OkxAcceptedOrder>,
    },
}

#[derive(Debug)]
pub enum OkxDispatchOnceResult {
    PendingReadback(OkxPendingMutation),
}

#[derive(Debug)]
pub struct OkxPendingMutation {
    binding: GatewayBinding,
    capability_version: u64,
    probe_sha256: String,
    request: OkxPendingRequest,
}

impl OkxPendingMutation {
    #[must_use]
    pub const fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    #[must_use]
    pub const fn capability_version(&self) -> u64 {
        self.capability_version
    }

    #[must_use]
    pub fn probe_sha256(&self) -> &str {
        &self.probe_sha256
    }

    fn validate(&self, candidate: &OkxPhysicalCandidate) -> Result<(), OkxPhysicalError> {
        if self.binding != *candidate.binding()
            || self.capability_version != candidate.capability.version
            || self.probe_sha256 != candidate.probe.evidence_sha256
        {
            return Err(OkxPhysicalError::Scope);
        }
        Ok(())
    }

    fn converge(
        self,
        response: OkxHttpResponse,
    ) -> Result<OkxPhysicalReadbackResult, OkxPhysicalError> {
        match &self.request {
            OkxPendingRequest::OrderAck(request) => {
                settle_order_readback(parse_order_detail(response, request)?)
            }
            OkxPendingRequest::OrderUnknown(request) => {
                settle_unknown_order_readback(parse_unknown_order_readback(response, request)?)
            }
            OkxPendingRequest::CancelAck(request) => {
                let readback = parse_order_detail(response, request)?;
                if terminal(readback.order.order.state) {
                    Ok(OkxPhysicalReadbackResult::Confirmed(readback))
                } else {
                    Ok(OkxPhysicalReadbackResult::PendingUnknown(Box::new(self)))
                }
            }
            OkxPendingRequest::CancelUnknown(request) => {
                match parse_unknown_cancel_readback(response, request)? {
                    OkxUnknownCancelResolution::Terminal(readback) => {
                        Ok(OkxPhysicalReadbackResult::Confirmed(readback))
                    }
                    OkxUnknownCancelResolution::StillUnknown(_) => {
                        Ok(OkxPhysicalReadbackResult::PendingUnknown(Box::new(self)))
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum OkxPendingRequest {
    OrderAck(OkxOrderReadbackRequest),
    OrderUnknown(OkxUnknownOrderReadbackRequest),
    CancelAck(OkxOrderReadbackRequest),
    CancelUnknown(OkxUnknownCancelReadbackRequest),
}

#[derive(Debug)]
pub enum OkxPhysicalReadbackResult {
    Confirmed(OkxOrderReadback),
    ConfirmedUnknown(OkxUnknownOrderReadback),
    Rejected,
    PendingUnknown(Box<OkxPendingMutation>),
}

fn settle_order_readback(
    readback: OkxOrderReadback,
) -> Result<OkxPhysicalReadbackResult, OkxPhysicalError> {
    if readback.order.order.state == OrderState::Rejected {
        Ok(OkxPhysicalReadbackResult::Rejected)
    } else {
        Ok(OkxPhysicalReadbackResult::Confirmed(readback))
    }
}

fn settle_unknown_order_readback(
    readback: OkxUnknownOrderReadback,
) -> Result<OkxPhysicalReadbackResult, OkxPhysicalError> {
    if readback.order.order.state == OrderState::Rejected {
        Ok(OkxPhysicalReadbackResult::Rejected)
    } else {
        Ok(OkxPhysicalReadbackResult::ConfirmedUnknown(readback))
    }
}

const fn terminal(state: OrderState) -> bool {
    matches!(
        state,
        OrderState::Cancelled | OrderState::Filled | OrderState::Expired | OrderState::Rejected
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OkxPhysicalError {
    #[error("OKX physical capability probe is invalid, incomplete, or stale")]
    Capability,
    #[error("OKX physical binding, mode, or generation does not match")]
    Scope,
    #[error("OKX physical mutation intent is invalid")]
    Intent,
    #[error("the shared canonical command has no safe OKX mutation mapping")]
    UnsupportedCommand,
    #[error("OKX physical transport or exact readback failed closed")]
    Transport,
}

impl From<OkxError> for OkxPhysicalError {
    fn from(error: OkxError) -> Self {
        match error {
            OkxError::Capability => Self::Capability,
            OkxError::Binding | OkxError::PositionMode | OkxError::Sequence => Self::Scope,
            OkxError::Precision | OkxError::Identity | OkxError::Payload => Self::Intent,
            OkxError::Credentials
            | OkxError::SigningInput
            | OkxError::Rejected
            | OkxError::Pagination
            | OkxError::Persistence => Self::Transport,
        }
    }
}

impl From<OkxTransportError> for OkxPhysicalError {
    fn from(_: OkxTransportError) -> Self {
        Self::Transport
    }
}
