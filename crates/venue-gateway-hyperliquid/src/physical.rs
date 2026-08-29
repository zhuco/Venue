use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use venue_domain::domain::OrderState;
use venue_gateway_api::GatewayBinding;

use crate::{
    HyperliquidActionKind, HyperliquidError, HyperliquidExchangeConvergence,
    HyperliquidExchangeOutcome, HyperliquidExchangeReadbackPlan, HyperliquidExchangeRequest,
    HyperliquidHttpResponse, HyperliquidHttpTransport, HyperliquidOrderStatus,
    HyperliquidPrivateStreamBinding, HyperliquidTransportError, begin_exchange_readback,
    parse_exchange_ack,
};

/// The exchange call used by the linear adapter dispatch. Implementations must perform one HTTP
/// attempt and must not retry. The production implementation delegates to the transport whose
/// reqwest retry policy is fixed to `never`.
pub(crate) trait HyperliquidExchangeDispatch {
    fn post_exchange<'a>(
        &'a mut self,
        expected_binding: &'a crate::HyperliquidReadBinding,
        request: &'a HyperliquidExchangeRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<HyperliquidHttpResponse, HyperliquidTransportError>>
                + Send
                + 'a,
        >,
    >;
}

impl HyperliquidExchangeDispatch for HyperliquidHttpTransport {
    fn post_exchange<'a>(
        &'a mut self,
        expected_binding: &'a crate::HyperliquidReadBinding,
        request: &'a HyperliquidExchangeRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<HyperliquidHttpResponse, HyperliquidTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(HyperliquidHttpTransport::post_exchange(
            self,
            expected_binding,
            request,
        ))
    }
}

/// A signed action paired with its exact private generation. This type is deliberately neither
/// `Clone` nor serializable. Calling `dispatch_once` consumes the only copy, so a timeout or ACK
/// disconnect can expose only its read-only `orderStatus` recovery plan, never the signed body.
pub struct HyperliquidPhysicalDispatch {
    request: HyperliquidExchangeRequest,
    private_binding: HyperliquidPrivateStreamBinding,
    fallback_plan: HyperliquidExchangeReadbackPlan,
}

impl HyperliquidPhysicalDispatch {
    pub fn new(
        request: HyperliquidExchangeRequest,
        private_binding: &HyperliquidPrivateStreamBinding,
    ) -> Result<Self, HyperliquidError> {
        let fallback_plan = begin_exchange_readback(&request, None, private_binding)?;
        Ok(Self {
            request,
            private_binding: private_binding.clone(),
            fallback_plan,
        })
    }

    /// Performs exactly one physical POST. Any transport failure, malformed response, or binding
    /// mismatch is UNKNOWN because the request may have reached the venue. The result contains no
    /// request body and therefore cannot be resubmitted.
    pub async fn dispatch_once(
        self,
        dispatch: &mut HyperliquidHttpTransport,
    ) -> HyperliquidPhysicalDispatchResult {
        self.dispatch_once_inner(dispatch).await
    }

    #[cfg(test)]
    pub(crate) async fn dispatch_once_for_test<D: HyperliquidExchangeDispatch>(
        self,
        dispatch: &mut D,
    ) -> HyperliquidPhysicalDispatchResult {
        self.dispatch_once_inner(dispatch).await
    }

    async fn dispatch_once_inner<D: HyperliquidExchangeDispatch>(
        self,
        dispatch: &mut D,
    ) -> HyperliquidPhysicalDispatchResult {
        let fallback = PendingReadbackFields::from_request(&self.request, self.fallback_plan);
        let response = dispatch
            .post_exchange(self.request.binding(), &self.request)
            .await;
        let acknowledgement = match response {
            Ok(response) if response.binding == *self.request.binding() => {
                parse_exchange_ack(&response.body, &self.request).ok()
            }
            Ok(_) | Err(_) => None,
        };
        if let Some(HyperliquidExchangeOutcome::Rejected { reason }) = acknowledgement.as_ref() {
            return HyperliquidPhysicalDispatchResult::Rejected {
                reason: reason.clone(),
            };
        }
        let plan = begin_exchange_readback(
            &self.request,
            acknowledgement.as_ref(),
            &self.private_binding,
        )
        .unwrap_or(fallback.plan);
        HyperliquidPhysicalDispatchResult::PendingReadback(Box::new(HyperliquidPendingReadback {
            plan,
            gateway_binding: fallback.gateway_binding,
            user_address: fallback.user_address,
            vault_address: fallback.vault_address,
            native_coin: fallback.native_coin,
            connection_id: fallback.connection_id,
        }))
    }
}

struct PendingReadbackFields {
    plan: HyperliquidExchangeReadbackPlan,
    gateway_binding: GatewayBinding,
    user_address: String,
    vault_address: Option<String>,
    native_coin: String,
    connection_id: [u8; 32],
}

impl PendingReadbackFields {
    fn from_request(
        request: &HyperliquidExchangeRequest,
        plan: HyperliquidExchangeReadbackPlan,
    ) -> Self {
        let native_coin = plan.binding().scope().native_coin().to_owned();
        Self {
            plan,
            gateway_binding: request.binding().gateway().gateway_binding().clone(),
            user_address: request.binding().user_address().to_owned(),
            vault_address: request.vault_address().map(str::to_owned),
            native_coin,
            connection_id: request.connection_id(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HyperliquidPhysicalDispatchResult {
    PendingReadback(Box<HyperliquidPendingReadback>),
    Rejected { reason: String },
}

/// Read-only recovery state after the signed request has been consumed. `PendingUnknown` returns
/// this plan again; there is intentionally no transition back to a dispatchable request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperliquidPendingReadback {
    plan: HyperliquidExchangeReadbackPlan,
    gateway_binding: GatewayBinding,
    user_address: String,
    vault_address: Option<String>,
    native_coin: String,
    connection_id: [u8; 32],
}

impl HyperliquidPendingReadback {
    #[must_use]
    pub const fn plan(&self) -> &HyperliquidExchangeReadbackPlan {
        &self.plan
    }

    pub fn order_status_request(
        &self,
        meta: &crate::HyperliquidPerpMeta,
    ) -> Result<crate::HyperliquidInfoRequest, HyperliquidError> {
        self.plan.order_status_request(meta)
    }

    pub fn reconcile(
        self,
        status: Option<&HyperliquidOrderStatus>,
    ) -> Result<HyperliquidPhysicalReadbackResult, HyperliquidError> {
        match self.plan.reconcile(status)? {
            HyperliquidExchangeConvergence::PendingUnknown => Ok(
                HyperliquidPhysicalReadbackResult::PendingUnknown(Box::new(self)),
            ),
            HyperliquidExchangeConvergence::Rejected { reason } => {
                Ok(HyperliquidPhysicalReadbackResult::Rejected { reason })
            }
            HyperliquidExchangeConvergence::Confirmed {
                order_id,
                state,
                exchange_time_ms,
            } => Ok(HyperliquidPhysicalReadbackResult::Confirmed(
                HyperliquidProbeActionReceipt {
                    gateway_binding: self.gateway_binding,
                    user_address: self.user_address,
                    vault_address: self.vault_address,
                    native_coin: self.native_coin,
                    private_generation: self.plan.binding().generation(),
                    kind: self.plan.kind(),
                    nonce: self.plan.nonce(),
                    connection_id: self.connection_id,
                    order_id,
                    state,
                    exchange_time_ms,
                },
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HyperliquidPhysicalReadbackResult {
    PendingUnknown(Box<HyperliquidPendingReadback>),
    Rejected { reason: String },
    Confirmed(HyperliquidProbeActionReceipt),
}

/// Persistable terminal proof for one narrow action. A capability probe accepts only the exact
/// ALO-resting, cancel-cancelled, and IOC-reduce-only-filled sequence from one binding/generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HyperliquidProbeActionReceipt {
    gateway_binding: GatewayBinding,
    user_address: String,
    vault_address: Option<String>,
    native_coin: String,
    private_generation: u64,
    kind: HyperliquidActionKind,
    nonce: u64,
    connection_id: [u8; 32],
    order_id: u64,
    state: OrderState,
    exchange_time_ms: u64,
}

impl HyperliquidProbeActionReceipt {
    #[must_use]
    pub const fn gateway_binding(&self) -> &GatewayBinding {
        &self.gateway_binding
    }

    #[must_use]
    pub fn user_address(&self) -> &str {
        &self.user_address
    }

    #[must_use]
    pub fn vault_address(&self) -> Option<&str> {
        self.vault_address.as_deref()
    }

    #[must_use]
    pub fn native_coin(&self) -> &str {
        &self.native_coin
    }

    #[must_use]
    pub const fn private_generation(&self) -> u64 {
        self.private_generation
    }

    #[must_use]
    pub const fn kind(&self) -> HyperliquidActionKind {
        self.kind
    }

    #[must_use]
    pub const fn nonce(&self) -> u64 {
        self.nonce
    }

    #[must_use]
    pub const fn connection_id(&self) -> [u8; 32] {
        self.connection_id
    }

    #[must_use]
    pub const fn order_id(&self) -> u64 {
        self.order_id
    }

    #[must_use]
    pub const fn state(&self) -> OrderState {
        self.state
    }

    #[must_use]
    pub const fn exchange_time_ms(&self) -> u64 {
        self.exchange_time_ms
    }
}
