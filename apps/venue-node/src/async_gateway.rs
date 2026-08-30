use std::{future::Future, time::Duration};

#[cfg(test)]
use venue_gateway_api::HostAdmissionEvidence;
use venue_gateway_api::{CapabilityFlags, CapabilitySnapshot, GatewayBinding};

#[cfg(test)]
use crate::safe_host::TestAdmittedCapability;
use crate::{
    DispatchPermit, GatewayDispatchResult, GatewayRecoveryPermit, PhysicalGateway,
    SignedReadbackReceipt, SignedReadbackRequest,
};

/// Async adapter error classification at the point where a request may already have left the
/// process. Disconnect never carries a retryable request back across the host boundary.
#[derive(Debug)]
pub enum AsyncGatewayCallError<E> {
    Disconnected,
    Failed(E),
}

/// Tokio-native adapter surface. The synchronous account host owns the sole runtime and invokes
/// these futures linearly; implementations must not create another runtime or retry mutations.
pub trait AsyncPhysicalGateway {
    type Error;

    fn binding(&self) -> &GatewayBinding;

    fn capability_snapshot(&self) -> CapabilitySnapshot;

    fn connect_after_recovery(
        &mut self,
        permit: GatewayRecoveryPermit,
    ) -> impl Future<Output = Result<(), AsyncGatewayCallError<Self::Error>>> + Send;

    fn signed_readback(
        &mut self,
        request: SignedReadbackRequest,
    ) -> impl Future<Output = Result<SignedReadbackReceipt, AsyncGatewayCallError<Self::Error>>> + Send;

    fn verify_signed_readback(&self, receipt: &SignedReadbackReceipt) -> Result<(), Self::Error>;

    /// Positive mutation wiring remains a test fixture until real durable authorities are sealed.
    #[cfg(test)]
    #[allow(
        private_interfaces,
        reason = "the test-only authority must remain crate-local even though the production trait is public"
    )]
    fn dispatch(
        &mut self,
        admitted_capability: TestAdmittedCapability,
        admission_evidence: HostAdmissionEvidence,
        permit: DispatchPermit,
    ) -> impl Future<Output = Result<GatewayDispatchResult, AsyncGatewayCallError<Self::Error>>> + Send;
}

/// Minimal host contract for the existing Tokio runtime. The node owns one driver value and never
/// constructs a runtime per call; the eventual adapter wiring implements this with Tokio
/// `Runtime::block_on` plus `tokio::time::timeout`.
pub trait TokioRuntimeDriver {
    /// Runs the future once. `TimedOut` means the future was dropped and must never be polled or
    /// reconstructed by this driver.
    fn run<F: Future + Send>(&mut self, timeout: Duration, future: F)
    -> TokioRuntimeRun<F::Output>;

    /// Boundary clock sampled before send and after completion in the test-only mutation fixture.
    #[cfg(test)]
    fn execution_now_ms(&self) -> u64;
}

#[derive(Debug)]
pub enum TokioRuntimeRun<T> {
    Completed(T),
    TimedOut,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AsyncGatewayTimeouts {
    connect: Duration,
    readback: Duration,
    dispatch: Duration,
}

impl AsyncGatewayTimeouts {
    pub fn new(
        connect: Duration,
        readback: Duration,
        dispatch: Duration,
    ) -> Result<Self, AsyncGatewayBoundaryError> {
        if connect.is_zero() || readback.is_zero() || dispatch.is_zero() {
            return Err(AsyncGatewayBoundaryError::InvalidTimeout);
        }
        Ok(Self {
            connect,
            readback,
            dispatch,
        })
    }
}

impl Default for AsyncGatewayTimeouts {
    fn default() -> Self {
        Self {
            connect: Duration::from_secs(10),
            readback: Duration::from_secs(10),
            dispatch: Duration::from_secs(10),
        }
    }
}

/// The only sync-to-async execution boundary in the node. It owns exactly one Tokio runtime driver
/// and one adapter, so all connection, readback and mutation calls are linear.
pub struct TokioPhysicalGateway<G, R> {
    runtime: R,
    adapter: G,
    timeouts: AsyncGatewayTimeouts,
}

impl<G: AsyncPhysicalGateway, R: TokioRuntimeDriver> TokioPhysicalGateway<G, R> {
    pub fn new(
        adapter: G,
        runtime: R,
        timeouts: AsyncGatewayTimeouts,
    ) -> Result<Self, AsyncGatewayBoundaryError> {
        validate_capability_preflight(adapter.binding(), &adapter.capability_snapshot())?;
        Ok(Self {
            runtime,
            adapter,
            timeouts,
        })
    }

    fn connect_linear(
        &mut self,
        permit: GatewayRecoveryPermit,
    ) -> Result<(), AsyncGatewayBoundaryError> {
        let future = self.adapter.connect_after_recovery(permit);
        match self.runtime.run(self.timeouts.connect, future) {
            TokioRuntimeRun::Completed(Ok(())) => Ok(()),
            TokioRuntimeRun::TimedOut => Err(AsyncGatewayBoundaryError::Timeout),
            TokioRuntimeRun::Failed => Err(AsyncGatewayBoundaryError::Runtime),
            TokioRuntimeRun::Completed(Err(AsyncGatewayCallError::Disconnected)) => {
                Err(AsyncGatewayBoundaryError::Disconnected)
            }
            TokioRuntimeRun::Completed(Err(AsyncGatewayCallError::Failed(_))) => {
                Err(AsyncGatewayBoundaryError::Adapter)
            }
        }
    }

    fn readback_linear(
        &mut self,
        request: SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, AsyncGatewayBoundaryError> {
        let future = self.adapter.signed_readback(request);
        match self.runtime.run(self.timeouts.readback, future) {
            TokioRuntimeRun::Completed(Ok(receipt)) => Ok(receipt),
            TokioRuntimeRun::TimedOut => Err(AsyncGatewayBoundaryError::Timeout),
            TokioRuntimeRun::Failed => Err(AsyncGatewayBoundaryError::Runtime),
            TokioRuntimeRun::Completed(Err(AsyncGatewayCallError::Disconnected)) => {
                Err(AsyncGatewayBoundaryError::Disconnected)
            }
            TokioRuntimeRun::Completed(Err(AsyncGatewayCallError::Failed(_))) => {
                Err(AsyncGatewayBoundaryError::Adapter)
            }
        }
    }

    #[cfg(test)]
    fn dispatch_linear(&mut self, permit: DispatchPermit) -> GatewayDispatchResult {
        let execution_now_ms = self.runtime.execution_now_ms();
        let snapshot = self.adapter.capability_snapshot();
        let Ok((admitted_capability, admission_evidence, permit)) =
            permit.into_async_parts(execution_now_ms, &snapshot)
        else {
            return GatewayDispatchResult::Rejected {
                reason_code: "host_admission_invalid".to_owned(),
            };
        };
        let admission_expires_ms = permit.admission_expires_ms();
        let future = self
            .adapter
            .dispatch(admitted_capability, admission_evidence, permit);
        let result = self.runtime.run(self.timeouts.dispatch, future);
        if self.runtime.execution_now_ms() >= admission_expires_ms {
            return GatewayDispatchResult::Unknown;
        }
        match result {
            TokioRuntimeRun::Completed(Ok(result)) => result,
            TokioRuntimeRun::TimedOut
            | TokioRuntimeRun::Failed
            | TokioRuntimeRun::Completed(Err(AsyncGatewayCallError::Disconnected))
            | TokioRuntimeRun::Completed(Err(AsyncGatewayCallError::Failed(_))) => {
                GatewayDispatchResult::Unknown
            }
        }
    }

    #[cfg(not(test))]
    fn dispatch_linear(&mut self, _permit: DispatchPermit) -> GatewayDispatchResult {
        // Real Control/Owner/WAL/Canary authorities are not connected in this composition.
        GatewayDispatchResult::Rejected {
            reason_code: "host_admission_unavailable".to_owned(),
        }
    }
}

impl<G: AsyncPhysicalGateway, R: TokioRuntimeDriver> PhysicalGateway
    for TokioPhysicalGateway<G, R>
{
    type Error = AsyncGatewayBoundaryError;

    fn binding(&self) -> &GatewayBinding {
        self.adapter.binding()
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.adapter.capability_snapshot()
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        self.connect_linear(permit)
    }

    fn signed_readback(
        &mut self,
        request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        self.readback_linear(request.clone())
    }

    fn verify_signed_readback(&self, receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        self.adapter
            .verify_signed_readback(receipt)
            .map_err(|_| AsyncGatewayBoundaryError::Adapter)
    }

    fn dispatch(&mut self, permit: DispatchPermit) -> GatewayDispatchResult {
        self.dispatch_linear(permit)
    }
}

pub(crate) fn validate_capability_preflight(
    binding: &GatewayBinding,
    capability: &CapabilitySnapshot,
) -> Result<(), AsyncGatewayBoundaryError> {
    binding
        .validate()
        .map_err(|_| AsyncGatewayBoundaryError::CapabilityScope)?;
    if capability.binding != *binding {
        return Err(AsyncGatewayBoundaryError::CapabilityScope);
    }
    if capability.flags.is_empty() {
        return Err(AsyncGatewayBoundaryError::CapabilityClosed);
    }
    let recovery_reads = CapabilityFlags::READ_ACCOUNT
        | CapabilityFlags::READ_ORDERS
        | CapabilityFlags::READ_FILLS
        | CapabilityFlags::PRIVATE_STREAM;
    if !capability.flags.contains(recovery_reads)
        || capability.flags.contains(CapabilityFlags::WITHDRAW)
    {
        return Err(AsyncGatewayBoundaryError::CapabilityIncomplete);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AsyncGatewayBoundaryError {
    #[error("async gateway capability is empty and remains fail-closed")]
    CapabilityClosed,
    #[error("async gateway capability does not cover complete recovery read evidence")]
    CapabilityIncomplete,
    #[error("async gateway capability does not match the fixed node binding")]
    CapabilityScope,
    #[error("async gateway timeout must be nonzero")]
    InvalidTimeout,
    #[error("the node's single Tokio runtime failed")]
    Runtime,
    #[error("async gateway operation timed out")]
    Timeout,
    #[error("async gateway disconnected")]
    Disconnected,
    #[error("async gateway operation failed closed")]
    Adapter,
}
