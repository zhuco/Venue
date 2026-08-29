use crate::{
    controller::ControlTarget,
    strategy::scalping::{ExposureState, ProtectionState, SafetyProjection},
};

use super::{
    BinancePrivateFactsWorker, ControlDisposition, CustodyStatus, LifecycleFault, LifecycleInput,
    LifecycleReport, PrivateExposure, PrivateFacts, PrivateFactsReadiness, ScalpingInput,
    report_lifecycle,
};

/// A generation- and watermark-bound authoritative status. Unknown is explicit so composition
/// cannot translate absent execution, ownership, protection, or risk projections into Ready.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrivateProjection<T> {
    pub generation: u64,
    pub observed_at_ms: u64,
    pub value: T,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionProjection {
    Known,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnerProjection {
    Clear,
    Conflict,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtectionProjection {
    Complete,
    Gap,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskBudgetProjection {
    Available,
    Unavailable,
    Unknown,
}

/// Independent authoritative projections needed to turn a worker's committed anonymous account
/// shape into coordinator facts. Every source must match the worker's exact generation and
/// signed-readback watermark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrivateFactsProjectionInput {
    pub execution: PrivateProjection<ExecutionProjection>,
    pub owner: PrivateProjection<OwnerProjection>,
    pub protection: PrivateProjection<ProtectionProjection>,
    pub risk_budget: PrivateProjection<RiskBudgetProjection>,
}

impl PrivateFactsProjectionInput {
    #[must_use]
    pub fn build(self, readiness: PrivateFactsReadiness) -> Option<PrivateFacts> {
        let projections_match = [
            (self.execution.generation, self.execution.observed_at_ms),
            (self.owner.generation, self.owner.observed_at_ms),
            (self.protection.generation, self.protection.observed_at_ms),
            (self.risk_budget.generation, self.risk_budget.observed_at_ms),
        ]
        .into_iter()
        .all(|identity| identity == (readiness.generation, readiness.observed_at_ms));
        if !projections_match
            || matches!(self.execution.value, ExecutionProjection::Unknown)
            || matches!(self.owner.value, OwnerProjection::Unknown)
            || matches!(self.protection.value, ProtectionProjection::Unknown)
            || matches!(self.risk_budget.value, RiskBudgetProjection::Unknown)
        {
            return None;
        }
        let custody = match readiness.exposure {
            PrivateExposure::Flat if readiness.ordinary_order_debt || readiness.algo_order_debt => {
                CustodyStatus::Incomplete
            }
            PrivateExposure::Flat => CustodyStatus::Complete,
            PrivateExposure::Open
                if self.execution.value == ExecutionProjection::Known
                    && self.owner.value == OwnerProjection::Clear
                    && self.protection.value == ProtectionProjection::Complete =>
            {
                CustodyStatus::Complete
            }
            PrivateExposure::Open => CustodyStatus::Incomplete,
        };
        Some(PrivateFacts {
            generation: readiness.generation,
            observed_at_ms: readiness.observed_at_ms,
            root_cause_fact_id: readiness.root_cause_fact_id,
            safety: SafetyProjection {
                private_snapshot_ready: true,
                exposure: match readiness.exposure {
                    PrivateExposure::Flat => ExposureState::Flat,
                    PrivateExposure::Open => ExposureState::Open,
                },
                execution_unknown: self.execution.value != ExecutionProjection::Known,
                protection: match self.protection.value {
                    ProtectionProjection::Complete => ProtectionState::Complete,
                    ProtectionProjection::Gap => ProtectionState::Gap,
                    ProtectionProjection::Unknown => ProtectionState::Unknown,
                },
                owner_conflict: self.owner.value == OwnerProjection::Conflict,
                risk_budget_available: self.risk_budget.value == RiskBudgetProjection::Available,
            },
            custody,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrivateEntryGateInput {
    pub active_episode: bool,
    pub entry_requested: bool,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateEntryGateReport {
    pub lifecycle: LifecycleReport,
    pub entry_ready: bool,
    pub forwarded_private: Option<PrivateFacts>,
    pub control: Option<ControlTarget>,
}

impl PrivateEntryGateReport {
    #[must_use]
    pub fn coordinator_inputs(&self) -> Vec<ScalpingInput> {
        let mut inputs = Vec::with_capacity(2);
        if let Some(control) = self.control {
            inputs.push(ScalpingInput::Control(control));
        }
        if let Some(private) = &self.forwarded_private {
            inputs.push(ScalpingInput::Private(private.clone()));
        }
        inputs
    }
}

/// Bridges the resident worker's durable bootstrap to supervisor and coordinator inputs. It starts
/// fenced and cannot create an authorization, writer, permit, exchange request, or mutation.
#[derive(Clone, Debug, Default)]
pub struct PrivateEntryGate {
    last_forwarded: Option<(u64, u64)>,
    previously_complete: bool,
    retained_private: Option<PrivateFacts>,
    periodic_retention_ms: u64,
}

impl PrivateEntryGate {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Retains the last complete signed projection only during the worker's explicitly marked
    /// periodic readback, and never beyond its configured freshness window.
    #[must_use]
    pub fn with_periodic_retention(periodic_retention_ms: u64) -> Self {
        Self {
            periodic_retention_ms,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn observe_worker(
        &mut self,
        worker: &BinancePrivateFactsWorker,
        projections: PrivateFactsProjectionInput,
        input: PrivateEntryGateInput,
    ) -> PrivateEntryGateReport {
        let private = worker
            .readiness()
            .ok()
            .flatten()
            .and_then(|readiness| projections.build(readiness));
        self.observe_private(private, input)
    }

    /// Uses only the worker's resolver-produced projection from the same committed bootstrap.
    /// Workers opened without an authority, or any projection/readiness error, remain fenced.
    #[must_use]
    pub fn observe_authoritative_worker(
        &mut self,
        worker: &BinancePrivateFactsWorker,
        input: PrivateEntryGateInput,
    ) -> PrivateEntryGateReport {
        let private = worker
            .readiness()
            .ok()
            .flatten()
            .zip(worker.authoritative_projections().ok().flatten())
            .and_then(|(readiness, projections)| projections.build(readiness));
        self.observe_private_or_periodic(private, worker.periodic_readback_in_progress(), input)
    }

    /// Pure form used by schedulers that have already sampled the worker's durable readiness.
    /// It retains the same source-identity checks as `observe_worker`.
    #[must_use]
    pub fn observe_readiness(
        &mut self,
        readiness: Option<PrivateFactsReadiness>,
        projections: PrivateFactsProjectionInput,
        input: PrivateEntryGateInput,
    ) -> PrivateEntryGateReport {
        self.observe_private(readiness.and_then(|value| projections.build(value)), input)
    }

    fn observe_private(
        &mut self,
        private: Option<PrivateFacts>,
        input: PrivateEntryGateInput,
    ) -> PrivateEntryGateReport {
        let Some(private) = private else {
            return self.fence(input);
        };
        self.retained_private = private_complete(&private).then(|| private.clone());
        if !private_complete(&private) {
            if !input.active_episode {
                return self.fence(input);
            }
            let identity = (private.generation, private.observed_at_ms);
            let forward = self.last_forwarded != Some(identity);
            self.last_forwarded = Some(identity);
            self.previously_complete = false;
            let lifecycle = report_lifecycle(LifecycleInput {
                active_episode: true,
                entry_armed: false,
                now_ms: input.now_ms,
                admission_deadline_ms: None,
                fault: Some(LifecycleFault::CapabilityGap),
            });
            return PrivateEntryGateReport {
                lifecycle,
                entry_ready: false,
                forwarded_private: forward.then_some(private),
                control: Some(ControlTarget::StopAndProtect),
            };
        }
        let identity = (private.generation, private.observed_at_ms);
        let forward = self.last_forwarded != Some(identity);
        self.last_forwarded = Some(identity);
        self.previously_complete = true;
        let entry_ready = input.entry_requested && entry_safe(&private);
        let lifecycle = report_lifecycle(LifecycleInput {
            active_episode: input.active_episode,
            entry_armed: entry_ready,
            now_ms: input.now_ms,
            admission_deadline_ms: None,
            fault: None,
        });
        PrivateEntryGateReport {
            lifecycle,
            entry_ready,
            forwarded_private: forward.then_some(private),
            control: None,
        }
    }

    fn observe_private_or_periodic(
        &mut self,
        private: Option<PrivateFacts>,
        periodic_readback_in_progress: bool,
        input: PrivateEntryGateInput,
    ) -> PrivateEntryGateReport {
        if private.is_some() {
            return self.observe_private(private, input);
        }
        let retained = periodic_readback_in_progress
            .then(|| self.retained_private.clone())
            .flatten()
            .filter(|value| {
                input.now_ms >= value.observed_at_ms
                    && input.now_ms.saturating_sub(value.observed_at_ms)
                        <= self.periodic_retention_ms
            });
        self.observe_private(retained, input)
    }

    fn fence(&mut self, input: PrivateEntryGateInput) -> PrivateEntryGateReport {
        let fault = if self.previously_complete {
            LifecycleFault::CapabilityGenerationChanged
        } else {
            LifecycleFault::PrivateReconciliationTransient
        };
        self.previously_complete = false;
        self.last_forwarded = None;
        self.retained_private = None;
        let lifecycle = report_lifecycle(LifecycleInput {
            active_episode: input.active_episode,
            entry_armed: false,
            now_ms: input.now_ms,
            admission_deadline_ms: None,
            fault: Some(fault),
        });
        PrivateEntryGateReport {
            lifecycle,
            entry_ready: false,
            forwarded_private: None,
            control: (lifecycle.control == ControlDisposition::StopAndProtect)
                .then_some(ControlTarget::StopAndProtect),
        }
    }
}

fn private_complete(private: &PrivateFacts) -> bool {
    private.safety.private_snapshot_ready
        && !private.safety.execution_unknown
        && !private.safety.owner_conflict
        && private.safety.protection == ProtectionState::Complete
        && private.custody == CustodyStatus::Complete
}

fn entry_safe(private: &PrivateFacts) -> bool {
    private_complete(private)
        && private.safety.exposure == ExposureState::Flat
        && private.safety.risk_budget_available
}

#[cfg(test)]
mod tests {
    use super::*;

    fn safe_private() -> PrivateFacts {
        PrivateFacts {
            generation: 2,
            observed_at_ms: 1_000,
            root_cause_fact_id: "private-readback:2:1000:3".to_owned(),
            safety: SafetyProjection {
                private_snapshot_ready: true,
                exposure: ExposureState::Flat,
                execution_unknown: false,
                protection: ProtectionState::Complete,
                owner_conflict: false,
                risk_budget_available: true,
            },
            custody: CustodyStatus::Complete,
        }
    }

    #[test]
    fn scheduled_readback_retains_complete_fresh_projection_only_within_ttl() {
        let mut gate = PrivateEntryGate::with_periodic_retention(1_000);
        let initial = gate.observe_private_or_periodic(
            Some(safe_private()),
            false,
            PrivateEntryGateInput {
                active_episode: false,
                entry_requested: true,
                now_ms: 1_000,
            },
        );
        assert!(initial.entry_ready);

        let retained = gate.observe_private_or_periodic(
            None,
            true,
            PrivateEntryGateInput {
                active_episode: false,
                entry_requested: true,
                now_ms: 1_500,
            },
        );
        assert!(retained.entry_ready);
        assert!(retained.forwarded_private.is_none());

        let expired = gate.observe_private_or_periodic(
            None,
            true,
            PrivateEntryGateInput {
                active_episode: false,
                entry_requested: true,
                now_ms: 2_001,
            },
        );
        assert!(!expired.entry_ready);
    }

    #[test]
    fn non_periodic_readback_never_retains_projection() {
        let mut gate = PrivateEntryGate::with_periodic_retention(1_000);
        let _ = gate.observe_private_or_periodic(
            Some(safe_private()),
            false,
            PrivateEntryGateInput {
                active_episode: false,
                entry_requested: true,
                now_ms: 1_000,
            },
        );
        let report = gate.observe_private_or_periodic(
            None,
            false,
            PrivateEntryGateInput {
                active_episode: false,
                entry_requested: true,
                now_ms: 1_001,
            },
        );
        assert!(!report.entry_ready);
        assert!(report.forwarded_private.is_none());
    }
}
