use super::*;

#[derive(Clone, Debug)]
pub struct PrivateRoutePlan {
    pub(super) base_revision: u64,
    pub(super) strategy_state_revision: u64,
    pub(super) connection_generation: u64,
    pub(super) evidence_sequence: u64,
    pub(super) next_router: PrivateRouter,
    pub(super) report: PrivateRouteReport,
}

impl PrivateRoutePlan {
    #[must_use]
    pub const fn report(&self) -> &PrivateRouteReport {
        &self.report
    }
}

/// Opaque acknowledgement that every delivery in a route plan was appended to the durable actor
/// inbox transaction. Only then may AccountRuntime commit router cursor and in-memory mailboxes.
#[derive(Clone, Debug)]
pub struct PersistedPrivateDispatchReceipt {
    pub(super) plan: PrivateRoutePlan,
}

impl PersistedPrivateDispatchReceipt {
    pub(crate) fn persisted(plan: PrivateRoutePlan) -> Self {
        Self { plan }
    }
}

impl AccountRuntime {
    /// Opens the one normalized account facts journal used to make private adapter events
    /// durable. It is attached once; callers never receive the journal or a persistence flag.
    pub fn attach_private_ingress(
        &mut self,
        facts_path: std::path::PathBuf,
    ) -> Result<(), AccountRuntimeError> {
        if self.private_ingress.is_some() {
            return Err(AccountRuntimeError::PrivateIngressAttached);
        }
        self.private_ingress = Some(AccountPrivateIngress::open(facts_path)?);
        Ok(())
    }

    /// Fsyncs one normalized private event through the attached account facts journal, then
    /// immediately routes and commits it in this Runtime. No caller may claim persistence or
    /// advance the private cursor independently.
    pub fn ingest_private(
        &mut self,
        input: AccountPrivateFactInput,
    ) -> Result<PrivateRouteReport, AccountRuntimeError> {
        let fact = self
            .private_ingress
            .as_mut()
            .ok_or(AccountRuntimeError::PrivateIngressUnavailable)?
            .persist(input)?;
        self.ingest_private_fact(fact)
    }

    /// Accepts only a fact reconstructed by the account facts journal after fsync. Routing and
    /// committing happen in one Runtime call, so no caller can claim a private delivery was
    /// persisted or advance the router with a boolean acknowledgement.
    pub(crate) fn ingest_private_fact(
        &mut self,
        fact: PersistedPrivateFact,
    ) -> Result<PrivateRouteReport, AccountRuntimeError> {
        let plan = self.plan_private_route(fact)?;
        self.commit_private_route(PersistedPrivateDispatchReceipt::persisted(plan))
    }
}
