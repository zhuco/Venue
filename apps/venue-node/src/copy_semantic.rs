use serde::Deserialize;
use serde_json::Value;
use venue_control_protocol::AccountDeliveryPayload;
use venue_copy::{FollowerDeliveryManifest, TargetExposurePlan};
use venue_domain::domain::{OrderOwner, OrderPurpose};
use venue_runtime::{AccountKey, StrategyBinding, StrategyInstanceKey, StrategyKind};

use crate::ActorDeliveryTurn;

#[derive(Debug, Deserialize)]
struct WireCopySemanticJob {
    target: TargetExposurePlan,
    leader_intent: Value,
}

/// Validated, mutation-free Copy work for one exact follower actor.
///
/// Control supplies immutable data only. This value deliberately has no execution-command
/// conversion: a runtime must first durably apply it against its recovered Actor/WAL state and
/// then let the existing account lane decide whether a semantic intent is admissible.
#[derive(Clone, Debug)]
pub struct CopySemanticDelivery {
    manifest: FollowerDeliveryManifest,
    target: TargetExposurePlan,
    actor: StrategyBinding,
    owner: OrderOwner,
    delivery_digest: [u8; 32],
    durable_inbox_digest: [u8; 32],
    durable_inbox_sequence: u64,
    durable_inbox_root_digest: [u8; 32],
}

impl CopySemanticDelivery {
    pub fn from_actor_turn(
        turn: &ActorDeliveryTurn,
        now_ms: u64,
    ) -> Result<Self, CopySemanticError> {
        let AccountDeliveryPayload::CopySemanticJob(job) = turn.payload() else {
            return Err(CopySemanticError::Kind);
        };
        if now_ms == 0 || now_ms >= job.expires_at_ms {
            return Err(CopySemanticError::Expired);
        }
        let manifest: FollowerDeliveryManifest = serde_json::from_value(job.manifest.clone())
            .map_err(|_| CopySemanticError::Manifest)?;
        manifest
            .validate(now_ms)
            .map_err(|_| CopySemanticError::Manifest)?;
        let semantic: WireCopySemanticJob = serde_json::from_value(job.semantic_job.clone())
            .map_err(|_| CopySemanticError::SemanticJob)?;
        if semantic.leader_intent.is_null()
            || job.job_id != manifest.identities.job_id.to_string()
            || job.job_digest != manifest.plan_digest
            || job.symbol != manifest.binding.instrument.symbol
            || job.created_at_ms != manifest.issued_at_ms
            || job.expires_at_ms != manifest.expires_at_ms
            || semantic.target.snapshot_generation != manifest.snapshot_generation
            || manifest.binding.account_id != turn.lease().binding.trading_account_id
            || manifest.binding.instrument.symbol != turn.lease().binding.symbol
        {
            return Err(CopySemanticError::Binding);
        }
        let account = AccountKey::new(
            turn.lease().binding.venue,
            turn.lease().binding.trading_account_id.clone(),
        )
        .map_err(|_| CopySemanticError::Binding)?;
        let key = StrategyInstanceKey::new(
            account,
            StrategyKind::Copy,
            turn.lease().binding.instance_id.clone(),
            turn.lease().binding.symbol.clone(),
        )
        .map_err(|_| CopySemanticError::Binding)?;
        let run_id = manifest.binding.follower_binding_id.to_string();
        let actor = StrategyBinding::new(key, run_id.clone(), encode_hex(&manifest.plan_digest))
            .map_err(|_| CopySemanticError::Binding)?;
        let owner = OrderOwner {
            strategy_instance_id: actor.key.instance_id.clone(),
            run_id,
            exchange: actor.key.account.exchange.as_str().to_owned(),
            account: actor.key.account.account.clone(),
            symbol: actor.key.symbol.clone(),
            purpose: owner_purpose(&semantic.target),
        };
        owner.validate().map_err(|_| CopySemanticError::Binding)?;
        Ok(Self {
            delivery_digest: manifest.delivery_digest(),
            manifest,
            target: semantic.target,
            actor,
            owner,
            durable_inbox_digest: turn.durable_inbox_digest(),
            durable_inbox_sequence: turn.durable_inbox_sequence(),
            durable_inbox_root_digest: turn.durable_inbox_root_digest(),
        })
    }

    #[must_use]
    pub const fn manifest(&self) -> &FollowerDeliveryManifest {
        &self.manifest
    }

    #[must_use]
    pub const fn target(&self) -> &TargetExposurePlan {
        &self.target
    }

    #[must_use]
    pub const fn actor(&self) -> &StrategyBinding {
        &self.actor
    }

    #[must_use]
    pub const fn owner(&self) -> &OrderOwner {
        &self.owner
    }

    #[must_use]
    pub const fn delivery_digest(&self) -> [u8; 32] {
        self.delivery_digest
    }

    #[must_use]
    pub const fn durable_inbox_digest(&self) -> [u8; 32] {
        self.durable_inbox_digest
    }

    #[must_use]
    pub const fn durable_inbox_sequence(&self) -> u64 {
        self.durable_inbox_sequence
    }

    #[must_use]
    pub const fn durable_inbox_root_digest(&self) -> [u8; 32] {
        self.durable_inbox_root_digest
    }

    pub fn runtime_commitment(
        &self,
    ) -> Result<venue_runtime::account::CopyActorCommitment, CopySemanticError> {
        venue_runtime::account::CopyActorCommitment::new(
            self.delivery_digest,
            self.durable_inbox_digest,
            self.durable_inbox_sequence,
            self.durable_inbox_root_digest,
        )
        .map_err(|_| CopySemanticError::Binding)
    }

    /// Applies only the semantic Copy checkpoint through a recovered runtime. A missing real WAL
    /// head or an unready runtime fails closed; this method has no path to an execution command.
    pub fn apply_to_runtime(
        &self,
        runtime: &mut venue_runtime::account::AccountRuntime,
    ) -> Result<venue_runtime::account::CopyActorAppliedReceipt, CopySemanticError> {
        let commitment = self.runtime_commitment()?;
        runtime
            .apply_copy_actor_turn(&self.actor, commitment)
            .map_err(|_| CopySemanticError::RuntimeUnavailable)
    }

    #[must_use]
    pub const fn grants_gateway_capability(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_writer_lease(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_wal_authority(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_dispatch_permit(&self) -> bool {
        false
    }
}

fn owner_purpose(target: &TargetExposurePlan) -> OrderPurpose {
    let target_value = target.target_exposure.value;
    let delta_value = target.delta_exposure.value;
    if target_value.is_zero()
        || (!target_value.is_zero()
            && !delta_value.is_zero()
            && target_value.is_sign_positive() != delta_value.is_sign_positive())
    {
        OrderPurpose::Reduce
    } else {
        OrderPurpose::Entry
    }
}

fn encode_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CopySemanticError {
    #[error("delivery is not a Copy semantic job")]
    Kind,
    #[error("Copy semantic job has expired")]
    Expired,
    #[error("Copy delivery manifest is invalid")]
    Manifest,
    #[error("Copy semantic job is invalid")]
    SemanticJob,
    #[error("Copy semantic job conflicts with its exact follower binding")]
    Binding,
    #[error("Copy runtime is not durably recovered and ready for this Actor turn")]
    RuntimeUnavailable,
}
