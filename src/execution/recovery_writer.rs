use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use fs2::FileExt;
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    domain::{
        CancelCommand, CommandId, Instrument, NativeOrderFamily, OrderCommand, OrderOwner,
        OrderPurpose, OrderSide, Position, PositionSide, Price, Symbol,
    },
    risk::authorize_reduction,
};

use super::external_algo_cleanup::{ExternalAlgoCancelCommand, ExternalAlgoCustody};

use super::{CanaryRunBinding, CommandJournal};

const PROOF_MAX_AGE_MS: u64 = 30_000;
const PERMIT_TTL_MS: u64 = 500;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryWriterScope {
    pub exchange: String,
    pub account: String,
    pub symbol: Symbol,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryObservationProof {
    pub generation: u64,
    pub observed_at_ms: u64,
    pub valid_until_ms: u64,
    pub payload_sha256: String,
    pub signature_verified: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct RecoveryCancelInput<'a> {
    pub binding: &'a CanaryRunBinding,
    pub original_command_id: &'a str,
    pub client_id: &'a str,
    pub family: NativeOrderFamily,
    pub commands: &'a CommandJournal,
    pub proof: &'a RecoveryObservationProof,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryCancelAuthorization {
    pub command: CancelCommand,
    permit: RecoveryPermit,
}

#[derive(Clone, Copy, Debug)]
pub struct RecoveryReduceInput<'a> {
    pub binding: &'a CanaryRunBinding,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub instrument: &'a Instrument,
    pub market_price: Price,
    pub market_price_valid_until_ms: u64,
    pub commands: &'a CommandJournal,
    pub proof: &'a RecoveryObservationProof,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryReduceAuthorization {
    pub command: OrderCommand,
    pub position: Position,
    pub instrument: Instrument,
    permit: RecoveryPermit,
}

#[derive(Clone, Copy, Debug)]
pub struct ExternalAlgoCancelInput<'a> {
    pub scope: &'a RecoveryWriterScope,
    pub custody: &'a ExternalAlgoCustody,
    pub proof: &'a RecoveryObservationProof,
    pub now_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalAlgoCancelAuthorization {
    pub command: ExternalAlgoCancelCommand,
    permit: RecoveryPermit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActionKind {
    Cancel,
    Reduce,
    ExternalAlgoCancel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveryPermit {
    scope: RecoveryWriterScope,
    action: ActionKind,
    command_sha256: String,
    valid_until_ms: u64,
}

#[derive(Debug)]
pub struct RecoveryWriterAuthority {
    authority_path: PathBuf,
    scope: RecoveryWriterScope,
}

/// Owns the exact same `writer.json.lock` used by the normal writer. It has no writer session and
/// exposes only cancel/reduce dispatch methods, so an expired entry writer is never revived.
#[derive(Debug)]
pub struct RecoveryDispatchGuard {
    file: std::fs::File,
}

impl RecoveryWriterAuthority {
    pub fn open(
        authority_path: impl Into<PathBuf>,
        scope: RecoveryWriterScope,
    ) -> Result<Self, RecoveryWriterError> {
        let authority_path = authority_path.into();
        validate_scope(&scope)?;
        if !authority_path.is_absolute() {
            return Err(RecoveryWriterError::Path);
        }
        Ok(Self {
            authority_path,
            scope,
        })
    }

    pub fn dispatch_cancel(
        &self,
        authorization: &RecoveryCancelAuthorization,
        now_ms: u64,
    ) -> Result<RecoveryDispatchGuard, RecoveryWriterError> {
        validate_permit(
            &authorization.permit,
            &self.scope,
            ActionKind::Cancel,
            &authorization.command,
            now_ms,
        )?;
        self.lock()
    }

    pub fn dispatch_reduce(
        &self,
        authorization: &RecoveryReduceAuthorization,
        now_ms: u64,
    ) -> Result<RecoveryDispatchGuard, RecoveryWriterError> {
        validate_permit(
            &authorization.permit,
            &self.scope,
            ActionKind::Reduce,
            &authorization.command,
            now_ms,
        )?;
        self.lock()
    }

    /// Acquires the canonical writer lock before the signed cleanup readback. The caller still
    /// needs a short-lived, payload-bound permit before the guard can dispatch a mutation.
    pub(crate) fn lock_external_algo_cleanup(
        &self,
    ) -> Result<RecoveryDispatchGuard, RecoveryWriterError> {
        self.lock()
    }

    fn lock(&self) -> Result<RecoveryDispatchGuard, RecoveryWriterError> {
        let parent = self
            .authority_path
            .parent()
            .ok_or(RecoveryWriterError::Path)?;
        std::fs::create_dir_all(parent).map_err(RecoveryWriterError::Io)?;
        let path = sibling(&self.authority_path, ".lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(RecoveryWriterError::Io)?;
        let mut last_error = None;
        for attempt in 0..100 {
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(RecoveryDispatchGuard { file }),
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 99 {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        }
        Err(RecoveryWriterError::Lock(last_error.unwrap_or_else(|| {
            std::io::Error::other("recovery writer lock unavailable")
        })))
    }
}

impl Drop for RecoveryDispatchGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

pub fn authorize_recovery_cancel(
    input: RecoveryCancelInput<'_>,
) -> Result<RecoveryCancelAuthorization, RecoveryWriterError> {
    validate_binding(input.binding)?;
    validate_proof(input.proof, input.binding, input.now_ms)?;
    let original_command_id =
        CommandId::new(input.original_command_id).map_err(RecoveryWriterError::Command)?;
    let client_id = CommandId::new(input.client_id).map_err(RecoveryWriterError::Command)?;
    let identity = input
        .commands
        .order_identity(&original_command_id)
        .ok_or(RecoveryWriterError::Identity)?;
    if identity.client_id != &client_id
        || identity.family != input.family
        || identity.owner.exchange != input.binding.exchange
        || identity.owner.account != input.binding.account
        || identity.owner.symbol != input.binding.symbol
    {
        return Err(RecoveryWriterError::Identity);
    }
    let command = CancelCommand {
        command_id: derived_id(
            "recovery_cancel",
            &(
                input.binding,
                input.original_command_id,
                input.client_id,
                input.family,
                &input.proof.payload_sha256,
            ),
        )?,
        owner: identity.owner.clone(),
        target_client_order_id: client_id,
    };
    let permit = permit(
        input.binding,
        ActionKind::Cancel,
        &command,
        input.proof,
        input.now_ms,
        input.proof.valid_until_ms,
    )?;
    Ok(RecoveryCancelAuthorization { command, permit })
}

pub fn authorize_recovery_reduce(
    input: RecoveryReduceInput<'_>,
) -> Result<RecoveryReduceAuthorization, RecoveryWriterError> {
    validate_binding(input.binding)?;
    validate_proof(input.proof, input.binding, input.now_ms)?;
    if input.commands.has_unresolved_entry_or_reduce()
        || !matches!(
            input.position_side,
            PositionSide::Long | PositionSide::Short
        )
        || !input.quantity.is_sign_positive()
        || input.quantity.is_zero()
        || input.instrument.symbol != input.binding.symbol
        || input.market_price_valid_until_ms <= input.now_ms
    {
        return Err(RecoveryWriterError::Reduce);
    }
    let position = Position {
        symbol: input.binding.symbol.clone(),
        side: input.position_side,
        quantity: input.quantity,
        entry_price: None,
        mark_price: Some(input.market_price),
    };
    let identity = (
        input.binding,
        input.position_side,
        input.quantity,
        input.market_price,
        &input.proof.payload_sha256,
    );
    let command = OrderCommand {
        command_id: derived_id("recovery_reduce", &identity)?,
        client_order_id: derived_id("vrr", &identity)?,
        owner: OrderOwner {
            strategy_instance_id: "canary_recovery".to_owned(),
            run_id: derived_id("recovery_run", &input.binding.canary_id)?
                .as_str()
                .to_owned(),
            exchange: input.binding.exchange.clone(),
            account: input.binding.account.clone(),
            symbol: input.binding.symbol.clone(),
            purpose: OrderPurpose::Reduce,
        },
        side: match input.position_side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
            PositionSide::Net => return Err(RecoveryWriterError::Reduce),
        },
        position_side: input.position_side,
        quantity: input.quantity,
        limit_price: input.market_price,
        reduce_only: true,
    };
    authorize_reduction(&command, input.instrument, &position)?;
    let permit = permit(
        input.binding,
        ActionKind::Reduce,
        &command,
        input.proof,
        input.now_ms,
        input.market_price_valid_until_ms,
    )?;
    Ok(RecoveryReduceAuthorization {
        command,
        position,
        instrument: input.instrument.clone(),
        permit,
    })
}

pub fn authorize_external_algo_cancel(
    input: ExternalAlgoCancelInput<'_>,
) -> Result<ExternalAlgoCancelAuthorization, RecoveryWriterError> {
    validate_scope(input.scope)?;
    validate_external_proof(input.proof, input.now_ms)?;
    input.custody.validate(input.scope)?;
    let command = ExternalAlgoCancelCommand {
        custody: input.custody.clone(),
        signed_payload_sha256: input.proof.payload_sha256.clone(),
        observed_at_ms: input.proof.observed_at_ms,
    };
    let valid_until_ms = input.proof.valid_until_ms.min(
        input
            .now_ms
            .checked_add(PERMIT_TTL_MS)
            .ok_or(RecoveryWriterError::Clock)?,
    );
    if valid_until_ms <= input.now_ms {
        return Err(RecoveryWriterError::Expired);
    }
    let permit = RecoveryPermit {
        scope: input.scope.clone(),
        action: ActionKind::ExternalAlgoCancel,
        command_sha256: digest(&command)?,
        valid_until_ms,
    };
    Ok(ExternalAlgoCancelAuthorization { command, permit })
}

pub(crate) fn validate_recovery_cancel_dispatch(
    authorization: &RecoveryCancelAuthorization,
    now_ms: u64,
) -> Result<(), RecoveryWriterError> {
    validate_permit(
        &authorization.permit,
        &authorization.permit.scope,
        ActionKind::Cancel,
        &authorization.command,
        now_ms,
    )
}

pub(crate) fn validate_recovery_reduce_dispatch(
    authorization: &RecoveryReduceAuthorization,
    now_ms: u64,
) -> Result<(), RecoveryWriterError> {
    validate_permit(
        &authorization.permit,
        &authorization.permit.scope,
        ActionKind::Reduce,
        &authorization.command,
        now_ms,
    )
}

pub(crate) fn validate_external_algo_cancel_dispatch(
    authorization: &ExternalAlgoCancelAuthorization,
    now_ms: u64,
) -> Result<(), RecoveryWriterError> {
    validate_permit(
        &authorization.permit,
        &authorization.permit.scope,
        ActionKind::ExternalAlgoCancel,
        &authorization.command,
        now_ms,
    )
}

fn permit(
    binding: &CanaryRunBinding,
    action: ActionKind,
    command: &impl Serialize,
    proof: &RecoveryObservationProof,
    now_ms: u64,
    action_valid_until_ms: u64,
) -> Result<RecoveryPermit, RecoveryWriterError> {
    let valid_until_ms = [
        proof.valid_until_ms,
        action_valid_until_ms,
        now_ms
            .checked_add(PERMIT_TTL_MS)
            .ok_or(RecoveryWriterError::Clock)?,
    ]
    .into_iter()
    .min()
    .ok_or(RecoveryWriterError::Expired)?;
    if valid_until_ms <= now_ms {
        return Err(RecoveryWriterError::Expired);
    }
    Ok(RecoveryPermit {
        scope: RecoveryWriterScope {
            exchange: binding.exchange.clone(),
            account: binding.account.clone(),
            symbol: binding.symbol.clone(),
        },
        action,
        command_sha256: digest(command)?,
        valid_until_ms,
    })
}

fn validate_permit(
    permit: &RecoveryPermit,
    scope: &RecoveryWriterScope,
    action: ActionKind,
    command: &impl Serialize,
    now_ms: u64,
) -> Result<(), RecoveryWriterError> {
    if permit.scope != *scope
        || permit.action != action
        || permit.valid_until_ms <= now_ms
        || permit.command_sha256 != digest(command)?
    {
        return Err(RecoveryWriterError::Permit);
    }
    Ok(())
}

fn validate_binding(binding: &CanaryRunBinding) -> Result<(), RecoveryWriterError> {
    if binding.canary_id.trim().is_empty()
        || binding.exchange.trim().is_empty()
        || binding.account.trim().is_empty()
        || binding.owner_scope.trim().is_empty()
        || binding.release_id.trim().is_empty()
        || binding.position_side == PositionSide::Net
        || binding.writer_generation == 0
        || binding.readback_generation == 0
    {
        return Err(RecoveryWriterError::Binding);
    }
    Ok(())
}

fn validate_scope(scope: &RecoveryWriterScope) -> Result<(), RecoveryWriterError> {
    if scope.exchange.trim().is_empty() || scope.account.trim().is_empty() {
        return Err(RecoveryWriterError::Scope);
    }
    Ok(())
}

fn validate_proof(
    proof: &RecoveryObservationProof,
    binding: &CanaryRunBinding,
    now_ms: u64,
) -> Result<(), RecoveryWriterError> {
    if proof.generation != binding.readback_generation
        || proof.observed_at_ms > now_ms
        || proof.valid_until_ms <= now_ms
        || now_ms.saturating_sub(proof.observed_at_ms) > PROOF_MAX_AGE_MS
        || !proof.signature_verified
        || !valid_digest(&proof.payload_sha256)
    {
        return Err(RecoveryWriterError::Proof);
    }
    Ok(())
}

fn validate_external_proof(
    proof: &RecoveryObservationProof,
    now_ms: u64,
) -> Result<(), RecoveryWriterError> {
    if proof.generation == 0
        || proof.observed_at_ms > now_ms
        || proof.valid_until_ms <= now_ms
        || now_ms.saturating_sub(proof.observed_at_ms) > PROOF_MAX_AGE_MS
        || !proof.signature_verified
        || !valid_digest(&proof.payload_sha256)
    {
        return Err(RecoveryWriterError::Proof);
    }
    Ok(())
}

fn derived_id(prefix: &str, value: &impl Serialize) -> Result<CommandId, RecoveryWriterError> {
    let digest = digest(value)?;
    let suffix_len = 35_usize
        .checked_sub(prefix.len())
        .filter(|length| *length > 0)
        .ok_or(RecoveryWriterError::Identity)?;
    CommandId::new(format!("{prefix}_{}", &digest[..suffix_len]))
        .map_err(RecoveryWriterError::Command)
}

fn digest(value: &impl Serialize) -> Result<String, RecoveryWriterError> {
    let bytes = serde_json::to_vec(value).map_err(RecoveryWriterError::Encode)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let mut name: std::ffi::OsString = path.file_name().map_or_else(|| "writer".into(), Into::into);
    name.push(suffix);
    path.with_file_name(name)
}

#[derive(Debug, thiserror::Error)]
pub enum RecoveryWriterError {
    #[error("recovery writer authority path must be absolute")]
    Path,
    #[error("recovery writer scope is invalid")]
    Scope,
    #[error("recovery run binding is invalid")]
    Binding,
    #[error("signed recovery observation proof is invalid or stale")]
    Proof,
    #[error("recovery target identity does not match the durable command journal")]
    Identity,
    #[error("recovery reduction is not an exact current Hedge leg")]
    Reduce,
    #[error("recovery dispatch permit is invalid, changed, or expired")]
    Permit,
    #[error("recovery dispatch permit clock overflowed")]
    Clock,
    #[error("recovery dispatch permit expired")]
    Expired,
    #[error("recovery writer lock is unavailable: {0}")]
    Lock(#[source] std::io::Error),
    #[error("recovery writer I/O failed: {0}")]
    Io(#[source] std::io::Error),
    #[error("recovery writer encoding failed: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("recovery command identity is invalid: {0}")]
    Command(#[source] crate::domain::CommandError),
    #[error("recovery reduction risk check failed: {0}")]
    Risk(#[from] crate::risk::RiskError),
}
