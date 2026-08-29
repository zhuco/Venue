use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const UUID_BYTES: usize = 16;
const DIGEST_BYTES: usize = 32;

/// The accepted leader action is part of the durable copy-job identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CopyAction {
    New,
    Amend,
    Cancel,
}

impl CopyAction {
    const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::New => b"NEW",
            Self::Amend => b"AMEND",
            Self::Cancel => b"CANCEL",
        }
    }
}

/// Frozen UUID bytes required to reproduce the KOL planner's accepted identity contract.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyIdentityInput {
    pub event_id: [u8; UUID_BYTES],
    pub source_event_id: [u8; UUID_BYTES],
    pub follower_account_id: [u8; UUID_BYTES],
    pub follower_binding_id: [u8; UUID_BYTES],
    pub leader_order_id: [u8; UUID_BYTES],
    pub revision: u32,
    pub action: CopyAction,
}

/// One deterministic UUID-shaped identity. It is data only and grants no execution authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CopyId([u8; UUID_BYTES]);

impl CopyId {
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; UUID_BYTES] {
        &self.0
    }

    #[must_use]
    pub fn is_nil(&self) -> bool {
        self.0 == [0; UUID_BYTES]
    }
}

impl fmt::Display for CopyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, byte) in self.0.iter().enumerate() {
            if matches!(index, 4 | 6 | 8 | 10) {
                formatter.write_str("-")?;
            }
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Full SHA-256 idempotency commitment persisted alongside the derived job.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct IdempotencyKey([u8; DIGEST_BYTES]);

impl IdempotencyKey {
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DIGEST_BYTES] {
        &self.0
    }

    #[must_use]
    pub fn is_zero(&self) -> bool {
        self.0 == [0; DIGEST_BYTES]
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Stable identities for one frozen leader event and follower binding.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyIdentitySet {
    pub job_id: CopyId,
    pub planning_snapshot_id: CopyId,
    pub child_order_id: CopyId,
    pub idempotency_key: IdempotencyKey,
}

/// Reproduces KOL's length-delimited, domain-separated identity algorithm without importing its
/// planner, database, runtime, or UUID dependency. All inputs must already be frozen and durable.
pub fn derive_copy_identities(
    input: &CopyIdentityInput,
) -> Result<CopyIdentitySet, CopyIdentityError> {
    if input.revision == 0 {
        return Err(CopyIdentityError::Revision);
    }
    if [
        input.event_id,
        input.source_event_id,
        input.follower_account_id,
        input.follower_binding_id,
        input.leader_order_id,
    ]
    .contains(&[0; UUID_BYTES])
    {
        return Err(CopyIdentityError::Identifier);
    }

    let revision = input.revision.to_be_bytes();
    let action = input.action.as_bytes();
    Ok(CopyIdentitySet {
        job_id: derive_uuid(
            b"copy-job-v1",
            &[
                &input.event_id,
                &input.follower_account_id,
                &revision,
                action,
            ],
        ),
        planning_snapshot_id: derive_uuid(
            b"planning-snapshot-v1",
            &[&input.source_event_id, &input.follower_binding_id],
        ),
        child_order_id: derive_uuid(
            b"child-order-v1",
            &[&input.leader_order_id, &input.follower_binding_id],
        ),
        idempotency_key: IdempotencyKey(derive(
            b"copy-job-key-v1",
            &[
                &input.event_id,
                &input.follower_account_id,
                &revision,
                action,
            ],
        )),
    })
}

fn derive_uuid(domain: &[u8], parts: &[&[u8]]) -> CopyId {
    let digest = derive(domain, parts);
    let mut bytes = [0_u8; UUID_BYTES];
    bytes.copy_from_slice(&digest[..UUID_BYTES]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    CopyId(bytes)
}

fn derive(domain: &[u8], parts: &[&[u8]]) -> [u8; DIGEST_BYTES] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update([0]);
    for part in parts {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    hash.finalize().into()
}

pub(crate) fn derive_commitment(domain: &[u8], parts: &[&[u8]]) -> [u8; DIGEST_BYTES] {
    derive(domain, parts)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CopyIdentityError {
    #[error("copy identity inputs must be non-zero stable UUID bytes")]
    Identifier,
    #[error("copy event revision must be positive")]
    Revision,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> CopyIdentityInput {
        CopyIdentityInput {
            event_id: [1; UUID_BYTES],
            source_event_id: [2; UUID_BYTES],
            follower_account_id: [3; UUID_BYTES],
            follower_binding_id: [4; UUID_BYTES],
            leader_order_id: [5; UUID_BYTES],
            revision: 7,
            action: CopyAction::New,
        }
    }

    #[test]
    fn replay_is_stable_and_domains_are_separated() -> Result<(), CopyIdentityError> {
        let identities = derive_copy_identities(&input())?;
        assert_eq!(derive_copy_identities(&input())?, identities);
        assert_ne!(identities.job_id, identities.planning_snapshot_id);
        assert_ne!(identities.job_id, identities.child_order_id);
        assert_eq!(
            identities.job_id.to_string(),
            "979f862e-07d3-5541-a6e9-75c8146c39a3"
        );
        assert_eq!(
            identities.planning_snapshot_id.to_string(),
            "72dcd7d4-ea47-55bc-bb89-2369b89480f0"
        );
        assert_eq!(
            identities.child_order_id.to_string(),
            "2b28120a-0eb0-517d-935f-840928213903"
        );
        assert_eq!(
            identities.idempotency_key.to_string(),
            "d1152ca7869fcb43dd132e50d26dfaab11ff5ce0ee52807ec527611b640ce7fc"
        );
        Ok(())
    }

    #[test]
    fn revision_and_action_change_only_the_job_contract() -> Result<(), CopyIdentityError> {
        let baseline = derive_copy_identities(&input())?;
        let mut changed = input();
        changed.revision = 8;
        changed.action = CopyAction::Cancel;
        let changed = derive_copy_identities(&changed)?;
        assert_ne!(baseline.job_id, changed.job_id);
        assert_ne!(baseline.idempotency_key, changed.idempotency_key);
        assert_eq!(baseline.planning_snapshot_id, changed.planning_snapshot_id);
        assert_eq!(baseline.child_order_id, changed.child_order_id);
        Ok(())
    }

    #[test]
    fn malformed_frozen_identity_fails_closed() {
        let mut invalid = input();
        invalid.follower_binding_id = [0; UUID_BYTES];
        assert_eq!(
            derive_copy_identities(&invalid),
            Err(CopyIdentityError::Identifier)
        );
        invalid = input();
        invalid.revision = 0;
        assert_eq!(
            derive_copy_identities(&invalid),
            Err(CopyIdentityError::Revision)
        );
    }
}
