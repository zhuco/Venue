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

    /// Decodes the relation UUID persisted by Control without adding a UUID runtime dependency.
    pub fn parse(raw: &str) -> Result<Self, CopyIdentityError> {
        let bytes = raw.as_bytes();
        if bytes.len() != 36
            || [8, 13, 18, 23]
                .into_iter()
                .any(|index| bytes[index] != b'-')
        {
            return Err(CopyIdentityError::Identifier);
        }
        let mut output = [0_u8; UUID_BYTES];
        let mut output_index = 0;
        let mut index = 0;
        while index < bytes.len() {
            if [8, 13, 18, 23].contains(&index) {
                index += 1;
                continue;
            }
            let high = hex(bytes[index]).ok_or(CopyIdentityError::Identifier)?;
            let low = hex(bytes[index + 1]).ok_or(CopyIdentityError::Identifier)?;
            output[output_index] = (high << 4) | low;
            output_index += 1;
            index += 2;
        }
        let value = Self(output);
        if value.is_nil() {
            return Err(CopyIdentityError::Identifier);
        }
        Ok(value)
    }
}

const fn hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
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

/// Derives a distinct, replay-stable semantic repair identity from an already durable Copy job
/// and its signed closing fact. It is data for a drift job only; it grants no delivery, writer,
/// WAL, or dispatch authority.
pub fn derive_repair_identities(
    source_job_id: &CopyId,
    receipt_sequence: u64,
    position_fact_digest: &[u8; DIGEST_BYTES],
) -> Result<CopyIdentitySet, CopyIdentityError> {
    if source_job_id.is_nil() || receipt_sequence == 0 || *position_fact_digest == [0; DIGEST_BYTES]
    {
        return Err(CopyIdentityError::RepairInput);
    }
    let sequence = receipt_sequence.to_be_bytes();
    let job_id = derive_uuid(
        b"copy-repair-job-v1",
        &[source_job_id.as_bytes(), &sequence, position_fact_digest],
    );
    if job_id == *source_job_id {
        return Err(CopyIdentityError::RepairInput);
    }
    Ok(CopyIdentitySet {
        job_id,
        planning_snapshot_id: derive_uuid(
            b"copy-repair-snapshot-v1",
            &[source_job_id.as_bytes(), &sequence, position_fact_digest],
        ),
        child_order_id: derive_uuid(
            b"copy-repair-child-v1",
            &[source_job_id.as_bytes(), &sequence, position_fact_digest],
        ),
        idempotency_key: IdempotencyKey(derive(
            b"copy-repair-key-v1",
            &[source_job_id.as_bytes(), &sequence, position_fact_digest],
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

/// Identity of a position-target observation, not a fabricated exchange order. Account and
/// strategy bindings remain stable across refreshed facts; each fact pair gets distinct jobs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopyTargetObservationIdentity {
    pub input: CopyIdentityInput,
    pub leader_id: CopyId,
    pub follower_id: CopyId,
    pub follower_binding_id: CopyId,
    pub policy_id: CopyId,
}

pub fn derive_target_observation_identity(
    relation: &crate::RelationCommitment,
    follower_account: &CopyId,
    leader_binding_digest: &[u8; DIGEST_BYTES],
    follower_binding_digest: &[u8; DIGEST_BYTES],
    paired_fact_digest: &[u8; DIGEST_BYTES],
) -> Result<CopyTargetObservationIdentity, CopyIdentityError> {
    if relation.validate().is_err()
        || follower_account.is_nil()
        || [
            *leader_binding_digest,
            *follower_binding_digest,
            *paired_fact_digest,
        ]
        .contains(&[0; DIGEST_BYTES])
    {
        return Err(CopyIdentityError::Identifier);
    }
    let revision = u32::try_from(relation.revision).map_err(|_| CopyIdentityError::Revision)?;
    let leader_id = derive_uuid(b"copy-target-leader-v1", &[leader_binding_digest]);
    let follower_binding_id = derive_uuid(b"copy-target-follower-v1", &[follower_binding_digest]);
    let policy_id = derive_uuid(
        b"copy-target-policy-v1",
        &[
            relation.relation_id.as_bytes(),
            &relation.revision.to_be_bytes(),
            &relation.policy_digest,
        ],
    );
    let event = derive_uuid(
        b"copy-target-observation-v1",
        &[
            policy_id.as_bytes(),
            leader_id.as_bytes(),
            follower_binding_id.as_bytes(),
            paired_fact_digest,
        ],
    );
    let input = CopyIdentityInput {
        event_id: *event.as_bytes(),
        source_event_id: *event.as_bytes(),
        follower_account_id: *follower_account.as_bytes(),
        follower_binding_id: *follower_binding_id.as_bytes(),
        leader_order_id: *event.as_bytes(),
        revision,
        action: CopyAction::New,
    };
    derive_copy_identities(&input)?;
    Ok(CopyTargetObservationIdentity {
        input,
        leader_id,
        follower_id: *follower_account,
        follower_binding_id,
        policy_id,
    })
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
    #[error("copy repair identity inputs are incomplete or collide with the source job")]
    RepairInput,
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
    fn repair_identity_is_stable_distinct_and_binds_the_signed_fact()
    -> Result<(), CopyIdentityError> {
        let source = derive_copy_identities(&input())?;
        let first = derive_repair_identities(&source.job_id, 4, &[7; DIGEST_BYTES])?;
        assert_eq!(
            first,
            derive_repair_identities(&source.job_id, 4, &[7; DIGEST_BYTES])?
        );
        assert_ne!(first.job_id, source.job_id);
        assert_ne!(
            first,
            derive_repair_identities(&source.job_id, 5, &[7; DIGEST_BYTES])?
        );
        assert_eq!(
            derive_repair_identities(&source.job_id, 0, &[7; DIGEST_BYTES]),
            Err(CopyIdentityError::RepairInput)
        );
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

    #[test]
    fn target_observation_keeps_binding_but_changes_each_frozen_event()
    -> Result<(), CopyIdentityError> {
        let relation = crate::RelationCommitment {
            relation_id: CopyId::parse("00000000-0000-4000-8000-000000000001")?,
            revision: 1,
            policy_digest: [1; DIGEST_BYTES],
        };
        let account = CopyId::parse("00000000-0000-4000-8000-000000000002")?;
        let first = derive_target_observation_identity(
            &relation,
            &account,
            &[2; DIGEST_BYTES],
            &[3; DIGEST_BYTES],
            &[4; DIGEST_BYTES],
        )?;
        assert_eq!(
            first,
            derive_target_observation_identity(
                &relation,
                &account,
                &[2; DIGEST_BYTES],
                &[3; DIGEST_BYTES],
                &[4; DIGEST_BYTES]
            )?
        );
        let next = derive_target_observation_identity(
            &relation,
            &account,
            &[2; DIGEST_BYTES],
            &[3; DIGEST_BYTES],
            &[5; DIGEST_BYTES],
        )?;
        assert_eq!(first.follower_binding_id, next.follower_binding_id);
        assert_eq!(first.leader_id, next.leader_id);
        assert_ne!(
            derive_copy_identities(&first.input)?,
            derive_copy_identities(&next.input)?
        );
        assert!(
            derive_target_observation_identity(
                &relation,
                &account,
                &[0; DIGEST_BYTES],
                &[3; DIGEST_BYTES],
                &[4; DIGEST_BYTES]
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn control_relation_uuid_decodes_to_canonical_copy_identity() {
        assert_eq!(
            CopyId::parse("00000000-0000-4000-8000-000000000001").map(|value| value.to_string()),
            Ok("00000000-0000-4000-8000-000000000001".to_owned())
        );
        assert!(CopyId::parse("not-a-uuid").is_err());
        assert!(CopyId::parse("00000000-0000-0000-0000-000000000000").is_err());
    }
}
