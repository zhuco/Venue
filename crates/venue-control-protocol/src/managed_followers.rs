//! Saved managed credentials are distinct from enabled follow relationships.
use crate::accounts::{ApiVerificationState, BindCredentialRequest};
use serde::{Deserialize, Serialize};

pub const MANAGED_FOLLOWERS_PATH: &str = "/v2/kol/managed-followers";
pub const MANAGED_VERIFY_PATH: &str = "/v2/kol/managed-followers/verify";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFollowerCreateRequest {
    pub request_id: String,
    pub credential: BindCredentialRequest,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFollowerVerifyRequest {
    pub managed_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFollowerSummary {
    pub managed_id: String,
    pub label: String,
    pub masked_key: String,
    pub verification: ApiVerificationState,
    pub verified_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFollowers {
    pub can_manage: bool,
    pub accounts: Vec<ManagedFollowerSummary>,
}
