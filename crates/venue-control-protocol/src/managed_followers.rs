//! Saved managed credentials are distinct from enabled follow relationships.
use crate::{
    accounts::{ApiVerificationState, BindCredentialRequest},
    kol::{FollowLifecycleAction, FollowLifecycleState},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::Symbol;

pub const MANAGED_FOLLOWERS_PATH: &str = "/v2/kol/managed-followers";
pub const MANAGED_VERIFY_PATH: &str = "/v2/kol/managed-followers/verify";
pub const MANAGED_FOLLOW_SETTINGS_PATH: &str = "/v2/kol/managed-followers/follow/settings";
pub const MANAGED_FOLLOW_LIFECYCLE_PATH: &str = "/v2/kol/managed-followers/follow/lifecycle";
pub const MANAGED_FOLLOW_STATUS_PATH: &str = "/v2/kol/managed-followers/follow/status";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFollowStatusRequest {
    pub managed_id: String,
}

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

/// Risk inputs deliberately exclude the internal credential identity. The Control service
/// resolves that identity from the KOL-owned managed record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFollowRiskSettings {
    #[serde(default)]
    pub sizing: crate::follow_sizing::FollowSizing,
    #[serde(with = "rust_decimal::serde::str")]
    pub allocated_capital: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub multiplier: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_notional: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub max_total_notional: Decimal,
    pub max_deviation_bps: u32,
    pub allowed_symbols: Vec<Symbol>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFollowSettingsUpsertRequest {
    pub request_id: String,
    pub managed_id: String,
    pub settings: ManagedFollowRiskSettings,
    pub expected_revision: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFollowLifecycleRequest {
    pub request_id: String,
    pub managed_id: String,
    pub relation_id: String,
    pub expected_revision: u64,
    pub action: FollowLifecycleAction,
    pub risk_confirmed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedFollowRelationSummary {
    pub managed_id: String,
    pub relation_id: String,
    pub state: FollowLifecycleState,
    pub revision: u64,
    pub settings: ManagedFollowRiskSettings,
    pub activation_requested: bool,
}
