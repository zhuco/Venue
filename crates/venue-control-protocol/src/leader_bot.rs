//! Account-owned order mirroring controls. UI permissions are display hints; the server and
//! executor independently enforce the current database grant before creating risk.
use crate::kol::KolProfileState;
use serde::{Deserialize, Serialize};

pub const LEADER_BOT_PATH: &str = "/v2/kol/leader-bot";
pub const LEADER_BOT_LIFECYCLE_PATH: &str = "/v2/kol/leader-bot/lifecycle";
pub const LEADER_BOT_SCHEMA_VERSION: u16 = 1;
pub const MIRROR_ORDERS_PATH: &str = "/v2/kol/follow/orders";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorOrderSummary {
    pub mirror_id: String,
    pub symbol: venue_domain::Symbol,
    pub source_order_id: String,
    pub child_client_order_id: String,
    pub state: String,
    pub requested_quantity: String,
    pub filled_quantity: String,
    pub attention_code: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaderBotState {
    Stopped,
    Running,
    Draining,
    NeedsAttention,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderBotSummary {
    pub bot_id: String,
    pub trading_account_id: String,
    pub credential_id: String,
    pub state: LeaderBotState,
    pub revision: u64,
    pub active_followers: u32,
    pub pending_orders: u32,
    pub attention_code: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderBotAccess {
    pub schema_version: u16,
    pub profile_state: Option<KolProfileState>,
    pub can_use: bool,
    pub permission_revision: u64,
    // Existing exposure remains inspectable after revocation; only the creation entry hides.
    pub bot: Option<LeaderBotSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderBotCreateRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub credential_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaderBotAction {
    Start,
    Stop,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderBotLifecycleRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub bot_id: String,
    pub expected_revision: u64,
    pub action: LeaderBotAction,
    pub risk_confirmed: bool,
}

pub fn valid_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

impl LeaderBotCreateRequest {
    pub fn valid(&self) -> bool {
        self.schema_version == LEADER_BOT_SCHEMA_VERSION
            && valid_id(&self.request_id)
            && valid_id(&self.credential_id)
    }
}

impl LeaderBotLifecycleRequest {
    pub fn valid(&self) -> bool {
        self.schema_version == LEADER_BOT_SCHEMA_VERSION
            && valid_id(&self.request_id)
            && valid_id(&self.bot_id)
            && self.expected_revision > 0
            && (self.action == LeaderBotAction::Start) == self.risk_confirmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn client_cannot_smuggle_a_grant_or_owner() {
        assert!(serde_json::from_str::<LeaderBotCreateRequest>(
            r#"{"schema_version":1,"request_id":"00000000-0000-4000-8000-000000000001","credential_id":"00000000-0000-4000-8000-000000000002","can_use":true}"#
        ).is_err());
    }
    #[test]
    fn start_requires_explicit_confirmation_and_stop_does_not() {
        let mut request = LeaderBotLifecycleRequest {
            schema_version: 1,
            request_id: "00000000-0000-4000-8000-000000000001".into(),
            bot_id: "00000000-0000-4000-8000-000000000002".into(),
            expected_revision: 1,
            action: LeaderBotAction::Start,
            risk_confirmed: false,
        };
        assert!(!request.valid());
        request.risk_confirmed = true;
        assert!(request.valid());
        request.action = LeaderBotAction::Stop;
        assert!(!request.valid());
        request.risk_confirmed = false;
        assert!(request.valid());
    }
}
