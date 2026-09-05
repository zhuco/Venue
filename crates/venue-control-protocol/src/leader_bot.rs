//! Account-owned order mirroring controls. UI permissions are display hints; the server and
//! executor independently enforce the current database grant before creating risk.
use crate::kol::KolProfileState;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub const LEADER_BOT_PATH: &str = "/v2/kol/leader-bot";
pub const LEADER_BOT_LIFECYCLE_PATH: &str = "/v2/kol/leader-bot/lifecycle";
pub const LEADER_BOT_SCHEMA_VERSION: u16 = 1;
pub const LEADER_BOTS_PATH: &str = "/v2/kol/leader-bots";
pub const LEADER_BOTS_UPDATE_PATH: &str = "/v2/kol/leader-bots/update";
pub const LEADER_BOTS_LIFECYCLE_PATH: &str = "/v2/kol/leader-bots/lifecycle";
pub const LEADER_BOTS_SCHEMA_VERSION: u16 = 2;
pub const MAX_LEADER_BOTS_PER_KOL: u32 = 20;
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
pub struct LeaderBotConfig {
    pub name: String,
    pub description: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub strategy_capital: Decimal,
}

impl LeaderBotConfig {
    pub fn valid(&self) -> bool {
        bounded_text(&self.name, 1, 64)
            && bounded_text(&self.description, 0, 500)
            && self.strategy_capital.is_sign_positive()
            && !self.strategy_capital.is_zero()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderBotListItem {
    pub bot_id: String,
    pub trading_account_id: String,
    pub credential_id: String,
    pub config: LeaderBotConfig,
    pub state: LeaderBotState,
    pub revision: u64,
    pub config_revision: u64,
    pub active_followers: u32,
    pub pending_orders: u32,
    pub attention_code: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
}

impl LeaderBotListItem {
    pub fn valid(&self) -> bool {
        valid_id(&self.bot_id)
            && venue_domain::domain::is_canonical_trading_account_id(&self.trading_account_id)
            && valid_id(&self.credential_id)
            && self.config.valid()
            && self.revision > 0
            && self.config_revision > 0
            && self.created_ms > 0
            && self.updated_ms >= self.created_ms
            && self
                .attention_code
                .as_deref()
                .is_none_or(|value| bounded_text(value, 1, 64))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderBotsAccess {
    pub schema_version: u16,
    pub profile_state: Option<KolProfileState>,
    pub can_use: bool,
    pub permission_revision: u64,
    pub bots: Vec<LeaderBotListItem>,
}

impl LeaderBotsAccess {
    pub fn valid(&self) -> bool {
        self.schema_version == LEADER_BOTS_SCHEMA_VERSION
            && self.bots.len() <= MAX_LEADER_BOTS_PER_KOL as usize
            && self.bots.iter().all(LeaderBotListItem::valid)
            && self
                .bots
                .iter()
                .filter(|bot| bot.state != LeaderBotState::Stopped)
                .count()
                <= 1
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderBotAccess {
    pub schema_version: u16,
    #[serde(default)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderBotConfiguredCreateRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub credential_id: String,
    pub config: LeaderBotConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaderBotUpdateRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub bot_id: String,
    pub expected_revision: u64,
    pub credential_id: String,
    pub config: LeaderBotConfig,
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

impl LeaderBotConfiguredCreateRequest {
    pub fn valid(&self) -> bool {
        self.schema_version == LEADER_BOTS_SCHEMA_VERSION
            && valid_id(&self.request_id)
            && valid_id(&self.credential_id)
            && self.config.valid()
    }
}

impl LeaderBotUpdateRequest {
    pub fn valid(&self) -> bool {
        self.schema_version == LEADER_BOTS_SCHEMA_VERSION
            && valid_id(&self.request_id)
            && valid_id(&self.bot_id)
            && valid_id(&self.credential_id)
            && self.expected_revision > 0
            && self.config.valid()
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

fn bounded_text(value: &str, minimum: usize, maximum: usize) -> bool {
    let trimmed = value.trim();
    (minimum..=maximum).contains(&trimmed.chars().count())
        && value == trimmed
        && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn older_access_response_without_profile_state_remains_readable() {
        let result = serde_json::from_str::<LeaderBotAccess>(
            r#"{"schema_version":1,"can_use":false,"permission_revision":0,"bot":null}"#,
        );
        assert!(result.is_ok());
        let Ok(access) = result else {
            return;
        };
        assert_eq!(access.profile_state, None);
    }

    #[test]
    fn configured_bot_requests_are_bounded_and_revisioned() {
        let config = LeaderBotConfig {
            name: "主账户带单".into(),
            description: "同步普通限价挂单".into(),
            strategy_capital: Decimal::from(100),
        };
        assert!(config.valid());
        assert!(
            LeaderBotConfiguredCreateRequest {
                schema_version: LEADER_BOTS_SCHEMA_VERSION,
                request_id: "00000000-0000-4000-8000-000000000001".into(),
                credential_id: "00000000-0000-4000-8000-000000000002".into(),
                config: config.clone(),
            }
            .valid()
        );
        assert!(
            !LeaderBotUpdateRequest {
                schema_version: LEADER_BOTS_SCHEMA_VERSION,
                request_id: "00000000-0000-4000-8000-000000000003".into(),
                bot_id: "00000000-0000-4000-8000-000000000004".into(),
                expected_revision: 0,
                credential_id: "00000000-0000-4000-8000-000000000002".into(),
                config,
            }
            .valid()
        );
    }

    #[test]
    fn catalog_accepts_multiple_presets_but_rejects_two_active_bots() {
        let item = |bot_id: &str, state| LeaderBotListItem {
            bot_id: bot_id.to_owned(),
            trading_account_id: "00000000-0000-4000-8000-000000000010".into(),
            credential_id: "00000000-0000-4000-8000-000000000011".into(),
            config: LeaderBotConfig {
                name: "KOL 带单".into(),
                description: String::new(),
                strategy_capital: Decimal::from(100),
            },
            state,
            revision: 1,
            config_revision: 1,
            active_followers: 0,
            pending_orders: 0,
            attention_code: None,
            created_ms: 1,
            updated_ms: 1,
        };
        let mut access = LeaderBotsAccess {
            schema_version: LEADER_BOTS_SCHEMA_VERSION,
            profile_state: Some(KolProfileState::Enabled),
            can_use: true,
            permission_revision: 1,
            bots: vec![
                item(
                    "00000000-0000-4000-8000-000000000001",
                    LeaderBotState::Running,
                ),
                item(
                    "00000000-0000-4000-8000-000000000002",
                    LeaderBotState::Stopped,
                ),
            ],
        };
        assert!(access.valid());
        access.bots[1].state = LeaderBotState::Draining;
        assert!(!access.valid());
    }

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
