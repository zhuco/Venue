//! Owner-scoped invitation management. Codes grant registration attribution, never trading rights.
use serde::{Deserialize, Serialize};

pub const KOL_INVITE_PATH: &str = "/v2/kol/invite";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KolInviteCreateRequest {
    pub request_id: String,
    pub expected_invite_id: Option<String>,
    pub invite_code: Option<String>,
}
impl KolInviteCreateRequest {
    pub fn valid(&self) -> bool {
        self.invite_code.as_deref().is_none_or(|code| {
            let code = code.trim();
            (crate::accounts::MIN_INVITE_CODE_CHARS..=crate::accounts::MAX_INVITE_CODE_CHARS)
                .contains(&code.len())
                && code
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        }) && crate::leader_bot::valid_id(&self.request_id)
            && self
                .expected_invite_id
                .as_deref()
                .is_none_or(crate::leader_bot::valid_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KolInviteSummary {
    pub invite_id: String,
    pub invite_code: Option<String>,
    pub active: bool,
    pub created_ms: u64,
}
