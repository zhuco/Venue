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
                && code.bytes().all(|c| c.is_ascii_alphanumeric())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_invites_require_four_to_sixty_four_ascii_alphanumerics() {
        let mut request = KolInviteCreateRequest {
            request_id: "00000000-0000-4000-8000-000000000001".into(),
            expected_invite_id: None,
            invite_code: None,
        };
        assert!(request.valid());
        for code in ["Ab12", "1234", "a".repeat(64).as_str()] {
            request.invite_code = Some(code.into());
            assert!(request.valid());
        }
        for code in ["Ab1", "AB_1", "AB-1", "中文12", "a".repeat(65).as_str()] {
            request.invite_code = Some(code.into());
            assert!(!request.valid());
        }
    }
}
