//! Account management transport. Secret fields are request-only or one-time session responses;
//! they must never be embedded in Control snapshots, events, receipts or UI preferences.

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const REGISTER_PATH: &str = "/v2/account/register";
pub const LOGIN_PATH: &str = "/v2/account/login";
pub const LOGOUT_PATH: &str = "/v2/account/logout";
pub const SESSION_PATH: &str = "/v2/account/session";
pub const CREDENTIALS_PATH: &str = "/v2/account/credentials";
pub const VERIFY_PATH: &str = "/v2/account/credentials/verify";
pub const DELETE_PATH: &str = "/v2/account/credentials/delete";
pub const SELECT_PATH: &str = "/v2/account/select";
pub const MIN_PASSWORD_CHARS: usize = 8;
pub const MIN_INVITE_CODE_CHARS: usize = 24;
pub const MAX_INVITE_CODE_CHARS: usize = 64;

#[derive(Clone, Debug)]
pub struct SecretValue(SecretString);

impl SecretValue {
    pub fn new(value: String) -> Self {
        Self(SecretString::from(value))
    }
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

impl Serialize for SecretValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.expose())
    }
}

impl<'de> Deserialize<'de> for SecretValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::new)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoginRequest {
    pub username: String,
    pub password: SecretValue,
}

impl LoginRequest {
    pub fn normalized_username(&self) -> Option<String> {
        let name = self.username.trim();
        (name.len() >= 3
            && name.len() <= 64
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-@".contains(&b)))
        .then(|| name.to_ascii_lowercase())
    }
    pub fn valid_password(&self) -> bool {
        (MIN_PASSWORD_CHARS..=128).contains(&self.password.expose().chars().count())
            && self.password.expose().len() <= 512
    }
}

/// Registration input for the KOL MVP. The invite is resolved again inside the same database
/// transaction that creates the user and immutable KOL binding.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterRequest {
    pub username: String,
    pub password: SecretValue,
    pub invite_code: String,
}

impl RegisterRequest {
    pub fn normalized_username(&self) -> Option<String> {
        let name = self.username.trim();
        (name.len() >= 3
            && name.len() <= 64
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-@".contains(&byte)))
        .then(|| name.to_ascii_lowercase())
    }

    pub fn valid_password(&self) -> bool {
        (MIN_PASSWORD_CHARS..=128).contains(&self.password.expose().chars().count())
            && self.password.expose().len() <= 512
    }

    pub fn normalized_invite_code(&self) -> Option<String> {
        let code = self.invite_code.trim();
        ((MIN_INVITE_CODE_CHARS..=MAX_INVITE_CODE_CHARS).contains(&code.len())
            && code
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
        .then(|| code.to_owned())
    }

    pub fn valid(&self) -> bool {
        self.normalized_username().is_some()
            && self.valid_password()
            && self.normalized_invite_code().is_some()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UserSummary {
    pub user_id: String,
    pub username: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionResponse {
    pub user: UserSummary,
    pub token: SecretValue,
    pub expires_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindCredentialRequest {
    pub label: String,
    pub api_key: SecretValue,
    pub api_secret: SecretValue,
}

impl BindCredentialRequest {
    pub fn valid(&self) -> bool {
        let label = self.label.trim();
        !label.is_empty()
            && label.chars().count() <= 64
            && !label.chars().any(char::is_control)
            && [self.api_key.expose(), self.api_secret.expose()]
                .into_iter()
                .all(|v| {
                    (16..=256).contains(&v.len()) && v.bytes().all(|b| b.is_ascii_alphanumeric())
                })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiVerificationState {
    Unverified,
    Verified,
    InvalidCredentials,
    PermissionDenied,
    ModeMismatch,
    NetworkUnavailable,
    AccountConflict,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CredentialSummary {
    pub credential_id: String,
    pub label: String,
    pub venue: crate::VenueId,
    pub masked_key: String,
    pub trading_account_id: Option<String>,
    pub verification: ApiVerificationState,
    pub verified_ms: Option<u64>,
    pub expires_ms: Option<u64>,
    pub api_reachable: bool,
    pub dual_position: bool,
    pub account_mode: Option<String>,
    pub has_exposure: Option<bool>,
}

impl CredentialSummary {
    pub fn selectable(&self, now_ms: u64) -> bool {
        self.verification == ApiVerificationState::Verified
            && self.api_reachable
            && self.dual_position
            && self.trading_account_id.is_some()
            && self.verified_ms.is_some_and(|t| t <= now_ms)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountOverview {
    pub user: UserSummary,
    pub credentials: Vec<CredentialSummary>,
    pub selected_credential_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialRequest {
    pub credential_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteCredentialRequest {
    pub credential_id: String,
    pub password: SecretValue,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountErrorCode {
    InvalidInput,
    InvalidLogin,
    UsernameUnavailable,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    VerificationRequired,
    AccountInUse,
    RateLimited,
    Unavailable,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountErrorResponse {
    pub code: AccountErrorCode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_redacted_and_inputs_are_bounded() {
        let request = LoginRequest {
            username: " Alice ".into(),
            password: SecretValue::new("a safe long passphrase".into()),
        };
        assert_eq!(request.normalized_username().as_deref(), Some("alice"));
        assert!(request.valid_password());
        assert!(
            !LoginRequest {
                username: "alice".into(),
                password: SecretValue::new("1234567".into()),
            }
            .valid_password()
        );
        assert!(
            LoginRequest {
                username: "alice".into(),
                password: SecretValue::new("12345678".into()),
            }
            .valid_password()
        );
        assert!(!format!("{request:?}").contains("a safe long passphrase"));
        let decoded = serde_json::from_str::<LoginRequest>(
            r#"{"username":"alice","password":"secret","admin":true}"#,
        );
        assert!(decoded.is_err());
    }

    #[test]
    fn registration_requires_a_bounded_url_safe_invite() {
        let request = RegisterRequest {
            username: "Alice".into(),
            password: SecretValue::new("a safe long passphrase".into()),
            invite_code: "Abcdefghijklmnop_1234567".into(),
        };
        assert!(request.valid());
        assert_eq!(
            request.normalized_invite_code().as_deref(),
            Some("Abcdefghijklmnop_1234567")
        );
        let unknown = serde_json::from_str::<RegisterRequest>(
            r#"{"username":"alice","password":"a safe passphrase","invite_code":"Abcdefghijklmnop_1234567","kol_id":"forged"}"#,
        );
        assert!(unknown.is_err());
        let mut invalid = request;
        invalid.invite_code = "contains/slash-and-is-not-valid".into();
        assert!(!invalid.valid());
    }

    #[test]
    fn verified_api_binding_does_not_expire_on_a_ui_timer() {
        let summary = CredentialSummary {
            credential_id: "credential".into(),
            label: "主账户".into(),
            venue: crate::VenueId::Binance,
            masked_key: "••••1234".into(),
            trading_account_id: Some("account".into()),
            verification: ApiVerificationState::Verified,
            verified_ms: Some(100),
            expires_ms: Some(200),
            api_reachable: true,
            dual_position: true,
            account_mode: Some("Portfolio Margin · UM".into()),
            has_exposure: Some(false),
        };
        assert!(summary.selectable(10_000));
    }
}
