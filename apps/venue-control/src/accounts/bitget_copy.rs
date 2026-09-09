use super::{AccountError, AccountService, Principal, error};
use crate::{
    multi_venue_credentials::{StrategyCredentialBindError, StrategyCredentialStore},
    multi_venue_exchange::StrategyCredentials,
};
use venue_control_protocol::accounts::{
    AccountErrorCode as Code, BindBitgetCopyCredentialRequest, CredentialSummary,
};
use venue_domain::domain::Symbol;

impl AccountService {
    pub async fn bind_bitget_copy_credential(
        &self,
        principal: &Principal,
        request: BindBitgetCopyCredentialRequest,
        now_ms: u64,
    ) -> Result<CredentialSummary, AccountError> {
        if !request.valid() {
            return Err(error(Code::InvalidInput));
        }
        self.rate_limit(&format!("bind:{}", principal.user.user_id), 10, now_ms)
            .await?;
        let account = super::strategy_credential_id()?;
        let symbol = Symbol::new("BTC", "USDT").map_err(|_| error(Code::InvalidInput))?;
        StrategyCredentialStore::new_shared(self.pool.clone(), self.cipher.clone())
            .bind_bitget_copy(
                &principal.user.user_id,
                &account,
                &request.label,
                symbol,
                StrategyCredentials::BitgetCopy {
                    api_key: request.api_key,
                    api_secret: request.api_secret,
                    passphrase: request.passphrase,
                },
                now_ms,
            )
            .await
            .map_err(|bind_error| match bind_error {
                StrategyCredentialBindError::AccountInUse => error(Code::AccountInUse),
                StrategyCredentialBindError::IdentityConflict => error(Code::Conflict),
                StrategyCredentialBindError::Exchange => error(Code::VerificationRequired),
            })
    }
}
