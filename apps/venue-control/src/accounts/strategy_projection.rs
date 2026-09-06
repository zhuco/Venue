use super::{AccountError, AccountService, Principal, error};
use crate::{
    multi_venue_credentials::StrategyCredentialStore,
    private_projection::{ActiveProjectionSource, project},
};
use venue_control_protocol::{
    accounts::{AccountErrorCode as Code, CredentialSummary},
    kol::{TerminalAccountProjection, TerminalProjectionRequest},
};

impl AccountService {
    pub(super) async fn strategy_terminal_projection(
        &self,
        principal: &Principal,
        credential: &CredentialSummary,
        request: &TerminalProjectionRequest,
    ) -> Result<TerminalAccountProjection, AccountError> {
        // The selected chart may have no contract on this venue. Reuse the contract already
        // validated for this account to obtain its account-wide signed surface.
        let saved: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT strategy_snapshot FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL",
        )
        .bind(&credential.credential_id)
        .bind(&principal.user.user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| error(Code::Unavailable))?;
        let saved = saved
            .map(serde_json::from_value::<venue_execution::SignedAccountSnapshot>)
            .transpose()
            .map_err(|_| error(Code::Unavailable))?;
        let symbol = saved
            .as_ref()
            .filter(|snapshot| {
                snapshot.binding().venue == credential.venue
                    && credential.trading_account_id.as_deref()
                        == Some(snapshot.binding().trading_account_id.as_str())
            })
            .map(|snapshot| snapshot.binding().symbol.clone())
            .or_else(|| request.symbols.first().cloned())
            .ok_or(error(Code::InvalidInput))?;
        let snapshot = StrategyCredentialStore::new_shared(self.pool.clone(), self.cipher.clone())
            .snapshot(&principal.user.user_id, &credential.credential_id, symbol)
            .await
            .map_err(|_| error(Code::Unavailable))?;
        if snapshot.binding().venue != credential.venue
            || credential.trading_account_id.as_deref()
                != Some(snapshot.binding().trading_account_id.as_str())
        {
            return Err(error(Code::Conflict));
        }
        let source = ActiveProjectionSource {
            kol_user_id: None,
            owner_user_id: principal.user.user_id.clone(),
            credential_id: credential.credential_id.clone(),
            trading_account_id: snapshot.binding().trading_account_id.clone(),
            symbols: request.symbols.iter().cloned().collect(),
            previous_fills_cursor: None,
        };
        let received_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| error(Code::Unavailable))?
                .as_millis(),
        )
        .map_err(|_| error(Code::Unavailable))?;
        project(&source, &snapshot, received_ms).map_err(|_| error(Code::Unavailable))
    }
}
