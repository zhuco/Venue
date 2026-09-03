//! Authenticated Binance Grid configuration and lifecycle boundary.
//!
//! This module owns only user-scoped intent. The singleton Executor remains the sole process
//! allowed to turn a running instance into exchange mutations.

use sqlx::Row;
use venue_control_protocol::{
    VenueId,
    accounts::AccountErrorCode as Code,
    grid::{
        GridConfigUpdateRequest, GridInstanceCreateRequest, GridInstanceSummary,
        GridLifecycleAction, GridLifecycleRequest,
    },
};

use crate::{BinanceGridStore, GridStoreError};

use super::{AccountError, AccountService, Principal, database_error, error};

impl AccountService {
    pub async fn grid_instances(
        &self,
        principal: &Principal,
    ) -> Result<Vec<GridInstanceSummary>, AccountError> {
        BinanceGridStore::new(self.pool.clone())
            .list_owned(&principal.user.user_id)
            .await
            .map_err(grid_error)
    }

    pub async fn create_grid_instance(
        &self,
        principal: &Principal,
        request: GridInstanceCreateRequest,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        self.rate_limit(&format!("grid:{}", principal.user.user_id), 60, now_ms)
            .await?;
        let row = sqlx::query(
            "SELECT trading_account_id,verification_json FROM venue_api_credentials \
             WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL",
        )
        .bind(&request.credential_id)
        .bind(&principal.user.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(database_error)?
        .ok_or(error(Code::VerificationRequired))?;
        let trading_account_id: Option<String> =
            row.try_get("trading_account_id").map_err(database_error)?;
        let trading_account_id = trading_account_id.ok_or(error(Code::VerificationRequired))?;
        let credential = super::credentials::decode_summary(
            row.try_get("verification_json").map_err(database_error)?,
        )?;
        validate_grid_credential(
            &credential,
            &request.credential_id,
            &trading_account_id,
            now_ms,
        )?;
        let instance_id = super::crypto::opaque_id()?;
        BinanceGridStore::new(self.pool.clone())
            .create_instance(
                &principal.user.user_id,
                &trading_account_id,
                &instance_id,
                &request,
                now_ms,
            )
            .await
            .map_err(grid_error)
    }

    pub async fn update_grid_config(
        &self,
        principal: &Principal,
        request: GridConfigUpdateRequest,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        BinanceGridStore::new(self.pool.clone())
            .update_config(&principal.user.user_id, &request, now_ms)
            .await
            .map_err(grid_error)
    }

    pub async fn request_grid_lifecycle(
        &self,
        principal: &Principal,
        request: GridLifecycleRequest,
        now_ms: u64,
    ) -> Result<GridInstanceSummary, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        if matches!(
            request.action,
            GridLifecycleAction::Start | GridLifecycleAction::Resume
        ) {
            let row = sqlx::query(
                "SELECT i.credential_id,i.trading_account_id,c.verification_json \
                 FROM venue_binance_grid_instances i \
                 JOIN venue_api_credentials c ON c.credential_id=i.credential_id \
                    AND c.user_id=i.owner_user_id AND c.trading_account_id=i.trading_account_id \
                 WHERE i.instance_id=$1 AND i.owner_user_id=$2 AND c.deleted_ms IS NULL",
            )
            .bind(&request.instance_id)
            .bind(&principal.user.user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(database_error)?
            .ok_or(error(Code::Forbidden))?;
            let credential_id: String = row.try_get("credential_id").map_err(database_error)?;
            let trading_account_id: String =
                row.try_get("trading_account_id").map_err(database_error)?;
            let credential = super::credentials::decode_summary(
                row.try_get("verification_json").map_err(database_error)?,
            )?;
            validate_grid_credential(&credential, &credential_id, &trading_account_id, now_ms)?;
        }
        BinanceGridStore::new(self.pool.clone())
            .request_lifecycle(&principal.user.user_id, &request, now_ms)
            .await
            .map_err(grid_error)
    }
}

fn validate_grid_credential(
    credential: &venue_control_protocol::accounts::CredentialSummary,
    credential_id: &str,
    trading_account_id: &str,
    now_ms: u64,
) -> Result<(), AccountError> {
    if credential.credential_id != credential_id
        || credential.venue != VenueId::Binance
        || credential.trading_account_id.as_deref() != Some(trading_account_id)
    {
        return Err(error(Code::Unavailable));
    }
    if !credential.selectable(now_ms) {
        return Err(error(Code::VerificationRequired));
    }
    Ok(())
}

fn grid_error(value: GridStoreError) -> AccountError {
    error(match value {
        GridStoreError::Invalid => Code::InvalidInput,
        GridStoreError::Forbidden => Code::Forbidden,
        GridStoreError::Conflict => Code::Conflict,
        GridStoreError::Unavailable | GridStoreError::Corrupt => Code::Unavailable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::accounts::{ApiVerificationState, CredentialSummary};

    fn credential() -> CredentialSummary {
        CredentialSummary {
            credential_id: "00000000-0000-4000-8000-000000000010".to_owned(),
            label: "primary".to_owned(),
            venue: VenueId::Binance,
            masked_key: "••••1234".to_owned(),
            trading_account_id: Some("00000000-0000-4000-8000-000000000011".to_owned()),
            verification: ApiVerificationState::Verified,
            verified_ms: Some(100),
            expires_ms: None,
            api_reachable: true,
            dual_position: true,
            account_mode: Some("portfolio_margin_um".to_owned()),
            has_exposure: Some(false),
        }
    }

    #[test]
    fn grid_start_eligibility_binds_exact_credential_and_account() {
        let value = credential();
        assert!(
            validate_grid_credential(
                &value,
                "00000000-0000-4000-8000-000000000010",
                "00000000-0000-4000-8000-000000000011",
                200,
            )
            .is_ok()
        );
        let mismatch = validate_grid_credential(
            &value,
            "00000000-0000-4000-8000-000000000012",
            "00000000-0000-4000-8000-000000000011",
            200,
        );
        assert_eq!(mismatch.map_err(|error| error.code), Err(Code::Unavailable));
    }

    #[test]
    fn grid_start_eligibility_rejects_a_stale_verification() {
        let mut value = credential();
        value.api_reachable = false;
        assert_eq!(
            validate_grid_credential(
                &value,
                "00000000-0000-4000-8000-000000000010",
                "00000000-0000-4000-8000-000000000011",
                200,
            )
            .map_err(|error| error.code),
            Err(Code::VerificationRequired)
        );
    }
}
