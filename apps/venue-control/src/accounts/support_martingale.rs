use super::{AccountError, AccountService, Principal, error};
use crate::multi_venue_credentials::StrategyCredentialStore;
use crate::support_martingale::{SupportMartingaleStore, SupportMartingaleStoreError};
use venue_control_protocol::accounts::AccountErrorCode as Code;
use venue_control_protocol::support_martingale::{
    SupportMartingaleCreateRequest, SupportMartingaleInstance, SupportMartingaleLifecycleRequest,
    SupportMartingaleListItem, SupportMartingalePreflightCheck,
    SupportMartingalePreflightCheckCode as CheckCode,
    SupportMartingalePreflightCheckStatus as CheckStatus, SupportMartingalePreflightRequest,
    SupportMartingalePreflightResponse,
};
use venue_execution::SignedAccountPositionMode;
use venue_gateway_api::{GatewayMode, VenueId};

const SIGNED_FACT_MAX_AGE_MS: u64 = 60_000;

impl AccountService {
    pub async fn support_martingale_instances(
        &self,
        principal: &Principal,
    ) -> Result<Vec<SupportMartingaleListItem>, AccountError> {
        SupportMartingaleStore::new(self.pool.clone())
            .list(&principal.user.user_id)
            .await
            .map_err(support_martingale_error)
    }
    pub async fn support_martingale_instance(
        &self,
        principal: &Principal,
        instance_id: &str,
    ) -> Result<SupportMartingaleInstance, AccountError> {
        SupportMartingaleStore::new(self.pool.clone())
            .get(&principal.user.user_id, instance_id)
            .await
            .map_err(support_martingale_error)
    }
    pub async fn create_support_martingale(
        &self,
        principal: &Principal,
        request: SupportMartingaleCreateRequest,
        now_ms: u64,
    ) -> Result<SupportMartingaleInstance, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        let id = SupportMartingaleStore::new(self.pool.clone())
            .create(&principal.user.user_id, request, now_ms)
            .await
            .map_err(support_martingale_error)?;
        self.support_martingale_instance(principal, &id).await
    }
    pub async fn support_martingale_lifecycle(
        &self,
        principal: &Principal,
        request: SupportMartingaleLifecycleRequest,
        now_ms: u64,
    ) -> Result<SupportMartingaleInstance, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        if request.action
            == venue_control_protocol::support_martingale::SupportMartingaleAction::Start
        {
            let preflight = self
                .support_martingale_preflight(
                    principal,
                    SupportMartingalePreflightRequest {
                        schema_version: request.schema_version,
                        instance_id: request.instance_id.clone(),
                        expected_revision: request.expected_revision,
                    },
                    now_ms,
                )
                .await?;
            if !preflight.ready {
                let code = if preflight.checks.iter().any(|check| {
                    check.code == CheckCode::AccountExclusive && check.status == CheckStatus::Failed
                }) {
                    Code::AccountInUse
                } else {
                    Code::VerificationRequired
                };
                return Err(error(code));
            }
        }
        let id = request.instance_id.clone();
        SupportMartingaleStore::new(self.pool.clone())
            .lifecycle(&principal.user.user_id, request, now_ms)
            .await
            .map_err(support_martingale_error)?;
        self.support_martingale_instance(principal, &id).await
    }

    pub async fn support_martingale_preflight(
        &self,
        principal: &Principal,
        request: SupportMartingalePreflightRequest,
        now_ms: u64,
    ) -> Result<SupportMartingalePreflightResponse, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        let store = SupportMartingaleStore::new(self.pool.clone());
        let instance = store
            .get(&principal.user.user_id, &request.instance_id)
            .await
            .map_err(support_martingale_error)?;
        let database = store
            .database_preflight(
                &principal.user.user_id,
                &request.instance_id,
                request.expected_revision,
            )
            .await
            .map_err(support_martingale_error)?;
        let mut checks = vec![
            check(CheckCode::LiveOnly, true),
            check(CheckCode::CredentialVerified, database.credential_verified),
            check(CheckCode::AccountExclusive, database.account_exclusive),
        ];
        if !database.credential_verified || !database.account_exclusive {
            checks.extend(skipped_signed_checks());
            return preflight_response(instance, now_ms, checks);
        }
        let snapshot = StrategyCredentialStore::new_shared(self.pool.clone(), self.cipher.clone())
            .preflight_snapshot(
                &principal.user.user_id,
                &instance.credential_id,
                instance.config.symbols.clone(),
            )
            .await
            .map_err(|_| error(Code::Unavailable))?;
        let checked_at_ms =
            crate::multi_venue_runtime::now_ms().map_err(|_| error(Code::Unavailable))?;
        let binding = snapshot.binding();
        checks[0] = check(
            CheckCode::LiveOnly,
            instance.mode == GatewayMode::Live
                && binding.mode == GatewayMode::Live
                && binding.venue == instance.execution_venue
                && binding.trading_account_id == instance.trading_account_id,
        );
        checks.push(check(
            CheckCode::SignedFactsFresh,
            snapshot.observed_at_ms() <= checked_at_ms
                && checked_at_ms.saturating_sub(snapshot.observed_at_ms())
                    <= SIGNED_FACT_MAX_AGE_MS,
        ));
        let expected_mode = if instance.execution_venue == VenueId::Hyperliquid {
            SignedAccountPositionMode::Net
        } else {
            SignedAccountPositionMode::Hedge
        };
        checks.push(check(
            CheckCode::PositionMode,
            snapshot.position_mode() == expected_mode,
        ));
        checks.push(check(
            CheckCode::NoPositions,
            snapshot
                .positions()
                .iter()
                .all(|position| position.quantity.is_zero()),
        ));
        checks.push(check(
            CheckCode::NoOpenOrders,
            snapshot.open_orders().is_empty(),
        ));
        checks.push(check(
            CheckCode::NoUnknownResults,
            snapshot.unknown_results().is_empty(),
        ));
        let quote = instance.config.symbols[0].quote();
        let budget_available = snapshot.balances().iter().any(|balance| {
            balance.asset.as_str() == quote
                && balance
                    .available_margin
                    .is_some_and(|available| available >= instance.config.total_budget)
        });
        checks.push(check(CheckCode::BudgetAvailable, budget_available));
        preflight_response(instance, checked_at_ms, checks)
    }
}

fn check(code: CheckCode, passed: bool) -> SupportMartingalePreflightCheck {
    SupportMartingalePreflightCheck {
        code,
        status: if passed {
            CheckStatus::Passed
        } else {
            CheckStatus::Failed
        },
    }
}

fn skipped_signed_checks() -> Vec<SupportMartingalePreflightCheck> {
    [
        CheckCode::SignedFactsFresh,
        CheckCode::PositionMode,
        CheckCode::NoPositions,
        CheckCode::NoOpenOrders,
        CheckCode::NoUnknownResults,
        CheckCode::BudgetAvailable,
    ]
    .into_iter()
    .map(|code| SupportMartingalePreflightCheck {
        code,
        status: CheckStatus::Skipped,
    })
    .collect()
}

fn preflight_response(
    instance: SupportMartingaleInstance,
    now_ms: u64,
    checks: Vec<SupportMartingalePreflightCheck>,
) -> Result<SupportMartingalePreflightResponse, AccountError> {
    let response = SupportMartingalePreflightResponse {
        instance_id: instance.instance_id,
        revision: instance.revision,
        checked_at_ms: now_ms,
        mode: GatewayMode::Live,
        ready: checks
            .iter()
            .all(|check| check.status == CheckStatus::Passed),
        checks,
    };
    response.validate().map_err(|_| error(Code::Unavailable))?;
    Ok(response)
}

fn support_martingale_error(value: SupportMartingaleStoreError) -> AccountError {
    error(match value {
        SupportMartingaleStoreError::Invalid => Code::InvalidInput,
        SupportMartingaleStoreError::Conflict => Code::Conflict,
        SupportMartingaleStoreError::Unavailable => Code::Unavailable,
    })
}
