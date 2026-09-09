use super::{AccountError, AccountService, Principal, error};
use crate::{
    executor_secret::ExecutorSecretProvider,
    inventory_mm::{InventoryMmStore, InventoryMmStoreError},
    private_projection::BinancePrivateProjectionStore,
};
use venue_control_protocol::{accounts::AccountErrorCode as Code, inventory_mm::*};

impl AccountService {
    pub async fn inventory_mm_instances(
        &self,
        principal: &Principal,
    ) -> Result<Vec<InventoryMmInstance>, AccountError> {
        InventoryMmStore::new(self.pool.clone())
            .list(&principal.user.user_id)
            .await
            .map_err(mm_error)
    }
    pub async fn create_inventory_mm(
        &self,
        principal: &Principal,
        request: InventoryMmCreateRequest,
        now: u64,
    ) -> Result<InventoryMmInstance, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        self.rate_limit(&format!("inventory_mm:{}", principal.user.user_id), 60, now)
            .await?;
        let id = super::crypto::opaque_id()?;
        InventoryMmStore::new(self.pool.clone())
            .create(&principal.user.user_id, &id, &request, now)
            .await
            .map_err(mm_error)
    }
    pub async fn inventory_mm_preflight(
        &self,
        principal: &Principal,
        request: InventoryMmPreflightRequest,
        now: u64,
    ) -> Result<InventoryMmPreflight, AccountError> {
        let store = InventoryMmStore::new(self.pool.clone());
        let instance = store
            .get(&principal.user.user_id, &request.instance_id)
            .await
            .map_err(mm_error)?;
        let projections = BinancePrivateProjectionStore::new(self.pool.clone());
        projections
            .subscribe(
                &principal.user.user_id,
                &instance.credential_id,
                std::slice::from_ref(&instance.config.symbol),
                now,
            )
            .await
            .map_err(|_| error(Code::Unavailable))?;
        let mut result = store
            .preflight(
                &principal.user.user_id,
                &request.instance_id,
                request.expected_revision,
                now,
            )
            .await
            .map_err(mm_error)?;
        if result.ready {
            match self.inventory_mm_signed_gate(&instance).await {
                Ok(_) => {}
                Err(_) => result
                    .blockers
                    .push("signed_leverage_or_margin_unavailable".into()),
            }
        }
        result.ready = result.blockers.is_empty();
        result.checked_ms =
            crate::multi_venue_runtime::now_ms().map_err(|_| error(Code::Unavailable))?;
        Ok(result)
    }
    pub async fn inventory_mm_lifecycle(
        &self,
        principal: &Principal,
        request: InventoryMmLifecycleRequest,
        now: u64,
    ) -> Result<InventoryMmInstance, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        let store = InventoryMmStore::new(self.pool.clone());
        let instance = store
            .get(&principal.user.user_id, &request.instance_id)
            .await
            .map_err(mm_error)?;
        let leverage = if request.action == InventoryMmAction::Start {
            let preflight = store
                .preflight(
                    &principal.user.user_id,
                    &instance.instance_id,
                    request.expected_revision,
                    now,
                )
                .await
                .map_err(mm_error)?;
            if !preflight.ready {
                return Err(error(Code::VerificationRequired));
            }
            Some(self.inventory_mm_signed_gate(&instance).await?)
        } else if request.action == InventoryMmAction::Resume {
            let secrets =
                ExecutorSecretProvider::new_shared(self.pool.clone(), self.cipher.clone());
            Some(
                crate::inventory_mm::signed_resume_gate(self.pool.clone(), secrets, &instance)
                    .await
                    .map_err(mm_error)?,
            )
        } else {
            None
        };
        let checked = crate::multi_venue_runtime::now_ms().map_err(|_| error(Code::Unavailable))?;
        store
            .lifecycle(&principal.user.user_id, &request, leverage, checked)
            .await
            .map_err(mm_error)
    }
    async fn inventory_mm_signed_gate(
        &self,
        instance: &InventoryMmInstance,
    ) -> Result<(u8, u64), AccountError> {
        let secrets = ExecutorSecretProvider::new_shared(self.pool.clone(), self.cipher.clone());
        crate::inventory_mm::signed_gate(self.pool.clone(), secrets, instance)
            .await
            .map_err(mm_error)
    }
}
fn mm_error(v: InventoryMmStoreError) -> AccountError {
    error(match v {
        InventoryMmStoreError::Invalid => Code::InvalidInput,
        InventoryMmStoreError::Forbidden => Code::Forbidden,
        InventoryMmStoreError::Conflict => Code::Conflict,
        InventoryMmStoreError::Unavailable => Code::Unavailable,
    })
}
