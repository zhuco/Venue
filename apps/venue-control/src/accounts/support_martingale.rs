use super::{AccountError, AccountService, Principal, error};
use crate::support_martingale::{SupportMartingaleStore, SupportMartingaleStoreError};
use venue_control_protocol::accounts::AccountErrorCode as Code;
use venue_control_protocol::support_martingale::{
    SupportMartingaleCreateRequest, SupportMartingaleInstance, SupportMartingaleLifecycleRequest,
    SupportMartingaleListItem,
};

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
        let id = request.instance_id.clone();
        SupportMartingaleStore::new(self.pool.clone())
            .lifecycle(&principal.user.user_id, request, now_ms)
            .await
            .map_err(support_martingale_error)?;
        self.support_martingale_instance(principal, &id).await
    }
}

fn support_martingale_error(value: SupportMartingaleStoreError) -> AccountError {
    error(match value {
        SupportMartingaleStoreError::Invalid => Code::InvalidInput,
        SupportMartingaleStoreError::Conflict => Code::Conflict,
        SupportMartingaleStoreError::Unavailable => Code::Unavailable,
    })
}
