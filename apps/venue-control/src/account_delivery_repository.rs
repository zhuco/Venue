use std::future::Future;

use thiserror::Error;
use venue_control_protocol::{
    AccountDeliveryAck, AccountDeliveryBinding, AccountDeliveryClaim, AccountDeliveryReceipt,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryStoreResult {
    Stored,
    Existing,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AccountDeliveryRepositoryError {
    #[error("account delivery repository input is invalid")]
    InvalidData,
    #[error("account delivery repository is unavailable")]
    Database,
    #[error("account delivery repository contains invalid encoded data")]
    CorruptData,
    #[error("account delivery value exceeds PostgreSQL integer range")]
    NumericRange,
    #[error("account delivery binding or source payload conflicts with durable state")]
    BindingConflict,
    #[error("account delivery lease is stale, expired, duplicated, or held by another node")]
    LeaseConflict,
    #[error("account delivery ACK conflicts with durable custody")]
    AckConflict,
    #[error("account delivery receipt conflicts with durable custody or state")]
    ReceiptConflict,
}

pub trait AccountDeliveryRepository: Send + Sync {
    fn claim_account_deliveries(
        &self,
        binding: &AccountDeliveryBinding,
        node_id: &str,
        leased_at_ms: u64,
        expires_at_ms: u64,
        limit: u32,
    ) -> impl Future<Output = Result<Vec<AccountDeliveryClaim>, AccountDeliveryRepositoryError>> + Send;

    fn acknowledge_account_delivery(
        &self,
        ack: &AccountDeliveryAck,
    ) -> impl Future<Output = Result<DeliveryStoreResult, AccountDeliveryRepositoryError>> + Send;

    fn record_account_delivery_receipt(
        &self,
        receipt: &AccountDeliveryReceipt,
    ) -> impl Future<Output = Result<DeliveryStoreResult, AccountDeliveryRepositoryError>> + Send;
}
