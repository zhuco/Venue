//! Bounded HTTP adapter for account-node polling, durable inbox ACKs, and terminal receipts.
//!
//! This boundary only transports schema-versioned semantic delivery records. The PostgreSQL
//! repository remains authoritative for lease sequencing and state transitions, and no successful
//! response is a gateway capability, writer lease, WAL authority, or dispatch permit.

use std::{collections::BTreeSet, time::Duration};

use venue_control_protocol::{
    ACCOUNT_DELIVERY_ACK_PATH, ACCOUNT_DELIVERY_CLAIM_PATH, ACCOUNT_DELIVERY_RECEIPT_PATH,
    AccountDeliveryAck, AccountDeliveryClaim, AccountDeliveryClaimRequest, AccountDeliveryPurpose,
    AccountDeliveryReceipt,
};

use crate::{AccountDeliveryRepository, ControlService, ServiceError};

pub const MAX_ACCOUNT_NODE_HTTP_BODY_BYTES: usize = 64 * 1024;
pub const MAX_ACCOUNT_NODE_HTTP_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_ACCOUNT_NODE_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AccountNodeRoute {
    Claim,
    Ack,
    Receipt,
}

impl AccountNodeRoute {
    pub(crate) fn from_path(path: &str) -> Option<Self> {
        match path {
            ACCOUNT_DELIVERY_CLAIM_PATH => Some(Self::Claim),
            ACCOUNT_DELIVERY_ACK_PATH => Some(Self::Ack),
            ACCOUNT_DELIVERY_RECEIPT_PATH => Some(Self::Receipt),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum AccountNodePollError {
    InvalidRequest,
    PayloadTooLarge,
    Timeout,
    Service(ServiceError),
    InvalidRepositoryResponse,
}

pub(crate) async fn handle_account_node_request<R>(
    service: &ControlService<R>,
    route: AccountNodeRoute,
    body: &[u8],
    observed_ms: u64,
    configured_timeout: Duration,
) -> Result<Vec<u8>, AccountNodePollError>
where
    R: AccountDeliveryRepository,
{
    if body.is_empty() {
        return Err(AccountNodePollError::InvalidRequest);
    }
    if body.len() > MAX_ACCOUNT_NODE_HTTP_BODY_BYTES {
        return Err(AccountNodePollError::PayloadTooLarge);
    }
    let timeout = configured_timeout.min(MAX_ACCOUNT_NODE_HTTP_TIMEOUT);
    let response = tokio::time::timeout(timeout, async {
        match route {
            AccountNodeRoute::Claim => claim(service, body, observed_ms).await,
            AccountNodeRoute::Ack => acknowledge(service, body).await,
            AccountNodeRoute::Receipt => record_receipt(service, body).await,
        }
    })
    .await
    .map_err(|_| AccountNodePollError::Timeout)??;
    if response.len() > MAX_ACCOUNT_NODE_HTTP_RESPONSE_BYTES {
        return Err(AccountNodePollError::InvalidRepositoryResponse);
    }
    Ok(response)
}

async fn claim<R>(
    service: &ControlService<R>,
    body: &[u8],
    leased_at_ms: u64,
) -> Result<Vec<u8>, AccountNodePollError>
where
    R: AccountDeliveryRepository,
{
    let request = serde_json::from_slice::<AccountDeliveryClaimRequest>(body)
        .map_err(|_| AccountNodePollError::InvalidRequest)?;
    let expected_expires_at_ms = leased_at_ms
        .checked_add(request.lease_duration_ms)
        .ok_or(AccountNodePollError::InvalidRequest)?;
    let claims = service
        .claim_account_deliveries(&request, leased_at_ms)
        .await
        .map_err(AccountNodePollError::Service)?;
    validate_claim_batch(&request, leased_at_ms, expected_expires_at_ms, &claims)?;
    serde_json::to_vec(&claims).map_err(|_| AccountNodePollError::InvalidRepositoryResponse)
}

async fn acknowledge<R>(
    service: &ControlService<R>,
    body: &[u8],
) -> Result<Vec<u8>, AccountNodePollError>
where
    R: AccountDeliveryRepository,
{
    let ack = serde_json::from_slice::<AccountDeliveryAck>(body)
        .map_err(|_| AccountNodePollError::InvalidRequest)?;
    service
        .acknowledge_account_delivery(&ack)
        .await
        .map_err(AccountNodePollError::Service)?;
    serde_json::to_vec(&ack).map_err(|_| AccountNodePollError::InvalidRepositoryResponse)
}

async fn record_receipt<R>(
    service: &ControlService<R>,
    body: &[u8],
) -> Result<Vec<u8>, AccountNodePollError>
where
    R: AccountDeliveryRepository,
{
    let receipt = serde_json::from_slice::<AccountDeliveryReceipt>(body)
        .map_err(|_| AccountNodePollError::InvalidRequest)?;
    service
        .record_account_delivery_receipt(&receipt)
        .await
        .map_err(AccountNodePollError::Service)?;
    serde_json::to_vec(&receipt).map_err(|_| AccountNodePollError::InvalidRepositoryResponse)
}

fn validate_claim_batch(
    request: &AccountDeliveryClaimRequest,
    leased_at_ms: u64,
    expires_at_ms: u64,
    claims: &[AccountDeliveryClaim],
) -> Result<(), AccountNodePollError> {
    if claims.len() > request.limit as usize {
        return Err(AccountNodePollError::InvalidRepositoryResponse);
    }
    let mut deliveries = BTreeSet::new();
    for claim in claims {
        claim
            .validate()
            .map_err(|_| AccountNodePollError::InvalidRepositoryResponse)?;
        let lease = &claim.lease;
        if lease.binding != request.binding
            || lease.node_id != request.node_id
            || lease.leased_at_ms != leased_at_ms
            || lease.expires_at_ms != expires_at_ms
            || (lease.purpose == AccountDeliveryPurpose::ReconcileOnly && lease.lease_epoch < 2)
            || !deliveries.insert(lease.delivery_id.as_str())
            || lease.grants_mutation_authority()
            || claim.grants_mutation_authority()
        {
            return Err(AccountNodePollError::InvalidRepositoryResponse);
        }
    }
    Ok(())
}
