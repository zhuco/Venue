use std::{
    collections::BTreeSet,
    net::IpAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::{
    Response, StatusCode, Url,
    header::{CONTENT_LENGTH, CONTENT_TYPE},
    redirect::Policy,
};
use serde::{Serialize, de::DeserializeOwned};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_ACK_PATH, ACCOUNT_DELIVERY_CLAIM_PATH, ACCOUNT_DELIVERY_RECEIPT_PATH,
    ACCOUNT_DELIVERY_SCHEMA_VERSION, ACCOUNT_NODE_PROJECTION_PATH, AccountDeliveryAck,
    AccountDeliveryClaim, AccountDeliveryClaimRequest, AccountDeliveryPurpose,
    AccountDeliveryReceipt, COPY_RELATION_PATH, CopyRelationRecord, NodeProjectionEnvelope,
};

use crate::control_delivery::{
    ActorDeliveryCompletion, ActorDeliveryTurn, ClaimAcceptance, ControlDeliveryError,
    ControlDeliveryInbox, ControlDeliveryJournal, ReconciliationCompletion, ReconciliationTurn,
};

pub const MAX_CONTROL_HTTP_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_CONTROL_HTTP_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_CONTROL_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_CONTROL_LEASE_DURATION_MS: u64 = 60_000;
pub const MAX_CONTROL_CLAIM_LIMIT: u32 = 256;

#[derive(Clone, Debug)]
pub struct ControlHttpClientConfig {
    pub base_url: String,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_response_bytes: usize,
}

impl ControlHttpClientConfig {
    #[must_use]
    pub fn local(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            max_response_bytes: MAX_CONTROL_HTTP_RESPONSE_BYTES,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ControlHttpClient {
    client: reqwest::Client,
    claim_url: Url,
    ack_url: Url,
    receipt_url: Url,
    projection_url: Url,
    copy_relation_url: Url,
    max_response_bytes: usize,
    node_token: Option<venue_control_protocol::accounts::SecretValue>,
}

impl ControlHttpClient {
    pub fn new(config: ControlHttpClientConfig) -> Result<Self, ControlHttpClientError> {
        if config.connect_timeout.is_zero()
            || config.request_timeout.is_zero()
            || config.connect_timeout > config.request_timeout
            || config.request_timeout > MAX_CONTROL_HTTP_TIMEOUT
            || !(1..=MAX_CONTROL_HTTP_RESPONSE_BYTES).contains(&config.max_response_bytes)
        {
            return Err(ControlHttpClientError::InvalidConfig);
        }
        let base_url = validate_base_url(&config.base_url)?;
        let claim_url = endpoint(&base_url, ACCOUNT_DELIVERY_CLAIM_PATH)?;
        let ack_url = endpoint(&base_url, ACCOUNT_DELIVERY_ACK_PATH)?;
        let receipt_url = endpoint(&base_url, ACCOUNT_DELIVERY_RECEIPT_PATH)?;
        let projection_url = endpoint(&base_url, ACCOUNT_NODE_PROJECTION_PATH)?;
        let copy_relation_url = endpoint(&base_url, COPY_RELATION_PATH)?;
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .pool_max_idle_per_host(1)
            .build()
            .map_err(|_| ControlHttpClientError::Transport)?;
        Ok(Self {
            client,
            claim_url,
            ack_url,
            receipt_url,
            projection_url,
            copy_relation_url,
            max_response_bytes: config.max_response_bytes,
            node_token: std::env::var("VENUE_CONTROL_NODE_TOKEN")
                .ok()
                .map(venue_control_protocol::accounts::SecretValue::new),
        })
    }

    pub async fn claim(
        &self,
        request: &AccountDeliveryClaimRequest,
    ) -> Result<Vec<AccountDeliveryClaim>, ControlHttpClientError> {
        request
            .validate()
            .map_err(|_| ControlHttpClientError::InvalidRequest)?;
        if request.lease_duration_ms > MAX_CONTROL_LEASE_DURATION_MS
            || request.limit > MAX_CONTROL_CLAIM_LIMIT
        {
            return Err(ControlHttpClientError::InvalidRequest);
        }
        let claims: Vec<AccountDeliveryClaim> = self.post_json(&self.claim_url, request).await?;
        validate_claim_batch(request, &claims)?;
        Ok(claims)
    }

    pub async fn acknowledge(
        &self,
        ack: &AccountDeliveryAck,
    ) -> Result<AccountDeliveryAck, ControlHttpClientError> {
        ack.validate()
            .map_err(|_| ControlHttpClientError::InvalidRequest)?;
        let echoed: AccountDeliveryAck = self.post_json(&self.ack_url, ack).await?;
        if echoed != *ack {
            return Err(ControlHttpClientError::ResponseConflict);
        }
        Ok(echoed)
    }

    pub async fn record_receipt(
        &self,
        receipt: &AccountDeliveryReceipt,
    ) -> Result<AccountDeliveryReceipt, ControlHttpClientError> {
        receipt
            .validate()
            .map_err(|_| ControlHttpClientError::InvalidRequest)?;
        let echoed: AccountDeliveryReceipt = self.post_json(&self.receipt_url, receipt).await?;
        if echoed != *receipt {
            return Err(ControlHttpClientError::ResponseConflict);
        }
        Ok(echoed)
    }

    /// Uploads an already durable, node-owned read projection. The response must be the exact
    /// envelope, so a retry after an uncertain HTTP result cannot acknowledge a different cursor.
    pub async fn publish_projection(
        &self,
        projection: &NodeProjectionEnvelope,
    ) -> Result<NodeProjectionEnvelope, ControlHttpClientError> {
        projection
            .validate()
            .map_err(|_| ControlHttpClientError::InvalidRequest)?;
        let echoed: NodeProjectionEnvelope =
            self.post_json(&self.projection_url, projection).await?;
        if echoed != *projection {
            return Err(ControlHttpClientError::ResponseConflict);
        }
        Ok(echoed)
    }

    /// Reads the current durable relation configurations. The returned data stays configuration
    /// only: it grants no delivery lease, Actor authority, or execution capability.
    pub async fn copy_relations(&self) -> Result<Vec<CopyRelationRecord>, ControlHttpClientError> {
        let relations: Vec<CopyRelationRecord> = self.get_json(&self.copy_relation_url).await?;
        if relations
            .iter()
            .any(|relation| relation.validate().is_err())
        {
            return Err(ControlHttpClientError::InvalidResponse);
        }
        Ok(relations)
    }

    async fn post_json<T: Serialize + ?Sized, U: DeserializeOwned>(
        &self,
        url: &Url,
        value: &T,
    ) -> Result<U, ControlHttpClientError> {
        let body = serde_json::to_vec(value).map_err(|_| ControlHttpClientError::InvalidRequest)?;
        if body.is_empty() || body.len() > MAX_CONTROL_HTTP_REQUEST_BYTES {
            return Err(ControlHttpClientError::RequestTooLarge);
        }
        let mut request = self
            .client
            .post(url.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(token) = &self.node_token {
            request = request.bearer_auth(token.expose());
        }
        let response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                ControlHttpClientError::Timeout
            } else {
                ControlHttpClientError::Transport
            }
        })?;
        self.decode_response(response).await
    }

    async fn get_json<U: DeserializeOwned>(&self, url: &Url) -> Result<U, ControlHttpClientError> {
        let mut request = self
            .client
            .get(url.clone())
            .header(CONTENT_TYPE, "application/json");
        if let Some(token) = &self.node_token {
            request = request.bearer_auth(token.expose());
        }
        let response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                ControlHttpClientError::Timeout
            } else {
                ControlHttpClientError::Transport
            }
        })?;
        self.decode_response(response).await
    }

    async fn decode_response<U: DeserializeOwned>(
        &self,
        mut response: Response,
    ) -> Result<U, ControlHttpClientError> {
        if response.status() != StatusCode::OK {
            return Err(match response.status() {
                StatusCode::CONFLICT => ControlHttpClientError::ResponseConflict,
                StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => {
                    ControlHttpClientError::Timeout
                }
                status => ControlHttpClientError::HttpStatus(status.as_u16()),
            });
        }
        if !response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .is_some_and(|media| media.trim().eq_ignore_ascii_case("application/json"))
            })
        {
            return Err(ControlHttpClientError::InvalidResponse);
        }
        if response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|length| length > self.max_response_bytes)
        {
            return Err(ControlHttpClientError::ResponseTooLarge);
        }
        let mut encoded = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| ControlHttpClientError::Transport)?
        {
            if encoded.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err(ControlHttpClientError::ResponseTooLarge);
            }
            encoded.extend_from_slice(&chunk);
        }
        if encoded.is_empty() {
            return Err(ControlHttpClientError::InvalidResponse);
        }
        serde_json::from_slice(&encoded).map_err(|_| ControlHttpClientError::InvalidResponse)
    }

    #[must_use]
    pub const fn grants_gateway_capability(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_writer_lease(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_wal_authority(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_dispatch_permit(&self) -> bool {
        false
    }
}

fn validate_base_url(raw: &str) -> Result<Url, ControlHttpClientError> {
    let url = Url::parse(raw).map_err(|_| ControlHttpClientError::InvalidConfig)?;
    let loopback = url
        .host_str()
        .and_then(|host| host.parse::<IpAddr>().ok())
        .is_some_and(|ip| ip.is_loopback());
    if url.scheme() != "http"
        || !loopback
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(ControlHttpClientError::InvalidConfig);
    }
    Ok(url)
}

fn endpoint(base: &Url, path: &str) -> Result<Url, ControlHttpClientError> {
    base.join(path.trim_start_matches('/'))
        .map_err(|_| ControlHttpClientError::InvalidConfig)
}

fn validate_claim_batch(
    request: &AccountDeliveryClaimRequest,
    claims: &[AccountDeliveryClaim],
) -> Result<(), ControlHttpClientError> {
    if claims.len() > request.limit as usize {
        return Err(ControlHttpClientError::InvalidResponse);
    }
    let mut delivery_ids = BTreeSet::new();
    for claim in claims {
        claim
            .validate()
            .map_err(|_| ControlHttpClientError::InvalidResponse)?;
        let lease = &claim.lease;
        if lease.binding != request.binding
            || lease.node_id != request.node_id
            || lease
                .expires_at_ms
                .checked_sub(lease.leased_at_ms)
                .is_none_or(|duration| duration != request.lease_duration_ms)
            || (lease.purpose == AccountDeliveryPurpose::ReconcileOnly && lease.lease_epoch < 2)
            || !delivery_ids.insert(lease.delivery_id.as_str())
            || lease.grants_mutation_authority()
            || claim.grants_mutation_authority()
        {
            return Err(ControlHttpClientError::InvalidResponse);
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ControlHttpClientError {
    #[error("control HTTP client configuration is invalid or not exact loopback HTTP")]
    InvalidConfig,
    #[error("control HTTP request is invalid")]
    InvalidRequest,
    #[error("control HTTP request exceeds the 64 KiB bound")]
    RequestTooLarge,
    #[error("control HTTP response exceeds the configured bound")]
    ResponseTooLarge,
    #[error("control HTTP response is invalid")]
    InvalidResponse,
    #[error("control HTTP response conflicts with the exact durable value")]
    ResponseConflict,
    #[error("control HTTP request timed out")]
    Timeout,
    #[error("control HTTP transport is unavailable")]
    Transport,
    #[error("control HTTP server returned non-success status {0}")]
    HttpStatus(u16),
}

#[derive(Debug)]
pub enum ControlDeliveryWork {
    Actor(ActorDeliveryTurn),
    Reconcile(ReconciliationTurn),
}

impl ControlDeliveryWork {
    #[must_use]
    pub const fn grants_gateway_capability(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_writer_lease(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_wal_authority(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_dispatch_permit(&self) -> bool {
        false
    }
}

/// Drives the semantic delivery protocol without connecting it to physical execution.
///
/// Every transport step follows a durable inbox transition. A recovered outbox is flushed before
/// new claims, and already accepted Actor or reconciliation work is returned before polling for
/// more work.
pub struct ControlDeliveryDriver<J> {
    client: ControlHttpClient,
    inbox: ControlDeliveryInbox<J>,
    lease_duration_ms: u64,
    claim_limit: u32,
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl<J: ControlDeliveryJournal> ControlDeliveryDriver<J> {
    pub fn new(
        client: ControlHttpClient,
        inbox: ControlDeliveryInbox<J>,
        lease_duration_ms: u64,
        claim_limit: u32,
    ) -> Result<Self, ControlDeliveryDriverError> {
        if !(1..=MAX_CONTROL_LEASE_DURATION_MS).contains(&lease_duration_ms)
            || !(1..=MAX_CONTROL_CLAIM_LIMIT).contains(&claim_limit)
        {
            return Err(ControlDeliveryDriverError::InvalidConfig);
        }
        Ok(Self {
            client,
            inbox,
            lease_duration_ms,
            claim_limit,
            clock: Arc::new(system_time_ms),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_with_clock(
        client: ControlHttpClient,
        inbox: ControlDeliveryInbox<J>,
        lease_duration_ms: u64,
        claim_limit: u32,
        clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Result<Self, ControlDeliveryDriverError> {
        let mut driver = Self::new(client, inbox, lease_duration_ms, claim_limit)?;
        driver.clock = clock;
        Ok(driver)
    }

    pub async fn poll(
        &mut self,
        observed_ms: u64,
    ) -> Result<Vec<ControlDeliveryWork>, ControlDeliveryDriverError> {
        if observed_ms == 0 {
            return Err(ControlDeliveryDriverError::InvalidTime);
        }
        self.flush_outbox_current().await?;
        let pending = self.pending_work(self.now_ms()?)?;
        if !pending.is_empty() {
            return Ok(pending);
        }
        let request = AccountDeliveryClaimRequest {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            binding: self.inbox.binding().clone(),
            node_id: self.inbox.node_id().to_owned(),
            lease_duration_ms: self.lease_duration_ms,
            limit: self.claim_limit,
        };
        let claims = self.client.claim(&request).await?;
        for claim in claims {
            let received_ms = self.now_ms()?;
            if !lease_is_active(&claim, received_ms) {
                continue;
            }
            match self.inbox.accept_claim(claim, received_ms)? {
                ClaimAcceptance::Install(output) => {
                    let ack = output.value().clone();
                    self.client.acknowledge(&ack).await?;
                    let confirmed_ms = self.now_ms()?;
                    match self.inbox.confirm_acknowledgement(&ack, confirmed_ms) {
                        Ok(_) | Err(ControlDeliveryError::LeaseExpired) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                ClaimAcceptance::Reconcile(_) => {}
            }
        }
        Ok(self.pending_work(self.now_ms()?)?)
    }

    pub async fn submit_actor_completion(
        &mut self,
        completion: ActorDeliveryCompletion,
        confirmed_ms: u64,
    ) -> Result<(), ControlDeliveryDriverError> {
        if confirmed_ms == 0 {
            return Err(ControlDeliveryDriverError::InvalidTime);
        }
        let recorded_ms = self.now_ms()?;
        let output = self
            .inbox
            .record_actor_completion(completion, recorded_ms)?;
        let receipt = output.value().clone();
        self.client.record_receipt(&receipt).await?;
        let receipt_confirmed_ms = self.now_ms()?;
        self.inbox.confirm_receipt(&receipt, receipt_confirmed_ms)?;
        Ok(())
    }

    pub async fn submit_reconciliation(
        &mut self,
        completion: ReconciliationCompletion,
        confirmed_ms: u64,
    ) -> Result<(), ControlDeliveryDriverError> {
        if confirmed_ms == 0 {
            return Err(ControlDeliveryDriverError::InvalidTime);
        }
        let recorded_ms = self.now_ms()?;
        let output = self.inbox.record_reconciliation(completion, recorded_ms)?;
        let receipt = output.value().clone();
        self.client.record_receipt(&receipt).await?;
        let receipt_confirmed_ms = self.now_ms()?;
        self.inbox.confirm_receipt(&receipt, receipt_confirmed_ms)?;
        Ok(())
    }

    pub async fn flush_outbox(
        &mut self,
        observed_ms: u64,
    ) -> Result<(), ControlDeliveryDriverError> {
        if observed_ms == 0 {
            return Err(ControlDeliveryDriverError::InvalidTime);
        }
        self.flush_outbox_current().await
    }

    async fn flush_outbox_current(&mut self) -> Result<(), ControlDeliveryDriverError> {
        let now_ms = self.now_ms()?;
        for ack in self.inbox.pending_acknowledgements(now_ms) {
            let sending_ms = self.now_ms()?;
            if let Err(error) = self
                .inbox
                .validate_ack_confirmation_session(&ack, sending_ms)
            {
                if matches!(error, ControlDeliveryError::LeaseExpired) {
                    continue;
                }
                return Err(error.into());
            }
            self.client.acknowledge(&ack).await?;
            let confirmed_ms = self.now_ms()?;
            match self.inbox.confirm_acknowledgement(&ack, confirmed_ms) {
                Ok(_) | Err(ControlDeliveryError::LeaseExpired) => {}
                Err(error) => return Err(error.into()),
            }
        }
        let now_ms = self.now_ms()?;
        for receipt in self.inbox.pending_receipts(now_ms) {
            let sending_ms = self.now_ms()?;
            if let Err(error) = self
                .inbox
                .validate_receipt_confirmation_session(&receipt, sending_ms)
            {
                if matches!(error, ControlDeliveryError::LeaseExpired) {
                    continue;
                }
                return Err(error.into());
            }
            self.client.record_receipt(&receipt).await?;
            let confirmed_ms = self.now_ms()?;
            match self.inbox.confirm_receipt(&receipt, confirmed_ms) {
                Ok(_) | Err(ControlDeliveryError::LeaseExpired) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn now_ms(&self) -> Result<u64, ControlDeliveryDriverError> {
        let now_ms = (self.clock)();
        if now_ms == 0 {
            Err(ControlDeliveryDriverError::InvalidTime)
        } else {
            Ok(now_ms)
        }
    }

    fn pending_work(
        &self,
        observed_ms: u64,
    ) -> Result<Vec<ControlDeliveryWork>, ControlDeliveryError> {
        let mut work = self
            .inbox
            .pending_actor_turns(observed_ms)?
            .into_iter()
            .map(ControlDeliveryWork::Actor)
            .collect::<Vec<_>>();
        work.extend(
            self.inbox
                .pending_reconciliation_turns(observed_ms)?
                .into_iter()
                .map(ControlDeliveryWork::Reconcile),
        );
        Ok(work)
    }

    #[must_use]
    pub const fn inbox(&self) -> &ControlDeliveryInbox<J> {
        &self.inbox
    }

    pub fn into_inbox(self) -> ControlDeliveryInbox<J> {
        self.inbox
    }

    #[must_use]
    pub const fn grants_gateway_capability(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_writer_lease(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_wal_authority(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn grants_dispatch_permit(&self) -> bool {
        false
    }
}

fn system_time_ms() -> u64 {
    let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    let Ok(now_ms) = u64::try_from(elapsed.as_millis()) else {
        return 0;
    };
    now_ms
}

fn lease_is_active(claim: &AccountDeliveryClaim, now_ms: u64) -> bool {
    now_ms >= claim.lease.leased_at_ms && now_ms < claim.lease.expires_at_ms
}

#[derive(Debug, thiserror::Error)]
pub enum ControlDeliveryDriverError {
    #[error("control delivery driver configuration is invalid")]
    InvalidConfig,
    #[error("control delivery driver observed time is invalid")]
    InvalidTime,
    #[error(transparent)]
    Http(#[from] ControlHttpClientError),
    #[error(transparent)]
    Inbox(#[from] ControlDeliveryError),
}
