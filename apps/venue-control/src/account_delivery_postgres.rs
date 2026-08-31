use sqlx::{Postgres, Row, Transaction};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryAck, AccountDeliveryBinding,
    AccountDeliveryClaim, AccountDeliveryKind, AccountDeliveryLease, AccountDeliveryPayload,
    AccountDeliveryPurpose, AccountDeliveryReceipt, AccountDeliveryReceiptState,
    ControlCommandRequest, CopySemanticJobDelivery, GatewayMode, UiAccountScope, UiEventKind,
    VenueId,
};

use crate::copy_postgres::ensure_current_relation;
use crate::{
    AccountDeliveryRepository, AccountDeliveryRepositoryError, CopyJob, DeliveryStoreResult,
    PgControlRepository,
};

pub const MIGRATION_0004: &str = include_str!("../migrations/0004_account_node_delivery.sql");
pub const MAX_ACCOUNT_DELIVERY_LEASE_MS: u64 = 60_000;
pub const MAX_ACCOUNT_DELIVERY_CLAIM: u32 = 256;

impl AccountDeliveryRepository for PgControlRepository {
    async fn claim_account_deliveries(
        &self,
        binding: &AccountDeliveryBinding,
        node_id: &str,
        leased_at_ms: u64,
        expires_at_ms: u64,
        limit: u32,
    ) -> Result<Vec<AccountDeliveryClaim>, AccountDeliveryRepositoryError> {
        binding
            .validate()
            .map_err(|_| AccountDeliveryRepositoryError::InvalidData)?;
        validate_lease_window(node_id, leased_at_ms, expires_at_ms)?;
        if !(1..=MAX_ACCOUNT_DELIVERY_CLAIM).contains(&limit) {
            return Err(AccountDeliveryRepositoryError::InvalidData);
        }
        let leased_at = to_i64(leased_at_ms)?;
        let expires_at = to_i64(expires_at_ms)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        let current_scope = sqlx::query(
            "SELECT 1 FROM venue_control_strategy_scopes \
             WHERE venue = $1 AND mode = $2 AND trading_account_id = $3 AND symbol = $4 \
               AND instance_id = $5 AND config_epoch = $6 FOR SHARE",
        )
        .bind(binding.venue.as_str())
        .bind(binding.mode.as_str())
        .bind(&binding.trading_account_id)
        .bind(binding.symbol.to_string())
        .bind(&binding.instance_id)
        .bind(to_i64(binding.config_epoch)?)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?;
        if current_scope.is_none() {
            return Err(AccountDeliveryRepositoryError::BindingConflict);
        }
        let rows = sqlx::query(
            "SELECT d.delivery_id, d.source_kind, d.source_id, d.payload_json, \
                    d.delivery_state, d.lease_epoch, j.expires_at_ms AS copy_expires_at_ms \
             FROM venue_account_deliveries d \
             JOIN venue_control_strategy_scopes s \
               ON s.venue = d.venue AND s.mode = d.mode \
              AND s.trading_account_id = d.trading_account_id AND s.symbol = d.symbol \
              AND s.instance_id = d.instance_id AND s.config_epoch = d.config_epoch \
             LEFT JOIN venue_copy_jobs j \
               ON d.source_kind = 'copy_semantic_job' AND j.job_id = d.source_id \
             WHERE d.venue = $1 AND d.mode = $2 AND d.trading_account_id = $3 \
               AND d.symbol = $4 AND d.instance_id = $5 AND d.config_epoch = $6 \
               AND (d.delivery_state = 'pending' \
                    OR (d.delivery_state = 'claimed' AND d.lease_expires_at_ms <= $7) \
                    OR (d.delivery_state = 'acknowledged' AND d.lease_expires_at_ms <= $7) \
                    OR (d.delivery_state = 'reconciliation_required' \
                        AND (d.lease_purpose = 'install' OR d.lease_expires_at_ms <= $7))) \
               AND (d.source_kind = 'control_command' \
                    OR d.delivery_state IN ('acknowledged', 'reconciliation_required') \
                    OR j.expires_at_ms >= $8 \
                    OR (d.delivery_state = 'claimed' AND j.expires_at_ms <= $7)) \
             ORDER BY d.created_at_ms, d.delivery_id \
             FOR UPDATE OF d SKIP LOCKED LIMIT $9",
        )
        .bind(binding.venue.as_str())
        .bind(binding.mode.as_str())
        .bind(&binding.trading_account_id)
        .bind(binding.symbol.to_string())
        .bind(&binding.instance_id)
        .bind(to_i64(binding.config_epoch)?)
        .bind(leased_at)
        .bind(expires_at)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(database_error)?;

        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let delivery_id: String = row.try_get("delivery_id").map_err(database_error)?;
            let payload: AccountDeliveryPayload =
                decode(row.try_get("payload_json").map_err(database_error)?)?;
            payload
                .validate_for_account_delivery(binding)
                .map_err(|_| AccountDeliveryRepositoryError::CorruptData)?;
            let state: String = row.try_get("delivery_state").map_err(database_error)?;
            let copy_expired_claim = state == "claimed"
                && row
                    .try_get::<Option<i64>, _>("copy_expires_at_ms")
                    .map_err(database_error)?
                    .is_some_and(|expires| expires <= leased_at);
            let purpose = if matches!(state.as_str(), "acknowledged" | "reconciliation_required")
                || copy_expired_claim
            {
                AccountDeliveryPurpose::ReconcileOnly
            } else {
                AccountDeliveryPurpose::Install
            };
            verify_delivery_source(
                &mut transaction,
                row.try_get("source_kind").map_err(database_error)?,
                row.try_get("source_id").map_err(database_error)?,
                binding,
                &payload,
                purpose,
            )
            .await?;
            let current_epoch: i64 = row.try_get("lease_epoch").map_err(database_error)?;
            let lease_epoch = current_epoch
                .checked_add(1)
                .ok_or(AccountDeliveryRepositoryError::NumericRange)?;
            let lease = AccountDeliveryLease {
                schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
                delivery_id: delivery_id.clone(),
                binding: binding.clone(),
                node_id: node_id.to_owned(),
                lease_epoch: from_i64(lease_epoch)?,
                leased_at_ms,
                expires_at_ms,
                purpose,
            };
            let claim = AccountDeliveryClaim { lease, payload };
            claim
                .validate()
                .map_err(|_| AccountDeliveryRepositoryError::CorruptData)?;
            let updated = sqlx::query(
                "UPDATE venue_account_deliveries \
                 SET delivery_state = $2, lease_epoch = $3, leased_by = $4, \
                     lease_purpose = $5, leased_at_ms = $6, lease_expires_at_ms = $7, \
                     updated_at_ms = $6 \
                 WHERE delivery_id = $1 AND lease_epoch = $8",
            )
            .bind(&delivery_id)
            .bind(if purpose == AccountDeliveryPurpose::Install {
                "claimed"
            } else {
                "reconciliation_required"
            })
            .bind(lease_epoch)
            .bind(node_id)
            .bind(purpose_str(purpose))
            .bind(leased_at)
            .bind(expires_at)
            .bind(current_epoch)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            if updated.rows_affected() != 1 {
                return Err(AccountDeliveryRepositoryError::LeaseConflict);
            }
            sqlx::query(
                "INSERT INTO venue_account_delivery_claims \
                 (delivery_id, lease_epoch, node_id, purpose, leased_at_ms, expires_at_ms, claim_json) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(&delivery_id)
            .bind(lease_epoch)
            .bind(node_id)
            .bind(purpose_str(purpose))
            .bind(leased_at)
            .bind(expires_at)
            .bind(encode(&claim)?)
            .execute(&mut *transaction)
            .await
            .map_err(database_error)?;
            claims.push(claim);
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(claims)
    }

    async fn acknowledge_account_delivery(
        &self,
        ack: &AccountDeliveryAck,
    ) -> Result<DeliveryStoreResult, AccountDeliveryRepositoryError> {
        ack.validate()
            .map_err(|_| AccountDeliveryRepositoryError::InvalidData)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        if let Some(row) = sqlx::query(
            "SELECT ack_json FROM venue_account_delivery_acks WHERE delivery_id = $1 FOR SHARE",
        )
        .bind(&ack.lease.delivery_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        {
            let durable: AccountDeliveryAck =
                decode(row.try_get("ack_json").map_err(database_error)?)?;
            transaction.commit().await.map_err(database_error)?;
            return if durable == *ack {
                Ok(DeliveryStoreResult::Existing)
            } else {
                Err(AccountDeliveryRepositoryError::AckConflict)
            };
        }
        lock_exact_lease(&mut transaction, &ack.lease, ack.acknowledged_ms, "claimed").await?;
        sqlx::query(
            "INSERT INTO venue_account_delivery_acks \
             (delivery_id, lease_epoch, acknowledged_ms, durable_inbox_digest, ack_json) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(&ack.lease.delivery_id)
        .bind(to_i64(ack.lease.lease_epoch)?)
        .bind(to_i64(ack.acknowledged_ms)?)
        .bind(ack.durable_inbox_digest.to_vec())
        .bind(encode(ack)?)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        update_state(
            &mut transaction,
            &ack.lease.delivery_id,
            "claimed",
            "acknowledged",
            ack.acknowledged_ms,
        )
        .await?;
        insert_delivery_wakeup(&mut transaction, ack.acknowledged_ms, &ack.lease.binding).await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(DeliveryStoreResult::Stored)
    }

    async fn record_account_delivery_receipt(
        &self,
        receipt: &AccountDeliveryReceipt,
    ) -> Result<DeliveryStoreResult, AccountDeliveryRepositoryError> {
        receipt
            .validate()
            .map_err(|_| AccountDeliveryRepositoryError::InvalidData)?;
        let mut transaction = self.pool().begin().await.map_err(database_error)?;
        if let Some(row) = sqlx::query(
            "SELECT receipt_json FROM venue_account_delivery_receipts \
             WHERE delivery_id = $1 AND receipt_id = $2 FOR SHARE",
        )
        .bind(&receipt.lease.delivery_id)
        .bind(&receipt.receipt_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        {
            let durable: AccountDeliveryReceipt =
                decode(row.try_get("receipt_json").map_err(database_error)?)?;
            transaction.commit().await.map_err(database_error)?;
            return if durable == *receipt {
                Ok(DeliveryStoreResult::Existing)
            } else {
                Err(AccountDeliveryRepositoryError::ReceiptConflict)
            };
        }
        if sqlx::query(
            "SELECT 1 FROM venue_account_delivery_receipts \
             WHERE delivery_id = $1 AND lease_epoch = $2 AND receipt_state = $3 FOR SHARE",
        )
        .bind(&receipt.lease.delivery_id)
        .bind(to_i64(receipt.lease.lease_epoch)?)
        .bind(receipt_state_str(receipt.state))
        .fetch_optional(&mut *transaction)
        .await
        .map_err(database_error)?
        .is_some()
        {
            return Err(AccountDeliveryRepositoryError::ReceiptConflict);
        }
        let (expected_state, next_state): (String, &str) = match receipt.state {
            AccountDeliveryReceiptState::Applied | AccountDeliveryReceiptState::Rejected => {
                ("acknowledged".to_owned(), "settled")
            }
            AccountDeliveryReceiptState::Unknown => {
                let current = current_delivery_state(&mut transaction, &receipt.lease).await?;
                if !matches!(current.as_str(), "claimed" | "acknowledged") {
                    return Err(AccountDeliveryRepositoryError::ReceiptConflict);
                }
                (current, "reconciliation_required")
            }
            AccountDeliveryReceiptState::Reconciled => {
                ("reconciliation_required".to_owned(), "settled")
            }
        };
        lock_exact_lease(
            &mut transaction,
            &receipt.lease,
            receipt.observed_ms,
            &expected_state,
        )
        .await?;
        if matches!(
            receipt.state,
            AccountDeliveryReceiptState::Applied | AccountDeliveryReceiptState::Rejected
        ) {
            let ack_epoch = sqlx::query(
                "SELECT lease_epoch FROM venue_account_delivery_acks WHERE delivery_id = $1",
            )
            .bind(&receipt.lease.delivery_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(database_error)?
            .ok_or(AccountDeliveryRepositoryError::ReceiptConflict)?
            .try_get::<i64, _>("lease_epoch")
            .map_err(database_error)?;
            if from_i64(ack_epoch)? != receipt.lease.lease_epoch {
                return Err(AccountDeliveryRepositoryError::ReceiptConflict);
            }
        }
        sqlx::query(
            "INSERT INTO venue_account_delivery_receipts \
             (delivery_id, receipt_id, lease_epoch, receipt_state, observed_ms, receipt_json) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&receipt.lease.delivery_id)
        .bind(&receipt.receipt_id)
        .bind(to_i64(receipt.lease.lease_epoch)?)
        .bind(receipt_state_str(receipt.state))
        .bind(to_i64(receipt.observed_ms)?)
        .bind(encode(receipt)?)
        .execute(&mut *transaction)
        .await
        .map_err(database_error)?;
        update_state(
            &mut transaction,
            &receipt.lease.delivery_id,
            &expected_state,
            next_state,
            receipt.observed_ms,
        )
        .await?;
        insert_delivery_wakeup(
            &mut transaction,
            receipt.observed_ms,
            &receipt.lease.binding,
        )
        .await?;
        transaction.commit().await.map_err(database_error)?;
        Ok(DeliveryStoreResult::Stored)
    }
}

pub(crate) async fn insert_control_account_delivery(
    transaction: &mut Transaction<'_, Postgres>,
    command: &ControlCommandRequest,
    created_at_ms: u64,
) -> Result<(), AccountDeliveryRepositoryError> {
    let binding = AccountDeliveryBinding {
        venue: command.venue,
        mode: command.mode,
        trading_account_id: command.trading_account_id.clone(),
        symbol: command.symbol.clone(),
        instance_id: command.instance_id.clone(),
        config_epoch: command.expected_config_epoch,
    };
    insert_delivery(
        transaction,
        format!("command:{}", command.request_id),
        AccountDeliveryKind::ControlCommand,
        &command.request_id,
        &binding,
        &AccountDeliveryPayload::ControlCommand(command.clone()),
        created_at_ms,
    )
    .await
}

pub(crate) async fn insert_copy_account_delivery(
    transaction: &mut Transaction<'_, Postgres>,
    job: &CopyJob,
    created_at_ms: u64,
) -> Result<(), AccountDeliveryRepositoryError> {
    let relation = ensure_current_relation(transaction, job)
        .await
        .map_err(|_| AccountDeliveryRepositoryError::BindingConflict)?;
    let follower = &relation.follower;
    let scope = sqlx::query(
        "SELECT config_epoch FROM venue_control_strategy_scopes \
         WHERE venue = $1 AND mode = 'LIVE' AND trading_account_id = $2 AND symbol = $3 \
           AND instance_id = $4 FOR SHARE",
    )
    .bind(follower.venue.as_str())
    .bind(&follower.trading_account_id)
    .bind(follower.symbol.to_string())
    .bind(&follower.instance_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    .ok_or(AccountDeliveryRepositoryError::BindingConflict)?;
    let binding = AccountDeliveryBinding {
        venue: follower.venue,
        mode: GatewayMode::Live,
        trading_account_id: follower.trading_account_id.clone(),
        symbol: follower.symbol.clone(),
        instance_id: follower.instance_id.clone(),
        config_epoch: from_i64(scope.try_get("config_epoch").map_err(database_error)?)?,
    };
    let job_id = job.identities.job_id.to_string();
    let payload = AccountDeliveryPayload::CopySemanticJob(CopySemanticJobDelivery {
        job_id: job_id.clone(),
        job_digest: job.job_digest,
        symbol: job.manifest.binding.instrument.symbol.clone(),
        manifest: serde_json::to_value(&job.manifest)
            .map_err(|_| AccountDeliveryRepositoryError::CorruptData)?,
        semantic_job: job.semantic_job.clone(),
        created_at_ms: job.created_at_ms,
        expires_at_ms: job.manifest.expires_at_ms,
    });
    insert_delivery(
        transaction,
        format!("copy:{job_id}"),
        AccountDeliveryKind::CopySemanticJob,
        &job_id,
        &binding,
        &payload,
        created_at_ms,
    )
    .await
}

async fn insert_delivery(
    transaction: &mut Transaction<'_, Postgres>,
    delivery_id: String,
    kind: AccountDeliveryKind,
    source_id: &str,
    binding: &AccountDeliveryBinding,
    payload: &AccountDeliveryPayload,
    created_at_ms: u64,
) -> Result<(), AccountDeliveryRepositoryError> {
    binding
        .validate()
        .map_err(|_| AccountDeliveryRepositoryError::BindingConflict)?;
    payload
        .validate_for_account_delivery(binding)
        .map_err(|_| AccountDeliveryRepositoryError::BindingConflict)?;
    let inserted = sqlx::query(
        "INSERT INTO venue_account_deliveries \
         (delivery_id, source_kind, source_id, venue, mode, trading_account_id, symbol, \
          instance_id, config_epoch, payload_json, created_at_ms, updated_at_ms) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11) \
         ON CONFLICT (delivery_id) DO NOTHING",
    )
    .bind(&delivery_id)
    .bind(kind_str(kind))
    .bind(source_id)
    .bind(binding.venue.as_str())
    .bind(binding.mode.as_str())
    .bind(&binding.trading_account_id)
    .bind(binding.symbol.to_string())
    .bind(&binding.instance_id)
    .bind(to_i64(binding.config_epoch)?)
    .bind(encode(payload)?)
    .bind(to_i64(created_at_ms)?)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    if inserted.rows_affected() == 1 {
        insert_delivery_wakeup(transaction, created_at_ms, binding).await?;
        return Ok(());
    }
    let row = sqlx::query(
        "SELECT source_kind, source_id, venue, mode, trading_account_id, symbol, instance_id, \
                config_epoch, payload_json FROM venue_account_deliveries WHERE delivery_id = $1",
    )
    .bind(&delivery_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;
    let durable_payload: AccountDeliveryPayload =
        decode(row.try_get("payload_json").map_err(database_error)?)?;
    let matches = row
        .try_get::<String, _>("source_kind")
        .map_err(database_error)?
        == kind_str(kind)
        && row
            .try_get::<String, _>("source_id")
            .map_err(database_error)?
            == source_id
        && row.try_get::<String, _>("venue").map_err(database_error)? == binding.venue.as_str()
        && row.try_get::<String, _>("mode").map_err(database_error)? == binding.mode.as_str()
        && row
            .try_get::<String, _>("trading_account_id")
            .map_err(database_error)?
            == binding.trading_account_id
        && row.try_get::<String, _>("symbol").map_err(database_error)?
            == binding.symbol.to_string()
        && row
            .try_get::<String, _>("instance_id")
            .map_err(database_error)?
            == binding.instance_id
        && from_i64(row.try_get("config_epoch").map_err(database_error)?)? == binding.config_epoch
        && durable_payload == *payload;
    if matches {
        Ok(())
    } else {
        Err(AccountDeliveryRepositoryError::BindingConflict)
    }
}

async fn insert_delivery_wakeup(
    transaction: &mut Transaction<'_, Postgres>,
    observed_ms: u64,
    binding: &AccountDeliveryBinding,
) -> Result<(), AccountDeliveryRepositoryError> {
    crate::postgres::insert_ui_event(
        transaction,
        observed_ms,
        UiEventKind::Delivery,
        UiAccountScope {
            venue: binding.venue,
            mode: binding.mode,
            trading_account_id: binding.trading_account_id.clone(),
        },
    )
    .await
    .map_err(|_| AccountDeliveryRepositoryError::Database)?;
    Ok(())
}

async fn verify_delivery_source(
    transaction: &mut Transaction<'_, Postgres>,
    source_kind: String,
    source_id: String,
    binding: &AccountDeliveryBinding,
    payload: &AccountDeliveryPayload,
    purpose: AccountDeliveryPurpose,
) -> Result<(), AccountDeliveryRepositoryError> {
    match (source_kind.as_str(), payload) {
        ("control_command", AccountDeliveryPayload::ControlCommand(command)) => {
            let row = sqlx::query(
                "SELECT command_json FROM venue_control_command_inbox WHERE request_id = $1 FOR SHARE",
            )
            .bind(&source_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(database_error)?
            .ok_or(AccountDeliveryRepositoryError::CorruptData)?;
            let durable: ControlCommandRequest =
                decode(row.try_get("command_json").map_err(database_error)?)?;
            if durable != *command || source_id != command.request_id {
                return Err(AccountDeliveryRepositoryError::CorruptData);
            }
        }
        ("copy_semantic_job", AccountDeliveryPayload::CopySemanticJob(wire)) => {
            let row = sqlx::query(
                "SELECT job_json FROM venue_copy_jobs WHERE job_id = $1 AND mode = 'LIVE' FOR SHARE",
            )
            .bind(&source_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(database_error)?
            .ok_or(AccountDeliveryRepositoryError::CorruptData)?;
            let durable: CopyJob = decode(row.try_get("job_json").map_err(database_error)?)?;
            let expected = CopySemanticJobDelivery {
                job_id: durable.identities.job_id.to_string(),
                job_digest: durable.job_digest,
                symbol: durable.manifest.binding.instrument.symbol.clone(),
                manifest: serde_json::to_value(&durable.manifest)
                    .map_err(|_| AccountDeliveryRepositoryError::CorruptData)?,
                semantic_job: durable.semantic_job.clone(),
                created_at_ms: durable.created_at_ms,
                expires_at_ms: durable.manifest.expires_at_ms,
            };
            if expected != *wire
                || source_id != wire.job_id
                || durable.manifest.binding.follower_instance_id != binding.instance_id
                || durable.manifest.binding.account_id != binding.trading_account_id
                || durable.scope.venue != binding.venue
                || durable.scope.mode != binding.mode
                || durable.manifest.binding.instrument.symbol != binding.symbol
            {
                return Err(AccountDeliveryRepositoryError::CorruptData);
            }
            // The frozen source and exact follower remain mandatory during recovery. A newer
            // relation may prohibit installation, but cannot prevent reading an old Unknown.
            if purpose == AccountDeliveryPurpose::Install {
                let relation = ensure_current_relation(transaction, &durable)
                    .await
                    .map_err(|_| AccountDeliveryRepositoryError::BindingConflict)?;
                if relation.follower.venue != binding.venue
                    || relation.follower.trading_account_id != binding.trading_account_id
                    || relation.follower.instance_id != binding.instance_id
                    || relation.follower.symbol != binding.symbol
                {
                    return Err(AccountDeliveryRepositoryError::BindingConflict);
                }
            }
        }
        _ => return Err(AccountDeliveryRepositoryError::CorruptData),
    }
    Ok(())
}

async fn current_delivery_state(
    transaction: &mut Transaction<'_, Postgres>,
    lease: &AccountDeliveryLease,
) -> Result<String, AccountDeliveryRepositoryError> {
    let row = sqlx::query(
        "SELECT delivery_state FROM venue_account_deliveries WHERE delivery_id = $1 FOR UPDATE",
    )
    .bind(&lease.delivery_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    .ok_or(AccountDeliveryRepositoryError::LeaseConflict)?;
    row.try_get("delivery_state").map_err(database_error)
}

async fn lock_exact_lease(
    transaction: &mut Transaction<'_, Postgres>,
    lease: &AccountDeliveryLease,
    observed_ms: u64,
    expected_state: &str,
) -> Result<(), AccountDeliveryRepositoryError> {
    let row = sqlx::query(
        "SELECT venue, mode, trading_account_id, symbol, instance_id, config_epoch, \
                delivery_state, lease_epoch, leased_by, lease_purpose, leased_at_ms, lease_expires_at_ms \
         FROM venue_account_deliveries WHERE delivery_id = $1 FOR UPDATE",
    )
    .bind(&lease.delivery_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?
    .ok_or(AccountDeliveryRepositoryError::LeaseConflict)?;
    let durable_binding = AccountDeliveryBinding {
        venue: parse_venue(row.try_get("venue").map_err(database_error)?)?,
        mode: parse_mode(row.try_get("mode").map_err(database_error)?)?,
        trading_account_id: row.try_get("trading_account_id").map_err(database_error)?,
        symbol: row
            .try_get::<String, _>("symbol")
            .map_err(database_error)?
            .parse()
            .map_err(|_| AccountDeliveryRepositoryError::CorruptData)?,
        instance_id: row.try_get("instance_id").map_err(database_error)?,
        config_epoch: from_i64(row.try_get("config_epoch").map_err(database_error)?)?,
    };
    let state: String = row.try_get("delivery_state").map_err(database_error)?;
    let epoch = from_i64(row.try_get("lease_epoch").map_err(database_error)?)?;
    let leased_by: Option<String> = row.try_get("leased_by").map_err(database_error)?;
    let purpose: Option<String> = row.try_get("lease_purpose").map_err(database_error)?;
    let leased_at = row
        .try_get::<Option<i64>, _>("leased_at_ms")
        .map_err(database_error)?
        .ok_or(AccountDeliveryRepositoryError::CorruptData)?;
    let expires_at = row
        .try_get::<Option<i64>, _>("lease_expires_at_ms")
        .map_err(database_error)?
        .ok_or(AccountDeliveryRepositoryError::CorruptData)?;
    if durable_binding != lease.binding
        || state != expected_state
        || epoch != lease.lease_epoch
        || leased_by.as_deref() != Some(lease.node_id.as_str())
        || purpose.as_deref() != Some(purpose_str(lease.purpose))
        || from_i64(leased_at)? != lease.leased_at_ms
        || from_i64(expires_at)? != lease.expires_at_ms
        || observed_ms < lease.leased_at_ms
        || observed_ms >= lease.expires_at_ms
    {
        return Err(AccountDeliveryRepositoryError::LeaseConflict);
    }
    let current_scope = sqlx::query(
        "SELECT 1 FROM venue_control_strategy_scopes \
         WHERE venue = $1 AND mode = $2 AND trading_account_id = $3 AND symbol = $4 \
           AND instance_id = $5 AND config_epoch = $6 FOR SHARE",
    )
    .bind(lease.binding.venue.as_str())
    .bind(lease.binding.mode.as_str())
    .bind(&lease.binding.trading_account_id)
    .bind(lease.binding.symbol.to_string())
    .bind(&lease.binding.instance_id)
    .bind(to_i64(lease.binding.config_epoch)?)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(database_error)?;
    if current_scope.is_none() {
        return Err(AccountDeliveryRepositoryError::LeaseConflict);
    }
    Ok(())
}

async fn update_state(
    transaction: &mut Transaction<'_, Postgres>,
    delivery_id: &str,
    expected: &str,
    next: &str,
    updated_ms: u64,
) -> Result<(), AccountDeliveryRepositoryError> {
    let updated = sqlx::query(
        "UPDATE venue_account_deliveries SET delivery_state = $2, updated_at_ms = $3 \
         WHERE delivery_id = $1 AND delivery_state = $4",
    )
    .bind(delivery_id)
    .bind(next)
    .bind(to_i64(updated_ms)?)
    .bind(expected)
    .execute(&mut **transaction)
    .await
    .map_err(database_error)?;
    if updated.rows_affected() != 1 {
        return Err(AccountDeliveryRepositoryError::ReceiptConflict);
    }
    Ok(())
}

fn validate_lease_window(
    node_id: &str,
    leased_at_ms: u64,
    expires_at_ms: u64,
) -> Result<(), AccountDeliveryRepositoryError> {
    let duration = expires_at_ms
        .checked_sub(leased_at_ms)
        .ok_or(AccountDeliveryRepositoryError::InvalidData)?;
    if node_id.trim().is_empty()
        || leased_at_ms == 0
        || duration == 0
        || duration > MAX_ACCOUNT_DELIVERY_LEASE_MS
    {
        return Err(AccountDeliveryRepositoryError::InvalidData);
    }
    Ok(())
}

fn encode<T: serde::Serialize>(
    value: &T,
) -> Result<serde_json::Value, AccountDeliveryRepositoryError> {
    serde_json::to_value(value).map_err(|_| AccountDeliveryRepositoryError::CorruptData)
}

fn decode<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
) -> Result<T, AccountDeliveryRepositoryError> {
    serde_json::from_value(value).map_err(|_| AccountDeliveryRepositoryError::CorruptData)
}

fn to_i64(value: u64) -> Result<i64, AccountDeliveryRepositoryError> {
    i64::try_from(value).map_err(|_| AccountDeliveryRepositoryError::NumericRange)
}

fn from_i64(value: i64) -> Result<u64, AccountDeliveryRepositoryError> {
    u64::try_from(value).map_err(|_| AccountDeliveryRepositoryError::CorruptData)
}

const fn kind_str(kind: AccountDeliveryKind) -> &'static str {
    match kind {
        AccountDeliveryKind::ControlCommand => "control_command",
        AccountDeliveryKind::CopySemanticJob => "copy_semantic_job",
    }
}

const fn purpose_str(purpose: AccountDeliveryPurpose) -> &'static str {
    match purpose {
        AccountDeliveryPurpose::Install => "install",
        AccountDeliveryPurpose::ReconcileOnly => "reconcile_only",
    }
}

const fn receipt_state_str(state: AccountDeliveryReceiptState) -> &'static str {
    match state {
        AccountDeliveryReceiptState::Applied => "applied",
        AccountDeliveryReceiptState::Rejected => "rejected",
        AccountDeliveryReceiptState::Unknown => "unknown",
        AccountDeliveryReceiptState::Reconciled => "reconciled",
    }
}

fn parse_mode(value: String) -> Result<GatewayMode, AccountDeliveryRepositoryError> {
    match value.as_str() {
        "LIVE" => Ok(GatewayMode::Live),
        _ => Err(AccountDeliveryRepositoryError::CorruptData),
    }
}

fn parse_venue(value: String) -> Result<VenueId, AccountDeliveryRepositoryError> {
    value
        .parse()
        .map_err(|_| AccountDeliveryRepositoryError::CorruptData)
}

fn database_error(_: sqlx::Error) -> AccountDeliveryRepositoryError {
    AccountDeliveryRepositoryError::Database
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_and_api_never_define_mutation_authority() {
        assert!(MIGRATION_0004.contains("mode IN ('TEST', 'LIVE')"));
        assert!(crate::MIGRATION_0005.contains("CHECK (mode = 'LIVE')"));
        assert!(!crate::MIGRATION_0005.contains("UPDATE "));
        assert!(MIGRATION_0004.contains("lease_epoch"));
        for forbidden in [
            "writer_generation BIGINT",
            "dispatch_permit JSONB",
            "capability JSONB",
            "wal_position BIGINT",
            "mutation_authority BOOLEAN",
        ] {
            assert!(!MIGRATION_0004.contains(forbidden));
        }
    }
}
