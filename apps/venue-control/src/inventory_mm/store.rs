use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool, Row};
use venue_control_protocol::{
    accounts::CredentialSummary,
    inventory_mm::*,
    kol::{ExecutorCommandState, TerminalAccountProjection, TerminalPositionMode},
};
use venue_domain::{OrderSide, PositionSide};

#[derive(Clone)]
pub struct InventoryMmStore {
    pool: PgPool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InventoryMmStoreError {
    #[error("inventory MM input is invalid")]
    Invalid,
    #[error("inventory MM ownership rejected")]
    Forbidden,
    #[error("inventory MM state or admission changed")]
    Conflict,
    #[error("inventory MM storage unavailable")]
    Unavailable,
}
type Result<T> = std::result::Result<T, InventoryMmStoreError>;
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MmCommandIntent {
    Limit {
        side: OrderSide,
        position_side: PositionSide,
        quantity: Decimal,
        price: Decimal,
        reducing: bool,
    },
    Cancel {
        target_client_order_id: String,
        native_order_id: Option<String>,
    },
}
#[derive(Clone, Debug)]
pub struct MmCommandRecord {
    pub command_id: String,
    pub client_order_id: String,
    pub target_client_order_id: Option<String>,
    pub native_order_id: Option<String>,
    pub state: ExecutorCommandState,
    pub intent: MmCommandIntent,
    pub created_ms: u64,
    pub updated_ms: u64,
}
impl InventoryMmStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn list(&self, owner: &str) -> Result<Vec<InventoryMmInstance>> {
        let rows=sqlx::query("SELECT * FROM venue_inventory_mm_instances WHERE owner_user_id=$1 ORDER BY created_ms,instance_id")
            .bind(owner).fetch_all(&self.pool).await.map_err(db)?;
        rows.iter().map(decode).collect()
    }
    pub async fn active(&self) -> Result<Vec<InventoryMmInstance>> {
        let rows=sqlx::query("SELECT * FROM venue_inventory_mm_instances WHERE instance_state<>'stopped' ORDER BY instance_id")
            .fetch_all(&self.pool).await.map_err(db)?;
        rows.iter().map(decode).collect()
    }
    pub async fn get(&self, owner: &str, id: &str) -> Result<InventoryMmInstance> {
        let row = sqlx::query(
            "SELECT * FROM venue_inventory_mm_instances WHERE instance_id=$1 AND owner_user_id=$2",
        )
        .bind(id)
        .bind(owner)
        .fetch_optional(&self.pool)
        .await
        .map_err(db)?
        .ok_or(InventoryMmStoreError::Forbidden)?;
        decode(&row)
    }
    pub async fn create(
        &self,
        owner: &str,
        id: &str,
        request: &InventoryMmCreateRequest,
        now: u64,
    ) -> Result<InventoryMmInstance> {
        request
            .validate()
            .map_err(|_| InventoryMmStoreError::Invalid)?;
        if !venue_domain::is_canonical_trading_account_id(id) {
            return Err(InventoryMmStoreError::Invalid);
        }
        let digest = digest(request)?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        let row=sqlx::query("SELECT trading_account_id,verification_json FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND venue='binance' AND deleted_ms IS NULL FOR UPDATE")
            .bind(&request.credential_id).bind(owner).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(InventoryMmStoreError::Forbidden)?;
        let account: Option<String> = row.try_get("trading_account_id").map_err(db)?;
        let account = account.ok_or(InventoryMmStoreError::Forbidden)?;
        let summary: CredentialSummary =
            serde_json::from_value(row.try_get("verification_json").map_err(db)?)
                .map_err(|_| InventoryMmStoreError::Unavailable)?;
        if !summary.selectable(now)
            || summary.venue != venue_control_protocol::VenueId::Binance
            || !summary.dual_position
        {
            return Err(InventoryMmStoreError::Conflict);
        }
        if let Some(prior)=sqlx::query("SELECT * FROM venue_inventory_mm_instances WHERE owner_user_id=$1 AND create_request_id=$2")
            .bind(owner).bind(&request.request_id).fetch_optional(&mut *tx).await.map_err(db)? {
            let prior_digest:Vec<u8>=prior.try_get("create_digest").map_err(db)?;
            if prior_digest != digest {return Err(InventoryMmStoreError::Conflict);}
            return decode(&prior);
        }
        sqlx::query("INSERT INTO venue_inventory_mm_instances(instance_id,owner_user_id,trading_account_id,credential_id,create_request_id,create_digest,symbol,config_json,instance_state,revision,created_ms,updated_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'stopped',1,$9,$9)")
            .bind(id).bind(owner).bind(&account).bind(&request.credential_id).bind(&request.request_id).bind(digest)
            .bind(request.config.symbol.to_string()).bind(serde_json::to_value(&request.config).map_err(|_|InventoryMmStoreError::Invalid)?).bind(ms(now)?)
            .execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        self.get(owner, id).await
    }
    pub async fn preflight(
        &self,
        owner: &str,
        id: &str,
        revision: u64,
        now: u64,
    ) -> Result<InventoryMmPreflight> {
        let instance = self.get(owner, id).await?;
        if instance.revision != revision {
            return Err(InventoryMmStoreError::Conflict);
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        let blockers = admission(&mut tx, &instance, now, true).await?;
        Ok(InventoryMmPreflight {
            instance_id: id.to_owned(),
            revision,
            checked_ms: now,
            ready: blockers.is_empty(),
            blockers,
        })
    }
    pub async fn preflight_resume(
        &self,
        owner: &str,
        id: &str,
        revision: u64,
        now: u64,
    ) -> Result<InventoryMmPreflight> {
        let instance = self.get(owner, id).await?;
        if instance.revision != revision || !resumable(&instance) {
            return Err(InventoryMmStoreError::Conflict);
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        let blockers = admission(&mut tx, &instance, now, false).await?;
        Ok(InventoryMmPreflight {
            instance_id: id.to_owned(),
            revision,
            checked_ms: now,
            ready: blockers.is_empty(),
            blockers,
        })
    }
    /// Save and read-only preflight do not start a writer. Start rechecks the database facts
    /// under the same credential lock used by every shared account command producer.
    pub async fn lifecycle(
        &self,
        owner: &str,
        request: &InventoryMmLifecycleRequest,
        leverage: Option<(u8, u64)>,
        now: u64,
    ) -> Result<InventoryMmInstance> {
        request
            .validate()
            .map_err(|_| InventoryMmStoreError::Invalid)?;
        let initial = self.get(owner, &request.instance_id).await?;
        let mut tx = self.pool.begin().await.map_err(db)?;
        lock_queue(&mut tx, &initial).await?;
        let row=sqlx::query("SELECT * FROM venue_inventory_mm_instances WHERE instance_id=$1 AND owner_user_id=$2 FOR UPDATE")
            .bind(&request.instance_id).bind(owner).fetch_one(&mut *tx).await.map_err(db)?;
        let instance = decode(&row)?;
        let request_digest = digest(request)?;
        let prior:Option<Vec<u8>>=sqlx::query_scalar("SELECT request_digest FROM venue_inventory_mm_lifecycle WHERE owner_user_id=$1 AND request_id=$2")
            .bind(owner).bind(&request.request_id).fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some(prior) = prior {
            if prior != request_digest {
                return Err(InventoryMmStoreError::Conflict);
            }
            return Ok(instance);
        }
        if instance.revision != request.expected_revision {
            return Err(InventoryMmStoreError::Conflict);
        }
        let next = match request.action {
            InventoryMmAction::Start => {
                if instance.state != InventoryMmState::Stopped
                    || !admission(&mut tx, &instance, now, true).await?.is_empty()
                    || !leverage.is_some_and(|(v, t)| {
                        v == instance.config.required_leverage && t <= now && now - t <= 5_000
                    })
                {
                    return Err(InventoryMmStoreError::Conflict);
                }
                "start_pending"
            }
            InventoryMmAction::Resume => {
                if !resumable(&instance)
                    || !admission(&mut tx, &instance, now, false).await?.is_empty()
                    || !leverage.is_some_and(|(v, t)| {
                        v == instance.config.required_leverage && t <= now && now - t <= 5_000
                    })
                {
                    return Err(InventoryMmStoreError::Conflict);
                }
                "start_pending"
            }
            InventoryMmAction::Stop => "stop_pending",
        };
        // Locally pending quotes were never dispatched and can be cancelled. Sending/Unknown
        // commands retain their original identity and must reconcile before Stop can finish.
        if request.action == InventoryMmAction::Stop {
            sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',updated_ms=$2,sanitized_error_code='inventory_mm_stop' WHERE inventory_mm_instance_id=$1 AND command_state='pending' AND command_phase<>'cancel'")
                .bind(&instance.instance_id).bind(ms(now)?).execute(&mut *tx).await.map_err(db)?;
        }
        sqlx::query("UPDATE venue_inventory_mm_instances SET instance_state=$2,revision=revision+1,attention=NULL,updated_ms=$3 WHERE instance_id=$1")
            .bind(&instance.instance_id).bind(next).bind(ms(now)?).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO venue_inventory_mm_lifecycle(owner_user_id,request_id,instance_id,request_digest) VALUES($1,$2,$3,$4)")
            .bind(owner).bind(&request.request_id).bind(&instance.instance_id).bind(request_digest).execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        self.get(owner, &instance.instance_id).await
    }

    pub async fn commands(&self, id: &str) -> Result<Vec<MmCommandRecord>> {
        // Keep all unresolved and currently signed-live identities plus the recent settlement
        // window. A long-running market maker must not reload its entire historic ledger each tick.
        let cutoff = crate::multi_venue_runtime::now_ms()
            .map_err(|_| InventoryMmStoreError::Unavailable)?
            .saturating_sub(60_000);
        let rows=sqlx::query("SELECT command_id,client_order_id,target_client_order_id,native_order_id,selected_native_order_id,command_state,command_phase,order_side,position_side,requested_quantity,limit_price,created_ms,updated_ms FROM venue_binance_commands c WHERE inventory_mm_instance_id=$1 AND (command_state IN ('pending','sending','accepted','reconcile_required') OR updated_ms>=$2 OR client_order_id IN (SELECT o->>'client_order_id' FROM venue_inventory_mm_instances i JOIN venue_binance_account_projections p ON p.credential_id=i.credential_id CROSS JOIN LATERAL jsonb_array_elements(p.projection_json#>'{projection,open_orders}') o WHERE i.instance_id=$1)) ORDER BY created_ms,command_id")
            .bind(id).bind(ms(cutoff)?).fetch_all(&self.pool).await.map_err(db)?;
        rows.iter()
            .map(|r| {
                let phase: String = r.try_get("command_phase").map_err(db)?;
                let target: Option<String> = r.try_get("target_client_order_id").map_err(db)?;
                let intent = if phase == "cancel" {
                    MmCommandIntent::Cancel {
                        target_client_order_id: target
                            .clone()
                            .ok_or(InventoryMmStoreError::Unavailable)?,
                        native_order_id: r.try_get("selected_native_order_id").map_err(db)?,
                    }
                } else {
                    let side: String = r.try_get("order_side").map_err(db)?;
                    let leg: String = r.try_get("position_side").map_err(db)?;
                    MmCommandIntent::Limit {
                        side: decode_enum(side)?,
                        position_side: decode_enum(leg)?,
                        quantity: parse(r.try_get("requested_quantity").map_err(db)?)?,
                        price: parse(r.try_get("limit_price").map_err(db)?)?,
                        reducing: phase == "close",
                    }
                };
                Ok(MmCommandRecord {
                    command_id: r.try_get("command_id").map_err(db)?,
                    client_order_id: r.try_get("client_order_id").map_err(db)?,
                    target_client_order_id: target,
                    native_order_id: r.try_get("native_order_id").map_err(db)?,
                    state: decode_enum(r.try_get("command_state").map_err(db)?)?,
                    intent,
                    created_ms: unsigned(r.try_get("created_ms").map_err(db)?)?,
                    updated_ms: unsigned(r.try_get("updated_ms").map_err(db)?)?,
                })
            })
            .collect()
    }
    /// Only a homogeneous cancel set OR a fresh quote pair can be committed. Replacement
    /// quotes require all previous cancellations to have signed terminal ledger results.
    pub async fn enqueue(
        &self,
        expected: &InventoryMmInstance,
        intents: &[MmCommandIntent],
        generation: u64,
        observed: u64,
        now: u64,
    ) -> Result<bool> {
        if intents.is_empty() {
            return Ok(false);
        }
        let cancelling = intents
            .iter()
            .all(|v| matches!(v, MmCommandIntent::Cancel { .. }));
        if intents.len() > 2
            || (!cancelling
                && intents
                    .iter()
                    .any(|v| matches!(v, MmCommandIntent::Cancel { .. })))
            || (!cancelling && (observed > now || now - observed > 5_000))
        {
            return Err(InventoryMmStoreError::Invalid);
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        let depth = lock_queue(&mut tx, expected).await?;
        if !crate::executor_store::account_queue_has_capacity(depth, intents.len()) {
            return Ok(false);
        }
        let row = sqlx::query(
            "SELECT * FROM venue_inventory_mm_instances WHERE instance_id=$1 FOR UPDATE",
        )
        .bind(&expected.instance_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        let instance = decode(&row)?;
        if instance.revision != expected.revision
            || instance.state == InventoryMmState::Stopped
            || (!cancelling && instance.state != InventoryMmState::Running)
        {
            return Err(InventoryMmStoreError::Conflict);
        }
        let projection_digest:Option<Vec<u8>>=sqlx::query_scalar("SELECT venue_mm_projection_digest(projection_json) FROM venue_binance_account_projections WHERE credential_id=$1 AND owner_user_id=$2 AND private_generation=$3 AND observed_ms=$4 AND COALESCE((projection_json->>'stream_healthy')::boolean,false)")
            .bind(&instance.credential_id).bind(&instance.owner_user_id).bind(ms(generation)?).bind(ms(observed)?).fetch_optional(&mut *tx).await.map_err(db)?;
        if !cancelling && projection_digest.is_none() {
            return Ok(false);
        }
        let own_pending:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE inventory_mm_instance_id=$1 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
            .bind(&instance.instance_id).fetch_one(&mut *tx).await.map_err(db)?;
        if own_pending {
            return Ok(false);
        }
        if !cancelling && !admission(&mut tx, &instance, now, false).await?.is_empty() {
            return Ok(false);
        }
        if !cancelling {
            let unobserved:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE inventory_mm_instance_id=$1 AND command_state='reconciled' AND updated_ms>=$2)")
                .bind(&instance.instance_id).bind(ms(observed)?).fetch_one(&mut *tx).await.map_err(db)?;
            if unobserved {
                return Ok(false);
            }
        }
        for (index, intent) in intents.iter().enumerate() {
            let commitment = digest(&(
                instance.instance_id.as_str(),
                instance.revision,
                index,
                intent,
            ))?;
            let hex = commitment
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            let command_id = format!("mm-{}", &hex[..48]);
            let client = format!("mm{}", &hex[..30]);
            let (phase, kind, side, leg, qty, price, target, native) = match intent {
                MmCommandIntent::Limit {
                    side,
                    position_side,
                    quantity,
                    price,
                    reducing,
                } => {
                    if *quantity <= Decimal::ZERO
                        || *price <= Decimal::ZERO
                        || *position_side == PositionSide::Net
                        || *reducing
                            != matches!(
                                (*position_side, *side),
                                (PositionSide::Long, OrderSide::Sell)
                                    | (PositionSide::Short, OrderSide::Buy)
                            )
                    {
                        return Err(InventoryMmStoreError::Invalid);
                    }
                    (
                        if *reducing { "close" } else { "open" },
                        "limit_post_only",
                        Some(side_name(*side)),
                        Some(leg_name(*position_side)?),
                        Some(quantity.to_string()),
                        Some(price.to_string()),
                        None,
                        None,
                    )
                }
                MmCommandIntent::Cancel {
                    target_client_order_id,
                    native_order_id,
                } => {
                    let owned:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE inventory_mm_instance_id=$1 AND client_order_id=$2 AND command_phase IN ('open','close') AND command_state='reconciled' AND ($3::text IS NULL OR native_order_id=$3))")
                        .bind(&instance.instance_id).bind(target_client_order_id).bind(native_order_id).fetch_one(&mut *tx).await.map_err(db)?;
                    if !owned {
                        return Err(InventoryMmStoreError::Conflict);
                    }
                    (
                        "cancel",
                        "cancel_exact",
                        None,
                        None,
                        None,
                        None,
                        Some(target_client_order_id.as_str()),
                        native_order_id.as_deref(),
                    )
                }
            };
            sqlx::query("INSERT INTO venue_binance_commands(command_id,command_origin,owner_user_id,trading_account_id,credential_id,symbol,command_phase,order_kind,order_side,position_side,requested_quantity,limit_price,target_client_order_id,selected_native_order_id,client_order_id,command_state,created_ms,updated_ms,inventory_mm_instance_id,rule_version,source_digest,inventory_mm_private_generation,inventory_mm_observed_ms,inventory_mm_projection_digest) VALUES($1,'inventory_mm',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'pending',$15,$15,$16,$17,$18,$19,$20,$21)")
                .bind(command_id).bind(&instance.owner_user_id).bind(&instance.trading_account_id).bind(&instance.credential_id).bind(instance.config.symbol.to_string())
                .bind(phase).bind(kind).bind(side).bind(leg).bind(qty).bind(price).bind(target).bind(native).bind(client).bind(ms(now)?).bind(&instance.instance_id)
                .bind(format!("inventory-mm-v1-private-{generation}")).bind(commitment)
                .bind(ms(generation)?).bind(ms(observed)?).bind(&projection_digest)
                .execute(&mut *tx).await.map_err(db)?;
        }
        sqlx::query("UPDATE venue_inventory_mm_instances SET revision=revision+1,updated_ms=$2,last_quote_ms=CASE WHEN $3 THEN last_quote_ms ELSE $2 END WHERE instance_id=$1")
            .bind(&instance.instance_id).bind(ms(now)?).bind(cancelling).execute(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(true)
    }
    /// Recheck under row locks before escalating. Readback retries and process restarts
    /// must not extend the original dispatch's bounded recovery window.
    pub async fn latch_stalled_reconciliation(
        &self,
        instance: &InventoryMmInstance,
        now: u64,
    ) -> Result<bool> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        lock_queue(&mut tx, instance).await?;
        let stalled: Vec<(String, i64)> = sqlx::query_as(
            "SELECT command_id,COALESCE(sending_ms,created_ms) FROM venue_binance_commands WHERE inventory_mm_instance_id=$1 AND owner_user_id=$2 AND trading_account_id=$3 AND credential_id=$4 AND command_state='reconcile_required' ORDER BY command_id FOR UPDATE",
        )
        .bind(&instance.instance_id)
        .bind(&instance.owner_user_id)
        .bind(&instance.trading_account_id)
        .bind(&instance.credential_id)
        .fetch_all(&mut *tx).await.map_err(db)?;
        let deadline = ms(now.saturating_sub(120_000))?;
        let Some((command_id, _)) = stalled.iter().find(|(_, sent)| *sent <= deadline) else {
            tx.commit().await.map_err(db)?;
            return Ok(false);
        };
        let changed = sqlx::query("UPDATE venue_inventory_mm_instances SET attention='command_reconcile_timeout',instance_state='needs_attention',revision=revision+1,updated_ms=$3 WHERE instance_id=$1 AND revision=$2 AND instance_state IN ('running','start_pending') AND attention IS NULL")
            .bind(&instance.instance_id).bind(ms(instance.revision)?).bind(ms(now)?)
            .execute(&mut *tx).await.map_err(db)?.rows_affected() == 1;
        tx.commit().await.map_err(db)?;
        if changed {
            tracing::warn!(instance_id=%instance.instance_id, %command_id,
                "inventory MM original command readback exceeded 120 seconds; quotes remain fenced");
        }
        Ok(changed)
    }

    pub async fn observe(
        &self,
        instance: &InventoryMmInstance,
        baseline: Option<Decimal>,
        peak: Option<Decimal>,
        attention: Option<&str>,
        now: u64,
    ) -> Result<()> {
        let changed=sqlx::query("UPDATE venue_inventory_mm_instances SET baseline_equity=COALESCE(baseline_equity,$3),peak_equity=CASE WHEN $4::text IS NULL THEN peak_equity WHEN peak_equity IS NULL OR peak_equity::numeric<$4::numeric THEN $4 ELSE peak_equity END,attention=COALESCE($5,attention),instance_state=CASE WHEN $5::text IS NOT NULL AND instance_state NOT IN ('stopped','stop_pending') THEN 'needs_attention' ELSE instance_state END,revision=revision+1,updated_ms=$6 WHERE instance_id=$1 AND revision=$2")
            .bind(&instance.instance_id).bind(ms(instance.revision)?).bind(baseline.map(|v|v.to_string())).bind(peak.map(|v|v.to_string())).bind(attention).bind(ms(now)?).execute(&self.pool).await.map_err(db)?;
        if changed.rows_affected() != 1 {
            return Err(InventoryMmStoreError::Conflict);
        }
        Ok(())
    }
    pub async fn mark_running(
        &self,
        instance: &InventoryMmInstance,
        baseline: Decimal,
        now: u64,
    ) -> Result<()> {
        let changed=sqlx::query("UPDATE venue_inventory_mm_instances SET instance_state='running',baseline_equity=COALESCE(baseline_equity,$3),peak_equity=CASE WHEN peak_equity IS NULL OR peak_equity::numeric<$3::numeric THEN $3 ELSE peak_equity END,revision=revision+1,updated_ms=$4 WHERE instance_id=$1 AND revision=$2 AND instance_state='start_pending'")
            .bind(&instance.instance_id).bind(ms(instance.revision)?).bind(baseline.to_string()).bind(ms(now)?).execute(&self.pool).await.map_err(db)?;
        if changed.rows_affected() != 1 {
            return Err(InventoryMmStoreError::Conflict);
        }
        Ok(())
    }
    pub async fn finish_stop(&self, instance: &InventoryMmInstance, now: u64) -> Result<()> {
        let changed=sqlx::query("UPDATE venue_inventory_mm_instances i SET instance_state='stopped',revision=revision+1,updated_ms=$3 WHERE instance_id=$1 AND revision=$2 AND instance_state='stop_pending' AND NOT EXISTS(SELECT 1 FROM venue_binance_commands c WHERE c.inventory_mm_instance_id=i.instance_id AND c.command_state IN ('pending','sending','accepted','reconcile_required')) AND EXISTS(SELECT 1 FROM venue_binance_account_projections p WHERE p.credential_id=i.credential_id AND p.observed_ms>$3-5000 AND p.observed_ms<=$3 AND COALESCE((p.projection_json->>'stream_healthy')::boolean,false) AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(p.projection_json#>'{projection,open_orders}') o JOIN venue_binance_commands c ON c.client_order_id=o->>'client_order_id' WHERE c.inventory_mm_instance_id=i.instance_id))")
            .bind(&instance.instance_id).bind(ms(instance.revision)?).bind(ms(now)?).execute(&self.pool).await.map_err(db)?;
        if changed.rows_affected() != 1 {
            return Err(InventoryMmStoreError::Conflict);
        }
        Ok(())
    }
}

async fn lock_queue(
    connection: &mut PgConnection,
    instance: &InventoryMmInstance,
) -> Result<usize> {
    let depth = crate::executor_store::lock_account_command_queue(
        connection,
        &instance.owner_user_id,
        &instance.trading_account_id,
        &instance.credential_id,
    )
    .await
    .map_err(|_| InventoryMmStoreError::Conflict)?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('venue-strategy-admission:'||$1,0))",
    )
    .bind(&instance.trading_account_id)
    .execute(connection)
    .await
    .map_err(db)?;
    Ok(depth)
}
async fn admission(
    connection: &mut PgConnection,
    instance: &InventoryMmInstance,
    now: u64,
    flat: bool,
) -> Result<Vec<String>> {
    let mut blockers = Vec::new();
    let row=sqlx::query("SELECT verification_json FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND trading_account_id=$3 AND venue='binance' AND deleted_ms IS NULL")
        .bind(&instance.credential_id).bind(&instance.owner_user_id).bind(&instance.trading_account_id)
        .fetch_optional(&mut *connection).await.map_err(db)?;
    let verified = row
        .and_then(|r| r.try_get::<serde_json::Value, _>("verification_json").ok())
        .and_then(|v| serde_json::from_value::<CredentialSummary>(v).ok())
        .is_some_and(|v| {
            v.selectable(now)
                && v.dual_position
                && v.venue == venue_control_protocol::VenueId::Binance
        });
    if !verified {
        blockers.push("credential_not_verified".into());
    }
    // Users allocate strategies to symbols. Only a separate legacy execution path owns the
    // account exclusively; another strategy on this singleton is not an ownership conflict.
    let conflict:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_control_strategy_scopes WHERE trading_account_id=$1 AND venue='binance' AND mode='LIVE')")
        .bind(&instance.trading_account_id)
        .fetch_one(&mut *connection).await.map_err(db)?;
    if conflict {
        blockers.push("legacy_writer_in_use".into());
    }
    let unknown:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands WHERE trading_account_id=$1 AND command_state IN ('pending','sending','accepted','reconcile_required'))")
        .bind(&instance.trading_account_id).fetch_one(&mut *connection).await.map_err(db)?;
    if unknown {
        blockers.push("account_commands_unsettled".into());
    }
    let projection:Option<serde_json::Value>=sqlx::query_scalar("SELECT projection_json->'projection' FROM venue_binance_account_projections WHERE credential_id=$1 AND owner_user_id=$2 AND trading_account_id=$3 AND COALESCE((projection_json->>'stream_healthy')::boolean,false)")
        .bind(&instance.credential_id).bind(&instance.owner_user_id).bind(&instance.trading_account_id)
        .fetch_optional(&mut *connection).await.map_err(db)?;
    match projection.and_then(|v| serde_json::from_value::<TerminalAccountProjection>(v).ok()) {
        Some(p)
            if p.validate().is_ok()
                && p.observed_ms <= now
                && now - p.observed_ms <= 5_000
                && p.position_mode == TerminalPositionMode::Hedge =>
        {
            if (flat
                && p.positions
                    .iter()
                    .any(|v| v.symbol == instance.config.symbol && !v.quantity.is_zero()))
                || p.open_orders
                    .iter()
                    .any(|v| v.symbol == instance.config.symbol)
                || p.conditional_orders
                    .iter()
                    .any(|v| v.symbol == instance.config.symbol)
            {
                blockers.push("symbol_not_flat".into());
            }
        }
        _ => blockers.push("fresh_signed_hedge_projection_required".into()),
    }
    Ok(blockers)
}
fn resumable(instance: &InventoryMmInstance) -> bool {
    matches!(
        instance.state,
        InventoryMmState::Stopped | InventoryMmState::NeedsAttention
    ) && instance.baseline_equity.is_some()
        && instance.last_quote_ms.is_some()
}
fn decode(row: &sqlx::postgres::PgRow) -> Result<InventoryMmInstance> {
    let config: InventoryMmConfig = serde_json::from_value(row.try_get("config_json").map_err(db)?)
        .map_err(|_| InventoryMmStoreError::Unavailable)?;
    config
        .validate()
        .map_err(|_| InventoryMmStoreError::Unavailable)?;
    let state: String = row.try_get("instance_state").map_err(db)?;
    Ok(InventoryMmInstance {
        instance_id: row.try_get("instance_id").map_err(db)?,
        owner_user_id: row.try_get("owner_user_id").map_err(db)?,
        trading_account_id: row.try_get("trading_account_id").map_err(db)?,
        credential_id: row.try_get("credential_id").map_err(db)?,
        config,
        state: serde_json::from_value(serde_json::Value::String(state))
            .map_err(|_| InventoryMmStoreError::Unavailable)?,
        revision: unsigned(row.try_get("revision").map_err(db)?)?,
        baseline_equity: optional_decimal(row.try_get("baseline_equity").map_err(db)?)?,
        peak_equity: optional_decimal(row.try_get("peak_equity").map_err(db)?)?,
        attention: row.try_get("attention").map_err(db)?,
        updated_ms: unsigned(row.try_get("updated_ms").map_err(db)?)?,
        last_quote_ms: row
            .try_get::<Option<i64>, _>("last_quote_ms")
            .map_err(db)?
            .map(unsigned)
            .transpose()?,
    })
}
fn optional_decimal(v: Option<String>) -> Result<Option<Decimal>> {
    v.map(|v| v.parse().map_err(|_| InventoryMmStoreError::Unavailable))
        .transpose()
}
fn ms(v: u64) -> Result<i64> {
    i64::try_from(v).map_err(|_| InventoryMmStoreError::Invalid)
}
fn unsigned(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| InventoryMmStoreError::Unavailable)
}
fn digest(v: &impl Serialize) -> Result<Vec<u8>> {
    Ok(Sha256::digest(serde_json::to_vec(v).map_err(|_| InventoryMmStoreError::Invalid)?).to_vec())
}
fn db(_: sqlx::Error) -> InventoryMmStoreError {
    InventoryMmStoreError::Unavailable
}
fn parse(v: String) -> Result<Decimal> {
    v.parse().map_err(|_| InventoryMmStoreError::Unavailable)
}
fn decode_enum<T: serde::de::DeserializeOwned>(v: String) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(v))
        .map_err(|_| InventoryMmStoreError::Unavailable)
}
fn side_name(v: OrderSide) -> &'static str {
    match v {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}
fn leg_name(v: PositionSide) -> Result<&'static str> {
    match v {
        PositionSide::Long => Ok("long"),
        PositionSide::Short => Ok("short"),
        PositionSide::Net => Err(InventoryMmStoreError::Invalid),
    }
}
