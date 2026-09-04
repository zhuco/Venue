use super::*;

impl PgExecutorStore {
    pub async fn dirty_copy_accounts(&self) -> Result<Vec<String>, BinanceCommandLedgerError> {
        sqlx::query_scalar("SELECT DISTINCT r.follower_trading_account_id FROM venue_kol_follow_relations r JOIN venue_kol_profiles p ON p.kol_user_id=r.kol_user_id JOIN venue_kol_copy_targets t ON t.relation_id=r.relation_id WHERE r.relation_state='active' AND r.baseline_json->>'target_model'='1' AND p.profile_state='enabled' AND t.dirty ORDER BY r.follower_trading_account_id LIMIT 200")
            .fetch_all(&self.pool).await.map_err(unavailable)
    }

    /// The prior physical order is settled before another delta is computed. Fills which arrived
    /// in flight only changed the desired target, not an additional independent order quantity.
    pub async fn plan_dirty_copy_target(
        &self,
        account: &str,
        now_ms: u64,
    ) -> Result<bool, BinanceCommandLedgerError> {
        let now = ms(now_ms)?;
        let mut tx = self.pool.begin().await.map_err(unavailable)?;
        let relation = sqlx::query("SELECT r.relation_id,r.follower_user_id,r.credential_id,r.revision,r.max_order_notional,r.max_total_notional,r.max_deviation_bps FROM venue_kol_follow_relations r JOIN venue_kol_profiles p ON p.kol_user_id=r.kol_user_id WHERE r.follower_trading_account_id=$1 AND r.relation_state='active' AND r.baseline_json->>'target_model'='1' AND p.profile_state='enabled' ORDER BY r.relation_id FOR UPDATE OF r")
            .bind(account).fetch_optional(&mut *tx).await.map_err(unavailable)?;
        let Some(relation) = relation else {
            return Ok(false);
        };
        let relation_id: String = relation.try_get("relation_id").map_err(unavailable)?;
        let owner: String = relation.try_get("follower_user_id").map_err(unavailable)?;
        let credential: String = relation.try_get("credential_id").map_err(unavailable)?;
        let revision: i64 = relation.try_get("revision").map_err(unavailable)?;
        if lock_account_command_queue(&mut tx, &owner, account, &credential).await? != 0 {
            return Ok(false);
        }
        let targets = sqlx::query("SELECT t.symbol,t.position_side,t.target_quantity,t.observed_quantity,t.target_revision,f.payload_digest,f.price,f.occurred_ms FROM venue_kol_copy_targets t JOIN venue_kol_follow_relations r ON r.relation_id=t.relation_id JOIN venue_kol_source_fills f ON f.kol_trading_account_id=r.leader_trading_account_id AND f.native_symbol=t.last_native_symbol AND f.native_trade_id=t.last_native_trade_id WHERE t.relation_id=$1 AND t.dirty AND r.allowed_symbols @> jsonb_build_array(t.symbol) ORDER BY (t.target_quantity::numeric<t.observed_quantity::numeric) DESC,t.symbol,t.position_side FOR UPDATE OF t")
            .bind(&relation_id).fetch_all(&mut *tx).await.map_err(unavailable)?;
        for target in targets {
            let desired = decimal(&target, "target_quantity")?;
            let observed = decimal(&target, "observed_quantity")?;
            if desired < Decimal::ZERO || observed < Decimal::ZERO {
                return Err(BinanceCommandLedgerError::Conflict);
            }
            let symbol: String = target.try_get("symbol").map_err(unavailable)?;
            let leg: String = target.try_get("position_side").map_err(unavailable)?;
            let side = match leg.as_str() {
                "long" => venue_domain::domain::PositionSide::Long,
                "short" => venue_domain::domain::PositionSide::Short,
                _ => return Err(BinanceCommandLedgerError::Conflict),
            };
            let target_revision: i64 = target.try_get("target_revision").map_err(unavailable)?;
            let mut inserted = false;
            if desired != observed {
                let opening = desired > observed;
                let phase = if opening { "open" } else { "close" };
                let id = deterministic_id(&relation_id, &symbol, side, target_revision, phase);
                let digest: Vec<u8> = target.try_get("payload_digest").map_err(unavailable)?;
                let copy_risk = crate::executor_exchange::CopyRiskContext {
                    max_order_notional: decimal(&relation, "max_order_notional")?,
                    max_total_notional: decimal(&relation, "max_total_notional")?,
                    max_deviation_bps: u32::try_from(
                        relation
                            .try_get::<i32, _>("max_deviation_bps")
                            .map_err(unavailable)?,
                    )
                    .map_err(|_| BinanceCommandLedgerError::Conflict)?,
                    source_price: decimal(&target, "price")?,
                    source_occurred_ms: unsigned_ms(&target, "occurred_ms")?,
                };
                copy_risk
                    .validate()
                    .map_err(|_| BinanceCommandLedgerError::Conflict)?;
                let copy_risk = serde_json::to_value(copy_risk)
                    .map_err(|_| BinanceCommandLedgerError::Conflict)?;
                inserted = sqlx::query("INSERT INTO venue_binance_commands (command_id,command_origin,relation_id,relation_revision,target_revision,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,target_quantity,rule_version,client_order_id,command_state,source_digest,created_ms,updated_ms,copy_risk) VALUES ($1,'copy',$2,$3,$4,$5,$6,$7,$8,$9,$10,'market',$11,$12,$13,'binance-pm-um-v1',$1,'pending',$14,$15,$15,$16) ON CONFLICT DO NOTHING")
                    .bind(id).bind(&relation_id).bind(revision).bind(target_revision).bind(&owner)
                    .bind(account).bind(&credential).bind(&symbol).bind(&leg).bind(phase).bind(order_for(side, opening))
                    .bind((desired-observed).abs().to_string()).bind(desired.to_string()).bind(digest).bind(now).bind(copy_risk)
                    .execute(&mut *tx).await.map_err(unavailable)?.rows_affected() == 1;
            }
            // A rejected identical target is not an automatic retry. New source/position facts
            // increment the revision and permit a new deterministic command identity.
            sqlx::query("UPDATE venue_kol_copy_targets SET dirty=false,updated_ms=$4 WHERE relation_id=$1 AND symbol=$2 AND position_side=$3 AND target_revision=$5")
                .bind(&relation_id).bind(&symbol).bind(&leg).bind(now).bind(target_revision)
                .execute(&mut *tx).await.map_err(unavailable)?;
            if inserted {
                tx.commit().await.map_err(unavailable)?;
                return Ok(true);
            }
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(false)
    }
}

fn unavailable(_: sqlx::Error) -> BinanceCommandLedgerError {
    BinanceCommandLedgerError::Unavailable
}
