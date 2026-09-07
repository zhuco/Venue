use super::*;

impl AccountService {
    pub(super) async fn enqueue_terminal_replace(
        &self,
        principal: &Principal,
        request: TerminalCancelRequest,
        now: u64,
    ) -> Result<ExecutorCommandSummary, AccountError> {
        let price = request.replacement_price.ok_or(error(Code::InvalidInput))?;
        let owner = &principal.user.user_id;
        let digest: [u8; 32] =
            Sha256::digest(serde_json::to_vec(&request).map_err(|_| error(Code::InvalidInput))?)
                .into();
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        if let Some(row) = load_terminal_command(&mut tx, owner, &request.request_id).await? {
            validate_replay_digest(&row, &digest)?;
            return command_summary(&row);
        }
        let account: String = sqlx::query_scalar("SELECT trading_account_id FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL AND verification_json->>'verification'='verified' AND EXISTS(SELECT 1 FROM venue_user_trading_accounts a WHERE a.trading_account_id=venue_api_credentials.trading_account_id AND a.user_id=$2 AND a.venue='binance')")
            .bind(&request.credential_id).bind(owner).fetch_optional(&mut *tx).await.map_err(database_error)?.ok_or(error(Code::VerificationRequired))?;
        let depth = lock_account_command_queue(&mut tx, owner, &account, &request.credential_id)
            .await
            .map_err(account_admission_error)?;
        if let Some(row) = load_terminal_command(&mut tx, owner, &request.request_id).await? {
            validate_replay_digest(&row, &digest)?;
            return command_summary(&row);
        }
        let verified: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND trading_account_id=$3 AND deleted_ms IS NULL AND verification_json->>'verification'='verified')")
            .bind(&request.credential_id).bind(owner).bind(&account).fetch_one(&mut *tx).await.map_err(database_error)?;
        if !verified {
            return Err(error(Code::VerificationRequired));
        }
        if !account_queue_has_capacity(depth, 2) {
            return Err(error(Code::RateLimited));
        }
        reject_legacy_writer(&self.pool, &account).await?;
        let projection =
            crate::private_projection::BinancePrivateProjectionStore::new(self.pool.clone())
                .load_owned(owner, &request.credential_id)
                .await
                .map_err(|_| error(Code::Unavailable))?
                .ok_or(error(Code::VerificationRequired))?;
        if projection.trading_account_id != account
            || projection.observed_ms > now
            || now.saturating_sub(projection.observed_ms) > MAX_TERMINAL_PROJECTION_AGE_MS
        {
            return Err(error(Code::VerificationRequired));
        }
        let order = projection
            .open_orders
            .iter()
            .find(|o| {
                o.symbol == request.symbol
                    && o.native_order_id.as_deref() == Some(&request.native_order_id)
            })
            .ok_or(error(Code::Conflict))?;
        let quantity = order.quantity;
        if order.client_order_id.trim().is_empty()
            || quantity <= Decimal::ZERO
            || order
                .filled_quantity
                .is_none_or(|q| q < Decimal::ZERO || q >= quantity)
            || order
                .limit_price
                .is_none_or(|p| p <= Decimal::ZERO || p == price)
        {
            return Err(error(Code::Conflict));
        }
        let kind = match (order.post_only, order.time_in_force) {
            (true, Some(venue_domain::LimitTimeInForce::PostOnly)) => "limit_post_only",
            (false, Some(venue_domain::LimitTimeInForce::Gtc)) => "limit_gtc",
            _ => return Err(error(Code::InvalidInput)),
        };
        let reducing = match (order.position_side, order.order_side) {
            (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy) => true,
            (PositionSide::Long, OrderSide::Buy) | (PositionSide::Short, OrderSide::Sell)
                if !order.reduce_only =>
            {
                false
            }
            _ => return Err(error(Code::InvalidInput)),
        };
        let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands c WHERE c.trading_account_id=$1 AND c.symbol=$2 AND c.selected_native_order_id=$3 AND (c.command_state IN ('pending','sending','accepted','reconcile_required') OR (c.command_state='reconciled' AND EXISTS(SELECT 1 FROM venue_terminal_replacements r WHERE r.cancel_command_id=c.command_id))))")
            .bind(&account).bind(request.symbol.to_string()).bind(&request.native_order_id).fetch_one(&mut *tx).await.map_err(database_error)?;
        if busy {
            return Err(error(Code::Conflict));
        }
        let cancel_id = super::super::crypto::opaque_id()?;
        let cancel_request = super::super::crypto::opaque_id()?;
        let child_id = super::super::crypto::opaque_id()?;
        sqlx::query("INSERT INTO venue_binance_commands(command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,symbol,command_phase,order_kind,rule_version,selected_native_order_id,target_client_order_id,client_order_id,command_state,source_digest,created_ms,updated_ms) VALUES($1,'terminal',$2,$3,$4,$5,$6,'cancel','cancel_exact','terminal-replace-v1',$7,$8,$9,'pending',$10,$11,$11)")
            .bind(&cancel_id).bind(&cancel_request).bind(owner).bind(&account).bind(&request.credential_id).bind(request.symbol.to_string())
            .bind(&request.native_order_id).bind(&order.client_order_id).bind(terminal_client_order_id(owner, &cancel_request)).bind(digest.as_slice()).bind(ms(now)?).execute(&mut *tx).await.map_err(database_error)?;
        sqlx::query("INSERT INTO venue_binance_commands(command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,symbol,command_phase,order_kind,rule_version,position_side,order_side,requested_quantity,limit_price,client_order_id,command_state,source_digest,created_ms,updated_ms) VALUES($1,'terminal',$2,$3,$4,$5,$6,$7,$8,'terminal-replace-v1',$9,$10,$11,$12,$13,'pending',$14,$15,$15)")
            .bind(&child_id).bind(&request.request_id).bind(owner).bind(&account).bind(&request.credential_id).bind(request.symbol.to_string())
            .bind(if reducing {"close"} else {"open"}).bind(kind).bind(position_side_name(order.position_side)).bind(order_side_name(order.order_side))
            .bind(quantity.normalize().to_string()).bind(price.normalize().to_string()).bind(terminal_client_order_id(owner, &request.request_id)).bind(digest.as_slice()).bind(ms(now)?).execute(&mut *tx).await.map_err(database_error)?;
        sqlx::query("INSERT INTO venue_terminal_replacements(command_id,cancel_command_id,original_quantity) VALUES($1,$2,$3)")
            .bind(child_id).bind(cancel_id).bind(quantity.normalize().to_string()).execute(&mut *tx).await.map_err(database_error)?;
        let row = load_terminal_command(&mut tx, owner, &request.request_id)
            .await?
            .ok_or(error(Code::Unavailable))?;
        let summary = command_summary(&row)?;
        tx.commit().await.map_err(database_error)?;
        Ok(summary)
    }
}
