use super::*;
use venue_control_protocol::terminal_position::{PositionAction, TerminalPositionActionRequest};

impl AccountService {
    pub async fn enqueue_position_action(
        &self,
        principal: &Principal,
        request: TerminalPositionActionRequest,
        now_ms: u64,
    ) -> Result<ExecutorCommandSummary, AccountError> {
        request.validate().map_err(|_| error(Code::InvalidInput))?;
        let digest: [u8; 32] =
            Sha256::digest(serde_json::to_vec(&request).map_err(|_| error(Code::InvalidInput))?)
                .into();
        let owner = &principal.user.user_id;
        let mut tx = self.pool.begin().await.map_err(database_error)?;
        if let Some(row) = load_terminal_command(&mut tx, owner, &request.request_id).await? {
            validate_replay_digest(&row, &digest)?;
            let summary = command_summary(&row)?;
            tx.commit().await.map_err(database_error)?;
            return Ok(summary);
        }
        let account: String = sqlx::query_scalar("SELECT trading_account_id FROM venue_api_credentials WHERE credential_id=$1 AND user_id=$2 AND deleted_ms IS NULL AND trading_account_id IS NOT NULL AND verification_json->>'verification'='verified'")
            .bind(&request.credential_id).bind(owner).fetch_optional(&mut *tx)
            .await.map_err(database_error)?.ok_or(error(Code::VerificationRequired))?;
        let depth = lock_account_command_queue(&mut *tx, owner, &account, &request.credential_id)
            .await
            .map_err(account_admission_error)?;
        if let Some(row) = load_terminal_command(&mut tx, owner, &request.request_id).await? {
            validate_replay_digest(&row, &digest)?;
            let summary = command_summary(&row)?;
            tx.commit().await.map_err(database_error)?;
            return Ok(summary);
        }
        let reverse = request.action == PositionAction::Reverse;
        if !account_queue_has_capacity(depth, if reverse { 2 } else { 1 }) {
            return Err(error(Code::RateLimited));
        }
        let busy: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM venue_binance_commands c JOIN venue_terminal_position_commands p ON p.command_id=c.command_id WHERE c.trading_account_id=$1 AND c.symbol=$2 AND c.command_state IN ('pending','sending','accepted','reconcile_required'))")
            .bind(&account).bind(request.symbol.to_string()).fetch_one(&mut *tx).await.map_err(database_error)?;
        if busy {
            return Err(error(Code::Conflict));
        }
        reject_legacy_writer(&self.pool, &account).await?;
        let projection =
            crate::private_projection::BinancePrivateProjectionStore::new(self.pool.clone())
                .load_healthy_owned(owner, &request.credential_id)
                .await
                .map_err(|_| error(Code::Unavailable))?
                .ok_or(error(Code::VerificationRequired))?;
        if projection.trading_account_id != account
            || projection.observed_ms > now_ms
            || now_ms.saturating_sub(projection.observed_ms) > MAX_TERMINAL_PROJECTION_AGE_MS
        {
            return Err(error(Code::VerificationRequired));
        }
        let quantity = projection
            .positions
            .iter()
            .find(|row| row.symbol == request.symbol && row.position_side == request.position_side)
            .map(|row| request.quantity.min(row.quantity))
            .filter(|quantity| *quantity > Decimal::ZERO)
            .ok_or(error(Code::Conflict))?;
        let close_id = super::super::crypto::opaque_id()?;
        let close_request = if reverse {
            super::super::crypto::opaque_id()?
        } else {
            request.request_id.clone()
        };
        insert_position_command(
            &mut tx,
            owner,
            &account,
            &request,
            &close_id,
            &close_request,
            request.position_side,
            true,
            quantity,
            &digest,
            now_ms,
        )
        .await?;
        sqlx::query("INSERT INTO venue_terminal_position_commands(command_id) VALUES ($1)")
            .bind(&close_id)
            .execute(&mut *tx)
            .await
            .map_err(database_error)?;
        if reverse {
            let open_id = super::super::crypto::opaque_id()?;
            let side = match request.position_side {
                PositionSide::Long => PositionSide::Short,
                PositionSide::Short => PositionSide::Long,
                PositionSide::Net => return Err(error(Code::InvalidInput)),
            };
            insert_position_command(
                &mut tx,
                owner,
                &account,
                &request,
                &open_id,
                &request.request_id,
                side,
                false,
                quantity,
                &digest,
                now_ms,
            )
            .await?;
            sqlx::query("INSERT INTO venue_terminal_position_commands(command_id,reverse_parent_id,released) VALUES ($1,$2,false)")
                .bind(open_id).bind(close_id).execute(&mut *tx).await.map_err(database_error)?;
        }
        let row = load_terminal_command(&mut tx, owner, &request.request_id)
            .await?
            .ok_or(error(Code::Unavailable))?;
        let summary = command_summary(&row)?;
        tx.commit().await.map_err(database_error)?;
        Ok(summary)
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_position_command(
    connection: &mut PgConnection,
    owner: &str,
    account: &str,
    request: &TerminalPositionActionRequest,
    command_id: &str,
    request_id: &str,
    position_side: PositionSide,
    reducing: bool,
    quantity: Decimal,
    digest: &[u8; 32],
    now_ms: u64,
) -> Result<(), AccountError> {
    let side = match (position_side, reducing) {
        (PositionSide::Long, true) | (PositionSide::Short, false) => OrderSide::Sell,
        (PositionSide::Long, false) | (PositionSide::Short, true) => OrderSide::Buy,
        _ => return Err(error(Code::InvalidInput)),
    };
    sqlx::query("INSERT INTO venue_binance_commands(command_id,command_origin,request_id,owner_user_id,trading_account_id,credential_id,symbol,position_side,command_phase,order_kind,order_side,requested_quantity,rule_version,client_order_id,command_state,source_digest,created_ms,updated_ms) VALUES ($1,'terminal',$2,$3,$4,$5,$6,$7,$8,'market',$9,$10,'terminal-position-v1',$11,'pending',$12,$13,$13)")
        .bind(command_id).bind(request_id).bind(owner).bind(account).bind(&request.credential_id)
        .bind(request.symbol.to_string()).bind(position_side_name(position_side))
        .bind(if reducing { "close" } else { "open" }).bind(order_side_name(side))
        .bind(quantity.normalize().to_string()).bind(terminal_client_order_id(owner, request_id))
        .bind(digest.as_slice()).bind(ms(now_ms)?).execute(connection).await.map_err(database_error)?;
    Ok(())
}
