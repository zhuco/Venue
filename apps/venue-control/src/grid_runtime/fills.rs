use super::*;

impl BinanceGridRuntime {
    pub(super) async fn load_fill_batch(
        &self,
        record: &GridRuntimeRecord,
        actual: &ActualSurface,
        included_through_ms: u64,
    ) -> Result<Vec<GridFillAllocation>, BinanceGridRuntimeError> {
        let fills = self
            .store
            .load_unallocated_fills(&record.instance.instance_id, 0, MAX_FILL_BATCH)
            .await?;
        if fills.len() == usize::from(MAX_FILL_BATCH) {
            return Err(BinanceGridRuntimeError::Facts);
        }
        require_complete_fill_baseline(
            fills.iter().map(|fill| fill.observed_ms),
            included_through_ms,
        )?;
        let mut unique = BTreeMap::new();
        for fill in fills {
            let owner = actual
                .ownership
                .get(&fill.client_order_id)
                .ok_or(BinanceGridRuntimeError::Facts)?;
            if fill.instance_id != record.instance.instance_id
                || fill.trading_account_id != record.instance.trading_account_id
                || fill.symbol != record.instance.symbol
                || !fill_matches_owner(&fill, owner)
            {
                return Err(BinanceGridRuntimeError::Facts);
            }
            if let Some(previous) = unique.insert(fill.native_trade_id.clone(), fill.clone())
                && previous != fill
            {
                return Err(BinanceGridRuntimeError::Facts);
            }
        }
        Ok(unique.into_values().collect())
    }
}
