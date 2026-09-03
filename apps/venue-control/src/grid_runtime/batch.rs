use sha2::{Digest, Sha256};

use super::{
    ActualSurface, BinanceGridRuntimeError, GridCommandIntent, GridDesiredOrder,
    GridDesiredSurface, GridLedgerCommand, GridOrderOwnership, GridOwnedOrderState,
    GridRuntimeRecord, RULE_VERSION_PREFIX, durable_id, rule_version,
};
use crate::grid_store::{GridBatchPlacement, GridMutationBatch};

pub(super) fn desired_diff<'a>(
    desired: &'a GridDesiredSurface,
    actual: &'a ActualSurface,
) -> (Vec<&'a GridDesiredOrder>, Vec<&'a str>) {
    let placements = desired
        .orders
        .iter()
        .filter(|order| {
            !actual.orders.contains_key(&order.client_order_id)
                && !actual.ownership.contains_key(&order.client_order_id)
        })
        .collect();
    let desired_clients = desired
        .orders
        .iter()
        .map(|order| order.client_order_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let cancellations = actual
        .orders
        .keys()
        .map(String::as_str)
        .filter(|client| !desired_clients.contains(client))
        .collect();
    (placements, cancellations)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn prepare_mutation_batch(
    record: &GridRuntimeRecord,
    desired: &GridDesiredSurface,
    placements: Vec<&GridDesiredOrder>,
    cancellations: Vec<&str>,
    in_flight: usize,
    instrument_generation: u64,
    now_ms: u64,
) -> Result<GridMutationBatch, BinanceGridRuntimeError> {
    let (placement_count, cancellation_count) =
        selected_batch_counts(placements.len(), cancellations.len(), in_flight);
    let placements = placements
        .into_iter()
        .take(placement_count)
        .map(|order| placement(record, desired, order, instrument_generation, now_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let cancellations = cancellations
        .into_iter()
        .take(cancellation_count)
        .map(|target| cancellation(record, desired, target))
        .collect::<Vec<_>>();
    let batch_id = mutation_batch_id(record, desired, &placements, &cancellations, &[]);
    Ok(GridMutationBatch {
        batch_id,
        instance_id: record.instance.instance_id.clone(),
        expected_instance_revision: record.instance.revision,
        config_revision: record.instance.config_revision,
        plan_revision: desired.plan_revision,
        desired_digest: desired.desired_digest,
        placements,
        cancellations,
    })
}

pub(super) fn selected_batch_counts(
    placements: usize,
    cancellations: usize,
    in_flight: usize,
) -> (usize, usize) {
    let capacity = crate::grid_store::MAX_GRID_MUTATION_BATCH_COMMANDS.saturating_sub(in_flight);
    let selected_placements = placements.min(capacity);
    let selected_cancellations = if selected_placements == placements {
        cancellations.min(capacity.saturating_sub(selected_placements))
    } else {
        0
    };
    (selected_placements, selected_cancellations)
}

pub(super) fn bind_plan_batch_identity(
    batch: &mut GridMutationBatch,
    record: &GridRuntimeRecord,
    desired: &GridDesiredSurface,
    native_trade_ids: &[String],
) {
    batch.batch_id = mutation_batch_id(
        record,
        desired,
        &batch.placements,
        &batch.cancellations,
        native_trade_ids,
    );
}

fn placement(
    record: &GridRuntimeRecord,
    desired: &GridDesiredSurface,
    order: &GridDesiredOrder,
    instrument_generation: u64,
    now_ms: u64,
) -> Result<GridBatchPlacement, BinanceGridRuntimeError> {
    let encoded = order.key.encoded();
    let semantic = format!("place:{encoded}");
    let command_id = durable_id(
        "gp",
        &record.instance.instance_id,
        record.instance.config_revision,
        desired.plan_revision,
        &semantic,
        58,
    );
    let command = GridLedgerCommand {
        command_id: command_id.clone(),
        client_order_id: order.client_order_id.clone(),
        instance_id: record.instance.instance_id.clone(),
        config_revision: record.instance.config_revision,
        plan_revision: desired.plan_revision,
        semantic_key: encoded,
        rule_version: rule_version(instrument_generation),
        source_digest: desired.desired_digest,
        intent: GridCommandIntent::LimitPostOnly {
            key: order.key.clone(),
            quantity: order.quantity,
            limit_price: order.limit_price,
        },
    };
    Ok(GridBatchPlacement {
        ownership: GridOrderOwnership {
            instance_id: record.instance.instance_id.clone(),
            trading_account_id: record.instance.trading_account_id.clone(),
            config_revision: record.instance.config_revision,
            plan_revision: desired.plan_revision,
            key: order.key.clone(),
            place_command_id: command_id,
            client_order_id: order.client_order_id.clone(),
            symbol: record.instance.symbol.clone(),
            quantity: order.quantity,
            filled_quantity: rust_decimal::Decimal::ZERO,
            limit_price: order.limit_price,
            native_order_id: None,
            state: GridOwnedOrderState::Working,
            first_seen_ms: now_ms,
            last_seen_ms: now_ms,
        },
        command,
    })
}

fn cancellation(
    record: &GridRuntimeRecord,
    desired: &GridDesiredSurface,
    target: &str,
) -> GridLedgerCommand {
    let semantic = format!("cancel:{target}");
    GridLedgerCommand {
        command_id: durable_id(
            "gc",
            &record.instance.instance_id,
            record.instance.config_revision,
            desired.plan_revision,
            &semantic,
            58,
        ),
        client_order_id: durable_id(
            "vgc",
            &record.instance.instance_id,
            record.instance.config_revision,
            desired.plan_revision,
            &semantic,
            36,
        ),
        instance_id: record.instance.instance_id.clone(),
        config_revision: record.instance.config_revision,
        plan_revision: desired.plan_revision,
        semantic_key: semantic,
        rule_version: RULE_VERSION_PREFIX.to_owned(),
        source_digest: desired.desired_digest,
        intent: GridCommandIntent::Cancel {
            target_client_order_id: target.to_owned(),
        },
    }
}

fn mutation_batch_id(
    record: &GridRuntimeRecord,
    desired: &GridDesiredSurface,
    placements: &[GridBatchPlacement],
    cancellations: &[GridLedgerCommand],
    native_trade_ids: &[String],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"venue-grid-mutation-batch-v1");
    hasher.update(record.instance.instance_id.as_bytes());
    hasher.update(record.instance.config_revision.to_be_bytes());
    hasher.update(desired.plan_revision.to_be_bytes());
    hasher.update(desired.desired_digest);
    for placement in placements {
        hasher.update(placement.command.command_id.as_bytes());
    }
    for cancellation in cancellations {
        hasher.update(cancellation.command_id.as_bytes());
    }
    let mut native_trade_ids = native_trade_ids
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    native_trade_ids.sort_unstable();
    for native_trade_id in native_trade_ids {
        hasher.update(native_trade_id.as_bytes());
    }
    let encoded = format!("{:x}", hasher.finalize());
    format!("gb-{}", &encoded[..61])
}
