use venue_control::{MIGRATION_0023, MIGRATION_0024};

#[test]
fn grid_hot_batch_migration_preserves_legacy_rows_and_bounds_new_batches() {
    for required in [
        "ADD COLUMN IF NOT EXISTS grid_batch_id TEXT",
        "ADD COLUMN IF NOT EXISTS dispatch_sequence BIGINT",
        "CREATE TABLE IF NOT EXISTS venue_binance_grid_mutation_batches",
        "ADD COLUMN IF NOT EXISTS private_generation BIGINT",
        "ADD COLUMN IF NOT EXISTS private_observed_ms BIGINT",
        "ADD COLUMN IF NOT EXISTS instrument_generation BIGINT",
        "ADD COLUMN IF NOT EXISTS source_event_received_ms BIGINT",
        "venue_binance_grid_mutation_batches_facts_v1",
        "command_count BETWEEN 0 AND 16",
        "SET grid_batch_id = command_id",
        "command_origin = 'grid'",
        "grid_batch_id IS NOT NULL",
        "dispatch_sequence IS NOT NULL",
        "dispatch_sequence BETWEEN 1 AND 16",
        "command_origin <> 'grid'",
        "dispatch_sequence IS NULL",
        "venue_binance_commands_grid_batch_fk",
        "DEFERRABLE INITIALLY DEFERRED",
        "venue_binance_commands_grid_batch_sequence",
        "venue_binance_commands_grid_batch_dispatch",
        "ADD COLUMN IF NOT EXISTS stream_private_generation BIGINT",
        "ADD COLUMN IF NOT EXISTS baseline_private_generation BIGINT",
        "ADD COLUMN IF NOT EXISTS original_quantity TEXT",
        "ADD COLUMN IF NOT EXISTS cumulative_filled_quantity TEXT",
        "ADD COLUMN IF NOT EXISTS order_state TEXT",
        "ADD COLUMN IF NOT EXISTS client_order_id TEXT",
        "venue_binance_account_fills_stream_context_v1",
        "stream_private_generation IS NOT NULL",
        "baseline_private_generation IS NOT NULL",
        "order_state IS NOT NULL",
        "cumulative_filled_quantity::NUMERIC = original_quantity::NUMERIC",
        "cumulative_filled_quantity::NUMERIC < original_quantity::NUMERIC",
    ] {
        assert!(MIGRATION_0023.contains(required), "missing {required}");
    }

    assert!(MIGRATION_0023.contains("WHERE command_origin = 'grid'\n  AND grid_batch_id IS NULL"));
    assert!(!MIGRATION_0023.contains("'legacy:'"));
    assert!(!MIGRATION_0023.contains("UPDATE venue_binance_account_fills"));
}

#[test]
fn batch_chain_migration_binds_input_surface_and_predecessor() {
    for required in [
        "input_desired_digest",
        "predecessor_batch_id",
        "grid_tail_batch_id",
        "venue_binance_grid_mutation_batches_successor",
        "venue_binance_grid_mutation_batches_predecessor_fk",
    ] {
        assert!(MIGRATION_0024.contains(required), "missing {required}");
    }
}

#[test]
fn grid_store_inserts_the_batch_receipt_before_referencing_commands() -> Result<(), &'static str> {
    let source = include_str!("../src/grid_store/hot_batch.rs");
    for function in [
        "pub async fn enqueue_mutation_batch",
        "pub async fn commit_plan_mutation_batch",
    ] {
        let body = source
            .split_once(function)
            .ok_or("missing batch function")?
            .1;
        let receipt = body
            .find("insert_receipt(")
            .ok_or("missing receipt insert")?;
        let commands = body
            .find("insert_mutations(")
            .ok_or("missing command insert")?;
        assert!(
            receipt < commands,
            "{function} references a batch before its receipt"
        );
    }
    Ok(())
}
