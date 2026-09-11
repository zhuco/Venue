-- Repair/check only unresolved native identities, not every historical grid owner.
CREATE INDEX IF NOT EXISTS venue_grid_owner_missing_native
ON venue_binance_grid_order_owners (instance_id, place_command_id)
WHERE native_order_id IS NULL;
CREATE INDEX IF NOT EXISTS venue_grid_command_missing_native
ON venue_binance_commands (grid_instance_id, command_id)
WHERE command_origin='grid' AND command_state='reconciled'
  AND native_order_id IS NULL AND selected_native_order_id IS NULL;
