-- Separate strategy identity and command origin; existing Grid rows remain unchanged.
CREATE TABLE IF NOT EXISTS venue_inventory_mm_instances (
 instance_id TEXT PRIMARY KEY,
 owner_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
 trading_account_id TEXT NOT NULL,
 credential_id TEXT NOT NULL,
 create_request_id TEXT NOT NULL,
 create_digest BYTEA NOT NULL CHECK(octet_length(create_digest)=32),
 symbol TEXT NOT NULL CHECK(symbol ~ '^[A-Z0-9]+/[A-Z0-9]+$'),
 config_json JSONB NOT NULL CHECK(jsonb_typeof(config_json)='object'),
 instance_state TEXT NOT NULL CHECK(instance_state IN ('stopped','start_pending','running','stop_pending','needs_attention')),
 revision BIGINT NOT NULL CHECK(revision>0),
 baseline_equity TEXT,
 peak_equity TEXT,
 attention TEXT,
 created_ms BIGINT NOT NULL CHECK(created_ms>0),
 updated_ms BIGINT NOT NULL CHECK(updated_ms>=created_ms),
 last_quote_ms BIGINT,
 FOREIGN KEY(credential_id,owner_user_id,trading_account_id)
 REFERENCES venue_api_credentials(credential_id,user_id,trading_account_id),
 UNIQUE(owner_user_id,create_request_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS venue_inventory_mm_identity ON venue_inventory_mm_instances(instance_id,owner_user_id,trading_account_id,credential_id,symbol);
-- Symbol allocation is a user convention, not a strategy occupancy constraint.
CREATE INDEX IF NOT EXISTS venue_inventory_mm_account_symbol ON venue_inventory_mm_instances(trading_account_id,symbol);
CREATE TABLE IF NOT EXISTS venue_inventory_mm_lifecycle (
 owner_user_id TEXT NOT NULL REFERENCES venue_users(user_id),
 request_id TEXT NOT NULL,
 instance_id TEXT NOT NULL REFERENCES venue_inventory_mm_instances(instance_id),
 request_digest BYTEA NOT NULL CHECK(octet_length(request_digest)=32),
 PRIMARY KEY(owner_user_id,request_id)
);
ALTER TABLE venue_binance_commands ADD COLUMN IF NOT EXISTS inventory_mm_instance_id TEXT REFERENCES venue_inventory_mm_instances(instance_id);
ALTER TABLE venue_binance_commands ADD COLUMN IF NOT EXISTS inventory_mm_private_generation BIGINT;
ALTER TABLE venue_binance_commands ADD COLUMN IF NOT EXISTS inventory_mm_observed_ms BIGINT;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS inventory_mm_command_identity;
ALTER TABLE venue_binance_commands ADD CONSTRAINT inventory_mm_command_identity FOREIGN KEY(inventory_mm_instance_id,owner_user_id,trading_account_id,credential_id,symbol) REFERENCES venue_inventory_mm_instances(instance_id,owner_user_id,trading_account_id,credential_id,symbol);
DO $$
DECLARE n TEXT; e TEXT;
BEGIN
 FOR n,e IN SELECT conname,pg_get_expr(conbin,conrelid) FROM pg_constraint
 WHERE conrelid='venue_binance_commands'::regclass AND contype='c'
 AND pg_get_expr(conbin,conrelid) NOT LIKE '%inventory_mm%'
 AND (pg_get_expr(conbin,conrelid) LIKE '%command_origin%'
 OR pg_get_expr(conbin,conrelid) LIKE '%command_phase%'
 OR pg_get_expr(conbin,conrelid) LIKE '%order_kind%')
 LOOP
  EXECUTE format('ALTER TABLE venue_binance_commands DROP CONSTRAINT %I',n);
  EXECUTE format('ALTER TABLE venue_binance_commands ADD CONSTRAINT %I CHECK(command_origin=''inventory_mm'' OR (%s))',n,e);
 END LOOP;
END $$;
ALTER TABLE venue_binance_commands DROP CONSTRAINT IF EXISTS inventory_mm_command_shape;
ALTER TABLE venue_binance_commands ADD CONSTRAINT inventory_mm_command_shape CHECK (
 (command_origin<>'inventory_mm' AND inventory_mm_instance_id IS NULL AND inventory_mm_private_generation IS NULL AND inventory_mm_observed_ms IS NULL) OR
 (command_origin='inventory_mm' AND inventory_mm_instance_id IS NOT NULL
 AND relation_id IS NULL AND request_id IS NULL AND relation_revision IS NULL AND target_revision IS NULL AND target_quantity IS NULL AND grid_instance_id IS NULL
 AND grid_batch_id IS NULL AND dispatch_sequence IS NULL AND grid_config_revision IS NULL
 AND grid_plan_revision IS NULL AND grid_semantic_key IS NULL AND mirror_order_id IS NULL
 AND copy_risk IS NULL AND strategy_command IS NULL AND strategy_venue IS NULL AND strategy_nonce IS NULL AND source_digest IS NOT NULL AND trigger_price IS NULL AND working_type IS NULL
 AND ((command_phase='cancel' AND order_kind='cancel_exact' AND target_client_order_id IS NOT NULL
       AND order_side IS NULL AND position_side IS NULL AND requested_quantity IS NULL AND limit_price IS NULL)
 OR (command_phase IN ('open','close') AND order_kind='limit_post_only'
       AND position_side IS NOT NULL AND order_side IS NOT NULL
       AND inventory_mm_private_generation IS NOT NULL AND inventory_mm_private_generation>0 AND inventory_mm_observed_ms IS NOT NULL AND inventory_mm_observed_ms>0
       AND target_client_order_id IS NULL AND selected_native_order_id IS NULL
       AND requested_quantity IS NOT NULL AND requested_quantity::numeric>0
       AND limit_price IS NOT NULL AND limit_price::numeric>0
       AND ((command_phase='open' AND ((position_side='long' AND order_side='buy') OR (position_side='short' AND order_side='sell')))
         OR (command_phase='close' AND ((position_side='long' AND order_side='sell') OR (position_side='short' AND order_side='buy')))))))
);
CREATE INDEX IF NOT EXISTS venue_inventory_mm_commands ON venue_binance_commands(inventory_mm_instance_id,created_ms,command_id)
 WHERE inventory_mm_instance_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS venue_inventory_mm_recent_commands ON venue_binance_commands(inventory_mm_instance_id,updated_ms)
 WHERE inventory_mm_instance_id IS NOT NULL;

CREATE OR REPLACE FUNCTION venue_reject_legacy_scope_with_inventory_mm() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.venue='binance' AND NEW.mode='LIVE' THEN
  PERFORM pg_advisory_xact_lock(hashtextextended('venue-strategy-admission:'||NEW.trading_account_id,0));
  IF EXISTS(SELECT 1 FROM venue_inventory_mm_instances WHERE trading_account_id=NEW.trading_account_id AND instance_state<>'stopped') THEN
   RAISE EXCEPTION 'legacy scope conflicts with inventory MM';
  END IF;
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS venue_reject_legacy_scope_with_inventory_mm_trigger ON venue_control_strategy_scopes;
CREATE TRIGGER venue_reject_legacy_scope_with_inventory_mm_trigger BEFORE INSERT OR UPDATE OF venue,mode,trading_account_id ON venue_control_strategy_scopes FOR EACH ROW EXECUTE FUNCTION venue_reject_legacy_scope_with_inventory_mm();

DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='venue_control_api') THEN
  GRANT SELECT,INSERT,UPDATE,DELETE ON venue_inventory_mm_instances,venue_inventory_mm_lifecycle TO venue_control_api;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='venue_binance_executor') THEN
  GRANT SELECT,INSERT,UPDATE,DELETE ON venue_inventory_mm_instances,venue_inventory_mm_lifecycle TO venue_binance_executor;
 END IF;
END $$;
