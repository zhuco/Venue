-- Existing relations retain proportional sizing; historical commands are unchanged.
ALTER TABLE venue_kol_follow_relations ADD COLUMN IF NOT EXISTS sizing_json JSONB NOT NULL
    DEFAULT '{"mode":"proportional"}'::jsonb
    CHECK (CASE
        WHEN sizing_json = '{"mode":"proportional"}'::jsonb THEN true
        WHEN jsonb_typeof(sizing_json)='object' AND sizing_json->>'mode'='fixed_notional'
          AND jsonb_typeof(sizing_json->'notional')='string'
          AND (sizing_json - 'mode' - 'notional')='{}'::jsonb
          AND (sizing_json->>'notional') ~ '^[0-9]+([.][0-9]+)?$'
        THEN (sizing_json->>'notional')::numeric > 0
          AND (sizing_json->>'notional')::numeric <= max_order_notional::numeric
        ELSE false END);
