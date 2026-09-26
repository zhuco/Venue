-- Preserve existing authorizations and historical command quantities.
ALTER TABLE venue_kol_follow_relations DROP CONSTRAINT venue_kol_follow_relations_check4;
ALTER TABLE venue_kol_follow_relations ADD CONSTRAINT venue_kol_follow_relations_sizing_json_check CHECK (CASE
 WHEN sizing_json = '{"mode":"proportional"}'::jsonb THEN true
 WHEN jsonb_typeof(sizing_json)='object' AND sizing_json->>'mode'='fixed_notional'
   AND jsonb_typeof(sizing_json->'notional')='string'
   AND (sizing_json - 'mode' - 'notional')='{}'::jsonb
   AND (sizing_json->>'notional') ~ '^[0-9]+([.][0-9]+)?$'
 THEN (sizing_json->>'notional')::numeric > 0
   AND (sizing_json->>'notional')::numeric <= max_order_notional::numeric
 WHEN jsonb_typeof(sizing_json)='object' AND sizing_json->>'mode'='source_ratio'
   AND jsonb_typeof(sizing_json->'ratio')='string'
   AND (sizing_json - 'mode' - 'ratio')='{}'::jsonb
   AND (sizing_json->>'ratio') ~ '^[0-9]+([.][0-9]+)?$'
 THEN (sizing_json->>'ratio')::numeric > 0
   AND (sizing_json->>'ratio')::numeric <= 1 AND multiplier::numeric = 1
 ELSE false END);
