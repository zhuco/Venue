ALTER TABLE venue_api_credentials
    ADD COLUMN IF NOT EXISTS follow_authorization_json JSONB;

UPDATE venue_api_credentials
SET follow_authorization_json='{"sizing":{"mode":"proportional"},"multiplier":"1"}'::jsonb
WHERE follow_authorization_json IS NULL;

ALTER TABLE venue_api_credentials
    ALTER COLUMN follow_authorization_json
        SET DEFAULT '{"sizing":{"mode":"proportional"},"multiplier":"1"}'::jsonb,
    ALTER COLUMN follow_authorization_json SET NOT NULL;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='venue_api_credentials'::regclass
          AND conname='venue_api_follow_authorization_shape'
    ) THEN
        ALTER TABLE venue_api_credentials
            ADD CONSTRAINT venue_api_follow_authorization_shape
            CHECK (jsonb_typeof(follow_authorization_json)='object');
    END IF;
END $$;
