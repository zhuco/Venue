-- Legacy hashes remain valid; only newly issued codes can be recovered by their owner.
ALTER TABLE venue_kol_invites ADD COLUMN IF NOT EXISTS code_envelope BYTEA;
ALTER TABLE venue_kol_invites ADD COLUMN IF NOT EXISTS replaced_invite_id TEXT;
ALTER TABLE venue_kol_invites ADD COLUMN IF NOT EXISTS request_hash BYTEA;
