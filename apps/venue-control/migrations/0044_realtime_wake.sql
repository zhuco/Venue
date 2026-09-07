-- Notifications are commit-time hints only. Durable rows and account fences remain authoritative.
CREATE FUNCTION venue_realtime_wake() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(TG_ARGV[0], '');
    RETURN NULL;
END
$$;
CREATE TRIGGER venue_command_insert_wake AFTER INSERT ON venue_binance_commands
    FOR EACH STATEMENT EXECUTE FUNCTION venue_realtime_wake('venue_executor_commands');
CREATE TRIGGER venue_projection_update_wake AFTER INSERT OR UPDATE ON venue_binance_account_projections
    FOR EACH STATEMENT EXECUTE FUNCTION venue_realtime_wake('venue_terminal_projection');
