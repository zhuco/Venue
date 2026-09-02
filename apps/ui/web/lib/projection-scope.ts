type Row = Record<string, unknown>;
const record = (value: unknown): value is Row => Boolean(value && typeof value === "object" && !Array.isArray(value));
const rows = (value: unknown): Row[] => Array.isArray(value) ? value.filter(record) : [];

export function relationInScope(value: unknown, scope: readonly string[]): boolean {
  if (!record(value) || !record(value.relation)) return false;
  return [value.relation.leader, value.relation.follower].every(binding =>
    record(binding) && binding.mode === "LIVE" && typeof binding.trading_account_id === "string"
      && scope.includes(binding.trading_account_id));
}

export function scopedSnapshot(body: Row, relations: unknown[], scope: readonly string[]): Row {
  const accountAllowed = (row: Row) => row.mode === "LIVE" && typeof row.trading_account_id === "string"
    && scope.includes(row.trading_account_id);
  const allStrategies = rows(body.strategies);
  const strategies = allStrategies.filter(accountAllowed);
  // Legacy ledger rows carry only instance_id. Ambiguous ownership must not leak across accounts.
  const instances = new Set(strategies.filter(strategy => allStrategies.filter(other =>
    other.instance_id === strategy.instance_id).length === 1).map(strategy => strategy.instance_id));
  const relationIds = new Set(relations.filter(value => relationInScope(value, scope))
    .map(value => ((value as Row).relation as Row).relation_id));
  return {
    schema_version: body.schema_version, generated_ms: body.generated_ms, connection: body.connection,
    accounts: rows(body.accounts).filter(accountAllowed), strategies,
    copy_relations: rows(body.copy_relations).filter(row => relationIds.has(row.relation_id)),
    ledger: rows(body.ledger).filter(row => instances.has(row.instance_id)),
    markets: rows(body.markets),
  };
}
