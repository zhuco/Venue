import type { WriteState } from "./types";

type EventPayload = {
  schema_version: 2;
  cursor: number;
  previous_cursor: number;
  event_type:
    | "snapshot"
    | "copy_relation"
    | "execution_facts"
    | "command"
    | "delivery";
  scope: { venue: string; mode: "LIVE"; trading_account_id: string };
};

export function safeControlEvent(raw: string): EventPayload | undefined {
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    const cursor = value.cursor;
    const previous = value.previous_cursor;
    const scope = value.scope as Record<string, unknown> | undefined;
    if (
      value.schema_version === 2 &&
      typeof cursor === "number" &&
      Number.isSafeInteger(cursor) &&
      cursor > 0 &&
      typeof previous === "number" &&
      Number.isSafeInteger(previous) &&
      previous >= 0 &&
      previous < cursor &&
      [
        "snapshot",
        "copy_relation",
        "execution_facts",
        "command",
        "delivery",
      ].includes(String(value.event_type)) &&
      scope &&
      typeof scope === "object" &&
      scope.mode === "LIVE" &&
      ["binance", "bitget", "bybit", "gate", "hyperliquid", "okx"].includes(
        String(scope.venue),
      ) &&
      typeof scope.trading_account_id === "string" &&
      scope.trading_account_id.trim().length > 0
    )
      return value as EventPayload;
  } catch {
    /* Invalid frames close the write gate. */
  }
  return undefined;
}

export function freshProjection(
  value: { schema_version: number; generated_ms: number } | undefined,
  now = Date.now(),
): boolean {
  return Boolean(
    value &&
      value.schema_version === 2 &&
      Number.isSafeInteger(value.generated_ms) &&
      value.generated_ms > 0 &&
      value.generated_ms <= now + 5_000 &&
      now - value.generated_ms <= 120_000,
  );
}

export function nextWriteState(
  current: WriteState,
  online: boolean,
  event: EventPayload | undefined,
): WriteState {
  if (!online || !event) return "recovering";
  return event.event_type === "snapshot" || current === "ready"
    ? "ready"
    : "recovering";
}
