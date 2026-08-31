import assert from "node:assert/strict";
import test from "node:test";
import { freshProjection, nextWriteState, safeControlEvent } from "./realtime";

const event =
  '{"schema_version":2,"cursor":1,"previous_cursor":0,"event_type":"snapshot","scope":{"venue":"binance","mode":"LIVE","trading_account_id":"account-a"}}';

test("schema v2 scoped control events reject unknown event kinds", () => {
  assert.equal(safeControlEvent(event)?.event_type, "snapshot");
  assert.equal(
    safeControlEvent(
      '{"schema_version":2,"cursor":1,"previous_cursor":0,"event_type":"unknown","scope":{}}',
    ),
    undefined,
  );
  assert.equal(safeControlEvent("not-json"), undefined);
});

test("connection loss and invalid payload immediately close the write gate", () => {
  assert.equal(nextWriteState("ready", false, undefined), "recovering");
  assert.equal(nextWriteState("ready", true, undefined), "recovering");
  const accepted = safeControlEvent(event);
  assert.ok(accepted);
  assert.equal(nextWriteState("recovering", true, accepted), "ready");
});

test("event scope and cursor must remain exact LIVE and monotonic", () => {
  const valid = JSON.parse(event);
  for (const scope of [
    { ...valid.scope, mode: "invalid" },
    { ...valid.scope, venue: "other" },
    { ...valid.scope, trading_account_id: "" },
  ]) {
    assert.equal(
      safeControlEvent(JSON.stringify({ ...valid, scope })),
      undefined,
    );
  }
  assert.equal(
    safeControlEvent(JSON.stringify({ ...valid, previous_cursor: 1 })),
    undefined,
  );
});

test("projection freshness rejects stale, future, malformed and missing timestamps", () => {
  const now = 1_000_000;
  assert.equal(
    freshProjection({ schema_version: 2, generated_ms: now - 120_000 }, now),
    true,
  );
  assert.equal(
    freshProjection({ schema_version: 2, generated_ms: now - 120_001 }, now),
    false,
  );
  assert.equal(
    freshProjection({ schema_version: 2, generated_ms: now + 5_001 }, now),
    false,
  );
  assert.equal(
    freshProjection({ schema_version: 1, generated_ms: now }, now),
    false,
  );
  assert.equal(
    freshProjection({ schema_version: 2, generated_ms: Number.NaN }, now),
    false,
  );
  assert.equal(freshProjection(undefined, now), false);
});
