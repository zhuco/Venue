import assert from "node:assert/strict";
import test from "node:test";
import { boundaryViolations } from "../scripts/verify-boundary.mjs";

test("production source and browser guard reject legacy routes, modes and exchange secrets", () => {
  for (const source of [
    'fetch("/v1/control/commands")',
    'mode: "TESTNET"',
    'mode: "DEMO"',
    'localStorage.setItem("apiSecret", value)',
    'process.env.OKX_API_PASSPHRASE',
    'fetch("https://api.bybit.com/v5/order/create")',
    'fetch("https://papi.binance.com/papi/v1/order")',
    'fetch("https://www.okx.com/api/v5/trade/order")',
    'url = "/api/session?token=" + value',
  ]) {
    assert.notEqual(boundaryViolations(source).length, 0);
  }
  assert.deepEqual(boundaryViolations('fetch("/api/snapshot"); const mode = "LIVE";'), []);
  assert.deepEqual(boundaryViolations('control("/v2/ui/snapshot"); process.env.VENUE_WEB_SESSION_SIGNING_KEY;'), []);
});
