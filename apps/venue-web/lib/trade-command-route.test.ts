import assert from "node:assert/strict";
import test from "node:test";
import { NextRequest } from "next/server";
import { POST } from "../app/api/control/commands/route";
import { bootstrapResponse } from "./server";

const account = "00000000-0000-4000-8000-000000000001";
const headers = (cookie: string, csrf: string) => ({ origin: "https://venue.test", host: "venue.test", cookie, "x-venue-csrf": csrf, "content-type": "application/json" });

test("TradeIntent BFF forwards only a valid existing protocol shape", async () => {
  const originalFetch = globalThis.fetch;
  const prior = Object.fromEntries(["VENUE_WEB_SESSION_SIGNING_KEY", "VENUE_WEB_SESSION_BOOTSTRAP_TOKEN", "VENUE_WEB_OPERATOR_SUBJECT", "VENUE_WEB_OPERATOR_ROLE", "VENUE_WEB_ACCOUNT_SCOPE"].map((key) => [key, process.env[key]]));
  try {
    process.env.VENUE_WEB_SESSION_SIGNING_KEY = "trade-route-signing-material";
    process.env.VENUE_WEB_SESSION_BOOTSTRAP_TOKEN = "trade-route-bootstrap";
    process.env.VENUE_WEB_OPERATOR_SUBJECT = "trade-route-operator";
    process.env.VENUE_WEB_OPERATOR_ROLE = "operator";
    process.env.VENUE_WEB_ACCOUNT_SCOPE = account;
    const issued = bootstrapResponse(new NextRequest("https://venue.test/api/session", { method: "POST", headers: { origin: "https://venue.test", host: "venue.test", "x-venue-bootstrap": "trade-route-bootstrap" } }));
    const session = await issued.json() as { csrf: string };
    const cookie = (issued.headers.get("set-cookie") ?? "").split(";")[0];
    let forwarded: unknown;
    globalThis.fetch = async (_url, init) => { forwarded = JSON.parse(String(init?.body)); return Response.json({ schema_version: 2, request_id: "018f3ae9-8a15-7d6c-b2a0-13b8d2d7b119", receipt_id: "fixture", state: "accepted", observed_ms: Date.now(), detail: "fixture" }); };
    const body = {
      request_id: "018f3ae9-8a15-7d6c-b2a0-13b8d2d7b119", venue: "Binance", mode: "LIVE", trading_account_id: account,
      instance_id: "manual-btc", symbol: "BTC/USDT", action: "trade", expected_config_epoch: 7,
      trade: { action: "close_long", quote_asset: "USDT", order_type: "limit", time_in_force: "gtc", post_only: false, reduce_only: true, selected_price: "100000.01", quote_notional: "5.00", close_quantity_cap: "0.00005", selected_order_id: null },
    };
    const accepted = await POST(new NextRequest("https://venue.test/api/control/commands", { method: "POST", headers: headers(cookie, session.csrf), body: JSON.stringify(body) }));
    assert.equal(accepted.status, 200);
    assert.deepEqual(forwarded, { schema_version: 2, ...body });
    const rejected = await POST(new NextRequest("https://venue.test/api/control/commands", { method: "POST", headers: headers(cookie, session.csrf), body: JSON.stringify({ ...body, trade: { ...body.trade, quote_asset: "BTC" } }) }));
    assert.equal(rejected.status, 400);
  } finally {
    globalThis.fetch = originalFetch;
    for (const [key, value] of Object.entries(prior)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
  }
});
