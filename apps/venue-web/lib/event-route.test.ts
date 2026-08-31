import assert from "node:assert/strict";
import test from "node:test";
import { NextRequest } from "next/server";
import { GET } from "../app/api/events/route";
import { sessionSignature } from "./server";

test("scoped SSE rejects invalid cursors and cancels its upstream with the browser", async () => {
  const priorKey = process.env.VENUE_WEB_SESSION_SIGNING_KEY;
  const priorToken = process.env.VENUE_WEB_CONTROL_SESSION_TOKEN;
  const originalFetch = globalThis.fetch;
  process.env.VENUE_WEB_SESSION_SIGNING_KEY = "event-test-material";
  process.env.VENUE_WEB_CONTROL_SESSION_TOKEN = "control-event-fixture-token";
  const payload = Buffer.from(JSON.stringify({ subject: "test", role: "operator", writable: true,
    account_scope: ["account-a"], csrf: "0123456789abcdef", expires_ms: Date.now() + 60_000 })).toString("base64url");
  const cookie = `venue_session=${payload}.${sessionSignature(payload, "event-test-material")}`;
  const url = "https://venue.test/api/events?venue=binance&trading_account_id=account-a";
  let upstreamSignal: AbortSignal | undefined;
  globalThis.fetch = async (target, init) => {
    assert.match(String(target), /mode=LIVE/);
    assert.match(String(target), /after=0/);
    assert.equal(new Headers(init?.headers).get("authorization"), "Bearer control-event-fixture-token");
    upstreamSignal = init?.signal ?? undefined;
    return new Response(new ReadableStream<Uint8Array>(), { headers: { "content-type": "text/event-stream" } });
  };
  try {
    const invalid = await GET(new NextRequest(url, { headers: { cookie, "last-event-id": "1e3" } }));
    assert.equal(invalid.status, 400);
    const response = await GET(new NextRequest(url, { headers: { cookie } }));
    assert.equal(response.status, 200);
    assert.equal(upstreamSignal?.aborted, false);
    await response.body?.cancel();
    assert.equal(upstreamSignal?.aborted, true);
    globalThis.fetch = async () => { throw new Error("unavailable"); };
    assert.equal((await GET(new NextRequest(url, { headers: { cookie } }))).status, 503);
  } finally {
    globalThis.fetch = originalFetch;
    if (priorKey === undefined) delete process.env.VENUE_WEB_SESSION_SIGNING_KEY;
    else process.env.VENUE_WEB_SESSION_SIGNING_KEY = priorKey;
    if (priorToken === undefined) delete process.env.VENUE_WEB_CONTROL_SESSION_TOKEN;
    else process.env.VENUE_WEB_CONTROL_SESSION_TOKEN = priorToken;
  }
});
