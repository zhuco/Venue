import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import test from "node:test";
import { NextRequest } from "next/server";
import { bootstrapResponse, control, controlOrigin, isValidSession, logoutResponse, sessionSignature } from "./server";

test("session signatures are HMACs and not concatenated hashes", () => {
  const payload = "payload"; const material = "unit-material";
  assert.equal(sessionSignature(payload, material), createHmac("sha256", material).update(payload).digest("base64url"));
});

test("decoded sessions fail closed on type, expiry and role mismatches", () => {
  const valid = { subject: "operator", role: "operator", csrf: "0123456789abcdef", expires_ms: Date.now() + 60_000, account_scope: ["account-a"], writable: true };
  assert.equal(isValidSession(valid), true);
  assert.equal(isValidSession({ ...valid, writable: false }), false);
  assert.equal(isValidSession({ ...valid, role: "constructor" }), false);
  assert.equal(isValidSession({ ...valid, role: "__proto__" }), false);
  assert.equal(isValidSession({ ...valid, expires_ms: 0 }), false);
  assert.equal(isValidSession({ ...valid, account_scope: [] }), false);
});

test("controlled bootstrap rejects bad credentials and only sets a secure session after origin-bound authorization", () => {
  const prior = { token: process.env.VENUE_WEB_SESSION_BOOTSTRAP_TOKEN, key: process.env.VENUE_WEB_SESSION_SIGNING_KEY, role: process.env.VENUE_WEB_OPERATOR_ROLE, subject: process.env.VENUE_WEB_OPERATOR_SUBJECT, scope: process.env.VENUE_WEB_ACCOUNT_SCOPE };
  process.env.VENUE_WEB_SESSION_BOOTSTRAP_TOKEN = "bootstrap-secret"; process.env.VENUE_WEB_SESSION_SIGNING_KEY = "signing-material"; process.env.VENUE_WEB_OPERATOR_ROLE = "operator"; process.env.VENUE_WEB_OPERATOR_SUBJECT = "controlled-operator"; process.env.VENUE_WEB_ACCOUNT_SCOPE = "account-a";
  try {
    const headers = { origin: "https://venue.test", host: "venue.test" };
    assert.equal(bootstrapResponse(new NextRequest("https://venue.test/api/session", { method: "POST", headers: { ...headers, "x-venue-bootstrap": "wrong" } })).status, 403);
    assert.equal(bootstrapResponse(new NextRequest("https://venue.test/api/session", { method: "POST", headers: { ...headers, "x-venue-bootstrap": "bootstrap-secret" } })).status, 200);
    const issued = bootstrapResponse(new NextRequest("https://venue.test/api/session", { method: "POST", headers: { ...headers, "x-venue-bootstrap": "bootstrap-secret" } }));
    const cookie = issued.headers.get("set-cookie") ?? "";
    assert.match(cookie, /HttpOnly/i); assert.match(cookie, /Path=\//i); assert.match(cookie, /Secure/i); assert.match(cookie, /SameSite=Strict/i);
    assert.match(logoutResponse().headers.get("set-cookie") ?? "", /Max-Age=0/i);
  } finally {
    for (const [key, value] of Object.entries(prior)) { const name = key === "token" ? "VENUE_WEB_SESSION_BOOTSTRAP_TOKEN" : key === "key" ? "VENUE_WEB_SESSION_SIGNING_KEY" : key === "role" ? "VENUE_WEB_OPERATOR_ROLE" : key === "subject" ? "VENUE_WEB_OPERATOR_SUBJECT" : "VENUE_WEB_ACCOUNT_SCOPE"; if (value === undefined) delete process.env[name]; else process.env[name] = value; }
  }
});

test("Control origin accepts only a root loopback HTTP origin", () => {
  const prior = process.env.VENUE_CONTROL_ORIGIN;
  try {
    for (const value of ["https://control.example", "http://control.example", "http://user@127.0.0.1:8080", "http://127.0.0.1:8080/control", "http://127.0.0.1:8080/?x=1", "http://[::1]:8080/#fragment"]) {
      process.env.VENUE_CONTROL_ORIGIN = value;
      assert.equal(controlOrigin(), undefined, value);
    }
    process.env.VENUE_CONTROL_ORIGIN = "http://[::1]:8080";
    assert.equal(controlOrigin(), "http://[::1]:8080");
    process.env.VENUE_CONTROL_ORIGIN = "http://localhost:8080";
    assert.equal(controlOrigin(), "http://localhost:8080");
  } finally { if (prior === undefined) delete process.env.VENUE_CONTROL_ORIGIN; else process.env.VENUE_CONTROL_ORIGIN = prior; }
});

test("Control transport failures are bounded and never automatically retry a mutation", async () => {
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async (_url, init) => {
    calls += 1;
    assert.ok(init?.signal);
    assert.equal(init?.redirect, "error");
    throw new Error("synthetic network failure");
  };
  try {
    const response = await control("/v2/control/commands", { method: "POST", body: "{}" });
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), { error: "control_unavailable" });
    assert.equal(calls, 1);
  } finally { globalThis.fetch = original; }
});

test("Control route and method allow-list is enforced at runtime before network I/O", async () => {
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => { calls += 1; return Response.json({ ok: true }); };
  try {
    for (const path of ["/v2/account-node/projection", "/v2/admin/reset", "//example.test", "/v2/ui/snapshot?token=x", "__proto__"]) {
      const response = await control(path as Parameters<typeof control>[0]);
      assert.equal(response.status, 400);
    }
    assert.equal((await control("/v2/ui/snapshot", { method: "POST" })).status, 400);
    assert.equal((await control("/v2/control/commands")).status, 400);
    assert.equal((await control("/v2/copy/relations", { method: "DELETE" })).status, 400);
    assert.equal(calls, 0);
    assert.equal((await control("/v2/ui/snapshot")).status, 200);
    assert.equal((await control("/v2/control/commands", { method: "POST", body: "{}" })).status, 200);
    assert.equal(calls, 2);
  } finally { globalThis.fetch = original; }
});
