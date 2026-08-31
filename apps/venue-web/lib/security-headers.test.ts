import assert from "node:assert/strict";
import test from "node:test";
import { securityHeaders } from "../next.config";

test("same-origin BFF response headers fence framing, browser capabilities, and external connections", () => {
  const values = new Map(securityHeaders.map((header) => [header.key, header.value]));
  assert.match(values.get("Content-Security-Policy") ?? "", /connect-src 'self'/);
  assert.match(values.get("Content-Security-Policy") ?? "", /frame-ancestors 'none'/);
  assert.equal(values.get("X-Frame-Options"), "DENY");
  assert.equal(values.get("X-Content-Type-Options"), "nosniff");
  assert.ok(values.get("Strict-Transport-Security")?.includes("max-age="));
});
