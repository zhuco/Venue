import assert from "node:assert/strict";
import test from "node:test";
import { NextRequest } from "next/server";
import { allowedOrigin } from "./server";

test("proxy origin is pinned to deployment configuration and ignores forged forwarding", () => {
  const prior = process.env.VENUE_WEB_PUBLIC_ORIGIN;
  const request = (origin: string, host = "venue.test") => new NextRequest("http://localhost:39200/api/kol/auth/login", {
    method: "POST", headers: { origin, host, "x-forwarded-host": "venue.test", "x-forwarded-proto": "https" },
  });
  try {
    delete process.env.VENUE_WEB_PUBLIC_ORIGIN;
    assert.equal(allowedOrigin(request("https://venue.test")), false);
    process.env.VENUE_WEB_PUBLIC_ORIGIN = "https://venue.test";
    assert.equal(allowedOrigin(request("https://venue.test")), true);
    for (const origin of ["https://evil.test", "http://venue.test", "https://venue.test:444", "https://venue.test/path", "https://user@venue.test", "null"]) {
      assert.equal(allowedOrigin(request(origin)), false, origin);
    }
    assert.equal(allowedOrigin(request("https://venue.test", "evil.test")), false);
    assert.equal(allowedOrigin(request("https://evil.test", "evil.test")), false);
    for (const configured of ["", "http://venue.test", "https://user@venue.test", "https://venue.test/path", "https://venue.test/", "https://venue.test?x=1"]) {
      process.env.VENUE_WEB_PUBLIC_ORIGIN = configured;
      assert.equal(allowedOrigin(request("https://venue.test")), false, configured);
    }
    delete process.env.VENUE_WEB_PUBLIC_ORIGIN;
    assert.equal(allowedOrigin(new NextRequest("https://venue.test/api/login", { headers: { origin: "https://venue.test", host: "venue.test" } })), true);
    assert.equal(allowedOrigin(new NextRequest("https://venue.test/api/login")), false);
  } finally {
    if (prior === undefined) delete process.env.VENUE_WEB_PUBLIC_ORIGIN;
    else process.env.VENUE_WEB_PUBLIC_ORIGIN = prior;
  }
});
