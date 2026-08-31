# Venue Web

Responsive Control client using schema v2 only. Requires Node.js >=22.18; package versions are pinned in `package-lock.json`.

## Build and run

Run `npm ci`, `npm run typecheck`, `npm test`, and `npm run build` from this directory. The build produces `.next/standalone`, including `.next/static` and optional `public` assets. `npm run start` runs the standalone server; `PORT` and `HOSTNAME` control its listener. Deploy the generated standalone directory as one versioned release, without environment files or QA artifacts. Production browser access requires HTTPS.

After building, run `npm run verify:boundary`. It scans application source and browser JavaScript for legacy Control endpoints, non-LIVE gateway modes, exchange credential fields, direct exchange API URLs and URL credentials. It reports only filenames/rule names, never matching secret text. CI runs this guard, typecheck, unit tests, production build and all five browser viewports; the isolated browser fixtures are not deployed.

Set these values through the deployment's process environment, never in browser storage or Git:

- `VENUE_CONTROL_ORIGIN`: root loopback HTTP origin, default `http://127.0.0.1:8080`; Control must be on the BFF host.
- `VENUE_WEB_SESSION_SIGNING_KEY` and `VENUE_WEB_SESSION_BOOTSTRAP_TOKEN`: distinct deployment secrets.
- `VENUE_WEB_OPERATOR_SUBJECT`, `VENUE_WEB_OPERATOR_ROLE` (`viewer`, `operator`, or `admin`), and `VENUE_WEB_ACCOUNT_SCOPE` (comma-separated authorized internal account IDs).

The browser holds only a short-lived Secure/HttpOnly/SameSite session. BFF reads filter account scope; mutations also require CSRF, exact Origin/Host, role and binding validation. No exchange credential or direct Control connection reaches the browser. Interrupted mutation responses retain the original request ID and are not automatically retried. Missing/stale snapshots, invalid events, connection loss and session expiry close writes. A control receipt is never displayed as a fill.

## Browser verification

Install the browser once with `npx playwright install chromium`. Alternatively set `VENUE_WEB_BROWSER_EXECUTABLE` to an existing Chromium-compatible browser's absolute executable path. The test configuration appends loopback addresses to `NO_PROXY`/`no_proxy` for both readiness checks and isolated requests, preserving other exclusions.

Use `VENUE_WEB_QA_DIR=G:\Build\Venue\venue-web-qa\<run-id>`, then run `npm run test:e2e` after a production build. Screenshots default to `<qa-dir>/screenshots`; `VENUE_WEB_SCREENSHOT_DIR` can override this with another absolute build-artifact path. Without overrides, Windows uses `G:/Build/Venue/venue-web-qa/local-<pid>` and other hosts use their temporary directory. QA never defaults to the source or trading-recovery directory. The suite starts isolated listeners on 3216 and 38080; both must be free. It covers all five migration viewports, scoped session recovery, drawer focus, exact control confirmation, relation idempotency, empty/error/offline/stale states, signed-fact layout and decimal preservation.

`control.spec.ts` uses browser request interception with synthetic account IDs for deterministic UI failure/layout cases. `performance.spec.ts` exercises the real BFF against a separate isolated test Control HTTP service, without interception. Its timing report is local BFF evidence only, not proof of Node, PostgreSQL, exchange latency or live trading. Those phases require the deployed end-to-end acceptance specified in `UNIFIED_GATEWAY_WEB_MIGRATION.md`. QA fixture services are never part of the standalone production release.
