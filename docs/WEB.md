# Venue Web

KOL 登录 `/` 或 `/login` 后可在“托管跟单账户”打开“添加托管 API Key”对话框；保存成功清空密钥输入并列出掩码，提供手动权限验证。批量最多 10 个，每行稳定请求编号用于结果不确定后的原内容重试，浏览器不将密钥保存至本地存储。BFF `managed-followers` / `managed-verify` 继续强制同源、加密 Cookie、CSRF 与响应字段白名单。该入口不启用跟单。

Current product target: the Binance KOL copy-trading MVP defined in [KOL_COPY_MVP](KOL_COPY_MVP.md). Web must provide the KOL invite landing page, real user registration/login, Binance API binding and verification, copy settings/status, a basic KOL terminal, and KOL-owned page-copy editing. Grid and every non-Binance venue are deferred. The browser never contains an exchange gateway or decrypted API credential.

Responsive Control client using schema v2 only. Product version and known limits: [root README](../README.md), [release notes](CHANGELOG.md). Development workflow: [DEVELOPMENT](DEVELOPMENT.md).

Use Node.js 24, matching CI. The package minimum is >=22.18; that lower bound does not certify every newer major. Next.js 16.3.3, React/React DOM 19.2.8 and TypeScript 7.0.2 are pinned in the package/lockfile. This is the user-facing DOM application, not the internal VenueFlow WASM canvas client.

The customer console at `/` provides login, owned API binding/verification, follow settings, mirrored orders and the permission-controlled leader bot. `/join/<invite_code>` provides invite registration and profile text. `lib/customer-server.ts` encrypts the actual Control user session in a Secure/HttpOnly/SameSite=Strict cookie; authenticated writes require same-origin JSON and CSRF. Only its server-side credential serializer is exempt from the source credential-field scan; browser chunks remain fully checked. See [leader bot contract](LEADER_ORDER_MIRROR.md).

The earlier operator console is retained at `/ops` with its separate environment-injected session. Customer requests never inherit this identity. KOL page editing and the full browser trading terminal remain separate product acceptance items; this change provides the leader bot controls and preserves the desktop terminal.

Target user routes are `/join/<invite_code>`, registration/login/logout, API management, copy settings/status, and the KOL public page. Target KOL routes add page preview/edit and a basic terminal scoped only to the KOL's own verified account. KOL pages may contain bounded plain text but no arbitrary HTML or script; fixed platform risk text is not editable.

## Build and run

Behind the HTTPS reverse proxy, set `VENUE_WEB_PUBLIC_ORIGIN=https://clawdbotweb.site` in the Web service environment. The BFF requires the exact configured HTTPS Origin and matching Host for writes; it does not trust caller-supplied forwarded headers or compare the public origin with Next.js's internal localhost URL. Without this setting, direct same-origin deployments retain URL-based validation.

Run `npm ci`, `npm run typecheck`, `npm test`, and `npm run build` from `apps/ui/web`. The build produces `.next/standalone`, including `.next/static` and optional `public` assets. `npm run start` runs the standalone server; `PORT` and `HOSTNAME` control its listener. Deploy the generated standalone directory as one versioned release, without environment files or QA artifacts. Production browser access requires HTTPS.

Next.js 16 does not run a linter during `next build`; this repository currently has no ESLint/Biome script. Typecheck and boundary scans must not be reported as a full lint pass. See the [official Next 16 changes](https://nextjs.org/blog/next-16); runtime support references are in [ARCHITECTURE](ARCHITECTURE.md).

After building, run `npm run verify:boundary`. It scans application source and browser JavaScript for legacy Control endpoints, non-LIVE gateway modes, exchange credential fields, direct exchange API URLs and URL credentials. It reports only filenames/rule names, never matching secret text. CI runs this guard, typecheck, unit tests, production build and all five browser viewports; the isolated browser fixtures are not deployed.

Both BFFs require `VENUE_CONTROL_ORIGIN` and a deployment `VENUE_WEB_SESSION_SIGNING_KEY` of at least 32 bytes; the customer cookie uses a separately derived encryption key. The remaining bootstrap/session/role settings below apply only to `/ops`. Set them through the deployment process environment, never in browser storage or Git:

- `VENUE_CONTROL_ORIGIN`: root loopback HTTP origin, default `http://127.0.0.1:39180`; Control must be on the BFF host.
- `VENUE_WEB_CONTROL_SESSION_TOKEN`: a valid Control user session obtained through its account login API. BFF attaches it only to fixed Control HTTP/SSE routes; never use the independent Node token here. Control still enforces account ownership, verification freshness and session expiry. This token is not sent to the browser, and missing/expired authorization never falls back to a privileged identity. Renew it after expiry through the controlled deployment environment.
- `VENUE_WEB_SESSION_SIGNING_KEY` and `VENUE_WEB_SESSION_BOOTSTRAP_TOKEN`: distinct deployment secrets.
- `VENUE_WEB_OPERATOR_SUBJECT`, `VENUE_WEB_OPERATOR_ROLE` (`viewer`, `operator`, or `admin`), and `VENUE_WEB_ACCOUNT_SCOPE` (comma-separated authorized internal account IDs).

The browser holds only a short-lived Secure/HttpOnly/SameSite session. BFF reads filter account scope; mutations also require CSRF, exact Origin/Host, role and binding validation. No exchange credential or direct Control connection reaches the browser. Interrupted mutation responses retain the original request ID and are not automatically retried. Missing/stale snapshots, invalid events, connection loss and session expiry close writes. A control receipt is never displayed as a fill.

## Desktop HTTPS access

Desktop access shares `https://clawdbotweb.site` but uses its own Control Bearer session, not BFF cookies. `scripts/configure_desktop_https.py` adds only the desktop client's exact HTTP methods/paths beneath the existing `venue-kol-web` Caddy route; all other traffic retains the Web fallback. It also exposes exactly three read-only Binance USD-M REST paths and two combined public WebSocket paths so installed clients do not require direct Binance reachability; authorization and cookie headers are removed before those requests reach Binance. The proxy disables response buffering for SSE/streams and sets `Cache-Control: no-store`. Node/internal routes are excluded; Control still binds only to loopback and performs authentication/ownership checks.

On the current server, install the script at `/home/cta/venue/desktop-https/configure_desktop_https.py` and `scripts/venue-desktop-https.conf` as `/home/cta/.config/systemd/user/venue-kol-caddy-route.service.d/desktop-https.conf`. This extends the existing enabled boot-time route restoration service. Reload that user unit's definitions and apply the script once; `--check` verifies without mutation. After a manual Caddy reload/restart, run the existing route restoration service again. The script uses Caddy's [ETag/If-Match contract](https://caddyserver.com/docs/api#concurrent-config-changes) and modifies only the verified Venue host route, leaving other sites intact.

## Browser verification

Install the browser once with `npx playwright install chromium`. Alternatively set `VENUE_WEB_BROWSER_EXECUTABLE` to an existing Chromium-compatible browser's absolute executable path. The test configuration appends loopback addresses to `NO_PROXY`/`no_proxy` for both readiness checks and isolated requests, preserving other exclusions.

Use `VENUE_WEB_QA_DIR=G:\Build\Venue\venue-web-qa\<run-id>`, then run `npm run test:e2e` after a production build. Screenshots default to `<qa-dir>/screenshots`; `VENUE_WEB_SCREENSHOT_DIR` can override this with another absolute build-artifact path. Without overrides, Windows uses `G:/Build/Venue/venue-web-qa/local-<pid>` and other hosts use their temporary directory. QA never defaults to the source or trading-recovery directory. The suite starts isolated listeners on 3216 and 38080; both must be free. It covers all five migration viewports, scoped session recovery, drawer focus, exact control confirmation, relation idempotency, empty/error/offline/stale states, signed-fact layout and decimal preservation.

`control.spec.ts` uses browser request interception with synthetic account IDs for deterministic UI failure/layout cases. `performance.spec.ts` exercises the real BFF against a separate isolated test Control HTTP service, without interception. Its timing report is local BFF evidence only, not proof of PostgreSQL, Executor, exchange latency or live trading. Product and capacity acceptance follow [KOL_COPY_MVP](KOL_COPY_MVP.md); QA fixture services are never part of the standalone production release.
