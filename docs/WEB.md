# Venue Web

Web 提供 Binance KOL 邀请页、注册登录、API 与托管账户、跟单设置和带单控制；`/ops` 保留独立运营会话。桌面交易、Grid 与支撑分批做多入口见 [UI 说明](../apps/ui/README.md)，五所独立策略范围见 [多交易所执行](MULTI_VENUE_EXECUTOR.md)。

本页维护 BFF 会话、运行配置、HTTPS 路由及浏览器验证。注册和凭证流程见 [账户管理](ACCOUNT_MANAGEMENT.md)，托管业务约束见 [KOL 契约](KOL_COPY_MVP.md)，机器人权限与生命周期见 [带单同步](LEADER_ORDER_MIRROR.md)。KOL 页面仅允许有界纯文本，固定平台风险说明不可编辑。

`lib/customer-server.ts` 将 Control 用户会话加密存入 Secure/HttpOnly/SameSite=Strict Cookie，浏览器只获得用户摘要和 CSRF；客户请求不使用运营环境令牌。写入校验 Origin/Host、JSON 与 CSRF。密钥仅在用户输入与受控绑定请求中短时传递，提交成功清空，不进入浏览器持久化或响应回显。构建产物不包含交易所网关或部署密钥。

使用与 CI 一致的 Node.js 24；版本和命令以 `apps/ui/web/package.json`、lockfile 为准。当前 BFF 使用 schema v2，不能由页面可见推断真实账户已启用。

## Build and run

Behind the HTTPS reverse proxy, set `VENUE_WEB_PUBLIC_ORIGIN=https://clawdbotweb.site` in the Web service environment. The BFF requires the exact configured HTTPS Origin and matching Host for writes; it does not trust caller-supplied forwarded headers or compare the public origin with Next.js's internal localhost URL. Without this setting, direct same-origin deployments retain URL-based validation.

Run `npm ci`, `npm run typecheck`, `npm test`, and `npm run build` from `apps/ui/web`. The build produces `.next/standalone`, including `.next/static` and optional `public` assets. `npm run start` runs the standalone server; `PORT` and `HOSTNAME` control its listener. Deploy the generated standalone directory as one versioned release, without environment files or QA artifacts. Production browser access requires HTTPS.

Next.js 16 does not run a linter during `next build`; this repository currently has no ESLint/Biome script. Typecheck and boundary scans must not be reported as a full lint pass. See the [official Next 16 changes](https://nextjs.org/blog/next-16); the supported project runtime is defined in [DEVELOPMENT](DEVELOPMENT.md).

After building, run `npm run verify:boundary`. It scans application source and browser JavaScript for legacy Control endpoints, non-LIVE gateway modes, exchange credential fields, direct exchange API URLs and URL credentials. It reports only filenames/rule names, never matching secret text. CI runs this guard, typecheck, unit tests, production build and all five browser viewports; the isolated browser fixtures are not deployed.

Both BFFs require `VENUE_CONTROL_ORIGIN` and a deployment `VENUE_WEB_SESSION_SIGNING_KEY` of at least 32 bytes; the customer cookie uses a separately derived encryption key. The remaining bootstrap/session/role settings below apply only to `/ops`. Set them through the deployment process environment, never in browser storage or Git:

- `VENUE_CONTROL_ORIGIN`: root loopback HTTP origin, default `http://127.0.0.1:39180`; Control must be on the BFF host.
- `VENUE_WEB_CONTROL_SESSION_TOKEN`: a valid Control user session obtained through its account login API. BFF attaches it only to fixed Control HTTP/SSE routes; never use the independent Node token here. Control still enforces account ownership, verification freshness and session expiry. This token is not sent to the browser, and missing/expired authorization never falls back to a privileged identity. Renew it after expiry through the controlled deployment environment.
- `VENUE_WEB_SESSION_SIGNING_KEY` and `VENUE_WEB_SESSION_BOOTSTRAP_TOKEN`: distinct deployment secrets.
- `VENUE_WEB_OPERATOR_SUBJECT`, `VENUE_WEB_OPERATOR_ROLE` (`viewer`, `operator`, or `admin`), and `VENUE_WEB_ACCOUNT_SCOPE` (comma-separated authorized internal account IDs).

The browser holds only a short-lived Secure/HttpOnly/SameSite session. BFF reads filter account scope; mutations also require CSRF, exact Origin/Host, role and binding validation. No exchange credential or direct Control connection reaches the browser. Interrupted mutation responses retain the original request ID and are not automatically retried. Missing/stale snapshots, invalid events, connection loss and session expiry close writes. A control receipt is never displayed as a fill.

## Desktop HTTPS access

Desktop access shares `https://clawdbotweb.site` but uses its own Control Bearer session, not BFF cookies. `scripts/configure_desktop_https.py` adds only the desktop client's exact HTTP methods/paths beneath the existing `venue-kol-web` Caddy route, including the support-martingale instance, preflight and lifecycle endpoints; all other traffic retains the Web fallback. It also exposes exactly three read-only Binance USD-M REST paths and two combined public WebSocket paths so installed clients do not require direct Binance reachability; authorization and cookie headers are removed before those requests reach Binance. The proxy disables response buffering for SSE/streams and sets `Cache-Control: no-store`. Node/internal routes are excluded; Control still binds only to loopback and performs authentication/ownership checks.

On the current server, install the script at `/home/cta/venue/desktop-https/configure_desktop_https.py` and `scripts/venue-desktop-https.conf` as `/home/cta/.config/systemd/user/venue-kol-caddy-route.service.d/desktop-https.conf`. This extends the existing enabled boot-time route restoration service. Reload that user unit's definitions and apply the script once; `--check` verifies without mutation. After a manual Caddy reload/restart, run the existing route restoration service again. The script uses Caddy's [ETag/If-Match contract](https://caddyserver.com/docs/api#concurrent-config-changes) and modifies only the verified Venue host route, leaving other sites intact.

## Browser verification

Install the browser once with `npx playwright install chromium`. Alternatively set `VENUE_WEB_BROWSER_EXECUTABLE` to an existing Chromium-compatible browser's absolute executable path. The test configuration appends loopback addresses to `NO_PROXY`/`no_proxy` for both readiness checks and isolated requests, preserving other exclusions.

Use `VENUE_WEB_QA_DIR=G:\Build\Venue\venue-web-qa\<run-id>`, then run `npm run test:e2e` after a production build. Screenshots default to `<qa-dir>/screenshots`; `VENUE_WEB_SCREENSHOT_DIR` can override this with another absolute build-artifact path. Without overrides, Windows uses `G:/Build/Venue/venue-web-qa/local-<pid>` and other hosts use their temporary directory. QA never defaults to the source or trading-recovery directory. The suite starts isolated listeners on 3216 and 38080; both must be free. It covers all five migration viewports, scoped session recovery, drawer focus, exact control confirmation, relation idempotency, empty/error/offline/stale states, signed-fact layout and decimal preservation.

`control.spec.ts` uses browser request interception with synthetic account IDs for deterministic UI failure/layout cases. `performance.spec.ts` exercises the real BFF against a separate isolated test Control HTTP service, without interception. Its timing report is local BFF evidence only, not proof of PostgreSQL, Executor, exchange latency or live trading. Product and capacity acceptance follow [KOL_COPY_MVP](KOL_COPY_MVP.md); QA fixture services are never part of the standalone production release.
