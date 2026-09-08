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

Desktop market routes additionally allow fixed public paths for Bybit, Bitget, Gate, OKX and Hyperliquid; `/exchange` and arbitrary upstreams remain excluded. Do not configure negative `flush_interval`: Caddy 2.6.2's `ignoreClientGoneContext` can panic on reload while a request remains connected. SSE is flushed automatically by its content type; WebSocket upgrades use their native stream path. When replacing an old negative-flush configuration, disconnect desktop SSE/WebSocket clients and verify their connections have drained before using `--drained-legacy-streams`. `scripts/test_caddy_stream_reload.py` tests loopback-only SSE delivery and five reloads on the installed Caddy; `--negative-flush` reproduces the old panic in that isolated process. It never addresses the production admin port.

Install the browser once with `npx playwright install chromium`. Alternatively set `VENUE_WEB_BROWSER_EXECUTABLE` to an existing Chromium-compatible browser's absolute executable path. The test configuration appends loopback addresses to `NO_PROXY`/`no_proxy` for both readiness checks and isolated requests, preserving other exclusions.

Use `VENUE_WEB_QA_DIR=G:\Build\Venue\venue-web-qa\<run-id>`, then run `npm run test:e2e` after a production build. Screenshots default to `<qa-dir>/screenshots`; `VENUE_WEB_SCREENSHOT_DIR` can override this with another absolute build-artifact path. Without overrides, Windows uses `G:/Build/Venue/venue-web-qa/local-<pid>` and other hosts use their temporary directory. QA never defaults to the source or trading-recovery directory. The suite starts isolated listeners on 3216 and 38080; both must be free. It covers all five migration viewports, scoped session recovery, drawer focus, exact control confirmation, relation idempotency, empty/error/offline/stale states, signed-fact layout and decimal preservation.

`control.spec.ts` uses browser request interception with synthetic account IDs for deterministic UI failure/layout cases. `performance.spec.ts` exercises the real BFF against a separate isolated test Control HTTP service, without interception. Its timing report is local BFF evidence only, not proof of PostgreSQL, Executor, exchange latency or live trading. Product and capacity acceptance follow [KOL_COPY_MVP](KOL_COPY_MVP.md); QA fixture services are never part of the standalone production release.

公开注册入口 `/register` 保留邀请码固定归属，普通注册用户只能添加本人跟单账户。每个账户在添加时选择定比或定额（每笔名义金额）；跟单资金默认账户验证时的全部权益，不设置总跟单金额或项目止损。跟单中参数只读，暂停并排空后修改。

KOL 与跟单页面明确要求币安统一账户（Portfolio Margin）、U 本位合约双向持仓及读取/交易权限、关闭提现。KOL 后台 `/v2/kol/source` 指定唯一带单账户，独立于登录会话的当前账户；需为本人验证通过的账户。管理员可预建无账户的 draft KOL，首次指定时以已验证权益初始化策略资金并占用全站最多 5 个名额之一；不自动创建或启动机器人，带单权限仍需独立授权。已有机器人或跟单关系时禁止更换源，保留历史身份。

KOL 后台可随机生成或输入 6–64 位 ASCII 字母数字及 `_`、`-` 邀请码，区分大小写，全平台（包括历史）唯一；`/v2/kol/invite` 使用数据库唯一约束、事务和请求摘要保障并发及重试。邀请码按现有密钥边界加密存储，注册归属保持不可变。KOL 隐藏误用跟随者范围的“我的同步订单”，普通跟单用户保留该模块。需要同步部署 Control 迁移 0046、0047 与 Web。

KOL 原生跟单的新指令不再使用软件单笔或总名义金额上限，也不再以初始权益乘 5 限制开仓。限价、市价与 STOP_MARKET 开仓均持久化 `copy_risk.notional_limit_policy=exchange_account`；币安账户保证金、持仓/订单规则、交易所步长/最小名义额/最大数量决定准入，原权限、同一身份对账、价格保护和只减仓约束保留。定比/定额决定计划数量，定比权益仍是验证快照，取消额度不代表动态重算跟单比例。历史无此字段的指令按 `stored_limits` 原规则恢复，已拒绝指令不自动补发。旧 wire 和数据库额度列仅为历史兼容，新模式不用于开仓额度；页面移除额度输入，API 字段显示统一为“API密钥”和“密钥”。

手工创建的 KOL 无需额外带单审批：验证币安 API 并指定唯一带单账户时自动授予初始带单权限；已有明确撤权不自动恢复。普通跟单注册不授予 KOL 身份。验证按钮必须展示验证结果，HTTP 200 不代表验证通过；空账户编号不得显示为已指定带单账户。

`/help/kol` 为公开可分享的 KOL 图文指南，采用 PR #2 的已检查导航图片并保留署名。按现有操作说明验证、唯一带单源、自动初始授权、邀请、定比/定额、实际状态与退出；不将 HTTP 成功等同于验证成功，不承诺无损或零费用。
