import { expect, test } from "@playwright/test";
import { mkdirSync } from "node:fs";
import type { Snapshot } from "../lib/types";
import { projectedFacts } from "./projected-facts";

const account = "00000000-0000-4000-8000-000000000001";
const session = {
  subject: "e2e-operator",
  role: "operator",
  account_scope: [account],
  csrf: "0123456789abcdef",
  expires_ms: 4_102_444_800_000,
  writable: true,
};
const binding = (instance_id: string) => ({
  venue: "Binance",
  mode: "LIVE" as const,
  trading_account_id: account,
  instance_id,
  symbol: "BTC/USDT",
});
const snapshot = (): Snapshot => ({
  schema_version: 2,
  generated_ms: Date.now(),
  connection: "LIVE",
  accounts: [
    {
      venue: "Binance",
      mode: "LIVE",
      trading_account_id: account,
      health: "healthy",
      equity: "10000.00",
      available_margin: "8000.00",
      unrealized_pnl: "0",
      private_generation: 1,
      writer_generation: 1,
      last_reconciled_ms: Date.now(),
    },
  ],
  strategies: [
    {
      ...binding("copy-btc"),
      kind: "copy",
      lifecycle: "running",
      config_epoch: 7,
      open_orders: 0,
      long_quantity: "0",
      short_quantity: "0",
      realized_pnl: "0",
      unrealized_pnl: "0",
      last_receipt_ms: Date.now(),
      attention: null,
    },
  ],
  copy_relations: [],
  markets: [],
  ledger: [],
});
const facts = () => ({
  schema_version: 2,
  generated_ms: Date.now(),
  orders: [],
  positions: [],
  fills: [],
  reconciliation: [],
  copy_ledger: [],
  drift: [],
  execution: [],
  risk: [],
  health: [],
});
const relation = {
  relation_id: "00000000-0000-4000-8000-000000000010",
  leader: binding("leader-btc"),
  follower: binding("copy-btc"),
  allocated_capital: "500.00",
  multiplier: "1.00",
  safety_reserve_rate: "0.10",
  risk: {
    max_total_notional: "1000.00",
    max_order_notional: "100.00",
    max_leverage: "3.00",
  },
  lifecycle: "paused",
};
let relationRequestIds: string[] = [];

test.beforeEach(async ({ page }) => {
  let authenticated = true;
  relationRequestIds = [];
  await page.addInitScript(
    ({ accountId }) => {
      const streams: TestEventSource[] = [];
      class TestEventSource {
        onerror?: () => void;
        private listener?: (event: MessageEvent<string>) => void;
        private closed = false;
        constructor() {
          streams.push(this);
        }
        emit(cursor: number, previous: number) {
          if (this.closed) return;
          this.listener?.(
            new MessageEvent("control", {
              data: JSON.stringify({
                schema_version: 2,
                cursor,
                previous_cursor: previous,
                event_type: "snapshot",
                scope: {
                  venue: "binance",
                  mode: "LIVE",
                  trading_account_id: accountId,
                },
              }),
              lastEventId: String(cursor),
            }),
          );
        }
        addEventListener(
          type: string,
          listener: (event: MessageEvent<string>) => void,
        ) {
          if (type === "control") {
            this.listener = listener;
            setTimeout(() => this.emit(1, 0), 0);
          }
        }
        close() {
          this.closed = true;
        }
      }
      Object.defineProperty(window, "EventSource", { value: TestEventSource });
      Object.defineProperty(window, "__venueTestStreams", { value: streams });
    },
    { accountId: account },
  );
  await page.route("**/api/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const method = route.request().method();
    const json = (body: unknown, status = 200) =>
      route.fulfill({
        status,
        contentType: "application/json",
        body: JSON.stringify(body),
      });
    if (path === "/api/session" && method === "GET")
      return authenticated ? json(session) : json({ writable: false }, 401);
    if (path === "/api/session" && method === "POST") {
      authenticated = true;
      return json(session);
    }
    if (path === "/api/session" && method === "DELETE") {
      authenticated = false;
      return json({ ok: true });
    }
    if (path === "/api/control/snapshot") return json(snapshot());
    if (path === "/api/control/execution-facts") return json(facts());
    if (path === "/api/control/relation-candidates")
      return json([
        {
          binding: binding("leader-btc"),
          lifecycle: "running",
          config_epoch: 7,
        },
        { binding: binding("copy-btc"), lifecycle: "running", config_epoch: 7 },
        { binding: binding("copy-alt"), lifecycle: "paused", config_epoch: 7 },
      ]);
    if (path === "/api/control/relations" && method === "GET")
      return json([{ relation, revision: 1 }]);
    if (path === "/api/control/relations") {
      const raw = route.request().postData() ?? "";
      if (!raw.includes('"forged"'))
        relationRequestIds.push(
          (JSON.parse(raw) as { request_id: string }).request_id,
        );
      return raw.includes('"forged"')
        ? json({ error: "candidate_rejected" }, 403)
        : json({
            schema_version: 2,
            relation_id: relation.relation_id,
            revision: 1,
            state: "existing",
            observed_ms: Date.now(),
          });
    }
    if (path === "/api/control/commands")
      return json({
        schema_version: 2,
        request_id: "018f3ae9-8a15-7d6c-b2a0-13b8d2d7b119",
        receipt_id: "receipt",
        state: "accepted",
        observed_ms: Date.now(),
        detail: "e2e",
      });
    return route.fulfill({ status: 404, body: "not found" });
  });
});

async function openControl(page: import("@playwright/test").Page) {
  if (
    test.info().project.name === "mobile" ||
    test.info().project.name === "tablet"
  )
    await page.getByRole("button", { name: "切换导航" }).click();
  await page.getByRole("button", { name: "控制" }).click();
  if (
    test.info().project.name === "mobile" ||
    test.info().project.name === "tablet"
  )
    await page.waitForTimeout(220);
  await expect(
    page.getByRole("heading", { name: "暂停、停止与平仓" }),
  ).toBeVisible();
}

async function openView(
  page: import("@playwright/test").Page,
  label: string,
  heading: string,
) {
  if (["mobile", "tablet"].includes(test.info().project.name)) {
    await page.getByRole("button", { name: "切换导航" }).click();
  }
  await page.getByRole("button", { name: label, exact: true }).click();
  await expect(
    page.getByRole("heading", { name: heading, exact: true }),
  ).toBeVisible();
  if (["mobile", "tablet"].includes(test.info().project.name)) {
    await expect(
      page.getByRole("button", { name: "切换导航" }),
    ).toHaveAttribute("aria-expanded", "false");
  }
}

async function captureView(
  page: import("@playwright/test").Page,
  name: string,
) {
  if (["mobile", "tablet"].includes(test.info().project.name))
    await page.waitForTimeout(220);
  const folder = process.env.VENUE_WEB_SCREENSHOT_DIR ?? "screenshots";
  mkdirSync(folder, { recursive: true });
  await page.screenshot({
    path: `${folder}/${test.info().project.name}-${name}.png`,
    fullPage: true,
  });
}

test("captures every operational view and the expanded relation editor without viewport overflow", async ({
  page,
}) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "控制面总览" })).toBeVisible();
  await captureView(page, "overview");
  for (const [label, heading, name] of [
    ["账户", "交易账户", "accounts"],
    ["订单与持仓", "订单、持仓与成交", "execution-empty"],
    ["回执与风险", "回执与风险", "receipts-empty"],
    ["交易", "基础手动交易", "trade"],
    ["跟单关系", "跟单关系", "relation-edit"],
  ]) {
    await openView(page, label, heading);
    if (name === "relation-edit") {
      await page.getByText("编辑 revision 1", { exact: true }).click();
      await expect(
        page.getByRole("button", { name: "保存 revision 编辑" }),
      ).toBeVisible();
      const editor = page.locator(".relation-editor .relation-form");
      await expect(editor.getByRole("textbox", { name: "分配资金", exact: true })).toHaveValue("500.00");
      await expect(editor.getByRole("textbox", { name: "保证金预留比例", exact: true }))
        .toHaveAccessibleDescription("填写小数，例如 0.10 表示预留 10%。");
      await expect(editor.getByRole("textbox", { name: "策略敞口倍率上限", exact: true }))
        .toHaveAccessibleDescription("只限制策略目标，不修改交易所账户杠杆。");
      await expect(editor.getByRole("option", { name: "暂停（不生成新增目标）", exact: true })).toHaveAttribute("value", "paused");
    }
    await expect
      .poll(() =>
        page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      )
      .toBe(true);
    await captureView(page, name);
  }
});

test("submits a Decimal-preserving TradeIntent only after explicit LIVE confirmation", async ({ page }) => {
  await page.goto("/");
  await openView(page, "交易", "基础手动交易");
  await page.getByLabel("限价").fill("100000.0100");
  await page.getByLabel("报价金额（USDT）").fill("5.00");
  const accepted = page.waitForResponse((response) => response.url().includes("/api/control/commands") && response.request().method() === "POST");
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "开多", exact: true }).click();
  const request = (await accepted).request();
  const body = JSON.parse(request.postData() ?? "{}") as { action?: string; trade?: Record<string, unknown> };
  expect(body.action).toBe("trade");
  expect(body.trade).toMatchObject({ action: "open_long", quote_asset: "USDT", order_type: "limit", time_in_force: "gtc", reduce_only: false, selected_price: "100000.0100", quote_notional: "5.00", selected_order_id: null });
});

test("SSE failure and cursor gaps close writes until a fresh reconnect", async ({
  page,
}) => {
  await page.goto("/");
  await openControl(page);
  await expect(page.getByRole("button", { name: "平仓" })).toBeEnabled();
  await page.evaluate(() => {
    const streams = (
      window as unknown as {
        __venueTestStreams: Array<{ onerror?: () => void }>;
      }
    ).__venueTestStreams;
    streams.at(-1)?.onerror?.();
  });
  await expect(page.getByRole("button", { name: "平仓" })).toBeDisabled();
  await captureView(page, "sse-reconnecting");
  await expect(page.getByRole("button", { name: "平仓" })).toBeEnabled();
  await page.evaluate(() => {
    const streams = (
      window as unknown as {
        __venueTestStreams: Array<{
          emit: (cursor: number, previous: number) => void;
        }>;
      }
    ).__venueTestStreams;
    streams.at(-1)?.emit(3, 2);
  });
  await expect(page.getByRole("button", { name: "平仓" })).toBeDisabled();
  await expect(page.getByRole("button", { name: "平仓" })).toBeEnabled();
});

test("renders signed facts and Unknown/Rejected/Reconciled without decimal loss or page overflow", async ({ page }) => {
  await page.route("**/api/control/execution-facts", route => route.fulfill({ json: projectedFacts(account) }));
  await page.route("**/api/control/snapshot", route => {
    const value = snapshot();
    value.accounts[0].balances = [
      { asset: "USDC", equity: "9007199254740993.01000000000000000001", available_margin: null },
    ];
    value.strategies[0].symbol = "SOL/USDC";
    value.strategies[0].realized_pnl = "1.25";
    value.strategies[0].unrealized_pnl = null;
    return route.fulfill({ json: value });
  });
  await page.goto("/");
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await captureView(page, "large-decimal-overview");
  await openView(page, "账户", "交易账户");
  await expect(page.getByText("USDC 9007199254740993.01000000000000000001", { exact: true })).toBeVisible();
  await expect(page.getByText("USDC —", { exact: true })).toBeVisible();
  await expect(page.getByText("已实现 USDC 1.25 · 未实现 — · 0 orders", { exact: true })).toBeVisible();
  await captureView(page, "large-decimal-accounts");
  await openView(page, "订单与持仓", "订单、持仓与成交");
  for (const state of ["semantic_applied", "rejected", "unknown", "reconciled"])
    await expect(page.locator('td[data-label="状态"]').filter({ hasText: new RegExp(`^${state}$`) })).toBeVisible();
  await expect(page.getByRole("heading", { name: "签名对账", exact: true })).toBeVisible();
  await expect(page.locator('td[data-label="数量"]').filter({ hasText: /^0\.00000000000000000001$/ }).first()).toBeVisible();
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await captureView(page, "signed-execution");
  await openView(page, "回执与风险", "回执与风险");
  await expect(page.getByRole("columnheader", { name: "预留风险" })).toBeAttached();
  await expect(page.locator('td[data-label="预留风险"]')).toHaveText("5.00");
  await expect(page.locator('td[data-label="允许增险"]')).toHaveText("禁止增险");
  await expect(page.locator('td[data-label="健康状态"]')).toHaveText("降级");
  await expect(page.getByRole("heading", { name: "跟单账本" })).toBeVisible();
  await expect(page.locator('td[data-label="账本序号"]')).toHaveText("—");
  await expect(page.getByRole("heading", { name: "跟单偏差" })).toBeVisible();
  await expect(page.locator('td[data-label="修复任务"]')).toHaveText("修复任务待处理");
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await captureView(page, "signed-risk-ledger");
});

test("a once-valid snapshot ages out without another network event", async ({
  page,
}) => {
  await page.clock.install({ time: new Date() });
  await page.goto("/");
  await openControl(page);
  await expect(page.getByRole("button", { name: "平仓" })).toBeEnabled();
  await page.clock.fastForward(121_001);
  await expect(page.getByText("恢复中 · 写入已关闭")).toBeVisible();
  await expect(page.getByRole("button", { name: "平仓" })).toBeDisabled();
  await captureView(page, "stale-control");
});

test("a failed snapshot remains an explicit recoverable error", async ({
  page,
}) => {
  let unavailable = true;
  await page.route("**/api/control/snapshot", (route) =>
    route.fulfill({
      status: unavailable ? 503 : 200,
      contentType: "application/json",
      body: JSON.stringify(unavailable ? { error: "unavailable" } : snapshot()),
    }),
  );
  await page.goto("/");
  await expect(page.getByRole("main").getByRole("alert")).toContainText(
    "请求失败 (503)",
  );
  await expect(page.getByText("恢复中 · 写入已关闭")).toBeVisible();
  await captureView(page, "snapshot-error");
  unavailable = false;
  await page.getByRole("button", { name: "重试连接" }).click();
  await expect(page.getByRole("heading", { name: "控制面总览" })).toBeVisible();
  await expect(page.getByText("已同步 · 可受控写入")).toBeVisible();
});

test("renders schema v2 snapshot and has a usable mobile navigation drawer", async ({
  page,
}) => {
  const document = await page.goto("/");
  expect(document?.headers()["content-security-policy"]).toContain(
    "connect-src 'self'",
  );
  expect(document?.headers()["x-frame-options"]).toBe("DENY");
  await expect(page.getByRole("heading", { name: "控制面总览" })).toBeVisible();
  await expect(page.getByText("fixture-operator")).toHaveCount(0);
  await openControl(page);
  await expect(page.getByRole("button", { name: "平仓" })).toBeEnabled();
  const folder = process.env.VENUE_WEB_SCREENSHOT_DIR ?? "screenshots";
  await page.screenshot({
    path: `${folder}/${test.info().project.name}-control.png`,
    fullPage: true,
  });
});

test("mobile drawer locks background scrolling, closes on Escape, and restores focus", async ({
  page,
}) => {
  test.skip(
    test.info().project.name !== "mobile",
    "Drawer behavior is mobile-only.",
  );
  await page.goto("/");
  const menu = page.getByRole("button", { name: "切换导航" });
  await menu.click();
  await expect
    .poll(() => page.locator("body").evaluate((body) => body.style.overflow))
    .toBe("hidden");
  await page.keyboard.press("Escape");
  await expect(menu).toHaveAttribute("aria-expanded", "false");
  await expect
    .poll(() => page.locator("body").evaluate((body) => body.style.overflow))
    .toBe("");
  await expect(menu).toBeFocused();
});

test("does not grant an unauthenticated browser context mutation authority and closes writes on logout or offline", async ({
  page,
  request,
}) => {
  const unreadable = await request.get("/api/control/snapshot");
  expect(unreadable.status()).toBe(401);
  const rejected = await request.post("/api/control/commands", {
    data: {
      request_id: "018f3ae9-8a15-7d6c-b2a0-13b8d2d7b119",
      venue: "Bybit",
      mode: "LIVE",
      trading_account_id: "00000000-0000-4000-8000-000000000001",
      instance_id: "copy-alpha",
      symbol: "DOGE/USDT",
      action: "pause",
      expected_config_epoch: 7,
    },
  });
  expect(rejected.status()).toBe(403);
  await page.goto("/");
  await openControl(page);
  await expect(page.getByRole("button", { name: "平仓" })).toBeEnabled();
  await page.context().setOffline(true);
  await expect(page.getByText("恢复中 · 写入已关闭")).toBeVisible();
  await expect(page.getByRole("button", { name: "平仓" })).toBeDisabled();
  await captureView(page, "offline-control");
  await page.context().setOffline(false);
  if (
    test.info().project.name === "mobile" ||
    test.info().project.name === "tablet"
  )
    await page.getByRole("button", { name: "切换导航" }).click();
  await page.getByRole("button", { name: "退出受控会话" }).click();
  await expect(page.getByText("只读 · 未获得受控会话")).toBeVisible();
  await expect(page.getByLabel("受控登录令牌")).toBeVisible();
  await captureView(page, "session-expired");
  await page.getByLabel("受控登录令牌").fill("fixture-only-token");
  await page.getByRole("button", { name: "建立受控会话" }).click();
  await expect(page.getByText("已同步 · 可受控写入")).toBeVisible();
});

test("requires an explicit LIVE confirmation and prevents duplicate flatten submission while a receipt is pending", async ({
  page,
}) => {
  await page.goto("/");
  await openControl(page);
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "平仓" }).click();
  await expect(page.getByText("恢复中 · 写入已关闭")).toBeVisible();
  await expect(page.getByRole("button", { name: "平仓" })).toBeDisabled();
});

test("orders and positions remain explicitly unavailable until projected", async ({
  page,
}) => {
  await page.goto("/");
  if (
    test.info().project.name === "mobile" ||
    test.info().project.name === "tablet"
  ) {
    await page.getByRole("button", { name: "切换导航" }).click();
  }
  await page.getByRole("button", { name: "订单与持仓" }).click();
  await expect(
    page.getByRole("heading", { name: "订单、持仓与成交" }),
  ).toBeVisible();
  await expect(page.getByRole("heading", { name: "签名订单" })).toBeVisible();
});

test("only accepts server-derived relation candidates and preserves decimal strings", async ({
  page,
}) => {
  await page.goto("/");
  if (
    test.info().project.name === "mobile" ||
    test.info().project.name === "tablet"
  )
    await page.getByRole("button", { name: "切换导航" }).click();
  await page.getByRole("button", { name: "跟单关系" }).click();
  await expect(page.getByRole("heading", { name: "创建关系" })).toBeVisible();
  if (
    test.info().project.name === "mobile" ||
    test.info().project.name === "tablet"
  )
    await page.waitForTimeout(220);
  await page.screenshot({
    path: `${process.env.VENUE_WEB_SCREENSHOT_DIR ?? "screenshots"}/${test.info().project.name}-relations.png`,
    fullPage: true,
  });
  await expect(page.getByLabel("Leader 策略")).toHaveValue("0");
  await page.getByLabel("Follower 策略").selectOption("2");
  await page.locator("input").first().fill("123.4500");
  const accepted = page.waitForResponse(
    (response) =>
      response.url().includes("/api/control/relations") &&
      response.request().method() === "POST",
  );
  const submit = page.getByRole("button", { name: "创建受控关系" });
  await Promise.all([submit.click(), submit.click()]);
  expect((await accepted).status()).toBe(200);
  expect(relationRequestIds).not.toHaveLength(0);
  expect(new Set(relationRequestIds).size).toBe(1);
  const forged = await page.evaluate(async () => {
    const session = (await fetch("/api/session").then((response) =>
      response.json(),
    )) as { csrf: string };
    return fetch("/api/control/relations", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-venue-csrf": session.csrf,
      },
      body: JSON.stringify({
        schema_version: 2,
        request_id: crypto.randomUUID(),
        relation: {
          relation_id: crypto.randomUUID(),
          leader: {
            venue: "Binance",
            mode: "LIVE",
            trading_account_id: "00000000-0000-4000-8000-000000000001",
            instance_id: "forged",
            symbol: "BTC/USDT",
          },
          follower: {
            venue: "Binance",
            mode: "LIVE",
            trading_account_id: "00000000-0000-4000-8000-000000000001",
            instance_id: "copy-alt",
            symbol: "BTC/USDT",
          },
          allocated_capital: "123.4500",
          multiplier: "1.00",
          safety_reserve_rate: "0.10",
          risk: {
            max_total_notional: "1000.00",
            max_order_notional: "100.00",
            max_leverage: "3.00",
          },
          lifecycle: "paused",
        },
      }),
    }).then((response) => response.status);
  });
  expect(forged).toBe(403);
});
