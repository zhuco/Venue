import { expect, test } from "@playwright/test";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

const account = "00000000-0000-4000-8000-000000000001";
const quantiles = (values: number[]) => { const sorted = [...values].sort((a, b) => a - b); const at = (p: number) => sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * p) - 1)]; return { samples: sorted.length, p50_ms: at(.5), p95_ms: at(.95), p99_ms: at(.99) }; };

test("real BFF to isolated test Control records segmented browser and SSE samples", async ({ page }) => {
  test.setTimeout(90_000);
  await page.goto("/");
  await page.getByLabel("受控登录令牌").fill("e2e-bootstrap-token");
  await page.getByRole("button", { name: "建立受控会话" }).click();
  await expect(page.getByText("已同步 · 可受控写入")).toBeVisible();
  const navigation: number[] = [];
  for (let index = 0; index < 10; index += 1) {
    await page.goto("/", { waitUntil: "domcontentloaded" });
    await expect(page.getByRole("heading", { name: "控制面总览" })).toBeVisible();
    navigation.push(await page.evaluate(() => performance.now()));
  }
  const samples = await page.evaluate(async (accountId) => {
    const snapshot: number[] = []; const bffControl: number[] = []; const sse: number[] = []; const command: number[] = [];
    const session = await fetch("/api/session").then((response) => response.json()) as { csrf: string };
    for (let index = 0; index < 25; index += 1) { const start = performance.now(); const response = await fetch("/api/control/snapshot", { cache: "no-store" }); if (!response.ok) throw new Error(`snapshot:${response.status}`); const timing = response.headers.get("server-timing")?.match(/bff-control;dur=([0-9.]+)/); if (!timing) throw new Error("missing bff-control timing"); bffControl.push(Number(timing[1])); await response.arrayBuffer(); snapshot.push(performance.now() - start); }
    for (let index = 0; index < 25; index += 1) { const start = performance.now(); await new Promise<void>((resolve, reject) => { const stream = new EventSource(`/api/events?venue=binance&trading_account_id=${accountId}`); stream.addEventListener("control", () => { stream.close(); resolve(); }); stream.onerror = () => { stream.close(); reject(new Error("sse")); }; }); sse.push(performance.now() - start); }
    for (let index = 0; index < 25; index += 1) { const start = performance.now(); const response = await fetch("/api/control/commands", { method: "POST", headers: { "content-type": "application/json", "x-venue-csrf": session.csrf }, body: JSON.stringify({ request_id: crypto.randomUUID(), venue: "Binance", mode: "LIVE", trading_account_id: accountId, instance_id: "copy-btc", symbol: "BTC/USDT", action: "pause", expected_config_epoch: 7 }) }); if (!response.ok) throw new Error(`command:${response.status}`); await response.arrayBuffer(); command.push(performance.now() - start); }
    return { snapshot, bffControl, sse, command };
  }, account);
  const report = { source: "production BFF and synthetic loopback Control; no Node, database or exchange", generated_at: new Date().toISOString(), samples: { browser_to_bff_snapshot: quantiles(samples.snapshot), bff_to_test_control_headers: quantiles(samples.bffControl), navigation_to_visible_snapshot: quantiles(navigation), sse_connect_to_first_event: quantiles(samples.sse), command_bff_ack: quantiles(samples.command), node_projection_to_render: "Not measured: fixture has no Node projection publisher.", control_durable_receipt: "Not measured: fixture ACK is not a database commit.", exchange_network: "Not measured: isolated Control fixture contains no exchange network." } };
  const file = test.info().outputPath("phase-performance.json"); mkdirSync(dirname(file), { recursive: true }); writeFileSync(file, `${JSON.stringify(report, null, 2)}\n`);
  expect(report.samples.browser_to_bff_snapshot.samples).toBe(25);
});
