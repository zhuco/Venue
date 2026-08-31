import { createServer } from "node:http";

const port = Number(process.env.VENUE_WEB_E2E_CONTROL_PORT ?? "38080");
const account = "00000000-0000-4000-8000-000000000001";
const binding = (instance_id) => ({ venue: "Binance", mode: "LIVE", trading_account_id: account, instance_id, symbol: "BTC/USDT" });
const write = (response, body, status = 200, headers = {}) => response.writeHead(status, { "content-type": "application/json", "cache-control": "no-store", ...headers }).end(JSON.stringify(body));
const snapshot = () => ({ schema_version: 2, generated_ms: Date.now(), connection: "LIVE", accounts: [{ venue: "Binance", mode: "LIVE", trading_account_id: account, health: "healthy", equity: "10000.00", available_margin: "8000.00", unrealized_pnl: "0", private_generation: 1, writer_generation: 1, last_reconciled_ms: Date.now() }], strategies: [{ ...binding("copy-btc"), kind: "copy", lifecycle: "running", config_epoch: 7, open_orders: 0, long_quantity: "0", short_quantity: "0", realized_pnl: "0", unrealized_pnl: "0", last_receipt_ms: Date.now(), attention: null }], copy_relations: [], markets: [], ledger: [] });
const facts = () => ({ schema_version: 2, generated_ms: Date.now(), orders: [], positions: [], fills: [], reconciliation: [], copy_ledger: [], drift: [], execution: [], risk: [], health: [] });

createServer((request, response) => {
  const target = new URL(request.url ?? "/", `http://127.0.0.1:${port}`);
  if (request.method === "GET" && target.pathname === "/v2/ui/snapshot") return write(response, snapshot());
  if (request.method === "GET" && target.pathname === "/v2/ui/execution-facts") return write(response, facts());
  if (request.method === "GET" && target.pathname === "/v2/copy/relation-candidates") return write(response, [{ binding: binding("leader-btc"), lifecycle: "running", config_epoch: 7 }, { binding: binding("copy-btc"), lifecycle: "running", config_epoch: 7 }]);
  if (request.method === "GET" && target.pathname === "/v2/copy/relations") return write(response, []);
  if (request.method === "POST" && (target.pathname === "/v2/copy/relations" || target.pathname === "/v2/control/commands")) return write(response, { schema_version: 2, request_id: "018f3ae9-8a15-7d6c-b2a0-13b8d2d7b119", receipt_id: "e2e", state: "accepted", observed_ms: Date.now(), detail: "test-control" });
  if (request.method === "GET" && target.pathname === "/v2/ui/events") {
    const venue = target.searchParams.get("venue"); const mode = target.searchParams.get("mode"); const scoped = target.searchParams.get("trading_account_id"); const after = target.searchParams.get("after");
    if (venue !== "binance" || mode !== "LIVE" || scoped !== account || after === null || request.headers["last-event-id"] !== after) return write(response, { error: "scope" }, 400);
    response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store", connection: "keep-alive" });
    response.write(`id: 1\nevent: control\ndata: ${JSON.stringify({ schema_version: 2, cursor: 1, previous_cursor: 0, event_type: "snapshot", scope: { venue, mode, trading_account_id: scoped } })}\n\n`);
    return;
  }
  return write(response, { error: "not_found" }, 404);
}).listen(port, "127.0.0.1");
