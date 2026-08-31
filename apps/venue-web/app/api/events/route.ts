import { NextRequest, NextResponse } from "next/server";
import { controlOrigin, getSession, noStore } from "@/lib/server";

export const dynamic = "force-dynamic";
const venues = new Set(["binance", "gate", "bitget", "bybit", "okx", "hyperliquid"]);

export async function GET(request: NextRequest) {
  const session = getSession(request); if (!session) return NextResponse.json({ error: "session_required" }, { status: 401, headers: noStore() });
  const venue = request.nextUrl.searchParams.get("venue"); const account = request.nextUrl.searchParams.get("trading_account_id");
  if (!venue || !venues.has(venue) || !account || !session.account_scope.includes(account) || request.nextUrl.searchParams.getAll("venue").length !== 1 || request.nextUrl.searchParams.getAll("trading_account_id").length !== 1) return NextResponse.json({ error: "event_scope_rejected" }, { status: 403, headers: noStore() });
  const origin = controlOrigin();
  if (!origin) return NextResponse.json({ error: "control_origin_rejected" }, { status: 503, headers: noStore() });
  const after = request.headers.get("last-event-id") ?? "0";
  if (!/^\d+$/.test(after) || !Number.isSafeInteger(Number(after)))
    return NextResponse.json({ error: "event_cursor_rejected" }, { status: 400, headers: noStore() });
  const target = new URL("/v2/ui/events", origin); target.searchParams.set("venue", venue); target.searchParams.set("mode", "LIVE"); target.searchParams.set("trading_account_id", account); target.searchParams.set("after", after);
  const abort = new AbortController();
  const disconnect = () => abort.abort();
  request.signal.addEventListener("abort", disconnect, { once: true });
  if (request.signal.aborted) disconnect();
  const deadline = setTimeout(disconnect, Math.max(0, session.expires_ms - Date.now()));
  const headersDeadline = setTimeout(disconnect, 10_000);
  const cleanup = () => {
    clearTimeout(deadline);
    clearTimeout(headersDeadline);
    request.signal.removeEventListener("abort", disconnect);
    abort.abort();
  };
  try {
    const upstream = await fetch(target, { signal: abort.signal, cache: "no-store", redirect: "error", headers: { "Last-Event-ID": after } });
    clearTimeout(headersDeadline);
    if (!upstream.ok || !upstream.body || !upstream.headers.get("content-type")?.includes("text/event-stream")) {
      cleanup();
      return NextResponse.json({ error: "event_unavailable" }, { status: 503, headers: noStore() });
    }
    const reader = upstream.body.getReader();
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const next = await reader.read();
          if (next.done) { cleanup(); controller.close(); }
          else controller.enqueue(next.value);
        } catch {
          cleanup();
          controller.error(new Error("event_stream_closed"));
        }
      },
      async cancel() { cleanup(); await reader.cancel().catch(() => undefined); },
    });
    return new NextResponse(body, { headers: { ...noStore(), "Content-Type": "text/event-stream", Connection: "keep-alive" } });
  } catch {
    cleanup();
    return NextResponse.json({ error: "event_unavailable" }, { status: 503, headers: noStore() });
  }
}
