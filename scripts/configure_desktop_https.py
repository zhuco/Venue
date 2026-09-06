#!/usr/bin/env python3
"""Install the desktop API and bounded public-market relay in the Venue HTTPS host."""

import argparse
import copy
import json
import re
import subprocess
import urllib.error
import urllib.request


ADMIN_ROUTE = "http://127.0.0.1:2019/id/venue-kol-web"
DESKTOP_ID = "venue-desktop-api"
MARKET_REST_ID = "venue-desktop-market-rest"
MARKET_STREAM_ID = "venue-desktop-market-stream"
GET_PATHS = [
    "/v2/account/session",
    "/v2/ui/snapshot",
    "/v2/ui/events",
    "/v2/copy/relations",
    "/v2/kol/executions",
    "/v2/grid/instances",
    "/v2/kol/leader-bot",
    "/v2/kol/leader-bots",
    "/v2/strategies/support-martingale/instances",
]
POST_PATHS = [
    "/v2/account/terminal/register",
    "/v2/account/login",
    "/v2/account/logout",
    "/v2/account/credentials",
    "/v2/account/credentials/verify",
    "/v2/account/credentials/delete",
    "/v2/account/select",
    "/v2/kol/terminal/account",
    "/v2/kol/terminal/orders",
    "/v2/kol/terminal/orders/cancel",
    "/v2/kol/terminal/positions/action",
    "/v2/grid/instances",
    "/v2/grid/lifecycle",
    "/v2/kol/leader-bot",
    "/v2/kol/leader-bot/lifecycle",
    "/v2/kol/leader-bots",
    "/v2/kol/leader-bots/update",
    "/v2/kol/leader-bots/lifecycle",
    "/v2/strategies/support-martingale/instances",
    "/v2/strategies/support-martingale/preflight",
    "/v2/strategies/support-martingale/lifecycle",
    "/v2/control/commands",
    "/v2/copy/relations",
]
MARKET_REST_PATHS = [
    "/fapi/v1/exchangeInfo",
    "/fapi/v1/ticker/24hr",
    "/fapi/v1/klines",
]
MARKET_STREAM_PATHS = [
    "/market/stream",
    "/public/stream",
]
WEB_HANDLERS = [
    {"handler": "reverse_proxy", "upstreams": [{"dial": "127.0.0.1:39200"}]}
]


def desktop_route():
    return {
        "@id": DESKTOP_ID,
        "match": [
            {"method": ["GET"], "path": GET_PATHS},
            {"method": ["POST"], "path": POST_PATHS},
        ],
        "handle": [
            {"handler": "headers", "response": {"set": {"Cache-Control": ["no-store"]}}},
            {
                "handler": "reverse_proxy",
                "upstreams": [{"dial": "127.0.0.1:39180"}],
                # SSE is automatically flushed by Content-Type. A negative interval
                # uses Caddy 2.6.2's inconsistent ignoreClientGoneContext on reload.
                "transport": {"protocol": "http", "versions": ["1.1"]},
            },
        ],
        "terminal": True,
    }


def market_route(route_id, paths, upstream, websocket=False):
    proxy = {
        "handler": "reverse_proxy",
        "upstreams": [{"dial": f"{upstream}:443"}],
        "headers": {
            "request": {
                "delete": ["Authorization", "Cookie"],
                "set": {"Host": [upstream]},
            }
        },
        "transport": {
            "protocol": "http",
            "tls": {"server_name": upstream},
            "versions": ["1.1"] if websocket else ["1.1", "2"],
        },
    }
    # WebSocket upgrades are bidirectional streams, not buffered HTTP bodies.
    return {
        "@id": route_id,
        "match": [{"method": ["GET"], "path": paths}],
        "handle": [
            {"handler": "headers", "response": {"set": {"Cache-Control": ["no-store"]}}},
            proxy,
        ],
        "terminal": True,
    }


def desired_routes():
    return [
        desktop_route(),
        market_route(MARKET_REST_ID, MARKET_REST_PATHS, "fapi.binance.com"),
        market_route(
            MARKET_STREAM_ID,
            MARKET_STREAM_PATHS,
            "fstream.binance.com",
            websocket=True,
        ),
        *multi_market_routes(),
        {"handle": WEB_HANDLERS},
    ]


def multi_market_routes():
    sources = {
        "bybit": ("api.bybit.com", ["/v5/market/" + path for path in
            ["instruments-info", "tickers", "kline", "orderbook", "recent-trade"]]),
        "bitget": ("api.bitget.com", ["/api/v3/market/" + path for path in
            ["instruments", "tickers", "candles", "orderbook", "fills"]]),
        "okx": ("www.okx.com", ["/api/v5/public/instruments"] +
            ["/api/v5/market/" + path for path in ["tickers", "candles", "history-candles", "books", "trades"]]),
        "gate": ("api.gateio.ws", ["/api/v4/futures/usdt/" + path for path in
            ["contracts", "tickers", "candlesticks", "order_book", "trades"]]),
        "hyperliquid": ("api.hyperliquid.xyz", ["/info"]),
    }
    result = []
    for venue, (host, paths) in sources.items():
        prefix = f"/quotes/{venue}"
        route = market_route(f"venue-desktop-market-{venue}", [prefix + p for p in paths], host)
        if venue == "hyperliquid":
            # /info is read-only; the exchange's order-writing /exchange is never exposed.
            route["match"][0]["method"] = ["POST"]
        route["handle"].insert(1, {"handler": "rewrite", "strip_path_prefix": prefix})
        result.append(route)
    return result


def configured_route(current):
    if (
        current.get("@id") != "venue-kol-web"
        or current.get("match") != [{"host": ["clawdbotweb.site"]}]
        or current.get("terminal") is not True
    ):
        raise ValueError("The existing Venue HTTPS host no longer matches this deployment")
    result = copy.deepcopy(current)
    handles = result.get("handle")
    if handles == WEB_HANDLERS:
        result["handle"] = [{
            "handler": "subroute",
            "routes": desired_routes(),
        }]
    elif (
        isinstance(handles, list) and len(handles) == 1
        and handles[0].get("handler") == "subroute"
    ):
        routes = handles[0].get("routes", [])
        old_layout = (
            len(routes) == 2
            and routes[0].get("@id") == DESKTOP_ID
            and routes[1] == {"handle": WEB_HANDLERS}
        )
        current_layout = (
            len(routes) == 4
            and [route.get("@id") for route in routes[:3]]
            == [DESKTOP_ID, MARKET_REST_ID, MARKET_STREAM_ID]
            and routes[3] == {"handle": WEB_HANDLERS}
        )
        multi_layout = (
            len(routes) == 9
            and [route.get("@id") for route in routes[:-1]] ==
                [route.get("@id") for route in desired_routes()[:-1]]
            and routes[-1] == {"handle": WEB_HANDLERS}
        )
        if not old_layout and not current_layout and not multi_layout:
            raise ValueError("Unexpected host handlers; refusing to overwrite another deployment")
        handles[0]["routes"] = desired_routes()
    else:
        raise ValueError("Unexpected host handlers; refusing to overwrite another deployment")
    return result


def read_route():
    # Caddy may return ETag as an HTTP trailer. curl preserves both header sections;
    # urllib's response.headers would silently lose the trailer and the CAS guard.
    response = subprocess.run(
        ["curl", "--silent", "--show-error", "--fail", "--max-time", "10",
         "--dump-header", "/dev/stderr", ADMIN_ROUTE],
        check=True, capture_output=True,
    )
    etags = re.findall(r"(?im)^etag:\s*([^\r\n]+)", response.stderr.decode("utf-8"))
    if len(etags) != 1:
        raise ValueError("Caddy did not supply a unique ETag; no configuration was changed")
    return json.loads(response.stdout), etags[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Verify without changing configuration")
    parser.add_argument("--drained-legacy-streams", action="store_true",
                        help="Confirm clients using the old negative-flush routes are disconnected")
    args = parser.parse_args()
    for _ in range(3):
        current, etag = read_route()
        desired = configured_route(current)
        if current == desired:
            print("Venue desktop HTTPS and market routes verified")
            return
        if args.check:
            raise SystemExit("Venue desktop HTTPS and market routes are not installed")
        if has_negative_flush(current) and not args.drained_legacy_streams:
            raise SystemExit("Disconnect old desktop SSE/WebSocket clients before reload; then use --drained-legacy-streams")
        request = urllib.request.Request(
            ADMIN_ROUTE, data=json.dumps(desired).encode("utf-8"), method="PATCH",
            headers={"Content-Type": "application/json", "If-Match": etag},
        )
        try:
            with urllib.request.urlopen(request, timeout=15):
                pass
        except urllib.error.HTTPError as error:
            if error.code == 412:
                continue
            raise SystemExit(f"Caddy rejected the desktop route (HTTP {error.code})") from None
        installed, _ = read_route()
        if installed != desired:
            raise SystemExit("Caddy readback differs; inspect the Venue host route")
        print("Venue desktop HTTPS and market routes installed and verified")
        return
    raise SystemExit("Caddy configuration changed concurrently; retry after the other deployment")


def has_negative_flush(value):
    if isinstance(value, dict):
        return value.get("flush_interval", 0) < 0 or any(has_negative_flush(v) for v in value.values())
    return isinstance(value, list) and any(has_negative_flush(v) for v in value)


if __name__ == "__main__":
    main()
