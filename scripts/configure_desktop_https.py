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
                "flush_interval": -1,
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
    if websocket:
        proxy["flush_interval"] = -1
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
        {"handle": WEB_HANDLERS},
    ]


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
        if not old_layout and not current_layout:
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
    args = parser.parse_args()
    for _ in range(3):
        current, etag = read_route()
        desired = configured_route(current)
        if current == desired:
            print("Venue desktop HTTPS and market routes verified")
            return
        if args.check:
            raise SystemExit("Venue desktop HTTPS and market routes are not installed")
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


if __name__ == "__main__":
    main()
