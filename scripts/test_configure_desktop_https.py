import copy
import unittest

from configure_desktop_https import (
    GET_PATHS,
    MARKET_REST_ID,
    MARKET_REST_PATHS,
    MARKET_STREAM_ID,
    MARKET_STREAM_PATHS,
    POST_PATHS,
    WEB_HANDLERS,
    configured_route,
)


class DesktopHttpsTests(unittest.TestCase):
    def setUp(self):
        self.original = {
            "@id": "venue-kol-web",
            "match": [{"host": ["clawdbotweb.site"]}],
            "handle": copy.deepcopy(WEB_HANDLERS),
            "terminal": True,
        }

    def test_preserves_web_fallback_and_is_idempotent(self):
        before = copy.deepcopy(self.original)
        result = configured_route(self.original)
        self.assertEqual(self.original, before)
        self.assertEqual(result, configured_route(result))
        self.assertEqual(result["match"], before["match"])
        routes = result["handle"][0]["routes"]
        self.assertEqual(routes[3], {"handle": before["handle"]})
        self.assertEqual(routes[0]["handle"][1]["flush_interval"], -1)
        self.assertTrue(routes[0]["terminal"])

    def test_only_exact_desktop_methods_and_paths_are_exposed(self):
        for paths in (GET_PATHS, POST_PATHS):
            self.assertEqual(len(paths), len(set(paths)))
            self.assertTrue(all("*" not in path for path in paths))
            self.assertFalse(any("account-node" in path or "admin" in path for path in paths))
        self.assertIn("/v2/ui/events", GET_PATHS)
        self.assertNotIn("/v2/ui/events", POST_PATHS)
        self.assertIn("/v2/kol/terminal/positions/action", POST_PATHS)
        self.assertNotIn("/v2/kol/terminal/positions/action", GET_PATHS)

    def test_market_relay_is_get_only_exact_and_strips_private_headers(self):
        routes = configured_route(self.original)["handle"][0]["routes"]
        rest, stream = routes[1:3]
        self.assertEqual(rest["@id"], MARKET_REST_ID)
        self.assertEqual(stream["@id"], MARKET_STREAM_ID)
        self.assertEqual(rest["match"], [{"method": ["GET"], "path": MARKET_REST_PATHS}])
        self.assertEqual(stream["match"], [{"method": ["GET"], "path": MARKET_STREAM_PATHS}])
        for route in (rest, stream):
            self.assertTrue(all("*" not in path for path in route["match"][0]["path"]))
            request_headers = route["handle"][1]["headers"]["request"]
            self.assertEqual(request_headers["delete"], ["Authorization", "Cookie"])
            self.assertTrue(route["terminal"])
        self.assertEqual(rest["handle"][1]["upstreams"], [{"dial": "fapi.binance.com:443"}])
        self.assertEqual(stream["handle"][1]["upstreams"], [{"dial": "fstream.binance.com:443"}])
        self.assertEqual(stream["handle"][1]["flush_interval"], -1)

    def test_upgrades_previous_desktop_only_layout(self):
        desktop_only = configured_route(self.original)
        routes = desktop_only["handle"][0]["routes"]
        desktop_only["handle"][0]["routes"] = [routes[0], routes[-1]]
        upgraded = configured_route(desktop_only)["handle"][0]["routes"]
        self.assertEqual([route.get("@id") for route in upgraded[:3]], [
            "venue-desktop-api", MARKET_REST_ID, MARKET_STREAM_ID
        ])

    def test_rejects_other_sites_and_unrecognized_handlers(self):
        for change in (
            {"match": [{"host": ["other.example.com"]}]},
            {"handle": [{"handler": "static_response"}]},
            {"terminal": False},
        ):
            with self.assertRaises(ValueError):
                configured_route(self.original | change)


if __name__ == "__main__":
    unittest.main()
