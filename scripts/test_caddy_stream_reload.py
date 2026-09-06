"""Isolated Caddy SSE reload regression; loopback ports and synthetic data only."""
import argparse
import http.server
import http.client
import json
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request


class Events(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        try:
            for _ in range(100):
                self.wfile.write(b"data: fixture\n\n")
                self.wfile.flush()
                time.sleep(0.1)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *_):
        pass


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run(negative):
    upstream = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Events)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    admin, public = port(), port()
    proxy = {"handler": "reverse_proxy", "upstreams": [{"dial": f"127.0.0.1:{upstream.server_port}"}]}
    if negative:
        proxy["flush_interval"] = -1
    config = {"admin": {"listen": f"127.0.0.1:{admin}"}, "apps": {"http": {"servers": {
        "fixture": {"listen": [f"127.0.0.1:{public}"], "routes": [{"handle": [proxy]}]}
    }}}}
    with tempfile.TemporaryDirectory(prefix="venue-caddy-reload-") as tmp:
        path = tmp + "/fixture.json"
        with open(path, "w") as stream:
            json.dump(config, stream)
        with tempfile.TemporaryFile(mode="w+") as log:
            process = subprocess.Popen(["caddy", "run", "--config", path], stdout=log, stderr=log)
            try:
                for _ in range(50):
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{admin}/config/", timeout=1):
                            break
                    except OSError:
                        if process.poll() is not None:
                            raise RuntimeError("isolated Caddy failed to start")
                        time.sleep(0.1)
                rounds = 0
                for attempt in range(5):
                    start = time.monotonic()
                    with urllib.request.urlopen(f"http://127.0.0.1:{public}/events", timeout=3) as response:
                        assert response.readline() == b"data: fixture\n"
                        assert time.monotonic() - start < 2, "SSE was buffered"
                        config["apps"]["http"]["servers"]["fixture"]["routes"][0]["@id"] = f"fixture-{attempt}"
                        request = urllib.request.Request(f"http://127.0.0.1:{admin}/load", data=json.dumps(config).encode(), headers={"Content-Type": "application/json"})
                        try:
                            with urllib.request.urlopen(request, timeout=3):
                                pass
                        except (OSError, http.client.RemoteDisconnected):
                            if not negative:
                                raise
                        time.sleep(0.2)
                        if process.poll() is not None:
                            break
                        assert response.readline() == b"\n"
                        assert response.readline() == b"data: fixture\n"
                        rounds += 1
                code = process.poll()
                log.seek(0)
                panic = "context: internal error: missing cancel error" in log.read()
                print(json.dumps({"negative_flush": negative, "completed_reloads": rounds, "exit_code": code, "matching_panic": panic}))
                if negative:
                    assert code == 2 and panic, "original failure not reproduced"
                else:
                    assert code is None and rounds == 5, "fixed reload failed"
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=10)
                upstream.shutdown()
                upstream.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--negative-flush", action="store_true")
    run(parser.parse_args().negative_flush)
