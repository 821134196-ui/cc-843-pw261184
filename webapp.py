"""Stdlib HTTP server that runs the fixed benchmark and serves the dashboard.

    python -m webapp            # http://127.0.0.1:8000/
    PORT=9000 python -m webapp

Endpoints:
    GET  /            dashboard page (static/index.html)
    GET  /api/run     run the fixed workload against both runtimes and
                      return the comparison report as JSON
    GET  /api/health  liveness probe
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from bench.runner import compare
from bench.workload import WorkloadConfig

STATIC_DIR = Path(__file__).parent / "static"

# Only one benchmark at a time: a run saturates all worker threads.
_run_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
        if self.path == "/api/health":
            self._send_json({"status": "ok"})
            return

        if self.path == "/api/run":
            if not _run_lock.acquire(blocking=False):
                self._send_json({"error": "benchmark already running"},
                                status=409)
                return
            try:
                report = compare(WorkloadConfig())
                self._send_json(report)
            finally:
                _run_lock.release()
            return

        if self.path in ("/", "/index.html"):
            page = (STATIC_DIR / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(page)))
            self.end_headers()
            self.wfile.write(page)
            return

        self._send_json({"error": "not found"}, status=404)

    def log_message(self, fmt: str, *args) -> None:  # quieter logs
        pass


def main() -> None:
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"dashboard: http://127.0.0.1:{port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
