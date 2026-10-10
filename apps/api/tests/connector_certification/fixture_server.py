from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socket import SHUT_RDWR
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit


@dataclass(frozen=True)
class FixtureResponse:
    status: int = 200
    body: Any = None
    headers: Mapping[str, str] = field(default_factory=dict)
    drop_connection: bool = False


@dataclass(frozen=True)
class RequestRecord:
    method: str
    path: str
    headers: Mapping[str, str]
    body: bytes


class FixtureServer:
    """Threaded synthetic HTTP server with per-route response fault sequences."""

    def __init__(self) -> None:
        self._routes: dict[tuple[str, str], tuple[FixtureResponse, ...]] = {}
        self._route_calls: dict[tuple[str, str], int] = {}
        self._lock = threading.RLock()
        self.request_log: list[RequestRecord] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:
                return

            def _dispatch(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                path = urlsplit(self.path).path
                record = RequestRecord(
                    method=self.command,
                    path=path,
                    headers=dict(self.headers.items()),
                    body=body,
                )
                response = owner._record_and_get_response(record)
                if response.drop_connection:
                    try:
                        self.connection.shutdown(SHUT_RDWR)
                    except OSError:
                        pass
                    self.connection.close()
                    return

                response_body = owner._encode_body(response.body)
                self.send_response(response.status)
                response_headers = dict(response.headers)
                lower_headers = {key.lower() for key in response_headers}
                if "content-type" not in lower_headers:
                    content_type = (
                        "application/json"
                        if isinstance(response.body, (dict, list))
                        else "application/octet-stream"
                    )
                    response_headers["Content-Type"] = content_type
                if "content-length" not in lower_headers:
                    response_headers["Content-Length"] = str(len(response_body))
                for key, value in response_headers.items():
                    self.send_header(key, value)
                self.end_headers()
                if self.command != "HEAD" and response_body:
                    self.wfile.write(response_body)

            do_GET = _dispatch
            do_POST = _dispatch
            do_PUT = _dispatch
            do_PATCH = _dispatch
            do_DELETE = _dispatch
            do_HEAD = _dispatch

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._httpd.daemon_threads = True
        self._thread: threading.Thread | None = None

    @staticmethod
    def _encode_body(body: Any) -> bytes:
        if body is None:
            return b""
        if isinstance(body, bytes):
            return body
        if isinstance(body, str):
            return body.encode("utf-8")
        return json.dumps(body).encode("utf-8")

    @property
    def base_url(self) -> str:
        host, port = self._httpd.server_address[:2]
        return f"http://{host}:{port}"

    def add_route(
        self,
        path: str,
        response: FixtureResponse | None = None,
        *,
        responses: Iterable[FixtureResponse] | None = None,
        method: str = "GET",
    ) -> None:
        route_key = (method.upper(), path)
        sequence = tuple(responses) if responses is not None else (response or FixtureResponse(),)
        if not sequence:
            raise ValueError("A fixture route needs at least one response")
        with self._lock:
            self._routes[route_key] = sequence
            self._route_calls[route_key] = 0

    def _record_and_get_response(self, record: RequestRecord) -> FixtureResponse:
        route_key = (record.method, record.path)
        with self._lock:
            self.request_log.append(record)
            responses = self._routes.get(route_key)
            if not responses:
                return FixtureResponse(status=404, body={"error": "route not found"})
            index = self._route_calls.get(route_key, 0)
            self._route_calls[route_key] = index + 1
            return responses[min(index, len(responses) - 1)]

    def clear_request_log(self) -> None:
        with self._lock:
            self.request_log.clear()

    def __enter__(self) -> FixtureServer:
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._httpd.serve_forever,
                kwargs={"poll_interval": 0.01},
                daemon=True,
            )
            self._thread.start()
        return self

    def close(self) -> None:
        if self._thread is None:
            self._httpd.server_close()
            return
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=2)
        self._thread = None

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
