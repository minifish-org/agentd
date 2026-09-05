#!/usr/bin/env python3
"""Deterministic, loopback-only OpenAI-compatible provider for agentd demos."""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


MAX_REQUEST_BYTES = 1024 * 1024


class DemoHandler(BaseHTTPRequestHandler):
    server_version = "agentd-demo-provider/1"

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self._send_json(200, {"status": "ok"})
            return
        if self.path == "/v1/models":
            self._send_json(
                200,
                {"object": "list", "data": [{"id": "demo/chat", "object": "model"}]},
            )
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/chat/completions":
            self._send_json(404, {"error": "not found"})
            return
        try:
            request = self._read_request()
            model = request.get("model")
            messages = request.get("messages")
            if not isinstance(model, str) or not model.strip():
                raise ValueError("model must be a non-empty string")
            if not isinstance(messages, list) or not messages:
                raise ValueError("messages must be a non-empty array")
        except (ValueError, json.JSONDecodeError) as error:
            self._send_json(400, {"error": str(error)})
            return

        if model == "demo/sandbox-canary":
            try:
                response = self._sandbox_canary_response(model, messages)
            except ValueError as error:
                self._send_json(400, {"error": str(error)})
                return
            self._send_json(200, response)
            return

        content = json.dumps(
            {"reply": "agentd completed a deterministic demo turn"},
            separators=(",", ":"),
        )
        self._send_json(
            200,
            {
                "id": "chatcmpl-agentd-demo",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
            },
        )

    def _read_request(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise ValueError("Content-Length is required")
        length = int(raw_length)
        if length < 0 or length > MAX_REQUEST_BYTES:
            raise ValueError("request body exceeds 1 MiB")
        body = self.rfile.read(length)
        value = json.loads(body)
        if not isinstance(value, dict):
            raise ValueError("request body must be a JSON object")
        return value

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sandbox_canary_response(
        self, model: str, messages: list[dict[str, Any]]
    ) -> dict[str, Any]:
        scenario = None
        for message in messages:
            if message.get("role") != "user":
                continue
            content = message.get("content")
            if not isinstance(content, str):
                continue
            try:
                payload = json.loads(content)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                input_value = payload.get("input")
                if isinstance(input_value, dict):
                    scenario = input_value.get("scenario")
        if scenario not in {"full", "isolation", "cancel"}:
            raise ValueError("sandbox canary requires scenario full, isolation, or cancel")

        tool_results = [message for message in messages if message.get("role") == "tool"]
        plans: dict[str, list[dict[str, Any]]] = {
            "full": [
                {
                    "action": "shell",
                    "script": "command -v curl >/dev/null && command -v python3 >/dev/null && command -v node >/dev/null || (apt-get update -qq && apt-get install -y -qq --no-install-recommends ca-certificates curl python3 nodejs)",
                    "timeout_ms": 60000,
                },
                {
                    "action": "shell",
                    "script": "printf persistent > state.txt && printf %s \"$VALUE\"",
                    "env": {"VALUE": "env-ok"},
                },
                {"action": "exec", "command": "cat", "args": ["state.txt"]},
                {
                    "action": "exec",
                    "command": "python3",
                    "args": ["-c", "print('python-ok')"],
                },
                {
                    "action": "exec",
                    "command": "node",
                    "args": ["-e", "console.log('node-ok')"],
                },
                {
                    "action": "exec",
                    "command": "curl",
                    "args": ["-fsS", "https://example.com"],
                },
                {
                    "action": "shell",
                    "script": "printf nonzero >&2; exit 7",
                },
                {"action": "shell", "script": "sleep 2", "timeout_ms": 50},
            ],
            "isolation": [
                {
                    "action": "shell",
                    "script": "test ! -e /workspace/state.txt && printf isolated",
                }
            ],
            "cancel": [
                {
                    "action": "shell",
                    "script": "printf started; sleep 60",
                    "timeout_ms": 60000,
                }
            ],
        }
        plan = plans[scenario]
        if len(tool_results) < len(plan):
            call_number = len(tool_results) + 1
            arguments = json.dumps(plan[len(tool_results)], separators=(",", ":"))
            message: dict[str, Any] = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": f"sandbox-canary-{scenario}-{call_number}",
                        "type": "function",
                        "function": {
                            "name": "sandbox_session",
                            "arguments": arguments,
                        },
                    }
                ],
            }
            finish_reason = "tool_calls"
        else:
            message = {
                "role": "assistant",
                "content": json.dumps(
                    {"reply": f"sandbox canary {scenario} complete"},
                    separators=(",", ":"),
                ),
            }
            finish_reason = "stop"
        return {
            "id": f"chatcmpl-sandbox-canary-{scenario}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {"index": 0, "message": message, "finish_reason": finish_reason}
            ],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }

    def log_message(self, format: str, *args: Any) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18000)
    args = parser.parse_args()
    if args.host not in {"127.0.0.1", "localhost"}:
        parser.error("the demo provider may bind only to loopback")
    server = ThreadingHTTPServer((args.host, args.port), DemoHandler)
    print(f"agentd demo provider listening on http://{args.host}:{args.port}/v1", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
