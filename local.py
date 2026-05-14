"""ASGI adapter for running lambda_function.py locally.

Run with:

    uvicorn local:app --reload
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Any
from urllib.parse import parse_qsl

from lambda_function import lambda_handler

logger = logging.getLogger(__name__)


async def _read_body(receive: Any) -> bytes:
    body = b""

    while True:
        message = await receive()

        if message["type"] == "http.disconnect":
            break

        if message["type"] != "http.request":
            continue

        body += message.get("body", b"")
        if not message.get("more_body", False):
            break

    return body


def _decode_header(value: bytes) -> str:
    return value.decode("latin-1")


def _headers_to_event(headers: list[tuple[bytes, bytes]]) -> dict[str, str]:
    event_headers: dict[str, str] = {}

    for raw_name, raw_value in headers:
        name = _decode_header(raw_name)
        value = _decode_header(raw_value)
        if name in event_headers:
            event_headers[name] = f"{event_headers[name]},{value}"
        else:
            event_headers[name] = value

    return event_headers


async def to_lambda_event(scope: dict[str, Any], receive: Any) -> dict[str, Any]:
    """Convert an ASGI HTTP request into the API Gateway event shape."""

    path = scope.get("path", "/")
    method = scope.get("method", "GET").upper()
    raw_query_string = scope.get("query_string", b"")
    query_string = raw_query_string.decode("latin-1")
    query_string_parameters = dict(parse_qsl(query_string, keep_blank_values=True))

    body_bytes = await _read_body(receive)
    is_base64_encoded = False

    try:
        body = body_bytes.decode("utf-8") if body_bytes else None
    except UnicodeDecodeError:
        body = base64.b64encode(body_bytes).decode("ascii")
        is_base64_encoded = True

    client = scope.get("client") or ("", 0)

    return {
        "resource": path,
        "path": path,
        "httpMethod": method,
        "headers": _headers_to_event(scope.get("headers", [])),
        "queryStringParameters": query_string_parameters,
        "body": body,
        "isBase64Encoded": is_base64_encoded,
        "requestContext": {
            "httpMethod": method,
            "path": path,
            "identity": {
                "sourceIp": client[0],
            },
        },
    }


def _response_headers(response: dict[str, Any]) -> list[tuple[bytes, bytes]]:
    headers = response.get("headers") or {}
    return [
        (str(name).encode("latin-1"), str(value).encode("latin-1"))
        for name, value in headers.items()
    ]


def _response_body(response: dict[str, Any]) -> bytes:
    body = response.get("body", b"")

    if body is None:
        return b""

    if response.get("isBase64Encoded"):
        return base64.b64decode(body)

    if isinstance(body, bytes):
        return body

    if isinstance(body, str):
        return body.encode("utf-8")

    return json.dumps(body).encode("utf-8")


async def _handle_lifespan(receive: Any, send: Any) -> None:
    while True:
        message = await receive()

        if message["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
            return


async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
    """ASGI application entry point used by uvicorn."""

    if scope["type"] == "lifespan":
        await _handle_lifespan(receive, send)
        return

    if scope["type"] != "http":
        await send(
            {
                "type": "http.response.start",
                "status": 500,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b'{"success": false, "comment": "UNSUPPORTED_ASGI_SCOPE"}',
            }
        )
        return

    event = await to_lambda_event(scope, receive)
    logger.info("Local Lambda event: %s", event)

    response = lambda_handler(event, None)
    if response is None:
        response = {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(
                {
                    "success": False,
                    "comment": "LAMBDA_HANDLER_RETURNED_NONE",
                }
            ),
        }

    status = int(response.get("statusCode", 200))

    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": _response_headers(response),
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": _response_body(response),
        }
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("local:app", host="127.0.0.1", port=8000, reload=True)
