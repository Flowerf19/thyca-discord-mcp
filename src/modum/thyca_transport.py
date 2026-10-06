"""Unredirected loopback HTTP transport for the thin native Thyca client.

Canonical owner of the urllib transport: unredirected opener with
environment proxies explicitly disabled, bounded response reads with
declared-length truncation evidence, and native HTTPError status
preservation without ever reading error bodies.

Depends only on :mod:`modum.thyca_errors`; never on validation,
trace, or the client. No validation, no trace shaping, no business policy,
no retries. ``asyncio`` is used only for its timeout/cancellation markers;
all blocking I/O stays synchronous here and runs via ``asyncio.to_thread``
from the client.
"""

from __future__ import annotations

import asyncio
import http.client
import urllib.error
import urllib.request
from collections.abc import Awaitable, Callable

from .thyca_errors import ThycaProtocolError, _NetworkMarker, _TimeoutMarker

# Bounded response read so a corrupt/huge body cannot exhaust memory.
MAX_RESPONSE_BYTES = 10 * 1024 * 1024

# Technical per-request socket bound. Not a business budget; the worker owns
# queueing and retry policy (and must not blind-retry an uncertain POST).
DEFAULT_REQUEST_TIMEOUT_SECONDS = 120.0

# Transport injection for offline fixtures:
# async (method, url, body_bytes_or_None) -> (status_code, body_bytes).
HttpTransport = Callable[[str, str, bytes | None], Awaitable[tuple[int, bytes]]]


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Block every redirect so urllib can never follow off-loopback."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, ANN202
        raise urllib.error.HTTPError(req.get_full_url(), 310, "redirect blocked", {}, None)


def _declared_body_len(fp) -> int | None:  # noqa: ANN001
    """Declared remaining body bytes from the native reader, or None.

    ``http.client.HTTPResponse.length`` is the remaining body count; the
    native layer leaves it ``None`` for chunked, close-delimited, negative,
    or malformed ``Content-Length`` framing. Only a real non-negative
    ``int`` counts as evidence (``bool`` is not evidence); unknown readers
    and mocks yield ``None``, so a truncated body is never inferred without
    transport evidence.
    """
    value = getattr(fp, "length", None)
    if type(value) is int and value >= 0:
        return value
    return None


def _read_bounded(fp) -> bytes:  # noqa: ANN001
    # Captured before reading: HTTPResponse.length counts DOWN as bytes
    # arrive, and native read(amt) on an early EOF returns the short partial
    # body WITHOUT raising (no IncompleteRead for bounded reads).
    declared = _declared_body_len(fp)
    try:
        data = fp.read(MAX_RESPONSE_BYTES + 1)
    except (TimeoutError, asyncio.TimeoutError):
        # Includes socket.timeout (alias of TimeoutError). Never str(exc).
        raise _TimeoutMarker from None
    except (
        OSError,
        http.client.HTTPException,
        urllib.error.URLError,
        ValueError,
        EOFError,
    ):
        # ConnectionResetError, BrokenPipeError, IncompleteRead,
        # RemoteDisconnected, etc. Never propagate str(exc) (may leak creds).
        raise _NetworkMarker from None
    except Exception:
        raise ThycaProtocolError(reason="transport") from None
    if data is None:
        return b""
    if not isinstance(data, bytes):
        raise ThycaProtocolError(reason="invalid_json")
    if len(data) > MAX_RESPONSE_BYTES:
        # Size bound fires before any truncation inference.
        raise ThycaProtocolError(reason="body_too_large")
    if declared is not None and len(data) < declared:
        # The peer closed before delivering the declared body: a transport
        # disconnect, never parseable JSON evidence. Declared lengths that
        # are fulfilled fall through unchanged (malformed JSON stays
        # protocol/invalid_json); unknown framing (declared None) keeps its
        # existing read-error mapping only.
        raise _NetworkMarker from None
    return data


def sync_http_request(
    method: str, url: str, body: bytes | None, request_timeout_seconds: float
) -> tuple[int, bytes]:
    """Synchronous unredirected HTTP; timeout is an explicit caller argument.

    Moved verbatim from ``ThycaClient._sync_http``; the only change is
    ``self._timeout`` replaced by the explicit ``request_timeout_seconds``
    parameter. Non-200 bodies are never read (status only); close failures
    never erase the known status.
    """
    if method == "POST":
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        data: bytes | None = body if body is not None else b"{}"
    else:
        headers = {"Accept": "application/json"}
        data = None
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    # ProxyHandler({}) explicitly disables environment proxies so
    # loopback POST text can never be sent to a configured proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirectHandler)
    try:
        with opener.open(request, timeout=request_timeout_seconds) as response:
            status = int(response.status)
            return status, _read_bounded(response)
    except urllib.error.HTTPError as exc:
        # Non-200 bodies are never used; preserve only the authoritative
        # status. Never call exc.read() (unbounded). Close best-effort;
        # close failures must not erase the known status.
        try:
            status_code = int(exc.code)
        except (TypeError, ValueError):
            raise ThycaProtocolError(reason="http_status") from None
        fp = getattr(exc, "fp", None)
        if fp is not None:
            try:
                fp.close()
            except Exception:
                pass
        try:
            exc.close()
        except Exception:
            pass
        return status_code, b""
    except (_TimeoutMarker, _NetworkMarker):
        raise
    except (TimeoutError, asyncio.TimeoutError):
        raise _TimeoutMarker from None
    except ThycaProtocolError:
        raise
    except (
        OSError,
        urllib.error.URLError,
        ValueError,
        http.client.HTTPException,
        EOFError,
    ):
        # URLError wraps socket timeouts/disconnects; ValueError covers
        # malformed responses; HTTPException covers IncompleteRead,
        # RemoteDisconnected, etc. Never propagate str(exc) (may leak creds).
        raise _NetworkMarker from None
