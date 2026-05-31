"""Shared HTTP session and concurrency knobs for Wikimedia API access.

Phase 1 of the optimization plan (doc/wikipedia-crawler-optimization.md). A single
keep-alive ``requests.Session`` is reused across every Wikimedia call so the crawler
stops re-paying DNS + TCP + TLS on each request, and a built-in ``urllib3`` retry
policy backs off on 429/5xx (honoring ``Retry-After``) instead of the bare
``requests.get`` calls used previously.

The session is safe to share across threads: ``Session.get`` is thread-safe and the
underlying connection pool is sized to the worker count, so the Phase 1d thread pool
can fan out without exhausting the pool. A small global rate limiter caps total
requests/sec to stay within Wikimedia etiquette regardless of worker count.
"""

import os
import threading
import time

import requests
from requests.adapters import HTTPAdapter

try:  # urllib3 ships with requests; import path is stable across 1.x/2.x
    from urllib3.util.retry import Retry
except ImportError:  # pragma: no cover - defensive fallback
    from requests.packages.urllib3.util.retry import Retry


def get_user_agent() -> str:
    """Return the configured Wikimedia User-Agent (env first, static fallback)."""
    return (
        os.getenv("WIKIMEDIA_USER_AGENT")
        or "wikipedia-crawler/1.0 (https://github.com/; contact: unknown)"
    )


def get_worker_count() -> int:
    """Number of concurrent fetch workers (``WIKIPEDIA_CRAWLER_WORKERS``, default 8)."""
    try:
        n = int(os.getenv("WIKIPEDIA_CRAWLER_WORKERS", "8"))
    except (TypeError, ValueError):
        n = 8
    return max(1, n)


def _get_max_rps() -> float:
    """Global request/sec cap (``WIKIPEDIA_CRAWLER_MAX_RPS``, default 20; 0 disables)."""
    try:
        return float(os.getenv("WIKIPEDIA_CRAWLER_MAX_RPS", "20"))
    except (TypeError, ValueError):
        return 20.0


_session = None
_session_lock = threading.Lock()


def _build_session() -> requests.Session:
    session = requests.Session()
    # Size the pool to the worker count so concurrent workers reuse connections
    # instead of opening (and discarding) one per request.
    pool = max(10, get_worker_count() * 2)
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool, max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": get_user_agent(),
            # Wikimedia serves gzip when asked; cuts transfer size on large HTML.
            "Accept-Encoding": "gzip",
        }
    )
    return session


def get_session() -> requests.Session:
    """Return the process-wide shared session, building it on first use."""
    global _session
    if _session is None:
        with _session_lock:
            if _session is None:
                _session = _build_session()
    return _session


class _RateLimiter:
    """Thread-safe minimum-interval limiter shared across all workers."""

    def __init__(self, max_per_second: float):
        self._min_interval = 1.0 / max_per_second if max_per_second > 0 else 0.0
        self._lock = threading.Lock()
        self._next_time = 0.0

    def wait(self) -> None:
        if self._min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            if self._next_time <= now:
                self._next_time = now + self._min_interval
                return
            sleep_for = self._next_time - now
            self._next_time += self._min_interval
        time.sleep(sleep_for)


_limiter = _RateLimiter(_get_max_rps())


def rate_limit() -> None:
    """Block just long enough to keep global request rate under the configured cap."""
    _limiter.wait()
