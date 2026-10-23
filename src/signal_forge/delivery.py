"""Manual, at-least-once delivery with credential-safe outcomes."""

from collections.abc import Callable
from datetime import UTC, datetime

import httpx

from .models import DispatchResult
from .storage import Store


def send_webhook(url: str, payload: str, delivery_id: str) -> int:
    with httpx.Client(timeout=10, follow_redirects=False, trust_env=False) as client:
        with client.stream(
            "POST",
            url,
            content=payload,
            headers={"Content-Type": "application/json", "Idempotency-Key": delivery_id},
        ) as response:
            return response.status_code


def dispatch(
    store: Store,
    url: str,
    limit: int,
    sender: Callable[[str, str, str], int] = send_webhook,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> DispatchResult:
    result = DispatchResult()
    seen: list[str] = []
    for _ in range(limit):
        claim = store.claim(clock(), seen)
        if claim is None:
            break
        seen.append(claim["id"])
        try:
            code = sender(url, claim["payload"], claim["id"])
        except Exception:
            # Exception text may contain the URL, credentials, or receiver content.
            code = None
        outcome = store.finish(claim, clock(), code)
        result.attempted += 1
        setattr(result, outcome, getattr(result, outcome) + 1)
    return result
