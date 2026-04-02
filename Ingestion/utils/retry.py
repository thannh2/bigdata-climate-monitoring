from __future__ import annotations

import time
from typing import Callable, TypeVar


T = TypeVar("T")


def retry_call(
    func: Callable[[], T],
    retries: int = 3,
    base_delay_seconds: float = 1.0,
    retryable_exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    last_error: BaseException | None = None
    for attempt in range(1, retries + 1):
        try:
            return func()
        except retryable_exceptions as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(base_delay_seconds * (2 ** (attempt - 1)))
    assert last_error is not None
    raise last_error
