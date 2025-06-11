"""Shared asynchronous rate-limiter utility.

This module provides the `RateLimiter` class that can be reused by any part
of the code-base that needs to enforce a maximum number of calls per second.
It centralises the logic that previously lived in both `geolocation.llm` and
`geolocation.geocoding`, thereby removing duplication.
"""

from __future__ import annotations

import asyncio
from typing import Final

from innpulsa.logging import configure_logger

logger = configure_logger("innpulsa.utils.rate_limiter")


class RateLimiter:  # pylint: disable=too-few-public-methods
    """Simple *async* rate-limiter based on a minimum interval between calls.
    """

    _GLOBAL_ACTIVE: Final[str] = "active_batches"

    active_batches: int = 0  # Class-level counter for concurrent calls

    def __init__(self, calls_per_second: float = 1.0):
        if calls_per_second <= 0:
            raise ValueError("`calls_per_second` must be > 0")

        self.min_interval = 1.0 / calls_per_second
        self.last_call_time = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until the next call is allowed under the rate limit."""
        async with self._lock:
            # Track the number of overlapping operations for debugging purposes.
            RateLimiter.active_batches += 1
            logger.debug("active batches: %d", RateLimiter.active_batches)

            now = asyncio.get_event_loop().time()
            time_since_last_call = now - self.last_call_time
            if time_since_last_call < self.min_interval:
                await asyncio.sleep(self.min_interval - time_since_last_call)
            self.last_call_time = asyncio.get_event_loop().time()

    async def release(self) -> None:
        """Mark the completion of an operation previously protected by `acquire`."""
        async with self._lock:
            RateLimiter.active_batches -= 1
            logger.debug("active batches: %d", RateLimiter.active_batches)

    async def __aenter__(self) -> "RateLimiter":  # pylint: disable=unused-argument
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):  # pylint: disable=unused-argument
        await self.release()