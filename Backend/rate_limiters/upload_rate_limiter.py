from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import redis


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    wait_seconds: int = 0


# Limits health data CSV uploads to 10 requests per minute for a user
class RedisUploadRateLimiter:
    def __init__(self, redis_url: str):
        # Higher throughput for raw-sample mirroring (initial backfills can require many chunks).
        self.max_requests = 60
        self.window_seconds = 60
        self._client = redis.from_url(redis_url, decode_responses=True)

    def check(self, user_id: str) -> RateLimitDecision:
        key = f"upload_rate:{user_id}"
        now_ts = datetime.now(timezone.utc).timestamp()  # get the current timestamp in seconds in UTC
        min_ts = now_ts - self.window_seconds            # compute the start of the sliding window; anything with a smaller timestamp is outside the window (i.e. older than 60 seconds)

        try:
            pipe = self._client.pipeline(transaction=True)  # create a Redis pipeline so multiple commands can run together
            pipe.zremrangebyscore(key, 0, min_ts)           # delete old request timestamps (older than the last 60 sec window)
            pipe.zcard(key)                                 # count the number of requests in the window
            pipe.zrange(key, 0, 0, withscores=True)         # get the oldest request in timestamp (used to compute how long until the user is allowed again if they're over the limit)
            pipe.expire(key, self.window_seconds + 5)       # set a 5 second buffer to ensure the window is expired before the next request
            _, count, oldest, _ = pipe.execute()            # execute the pipeline and get results

            # If number of requests in window >= max requests per minute, then user is over the limit and we dismiss it
            if int(count) >= self.max_requests:
                oldest_ts = None
                if oldest and isinstance(oldest, list):
                    oldest_ts = float(oldest[0][1])
                if oldest_ts is None:
                    return RateLimitDecision(allowed=False, wait_seconds=self.window_seconds)
                wait_s = max(0, int((oldest_ts + self.window_seconds) - now_ts))
                return RateLimitDecision(allowed=False, wait_seconds=wait_s)

            # If the user under the limit, add the current request
            self._client.zadd(key, {str(now_ts): now_ts})
            self._client.expire(key, self.window_seconds + 5)
            return RateLimitDecision(allowed=True, wait_seconds=0)
        # If Redis is down/unreachable, do not block uploads and allow the upload
        except Exception:
            return RateLimitDecision(allowed=True, wait_seconds=0)


_singleton: Optional[RedisUploadRateLimiter] = None


def get_upload_rate_limiter() -> RedisUploadRateLimiter:
    global _singleton
    if _singleton is not None:
        return _singleton

    redis_url = os.getenv("REDIS_URL") or os.getenv("CELERY_BROKER_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL (or CELERY_BROKER_URL) must be set for upload rate limiting.")
    _singleton = RedisUploadRateLimiter(redis_url=redis_url)
    return _singleton


