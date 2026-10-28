import contextlib

import redis

from app.config import get_settings

_client: redis.Redis | None = None


def _redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis.from_url(get_settings().redis_url)
    return _client


def hit(key: str, limit: int, window_s: int) -> bool:
    """Fixed-window counter. Returns True if the call is allowed.

    Fails open if Redis is unavailable so a cache outage cannot lock every user out;
    the failure is visible in /readyz.
    """
    try:
        r = _redis()
        pipe = r.pipeline()
        pipe.incr(f"rl:{key}")
        pipe.expire(f"rl:{key}", window_s, nx=True)
        count, _ = pipe.execute()
        return int(count) <= limit
    except redis.RedisError:
        return True


def reset(key: str) -> None:
    with contextlib.suppress(redis.RedisError):
        _redis().delete(f"rl:{key}")


def ping() -> bool:
    try:
        return bool(_redis().ping())
    except redis.RedisError:
        return False
