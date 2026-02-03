from __future__ import annotations

import json
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from starlette.responses import Response

from app.core.config import settings

_redis_client: Optional[Redis] = None


def get_redis() -> Optional[Redis]:
    global _redis_client
    if not settings.REDIS_URL:
        return None
    if _redis_client is None:
        _redis_client = Redis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
        )
    return _redis_client


async def close_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.close()
        _redis_client = None


def _seconds_until_expire(
    hour: int,
    minute: int,
    tz_name: str,
) -> int:
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        tz = ZoneInfo("UTC")
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    seconds = int((target - now).total_seconds())
    return max(seconds, 60)


def _build_cache_key(
    namespace: str,
    request: Request,
    current_user: Any = None,
    vary_by_user: bool = True,
) -> str:
    params = sorted(request.query_params.items())
    query = "&".join(f"{k}={v}" for k, v in params)
    base = f"{request.url.path}?{query}" if query else request.url.path
    user_part = ""
    if vary_by_user and current_user is not None:
        user_id = getattr(current_user, "id", None)
        wq_id = getattr(current_user, "wq_id", None)
        if user_id is not None:
            user_part = f":uid:{user_id}"
        elif wq_id:
            user_part = f":wq:{wq_id}"
    return f"{namespace}:{base}{user_part}"


def cache_response(
    namespace: str,
    *,
    vary_by_user: bool = True,
) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            request: Optional[Request] = kwargs.get("request")
            current_user = kwargs.get("current_user")
            redis = get_redis()
            if request is None or redis is None:
                return await func(*args, **kwargs)

            ttl = _seconds_until_expire(
                settings.CACHE_EXPIRE_HOUR,
                settings.CACHE_EXPIRE_MINUTE,
                settings.CACHE_TIMEZONE,
            )
            cache_key = _build_cache_key(namespace, request, current_user, vary_by_user)

            try:
                cached = await redis.get(cache_key)
            except Exception:
                cached = None

            if cached:
                try:
                    return JSONResponse(content=json.loads(cached))
                except Exception:
                    pass

            result = await func(*args, **kwargs)
            try:
                payload: Any = None
                if isinstance(result, Response):
                    if result.body:
                        payload = json.loads(result.body.decode("utf-8"))
                else:
                    payload = jsonable_encoder(result)
                if payload is not None:
                    await redis.set(cache_key, json.dumps(payload, ensure_ascii=False), ex=ttl)
            except Exception:
                # Cache failures should never break responses.
                pass
            return result

        return wrapper

    return decorator
