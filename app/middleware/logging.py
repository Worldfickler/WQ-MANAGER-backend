import asyncio
import json
import time
from typing import Callable, Optional

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from app.core.database import get_session
from app.core.security import decode_access_token
from app.models.request_log import RequestLog


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Request logging middleware."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()

        method = request.method
        path = request.url.path
        query_params = str(request.query_params) if request.query_params else None
        client_ip = request.client.host if request.client else None
        user_agent = request.headers.get("user-agent")

        body: Optional[str] = None
        if method in {"POST", "PUT", "PATCH"}:
            try:
                body_bytes = await request.body()
                if body_bytes:
                    body_str = body_bytes.decode("utf-8")[:1000]
                    try:
                        body_json = json.loads(body_str)
                        for field in ("password", "token"):
                            if field in body_json:
                                body_json[field] = "***"
                        body = json.dumps(body_json, ensure_ascii=False)
                    except Exception:
                        body = body_str
            except Exception:
                body = None

        wq_id, user_id = self._parse_auth(request)

        response = await call_next(request)

        process_time = (time.time() - start_time) * 1000
        asyncio.create_task(
            self._log_request(
                method=method,
                path=path,
                query_params=query_params,
                body=body,
                status_code=response.status_code,
                response_time=process_time,
                client_ip=client_ip,
                user_agent=user_agent,
                wq_id=wq_id,
                user_id=user_id,
            )
        )

        return response

    def _parse_auth(self, request: Request) -> tuple[Optional[str], Optional[int]]:
        wq_id = None
        user_id = None
        try:
            auth_header = request.headers.get("Authorization")
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header.split(" ", 1)[1]
                payload = decode_access_token(token)
                if payload:
                    wq_id = payload.get("sub")
                    user_id = payload.get("user_id")
        except Exception:
            return None, None
        return wq_id, user_id

    async def _log_request(
        self,
        method: str,
        path: str,
        query_params: Optional[str],
        body: Optional[str],
        status_code: int,
        response_time: float,
        client_ip: Optional[str],
        user_agent: Optional[str],
        wq_id: Optional[str],
        user_id: Optional[int],
    ) -> None:
        try:
            async with get_session() as session:
                log_entry = RequestLog(
                    method=method,
                    path=path,
                    query_params=query_params,
                    body=body,
                    status_code=status_code,
                    response_time=response_time,
                    ip_address=client_ip,
                    user_agent=user_agent,
                    wq_id=wq_id,
                    user_id=user_id,
                )
                session.add(log_entry)
                await session.commit()
        except Exception:
            # Logging should never block a response.
            return
