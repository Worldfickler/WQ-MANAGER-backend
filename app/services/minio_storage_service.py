from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import List, Tuple
from urllib.parse import quote, urlparse
from uuid import uuid4

from fastapi import HTTPException, UploadFile
from minio import Minio
from minio.error import S3Error

from app.core.config import settings

MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10MB


class MinIOConfigError(RuntimeError):
    pass


@dataclass
class UploadedImage:
    file_name: str
    object_name: str
    content_type: str
    size: int
    url: str


def _resolve_endpoint_and_secure() -> Tuple[str, bool]:
    raw_endpoint = (settings.MINIO_ENDPOINT or "").strip()
    if not raw_endpoint:
        raise MinIOConfigError("MINIO_ENDPOINT 未配置")

    if raw_endpoint.startswith("http://") or raw_endpoint.startswith("https://"):
        parsed = urlparse(raw_endpoint)
        if not parsed.netloc:
            raise MinIOConfigError("MINIO_ENDPOINT 格式错误")
        return parsed.netloc, parsed.scheme == "https"

    return raw_endpoint, bool(settings.MINIO_SECURE)


def _build_public_url(bucket: str, object_name: str) -> str:
    base_url = (settings.MINIO_PUBLIC_BASE_URL or "").strip()
    if not base_url:
        endpoint = (settings.MINIO_ENDPOINT or "").strip().rstrip("/")
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            base_url = endpoint
        else:
            scheme = "https" if settings.MINIO_SECURE else "http"
            base_url = f"{scheme}://{endpoint}"

    return f"{base_url.rstrip('/')}/{quote(bucket)}/{quote(object_name, safe='/')}"


def build_public_url(object_name: str, bucket: str | None = None) -> str:
    target_bucket = (bucket or settings.MINIO_BUCKET or "wqmanager").strip() or "wqmanager"
    normalized = (object_name or "").strip().lstrip("/")
    if not normalized:
        return ""
    return _build_public_url(target_bucket, normalized)


def normalize_object_name(value: str, bucket: str | None = None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""

    target_bucket = (bucket or settings.MINIO_BUCKET or "wqmanager").strip() or "wqmanager"

    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        path = (parsed.path or "").lstrip("/")
        bucket_prefix = f"{target_bucket}/"
        if path.startswith(bucket_prefix):
            return path[len(bucket_prefix):]
        return path

    return raw.lstrip("/")


def _sanitize_filename(file_name: str) -> str:
    path_name = Path(file_name or "image")
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", path_name.stem).strip("._") or "image"
    suffix = re.sub(r"[^A-Za-z0-9.]", "", path_name.suffix)
    return f"{stem}{suffix}"


def _build_object_name(record_date: date, wq_id: str, original_name: str) -> str:
    safe_name = _sanitize_filename(original_name)
    time_prefix = datetime.utcnow().strftime("%H%M%S")
    unique = uuid4().hex[:12]
    safe_user = re.sub(r"[^A-Za-z0-9._-]+", "_", (wq_id or "unknown")).strip("._") or "unknown"
    return f"base-payment/{record_date.isoformat()}/{safe_user}/{time_prefix}_{unique}_{safe_name}"


def _get_client() -> Minio:
    endpoint, secure = _resolve_endpoint_and_secure()

    access_key = (settings.MINIO_ACCESS_KEY or "").strip()
    secret_key = (settings.MINIO_SECRET_KEY or "").strip()
    if not access_key or not secret_key:
        raise MinIOConfigError("MINIO_ACCESS_KEY 或 MINIO_SECRET_KEY 未配置")

    return Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def _put_object(client: Minio, bucket: str, object_name: str, content: bytes, content_type: str) -> None:
    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=BytesIO(content),
        length=len(content),
        content_type=content_type,
    )


def _validate_image(file: UploadFile, content: bytes) -> None:
    content_type = (file.content_type or "").lower()
    if not content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail=f"文件 {file.filename} 不是图片格式")

    if len(content) == 0:
        raise HTTPException(status_code=400, detail=f"文件 {file.filename} 为空")

    if len(content) > MAX_IMAGE_SIZE:
        raise HTTPException(status_code=400, detail=f"文件 {file.filename} 超过 10MB 限制")


async def upload_base_payment_images(files: List[UploadFile], wq_id: str, record_date: date) -> List[UploadedImage]:
    if not files:
        raise HTTPException(status_code=400, detail="请至少上传一张图片")

    bucket = (settings.MINIO_BUCKET or "wqmanager").strip() or "wqmanager"

    try:
        client = _get_client()
        await asyncio.to_thread(_ensure_bucket, client, bucket)
    except MinIOConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except S3Error as exc:
        raise HTTPException(status_code=502, detail=f"MinIO bucket 初始化失败: {exc}") from exc

    uploaded_items: List[UploadedImage] = []

    for upload_file in files:
        content = await upload_file.read()
        _validate_image(upload_file, content)

        object_name = _build_object_name(record_date, wq_id, upload_file.filename or "image")
        content_type = upload_file.content_type or "application/octet-stream"

        try:
            await asyncio.to_thread(
                _put_object,
                client,
                bucket,
                object_name,
                content,
                content_type,
            )
        except S3Error as exc:
            raise HTTPException(status_code=502, detail=f"上传图片到 MinIO 失败: {exc}") from exc
        finally:
            await upload_file.close()

        uploaded_items.append(
            UploadedImage(
                file_name=upload_file.filename or "image",
                object_name=object_name,
                content_type=content_type,
                size=len(content),
                url=_build_public_url(bucket, object_name),
            )
        )

    return uploaded_items


def validate_submit_wait_image(content: bytes, content_type: str) -> str:
    """Only allow raster images; never trust the client MIME type alone."""
    detected = None
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        detected = ("image/png", ".png")
    elif content.startswith(b"\xff\xd8\xff"):
        detected = ("image/jpeg", ".jpg")
    elif content.startswith((b"GIF87a", b"GIF89a")):
        detected = ("image/gif", ".gif")
    elif content.startswith(b"RIFF") and content[8:12] == b"WEBP":
        detected = ("image/webp", ".webp")
    if not content or len(content) > MAX_IMAGE_SIZE:
        raise HTTPException(400, "图片不能为空，且每张不能超过 10MB")
    if detected is None or content_type.lower() != detected[0]:
        raise HTTPException(400, "仅支持内容与格式一致的 PNG、JPEG、GIF、WebP 图片")
    return detected[1]


async def upload_submit_wait_images(files: List[UploadFile]) -> List[UploadedImage]:
    if not 1 <= len(files) <= 9:
        raise HTTPException(400, "每次请上传 1 至 9 张图片")
    bucket = (settings.MINIO_BUCKET or "wqmanager").strip() or "wqmanager"
    client = None
    uploaded: List[UploadedImage] = []
    try:
        client = _get_client()
        await asyncio.to_thread(_ensure_bucket, client, bucket)
        for file in files:
            content = await file.read(MAX_IMAGE_SIZE + 1)
            content_type = (file.content_type or "").lower()
            suffix = validate_submit_wait_image(content, content_type)
            # Do not expose the author's WQ ID or original filename in anonymous posts.
            object_name = f"submit-wait/{datetime.now(timezone.utc).date().isoformat()}/{uuid4().hex}{suffix}"
            await asyncio.to_thread(_put_object, client, bucket, object_name, content, content_type)
            uploaded.append(UploadedImage(file.filename or "image", object_name, content_type,
                                          len(content), _build_public_url(bucket, object_name)))
        return uploaded
    except Exception as exc:
        if client is not None:
            for item in uploaded:
                try:
                    await asyncio.to_thread(client.remove_object, bucket, item.object_name)
                except Exception:
                    pass
        if isinstance(exc, HTTPException):
            raise
        if isinstance(exc, MinIOConfigError):
            raise HTTPException(503, "图片存储尚未配置，请联系管理员配置 MinIO") from exc
        raise HTTPException(502, "图片上传失败，请稍后重试") from exc
    finally:
        for file in files:
            await file.close()


async def open_submit_wait_image(object_name: str):
    try:
        client = _get_client()
        bucket = (settings.MINIO_BUCKET or "wqmanager").strip() or "wqmanager"
        return await asyncio.to_thread(client.get_object, bucket, object_name)
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
            raise HTTPException(404, "图片不存在") from exc
        raise HTTPException(502, "图片读取失败，请稍后重试") from exc
    except MinIOConfigError as exc:
        raise HTTPException(503, "图片存储尚未配置") from exc
    except Exception as exc:
        raise HTTPException(502, "图片读取失败，请稍后重试") from exc
