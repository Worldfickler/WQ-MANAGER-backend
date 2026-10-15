"""Import the checked-in sticker pack into the configured MySQL and private MinIO.

Run migrations/20261015_add_submit_wait_stickers.sql before this script.
Re-running imports the same content-addressed objects and updates catalog metadata.
"""
import asyncio
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import settings
from app.models.submit_wait import SubmitWaitSticker
from app.services.minio_storage_service import _ensure_bucket, _get_client, _put_object, validate_submit_wait_image


async def main():
    directory = ROOT / "assets" / "submit-wait" / "stickers"
    entries = json.loads((directory / "catalog.json").read_text(encoding="utf-8"))
    client = _get_client()
    bucket = settings.MINIO_BUCKET or "wqmanager"
    engine = create_async_engine(settings.ASYNC_DATABASE_URL, echo=False)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        await asyncio.to_thread(_ensure_bucket, client, bucket)
        async with sessions() as db:
            for index, entry in enumerate(entries):
                path = (directory / entry["file"]).resolve()
                if path.parent != directory.resolve():
                    raise ValueError("Sticker asset must be inside the pack directory")
                content = path.read_bytes()
                suffix = validate_submit_wait_image(content, "image/jpeg")
                object_name = f"submit-wait/stickers/{hashlib.sha256(content).hexdigest()}{suffix}"
                await asyncio.to_thread(_put_object, client, bucket, object_name, content, "image/jpeg")
                sticker = await db.scalar(select(SubmitWaitSticker).where(SubmitWaitSticker.object_name == object_name))
                if sticker is None:
                    sticker = SubmitWaitSticker(object_name=object_name)
                    db.add(sticker)
                sticker.name = entry["name"]
                sticker.width, sticker.height = entry["width"], entry["height"]
                sticker.sort_order, sticker.active = index, True
            await db.commit()
        print(f"Imported {len(entries)} stickers into MySQL and MinIO.")
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
