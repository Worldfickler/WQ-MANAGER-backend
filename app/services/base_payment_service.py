from __future__ import annotations

import json
from datetime import date, datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import and_, asc, case, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.base_payment import BasePayment
from app.models.leaderboard import LeaderboardConsultantUser
from app.services.minio_storage_service import build_public_url, normalize_object_name

__all__ = [
    "get_today_record_date",
    "get_user_record_by_date",
    "get_consultant_metrics_by_date",
    "has_uploaded_on_date",
    "upsert_user_payment_by_date",
    "get_user_today_record",
    "has_uploaded_today",
    "upsert_user_today_payment",
    "get_leaderboard",
    "get_dashboard_summary",
    "serialize_payment_record",
]


def get_today_record_date():
    tz_name = settings.CACHE_TIMEZONE or "Asia/Shanghai"
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        tz = ZoneInfo("UTC")
    return datetime.now(tz).date()


def parse_picture_values(raw_value: Optional[str]) -> list[str]:
    if not raw_value:
        return []

    value = raw_value.strip()
    if not value:
        return []

    try:
        decoded = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        decoded = value

    if isinstance(decoded, list):
        result: list[str] = []
        for item in decoded:
            if isinstance(item, str):
                stripped = item.strip()
                if stripped and stripped not in result:
                    result.append(stripped)
        return result

    if isinstance(decoded, str):
        stripped = decoded.strip()
        return [stripped] if stripped else []

    return []


def normalize_picture_storage(picture: Optional[str], pictures: Optional[list[str]]) -> Optional[str]:
    normalized: list[str] = []

    if pictures is not None:
        for item in pictures:
            if isinstance(item, str):
                stripped = normalize_object_name(item)
                if stripped and stripped not in normalized:
                    normalized.append(stripped)
    elif picture is not None:
        normalized = [
            normalized_item
            for item in parse_picture_values(picture)
            if (normalized_item := normalize_object_name(item))
        ]

    if not normalized:
        return None

    return json.dumps(normalized, ensure_ascii=False)


def serialize_payment_record(record: BasePayment, viewer_wq_id: str) -> Dict:
    is_owner = (record.wq_id or "") == viewer_wq_id
    is_public = int(record.anonymity or 0) == 1
    display_wq_id = record.wq_id if (is_owner or is_public) else "匿名用户"
    regular_payment = float(record.regular_payment) if record.regular_payment is not None else None
    super_payment = float(record.super_payment) if record.super_payment is not None else None
    total_payment = (
        float(record.regular_payment or 0) + float(record.super_payment or 0)
        if (record.regular_payment is not None or record.super_payment is not None)
        else None
    )
    picture_objects = [
        normalized
        for item in parse_picture_values(record.picture)
        if (normalized := normalize_object_name(item))
    ]
    picture_urls = [build_public_url(object_name) for object_name in picture_objects]

    return {
        "record_date": record.record_date.isoformat() if record.record_date else "",
        "wq_id": record.wq_id or "",
        "display_wq_id": display_wq_id or "匿名用户",
        "anonymity": int(record.anonymity or 0),
        "regular_payment": regular_payment,
        "super_payment": super_payment,
        "total_payment": total_payment,
        "regular_count": int(record.regular_count) if record.regular_count is not None else None,
        "super_count": int(record.super_count) if record.super_count is not None else None,
        "picture": picture_objects[0] if picture_objects else None,
        "pictures": picture_objects,
        "picture_urls": picture_urls,
        "value_factor": float(record.value_factor) if record.value_factor is not None else None,
        "daily_osmosis_rank": float(record.daily_osmosis_rank) if record.daily_osmosis_rank is not None else None,
        "is_owner": is_owner,
    }


async def get_user_record_by_date(db: AsyncSession, wq_id: str, target_date: date) -> Optional[BasePayment]:
    result = await db.execute(
        select(BasePayment).where(
            and_(
                BasePayment.delete_flag == False,
                BasePayment.record_date == target_date,
                BasePayment.wq_id == wq_id,
            )
        )
    )
    return result.scalars().first()


async def get_consultant_metrics_by_date(db: AsyncSession, wq_id: str, target_date: date) -> Optional[Dict]:
    normalized_wq_id = (wq_id or "").strip().upper()
    if not normalized_wq_id:
        return None

    result = await db.execute(
        select(LeaderboardConsultantUser)
        .where(
            and_(
                LeaderboardConsultantUser.delete_flag == False,
                LeaderboardConsultantUser.record_date == target_date,
                LeaderboardConsultantUser.user == normalized_wq_id,
            )
        )
        .order_by(LeaderboardConsultantUser.id.desc())
        .limit(1)
    )
    row = result.scalars().first()
    if row is None:
        return None

    return {
        "record_date": target_date.isoformat(),
        "value_factor": float(row.value_factor) if row.value_factor is not None else None,
        "daily_osmosis_rank": float(row.daily_osmosis_rank) if row.daily_osmosis_rank is not None else None,
    }


async def get_user_today_record(db: AsyncSession, wq_id: str) -> Optional[BasePayment]:
    return await get_user_record_by_date(db, wq_id, get_today_record_date())


async def has_uploaded_on_date(db: AsyncSession, wq_id: str, target_date: date) -> bool:
    record = await get_user_record_by_date(db, wq_id, target_date)
    return record is not None


async def has_uploaded_today(db: AsyncSession, wq_id: str) -> bool:
    return await has_uploaded_on_date(db, wq_id, get_today_record_date())


async def upsert_user_payment_by_date(
    db: AsyncSession,
    wq_id: str,
    record_date: date,
    anonymity: int,
    regular_payment: float,
    super_payment: float,
    regular_count: Optional[int],
    super_count: Optional[int],
    picture: Optional[str],
    pictures: Optional[list[str]],
    value_factor: Optional[float],
    daily_osmosis_rank: Optional[float],
):
    existing = await get_user_record_by_date(db, wq_id, record_date)
    picture_payload = normalize_picture_storage(picture=picture, pictures=pictures)

    if existing:
        existing.anonymity = anonymity
        existing.regular_payment = regular_payment
        existing.super_payment = super_payment
        existing.regular_count = regular_count
        existing.super_count = super_count
        existing.picture = picture_payload
        existing.value_factor = value_factor
        existing.daily_osmosis_rank = daily_osmosis_rank
        existing.update_dt = datetime.utcnow()
        await db.flush()
        await db.refresh(existing)
        return existing, False

    record = BasePayment(
        record_date=record_date,
        wq_id=wq_id,
        anonymity=anonymity,
        regular_payment=regular_payment,
        super_payment=super_payment,
        regular_count=regular_count,
        super_count=super_count,
        picture=picture_payload,
        value_factor=value_factor,
        daily_osmosis_rank=daily_osmosis_rank,
        delete_flag=False,
    )
    db.add(record)
    await db.flush()
    await db.refresh(record)
    return record, True


async def upsert_user_today_payment(
    db: AsyncSession,
    wq_id: str,
    anonymity: int,
    regular_payment: float,
    super_payment: float,
    regular_count: Optional[int],
    super_count: Optional[int],
    picture: Optional[str],
    pictures: Optional[list[str]],
    value_factor: Optional[float],
    daily_osmosis_rank: Optional[float],
):
    return await upsert_user_payment_by_date(
        db=db,
        wq_id=wq_id,
        record_date=get_today_record_date(),
        anonymity=anonymity,
        regular_payment=regular_payment,
        super_payment=super_payment,
        regular_count=regular_count,
        super_count=super_count,
        picture=picture,
        pictures=pictures,
        value_factor=value_factor,
        daily_osmosis_rank=daily_osmosis_rank,
    )


async def get_leaderboard(
    db: AsyncSession,
    viewer_wq_id: str,
    page: int,
    page_size: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    sort_by: str = "total_payment",
    sort_order: str = "desc",
) -> Dict:
    today = get_today_record_date()
    effective_start = start_date or today
    effective_end = end_date or today
    if effective_start > effective_end:
        effective_start, effective_end = effective_end, effective_start

    total_payment_expr = func.coalesce(BasePayment.regular_payment, 0) + func.coalesce(BasePayment.super_payment, 0)
    sort_field_map = {
        "total_payment": total_payment_expr,
        "regular_payment": BasePayment.regular_payment,
        "super_payment": BasePayment.super_payment,
        "regular_count": BasePayment.regular_count,
        "super_count": BasePayment.super_count,
        "value_factor": BasePayment.value_factor,
        "daily_osmosis_rank": BasePayment.daily_osmosis_rank,
    }
    sort_column = sort_field_map.get(sort_by, total_payment_expr)

    nulls_last_expr = case((sort_column.is_(None), 1), else_=0)
    primary_sort = asc(sort_column) if sort_order == "asc" else desc(sort_column)

    total_result = await db.execute(
        select(func.count(BasePayment.id)).where(
            and_(
                BasePayment.delete_flag == False,
                BasePayment.record_date >= effective_start,
                BasePayment.record_date <= effective_end,
            )
        )
    )
    total = int(total_result.scalar() or 0)

    offset = (page - 1) * page_size
    rows_result = await db.execute(
        select(BasePayment)
        .where(
            and_(
                BasePayment.delete_flag == False,
                BasePayment.record_date >= effective_start,
                BasePayment.record_date <= effective_end,
            )
        )
        .order_by(
            nulls_last_expr.asc(),
            primary_sort,
            desc(total_payment_expr),
            desc(BasePayment.regular_payment),
            desc(BasePayment.super_payment),
            desc(BasePayment.value_factor),
            desc(BasePayment.daily_osmosis_rank),
            desc(BasePayment.record_date),
            BasePayment.id.asc(),
        )
        .offset(offset)
        .limit(page_size)
    )
    rows = rows_result.scalars().all()

    items = []
    for idx, record in enumerate(rows, start=offset + 1):
        item = serialize_payment_record(record, viewer_wq_id)
        item["rank"] = idx
        items.append(item)

    return {
        "record_date": today.isoformat(),
        "start_date": effective_start.isoformat(),
        "end_date": effective_end.isoformat(),
        "sort_by": sort_by,
        "sort_order": sort_order,
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": items,
    }


async def get_dashboard_summary(
    db: AsyncSession,
    viewer_wq_id: str,
    target_date: date,
) -> Dict:
    conditions = and_(
        BasePayment.delete_flag == False,
        BasePayment.record_date == target_date,
    )

    total_payment_expr = func.coalesce(BasePayment.regular_payment, 0) + func.coalesce(BasePayment.super_payment, 0)
    picture_present_expr = and_(
        BasePayment.picture.is_not(None),
        func.length(func.trim(BasePayment.picture)) > 0,
    )

    aggregate_stmt = select(
        func.count(BasePayment.id).label("participant_count"),
        func.coalesce(func.sum(BasePayment.regular_payment), 0.0).label("regular_payment_sum"),
        func.coalesce(func.sum(BasePayment.super_payment), 0.0).label("super_payment_sum"),
        func.coalesce(func.sum(total_payment_expr), 0.0).label("total_payment_sum"),
        func.avg(total_payment_expr).label("average_total_payment"),
        func.max(total_payment_expr).label("max_total_payment"),
        func.min(total_payment_expr).label("min_total_payment"),
        func.coalesce(func.sum(case((total_payment_expr > 0, 1), else_=0)), 0).label("positive_count"),
        func.coalesce(func.sum(case((total_payment_expr < 0, 1), else_=0)), 0).label("negative_count"),
        func.coalesce(func.sum(case((total_payment_expr == 0, 1), else_=0)), 0).label("flat_count"),
        func.coalesce(func.sum(case((BasePayment.anonymity == 0, 1), else_=0)), 0).label("anonymity_count"),
        func.coalesce(func.sum(case((picture_present_expr, 1), else_=0)), 0).label("picture_count"),
        func.avg(BasePayment.regular_count).label("average_regular_count"),
        func.avg(BasePayment.super_count).label("average_super_count"),
        func.avg(BasePayment.value_factor).label("average_value_factor"),
        func.avg(BasePayment.daily_osmosis_rank).label("average_daily_osmosis_rank"),
    ).where(conditions)

    aggregate_row = (await db.execute(aggregate_stmt)).one()

    participant_count = int(aggregate_row.participant_count or 0)
    regular_payment_sum = float(aggregate_row.regular_payment_sum or 0.0)
    super_payment_sum = float(aggregate_row.super_payment_sum or 0.0)
    total_payment_sum = float(aggregate_row.total_payment_sum or 0.0)

    regular_magnitude = abs(regular_payment_sum)
    super_magnitude = abs(super_payment_sum)
    magnitude_total = regular_magnitude + super_magnitude
    regular_share_pct = (regular_magnitude / magnitude_total * 100) if magnitude_total else 0.0
    super_share_pct = (super_magnitude / magnitude_total * 100) if magnitude_total else 0.0

    positive_count = int(aggregate_row.positive_count or 0)
    negative_count = int(aggregate_row.negative_count or 0)
    flat_count = int(aggregate_row.flat_count or 0)
    anonymity_count = int(aggregate_row.anonymity_count or 0)
    picture_count = int(aggregate_row.picture_count or 0)

    top_rows_stmt = (
        select(BasePayment)
        .where(conditions)
        .order_by(
            desc(total_payment_expr),
            desc(BasePayment.regular_payment),
            desc(BasePayment.super_payment),
            desc(BasePayment.value_factor),
            desc(BasePayment.daily_osmosis_rank),
            BasePayment.id.asc(),
        )
        .limit(3)
    )
    top_rows = (await db.execute(top_rows_stmt)).scalars().all()

    top_performers = []
    for idx, record in enumerate(top_rows, start=1):
        item = serialize_payment_record(record, viewer_wq_id)
        top_performers.append(
            {
                "rank": idx,
                "display_wq_id": item["display_wq_id"],
                "total_payment": item["total_payment"],
                "regular_payment": item["regular_payment"],
                "super_payment": item["super_payment"],
                "value_factor": item["value_factor"],
                "daily_osmosis_rank": item["daily_osmosis_rank"],
            }
        )

    return {
        "record_date": target_date.isoformat(),
        "overview": {
            "participant_count": participant_count,
            "total_payment_sum": total_payment_sum,
            "regular_payment_sum": regular_payment_sum,
            "super_payment_sum": super_payment_sum,
            "regular_share_pct": regular_share_pct,
            "super_share_pct": super_share_pct,
            "average_total_payment": float(aggregate_row.average_total_payment) if aggregate_row.average_total_payment is not None else None,
            "max_total_payment": float(aggregate_row.max_total_payment) if aggregate_row.max_total_payment is not None else None,
            "min_total_payment": float(aggregate_row.min_total_payment) if aggregate_row.min_total_payment is not None else None,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "flat_count": flat_count,
            "positive_rate_pct": (positive_count / participant_count * 100) if participant_count else 0.0,
            "anonymity_count": anonymity_count,
            "anonymity_rate_pct": (anonymity_count / participant_count * 100) if participant_count else 0.0,
            "picture_count": picture_count,
            "picture_rate_pct": (picture_count / participant_count * 100) if participant_count else 0.0,
            "average_regular_count": float(aggregate_row.average_regular_count) if aggregate_row.average_regular_count is not None else None,
            "average_super_count": float(aggregate_row.average_super_count) if aggregate_row.average_super_count is not None else None,
            "average_value_factor": float(aggregate_row.average_value_factor) if aggregate_row.average_value_factor is not None else None,
            "average_daily_osmosis_rank": float(aggregate_row.average_daily_osmosis_rank) if aggregate_row.average_daily_osmosis_rank is not None else None,
        },
        "top_performers": top_performers,
    }
