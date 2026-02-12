from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, List

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.leaderboard import (
    EventUpdateRecord,
    LeaderboardConsultantUser,
    LeaderboardGeniusUser,
)

__all__ = [
    "get_user_history",
    "get_user_statistics",
    "get_user_combined_history_map",
    "get_user_metric_trends_by_event",
]


async def get_user_history(
    db: AsyncSession,
    wq_id: str,
    limit_days: int = 30,
) -> List[LeaderboardConsultantUser]:
    """Fetch user history for the recent period."""
    latest_date_result = await db.execute(
        select(LeaderboardConsultantUser.record_date)
        .where(LeaderboardConsultantUser.delete_flag == False)
        .order_by(desc(LeaderboardConsultantUser.record_date))
        .limit(1)
    )
    latest_date = latest_date_result.scalar_one_or_none()
    if not latest_date:
        return []

    start_date = latest_date - timedelta(days=limit_days - 1)
    history_result = await db.execute(
        select(LeaderboardConsultantUser)
        .where(
            LeaderboardConsultantUser.delete_flag == False,
            LeaderboardConsultantUser.user == wq_id,
            LeaderboardConsultantUser.record_date >= start_date,
            LeaderboardConsultantUser.record_date <= latest_date,
        )
        .order_by(LeaderboardConsultantUser.record_date.asc())
    )
    return history_result.scalars().all()


async def get_user_statistics(db: AsyncSession, wq_id: str) -> Dict:
    """Compute summary statistics for a user."""
    latest_data_result = await db.execute(
        select(LeaderboardConsultantUser)
        .where(
            LeaderboardConsultantUser.delete_flag == False,
            LeaderboardConsultantUser.user == wq_id,
        )
        .order_by(desc(LeaderboardConsultantUser.record_date))
        .limit(1)
    )
    latest_data = latest_data_result.scalars().first()
    if not latest_data:
        return {}

    all_data_result = await db.execute(
        select(LeaderboardConsultantUser)
        .where(
            LeaderboardConsultantUser.delete_flag == False,
            LeaderboardConsultantUser.user == wq_id,
        )
        .order_by(LeaderboardConsultantUser.record_date.asc())
    )
    all_data = all_data_result.scalars().all()
    if not all_data:
        return {}

    max_weight = max(
        [d.weight_factor for d in all_data if d.weight_factor is not None],
        default=0,
    )
    total_submissions = (latest_data.submissions_count or 0) + (
        latest_data.super_alpha_submissions_count or 0
    )

    max_daily_change = 0
    max_change_date = None
    for i in range(1, len(all_data)):
        prev_weight = all_data[i - 1].weight_factor
        curr_weight = all_data[i].weight_factor
        if prev_weight is not None and curr_weight is not None:
            daily_change = abs(curr_weight - prev_weight)
            if daily_change > max_daily_change:
                max_daily_change = daily_change
                max_change_date = all_data[i].record_date

    record_days = len(all_data)
    daily_change = 0
    if len(all_data) >= 2:
        latest_weight = all_data[-1].weight_factor
        prev_weight = all_data[-2].weight_factor
        if latest_weight is not None and prev_weight is not None:
            daily_change = latest_weight - prev_weight

    return {
        "current_weight": latest_data.weight_factor or 0,
        "current_value": latest_data.value_factor or 0,
        "current_submissions": total_submissions,
        "max_weight": max_weight,
        "max_daily_change": round(max_daily_change, 2),
        "max_change_date": max_change_date.isoformat() if max_change_date else None,
        "total_submissions": total_submissions,
        "record_days": record_days,
        "daily_change": round(daily_change, 2),
        "university": latest_data.university,
        "country": latest_data.country,
        "latest_date": latest_data.record_date.isoformat() if latest_data.record_date else None,
    }


async def get_user_combined_history_map(
    db: AsyncSession,
    wq_id: str,
    start_date: date,
    end_date: date,
) -> Dict[date, Dict[str, float | None]]:
    """Fetch daily combined metrics for a user and return a date-indexed map."""
    rows_result = await db.execute(
        select(
            LeaderboardGeniusUser.record_date.label("record_date"),
            func.max(LeaderboardGeniusUser.combined_alpha_performance).label(
                "combined_alpha_performance"
            ),
            func.max(LeaderboardGeniusUser.combined_power_pool_alpha_performance).label(
                "combined_power_pool_alpha_performance"
            ),
            func.max(LeaderboardGeniusUser.combined_selected_alpha_performance).label(
                "combined_selected_alpha_performance"
            ),
        )
        .where(
            LeaderboardGeniusUser.delete_flag == False,
            LeaderboardGeniusUser.user == wq_id,
            LeaderboardGeniusUser.record_date >= start_date,
            LeaderboardGeniusUser.record_date <= end_date,
        )
        .group_by(LeaderboardGeniusUser.record_date)
        .order_by(LeaderboardGeniusUser.record_date.asc())
    )
    rows = rows_result.all()

    return {
        row.record_date: {
            "combined_alpha_performance": float(row.combined_alpha_performance)
            if row.combined_alpha_performance is not None
            else None,
            "combined_power_pool_alpha_performance": float(
                row.combined_power_pool_alpha_performance
            )
            if row.combined_power_pool_alpha_performance is not None
            else None,
            "combined_selected_alpha_performance": float(
                row.combined_selected_alpha_performance
            )
            if row.combined_selected_alpha_performance is not None
            else None,
        }
        for row in rows
    }


async def get_user_metric_trends_by_event(
    db: AsyncSession,
    wq_id: str,
    start_date: date,
    end_date: date,
) -> Dict[str, List[Dict]]:
    """Build value factor / combined trends based on event_update_record."""
    events_result = await db.execute(
        select(
            EventUpdateRecord.id,
            EventUpdateRecord.update_content,
            EventUpdateRecord.update_date,
            EventUpdateRecord.date_range,
        )
        .where(
            EventUpdateRecord.update_date.isnot(None),
            EventUpdateRecord.update_date >= start_date,
            EventUpdateRecord.update_date <= end_date,
            func.lower(EventUpdateRecord.update_content).in_(["value_factor", "combined"]),
        )
        .order_by(EventUpdateRecord.update_date.asc(), EventUpdateRecord.id.asc())
    )
    event_rows = events_result.all()

    value_events_by_date: Dict[date, Dict] = {}
    combined_events_by_date: Dict[date, Dict] = {}

    for row in event_rows:
        event_payload = {
            "update_date": row.update_date,
            "date_range": row.date_range,
        }
        content = (row.update_content or "").lower()
        if content == "value_factor":
            value_events_by_date[row.update_date] = event_payload
        elif content == "combined":
            combined_events_by_date[row.update_date] = event_payload

    value_event_dates = sorted(value_events_by_date.keys())
    combined_event_dates = sorted(combined_events_by_date.keys())

    value_map: Dict[date, float | None] = {}
    if value_event_dates:
        value_rows_result = await db.execute(
            select(
                LeaderboardConsultantUser.record_date.label("record_date"),
                func.max(LeaderboardConsultantUser.value_factor).label("value_factor"),
            )
            .where(
                LeaderboardConsultantUser.delete_flag == False,
                LeaderboardConsultantUser.user == wq_id,
                LeaderboardConsultantUser.record_date.in_(value_event_dates),
            )
            .group_by(LeaderboardConsultantUser.record_date)
        )
        value_map = {
            row.record_date: float(row.value_factor) if row.value_factor is not None else None
            for row in value_rows_result.all()
        }

    combined_map: Dict[date, Dict[str, float | None]] = {}
    if combined_event_dates:
        combined_rows_result = await db.execute(
            select(
                LeaderboardGeniusUser.record_date.label("record_date"),
                func.max(LeaderboardGeniusUser.combined_alpha_performance).label("combined_alpha_performance"),
                func.max(LeaderboardGeniusUser.combined_power_pool_alpha_performance).label(
                    "combined_power_pool_alpha_performance"
                ),
                func.max(LeaderboardGeniusUser.combined_selected_alpha_performance).label(
                    "combined_selected_alpha_performance"
                ),
            )
            .where(
                LeaderboardGeniusUser.delete_flag == False,
                LeaderboardGeniusUser.user == wq_id,
                LeaderboardGeniusUser.record_date.in_(combined_event_dates),
            )
            .group_by(LeaderboardGeniusUser.record_date)
        )

        combined_map = {
            row.record_date: {
                "combined_alpha_performance": float(row.combined_alpha_performance)
                if row.combined_alpha_performance is not None
                else None,
                "combined_power_pool_alpha_performance": float(row.combined_power_pool_alpha_performance)
                if row.combined_power_pool_alpha_performance is not None
                else None,
                "combined_selected_alpha_performance": float(row.combined_selected_alpha_performance)
                if row.combined_selected_alpha_performance is not None
                else None,
            }
            for row in combined_rows_result.all()
        }

    value_factor_trend = [
        {
            "update_date": event["update_date"].isoformat(),
            "date_range": event["date_range"] or event["update_date"].isoformat(),
            "value_factor": value_map.get(event["update_date"]),
        }
        for event in [value_events_by_date[d] for d in value_event_dates]
    ]

    combined_trend = [
        {
            "update_date": event["update_date"].isoformat(),
            "date_range": event["date_range"] or event["update_date"].isoformat(),
            "combined_alpha_performance": combined_map.get(event["update_date"], {}).get(
                "combined_alpha_performance"
            ),
            "combined_power_pool_alpha_performance": combined_map.get(event["update_date"], {}).get(
                "combined_power_pool_alpha_performance"
            ),
            "combined_selected_alpha_performance": combined_map.get(event["update_date"], {}).get(
                "combined_selected_alpha_performance"
            ),
        }
        for event in [combined_events_by_date[d] for d in combined_event_dates]
    ]

    return {
        "value_factor_trend": value_factor_trend,
        "combined_trend": combined_trend,
    }
