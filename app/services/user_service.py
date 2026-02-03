from __future__ import annotations

from datetime import timedelta
from typing import Dict, List

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.leaderboard import LeaderboardConsultantUser

__all__ = ["get_user_history", "get_user_statistics"]


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
