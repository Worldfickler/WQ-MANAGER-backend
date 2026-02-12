from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc, text, and_
from app.models.leaderboard import (
    LeaderboardConsultantCountryOrRegion,
    LeaderboardConsultantUser,
    LeaderboardGeniusCountryOrRegion,
    LeaderboardGeniusUser,
    EventUpdateRecord,
)
from typing import List, Dict
from datetime import date

__all__ = [
    "get_country_weight_time_series",
    "get_country_submission_time_series",
    "get_genius_country_time_series",
    "get_genius_weight_sum_time_series",
    "get_genius_user_weight_changes",
    "get_genius_level_weight_changes",
    "get_user_weight_time_series",
    "get_genius_available_countries",
    "get_genius_available_levels",
    "get_available_countries",
    "get_country_leaderboard",
    "get_user_leaderboard",
    "get_summary_statistics",
    "get_value_factor_analysis",
    "get_value_factor_user_changes",
    "get_combined_analysis",
    "get_combined_user_changes",
    "get_user_metric_trends_by_event",
]

VALUE_FACTOR_BASE_DATE = date(2026, 2, 10)
VALUE_FACTOR_TARGET_DATE = date(2026, 2, 11)
COMBINED_BASE_DATE = date(2026, 2, 10)
COMBINED_TARGET_DATE = date(2026, 2, 11)


def get_country_weight_time_series(db: Session, countries: List[str] = None, limit_days: int = 30) -> Dict:
    """
    Get weight_factor time series data for specified countries

    Args:
        db: Database session
        countries: List of country codes (if None, get all countries)
        limit_days: Number of recent days to fetch (default 30)

    Returns:
        Dictionary with country data organized by country code
    """
    # If no countries specified, get all available countries
    if not countries:
        all_countries = db.query(
            LeaderboardConsultantCountryOrRegion.country
        ).filter(
            LeaderboardConsultantCountryOrRegion.delete_flag == False
        ).distinct().all()

        countries = [country[0] for country in all_countries if country[0]]

    # Get the most recent date
    latest_date_result = db.query(
        func.max(LeaderboardConsultantCountryOrRegion.record_date)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False
    ).scalar()

    if not latest_date_result:
        return {}

    # Calculate the start date based on limit_days
    from datetime import timedelta
    start_date = latest_date_result - timedelta(days=limit_days - 1)

    # Query data for specified countries within the date range
    query = db.query(LeaderboardConsultantCountryOrRegion).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.country.in_(countries),
        LeaderboardConsultantCountryOrRegion.record_date >= start_date,
        LeaderboardConsultantCountryOrRegion.record_date <= latest_date_result
    ).order_by(LeaderboardConsultantCountryOrRegion.record_date.asc())

    results = query.all()

    # Organize data by country
    country_data = {}
    for record in results:
        if record.country not in country_data:
            country_data[record.country] = {
                'dates': [],
                'weights': []
            }
        country_data[record.country]['dates'].append(record.record_date.isoformat())
        country_data[record.country]['weights'].append(record.weight_factor or 0.0)

    return country_data


def get_country_submission_time_series(
    db: Session,
    countries: List[str] = None,
    limit_days: int = 30,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Dict:
    """
    Get submission count time series data for specified countries

    Args:
        db: Database session
        countries: List of country codes (if None, get all countries)
        limit_days: Number of recent days to fetch (default 30)

    Returns:
        Dictionary with country data organized by country code
        {
            'country_code': {
                'dates': ['2024-01-01', '2024-01-02', ...],
                'submissions_count': [100, 102, ...],
                'super_alpha_submissions_count': [10, 12, ...],
                'submissions_change': [0, 2, -1, ...],  # 每日变化量
                'super_alpha_submissions_change': [0, 1, 0, ...]
            }
        }
    """
    # If no countries specified, get all available countries
    if not countries:
        all_countries = db.query(
            LeaderboardConsultantCountryOrRegion.country
        ).filter(
            LeaderboardConsultantCountryOrRegion.delete_flag == False
        ).distinct().all()

        countries = [country[0] for country in all_countries if country[0]]

    # Get the most recent date
    latest_date_result = db.query(
        func.max(LeaderboardConsultantCountryOrRegion.record_date)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False
    ).scalar()

    if not latest_date_result:
        return {}

    from datetime import datetime, timedelta

    start: date | None = None
    end: date | None = None
    if start_date:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
    if end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if end is None:
        end = latest_date_result
    if start is None:
        start = end - timedelta(days=limit_days - 1)

    if start > end:
        start, end = end, start

    # Query data for specified countries within the date range
    query = db.query(LeaderboardConsultantCountryOrRegion).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.country.in_(countries),
        LeaderboardConsultantCountryOrRegion.record_date >= start,
        LeaderboardConsultantCountryOrRegion.record_date <= end
    ).order_by(
        LeaderboardConsultantCountryOrRegion.country.asc(),
        LeaderboardConsultantCountryOrRegion.record_date.asc()
    )

    results = query.all()

    # Organize data by country
    country_data: Dict[str, Dict[str, List]] = {}
    for record in results:
        if record.country not in country_data:
            country_data[record.country] = {
                'dates': [],
                'submissions_count': [],
                'super_alpha_submissions_count': [],
                'submissions_change': [],
                'super_alpha_submissions_change': []
            }
        country_data[record.country]['dates'].append(record.record_date.isoformat())
        country_data[record.country]['submissions_count'].append(record.submissions_count or 0)
        country_data[record.country]['super_alpha_submissions_count'].append(record.super_alpha_submissions_count or 0)

    # 计算变化量（每日较前一日的变化）
    for country in country_data:
        submissions = country_data[country]['submissions_count']
        sa_submissions = country_data[country]['super_alpha_submissions_count']

        # 计算RA提交变化量
        for i in range(len(submissions)):
            if i == 0:
                country_data[country]['submissions_change'].append(0)
            else:
                country_data[country]['submissions_change'].append(submissions[i] - submissions[i - 1])

        # 计算SA提交变化量
        for i in range(len(sa_submissions)):
            if i == 0:
                country_data[country]['super_alpha_submissions_change'].append(0)
            else:
                country_data[country]['super_alpha_submissions_change'].append(sa_submissions[i] - sa_submissions[i - 1])

    return country_data


def get_available_countries(db: Session) -> List[str]:
    """Get list of all available countries"""
    countries = db.query(
        LeaderboardConsultantCountryOrRegion.country
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False
    ).distinct().all()

    return [country[0] for country in countries if country[0]]


def get_country_leaderboard(db: Session, limit: int = 10, days: int = 7) -> List[LeaderboardConsultantCountryOrRegion]:
    """
    Get country leaderboard sorted by weight_factor (highest first)

    Args:
        db: Database session
        limit: Maximum number of countries to return (default 10)
        days: Number of days to look back for change calculation (default 7)

    Returns:
        List of country records sorted by weight_factor descending
    """
    from datetime import timedelta

    # Get the most recent date
    latest_date = db.query(
        func.max(LeaderboardConsultantCountryOrRegion.record_date)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False
    ).scalar()

    if not latest_date:
        return []

    # Calculate the start date for change comparison
    start_date = latest_date - timedelta(days=days)

    # Query countries with their weight_factor for the latest date
    # Sort by weight_factor descending
    query = db.query(LeaderboardConsultantCountryOrRegion).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.record_date == latest_date,
        LeaderboardConsultantCountryOrRegion.weight_factor.isnot(None)
    ).order_by(
        LeaderboardConsultantCountryOrRegion.weight_factor.desc()
    ).limit(limit)

    results = query.all()

    # Attach change data to each result
    for country in results:
        # Get historical weight_factor for the exact start_date
        historical_data = db.query(LeaderboardConsultantCountryOrRegion).filter(
            LeaderboardConsultantCountryOrRegion.delete_flag == False,
            LeaderboardConsultantCountryOrRegion.country == country.country,
            LeaderboardConsultantCountryOrRegion.record_date == start_date,
            LeaderboardConsultantCountryOrRegion.weight_factor.isnot(None)
        ).first()

        # Calculate change
        if historical_data and historical_data.weight_factor is not None:
            country.weight_change = country.weight_factor - historical_data.weight_factor
            country.weight_change_percent = ((country.weight_factor - historical_data.weight_factor) / historical_data.weight_factor * 100) if historical_data.weight_factor != 0 else 100.0
        else:
            # No historical data, assume growth from 0
            country.weight_change = country.weight_factor
            country.weight_change_percent = 100.0

    return results


def get_user_leaderboard(
    db: Session,
    limit: int = 6,
    days: int = 7,
    order: str = "desc"
) -> List[LeaderboardConsultantUser]:
    """
    Get user leaderboard sorted by weight_factor change

    Args:
        db: Database session
        limit: Maximum number of users to return (default 6)
        days: Number of days to look back for change calculation (default 7)
        order: Sort order - "desc" for positive change first, "asc" for negative change first (default "desc")

    Returns:
        List of user records sorted by weight_change
    """
    from datetime import timedelta
    from sqlalchemy import case

    # Get the most recent date
    latest_date = db.query(
        func.max(LeaderboardConsultantUser.record_date)
    ).filter(
        LeaderboardConsultantUser.delete_flag == False
    ).scalar()

    if not latest_date:
        return []

    # Calculate the start date for change comparison
    start_date = latest_date - timedelta(days=days)

    # Subquery for current weights (latest date)
    current_subq = db.query(
        LeaderboardConsultantUser.user,
        LeaderboardConsultantUser.weight_factor.label('current_weight'),
        LeaderboardConsultantUser.country,
        LeaderboardConsultantUser.id
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == latest_date,
        LeaderboardConsultantUser.weight_factor.isnot(None)
    ).subquery()

    # Subquery for historical weights (exact start_date)
    historical_subq = db.query(
        LeaderboardConsultantUser.user,
        LeaderboardConsultantUser.weight_factor.label('historical_weight')
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == start_date,
        LeaderboardConsultantUser.weight_factor.isnot(None)
    ).subquery()

    # Calculate weight change in database and sort/limit
    weight_change_expr = func.coalesce(
        current_subq.c.current_weight - historical_subq.c.historical_weight,
        current_subq.c.current_weight
    )

    weight_change_percent_expr = case(
        (historical_subq.c.historical_weight.isnot(None),
         case(
             (historical_subq.c.historical_weight != 0,
              (current_subq.c.current_weight - historical_subq.c.historical_weight) / historical_subq.c.historical_weight * 100),
             else_=100.0  # Historical weight is 0, treat as 100% growth
         )),
        else_=100.0  # No historical data, treat as 100% growth
    )

    # Main query with calculated changes
    query = db.query(
        current_subq.c.id,
        current_subq.c.user,
        current_subq.c.country,
        current_subq.c.current_weight.label('weight_factor'),
        weight_change_expr.label('weight_change'),
        weight_change_percent_expr.label('weight_change_percent')
    ).outerjoin(
        historical_subq,
        current_subq.c.user == historical_subq.c.user
    )

    # Apply sorting
    if order == "desc":
        query = query.order_by(desc(weight_change_expr))
    else:
        query = query.order_by(asc(weight_change_expr))

    # Apply limit and execute
    results = query.limit(limit).all()

    # Convert results to LeaderboardConsultantUser objects
    user_list = []
    for row in results:
        user_obj = LeaderboardConsultantUser()
        user_obj.id = row.id
        user_obj.user = row.user
        user_obj.country = row.country
        user_obj.weight_factor = row.weight_factor
        user_obj.weight_change = row.weight_change
        user_obj.weight_change_percent = row.weight_change_percent
        user_obj.record_date = latest_date
        user_list.append(user_obj)

    return user_list


def get_summary_statistics(db: Session, days: int = 7) -> Dict:
    """
    Get summary statistics for dashboard cards

    Args:
        db: Database session
        days: Number of days to look back for change calculation (default 7)

    Returns:
        Dictionary containing summary statistics
    """
    from datetime import timedelta

    # Get the most recent date
    latest_date = db.query(
        func.max(LeaderboardConsultantCountryOrRegion.record_date)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False
    ).scalar()

    if not latest_date:
        return {
            "total_users": 0,
            "user_change": 0,
            "total_alpha": 0,
            "total_weight": 0,
            "weight_change": 0,
            "total_records": 0,
            "latest_record_date": None,
        }

    # Calculate the start date for change comparison
    start_date = latest_date - timedelta(days=days)

    # 1. Total users and change from genius leaderboard
    current_users = db.query(
        func.sum(LeaderboardConsultantCountryOrRegion.user)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.record_date == latest_date
    ).scalar() or 0

    # Query historical users for the exact start_date
    historical_users = db.query(
        func.sum(LeaderboardConsultantCountryOrRegion.user)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.record_date == start_date
    ).scalar()

    # If no historical data, treat as growth from 0
    historical_users_count = historical_users if historical_users and historical_users > 0 else 0
    user_change = current_users - historical_users_count if historical_users_count > 0 else current_users

    # 2. Total alpha count and change from genius leaderboard
    current_alpha = db.query(
        func.sum(LeaderboardConsultantCountryOrRegion.submissions_count) + func.sum(LeaderboardConsultantCountryOrRegion.super_alpha_submissions_count)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.record_date == latest_date
    ).scalar() or 0

    # Query historical alpha for the exact start_date
    historical_alpha = db.query(
        func.sum(LeaderboardConsultantCountryOrRegion.submissions_count) + func.sum(LeaderboardConsultantCountryOrRegion.super_alpha_submissions_count)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.record_date == start_date
    ).scalar()

    # If no historical data, treat as growth from 0
    historical_alpha_count = historical_alpha if historical_alpha and historical_alpha > 0 else 0
    alpha_change = current_alpha - historical_alpha_count if historical_alpha_count > 0 else current_alpha

    # 3. Total weight and change from consultant leaderboard
    current_weight = db.query(
        func.sum(LeaderboardConsultantCountryOrRegion.weight_factor)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.record_date == latest_date,
        LeaderboardConsultantCountryOrRegion.weight_factor.isnot(None)
    ).scalar() or 0

    # Query historical weight for the exact start_date
    historical_weight = db.query(
        func.sum(LeaderboardConsultantCountryOrRegion.weight_factor)
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.record_date == start_date,
        LeaderboardConsultantCountryOrRegion.weight_factor.isnot(None)
    ).scalar()

    # If no historical data, treat as growth from 0
    historical_weight_count = historical_weight if historical_weight and historical_weight > 0 else 0
    weight_change = current_weight - historical_weight_count if historical_weight_count > 0 else current_weight

    # 4. Total records count from all leaderboard tables
    total_records = 0

    # Count records from all leaderboard tables
    table_queries = [
        "SELECT COUNT(*) FROM leaderboard_genius_country_or_region WHERE delete_flag = 0",
        "SELECT COUNT(*) FROM leaderboard_genius_user WHERE delete_flag = 0",
        "SELECT COUNT(*) FROM leaderboard_consultant_country_or_region WHERE delete_flag = 0",
        "SELECT COUNT(*) FROM leaderboard_consultant_user WHERE delete_flag = 0",
        "SELECT COUNT(*) FROM leaderboard_consultant_university WHERE delete_flag = 0"
    ]

    for query_str in table_queries:
        result = db.execute(text(query_str)).scalar()
        total_records += result or 0

    return {
        "total_users": int(current_users),
        "user_change": int(user_change),
        "total_alpha": int(current_alpha),
        "alpha_change": int(alpha_change),
        "total_weight": round(float(current_weight), 2),
        "weight_change": round(float(weight_change), 2),
        "total_records": int(total_records),
        "latest_record_date": latest_date.isoformat() if latest_date else None,
    }


def get_genius_country_time_series(db: Session, countries: List[str] = None, start_date: str = None, end_date: str = None) -> Dict:
    """
    Get alpha_count_change time series data for specified countries from genius leaderboard
    """
    from datetime import datetime

    if not countries:
        all_countries = db.query(
            LeaderboardGeniusCountryOrRegion.country
        ).filter(
            LeaderboardGeniusCountryOrRegion.delete_flag == False
        ).distinct().all()
        countries = [country[0] for country in all_countries if country[0]]

    # Build date filter
    query = db.query(LeaderboardGeniusCountryOrRegion).filter(
        LeaderboardGeniusCountryOrRegion.delete_flag == False,
        LeaderboardGeniusCountryOrRegion.country.in_(countries)
    )

    if start_date:
        query = query.filter(LeaderboardGeniusCountryOrRegion.record_date >= datetime.strptime(start_date, '%Y-%m-%d').date())
    if end_date:
        query = query.filter(LeaderboardGeniusCountryOrRegion.record_date <= datetime.strptime(end_date, '%Y-%m-%d').date())

    query = query.order_by(
        LeaderboardGeniusCountryOrRegion.country.asc(),
        LeaderboardGeniusCountryOrRegion.record_date.asc()
    )

    results = query.all()

    if not results:
        return {}

    # Organize data by country
    country_data: Dict[str, Dict[str, List]] = {}
    for record in results:
        if record.country not in country_data:
            country_data[record.country] = {
                'dates': [],
                '_alpha_count': [],
                'alpha_count_change': []
            }
        country_data[record.country]['dates'].append(record.record_date.isoformat())
        country_data[record.country]['_alpha_count'].append(record.alpha_count or 0)

    # 计算alpha数量变化量
    for country in country_data:
        alpha_counts = country_data[country]['_alpha_count']
        for i in range(len(alpha_counts)):
            if i == 0:
                country_data[country]['alpha_count_change'].append(0)
            else:
                country_data[country]['alpha_count_change'].append(alpha_counts[i] - alpha_counts[i - 1])
        del country_data[country]['_alpha_count']

    return country_data


def get_genius_available_countries(db: Session) -> List[str]:
    countries: set[str] = set()
    genius_user_countries = db.query(
        LeaderboardGeniusUser.country
    ).filter(
        LeaderboardGeniusUser.delete_flag == False,
        LeaderboardGeniusUser.country.isnot(None)
    ).distinct().all()
    countries.update([country[0] for country in genius_user_countries if country[0]])

    genius_country_countries = db.query(
        LeaderboardGeniusCountryOrRegion.country
    ).filter(
        LeaderboardGeniusCountryOrRegion.delete_flag == False,
        LeaderboardGeniusCountryOrRegion.country.isnot(None)
    ).distinct().all()
    countries.update([country[0] for country in genius_country_countries if country[0]])

    consultant_country_countries = db.query(
        LeaderboardConsultantCountryOrRegion.country
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.country.isnot(None)
    ).distinct().all()
    countries.update([country[0] for country in consultant_country_countries if country[0]])

    return sorted(countries)


def get_genius_available_levels(db: Session) -> List[str]:
    levels = db.query(
        LeaderboardGeniusUser.genius_level
    ).filter(
        LeaderboardGeniusUser.delete_flag == False,
        LeaderboardGeniusUser.genius_level.isnot(None)
    ).distinct().all()

    return [level[0] for level in levels if level[0]]


def _resolve_date_range(db: Session, start_date: str | None, end_date: str | None):
    from datetime import datetime, timedelta

    if start_date:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        start = None

    if end_date:
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end = None

    if start and end:
        return start, end

    latest_date = db.query(
        func.max(LeaderboardGeniusUser.record_date)
    ).filter(
        LeaderboardGeniusUser.delete_flag == False
    ).scalar()

    if not latest_date:
        return None, None

    if end is None:
        end = latest_date

    if start is None:
        start = end - timedelta(days=29)

    return start, end


def get_genius_weight_sum_time_series(
    db: Session,
    genius_levels: List[str] | None = None,
    countries: List[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Dict:
    start, end = _resolve_date_range(db, start_date, end_date)
    if not start or not end:
        return {}

    country_expr = func.coalesce(LeaderboardGeniusUser.country, LeaderboardConsultantUser.country)

    query = db.query(
        LeaderboardGeniusUser.record_date.label("record_date"),
        LeaderboardGeniusUser.genius_level.label("genius_level"),
        country_expr.label("country"),
        func.sum(LeaderboardConsultantUser.weight_factor).label("total_weight"),
    ).join(
        LeaderboardConsultantUser,
        and_(
            LeaderboardConsultantUser.delete_flag == False,
            LeaderboardGeniusUser.user == LeaderboardConsultantUser.user,
            LeaderboardGeniusUser.record_date == LeaderboardConsultantUser.record_date,
        )
    ).filter(
        LeaderboardGeniusUser.delete_flag == False,
        LeaderboardGeniusUser.record_date >= start,
        LeaderboardGeniusUser.record_date <= end,
    )

    if genius_levels:
        query = query.filter(LeaderboardGeniusUser.genius_level.in_(genius_levels))

    if countries:
        query = query.filter(country_expr.in_(countries))

    query = query.group_by(
        LeaderboardGeniusUser.record_date,
        LeaderboardGeniusUser.genius_level,
        country_expr,
    ).order_by(
        LeaderboardGeniusUser.record_date.asc()
    )

    results = query.all()
    if not results:
        return {}

    series_map: Dict[str, Dict[str, Dict[str, List]]] = {}
    for row in results:
        level = row.genius_level or "UNKNOWN"
        country = row.country or "UNKNOWN"
        key = f"{level}|{country}"
        if key not in series_map:
            series_map[key] = {
                "genius_level": level,
                "country": country,
                "dates": [],
                "weights": []
            }
        series_map[key]["dates"].append(row.record_date.isoformat())
        series_map[key]["weights"].append(float(row.total_weight or 0))

    return series_map


def get_genius_user_weight_changes(
    db: Session,
    genius_levels: List[str] | None = None,
    countries: List[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    order: str = "desc",
) -> List[Dict]:
    start, end = _resolve_date_range(db, start_date, end_date)
    if not start or not end:
        return []

    # If only a single day is selected, compare against the previous day.
    baseline_start = start
    if start == end:
        from datetime import timedelta
        baseline_start = start - timedelta(days=1)

    country_expr = func.coalesce(LeaderboardGeniusUser.country, LeaderboardConsultantUser.country)

    query = db.query(
        LeaderboardGeniusUser.user,
        LeaderboardGeniusUser.genius_level,
        country_expr.label("country"),
        LeaderboardGeniusUser.record_date,
        LeaderboardConsultantUser.weight_factor,
    ).join(
        LeaderboardConsultantUser,
        and_(
            LeaderboardConsultantUser.delete_flag == False,
            LeaderboardGeniusUser.user == LeaderboardConsultantUser.user,
            LeaderboardGeniusUser.record_date == LeaderboardConsultantUser.record_date,
        )
    ).filter(
        LeaderboardGeniusUser.delete_flag == False,
        LeaderboardGeniusUser.record_date >= baseline_start,
        LeaderboardGeniusUser.record_date <= end,
    )

    if genius_levels:
        query = query.filter(LeaderboardGeniusUser.genius_level.in_(genius_levels))
    if countries:
        query = query.filter(country_expr.in_(countries))

    query = query.order_by(
        LeaderboardGeniusUser.user.asc(),
        LeaderboardGeniusUser.record_date.asc(),
    )

    rows = query.all()
    if not rows:
        return []

    user_map: Dict[str, Dict] = {}
    for row in rows:
        user = row.user
        if not user:
            continue
        weight = float(row.weight_factor or 0)
        entry = user_map.get(user)
        if not entry:
            user_map[user] = {
                "user": user,
                "genius_level": row.genius_level,
                "country": row.country,
                "start_weight": weight,
                "end_weight": weight,
            }
        else:
            entry["end_weight"] = weight
            if entry.get("genius_level") is None and row.genius_level:
                entry["genius_level"] = row.genius_level
            if entry.get("country") is None and row.country:
                entry["country"] = row.country

    results: List[Dict] = []
    for entry in user_map.values():
        start_weight = entry.get("start_weight", 0)
        end_weight = entry.get("end_weight", 0)
        weight_change = end_weight - start_weight
        weight_change_percent = None
        if start_weight != 0:
            weight_change_percent = (weight_change / start_weight) * 100
        results.append({
            "user": entry["user"],
            "genius_level": entry.get("genius_level"),
            "country": entry.get("country"),
            "start_weight": float(start_weight),
            "end_weight": float(end_weight),
            "weight_change": float(weight_change),
            "weight_change_percent": float(weight_change_percent) if weight_change_percent is not None else None,
        })

    if order == "asc":
        results.sort(key=lambda x: x["weight_change"])
    else:
        results.sort(key=lambda x: x["weight_change"], reverse=True)

    total = len(results)
    for idx, entry in enumerate(results, start=1):
        entry["rank"] = idx
        if total <= 1:
            entry["percentile"] = 100.0
        else:
            entry["percentile"] = round((1 - (idx - 1) / (total - 1)) * 100, 2)

    return results


def get_genius_level_weight_changes(
    db: Session,
    days: int = 7,
) -> List[Dict]:
    from datetime import timedelta
    from sqlalchemy import case

    latest_date = db.query(
        func.max(LeaderboardConsultantUser.record_date)
    ).filter(
        LeaderboardConsultantUser.delete_flag == False
    ).scalar()

    if not latest_date:
        return []

    start_date = latest_date - timedelta(days=days)

    standard_levels = ["GRANDMASTER", "MASTER", "EXPERT", "GOLD"]
    level_users_subq = db.query(
        LeaderboardGeniusUser.user.label("user"),
        LeaderboardGeniusUser.genius_level.label("genius_level"),
    ).filter(
        LeaderboardGeniusUser.delete_flag == False,
        LeaderboardGeniusUser.record_date == latest_date,
        LeaderboardGeniusUser.genius_level.in_(standard_levels),
        LeaderboardGeniusUser.user.isnot(None),
    ).distinct().subquery()

    current_subq = db.query(
        LeaderboardConsultantUser.user.label("user"),
        LeaderboardConsultantUser.weight_factor.label("weight_factor"),
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == latest_date,
    ).subquery()

    historical_subq = db.query(
        LeaderboardConsultantUser.user.label("user"),
        LeaderboardConsultantUser.weight_factor.label("weight_factor"),
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == start_date,
    ).subquery()

    level_expr = level_users_subq.c.genius_level.label("genius_level")

    current_rows = db.query(
        level_expr,
        func.count(func.distinct(level_users_subq.c.user)).label("total_users"),
        func.sum(func.coalesce(current_subq.c.weight_factor, 0)).label("total_weight"),
    ).join(
        level_users_subq,
        current_subq.c.user == level_users_subq.c.user,
    ).group_by(
        level_expr
    ).all()

    historical_rows = db.query(
        level_expr,
        func.sum(func.coalesce(historical_subq.c.weight_factor, 0)).label("total_weight"),
    ).join(
        level_users_subq,
        historical_subq.c.user == level_users_subq.c.user,
    ).group_by(
        level_expr
    ).all()

    current_map = {row.genius_level or "UNKNOWN": float(row.total_weight or 0) for row in current_rows}
    current_user_map = {row.genius_level or "UNKNOWN": int(row.total_users or 0) for row in current_rows}
    historical_map = {row.genius_level or "UNKNOWN": float(row.total_weight or 0) for row in historical_rows}

    levels = sorted(set(current_map.keys()) | set(historical_map.keys()))
    results: List[Dict] = []
    for level in levels:
        current_weight = current_map.get(level, 0.0)
        historical_weight = historical_map.get(level, 0.0)
        total_users = current_user_map.get(level, 0)
        if historical_weight:
            change = current_weight - historical_weight
            change_percent = (change / historical_weight) * 100
        else:
            change = current_weight
            change_percent = 100.0 if current_weight != 0 else 0.0
        results.append({
            "genius_level": level,
            "total_users": total_users,
            "total_weight": round(float(current_weight), 2),
            "weight_change": round(float(change), 2),
            "weight_change_percent": round(float(change_percent), 2),
        })

    results.sort(key=lambda x: x["total_weight"], reverse=True)
    return results


def get_user_weight_time_series(
    db: Session,
    user: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Dict:
    from datetime import datetime, timedelta

    normalized = user.strip().upper() if user else ""
    if not normalized:
        return {"user": "", "dates": [], "weights": []}

    start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    if start is None or end is None:
        latest_date = db.query(
            func.max(LeaderboardConsultantUser.record_date)
        ).filter(
            LeaderboardConsultantUser.delete_flag == False,
            LeaderboardConsultantUser.user == normalized,
        ).scalar()
        if not latest_date:
            return {"user": normalized, "dates": [], "weights": []}

        if end is None:
            end = latest_date
        if start is None:
            start = end - timedelta(days=29)

    query = db.query(
        LeaderboardConsultantUser.record_date,
        LeaderboardConsultantUser.weight_factor,
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.user == normalized,
        LeaderboardConsultantUser.record_date >= start,
        LeaderboardConsultantUser.record_date <= end,
    ).order_by(
        LeaderboardConsultantUser.record_date.asc()
    )

    results = query.all()
    dates = [row.record_date.isoformat() for row in results]
    weights = [float(row.weight_factor or 0) for row in results]

    return {
        "user": normalized,
        "dates": dates,
        "weights": weights,
    }


def _median(values: List[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 0:
        return (sorted_values[mid - 1] + sorted_values[mid]) / 2
    return sorted_values[mid]


def _build_distribution(values: List[float], bins: int = 10) -> Dict[str, List]:
    if not values:
        return {"labels": [], "counts": []}

    min_value = min(values)
    max_value = max(values)
    if min_value == max_value:
        return {
            "labels": [f"{min_value:.4f}"],
            "counts": [len(values)],
        }

    step = (max_value - min_value) / bins
    counts = [0 for _ in range(bins)]
    labels: List[str] = []
    for i in range(bins):
        start = min_value + step * i
        end = max_value if i == bins - 1 else min_value + step * (i + 1)
        labels.append(f"{start:.4f}~{end:.4f}")

    for value in values:
        index = min(int((value - min_value) / step), bins - 1)
        counts[index] += 1

    return {"labels": labels, "counts": counts}


def _aggregate_value_factor_dimension(
    records: List[Dict],
    key_name: str,
    *,
    top_n: int = 20,
) -> List[Dict]:
    grouped: Dict[str, Dict[str, List[float] | int]] = {}
    for item in records:
        key = str(item.get(key_name) or "UNKNOWN")
        if key not in grouped:
            grouped[key] = {
                "target_values": [],
                "base_values": [],
                "changes": [],
                "increased": 0,
                "decreased": 0,
                "unchanged": 0,
            }

        change = float(item["change"])
        grouped[key]["target_values"].append(float(item["target_value_factor"]))
        grouped[key]["base_values"].append(float(item["base_value_factor"]))
        grouped[key]["changes"].append(change)
        if change > 0:
            grouped[key]["increased"] += 1
        elif change < 0:
            grouped[key]["decreased"] += 1
        else:
            grouped[key]["unchanged"] += 1

    results: List[Dict] = []
    for key, agg in grouped.items():
        target_values = agg["target_values"]
        base_values = agg["base_values"]
        changes = agg["changes"]
        comparable_users = len(changes)
        if comparable_users == 0:
            continue

        results.append({
            "dimension": key,
            "comparable_users": comparable_users,
            "avg_target_value_factor": round(sum(target_values) / comparable_users, 4),
            "avg_base_value_factor": round(sum(base_values) / comparable_users, 4),
            "avg_change": round(sum(changes) / comparable_users, 4),
            "median_change": round(_median(changes), 4),
            "increased_users": int(agg["increased"]),
            "decreased_users": int(agg["decreased"]),
            "unchanged_users": int(agg["unchanged"]),
        })

    results.sort(key=lambda item: (-item["comparable_users"], -item["avg_change"], item["dimension"]))
    return results[:top_n]


def get_value_factor_user_changes(
    db: Session,
    sort_by: str = "change",
    sort_order: str = "desc",
    page: int = 1,
    page_size: int = 20,
    countries: List[str] | None = None,
    genius_levels: List[str] | None = None,
    exclude_both_half: bool = False,
) -> Dict:
    target_consultant_subq = db.query(
        LeaderboardConsultantUser.user.label("user"),
        func.max(LeaderboardConsultantUser.value_factor).label("target_value_factor"),
        func.max(LeaderboardConsultantUser.country).label("target_country"),
        func.max(LeaderboardConsultantUser.university).label("target_university"),
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == VALUE_FACTOR_TARGET_DATE,
        LeaderboardConsultantUser.user.isnot(None),
        LeaderboardConsultantUser.value_factor.isnot(None),
    ).group_by(
        LeaderboardConsultantUser.user,
    ).subquery()

    base_consultant_subq = db.query(
        LeaderboardConsultantUser.user.label("user"),
        func.max(LeaderboardConsultantUser.value_factor).label("base_value_factor"),
        func.max(LeaderboardConsultantUser.country).label("base_country"),
        func.max(LeaderboardConsultantUser.university).label("base_university"),
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == VALUE_FACTOR_BASE_DATE,
        LeaderboardConsultantUser.user.isnot(None),
        LeaderboardConsultantUser.value_factor.isnot(None),
    ).group_by(
        LeaderboardConsultantUser.user,
    ).subquery()

    target_genius_subq = db.query(
        LeaderboardGeniusUser.user.label("user"),
        func.max(LeaderboardGeniusUser.genius_level).label("target_genius_level"),
        func.max(LeaderboardGeniusUser.country).label("target_genius_country"),
    ).filter(
        LeaderboardGeniusUser.delete_flag == False,
        LeaderboardGeniusUser.record_date == VALUE_FACTOR_TARGET_DATE,
        LeaderboardGeniusUser.user.isnot(None),
    ).group_by(
        LeaderboardGeniusUser.user,
    ).subquery()

    base_genius_subq = db.query(
        LeaderboardGeniusUser.user.label("user"),
        func.max(LeaderboardGeniusUser.genius_level).label("base_genius_level"),
        func.max(LeaderboardGeniusUser.country).label("base_genius_country"),
    ).filter(
        LeaderboardGeniusUser.delete_flag == False,
        LeaderboardGeniusUser.record_date == VALUE_FACTOR_BASE_DATE,
        LeaderboardGeniusUser.user.isnot(None),
    ).group_by(
        LeaderboardGeniusUser.user,
    ).subquery()

    users_subq = db.query(target_consultant_subq.c.user.label("user")).union(
        db.query(base_consultant_subq.c.user.label("user"))
    ).subquery()

    rows = db.query(
        users_subq.c.user,
        target_consultant_subq.c.target_value_factor,
        base_consultant_subq.c.base_value_factor,
        func.coalesce(
            target_genius_subq.c.target_genius_level,
            base_genius_subq.c.base_genius_level,
        ).label("genius_level"),
        func.coalesce(
            target_genius_subq.c.target_genius_country,
            base_genius_subq.c.base_genius_country,
            target_consultant_subq.c.target_country,
            base_consultant_subq.c.base_country,
        ).label("country"),
        func.coalesce(
            target_consultant_subq.c.target_university,
            base_consultant_subq.c.base_university,
        ).label("university"),
    ).outerjoin(
        target_consultant_subq,
        users_subq.c.user == target_consultant_subq.c.user,
    ).outerjoin(
        base_consultant_subq,
        users_subq.c.user == base_consultant_subq.c.user,
    ).outerjoin(
        target_genius_subq,
        users_subq.c.user == target_genius_subq.c.user,
    ).outerjoin(
        base_genius_subq,
        users_subq.c.user == base_genius_subq.c.user,
    ).all()

    filtered_rows: List[Dict] = []
    for row in rows:
        target_value = float(row.target_value_factor) if row.target_value_factor is not None else None
        base_value = float(row.base_value_factor) if row.base_value_factor is not None else None
        if target_value is None or base_value is None:
            continue

        if exclude_both_half and abs(base_value - 0.5) < 1e-9 and abs(target_value - 0.5) < 1e-9:
            continue

        row_country = str(row.country) if row.country is not None else None
        row_genius_level = str(row.genius_level) if row.genius_level is not None else None

        if countries and row_country not in countries:
            continue
        if genius_levels and row_genius_level not in genius_levels:
            continue

        filtered_rows.append({
            "user": row.user,
            "country": row_country,
            "university": row.university,
            "genius_level": row_genius_level,
            "base_value_factor": base_value,
            "target_value_factor": target_value,
            "change": target_value - base_value,
        })

    sort_field = "change"
    if sort_by in {"base_value_factor", "target_value_factor", "change"}:
        sort_field = sort_by

    reverse = sort_order != "asc"
    filtered_rows.sort(key=lambda item: item[sort_field], reverse=reverse)

    safe_page = max(page, 1)
    safe_page_size = max(page_size, 1)
    total = len(filtered_rows)
    start_index = (safe_page - 1) * safe_page_size
    end_index = start_index + safe_page_size
    page_items = filtered_rows[start_index:end_index]

    return {
        "total": total,
        "page": safe_page,
        "page_size": safe_page_size,
        "items": [
            {
                "user": item["user"],
                "country": item["country"],
                "university": item["university"],
                "genius_level": item["genius_level"],
                "base_value_factor": round(item["base_value_factor"], 4),
                "target_value_factor": round(item["target_value_factor"], 4),
                "change": round(item["change"], 4),
            }
            for item in page_items
        ],
    }


def _collect_combined_rows(
    db: Session,
    countries: List[str] | None = None,
    genius_levels: List[str] | None = None,
    exclude_alpha_both_zero: bool = False,
    exclude_power_pool_both_zero: bool = False,
    exclude_selected_both_zero: bool = False,
) -> Dict:
    target_subq = db.query(
        LeaderboardGeniusUser.user.label("user"),
        func.max(LeaderboardGeniusUser.combined_alpha_performance).label("target_alpha"),
        func.max(LeaderboardGeniusUser.combined_power_pool_alpha_performance).label("target_power_pool"),
        func.max(LeaderboardGeniusUser.combined_selected_alpha_performance).label("target_selected"),
        func.max(LeaderboardGeniusUser.country).label("target_country"),
        func.max(LeaderboardGeniusUser.genius_level).label("target_genius_level"),
    ).filter(
        LeaderboardGeniusUser.delete_flag == False,
        LeaderboardGeniusUser.record_date == COMBINED_TARGET_DATE,
        LeaderboardGeniusUser.user.isnot(None),
    ).group_by(
        LeaderboardGeniusUser.user,
    ).subquery()

    base_subq = db.query(
        LeaderboardGeniusUser.user.label("user"),
        func.max(LeaderboardGeniusUser.combined_alpha_performance).label("base_alpha"),
        func.max(LeaderboardGeniusUser.combined_power_pool_alpha_performance).label("base_power_pool"),
        func.max(LeaderboardGeniusUser.combined_selected_alpha_performance).label("base_selected"),
        func.max(LeaderboardGeniusUser.country).label("base_country"),
        func.max(LeaderboardGeniusUser.genius_level).label("base_genius_level"),
    ).filter(
        LeaderboardGeniusUser.delete_flag == False,
        LeaderboardGeniusUser.record_date == COMBINED_BASE_DATE,
        LeaderboardGeniusUser.user.isnot(None),
    ).group_by(
        LeaderboardGeniusUser.user,
    ).subquery()

    users_subq = db.query(target_subq.c.user.label("user")).union(
        db.query(base_subq.c.user.label("user"))
    ).subquery()

    rows = db.query(
        users_subq.c.user,
        target_subq.c.target_alpha,
        base_subq.c.base_alpha,
        target_subq.c.target_power_pool,
        base_subq.c.base_power_pool,
        target_subq.c.target_selected,
        base_subq.c.base_selected,
        func.coalesce(target_subq.c.target_country, base_subq.c.base_country).label("country"),
        func.coalesce(target_subq.c.target_genius_level, base_subq.c.base_genius_level).label("genius_level"),
    ).outerjoin(
        target_subq,
        users_subq.c.user == target_subq.c.user,
    ).outerjoin(
        base_subq,
        users_subq.c.user == base_subq.c.user,
    ).all()

    comparable_rows: List[Dict] = []
    users_on_target_date = 0
    users_on_base_date = 0
    new_users = 0
    missing_users = 0

    for row in rows:
        target_ready = (
            row.target_alpha is not None
            and row.target_power_pool is not None
            and row.target_selected is not None
        )
        base_ready = (
            row.base_alpha is not None
            and row.base_power_pool is not None
            and row.base_selected is not None
        )

        if target_ready:
            users_on_target_date += 1
        if base_ready:
            users_on_base_date += 1
        if target_ready and not base_ready:
            new_users += 1
        if base_ready and not target_ready:
            missing_users += 1
        if not target_ready or not base_ready:
            continue

        row_country = str(row.country) if row.country is not None else None
        row_genius_level = str(row.genius_level) if row.genius_level is not None else None
        if countries and row_country not in countries:
            continue
        if genius_levels and row_genius_level not in genius_levels:
            continue

        base_alpha = float(row.base_alpha)
        target_alpha = float(row.target_alpha)
        base_power_pool = float(row.base_power_pool)
        target_power_pool = float(row.target_power_pool)
        base_selected = float(row.base_selected)
        target_selected = float(row.target_selected)

        if exclude_alpha_both_zero and abs(base_alpha) < 1e-12 and abs(target_alpha) < 1e-12:
            continue
        if exclude_power_pool_both_zero and abs(base_power_pool) < 1e-12 and abs(target_power_pool) < 1e-12:
            continue
        if exclude_selected_both_zero and abs(base_selected) < 1e-12 and abs(target_selected) < 1e-12:
            continue

        comparable_rows.append({
            "user": row.user,
            "country": row_country,
            "genius_level": row_genius_level,
            "base_alpha": base_alpha,
            "target_alpha": target_alpha,
            "alpha_change": target_alpha - base_alpha,
            "base_power_pool": base_power_pool,
            "target_power_pool": target_power_pool,
            "power_pool_change": target_power_pool - base_power_pool,
            "base_selected": base_selected,
            "target_selected": target_selected,
            "selected_change": target_selected - base_selected,
        })

    return {
        "users_on_target_date": users_on_target_date,
        "users_on_base_date": users_on_base_date,
        "new_users": new_users,
        "missing_users": missing_users,
        "rows": comparable_rows,
    }


def _build_combined_metric_summary(
    rows: List[Dict],
    metric: str,
    display_name: str,
    base_key: str,
    target_key: str,
    change_key: str,
) -> Dict:
    changes = [float(row[change_key]) for row in rows]
    target_values = [float(row[target_key]) for row in rows]
    base_values = [float(row[base_key]) for row in rows]
    increased_users = sum(1 for value in changes if value > 0)
    decreased_users = sum(1 for value in changes if value < 0)
    unchanged_users = sum(1 for value in changes if value == 0)

    return {
        "metric": metric,
        "display_name": display_name,
        "avg_target": round(sum(target_values) / len(target_values), 4) if target_values else 0.0,
        "avg_base": round(sum(base_values) / len(base_values), 4) if base_values else 0.0,
        "avg_change": round(sum(changes) / len(changes), 4) if changes else 0.0,
        "median_change": round(_median(changes), 4),
        "max_increase": round(max(changes), 4) if changes else 0.0,
        "max_decrease": round(min(changes), 4) if changes else 0.0,
        "increased_users": increased_users,
        "decreased_users": decreased_users,
        "unchanged_users": unchanged_users,
    }


def get_combined_analysis(
    db: Session,
    countries: List[str] | None = None,
    genius_levels: List[str] | None = None,
    exclude_alpha_both_zero: bool = False,
    exclude_power_pool_both_zero: bool = False,
    exclude_selected_both_zero: bool = False,
) -> Dict:
    payload = _collect_combined_rows(
        db,
        countries=countries,
        genius_levels=genius_levels,
        exclude_alpha_both_zero=exclude_alpha_both_zero,
        exclude_power_pool_both_zero=exclude_power_pool_both_zero,
        exclude_selected_both_zero=exclude_selected_both_zero,
    )
    rows = payload["rows"]

    metric_summaries = [
        _build_combined_metric_summary(
            rows,
            "combined_alpha_performance",
            "Combined Alpha",
            "base_alpha",
            "target_alpha",
            "alpha_change",
        ),
        _build_combined_metric_summary(
            rows,
            "combined_power_pool_alpha_performance",
            "Power Pool",
            "base_power_pool",
            "target_power_pool",
            "power_pool_change",
        ),
        _build_combined_metric_summary(
            rows,
            "combined_selected_alpha_performance",
            "Selected Alpha",
            "base_selected",
            "target_selected",
            "selected_change",
        ),
    ]

    distributions = {
        "combined_alpha_performance": _build_distribution(
            [float(row["alpha_change"]) for row in rows],
            bins=10,
        ),
        "combined_power_pool_alpha_performance": _build_distribution(
            [float(row["power_pool_change"]) for row in rows],
            bins=10,
        ),
        "combined_selected_alpha_performance": _build_distribution(
            [float(row["selected_change"]) for row in rows],
            bins=10,
        ),
    }

    return {
        "base_record_date": COMBINED_BASE_DATE.isoformat(),
        "target_record_date": COMBINED_TARGET_DATE.isoformat(),
        "summary": {
            "users_on_target_date": payload["users_on_target_date"],
            "users_on_base_date": payload["users_on_base_date"],
            "comparable_users": len(rows),
            "new_users": payload["new_users"],
            "missing_users": payload["missing_users"],
        },
        "metric_summaries": metric_summaries,
        "distributions": distributions,
    }


def get_combined_user_changes(
    db: Session,
    sort_by: str = "alpha_change",
    sort_order: str = "desc",
    page: int = 1,
    page_size: int = 20,
    countries: List[str] | None = None,
    genius_levels: List[str] | None = None,
    exclude_alpha_both_zero: bool = False,
    exclude_power_pool_both_zero: bool = False,
    exclude_selected_both_zero: bool = False,
) -> Dict:
    payload = _collect_combined_rows(
        db,
        countries=countries,
        genius_levels=genius_levels,
        exclude_alpha_both_zero=exclude_alpha_both_zero,
        exclude_power_pool_both_zero=exclude_power_pool_both_zero,
        exclude_selected_both_zero=exclude_selected_both_zero,
    )
    rows = payload["rows"]

    sort_key_map = {
        "alpha_change": "alpha_change",
        "power_pool_change": "power_pool_change",
        "selected_change": "selected_change",
        "base_alpha": "base_alpha",
        "target_alpha": "target_alpha",
        "base_power_pool": "base_power_pool",
        "target_power_pool": "target_power_pool",
        "base_selected": "base_selected",
        "target_selected": "target_selected",
    }
    sort_key = sort_key_map.get(sort_by, "alpha_change")
    reverse = sort_order != "asc"
    rows.sort(key=lambda item: float(item[sort_key]), reverse=reverse)

    safe_page = max(page, 1)
    safe_page_size = max(page_size, 1)
    total = len(rows)
    start_index = (safe_page - 1) * safe_page_size
    end_index = start_index + safe_page_size
    page_items = rows[start_index:end_index]

    return {
        "total": total,
        "page": safe_page,
        "page_size": safe_page_size,
        "items": [
            {
                "user": item["user"],
                "country": item["country"],
                "genius_level": item["genius_level"],
                "base_alpha": round(float(item["base_alpha"]), 4),
                "target_alpha": round(float(item["target_alpha"]), 4),
                "alpha_change": round(float(item["alpha_change"]), 4),
                "base_power_pool": round(float(item["base_power_pool"]), 4),
                "target_power_pool": round(float(item["target_power_pool"]), 4),
                "power_pool_change": round(float(item["power_pool_change"]), 4),
                "base_selected": round(float(item["base_selected"]), 4),
                "target_selected": round(float(item["target_selected"]), 4),
                "selected_change": round(float(item["selected_change"]), 4),
            }
            for item in page_items
        ],
    }


def get_value_factor_analysis(db: Session, exclude_both_half: bool = False) -> Dict:
    target_subq = db.query(
        LeaderboardConsultantUser.user.label("user"),
        func.max(LeaderboardConsultantUser.value_factor).label("target_value_factor"),
        func.max(LeaderboardConsultantUser.country).label("target_country"),
        func.max(LeaderboardConsultantUser.university).label("target_university"),
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == VALUE_FACTOR_TARGET_DATE,
        LeaderboardConsultantUser.user.isnot(None),
        LeaderboardConsultantUser.value_factor.isnot(None),
    ).group_by(
        LeaderboardConsultantUser.user,
    ).subquery()

    base_subq = db.query(
        LeaderboardConsultantUser.user.label("user"),
        func.max(LeaderboardConsultantUser.value_factor).label("base_value_factor"),
        func.max(LeaderboardConsultantUser.country).label("base_country"),
        func.max(LeaderboardConsultantUser.university).label("base_university"),
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == VALUE_FACTOR_BASE_DATE,
        LeaderboardConsultantUser.user.isnot(None),
        LeaderboardConsultantUser.value_factor.isnot(None),
    ).group_by(
        LeaderboardConsultantUser.user,
    ).subquery()

    users_subq = db.query(target_subq.c.user.label("user")).union(
        db.query(base_subq.c.user.label("user"))
    ).subquery()

    rows = db.query(
        users_subq.c.user,
        target_subq.c.target_value_factor,
        base_subq.c.base_value_factor,
        func.coalesce(target_subq.c.target_country, base_subq.c.base_country).label("country"),
        func.coalesce(target_subq.c.target_university, base_subq.c.base_university).label("university"),
    ).outerjoin(
        target_subq,
        users_subq.c.user == target_subq.c.user,
    ).outerjoin(
        base_subq,
        users_subq.c.user == base_subq.c.user,
    ).all()

    comparable_rows: List[Dict] = []
    users_on_target_date = 0
    users_on_base_date = 0
    new_users = 0
    missing_users = 0

    for row in rows:
        target_value = float(row.target_value_factor) if row.target_value_factor is not None else None
        base_value = float(row.base_value_factor) if row.base_value_factor is not None else None
        if target_value is not None:
            users_on_target_date += 1
        if base_value is not None:
            users_on_base_date += 1

        if target_value is not None and base_value is None:
            new_users += 1
        if target_value is None and base_value is not None:
            missing_users += 1

        if target_value is None or base_value is None:
            continue

        # Exclude rows only when both dates are approximately 0.5.
        if exclude_both_half and abs(base_value - 0.5) < 1e-9 and abs(target_value - 0.5) < 1e-9:
            continue

        comparable_rows.append({
            "user": row.user,
            "country": row.country,
            "university": row.university,
            "base_value_factor": base_value,
            "target_value_factor": target_value,
            "change": target_value - base_value,
        })

    changes = [item["change"] for item in comparable_rows]
    target_values = [item["target_value_factor"] for item in comparable_rows]
    base_values = [item["base_value_factor"] for item in comparable_rows]
    increased_users = sum(1 for value in changes if value > 0)
    decreased_users = sum(1 for value in changes if value < 0)
    unchanged_users = sum(1 for value in changes if value == 0)

    summary = {
        "users_on_target_date": users_on_target_date,
        "users_on_base_date": users_on_base_date,
        "comparable_users": len(comparable_rows),
        "new_users": new_users,
        "missing_users": missing_users,
        "increased_users": increased_users,
        "decreased_users": decreased_users,
        "unchanged_users": unchanged_users,
        "avg_target_value_factor": round(sum(target_values) / len(target_values), 4) if target_values else 0.0,
        "avg_base_value_factor": round(sum(base_values) / len(base_values), 4) if base_values else 0.0,
        "avg_change": round(sum(changes) / len(changes), 4) if changes else 0.0,
        "median_change": round(_median(changes), 4),
        "max_increase": round(max(changes), 4) if changes else 0.0,
        "max_decrease": round(min(changes), 4) if changes else 0.0,
    }

    sorted_desc = sorted(comparable_rows, key=lambda item: item["change"], reverse=True)
    sorted_asc = sorted(comparable_rows, key=lambda item: item["change"])

    top_gainers = [
        {
            "user": item["user"],
            "country": item["country"],
            "university": item["university"],
            "base_value_factor": round(item["base_value_factor"], 4),
            "target_value_factor": round(item["target_value_factor"], 4),
            "change": round(item["change"], 4),
        }
        for item in sorted_desc[:20]
    ]
    top_decliners = [
        {
            "user": item["user"],
            "country": item["country"],
            "university": item["university"],
            "base_value_factor": round(item["base_value_factor"], 4),
            "target_value_factor": round(item["target_value_factor"], 4),
            "change": round(item["change"], 4),
        }
        for item in sorted_asc[:20]
    ]

    return {
        "base_record_date": VALUE_FACTOR_BASE_DATE.isoformat(),
        "target_record_date": VALUE_FACTOR_TARGET_DATE.isoformat(),
        "summary": summary,
        "by_country": _aggregate_value_factor_dimension(comparable_rows, "country", top_n=20),
        "by_university": _aggregate_value_factor_dimension(comparable_rows, "university", top_n=20),
        "top_gainers": top_gainers,
        "top_decliners": top_decliners,
        "distribution": _build_distribution(changes, bins=10),
    }


def get_user_metric_trends_by_event(db: Session, user: str) -> Dict:
    events = db.query(
        EventUpdateRecord.id,
        EventUpdateRecord.update_content,
        EventUpdateRecord.update_date,
        EventUpdateRecord.date_range,
    ).filter(
        EventUpdateRecord.update_date.isnot(None),
        func.lower(EventUpdateRecord.update_content).in_(["value_factor", "combined"]),
    ).order_by(
        EventUpdateRecord.update_date.asc(),
        EventUpdateRecord.id.asc(),
    ).all()

    value_events_by_date: Dict[date, Dict] = {}
    combined_events_by_date: Dict[date, Dict] = {}
    for row in events:
        content = (row.update_content or "").lower()
        event_payload = {
            "update_date": row.update_date,
            "date_range": row.date_range or row.update_date.isoformat(),
        }
        if content == "value_factor":
            value_events_by_date[row.update_date] = event_payload
        elif content == "combined":
            combined_events_by_date[row.update_date] = event_payload

    value_event_dates = sorted(value_events_by_date.keys())
    combined_event_dates = sorted(combined_events_by_date.keys())

    value_map: Dict[date, float | None] = {}
    if value_event_dates:
        value_rows = db.query(
            LeaderboardConsultantUser.record_date.label("record_date"),
            func.max(LeaderboardConsultantUser.value_factor).label("value_factor"),
        ).filter(
            LeaderboardConsultantUser.delete_flag == False,
            LeaderboardConsultantUser.user == user,
            LeaderboardConsultantUser.record_date.in_(value_event_dates),
        ).group_by(
            LeaderboardConsultantUser.record_date,
        ).all()
        value_map = {
            row.record_date: float(row.value_factor) if row.value_factor is not None else None
            for row in value_rows
        }

    combined_map: Dict[date, Dict[str, float | None]] = {}
    if combined_event_dates:
        combined_rows = db.query(
            LeaderboardGeniusUser.record_date.label("record_date"),
            func.max(LeaderboardGeniusUser.combined_alpha_performance).label("combined_alpha_performance"),
            func.max(LeaderboardGeniusUser.combined_power_pool_alpha_performance).label(
                "combined_power_pool_alpha_performance"
            ),
            func.max(LeaderboardGeniusUser.combined_selected_alpha_performance).label(
                "combined_selected_alpha_performance"
            ),
        ).filter(
            LeaderboardGeniusUser.delete_flag == False,
            LeaderboardGeniusUser.user == user,
            LeaderboardGeniusUser.record_date.in_(combined_event_dates),
        ).group_by(
            LeaderboardGeniusUser.record_date,
        ).all()
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
            for row in combined_rows
        }

    value_factor_trend = [
        {
            "update_date": event["update_date"].isoformat(),
            "date_range": event["date_range"],
            "value_factor": value_map.get(event["update_date"]),
        }
        for event in [value_events_by_date[d] for d in value_event_dates]
    ]

    combined_trend = [
        {
            "update_date": event["update_date"].isoformat(),
            "date_range": event["date_range"],
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
