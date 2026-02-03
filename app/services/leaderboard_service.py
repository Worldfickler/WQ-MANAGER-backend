from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc, text, and_
from app.models.leaderboard import (
    LeaderboardConsultantCountryOrRegion,
    LeaderboardConsultantUser,
    LeaderboardGeniusCountryOrRegion,
    LeaderboardGeniusUser,
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
    "get_summary_statistics"
]


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
