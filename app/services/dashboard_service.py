from sqlalchemy.orm import Session
from sqlalchemy import func, desc, asc, case, literal
from app.models.leaderboard import LeaderboardConsultantUser, LeaderboardConsultantCountryOrRegion
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
import re


__all__ = [
    "get_country_rankings",
    "get_university_rankings",
    "get_top_users_by_weight",
    "get_top_users_by_weight_change",
    "get_top_users_by_submissions",
    "get_top_users_by_correlation",
    "get_country_history"
]


def parse_quarter(quarter_str: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    解析季度字符串，返回季度的开始日期和结束日期

    Args:
        quarter_str: 季度字符串，格式：2026-Q1、2025-Q4

    Returns:
        (quarter_start_date, quarter_end_date) 或 (None, None) 如果解析失败
    """
    if not quarter_str:
        return None, None

    # 解析季度字符串，格式：2026-Q1
    match = re.match(r'^(\d{4})-Q([1-4])$', quarter_str)
    if not match:
        return None, None

    year = int(match.group(1))
    quarter = int(match.group(2))

    # 计算季度的开始和结束月份
    start_month = (quarter - 1) * 3 + 1
    end_month = quarter * 3

    # 计算结束日期（该季度最后一天）
    if end_month == 12:
        quarter_end = datetime(year, 12, 31)
    else:
        # 下一个月的第一天减去一天
        next_month_first = datetime(year, end_month + 1, 1)
        quarter_end = next_month_first - timedelta(days=1)

    quarter_start = datetime(year, start_month, 1)

    return quarter_start, quarter_end


def get_previous_quarter_end(quarter_str: str) -> Optional[datetime]:
    """
    获取上一个季度的结束日期

    Args:
        quarter_str: 当前季度字符串，格式：2026-Q1

    Returns:
        上一个季度的结束日期
    """
    match = re.match(r'^(\d{4})-Q([1-4])$', quarter_str)
    if not match:
        return None

    year = int(match.group(1))
    quarter = int(match.group(2))

    # 计算上一个季度
    if quarter == 1:
        prev_year = year - 1
        prev_quarter = 4
    else:
        prev_year = year
        prev_quarter = quarter - 1

    # 计算上一个季度的结束月份
    prev_end_month = prev_quarter * 3

    # 计算上一个季度的结束日期
    if prev_end_month == 12:
        prev_quarter_end = datetime(prev_year, 12, 31)
    else:
        next_month_first = datetime(prev_year, prev_end_month + 1, 1)
        prev_quarter_end = next_month_first - timedelta(days=1)

    return prev_quarter_end


def get_country_rankings(db: Session, page: int = 1, page_size: int = 50, quarter: str = '') -> Tuple[List[Dict], int]:
    """
    按国家维度统计排名（使用 leaderboard_consultant_country_or_region 表）

    Args:
        quarter: 季度字符串，格式：2026-Q1。用于计算与上一季度的变化

    返回：(数据列表, 总数)
    """
    # 解析季度
    quarter_start, quarter_end = parse_quarter(quarter)

    # 如果没有提供季度或解析失败，使用最新日期
    if quarter_end is None:
        latest_date = db.query(
            func.max(LeaderboardConsultantCountryOrRegion.record_date)
        ).filter(
            LeaderboardConsultantCountryOrRegion.delete_flag == False
        ).scalar()
    else:
        # 查找该季度结束日期之前或当天的最新数据
        latest_date = db.query(
            func.max(LeaderboardConsultantCountryOrRegion.record_date)
        ).filter(
            LeaderboardConsultantCountryOrRegion.delete_flag == False,
            LeaderboardConsultantCountryOrRegion.record_date <= quarter_end
        ).scalar()

    if not latest_date:
        return [], 0

    # 计算比较日期（上一个季度的结束日期）
    if quarter:
        compare_date = get_previous_quarter_end(quarter)
        if compare_date:
            # 查找上一季度结束日期之前或当天的最新数据
            start_date = db.query(
                func.max(LeaderboardConsultantCountryOrRegion.record_date)
            ).filter(
                LeaderboardConsultantCountryOrRegion.delete_flag == False,
                LeaderboardConsultantCountryOrRegion.record_date <= compare_date
            ).scalar()
        else:
            start_date = None
    else:
        # 如果没有季度参数，查找最新日期之前的最近一条数据
        start_date = db.query(
            func.max(LeaderboardConsultantCountryOrRegion.record_date)
        ).filter(
            LeaderboardConsultantCountryOrRegion.delete_flag == False,
            LeaderboardConsultantCountryOrRegion.record_date < latest_date
        ).scalar()

    # 当前数据（最新日期）
    current_stats = db.query(
        LeaderboardConsultantCountryOrRegion.country,
        LeaderboardConsultantCountryOrRegion.user.label('user_count'),
        LeaderboardConsultantCountryOrRegion.weight_factor.label('weight_factor'),
        LeaderboardConsultantCountryOrRegion.value_factor.label('value_factor'),
        LeaderboardConsultantCountryOrRegion.submissions_count.label('submissions_count'),
        LeaderboardConsultantCountryOrRegion.super_alpha_submissions_count.label('super_alpha_submissions_count'),
        LeaderboardConsultantCountryOrRegion.mean_prod_correlation.label('mean_prod_correlation'),
        LeaderboardConsultantCountryOrRegion.mean_self_correlation.label('mean_self_correlation'),
        LeaderboardConsultantCountryOrRegion.super_alpha_mean_prod_correlation.label('super_alpha_mean_prod_correlation'),
        LeaderboardConsultantCountryOrRegion.super_alpha_mean_self_correlation.label('super_alpha_mean_self_correlation')
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.record_date == latest_date,
        LeaderboardConsultantCountryOrRegion.country.isnot(None)
    ).subquery()

    # 历史数据（用于计算变化）- 只有当 start_date 存在时才查询
    if start_date:
        historical_stats = db.query(
            LeaderboardConsultantCountryOrRegion.country,
            LeaderboardConsultantCountryOrRegion.weight_factor.label('historical_weight_factor'),
            LeaderboardConsultantCountryOrRegion.value_factor.label('historical_value_factor'),
            LeaderboardConsultantCountryOrRegion.submissions_count.label('historical_submissions_count'),
            LeaderboardConsultantCountryOrRegion.super_alpha_submissions_count.label('historical_super_alpha_submissions_count'),
            LeaderboardConsultantCountryOrRegion.mean_prod_correlation.label('historical_mean_prod_correlation'),
            LeaderboardConsultantCountryOrRegion.mean_self_correlation.label('historical_mean_self_correlation')
        ).filter(
            LeaderboardConsultantCountryOrRegion.delete_flag == False,
            LeaderboardConsultantCountryOrRegion.record_date == start_date,
            LeaderboardConsultantCountryOrRegion.country.isnot(None)
        ).subquery()

        # 合并查询
        base_query = db.query(
            current_stats.c.country,
            current_stats.c.user_count,
            current_stats.c.weight_factor,
            current_stats.c.value_factor,
            current_stats.c.submissions_count,
            current_stats.c.super_alpha_submissions_count,
            current_stats.c.mean_prod_correlation,
            current_stats.c.mean_self_correlation,
            current_stats.c.super_alpha_mean_prod_correlation,
            current_stats.c.super_alpha_mean_self_correlation,
            historical_stats.c.historical_weight_factor,
            historical_stats.c.historical_value_factor,
            historical_stats.c.historical_submissions_count,
            historical_stats.c.historical_super_alpha_submissions_count,
            historical_stats.c.historical_mean_prod_correlation,
            historical_stats.c.historical_mean_self_correlation
        ).outerjoin(
            historical_stats,
            current_stats.c.country == historical_stats.c.country
        ).order_by(
            desc(current_stats.c.weight_factor)
        )
    else:
        # 没有历史数据时，只查询当前数据
        base_query = db.query(
            current_stats.c.country,
            current_stats.c.user_count,
            current_stats.c.weight_factor,
            current_stats.c.value_factor,
            current_stats.c.submissions_count,
            current_stats.c.super_alpha_submissions_count,
            current_stats.c.mean_prod_correlation,
            current_stats.c.mean_self_correlation,
            current_stats.c.super_alpha_mean_prod_correlation,
            current_stats.c.super_alpha_mean_self_correlation,
            literal(None).label('historical_weight_factor'),
            literal(None).label('historical_value_factor'),
            literal(None).label('historical_submissions_count'),
            literal(None).label('historical_super_alpha_submissions_count'),
            literal(None).label('historical_mean_prod_correlation'),
            literal(None).label('historical_mean_self_correlation')
        ).order_by(
            desc(current_stats.c.weight_factor)
        )

    # 获取总数
    total = base_query.count()

    # 分页
    offset = (page - 1) * page_size
    results = base_query.limit(page_size).offset(offset).all()

    # 格式化结果
    output = []
    for row in results:
        # 计算变化值
        weight_change = None
        value_change = None
        submissions_change = None
        super_alpha_submissions_change = None
        prod_corr_change = None
        self_corr_change = None

        if row.historical_weight_factor is not None and row.weight_factor is not None:
            weight_change = row.weight_factor - row.historical_weight_factor
        if row.historical_value_factor is not None and row.value_factor is not None:
            value_change = row.value_factor - row.historical_value_factor
        if row.historical_submissions_count is not None and row.submissions_count is not None:
            submissions_change = row.submissions_count - row.historical_submissions_count
        if row.historical_super_alpha_submissions_count is not None and row.super_alpha_submissions_count is not None:
            super_alpha_submissions_change = row.super_alpha_submissions_count - row.historical_super_alpha_submissions_count
        if row.historical_mean_prod_correlation is not None and row.mean_prod_correlation is not None:
            prod_corr_change = row.mean_prod_correlation - row.historical_mean_prod_correlation
        if row.historical_mean_self_correlation is not None and row.mean_self_correlation is not None:
            self_corr_change = row.mean_self_correlation - row.historical_mean_self_correlation

        total_submissions = (row.submissions_count or 0) + (row.super_alpha_submissions_count or 0)

        # 计算总提交数变化
        total_submissions_change = None
        if submissions_change is not None and super_alpha_submissions_change is not None:
            total_submissions_change = submissions_change + super_alpha_submissions_change
        elif submissions_change is not None:
            total_submissions_change = submissions_change
        elif super_alpha_submissions_change is not None:
            total_submissions_change = super_alpha_submissions_change

        output.append({
            "country": row.country,
            "user_count": int(row.user_count or 0),
            "weight_factor": round(float(row.weight_factor or 0), 2),
            "value_factor": round(float(row.value_factor or 0), 2) if row.value_factor is not None else None,
            "submissions_count": int(row.submissions_count or 0),
            "super_alpha_submissions_count": int(row.super_alpha_submissions_count or 0),
            "total_submissions": total_submissions,
            "mean_prod_correlation": round(float(row.mean_prod_correlation or 0), 2) if row.mean_prod_correlation is not None else None,
            "mean_self_correlation": round(float(row.mean_self_correlation or 0), 2) if row.mean_self_correlation is not None else None,
            "super_alpha_mean_prod_correlation": round(float(row.super_alpha_mean_prod_correlation or 0), 2) if row.super_alpha_mean_prod_correlation is not None else None,
            "super_alpha_mean_self_correlation": round(float(row.super_alpha_mean_self_correlation or 0), 2) if row.super_alpha_mean_self_correlation is not None else None,
            # 变化值
            "weight_change": round(float(weight_change), 2) if weight_change is not None else None,
            "value_change": round(float(value_change), 2) if value_change is not None else None,
            "submissions_change": int(submissions_change) if submissions_change is not None else None,
            "super_alpha_submissions_change": int(super_alpha_submissions_change) if super_alpha_submissions_change is not None else None,
            "total_submissions_change": int(total_submissions_change) if total_submissions_change is not None else None,
            "prod_corr_change": round(float(prod_corr_change), 2) if prod_corr_change is not None else None,
            "self_corr_change": round(float(self_corr_change), 2) if self_corr_change is not None else None
        })

    return output, total


def get_university_rankings(db: Session, page: int = 1, page_size: int = 50, quarter: str = '') -> Tuple[List[Dict], int]:
    """
    按大学维度统计排名

    Args:
        quarter: 季度字符串，格式：2026-Q1

    返回：(数据列表, 总数)
    """
    # 解析季度
    quarter_start, quarter_end = parse_quarter(quarter)

    # 如果没有提供季度或解析失败，使用最新日期
    if quarter_end is None:
        latest_date = db.query(
            func.max(LeaderboardConsultantUser.record_date)
        ).filter(
            LeaderboardConsultantUser.delete_flag == False
        ).scalar()
    else:
        # 查找该季度结束日期之前或当天的最新数据
        latest_date = db.query(
            func.max(LeaderboardConsultantUser.record_date)
        ).filter(
            LeaderboardConsultantUser.delete_flag == False,
            LeaderboardConsultantUser.record_date <= quarter_end
        ).scalar()

    if not latest_date:
        return [], 0

    # 当前数据聚合
    base_query = db.query(
        LeaderboardConsultantUser.university,
        func.count(func.distinct(LeaderboardConsultantUser.user)).label('user_count'),
        func.avg(LeaderboardConsultantUser.weight_factor).label('avg_weight'),
        func.sum(LeaderboardConsultantUser.submissions_count +
                LeaderboardConsultantUser.super_alpha_submissions_count).label('total_submissions'),
        func.max(LeaderboardConsultantUser.weight_factor).label('max_weight')
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == latest_date,
        LeaderboardConsultantUser.university.isnot(None),
        LeaderboardConsultantUser.university != '',
        LeaderboardConsultantUser.weight_factor.isnot(None)
    ).group_by(
        LeaderboardConsultantUser.university
    ).order_by(
        desc(func.avg(LeaderboardConsultantUser.weight_factor))
    )

    # 获取总数
    total = base_query.count()

    # 分页
    offset = (page - 1) * page_size
    results = base_query.limit(page_size).offset(offset).all()

    # 格式化结果
    output = []
    for row in results:
        output.append({
            "university": row.university,
            "user_count": int(row.user_count or 0),
            "avg_weight": round(float(row.avg_weight or 0), 2),
            "max_weight": round(float(row.max_weight or 0), 2),
            "total_submissions": int(row.total_submissions or 0)
        })

    return output, total


def get_top_users_by_weight(db: Session, page: int = 1, page_size: int = 50, country: str = None) -> Tuple[List[Dict], int]:
    """
    按当前weight绝对值排名

    返回：(数据列表, 总数)
    """
    # 获取最新日期
    latest_date = db.query(
        func.max(LeaderboardConsultantUser.record_date)
    ).filter(
        LeaderboardConsultantUser.delete_flag == False
    ).scalar()

    if not latest_date:
        return [], 0

    # 查询
    query = db.query(LeaderboardConsultantUser).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == latest_date,
        LeaderboardConsultantUser.weight_factor.isnot(None)
    )

    # 国家筛选
    if country:
        query = query.filter(LeaderboardConsultantUser.country == country)

    base_query = query.order_by(desc(LeaderboardConsultantUser.weight_factor))

    # 获取总数
    total = base_query.count()

    # 分页
    offset = (page - 1) * page_size
    results = base_query.limit(page_size).offset(offset).all()

    # 格式化结果
    output = []
    for idx, user in enumerate(results, offset + 1):
        output.append({
            "rank": idx,
            "user": user.user,
            "weight_factor": round(float(user.weight_factor or 0), 2),
            "value_factor": round(float(user.value_factor or 0), 2) if user.value_factor else None,
            "total_submissions": (user.submissions_count or 0) + (user.super_alpha_submissions_count or 0),
            "country": user.country,
            "university": user.university
        })

    return output, total


def get_top_users_by_weight_change(
    db: Session,
    page: int = 1,
    page_size: int = 50,
    quarter: str = '',
    order: str = "desc",
    country: str = None
) -> Tuple[List[Dict], int]:
    """
    按weight变化排名

    Args:
        quarter: 季度字符串，格式：2026-Q1
        order: "desc" 上升最多, "asc" 下降最多

    返回：(数据列表, 总数)
    """
    # 解析季度
    quarter_start, quarter_end = parse_quarter(quarter)

    # 如果没有提供季度或解析失败，使用最新日期
    if quarter_end is None:
        latest_date = db.query(
            func.max(LeaderboardConsultantUser.record_date)
        ).filter(
            LeaderboardConsultantUser.delete_flag == False
        ).scalar()
    else:
        # 查找该季度结束日期之前或当天的最新数据
        latest_date = db.query(
            func.max(LeaderboardConsultantUser.record_date)
        ).filter(
            LeaderboardConsultantUser.delete_flag == False,
            LeaderboardConsultantUser.record_date <= quarter_end
        ).scalar()

    if not latest_date:
        return [], 0

    # 计算比较日期（上一个季度的结束日期）
    if quarter:
        compare_date = get_previous_quarter_end(quarter)
        if compare_date:
            # 查找上一季度结束日期之前或当天的最新数据
            start_date = db.query(
                func.max(LeaderboardConsultantUser.record_date)
            ).filter(
                LeaderboardConsultantUser.delete_flag == False,
                LeaderboardConsultantUser.record_date <= compare_date
            ).scalar()
        else:
            start_date = None
    else:
        # 如果没有季度参数，默认比较前一天
        start_date = latest_date - timedelta(days=1)

    # 当前数据
    current_subq = db.query(
        LeaderboardConsultantUser.user,
        LeaderboardConsultantUser.weight_factor.label('current_weight'),
        LeaderboardConsultantUser.country,
        LeaderboardConsultantUser.university
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == latest_date,
        LeaderboardConsultantUser.weight_factor.isnot(None)
    ).subquery()

    # 历史数据
    historical_subq = db.query(
        LeaderboardConsultantUser.user,
        LeaderboardConsultantUser.weight_factor.label('historical_weight')
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == start_date,
        LeaderboardConsultantUser.weight_factor.isnot(None)
    ).subquery()

    # 计算变化
    weight_change_expr = func.coalesce(
        current_subq.c.current_weight - historical_subq.c.historical_weight,
        current_subq.c.current_weight
    )

    # 查询
    query = db.query(
        current_subq.c.user,
        current_subq.c.current_weight,
        current_subq.c.country,
        current_subq.c.university,
        weight_change_expr.label('weight_change')
    ).outerjoin(
        historical_subq,
        current_subq.c.user == historical_subq.c.user
    )

    # 国家筛选
    if country:
        query = query.filter(current_subq.c.country == country)

    # 排序
    if order == "desc":
        base_query = query.order_by(desc(weight_change_expr))
    else:
        base_query = query.order_by(asc(weight_change_expr))

    # 获取总数
    total = base_query.count()

    # 分页
    offset = (page - 1) * page_size
    results = base_query.limit(page_size).offset(offset).all()

    # 格式化结果
    output = []
    for idx, row in enumerate(results, offset + 1):
        output.append({
            "rank": idx,
            "user": row.user,
            "current_weight": round(float(row.current_weight or 0), 2),
            "weight_change": round(float(row.weight_change or 0), 2),
            "country": row.country,
            "university": row.university
        })

    return output, total


def get_top_users_by_submissions(db: Session, page: int = 1, page_size: int = 50, country: str = None) -> Tuple[List[Dict], int]:
    """
    按提交总数排名

    返回：(数据列表, 总数)
    """
    # 获取最新日期
    latest_date = db.query(
        func.max(LeaderboardConsultantUser.record_date)
    ).filter(
        LeaderboardConsultantUser.delete_flag == False
    ).scalar()

    if not latest_date:
        return [], 0

    # 计算总提交数
    total_submissions_expr = (
        func.coalesce(LeaderboardConsultantUser.submissions_count, 0) +
        func.coalesce(LeaderboardConsultantUser.super_alpha_submissions_count, 0)
    )

    # 查询
    query = db.query(
        LeaderboardConsultantUser.user,
        LeaderboardConsultantUser.weight_factor,
        LeaderboardConsultantUser.submissions_count,
        LeaderboardConsultantUser.super_alpha_submissions_count,
        LeaderboardConsultantUser.country,
        LeaderboardConsultantUser.university,
        total_submissions_expr.label('total_submissions')
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == latest_date
    )

    # 国家筛选
    if country:
        query = query.filter(LeaderboardConsultantUser.country == country)

    base_query = query.order_by(desc(total_submissions_expr))

    # 获取总数
    total = base_query.count()

    # 分页
    offset = (page - 1) * page_size
    results = base_query.limit(page_size).offset(offset).all()

    # 格式化结果
    output = []
    for idx, row in enumerate(results, offset + 1):
        output.append({
            "rank": idx,
            "user": row.user,
            "weight_factor": round(float(row.weight_factor or 0), 2) if row.weight_factor else None,
            "regular_submissions": int(row.submissions_count or 0),
            "super_alpha_submissions": int(row.super_alpha_submissions_count or 0),
            "total_submissions": int(row.total_submissions or 0),
            "country": row.country,
            "university": row.university
        })

    return output, total


def get_top_users_by_correlation(
    db: Session,
    page: int = 1,
    page_size: int = 50,
    correlation_type: str = "prod",
    country: str = None
) -> Tuple[List[Dict], int]:
    """
    按相关性排名

    Args:
        correlation_type: "prod" 生产相关性, "self" 自相关性

    返回：(数据列表, 总数)
    """
    # 获取最新日期
    latest_date = db.query(
        func.max(LeaderboardConsultantUser.record_date)
    ).filter(
        LeaderboardConsultantUser.delete_flag == False
    ).scalar()

    if not latest_date:
        return [], 0

    # 选择相关性字段
    if correlation_type == "prod":
        correlation_field = LeaderboardConsultantUser.mean_prod_correlation
        super_correlation_field = LeaderboardConsultantUser.super_alpha_mean_prod_correlation
    else:
        correlation_field = LeaderboardConsultantUser.mean_self_correlation
        super_correlation_field = LeaderboardConsultantUser.super_alpha_mean_self_correlation

    # 计算平均相关性
    avg_correlation_expr = func.coalesce(
        (func.coalesce(correlation_field, 0) + func.coalesce(super_correlation_field, 0)) / 2,
        0
    )

    # 查询
    query = db.query(
        LeaderboardConsultantUser.user,
        LeaderboardConsultantUser.weight_factor,
        correlation_field,
        super_correlation_field,
        LeaderboardConsultantUser.country,
        LeaderboardConsultantUser.university,
        avg_correlation_expr.label('avg_correlation')
    ).filter(
        LeaderboardConsultantUser.delete_flag == False,
        LeaderboardConsultantUser.record_date == latest_date
    )

    # 国家筛选
    if country:
        query = query.filter(LeaderboardConsultantUser.country == country)

    base_query = query.order_by(desc(avg_correlation_expr))

    # 获取总数
    total = base_query.count()

    # 分页
    offset = (page - 1) * page_size
    results = base_query.limit(page_size).offset(offset).all()

    # 格式化结果
    output = []
    for idx, row in enumerate(results, offset + 1):
        output.append({
            "rank": idx,
            "user": row.user,
            "weight_factor": round(float(row.weight_factor or 0), 2) if row.weight_factor else None,
            "regular_correlation": round(float(getattr(row, correlation_field.name) or 0), 4) if getattr(row, correlation_field.name) else None,
            "super_alpha_correlation": round(float(getattr(row, super_correlation_field.name) or 0), 4) if getattr(row, super_correlation_field.name) else None,
            "avg_correlation": round(float(row.avg_correlation or 0), 4),
            "country": row.country,
            "university": row.university
        })

    return output, total


def get_country_history(db: Session, country: str, page: int = 1, page_size: int = 20) -> Tuple[List[Dict], int]:
    """
    获取某个国家的历史数据变化（分页）

    返回：(按日期倒序排列的历史数据列表, 总数)
    """
    # 基础查询
    base_query = db.query(
        LeaderboardConsultantCountryOrRegion
    ).filter(
        LeaderboardConsultantCountryOrRegion.delete_flag == False,
        LeaderboardConsultantCountryOrRegion.country == country
    ).order_by(
        LeaderboardConsultantCountryOrRegion.record_date.desc()  # 按日期倒序
    )

    # 获取总数
    total = base_query.count()

    # 分页
    offset = (page - 1) * page_size
    results = base_query.limit(page_size).offset(offset).all()

    # 格式化结果
    output = []
    for row in results:
        total_submissions = (row.submissions_count or 0) + (row.super_alpha_submissions_count or 0)

        output.append({
            "record_date": row.record_date.strftime('%Y-%m-%d') if row.record_date else None,
            "user_count": int(row.user or 0),
            "weight_factor": round(float(row.weight_factor or 0), 2),
            "value_factor": round(float(row.value_factor or 0), 2) if row.value_factor is not None else None,
            "submissions_count": int(row.submissions_count or 0),
            "super_alpha_submissions_count": int(row.super_alpha_submissions_count or 0),
            "total_submissions": total_submissions,
            "mean_prod_correlation": round(float(row.mean_prod_correlation or 0), 2) if row.mean_prod_correlation is not None else None,
            "mean_self_correlation": round(float(row.mean_self_correlation or 0), 2) if row.mean_self_correlation is not None else None,
            "super_alpha_mean_prod_correlation": round(float(row.super_alpha_mean_prod_correlation or 0), 2) if row.super_alpha_mean_prod_correlation is not None else None,
            "super_alpha_mean_self_correlation": round(float(row.super_alpha_mean_self_correlation or 0), 2) if row.super_alpha_mean_self_correlation is not None else None
        })

    return output, total
