from sqlalchemy import Column, Integer, String, DateTime, Date, Double, Boolean, BigInteger, Text
from sqlalchemy.sql import func
from app.core.database import Base


class LeaderboardGeniusCountryOrRegion(Base):
    __tablename__ = "leaderboard_genius_country_or_region"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    create_dt = Column(DateTime(timezone=True), server_default=func.now())
    update_dt = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    delete_dt = Column(DateTime(timezone=True), nullable=True)
    delete_flag = Column(Boolean, default=False)
    remark = Column(String(64), nullable=True)

    record_date = Column(Date, nullable=False, index=True)
    rank = Column(Integer, nullable=True)
    users = Column(Integer, nullable=True)
    alpha_count = Column(Integer, nullable=True)
    country = Column(String(32), nullable=True, index=True)

    def __repr__(self):
        return f"<LeaderboardGeniusCountryOrRegion(id={self.id}, country={self.country}, record_date={self.record_date})>"


class LeaderboardConsultantCountryOrRegion(Base):
    __tablename__ = "leaderboard_consultant_country_or_region"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    create_dt = Column(DateTime(timezone=True), server_default=func.now())
    update_dt = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    delete_dt = Column(DateTime(timezone=True), nullable=True)
    delete_flag = Column(Boolean, default=False)
    remark = Column(String(64), nullable=True)

    record_date = Column(Date, nullable=False)
    user = Column(Integer, nullable=True)
    weight_factor = Column(Double, nullable=True)
    value_factor = Column(Double, nullable=True)
    submissions_count = Column(Integer, nullable=True)
    mean_prod_correlation = Column(Double, nullable=True)
    mean_self_correlation = Column(Double, nullable=True)
    super_alpha_submissions_count = Column(Integer, nullable=True)
    super_alpha_mean_prod_correlation = Column(Double, nullable=True)
    super_alpha_mean_self_correlation = Column(Double, nullable=True)
    country = Column(String(32), nullable=True, index=True)

    def __repr__(self):
        return f"<LeaderboardConsultantCountryOrRegion(id={self.id}, country={self.country}, record_date={self.record_date})>"


class LeaderboardConsultantUser(Base):
    __tablename__ = "leaderboard_consultant_user"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    create_dt = Column(DateTime(timezone=True), server_default=func.now())
    update_dt = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    delete_dt = Column(DateTime(timezone=True), nullable=True)
    delete_flag = Column(Boolean, default=False)
    remark = Column(String(64), nullable=True)

    record_date = Column(Date, nullable=False, index=True)
    user = Column(String(16), nullable=False)
    weight_factor = Column(Double, nullable=True)
    value_factor = Column(Double, nullable=True)
    daily_osmosis_rank = Column(Double, nullable=True)
    data_fields_used = Column(Integer, nullable=True)
    submissions_count = Column(Integer, nullable=True)
    mean_prod_correlation = Column(Double, nullable=True)
    mean_self_correlation = Column(Double, nullable=True)
    super_alpha_submissions_count = Column(Integer, nullable=True)
    super_alpha_mean_prod_correlation = Column(Double, nullable=True)
    super_alpha_mean_self_correlation = Column(Double, nullable=True)
    university = Column(String(128), nullable=True)
    country = Column(String(32), nullable=True)

    def __repr__(self):
        return f"<LeaderboardConsultantUser(id={self.id}, user={self.user}, record_date={self.record_date})>"


class LeaderboardGeniusUser(Base):
    __tablename__ = "leaderboard_genius_user"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    create_dt = Column(DateTime(timezone=True), server_default=func.now())
    update_dt = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    delete_dt = Column(DateTime(timezone=True), nullable=True)
    delete_flag = Column(Boolean, default=False)
    remark = Column(String(64), nullable=True)

    record_date = Column(Date, nullable=False, index=True)
    rank = Column(Integer, nullable=True)
    user = Column(String(32), nullable=True, index=True)
    genius_level = Column(String(32), nullable=True, index=True)
    best_level = Column(String(32), nullable=True)
    alpha_count = Column(Integer, nullable=True)
    pyramid_count = Column(Integer, nullable=True)
    combined_alpha_performance = Column(Double, nullable=True)
    combined_power_pool_alpha_performance = Column(Double, nullable=True)
    combined_selected_alpha_performance = Column(Double, nullable=True)
    operator_count = Column(Integer, nullable=True)
    operator_avg = Column(Double, nullable=True)
    field_count = Column(Integer, nullable=True)
    field_avg = Column(Double, nullable=True)
    community_activity = Column(Double, nullable=True)
    max_simulation_streak = Column(Integer, nullable=True)
    country = Column(String(32), nullable=True, index=True)

    def __repr__(self):
        return f"<LeaderboardGeniusUser(id={self.id}, user={self.user}, record_date={self.record_date})>"


class EventUpdateRecord(Base):
    __tablename__ = "event_update_record"

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    update_content = Column(String(32), nullable=True)
    update_date = Column(Date, nullable=True, index=True)
    date_range = Column(String(64), nullable=True)
    remark = Column(Text, nullable=True)

    def __repr__(self):
        return f"<EventUpdateRecord(id={self.id}, update_content={self.update_content}, update_date={self.update_date})>"
