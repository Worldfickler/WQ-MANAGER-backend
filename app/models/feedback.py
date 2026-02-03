from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text
from sqlalchemy.sql import func

from app.core.database import Base


class UserFeedback(Base):
    """用户反馈表"""
    __tablename__ = "user_feedback"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    create_dt = Column(DateTime, server_default=func.now(), comment="创建时间")
    update_dt = Column(DateTime, server_default=func.now(), onupdate=func.now(), comment="更新时间")
    delete_dt = Column(DateTime, nullable=True)
    delete_flag = Column(Boolean, default=False)

    user_id = Column(Integer, nullable=True, index=True, comment="用户ID")
    wq_id = Column(String(32), nullable=True, index=True, comment="WQ_ID")
    username = Column(String(64), nullable=True, comment="用户名")

    content = Column(Text, nullable=False, comment="反馈内容")
    feedback_type = Column(String(32), nullable=False, default="bug", comment="反馈类型")
    page = Column(String(256), nullable=True, comment="页面路径")
    contact = Column(String(256), nullable=True, comment="联系方式")
    status = Column(String(32), nullable=False, default="new", comment="处理状态")

    def __repr__(self):
        return f"<UserFeedback(id={self.id}, user_id={self.user_id}, status={self.status})>"
