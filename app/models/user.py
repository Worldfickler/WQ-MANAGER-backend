from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.sql import func
from app.core.database import Base


class SystemUser(Base):
    """系统用户表 - 用于登录认证"""
    __tablename__ = "system_user"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    create_dt = Column(DateTime, server_default=func.now())
    update_dt = Column(DateTime, server_default=func.now(), onupdate=func.now())
    delete_dt = Column(DateTime, nullable=True)
    delete_flag = Column(Boolean, default=False)

    wq_id = Column(String(32), unique=True, nullable=False, index=True, comment="WorldQuant用户ID")
    username = Column(String(64), nullable=True, comment="用户名")
    email = Column(String(128), nullable=True, comment="邮箱")
    is_active = Column(Boolean, default=True, comment="是否激活")

    def __repr__(self):
        return f"<SystemUser(id={self.id}, wq_id={self.wq_id}, username={self.username})>"
