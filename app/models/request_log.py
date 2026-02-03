from sqlalchemy import Column, Integer, String, DateTime, Text, Float
from sqlalchemy.sql import func
from app.core.database import Base


class RequestLog(Base):
    """API请求日志表"""
    __tablename__ = "request_log"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    create_dt = Column(DateTime, server_default=func.now(), comment="创建时间")

    # 用户信息
    user_id = Column(Integer, nullable=True, index=True, comment="用户ID")
    wq_id = Column(String(32), nullable=True, index=True, comment="WQ_ID")

    # 请求信息
    method = Column(String(16), nullable=False, comment="请求方法: GET/POST/PUT/DELETE")
    path = Column(String(512), nullable=False, index=True, comment="请求路径")
    query_params = Column(Text, nullable=True, comment="查询参数")
    body = Column(Text, nullable=True, comment="请求体")

    # 响应信息
    status_code = Column(Integer, nullable=False, index=True, comment="HTTP状态码")
    response_time = Column(Float, nullable=False, comment="响应时间(毫秒)")

    # 额外信息
    ip_address = Column(String(64), nullable=True, comment="客户端IP")
    user_agent = Column(String(512), nullable=True, comment="用户代理")

    def __repr__(self):
        return f"<RequestLog(id={self.id}, method={self.method}, path={self.path}, status={self.status_code})>"
