from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Double,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from app.core.database import Base


class BasePayment(Base):
    """base payment记录表"""

    __tablename__ = "base_payment"
    __table_args__ = (
        UniqueConstraint("record_date", "wq_id", name="uk_user_record_date"),
    )

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    create_dt = Column(DateTime, server_default=func.now(), comment="创建时间")
    update_dt = Column(DateTime, server_default=func.now(), onupdate=func.now(), comment="更改时间")
    delete_dt = Column(DateTime, nullable=True, comment="删除时间")
    delete_flag = Column(Boolean, default=False, comment="删除标记，0表示未删除，1表示已删除")
    remark = Column(String(64), nullable=True, comment="备注信息")

    record_date = Column(Date, nullable=False, index=True, comment="记录时间")
    wq_id = Column(String(32), nullable=True, index=True, comment="wq平台用户id")
    anonymity = Column(Integer, nullable=True, comment="是否匿名（0-匿名，1-不匿名）")
    regular_payment = Column(Double, nullable=True, comment="regular收益")
    super_payment = Column(Double, nullable=True, comment="super收益")
    regular_count = Column(Integer, nullable=True, comment="regular数量")
    super_count = Column(Integer, nullable=True, comment="super数量")
    picture = Column(Text, nullable=True, comment="图片url")
    value_factor = Column(Double, nullable=True, comment="value factor值")
    daily_osmosis_rank = Column(Double, nullable=True, comment="osmosis每日分数")

    def __repr__(self):
        return f"<BasePayment(id={self.id}, wq_id={self.wq_id}, record_date={self.record_date})>"
