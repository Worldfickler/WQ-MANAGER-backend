from sqlalchemy import BigInteger, CheckConstraint, Column, Numeric, String

from app.core.database import Base


class SponsorDonation(Base):
    """手动维护的项目赞助记录。"""

    __tablename__ = "sponsor_donation"
    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_sponsor_donation_amount_positive"),
    )

    id = Column(BigInteger, primary_key=True, index=True, autoincrement=True)
    wq_id = Column(String(32), nullable=False, index=True, comment="赞助者 WQ_ID")
    amount = Column(Numeric(12, 2), nullable=False, comment="赞助金额")

    def __repr__(self):
        return f"<SponsorDonation(id={self.id}, wq_id={self.wq_id})>"
