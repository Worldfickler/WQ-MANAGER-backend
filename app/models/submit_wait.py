from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Index, Integer, String, Text,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from app.core.database import Base


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


class SubmitWaitGroup(Base):
    __tablename__ = "submit_wait_group"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(60), nullable=False)
    invite_code = Column(String(16), nullable=False, unique=True)
    owner_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=_utcnow, server_default=func.now())


class SubmitWaitMember(Base):
    __tablename__ = "submit_wait_member"
    __table_args__ = (UniqueConstraint("group_id", "user_id", name="uq_sw_member"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, ForeignKey("submit_wait_group.id"), nullable=False)
    user_id = Column(Integer, nullable=False, index=True)


class SubmitWaitPost(Base):
    __tablename__ = "submit_wait_post"
    __table_args__ = (
        Index("ix_sw_post_feed", "group_id", "deleted", "created_at"),
        Index("ix_sw_post_author", "author_id", "deleted"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    author_id = Column(Integer, nullable=False)
    group_id = Column(Integer, ForeignKey("submit_wait_group.id"), nullable=True)
    title = Column(String(120), nullable=False)
    content = Column(Text, nullable=False)
    anonymous = Column(Boolean, nullable=False, default=True)
    status = Column(String(16), nullable=False, default="open")
    outcome_note = Column(String(1000), nullable=False, default="")
    deleted = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, nullable=False, default=_utcnow, server_default=func.now())
    closed_at = Column(DateTime, nullable=True)


class SubmitWaitImage(Base):
    __tablename__ = "submit_wait_image"

    id = Column(Integer, primary_key=True, autoincrement=True)
    uploader_id = Column(Integer, nullable=False, index=True)
    post_id = Column(Integer, ForeignKey("submit_wait_post.id"), nullable=True, index=True)
    object_name = Column(String(512), nullable=False)
    created_at = Column(DateTime, nullable=False, default=_utcnow, server_default=func.now())


class SubmitWaitVote(Base):
    __tablename__ = "submit_wait_vote"
    __table_args__ = (UniqueConstraint("post_id", "user_id", name="uq_sw_vote"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey("submit_wait_post.id"), nullable=False)
    user_id = Column(Integer, nullable=False, index=True)
    decision = Column(String(16), nullable=False)
    reason = Column(String(1000), nullable=False)
    created_at = Column(DateTime, nullable=False, default=_utcnow, server_default=func.now())


class SubmitWaitFlower(Base):
    __tablename__ = "submit_wait_flower"
    __table_args__ = (UniqueConstraint("vote_id", "user_id", name="uq_sw_flower"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    vote_id = Column(Integer, ForeignKey("submit_wait_vote.id"), nullable=False)
    user_id = Column(Integer, nullable=False)


class SubmitWaitWatch(Base):
    __tablename__ = "submit_wait_watch"
    __table_args__ = (UniqueConstraint("post_id", "user_id", name="uq_sw_watch"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_id = Column(Integer, ForeignKey("submit_wait_post.id"), nullable=False)
    user_id = Column(Integer, nullable=False, index=True)
    created_at = Column(DateTime, nullable=False, default=_utcnow, server_default=func.now())


class SubmitWaitSticker(Base):
    __tablename__ = "submit_wait_sticker"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(60), nullable=False)
    object_name = Column(String(255), nullable=False, unique=True)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    sort_order = Column(Integer, nullable=False, default=0)
    active = Column(Boolean, nullable=False, default=True)


class SubmitWaitVoteSticker(Base):
    __tablename__ = "submit_wait_vote_sticker"
    __table_args__ = (UniqueConstraint("vote_id", "sticker_id", name="uq_sw_vote_sticker"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    vote_id = Column(Integer, ForeignKey("submit_wait_vote.id"), nullable=False)
    sticker_id = Column(Integer, ForeignKey("submit_wait_sticker.id"), nullable=False)
    position = Column(Integer, nullable=False, default=0)
