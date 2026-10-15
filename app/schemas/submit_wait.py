from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TrimmedModel(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)


class PostCreate(TrimmedModel):
    title: str = Field(min_length=1, max_length=120)
    content: str = Field(min_length=1, max_length=10000)
    anonymous: bool = True
    group_id: int | None = Field(default=None, gt=0)
    image_ids: list[int] = Field(default_factory=list, max_length=9)

    @field_validator("image_ids")
    @classmethod
    def unique_images(cls, value: list[int]) -> list[int]:
        if any(item <= 0 for item in value) or len(value) != len(set(value)):
            raise ValueError("图片 ID 必须为不重复的正整数")
        return value


class VoteCreate(TrimmedModel):
    decision: Literal["submit", "hold"]
    reason: str = Field(min_length=1, max_length=1000)
    sticker_ids: list[int] = Field(default_factory=list, max_length=3)

    @field_validator("sticker_ids")
    @classmethod
    def unique_stickers(cls, value: list[int]) -> list[int]:
        if any(item <= 0 for item in value) or len(value) != len(set(value)):
            raise ValueError("表情 ID 必须为不重复的正整数")
        return value


class OutcomeUpdate(TrimmedModel):
    status: Literal["submitted", "held"]
    note: str = Field(default="", max_length=1000)


class GroupCreate(TrimmedModel):
    name: str = Field(min_length=1, max_length=60)


class GroupJoin(TrimmedModel):
    invite_code: str = Field(min_length=1, max_length=16)


class ImageItem(BaseModel):
    id: int
    url: str


class GroupItem(BaseModel):
    id: int
    name: str
    invite_code: str
    member_count: int


class PostItem(BaseModel):
    id: int
    title: str
    content: str
    anonymous: bool
    author_name: str
    is_owner: bool
    group_id: int | None
    group_name: str | None
    status: Literal["open", "submitted", "held"]
    outcome_note: str
    created_at: datetime
    closed_at: datetime | None
    images: list[ImageItem]
    review_count: int
    watch_count: int
    has_watched: bool
    results_visible: bool
    submit_count: int | None
    hold_count: int | None
    my_decision: Literal["submit", "hold"] | None


class PostPage(BaseModel):
    items: list[PostItem]
    total: int
    page: int
    page_size: int


class StickerItem(BaseModel):
    id: int
    name: str
    url: str
    width: int
    height: int


class VoteItem(BaseModel):
    id: int
    reviewer_name: str
    decision: Literal["submit", "hold"]
    reason: str
    created_at: datetime
    is_mine: bool
    flower_count: int
    flowered: bool
    stickers: list[StickerItem] = Field(default_factory=list)


class VotePage(BaseModel):
    items: list[VoteItem]
    total: int
    page: int
    page_size: int


class MyStats(BaseModel):
    posts: int
    reviewed: int
    submitted: int
    held: int
