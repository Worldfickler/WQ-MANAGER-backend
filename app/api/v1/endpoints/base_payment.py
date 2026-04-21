from datetime import datetime

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.user import SystemUser
from app.schemas.base_payment import (
    BasePaymentDashboardResponse,
    BasePaymentImageUploadResponse,
    BasePaymentLeaderboardResponse,
    BasePaymentMyStatusResponse,
    BasePaymentUploadRequest,
    BasePaymentUploadResponse,
)
from app.services import base_payment_service, minio_storage_service

router = APIRouter()


@router.get("/dashboard", response_model=BasePaymentDashboardResponse)
async def get_base_payment_dashboard(
    record_date: str | None = Query(None, description="记录日期（YYYY-MM-DD），不传则默认今天"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    target_date = base_payment_service.get_today_record_date()
    if record_date:
        try:
            target_date = datetime.strptime(record_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="record_date 必须是 YYYY-MM-DD 格式")

    has_uploaded = await base_payment_service.has_uploaded_on_date(db, current_user.wq_id, target_date)
    if not has_uploaded:
        raise HTTPException(
            status_code=403,
            detail=f"未上传 {target_date.isoformat()} 的 base payment，无法查看该日数据",
        )

    return await base_payment_service.get_dashboard_summary(
        db=db,
        viewer_wq_id=current_user.wq_id,
        target_date=target_date,
    )


@router.get("/my-today", response_model=BasePaymentMyStatusResponse)
async def get_my_today_payment_status(
    record_date: str | None = Query(None, description="记录日期（YYYY-MM-DD），不传则默认今天"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    target_date = base_payment_service.get_today_record_date()
    if record_date:
        try:
            target_date = datetime.strptime(record_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="record_date 必须是 YYYY-MM-DD 格式")

    record = await base_payment_service.get_user_record_by_date(db, current_user.wq_id, target_date)
    if record is None:
        return {
            "has_uploaded_for_date": False,
            "record_date": target_date.isoformat(),
            "data": None,
        }

    return {
        "has_uploaded_for_date": True,
        "record_date": target_date.isoformat(),
        "data": base_payment_service.serialize_payment_record(record, current_user.wq_id),
    }


@router.post("/upload-images", response_model=BasePaymentImageUploadResponse)
async def upload_base_payment_images(
    files: list[UploadFile] = File(..., description="图片文件列表"),
    record_date: str | None = Form(None, description="记录日期（YYYY-MM-DD）"),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    _ = db
    target_date = base_payment_service.get_today_record_date()
    if record_date:
        try:
            target_date = datetime.strptime(record_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="record_date 必须是 YYYY-MM-DD 格式")

    if not files:
        raise HTTPException(status_code=400, detail="请至少上传一张图片")

    if len(files) > 9:
        raise HTTPException(status_code=400, detail="最多上传 9 张图片")

    uploaded = await minio_storage_service.upload_base_payment_images(
        files=files,
        wq_id=current_user.wq_id,
        record_date=target_date,
    )

    return {
        "success": True,
        "message": "图片上传成功",
        "items": [
            {
                "file_name": item.file_name,
                "object_name": item.object_name,
                "content_type": item.content_type,
                "size": item.size,
                "url": item.url,
            }
            for item in uploaded
        ],
    }


@router.post("/upload", response_model=BasePaymentUploadResponse)
async def upload_my_base_payment(
    payload: BasePaymentUploadRequest,
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    record, is_created = await base_payment_service.upsert_user_payment_by_date(
        db=db,
        wq_id=current_user.wq_id,
        record_date=payload.record_date,
        anonymity=payload.anonymity,
        regular_payment=payload.regular_payment,
        super_payment=payload.super_payment,
        regular_count=payload.regular_count,
        super_count=payload.super_count,
        picture=payload.picture,
        pictures=payload.pictures,
        value_factor=payload.value_factor,
        daily_osmosis_rank=payload.daily_osmosis_rank,
    )

    return {
        "success": True,
        "message": "上传成功" if is_created else "更新成功",
        "data": base_payment_service.serialize_payment_record(record, current_user.wq_id),
    }


@router.get("/leaderboard", response_model=BasePaymentLeaderboardResponse)
async def get_base_payment_leaderboard(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(50, ge=1, le=200, description="每页数量"),
    start_date: str | None = Query(None, description="开始日期（YYYY-MM-DD）"),
    end_date: str | None = Query(None, description="结束日期（YYYY-MM-DD）"),
    sort_by: str = Query(
        "total_payment",
        regex="^(total_payment|regular_payment|super_payment|regular_count|super_count|value_factor|daily_osmosis_rank)$",
        description="排序字段",
    ),
    sort_order: str = Query(
        "desc",
        regex="^(desc|asc)$",
        description="排序方向",
    ),
    db: AsyncSession = Depends(get_db),
    current_user: SystemUser = Depends(get_current_user),
):
    parsed_start_date = None
    parsed_end_date = None
    if start_date:
        try:
            parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="start_date 必须是 YYYY-MM-DD 格式")

    if end_date:
        try:
            parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="end_date 必须是 YYYY-MM-DD 格式")

    permission_date = parsed_start_date or parsed_end_date or base_payment_service.get_today_record_date()
    has_uploaded = await base_payment_service.has_uploaded_on_date(db, current_user.wq_id, permission_date)
    if not has_uploaded:
        raise HTTPException(
            status_code=403,
            detail=f"未上传 {permission_date.isoformat()} 的 base payment，无法查看该日数据",
        )

    if parsed_start_date and parsed_end_date and parsed_start_date != parsed_end_date:
        raise HTTPException(status_code=400, detail="当前仅支持单天查询，请保证 start_date 与 end_date 相同")

    return await base_payment_service.get_leaderboard(
        db=db,
        viewer_wq_id=current_user.wq_id,
        page=page,
        page_size=page_size,
        start_date=parsed_start_date,
        end_date=parsed_end_date,
        sort_by=sort_by,
        sort_order=sort_order,
    )
