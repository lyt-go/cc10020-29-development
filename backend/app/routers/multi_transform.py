"""多规则批量转换数据集 API。

围绕 multi_transform 领域服务暴露：多选规则提交、断点续做、分版本质检、
阻断待复核、版本发布、可用版本查询、字段血缘回查与跨映射源行对照。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.persistence import json_store
from app.services.multi_transform import (
    BATCH_KEY,
    REVIEW_KEY,
    VERSION_KEY,
    MultiTransformService,
)

router = APIRouter(prefix="/api/multi-transform", tags=["多规则批量转换数据集"])


class SubmitBatchRequest(BaseModel):
    batch_no: str
    rule_ids: list[int] = Field(min_length=1)
    source_records: list[dict[str, Any]] = Field(min_length=1)
    source_batch_ref: str = ""


class RunBatchRequest(BaseModel):
    # 允许空 body 调用，预留执行参数
    fail_rule_id: int | None = None
    fail_after_rows: int | None = None


class ResolveReviewRequest(BaseModel):
    remark: str = ""


def _service(payload: RunBatchRequest | None = None) -> MultiTransformService:
    svc = MultiTransformService()
    if payload and payload.fail_rule_id is not None and payload.fail_after_rows is not None:
        # 测试/演练用：模拟某规则处理若干行后中断
        svc.fail_after = {int(payload.fail_rule_id): int(payload.fail_after_rows)}
    return svc


@router.post("/batches")
def submit_batch(request: SubmitBatchRequest) -> dict[str, Any]:
    """多选规则提交：同批同源重提幂等，不产生重复版本成品。"""
    svc = MultiTransformService()
    return svc.submit_batch(
        batch_no=request.batch_no,
        rule_ids=request.rule_ids,
        source_records=request.source_records,
        source_batch_ref=request.source_batch_ref,
    )


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    """列出全部来源批次及其规则版本概况。"""
    batches = json_store.get_all(BATCH_KEY)
    versions = json_store.get_all(VERSION_KEY)
    items = []
    for batch in batches:
        batch_versions = [v for v in versions if v.get("批次编号") == batch.get("批次编号")]
        items.append({
            **batch,
            "版本数": len(batch_versions),
            "可领取版本数": sum(1 for v in batch_versions
                                if v.get("status") in ("质检通过", "已发布")),
            "阻断版本数": sum(1 for v in batch_versions if v.get("status") == "质检阻断"),
        })
    return {"ok": True, "total": len(items), "items": items}


@router.get("/batches/{batch_no}")
def get_batch(batch_no: str) -> dict[str, Any]:
    """批次详情：批次信息 + 全部规则版本。"""
    svc = MultiTransformService()
    batch = svc._find_batch(batch_no)
    if batch is None:
        return {"ok": False, "message": f"批次 {batch_no} 不存在"}
    return {
        "ok": True,
        "batch": batch,
        "versions": svc._versions_of_batch(batch_no),
    }


@router.post("/batches/{batch_no}/run")
def run_batch(batch_no: str, payload: RunBatchRequest | None = None) -> dict[str, Any]:
    """执行（或断点续做）批次下全部未完成版本的转换。"""
    return _service(payload).run_batch(batch_no)


@router.post("/batches/{batch_no}/resume")
def resume_batch(batch_no: str, payload: RunBatchRequest | None = None) -> dict[str, Any]:
    """续做：从未完成版本的断点行继续。"""
    return _service(payload).resume_batch(batch_no)


@router.get("/batches/{batch_no}/availability")
def batch_availability(batch_no: str) -> dict[str, Any]:
    """对账同事查询：本批可领取版本与仍在等待的原因。"""
    return MultiTransformService().get_batch_availability(batch_no)


@router.get("/batches/{batch_no}/compare")
def compare_rules(
    batch_no: str,
    rule_a_id: int = Query(..., description="规则A的 data_transform 规则ID"),
    rule_b_id: int = Query(..., description="规则B的 data_transform 规则ID"),
    source_row_no: int = Query(..., ge=1, description="源行号（1 基）"),
) -> dict[str, Any]:
    """按源行核对两套映射的实际数据与字段差异。"""
    return MultiTransformService().compare_rules_by_source_row(
        batch_no, rule_a_id, rule_b_id, source_row_no)


@router.post("/versions/{version_id}/run")
def run_version(version_id: int, payload: RunBatchRequest | None = None) -> dict[str, Any]:
    """单版本补做/重试（如统计映射修正类型转换后）。"""
    return _service(payload).retry_version(version_id)


@router.post("/versions/{version_id}/repair")
def repair_version(version_id: int, payload: RunBatchRequest | None = None) -> dict[str, Any]:
    """失败行补做：同一版本内原地重做失败行，不产生新成品，补做后需重新质检。"""
    return _service(payload).repair_version(version_id)


@router.post("/versions/{version_id}/quality-check")
def quality_check(version_id: int) -> dict[str, Any]:
    """分版本质量校验：阻断结论回写待复核清单并影响发布。"""
    return MultiTransformService().run_quality_check(version_id)


@router.post("/versions/{version_id}/publish")
def publish_version(version_id: int) -> dict[str, Any]:
    """发布规则版本；质检未通过或有阻断待复核项时拒绝。"""
    return MultiTransformService().publish_version(version_id)


@router.get("/versions/{version_id}/lineage")
def version_lineage(
    version_id: int,
    source_row_no: int = Query(..., ge=1, description="源行号（1 基）"),
) -> dict[str, Any]:
    """回查某版本某源行的实际字段来源（字段级血缘）。"""
    return MultiTransformService().get_field_lineage(version_id, source_row_no)


@router.get("/reviews")
def list_reviews(status: str | None = Query(default=None, description="待复核/已复核")) -> dict[str, Any]:
    """待复核清单查询。"""
    items = json_store.get_all(REVIEW_KEY)
    if status:
        items = [item for item in items if item.get("复核状态") == status]
    return {"ok": True, "total": len(items), "items": items}


@router.post("/reviews/{review_id}/resolve")
def resolve_review(review_id: int, payload: ResolveReviewRequest | None = None) -> dict[str, Any]:
    """处理待复核项；阻断项全部关闭后版本需重新质检。"""
    remark = payload.remark if payload else ""
    return MultiTransformService().resolve_review(review_id, remark)
