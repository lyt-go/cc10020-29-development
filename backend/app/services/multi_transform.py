"""多规则批量转换数据集领域服务。

在单规则 ``data_transform`` 能力之上，支持同一份源记录批次同时选择多套
转换规则（多选规则），为每套规则生成独立的规则版本成品，并维护：

- 源批次（multi_transform_batch）：同批源数据只登记一次，重复提交幂等；
- 规则版本（multi_transform_version）：一个来源批次 × 一套规则 = 一个版本，
  同批同源重提不会多出一份成品；
- 转换行（multi_transform_row）：逐行持久化，含字段级血缘，可回查实际字段来源；
- 待复核清单（multi_transform_review）：质量校验阻断结论回写，决定版本能否发布。

断点续做：批量过程中断后，续做从未完成版本继续，已完成的行与版本不会重跑。

跨模块：data_transform（规则与字段映射）、data_quality_rule（分版本质量校验）。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.data_quality_rule import DataQualityRuleService
from app.services.data_transform import DataTransformService

BATCH_KEY = "multi_transform_batch"
VERSION_KEY = "multi_transform_version"
ROW_KEY = "multi_transform_row"
REVIEW_KEY = "multi_transform_review"
SOURCE_KEY = "multi_transform_source"

# 版本状态机
VERSION_STATUSES = ("待转换", "转换中断", "转换中", "转换完成", "质检阻断", "质检通过", "已发布")
# 已经产出过成品、重提不应再生成新版本的状态
FINALIZED_STATUSES = ("转换完成", "质检阻断", "质检通过", "已发布")
# 允许领取（正式使用）的状态
USABLE_STATUSES = ("质检通过", "已发布")
# 允许质检重跑的状态
QUALIFIABLE_STATUSES = ("转换完成", "质检阻断", "质检通过")
# 允许发布的状态
PUBLISHABLE_STATUSES = ("质检通过",)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fingerprint(payload: Any) -> str:
    """对任意 JSON 可序列化内容计算稳定指纹，用于幂等判定。"""
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class MultiTransformService:
    """多规则批量转换数据集服务。"""

    def __init__(self) -> None:
        self.transform_svc = DataTransformService()
        self.quality_svc = DataQualityRuleService()
        # 测试钩子：{(rule_id): 在处理到第几条源行（1 基）后抛 RuntimeError}，
        # 用来模拟批量转换中途中断；生产路径不设置即为完整执行。
        self.fail_after: dict[int, int] = {}

    # ──────────────────────────────────────────────
    # 批次提交（多选规则 + 幂等）
    # ──────────────────────────────────────────────
    def submit_batch(
        self,
        batch_no: str,
        rule_ids: list[int],
        source_records: list[dict[str, Any]],
        source_batch_ref: str = "",
    ) -> dict[str, Any]:
        """提交一批源记录并按多套规则生成转换数据集。

        幂等规则：
        - 同 ``batch_no`` 重复提交且源数据指纹、规则集合完全一致：直接返回既有
          批次与版本，不新增任何成品；
        - 同 ``batch_no`` 但源数据/规则发生变化：拒绝（避免同批号下数据含义漂移），
          调用方应使用新批次号或走重试/续做接口；
        - 新批次：为每套已生效规则各创建一个独立规则版本。
        """
        batch_no = str(batch_no or "").strip()
        if not batch_no:
            return {"ok": False, "message": "来源批次号不能为空", "batch_id": None}
        if not rule_ids:
            return {"ok": False, "message": "至少选择一套转换规则", "batch_id": None}
        if not source_records:
            return {"ok": False, "message": "源记录不能为空", "batch_id": None}

        unique_rule_ids = sorted(dict.fromkeys(int(rid) for rid in rule_ids))
        source_hash = fingerprint(source_records)
        # 规则集合是无序概念：排序后取指纹，[A,B] 与 [B,A] 视为同一批规则
        rules_hash = fingerprint(unique_rule_ids)

        existing = self._find_batch(batch_no)
        if existing is not None:
            return self._handle_resubmission(
                existing, source_records, source_hash, rules_hash, unique_rule_ids)

        # 校验所有规则存在且已生效；任一不通过则整批不落地
        rules = self._load_rules(unique_rule_ids)
        if isinstance(rules, str):
            return {"ok": False, "message": rules, "batch_id": None}

        batch = json_store.insert(BATCH_KEY, {
            "批次编号": batch_no,
            "来源批次引用": source_batch_ref or batch_no,
            "源数据指纹": source_hash,
            "规则集合指纹": rules_hash,
            "源记录数": len(source_records),
            "规则数": len(unique_rule_ids),
            "status": "待转换",
            "pending": True,
            "abnormal": False,
            "提交时间": _now(),
            "完成时间": None,
        })
        # 源记录整批留存：中断发生在首行之前也能凭批次续做，且与行内快照互为校验。
        json_store.insert(SOURCE_KEY, {
            "批次编号": batch_no,
            "批次ID": batch["id"],
            "源数据指纹": source_hash,
            "源记录": source_records,
        })

        versions = []
        for rule in rules:
            version = json_store.insert(VERSION_KEY, {
                "批次编号": batch_no,
                "批次ID": batch["id"],
                "规则ID": rule["id"],
                "规则编号": rule.get("规则编号"),
                "规则版本": rule.get("规则版本"),
                "规则快照": {
                    "字段映射": rule.get("字段映射"),
                    "转换函数": rule.get("转换函数"),
                    "异常策略": rule.get("异常策略"),
                    "默认值填充": rule.get("默认值填充"),
                },
                "源数据指纹": source_hash,
                "源记录数": len(source_records),
                "已完成行数": 0,
                "成功行数": 0,
                "失败行数": 0,
                "status": "待转换",
                "pending": True,
                "abnormal": False,
                "质检阻断数": 0,
                "待复核数": 0,
                "最近质检时间": None,
                "发布时间": None,
                "中断原因": "",
            })
            versions.append(version)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 已登记，生成 {len(versions)} 个规则版本，可启动批量转换",
            "batch_id": batch["id"],
            "batch": batch,
            "versions": versions,
            "resubmitted": False,
        }

    def _handle_resubmission(
        self,
        batch: dict[str, Any],
        source_records: list[dict[str, Any]],
        source_hash: str,
        rules_hash: str,
        rule_ids: list[int],
    ) -> dict[str, Any]:
        """同一批次号重复提交的幂等处理。"""
        if batch.get("源数据指纹") != source_hash:
            return {
                "ok": False,
                "message": f"批次 {batch['批次编号']} 已存在且源数据与本次提交不一致，拒绝覆盖",
                "batch_id": batch["id"],
            }
        if batch.get("规则集合指纹") != rules_hash:
            return {
                "ok": False,
                "message": f"批次 {batch['批次编号']} 已存在且规则集合与本次提交不一致，拒绝覆盖",
                "batch_id": batch["id"],
            }

        versions = self._versions_of_batch(batch["批次编号"])
        return {
            "ok": True,
            "message": (
                f"批次 {batch['批次编号']} 已提交过，源数据与规则完全一致，"
                f"复用既有 {len(versions)} 个版本，未生成重复成品"
            ),
            "batch_id": batch["id"],
            "batch": batch,
            "versions": versions,
            "resubmitted": True,
        }

    # ──────────────────────────────────────────────
    # 批量转换执行（逐行落盘 + 断点续做）
    # ──────────────────────────────────────────────
    def run_batch(self, batch_no: str) -> dict[str, Any]:
        """执行批次下全部未完成版本的转换；中断后再次调用即断点续做。"""
        batch = self._find_batch(batch_no)
        if batch is None:
            return {"ok": False, "message": f"批次 {batch_no} 不存在"}

        source_records = self._load_source_records(batch)
        if isinstance(source_records, str):
            return {"ok": False, "message": source_records}

        json_store.update(BATCH_KEY, batch["id"], {"status": "转换中", "pending": True})
        run_versions = []
        interrupted = False
        interrupt_reason = ""

        for version in self._versions_of_batch(batch_no):
            if version["status"] in FINALIZED_STATUSES:
                # 已完成版本（含质检阻断/通过/已发布）续做时直接跳过，不重跑
                run_versions.append(version)
                continue

            outcome = self._run_version(version, source_records)
            run_versions.append(outcome["version"])
            if outcome["interrupted"]:
                interrupted = True
                interrupt_reason = outcome["reason"]
                break  # 中断点：保留已落盘进度，剩余版本等待续做

        self._refresh_batch_status(batch_no)
        refreshed_batch = self._find_batch(batch_no)

        if interrupted:
            return {
                "ok": False,
                "message": f"批次 {batch_no} 转换中断：{interrupt_reason}；已完成进度已保留，可续做",
                "interrupted": True,
                "resume_from": interrupt_reason,
                "batch": refreshed_batch,
                "versions": self._versions_of_batch(batch_no),
            }

        return {
            "ok": True,
            "message": f"批次 {batch_no} 全部规则版本转换完成",
            "interrupted": False,
            "batch": refreshed_batch,
            "versions": self._versions_of_batch(batch_no),
        }

    def resume_batch(self, batch_no: str) -> dict[str, Any]:
        """续做：从未完成版本的断点行继续转换。"""
        return self.run_batch(batch_no)

    def retry_version(self, version_id: int) -> dict[str, Any]:
        """单版本重试（如统计映射修正类型转换后补做）。

        只有未产出成品的版本（待转换/转换中断/转换中）允许重试；
        转换完成及之后状态的版本已有成品，重试不会生成第二份。
        """
        version = json_store.get_by_id(VERSION_KEY, version_id)
        if version is None:
            return {"ok": False, "message": f"规则版本 {version_id} 不存在"}
        if version["status"] in FINALIZED_STATUSES:
            return {
                "ok": False,
                "message": f"版本 {version_id} 状态为 {version['status']}，成品已存在，重试不会重复生成",
                "version": version,
            }

        batch = self._find_batch(version["批次编号"])
        source_records = self._load_source_records(batch)
        if isinstance(source_records, str):
            return {"ok": False, "message": source_records}

        outcome = self._run_version(version, source_records)
        self._refresh_batch_status(version["批次编号"])
        return {
            "ok": not outcome["interrupted"],
            "message": "版本补做完成" if not outcome["interrupted"]
            else f"版本再次中断：{outcome['reason']}",
            "interrupted": outcome["interrupted"],
            "version": outcome["version"],
        }

    def repair_version(self, version_id: int) -> dict[str, Any]:
        """映射修正后的失败行补做：在同一版本内原地重做失败行，不生成新版本/新成品副本。

        典型场景：统计映射的类型转换失败修正后，对该版本此前转换失败的源行
        重新转换并覆盖对应转换行；已成功的行保持不动。补做后版本回到
        「转换完成」，关联的待复核项自动关闭，需要重新分版本质检后才能发布。
        """
        version = json_store.get_by_id(VERSION_KEY, version_id)
        if version is None:
            return {"ok": False, "message": f"规则版本 {version_id} 不存在"}
        if version["status"] not in ("转换完成", "质检阻断", "质检通过"):
            return {
                "ok": False,
                "message": f"版本 {version_id} 状态为 {version['status']}，请先用续做完成转换",
                "version": version,
            }

        batch = self._find_batch(version["批次编号"])
        source_records = self._load_source_records(batch)
        if isinstance(source_records, str):
            return {"ok": False, "message": source_records}

        rule_id = int(version["规则ID"])
        failed_rows = [
            row for row in json_store.get_all(ROW_KEY)
            if int(row.get("版本ID", 0)) == version_id
            and row.get("转换状态") == "失败"
        ]
        if not failed_rows:
            return {
                "ok": True,
                "message": f"版本 {version_id} 没有失败行，无需补做",
                "repaired": 0,
                "version": version,
            }

        repaired = 0
        still_failed = 0
        fail_after = self.fail_after.get(rule_id)
        interrupted = False
        reason = ""
        for row in failed_rows:
            if fail_after is not None and repaired >= fail_after:
                interrupted = True
                reason = f"规则 {version.get('规则编号')} 补做到源行 {row['源行号']} 时中断"
                break
            source_row_no = int(row["源行号"])
            source_record = source_records[source_row_no - 1]
            outcome = self.transform_svc.transform_with_lineage(rule_id, source_record)
            updates = {
                "源记录": source_record,
                "目标数据": outcome["data"],
                "字段血缘": outcome["lineage"],
                "转换状态": "成功" if outcome["ok"] else "失败",
                "错误信息": outcome["errors"],
                "转换时间": _now(),
            }
            json_store.update(ROW_KEY, row["id"], updates)
            if outcome["ok"]:
                repaired += 1
            else:
                still_failed += 1

        all_rows = [
            row for row in json_store.get_all(ROW_KEY)
            if int(row.get("版本ID", 0)) == version_id
        ]
        success = sum(1 for row in all_rows if row.get("转换状态") == "成功")
        failed = sum(1 for row in all_rows if row.get("转换状态") == "失败")

        if interrupted:
            json_store.update(VERSION_KEY, version_id, {
                "status": "转换中断", "pending": True, "abnormal": True,
                "中断原因": reason, "成功行数": success, "失败行数": failed,
            })
            self._refresh_batch_status(version["批次编号"])
            return {
                "ok": False, "interrupted": True, "message": reason,
                "repaired": repaired, "still_failed": still_failed,
                "version": json_store.get_by_id(VERSION_KEY, version_id),
            }

        # 补做完成：关闭该版本既有「阻断级」待复核项（阻断结论已失效，需重新质检）；
        # 警告级条目保留，由重新质检后的结论决定。
        closed_reviews = []
        remaining_open = 0
        for item in json_store.get_all(REVIEW_KEY):
            if int(item.get("版本ID", 0)) == version_id \
                    and item.get("复核状态") == "待复核":
                if item.get("阻断级别") == "阻断":
                    json_store.update(REVIEW_KEY, item["id"], {
                        "复核状态": "已复核",
                        "处理时间": _now(),
                        "处理备注": "映射修正补做后自动关闭，等待重新质检",
                    })
                    closed_reviews.append(item["id"])
                else:
                    remaining_open += 1

        json_store.update(VERSION_KEY, version_id, {
            "status": "转换完成",
            "pending": True,
            "abnormal": False,
            "中断原因": "",
            "成功行数": success,
            "失败行数": failed,
            "质检阻断数": 0,
            "待复核数": remaining_open,
            "最近质检时间": None,
        })
        self._refresh_batch_status(version["批次编号"])
        return {
            "ok": True,
            "interrupted": False,
            "message": (
                f"版本 {version_id} 补做完成：修复 {repaired} 行，仍失败 {still_failed} 行；"
                f"自动关闭 {len(closed_reviews)} 条待复核项，请重新质检"
            ),
            "repaired": repaired,
            "still_failed": still_failed,
            "closed_review_ids": closed_reviews,
            "version": json_store.get_by_id(VERSION_KEY, version_id),
        }

    def _run_version(
        self, version: dict[str, Any], source_records: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """执行单个规则版本，逐行落盘，已完成行跳过。"""
        version_id = version["id"]
        rule_id = int(version["规则ID"])
        json_store.update(VERSION_KEY, version_id, {
            "status": "转换中", "pending": True, "中断原因": ""})

        done_source_rows = {
            int(row["源行号"]) for row in json_store.get_all(ROW_KEY)
            if int(row.get("版本ID", 0)) == version_id
        }
        processed = 0
        success = sum(
            1 for row in json_store.get_all(ROW_KEY)
            if int(row.get("版本ID", 0)) == version_id and row.get("转换状态") == "成功"
        )
        failed = len(done_source_rows) - success
        fail_after = self.fail_after.get(rule_id)
        interrupted = False
        reason = ""

        for index, source_record in enumerate(source_records):
            source_row_no = index + 1
            if source_row_no in done_source_rows:
                continue  # 断点续做：已落盘的行不重跑

            if fail_after is not None and processed >= fail_after:
                interrupted = True
                reason = f"规则 {version.get('规则编号')} 在第 {source_row_no} 行转换中断"
                break

            outcome = self.transform_svc.transform_with_lineage(rule_id, source_record)
            row_status = "成功" if outcome["ok"] else "失败"
            if outcome["ok"]:
                success += 1
            else:
                failed += 1

            json_store.insert(ROW_KEY, {
                "版本ID": version_id,
                "批次编号": version["批次编号"],
                "规则ID": rule_id,
                "规则编号": version.get("规则编号"),
                "源行号": source_row_no,
                "源记录": source_record,
                "目标数据": outcome["data"],
                "字段血缘": outcome["lineage"],
                "转换状态": row_status,
                "错误信息": outcome["errors"],
                "转换时间": _now(),
            })
            processed += 1

            json_store.update(VERSION_KEY, version_id, {
                "已完成行数": len(done_source_rows) + processed,
                "成功行数": success,
                "失败行数": failed,
            })

        final_status = "转换中断" if interrupted else "转换完成"
        json_store.update(VERSION_KEY, version_id, {
            "status": final_status,
            "pending": interrupted,
            "abnormal": interrupted,
            "中断原因": reason,
            "已完成行数": len(done_source_rows) + processed,
            "成功行数": success,
            "失败行数": failed,
        })
        return {
            "interrupted": interrupted,
            "reason": reason,
            "version": json_store.get_by_id(VERSION_KEY, version_id),
        }

    # ──────────────────────────────────────────────
    # 分版本质量校验（跨模块读取多规则转换数据集）
    # ──────────────────────────────────────────────
    def run_quality_check(self, version_id: int) -> dict[str, Any]:
        """对单个规则版本的成品数据集执行质量校验。

        跨模块读取 multi_transform_row 中该版本的全部转换行，逐行调用
        data_quality_rule；阻断策略命中时把结论回写待复核清单，并把版本
        置为「质检阻断」，该版本不可发布。
        """
        version = json_store.get_by_id(VERSION_KEY, version_id)
        if version is None:
            return {"ok": False, "message": f"规则版本 {version_id} 不存在"}
        if version["status"] not in QUALIFIABLE_STATUSES:
            return {
                "ok": False,
                "message": f"版本状态为 {version['status']}，需先完成转换才能质检",
                "version": version,
            }

        rows = [
            row for row in json_store.get_all(ROW_KEY)
            if int(row.get("版本ID", 0)) == version_id
        ]
        # 分版本质检：只取数据来源命中本规则版本（规则编号/规则ID）或通用
        # 来源（ALL/*/空）的质量规则，避免其他映射的规则误阻断本版本。
        allowed_sources = [
            str(version.get("规则编号", "")),
            str(version.get("规则ID", "")),
            "ALL", "*", "",
        ]
        open_reviews = [
            item for item in json_store.get_all(REVIEW_KEY)
            if int(item.get("版本ID", 0)) == version_id
            and item.get("复核状态") == "待复核" and item.get("阻断级别") == "阻断"
        ]

        blocked = 0
        checked = 0
        new_review_ids: list[int] = []
        for row in rows:
            result = self.quality_svc.validate_record(
                row.get("目标数据", {}),
                source_module="data_transform",
                allowed_sources=allowed_sources,
            )
            checked += 1
            if result.get("blocked") or not result.get("passed", True):
                # 同一行已有待复核阻断项则不重复登记
                exists = any(
                    int(item.get("转换行ID", 0)) == row["id"]
                    for item in open_reviews
                )
                if exists:
                    blocked += 1
                    continue
                failed_details = [
                    detail for detail in result.get("details", [])
                    if not detail.get("passed", True)
                ]
                is_block = bool(result.get("blocked")) or any(
                    detail.get("block_policy") == "阻断" for detail in failed_details)
                review = json_store.insert(REVIEW_KEY, {
                    "批次编号": version["批次编号"],
                    "版本ID": version_id,
                    "规则编号": version.get("规则编号"),
                    "转换行ID": row["id"],
                    "源行号": row.get("源行号"),
                    "阻断级别": "阻断" if is_block else "警告",
                    "复核状态": "待复核",
                    "问题字段": [detail.get("field") for detail in failed_details],
                    "问题描述": [
                        detail.get("range_result", {}).get("message")
                        or detail.get("expr_result", {}).get("message")
                        or "质量校验未通过"
                        for detail in failed_details
                    ],
                    "质量评分": result.get("score"),
                    "登记时间": _now(),
                    "处理时间": None,
                    "处理备注": "",
                })
                new_review_ids.append(review["id"])
                if is_block:
                    blocked += 1

        open_block_count = len([
            item for item in json_store.get_all(REVIEW_KEY)
            if int(item.get("版本ID", 0)) == version_id
            and item.get("复核状态") == "待复核"
            and item.get("阻断级别") == "阻断"
        ])
        review_total = len([
            item for item in json_store.get_all(REVIEW_KEY)
            if int(item.get("版本ID", 0)) == version_id
            and item.get("复核状态") == "待复核"
        ])

        if open_block_count > 0:
            new_status = "质检阻断"
            message = f"版本 {version_id} 质检阻断：{open_block_count} 条阻断项待复核，不可发布"
            ok = False
        else:
            new_status = "质检通过"
            message = f"版本 {version_id} 质检通过，共检查 {checked} 行，可发布"
            ok = True

        json_store.update(VERSION_KEY, version_id, {
            "status": new_status,
            "abnormal": open_block_count > 0,
            "pending": open_block_count > 0,
            "质检阻断数": open_block_count,
            "待复核数": review_total,
            "最近质检时间": _now(),
        })
        self._refresh_batch_status(version["批次编号"])

        return {
            "ok": ok,
            "message": message,
            "checked_rows": checked,
            "blocked_rows": blocked,
            "open_block_reviews": open_block_count,
            "new_review_ids": new_review_ids,
            "version": json_store.get_by_id(VERSION_KEY, version_id),
        }

    def resolve_review(self, review_id: int, remark: str = "") -> dict[str, Any]:
        """处理待复核清单项；阻断项关闭后版本需重新质检才能发布。"""
        review = json_store.get_by_id(REVIEW_KEY, review_id)
        if review is None:
            return {"ok": False, "message": f"待复核项 {review_id} 不存在"}
        if review.get("复核状态") != "待复核":
            return {"ok": False, "message": f"待复核项 {review_id} 已处理", "review": review}

        updated = json_store.update(REVIEW_KEY, review_id, {
            "复核状态": "已复核",
            "处理时间": _now(),
            "处理备注": remark or "人工复核确认",
        })
        version_id = int(review["版本ID"])
        version = json_store.get_by_id(VERSION_KEY, version_id)
        open_blocks = len([
            item for item in json_store.get_all(REVIEW_KEY)
            if int(item.get("版本ID", 0)) == version_id
            and item.get("复核状态") == "待复核"
            and item.get("阻断级别") == "阻断"
        ])
        open_total = len([
            item for item in json_store.get_all(REVIEW_KEY)
            if int(item.get("版本ID", 0)) == version_id
            and item.get("复核状态") == "待复核"
        ])
        if open_blocks == 0 and version["status"] == "质检阻断":
            # 阻断结论解除：回到转换完成，等待重新质检
            json_store.update(VERSION_KEY, version_id, {
                "status": "转换完成", "abnormal": False, "pending": True,
                "质检阻断数": 0, "待复核数": open_total,
            })
        else:
            json_store.update(VERSION_KEY, version_id, {"待复核数": open_total})
        self._refresh_batch_status(version["批次编号"])
        return {"ok": True, "message": f"待复核项 {review_id} 已处理", "review": updated}

    def publish_version(self, version_id: int) -> dict[str, Any]:
        """发布规则版本；质检未通过或存在阻断待复核项时拒绝发布。"""
        version = json_store.get_by_id(VERSION_KEY, version_id)
        if version is None:
            return {"ok": False, "message": f"规则版本 {version_id} 不存在"}

        open_blocks = len([
            item for item in json_store.get_all(REVIEW_KEY)
            if int(item.get("版本ID", 0)) == version_id
            and item.get("复核状态") == "待复核"
            and item.get("阻断级别") == "阻断"
        ])
        if open_blocks > 0:
            return {
                "ok": False,
                "message": f"版本 {version_id} 仍有 {open_blocks} 条阻断待复核项，不能发布",
                "version": version,
            }
        if version["status"] not in PUBLISHABLE_STATUSES:
            return {
                "ok": False,
                "message": f"版本 {version_id} 当前状态 {version['status']}，仅质检通过版本可发布",
                "version": version,
            }

        updated = json_store.update(VERSION_KEY, version_id, {
            "status": "已发布", "pending": False, "abnormal": False, "发布时间": _now()})
        self._refresh_batch_status(version["批次编号"])
        return {"ok": True, "message": f"版本 {version_id} 已发布", "version": updated}

    # ──────────────────────────────────────────────
    # 查询：可用版本、血缘回查、跨映射源行对照
    # ──────────────────────────────────────────────
    def get_batch_availability(self, batch_no: str) -> dict[str, Any]:
        """对账同事视角：本批哪些版本可领取，哪些仍在等待及原因。"""
        batch = self._find_batch(batch_no)
        if batch is None:
            return {"ok": False, "message": f"批次 {batch_no} 不存在"}

        waiting_reasons = {
            "待转换": "批量转换尚未启动",
            "转换中": "批量转换进行中",
            "转换中断": "批量转换中断，等待续做",
            "转换完成": "转换已完成，等待分版本质量校验",
            "质检阻断": "质量校验存在阻断项，待复核清单处理后重新质检",
            "质检通过": "质检通过，可领取（待发布）",
            "已发布": "正式版本已发布",
        }
        versions = []
        for version in self._versions_of_batch(batch_no):
            open_blocks = len([
                item for item in json_store.get_all(REVIEW_KEY)
                if int(item.get("版本ID", 0)) == version["id"]
                and item.get("复核状态") == "待复核"
                and item.get("阻断级别") == "阻断"
            ])
            usable = version["status"] in USABLE_STATUSES
            reason = waiting_reasons.get(version["status"], "状态未知")
            if version["status"] == "质检阻断" and open_blocks:
                reason = f"质量校验阻断：{open_blocks} 条待复核项处理完成前不可用"
            versions.append({
                "version_id": version["id"],
                "规则ID": version["规则ID"],
                "规则编号": version.get("规则编号"),
                "规则版本": version.get("规则版本"),
                "status": version["status"],
                "usable": usable,
                "waiting_reason": reason if not usable else "可正式领取",
                "已完成行数": version.get("已完成行数"),
                "源记录数": version.get("源记录数"),
                "成功行数": version.get("成功行数"),
                "失败行数": version.get("失败行数"),
                "待复核数": version.get("待复核数"),
            })

        return {
            "ok": True,
            "批次编号": batch_no,
            "批次状态": batch["status"],
            "usable_versions": [v for v in versions if v["usable"]],
            "waiting_versions": [v for v in versions if not v["usable"]],
            "versions": versions,
        }

    def get_field_lineage(self, version_id: int, source_row_no: int) -> dict[str, Any]:
        """回查某版本某源行的实际字段来源（血缘）。"""
        version = json_store.get_by_id(VERSION_KEY, version_id)
        if version is None:
            return {"ok": False, "message": f"规则版本 {version_id} 不存在"}
        row = self._find_row(version_id, source_row_no)
        if row is None:
            return {
                "ok": False,
                "message": f"版本 {version_id} 中源行 {source_row_no} 尚未转换或不存在",
            }
        return {
            "ok": True,
            "版本ID": version_id,
            "规则编号": version.get("规则编号"),
            "源行号": source_row_no,
            "转换状态": row.get("转换状态"),
            "源记录": row.get("源记录"),
            "目标数据": row.get("目标数据"),
            "字段血缘": row.get("字段血缘"),
            "错误信息": row.get("错误信息"),
        }

    def compare_rules_by_source_row(
        self, batch_no: str, rule_a_id: int, rule_b_id: int, source_row_no: int
    ) -> dict[str, Any]:
        """按源行核对两套映射的实际数据与字段差异。"""
        version_a = self._find_version(batch_no, int(rule_a_id))
        version_b = self._find_version(batch_no, int(rule_b_id))
        missing = [
            f"规则 {rid} 在批次 {batch_no} 下没有对应版本"
            for rid, ver in ((rule_a_id, version_a), (rule_b_id, version_b)) if ver is None
        ]
        if missing:
            return {"ok": False, "message": "；".join(missing)}

        row_a = self._find_row(version_a["id"], source_row_no)
        row_b = self._find_row(version_b["id"], source_row_no)
        missing_rows = [
            f"{name}（规则 {rid}）源行 {source_row_no} 尚未转换"
            for name, rid, row in (("版本A", rule_a_id, row_a), ("版本B", rule_b_id, row_b))
            if row is None
        ]
        if missing_rows:
            return {"ok": False, "message": "；".join(missing_rows)}

        data_a = row_a.get("目标数据", {})
        data_b = row_b.get("目标数据", {})
        fields = sorted(set(data_a) | set(data_b))
        field_diffs = []
        for field in fields:
            in_a = field in data_a
            in_b = field in data_b
            same = in_a and in_b and data_a[field] == data_b[field]
            field_diffs.append({
                "field": field,
                "规则A值": data_a.get(field) if in_a else None,
                "规则B值": data_b.get(field) if in_b else None,
                "规则A存在": in_a,
                "规则B存在": in_b,
                "一致": same,
            })

        lineage_a = {item["target_field"]: item for item in row_a.get("字段血缘", [])}
        lineage_b = {item["target_field"]: item for item in row_b.get("字段血缘", [])}
        source_diffs = []
        for field in fields:
            la = lineage_a.get(field)
            lb = lineage_b.get(field)
            if la is None and lb is None:
                continue
            if (la or {}).get("source_path") != (lb or {}).get("source_path") \
                    or (la or {}).get("source_value") != (lb or {}).get("source_value"):
                source_diffs.append({
                    "field": field,
                    "规则A来源": {"path": (la or {}).get("source_path"),
                                 "value": (la or {}).get("source_value")},
                    "规则B来源": {"path": (lb or {}).get("source_path"),
                                 "value": (lb or {}).get("source_value")},
                })

        return {
            "ok": True,
            "批次编号": batch_no,
            "源行号": source_row_no,
            "规则A": {"规则ID": rule_a_id, "规则编号": version_a.get("规则编号"),
                      "版本ID": version_a["id"], "status": version_a["status"]},
            "规则B": {"规则ID": rule_b_id, "规则编号": version_b.get("规则编号"),
                      "版本ID": version_b["id"], "status": version_b["status"]},
            "源记录": row_a.get("源记录"),
            "字段差异": field_diffs,
            "差异字段数": sum(1 for diff in field_diffs if not diff["一致"]),
            "来源差异": source_diffs,
        }

    # ──────────────────────────────────────────────
    # 辅助方法
    # ──────────────────────────────────────────────
    def _load_rules(self, rule_ids: list[int]) -> list[dict[str, Any]] | str:
        rules = []
        for rule_id in rule_ids:
            rule = json_store.get_by_id("data_transform", rule_id)
            if rule is None:
                return f"转换规则 {rule_id} 不存在"
            if rule.get("status") != "已生效":
                return f"转换规则 {rule.get('规则编号', rule_id)} 未生效，不能参与批量转换"
            rules.append(rule)
        return rules

    def _find_batch(self, batch_no: str) -> dict[str, Any] | None:
        for row in json_store.get_all(BATCH_KEY):
            if row.get("批次编号") == batch_no:
                return row
        return None

    def _versions_of_batch(self, batch_no: str) -> list[dict[str, Any]]:
        return sorted(
            (row for row in json_store.get_all(VERSION_KEY)
             if row.get("批次编号") == batch_no),
            key=lambda row: int(row["id"]),
        )

    def _find_version(self, batch_no: str, rule_id: int) -> dict[str, Any] | None:
        for row in json_store.get_all(VERSION_KEY):
            if row.get("批次编号") == batch_no and int(row.get("规则ID", 0)) == rule_id:
                return row
        return None

    def _find_row(self, version_id: int, source_row_no: int) -> dict[str, Any] | None:
        for row in json_store.get_all(ROW_KEY):
            if int(row.get("版本ID", 0)) == version_id \
                    and int(row.get("源行号", 0)) == int(source_row_no):
                return row
        return None

    def _load_source_records(self, batch: dict[str, Any]) -> list[dict[str, Any]] | str:
        """加载批次源记录；优先读批次级留存，缺失时从已落盘转换行重建。

        源记录在提交时整批留存、转换时随每行持久化，因此续做/重试不依赖
        调用方再次提交数据，且每行都保留了实际源数据用于血缘回查。
        """
        for item in json_store.get_all(SOURCE_KEY):
            if item.get("批次编号") == batch["批次编号"]:
                records = item.get("源记录", [])
                if records:
                    return records

        rows = [
            row for row in json_store.get_all(ROW_KEY)
            if row.get("批次编号") == batch["批次编号"]
        ]
        if not rows:
            return (
                f"批次 {batch['批次编号']} 尚未转换任何行且源记录留存缺失；"
                "请重新提交批次数据"
            )
        by_row: dict[int, dict[str, Any]] = {}
        for row in rows:
            by_row[int(row["源行号"])] = row["源记录"]
        return [by_row[no] for no in sorted(by_row)]

    def _refresh_batch_status(self, batch_no: str) -> None:
        """根据各规则版本状态汇总批次状态。"""
        batch = self._find_batch(batch_no)
        if batch is None:
            return
        versions = self._versions_of_batch(batch_no)
        statuses = [v["status"] for v in versions]

        if all(s == "已发布" for s in statuses):
            new_status, pending, abnormal = "已发布", False, False
        elif any(s == "转换中断" for s in statuses):
            new_status, pending, abnormal = "转换中断", True, True
        elif any(s in ("待转换", "转换中") for s in statuses):
            new_status, pending, abnormal = "转换中", True, False
        elif any(s == "质检阻断" for s in statuses):
            new_status, pending, abnormal = "质检阻断", True, True
        elif all(s in ("质检通过", "已发布") for s in statuses):
            new_status, pending, abnormal = "质检通过", False, False
        elif all(s in FINALIZED_STATUSES for s in statuses):
            # 全部转换完成且无阻断，但尚有版本未质检
            new_status, pending, abnormal = "转换完成", True, False
        else:
            new_status, pending, abnormal = "待转换", True, False

        updates: dict[str, Any] = {"status": new_status, "pending": pending, "abnormal": abnormal}
        if new_status in ("质检通过", "已发布"):
            updates["完成时间"] = _now()
        json_store.update(BATCH_KEY, batch["id"], updates)
