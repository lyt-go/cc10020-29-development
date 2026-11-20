"""多规则批量转换数据集测试。

覆盖需求：
1. 多选规则批量转换同源记录，生成多规则转换数据集；
2. 重复提交同批源数据幂等，只保留一个相同版本的成品；
3. 字段级血缘，结果可回查实际字段来源；
4. 转换中断后续做从未完成版本继续，已完成行/版本不重跑；
5. 质量校验跨模块读取多规则数据集、分版本执行，阻断结论回写待复核
   清单并决定版本能否发布；
6. 同批两套规则（对账/统计）场景：可用版本查询与等待原因、
   统计映射补做、按源行核对字段差异、已通过版本重提不产生副本。
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence as persistence
from app.persistence import JsonStore
import app.services.data_transform as dt_mod
import app.services.data_quality_rule as dq_mod
import app.services.multi_transform as mt_mod


@pytest.fixture
def svc():
    """每个测试使用独立临时存储，并把模块级 json_store 单例替换掉。"""
    original_store = persistence.json_store
    tmp_store = JsonStore(base_dir=tempfile.mkdtemp())
    persistence.json_store = tmp_store
    dt_mod.json_store = tmp_store
    dq_mod.json_store = tmp_store
    mt_mod.json_store = tmp_store
    service = mt_mod.MultiTransformService()
    yield service
    # 恢复原始单例，避免污染其他测试文件依赖的默认 data_store
    persistence.json_store = original_store
    dt_mod.json_store = original_store
    dq_mod.json_store = original_store
    mt_mod.json_store = original_store


def _make_rules(store: JsonStore):
    """构造同源于 src 的两套已生效规则：对账映射与统计映射。"""
    rec_rule = store.insert("data_transform", {
        "规则编号": "REC-01", "源Schema": "src", "目标Schema": "recon",
        "字段映射": json.dumps({"order_no": "id", "cust_name": "name"}, ensure_ascii=False),
        "转换函数": "passthrough", "默认值填充": "使用默认值", "异常策略": "跳过",
        "规则版本": "v1", "status": "已生效", "pending": False, "abnormal": False,
    })
    stat_rule = store.insert("data_transform", {
        "规则编号": "STAT-01", "源Schema": "src", "目标Schema": "stat",
        "字段映射": json.dumps({
            "order_id": {"path": "id", "function": "passthrough"},
            "amount": {"path": "amount", "function": "to_integer"},
        }, ensure_ascii=False),
        "转换函数": "passthrough", "默认值填充": "使用默认值", "异常策略": "报错",
        "规则版本": "v1", "status": "已生效", "pending": False, "abnormal": False,
    })
    return rec_rule, stat_rule


def _make_quality_rules(store: JsonStore):
    """阻断型规则定向到统计映射；警告型通用规则。"""
    store.insert("data_quality_rule", {
        "规则编号": "QR-AMT", "数据来源": "STAT-01", "校验字段": "amount",
        "校验表达式": "not_empty", "阈值下界": "0", "阈值上界": "0",
        "阻断策略": "阻断", "规则版本": "v1", "status": "已生效",
        "pending": False, "abnormal": False,
    })
    store.insert("data_quality_rule", {
        "规则编号": "QR-ALL", "数据来源": "ALL", "校验字段": "order_id",
        "校验表达式": "not_empty", "阈值下界": "0", "阈值上界": "0",
        "阻断策略": "警告", "规则版本": "v1", "status": "已生效",
        "pending": False, "abnormal": False,
    })


def _sources():
    return [
        {"id": "A1", "name": "张三", "amount": "100"},
        {"id": "A2", "name": "李四", "amount": "bad"},
        {"id": "A3", "name": "王五", "amount": "300"},
    ]


# ──────────────────────────────────────────────
# 1. 多选规则生成多规则转换数据集
# ──────────────────────────────────────────────
def test_submit_multi_rules_creates_dataset_per_rule(svc):
    rec, stat = _make_rules(persistence.json_store)
    result = svc.submit_batch("B-1", [rec["id"], stat["id"]], _sources())
    assert result["ok"]
    assert len(result["versions"]) == 2
    batch = svc._find_batch("B-1")
    assert batch["源记录数"] == 3 and batch["规则数"] == 2

    run = svc.run_batch("B-1")
    assert run["ok"]
    statuses = {v["规则编号"]: v["status"] for v in run["versions"]}
    assert statuses == {"REC-01": "转换完成", "STAT-01": "转换完成"}
    # 每个版本 3 行，共 6 行成品
    rows = persistence.json_store.get_all(mt_mod.ROW_KEY)
    assert len(rows) == 6
    stat_version = svc._find_version("B-1", stat["id"])
    assert stat_version["成功行数"] == 2 and stat_version["失败行数"] == 1


def test_submit_validates_rules(svc):
    rec, _ = _make_rules(persistence.json_store)
    assert not svc.submit_batch("B-X", [999], _sources())["ok"]
    persistence.json_store.update("data_transform", rec["id"], {"status": "草稿"})
    result = svc.submit_batch("B-Y", [rec["id"]], _sources())
    assert not result["ok"] and "未生效" in result["message"]
    # 校验失败不落地任何批次数据
    assert svc._find_batch("B-Y") is None


# ──────────────────────────────────────────────
# 2. 重复提交幂等
# ──────────────────────────────────────────────
def test_resubmit_same_batch_is_idempotent(svc):
    rec, stat = _make_rules(persistence.json_store)
    sources = _sources()
    first = svc.submit_batch("B-2", [rec["id"], stat["id"]], sources)
    svc.run_batch("B-2")

    second = svc.submit_batch("B-2", [rec["id"], stat["id"]], sources)
    assert second["ok"] and second["resubmitted"]
    assert len(persistence.json_store.get_all(mt_mod.VERSION_KEY)) == 2
    assert len(persistence.json_store.get_all(mt_mod.ROW_KEY)) == 6
    # 规则顺序变化但集合一致，仍视为同一批
    third = svc.submit_batch("B-2", [stat["id"], rec["id"]], sources)
    assert third["resubmitted"]


def test_resubmit_with_changed_data_is_rejected(svc):
    rec, stat = _make_rules(persistence.json_store)
    sources = _sources()
    svc.submit_batch("B-3", [rec["id"], stat["id"]], sources)
    changed = [dict(sources[0], amount="999"), *sources[1:]]
    result = svc.submit_batch("B-3", [rec["id"], stat["id"]], changed)
    assert not result["ok"] and "源数据" in result["message"]
    # 原批次未被污染
    assert svc._find_batch("B-3")["源记录数"] == 3


def test_resubmit_with_changed_ruleset_is_rejected(svc):
    rec, stat = _make_rules(persistence.json_store)
    svc.submit_batch("B-4", [rec["id"], stat["id"]], _sources())
    result = svc.submit_batch("B-4", [rec["id"]], _sources())
    assert not result["ok"] and "规则集合" in result["message"]


# ──────────────────────────────────────────────
# 3. 字段级血缘回查
# ──────────────────────────────────────────────
def test_field_lineage_traces_actual_source(svc):
    rec, stat = _make_rules(persistence.json_store)
    svc.submit_batch("B-5", [rec["id"], stat["id"]], _sources())
    svc.run_batch("B-5")
    stat_version = svc._find_version("B-5", stat["id"])

    lineage = svc.get_field_lineage(stat_version["id"], 1)
    assert lineage["ok"]
    by_target = {item["target_field"]: item for item in lineage["字段血缘"]}
    assert by_target["amount"]["source_path"] == "amount"
    assert by_target["amount"]["source_value"] == "100"
    assert by_target["amount"]["output_value"] == 100  # 字段级 to_integer 已生效
    assert by_target["amount"]["function"] == "to_integer"
    assert by_target["order_id"]["function"] == "passthrough"
    # 失败行同样保留血缘与错误
    failed = svc.get_field_lineage(stat_version["id"], 2)
    assert failed["转换状态"] == "失败"
    assert failed["字段血缘"][1]["status"] == "error"
    assert failed["错误信息"]


# ──────────────────────────────────────────────
# 4. 中断续做：从未完成版本继续
# ──────────────────────────────────────────────
def test_interrupted_batch_resumes_from_unfinished_version(svc):
    rec, stat = _make_rules(persistence.json_store)
    svc.submit_batch("B-6", [rec["id"], stat["id"]], _sources())

    # 统计映射处理 1 行后中断（对账版本应已完整完成）
    svc.fail_after = {stat["id"]: 1}
    first_run = svc.run_batch("B-6")
    assert first_run["interrupted"]
    versions = {v["规则编号"]: v for v in first_run["versions"]}
    assert versions["REC-01"]["status"] == "转换完成"
    assert versions["STAT-01"]["status"] == "转换中断"
    assert versions["STAT-01"]["已完成行数"] == 1
    assert svc._find_batch("B-6")["status"] == "转换中断"

    # 续做：已完成的对账版本跳过，统计版本从第 2 行继续
    svc.fail_after = {}
    resume = svc.resume_batch("B-6")
    assert resume["ok"] and not resume["interrupted"]
    rows = [r for r in persistence.json_store.get_all(mt_mod.ROW_KEY)
            if r["规则编号"] == "STAT-01"]
    assert sorted(r["源行号"] for r in rows) == [1, 2, 3]
    assert len(rows) == 3  # 第 1 行未重跑，没有重复落盘
    assert svc._find_batch("B-6")["status"] == "转换完成"


def test_resume_works_even_when_interrupted_before_first_row(svc):
    rec, stat = _make_rules(persistence.json_store)
    svc.submit_batch("B-7", [rec["id"], stat["id"]], _sources())
    svc.fail_after = {rec["id"]: 0}
    assert svc.run_batch("B-7")["interrupted"]
    svc.fail_after = {}
    resume = svc.resume_batch("B-7")
    assert resume["ok"]
    assert len(persistence.json_store.get_all(mt_mod.ROW_KEY)) == 6


# ──────────────────────────────────────────────
# 5. 分版本质量校验、阻断与发布
# ──────────────────────────────────────────────
def test_quality_check_scoped_per_version_and_blocks_publish(svc):
    rec, stat = _make_rules(persistence.json_store)
    _make_quality_rules(persistence.json_store)
    svc.submit_batch("B-8", [rec["id"], stat["id"]], _sources())
    svc.run_batch("B-8")
    rec_version = svc._find_version("B-8", rec["id"])
    stat_version = svc._find_version("B-8", stat["id"])

    # 对账版本不含 amount，定向规则不应误伤；质检通过并可发布
    rec_qc = svc.run_quality_check(rec_version["id"])
    assert rec_qc["ok"]
    assert svc.publish_version(rec_version["id"])["ok"]

    # 统计版本存在 amount 转换失败行 -> 阻断，结论回写待复核清单
    stat_qc = svc.run_quality_check(stat_version["id"])
    assert not stat_qc["ok"] and stat_qc["open_block_reviews"] == 1
    blocked = persistence.json_store.get_by_id(mt_mod.VERSION_KEY, stat_version["id"])
    assert blocked["status"] == "质检阻断"
    reviews = [r for r in persistence.json_store.get_all(mt_mod.REVIEW_KEY)
               if r["版本ID"] == stat_version["id"]]
    assert reviews and reviews[0]["阻断级别"] == "阻断"
    assert reviews[0]["复核状态"] == "待复核"

    # 阻断版本不可发布
    publish = svc.publish_version(stat_version["id"])
    assert not publish["ok"] and "待复核" in publish["message"]

    # 重复质检不会重复登记同一条复核项
    again_qc = svc.run_quality_check(stat_version["id"])
    assert again_qc["open_block_reviews"] == 1
    assert len([r for r in persistence.json_store.get_all(mt_mod.REVIEW_KEY)
                if r["版本ID"] == stat_version["id"]]) == 1


def test_resolve_review_then_recheck_allows_publish(svc):
    rec, stat = _make_rules(persistence.json_store)
    _make_quality_rules(persistence.json_store)
    svc.submit_batch("B-9", [rec["id"], stat["id"]], _sources())
    svc.run_batch("B-9")
    stat_version = svc._find_version("B-9", stat["id"])
    svc.run_quality_check(stat_version["id"])

    review = next(r for r in persistence.json_store.get_all(mt_mod.REVIEW_KEY)
                  if r["版本ID"] == stat_version["id"])
    resolved = svc.resolve_review(review["id"], "人工确认")
    assert resolved["ok"]
    # 阻断解除后版本回到转换完成，需重新质检
    assert persistence.json_store.get_by_id(
        mt_mod.VERSION_KEY, stat_version["id"])["status"] == "转换完成"
    # 失败行仍在，重新质检依旧阻断
    assert not svc.run_quality_check(stat_version["id"])["ok"]


def test_cannot_quality_check_unfinished_version(svc):
    rec, stat = _make_rules(persistence.json_store)
    svc.submit_batch("B-10", [rec["id"], stat["id"]], _sources())
    stat_version = svc._find_version("B-10", stat["id"])
    result = svc.run_quality_check(stat_version["id"])
    assert not result["ok"] and "完成转换" in result["message"]


# ──────────────────────────────────────────────
# 6. 对账/统计双规则完整业务场景
# ──────────────────────────────────────────────
def test_recon_and_stat_two_rule_business_scenario(svc):
    rec, stat = _make_rules(persistence.json_store)
    _make_quality_rules(persistence.json_store)
    sources = _sources()
    submit = svc.submit_batch("B-11", [rec["id"], stat["id"]], sources)
    rec_version, stat_version = submit["versions"]
    svc.run_batch("B-11")

    # 对账映射先通过并发布；统计映射仍在修正
    svc.run_quality_check(rec_version["id"])
    svc.publish_version(rec_version["id"])
    svc.run_quality_check(stat_version["id"])

    availability = svc.get_batch_availability("B-11")
    assert availability["ok"]
    usable = {v["规则编号"] for v in availability["usable_versions"]}
    waiting = {v["规则编号"]: v["waiting_reason"]
               for v in availability["waiting_versions"]}
    assert usable == {"REC-01"}
    assert "STAT-01" in waiting and "阻断" in waiting["STAT-01"]

    # 统计映射补做：修正源数据后对失败行原地修复，不产生新版本
    fixed = [dict(row, amount="200") if row["id"] == "A2" else row
             for row in sources]
    for item in persistence.json_store.get_all(mt_mod.SOURCE_KEY):
        if item["批次编号"] == "B-11":
            persistence.json_store.update(
                mt_mod.SOURCE_KEY, item["id"], {"源记录": fixed})

    version_count_before = len(persistence.json_store.get_all(mt_mod.VERSION_KEY))
    repair = svc.repair_version(stat_version["id"])
    assert repair["ok"] and repair["repaired"] == 1 and repair["still_failed"] == 0
    assert len(persistence.json_store.get_all(mt_mod.VERSION_KEY)) == version_count_before
    repaired = persistence.json_store.get_by_id(
        mt_mod.VERSION_KEY, stat_version["id"])
    assert repaired["id"] == stat_version["id"]  # 同一版本
    assert repaired["失败行数"] == 0 and repaired["status"] == "转换完成"

    # 重新质检通过后发布
    assert svc.run_quality_check(stat_version["id"])["ok"]
    assert svc.publish_version(stat_version["id"])["ok"]

    # 补做后按源行核对两套映射的实际数据差异
    comparison = svc.compare_rules_by_source_row(
        "B-11", rec["id"], stat["id"], 2)
    assert comparison["ok"]
    # 两套映射目标字段互不重叠（order_no/cust_name vs order_id/amount）
    assert comparison["差异字段数"] == 4
    values = {d["field"]: (d["规则A值"], d["规则B值"])
              for d in comparison["字段差异"]}
    assert values["cust_name"] == ("李四", None)
    assert values["amount"] == (None, 200)
    assert values["order_id"] == (None, "A2")
    # 来源差异可回溯到具体源路径
    assert any(d["field"] == "amount" for d in comparison["来源差异"])

    # 已通过/已发布版本重提不会多出一份成品
    again = svc.submit_batch("B-11", [rec["id"], stat["id"]], sources)
    assert again["resubmitted"]
    assert len(persistence.json_store.get_all(mt_mod.VERSION_KEY)) == 2
    assert len(persistence.json_store.get_all(mt_mod.ROW_KEY)) == 6

    # 全部发布后批次收口
    assert svc._find_batch("B-11")["status"] == "已发布"


def test_repair_rejected_for_unfinished_and_noop_when_no_failure(svc):
    rec, stat = _make_rules(persistence.json_store)
    svc.submit_batch("B-12", [rec["id"], stat["id"]], _sources())
    stat_version = svc._find_version("B-12", stat["id"])
    # 未转换完成不允许补做
    assert not svc.repair_version(stat_version["id"])["ok"]

    svc.run_batch("B-12")
    rec_version = svc._find_version("B-12", rec["id"])
    # 对账版本无失败行 -> 空操作
    noop = svc.repair_version(rec_version["id"])
    assert noop["ok"] and noop["repaired"] == 0
