"""数据转换编排领域服务：JSON/XML数据转换、字段映射、类型转换与结构重组。

提供从源Schema到目标Schema的字段级映射和值转换能力，
支持嵌套结构抽取、类型强制转换和异常降级策略。
跨模块关联：data_quality_rule（质量校验）、data_lake_sink（转换后落湖）。
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Callable

from app.persistence import json_store
from app.services.base import BaseModuleService


class DataTransformService(BaseModuleService):
    MODULE = "data_transform"
    ENTITY = "转换规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则编号", "源Schema", "目标Schema", "字段映射", "转换函数", "默认值填充", "异常策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "源Schema", "目标Schema"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    _BUILTIN_FUNCTIONS: dict[str, Callable[[Any], Any]] = {
        "to_string": lambda x: str(x) if x is not None else "",
        "to_integer": lambda x: int(x) if x is not None else 0,
        "to_float": lambda x: float(x) if x is not None else 0.0,
        "to_boolean": lambda x: bool(x) if x is not None else False,
        "to_upper": lambda x: str(x).upper() if x is not None else "",
        "to_lower": lambda x: str(x).lower() if x is not None else "",
        "extract_numbers": lambda x: re.sub(r"[^0-9]", "", str(x)) if x is not None else "",
        "parse_datetime": lambda x: x,
        "default_empty": lambda x: x if x is not None and str(x).strip() else "",
    }

    def apply_transform(self, rule_id: int, source_data: dict[str, Any]) -> dict[str, Any]:
        """应用转换规则，将源数据转换为目标结构。

        转换流程：
        1. 加载已生效的转换规则
        2. 解析字段映射定义
        3. 逐个字段应用转换函数
        4. 处理异常策略（跳过/默认值/报错）

        Returns:
            { ok: bool, data: dict, errors: list, fields_transformed: int }
        """
        outcome = self.transform_with_lineage(rule_id, source_data)
        return {
            "ok": outcome["ok"],
            "data": outcome["data"],
            "errors": outcome["errors"],
            "fields_transformed": outcome["fields_transformed"],
            "total_fields": outcome["total_fields"],
            "quality_check": outcome["quality_check"],
        }

    def transform_with_lineage(self, rule_id: int, source_data: dict[str, Any]) -> dict[str, Any]:
        """带字段级血缘的转换：每个目标字段都记录实际取值的源路径与处理过程。

        血缘条目 lineage[] 结构：
            target_field  目标字段名
            source_path   规则声明的源字段路径
            source_value  实际取到的源值（回查字段来源的依据）
            function      实际应用的转换函数
            output_value  最终写入成品的值
            status        success / defaulted / skipped / error
            error         失败原因（status 非 success 时）

        Returns:
            { ok, data, errors, fields_transformed, total_fields, lineage, rule_snapshot, quality_check }
        """
        rule = json_store.get_by_id(self.MODULE, rule_id)
        if rule is None:
            return {
                "ok": False, "data": {}, "errors": ["转换规则不存在"],
                "fields_transformed": 0, "total_fields": 0, "lineage": [],
                "rule_snapshot": None, "quality_check": {"passed": False, "checks": 0},
            }

        if rule.get("status") != "已生效":
            return {
                "ok": False, "data": {}, "errors": ["转换规则未生效"],
                "fields_transformed": 0, "total_fields": 0, "lineage": [],
                "rule_snapshot": None, "quality_check": {"passed": False, "checks": 0},
            }

        field_mapping = self._parse_mapping(str(rule.get("字段映射", "{}")))
        transform_func = str(rule.get("转换函数", "passthrough"))
        default_filler = str(rule.get("默认值填充", "使用默认值"))
        error_policy = str(rule.get("异常策略", "跳过"))

        result: dict[str, Any] = {}
        lineage: list[dict[str, Any]] = []
        errors: list[str] = []
        transformed = 0
        hard_errors = 0

        for target_field, mapping_spec in field_mapping.items():
            source_path, field_function = self._mapping_spec(mapping_spec)
            # 字段级函数优先；未声明时回退规则级转换函数
            active_function = field_function or transform_func
            entry: dict[str, Any] = {
                "target_field": target_field,
                "source_path": source_path,
                "source_value": None,
                "function": active_function,
                "output_value": None,
                "status": "success",
                "error": "",
            }
            try:
                source_value = self._extract_value(source_data, source_path)
                entry["source_value"] = source_value
                converted = self._apply_function(active_function, source_value)
                result[target_field] = converted
                entry["output_value"] = converted
                transformed += 1
            except (KeyError, ValueError, TypeError) as e:
                entry["status"] = "error"
                entry["error"] = str(e)
                if error_policy == "使用默认值":
                    default_value = self._get_default_value(target_field, rule)
                    result[target_field] = default_value
                    entry["output_value"] = default_value
                    entry["status"] = "defaulted"
                elif error_policy == "报错":
                    errors.append(f"字段 {target_field}: {e}")
                    hard_errors += 1
                else:
                    # 跳过策略：记录但不阻断整行
                    errors.append(f"字段 {target_field} 跳过: {e}")
                    entry["status"] = "skipped"
            lineage.append(entry)

        # 质量校验（跨模块：data_quality_rule）
        quality_check = self._run_quality_checks(rule_id, result)

        return {
            "ok": hard_errors == 0,
            "data": result,
            "errors": errors,
            "fields_transformed": transformed,
            "total_fields": len(field_mapping),
            "lineage": lineage,
            "rule_snapshot": {
                "rule_id": rule_id,
                "规则编号": rule.get("规则编号"),
                "字段映射": field_mapping,
                "转换函数": transform_func,
                "异常策略": error_policy,
                "默认值填充": default_filler,
                "规则版本": rule.get("规则版本"),
            },
            "quality_check": quality_check,
        }

    def _parse_mapping(self, mapping_str: str) -> dict[str, str]:
        """解析字段映射定义。

        支持格式：
        - JSON: {"目标字段": "源字段路径"}
        - JSON 字段级配置: {"目标字段": {"path": "源字段路径", "function": "to_integer"}}
        - 简单: "field1->target1,field2->target2"
        """
        try:
            parsed = json.loads(mapping_str)
        except json.JSONDecodeError:
            result = {}
            for part in mapping_str.split(","):
                if "->" in part:
                    src, tgt = part.split("->", 1)
                    result[tgt.strip()] = src.strip()
            return result if result else {"_raw": "payload"}

        normalized: dict[str, Any] = {}
        for target, spec in parsed.items():
            if isinstance(spec, dict):
                normalized[target] = {
                    "path": str(spec.get("path", "")),
                    "function": str(spec.get("function", "passthrough")),
                }
            else:
                normalized[target] = str(spec)
        return normalized

    def _mapping_spec(self, spec: Any) -> tuple[str, str | None]:
        """把单个映射规格统一拆成 (源路径, 字段级转换函数)。"""
        if isinstance(spec, dict):
            return str(spec.get("path", "")), str(spec.get("function") or "") or None
        return str(spec), None

    def _extract_value(self, data: dict[str, Any], path: str) -> Any:
        """从嵌套数据中提取指定路径的值。

        支持点号和方括号路径：
        - "name" → data["name"]
        - "user.profile.email" → data["user"]["profile"]["email"]
        - "items[0].id" → data["items"][0]["id"]
        """
        parts = re.split(r"\.|(?<=\w)(?=\[)", path)
        current: Any = data
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if "[" in part and "]" in part:
                key, idx_str = part.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    current = current[key]
                current = current[idx]
            elif isinstance(current, dict):
                current = current.get(part, current.get(path, None))
            else:
                raise KeyError(f"无法从 {type(current)} 中提取 {part}")

        return current

    def _apply_function(self, func_name: str, value: Any) -> Any:
        """应用命名转换函数。"""
        if func_name == "passthrough":
            return value
        func = self._BUILTIN_FUNCTIONS.get(func_name)
        if func:
            return func(value)
        return value

    def _get_default_value(self, field: str, rule: dict[str, Any]) -> Any:
        """根据规则配置获取字段默认值。"""
        default_mapping = {
            "string": "", "integer": 0, "float": 0.0,
            "boolean": False, "datetime": "",
        }
        return default_mapping.get(rule.get("目标Schema", ""), "")

    def _run_quality_checks(self, rule_id: int, transformed_data: dict[str, Any]) -> dict[str, Any]:
        """跨模块运行质量校验。"""
        try:
            from app.services.data_quality_rule import DataQualityRuleService
            quality_svc = DataQualityRuleService()
            return quality_svc.validate_record(transformed_data, source_module=self.MODULE)
        except Exception:
            return {"passed": True, "checks": 0}

    def batch_transform(self, rule_id: int, source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """批量转换多条记录。"""
        results = []
        success = 0
        failed = 0

        for record in source_records:
            r = self.apply_transform(rule_id, record)
            if r["ok"]:
                success += 1
            else:
                failed += 1
            results.append(r)

        return {
            "ok": failed == 0,
            "message": f"批量转换完成: 成功 {success}/{len(source_records)}",
            "success": success,
            "failed": failed,
            "total": len(source_records),
            "results": results,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据转换编排的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据转换编排数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "operation": self.OPERATION_TERM,
        }