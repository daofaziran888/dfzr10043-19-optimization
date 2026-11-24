"""审计轨迹服务：变更记录、核查流转与封存归档。

记录流程发布、凭据轮换、批次确认等关键操作的变更摘要，
形成可封存核查的审计链。支持按操作类型和追溯标记查询。

运行指标核对的审计按核对指纹判重：同一执行集合的并发/重复核对
只保留一条审计，重复命中累计复用次数；审计内回写当次执行集合、
成功数与总数，保证审计清单与指标读数的核对证据对得上。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


class AuditTrailService(BaseModuleService):
    MODULE = "audit_trail"
    ENTITY = "审计条目"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "条目编号", "操作对象", "操作类型", "操作者标识", "操作时间",
        "变更摘要", "关联执行", "追溯标记",
    ]
    REQUIRED_FIELDS = ["条目编号", "操作对象", "操作类型"]
    STATUS_ORDER = ["已记录", "待核查", "已核查", "已封存"]
    ACTION_RULES = {"提交核查": "待核查", "完成核查": "已核查", "封存条目": "已封存"}
    NEGATIVE_ACTIONS = []

    def query_by_trace(self, trace_mark: str) -> list[dict[str, Any]]:
        """按追溯标记查询关联审计条目。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if str(it.get("追溯标记", "")) == trace_mark]

    def query_by_operation(self, operation_type: str) -> list[dict[str, Any]]:
        """按操作类型查询审计条目。"""
        items, _ = self.list_entries(size=10000)
        return [it for it in items if it.get("操作类型") == operation_type]

    def find_metric_audit(self, metric_id: int, fingerprint: str) -> dict[str, Any] | None:
        """按指标读数与核对指纹查找已有核对审计（重复核对的判重入口）。"""
        for it in self.list_entries(size=10000)[0]:
            if (
                it.get("操作类型") == "指标核对"
                and int(it.get("指标ID", -1)) == int(metric_id)
                and it.get("核对指纹") == fingerprint
            ):
                return it
        return None

    def list_metric_audits(self, metric_id: int) -> list[dict[str, Any]]:
        """列出某条指标读数的全部核对审计，按汇总序号排序。"""
        items = [
            it for it in self.list_entries(size=10000)[0]
            if it.get("操作类型") == "指标核对" and int(it.get("指标ID", -1)) == int(metric_id)
        ]
        return sorted(items, key=lambda it: int(it.get("汇总序号", 0)))

    def upsert_metric_audit(self, metric_id: int, metric_code: str, values: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """登记或复用一条指标核对审计。

        相同（指标ID + 核对指纹）的审计已存在则只累加复用次数，
        返回 (审计条目, 是否为复用)。values 需带核对指纹与回写依据。
        """
        fingerprint = values.get("核对指纹", "")
        existing = self.find_metric_audit(metric_id, fingerprint)
        if existing is not None:
            existing["复用次数"] = int(existing.get("复用次数", 1)) + 1
            return existing, True

        entry, _ = self.create_entry(values)
        # 回写执行集合与核对结果，保证审计清单与当次汇总对得上
        entry["指标ID"] = int(metric_id)
        entry["指标编号"] = metric_code
        entry["核对指纹"] = fingerprint
        entry["汇总序号"] = int(values.get("汇总序号", 0))
        entry["成功数"] = int(values.get("成功数", 0))
        entry["总数"] = int(values.get("总数", 0))
        entry["失败数"] = int(values.get("失败数", 0))
        entry["成功率"] = str(values.get("成功率", "0"))
        entry["执行集合"] = list(values.get("执行集合", []))
        entry["核对结论"] = values.get("核对结论", "")
        entry["提交状态"] = values.get("提交状态", "已提交")
        entry["复用次数"] = 1
        return entry, False

    def mark_submission(self, audit_id: int, state: str) -> dict[str, Any] | None:
        """更新核对审计的提交状态：已提交 / 迟到拒收。"""
        entry = self.get_entry(audit_id)
        if entry is None:
            return None
        entry["提交状态"] = state
        return entry

    def seal_verified_entries(self) -> dict[str, Any]:
        """封存所有已核查的审计条目。"""
        items, _ = self.list_entries(size=10000)
        sealed = 0
        for it in items:
            if it.get("status") == "已核查":
                self.run_action(it["id"], "封存条目")
                sealed += 1
        return {"ok": True, "sealed_count": sealed}

    def get_unreviewed_count(self) -> int:
        """获取待核查条目数量。"""
        items, _ = self.list_entries(size=10000)
        return sum(1 for it in items if it.get("status") == "待核查")
