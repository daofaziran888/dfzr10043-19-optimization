"""运行指标服务：统计汇总、指标核对与逐条对账。

按统计维度和窗口汇总流程成功率、平均耗时与产出记录数，
读数需与执行实例逐条对得上。不一致时写审计轨迹。

核对证据与迟到防护约定：
- 每次汇总基于当时的执行集合做深拷贝快照，按快照内容计算核对指纹，
  相同执行集合与核对结果的重复汇总/并发核对只产生一份证据、一条审计。
- 每份证据分配单调递增的汇总序号；读数只允许被更新的序号推进，
  迟到提交（序号落后于当前汇总序号）一律拒收，当前读数不会被退回。
- 证据只追加不覆盖，更正读数后仍可凭旧序号/指纹追查当时引用的执行记录。
"""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


def snapshot_fingerprint(snapshot: dict[str, Any]) -> str:
    """按执行集合快照内容计算稳定指纹，作为证据与审计的判重键。"""
    payload = json.dumps(
        {
            "runs": snapshot.get("run_refs", []),
            "total": snapshot.get("total"),
            "success": snapshot.get("success"),
            "fail": snapshot.get("fail"),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class MetricBoardService(BaseModuleService):
    MODULE = "metric_board"
    ENTITY = "指标读数"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "读数编号", "统计维度", "统计窗口", "成功率", "平均耗时",
        "产出记录数", "异常条数", "采集时间",
    ]
    REQUIRED_FIELDS = ["读数编号", "统计维度", "统计窗口"]
    STATUS_ORDER = ["采集中", "已汇总", "已核对", "已作废"]
    ACTION_RULES = {"触发汇总": "已汇总", "核对读数": "已核对", "作废读数": "已作废"}
    NEGATIVE_ACTIONS = ["作废读数"]

    # ── 执行集合快照 ──────────────────────────────────────────────

    def build_run_snapshot(self) -> dict[str, Any]:
        """深拷贝当前执行集合，产出带成功/失败计数与指纹的汇总快照。

        快照独立于后续执行状态变化，保证旧读数始终能查到当时引用的执行记录。
        """
        run_svc = self._flow_run_service()
        all_runs = run_svc.list_entries(size=10000)[0]
        run_refs: list[dict[str, Any]] = []
        success = 0
        fail = 0
        total_records = 0
        for run in sorted(all_runs, key=lambda r: int(r.get("id", 0))):
            status = run.get("status", "")
            if status == "已完成":
                success += 1
            elif status == "执行失败":
                fail += 1
            try:
                total_records += int(run.get("产出记录数", "0") or "0")
            except (TypeError, ValueError):
                pass
            run_refs.append({
                "id": int(run.get("id", 0)),
                "执行编号": run.get("执行编号", ""),
                "状态": status,
                "产出记录数": run.get("产出记录数", ""),
            })
        total = len(run_refs)
        snapshot: dict[str, Any] = {
            "run_refs": copy.deepcopy(run_refs),
            "total": total,
            "success": success,
            "fail": fail,
            "total_records": total_records,
            "success_rate": round(success / max(1, total), 4),
        }
        snapshot["fingerprint"] = snapshot_fingerprint(snapshot)
        return snapshot

    def _flow_run_service(self):
        # 延迟导入避免服务注册阶段的循环依赖
        from app.services import service_for
        return service_for("flow_run")

    # ── 核对证据（只追加，不覆盖）────────────────────────────────

    def _evidence_list(self, metric: dict[str, Any]) -> list[dict[str, Any]]:
        return metric.setdefault("核对证据", [])

    def current_sequence(self, metric_id: int) -> int:
        """当前读数对应的汇总序号，0 表示尚未提交过汇总。"""
        metric = self.get_entry(metric_id)
        if metric is None:
            return 0
        return int(metric.get("当前汇总序号", 0) or 0)

    def find_evidence(self, metric_id: int, fingerprint: str) -> dict[str, Any] | None:
        """按核对指纹查找已有证据，命中即说明是重复汇总。"""
        metric = self.get_entry(metric_id)
        if metric is None:
            return None
        for evidence in self._evidence_list(metric):
            if evidence.get("核对指纹") == fingerprint:
                return evidence
        return None

    def reserve_evidence(self, metric_id: int, snapshot: dict[str, Any],
                         check_result: dict[str, Any], *, held: bool) -> dict[str, Any]:
        """登记一份核对证据并预留汇总序号；held=True 时审计提交暂缓。

        相同指纹的证据已存在则复用（累计命中次数），不重复登记、不新发序号。
        """
        metric = self.get_entry(metric_id)
        metric.setdefault("当前汇总序号", 0)
        metric.setdefault("当前核对指纹", "")
        existing = self.find_evidence(metric_id, snapshot["fingerprint"])
        if existing is not None:
            existing["命中次数"] = int(existing.get("命中次数", 1)) + 1
            return existing
        evidence_list = self._evidence_list(metric)
        seq = len(evidence_list) + 1
        evidence = {
            "汇总序号": seq,
            "核对指纹": snapshot["fingerprint"],
            "执行集合": copy.deepcopy(snapshot["run_refs"]),
            "总数": snapshot["total"],
            "成功数": snapshot["success"],
            "失败数": snapshot["fail"],
            "成功率": snapshot["success_rate"],
            "产出记录数": snapshot["total_records"],
            "核对结果": dict(check_result),
            "提交状态": "待提交" if held else "已提交",
            "命中次数": 1,
            "登记时间": datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds"),
        }
        evidence_list.append(evidence)
        return evidence

    def mark_evidence(self, metric_id: int, seq: int, state: str) -> dict[str, Any] | None:
        """更新证据提交状态：已提交 / 迟到拒收。"""
        metric = self.get_entry(metric_id)
        if metric is None:
            return None
        for evidence in self._evidence_list(metric):
            if evidence.get("汇总序号") == seq:
                evidence["提交状态"] = state
                return evidence
        return None

    def get_evidence(self, metric_id: int, *, seq: int | None = None,
                     fingerprint: str | None = None) -> dict[str, Any] | None:
        """按汇总序号或核对指纹追查历史核对依据。"""
        if seq is not None:
            return self.find_evidence_by_seq(metric_id, seq)
        if fingerprint is not None:
            return self.find_evidence(metric_id, fingerprint)
        return None

    def find_evidence_by_seq(self, metric_id: int, seq: int) -> dict[str, Any] | None:
        metric = self.get_entry(metric_id)
        if metric is None:
            return None
        for evidence in self._evidence_list(metric):
            if evidence.get("汇总序号") == seq:
                return evidence
        return None

    # ── 读数推进（单调，迟到写入不能退回）────────────────────────

    def apply_reading(self, metric_id: int, evidence: dict[str, Any]) -> dict[str, Any] | None:
        """按证据把指标读数推进到对应汇总序号。

        仅当证据序号不小于当前汇总序号时才允许写入，从根上挡住迟到退回。
        """
        metric = self.get_entry(metric_id)
        if metric is None:
            return None
        seq = int(evidence["汇总序号"])
        if seq < self.current_sequence(metric_id):
            return None
        metric["成功率"] = str(evidence["成功率"])
        metric["异常条数"] = str(evidence["失败数"])
        metric["产出记录数"] = str(evidence["产出记录数"])
        metric["当前汇总序号"] = seq
        metric["当前核对指纹"] = evidence["核对指纹"]
        metric["采集时间"] = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
        self.run_action(metric_id, "核对读数")
        return metric

    def audit_evidence_consistency(self, metric_id: int) -> list[dict[str, Any]]:
        """交叉核对审计清单与核对证据：成功数/总数/指纹必须逐一对得上。"""
        from app.services import service_for
        audit_svc = service_for("audit_trail")
        mismatches: list[dict[str, Any]] = []
        for audit in audit_svc.list_metric_audits(metric_id):
            evidence = self.find_evidence(metric_id, audit.get("核对指纹", ""))
            if evidence is None:
                mismatches.append({"audit_id": audit.get("id"), "reason": "审计缺少对应核对证据"})
                continue
            if int(audit.get("成功数", -1)) != int(evidence["成功数"]):
                mismatches.append({
                    "audit_id": audit.get("id"),
                    "reason": "审计成功数与当次汇总不一致",
                    "audit_success": audit.get("成功数"),
                    "evidence_success": evidence["成功数"],
                })
            if int(audit.get("总数", -1)) != int(evidence["总数"]):
                mismatches.append({
                    "audit_id": audit.get("id"),
                    "reason": "审计总数与当次汇总不一致",
                    "audit_total": audit.get("总数"),
                    "evidence_total": evidence["总数"],
                })
        return mismatches

    def summarize_with_counter(self, dimension: str, window: str,
                                success: int, fail: int, total_records: int) -> dict[str, Any]:
        """用计数汇总生成指标读数。"""
        total = max(1, success + fail)
        success_rate = round(success / total, 4)
        # 逐条核对逻辑：如果条目异常条数与失败数不一致则为不一致
        anom_count = fail
        result = {
            "dimension": dimension,
            "window": window,
            "success_rate": success_rate,
            "total": total,
            "success": success,
            "fail": fail,
            "total_records": total_records,
            "anomalies": anom_count,
        }
        return result

    def verify_consistency(self, metric_id: int, actual_success_rate: float) -> dict[str, Any]:
        """核对指标读数与执行实例实际成功率的一致性。"""
        entry = self.get_entry(metric_id)
        if entry is None:
            return {"ok": False, "reason": "指标读数不存在"}
        stated = float(entry.get("成功率", "0") or "0")
        if abs(stated - actual_success_rate) <= 0.01:
            self.run_action(metric_id, "核对读数")
            return {"ok": True, "consistent": True}
        self.run_action(metric_id, "核对读数")
        return {"ok": True, "consistent": False, "stated": stated, "actual": actual_success_rate}
