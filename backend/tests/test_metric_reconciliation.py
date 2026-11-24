"""链路10 指标汇总核对的去重、迟到防护与可追溯测试。

覆盖诉求：
- 重复汇总/同时提交的相同核对只保留一份核对证据与一条审计；
- 审计清单回写执行集合与成功数/总数，并与当次汇总对得上；
- 暂缓提交期间执行集合变化，迟到提交不能让当前读数退回；
- 读数更正后仍能凭旧汇总序号/指纹追查当时引用的执行记录。
"""
from __future__ import annotations

from app.pipeline import commit_metric_summary, summarize_metrics
from app.services import service_for


def _metric_id() -> int:
    metric_svc = service_for("metric_board")
    return metric_svc.list_entries(size=1000)[0][0]["id"]


class TestRepeatedSummaryDedup:
    """三条执行里已有一条完成，连续汇总两次仍只有一份核对证据。"""

    def test_consecutive_summaries_keep_single_evidence_and_audit(self):
        metric_svc = service_for("metric_board")
        audit_svc = service_for("audit_trail")
        metric_id = _metric_id()

        first = summarize_metrics(metric_id)
        second = summarize_metrics(metric_id)

        # 种子数据：三条执行、一条已完成 → 1/3
        assert first["total_runs"] == 3
        assert first["success_runs"] == 1
        assert first["success_rate_actual"] == round(1 / 3, 4)

        # 第二次是完全相同的核对：指纹一致、证据复用、审计复用
        assert second["fingerprint"] == first["fingerprint"]
        assert second["duplicate"] is True
        assert second["audit_reused"] is True
        assert second["audit_id"] == first["audit_id"]

        metric = metric_svc.get_entry(metric_id)
        assert len(metric["核对证据"]) == 1
        assert metric["当前汇总序号"] == first["sequence"]

        audits = audit_svc.list_metric_audits(metric_id)
        assert len(audits) == 1
        assert audits[0]["复用次数"] == 2

    def test_audit_counts_match_summary_evidence(self):
        metric_svc = service_for("metric_board")
        audit_svc = service_for("audit_trail")
        metric_id = _metric_id()

        result = summarize_metrics(metric_id)
        evidence = metric_svc.find_evidence(metric_id, result["fingerprint"])
        audit = audit_svc.find_metric_audit(metric_id, result["fingerprint"])

        # 审计清单里的成功数/总数与当次汇总证据逐字段对得上
        assert audit["成功数"] == evidence["成功数"] == 1
        assert audit["总数"] == evidence["总数"] == 3
        assert audit["失败数"] == evidence["失败数"]
        assert audit["核对指纹"] == evidence["核对指纹"]
        assert metric_svc.audit_evidence_consistency(metric_id) == []

        # 交叉核对能发现被篡改的审计
        audit["成功数"] = 9
        mismatches = metric_svc.audit_evidence_consistency(metric_id)
        assert len(mismatches) == 1
        assert "成功数" in mismatches[0]["reason"]


class TestDeferredCommitAndLateArrival:
    """暂缓审计提交，期间另一条执行完成，迟到写入不能退回当前读数。"""

    def test_deferred_then_change_then_late_commit_keeps_current_reading(self):
        metric_svc = service_for("metric_board")
        audit_svc = service_for("audit_trail")
        run_svc = service_for("flow_run")
        metric_id = _metric_id()

        # 第一次核对暂缓提交：证据登记、序号预留、审计挂起、读数不动
        held = summarize_metrics(metric_id, defer_audit=True)
        assert held["deferred"] is True
        assert held["sequence"] == 1
        metric = metric_svc.get_entry(metric_id)
        assert metric["当前汇总序号"] == 0
        held_evidence = metric_svc.find_evidence_by_seq(metric_id, 1)
        assert held_evidence["提交状态"] == "待提交"
        held_audit = audit_svc.find_metric_audit(metric_id, held["fingerprint"])
        assert held_audit["提交状态"] == "待提交"

        # 值班员把另一条执行标为完成并再次核对（立即提交）
        target_run = next(
            r for r in run_svc.list_entries(size=1000)[0] if r.get("status") == "执行中"
        )
        run_svc.complete_run(target_run["id"], record_count=12)
        fresh = summarize_metrics(metric_id)
        assert fresh["sequence"] == 2
        assert fresh["success_runs"] == 2
        assert fresh["total_runs"] == 3
        assert fresh["reading_applied"] is True

        # 两次提交都结束后，当前读数应为 2/3
        metric = metric_svc.get_entry(metric_id)
        assert metric["当前汇总序号"] == 2
        assert float(metric["成功率"]) == round(2 / 3, 4)

        # 迟到的第一次提交被拒收，当前读数不退回
        late = commit_metric_summary(metric_id, 1)
        assert late["status"] == "late_rejected"
        assert late["current_sequence"] == 2
        metric = metric_svc.get_entry(metric_id)
        assert float(metric["成功率"]) == round(2 / 3, 4)
        assert metric["当前汇总序号"] == 2

        # 迟到证据与审计保留并标记，仍可追查 1/3 的核对依据
        old_evidence = metric_svc.find_evidence_by_seq(metric_id, 1)
        assert old_evidence["提交状态"] == "迟到拒收"
        assert old_evidence["成功数"] == 1
        assert old_evidence["总数"] == 3
        old_audit = audit_svc.find_metric_audit(metric_id, held["fingerprint"])
        assert old_audit["提交状态"] == "迟到拒收"
        assert old_audit["成功数"] == 1

        # 新读数对应的证据为已提交、2/3
        new_evidence = metric_svc.find_evidence_by_seq(metric_id, 2)
        assert new_evidence["提交状态"] == "已提交"
        assert new_evidence["成功数"] == 2

    def test_old_reading_still_traces_run_records_used_at_the_time(self):
        metric_svc = service_for("metric_board")
        run_svc = service_for("flow_run")
        metric_id = _metric_id()

        held = summarize_metrics(metric_id, defer_audit=True)
        target_run = next(
            r for r in run_svc.list_entries(size=1000)[0] if r.get("status") == "执行中"
        )
        run_svc.complete_run(target_run["id"], record_count=7)
        summarize_metrics(metric_id)
        commit_metric_summary(metric_id, held["sequence"])

        # 沿用旧读数编号追查：证据里仍是当时快照（目标执行当时为执行中）
        old_evidence = metric_svc.get_evidence(metric_id, seq=held["sequence"])
        old_ref = next(ref for ref in old_evidence["执行集合"] if ref["id"] == target_run["id"])
        assert old_ref["状态"] == "执行中"
        assert {ref["id"] for ref in old_evidence["执行集合"]} == {1, 2, 3}

        # 当前执行已是已完成，但旧证据不被后续变化污染
        current_ref = next(
            ref for ref in metric_svc.find_evidence_by_seq(metric_id, 2)["执行集合"]
            if ref["id"] == target_run["id"]
        )
        assert current_ref["状态"] == "已完成"

    def test_commit_unknown_sequence_returns_not_found(self):
        result = commit_metric_summary(_metric_id(), 999)
        assert result["status"] == "not_found"
