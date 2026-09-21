#!/usr/bin/env python3
"""Render accepted investigation projections, without inventing claims."""

import os
import re
import sys
from contextlib import ExitStack

sys.dont_write_bytecode = True
if "pwc_report_io" not in sys.modules:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pwc_report_io import fenced, open_candidate, read_ref, register_report, require, write_report

REPORT_ID = "triage-report"
REPORT_PATH = "artifacts/triage-report.md"


def render(meta, data, documents):
    sections = ["# 調查報告\n"]

    def text(label, value):
        if value is None or value == "":
            return
        body = str(value)
        fence = "`" * max(3, 1 + max((len(run) for run in re.findall(r"`+", body)), default=0))
        sections.append(label + "\n\n" + fence + "text\n" + body + "\n" + fence + "\n")

    def reference(label, ref):
        if ref is not None:
            sections.append(label + "\n\n" + fenced(dict(sorted(ref.items()))))

    def evidence(label, entries):
        for entry in entries or []:
            reference(label, entry["ref"])
            text("File ID", entry["file_id"])

    def fact(label, value):
        if value:
            text(label, value["value"])
            evidence(label + "依據", value["evidence"])

    def issues(label, entries):
        for issue in entries or []:
            text(label, issue["statement"])
            text("處置", issue["disposition"])
            text("理由", issue["reason"])
            evidence("依據", issue["basis"])

    def assessment(value):
        for key, label in (("support", "支持程度"), ("reason", "支持理由"),
                           ("measurement", "量測條件"), ("window", "查詢時間窗"),
                           ("filter", "查詢條件"), ("environment", "環境"), ("release", "版本")):
            text(label, value[key])
        evidence("支持依據", value["basis"])
        evidence("Runtime 依據", value["runtime_basis"])
        issues("反例", value["counterexamples"])
        for gap in value["gaps"]:
            text("驗證缺口", gap)

    def document(ref):
        matches = [d["data"] for d in documents if d["ref"] == ref]
        require(len(matches) == 1, "report requires one exact input owner")
        return matches[0]

    sections.append("## 報告與調查依據\n")
    for key in ("run_id", "attempt_id", "schema_id", "version"):
        text(key, meta.get(key))
    reference("Accepted state", data["state"])
    reference("Context", data["context"])
    context = document(data["context"])
    state = document(data["state"])
    sections.append("## 問題、身份與環境\n")
    text("問題", context["problem"])
    scope = context.get("scope") or {}
    for key in ("ticket", "stack", "pop", "binding"):
        text("授權 " + key, scope.get(key))
    for tenant in scope.get("tenant_ids") or []:
        text("授權 tenant", tenant)
    identity = context.get("identity") or {}
    text("身份解析狀態", identity.get("status"))
    if identity.get("lookup"):
        evidence("身份查詢", [identity["lookup"]])
    for key in ("stack", "pop", "binding", "tenant_id", "orgkey", "userkey", "release"):
        fact(key, identity.get(key))
    sections.append("## 已解析時間線\n")
    timeline = context.get("time") or {}
    for key in ("status", "from", "to"):
        text(key, timeline.get(key))
    for anchor in timeline.get("anchors") or []:
        for key in ("event", "original", "format", "source_tz", "utc", "offset_seconds", "paired_epoch_millis"):
            text(key, anchor.get(key))
        evidence("時間依據", [anchor["evidence"]])
        if anchor.get("paired_evidence"):
            evidence("同事件絕對時間依據", [anchor["paired_evidence"]])
    for observation in context.get("observations") or []:
        fact("觀察", observation)
    for resolved in context.get("resolved_gaps") or []:
        fact("已交代缺口", resolved)
    sections.append("## 完整性、結案理由與限制\n")
    for key, label in (("attachment_complete", "附件完整性"), ("wiki_status", "Wiki 搜尋狀態"), ("readiness", "Context readiness")):
        text(label, context.get(key))
    text("Planner 完整性", data["completeness"])
    text("結案理由", data["closure"])
    text("調查理由", state.get("rationale"))
    for gap in context.get("gaps") or []:
        text("Context 缺口", gap)
    for hypothesis in state.get("hypotheses") or []:
        text("假說 ID", hypothesis["id"])
        text("假說原文", hypothesis["statement"])
        text("假說判讀", hypothesis["assessment"])
        evidence("假說依據", hypothesis["evidence"])
    for index, selection in enumerate(data["claims"], 1):
        claim = document(selection["claim"])
        assessment_owner = document(selection["assessment_owner"])
        deliveries = [d for d in assessment_owner["verification"]["deliveries"]
                      if d["id"] == selection["delivery_id"] and d["claim"] == selection["claim"]]
        require(len(deliveries) == 1, "report delivery mismatch")
        delivery = deliveries[0]
        sections.append("## Claim " + str(index) + "\n")
        reference("Claim exact Ref", selection["claim"])
        reference("Claim 原始 state", claim["parent_state"])
        reference("Claim 原始 context", claim["context"])
        candidate = claim["candidate"]
        text("Claim ID", candidate["id"])
        text("Claim 原文", candidate["statement"])
        for premise in candidate["premises"]:
            text("前提原文", premise)
        evidence("Allowed evidence", candidate["allowed_evidence"])
        sections.append("### Planner assessment\n")
        reference("真正 assessment owner", selection["assessment_owner"])
        text("同版 delivery ID", delivery["id"])
        reference("Delivery proposal", delivery["proposal"])
        review = assessment_owner["verification_review"]
        assessment(review["assessment"])
        issues("分歧", review["disputes"])
        text("判讀後下一步", review["next_action"])
        for role in delivery["roles"]:
            sections.append("### Verifier " + role["role"] + "\n")
            text("Unavailable", role["unavailable"])
            text("Exhausted", role["exhausted"])
            for failure in role["failures"] or []:
                for key in ("stage", "task_id", "code", "origin", "dispatch", "run_id", "step_id", "attempt_id", "diagnostic"):
                    text("執行失敗 " + key, failure.get(key))
            if role["result"] is not None:
                reference("Verifier result", role["result"])
                result = document(role["result"])
                reference("Verifier claim", result["claim"])
                text("Verifier role", result["role"])
                evidence("Verifier allowed evidence", result["allowed_evidence"])
                assessment(result["assessment"])
    sections.append("## 缺口與下一步\n")
    for gap in data["gaps"]:
        text("缺口", gap)
    for step in data["next_steps"]:
        text("下一步", step)
    sections.append("## 限制\n\n支持程度及 closure 是 Planner 的判讀，不是 Controller 投票或 renderer 認列因果。新 claim 或改義必須回到獨立驗證；未完成查詢或執行失敗不是反證。完整 SourceJSON、ledger、checkpoint 與 producer registry 留於 committed 工作資料，不在本報告展開。\n")
    return "\n".join(sections).encode("utf-8")

def main(request_path, candidate_path):
    with ExitStack() as stack:
        attempt, candidate_fd, request, candidate = open_candidate(
            stack, request_path, candidate_path, "triage.report.v1")
        documents = []
        for ref in request["inputs"]:
            with ExitStack() as ref_stack:
                documents.append({"ref": ref, "data": read_ref(ref_stack, ref, ref["schema_id"])})
        register_report(candidate, REPORT_ID, REPORT_PATH)
        report = render(candidate["meta"], candidate["data"], documents)
        write_report(stack, attempt, candidate_fd, candidate, report, "triage-report.md")


if __name__ == "__main__":
    try:
        require(len(sys.argv) == 3, "usage: python3 -B render_report.py absoluteRequest absoluteCandidate")
        main(sys.argv[1], sys.argv[2])
    except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
        print("render_report: " + str(error), file=sys.stderr)
        sys.exit(1)
