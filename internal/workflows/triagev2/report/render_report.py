#!/usr/bin/env python3
"""Render the triage report from its committed inputs, without inventing claims."""

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

    def document(ref):
        matches = [doc["data"] for doc in documents if doc["ref"] == ref]
        require(len(matches) == 1, "report input missing or ambiguous")
        return matches[0]

    text("原始請求", data["request"])
    sections.append("## 結論\n")
    text("結論", data["conclusion"])
    text("完整性", data["completeness"])
    text("提前結束的原因", data["limit"])
    claim = data["claim"]
    text("驗證結果", claim["outcome"])
    if claim["claim"] is not None:
        sections.append("## 候選結論與獨立驗證\n")
        reference("Claim", claim["claim"])
        candidate = document(claim["claim"])["candidate"]
        text("陳述", candidate["statement"])
        for premise in candidate["premises"]:
            text("前提", premise)
        text("Investigator 標示", candidate["verification"])
        evidence("允許的證據", candidate["allowed_evidence"])
        for code in candidate["code_refs"]:
            text("程式碼", code["repo"] + "@" + code["ref"] + " " + code["path"] + " (" + code["relation"] + ")")
        if candidate["no_code_basis"]:
            text("程式碼", "此結論不依據程式碼")
        if claim["delivery"] is not None:
            for role in document(claim["delivery"])["roles"]:
                text("角色", role["role"])
                if role["unavailable"]:
                    text("狀態", "unavailable")
                    for failure in role["failures"]:
                        text("失敗", failure["code"] + " " + failure["diagnostic"])
                    continue
                assessment = document(role["result"])["assessment"]
                for key, label in (("support", "支持程度"), ("reason", "支持理由"),
                                   ("measurement", "量測條件"), ("window", "查詢時間窗"),
                                   ("filter", "查詢條件"), ("environment", "環境"), ("release", "版本")):
                    text(label, assessment[key])
                evidence("支持依據", assessment["basis"])
                evidence("Runtime 依據", assessment["runtime_basis"])
                for issue in assessment["counterexamples"]:
                    text("反例", issue["statement"])
                    text("處置", issue["disposition"])
                    text("理由", issue["reason"])
                for gap in assessment["gaps"]:
                    text("驗證缺口", gap)
        if claim["t2a"] is not None:
            for note in document(claim["t2a"])["notes"]:
                text("Steward T2a 備註", note)
        if claim["t2b"] is not None:
            steward = document(claim["t2b"])
            text("Steward T2b", steward["verdict"])
            for note in steward["notes"]:
                text("Steward 備註", note)
            for item in steward["items"]:
                text("Steward 質疑", "[" + item["pattern_id"] + "] " + item["question"])
    audits = [doc for doc in documents if doc["ref"]["schema_id"] == "triage.audit.v1"]
    if any(doc["data"]["findings"] for doc in audits):
        sections.append("## 稽核發現\n")
        for doc in audits:
            for finding in doc["data"]["findings"]:
                text("發現", finding["category"] + " (" + finding["effect"] + "): " + finding["reason"])
    sections.append("## 缺口與下一步\n")
    for gap in data["gaps"]:
        reference("缺口來源", gap["ref"])
        recorded = [g for g in document(gap["ref"])["gaps"] if g["id"] == gap["id"]]
        require(len(recorded) == 1, "report gap missing from its input")
        text("缺口", gap["id"] + ": " + recorded[0]["text"])
        text("處置", gap["disposition"] + ": " + gap["note"])
        evidence("處置依據", gap["evidence"])
    for gap in data["new_gaps"]:
        text("新缺口", gap["id"] + ": " + gap["text"])
    for step in data["next_steps"]:
        text("下一步", step)
    sections.append("## 限制\n\n支持程度與結論是調查角色的判讀，不是 Controller 投票或 renderer 認列因果。新的或改寫的結論必須回到獨立驗證；未完成的查詢或執行失敗不是反證。完整資料留於 committed 工作資料，不在本報告展開。\n")
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
