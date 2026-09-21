#!/usr/bin/env python3
"""Render controller-owned review JSON, without reviewing or publishing it."""

import os
import sys
from contextlib import ExitStack

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from pwc_report_io import fenced, open_candidate, read_ref, register_report, require, write_report

REPORT_ID = "review-report"
REPORT_PATH = "artifacts/review-report.md"


def render(meta, prepared, validation, reviewed):
    sections = ["# 固定三角色程式碼審閱報告\n"]

    def section(title, value):
        sections.append(title + "\n\n" + fenced(value))

    section("## 報告契約身分", meta)
    section("## 完整版本 Pin", {"prepared": prepared["pin"], "validation": validation["pin"]})
    section("## 完整共同 ContextRef", validation["context"])
    sections.append("## 三位 reviewer 的完整結果與限制\n")
    for index, row in enumerate(validation["reviewers"], 1):
        section("### Reviewer " + str(index), row)
        original = reviewed.get(row["role"])
        section("#### 原始 reviewer limitations", None if original is None else original["limitations"])
    sections.append("## 來源列表\n")
    if not prepared["sources"]:
        section("### 來源", [])
    for index, source in enumerate(prepared["sources"], 1):
        section("### 來源 " + str(index), source)
    sections.append("## 基準需求逐項對照\n")
    if not prepared["requirements"]:
        section("### 基準需求", [])
    for index, requirement in enumerate(prepared["requirements"], 1):
        section("### 需求 " + str(index) + "：原始 statement、kind 與來源", requirement)
        section("#### Validation 判定、證據與理由", [
            item for item in validation["requirements"]
            if item["requirement_id"] == requirement["id"]
        ])
        section("#### Code 原始判定（未提供時為空陣列）", [
            item for item in reviewed.get("code", {}).get("requirements", [])
            if item["requirement_id"] == requirement["id"]
        ])
    sections.append("## 最終已確認 findings\n")
    if not validation["findings"]:
        section("### 已確認 findings（不代表程式正確）", [])
    for index, finding in enumerate(validation["findings"], 1):
        section("### Finding " + str(index), finding)
    sections.append("## 每項 finding 的處置與原因\n")
    if not validation["dispositions"]:
        section("### 處置", [])
    for index, disposition in enumerate(validation["dispositions"], 1):
        section("### 處置 " + str(index), disposition)
    section("## Prepared 尚待釐清問題", prepared["open_questions"])
    section("## Validation 限制", validation["limitations"])
    section("## 完整性與結論", {
        key: validation[key] for key in ("completeness", "conclusion", "report_file")
    })
    sections.append(
        "## 執行範圍聲明\n\n"
        "本報告僅呈現固定 code、scale、simplicity 三位 reviewer 的唯讀分析與 Validation 結果。\n"
        "沒有執行被審專案的 tests/build/compile/deploy 或 repository scripts。\n"
        "沒有執行 OCR 或 fallback，不宣稱 local correctness coverage 或 local coverage。\n"
        "沒有外部 write：未提交 GitHub/Jira/Confluence/Slack 評論、未 approve、未 commit/push、未寫 wiki。\n"
        "no_confirmed_findings 不代表程式正確或測試通過；renderer 不核實內容、不提升未知判定。\n"
    )
    sections.append("## 結構化追溯附錄\n\n<!-- pwc-review-data -->\n" + fenced({
        "prepared": prepared, "validation": validation,
    }))
    return "\n".join(sections).encode("utf-8")


def main(request_path, candidate_path):
    with ExitStack() as stack:
        attempt, candidate_fd, request, candidate = open_candidate(
            stack, request_path, candidate_path, "review.validation.v1")
        validation = candidate["data"]
        context = request["inputs"][0]
        require(validation["context"] == context, "candidate context mismatch")
        prepared = read_ref(stack, context, "review.prepare.v1")
        require(validation["pin"] == prepared["pin"], "candidate pin mismatch")
        reviewed = {}
        for ref in request["inputs"][1:]:
            rows = [row for row in validation["reviewers"]
                    if row["ref"] == ref and row["status"] == "succeeded"]
            require(len(rows) == 1, "reviewer input does not match candidate result")
            original = read_ref(stack, ref, "review.reviewer.v1")
            role = rows[0]["role"]
            require(original["role"] == role and role not in reviewed and
                    original["context"] == context and original["pin"] == prepared["pin"],
                    "reviewer identity/context mismatch")
            reviewed[role] = original
        require(all(row["status"] != "succeeded" or row["role"] in reviewed
                    for row in validation["reviewers"]), "succeeded reviewer input missing")
        register_report(candidate, REPORT_ID, REPORT_PATH)
        report = render(candidate["meta"], prepared, validation, reviewed)
        write_report(stack, attempt, candidate_fd, candidate, report, "review-report.md")


if __name__ == "__main__":
    try:
        require(len(sys.argv) == 3, "usage: python3 render_report.py absoluteRequest absoluteCandidate")
        main(sys.argv[1], sys.argv[2])
    except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
        print("render_report: " + str(error), file=sys.stderr)
        sys.exit(1)
