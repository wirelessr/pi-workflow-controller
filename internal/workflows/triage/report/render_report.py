#!/usr/bin/env python3
"""Render the triage report from its committed inputs, without inventing claims."""

import json
import os
import re
import sys
from contextlib import ExitStack

sys.dont_write_bytecode = True
if "pwc_report_io" not in sys.modules:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pwc_report_io import open_candidate, read_ref, register_report, require, write_report

REPORT_ID = "triage-report"
REPORT_PATH = "artifacts/triage-report.md"


BASIS = {"verified-claim": "已驗證", "evidence": "證據直接顯示", "inference": "推論，未驗證"}


def render(meta, data, documents):
    sections = ["# 調查報告\n"]

    def plain(value):
        """Agent prose as Markdown text that cannot open a block or HTML."""
        lines = []
        for line in str(value).replace("\r\n", "\n").replace("\r", "\n").split("\n"):
            # Backslash escapes keep agent text from opening HTML, links,
            # images, link or footnote definitions and tables anywhere, and
            # a heading, list, quote, rule or fence at the start of a line.
            line = line.strip()
            for ch in "\\<[]|":
                line = line.replace(ch, "\\" + ch)
            line = re.sub(r"^([#+*=`~>:_-])", r"\\\1", line)
            line = re.sub(r"^(\d+)([.)])", r"\1\\\2", line)
            lines.append(line)
        return "\n".join(lines).strip()

    def oneline(value):
        return re.sub(r"\s*\n\s*", " ", plain(value))

    def para(label, value):
        if value is None or value == "":
            return
        sections.append("**" + label + "**：" + plain(value) + "\n")

    def inline(value):
        body = str(value)
        if "\n" in body or "\r" in body:
            # A code span cannot hold a line break; escape it instead.
            body = json.dumps(body, ensure_ascii=False)
        fence = "`" * (1 + max((len(run) for run in re.findall(r"`+", body)), default=0))
        pad = " " if body.startswith("`") or body.endswith("`") else ""
        return fence + pad + body + pad + fence

    # One line per Ref: the exact Refs stay in the report contract, the
    # report names the schema, attempt and path a reader opens.
    def described(ref):
        return inline(ref["schema_id"]) + " attempt " + inline(ref["attempt_id"]) + " " + inline(ref["path"])

    def located(entry):
        source = "本 attempt" if entry["ref"] is None else described(entry["ref"])
        where = ""
        locator = entry.get("locator") or {}
        if "pointer" in locator:
            where = " at " + inline(locator["pointer"]) if locator["pointer"] else "（整份檔案）"
        elif "offset" in locator:
            where = " bytes " + str(locator["offset"]) + "+" + str(locator["length"])
        return inline(entry["file_id"]) + where + " in " + source

    def cited(entries, indent="   "):
        return "".join(indent + "- 依據：" + located(e) + "\n" for e in entries or [])

    def document(ref):
        matches = [doc["data"] for doc in documents if doc["ref"] == ref]
        require(len(matches) == 1, "report input missing or ambiguous")
        return matches[0]

    def recorded(gap):
        found = [g for g in document(gap["ref"]).get("gaps", []) if g["id"] == gap["id"]]
        require(len(found) == 1, "report gap missing from its input")
        return found[0]["text"]

    claim = data["claim"]
    sections.append("原始請求：" + inline(data["request"]) + "\n")
    sections.append("## 結論\n")
    question = data["question"]
    sections.append("**問題**：" + plain(question["text"]) + "\n" + cited(question["evidence"], ""))
    para("答案", data["answer"])
    sections.append("| 完整性 | 驗證結果 | 提前結束 |\n|---|---|---|\n| " + inline(data["completeness"]) + " | " + inline(claim["outcome"]) + " | "
                    + (oneline(data["limit"]) if data["limit"] else "無") + " |\n")
    para("把握程度", data["certainty"])

    sections.append("## 因果鏈\n")
    for n, step in enumerate(data["chain"], 1):
        sections.append(str(n) + ". [" + BASIS[step["basis"]] + "] " + oneline(step["statement"]) + "\n" + cited(step["evidence"]))

    sections.append("## 建議行動\n")
    if not data["actions"]:
        sections.append("報告沒有提出行動；見下一步。\n")
    for n, action in enumerate(data["actions"], 1):
        sections.append(str(n) + ". **" + oneline(action["audience"]) + "**：" + oneline(action["action"]) + "\n   - 理由：" + oneline(action["reason"]) + "\n" + cited(action["evidence"]))

    opened = [g for g in data["gaps"] if g["disposition"] == "open"]
    sections.append("## 未解問題與下一步\n")
    for gap in opened:
        sections.append("- 未解缺口 " + inline(gap["id"]) + "：" + oneline(recorded(gap)) + "\n  - 說明：" + oneline(gap["note"]) + "\n" + cited(gap["evidence"], "  "))
    for gap in data["new_gaps"]:
        sections.append("- 新缺口 " + inline(gap["id"]) + "：" + oneline(gap["text"]) + "\n")
    for step in data["next_steps"]:
        sections.append("- 下一步：" + oneline(step) + "\n")
    if not opened and not data["new_gaps"] and not data["next_steps"]:
        sections.append("無。\n")

    closed = [g for g in data["gaps"] if g["disposition"] != "open"]
    if closed:
        sections.append("## 已處理的缺口\n")
        for gap in closed:
            sections.append("- " + inline(gap["id"]) + "（" + gap["disposition"] + "）：" + oneline(recorded(gap)) + "\n  - 處置：" + oneline(gap["note"]) + "\n"
                            + "  - 來源：" + described(gap["ref"]) + "\n" + cited(gap["evidence"], "  "))

    audits = [doc for doc in documents if doc["ref"]["schema_id"] == "triage.audit.v1"]
    if any(doc["data"]["findings"] for doc in audits):
        sections.append("## 稽核發現\n")
        for doc in audits:
            for finding in doc["data"]["findings"]:
                sections.append("- " + inline(finding["category"]) + "（" + finding["effect"] + "）：" + oneline(finding["reason"]) + "\n")

    if claim["claim"] is not None:
        sections.append("## 附錄：候選結論與獨立驗證\n")
        sections.append("Claim：" + described(claim["claim"]) + "\n")
        candidate = document(claim["claim"])["candidate"]
        para("陳述", candidate["statement"])
        for premise in candidate["premises"]:
            para("前提", premise)
        para("Investigator 標示", candidate["verification"])
        for e in candidate["allowed_evidence"]:
            sections.append("- 允許的證據：" + located(e) + "\n")
        for code in candidate["code_refs"]:
            sections.append("- 程式碼：" + inline(code["repo"] + "@" + code["ref"] + " " + code["path"]) + "（" + code["relation"] + "）\n")
        if candidate["no_code_basis"]:
            sections.append("- 程式碼：此結論不依據程式碼\n")
        if claim["delivery"] is not None:
            for role in document(claim["delivery"])["roles"]:
                sections.append("### 驗證角色 " + inline(role["role"]) + "\n")
                if role["unavailable"]:
                    para("狀態", "unavailable")
                    for failure in role["failures"]:
                        para("失敗", failure["code"] + " " + failure["diagnostic"])
                    continue
                assessment = document(role["result"])["assessment"]
                for key, label in (("support", "支持程度"), ("reason", "支持理由"),
                                   ("measurement", "量測條件"), ("window", "查詢時間窗"),
                                   ("filter", "查詢條件"), ("environment", "環境"), ("release", "版本")):
                    para(label, assessment[key])
                for e in assessment["basis"]:
                    sections.append("- 支持依據：" + located(e) + "\n")
                for e in assessment["runtime_basis"]:
                    sections.append("- Runtime 依據：" + located(e) + "\n")
                for issue in assessment["counterexamples"]:
                    para("反例", issue["statement"])
                    para("處置", issue["disposition"])
                    para("理由", issue["reason"])
                for gap in assessment["gaps"]:
                    para("驗證缺口", gap)
        if claim["t2a"] is not None:
            for note in document(claim["t2a"])["notes"]:
                para("Steward T2a 備註", note)
        if claim["t2b"] is not None:
            steward = document(claim["t2b"])
            para("Steward T2b", steward["verdict"])
            for note in steward["notes"]:
                para("Steward 備註", note)
            for item in steward["items"]:
                para("Steward 質疑", "[" + item["pattern_id"] + "] " + item["question"])
    sections.append("## 限制\n\n「已驗證」只表示該步屬於通過獨立驗證的 claim，不是 Controller 認列因果；「推論」未經驗證。新的或改寫的結論必須回到獨立驗證；未完成的查詢或執行失敗不是反證。完整資料留於 committed 工作資料。\n")
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
