#!/usr/bin/env python3
"""Render controller-owned review JSON, without reviewing or publishing it."""

import hashlib
import json
import os
import re
import stat
import sys
from contextlib import ExitStack


REPORT_ID = "review-report"
REPORT_PATH = "artifacts/review-report.md"
JSON_LIMIT = 1 << 20


def require(condition, message):
    if not condition:
        raise ValueError(message)


def object_pairs(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate JSON key")
        value[key] = item
    return value


def dumps(value):
    return json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)


def absolute_parts(path):
    require(os.path.isabs(path), "path must be absolute")
    parts = path.split("/")[1:]
    require(all(part not in ("", ".", "..") for part in parts),
            "path must be canonical without traversal")
    return parts


def directory(stack, path):
    # Walk descriptors rather than following a replaced ancestor symlink.
    parts = absolute_parts(path)
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    stack.callback(os.close, fd)
    for part in parts:
        fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                     dir_fd=fd)
        stack.callback(os.close, fd)
    return fd


def regular(stack, parent, name, writable=False):
    flags = os.O_RDWR if writable else os.O_RDONLY
    fd = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    stack.callback(os.close, fd)
    info = os.fstat(fd)
    require(stat.S_ISREG(info.st_mode), "JSON must be a regular file")
    require(not writable or info.st_nlink == 1, "candidate must not be hardlinked")
    return fd


def read_json(fd):
    with os.fdopen(os.dup(fd), "rb") as source:
        raw = source.read(JSON_LIMIT + 1)
    require(len(raw) <= JSON_LIMIT, "JSON exceeds candidate byte limit")
    return raw, json.loads(raw.decode("utf-8"), object_pairs_hook=object_pairs)


def read_ref(stack, ref, schema):
    require(ref["schema_id"] == schema, "input schema mismatch")
    absolute_parts(ref["path"])
    parent = directory(stack, os.path.dirname(ref["path"]))
    raw, envelope = read_json(regular(stack, parent, os.path.basename(ref["path"])))
    require(hashlib.sha256(raw).hexdigest() == ref["sha256"], "input sha256 mismatch")
    meta = envelope["meta"]
    require(type(meta["version"]) is int and meta["version"] == 1,
            "input version mismatch")
    require(all(meta[key] == ref[key] for key in ("run_id", "attempt_id", "schema_id")),
            "input identity mismatch")
    # The engine, not this consumer, authorizes commits and authenticates artifacts.
    return envelope["data"]


def fenced(value):
    body = dumps(value)
    longest = max((len(run) for run in re.findall(r"`+", body)), default=0)
    fence = "`" * max(3, longest + 1)
    return fence + "json\n" + body + "\n" + fence + "\n"


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
    absolute_parts(request_path)
    absolute_parts(candidate_path)
    require(os.path.basename(request_path) == "request.json" and
            os.path.basename(candidate_path) == "candidate.json" and
            os.path.dirname(request_path) == os.path.dirname(candidate_path),
            "request and candidate must be fixed files in the same attempt directory")
    with ExitStack() as stack:
        attempt = directory(stack, os.path.dirname(candidate_path))
        _, request = read_json(regular(stack, attempt, "request.json"))
        candidate_fd = regular(stack, attempt, "candidate.json", writable=True)
        _, candidate = read_json(candidate_fd)
        expected = dict(request["identity"], version=1, schema_id=request["output"]["schema_id"])
        require(candidate["meta"] == expected and type(candidate["meta"]["version"]) is int,
                "candidate meta mismatch")
        require(expected["schema_id"] == "review.validation.v1", "candidate schema mismatch")
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
        ids, paths = set(), set()
        for entry in candidate["files"]:
            path = entry["path"]
            parts = path.split("/")
            require(len(parts) >= 2 and all(part not in ("", ".", "..") for part in parts)
                    and parts[0] in ("artifacts", "evidence"), "unsafe file entry path")
            require(entry["id"] not in ids and path not in paths, "duplicate file ID/path")
            require(entry["id"] != REPORT_ID and path != REPORT_PATH, "report file ID/path collision")
            ids.add(entry["id"])
            paths.add(path)
        validation["report_file"] = REPORT_ID
        candidate["files"].append({"id": REPORT_ID, "kind": "artifact", "path": REPORT_PATH})
        report = render(candidate["meta"], prepared, validation, reviewed)
        updated = (dumps(candidate) + "\n").encode("utf-8")
        require(len(updated) <= JSON_LIMIT, "rendered candidate exceeds byte limit")
        try:
            os.mkdir("artifacts", 0o700, dir_fd=attempt)
        except FileExistsError:
            pass
        artifacts = os.open("artifacts", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=attempt)
        stack.callback(os.close, artifacts)
        # One invocation only: even an existing regular report is never overwritten.
        report_fd = os.open("review-report.md", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                            0o600, dir_fd=artifacts)
        with os.fdopen(report_fd, "wb") as output:
            output.write(report)
        os.lseek(candidate_fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(candidate_fd), "wb") as output:
            output.write(updated)
            output.truncate()


if __name__ == "__main__":
    try:
        require(len(sys.argv) == 3, "usage: python3 render_report.py absoluteRequest absoluteCandidate")
        main(sys.argv[1], sys.argv[2])
    except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
        print("render_report: " + str(error), file=sys.stderr)
        sys.exit(1)
