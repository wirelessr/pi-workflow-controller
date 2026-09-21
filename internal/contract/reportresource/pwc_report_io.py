"""Mechanical report I/O. Commit authorization belongs to the engine."""

import hashlib
import json
import os
import re
import stat

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


def open_candidate(stack, request_path, candidate_path, schema):
    absolute_parts(request_path)
    absolute_parts(candidate_path)
    require(os.path.basename(request_path) == "request.json" and
            os.path.basename(candidate_path) == "candidate.json" and
            os.path.dirname(request_path) == os.path.dirname(candidate_path),
            "request and candidate must be fixed files in the same attempt directory")
    attempt = directory(stack, os.path.dirname(candidate_path))
    _, request = read_json(regular(stack, attempt, "request.json"))
    candidate_fd = regular(stack, attempt, "candidate.json", writable=True)
    _, candidate = read_json(candidate_fd)
    expected = dict(request["identity"], version=1, schema_id=request["output"]["schema_id"])
    require(candidate["meta"] == expected and type(candidate["meta"]["version"]) is int,
            "candidate meta mismatch")
    require(expected["schema_id"] == schema, "candidate schema mismatch")
    return attempt, candidate_fd, request, candidate


def register_report(candidate, report_id, report_path):
    ids, paths = set(), set()
    for entry in candidate["files"]:
        path = entry["path"]
        parts = path.split("/")
        require(len(parts) >= 2 and all(part not in ("", ".", "..") for part in parts)
                and parts[0] in ("artifacts", "evidence"), "unsafe file entry path")
        require(entry["id"] not in ids and path not in paths, "duplicate file ID/path")
        require(entry["id"] != report_id and path != report_path, "report file ID/path collision")
        ids.add(entry["id"])
        paths.add(path)
    candidate["data"]["report_file"] = report_id
    candidate["files"].append({"id": report_id, "kind": "artifact", "path": report_path})


def write_report(stack, attempt, candidate_fd, candidate, report, filename):
    updated = (dumps(candidate) + "\n").encode("utf-8")
    require(len(updated) <= JSON_LIMIT, "rendered candidate exceeds byte limit")
    try:
        os.mkdir("artifacts", 0o700, dir_fd=attempt)
    except FileExistsError:
        pass
    artifacts = os.open("artifacts", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=attempt)
    stack.callback(os.close, artifacts)
    # One invocation only: even an existing regular report is never overwritten.
    report_fd = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                        0o600, dir_fd=artifacts)
    with os.fdopen(report_fd, "wb") as output:
        output.write(report)
    os.lseek(candidate_fd, 0, os.SEEK_SET)
    with os.fdopen(os.dup(candidate_fd), "wb") as output:
        output.write(updated)
        output.truncate()
