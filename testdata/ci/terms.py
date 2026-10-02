"""Internal-term gate for the public repository (stdlib only).

The term list is private and never stored in this repository. Format:
`[hard]` and `[soft]` sections, one case-insensitive regex per line matched
with word boundaries, `#` starts a comment line. Hard matches fail the gate,
soft matches are listed for human review. A list missing at the default
location is SKIP, not PASS.

Both the index (staged) and working-tree versions of tracked files, the
contents of untracked files that are not ignored, symlink targets and path
names are scanned.

Exit codes: 0 pass (soft matches may be listed), 1 hard match or invalid
list, 3 skipped because no list was found at the default location.
"""
import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

PASS, FAIL, SKIP = 0, 1, 3


def parse(text):
    sections, section = {"hard": [], "soft": []}, None
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line in ("[hard]", "[soft]"):
            section = line[1:-1]
            continue
        if re.fullmatch(r"\[\s*[A-Za-z]+\s*\](\s*#.*)?", line):
            raise ValueError(f"line {number}: unknown section header")
        if section is None:
            raise ValueError(f"line {number}: pattern outside [hard]/[soft] section")
        try:
            pattern = re.compile(r"\b(?:" + line + r")\b", re.IGNORECASE)
        except re.error as err:
            raise ValueError(f"line {number}: invalid regex: {err}") from err
        sections[section].append((number, pattern))
    if not sections["hard"]:
        raise ValueError("no [hard] patterns")
    return sections


def git(root, *args, data=None):
    return subprocess.run(["git", "-C", str(root), *args], input=data, check=True, capture_output=True).stdout


def texts(root):
    """Yield (name, version, text) for every version of every candidate file."""
    names = git(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard").decode("utf-8", "surrogateescape").split("\0")
    blobs = []
    for entry in git(root, "ls-files", "-z", "--stage").decode("utf-8", "surrogateescape").split("\0"):
        if entry:
            meta, name = entry.split("\t", 1)
            if meta.split()[0] != "160000":
                blobs.append((name, meta.split()[1]))
    out = git(root, "cat-file", "--batch", data="".join(f"{oid}\n" for _, oid in blobs).encode())
    for name, oid in blobs:
        header, out = out.split(b"\n", 1)
        if header.endswith(b" missing"):
            raise ValueError(f"index object {oid} for {name} is missing")
        size = int(header.split()[2])
        yield name, "index", out[:size].decode("utf-8", "replace")
        out = out[size + 1:]
    for name in sorted({n for n in names if n}):
        path = root / name
        yield name, "path", name
        if path.is_symlink():
            yield name, "symlink", os.readlink(path)
        elif path.is_file():
            yield name, "worktree", path.read_bytes().decode("utf-8", "replace")


def scan(root, sections):
    hits = {"hard": set(), "soft": set()}
    for name, version, text in texts(root):
        lines = [(0, text)] if version == "path" else list(enumerate(text.split("\n"), 1))
        for kind, patterns in sections.items():
            for number, line in lines:
                for list_line, pattern in patterns:
                    if pattern.search(line):
                        hits[kind].add((name, version, number, list_line))
    return {kind: sorted(found) for kind, found in hits.items()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--list", help="term list path; default $PWC_TRIAGE_SKILLS_DIR/denylist.txt")
    parser.add_argument("--root", default=".", help="repository root")
    args = parser.parse_args(argv)
    source = args.list
    if source is None:
        if os.environ.get("PWC_TRIAGE_SKILLS_DIR"):
            source = os.path.join(os.environ["PWC_TRIAGE_SKILLS_DIR"], "denylist.txt")
        if source is None or not os.path.isfile(source):
            print(f"SKIP: term list not found ({source or 'PWC_TRIAGE_SKILLS_DIR unset'}); this is not a pass")
            return SKIP
    try:
        sections = parse(Path(source).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as err:
        print(f"FAIL: unreadable term list: {err}")
        return FAIL
    root = Path(subprocess.run(["git", "-C", args.root, "rev-parse", "--show-toplevel"],
                               check=True, capture_output=True, text=True).stdout.strip())
    try:
        hits = scan(root, sections)
    except ValueError as err:
        print(f"FAIL: {err}")
        return FAIL
    for kind in ("soft", "hard"):
        for name, version, number, list_line in hits[kind]:
            where = f"{name} (path)" if version == "path" else f"{name}:{number} ({version})"
            print(f"{kind.upper()}: {where} matches list line {list_line}")
    if hits["hard"]:
        print(f"FAIL: {len(hits['hard'])} hard match(es)")
        return FAIL
    print(f"PASS: 0 hard matches, {len(hits['soft'])} soft match(es) for review")
    return PASS


if __name__ == "__main__":
    # Non-UTF-8 path names arrive surrogate-escaped; report them, never crash.
    sys.stdout.reconfigure(errors="backslashreplace")
    sys.exit(main())
