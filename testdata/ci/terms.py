"""Internal-term gate for the public repository (stdlib only).

The term list is private and never stored in this repository. Format:
`[hard]` and `[soft]` sections, one case-insensitive regex per line matched
with word boundaries, `#` starts a comment line. Hard matches fail the gate,
soft matches are listed for human review. A missing list is SKIP, not PASS.

Exit codes: 0 pass (soft matches may be listed), 1 hard match or invalid
list, 2 skipped because no list was found.
"""
import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

PASS, FAIL, SKIP = 0, 1, 2


def parse(text):
    sections, section = {"hard": [], "soft": []}, None
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line in ("[hard]", "[soft]"):
            section = line[1:-1]
            continue
        if section is None:
            raise ValueError(f"line {number}: pattern outside [hard]/[soft] section")
        try:
            pattern = re.compile(r"\b(?:" + line + r")\b", re.IGNORECASE)
        except re.error as err:
            raise ValueError(f"line {number}: invalid regex: {err}") from err
        sections[section].append((number, pattern))
    return sections


def candidates(root):
    out = subprocess.run(["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                         check=True, capture_output=True).stdout
    return sorted({name for name in out.decode("utf-8", "surrogateescape").split("\0") if name})


def scan(root, sections):
    hits = {"hard": [], "soft": []}
    for name in candidates(root):
        path = root / name
        lines = [(0, name)]
        if path.is_file() and not path.is_symlink():
            text = path.read_bytes().decode("utf-8", "replace")
            lines += list(enumerate(text.splitlines(), 1))
        for kind, patterns in sections.items():
            for number, line in lines:
                for list_line, pattern in patterns:
                    if pattern.search(line):
                        hits[kind].append((name, number, list_line))
    return hits


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--list", help="term list path; default $PWC_TRIAGE_SKILLS_DIR/denylist.txt")
    parser.add_argument("--root", default=".", help="repository root")
    args = parser.parse_args(argv)
    source = args.list
    if source is None and os.environ.get("PWC_TRIAGE_SKILLS_DIR"):
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
    hits = scan(root, sections)
    for kind in ("soft", "hard"):
        for name, number, list_line in hits[kind]:
            where = f"{name} (path)" if number == 0 else f"{name}:{number}"
            print(f"{kind.upper()}: {where} matches list line {list_line}")
    if hits["hard"]:
        print(f"FAIL: {len(hits['hard'])} hard match(es)")
        return FAIL
    print(f"PASS: 0 hard matches, {len(hits['soft'])} soft match(es) for review")
    return PASS


if __name__ == "__main__":
    sys.exit(main())
