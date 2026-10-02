"""Exercise the internal-term gate with anonymous term lists."""
import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import terms


class TermsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        self.root.mkdir()
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        (self.root / "doc.md").write_text("The Widgetron host is fine.\nA widgetronic word.\n", encoding="utf-8")
        (self.root / "gizmo-notes.txt").write_text("nothing here\n", encoding="utf-8")
        (self.root / "untracked.txt").write_text("a gadgetry note\n", encoding="utf-8")
        (self.root / ".gitignore").write_text("ignored.txt\n", encoding="utf-8")
        (self.root / "ignored.txt").write_text("sprocket\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.root), "add", "doc.md", ".gitignore"], check=True)

    def gate(self, listing):
        path = Path(self.temp.name) / "list.txt"
        path.write_text(listing, encoding="utf-8")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = terms.main(["--root", str(self.root), "--list", str(path)])
        return code, out.getvalue()

    def test_outcomes(self):
        for name, listing, want in [
            ("hard match is case-insensitive with word boundaries", "[hard]\nwidgetron\n", terms.FAIL),
            ("soft match only lists", "# comment\n[hard]\nabsentterm\n[soft]\nwidgetron\n", terms.PASS),
            ("path names are scanned", "[hard]\ngizmo\n", terms.FAIL),
            ("untracked file content is scanned", "[hard]\ngadgetry\n", terms.FAIL),
            ("ignored file is not scanned", "[hard]\nsprocket\n", terms.PASS),
            ("trailing partial word does not match", "[hard]\nwidget\n", terms.PASS),
            ("leading partial word does not match", "[hard]\nidgetron\n", terms.PASS),
            ("pattern outside a section is invalid", "widgetron\n", terms.FAIL),
            ("invalid regex is invalid", "[hard]\n(\n", terms.FAIL),
            ("misspelled header is invalid, not a pattern", "[hard]\nabsentterm\n[Hard]\nwidgetron\n", terms.FAIL),
            ("header with trailing comment is invalid", "[hard]\nabsentterm\n[soft]\nnope\n[hard]  # hosts\nwidgetron\n", terms.FAIL),
            ("bracketed regex is still a pattern", "[hard]\n[w]idgetro[n]\n", terms.FAIL),
            ("list without hard patterns is invalid", "# only comments\n[hard]\n[soft]\nwidgetron\n", terms.FAIL),
            ("empty list is invalid", "", terms.FAIL),
        ]:
            with self.subTest(name):
                self.assertEqual(self.gate(listing)[0], want)

    def test_output_names_locations_not_terms(self):
        code, out = self.gate("[hard]\nwidgetron\n[soft]\ngadgetry\n")
        self.assertEqual(code, terms.FAIL)
        self.assertIn("HARD: doc.md:1 (index) matches list line 2", out)
        self.assertIn("HARD: doc.md:1 (worktree) matches list line 2", out)
        self.assertIn("SOFT: untracked.txt:1 (worktree) matches list line 4", out)
        self.assertNotIn("idgetron", out.lower())
        self.assertNotIn("gadgetry", out.lower())

    def test_both_versions_of_tracked_files_are_scanned(self):
        (self.root / "doc.md").write_text("clean now\n", encoding="utf-8")
        code, out = self.gate("[hard]\nwidgetron\n")
        self.assertEqual(code, terms.FAIL, "staged blob still holds the term")
        self.assertIn("(index)", out)
        self.assertNotIn("(worktree)", out)
        subprocess.run(["git", "-C", str(self.root), "add", "doc.md"], check=True)
        self.assertEqual(self.gate("[hard]\nwidgetron\n")[0], terms.PASS)
        (self.root / "doc.md").write_text("now a widgetron again\n", encoding="utf-8")
        code, out = self.gate("[hard]\nwidgetron\n")
        self.assertEqual(code, terms.FAIL, "working tree only")
        self.assertIn("(worktree)", out)
        self.assertNotIn("(index)", out)

    def test_symlink_target_is_scanned(self):
        (self.root / "link").symlink_to("/elsewhere/widgetron")
        (self.root / "doc.md").write_text("clean\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.root), "add", "doc.md"], check=True)
        code, out = self.gate("[hard]\nwidgetron\n")
        self.assertEqual(code, terms.FAIL)
        self.assertIn("link:1 (symlink)", out)

    def test_list_location(self):
        lists = Path(self.temp.name) / "skills"
        lists.mkdir()
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(terms.main(["--root", str(self.root), "--list", str(lists / "absent")]), terms.FAIL, "explicit list must exist")
            with mock.patch.dict(os.environ, {"PWC_TRIAGE_SKILLS_DIR": str(lists)}):
                self.assertEqual(terms.main(["--root", str(self.root)]), terms.SKIP, "default list absent")
                (lists / "denylist.txt").write_text("[hard]\nwidgetron\n", encoding="utf-8")
                self.assertEqual(terms.main(["--root", str(self.root)]), terms.FAIL, "default list used")
            with mock.patch.dict(os.environ):
                os.environ.pop("PWC_TRIAGE_SKILLS_DIR", None)
                self.assertEqual(terms.main(["--root", str(self.root)]), terms.SKIP, "variable unset")


if __name__ == "__main__":
    unittest.main()
