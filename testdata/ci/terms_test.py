"""Exercise the internal-term gate with anonymous term lists."""
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
        (self.root / "plain.go").write_text("package plain\n", encoding="utf-8")
        (self.root / "gizmo-notes.txt").write_text("nothing here\n", encoding="utf-8")
        (self.root / ".gitignore").write_text("ignored.txt\n", encoding="utf-8")
        (self.root / "ignored.txt").write_text("sprocket\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.root), "add", "doc.md", ".gitignore"], check=True)

    def gate(self, listing):
        path = Path(self.temp.name) / "list.txt"
        path.write_text(listing, encoding="utf-8")
        return terms.main(["--root", str(self.root), "--list", str(path)])

    def test_outcomes(self):
        for name, listing, want in [
            ("hard match is case-insensitive with word boundaries", "[hard]\nwidgetron\n", terms.FAIL),
            ("soft match only lists", "# comment\n[soft]\nwidgetron\n", terms.PASS),
            ("untracked but not ignored file and path names are scanned", "[hard]\ngizmo\n", terms.FAIL),
            ("ignored file is not scanned", "[hard]\nsprocket\n", terms.PASS),
            ("partial word does not match", "[hard]\nwidget\n", terms.PASS),
            ("pattern outside a section is invalid", "widgetron\n", terms.FAIL),
            ("invalid regex is invalid", "[hard]\n(\n", terms.FAIL),
        ]:
            with self.subTest(name):
                self.assertEqual(self.gate(listing), want)

    def test_missing_list_is_skip(self):
        for env in [{"PWC_TRIAGE_SKILLS_DIR": str(Path(self.temp.name) / "absent")}, {}]:
            with self.subTest(env=bool(env)), mock.patch.dict(os.environ, env, clear=False):
                if not env:
                    os.environ.pop("PWC_TRIAGE_SKILLS_DIR", None)
                self.assertEqual(terms.main(["--root", str(self.root)]), terms.SKIP)


if __name__ == "__main__":
    unittest.main()
