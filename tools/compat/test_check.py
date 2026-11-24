# SPDX-License-Identifier: GPL-3.0-only
import contextlib
import io
from pathlib import Path
import tempfile
import subprocess
import unittest
from unittest.mock import patch

import check


class CompatibilityRunner(unittest.TestCase):
    def test_each_format_revision_requires_its_own_pre_change_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(check, "ROOT", Path(tmp)):
            root = Path(tmp)

            def git(*args):
                return subprocess.check_output(["git", *args], cwd=root, text=True).strip()

            git("init", "-q")
            git("config", "user.name", "CI test")
            git("config", "user.email", "ci@example.invalid")
            (root / "volume").mkdir()
            file = root / "volume" / "format.go"
            file.write_text("format 1\n")
            git("add", "."); git("commit", "-qm", "first")
            first = git("rev-parse", "HEAD")
            git("tag", "release-1")
            file.write_text("format 2\n")
            git("add", "."); git("commit", "-qm", "second")
            second = git("rev-parse", "HEAD")
            check.require_checkpoint(first, ["refs/tags/release-1"])
            file.write_text("format 3\n")
            git("add", "."); git("commit", "-qm", "third")
            with self.assertRaises(RuntimeError):
                check.require_checkpoint(second, ["refs/tags/release-1"])
            git("tag", "checkpoint-2", second)
            check.require_checkpoint(second, ["refs/tags/release-1", "refs/tags/checkpoint-2"])

    def test_skipped_or_missing_tests_cannot_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "test.log"
            for text in ("PASS\n", "--- SKIP: TestFormatCompatibilityFixture (0s)\nPASS\n",
                         "--- PASS: TestFormatCompatibilityFixture (0s)\n    --- SKIP: TestFormatCompatibilityFixture/file (0s)\nPASS\n"):
                log.write_text(text)
                with self.assertRaises(RuntimeError):
                    check.ensure_fixture_ran(log, 1)
            log.write_text("--- PASS: TestFormatCompatibilityFixture (0s)\n    --- PASS: TestFormatCompatibilityFixture/file (0s)\nPASS\n")
            check.ensure_fixture_ran(log, 1)
            with self.assertRaises(RuntimeError):
                check.ensure_fixture_ran(log, 2)

    def test_empty_tag_set_fails_before_any_build(self):
        with patch.object(check, "tags", return_value=[]), patch.object(check, "build") as build, patch("sys.argv", ["check.py"]), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                check.main()
            self.assertEqual(error.exception.code, 2)
            build.assert_not_called()

    def test_one_bad_tag_does_not_skip_remaining_tags_or_turn_green(self):
        with tempfile.TemporaryDirectory() as tmp:
            reports = Path(tmp) / "reports"
            calls = []

            def fixture(ref, _reader, _reports):
                calls.append(ref)
                if ref == "bad":
                    raise RuntimeError("old data cannot be read")
                return {"tag": ref, "cases": 23, "verified_restarts": 2}

            with patch.object(check, "tags", return_value=["first", "bad", "last"]), patch.object(check, "event_base", return_value=None), patch.object(check, "build"), patch.object(check, "check", side_effect=fixture), patch("sys.argv", ["check.py", "--reports", str(reports)]), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(RuntimeError):
                    check.main()
            self.assertEqual(calls, ["first", "bad", "last"])
            self.assertIn("old data cannot be read", (reports / "summary.json").read_text())


if __name__ == "__main__":
    unittest.main()
