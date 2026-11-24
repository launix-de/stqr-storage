#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only
"""Build a real .deb, inspect it and exercise maintainer hooks in a fake host.

No package is installed on the host and no real service/account is changed.
The hook tests replace absolute host paths and service/account commands only;
dpkg extraction and the resulting daemon executable are real.
"""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class DebianPackage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(prefix="stqr-deb-test-")
        cls.base = Path(cls.tmp.name)
        subprocess.run([str(ROOT / "tools/build-deb.sh"), "0.1.0~test1", str(cls.base)], check=True)
        cls.package = next(cls.base.glob("*.deb"))
        cls.payload = cls.base / "payload"
        cls.control = cls.base / "control"
        subprocess.run(["dpkg-deb", "--extract", str(cls.package), str(cls.payload)], check=True)
        subprocess.run(["dpkg-deb", "--control", str(cls.package), str(cls.control)], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_payload(self):
        version = subprocess.check_output([str(self.payload / "usr/bin/stqr"), "--version"], text=True)
        self.assertIn("stqr 0.1.0~test1 (", version)
        self.assertIn(subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(), version)
        fields = subprocess.check_output(["dpkg-deb", "--field", str(self.package)], text=True)
        self.assertIn("Package: stqr", fields)
        self.assertIn("init-system-helpers", fields)
        self.assertFalse((self.payload / "etc/stqr/stqr.yaml").exists(), "never ship secrets")
        listing = subprocess.check_output(["dpkg-deb", "--contents", str(self.package)], text=True)
        self.assertTrue(listing.splitlines()[0].startswith("drwxr-xr-x root/root"), listing.splitlines()[0])
        unit = (self.payload / "usr/lib/systemd/system/stqr.service").read_text()
        self.assertIn("KillSignal=SIGTERM", unit)
        self.assertIn("TimeoutStopSec=infinity", unit)
        self.assertIn("User=stqr", unit)
        self.assertNotIn("drain", unit.lower())
        for hook in ("preinst", "postinst", "prerm", "postrm"):
            subprocess.run(["sh", "-n", str(self.control / hook)], check=True)

    def host(self, folder):
        folder.mkdir()
        commands = folder / "bin"
        commands.mkdir()
        log = folder / "events"
        systemd = folder / "systemd"
        systemd.mkdir()
        for name in ("systemctl", "deb-systemd-invoke", "deb-systemd-helper", "adduser", "addgroup", "getent", "install", "chown"):
            command = commands / name
            command.write_text('''#!/bin/sh
name=${0##*/}
printf '%s %s\\n' "$name" "$*" >> "$STQR_TEST_LOG"
case "$name" in
 getent) exit 1;;
 deb-systemd-invoke) if [ "$1" = stop ] && [ "${STQR_FAIL_STOP:-0}" = 1 ]; then exit 42; fi;;
 install)
  # Ignore account ownership in this unprivileged, isolated fixture.
  set -- "$@"
  while [ "$#" -gt 0 ]; do
   case "$1" in -o|-g) shift 2;; *) break;; esac
  done
  # Production uses install -d -m 0750 -o stqr -g stqr PATH... .
  mkdir -p "$STQR_TEST_ETC" "$STQR_TEST_STATE";;
esac
exit 0
''')
            command.chmod(0o755)
        env = dict(os.environ, PATH=f"{commands}:{os.environ['PATH']}", STQR_TEST_LOG=str(log),
                   STQR_TEST_ETC=str(folder / "etc"), STQR_TEST_STATE=str(folder / "state"))
        hooks = {}
        for name in ("preinst", "postinst", "prerm", "postrm"):
            text = (self.control / name).read_text()
            text = text.replace("/run/systemd/system", str(systemd))
            text = text.replace("/etc/stqr", str(folder / "etc")).replace("/var/lib/stqr", str(folder / "state"))
            hooks[name] = folder / name
            hooks[name].write_text(text)
        return env, hooks, log

    def test_upgrade_stops_before_unpack_and_restarts(self):
        env, hooks, log = self.host(self.base / self._testMethodName)
        subprocess.run(["sh", str(hooks["preinst"]), "upgrade", "0.0.1"], env=env, check=True)
        with log.open("a") as stream:
            stream.write("dpkg unpack new executable\n")
        subprocess.run(["sh", str(hooks["postinst"]), "configure", "0.0.1"], env=env, check=True)
        lines = log.read_text().splitlines()
        self.assertLess(lines.index("deb-systemd-invoke stop stqr.service"), lines.index("dpkg unpack new executable"))
        self.assertLess(lines.index("dpkg unpack new executable"), lines.index("deb-systemd-invoke start stqr.service"))
        config = Path(env["STQR_TEST_ETC"]) / "stqr.yaml"
        self.assertIn("data_dir:", config.read_text())
        self.assertEqual(config.stat().st_mode & 0o777, 0o600)
        config.write_text("existing configuration and credentials\n")
        subprocess.run(["sh", str(hooks["postinst"]), "configure", "0.0.1"], env=env, check=True)
        self.assertEqual(config.read_text(), "existing configuration and credentials\n")
        subprocess.run(["sh", str(hooks["postrm"]), "purge"], env=env, check=True)
        self.assertTrue(config.exists(), "purge must preserve storage and identity")

    def test_failed_stop_aborts_upgrade(self):
        env, hooks, log = self.host(self.base / self._testMethodName)
        result = subprocess.run(["sh", str(hooks["preinst"]), "upgrade", "0.0.1"], env=dict(env, STQR_FAIL_STOP="1"))
        self.assertEqual(result.returncode, 42)
        self.assertNotIn("start stqr.service", log.read_text())

    def test_aborted_upgrade_restarts_old_service(self):
        env, hooks, log = self.host(self.base / self._testMethodName)
        subprocess.run(["sh", str(hooks["postinst"]), "abort-upgrade", "0.0.1"], env=env, check=True)
        self.assertIn("deb-systemd-invoke start stqr.service", log.read_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
