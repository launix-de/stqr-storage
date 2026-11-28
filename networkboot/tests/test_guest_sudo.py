# SPDX-License-Identifier: GPL-3.0-only
import json
import importlib.machinery
import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
loader = importlib.machinery.SourceFileLoader('stqr_home_sudo', str(ROOT / 'pam/stqr-home'))
spec = importlib.util.spec_from_loader(loader.name, loader)
helper = importlib.util.module_from_spec(spec)
loader.exec_module(helper)


class GuestSudo(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name) / 'sudoers.d'
        self.account = SimpleNamespace(pw_uid=2103648172, pw_name='ALL')
        self.addCleanup(patch.stopall)
        patch.object(helper, 'SUDOERS', self.directory).start()
        original = Path.lstat
        # The production helper runs as root. Model its root-owned directory
        # while keeping this fixture unprivileged and outside host /etc.
        patch.object(Path, 'lstat', lambda path: SimpleNamespace(st_uid=0, st_mode=0o40755)
                     if path == self.directory else original(path)).start()

    def test_disabled_on_ordinary_hosts(self):
        helper.guest_sudo({}, self.account, True)
        self.assertFalse(self.directory.exists())
        helper.guest_sudo({'guest_sudo': 'true'}, self.account, True)
        self.assertFalse(self.directory.exists())

    def test_only_authenticated_uid_policy_and_cleanup(self):
        helper.guest_sudo({'guest_sudo': True}, self.account, True)
        policy = self.directory / '90-stqr-uid-2103648172'
        text = policy.read_text()
        self.assertIn('"ALL" ALL=(ALL:ALL) PASSWD: ALL', text)
        self.assertIn('Defaults:"ALL" !rootpw, !targetpw', text)
        self.assertNotIn('NOPASSWD', text)
        self.assertNotIn('%stqr-login', text)
        self.assertNotIn('ALL ALL=', text)
        self.assertEqual(policy.stat().st_mode & 0o777, 0o440)
        before = text
        helper.guest_sudo({'guest_sudo': True}, self.account, True)
        self.assertEqual(policy.read_text(), before)
        other = self.directory / 'local-rescue'
        other.write_text('preserve local policy')
        helper.guest_sudo({'guest_sudo': True}, self.account, False)
        self.assertFalse(policy.exists())
        self.assertEqual(other.read_text(), 'preserve local policy')

    def test_sudo_parsers_accept_literal_account_rule(self):
        visudo = shutil.which('visudo')
        if not visudo:
            self.skipTest('visudo required for independent policy parsing')
        helper.guest_sudo({'guest_sudo': True}, self.account, True)
        for validator in dict.fromkeys([visudo, shutil.which('visudo.ws')]):
            if validator:
                result = subprocess.run([validator, '-c', '-f', str(self.directory / '90-stqr-uid-2103648172')],
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_root_and_invalid_ids_never_gain_policy(self):
        for uid in (0, -1, 1 << 32, '1000'):
            with self.assertRaisesRegex(RuntimeError, 'invalid guest sudo UID'):
                helper.guest_sudo({'guest_sudo': True}, SimpleNamespace(pw_uid=uid, pw_name='user'), True)
        self.assertFalse(self.directory.exists())

    def test_account_name_injection_is_rejected(self):
        for name in ('root', 'user\nALL', 'user"', 'user ALL=', 'user\\name', '%sudo'):
            with self.assertRaisesRegex(RuntimeError, 'invalid guest sudo account'):
                helper.guest_sudo({'guest_sudo': True}, SimpleNamespace(pw_uid=1000, pw_name=name), True)
        self.assertFalse(self.directory.exists())

    def test_reserved_looking_name_does_not_match_another_user(self):
        converter = shutil.which('cvtsudoers.ws') or shutil.which('cvtsudoers')
        if not converter:
            self.skipTest('cvtsudoers required for independent account filter proof')
        helper.guest_sudo({'guest_sudo': True}, self.account, True)
        policy = self.directory / '90-stqr-uid-2103648172'
        result = subprocess.run([converter, '-f', 'json', '-m', 'user=unrelated-account', str(policy)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(json.loads(result.stdout or '{}').get('User_Specs'))
        policy.chmod(0o600)
        policy.write_text('ALL ALL=(ALL:ALL) ALL\n')
        result = subprocess.run([converter, '-f', 'json', '-m', 'user=unrelated-account', str(policy)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout or '{}').get('User_Specs'))
