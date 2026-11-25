# SPDX-License-Identifier: GPL-3.0-only
import copy
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
loader = importlib.machinery.SourceFileLoader('login_install', str(ROOT / 'pam/install-login'))
spec = importlib.util.spec_from_loader(loader.name, loader)
installer = importlib.util.module_from_spec(spec)
loader.exec_module(installer)


class LoginInstall(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'etc/pam.d').mkdir(parents=True)
        for name, text in {
            'passwd': 'root:x:0:0:root:/root:/bin/bash\nlaunix:x:1000:1000::/home/launix:/bin/bash\n',
            'group': 'root:x:0:\nlaunix:x:1000:\n',
            'shadow': 'root:!:20000:0:99999:7:::\nlaunix:local-hash:20000:0:99999:7:::\n',
            'gshadow': 'root:!::\nlaunix:!::\n',
            'pam.d/common-auth': 'auth required pam_unix.so\n',
            'pam.d/common-session': 'session optional pam_systemd.so\n',
        }.items():
            (self.root / 'etc' / name).write_text(text)
        self.plan = {'schema': 1, 'uri': 'http://127.0.0.1:7419', 'accounts': [
            {'name': 'user', 'id': '12345678' + '00' * 12, 'uid': 0x52345678, 'bucket': 'ab' * 16}]}

    def test_install_is_idempotent_routes_auth_and_exposes_only_stqr_users(self):
        installer.install(self.root, self.plan, '192.0.2.1')
        first = (self.root / 'etc/shadow').read_text()
        # The real installer is root; this test runner is unprivileged.
        (self.root / 'home/user').chmod(0o700)
        installer.install(self.root, self.plan, '192.0.2.1')
        self.assertEqual(first, (self.root / 'etc/shadow').read_text())
        self.assertIn('user:x:1379161720:1379161720:user:/home/user:/bin/bash', (self.root / 'etc/passwd').read_text())
        self.assertIn('launix:local-hash:', first)
        auth = (self.root / 'etc/pam.d/common-auth').read_text()
        self.assertEqual(auth.count(installer.START), 1)
        self.assertIn('[success=done default=die]', auth)
        self.assertLess(auth.index('pam_stqr.so'), auth.index('pam_unix.so'))
        session = (self.root / 'etc/pam.d/common-session').read_text()
        self.assertLess(session.index('pam_stqr.so'), session.index('pam_systemd.so'))
        self.assertIn('SystemAccount=true', (self.root / 'var/lib/AccountsService/users/launix').read_text())
        self.assertIn('SystemAccount=false', (self.root / 'var/lib/AccountsService/users/user').read_text())
        config = self.root / 'etc/security/stqr-home.json'
        self.assertEqual(json.loads(config.read_text())['uri'], 'http://192.0.2.1:7419')
        self.assertEqual(config.stat().st_mode & 0o777, 0o600)
        self.assertNotIn('password', config.read_text())

    def test_local_name_collision_is_rejected_without_changing_passwd(self):
        self.plan['accounts'][0]['name'] = 'launix'
        before = (self.root / 'etc/passwd').read_bytes()
        with self.assertRaisesRegex(ValueError, 'existing Unix account'):
            installer.install(self.root, self.plan)
        self.assertEqual(before, (self.root / 'etc/passwd').read_bytes())

    def test_uid_collision_and_nonempty_home_are_rejected(self):
        p = self.root / 'etc/passwd'
        p.write_text(p.read_text() + 'other:x:1379161720:1::/:/bin/false\n')
        with self.assertRaisesRegex(ValueError, 'UID collision'):
            installer.install(self.root, self.plan)
        p.write_text(p.read_text().split('other:')[0])
        home = self.root / 'home/user'
        home.mkdir(parents=True)
        (home / 'file').write_text('keep')
        with self.assertRaisesRegex(ValueError, 'nonempty'):
            installer.install(self.root, self.plan)
        self.assertEqual((home / 'file').read_text(), 'keep')

    def test_recreated_identity_cannot_take_old_unix_identity(self):
        installer.install(self.root, self.plan)
        self.plan['accounts'][0]['id'] = '12345678' + '01' * 12
        with self.assertRaisesRegex(ValueError, 'existing Unix account'):
            installer.install(self.root, self.plan)

    def test_removed_account_is_hidden_and_locked(self):
        installer.install(self.root, self.plan)
        plan = copy.deepcopy(self.plan)
        plan['accounts'] = []
        installer.install(self.root, plan)
        self.assertIn('user:!:', (self.root / 'etc/shadow').read_text())
        self.assertIn('SystemAccount=true', (self.root / 'var/lib/AccountsService/users/user').read_text())
