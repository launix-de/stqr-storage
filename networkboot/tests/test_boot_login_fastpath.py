# SPDX-License-Identifier: GPL-3.0-only
import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
loader = importlib.machinery.SourceFileLoader('fastpath_installer', str(ROOT / 'pam/install-login'))
spec = importlib.util.spec_from_loader(loader.name, loader)
installer = importlib.util.module_from_spec(spec)
loader.exec_module(installer)
KERNEL = '7.0-test'
MARKER = 'usr/lib/stqr-networkboot/prepared.sha256'
ASSETS = [f'lib/modules/{KERNEL}/extra/kstqr.ko',
          'usr/lib/stqr-networkboot/login.json', 'usr/lib/stqr-networkboot/install-login',
          'usr/lib/stqr-networkboot/pam_stqr.so', 'usr/lib/stqr-networkboot/home-template.json',
          'usr/libexec/stqr-home', 'usr/libexec/stqr-bootctl']


class PreparedLogin(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build = tempfile.TemporaryDirectory()
        cls.binary = Path(cls.build.name) / 'stqr-boot-root'
        subprocess.run(['cc', '-std=c11', '-Wall', '-Wextra', '-Werror', '-O2', '-static',
                        '-o', cls.binary, ROOT / 'linux-client/boot-root.c'], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.build.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name)
        self.root, self.payload = self.work/'root', self.work/'ram'
        (self.root/'etc/pam.d').mkdir(parents=True)
        for name, text in {
            'passwd': 'root:x:0:0:root:/root:/bin/bash\n',
            'group': 'root:x:0:\n', 'shadow': 'root:!:::::::\n', 'gshadow': 'root:!::\n',
            'pam.d/common-auth': 'auth required pam_unix.so\n',
            'pam.d/common-session': 'session optional pam_systemd.so\n',
        }.items():
            (self.root/'etc'/name).write_text(text)
        self.plan = {'schema': 1, 'uri': 'http://127.0.0.1:7419', 'accounts': [
            {'name': 'user', 'id': '12345678'+'00'*12, 'uid': 0x52345678, 'bucket': 'ab'*16}]}
        installer.install(self.root, self.plan)
        (self.root/'home/user').chmod(0o700)
        for asset in ASSETS:
            path = self.root/asset
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                path.write_bytes(b'payload')
        # A representative expensive helper, whose copy must disappear entirely.
        (self.root/'usr/libexec/stqr-bootctl').write_bytes(b'x'*(12<<20))
        (self.root/'usr/lib/stqr-networkboot/login.json').write_text(json.dumps(self.plan))
        for asset in ASSETS:
            dest = self.payload/asset
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.root/asset, dest)
        (self.payload/'bin').mkdir()
        shutil.copy2(self.binary, self.payload/'bin/stqr-boot-root')
        self.runner = self.work/'install.py'
        self.runner.write_text(f'''import importlib.machinery,importlib.util,json,sys
from pathlib import Path
loader=importlib.machinery.SourceFileLoader('installer',{str(ROOT/'pam/install-login')!r})
spec=importlib.util.spec_from_loader(loader.name,loader)
i=importlib.util.module_from_spec(spec);loader.exec_module(i)
r=Path(sys.argv[1]);i.install(r,json.loads((r/'usr/lib/stqr-networkboot/login.json').read_text()),sys.argv[2])
''')
        self.write_marker()

    def write_marker(self):
        output = subprocess.check_output(['sh', '-c', '. "$1"; stqr_login_manifest "$2" "$3"',
                                         'sh', ROOT/'initramfs/prepare-login', self.root, KERNEL])
        (self.root/MARKER).write_bytes(output)

    def prepare(self, server='192.0.2.8', fail=False):
        script = '''. "$1"; shift
cp() { printf '%s\\n' "$*" >> "$TEST_WORK/copies"; command cp "$@"; }
# Shell unit fixture: visibility simulation only. The separate namespace
# fixture exercises actual mount/read-only/pivot behavior.
stqr_login_mount() { printf '%s\\n' "$*" >> "$TEST_WORK/mounts"; if [ "$1" = --bind ]; then command cp -p "$2" "$3"; fi; }
stqr_login_install() { printf '%s\\n' install >> "$TEST_WORK/installs"; python3 "$TEST_WORK/install.py" "$1" "$2"; }
stqr_prepare_login "$@"
'''
        if fail:
            script = script.replace('python3 "$TEST_WORK/install.py" "$1" "$2"', 'return 1')
        return subprocess.run(['sh', '-c', script, 'sh', ROOT/'initramfs/prepare-login',
                               self.root, server, KERNEL, self.payload, self.work],
                              env={**os.environ, 'TEST_WORK': str(self.work)}, capture_output=True, text=True)

    def checked_prepare(self, **kw):
        result = self.prepare(**kw)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_editor_requires_helper_with_login_uri_and_preserves_initrd_on_failure(self):
        loader = importlib.machinery.SourceFileLoader('login_editor_serve', str(ROOT/'serve'))
        spec = importlib.util.spec_from_loader(loader.name, loader)
        serve = importlib.util.module_from_spec(spec)
        with patch.object(sys, 'path', [str(ROOT), *sys.path]):
            loader.exec_module(serve)
        binary_dir = self.work/'helpers'
        binary_dir.mkdir()
        helper = binary_dir/'stqr-boot-root'
        # Reproduce the old ignored binary: supports mounting but not login-uri.
        helper.write_text('#!/bin/sh\necho "Usage: stqr-boot-root SERVER PORT TOKEN ROOTMOUNT" >&2\nexit 2\n')
        helper.chmod(0o755)
        artifact = self.work/'editor-initrd'
        artifact.write_bytes(b'published-initrd')
        with patch.object(serve, 'BIN', binary_dir):
            with self.assertRaisesRegex(RuntimeError, 'make -C networkboot'):
                serve.editor_initrd(artifact)
            self.assertEqual(artifact.read_bytes(), b'published-initrd')
            shutil.copy2(self.binary, helper)
            serve.editor_initrd(artifact)
        self.assertGreater(artifact.stat().st_size, len(b'published-initrd'))
        self.assertEqual(subprocess.check_output([helper, '--capabilities']).decode().splitlines(),
                         ['mount-overlay', 'mount-edit', 'login-uri'])

    def test_matching_marker_skips_all_payload_copies_and_python(self):
        heavy = self.root/'usr/libexec/stqr-bootctl'
        identity = self.root/'etc/passwd'
        before = (heavy.stat().st_ino, heavy.stat().st_mtime_ns, identity.stat().st_ino)
        self.checked_prepare()
        self.assertFalse((self.work/'copies').exists())
        self.assertFalse((self.work/'installs').exists())
        self.assertEqual(before, (heavy.stat().st_ino, heavy.stat().st_mtime_ns, identity.stat().st_ino))
        self.assertEqual(json.loads((self.root/'etc/security/stqr-home.json').read_text())['uri'], 'http://192.0.2.8:7419')

    def test_module_only_mismatch_uses_ram_bind_without_advancing_marker(self):
        marker_before = (self.root/MARKER).read_bytes()
        (self.payload/ASSETS[0]).write_bytes(b'updated module')
        self.checked_prepare()
        copies = (self.work/'copies').read_text().splitlines()
        self.assertEqual(len(copies), 1)
        self.assertNotIn(str(self.root), copies[0])
        self.assertEqual((self.root/MARKER).read_bytes(), marker_before)
        self.assertIn('--bind', (self.work/'mounts').read_text())
        self.assertIn('kstqr.ko', copies[0])
        self.assertFalse((self.work/'installs').exists())
        self.assertEqual((self.root/ASSETS[0]).read_bytes(), b'updated module')

    def test_missing_marker_uses_ram_each_boot_without_false_persistent_marker(self):
        (self.root/MARKER).unlink()
        self.checked_prepare()
        self.assertEqual((self.work/'installs').read_text(), 'install\n')
        self.assertEqual(len((self.work/'copies').read_text().splitlines()), len(ASSETS)-1)
        (self.work/'copies').unlink(); (self.work/'installs').unlink()
        self.checked_prepare()
        self.assertEqual(len((self.work/'copies').read_text().splitlines()), len(ASSETS)-1)
        self.assertEqual((self.work/'installs').read_text(), 'install\n')
        self.assertFalse((self.root/MARKER).exists())
        for line in (self.work/'copies').read_text().splitlines():
            self.assertNotIn(str(self.root), line)

    def test_changed_login_plan_refreshes_dynamic_users(self):
        self.plan['accounts'].append({'name': 'other', 'id': '22345678'+'00'*12,
                                     'uid': 0x62345678, 'bucket': 'cd'*16})
        (self.payload/'usr/lib/stqr-networkboot/login.json').write_text(json.dumps(self.plan))
        template = {'uri': 'http://@STQR_BOOT_SERVER@:7419', 'accounts': {
            a['name']: {'id': a['id'], 'bucket': a['bucket']} for a in self.plan['accounts']}}
        (self.payload/'usr/lib/stqr-networkboot/home-template.json').write_text(json.dumps(template)+'\n')
        self.checked_prepare()
        self.assertIn('other:x:1647597176:', (self.root/'etc/passwd').read_text())
        self.assertIn('other', json.loads((self.root/'etc/security/stqr-home.json').read_text())['accounts'])
        self.assertNotIn('stqr-bootctl', (self.work/'copies').read_text())
        self.assertEqual((self.work/'installs').read_text(), 'install\n')
        self.assertNotIn('home-template.json', (self.work/'mounts').read_text())
        regenerated = json.loads((self.root/'usr/lib/stqr-networkboot/home-template.json').read_text())
        self.assertEqual(regenerated['accounts'], template['accounts'])
        self.assertEqual((self.root/'usr/lib/stqr-networkboot/home-template.json').stat().st_mode & 0o777, 0o600)

    def test_failed_installer_does_not_advance_completion_marker(self):
        before = (self.root/MARKER).read_bytes()
        (self.payload/'usr/lib/stqr-networkboot/login.json').write_text(json.dumps({**self.plan, 'accounts': []}))
        self.assertNotEqual(self.prepare(fail=True).returncode, 0)
        self.assertEqual((self.root/MARKER).read_bytes(), before)

    def test_static_endpoint_ipv6_and_idempotence(self):
        self.checked_prepare(server='2001:db8::3')
        config = self.root/'etc/security/stqr-home.json'
        self.assertEqual(json.loads(config.read_text())['uri'], 'http://[2001:db8::3]:7419')
        before = (config.stat().st_ino, config.stat().st_mtime_ns)
        self.checked_prepare(server='2001:db8::3')
        self.assertEqual(before, (config.stat().st_ino, config.stat().st_mtime_ns))
        self.assertEqual(config.stat().st_mode & 0o777, 0o600)
        self.assertNotEqual(self.prepare(server='bad"host').returncode, 0)

    def test_nonloopback_prepared_endpoint_is_preserved(self):
        self.plan['uri'] = 'http://storage.internal:7419'
        installer.install(self.root, self.plan)
        shutil.copy2(self.root/'usr/lib/stqr-networkboot/home-template.json', self.payload/'usr/lib/stqr-networkboot/home-template.json')
        self.write_marker()
        self.checked_prepare()
        self.assertEqual(json.loads((self.root/'etc/security/stqr-home.json').read_text())['uri'], self.plan['uri'])


    def test_invalid_template_is_rejected_without_replacing_config(self):
        template = self.root/'usr/lib/stqr-networkboot/home-template.json'
        config = self.root/'etc/security/stqr-home.json'
        before = config.read_bytes()
        for data in (b'', b'bad\x00template', b'@STQR_BOOT_SERVER@ @STQR_BOOT_SERVER@'):
            template.write_bytes(data)
            result = subprocess.run([self.binary, '--login-uri', self.root, '192.0.2.8'], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(config.read_bytes(), before)

    def test_runtime_installer_never_fsyncs_offline_persist_is_explicit(self):
        with patch.object(installer.os, 'fsync', side_effect=AssertionError('implicit fsync')):
            installer.install(self.root, self.plan, '192.0.2.9')
        with patch.object(installer.os, 'fsync') as sync:
            installer.install(self.root, self.plan, '192.0.2.10', persist=True)
            self.assertGreater(sync.call_count, 0)
        with patch.object(installer.os, 'fsync') as sync:
            installer.install(self.root, self.plan, '192.0.2.10', persist=True)
            self.assertEqual(sync.call_count, 0)
