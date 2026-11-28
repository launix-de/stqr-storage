# SPDX-License-Identifier: GPL-3.0-only
import copy
import importlib.machinery
import importlib.util
import json
import http.server
import threading
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
loader = importlib.machinery.SourceFileLoader('boot_update_policy', str(ROOT/'initramfs/update-policy'))
spec = importlib.util.spec_from_loader(loader.name, loader)
policy = importlib.util.module_from_spec(spec)
loader.exec_module(policy)


class BootUpdatePolicy(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def put(self, name, text):
        path = self.root/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return path

    def state(self, value):
        return self.put(policy.STATE_PATH, json.dumps(value))

    def read_state(self):
        return json.loads((self.root/policy.STATE_PATH).read_text())

    def test_existing_seed_and_install_tasks_preserved_auto_refresh_only(self):
        state = {'data': {'seeded': False, 'snaps': {'firefox': {'sequence': [8763]}},
                          'config': {'core': {'refresh': {'timer': 'fri,12:00'}}}},
                 'tasks': {'1': {'kind': 'mount-snap', 'data': {'seed': True}}},
                 'changes': {'1': {'kind': 'seed', 'tasks': ['1']}}, 'last-task-id': 7}
        self.state(state)
        self.put('etc/apt/apt.conf.d/50unattended-upgrades', 'existing administrator config')
        policy.apply(self.root, 'overlay')
        expected = copy.deepcopy(state)
        expected['data']['config']['core']['refresh']['hold'] = 'forever'
        self.assertEqual(self.read_state(), expected)
        self.assertEqual((self.root/'etc/apt/apt.conf.d/50unattended-upgrades').read_text(), 'existing administrator config')
        for unit in policy.UNITS:
            text = (self.root/'etc/systemd/system'/f'{unit}.d/90-stqr-ephemeral-updates.conf').read_text()
            self.assertIn('ConditionPathExists=', text)
        self.assertFalse((self.root/'etc/systemd/system/snapd.service').exists())
        self.assertFalse((self.root/'etc/systemd/system/snapd.seeded.service').exists())
        self.assertIn('APT::Periodic::Enable "0";', (self.root/policy.APT_PATH).read_text())

    def test_missing_state_bootstraps_empty_checkpoint_without_marking_seed_complete(self):
        self.put('usr/lib/snapd/snapd', 'installed daemon')
        policy.apply(self.root, 'overlay')
        state = self.read_state()
        self.assertEqual(state['changes'], {})
        self.assertEqual(state['tasks'], {})
        self.assertNotIn('seeded', state['data'])
        self.assertNotIn('snaps', state['data'])
        self.assertEqual(state['data']['config']['core']['refresh']['hold'], 'forever')
        self.assertEqual((self.root/policy.STATE_PATH).stat().st_mode & 0o777, 0o600)

    def test_no_snap_installation_does_not_create_state(self):
        policy.apply(self.root, 'overlay')
        self.assertFalse((self.root/policy.STATE_PATH).exists())

    def test_rerun_then_edit_restores_prior_hold_preserving_new_seed_state(self):
        state = {'data': {'config': {'core': {'refresh': {'hold': '2030-01-01T00:00:00Z'}}}},
                 'changes': {}, 'tasks': {}}
        self.state(state)
        policy.apply(self.root, 'overlay')
        record = (self.root/policy.RECORD_PATH).read_text()
        policy.apply(self.root, 'overlay')
        self.assertEqual((self.root/policy.RECORD_PATH).read_text(), record)
        current = self.read_state()
        current['data']['seeded'] = True
        current['tasks']['2'] = {'kind': 'install-snap'}
        self.state(current)
        policy.apply(self.root, 'edit')
        self.assertEqual(self.read_state()['data']['config']['core']['refresh']['hold'], '2030-01-01T00:00:00Z')
        self.assertTrue(self.read_state()['data']['seeded'])
        self.assertIn('2', self.read_state()['tasks'])
        self.assertFalse((self.root/policy.APT_PATH).exists())
        self.assertFalse((self.root/policy.RECORD_PATH).exists())

    def test_edit_without_inherited_policy_leaves_image_byte_identical(self):
        path = self.state({'data': {}, 'tasks': {}, 'changes': {}})
        before = path.read_bytes()
        policy.apply(self.root, 'edit')
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(sorted(p.relative_to(self.root).as_posix() for p in self.root.rglob('*') if p.is_file()), [policy.STATE_PATH])

    def test_edit_clears_only_our_hold_and_preserves_admin_override(self):
        self.state({'data': {}, 'tasks': {}, 'changes': {}})
        policy.apply(self.root, 'overlay')
        current = self.read_state()
        current['data']['config']['core']['refresh']['hold'] = '2031-01-01T00:00:00Z'
        self.state(current)
        policy.apply(self.root, 'edit')
        self.assertEqual(self.read_state()['data']['config']['core']['refresh']['hold'], '2031-01-01T00:00:00Z')

    def test_malformed_state_and_unknown_mode_fail_before_policy_writes(self):
        self.put(policy.STATE_PATH, '{bad json')
        with self.assertRaises(ValueError):
            policy.apply(self.root, 'overlay')
        self.assertFalse((self.root/policy.RECORD_PATH).exists())
        self.assertFalse((self.root/policy.APT_PATH).exists())
        with self.assertRaises(ValueError):
            policy.apply(self.root, 'unknown')

    def test_successful_claim_invocation_emits_only_authenticated_mode(self):
        # Exercise the real HTTP claim parser/main/mount-success path. Only the
        # privileged mount/sys-directory boundary is stubbed; credentials use a
        # memfd, so the fixture never writes host /run or invokes mount.kstqr.
        source = self.root/'claim-main.c'
        source.write_text('#define main claim_main\n#define open fixture_open\n#define mkdir fixture_mkdir\n'
                          '#define unlink fixture_unlink\n#define fork fixture_fork\n#define waitpid fixture_waitpid\n'
                          '#include '+json.dumps(str(ROOT/'linux-client/boot-root.c'))+'\n'
                          '#undef main\n'
                          'int fixture_open(const char *p,int flags,...) { (void)p; (void)flags; return memfd_create("fixture",0); }\n'
                          'int fixture_mkdir(const char *p,mode_t mode) { (void)p; (void)mode; return 0; }\n'
                          'int fixture_unlink(const char *p) { (void)p; return 0; }\n'
                          'pid_t fixture_fork(void) { return 42; }\n'
                          'pid_t fixture_waitpid(pid_t p,int *status,int flags) { (void)flags; *status=0; return p; }\n'
                          'int main(int argc,char **argv) { return claim_main(argc,argv); }\n')
        binary = self.root/'claim-main'
        subprocess.run(['cc', '-std=c11', '-Wall', '-Wextra', '-Werror', '-O2', '-o', binary, source], check=True)
        for mode in ('overlay', 'edit'):
            upper, lower = 'ab'*16, ('ab' if mode == 'edit' else 'cd')*16
            body = ('STQR-BOOT-1\nuri=http://127.0.0.1:7419\nusername=fixture\npassword=fixture\n'
                    f'lower={lower}\nupper={upper}\nmode={mode}\n').encode()
            class Handler(http.server.BaseHTTPRequestHandler):
                protocol_version = 'HTTP/1.1'
                def do_GET(self):
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                def log_message(self, *args):
                    pass
            server = http.server.HTTPServer(('127.0.0.1', 0), Handler)
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            try:
                result = subprocess.run([binary, '127.0.0.1', str(server.server_port), '0'*64,
                                         '/disposable-fixture'], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, mode+'\n')
            finally:
                server.server_close()
                thread.join(timeout=5)

    def test_claim_mode_validation_and_stdout_after_successful_mount(self):
        source = (ROOT/'linux-client/boot-root.c').read_text()
        # Execute the production claim validator; no mount, credentials, sockets
        # or host directories are involved in this C fixture.
        test = self.root/'claim-mode.c'
        test.write_text('#define main stqr_boot_main\n#include '+json.dumps(str(ROOT/'linux-client/boot-root.c'))+'\n#undef main\nint main(void) {\n'
                        'if(claim_edit_mode("overlay","a","b")!=0 || claim_edit_mode("edit","a","a")!=1 || '
                        'claim_edit_mode("edit","a","b")!=-1 || claim_edit_mode("overlay","a","a")!=-1 || '
                        'claim_edit_mode("wrong","a","b")!=-1) { return 1; }\nreturn 0; }\n')
        binary = self.root/'claim-mode'
        subprocess.run(['cc', '-std=c11', '-Wall', '-Wextra', '-Werror', '-O2', '-o', binary, test], check=True)
        subprocess.run([binary], check=True)
        self.assertLess(source.index('mounted(uri, upper, lower, fd, argv[4], edit);'), source.index('puts(edit ? "edit" : "overlay")'))
        script = (ROOT/'initramfs/scripts/stqr').read_text()
        self.assertIn('stqr_mode=$(/bin/stqr-boot-root ', script)
        self.assertLess(script.index('update-policy || panic'), script.index('stqr_prepare_login '))
        self.assertNotIn('stqr.mode=', script)
