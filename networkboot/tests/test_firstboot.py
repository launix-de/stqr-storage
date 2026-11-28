# SPDX-License-Identifier: GPL-3.0-only
import json
from pathlib import Path
import subprocess
import tempfile
import types
import unittest
from unittest.mock import patch, Mock
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import firstboot
import common


class InstalledFirstBoot(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name)
        self.args = types.SimpleNamespace(memory=4096, cpus=2, uefi='/firmware.fd')

    def test_non_snap_and_already_seeded_never_start_vm_or_write_disk(self):
        for installed in (False, True):
            with self.subTest(installed=installed), patch.object(firstboot, 'is_file', side_effect=lambda socket,path: installed and path!=firstboot.OWNER), patch.object(firstboot, 'seed_state', return_value={'data': {'seeded': True}}), patch.object(firstboot, 'boot') as boot, patch.object(firstboot, 'guest_many') as guest:
                firstboot.prepare_first_boot('/disk.sock', self.work, self.args)
                boot.assert_not_called()
                guest.assert_not_called()
                self.assertEqual(json.loads((self.work/firstboot.STAMP).read_text())['snap_seed'], 'already-seeded' if installed else 'not-installed')

    def test_pending_seed_boots_installed_disk_and_requires_persisted_success(self):
        nonce = '01'*16
        pending = {'data': {}, 'changes': {'1': {'kind': 'seed', 'task-ids': ['1']}}, 'tasks': {'1': {'status': 0}}}
        complete = {'data': {'seeded': True}, 'changes': {'1': {'kind': 'seed', 'task-ids': ['1']}}, 'tasks': {'1': {'status': 4}}}
        with patch.object(firstboot, 'is_file', side_effect=lambda socket,path: path!=firstboot.OWNER), patch.object(firstboot, 'seed_state', side_effect=[pending, complete]), patch.object(firstboot, 'require'), patch.object(firstboot.secrets, 'token_hex', return_value=nonce), patch.object(firstboot, 'guest', return_value=types.SimpleNamespace(stdout='false\n'*4)), patch.object(firstboot, 'guest_many') as guest, patch.object(firstboot, 'boot') as boot, patch.object(firstboot, 'cleanup_owned', side_effect=lambda socket,record: firstboot.guest_many(socket,[('rm-f',firstboot.OWNER)])), patch.object(firstboot, 'read_text', return_value=json.dumps({'schema': 1, 'nonce': nonce, 'status': 'seed-loaded'})):
            firstboot.prepare_first_boot('/disk.sock', self.work, self.args)
            boot.assert_called_once_with('/disk.sock', self.args, self.work)
            self.assertEqual(json.loads((self.work/firstboot.STAMP).read_text())['snap_seed'], 'completed')
            install, cleanup = [x.args[1] for x in guest.call_args_list]
            self.assertTrue(any(x[0] == 'ln-s' for x in install))
            self.assertTrue(all(x[0] == 'rm-f' for x in cleanup))
            script = (self.work/('stqr-first-boot-'+nonce+'.py')).read_text()
            self.assertIn("'seed.loaded'", script)
            self.assertIn('timeout=600', script)
            self.assertIn('if seeded(state):', script)
            compile(script, '<generated-first-boot>', 'exec')
            self.assertNotIn('seeded=true', script)
            unit = (self.work/('stqr-first-boot-'+nonce+'.service')).read_text()
            self.assertNotIn('Before=display-manager', unit)

    def test_failed_timeout_wrong_nonce_and_incomplete_seed_never_validate(self):
        for failure in ('timeout', 'nonce', 'incomplete', 'missing'):
            with self.subTest(failure=failure):
                (self.work/firstboot.STAMP).unlink(missing_ok=True)
                state = Mock(side_effect=[{'data': {}}, {'data': {}}])
                def is_file(socket, path):
                    return path!=firstboot.OWNER and not (failure == 'missing' and path.endswith('.json') and 'stqr-first-boot-' in path)
                with patch.object(firstboot, 'is_file', side_effect=is_file), patch.object(firstboot, 'seed_state', state), patch.object(firstboot, 'require'), patch.object(firstboot.secrets, 'token_hex', return_value='02'*16), patch.object(firstboot, 'guest', return_value=types.SimpleNamespace(stdout='false\n'*4)), patch.object(firstboot, 'guest_many') as guest, patch.object(firstboot, 'cleanup_owned', side_effect=lambda socket,record: firstboot.guest_many(socket,[('rm-f',firstboot.OWNER)])), patch.object(firstboot, 'boot', side_effect=RuntimeError('timeout') if failure == 'timeout' else None), patch.object(firstboot, 'read_text', return_value=json.dumps({'schema': 1, 'nonce': 'wrong' if failure == 'nonce' else '02'*16, 'status': 'seed-loaded'})), self.assertRaises(RuntimeError):
                    firstboot.prepare_first_boot('/disk.sock', self.work, self.args)
                self.assertFalse((self.work/firstboot.STAMP).exists())
                self.assertTrue(all(x[0] == 'rm-f' for x in guest.call_args.args[1]))

    def test_first_boot_command_reuses_normal_firmware_network_and_no_iso(self):
        process = Mock()
        process.wait.return_value = 0
        process.poll.return_value = 0
        with patch.object(firstboot.subprocess, 'Popen', return_value=process) as popen:
            firstboot.boot('/private/disk.sock', self.args, self.work)
        cmd = popen.call_args.args[0]
        self.assertIn('/firmware.fd', cmd)
        self.assertIn('user,id=net0', cmd)
        self.assertIn('virtio-net-pci,netdev=net0', cmd)
        self.assertIn('-display', cmd)
        self.assertIn('-no-reboot', cmd)
        self.assertFalse(any('installer' in str(x) or 'ide-cd' in str(x) for x in cmd))
        self.assertIn('/private/disk.sock', ' '.join(cmd))

    def test_timeout_requests_shutdown_then_kills_boundedly(self):
        process = Mock()
        process.wait.side_effect = [subprocess.TimeoutExpired('qemu',720), subprocess.TimeoutExpired('qemu',120), 0]
        process.poll.return_value = 0
        with patch.object(firstboot.subprocess, 'Popen', return_value=process), self.assertRaisesRegex(RuntimeError, 'timed out'):
            firstboot.boot('/disk.sock', self.args, self.work)
        process.stdin.write.assert_called_once_with(b'system_powerdown\n')
        process.kill.assert_called_once()

    def test_seeded_flag_cannot_override_failed_seed_tasks(self):
        self.assertFalse(firstboot.seeded({'data': {'seeded': True}, 'changes': {'1': {'kind': 'seed', 'status': 9}}}))
        self.assertFalse(firstboot.seeded({'data': {'seeded': True}, 'changes': {'1': {'kind': 'seed','task-ids':['2']}}, 'tasks': {'2': {'status': 7}}}))

    def test_owned_cleanup_validates_hashes_and_never_accepts_arbitrary_paths(self):
        import hashlib
        nonce='03'*16
        helper, unit, link, result=firstboot.owned_paths(nonce)
        content={helper:'owned helper',unit:'owned unit'}
        record={'schema':1,'nonce':nonce,'helper_sha256':hashlib.sha256(content[helper].encode()).hexdigest(),'unit_sha256':hashlib.sha256(content[unit].encode()).hexdigest()}
        with patch.object(firstboot,'is_file',return_value=True),patch.object(firstboot,'read_text',side_effect=lambda socket,path:content[path]),patch.object(firstboot,'guest_many') as guest:
            firstboot.cleanup_owned('/disk.sock',record)
            self.assertEqual(guest.call_args.args[1],[('rm-f',p) for p in (link,unit,helper,result,result+'.tmp',firstboot.OWNER)])
        content[helper]='administrator replacement'
        with patch.object(firstboot,'is_file',return_value=True),patch.object(firstboot,'read_text',side_effect=lambda socket,path:content[path]),patch.object(firstboot,'guest_many') as guest,self.assertRaisesRegex(RuntimeError,'refusing cleanup'):
            firstboot.cleanup_owned('/disk.sock',record)
        guest.assert_not_called()
        with self.assertRaises(RuntimeError):firstboot.owned_paths('../../unowned')

    def test_retry_cleans_owned_leftovers_even_if_seed_already_completed(self):
        record={'schema':1,'nonce':'04'*16}
        with patch.object(firstboot,'is_file',return_value=True),patch.object(firstboot,'seed_state',return_value={'data':{'seeded':True}}),patch.object(firstboot,'read_text',return_value=json.dumps(record)),patch.object(firstboot,'cleanup_owned') as cleanup,patch.object(firstboot,'boot') as boot:
            firstboot.prepare_first_boot('/disk.sock',self.work,self.args)
            cleanup.assert_called_once_with('/disk.sock',record)
            boot.assert_not_called()
            self.assertEqual(json.loads((self.work/firstboot.STAMP).read_text())['snap_seed'],'already-seeded')

    def test_legacy_prepared_archive_is_retained_and_rejected(self):
        with patch.dict('os.environ', {'XDG_STATE_HOME':str(self.work/'state')}):
            work=self.work/'release';work.mkdir()
            for name in ('rootfs.tar','vmlinuz','initrd.img','release.json'):(work/name).write_bytes(b'prepared')
            common.remember_preparation('config','ubuntu',work)
            with self.assertRaisesRegex(RuntimeError,'predates first-boot validation'):
                common.prepared_release('config','ubuntu')
            self.assertTrue((work/'rootfs.tar').exists())
