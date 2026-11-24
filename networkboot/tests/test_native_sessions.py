# SPDX-License-Identifier: GPL-3.0-only
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from native_sessions import NativeSessions

class Sessions(unittest.TestCase):
    def session(self):
        return dict(uri='http://192.0.2.1:7419', username='boot-test', password='test-secret',
                    user=[1]*16, lower=[2]*16, upper=[3]*16)

    def provision(self, data=None):
        data = data or self.session()
        def command(*args, **kwargs):
            if 'boot-session-create' in args:
                marker=Path(args[args.index('--session-record')+1])
                marker.write_text(json.dumps({k:bytes(data[k]).hex() for k in ('user','upper','lower')}))
                marker.chmod(0o600)
                return json.dumps(data)
            return ''
        return command

    def test_scope_single_claim_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory, patch('native_sessions.ctl', side_effect=self.provision()) as ctl:
            sessions=NativeSessions('config', directory, 4, 1)
            try:
                token=sessions.allocate('001122334455','192.0.2.2','lower')
                self.assertEqual(len(token),64)
                self.assertEqual(sessions.allocate('001122334455','192.0.2.2','lower'),token)
                with self.assertRaises(RuntimeError):sessions.allocate('001122334466','192.0.2.2','lower')
                with self.assertRaises(RuntimeError):sessions.claim(token,'192.0.2.3',Mock())
                item,payload=sessions.claim(token,'192.0.2.2',Mock())
                self.assertTrue(payload.startswith(b'STQR-BOOT-1\n'))
                self.assertIn(b'password=test-secret\n',payload)
                self.assertNotIn('test-secret',item['marker'].read_text())
                with self.assertRaises(RuntimeError):sessions.claim(token,'192.0.2.2',Mock())
                sessions.release(item)
                ctl.assert_called_with('config','boot-session-delete','01'*16,'03'*16,'02'*16)
                self.assertFalse(list(Path(directory).glob('*.json')))
                self.assertEqual(item['password'],'')
            finally:sessions.close()

    def test_failed_cleanup_retains_recovery_marker_without_password(self):
        with tempfile.TemporaryDirectory() as directory, patch('native_sessions.ctl', side_effect=self.provision()) as ctl:
            sessions=NativeSessions('config', directory, 4)
            try:
                token=sessions.allocate('001122334455','192.0.2.2','lower')
                item,_=sessions.claim(token,'192.0.2.2',Mock())
                ctl.side_effect=RuntimeError('not idle')
                sessions.release(item)
                self.assertTrue(item['marker'].exists())
                self.assertNotIn('password',item['marker'].read_text())
                self.assertEqual(sessions.items,{})
            finally:sessions.close()

    def test_loopback_daemon_uri_is_advertised_on_boot_lan(self):
        data=self.session();data['uri']='http://127.0.0.1:7419'
        with tempfile.TemporaryDirectory() as directory, patch('native_sessions.ctl', side_effect=self.provision(data)):
            sessions=NativeSessions('config',directory,4,server='192.0.2.10')
            try:
                token=sessions.allocate('001122334455','192.0.2.2','lower')
                item,payload=sessions.claim(token,'192.0.2.2',Mock())
                self.assertIn(b'uri=http://192.0.2.10:7419\n',payload)
                sessions.release(item)
            finally:sessions.close()
