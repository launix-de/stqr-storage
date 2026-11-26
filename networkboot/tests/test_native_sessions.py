# SPDX-License-Identifier: GPL-3.0-only
import json
from pathlib import Path
import sys
import tempfile
import contextlib
import io
import subprocess
import threading
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
            if 'boot-session-create' in args or 'edit-session-create' in args:
                marker=Path(args[args.index('--session-record')+1])
                record={k:bytes(data[k]).hex() for k in ('user','upper','lower')}
                record['mode']=data.get('mode','overlay')
                marker.write_text(json.dumps(record))
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
                ctl.assert_called_with('config','boot-session-delete','01'*16,'03'*16,'02'*16,timeout=15)
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

    def test_editor_cleanup_never_uses_bucket_deleting_command(self):
        data=self.session();data['upper']=data['lower'];data['mode']='edit'
        with tempfile.TemporaryDirectory() as directory, patch('native_sessions.ctl',side_effect=self.provision(data)) as ctl:
            sessions=NativeSessions('config',directory,16,edit=True,image_name='ubuntu')
            try:
                token=sessions.allocate('001122334455','192.0.2.2','lower')
                item,payload=sessions.claim(token,'192.0.2.2',Mock())
                self.assertIn(b'mode=edit\n',payload)
                sessions.release(item)
                ctl.assert_called_with('config','edit-session-delete','01'*16,'02'*16,'02'*16,timeout=15)
                self.assertFalse(any('boot-session-delete' in c.args for c in ctl.call_args_list))
            finally:sessions.close()

    def test_restart_retries_only_private_inactive_session_records(self):
        with tempfile.TemporaryDirectory() as directory:
            directory=Path(directory)
            def marker(name,**data):
                file=directory/name
                file.write_text(json.dumps(data));file.chmod(0o600)
                return file
            idle=marker('boot-idle.json',user='01'*16,upper='03'*16,lower='02'*16)
            active=marker('boot-active.json',user='04'*16,upper='05'*16,lower='02'*16)
            edit=marker('boot-editor.json',user='06'*16,upper='02'*16,lower='02'*16,mode='edit')
            unsafe=marker('boot-invalid.json',user='07'*16,upper='02'*16,lower='02'*16)
            foreign=marker('someone-else.json',user='08'*16,upper='09'*16,lower='02'*16)
            public=marker('boot-public.json',user='0a'*16,upper='0b'*16,lower='02'*16)
            public.chmod(0o644)
            def cleanup(config,command,user,upper,lower,**kwargs):
                if user=='04'*16:
                    raise RuntimeError('upper still active')
            with patch('native_sessions.ctl',side_effect=cleanup) as ctl:
                sessions=NativeSessions('config',directory,16)
                try:
                    sessions.recover()
                    self.assertFalse(idle.exists())
                    self.assertFalse(edit.exists())
                    self.assertTrue(active.exists())
                    self.assertTrue(unsafe.exists())
                    self.assertTrue(foreign.exists())
                    self.assertTrue(public.exists())
                    self.assertEqual(ctl.call_count,3)
                    ctl.assert_any_call('config','edit-session-delete','06'*16,'02'*16,'02'*16,timeout=15)
                    ctl.side_effect=None
                    with patch("native_sessions.time.monotonic", return_value=10**12):
                        sessions.recover()
                    self.assertFalse(active.exists())
                finally:sessions.close()

    def test_failed_allocation_marker_is_recovered_without_creating_an_item(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            def allocate(*args,**kwargs):
                path=Path(args[args.index('--session-record')+1])
                path.write_text(json.dumps(dict(user='01'*16,upper='03'*16,lower='02'*16)))
                path.chmod(0o600)
                raise subprocess.CalledProcessError(1,['boot-session-create'])
            with patch('native_sessions.ctl',side_effect=allocate) as ctl:
                sessions=NativeSessions('config',directory,16)
                try:
                    with self.assertRaises(subprocess.CalledProcessError):
                        sessions.allocate('001122334455','192.0.2.2','lower')
                    self.assertEqual(sessions.items,{})
                    self.assertEqual(len(list(Path(directory).glob('boot-*.json'))),1)
                    ctl.side_effect=None
                    sessions.recover()
                    self.assertFalse(list(Path(directory).glob('boot-*.json')))
                finally:sessions.close()

    def test_expiration_rechecks_claim_before_revoking_session(self):
        with tempfile.TemporaryDirectory() as directory, patch('native_sessions.ctl',side_effect=self.provision()) as ctl:
            sessions=NativeSessions('config',directory,16)
            try:
                token=sessions.allocate('001122334455','192.0.2.2','lower')
                item,_=sessions.claim(token,'192.0.2.2',Mock())
                before=ctl.call_count
                sessions.release(item,only_unclaimed=True)
                self.assertEqual(ctl.call_count,before)
                self.assertIn('001122334455',sessions.items)
                self.assertEqual(sessions.boot_version(token),None)
                sessions.release(item)
            finally:sessions.close()

    def test_active_cleanup_backoff_is_quiet_and_event_forces_retry(self):
        with tempfile.TemporaryDirectory() as directory, patch('native_sessions.ctl', side_effect=self.provision()) as ctl:
            sessions=NativeSessions('config',directory,16)
            try:
                token=sessions.allocate('a','ip','lower')
                item,_=sessions.claim(token,'ip',Mock())
                ctl.side_effect=subprocess.CalledProcessError(75,['collect'])
                with contextlib.redirect_stdout(io.StringIO()) as output, patch('native_sessions.time.monotonic', return_value=100):
                    sessions.release(item)
                    for _ in range(100):sessions.recover()
                self.assertEqual(ctl.call_count,2) # create and one cleanup
                self.assertEqual(output.getvalue().count('storage connections still active'),1)
                self.assertTrue(item['marker'].exists())
                with patch('native_sessions.time.monotonic', return_value=101):
                    sessions.recover()
                self.assertEqual(ctl.call_count,3)
                # An explicit lifecycle event bypasses the next retry deadline.
                ctl.side_effect=None
                sessions._collect(item['marker'], item, force=True)
                self.assertFalse(item['marker'].exists())
                self.assertEqual(sessions.retries,{})
            finally:sessions.close()

    def test_slow_provision_does_not_block_other_mac_claim_or_release(self):
        with tempfile.TemporaryDirectory() as directory:
            started,finish=threading.Event(),threading.Event()
            errors=[]
            provision=self.provision()
            def command(*args,**kwargs):
                if args[-1]=='slow':
                    started.set()
                    if not finish.wait(5):raise RuntimeError('test timeout')
                return provision(*args,**kwargs)
            with patch('native_sessions.ctl',side_effect=command):
                sessions=NativeSessions('config',directory,16)
                def slow():
                    try:sessions.allocate('slow','ip','slow')
                    except Exception as e:errors.append(e)
                worker=threading.Thread(target=slow)
                worker.start()
                try:
                    self.assertTrue(started.wait(2))
                    with self.assertRaisesRegex(RuntimeError,'being provisioned'):
                        sessions.allocate('slow','ip','lower')
                    token=sessions.allocate('fast','ip','lower')
                    item,_=sessions.claim(token,'ip',Mock())
                    sessions.release(item)
                    self.assertNotIn('fast',sessions.items)
                    # Recovery must exclude the in-flight reservation marker.
                    sessions.recover()
                finally:
                    finish.set();worker.join(5);sessions.close()
                self.assertFalse(worker.is_alive())
                self.assertEqual(errors,[])

    def test_stop_during_provision_rolls_back_without_publishing_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            started,finish=threading.Event(),threading.Event()
            provision=self.provision()
            errors=[]
            def command(*args,**kwargs):
                result=provision(*args,**kwargs)
                if 'boot-session-create' in args:
                    started.set();finish.wait(5)
                return result
            with patch('native_sessions.ctl',side_effect=command):
                sessions=NativeSessions('config',directory,16)
                def allocate():
                    try:sessions.allocate('mac','ip','lower')
                    except Exception as error:errors.append(error)
                worker=threading.Thread(target=allocate);worker.start()
                self.assertTrue(started.wait(2))
                sessions.close();finish.set();worker.join(5)
                self.assertFalse(worker.is_alive())
                self.assertEqual(sessions.items,{})
                self.assertEqual(sessions.tokens,{})
                self.assertEqual(sessions.provisioning,{})
                self.assertEqual(len(errors),1)
                self.assertIn('stopping',str(errors[0]))
                self.assertFalse(list(Path(directory).glob('boot-*.json')))

    def test_cleanup_single_inflight_per_marker(self):
        with tempfile.TemporaryDirectory() as directory, patch('native_sessions.ctl',side_effect=self.provision()) as ctl:
            sessions=NativeSessions('config',directory,16)
            try:
                token=sessions.allocate('mac','ip','lower')
                item=sessions.items['mac']
                started,finish=threading.Event(),threading.Event()
                def command(*args,**kwargs):
                    started.set();finish.wait(5)
                ctl.side_effect=command
                worker=threading.Thread(target=lambda:sessions.release(item));worker.start()
                try:
                    self.assertTrue(started.wait(2))
                    sessions._collect(item['marker'],item,force=True)
                    sessions.recover()
                    self.assertEqual(ctl.call_count,2)
                finally:finish.set();worker.join(5)
                self.assertEqual(sessions.collecting,set())
            finally:sessions.close()

    def test_recovery_skips_live_registry_and_malformed_marker(self):
        with tempfile.TemporaryDirectory() as directory, patch('native_sessions.ctl',side_effect=self.provision()) as ctl:
            sessions=NativeSessions('config',directory,16)
            try:
                sessions.allocate('001122334455','192.0.2.2','lower')
                invalid=Path(directory)/'boot-bad.json'
                invalid.write_text('[]');invalid.chmod(0o600)
                before=ctl.call_count
                sessions.recover()
                self.assertEqual(ctl.call_count,before)
                self.assertTrue(invalid.exists())
            finally:sessions.close()
