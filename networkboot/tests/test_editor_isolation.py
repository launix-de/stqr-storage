# SPDX-License-Identifier: GPL-3.0-only
import contextlib
import io
import json
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from unittest.mock import Mock, patch

from test_workflows import load


class EditorIsolation(unittest.TestCase):
    def test_manual_editor_cannot_listen_on_the_boot_lan(self):
        script = load('serve')
        error = io.StringIO()
        with patch.object(sys, 'argv', ['serve', 'ubuntu', '--edit', '--no-dhcp', '--bind', '192.0.2.10']), patch.object(script, 'ctl') as ctl, patch.object(script, 'require'), patch.object(script.http.server, 'ThreadingHTTPServer') as server, contextlib.redirect_stderr(error), self.assertRaises(SystemExit):
            script.main()
        self.assertIn('editor HTTP listener must bind to loopback', error.getvalue())
        ctl.assert_not_called()
        server.assert_not_called()

    def test_editor_binds_loopback_and_advertises_qemu_host_alias(self):
        script = load('serve')
        with tempfile.TemporaryDirectory() as directory:
            cache, ready = Path(directory) / 'cache', Path(directory) / 'ready.json'
            addresses, handlers, failures = [], {}, []
            real_server = script.http.server.ThreadingHTTPServer
            sessions = Mock()
            sessions.allocate.return_value = 'ab' * 32
            sessions.boot_version.return_value = 'cd' * 32
            def listener(address, handler):
                server = real_server(address, handler)
                addresses.append(server.server_address)
                return server
            def run():
                try:
                    script.main()
                except BaseException as error:
                    failures.append(error)
            args = ['serve', 'ubuntu', '--edit', '--no-dhcp', '--http-port', '0',
                    '--cache', str(cache), '--ready-file', str(ready)]
            with patch.object(sys, 'argv', args), patch.object(script, 'require'), patch.object(script, 'ctl'), patch.object(script, 'NativeSessions', return_value=sessions) as native, patch.object(script, 'cache_release', return_value=('cd' * 32, cache, '01' * 16)), patch.object(script.signal, 'signal', side_effect=lambda kind, handler: handlers.__setitem__(kind, handler)), patch.object(script.http.server, 'ThreadingHTTPServer', side_effect=listener), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                worker = threading.Thread(target=run)
                worker.start()
                try:
                    deadline = time.monotonic() + 5
                    while not ready.exists() and worker.is_alive() and time.monotonic() < deadline:
                        time.sleep(.01)
                    self.assertTrue(ready.exists(), failures)
                    self.assertEqual(addresses[0][0], '127.0.0.1')
                    port = addresses[0][1]
                    self.assertEqual(json.loads(ready.read_text())['url'], f'http://10.0.2.2:{port}/boot.ipxe')
                    with urllib.request.urlopen(f'http://127.0.0.1:{port}/session.ipxe?mac=001122334455', timeout=2) as response:
                        body = response.read().decode()
                    self.assertIn(f'http://10.0.2.2:{port}/releases/', body)
                    self.assertNotIn('http://127.0.0.1:', body)
                    self.assertEqual(native.call_args.kwargs['server'], '10.0.2.2')
                    self.assertTrue(native.call_args.kwargs['edit'])
                finally:
                    if signal.SIGTERM in handlers:
                        handlers[signal.SIGTERM](None, None)
                    worker.join(5)
                self.assertFalse(worker.is_alive())
                self.assertEqual(failures, [])
