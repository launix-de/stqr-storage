# SPDX-License-Identifier: GPL-3.0-only
import importlib.machinery
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import json
import contextlib
import io
import os

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def load(name):
    loader = importlib.machinery.SourceFileLoader(name, str(ROOT / name))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module

class Workflows(unittest.TestCase):
    disk_id = '01' * 16

    def remember_disk(self):
        import common
        common.remember_installation('stqr.yaml', 'ubuntu', self.disk_id)

    def setUp(self):
        state = tempfile.TemporaryDirectory()
        self.addCleanup(state.cleanup)
        env = patch.dict(os.environ, {'XDG_STATE_HOME': state.name})
        env.start()
        self.addCleanup(env.stop)

    def test_bucket_only_resumes_failed_publication_without_reextracting(self):
        self.remember_disk()
        import common
        script = load('prepare-system')
        with tempfile.TemporaryDirectory() as directory:
            @contextlib.contextmanager
            def disk(*args):
                yield Path(directory) / 'disk.sock'
            def extract(socket, work, version, config):
                for name in ('rootfs.tar', 'vmlinuz', 'initrd.img', 'release.json'):
                    Path(work, name).write_bytes(b'prepared')
            def fail_publish(config, op, *args):
                if op == 'publish':
                    raise subprocess.CalledProcessError(1, ['publish'])
            argv = ['prepare-system', 'ubuntu', '--work-dir', directory]
            with patch.object(sys, 'argv', argv), patch.object(script, 'require'), patch.object(script, 'ctl', side_effect=fail_publish), patch.object(script, 'nbd', side_effect=disk), patch.object(script, 'qemu') as vm, patch.object(script, 'extract', side_effect=extract), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(subprocess.CalledProcessError):
                script.main()
            vm.assert_not_called()
            prepared = common.prepared_release('stqr.yaml', 'ubuntu')
            self.assertIsNotNone(prepared)
            with patch.object(sys, 'argv', argv), patch.object(script, 'require'), patch.object(script, 'ctl') as ctl, patch.object(script, 'nbd') as nbd, patch.object(script, 'extract') as extract, contextlib.redirect_stdout(io.StringIO()):
                script.main()
            nbd.assert_not_called()
            extract.assert_not_called()
            ctl.assert_any_call('stqr.yaml', 'publish', 'ubuntu', prepared['work'])
            self.assertFalse(Path(prepared['work']).exists())
            self.assertIsNone(common.prepared_release('stqr.yaml', 'ubuntu'))

    def test_published_preparation_retries_cleanup_without_republishing(self):
        import common
        script = load('prepare-system')
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory) / 'release'
            work.mkdir()
            for name in ('rootfs.tar', 'vmlinuz', 'initrd.img', 'release.json'):
                (work / name).write_bytes(b'prepared')
            common.remember_preparation('stqr.yaml', 'ubuntu', work, 'published')
            with patch.object(sys, 'argv', ['prepare-system', 'ubuntu']), patch.object(script, 'require'), patch.object(script, 'ctl') as ctl, patch.object(script, 'nbd') as nbd, contextlib.redirect_stdout(io.StringIO()):
                script.main()
            nbd.assert_not_called()
            self.assertFalse(any('publish' in call.args for call in ctl.call_args_list))
            self.assertFalse(any(call.args[1] in ('gc', 'delete', 'clear') for call in ctl.call_args_list))
            self.assertFalse(work.exists())

    def test_preparation_record_cannot_select_an_unmarked_directory(self):
        import common
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            for name in ('rootfs.tar', 'vmlinuz', 'initrd.img', 'release.json'):
                (work / name).write_bytes(b'prepared')
            common.remember_preparation('stqr.yaml', 'ubuntu', work)
            (work / '.stqr-preparation.json').unlink()
            self.assertIsNone(common.prepared_release('stqr.yaml', 'ubuntu'))
            self.assertTrue((work / 'rootfs.tar').exists())
    def test_malformed_http_is_rejected_without_logging_claim_tokens(self):
        import http.server
        import socket
        import threading
        serve = load('serve')
        failures = []
        class Server(http.server.HTTPServer):
            def handle_error(self, request, address):
                failures.append(sys.exc_info()[1])
        class Handler(serve.BootHTTPHandler):
            protocol_version = 'HTTP/1.1'
            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-Length', '2')
                self.end_headers()
                self.wfile.write(b'ok')
        server = Server(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        token = b'private-claim-capability'
        logs = io.StringIO()
        with contextlib.redirect_stderr(logs):
            thread.start()
            try:
                for request, expected in ((b'GET /claim/'+token+b' HTTP/9.9\r\n\r\n', b'505'),
                                          (b'POST /claim/'+token+b'\r\n\r\n', b'400'),
                                          (b'GET /claim/'+token+b' HTTP/1.0\r\n\r\n', b'200'),
                                          (b'GET /healthy HTTP/1.0\r\n\r\n', b'200')):
                    with socket.create_connection(server.server_address, timeout=3) as wire:
                        wire.sendall(request)
                        response = b''
                        while data := wire.recv(4096):
                            response += data
                        self.assertIn(expected, response)
                with socket.create_connection(server.server_address, timeout=3) as wire:
                    source = wire.makefile('rb')
                    wire.sendall(b'GET /healthy HTTP/1.1\r\nHost: localhost\r\n\r\n')
                    self.assertIn(b'200', source.readline())
                    while source.readline() != b'\r\n':
                        pass
                    self.assertEqual(source.read(2), b'ok')
                    wire.sendall(b'GET /claim/'+token+b' HTTP/9.9\r\n\r\n')
                    self.assertIn(b'505', source.read())
                    source.close()
                self.assertEqual(failures, [])
            finally:
                server.shutdown()
                server.server_close()
                thread.join()
        self.assertIn('Malformed HTTP request rejected', logs.getvalue())
        self.assertNotIn(token.decode(), logs.getvalue())

    def test_aborted_artifact_download_does_not_report_server_failure(self):
        import http.server
        import socket
        import struct
        import threading
        import urllib.request
        serve = load('serve')
        failures = []
        class Server(http.server.HTTPServer):
            def handle_error(self, request, address):
                failures.append(sys.exc_info()[1])
        class Handler(serve.BootHTTPHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                if self.path == '/abort':
                    self.send_response(200)
                    self.send_header('Content-Length', str(8 << 20))
                    self.end_headers()
                    for _ in range(2048):
                        self.wfile.write(b'x' * 4096)
                else:
                    self.send_response(200)
                    self.send_header('Content-Length', '2')
                    self.end_headers()
                    self.wfile.write(b'ok')
        server = Server(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with socket.create_connection(server.server_address, timeout=3) as wire:
                wire.sendall(b'GET /abort HTTP/1.0\r\nHost: localhost\r\n\r\n')
                self.assertTrue(wire.recv(1024))
                wire.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            with urllib.request.urlopen('http://127.0.0.1:%d/healthy' % server.server_port, timeout=3) as reply:
                self.assertEqual(reply.read(), b'ok')
            self.assertEqual(failures, [])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_proxy_dhcp_does_not_replace_dns_or_address_server(self):
        serve = load('serve')
        config = serve.dnsmasq_config('192.0.2.10', 'eth0', '192.0.2.0/24', '/tmp/tftp', 8088)
        self.assertIn('port=0\n', config)
        self.assertIn('dhcp-range=192.0.2.0,proxy,255.255.255.0', config)
        self.assertIn('tag:ipxe,http://192.0.2.10:8088/boot.ipxe', config)
        with tempfile.TemporaryDirectory() as d:
            conf = Path(d) / 'dnsmasq.conf'
            conf.write_text(config)
            result = subprocess.run(['dnsmasq', '--test', '--conf-file=' + str(conf)], capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
    def test_injected_interface_rejected(self):
        with self.assertRaises(ValueError):
            load('serve').dnsmasq_config('192.0.2.1', 'eth0\nport=53', '192.0.2.0/24', '/tmp/tftp', 8088)
    def test_boot_network_from_default_route(self):
        serve=load('serve')
        addresses=[{'addr_info':[{'scope':'global','local':'192.0.2.10','prefixlen':24}]}]
        with patch.object(serve.subprocess,'check_output',side_effect=[json.dumps([{'dev':'eth0'}]),json.dumps(addresses)]):
            self.assertEqual(serve.boot_network(),('eth0','192.0.2.10','192.0.2.0/24'))
        with patch.object(serve.subprocess,'check_output',return_value='[{"dev":"eth0"},{"dev":"eth1"}]'):
            with self.assertRaises(ValueError):
                serve.boot_network()
    def test_minimal_prepare_arguments_use_named_installation_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            image=Path(directory)/'ubuntu.iso'
            image.touch()
            script=load('prepare-system')
            @contextlib.contextmanager
            def disk(*args):
                yield Path(directory)/'disk.sock'
            with patch.object(sys,'argv',['prepare-system','ubuntu',str(image),'--work-dir',directory]),patch.object(script,'require'),patch.object(script,'ctl',return_value=self.disk_id) as ctl,patch.object(script,'nbd',side_effect=disk),patch.object(script,'qemu') as qemu,patch.object(script,'extract'),contextlib.redirect_stdout(io.StringIO()):
                script.main()
            ctl.assert_any_call('stqr.yaml','create','ubuntu-bd',capture=True)
            ctl.assert_any_call('stqr.yaml','publish','ubuntu',unittest.mock.ANY)
            ctl.assert_any_call('stqr.yaml','delete',self.disk_id)
            self.assertEqual(qemu.call_args.args[1].memory,4096)

    def test_edit_mounts_published_fs_directly_and_cow_clone_is_optional(self):
        for options,target,source in [([], 'ubuntu', None),
                                      (['--copy-from=base'], 'ubuntu', 'base'),
                                      (['--to-bucket=ubuntu-next'], 'ubuntu-next', 'ubuntu')]:
            script=load('edit-image')
            @contextlib.contextmanager
            def gateway(config,bucket,work):
                self.assertEqual(bucket,target)
                yield 'http://192.0.2.1:8089/boot.ipxe'
            with tempfile.TemporaryDirectory() as directory, patch.object(sys,'argv',['edit-image','ubuntu','--work-dir',directory,*options]), patch.object(script,'require'), patch.object(script,'ctl') as ctl, patch.object(script,'editor_gateway',side_effect=gateway), patch.object(script,'run') as vm, contextlib.redirect_stdout(io.StringIO()):
                script.main()
            if source:
                ctl.assert_any_call('stqr.yaml','clone-image',source,target)
            else:
                self.assertFalse(any('clone-image' in c.args for c in ctl.call_args_list))
            ctl.assert_any_call('stqr.yaml','check-image',target,capture=True)
            self.assertFalse(any(c.args[1] in ('create','clone','publish','delete','clear','nbd','init-disk') for c in ctl.call_args_list))
            self.assertIn('user,model=virtio-net-pci,bootfile=http://192.0.2.1:8089/boot.ipxe',vm.call_args.args)
            self.assertNotIn('-blockdev',vm.call_args.args)

    def test_editor_initrd_injects_helper_without_modifying_published_artifact(self):
        import gzip
        import hashlib
        script=load('serve')
        with tempfile.TemporaryDirectory() as directory:
            directory=Path(directory)
            (directory/'stqr-boot-root').write_bytes(b'current-helper')
            original=gzip.compress(b'original-archive')
            initrd=directory/'initrd.img';initrd.write_bytes(original)
            probe = subprocess.CompletedProcess([], 0, b'mount-overlay\nmount-edit\nlogin-uri\n', b'')
            with patch.object(script,'BIN',directory), patch.object(script.subprocess, 'run', return_value=probe):
                digest=script.editor_initrd(initrd)
            self.assertTrue(initrd.read_bytes().startswith(original))
            self.assertEqual(digest,hashlib.sha256(b'current-helper').digest())
            # Parse the appended newc entry independently, including its mode,
            # encoded length and payload. Linux extracts it after the old image.
            archive=gzip.decompress(initrd.read_bytes()[len(original):])
            self.assertEqual(archive[:6],b'070701')
            fields=[int(archive[6+i*8:14+i*8],16) for i in range(13)]
            self.assertEqual(fields[1],0o100755)
            name=archive[110:110+fields[11]-1]
            self.assertEqual(name,b'usr/bin/stqr-boot-root')
            start=(110+fields[11]+3)//4*4
            self.assertEqual(archive[start:start+fields[6]],b'current-helper')
            self.assertIn(b'TRAILER!!!\0',archive)

    def test_minimal_serve_arguments(self):
        script=load('serve')
        with patch.object(sys,'argv',['serve','ubuntu','--check']),patch.object(script,'require'),patch.object(script,'ctl') as ctl,patch.object(script,'boot_network',return_value=('eth0','192.0.2.10','192.0.2.0/24')),patch.object(Path,'is_file',return_value=True),contextlib.redirect_stdout(io.StringIO()):
            script.main()
            ctl.assert_any_call('stqr.yaml','resolve','ubuntu')
    def test_resume_preserves_existing_installation_disk(self):
        self.remember_disk()
        script = load('prepare-system')
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / 'ubuntu.iso'
            image.touch()
            @contextlib.contextmanager
            def disk(*args):
                yield Path(directory) / 'disk.sock'
            with patch.object(sys, 'argv', ['prepare-system', '--resume', 'ubuntu', str(image)]), patch.object(script, 'require'), patch.object(script, 'ctl') as ctl, patch.object(script, 'nbd', side_effect=disk), patch.object(script, 'qemu'), patch.object(script, 'extract'), contextlib.redirect_stdout(io.StringIO()):
                script.main()
            ctl.assert_any_call('stqr.yaml', '--size', str(32 << 30), 'check-install-disk', self.disk_id)
            self.assertFalse(any('create' in call.args or 'init-disk' in call.args for call in ctl.call_args_list))
    def test_publish_only_never_recreates_disk_or_starts_vm(self):
        self.remember_disk()
        script = load('prepare-system')
        @contextlib.contextmanager
        def disk(*args):
            yield Path('/private/disk.sock')
        with patch.object(sys, 'argv', ['prepare-system', '--publish-only', 'ubuntu']), patch.object(script, 'require'), patch.object(script, 'ctl') as ctl, patch.object(script, 'nbd', side_effect=disk), patch.object(script, 'qemu') as qemu, patch.object(script, 'extract') as extract, contextlib.redirect_stdout(io.StringIO()):
            script.main()
        qemu.assert_not_called()
        extract.assert_called_once()
        ctl.assert_any_call('stqr.yaml', 'publish', 'ubuntu', unittest.mock.ANY)
        self.assertFalse(any('create' in call.args or 'init-disk' in call.args for call in ctl.call_args_list))

    def test_temporary_disk_cleanup_failure_reports_image_already_published(self):
        self.remember_disk()
        script = load('prepare-system')
        @contextlib.contextmanager
        def disk(*args):
            yield Path('/private/disk.sock')
        def command(config, op, *args):
            if op == 'delete':
                raise subprocess.CalledProcessError(1, ['delete'])
        with patch.object(sys, 'argv', ['prepare-system', '--publish-only', 'ubuntu']), patch.object(script, 'staging', side_effect=disk), patch.object(script, 'require'), patch.object(script, 'ctl', side_effect=command), patch.object(script, 'nbd', side_effect=disk), patch.object(script, 'extract'), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()), self.assertRaisesRegex(RuntimeError, 'ubuntu is published, but temporary installation disk cleanup failed'):
            script.main()

    def test_installation_uses_pinned_id_after_bucket_name_is_reused(self):
        script = load('prepare-system')
        original, replacement = '01' * 16, '02' * 16
        names, attached, deleted = {'ubuntu-bd': original}, [], []
        with tempfile.TemporaryDirectory() as directory:
            iso = Path(directory) / 'installer.iso'
            iso.write_bytes(b'installer')
            def command(config, *args, **kwargs):
                op = args[0]
                if op == 'create':
                    return original
                if 'init-disk' in args:
                    # Another administrator renames the original and reuses
                    # its visible name while the long installation continues.
                    names['ubuntu-bd'] = replacement
                if op == 'delete':
                    deleted.append(names.get(args[1], args[1]))
                return ''
            @contextlib.contextmanager
            def disk(config, selector, work):
                selected = names.get(selector, selector)
                attached.append(selected)
                self.assertEqual(selected, original,
                                 'replacement disk attached through reused name')
                yield Path(work) / 'disk.sock'
            def extract(socket, work, version, config):
                for name in ('rootfs.tar', 'vmlinuz', 'initrd.img', 'release.json'):
                    Path(work, name).write_bytes(b'prepared')
            argv = ['prepare-system', 'ubuntu', str(iso), '--work-dir', directory]
            with patch.object(sys, 'argv', argv), patch.object(script, 'require'), patch.object(script, 'ctl', side_effect=command), patch.object(script, 'nbd', side_effect=disk), patch.object(script, 'qemu'), patch.object(script, 'extract', side_effect=extract), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                script.main()
            self.assertEqual(attached, [original])
            self.assertEqual(deleted, [original])
            self.assertEqual(names['ubuntu-bd'], replacement)

    def test_installer_disk_is_temporary_and_named_image_is_never_deleted(self):
        import common
        script = load('prepare-system')
        with tempfile.TemporaryDirectory() as directory:
            iso = Path(directory) / 'installer.iso'
            iso.write_bytes(b'installer')
            @contextlib.contextmanager
            def disk(*args):
                self.assertEqual(args[1], self.disk_id)
                yield Path(directory) / 'disk.sock'
            def extract(socket, work, version, config):
                for name in ('rootfs.tar', 'vmlinuz', 'initrd.img', 'release.json'):
                    Path(work, name).write_bytes(b'prepared')
            argv = ['prepare-system', 'ubuntu', str(iso), '--work-dir', directory]
            with patch.object(sys, 'argv', argv), patch.object(script, 'temporary_installation_name', return_value='ubuntu-bd'), patch.object(script, 'require'), patch.object(script, 'ctl', return_value=self.disk_id) as ctl, patch.object(script, 'nbd', side_effect=disk), patch.object(script, 'qemu') as qemu, patch.object(script, 'extract', side_effect=extract), contextlib.redirect_stdout(io.StringIO()):
                script.main()
            ctl.assert_any_call('stqr.yaml', 'create', 'ubuntu-bd', capture=True)
            ctl.assert_any_call('stqr.yaml', '--size', str(32 << 30), 'init-disk', self.disk_id)
            ctl.assert_any_call('stqr.yaml', 'publish', 'ubuntu', unittest.mock.ANY)
            ctl.assert_any_call('stqr.yaml', 'delete', self.disk_id)
            self.assertFalse(any(c.args[1] in ('delete', 'gc', 'clear') and c.args[2] == 'ubuntu' for c in ctl.call_args_list))
            self.assertIsNone(common.installed_disk('stqr.yaml', 'ubuntu'))
            qemu.assert_called_once()

    def test_published_image_cleanup_retry_deletes_only_recorded_temporary_disk(self):
        import common
        script = load('prepare-system')
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory) / 'release'
            work.mkdir()
            for name in ('rootfs.tar', 'vmlinuz', 'initrd.img', 'release.json'):
                (work / name).write_bytes(b'prepared')
            common.remember_preparation('stqr.yaml', 'ubuntu', work, 'published')
            common.remember_installation('stqr.yaml', 'ubuntu', self.disk_id)
            with patch.object(sys, 'argv', ['prepare-system', 'ubuntu']), patch.object(script, 'require'), patch.object(script, 'ctl') as ctl, patch.object(script, 'nbd') as nbd, contextlib.redirect_stdout(io.StringIO()):
                script.main()
            nbd.assert_not_called()
            ctl.assert_any_call('stqr.yaml', 'delete', self.disk_id)
            self.assertFalse(any(c.args[1] in ('publish', 'gc', 'clear') for c in ctl.call_args_list))
            self.assertIsNone(common.installed_disk('stqr.yaml', 'ubuntu'))

    def test_resume_without_identity_record_does_not_adopt_a_name(self):
        script = load('prepare-system')
        with tempfile.TemporaryDirectory() as directory:
            iso = Path(directory) / 'installer.iso'
            iso.write_bytes(b'installer')
            for args in (['ubuntu', '--publish-only'],
                         ['--resume', 'ubuntu', str(iso)]):
                with patch.object(sys, 'argv', ['prepare-system', *args]), patch.object(script, 'require'), patch.object(script, 'ctl') as ctl, patch.object(script, 'nbd') as nbd, self.assertRaisesRegex(RuntimeError, 'no canonical installation record'):
                    script.main()
                nbd.assert_not_called()
                self.assertFalse(any(op in call.args for call in ctl.call_args_list
                                     for op in ('resolve', 'create', 'delete', 'init-disk', 'check-install-disk')))

    def test_legacy_name_only_installation_record_is_retained_for_manual_recovery(self):
        import common
        record = common.installation_record('stqr.yaml', 'ubuntu')
        record.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        original = json.dumps({'target': 'ubuntu', 'disk': 'ubuntu-bd'})
        record.write_text(original)
        record.chmod(0o600)
        with self.assertRaisesRegex(RuntimeError, 'lacks a canonical bucket ID'):
            common.installed_disk('stqr.yaml', 'ubuntu')
        self.assertEqual(record.read_text(), original)

    def test_installation_state_cannot_select_published_image_for_deletion(self):
        import common
        with self.assertRaises(ValueError):
            common.remember_installation('stqr.yaml', 'ubuntu', 'ubuntu')
        record = common.installation_record('stqr.yaml', 'ubuntu')
        record.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        record.write_text(json.dumps({'target': 'ubuntu', 'disk': 'ubuntu'}))
        record.chmod(0o600)
        with self.assertRaises(RuntimeError):
            common.installed_disk('stqr.yaml', 'ubuntu')

    def test_guest_uses_explicit_raw_format_for_readonly_nbd(self):
        import common
        with patch.object(common, 'run') as run:
            common.guest('/private/disk.sock', 'ls', '/boot', readonly=True)
        self.assertEqual(run.call_args.args[:5], ('guestfish', '--ro', '--format=raw', '-a', 'nbd:///?socket=/private/disk.sock'))

    def test_cli_keeps_captured_diagnostic(self):
        from common import cli
        output = io.StringIO()
        with contextlib.redirect_stderr(output), self.assertRaises(SystemExit):
            cli(lambda: (_ for _ in ()).throw(subprocess.CalledProcessError(1, ['fixture'], stderr='disk inspection failed')))
        self.assertIn('disk inspection failed', output.getvalue())

    def test_cli_operational_failure_has_no_traceback(self):
        from common import cli
        with self.assertRaises(SystemExit) as stopped:
            cli(lambda: (_ for _ in ()).throw(subprocess.CalledProcessError(7,['fixture'])))
        self.assertEqual(stopped.exception.code,7)
    def test_diskless_pxe_command(self):
        script = load('test-boot')
        with patch.object(sys, 'argv', ['test-boot']), patch.object(script, 'boot_network', return_value=('eth0', '192.0.2.10', '192.0.2.0/24')), patch.object(script.shutil, 'which', return_value='/usr/bin/qemu-system-x86_64'), patch.object(script, 'wait_for_gateway') as ready, patch.object(script, 'run') as run, contextlib.redirect_stdout(io.StringIO()):
            script.main()
        ready.assert_called_once_with('http://192.0.2.10:8088/boot.ipxe')
        args = run.call_args.args
        self.assertRegex(args[args.index('-nic')+1], r'^user,model=virtio-net-pci,mac=(?:[0-9a-f]{2}:){5}[0-9a-f]{2},bootfile=http://192.0.2.10:8088/boot.ipxe$')
        self.assertIn('n', args)
        self.assertNotIn('-drive', args)
        self.assertNotIn('-blockdev', args)

    def test_plain_boot_uses_separate_copy_and_private_gateway(self):
        script = load('test-boot')
        for extra, source in (([], 'ubuntu'), (['custom-image'], 'custom-image')):
            with self.subTest(source=source), patch.object(sys, 'argv', ['test-boot', '--no-overlay', *extra]), patch.object(script, 'boot_network') as network, patch.object(script, 'wait_for_gateway') as ready, patch.object(script, 'run') as launch, contextlib.redirect_stdout(io.StringIO()):
                script.main()
            command = launch.call_args.args
            self.assertEqual(command[0], str(ROOT / 'edit-image'))
            self.assertRegex(command[1], r'^test-plain-[0-9a-f]{16}$')
            self.assertIn('--copy-from=' + source, command)
            network.assert_not_called()
            ready.assert_not_called()

    def test_parallel_pxe_launches_have_distinct_mac_and_explicit_mac_validates(self):
        script=load('test-boot')
        macs=[script.default_mac() for _ in range(20)]
        self.assertEqual(len(set(macs)),20)
        for mac in macs:
            self.assertEqual(script.mac_address(mac),mac)
            self.assertEqual(int(mac[:2],16)&3,2)
        self.assertEqual(script.mac_address('52:54:00:12:AB:CD'),'52:54:00:12:ab:cd')
        for mac in ('invalid','52:54:00:12:34','ff:ff:ff:ff:ff:ff','00:00:00:00:00:00'):
            with self.assertRaises(script.argparse.ArgumentTypeError):script.mac_address(mac)
        with patch.object(sys,'argv',['test-boot','--server','192.0.2.10','--mac','52:54:00:12:AB:CD']), patch.object(script.shutil,'which',return_value='/qemu'), patch.object(script,'wait_for_gateway'), patch.object(script,'run') as run, contextlib.redirect_stdout(io.StringIO()):
            script.main()
        self.assertIn('mac=52:54:00:12:ab:cd',run.call_args.args[-1])

    def test_pxe_waits_for_gateway_before_launch(self):
        script = load('test-boot')
        response = unittest.mock.MagicMock(status=200)
        response.read.return_value = b'#!ipxe\necho ready\n'
        response.__enter__.return_value = response
        with patch.object(script.urllib.request, 'urlopen', side_effect=[ConnectionRefusedError(), response]) as probe, patch.object(script.time, 'sleep'), contextlib.redirect_stdout(io.StringIO()):
            script.wait_for_gateway('http://192.0.2.10:8088/boot.ipxe')
        self.assertEqual(probe.call_count, 2)

    def test_pxe_gateway_timeout_does_not_launch_vm(self):
        script = load('test-boot')
        with patch.object(sys, 'argv', ['test-boot', '--server', '192.0.2.10']), patch.object(script.shutil, 'which', return_value='/qemu'), patch.object(script, 'wait_for_gateway', side_effect=RuntimeError('not ready')), patch.object(script, 'run') as qemu, self.assertRaisesRegex(RuntimeError, 'not ready'):
            script.main()
        qemu.assert_not_called()

    def test_failed_staging_retains_archive_and_restores_environment(self):
        import common
        import os
        with tempfile.TemporaryDirectory() as parent, patch.dict(os.environ, {'TMPDIR': '/previous'}):
            with self.assertRaises(RuntimeError), contextlib.redirect_stderr(io.StringIO()):
                with common.staging(parent, 'failed-') as work:
                    self.assertEqual(os.environ['TMPDIR'], work)
                    Path(work, 'rootfs.tar').write_bytes(b'finished archive')
                    raise RuntimeError('upload failed')
            self.assertEqual(os.environ['TMPDIR'], '/previous')
            self.assertEqual(Path(work, 'rootfs.tar').read_bytes(), b'finished archive')

    def test_initrds_missing_boot_hook_are_rejected(self):
        import common
        from types import SimpleNamespace
        def guest(socket, command=None, *args, **kwargs):
            output = 'vmlinuz-7.0.0-44-generic\ninitrd.img-7.0.0-44-generic\n' if command == 'ls' else 'true\n'
            return SimpleNamespace(stdout=output)
        with tempfile.TemporaryDirectory() as work, patch.object(common, 'guest', side_effect=guest), patch.object(common, 'install_native_kernel'), patch.object(common, 'install_logins'), patch.object(common, 'run', return_value=SimpleNamespace(stdout='etc/dracut\n')), contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, 'missing the STQR boot script'):
            common.extract('/private/disk.sock', work, config='private-config')

    def test_help_has_no_side_effects(self):
        for script in ['prepare-system', 'edit-image', 'serve', 'test-boot', 'pam/stqr-home']:
            result = subprocess.run([sys.executable, str(ROOT / script), '--help'], capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)

class NativePackaging(unittest.TestCase):
    def test_kernel_sources_are_self_contained_and_exclude_build_products(self):
        import common
        import tarfile
        with tempfile.TemporaryDirectory() as directory:
            bundle=Path(directory)/'sources.tar'
            common.source_archive(bundle)
            with tarfile.open(bundle) as archive:
                names=set(archive.getnames())
            self.assertTrue({'kernel/kstqr/Makefile','kernel/kstqr/vfs.c',
                             'client/c/src/core.c','client/c/src/port.h',
                             'client/c/include/stqr.h'} <= names)
            self.assertFalse(any(name.endswith(('.mod.c','.o','.ko')) for name in names))
            self.assertFalse(any('..' in Path(name).parts for name in names))

    def test_batch_guest_arguments_cannot_insert_commands(self):
        import common
        with patch.object(common,'guest') as guest:
            common.guest_many('/private/disk.sock',[('upload','/tmp/file"\nshutdown','/etc/target')])
        text=guest.call_args.kwargs['input']
        self.assertEqual(text.count('\n'),1)
        self.assertIn('\\nshutdown',text)
