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
    def setUp(self):
        state = tempfile.TemporaryDirectory()
        self.addCleanup(state.cleanup)
        env = patch.dict(os.environ, {'XDG_STATE_HOME': state.name})
        env.start()
        self.addCleanup(env.stop)

    def test_bucket_only_resumes_failed_publication_without_reextracting(self):
        import common
        script = load('prepare-system')
        with tempfile.TemporaryDirectory() as directory:
            @contextlib.contextmanager
            def disk(*args):
                yield Path(directory) / 'disk.sock'
            def extract(socket, work, version):
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
            ctl.assert_any_call('stqr.yaml', 'gc', 'ubuntu')
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
    def test_minimal_prepare_and_edit_arguments(self):
        with tempfile.TemporaryDirectory() as directory:
            image=Path(directory)/'ubuntu.iso'
            image.touch()
            for name,argv,bucket in [('prepare-system',['prepare-system','ubuntu',str(image)],'ubuntu'),('edit-image',['edit-image','ubuntu','--to-bucket','ubuntu2'],'ubuntu2')]:
                script=load(name)
                @contextlib.contextmanager
                def disk(*args):
                    yield Path(directory)/'disk.sock'
                with patch.object(sys,'argv',argv),patch.object(script,'require'),patch.object(script,'ctl') as ctl,patch.object(script,'nbd',side_effect=disk),patch.object(script,'qemu') as qemu,patch.object(script,'extract'),contextlib.redirect_stdout(io.StringIO()):
                    script.main()
                    self.assertIn(unittest.mock.call('stqr.yaml','publish',bucket,unittest.mock.ANY),ctl.call_args_list)
                    self.assertEqual(qemu.call_args.args[1].memory,4096)
    def test_minimal_serve_arguments(self):
        script=load('serve')
        with patch.object(sys,'argv',['serve','ubuntu','--check']),patch.object(script,'require'),patch.object(script,'ctl') as ctl,patch.object(script,'boot_network',return_value=('eth0','192.0.2.10','192.0.2.0/24')),patch.object(Path,'is_file',return_value=True),contextlib.redirect_stdout(io.StringIO()):
            script.main()
            ctl.assert_any_call('stqr.yaml','resolve','ubuntu')
    def test_resume_preserves_existing_installation_disk(self):
        script = load('prepare-system')
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / 'ubuntu.iso'
            image.touch()
            @contextlib.contextmanager
            def disk(*args):
                yield Path(directory) / 'disk.sock'
            with patch.object(sys, 'argv', ['prepare-system', '--resume', 'ubuntu', str(image)]), patch.object(script, 'require'), patch.object(script, 'ctl') as ctl, patch.object(script, 'nbd', side_effect=disk), patch.object(script, 'qemu'), patch.object(script, 'extract'), contextlib.redirect_stdout(io.StringIO()):
                script.main()
            ctl.assert_any_call('stqr.yaml', '--size', str(32 << 30), 'check-install-disk', 'ubuntu')
            self.assertFalse(any('create' in call.args or 'init-disk' in call.args for call in ctl.call_args_list))
    def test_publish_only_never_recreates_disk_or_starts_vm(self):
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

    def test_gc_failure_reports_image_already_published(self):
        script = load('prepare-system')
        @contextlib.contextmanager
        def disk(*args):
            yield Path('/private/disk.sock')
        def command(config, op, *args):
            if op == 'gc':
                raise subprocess.CalledProcessError(1, ['gc'])
        with patch.object(sys, 'argv', ['prepare-system', '--publish-only', 'ubuntu']), patch.object(script, 'staging', side_effect=disk), patch.object(script, 'require'), patch.object(script, 'ctl', side_effect=command), patch.object(script, 'nbd', side_effect=disk), patch.object(script, 'extract'), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()), self.assertRaisesRegex(RuntimeError, 'ubuntu is published, but cleanup failed'):
            script.main()

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
        self.assertIn('user,model=virtio-net-pci,bootfile=http://192.0.2.10:8088/boot.ipxe', args)
        self.assertIn('n', args)
        self.assertNotIn('-drive', args)
        self.assertNotIn('-blockdev', args)

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
        with tempfile.TemporaryDirectory() as work, patch.object(common, 'guest', side_effect=guest), patch.object(common, 'install_native_kernel'), patch.object(common, 'run', return_value=SimpleNamespace(stdout='etc/dracut\n')), contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, 'missing the STQR boot script'):
            common.extract('/private/disk.sock', work)

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
