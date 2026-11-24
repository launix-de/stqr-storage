# SPDX-License-Identifier: GPL-3.0-only
import contextlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

HERE = Path(__file__).resolve().parent
BIN = HERE / 'bin'

def run(*args, **kwargs):
    return subprocess.run([str(a) for a in args], check=True, **kwargs)

def ctl(config, *args, capture=False):
    result = run(BIN / 'stqr-bootctl', '--config', config, *args,
                 stdout=subprocess.PIPE if capture else None, text=True)
    return result.stdout.strip() if capture else None

def require(*programs):
    for name in programs:
        if not shutil.which(name):
            raise RuntimeError(f'Missing dependency: {name}')
    if not (BIN / 'stqr-bootctl').is_file():
        raise RuntimeError('Run make -C networkboot first')

@contextlib.contextmanager
def nbd(config, bucket, work, readonly=False):
    socket = Path(work) / 'disk.sock'
    args = [BIN / 'stqr-bootctl', '--config', config, '--socket', socket]
    if readonly:
        args += ['--readonly']
    process = subprocess.Popen([str(a) for a in args + ['nbd', bucket]])
    try:
        for _ in range(300):
            if process.poll() is not None:
                raise RuntimeError('NBD export failed; bucket must be idle and exclusive maintenance available')
            if socket.exists():
                break
            time.sleep(.1)
        else:
            raise RuntimeError('NBD startup timed out')
        yield socket
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise RuntimeError('NBD failed to checkpoint before termination; inspect disk before publishing')
        if process.returncode:
            raise RuntimeError(f'NBD exited with status {process.returncode}; image not published')

def guest(socket, *commands, readonly=False, **kwargs):
    args = ['guestfish'] + (['--ro'] if readonly else [])
    # libguestfs uses the same standard NBD endpoint as QEMU.
    return run(*args, '-a', f'nbd:///?socket={socket}', '-i', *commands, **kwargs)

def qemu(socket, args, installer=None):
    block = {'driver': 'nbd', 'server': {'type': 'unix', 'path': str(socket)}, 'node-name': 'stqr-disk'}
    command = ['qemu-system-x86_64', '-name', 'STQR image builder', '-m', str(args.memory),
               '-smp', str(args.cpus), '-machine', 'accel=kvm:tcg', '-netdev', 'user,id=net0',
               '-device', 'virtio-net-pci,netdev=net0', '-blockdev', json.dumps(block),
               '-device', 'virtio-blk-pci,drive=stqr-disk,bootindex=2']
    if args.uefi:
        command += ['-bios', str(Path(args.uefi).resolve())]
    if installer:
        command += ['-blockdev', json.dumps({'driver': 'raw', 'file': {'driver': 'file', 'filename': str(Path(installer).resolve())}, 'node-name': 'installer', 'read-only': True}), '-device', 'ide-cd,drive=installer,bootindex=1', '-no-reboot']
    run(*command)

def extract(socket, work, kernel_version=None):
    """Install boot support offline, then archive root with all Linux metadata."""
    work = Path(work)
    guest(socket, 'mkdir-p', '/usr/lib/stqr-networkboot')
    guest(socket, 'upload', BIN / 'stqr-nbd-client', '/usr/lib/stqr-networkboot/nbd-client')
    guest(socket, 'chmod', '0755', '/usr/lib/stqr-networkboot/nbd-client')
    guest(socket, 'mkdir-p', '/etc/initramfs-tools/scripts')
    guest(socket, 'upload', HERE / 'initramfs/scripts/stqr', '/etc/initramfs-tools/scripts/stqr')
    guest(socket, 'chmod', '0755', '/etc/initramfs-tools/scripts/stqr')
    guest(socket, 'mkdir-p', '/etc/initramfs-tools/hooks')
    guest(socket, 'mkdir-p', '/etc/initramfs-tools/conf.d')
    guest(socket, 'upload', HERE / 'initramfs/modules.conf', '/etc/initramfs-tools/conf.d/stqr')
    guest(socket, 'upload', HERE / 'initramfs/hooks/stqr', '/etc/initramfs-tools/hooks/stqr')
    guest(socket, 'chmod', '0755', '/etc/initramfs-tools/hooks/stqr')
    # Fail rather than publishing a disk with an unbootable, mismatched initramfs.
    guest(socket, 'command', '/usr/sbin/update-initramfs -u -k all')
    listing = subprocess.run(['guestfish', '--ro', '-a', f'nbd:///?socket={socket}', '-i', 'ls', '/boot'],
                             check=True, capture_output=True, text=True).stdout.splitlines()
    versions = [p[len('vmlinuz-'):] for p in listing if p.startswith('vmlinuz-') and 'initrd.img-' + p[len('vmlinuz-'):] in listing]
    if kernel_version:
        versions = [v for v in versions if v == kernel_version]
    if not versions:
        raise RuntimeError('No matching Ubuntu/Debian vmlinuz and initrd.img in /boot; use --kernel-version')
    version = subprocess.run(['sort', '-V'], input='\n'.join(versions), text=True,
                             capture_output=True, check=True).stdout.splitlines()[-1]
    guest(socket, 'download', '/boot/vmlinuz-' + version, work / 'vmlinuz', readonly=True)
    guest(socket, 'download', '/boot/initrd.img-' + version, work / 'initrd.img', readonly=True)
    # Work on the disk itself only after the VM has disconnected. Replace disk
    # fstab entries for network root, while preserving the original for editing.
    guest(socket, 'sh', 'cp -p /etc/fstab /etc/fstab.stqr-original; printf "# STQR network root\\n" > /etc/fstab')
    try:
        guest(socket, 'tar-out', '/', work / 'rootfs.tar', 'numericowner:true',
              'xattrs:true', 'acls:true', 'selinux:true',
              'excludes:./proc/* ./sys/* ./dev/* ./run/* ./tmp/* ./home/*', readonly=True)
    finally:
        guest(socket, 'command', '/bin/cp -p /etc/fstab.stqr-original /etc/fstab')
    (work / 'release.json').write_text(json.dumps({'schema': 1, 'kernel': version,
        'boot': 'nfs3-readonly-with-tmpfs-overlay', 'architecture': 'x86_64'}))


def cli(main):
    """Report operational failures without a Python implementation traceback."""
    try:
        main()
    except subprocess.CalledProcessError as error:
        raise SystemExit(error.returncode)
    except (RuntimeError, OSError) as error:
        print(f'{Path(sys.argv[0]).name}: {error}', file=sys.stderr)
        raise SystemExit(1)
    except KeyboardInterrupt:
        raise SystemExit(130)
