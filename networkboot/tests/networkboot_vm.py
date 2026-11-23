#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only
"""Opt-in isolated NFS -> SquashFS -> overlay -> switch_root QEMU regression."""
import hashlib
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]

def run(*args, **kwargs):
    return subprocess.run([str(x) for x in args], check=True, **kwargs)

def main():
    kernel = Path(os.environ['STQR_KERNEL_IMAGE'])
    version = os.environ.get('STQR_KERNEL_VERSION', os.uname().release)
    with tempfile.TemporaryDirectory(prefix='stqr-netboot-vm-') as temporary:
        work = Path(temporary); image_root = work / 'image'; initrd = work / 'initrd'; export = work / 'export'
        for root in [image_root, initrd]:
            for name in ['bin', 'dev', 'proc', 'sys', 'run', 'sbin', 'etc', 'lib', 'newroot']:
                (root / name).mkdir(parents=True, exist_ok=True)
            shutil.copyfile(shutil.which('busybox'), root / 'bin/busybox');(root / 'bin/busybox').chmod(0o755)
            (root / 'bin/sh').symlink_to('busybox')
        (image_root / 'etc/base').write_text('immutable base')
        (image_root / 'sbin/init').write_text('#!/bin/sh\nset -e\n[ "$(cat /etc/base)" = "immutable base" ]\necho private > /etc/private\n[ "$(cat /etc/private)" = private ]\n/bin/busybox sync\necho STQR_NETWORK_ROOT_PASS\n/bin/busybox poweroff -f\n')
        (image_root / 'sbin/init').chmod(0o755)
        # Busybox applets used by the root init script.
        for name in ['cat', 'poweroff']:
            (image_root / 'bin' / name).symlink_to('busybox')
        export.mkdir(); run('mksquashfs', image_root, export / 'rootfs.squashfs', '-noappend', '-no-progress', stdout=subprocess.DEVNULL)
        modules = []
        for name in ['virtio_net', 'nfs', 'nfsv3', 'squashfs', 'overlay', 'loop']:
            output = run('modprobe', '--show-depends', '-S', version, name, capture_output=True, text=True).stdout
            for line in output.splitlines():
                if line.startswith('insmod '):
                    source = Path(line.split()[1]);target = initrd / 'lib' / (source.name.removesuffix('.zst'))
                    if target.name not in modules:
                        if source.suffix == '.zst':
                            with target.open('wb') as f: run('zstd', '-d', '-c', source, stdout=f)
                        else: shutil.copyfile(source, target)
                        modules.append(target.name)
        probe = socket.socket();probe.bind(('127.0.0.1', 0));port = probe.getsockname()[1];probe.close()
        nbd = None
        nbd_port = None
        if os.environ.get('STQR_TEST_NBD_UPPER') == '1':
            raw = work / 'upper.raw'
            with raw.open('wb') as f: f.truncate(64<<20)
            run('mkfs.ext4','-q','-F',raw)
            probe = socket.socket();probe.bind(('127.0.0.1',0));nbd_port=probe.getsockname()[1];probe.close()
            nbd = subprocess.Popen(['qemu-nbd','--persistent','--format=raw','--bind=127.0.0.1','--port='+str(nbd_port),str(raw)])
            shutil.copyfile(ROOT / 'bin/stqr-nbd-client',initrd / 'bin/stqr-nbd-client')
            (initrd / 'bin/stqr-nbd-client').chmod(0o755)
            for name in ['nbd','ext4']:
                output=run('modprobe','--show-depends','-S',version,name,capture_output=True,text=True).stdout
                for line in output.splitlines():
                    if line.startswith('insmod '):
                        source=Path(line.split()[1]);target=initrd/'lib'/source.name.removesuffix('.zst')
                        if target.name not in modules:
                            if source.suffix=='.zst':
                                with target.open('wb') as f:run('zstd','-d','-c',source,stdout=f)
                            else:shutil.copyfile(source,target)
                            modules.append(target.name)
            # Preformatted ext4 tests the kernel NBD client and root overlay.
            # The production initramfs formats its fresh export itself.
        nfs = subprocess.Popen([str(ROOT / 'bin/stqr-boot-nfs'), '--root', str(export), '--listen', f'127.0.0.1:{port}', '--allow', '127.0.0.0/8', '--release', hashlib.sha256(b'test').hexdigest()])
        try:
            time.sleep(.3)
            script = '#!/bin/sh\nset -ex\nexport PATH=/bin\n/bin/busybox --install -s /bin\nmount -t proc proc /proc\nmount -t sysfs sysfs /sys\nmount -t devtmpfs devtmpfs /dev\nmount -t tmpfs tmpfs /run\n'
            script += ''.join(f'insmod /lib/{name}\n' for name in modules)
            script += f'''ip link set eth0 up
ip addr add 10.0.2.15/24 dev eth0
mkdir -p /run/stqr/export /run/stqr/lower /run/stqr/ram
mount -t nfs -o ro,vers=3,proto=tcp,port={port},mountport={port},mountproto=tcp,nolock,noacl 10.0.2.2:/ /run/stqr/export
mount -t squashfs -o loop,ro /run/stqr/export/rootfs.squashfs /run/stqr/lower
mount -t tmpfs tmpfs /run/stqr/ram
mkdir -p /run/stqr/ram/upper /run/stqr/ram/work
mount -t overlay overlay -o lowerdir=/run/stqr/lower,upperdir=/run/stqr/ram/upper,workdir=/run/stqr/ram/work /newroot
mount --move /proc /newroot/proc
mount --move /sys /newroot/sys
mount --move /dev /newroot/dev
mount --move /run /newroot/run
exec switch_root /newroot /sbin/init
'''
            if nbd_port:
                script=script.replace('mount -t tmpfs tmpfs /run/stqr/ram',f'ls -l /bin/stqr-nbd-client\n/bin/stqr-nbd-client 10.0.2.2 {nbd_port} /dev/nbd0\nmount -t ext4 /dev/nbd0 /run/stqr/ram')
            (initrd / 'init').write_text(script);(initrd / 'init').chmod(0o755)
            listing = b''.join(str(p.relative_to(initrd)).encode() + b'\0' for p in initrd.rglob('*'))
            archive = work / 'initrd.cpio'
            with archive.open('wb') as f:
                run('cpio', '--null', '-o', '-H', 'newc', input=listing, cwd=initrd, stdout=f, stderr=subprocess.DEVNULL)
            result = subprocess.run(['qemu-system-x86_64', '-machine', 'q35,accel=kvm:tcg', '-m', '512M', '-smp', '2', '-display', 'none', '-serial', 'stdio', '-monitor', 'none', '-no-reboot', '-kernel', str(kernel), '-initrd', str(archive), '-append', 'console=ttyS0 panic=1 rdinit=/init loglevel=5', '-nic', 'user,model=virtio-net-pci'], capture_output=True, text=True, timeout=90)
            log = Path(os.environ.get('STQR_NETWORKBOOT_VM_LOG', '/tmp/stqr-networkboot-vm.log'));log.write_text(result.stdout + result.stderr)
            if 'STQR_NETWORK_ROOT_PASS' not in result.stdout:
                raise RuntimeError(f'network root boot failed; see {log}')
            print('STQR_NETWORK_ROOT_PASS')
        finally:
            nfs.terminate();nfs.wait(timeout=10)
            if nbd:
                nbd.terminate();nbd.wait(timeout=10)

if __name__ == '__main__':
    main()
