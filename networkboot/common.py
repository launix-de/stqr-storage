# SPDX-License-Identifier: GPL-3.0-only
import contextlib
import hashlib
import json
import ipaddress
import re
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import tarfile

HERE = Path(__file__).resolve().parent
BIN = HERE / 'bin'

def run(*args, **kwargs):
    return subprocess.run([str(a) for a in args], check=True, **kwargs)

@contextlib.contextmanager
def staging(base, prefix, existing=None):
    """Keep failed exports for retry; put bounded SDK spools on the work disk."""
    work = str(existing) if existing is not None else tempfile.mkdtemp(prefix=prefix, dir=base)
    old = os.environ.get('TMPDIR')
    os.environ['TMPDIR'] = str(Path(work).resolve())
    try:
        yield work
    except BaseException:
        print(f'Failed; staging retained at {work}', file=sys.stderr)
        raise
    else:
        shutil.rmtree(work)
    finally:
        if old is None:
            os.environ.pop('TMPDIR', None)
        else:
            os.environ['TMPDIR'] = old

def preparation_record(config, bucket):
    identity = json.dumps([str(Path(config).resolve()), bucket]).encode()
    base = Path(os.environ.get('XDG_STATE_HOME', Path.home() / '.local/state')) / 'stqr/networkboot'
    return base / (hashlib.sha256(identity).hexdigest() + '.json')

def remember_preparation(config, bucket, work, phase='prepared'):
    """Remember completed extraction/publication without exposing work paths in the CLI."""
    work = Path(work).resolve()
    if not all((work / name).is_file() for name in ('rootfs.tar', 'vmlinuz', 'initrd.img', 'release.json')):
        return
    record = preparation_record(config, bucket)
    record.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    data = json.dumps({'work': str(work), 'phase': phase, 'login_setup': 1, 'image_layout': 2})
    marker = work / '.stqr-preparation.json'
    marker.write_text(str(record))
    marker.chmod(0o600)
    fd, temporary = tempfile.mkstemp(prefix='.prepare-', dir=record.parent)
    try:
        with os.fdopen(fd, 'w') as out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, record)
    finally:
        Path(temporary).unlink(missing_ok=True)

def prepared_release(config, bucket):
    record = preparation_record(config, bucket)
    try:
        if record.is_symlink() or record.stat().st_uid != os.getuid() or record.stat().st_mode & 0o077:
            return None
        data = json.loads(record.read_text())
        work = Path(data['work'])
        marker = work / '.stqr-preparation.json'
        if (work.is_symlink() or work.stat().st_uid != os.getuid()
                or data['phase'] not in ('prepared', 'published')
                or data['phase'] == 'prepared' and data.get('login_setup') != 1
                or marker.read_text() != str(record)
                or not all((work / name).is_file() for name in ('rootfs.tar', 'vmlinuz', 'initrd.img', 'release.json'))):
            return None
        return data
    except (OSError, ValueError, KeyError, TypeError):
        return None

def temporary_installation_name(bucket):
    if not bucket or len((bucket + '-bd').encode()) > 255:
        raise ValueError('image name is too long for its installation bucket')
    return bucket + '-bd'

def installation_record(config, bucket):
    base = preparation_record(config, bucket)
    return base.with_name('installation-' + base.name)

def remember_installation(config, target, disk):
    if disk != temporary_installation_name(target):
        raise ValueError('installation disk must be named [image]-bd')
    record = installation_record(config, target)
    record.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.install-', dir=record.parent)
    try:
        with os.fdopen(fd, 'w') as out:
            json.dump({'target': target, 'disk': disk}, out)
            out.flush()
            os.fsync(out.fileno())
        os.replace(temporary, record)
    finally:
        Path(temporary).unlink(missing_ok=True)

def installed_disk(config, target):
    record = installation_record(config, target)
    try:
        if record.is_symlink() or record.stat().st_uid != os.getuid() or record.stat().st_mode & 0o077:
            raise RuntimeError('unsafe installation state file')
        data = json.loads(record.read_text())
        if data['target'] != target or not isinstance(data['disk'], str) or data['disk'] != temporary_installation_name(target):
            raise RuntimeError('invalid installation state')
        return data['disk']
    except FileNotFoundError:
        return None

def forget_installation(config, target):
    installation_record(config, target).unlink(missing_ok=True)

def forget_preparation(config, bucket):
    preparation_record(config, bucket).unlink(missing_ok=True)

def ctl(config, *args, capture=False, timeout=None):
    result = run(BIN / 'stqr-bootctl', '--config', config, *args,
                 stdout=subprocess.PIPE if capture else None, text=True,
                 **({'timeout': timeout} if timeout is not None else {}))
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
    # Builder devices are raw disks. Suppress format probing: libguestfs
    # otherwise invokes qemu-img info for readonly NBD accesses.
    return run(*args, '--format=raw', '-a', f'nbd:///?socket={socket}', '-i', *commands, **kwargs)

def guest_many(socket, commands, readonly=False):
    # Reuse one libguestfs appliance for a sequence of bounded offline changes.
    text = ''.join(command[0] + ' ' + ' '.join(json.dumps(str(value), ensure_ascii=False)
                    for value in command[1:]) + '\n' for command in commands)
    return guest(socket, readonly=readonly, input=text, text=True)


def qemu(socket, args, installer=None):
    block = {'driver': 'nbd', 'server': {'type': 'unix', 'path': str(socket)}, 'node-name': 'stqr-disk', 'discard': 'unmap'}
    command = ['qemu-system-x86_64', '-name', 'STQR image builder', '-m', str(args.memory),
               '-smp', str(args.cpus), '-machine', 'accel=kvm:tcg', '-netdev', 'user,id=net0',
               '-device', 'virtio-net-pci,netdev=net0', '-blockdev', json.dumps(block),
               '-device', 'virtio-blk-pci,drive=stqr-disk,bootindex=2']
    if args.uefi:
        command += ['-bios', str(Path(args.uefi).resolve())]
    if installer:
        command += ['-blockdev', json.dumps({'driver': 'raw', 'file': {'driver': 'file', 'filename': str(Path(installer).resolve())}, 'node-name': 'installer', 'read-only': True}), '-device', 'ide-cd,drive=installer,bootindex=1', '-no-reboot']
    run(*command)

def source_archive(destination):
    """Ship exactly the sources matching the built mount helpers."""
    with tarfile.open(destination, 'w') as archive:
        for relative in ('kernel/kstqr', 'client/c/include', 'client/c/src'):
            for item in (HERE.parent / relative).iterdir():
                if item.is_file() and (item.suffix in ('.c', '.h') and not item.name.endswith('.mod.c') or item.name == 'Makefile'):
                    archive.add(item, arcname=str(Path(relative) / item.name), recursive=False)


def install_native_kernel(socket, work, version):
    """Build outside the guest, against its exact installed kernel headers."""
    require('make', 'cc')
    if not all((BIN / name).is_file() for name in ('stqr-boot-root', 'mount.kstqr')):
        raise RuntimeError('Run make -C networkboot to build native boot helpers')
    headers = Path(work) / 'headers'
    headers.mkdir(exist_ok=True)
    print(f'Exporting kernel headers for {version}...', flush=True)
    kdir = headers / ('linux-headers-' + version)
    if not (kdir / 'Makefile').is_file():
        archive = Path(work) / 'headers.tar'
        guest(socket, 'tar-out', '/usr/src', archive, readonly=True)
        with tarfile.open(archive) as source:
            source.extractall(headers, filter='data')
        archive.unlink()
    if not (kdir / 'Makefile').is_file():
        raise RuntimeError(f'Install linux-headers-{version} in the builder VM before publishing')
    source = Path(work) / 'module-source'
    source.mkdir(exist_ok=True)
    bundle = BIN / 'kstqr-source.tar'
    if not bundle.is_file():
        raise RuntimeError('Run make -C networkboot to package the matching kernel sources')
    with tarfile.open(bundle) as archive:
        archive.extractall(source, filter='data')
    module = source / 'kernel/kstqr'
    print(f'Building kstqr for {version}...', flush=True)
    run('make', '-C', module, 'KDIR=' + str(kdir.resolve()), '-j' + str(min(4, os.cpu_count() or 1)))
    commands = []
    for directory in ('/usr/lib/stqr-networkboot', '/etc/initramfs-tools/scripts',
                      '/etc/initramfs-tools/hooks', '/etc/initramfs-tools/conf.d',
                      f'/lib/modules/{version}/extra'):
        commands.append(('mkdir-p', directory))
    for origin, target, mode in (
        (module / 'kstqr.ko', f'/lib/modules/{version}/extra/kstqr.ko', '0644'),
        (BIN / 'stqr-boot-root', '/usr/lib/stqr-networkboot/boot-root', '0755'),
        (BIN / 'mount.kstqr', '/sbin/mount.kstqr', '0755'),
        (HERE / 'initramfs/scripts/stqr', '/etc/initramfs-tools/scripts/stqr', '0755'),
        (HERE / 'initramfs/hooks/stqr', '/etc/initramfs-tools/hooks/stqr', '0755'),
        (HERE / 'initramfs/modules.conf', '/etc/initramfs-tools/conf.d/stqr', '0644'),
    ):
        commands.append(('upload', origin, target))
        commands.append(('chmod', mode, target))
    commands.append(('command', f'/sbin/depmod -a {version}'))
    guest_many(socket, commands)


def install_logins(socket, work, config):
    """Prepare identities using host admin rights; publish no admin credentials."""
    plan = Path(work) / 'login.json'
    plan.unlink(missing_ok=True)
    ctl(config, 'prepare-logins', plan)
    if not (BIN / 'pam_stqr.so').is_file():
        raise RuntimeError('Run make -C networkboot all pam before preparing logins')
    commands = [('mkdir-p', '/usr/libexec'), ('mkdir-p', '/usr/lib/stqr-networkboot')]
    for source, target, mode in (
        (plan, '/usr/lib/stqr-networkboot/login.json', '0600'),
        (BIN / 'pam_stqr.so', '/usr/lib/stqr-networkboot/pam_stqr.so', '0644'),
        (BIN / 'stqr-bootctl', '/usr/libexec/stqr-bootctl', '0755'),
        (HERE / 'pam/stqr-home', '/usr/libexec/stqr-home', '0755'),
        (HERE / 'pam/install-login', '/usr/lib/stqr-networkboot/install-login', '0755'),
    ):
        commands.extend([('upload', source, target), ('chmod', mode, target)])
    commands.append(('command', '/usr/bin/python3 /usr/lib/stqr-networkboot/install-login /usr/lib/stqr-networkboot/login.json'))
    guest_many(socket, commands)


def extract(socket, work, kernel_version=None, config=None):
    """Install boot support offline, then archive root with all Linux metadata."""
    work = Path(work)
    listing = guest(socket, 'ls', '/boot', readonly=True,
                    capture_output=True, text=True).stdout.splitlines()
    versions = [p[len('vmlinuz-'):] for p in listing if p.startswith('vmlinuz-') and 'initrd.img-' + p[len('vmlinuz-'):] in listing]
    if kernel_version:
        versions = [v for v in versions if v == kernel_version]
    if not versions:
        raise RuntimeError('No matching Ubuntu/Debian vmlinuz and initrd.img in /boot; use --kernel-version')
    version = subprocess.run(['sort', '-V'], input='\n'.join(versions), text=True,
                             capture_output=True, check=True).stdout.splitlines()[-1]
    if not re.fullmatch(r'[0-9][a-zA-Z0-9_.+\-]*', version):
        raise RuntimeError('Invalid kernel version in /boot')
    # Ubuntu can provide a Dracut-backed update-initramfs shim alongside
    # mkinitramfs. Build a separate network initrd using our supported hooks;
    # leave the distribution's ordinary VM-boot initrd untouched.
    available = guest(socket, 'is-file', '/usr/sbin/mkinitramfs', readonly=True,
                      capture_output=True, text=True).stdout.strip()
    if available != 'true':
        raise RuntimeError('Networkboot preparation requires initramfs-tools-core (mkinitramfs) in the installed image')
    if config is None:
        raise RuntimeError("STQR configuration required for login preparation")
    install_logins(socket, work, config)
    install_native_kernel(socket, work, version)
    print('Generating dedicated networkboot initramfs...', flush=True)
    network_initrd = '/boot/initrd.stqr-' + version
    guest(socket, 'command', f'/usr/sbin/mkinitramfs -o {network_initrd} {version}')
    guest_many(socket, [('download', '/boot/vmlinuz-' + version, work / 'vmlinuz'),
                        ('download', network_initrd, work / 'initrd.img')], readonly=True)
    contents = run('lsinitramfs', work / 'initrd.img', capture_output=True, text=True).stdout.splitlines()
    if not any(p.lstrip('./') == 'scripts/stqr' for p in contents):
        raise RuntimeError('Generated initramfs is missing the STQR boot script; image not published')
    if not any(p.endswith('/kstqr.ko') for p in contents):
        raise RuntimeError('Generated initramfs is missing the matching kstqr module; image not published')
    # Work on the disk itself only after the VM has disconnected. Replace disk
    # fstab entries for network root, while preserving the original for editing.
    guest(socket, 'sh', 'cp -p /etc/fstab /etc/fstab.stqr-original; printf "# STQR network root\\n" > /etc/fstab')
    print('Exporting installed root filesystem...', flush=True)
    try:
        guest(socket, 'tar-out', '/', work / 'rootfs.tar', 'numericowner:true',
              'xattrs:true', 'acls:true', 'selinux:true',
              'excludes:./proc/* ./sys/* ./dev/* ./run/* ./tmp/* ./home/*', readonly=True)
    finally:
        guest(socket, 'command', '/bin/cp -p /etc/fstab.stqr-original /etc/fstab')
    (work / 'release.json').write_text(json.dumps({'schema': 1, 'kernel': version,
        'boot': 'kstqr-overlay', 'architecture': 'x86_64', 'login_setup': 1}))


def cli(main):
    """Report operational failures without a Python implementation traceback."""
    try:
        main()
    except subprocess.CalledProcessError as error:
        if error.stderr:
            detail = error.stderr.decode(errors='replace') if isinstance(error.stderr, bytes) else error.stderr
            print(detail.rstrip(), file=sys.stderr)
        raise SystemExit(error.returncode)
    except (RuntimeError, OSError) as error:
        print(f'{Path(sys.argv[0]).name}: {error}', file=sys.stderr)
        raise SystemExit(1)
    except KeyboardInterrupt:
        raise SystemExit(130)


def boot_network(interface=None, server=None, clients=None):
    """Infer the boot LAN from the default route and its configured IPv4 subnet."""
    if interface and server and clients:
        return interface, server, clients
    if not interface:
        routes=json.loads(subprocess.check_output(['ip','-j','-4','route','show','default'],text=True))
        devices={route['dev'] for route in routes if 'dev' in route}
        if len(devices)!=1:
            raise ValueError('cannot select a unique boot interface; specify --interface')
        interface=devices.pop()
    addresses=json.loads(subprocess.check_output(['ip','-j','-4','addr','show','dev',interface],text=True))
    candidates=[a for link in addresses for a in link.get('addr_info',[]) if a.get('scope')=='global']
    if server:
        candidates=[a for a in candidates if a['local']==server]
    if len(candidates)!=1:
        raise ValueError('cannot select a unique boot IPv4 address; specify --interface and --server')
    address=candidates[0]
    server=server or address['local']
    clients=clients or str(ipaddress.ip_network(f"{server}/{address['prefixlen']}",strict=False))
    return interface,server,clients
