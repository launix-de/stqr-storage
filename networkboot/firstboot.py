# SPDX-License-Identifier: GPL-3.0-only
"""Complete a real installed-disk first boot before publishing a Snap image.

Only a caller-owned temporary installation disk is modified. snapd runs under
its own installed PID1, never inside guestfish/chroot. The host accepts success
only after a nonce-bound result and the stopped disk's actual seeded state.
"""
import json
import hashlib
import inspect
import re
from pathlib import Path
import secrets
import subprocess

from common import guest, guest_many, qemu_command, require

STAMP = 'first-boot-validated.json'
STATE = '/var/lib/snapd/state.json'
OWNER = '/var/lib/stqr-networkboot/first-boot-owner.json'


def read_text(socket, path):
    return guest(socket, 'cat', path, readonly=True, capture_output=True, text=True).stdout


def is_file(socket, path):
    return guest(socket, 'is-file', path, readonly=True, capture_output=True, text=True).stdout.strip() == 'true'


def seed_state(socket):
    if not is_file(socket, STATE):
        return None
    state = json.loads(read_text(socket, STATE))
    if not isinstance(state, dict) or not isinstance(state.get('data'), dict):
        raise RuntimeError('invalid installed snapd state; image not published')
    return state


def seeded(state):
    if state is None or state['data'].get('seeded') is not True:
        return False
    # Snapd change status0 is computed from its task statuses (default task0=Do).
    # Never accept a failed/incomplete seed just because the flag is present.
    tasks = state.get('tasks', {})
    for change in state.get('changes', {}).values():
        if change.get('kind') != 'seed':
            continue
        if change.get('status', 0) not in (0, 4):
            return False
        for task_id in change.get('task-ids', []):
            if tasks.get(task_id, {}).get('status', 0) != 4:
                return False
    return True


def write_stamp(work, disposition):
    (Path(work)/STAMP).write_text(json.dumps({'schema': 1, 'snap_seed': disposition}) + '\n')


def boot(socket, args, work, timeout=720):
    """Use the installed BIOS/UEFI bootloader and real network, without the ISO."""
    log = Path(work)/'first-boot-qemu.log'
    serial = Path(work)/'first-boot-serial.log'
    command = qemu_command(socket, args)
    command[command.index('-name')+1] = 'STQR installed-system initialization'
    command += ['-display', 'none',
                '-serial', 'file:'+str(serial), '-monitor', 'stdio', '-no-reboot']
    with log.open('wb') as output:
        process = subprocess.Popen([str(a) for a in command], stdin=subprocess.PIPE,
                                   stdout=output, stderr=subprocess.STDOUT)
        try:
            try:
                status = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                # Give the real OS a clean shutdown before the final bounded
                # kill. Any timeout still rejects publication, even with a marker.
                try:
                    process.stdin.write(b'system_powerdown\n')
                    process.stdin.flush()
                except (OSError, BrokenPipeError):
                    pass
                try:
                    process.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                raise RuntimeError(f'installed first boot timed out; disk retained, diagnostics: {work}')
            if status:
                raise RuntimeError(f'installed first boot exited {status}; disk retained, diagnostics: {work}')
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
            process.stdin.close()


def owned_paths(nonce):
    if not isinstance(nonce, str) or not re.fullmatch('[0-9a-f]{32}', nonce):
        raise RuntimeError('invalid first-boot ownership receipt; disk retained')
    name = 'stqr-first-boot-'+nonce
    return ('/usr/libexec/'+name, '/etc/systemd/system/'+name+'.service',
            '/etc/systemd/system/multi-user.target.wants/'+name+'.service',
            '/var/lib/stqr-networkboot/'+name+'.json')


def cleanup_owned(socket, record):
    if record.get('schema') != 1:
        raise RuntimeError('unknown first-boot ownership receipt; disk retained')
    helper, unit, link, result = owned_paths(record.get('nonce'))
    for path, key in ((helper, 'helper_sha256'), (unit, 'unit_sha256')):
        if is_file(socket, path) and hashlib.sha256(read_text(socket, path).encode()).hexdigest() != record.get(key):
            raise RuntimeError('first-boot temporary asset changed; refusing cleanup: '+path)
    guest_many(socket, [('rm-f', link), ('rm-f', unit), ('rm-f', helper),
                        ('rm-f', result), ('rm-f', result+'.tmp'), ('rm-f', OWNER)])


def completion_program(nonce, result):
    # Same actual-state predicate on both sides of the clean shutdown. The
    # seed.loaded API can become ready before final seed tasks finish.
    return '#!/usr/bin/python3\nimport json,os,subprocess,time\n'+inspect.getsource(seeded)+"""
status = 'failed'
deadline = time.monotonic()+600
try:
    subprocess.run(['/usr/bin/snap','wait','system','seed.loaded'], check=True, timeout=600)
    while time.monotonic() < deadline:
        with open('/var/lib/snapd/state.json') as stream:
            state = json.load(stream)
        if seeded(state):
            status = 'seed-loaded'
            break
        if any(change.get('kind') == 'seed' and change.get('status') in (6,7,8,9)
               for change in state.get('changes', {}).values()):
            break
        time.sleep(1)
except (OSError,ValueError,subprocess.SubprocessError):
    pass
finally:
    os.umask(0o077)
    os.makedirs('/var/lib/stqr-networkboot', exist_ok=True)
    result = """+repr(result)+"""
    with open(result+'.tmp','w') as stream:
        json.dump({'schema':1,'nonce':"""+repr(nonce)+""",'status':status}, stream)
    os.replace(result+'.tmp',result)
    # Explicit image-publication checkpoint, never a per-write implicit sync.
    os.sync()
    subprocess.run(['/usr/bin/systemctl','--no-block','poweroff'],check=True)
"""


def prepare_first_boot(socket, work, args):
    # A cancelled prior attempt must not archive an old auto-poweroff unit.
    if is_file(socket, OWNER):
        cleanup_owned(socket, json.loads(read_text(socket, OWNER)))
    # Non-Snap operating systems retain the existing workflow and need no QEMU.
    if not is_file(socket, '/usr/bin/snap'):
        write_stamp(work, 'not-installed')
        return
    if seeded(seed_state(socket)):
        write_stamp(work, 'already-seeded')
        return
    if not is_file(socket, '/usr/bin/systemctl') or not is_file(socket, '/usr/bin/python3'):
        raise RuntimeError('pending Snap seed requires installed systemd/Python; disk retained')
    require('qemu-system-x86_64')
    nonce = secrets.token_hex(16)
    name = 'stqr-first-boot-'+nonce
    helper, unit, link, result = owned_paths(nonce)
    script = Path(work)/(name+'.py')
    service = Path(work)/(name+'.service')
    receipt = Path(work)/(name+'.owner.json')
    script.write_text(completion_program(nonce, result))
    service.write_text('''[Unit]
Description=Complete STQR installed-image Snap initialization
Wants=network-online.target snapd.service
After=network-online.target snapd.service
[Service]
Type=oneshot
TimeoutStartSec=660
ExecStart='''+helper+'''
''')
    # Unique owned paths cannot collide with administrator units. No default
    # target, existing unit, snap config, task, seed flag or firmware is replaced.
    existing = guest(socket, readonly=True, capture_output=True, text=True,
                     input=''.join('exists '+json.dumps(path)+'\n' for path in (helper, unit, link, result))).stdout.splitlines()
    if existing != ['false'] * 4:
        raise RuntimeError('refusing pre-existing first-boot fixture paths; installation disk retained')
    record = {'schema': 1, 'nonce': nonce, 'helper_sha256': hashlib.sha256(script.read_bytes()).hexdigest(),
              'unit_sha256': hashlib.sha256(service.read_bytes()).hexdigest()}
    receipt.write_text(json.dumps(record)+'\n')
    commands = [('mkdir-p', '/var/lib/stqr-networkboot'), ('upload', receipt, OWNER), ('chmod', '0600', OWNER),
                ('mkdir-p', '/usr/libexec'), ('mkdir-p', '/etc/systemd/system/multi-user.target.wants'),
                ('upload', script, helper), ('chmod', '0755', helper),
                ('upload', service, unit), ('chmod', '0644', unit), ('ln-s', unit, link)]
    try:
        guest_many(socket, commands)
        print('Completing initial Snap seed in the installed system...', flush=True)
        boot(socket, args, work)
        if not is_file(socket, result):
            raise RuntimeError(f'installed system did not confirm initialization; disk retained, diagnostics: {work}')
        completed = json.loads(read_text(socket, result))
        if completed != {'schema': 1, 'nonce': nonce, 'status': 'seed-loaded'}:
            raise RuntimeError(f'initial Snap seed failed; disk retained, diagnostics: {work}')
        if not seeded(seed_state(socket)):
            raise RuntimeError(f'Snap seed incomplete after shutdown; disk retained, diagnostics: {work}')
    finally:
        # guestfish is invoked only after QEMU has stopped. A failed seed or a
        # timeout keeps the installation BD and staging; cleanup removes only
        # this invocation's owned temporary files, never existing guest assets.
        cleanup_owned(socket, record)
    write_stamp(work, 'completed')
