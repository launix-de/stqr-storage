# SPDX-License-Identifier: GPL-3.0-only
"""One temporary identity and private kstqr upper per boot.

The claim connection is owned by the mounted kernel superblock, not a userspace
helper. EOF triggers revocation and collection. Uncollected sessions retain a
private recovery marker; restarting a gateway never guesses that they are idle.
"""
import json
import os
import subprocess
from pathlib import Path
import re
import secrets
import socket
import threading
import time
import urllib.parse
from common import ctl

class NativeSessions:
    def __init__(self, config, runtime, upper_gib, max_sessions=64, server=None, edit=False, image_name=None):
        self.config, self.runtime = config, Path(runtime)
        self.server = server
        self.mode = 'edit' if edit else 'overlay'
        self.image_name = image_name
        self.create_command = 'edit-session-create' if edit else 'boot-session-create'
        self.delete_command = 'edit-session-delete' if edit else 'boot-session-delete'
        self.quota, self.maximum = upper_gib << 30, max_sessions
        self.runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.items = {}
        self.tokens = {}
        self.closed = threading.Event()
        self.reaper = threading.Thread(target=self._expire, daemon=True)
        self.reaper.start()

    def allocate(self, mac, ip, lower, release=None):
        with self.lock:
            if self.closed.is_set():
                raise RuntimeError('gateway is stopping')
            old = self.items.get(mac)
            if old:
                if old['ip'] != ip or old['claimed'] or old.get('cleaning'):
                    raise RuntimeError('this MAC already has an active boot session')
                return old['token']
            if len(self.items) >= self.maximum:
                raise RuntimeError('maximum concurrent boot sessions reached')
            # Serialize provisioning to prevent concurrent duplicate identities.
            marker = self.runtime / ('boot-' + secrets.token_hex(16) + '.json')
            options = ['--size', str(self.quota), '--session-record', str(marker)]
            if self.image_name:
                options += ['--image-name', self.image_name]
            data = json.loads(ctl(self.config, *options, self.create_command, lower, capture=True))
            for field in ('user', 'upper', 'lower'):
                # Go ID is JSON encoded as an array of sixteen bytes.
                value = data[field]
                if isinstance(value, list) and len(value) == 16:
                    data[field] = bytes(value).hex()
                if not re.fullmatch('[0-9a-f]{32}', data[field]):
                    raise RuntimeError('invalid boot session ID')
            data.setdefault('mode', 'overlay')
            if data['mode'] != self.mode or (data['upper'] == data['lower']) != (self.mode == 'edit'):
                raise RuntimeError('session mode does not match bucket identities')
            if any(not isinstance(data[k], str) or '\n' in data[k] or '\r' in data[k]
                   for k in ('uri', 'username', 'password')):
                raise RuntimeError('invalid boot credentials')
            endpoint = urllib.parse.urlsplit(data['uri'])
            if self.server and endpoint.hostname in ('localhost', '127.0.0.1', '::1'):
                data['uri'] = urllib.parse.urlunsplit((endpoint.scheme,
                    f'{self.server}:{endpoint.port or 80}', endpoint.path, '', ''))
            try:
                recorded = json.loads(marker.read_text())
                if any(recorded.get(k) != data[k] for k in ('user', 'upper', 'lower')) or recorded.get('mode', 'overlay') != self.mode:
                    raise RuntimeError('boot session recovery record differs from provisioned identity')
            except BaseException:
                ctl(self.config, self.delete_command, data['user'], data['upper'], data['lower'])
                raise
            data.update(ip=ip, mac=mac, claimed=False, socket=None, marker=marker,
                        created=time.monotonic(), token=secrets.token_hex(32), release=release)
            self.items[mac] = data
            self.tokens[data['token']] = data
            return data['token']

    def boot_version(self, token):
        with self.lock:
            item = self.tokens.get(token)
            if not item or item.get('cleaning'):
                raise RuntimeError('boot session expired')
            return item['release']

    def claim(self, token, ip, connection):
        with self.lock:
            item = self.tokens.get(token)
            if not item or item['ip'] != ip or item['claimed'] or item.get('cleaning') or self.closed.is_set():
                raise RuntimeError('invalid or already claimed boot session')
            item['claimed'], item['socket'] = True, connection
            payload = 'STQR-BOOT-1\n' + ''.join(f'{k}={item[k]}\n' for k in
                         ('uri', 'username', 'password', 'lower', 'upper', 'mode')) + '\n'
            return item, payload.encode()

    def release(self, item, only_unclaimed=False):
        with self.lock:
            if only_unclaimed and item['claimed']:
                return
            if item.get('cleaning'):
                return
            item['cleaning'] = True
        # Preserve the marker if revocation or exclusive collection fails. No
        # forced unmount or deletion of a bucket with active handles is allowed.
        try:
            ctl(self.config, self.delete_command, item['user'], item['upper'], item['lower'])
            item['marker'].unlink(missing_ok=True)
        except Exception:
            print(f"Boot session retained for recovery: {item['marker']}", flush=True)
        finally:
            with self.lock:
                if self.items.get(item['mac']) is item:
                    del self.items[item['mac']]
                self.tokens.pop(item['token'], None)
            item['password'] = ''

    def recover(self):
        """Collect only our private recovery records, never bucket name patterns.

        The helper proves upper inactivity with cluster-wide maintenance before
        revocation. A gateway restart, idle timer or missing claim is not proof
        that a mounted guest is gone. Busy sessions retain their markers.
        """
        with self.lock:
            current = {item['marker'] for item in self.items.values()}
            # Capture names while allocation is excluded. A newly written
            # marker must not be mistaken for a leftover before provisioning
            # has completed and the session has entered the live registry.
            candidates = [p for p in self.runtime.glob('boot-*.json') if p not in current]
        for marker in candidates:
            if self.closed.is_set():
                return
            if marker in current:
                continue
            try:
                st = marker.lstat()
                if marker.is_symlink() or st.st_uid != os.geteuid() or st.st_mode & 0o077:
                    continue
                record = json.loads(marker.read_text())
                if not isinstance(record, dict):
                    continue
                if any(not isinstance(record.get(k), str) or not re.fullmatch('[0-9a-f]{32}', record[k]) for k in ('user', 'upper', 'lower')):
                    continue
                mode = record.get('mode', 'overlay')
                if mode not in ('overlay', 'edit') or (record['upper'] == record['lower']) != (mode == 'edit'):
                    continue
                command = 'edit-session-delete' if mode == 'edit' else 'boot-session-delete'
                ctl(self.config, command, record['user'], record['upper'], record['lower'], timeout=15)
                marker.unlink(missing_ok=True)
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
                # A live client, unavailable member or uncertain admin result
                # keeps the record for a later retry. Never force disconnection.
                continue

    def _expire(self):
        while not self.closed.wait(5):
            self.recover()
            with self.lock:
                stale = [i for i in self.items.values()
                         if not i['claimed'] and time.monotonic()-i['created'] >= 3600]
            for item in stale:
                self.release(item, only_unclaimed=True)

    def close(self):
        self.closed.set()
        self.reaper.join()
        with self.lock:
            items = list(self.items.values())
        for item in items:
            if item['socket']:
                try:
                    item['socket'].shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            self.release(item)
