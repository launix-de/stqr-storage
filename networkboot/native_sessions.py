# SPDX-License-Identifier: GPL-3.0-only
"""One temporary identity and private kstqr upper per boot.

The claim connection is owned by the mounted kernel superblock, not a userspace
helper. EOF triggers revocation and collection. Uncollected sessions retain a
private recovery marker; restarting a gateway never guesses that they are idle.
"""
import json
from pathlib import Path
import re
import secrets
import socket
import threading
import time
import urllib.parse
from common import ctl

class NativeSessions:
    def __init__(self, config, runtime, upper_gib, max_sessions=64, server=None):
        self.config, self.runtime = config, Path(runtime)
        self.server = server
        self.quota, self.maximum = upper_gib << 30, max_sessions
        self.runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.items = {}
        self.closed = threading.Event()
        self.reaper = threading.Thread(target=self._expire, daemon=True)
        self.reaper.start()

    def allocate(self, mac, ip, lower, release=None):
        with self.lock:
            if self.closed.is_set():
                raise RuntimeError('gateway is stopping')
            old = self.items.get(mac)
            if old:
                if old['ip'] != ip or old['claimed']:
                    raise RuntimeError('this MAC already has an active boot session')
                return old['token']
            if len(self.items) >= self.maximum:
                raise RuntimeError('maximum concurrent boot sessions reached')
            # Serialize provisioning to prevent concurrent duplicate identities.
            marker = self.runtime / ('boot-' + secrets.token_hex(16) + '.json')
            data = json.loads(ctl(self.config, '--size', str(self.quota),
                                  '--session-record', str(marker), 'boot-session-create', lower, capture=True))
            for field in ('user', 'upper', 'lower'):
                # Go ID is JSON encoded as an array of sixteen bytes.
                value = data[field]
                if isinstance(value, list) and len(value) == 16:
                    data[field] = bytes(value).hex()
                if not re.fullmatch('[0-9a-f]{32}', data[field]):
                    raise RuntimeError('invalid boot session ID')
            if any(not isinstance(data[k], str) or '\n' in data[k] or '\r' in data[k]
                   for k in ('uri', 'username', 'password')):
                raise RuntimeError('invalid boot credentials')
            endpoint = urllib.parse.urlsplit(data['uri'])
            if self.server and endpoint.hostname in ('localhost', '127.0.0.1', '::1'):
                data['uri'] = urllib.parse.urlunsplit((endpoint.scheme,
                    f'{self.server}:{endpoint.port or 80}', endpoint.path, '', ''))
            try:
                recorded = json.loads(marker.read_text())
                if any(recorded.get(k) != data[k] for k in ('user', 'upper', 'lower')):
                    raise RuntimeError('boot session recovery record differs from provisioned identity')
            except BaseException:
                ctl(self.config, 'boot-session-delete', data['user'], data['upper'], data['lower'])
                raise
            data.update(ip=ip, mac=mac, claimed=False, socket=None, marker=marker,
                        created=time.monotonic(), token=secrets.token_hex(32), release=release)
            self.items[mac] = data
            return data['token']

    def boot_version(self, token):
        with self.lock:
            return next(v['release'] for v in self.items.values() if v['token'] == token)

    def claim(self, token, ip, connection):
        with self.lock:
            item = next((v for v in self.items.values()
                         if secrets.compare_digest(v['token'], token)), None)
            if not item or item['ip'] != ip or item['claimed'] or self.closed.is_set():
                raise RuntimeError('invalid or already claimed boot session')
            item['claimed'], item['socket'] = True, connection
            payload = 'STQR-BOOT-1\n' + ''.join(f'{k}={item[k]}\n' for k in
                         ('uri', 'username', 'password', 'lower', 'upper')) + '\n'
            return item, payload.encode()

    def release(self, item):
        with self.lock:
            if item.get('cleaning'):
                return
            item['cleaning'] = True
        # Preserve the marker if revocation or exclusive collection fails. No
        # forced unmount or deletion of a bucket with active handles is allowed.
        try:
            ctl(self.config, 'boot-session-delete', item['user'], item['upper'], item['lower'])
            item['marker'].unlink(missing_ok=True)
        except Exception:
            print(f"Boot session retained for recovery: {item['marker']}", flush=True)
        finally:
            with self.lock:
                if self.items.get(item['mac']) is item:
                    del self.items[item['mac']]
            item['password'] = ''

    def _expire(self):
        while not self.closed.wait(5):
            with self.lock:
                stale = [i for i in self.items.values()
                         if not i['claimed'] and time.monotonic()-i['created'] >= 3600]
            for item in stale:
                self.release(item)

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
