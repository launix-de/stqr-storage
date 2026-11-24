# SPDX-License-Identifier: GPL-3.0-only
"""Per-boot NBD upper buckets. MAC selects a session; client IP scopes its port."""
import json
from pathlib import Path
import select
import socket
import subprocess
import threading
import time
import uuid
from common import BIN, ctl

class Sessions:
    def __init__(self, config, server, runtime, upper_gib, max_sessions=64):
        self.config, self.server, self.runtime = config, server, Path(runtime)
        self.upper_gib = upper_gib
        self.lock = threading.Lock()
        self.items = {}
        self.stopping = threading.Event()
        self.workers = set()
        self.max_sessions = max_sessions
        self.runtime.mkdir(mode=0o700, parents=True, exist_ok=True)

    def allocate(self, mac, ip):
        with self.lock:
            if self.stopping.is_set():
                raise RuntimeError('gateway is stopping')
            old = self.items.get(mac)
            if old:
                if old['ip'] != ip or old['connected']:
                    raise RuntimeError('this MAC already has an active boot session')
                return old['port']
            if len(self.workers)>=self.max_sessions:
                raise RuntimeError('maximum concurrent boot sessions reached')
            bucket = 'boot-' + mac + '-' + uuid.uuid4().hex
            marker = self.runtime / (bucket + '.json')
            ctl(self.config, 'create', bucket)
            marker.write_text(json.dumps({'bucket': bucket, 'mac': mac, 'ip': ip}))
            marker.chmod(0o600)
            ctl(self.config, '--size', str(self.upper_gib << 30), 'init-disk', bucket)
            sock = self.runtime / (uuid.uuid4().hex + '.sock')
            process = subprocess.Popen([str(BIN / 'stqr-bootctl'), '--config', self.config,
                '--socket', str(sock), 'nbd', bucket])
            listener = socket.socket()
            try:
                for _ in range(300):
                    if process.poll() is not None:
                        raise RuntimeError('upper NBD failed (exclusive single-node maintenance required)')
                    if sock.exists():
                        break
                    time.sleep(.1)
                else:
                    raise RuntimeError('upper startup timeout')
                listener.bind((self.server, 0)); listener.listen(4); listener.settimeout(1)
                item = {'bucket': bucket, 'ip': ip, 'port': listener.getsockname()[1],
                        'connected': False, 'process': process, 'listener': listener}
                self.items[mac] = item
                thread = threading.Thread(target=self._serve, args=(mac, item, sock, marker), daemon=True)
                self.workers.add(thread); thread.start()
                return item['port']
            except BaseException:
                listener.close()
                process.terminate(); process.wait(timeout=30)
                # Persisted marker intentionally remains for explicit recovery.
                raise

    def _serve(self, mac, item, sock, marker):
        peers = []
        try:
            deadline = time.monotonic() + 3600
            while not self.stopping.is_set() and time.monotonic() < deadline:
                try:
                    peer, address = item['listener'].accept()
                except socket.timeout:
                    continue
                if address[0] != item['ip']:
                    peer.close(); continue
                peers.append(peer)
                with self.lock:
                    item['connected'] = True
                backend = socket.socket(socket.AF_UNIX)
                backend.connect(str(sock)); peers.append(backend)
                for p in peers:
                    p.settimeout(30)
                while not self.stopping.is_set():
                    ready, _, _ = select.select(peers, [], [], 1)
                    for source in ready:
                        payload = source.recv(65536)
                        if not payload:
                            return
                        (backend if source is peer else peer).sendall(payload)
                return
        except OSError:
            pass
        finally:
            for peer in peers:
                peer.close()
            item['listener'].close()
            process = item['process']
            if process.poll() is None:
                process.terminate()
            safe = False
            try:
                safe = process.wait(timeout=60) == 0
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
            # The previous connection is gone: a new boot can get a fresh
            # bucket while the old one is collected. Count cleanup workers too
            # so rapid reboot requests cannot grow resources without bound.
            with self.lock:
                if self.items.get(mac) is item:
                    del self.items[mac]
            if safe:
                for attempt in range(10):
                    try:
                        ctl(self.config, 'clear', item['bucket'])
                        ctl(self.config, 'delete', item['bucket'])
                        marker.unlink(missing_ok=True)
                        break
                    except subprocess.CalledProcessError:
                        time.sleep(.1)
            if marker.exists():
                print(f'Upper retained for recovery: {marker}',flush=True)
            with self.lock:
                if self.items.get(mac) is item:
                    del self.items[mac]
                self.workers.discard(threading.current_thread())

    def close(self):
        self.stopping.set()
        # Network peers are checked every second. Keep credentials/registry alive
        # until workers have either completed cleanup or recorded leftovers.
        with self.lock:
            workers=list(self.workers)
        for thread in workers:
            thread.join(timeout=75)
