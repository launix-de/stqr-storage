# SPDX-License-Identifier: GPL-3.0-only
"""Real ephemeral mount/chroot proof, confined to a disposable user namespace."""
import hashlib
import os
from pathlib import Path
import subprocess
import unittest

import test_boot_login_fastpath as fastpath


class RuntimePayloadMounts(unittest.TestCase):
    def test_runtime_payload_survives_run_move_and_executes_readonly(self):
        probe = subprocess.run(['unshare', '--user', '--map-root-user', '--mount', 'true'],
                               capture_output=True, text=True)
        if probe.returncode:
            self.skipTest('disposable user/mount namespaces unavailable: ' + probe.stderr.strip())
        fastpath.PreparedLogin.setUpClass()
        self.addCleanup(fastpath.PreparedLogin.tearDownClass)
        fixture = fastpath.PreparedLogin(methodName='test_matching_marker_skips_all_payload_copies_and_python')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        root, payload, work = fixture.root, fixture.payload, fixture.work
        (root/fastpath.MARKER).unlink()
        (root/'run').mkdir()
        (work/'run').mkdir()
        # An absent module leaf needs only an empty storage placeholder. Every
        # existing helper is observed through an open backing descriptor too.
        (root/fastpath.ASSETS[0]).unlink()
        original = (root/'usr/libexec/stqr-bootctl').read_bytes()
        source = work/'probe.c'
        source.write_text('#include <stdio.h>\nint main(void){puts("RUNTIME_BIND_FIXTURE_PASSED");return 0;}\n')
        executable = payload/'usr/libexec/stqr-bootctl'
        subprocess.run(['cc', '-std=c11', '-Wall', '-Wextra', '-Werror', '-O2', '-static',
                        '-o', executable, source], check=True)
        with executable.open('ab') as output:
            output.write(b'\0'*(12<<20))
        executable.chmod(0o755)
        # No real image/kernel is mounted. This source directory and every
        # mount target are fresh temporary fixtures owned by this test.
        script = work/'mount-proof.sh'
        script.write_text(r'''set -eu
mount --make-rprivate /
root=$1 payload=$2 work=$3 helper=$4
mount -t tmpfs -o size=64m,mode=0755,nodev,nosuid,noexec fixture-run "$work/run"
# Negative control: the outer /run really forbids execution.
cp "$payload/usr/libexec/stqr-bootctl" "$work/run/noexec-proof"
if "$work/run/noexec-proof" 2>/dev/null; then
    echo 'outer /run unexpectedly executable' >&2; exit 1
fi
rm "$work/run/noexec-proof"
exec 8<"$root/usr/libexec/stqr-bootctl"
. "$helper"
if [ -n "${STQR_TEST_BUSYBOX:-}" ]; then
    stqr_login_mount() { "$STQR_TEST_BUSYBOX" mount "$@"; }
fi
stqr_login_install() { python3 "$work/install.py" "$1" "$2"; }
stqr_prepare_login "$root" 192.0.2.8 7.0-test "$payload" "$work/run"
[ ! -e "$root/usr/lib/stqr-networkboot/prepared.sha256" ]
[ "$(sha256sum /proc/self/fd/8 | cut -d ' ' -f 1)" = "$ORIGINAL_SHA" ]
if (printf mutation > "$root/usr/libexec/stqr-bootctl") 2>/dev/null; then
    echo 'readonly runtime binary unexpectedly writable' >&2; exit 1
fi
# Match the real init: init-bottom runs first; then /run and all its child
# mounts move under the final root, before run-init/chroot starts userspace.
mount -n -o move "$work/run" "$root/run"
# Simulate initramfs cleanup: runtime bytes must not depend on the old source.
rm "$payload/usr/libexec/stqr-bootctl"
chroot "$root" /usr/libexec/stqr-bootctl
# Sources remain inspectable after the move, with a root-only parent. The
# original missing module still has zero backing bytes below its bind mount.
[ "$(find "$root/run" -maxdepth 1 -name 'stqr-boot-assets.*' -type d | wc -l)" -eq 1 ]
ram_source=$(find "$root/run" -maxdepth 1 -name 'stqr-boot-assets.*' -type d)
[ "$(stat -c %a "$ram_source")" = 700 ]
[ "$(stat -c %u "$ram_source")" = 0 ]
if (printf mutation > "$ram_source/usr/libexec/stqr-bootctl") 2>/dev/null; then
    echo 'runtime source unexpectedly writable' >&2; exit 1
fi
umount "$root/lib/modules/7.0-test/extra/kstqr.ko"
[ ! -s "$root/lib/modules/7.0-test/extra/kstqr.ko" ]
''')
        result = subprocess.run(['unshare', '--user', '--map-root-user', '--mount', '--fork',
                                 'sh', script, root, payload, work,
                                 fastpath.ROOT/'initramfs/prepare-login'],
                                env={**os.environ, 'ORIGINAL_SHA': hashlib.sha256(original).hexdigest()},
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout+'\n'+result.stderr)
        self.assertIn('RUNTIME_BIND_FIXTURE_PASSED', result.stdout)
        # All child mounts vanished with the namespace. The host-side fixture
        # now exposes the unchanged storage backing, never the staged binary.
        self.assertEqual((root/'usr/libexec/stqr-bootctl').read_bytes(), original)
        self.assertEqual((root/fastpath.ASSETS[0]).stat().st_size, 0)
        print('RUNTIME_BIND_FIXTURE_PASSED')
