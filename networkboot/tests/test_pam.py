# SPDX-License-Identifier: GPL-3.0-only
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

class PAM(unittest.TestCase):
    def test_password_session_lifetime_and_auth_failure(self):
        cflags = shlex.split(os.environ.get('PAM_CFLAGS', ''))
        if not cflags and not Path('/usr/include/security/pam_modules.h').exists():
            self.skipTest('libpam development headers required (or set PAM_CFLAGS)')
        with tempfile.TemporaryDirectory(prefix='stqr-pam-test-') as tmp:
            d = Path(tmp)
            helper = d / 'helper'
            helper.write_text('#!/bin/sh\n[ "$2" = -- ] && [ "$3" = test-user ] || exit 1\nread -r secret || :\ncase "$1" in check|open) [ "$secret" = test-password ] || exit 1;; esac\nprintf "%s\\n" "$1" >> "' + str(d / 'actions') + '"\n')
            helper.chmod(0o700)
            module = d / 'pam_stqr.so'
            subprocess.run(['cc', '-std=c11', '-Wall', '-Wextra', '-Werror', '-fPIC', '-shared',
                *cflags, '-DHELPER="' + str(helper) + '"', '-o', str(module), str(ROOT / 'pam/pam_stqr.c'), '-l:libpam.so.0'], check=True)
            (d / 'stqr-test').write_text(f'auth required {module}\nsession required {module}\n')
            harness = d / 'test.c'
            harness.write_text(r'''
#define _GNU_SOURCE
#include <security/pam_appl.h>
#include <stdlib.h>
#include <string.h>
static int conv(int n,const struct pam_message **m,struct pam_response **r,void *data){
 (void)m;*r=calloc(n,sizeof(**r));for(int j=0;j<n;j++)(*r)[j].resp=strdup(data);return PAM_SUCCESS;
}
int main(int argc,char **argv){
 if(argc!=3&&argc!=4)return 2;struct pam_conv c={conv,argv[2]};pam_handle_t *p=NULL;
 int e=pam_start_confdir("stqr-test",argc==4?argv[3]:"test-user",&c,argv[1],&p);if(e)return 3;
 e=pam_authenticate(p,0);if(!e)e=pam_open_session(p,0);if(!e)e=pam_close_session(p,0);
 pam_end(p,e);return e?1:0;
}
''')
            exe = d / 'test'
            subprocess.run(['cc', *cflags, '-o', str(exe), str(harness), '-l:libpam.so.0'], check=True)
            self.assertEqual(subprocess.run([str(exe), str(d), 'test-password']).returncode, 0)
            self.assertEqual((d / 'actions').read_text().splitlines(), ['check', 'open', 'close'])
            self.assertNotEqual(subprocess.run([str(exe), str(d), 'wrong-password']).returncode, 0)
            self.assertNotEqual(subprocess.run([str(exe), str(d), 'test-password', '--help']).returncode, 0)
            self.assertNotIn('password', (d / 'actions').read_text())
