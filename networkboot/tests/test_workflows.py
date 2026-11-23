# SPDX-License-Identifier: GPL-3.0-only
import importlib.machinery
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def load(name):
    loader = importlib.machinery.SourceFileLoader(name, str(ROOT / name))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module

class Workflows(unittest.TestCase):
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
    def test_help_has_no_side_effects(self):
        for script in ['prepare-system', 'edit-image', 'serve', 'pam/stqr-home']:
            result = subprocess.run([sys.executable, str(ROOT / script), '--help'], capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
