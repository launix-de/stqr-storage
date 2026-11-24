#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only
"""Discover the complete server suite and partition it across CI processes."""
import argparse
import re
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shard', type=int, required=True)
    parser.add_argument('--shards', type=int, required=True)
    parser.add_argument('--list', action='store_true', help='only show the selected tests')
    args = parser.parse_args()
    if not 0 <= args.shard < args.shards:
        parser.error('require 0 <= shard < shards')
    output = subprocess.check_output(['go', 'test', '-race', './server', '-list', '.'], text=True)
    names = sorted(line for line in output.splitlines() if re.fullmatch(r'(?:Test|Example|Fuzz)\w+', line))
    if not names or len(names) != len(set(names)):
        raise RuntimeError('empty or duplicate server test discovery')
    selected = names[args.shard::args.shards]
    if not selected:
        raise RuntimeError('empty server shard')
    print(f'Shard {args.shard}/{args.shards}: {len(selected)} of {len(names)} tests/examples/fuzz seeds', flush=True)
    print('\n'.join(selected), flush=True)
    if not args.list:
        pattern = '^(?:' + '|'.join(re.escape(name) for name in selected) + ')$'
        subprocess.run(['go', 'test', '-race', './server', '-run', pattern,
                        '-count=1', '-timeout=15m'], check=True)


if __name__ == '__main__':
    main()
