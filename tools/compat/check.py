#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-only
"""Run each tagged producer, then current readers twice against its disk state."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = "server/format_compat_test.go"
TEST = "^TestFormatCompatibilityFixture$"

def run(args, *, cwd=None, env=None, log=None, timeout=300):
    cwd = ROOT if cwd is None else cwd
    if log is None:
        return subprocess.check_output(args, cwd=cwd, env=env, text=True, timeout=timeout).strip()
    with log.open("w") as out:
        result = subprocess.run(args, cwd=cwd, env=env, stdout=out, stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode:
        print(log.read_text(), file=sys.stderr)
        raise RuntimeError(f"command failed ({result.returncode}); log: {log}")


def tags():
    # Full ref names are arguments, never shell source. Annotated and lightweight
    # release/checkpoint tags both participate; no tag is silently filtered out.
    return run(["git", "for-each-ref", "--format=%(refname)", "refs/tags/"]).splitlines()


def build(source, binary, log):
    env = os.environ.copy()
    # Compile the producer with its own recorded toolchain, not a future Go
    # version which might no longer build that historical release.
    for line in (source / "go.mod").read_text().splitlines():
        if line.startswith("go "):
            env["GOTOOLCHAIN"] = "go" + line.split()[1]
            break
    run(["go", "test", "-buildvcs=false", "-c", "-o", str(binary), "./server"],
        cwd=source, env=env, log=log, timeout=600)


def ensure_fixture_ran(log, cases=None):
    text = log.read_text()
    if not re.search(r"^--- PASS: TestFormatCompatibilityFixture \(", text, re.M) or "--- SKIP:" in text:
        raise RuntimeError(f"missing or skipped fixture execution: {log}")
    if cases is not None:
        passed = len(re.findall(r"^\s+--- PASS: TestFormatCompatibilityFixture/", text, re.M))
        if passed != cases:
            raise RuntimeError(f"verified {passed} data cases instead of {cases}: {log}")


def check(ref, current, reports):
    commit = run(["git", "rev-parse", "--verify", ref + "^{commit}"])
    report = reports / (commit + "-" + hashlib.sha256(ref.encode()).hexdigest()[:12])
    report.mkdir(parents=True, exist_ok=True)
    print(f"Checking {ref} ({commit})", flush=True)
    with tempfile.TemporaryDirectory(prefix="stqr-format-") as tmp:
        tmp = Path(tmp)
        source = tmp / "old"
        source.mkdir()
        archive = tmp / "source.tar"
        with archive.open("wb") as out:
            subprocess.run(["git", "archive", "--format=tar", commit], cwd=ROOT, stdout=out, check=True)
        with tarfile.open(archive) as contents:
            contents.extractall(source, filter="data")
        if not (source / FIXTURE).is_file():
            raise RuntimeError(f"{ref} has no checkpoint producer ({FIXTURE}); add an explicit historical adapter instead of skipping it")
        old = tmp / "producer.test"
        build(source, old, report / "build-producer.log")
        state = tmp / "state"
        env = os.environ.copy()
        env["STQR_FORMAT_FIXTURE_DIR"] = str(state)
        env["STQR_FORMAT_FIXTURE_MODE"] = "seed"
        run([str(old), "-test.run=" + TEST, "-test.v", "-test.timeout=180s"], cwd=source, env=env, log=report / "seed.log")
        ensure_fixture_ran(report / "seed.log")
        manifest = state / "manifest.json"
        if not manifest.is_file():
            raise RuntimeError(f"{ref} did not create its manifest; a skipped producer is not a passing checkpoint")
        original = manifest.read_bytes()
        case_count = len(json.loads(original)["Cases"])
        if not case_count:
            raise RuntimeError(f"{ref} generated an empty data corpus")
        (report / "manifest.json").write_bytes(original)
        env["STQR_FORMAT_FIXTURE_MODE"] = "verify"
        for restart in (1, 2):
            # Different binary, same config, journals and volume files. Process
            # termination between passes also tests persisted upgrade recovery.
            run([str(current), "-test.run=" + TEST, "-test.v", "-test.timeout=180s"], env=env, log=report / f"verify-{restart}.log")
            ensure_fixture_ran(report / f"verify-{restart}.log", case_count)
            if manifest.read_bytes() != original:
                raise RuntimeError("reader rewrote the expected-data manifest")
        return {"tag": ref, "commit": commit, "cases": case_count, "verified_restarts": 2}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path, default=ROOT / ".compat-reports")
    parser.add_argument("--tag", action="append", help="explicit tag/ref; HEAD is only for local bootstrap testing")
    args = parser.parse_args()
    refs = args.tag if args.tag else tags()
    if not refs:
        parser.error("no tags found: create the first format checkpoint; an empty compatibility matrix must not pass")
    reports = args.reports.resolve()
    reports.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="stqr-format-reader-") as tmp:
        current = Path(tmp) / "reader.test"
        build(ROOT, current, reports / "build-reader.log")
        results = []
        for ref in refs:
            try:
                results.append(check(ref, current, reports))
            except (RuntimeError, subprocess.SubprocessError, OSError, ValueError) as error:
                print(f"FAIL {ref}: {error}", file=sys.stderr, flush=True)
                results.append({"tag": ref, "error": str(error)})
    (reports / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    failures = [r for r in results if "error" in r]
    if failures:
        raise RuntimeError(f"{len(failures)} of {len(results)} tags failed; see {reports / 'summary.json'}")
    print(f"PASS: all {len(results)} tagged versions, {sum(r['cases'] for r in results)} data cases, two current-version starts each")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.SubprocessError, OSError, ValueError) as error:
        print(f"format compatibility FAILED: {error}", file=sys.stderr)
        sys.exit(1)
