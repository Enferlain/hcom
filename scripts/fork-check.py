#!/usr/bin/env python3
"""Validate the fork manifest, locate its commits, or run its focused checks."""

import argparse
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-series", action="store_true")
    parser.add_argument("--test", metavar="PATCH", help="patch id, or all")
    args = parser.parse_args()
    manifest = json.loads((ROOT / "docs/fork-patches.json").read_text())
    patches = manifest["patches"]
    seen = set()
    contracts = set(manifest["upstream_owned"]["contracts"])
    for patch in patches:
        patch_id = patch["id"]
        if patch_id in seen or not set(patch["depends_on"]).issubset(seen):
            raise ValueError(f"duplicate patch or dependency out of order: {patch_id}")
        seen.add(patch_id)
        for contract in patch["contracts"]:
            if contract in contracts:
                raise ValueError(f"contract has multiple owners: {contract}")
            contracts.add(contract)
        if not patch["focused_tests"]:
            raise ValueError(f"no focused checks: {patch_id}")
        for command in patch["focused_tests"]:
            if not command or command[:2] != ["cargo", "test"]:
                raise ValueError(f"invalid focused test command: {patch_id}")
    if contracts != manifest["contracts"].keys():
        raise ValueError("contract definitions and patch/upstream ownership differ")

    if args.verify_series:
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", manifest["upstream_base"], "HEAD"],
            cwd=ROOT,
            check=True,
        )
        log = subprocess.check_output(
            ["git", "log", "--reverse", "--format=%H%x00%B%x00", f"{manifest['upstream_base']}..HEAD"],
            cwd=ROOT,
            text=True,
        ).split("\0")
        commits = {}
        order = []
        for index in range(0, len(log) - 1, 2):
            commit, body = log[index].strip(), log[index + 1]
            for line in body.splitlines():
                if line.startswith("Fork-Patch: "):
                    patch_id = line.removeprefix("Fork-Patch: ").strip()
                    if patch_id not in seen or patch_id in commits:
                        raise ValueError(f"unknown or repeated patch trailer: {patch_id}")
                    commits[patch_id] = commit
                    order.append(patch_id)
        missing = seen - commits.keys()
        if missing:
            raise ValueError(f"missing feature commits: {', '.join(sorted(missing))}")
        if order != [patch["id"] for patch in patches]:
            raise ValueError("feature commit order differs from the manifest")
        for patch in patches:
            print(f"{commits[patch['id']]} {patch['id']}")

    if args.test:
        selected = patches if args.test == "all" else [p for p in patches if p["id"] == args.test]
        if not selected:
            parser.error(f"unknown patch: {args.test}")
        for patch in selected:
            print(f"Checking {patch['id']}", flush=True)
            for command in patch["focused_tests"]:
                subprocess.run(command, cwd=ROOT, check=True)
    elif not args.verify_series:
        for patch in patches:
            print(f"{patch['id']}: {', '.join(patch['contracts'])}")
        print(f"{len(patches)} patches; {len(contracts)} contracts including upstream-owned behavior")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, subprocess.CalledProcessError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
