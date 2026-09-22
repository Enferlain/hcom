#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

# Stdlib-only unittest suite: runs wherever python3 exists. Skipping loudly
# (the ci gate greps for SKIPPED) keeps a local `just ci` honest about what it
# did and did not verify, mirroring scripts/typecheck.sh.
if ! command -v python3 >/dev/null 2>&1; then
  echo "hermes_plugin: SKIPPED (python3 not found on PATH)"
  exit 0
fi

# The Rust side pins the embedded plugin source with contract tests in
# src/hooks/hermes.rs; this suite pins the plugin's side of the same
# hermes-* RPC contract by stubbing the hcom binary (HCOM_BIN).
cd "$repo_root"
python3 -m unittest discover -s src/hermes_plugin -v
