# hcom checkout and installation

Verified on 2026-11-20. `~/Projects/hcom` is the active source checkout and holds the common Git directory.

| Purpose | Ref | Verified commit |
| --- | --- | --- |
| Pristine upstream | [`upstream/main`](https://github.com/aannoo/hcom/tree/main) | `de33e31` (`v0.7.31`) |
| Maintained downstream | [`origin/patches/upstream-de33e31`](https://github.com/Enferlain/hcom/tree/patches/upstream-de33e31) | `4630fbd` |
| Older fork lineage | [`origin/main`](https://github.com/Enferlain/hcom/tree/main) | `9edca22` |

The local `patches/upstream-de33e31` branch tracks the matching branch on `origin`. Use `upstream/main` to inspect pristine source from the same Git repository.

```sh
cd ~/Projects/hcom
git fetch upstream --tags
git fetch origin
git log --oneline upstream/main..HEAD
git diff --stat upstream/main...HEAD
python3 scripts/fork-check.py --verify-series
```

The [patch manifest](fork-patches.json) records the 11 applied patches, their dependencies, and the contracts supplied by upstream. The [maintenance guide](fork-patches.md) gives the replay procedure for a new upstream revision. The [series validation record](fork-validation.md) describes the October qualification and its test scope.

The maintained source uses upstream's Hermes launch, plugin/backend and gateway integration, with downstream additions for ordinary session participation and transcript search. It also contains the observation helper and skill updates through `4630fbd`. The [OpenShell sandbox change](../openspec/changes/add-openshell-sandbox-mode/proposal.md) is a proposal. The legacy isolated-workspace implementation, standalone Hermes bridge/dashboard, older rebuild, and abandoned integration remain reference material outside the applied series.

## Installed command

| What | Location |
| --- | --- |
| Command on `PATH` | `~/.local/bin/hcom`, reports `hcom 0.7.31` |
| Resolved executable | `~/.local/share/hcom/releases/local-debug-0.7.31-69b403f5/hcom` |
| Configured `dev-root` | `~/.local/share/hcom/releases/patches-de33e31-d04da5f-20261023` |
| Default runtime state | `~/.hcom` |

The installed executable is a byte-for-byte copy of the debug binary previously reached through `~/Projects/hcom/target/debug/hcom`. The adjacent `source.json` records its SHA-256 (`69b403f51f2a9764f132d5edf5f2b5a59c5362bfa3beb167b93a32a04d9b3089`) and former path. The exact source commit for that debug build is unverified. The configured `dev-root` contains the separately qualified October executable at `target/release/hcom` and its `source.json` identifies `d04da5f`. Read `command -v hcom`, `readlink -f "$(command -v hcom)"`, `hcom --version`, and `hcom status` together when checking a deployment.

## Historical references

| Reference | What to recover |
| --- | --- |
| [Legacy design archive](https://github.com/Enferlain/hcom/tree/archive/legacy-design-20261120) at `9edca22` | Isolated-workspace design and scaffolding, earlier Hermes backend integration, worker-model notes, and legacy issue history. Start with [isolated worker workspaces](https://github.com/Enferlain/hcom/blob/9edca2213ea81a7ec2e237d80b0c0135e3bb4e86/docs/isolated-worker-workspaces.md). |
| [Upstream rebuild](https://github.com/Enferlain/hcom/tree/rebuild/upstream-f687a9c) at `c34bb9f` | Earlier upstream-based port and its [validation record](https://github.com/Enferlain/hcom/blob/c34bb9f/docs/fork-validation.json). |
| [Abandoned October integration](https://github.com/Enferlain/hcom/tree/archive/oct2-integration) at `379092d` | Attempted integration into the older fork lineage. |
| [Standalone Hermes plugin archive](https://github.com/Enferlain/hcom/tree/archive/hermes-standalone-plugin/src/hermes_plugin) at `0f6fd5e` | Exact standalone bridge, tests, and desktop dashboard source. |

Fetch the archival refs with `git fetch origin 'refs/tags/archive/*:refs/tags/archive/*'`. Inspect an archived file directly with `git show <ref>:<path>`.

Private local evidence is under `~/.local/state/hcom/checkout-archive/2026-11-20/`. It includes raw patch/rebuild evidence, the saved `.opencode` deletions from the retired patch worktree, the legacy agent/Beads configuration backup, and the original standalone Hermes folder. Local agent and Beads configuration still lives in the canonical checkout, excluded through `.git/info/exclude`. Beads currently reports database schema `v53` with 13 pending migrations and blocks writes to the remote-backed store.
