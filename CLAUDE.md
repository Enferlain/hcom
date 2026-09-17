# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:6cd5cc61 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->


## Delegating Work to hcom Workers

When work on this repository is delegated to worker agents through hcom
(spawning `hcom claude`, resuming, or messaging workers), keep the delegation
contract intact:

- **Construct the prompt directly and check it is non-empty.** Build
  `--hcom-prompt` from literals or validated variables; never
  `--hcom-prompt "$MAYBE_UNSET_VAR"` — an empty prompt starts a session that
  idles at the UI and wastes a terminal slot for the whole run.
- **Observe through one durable cursor/stream.** Capture
  `hcom events --cursor` before launch and follow progress with a single
  `hcom events stream --follow <name> --compact --heartbeat 15 --after-id
  <cursor>` (or, for a workflow that already has an explicit thread, one
  correlated `hcom events --wait <sec> --after-id <cursor> --thread
  <thread-id> --result-from <name>`). `--after-id` is an exclusive
  durable-event cursor: the observer ignores older events, so capture it
  before launching the worker. Keep that one observer alive when observation
  is useful; do not replace it with a loop of short timeouts, `list`, `--last`,
  log tails, or terminal snapshots. A healthy worker needs no supervision
  merely because it has not emitted prose recently.
- **Act on blockers; do not poll them away.** A surfaced
  `blocked:approval`/`pty:approval` (or unresolved `launch_blocked`) signal
  means the worker needs a decision: resolve it or redirect the task. After a
  plausible stall, make at most one targeted terminal inspection
  (`hcom term <name>`), then either fix the cause or kill the worker — do not
  loop inspections.

Normal interactive launch, copyable (opens the worker terminal):

```bash
task="Run cargo test --workspace and report failures"
[ -n "$task" ] || { echo "refusing to launch with an empty prompt" >&2; exit 1; }
hcom 1 claude --tag worker --go --hcom-prompt "$task"
```

Pass `--headless` only when a background PTY with no visible terminal is
explicitly wanted. It is not the normal interactive delegation path.

## Build & Test

_Add your build and test commands here_

```bash
# Example:
# npm install
# npm test
```

## Architecture Overview

_Add a brief overview of your project architecture_

## Conventions & Patterns

_Add your project-specific conventions here_
