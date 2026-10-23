---
name: hcom-agent-messaging
description: >
  Multi-agent communication for AI coding tools. Agents message, watch,
  and spawn each other across terminals. Use when setting up hcom,
  troubleshooting delivery, or writing multi-agent scripts.
---

# hcom — multi-agent communication for AI coding tools

hcom connects agents through a shared event database and tool integrations.
Use it to send requests, share results, inspect transcripts, and coordinate
authorized workers. Launching a worker requires authorization from the task
or applicable instructions; loading this skill does not grant it.

These references target this hcom checkout, including correlated result waits
and compact progress streams. Check `hcom <command> --help` when using another
version.

## Connect and diagnose

For installation paths and the distinction between hooks, startup instructions,
and optional skills, read [setup and distribution](references/setup.md).

Start with `hcom status` and `hcom list`. An existing working installation
does not need reinstalling. Launch supported tools with `hcom <tool>` for
automatic message delivery. Use `hcom start` inside other tools for manual
delivery through `hcom listen`.

| Integration | Tools | Setup |
|-------------|-------|-------|
| Per launch | Claude Code, Codex, OpenCode, Kilo, Pi, OMP, Copilot, Qoder | Launch with `hcom <tool>`; no persistent hooks to install |
| Persistent | Gemini, Antigravity (`agy`), Cursor (`cursor-agent`), Kimi | Inspect `hcom hooks status <tool>`; install missing hooks with `hcom hooks add <tool>`, then restart |
| Hermes profile plugin | Hermes | `hcom hermes` prepares its selected profile; `hcom hooks add hermes` explicitly installs into the ambient profile and reports activation details |
| Native connection | Grok | Launch with `hcom grok`; no hooks required |

Bare `hcom hooks add` detects the current tool, or selects all outside a
recognized tool. `hcom hooks add all` skips per-launch integrations and
Hermes; use the explicit Hermes command when needed. Hook setup, startup
bootstrap, executable installation, and discovery of this skill are separate.

If a supported agent is not participating, relaunch it through hcom or use
`hcom r --help` to resume its supported session. Inspect `hcom hooks status`
for integration problems and `hcom relay status` for cross-device problems.
Use reset only when intentionally archiving and clearing hcom state.

## Send messages and share context

```bash
hcom send @worker-name --thread task-unique-id --intent request -- "Do the bounded task; report the outcome."
hcom send @worker-name --thread task-unique-id --intent inform -- "Additional context."
hcom send @worker-name --reply-to 42 --intent ack -- "Received."
hcom send --broadcast --intent inform -- "Team-wide announcement."
```

Replace example names and event IDs with actual values. Forward an explicit
`--name <caller>` when issuing commands for that caller. Outside an agent,
`hcom send -b ...` uses the external `bigboss` identity.

- Exact names target one agent; `@tag-` targets all local agents with that
  exact tag. Prefer captured exact names for individual assignments.
- A first threaded message needs explicit targets. A later thread-only send
  reuses the thread's members. Threads organize conversations and routing;
  they are not access controls or inbox subscription filters.
- An unthreaded send without targets requires `--broadcast`.
- `request` asks for a response; `inform` permits a useful response; `ack`
  asks for no response and requires `--reply-to` to a request.
- All flags go before `--`. For complex payloads, use stdin or `--file`.

Use `hcom transcript @name --full --detailed` for conversation and tool I/O,
`hcom bundle --help` for structured context, and Git for file changes. Check
the actual diff and relevant tests before accepting a worker's result.

## Ordinary interactive delegation

Use the normal visible launch unless the user asks for a background worker.
`--headless` opens no terminal window. For Claude, it keeps a live PTY by
default; explicit `-p`/`--print` selects the separate print backend.

```bash
cursor=$(hcom events --cursor)  # exclusive boundary; capture before launch
hcom 1 claude --dir /absolute/project/path --go --hcom-prompt "<non-empty bounded task>"
```

Capture the worker's actual name from launch output. Scripts can use
`--json` and parse the `names` array, including on a partial failure.
Select model and permission flags according to the user's request and the
tool's supported options; `--go` only accepts hcom's launch preview.

For interactive delegation, run one persistent observer that streams progress
and returns the final report. Run this helper from the installed skill's
directory (or use its absolute path):

```bash
python3 references/scripts/watch-result.py --worker "$worker" \
  --after-id "$cursor" --thread "$result_thread" --timeout 1500
```

Agree on the unique result thread before dispatch. Only the final publisher
sends `inform` there; progress uses another thread. The helper starts one
normal generation-following stream and one correlated result wait. The stream
includes intermediate messages from the followed worker, with their text,
intent and thread, as well as status/activity events. It prints event JSON
Lines followed by a `kind: result`
record containing the wait's `exit_code` and full `payload`, preserves the
wait's exit status, and stops only its observers. It does not launch or kill
workers. Forward `--name <caller>` when operating for an explicit caller.
An approval-gated host may ask to approve the Python helper even when it
auto-approves direct `hcom events` commands; the helper changes no permissions.

Run the helper directly, not inside `$(...)` or another output capture that
buffers progress until completion. Keep the same tool execution/session running
while it emits progress. Resume
that execution if the host tool yields a session handle; do not launch another
hcom command or narrate an unchanged status on every heartbeat. Use a task-sized
overall deadline, not repeated 45/60-second waits. At the deadline, assess the
outcome instead of automatically restarting the wait. Intermediate messages
do not complete the wait; only the correlated final report does. Use a
resumable host execution so the main agent can read incoming stream output
and normal hook-delivered messages at tool boundaries. After a plausible
stall, inspect once with `hcom term "$worker" --clean`.

For observation alone, use `hcom events stream --follow "$worker"
--after-id "$cursor"`. It ends at the worker's stop boundary, not at the
final inform; idle is not completion. Use compact mode only when a bounded
activity display is sufficient: add `--compact` to the helper (optionally
`--heartbeat 15`). Compact emits phases, file paths, command categories and
quiet heartbeats, but suppresses all message events and their contents. A
heartbeat means the observer is alive, not proof of worker progress.

For scripts that only need the result, a single bounded silent wait remains
available. Capture the cursor before launching the worker, then use:

```bash
hcom events --wait 180 --after-id "$cursor" --thread "$result_thread" --result-from "$worker"
```

This correlates an exact worker generation and its `inform` message, not the
meaning of its text. Exit 0 means a result was received or recovered; inspect
it before claiming success. Wait outcomes use 1 deadline, 2 error, 3 stopped
without result, 4 blocked, 5 launch failure. Input validation can also exit 1
before waiting; read diagnostics rather than calling every exit 1 a timeout.
Supported stopped-provider recovery
includes provenance. Do not combine `--result-from` with SQL or extra event
filters. See [gotchas](references/gotchas.md) for timing and routing details.

## Workflow scripts

Place `.sh` or `.py` scripts in `${HCOM_DIR:-$HOME/.hcom}/scripts/`; run them
with `hcom run <name>`. Parse and forward an injected `--name` using an argument
array. Capture a cursor before launch or dispatch, give each run a unique
thread, and target exact captured worker names. Use bounded event waits;
preserve their exit status. Track every launched worker and use an `EXIT`
trap plus signal handling to clean up script-owned workers with `hcom kill`.
Keep interactive workers available when the task calls for follow-up.

| Reference | When to read |
|-----------|--------------|
| [Setup and distribution](references/setup.md) | Install/locate the optional skill, inspect hook/bootstrap setup, or resolve client/profile differences |
| [Script template](references/script-template.md) | Write a workflow or install the examples |
| [Patterns](references/patterns.md) | Choose among six coordination examples |
| [Cross-tool behavior](references/cross-tool.md) | Mix tools, check launch/permission differences |
| [Gotchas](references/gotchas.md) | Diagnose waits, delivery, setup, and cleanup |

## State location

By default, hcom stores `hcom.db`, `config.toml`, `env`, `.tmp/logs/`, and
`scripts/` under `~/.hcom`. `HCOM_DIR` changes hcom's state root; participants
must use the same root to share its local database. It does not relocate tool
configuration or persistent hooks.

```bash
HCOM_DIR="$PWD/.hcom" hcom claude
```

For full command details, use `hcom --help`, `hcom <command> --help`, and
`hcom run docs --cli`.
