# Native participation

Hcom normally installs Claude and Codex hooks for each `hcom <tool>` launch.
That per-run path remains the default. Native participation is a separate,
explicit opt-in for a tool session that was started outside hcom:

```text
hcom hooks native add claude
hcom hooks native add codex
hcom hooks native status claude
hcom hooks native status codex
hcom hooks native remove claude
hcom hooks native remove codex
```

The native installer adds only hcom-owned declarations and preserves foreign
settings, hook groups, and trust state. Codex installation enables its current
hooks feature, asks the current Codex `hooks/list` endpoint for every installed
declaration, and records the returned current hashes. Installation rolls back
if that inventory is unavailable or incomplete, so an old hash is never used
as trust evidence. Ordinary per-run cleanup leaves the clearly marked native
declarations in place.

Native hook commands use the explicit `hcom hooks native run <tool> <event>`
handshake. The dispatcher sets `HCOM_NATIVE_PARTICIPATION` only for that
command. A native hook does not infer identity from a transcript marker or an
unrelated pending row.

Claude's native `SessionStart` writes the authoritative session id and the
Claude opt-in marker to `CLAUDE_ENV_FILE`. An explicit `hcom start` in that
session creates or reuses a `claude` participant keyed by the native session.
Without the marker, `HCOM_CLAUDE_UNIX_SESSION_ID` is ignored and direct Claude
start keeps the ordinary `adhoc` behavior. Native Claude hook handling may
resolve only an already bound, opted-in Claude owner; the hook path never
creates a pending claim.

Codex wrapperless adoption is limited to an explicit `codex_vscode` originator,
`SessionStart` or `UserPromptSubmit`, and a nonempty native session id. The
check, name reservation, binding, and row creation are serialized and
transactional. A live or stopped binding owned by another tool rejects the
event; tool and stop hooks never mint an identity. Native Codex command sender
resolution likewise requires an existing Codex-owned session binding.
