# Cross-tool coordination

Use exact names and ordinary hcom messages across tools. The receiving tool's
integration handles delivery. A shared working directory exposes each
worker's edits immediately; give workers non-overlapping write ownership and
review their changes before accepting them.

## Choose the launch surface

| Tool | Relevant behavior |
|------|-------------------|
| Claude Code | `hcom claude --headless` keeps a live PTY; explicit `-p`/`--print` selects the print backend. Use the live session for ongoing messaging. |
| Codex | Per-launch hooks and bootstrap are prepared by `hcom codex`. Choose approval and sandbox flags from the local Codex help and task constraints. |
| Gemini | Persistent hooks; use `hcom gemini`. A missing integration may require `hcom hooks add gemini` and restart. |
| OpenCode, Kilo, Pi, OMP, Copilot, Qoder | Per-launch integration; launch through hcom. |
| Antigravity | `hcom agy` uses the desktop integration. Check `agy models` when choosing a model and pin it explicitly when the task requires one. |
| Cursor | `hcom cursor-agent` uses persistent hooks. Its background PTY preserves interactive delivery; it is not Claude's print backend. |
| Kimi | Persistent hooks; launch through `hcom kimi`. |
| Hermes | A profile-scoped plugin; `hcom hermes` prepares the selected launch profile. Explicit ambient setup uses `hcom hooks add hermes`. |
| Grok | `hcom grok` uses the tool's native connection for status and messages, without hooks. |
| Other tools | `hcom start` registers manual participation; use `hcom listen` to receive messages. |

Check `hcom f --help` and `hcom r --help` before depending on fork/resume:
capabilities differ by tool. Do not assume a new worker has bound its session
or has a usable transcript merely because a name was allocated. Inspect its
launch readiness and recorded metadata when that distinction matters.

## Permissions and configuration

`--headless` changes terminal presentation. `--go` skips hcom's launch
confirmation. Neither flag grants tool permissions. Follow the selected
tool's approval policy and the user's authorization; do not substitute a
permission-bypass flag just to make a workflow unattended.

`HCOM_DIR` selects only hcom state; it does not relocate persistent hooks or
tool configuration. For example, Gemini settings use its own
configuration root, including `GEMINI_CLI_HOME` when set. Inspect
`hcom hooks status <tool>` rather than assuming project-local hook setup.

## Results and context

- Send concise outcomes through `hcom send` with explicit recipients and
  the agreed thread. Use a separate result thread when a correlated wait
  must ignore progress messages.
- Read `hcom transcript @name --full --detailed` for recorded conversation
  and tool I/O. Parser capabilities and transcript availability vary by tool.
- Use `hcom bundle --help` for references to events, files, and transcript
  ranges. A bundle or transcript is context; inspect the resulting files and
  run the relevant checks.
- Across devices, inspect `hcom relay status`; local files are not replicated
  just because messages are relayed. Use an agreed Git transfer for artifacts.

The [cross-tool duo](scripts/cross-tool-duo.sh) and
[Codex worker](scripts/codex-worker.sh) examples use these same message and
result-wait rules. They inherit configured tool permissions and models.
