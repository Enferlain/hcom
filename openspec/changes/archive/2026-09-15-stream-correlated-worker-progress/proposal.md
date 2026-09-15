## Why

Agents and humans sometimes want ongoing visibility into a worker without
repeatedly calling `list`, `events`, `term`, or `transcript`. Existing
`events --wait` intentionally waits for one matching event and exits, while
`events sub` turns matches into hcom messages; neither is a continuous,
non-conversational observation surface.

## What Changes

- Add a separate `hcom events stream` mode that continuously emits future
  matching events until stopped, without changing one-shot `events --wait`.
- Reuse existing event filters and durable cursors so a stream can observe
  broad activity or follow one exact worker generation without race-prone
  snapshot polling.
- Add an optional compact worker-status projection with deduplication,
  coalescing, safe command categories, and rate-limited quiet heartbeats.
- Emit line-delimited records promptly so a terminal or tool host can keep the
  stream live in the background while the calling agent does other work.
- Keep ordinary hcom messages, requests, replies, subscriptions, and result
  waits authoritative and unchanged; the stream observes events but does not
  create or reinterpret conversation.
- Keep stream adoption explicit. Existing commands and `hcom run` workflows do
  not begin streaming unless the caller opts in.

## Capabilities

### New Capabilities

- `correlated-worker-waits`: Optional continuous event observation alongside
  existing one-shot waits, including safe compact status for an exact worker
  generation while preserving normal hcom interaction.

### Modified Capabilities

None.

## Impact

- Event command parsing, filtering, cursor handling, notification/recheck
  plumbing, and structured output in `src/commands/events.rs`.
- Compact status projection and correlation helpers in core modules.
- Event command help and regression/integration coverage.
- No default change to `events --wait`, `events sub`, hcom messaging, or bundled
  and user-created workflow behavior.
