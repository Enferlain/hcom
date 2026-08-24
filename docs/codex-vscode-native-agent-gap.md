# Codex VS Code: closing the native-agent integration gap

## Current behavior

hcom can identify Codex conversations hosted by the VS Code extension, resolve CLI commands to the
conversation's hcom identity, and deliver queued messages through Codex hooks. This makes ordinary
messaging work without launching Codex through hcom or running `hcom start`.

Hook delivery is event-driven. A queued message can be injected when Codex processes a prompt or a
supported tool hook, but writing the message to hcom's database does not itself start a Codex turn.
If the main conversation has already returned control to the user, the message remains queued until
the next hook-producing action.

## Why native subagents feel different

Native subagents are owned by Codex's orchestrator. The parent turn remains open while Codex waits
for child results, routes follow-ups, and collects the results before producing its final response.
hcom is currently external to that scheduler: hooks let it add context to an event that Codex is
already processing, but they do not provide a reverse channel for starting or resuming a turn.

This is an integration gap, not a fundamental closed-source limitation. Codex app-server is open
source and is the protocol layer used by rich clients such as the VS Code extension.

## Recommended next step: active-turn MCP wait

Add an hcom MCP operation with semantics similar to:

```text
wait_for_messages(thread, timeout, filters) -> structured messages | timeout | cancellation
```

The parent Codex agent would call it after delegating work through hcom. The MCP call would remain
pending while the external agent works and return as soon as a matching hcom message arrives. This
keeps the parent turn alive and provides native-like collection behavior without modifying the IDE:

1. Codex delegates work and records a workflow thread.
2. Codex calls the bounded wait operation.
3. An external agent sends a result on that thread.
4. The MCP call returns the structured result to the active Codex turn.
5. Codex verifies the result and finishes its response.

Requirements:

- use hcom's event waiting mechanism rather than polling with `sleep`;
- require or generate a workflow thread so unrelated messages cannot satisfy the wait;
- support timeout and cancellation without losing messages;
- return sender, intent, thread, event ID, timestamp, and message text as structured data;
- do not acknowledge delivery until the MCP result has been returned successfully;
- expose progress/health information where the MCP transport supports it;
- test late messages, cancellation, duplicate delivery, concurrent waits, and parent termination.

This solves the normal delegated-work case. It does not create a new model turn after the parent has
already finalized; native workflows normally avoid that situation by keeping the parent open too.

## Optional deeper integration: app-server wake bridge

If unsolicited hcom messages must start a new turn in an idle VS Code conversation, integrate at the
app-server/client layer. A bridge would need to associate hcom identities with Codex thread IDs,
subscribe to hcom events, distinguish active from idle turns, submit an explicit continuation, and
surface lifecycle state in the IDE.

That design needs explicit policy for:

- which senders and intents may initiate a model turn;
- quota/token consequences of automatically starting turns;
- duplicate suppression and restart recovery;
- user visibility, cancellation, approvals, and workspace trust;
- avoiding recursive agent-to-agent wake loops;
- compatibility with Codex app-server and IDE extension updates.

Prefer the MCP wait operation first. Build the app-server bridge only if starting entirely new turns
without user activity is a real requirement.
