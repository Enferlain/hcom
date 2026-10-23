# Coordination patterns

These six examples target this checkout's correlated waits. They request
background workers explicitly and inherit the configured models and tool
permissions. The scripts coordinate messaging; a result receipt does not
establish that its contents are correct.

All examples load [workflow-common.bash](scripts/workflow-common.bash), capture
a cursor before launch, create all recipients before dispatch, target exact
names, and clean up their own workers on exit. Use them when the task
authorizes launching these workers. Installation and arguments are described
in [script-template.md](script-template.md).

| Script | Workers | Protocol |
|--------|---------|----------|
| [basic-messaging.sh](scripts/basic-messaging.sh) | Claude worker + reviewer | Worker reports to reviewer; reviewer checks the answer and reports to the coordinator. |
| [review-loop.sh](scripts/review-loop.sh) | Claude worker + reviewer | Reviewer requests fixes or approves; worker reports after approval or a three-round limit. |
| [ensemble-consensus.sh](scripts/ensemble-consensus.sh) | Three Claude contestants + judge | Judge collects three distinct answers and synthesizes a verdict. |
| [cascade-pipeline.sh](scripts/cascade-pipeline.sh) | Claude planner, then executor | Coordinator receives a plan, then starts an executor with the full report and transcript reference. |
| [cross-tool-duo.sh](scripts/cross-tool-duo.sh) | Claude architect + Codex engineer | Architect sends a spec, engineer implements and reports, architect assesses the result. |
| [codex-worker.sh](scripts/codex-worker.sh) | Codex coder + Claude reviewer | Coder reports, reviewer inspects the diff and transcript before reporting. |

For an ordinary interactive delegation, use the visible launch and durable
progress observer from `SKILL.md`; the script examples intentionally own and
terminate background workers.

## Result protocol

Peer messages, progress, and feedback use the run's work thread. Only the
designated final publisher sends an `inform` on its result thread. The
coordinator waits with `--result-from` for that exact worker generation and
prints the result event. Failures and inconclusive results must be explicit
in the final text, since exit 0 only means that a result was received.

The cascade uses distinct result threads for planning and execution, with a
fresh cursor before each launch. The next stage receives the full result JSON,
including recovery provenance, so the handoff works without a recorded
transcript or a separately stored message for a recovered provider report.

## Applying the patterns

Use separate write ownership for parallel implementers. A transcript is useful
context, while the actual diff and relevant tests establish the outcome. Use
Git when the workers do not share the same filesystem. Keep a reviewer alive
through the whole feedback loop, and cap its rounds rather than relying on
unlimited autonomous retries.
