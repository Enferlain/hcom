# hcom-powered GitHub participation

Status: **Inactive design prototype. Do not deploy the generated workflows.**

This directory preserves exploratory `gh-aw`, Kuro App, and hcom/Antigravity
integration work for later reference. The provider-specific engine is not the
intended production architecture, and the generated workflow files must remain
inactive until hcom has a provider-neutral, isolated worker boundary. It is
retained to avoid discarding useful research, not as a supported feature.

## Architecture

GitHub Agentic Workflows (`gh-aw`) owns repository events, sandboxing, GitHub
read access, concurrency, budgets, durable thread memory, and validated writes.
The Kuro GitHub App supplies the identity for safe outputs. This avoids building
a second GitHub event coordinator inside hcom.

```text
issue or pull-request activity
            |
            v
compiled gh-aw workflow
            |
            v
sandboxed agent on a GitHub-hosted Linux runner
            |
            +---- read-only GitHub access through gh-proxy
            |
            v
validated gh-aw safe output
            |
            v
Kuro GitHub App comment, label, or review
```

The model never receives Kuro's private key or a GitHub write token. It can read
through the restricted GitHub tool layer and request only the safe-output types
declared by the workflow. Compiler-generated jobs mint short-lived App tokens
for the requested operation.

## Workflows

### `kuro-issue-participant`

Runs on issue creation and meaningful issue or comment changes. It may add one
substantive comment and up to two existing labels. A managed comment-memory
record prevents repeated conclusions and acknowledgement chatter across runs.

### `kuro-pr-participant`

Runs when a same-repository pull request is opened or updated and when pull
request discussion or review activity changes. Automatic `pull_request` runs
exclude forks because GitHub does not provide the required engine and App
secrets to those events; a later default-branch bridge can add fork support
without weakening that boundary. The workflow does not check out or execute
pull-request code. Until that bridge exists, a fork PR gets no automatic review
on open or synchronize, although later comments or reviews can trigger the
default-branch workflow safely. It may submit one `COMMENT` or
`REQUEST_CHANGES` review and at most three inline findings. `APPROVE` and merge
are not permitted.

Both workflows skip known bots (gh-aw accepts names with or without the
`[bot]` suffix), rate-limit community-triggered runs, apply a five-minute
cooldown, cap turns and AI credits, and require `noop` when there is nothing new
to contribute. The explicit Kuro skip entry prevents its own output from
recursively starting another agent run.

These are reactive workflows, not a continuously running model. Each event
starts a bounded run that reconstructs the current thread through GitHub and
the managed comment-memory record. That gives each issue or pull request a
durable watch history without paying for an idle agent.

## Activation prerequisites

The initial workflows use GitHub-hosted Linux runners. `gh-aw` installs and
sandboxes its supported Gemini engine there, so no persistent runner service is
needed for this milestone. A self-hosted runner becomes relevant only for a
future hcom engine that must reach local Antigravity or GLM sessions.

The initial workflow engine is Gemini. GitHub Actions must have a usable
`GEMINI_API_KEY` secret (or the workflow must later be changed to a configured
engine). The existing Kuro configuration is:

```text
Actions variable: KURO_CLIENT_ID
Actions secret:   KURO_PRIVATE_KEY
```

Kuro is installed only on `Enferlain/hcom`. Its authentication health check is
`.github/workflows/kuro-auth-check.yml`.

## Development and verification

The Markdown files are the editable sources. Commit their generated lock files
as well:

```bash
gh aw compile kuro-issue-participant kuro-pr-participant \
  --actionlint --validate
gh aw status
```

After a live run, inspect cost and security artifacts with:

```bash
gh aw audit <run-id>
```

Never edit the generated `*.lock.yml` files directly.

## hcom's remaining role

The first milestone uses a native `gh-aw` engine because GitHub events, MCP
access, memory, sandboxing, and safe outputs are already solved there. hcom is
still relevant for capabilities that `gh-aw` does not provide by itself:

- cross-provider worker/reviewer teams;
- resuming existing local agent sessions;
- Antigravity and GLM execution profiles;
- hcom completion and quiet-communication policies.

Add those later as an hcom-owned third-party `gh-aw` engine definition or a
bounded multi-agent stage. Do not replace `gh-aw`'s event or safe-output layers,
and do not expose Kuro credentials to hcom workers.

The earlier custom implementation in PR #8 predates this design and should not
be merged as the production integration.
