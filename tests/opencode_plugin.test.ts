// Regression tests for the hcom OpenCode plugin (src/opencode_plugin/hcom.ts).
// Run: bun test tests/opencode_plugin.test.ts
//
// Everything external is faked: Bun.which / Bun.listen / Bun.$ are replaced
// and the v1 client / v2 setup context are in-memory doubles, so no real hcom
// subprocess, CLI, or provider is ever invoked. HCOM_DIR/HOME point at a temp
// dir before the dynamic import because the plugin computes LOG_PATH once at
// module load. Tests assert observable effects: recorded shell commands,
// prompt/switchModel calls, injected message text, and disposal effects.

import { afterAll, afterEach, beforeEach, expect, test } from "bun:test"
import { mkdirSync, mkdtempSync, rmSync } from "fs"
import { tmpdir } from "os"
import { join } from "path"

const tempHome = mkdtempSync(join(tmpdir(), "hcom-plugin-tests-"))
const tempHcomDir = join(tempHome, ".hcom")
process.env.HCOM_DIR = tempHcomDir
process.env.HOME = tempHome
mkdirSync(join(tempHcomDir, ".tmp", "logs"), { recursive: true })

const MANAGED_ENV_KEYS = [
  "HCOM_DIR",
  "HOME",
  "HCOM_LAUNCHED",
  "HCOM_RESUME_SESSION_ID",
  "HCOM_OPENCODE_AGENT",
  "HCOM_OPENCODE_MODEL",
] as const
const envOriginals = new Map<string, string | undefined>(
  MANAGED_ENV_KEYS.map((key) => [key, process.env[key]]),
)

const realWhich = Bun.which
const realDollar = Bun.$
const realListen = Bun.listen

const mod: any = await import("../src/opencode_plugin/hcom.ts")

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

interface Deferred<T> {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (error: unknown) => void
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

type ShellResult = { exitCode: number; text: () => string; stderr: string }

const shellOk = (text: string): ShellResult => ({ exitCode: 0, text: () => text, stderr: "" })

type ShellHandler = (cmd: string) => ShellResult | Promise<ShellResult>

// Tagged-template shell double. The plugin always calls $.nothrow()`cmd`.quiet(),
// so nothrow must be a method on $ returning a tagged function whose result has
// .quiet(). Handlers can return deferred promises for in-flight windows.
function makeShell() {
  const calls: string[] = []
  let handler: ShellHandler = () => shellOk("")

  const nothrow = (strings: TemplateStringsArray, ...values: unknown[]) => {
    const cmd = strings.reduce(
      (acc, part, i) => acc + part + (i < values.length ? String(values[i]) : ""),
      "",
    )
    calls.push(cmd)
    return { quiet: async () => handler(cmd) }
  }
  const $: any = Object.assign(
    (strings: TemplateStringsArray, ...values: unknown[]) => nothrow(strings, ...values),
    // The source calls $.nothrow() first (a factory), then tags the result.
    { nothrow: () => nothrow },
  )

  return {
    $,
    calls,
    on(fn: ShellHandler) {
      handler = fn
    },
  }
}

type FakeListener = {
  port: number
  stopped: boolean
  stopCalls: number
  opts: any
  stop: (closeActiveConnections?: boolean) => void
}

// Bun.listen double: records the listener, counts stop() calls, and exposes
// wake() to simulate an hcom TCP notify connection.
function installListenMock() {
  let created: FakeListener | null = null
  ;(Bun as any).listen = (opts: any): FakeListener => {
    const listener: FakeListener = {
      port: 47111,
      stopped: false,
      stopCalls: 0,
      opts,
      stop: (closeActiveConnections?: boolean) => {
        void closeActiveConnections
        listener.stopCalls++
        listener.stopped = true
      },
    }
    created = listener
    return listener
  }
  return {
    get current(): FakeListener | null {
      return created
    },
  }
}

// v1 client double: records promptAsync requests and serves session.status
// from a mutable map.
function makeV1Client() {
  const statusMap: Record<string, { type: string }> = {}
  const prompts: { req: any }[] = []
  const client = {
    session: {
      promptAsync: (req: any) => {
        prompts.push({ req })
        return Promise.resolve({ ok: true })
      },
      status: async () => ({ data: statusMap }),
    },
  }
  return { client, statusMap, prompts }
}

// v2 setup-context double: abort-aware async-generator event queue, recorded
// hook registrations with inspectable disposals, prompt/switchModel recorders.
// session.hook registration can be gated for the readiness-race test.
function makeV2Ctx() {
  const registrations: { kind: string; disposed: boolean }[] = []
  const disposeAttempts: string[] = []
  const failDisposeKinds = new Set<string>()
  const hookCalls: Record<string, any[]> = {}
  const eventQueue: { type: string; data: any }[] = []
  const promptCalls: { sessionID: string; text: string; delivery?: string }[] = []
  const switchCalls: { sessionID: string; model: any }[] = []
  const switchDeferreds: Deferred<void>[] = []
  let agentDefault: string | null = null
  let consumerFinished = false
  let registrationGate: Deferred<void> | null = null
  let wakeWaiter: (() => void) | null = null

  const track = (kind: string) => {
    const reg = { kind, disposed: false }
    registrations.push(reg)
    return {
      dispose: async () => {
        disposeAttempts.push(kind)
        if (failDisposeKinds.has(kind)) {
          failDisposeKinds.delete(kind) // fail once; later dispose calls succeed
          throw new Error(`dispose failed: ${kind}`)
        }
        reg.disposed = true
      },
    }
  }

  const ctx: any = {
    event: {
      subscribe: (options?: { signal?: AbortSignal }) => {
        const signal = options?.signal
        async function* stream() {
          try {
            while (!signal?.aborted) {
              const next = eventQueue.shift()
              if (next !== undefined) {
                yield next
                continue
              }
              await new Promise<void>((resolve) => {
                wakeWaiter = resolve
                signal?.addEventListener("abort", () => resolve(), { once: true })
              })
            }
          } finally {
            consumerFinished = true
          }
        }
        return stream()
      },
    },
    agent: {
      transform: async (edit: (editor: { default: (id: string) => void }) => void) => {
        edit({ default: (id: string) => (agentDefault = id) })
        return track("agent")
      },
    },
    session: {
      prompt: async (input: { sessionID: string; text: string; delivery?: string }) => {
        promptCalls.push(input)
      },
      switchModel: (input: { sessionID: string; model: any }) => {
        switchCalls.push(input)
        const d = deferred<void>()
        switchDeferreds.push(d)
        return d.promise
      },
      hook: async (name: string, callback: any) => {
        if (registrationGate) await registrationGate.promise
        ;(hookCalls[name] ??= []).push(callback)
        return track(name)
      },
    },
    tool: {
      hook: async (name: string, callback: any) => {
        ;(hookCalls[`tool:${name}`] ??= []).push(callback)
        return track(`tool:${name}`)
      },
    },
  }

  return {
    ctx,
    registrations,
    disposeAttempts,
    hookCalls,
    promptCalls,
    switchCalls,
    switchDeferreds,
    agentDefault: () => agentDefault,
    consumerFinished: () => consumerFinished,
    failDisposeOn(kind: string) {
      failDisposeKinds.add(kind)
    },
    gateRegistrations(d: Deferred<void>) {
      registrationGate = d
    },
    pushEvent(type: string, data: any) {
      eventQueue.push({ type, data })
      const wake = wakeWaiter
      wakeWaiter = null
      wake?.()
    },
  }
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

const flush = async (rounds = 8) => {
  for (let i = 0; i < rounds; i++) await new Promise<void>((resolve) => setTimeout(resolve, 0))
}

const waitFor = async (condition: () => boolean, what: string, timeoutMs = 2000) => {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise<void>((resolve) => setTimeout(resolve, 5))
  }
}

const startPayload = (over: Record<string, unknown> = {}) =>
  JSON.stringify({
    name: "rx01",
    session_id: "sess-A",
    agent: "builder",
    model: { providerID: "prov", modelID: "m1", variant: "vx" },
    bootstrap: null,
    ...over,
  })

// Transform hook message shape; parts are mutable and carry synthetic when
// the plugin appends the bootstrap payload.
type TransformMessage = {
  info: { id: string; role: string; sessionID: string }
  parts: { type: string; text?: string; synthetic?: boolean }[]
}

const statusCalls = (calls: string[]) =>
  calls.filter((c) => c.includes("opencode-status") && !c.includes("--tool"))
const readCalls = (calls: string[]) =>
  calls.filter((c) => c.includes("opencode-read") && !c.includes("--ack"))
const ackCalls = (calls: string[]) =>
  calls.filter((c) => c.includes("opencode-read") && c.includes("--ack --up-to"))

// Per-test resources; afterEach drains everything so no timer, socket, or
// patched global outlives a case.
let teardown: (() => Promise<void>) | null = null
let listenMock: ReturnType<typeof installListenMock> | null = null
let currentHooks: any = null

function pushTeardown(fn: () => Promise<void>) {
  const prev = teardown
  teardown = async () => {
    await prev?.()
    await fn()
  }
}

function bindShellGlobals(shell: ReturnType<typeof makeShell>, which: string | null = "/fake/bin/hcom") {
  ;(Bun as any).which = () => which
  ;(Bun as any).$ = shell.$
  listenMock = installListenMock()
}

// Fresh v1 plugin instance against the fakes; disposed in afterEach. The hook
// object is kept in currentHooks for direct invocation.
async function makeV1(client: unknown, shell: ReturnType<typeof makeShell>, which: string | null = "/fake/bin/hcom") {
  bindShellGlobals(shell, which)
  const hooks: any = await mod.HcomPlugin({ client, $: shell.$ })
  pushTeardown(() => hooks.dispose())
  currentHooks = hooks
  return hooks
}

// Fresh v2 setup against the fakes; the returned cleanup runs in afterEach.
async function makeV2(shell: ReturnType<typeof makeShell>, ctx: unknown) {
  bindShellGlobals(shell)
  const cleanup: () => Promise<void> = await mod.default.setup(ctx)
  pushTeardown(cleanup)
  return cleanup
}

const fireEvent = (event: any) => currentHooks.event({ event })

beforeEach(() => {
  process.env.HCOM_LAUNCHED = "1"
  delete process.env.HCOM_RESUME_SESSION_ID
  delete process.env.HCOM_OPENCODE_AGENT
  delete process.env.HCOM_OPENCODE_MODEL
})

afterEach(async () => {
  const run = teardown
  teardown = null
  currentHooks = null
  if (run) await run()
  ;(Bun as any).which = realWhich
  ;(Bun as any).$ = realDollar
  ;(Bun as any).listen = realListen
  listenMock = null
})

afterAll(() => {
  for (const key of MANAGED_ENV_KEYS) {
    const original = envOriginals.get(key)
    if (original === undefined) delete process.env[key]
    else process.env[key] = original
  }
  ;(Bun as any).which = realWhich
  ;(Bun as any).$ = realDollar
  ;(Bun as any).listen = realListen
  rmSync(tempHome, { recursive: true, force: true })
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test("module exposes v1 named HcomPlugin and v2 default {id, server, setup}", () => {
  expect(typeof mod.HcomPlugin).toBe("function")
  expect(mod.default.id).toBe("hcom")
  expect(mod.default.server).toBe(mod.HcomPlugin)
  expect(typeof mod.default.setup).toBe("function")
})

test("resume binding delivers the unread inbox before any user prompt, without acking", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload({ bootstrap: "BOOTSTRAP-PAYLOAD" }))
    if (cmd.includes("opencode-read")) {
      return shellOk(
        JSON.stringify([
          { event_id: 5, from: "alice", message: "hello", intent: "inform", thread: "team" },
          { event_id: 6, from: "bob", message: "yo" },
        ]),
      )
    }
    return shellOk("")
  })
  const { client, prompts } = makeV1Client()
  await makeV1(client, shell)
  await flush()

  expect(shell.calls.some((c) => c.startsWith("hcom opencode-start --session-id sess-A --notify-port 47111"))).toBe(true)
  expect(prompts).toHaveLength(1)
  const req = prompts[0].req
  expect(req.path).toEqual({ id: "sess-A" })
  expect(req.body.parts[0].text).toBe(
    "<hcom>[2 new messages] | [inform:team #5] alice -> rx01: hello | [new message #6] bob -> rx01: yo</hcom>",
  )
  expect(req.body.agent).toBe("builder")
  expect(req.body.model).toEqual({ providerID: "prov", modelID: "m1" }) // variant stripped from model ref
  expect(req.body.variant).toBe("vx")
  // Deferred ack: nothing is acked until a transform actually runs.
  expect(ackCalls(shell.calls)).toHaveLength(0)
})

test("transform appends the bootstrap and acks the injected batch only once it runs", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload({ bootstrap: "BOOTSTRAP-PAYLOAD" }))
    if (cmd.includes("opencode-read")) {
      return shellOk(
        JSON.stringify([
          { event_id: 5, from: "alice", message: "hello" },
          { event_id: 6, from: "bob", message: "yo" },
        ]),
      )
    }
    return shellOk("")
  })
  const { client, prompts } = makeV1Client()
  await makeV1(client, shell)
  await flush()

  expect(prompts).toHaveLength(1)
  expect(ackCalls(shell.calls)).toHaveLength(0)
  const readsBeforeTransform = readCalls(shell.calls).length

  const messages: TransformMessage[] = [
    { info: { id: "u1", role: "user", sessionID: "sess-A" }, parts: [{ type: "text", text: "user typed this" }] },
  ]
  await currentHooks["experimental.chat.messages.transform"]({}, { messages })
  await flush()

  // Bootstrap is appended as a synthetic part; the typed text is untouched.
  expect(messages[0].parts).toHaveLength(2)
  expect(messages[0].parts[0].text).toBe("user typed this")
  expect(messages[0].parts[1].synthetic).toBe(true)
  expect(messages[0].parts[1].text).toBe("BOOTSTRAP-PAYLOAD")
  // The deferred ack lands exactly once, for the delivered batch (max id 6),
  // and the transform performs no extra inbox read (body fill not applicable).
  expect(ackCalls(shell.calls)).toEqual([
    expect.stringContaining("hcom opencode-read --name rx01 --ack --up-to 6"),
  ])
  expect(readCalls(shell.calls).length).toBe(readsBeforeTransform)
})

test("transform fills a bodyless <hcom> tag from the inbox and acks in the same pass", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const inbox: any[] = []
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload({ bootstrap: "BOOTSTRAP-PAYLOAD" }))
    if (cmd.includes("opencode-read")) return shellOk(JSON.stringify(inbox))
    return shellOk("")
  })
  const { client, prompts } = makeV1Client()
  await makeV1(client, shell)
  await flush()
  // Empty inbox at bind: nothing injected, nothing pending.
  expect(prompts).toHaveLength(0)

  inbox.push({ event_id: 3, from: "carol", message: "ping" })
  // Legacy v1 call shape: empty hook input (no sessionID) must still work.
  const messages: TransformMessage[] = [
    { info: { id: "u1", role: "user", sessionID: "sess-A" }, parts: [{ type: "text", text: "<hcom></hcom>" }] },
  ]
  await currentHooks["experimental.chat.messages.transform"]({}, { messages })
  await flush()

  expect(messages[0].parts[0].text).toBe("<hcom>[new message #3] carol -> rx01: ping</hcom>")
  expect(messages[0].parts[1].synthetic).toBe(true)
  expect(ackCalls(shell.calls)).toEqual([expect.stringContaining("--ack --up-to 3")])
  expect(prompts).toHaveLength(0) // body fill rewrites the message; no promptAsync
})

test("foreign sessions are filtered from events, tool status, transform, and compaction", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) return shellOk(JSON.stringify([]))
    return shellOk("")
  })
  const { client } = makeV1Client()
  await makeV1(client, shell)
  await flush()

  const before = shell.calls.length
  await fireEvent({ type: "session.status", properties: { sessionID: "sess-B", status: { type: "idle" } } })
  await fireEvent({ type: "session.created", properties: { info: { id: "sess-B" } } })
  await currentHooks["tool.execute.before"]({ tool: "edit", sessionID: "sess-B" }, { args: { filePath: "x.ts" } })

  const foreignMessages = [
    { info: { id: "u9", role: "user", sessionID: "sess-B" }, parts: [{ type: "text", text: "<hcom></hcom>" }] },
  ]
  await currentHooks["experimental.chat.messages.transform"]({ sessionID: "sess-B" }, { messages: foreignMessages })
  const compactionContext: string[] = []
  await currentHooks["experimental.session.compacting"]({ sessionID: "sess-B" }, { context: compactionContext })
  await flush()

  expect(shell.calls.slice(before)).toHaveLength(0)
  expect(foreignMessages[0].parts[0].text).toBe("<hcom></hcom>")
  expect(compactionContext).toHaveLength(0)
})

test("permission.asked reports blocked and suppresses tool status and delivery until replied", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const inbox: any[] = []
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) return shellOk(JSON.stringify(inbox))
    return shellOk("")
  })
  const { client, statusMap, prompts } = makeV1Client()
  await makeV1(client, shell)
  await flush()
  expect(prompts).toHaveLength(0)

  await fireEvent({ type: "permission.asked", properties: { id: "p1", sessionID: "sess-A", permission: "bash" } })
  await flush()
  expect(statusCalls(shell.calls)).toEqual([
    expect.stringContaining("--status blocked --context approval --detail bash"),
  ])

  // Tool status and idle delivery are suppressed while permission is pending.
  await currentHooks["tool.execute.before"]({ tool: "edit", sessionID: "sess-A" }, { args: { filePath: "x.ts" } })
  await fireEvent({ type: "session.status", properties: { sessionID: "sess-A", status: { type: "idle" } } })
  await flush()
  expect(shell.calls.some((c) => c.includes("--tool"))).toBe(false)
  expect(prompts).toHaveLength(0)

  inbox.push({ event_id: 11, from: "dave", message: "go" })
  statusMap["sess-A"] = { type: "idle" }
  await fireEvent({ type: "permission.replied", properties: { sessionID: "sess-A" } })
  await flush()

  const statuses = statusCalls(shell.calls).map((c) => c.match(/--status ([a-z]+)/)![1])
  expect(statuses).toEqual(["blocked", "listening"])
  expect(prompts).toHaveLength(1)
  expect(prompts[0].req.body.parts[0].text).toBe("<hcom>[new message #11] dave -> rx01: go</hcom>")
})

test("tool forwarding caps plain strings and preserves patch headers in capped bodies", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) return shellOk(JSON.stringify([]))
    return shellOk("")
  })
  const { client } = makeV1Client()
  await makeV1(client, shell)
  await flush()

  const longPatch = [
    "*** Begin Patch",
    "z".repeat(10000) + "TAILMARKER",
    "*** Update File: after.ts",
    "*** Move to: final.ts",
    "--- a/patched.ts",
    "+++ b/patched.ts",
  ].join("\n")
  await currentHooks["tool.execute.before"](
    { tool: "edit", sessionID: "sess-A" },
    { args: { filePath: "src/a.ts", paths: ["b.ts", "c.ts"], command: "y".repeat(3000), patchText: longPatch } },
  )
  await flush()

  const toolCalls = shell.calls.filter((c) => c.includes("--tool edit --input-json"))
  expect(toolCalls).toHaveLength(1)
  const payload = JSON.parse(toolCalls[0].slice(toolCalls[0].indexOf("--input-json ") + "--input-json ".length))
  expect(toolCalls[0]).toContain("--status active")
  expect(payload.filePath).toBe("src/a.ts")
  expect(payload.paths).toEqual(["b.ts", "c.ts"]) // non-string args pass through
  expect(payload.command).toHaveLength(2000) // plain long strings hard-capped
  // The patch body is sampled away, but every path header survives the cap.
  expect(payload.patchText.length).toBeLessThanOrEqual(8000)
  expect(payload.patchText).toContain("*** Begin Patch")
  expect(payload.patchText).toContain("*** Update File: after.ts")
  expect(payload.patchText).toContain("*** Move to: final.ts")
  expect(payload.patchText).toContain("--- a/patched.ts")
  expect(payload.patchText).toContain("+++ b/patched.ts")
  expect(payload.patchText).not.toContain("TAILMARKER")
})

test("v2 setup registers prompt/context/compaction/tool hooks and applies the launch agent", async () => {
  process.env.HCOM_OPENCODE_AGENT = "builder"
  const shell = makeShell()
  const v2 = makeV2Ctx()
  await makeV2(shell, v2.ctx)

  expect(v2.agentDefault()).toBe("builder")
  expect(Object.keys(v2.hookCalls).sort()).toEqual(["compaction", "context", "prompt", "tool:execute.before"])
  expect(v2.registrations.map((r: any) => r.kind)).toEqual([
    "agent",
    "prompt",
    "context",
    "tool:execute.before",
    "compaction",
  ])
})

test("v2 event mapping binds, maps busy/idle, delivers on idle, and rejects foreign drafts", async () => {
  const inbox: any[] = []
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload({ bootstrap: "BOOTSTRAP-PAYLOAD" }))
    if (cmd.includes("opencode-read") && cmd.includes("--ack")) {
      inbox.length = 0 // acked messages are no longer returned as unread
      return shellOk("")
    }
    if (cmd.includes("opencode-read")) return shellOk(JSON.stringify(inbox))
    return shellOk("")
  })
  const v2 = makeV2Ctx()
  await makeV2(shell, v2.ctx)

  v2.pushEvent("session.created", { sessionID: "sess-A" })
  await flush()
  expect(shell.calls.some((c) => c.includes("opencode-start --session-id sess-A"))).toBe(true)

  inbox.push({ event_id: 9, from: "carol", message: "ping" })
  v2.pushEvent("session.execution.started", { sessionID: "sess-A" })
  await flush()
  v2.pushEvent("session.execution.interrupted", { sessionID: "sess-A" })
  await flush()

  const statuses = statusCalls(shell.calls).map((c) => c.match(/--status ([a-z]+)/)![1])
  expect(statuses).toEqual(["active", "listening"])
  expect(v2.promptCalls).toEqual([
    {
      sessionID: "sess-A",
      text: "<hcom>[new message #9] carol -> rx01: ping</hcom>",
      delivery: "queue",
    },
  ])

  // session.idle while the injected batch is still pending ack: no duplicate
  // delivery and no redundant status flip.
  v2.pushEvent("session.idle", { sessionID: "sess-A" })
  await flush()
  expect(v2.promptCalls).toHaveLength(1)
  expect(statusCalls(shell.calls)).toHaveLength(2)

  // Unknown v2 events are dropped.
  const before = shell.calls.length
  v2.pushEvent("session.error", { sessionID: "sess-A" })
  await flush()
  expect(shell.calls.slice(before)).toHaveLength(0)

  // A foreign-session draft must not receive the bound bootstrap or ack the
  // bound pending inbox; the adapter leaves its system prompt untouched.
  const foreignDraft: any = {
    sessionID: "sess-B",
    system: [],
    messages: [{ id: "m1", role: "user", content: [{ type: "text", text: "hello" }] }],
  }
  await v2.hookCalls.context[0](foreignDraft)
  expect(foreignDraft.system).toHaveLength(0)
  expect(foreignDraft.messages[0].content[0].text).toBe("hello")
  const foreignCompaction: any = { sessionID: "sess-B", system: [] }
  await v2.hookCalls.compaction[0](foreignCompaction)
  expect(foreignCompaction.system).toHaveLength(0)
  // The bound session's pending inbox batch (pending ack id 9) is untouched:
  // the foreign transform must not read or ack it.
  expect(ackCalls(shell.calls)).toHaveLength(0)

  // Same-session drafts exercise the v2 adapter's positive path: the bootstrap
  // is lifted into the system prompt, the injected user text is preserved, and
  // the pending batch (event 9) is acked exactly once.
  const boundDraft: any = {
    sessionID: "sess-A",
    system: [],
    messages: [
      {
        id: "m2",
        role: "user",
        content: [{ type: "text", text: "<hcom>[new message #9] carol -> rx01: ping</hcom>" }],
      },
    ],
  }
  await v2.hookCalls.context[0](boundDraft)
  expect(boundDraft.system).toEqual([{ type: "text", text: "BOOTSTRAP-PAYLOAD" }])
  expect(boundDraft.messages[0].content).toHaveLength(1)
  expect(boundDraft.messages[0].content[0].text).toBe("<hcom>[new message #9] carol -> rx01: ping</hcom>")
  expect(ackCalls(shell.calls)).toEqual([expect.stringContaining("--ack --up-to 9")])
  // The wake queued during the pending-ack idle drains after the ack, but the
  // already-acked batch is not reinjected.
  await flush()
  expect(v2.promptCalls).toHaveLength(1)

  // Same-session compaction adds the bound-instance guidance to the system prompt.
  const boundCompaction: any = { sessionID: "sess-A", system: [] }
  await v2.hookCalls.compaction[0](boundCompaction)
  expect(boundCompaction.system).toHaveLength(1)
  expect(boundCompaction.system[0].text).toContain('connected to hcom as "rx01"')
  expect(boundCompaction.system[0].text).toContain("--name rx01")
})

test("setup readiness: fast bind and queued events wait for hook registration before prompting", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) {
      return shellOk(JSON.stringify([{ event_id: 4, from: "eve", message: "soon" }]))
    }
    return shellOk("")
  })
  const v2 = makeV2Ctx()
  const gate = deferred<void>()
  v2.gateRegistrations(gate)
  // Queue session events before setup: none may be processed before hooks are
  // registered, and the idle event must not bypass delivery-readiness either.
  v2.pushEvent("session.created", { sessionID: "sess-A" })
  v2.pushEvent("session.idle", { sessionID: "sess-A" })

  bindShellGlobals(shell)
  const setupDone = mod.default.setup(v2.ctx)
  try {
    await flush()
    // Binding ran, but the resume delivery is parked until hooks are registered.
    expect(shell.calls.filter((c) => c.includes("opencode-start")).length).toBe(1)
    expect(readCalls(shell.calls)).toHaveLength(0)
    expect(v2.promptCalls).toHaveLength(0)
  } finally {
    // Gated setup must always complete so its disposer is registered even if
    // a pre-release assertion fails.
    gate.resolve()
    pushTeardown(await setupDone)
  }
  await flush()

  expect(readCalls(shell.calls)).toHaveLength(1)
  expect(v2.promptCalls).toEqual([
    {
      sessionID: "sess-A",
      text: "<hcom>[new message #4] eve -> rx01: soon</hcom>",
      delivery: "queue",
    },
  ])
  // The queued session.created was processed after readiness, without a second bind.
  expect(shell.calls.filter((c) => c.includes("opencode-start")).length).toBe(1)
})

test("partial registration failure disposes earlier registrations and the socket, and rethrows the original error", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) return shellOk(JSON.stringify([]))
    return shellOk("")
  })
  const v2 = makeV2Ctx()
  const boom = new Error("tool registration failed")
  v2.ctx.tool.hook = async () => {
    throw boom
  }

  bindShellGlobals(shell)
  let caught: unknown = null
  try {
    await mod.default.setup(v2.ctx)
  } catch (e) {
    caught = e
  }
  expect(caught).toBe(boom)

  expect(v2.registrations.map((r: any) => r.kind)).toEqual(["prompt", "context"])
  expect(v2.registrations.every((r: any) => r.disposed)).toBe(true)
  expect(listenMock!.current?.stopped).toBe(true) // hooks.dispose ran on the failure path
  expect(ackCalls(shell.calls)).toHaveLength(0)

  // The event consumer never started, so queued events have no effects.
  const before = shell.calls.length
  v2.pushEvent("session.idle", { sessionID: "sess-A" })
  await flush()
  expect(shell.calls.slice(before)).toHaveLength(0)
})

test("cleanup attempts every registration disposal and rejects with the first failure", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) return shellOk(JSON.stringify([]))
    return shellOk("")
  })
  const v2 = makeV2Ctx()

  bindShellGlobals(shell)
  const cleanup = await mod.default.setup(v2.ctx)
  // The cleanup disposer is owned by this case rather than by afterEach. The
  // armed dispose failure is consumed by the first call; the finally runs
  // cleanup again for guaranteed teardown (the mock failure is one-shot).
  try {
    v2.failDisposeOn("prompt")
    let cleanupError: unknown = null
    try {
      await cleanup()
    } catch (e) {
      cleanupError = e
    }
    expect(String(cleanupError)).toContain("dispose failed: prompt")
    // Every registration disposal was attempted despite the failure.
    expect(v2.disposeAttempts).toEqual(["prompt", "context", "tool:execute.before", "compaction"])
    // Non-failing registrations were still disposed.
    expect(v2.registrations.find((r: any) => r.kind === "context")!.disposed).toBe(true)
    expect(v2.registrations.find((r: any) => r.kind === "compaction")!.disposed).toBe(true)
  } finally {
    await cleanup()
  }
})

test("setup registration error stays original even when an earlier disposal fails", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) return shellOk(JSON.stringify([]))
    return shellOk("")
  })
  const v2 = makeV2Ctx()
  v2.failDisposeOn("context")
  const boom = new Error("tool registration failed")
  v2.ctx.tool.hook = async () => {
    throw boom
  }

  bindShellGlobals(shell)
  let caught: unknown = null
  try {
    await mod.default.setup(v2.ctx)
  } catch (e) {
    caught = e
  }
  // The registration error is reported, not the failing cleanup.
  expect(caught).toBe(boom)
  // Remaining registrations were attempted: prompt disposed, context attempted.
  expect(v2.disposeAttempts).toEqual(["prompt", "context"])
  expect(v2.registrations.find((r: any) => r.kind === "prompt")!.disposed).toBe(true)
})

test("disposal during in-flight bind, delivery read, or transform read leaves no late effects", async () => {
  // Part A: opencode-start is still pending when teardown runs.
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const startCall = deferred<ShellResult>()
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return startCall.promise
    if (cmd.includes("opencode-read")) {
      return shellOk(JSON.stringify([{ event_id: 21, from: "fred", message: "late" }]))
    }
    return shellOk("")
  })
  const v2 = makeV2Ctx()
  const cleanupA = await makeV2(shell, v2.ctx)
  await flush()
  expect(shell.calls.some((c) => c.includes("opencode-start"))).toBe(true)

  await cleanupA() // must resolve without the bind: teardown never awaits unabortable shells
  startCall.resolve(shellOk(startPayload()))
  await flush()
  expect(readCalls(shell.calls)).toHaveLength(0)
  expect(v2.promptCalls).toHaveLength(0)
  expect(ackCalls(shell.calls)).toHaveLength(0)
  expect(listenMock!.current?.stopped).toBe(true)

  // Part B: a delivery read is in flight when teardown runs.
  const readCall = deferred<ShellResult>()
  const shellB = makeShell()
  shellB.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) return readCall.promise
    return shellOk("")
  })
  const v2B = makeV2Ctx()
  const cleanupB = await makeV2(shellB, v2B.ctx)
  await waitFor(() => readCalls(shellB.calls).length > 0, "delivery read")
  await cleanupB()
  readCall.resolve(shellOk(JSON.stringify([{ event_id: 22, from: "gina", message: "late" }])))
  await flush()
  expect(v2B.promptCalls).toHaveLength(0)
  expect(ackCalls(shellB.calls)).toHaveLength(0)

  // Part C: the transform's inbox read is in flight when the plugin unloads.
  const transformRead = deferred<ShellResult>()
  let parkNextRead = false
  const shellC = makeShell()
  shellC.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) {
      if (parkNextRead) return transformRead.promise
      return shellOk(JSON.stringify([]))
    }
    return shellOk("")
  })
  const { client } = makeV1Client()
  await makeV1(client, shellC)
  await flush()

  const messages = [
    { info: { id: "u1", role: "user", sessionID: "sess-A" }, parts: [{ type: "text", text: "<hcom></hcom>" }] },
  ]
  parkNextRead = true
  const transformDone = currentHooks["experimental.chat.messages.transform"]({}, { messages })
  await waitFor(() => readCalls(shellC.calls).length >= 2, "transform inbox read")
  await currentHooks.dispose()
  transformRead.resolve(shellOk(JSON.stringify([{ event_id: 8, from: "hank", message: "late" }])))
  await transformDone
  await flush()
  expect(messages[0].parts[0].text).toBe("<hcom></hcom>")
  expect(ackCalls(shellC.calls)).toHaveLength(0)
})

test("teardown aborts the event subscriber; later events have no effects", async () => {
  const shell = makeShell()
  shell.on((cmd) => {
    if (cmd.includes("opencode-start")) return shellOk(startPayload())
    if (cmd.includes("opencode-read")) return shellOk(JSON.stringify([]))
    return shellOk("")
  })
  const v2 = makeV2Ctx()
  const cleanup = await makeV2(shell, v2.ctx)

  v2.pushEvent("session.created", { sessionID: "sess-A" })
  await flush()
  expect(shell.calls.some((c) => c.includes("opencode-start"))).toBe(true)
  expect(v2.promptCalls).toHaveLength(0)

  await cleanup()
  await flush()
  expect(v2.consumerFinished()).toBe(true)

  const before = shell.calls.length
  v2.pushEvent("session.idle", { sessionID: "sess-A" })
  await flush()
  expect(shell.calls.slice(before)).toHaveLength(0)
})

test("first-prompt model switch is shared by concurrent prompts and retried after failure", async () => {
  process.env.HCOM_OPENCODE_MODEL = "prov/m1#fast"
  const shell = makeShell()
  const v2 = makeV2Ctx()
  await makeV2(shell, v2.ctx)
  const prompt = v2.hookCalls.prompt[0] as (draft: { sessionID: string }) => Promise<void>

  const first = prompt({ sessionID: "sess-A" })
  const second = prompt({ sessionID: "sess-A" })
  await flush()
  // Concurrent first prompts share one switch call.
  expect(v2.switchCalls).toHaveLength(1)
  expect(v2.switchCalls[0].model).toEqual({ providerID: "prov", id: "m1", variant: "fast" })

  // Attach rejection handlers before failing the shared switch, so both
  // prompts observe the rejection and nothing is left unhandled.
  const failures: unknown[] = []
  first.catch((e) => failures.push(e))
  second.catch((e) => failures.push(e))
  v2.switchDeferreds[0].reject(new Error("switch failed"))
  await flush()
  expect(failures).toHaveLength(2)
  expect(failures[0]).toBe(failures[1])

  // The rejected cache entry is evicted: the next prompt retries the switch.
  const retry = prompt({ sessionID: "sess-A" })
  await flush()
  expect(v2.switchCalls).toHaveLength(2)
  v2.switchDeferreds[1].resolve()
  await retry

  // A different session gets its own switch.
  const other = prompt({ sessionID: "sess-B" })
  await flush()
  expect(v2.switchCalls).toHaveLength(3)
  expect(v2.switchCalls[2].sessionID).toBe("sess-B")
  v2.switchDeferreds[2].resolve()
  await other
})

test("missing hcom binary disables binding, events, and tool status", async () => {
  process.env.HCOM_RESUME_SESSION_ID = "sess-A"
  const shell = makeShell()
  const { client } = makeV1Client()
  await makeV1(client, shell, null)
  await flush()

  expect(shell.calls).toHaveLength(0)
  expect(listenMock!.current).toBeNull()

  await fireEvent({ type: "session.created", properties: { info: { id: "sess-A" } } })
  await currentHooks["tool.execute.before"]({ tool: "edit", sessionID: "sess-A" }, { args: { filePath: "x.ts" } })
  await flush()
  expect(shell.calls).toHaveLength(0)
})
