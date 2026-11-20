import type { Plugin, PluginInput } from "@opencode-ai/plugin"
import type { Event } from "@opencode-ai/sdk"
import { appendFileSync } from "fs"
import { homedir } from "os"

const HCOM_DIR = process.env.HCOM_DIR || `${homedir()}/.hcom`
const LOG_PATH = `${HCOM_DIR}/.tmp/logs/hcom.log`

type PromptModel = {
  providerID: string
  modelID: string
  variant?: string
}

type PermissionAskedEvent = {
  type: "permission.asked"
  properties: {
    id: string
    sessionID: string
    permission: string
  }
}

type HcomEvent = Event | PermissionAskedEvent

function eventSessionID(event: HcomEvent): string | undefined {
  if (!event.properties || typeof event.properties !== "object") return undefined
  const properties = event.properties as Record<string, unknown>
  if (typeof properties.sessionID === "string") return properties.sessionID
  const info = properties.info
  if (info && typeof info === "object" && typeof (info as Record<string, unknown>).id === "string") {
    return (info as Record<string, unknown>).id as string
  }
  return undefined
}

// Best-effort fallback for non-hcom/manual plugin runs.
// Normal hcom launches seed launch agent/model through the `opencode-start`
// response payload, since the plugin process does not inherit the outer
// `hcom opencode --agent/--model` argv in PTY-launched OpenCode.
function parseCliArgValue(...flags: string[]): string | null {
  for (let i = 0; i < process.argv.length; i++) {
    const token = process.argv[i]
    for (const flag of flags) {
      if (token === flag) return process.argv[i + 1] ?? null
      if (token.startsWith(`${flag}=`)) return token.slice(flag.length + 1)
    }
  }
  return null
}

function parseCliModelArg() {
  const raw = parseCliArgValue("--model", "-m")
  if (!raw) return null
  const slash = raw.indexOf("/")
  if (slash <= 0 || slash === raw.length - 1) return null
  const variant = parseCliArgValue("--variant") ?? undefined
  return {
    providerID: raw.slice(0, slash),
    modelID: raw.slice(slash + 1),
    ...(variant ? { variant } : {}),
  }
}

function normalizePromptModel(model: unknown, inputVariant?: unknown) {
  if (!model || typeof model !== "object") return null
  const providerID = (model as Record<string, unknown>).providerID
  const modelID = (model as Record<string, unknown>).modelID
  if (typeof providerID !== "string" || typeof modelID !== "string") return null
  const modelVariant = (model as Record<string, unknown>).variant
  const variant = typeof inputVariant === "string"
    ? inputVariant
    : typeof modelVariant === "string"
      ? modelVariant
      : undefined
  return {
    providerID,
    modelID,
    ...(variant ? { variant } : {}),
  }
}

function promptModelRef(model: PromptModel | null) {
  if (!model) return undefined
  return {
    providerID: model.providerID,
    modelID: model.modelID,
  }
}

function isPatchHeader(line: string): boolean {
  const trimmed = line.trim()
  if (trimmed.startsWith("***")) return true
  if (line.startsWith("diff --git")) return true
  if (line.startsWith("--- ") || line.startsWith("+++ ")) return true
  if (line.startsWith("rename ") || line.startsWith("similarity ") || line.startsWith("copy ")) return true
  if (line.startsWith("index ") || line.startsWith("deleted file") || line.startsWith("new file")) return true
  if (line.startsWith("Binary files")) return true
  if (trimmed.toLowerCase().startsWith("move to")) return true
  return false
}

// Keep patch path headers (diff --git, ---, +++, rename, similarity, Move to, *** Update/Add/Delete/Move)
// across long patches while discarding oversized hunk bodies to prevent argv explosion.
function extractBoundToolString(key: string, value: string, maxLen = 2000): string {
  if (value.length <= maxLen) return value
  const isPatch = key === "patchText" || key === "patch" ||
    ((key === "command" || !key) && (value.includes("*** ") || value.includes("diff --git") || value.toLowerCase().includes("move to"))) ||
    value.includes("*** Begin Patch")
  if (isPatch) {
    const lines = value.split("\n")
    const preservedLines: string[] = []
    let bodySampleLen = 0
    for (const line of lines) {
      if (isPatchHeader(line)) {
        preservedLines.push(line)
      } else if (bodySampleLen < 300) {
        const remaining = 300 - bodySampleLen
        const cappedLine = line.length > remaining ? line.slice(0, remaining) : line
        preservedLines.push(cappedLine)
        bodySampleLen += cappedLine.length + 1
      }
    }
    const result = preservedLines.join("\n")
    return result.length > 8000 ? result.slice(0, 8000) : result
  }
  return value.slice(0, maxLen)
}

function log(
  level: "DEBUG" | "INFO" | "WARN" | "ERROR",
  event: string,
  instance?: string | null,
  extra?: Record<string, unknown>,
) {
  const entry = JSON.stringify({
    ts: new Date().toISOString().replace(/\.\d{3}Z$/, "Z"),
    level,
    subsystem: "plugin",
    event,
    ...(instance ? { instance } : {}),
    ...extra,
  })
  try { appendFileSync(LOG_PATH, entry + "\n") } catch {}
}

export const HcomPlugin: Plugin = async (input: PluginInput) => {
  const { client, $ } = input
  const v2Ready: Promise<void> | undefined = (input as any)?.v2Ready ?? (client as any)?.v2Ready
  let hcomChecked = false
  let hcomAvailable = false
  let instanceName: string | null = null      // IDEN-03: bound instance name
  let sessionId: string | null = null         // IDEN-02: tracked for messages.transform
  let bootstrapText: string | null = null     // BOOT-01: cached from opencode-start
  let bindingPromise: Promise<void> | null = null  // Prevents duplicate binding
  let reconcileTimer: ReturnType<typeof setInterval> | null = null  // Periodic status sync + delivery fallback
  let reconcileInFlight = false                 // Prevents concurrent reconcile calls from overlapping interval ticks
  let notifyServer: Bun.TCPSocketListener | null = null  // TCP notify server for instant message wake
  let lastReportedStatus: string | null = null  // Skip redundant status updates
  let pendingAckId: number | null = null        // Deferred ack: set by deliverPendingToIdle, acked by transform
  let deliveryInFlight = false                  // Delivery guard flag set before the first await
  let deliveryPending = false                   // Wake arrived while delivery was already in flight
  let deliveryRetryScheduled = false            // Avoid duplicate queued retry passes
  let permissionPending = false                  // Exact permission gate from OpenCode events
  let disposed = false                           // Plugin unloaded: its OpenCode client must not be called
  let launchedAgent: string | null = parseCliArgValue("--agent")
  let launchedModel: PromptModel | null = parseCliModelArg()
  let currentAgent: string | null = launchedAgent
  let currentModel: PromptModel | null = launchedModel

  // SAFE-02: Lazy PATH detection on first hook callback
  function checkHcom(): boolean {
    if (!hcomChecked) {
      hcomChecked = true
      hcomAvailable = Bun.which("hcom") !== null
      if (!hcomAvailable) {
        log("WARN", "plugin.no_hcom")
      }
    }
    return hcomAvailable
  }

  function isBoundSession(candidateSessionId?: string | null): boolean {
    return !candidateSessionId || !sessionId || candidateSessionId === sessionId
  }

  function ignoreForeignSession(event: string, candidateSessionId?: string | null): boolean {
    if (isBoundSession(candidateSessionId)) return false
    log("DEBUG", event, instanceName, {
      session_id: candidateSessionId,
      bound_session_id: sessionId,
    })
    return true
  }

  function formatMessagesForInjection(messages: any[], recipientName: string): string {
    const parts = messages.map((m: any) => {
      const prefix = m.intent
        ? m.thread
          ? `[${m.intent}:${m.thread} #${m.event_id}]`
          : `[${m.intent} #${m.event_id}]`
        : m.thread
          ? `[thread:${m.thread} #${m.event_id}]`
          : `[new message #${m.event_id}]`
      return `${prefix} ${m.from} -> ${recipientName}: ${m.message}`
    })
    if (messages.length === 1) return `<hcom>${parts[0]}</hcom>`
    return `<hcom>[${messages.length} new messages] | ${parts.join(" | ")}</hcom>`
  }

  function schedulePendingDelivery(sid: string, reason: string): void {
    if (disposed) return
    if (deliveryRetryScheduled) return
    deliveryRetryScheduled = true
    log("DEBUG", "plugin.delivery_retry_scheduled", instanceName, { reason })
    queueMicrotask(() => {
      deliveryRetryScheduled = false
      if (disposed) return
      if (!instanceName) return
      void deliverPendingToIdle(sid)
    })
  }

  // Re-arm a queued wake once nothing is mid-flight. The pendingAckId guard
  // leaves the still-injecting case to the post-ack drain so we do not
  // double-deliver the same unread message batch.
  function drainPendingDelivery(sid: string, reason: string): void {
    if (disposed) return
    if (deliveryPending && pendingAckId === null) {
      deliveryPending = false
      schedulePendingDelivery(sid, reason)
    }
  }

  // Deliver pending messages via promptAsync. Ack is deferred to transform
  // (fires on the loop iteration that actually processes the user message).
  //
  // Three-layer serialization:
  //   deliveryInFlight — guard flag set synchronously before the first await.
  //     Closes the TOCTOU window where TCP notify and idle-status wake paths
  //     could both pass a null check before either one set the value.
  //     Concurrent callers set deliveryPending so their wake is replayed after
  //     the current pass, or after the current injected message is acked.
  //   pendingAckId — set after messages are read, cleared by transform.
  //     Prevents re-delivery while a prior injection is still being processed.
  //     If promptAsync fails to queue, pendingAckId is cleared immediately.
  //   deliveryPending — a wake that arrived while delivery was in-flight or an
  //     injection was pending ack. drainPendingDelivery replays it at each exit
  //     point: normal finally, deferred ack, and promptAsync rejection.
  async function deliverPendingToIdle(sid: string): Promise<boolean> {
    if (disposed) return false
    if (v2Ready) await v2Ready
    if (disposed) return false
    if (permissionPending) {
      log("DEBUG", "plugin.delivery_skipped", instanceName, { reason: "permission_pending" })
      return false
    }
    if (!instanceName) return false
    if (ignoreForeignSession("plugin.delivery_ignored_foreign_session", sid)) {
      return false
    }
    if (deliveryInFlight) {
      deliveryPending = true
      log("DEBUG", "plugin.delivery_skipped", instanceName, { reason: "delivery_in_flight", queued: true })
      return false
    }
    if (pendingAckId !== null) {
      deliveryPending = true
      log("DEBUG", "plugin.delivery_skipped", instanceName, { reason: "pending_ack_in_flight", pending_ack: pendingAckId, queued: true })
      return false
    }
    deliveryInFlight = true
    try {
      const msgResult = await $.nothrow()`hcom opencode-read --name ${instanceName}`.quiet()
      // Unacked messages stay unread, so the next plugin instance delivers them.
      if (disposed) return false
      if (msgResult.exitCode !== 0) {
        log("WARN", "plugin.delivery_read_failed", instanceName, { exit_code: msgResult.exitCode, stderr: msgResult.stderr.toString().slice(0, 200) })
        return false
      }
      let rawMessages: any[] = []
      try { rawMessages = JSON.parse(msgResult.text()) } catch (e) {
        log("WARN", "plugin.delivery_parse_failed", instanceName, { error: String(e), raw: msgResult.text().slice(0, 200) })
        return false
      }
      if (disposed) return false
      if (!Array.isArray(rawMessages) || rawMessages.length === 0) {
        log("DEBUG", "plugin.delivery_no_messages", instanceName)
        return false
      }

      const maxId = Math.max(...rawMessages.map((m: any) => m.event_id || 0))
      if (maxId === 0) return false

      const formatted = formatMessagesForInjection(rawMessages, instanceName)
      // Don't ack here — defer to transform so cursor advances only when
      // the loop is actually processing the message. This keeps messages
      // unread until delivery is confirmed.
      pendingAckId = maxId
      log("DEBUG", "plugin.delivery_payload", instanceName, {
        session_id: sid,
        current_agent: currentAgent,
        current_model: currentModel?.modelID ?? null,
        current_variant: currentModel?.variant ?? null,
      })
      if (disposed) {
        pendingAckId = null
        return false
      }
      try {
        // Runtime contract note: keep this cast until the plugin's bundled client
        // typings are aligned across shipped OpenCode builds.
        const promptAsyncResult = client.session.promptAsync({
          path: { id: sid },
          body: {
            agent: currentAgent ?? undefined,
            model: promptModelRef(currentModel),
            variant: currentModel?.variant ?? undefined,
            parts: [{ type: "text", text: formatted }],
          },
        } as any) // SDK types don't expose agent/model on the async variant; body shape matches the sync prompt endpoint
        if (promptAsyncResult && typeof (promptAsyncResult as Promise<unknown>).then === "function") {
          void (promptAsyncResult as Promise<unknown>).catch((e) => {
            if (pendingAckId === maxId) pendingAckId = null
            if (!disposed) {
              log("ERROR", "plugin.delivery_prompt_failed", instanceName, {
                error: String(e),
                pending_ack: maxId,
              })
              drainPendingDelivery(sid, "prompt_async_failed_pending_wake")
            }
          })
        }
      } catch (e) {
        pendingAckId = null
        if (!disposed) {
          log("ERROR", "plugin.delivery_prompt_failed", instanceName, {
            error: String(e),
            pending_ack: maxId,
            sync_throw: true,
          })
        }
        return false
      }
      if (disposed) {
        pendingAckId = null
        return false
      }
      log("INFO", "plugin.delivery_pending", instanceName, {
        msg: `promptAsync, ack deferred to transform (maxId=${maxId})`,
        count: rawMessages.length,
        pending_ack: maxId,
      })
      return true
    } finally {
      deliveryInFlight = false
      if (!disposed) {
        drainPendingDelivery(sid, "delivery_in_flight_wake")
      }
    }
  }

  // Periodic status sync: polls session status API as a retry mechanism
  // in case the event-driven opencode-status call failed (subprocess error,
  // daemon down, etc. other made up scenario etc.). Does NOT deliver messages — that's handled by
  // TCP notify (on message arrival) and session.status events (on idle).
  async function reconcile(): Promise<void> {
    if (disposed) return
    if (reconcileInFlight) return
    if (permissionPending) return
    if (!instanceName || !sessionId) return
    reconcileInFlight = true
    try {
      const statusResult = await client.session.status()
      if (disposed) return
      if (!statusResult.data) return
      const current = statusResult.data[sessionId]
      const isIdle = !current || current.type === "idle"
      const hcomStatus = isIdle ? "listening" : "active"
      if (hcomStatus !== lastReportedStatus) {
        lastReportedStatus = hcomStatus
        if (disposed) return
        await $.nothrow()`hcom opencode-status --name ${instanceName} --status ${hcomStatus}`.quiet()
        if (disposed) return
        log("INFO", "plugin.reconcile_status", instanceName, { status: hcomStatus })
      }
    } catch (e) {
      if (!disposed) {
        log("ERROR", "plugin.reconcile_error", instanceName, { error: String(e) })
      }
    } finally {
      reconcileInFlight = false
    }
  }

  function startReconcileTimer(): void {
    if (disposed) return
    stopReconcileTimer()
    if (disposed) return
    reconcileTimer = setInterval(() => {
      if (disposed) {
        stopReconcileTimer()
        return
      }
      reconcile()
    }, 5_000)
  }

  function stopReconcileTimer(): void {
    if (reconcileTimer) { clearInterval(reconcileTimer); reconcileTimer = null }
  }

  // TCP notify server: instant wake for sends addressed to this instance and
  // for broadcasts. Other observed events are picked up by reconciliation.
  function startNotifyServer(): number | null {
    if (disposed) return null
    if (notifyServer) return notifyServer.port
    try {
      notifyServer = Bun.listen({
        hostname: "127.0.0.1",
        port: 0,
        socket: {
          open(socket) {
            socket.end()
            if (disposed) return
            log("DEBUG", "notify_server.wake", instanceName, { status: lastReportedStatus, pending_ack: pendingAckId })
            if (sessionId && instanceName) deliverPendingToIdle(sessionId)
          },
          data() {},
          close() {},
          error() {},
        },
      })
      if (disposed) {
        stopNotifyServer()
        return null
      }
      log("INFO", "notify_server.started", instanceName, { port: notifyServer.port })
      return notifyServer.port
    } catch (e) {
      if (!disposed) {
        log("ERROR", "notify_server.start_failed", instanceName, { error: String(e) })
      }
      return null
    }
  }

  function stopNotifyServer(): void {
    if (notifyServer) {
      try { notifyServer.stop(true) } catch {}
      notifyServer = null
    }
  }

  async function bindIdentity(sid: string): Promise<void> {
    if (disposed) return
    if (instanceName || bindingPromise) return
    if (process.env.HCOM_LAUNCHED !== "1") return

    bindingPromise = (async () => {
      try {
        if (disposed) return
        // Start TCP notify server before binding so port is registered atomically
        const notifyPort = startNotifyServer()
        if (disposed) { stopNotifyServer(); return }
        const result = notifyPort
          ? await $.nothrow()`hcom opencode-start --session-id ${sid} --notify-port ${String(notifyPort)}`.quiet()
          : await $.nothrow()`hcom opencode-start --session-id ${sid}`.quiet()
        if (disposed) { stopNotifyServer(); return }
        if (result.exitCode !== 0) { stopNotifyServer(); return }
        const json = JSON.parse(result.text())
        if (disposed) { stopNotifyServer(); return }
        if (json.error) {
          log("WARN", "plugin.bind_failed", null, { error: json.error })
          stopNotifyServer()
          return
        }
        const boundModel = normalizePromptModel(json.model)
        if (typeof json.agent === "string") launchedAgent = json.agent
        if (boundModel) launchedModel = boundModel
        instanceName = json.name
        sessionId = json.session_id
        bootstrapText = json.bootstrap || null
        currentAgent = launchedAgent
        currentModel = launchedModel
        log("INFO", "plugin.bound", instanceName, {
          session_id: sessionId,
          notify_port: notifyPort,
          bootstrap_len: bootstrapText?.length ?? 0,
          launched_agent: launchedAgent,
          launched_model: launchedModel?.modelID ?? null,
        })
      } catch (e) {
        if (!disposed) {
          log("ERROR", "plugin.bind_error", null, { error: String(e) })
        }
        stopNotifyServer()
      } finally {
        bindingPromise = null
      }
    })()
    await bindingPromise
    if (disposed) return
  }

  // Resumed session (`hcom r`): OpenCode emits no session.created, and no
  // session.status until the first prompt, so bind now and deliver anything
  // already pending. Without this, messages waited for the user to type.
  // Not awaited: plugin load must not wait on hcom.
  const resumeSessionId = process.env.HCOM_RESUME_SESSION_ID
  if (resumeSessionId && checkHcom()) {
    void (async () => {
      await bindIdentity(resumeSessionId)
      if (v2Ready) await v2Ready
      if (disposed || !instanceName || sessionId !== resumeSessionId) return
      log("INFO", "plugin.resume_bound", instanceName, { session_id: sessionId })
      lastReportedStatus = "listening"
      startReconcileTimer()
      await deliverPendingToIdle(resumeSessionId)
    })().catch((e) => {
      if (!disposed) log("ERROR", "plugin.resume_bind_error", instanceName, { error: String(e) })
    })
  }

  return {
    event: async ({ event }: { event: HcomEvent }) => {
      try {
        if (disposed) return
        if (!checkHcom()) return
        const eventSessionId = eventSessionID(event)
        if (eventSessionId && !sessionId) {
          sessionId = eventSessionId as string
        }
        if (instanceName && ignoreForeignSession("plugin.event_ignored_foreign_session", eventSessionId)) {
          return
        }
        switch (event.type) {
          case "session.created": {
            const createdSessionId = event.properties.info.id
            log("INFO", "plugin.session_created", instanceName, { session_id: createdSessionId })
            if (createdSessionId && !instanceName && !bindingPromise) {
              await bindIdentity(createdSessionId)
              if (disposed) return
            }
            break
          }
          case "permission.asked": {
            permissionPending = true
            const eventSessionId = event.properties.sessionID
            if (eventSessionId && !instanceName && !bindingPromise) {
              await bindIdentity(eventSessionId)
              if (disposed) return
            }
            if (disposed) return
            if (instanceName) {
              lastReportedStatus = "blocked"
              await $.nothrow()`hcom opencode-status --name ${instanceName} --status blocked --context ${"approval"} --detail ${String(event.properties.permission ?? "")}`.quiet()
              if (disposed) return
              log("INFO", "plugin.permission_asked", instanceName, { permission: event.properties.permission, request_id: event.properties.id })
            }
            break
          }
          case "permission.replied": {
            permissionPending = false
            const eventSessionId = event.properties.sessionID
            if (instanceName) {
              const statusResult = await client.session.status()
              if (disposed) return
              const current = eventSessionId ? statusResult.data?.[eventSessionId] : null
              const hcomStatus = !current || current.type === "idle" ? "listening" : "active"
              lastReportedStatus = hcomStatus
              if (disposed) return
              await $.nothrow()`hcom opencode-status --name ${instanceName} --status ${hcomStatus}`.quiet()
              if (disposed) return
              if (hcomStatus === "listening" && eventSessionId) {
                await deliverPendingToIdle(eventSessionId)
              }
            }
            break
          }
          case "session.status": {
            const statusType = event.properties.status.type
            const eventSessionId = event.properties.sessionID

            log("DEBUG", "plugin.session_status", instanceName, { status: statusType })

            // Bind identity on resume (session.created doesn't fire for existing sessions)
            if (eventSessionId && !instanceName && !bindingPromise) {
              await bindIdentity(eventSessionId)
              if (disposed) return
            }

            // Report status to hcom daemon (skip if unchanged)
            if (permissionPending) {
              startReconcileTimer()
              break
            }
            if (instanceName) {
              const hcomStatus = statusType === "idle" ? "listening" : "active"
              if (hcomStatus !== lastReportedStatus) {
                lastReportedStatus = hcomStatus
                if (disposed) return
                await $.nothrow()`hcom opencode-status --name ${instanceName} --status ${hcomStatus}`.quiet()
                if (disposed) return
              }
              // Ensure reconcile timer is running (catches missed idle events)
              startReconcileTimer()
            }

            // Idle transition: deliver any pending messages
            if (statusType === "idle" && instanceName && eventSessionId) {
              if (disposed) return
              await deliverPendingToIdle(eventSessionId)
            }
            break
          }
          case "session.deleted":
            log("INFO", "plugin.session_deleted", instanceName)
            stopNotifyServer()
            stopReconcileTimer()
            if (instanceName && !disposed) {
              await $.nothrow()`hcom opencode-stop --name ${instanceName} --reason closed`.quiet()
            }
            instanceName = null
            sessionId = null
            bootstrapText = null
            bindingPromise = null
            lastReportedStatus = null
            pendingAckId = null
            deliveryInFlight = false
            deliveryPending = false
            deliveryRetryScheduled = false
            permissionPending = false
            currentAgent = launchedAgent
            currentModel = launchedModel
            break
        }
      } catch (e) {
        if (!disposed) {
          log("ERROR", "plugin.event_error", instanceName, { error: String(e) })
        }
      }
    },

    "chat.message": async (input, output) => {
      try {
        if (disposed) return
        if (!checkHcom()) return
        if (input.sessionID && !sessionId) {
          sessionId = input.sessionID
        }
        if (bindingPromise) {
          await bindingPromise
          if (disposed) return
        }
        if (input.sessionID && !instanceName) {
          await bindIdentity(input.sessionID)
          if (disposed) return
        }
        if (isBoundSession(input.sessionID)) {
          if (input.agent) currentAgent = input.agent
          const resolvedModel = normalizePromptModel(input.model, input.variant)
          if (resolvedModel) currentModel = resolvedModel
        } else {
          ignoreForeignSession("plugin.chat_message_ignored_foreign_session", input.sessionID)
        }
        log("DEBUG", "plugin.chat_message", instanceName, {
          session_id: input.sessionID,
          agent: input.agent,
          model: input.model?.modelID,
          variant: input.variant,
        })
      } catch (e) {
        if (!disposed) {
          log("ERROR", "plugin.chat_message_error", instanceName, { error: String(e) })
        }
      }
    },

    // Tool activity for status/`events --cmd`/`--file`. hcom derives the detail
    // from the tool's status_detail mapping; long strings (write content, patch
    // bodies) are capped so argv stays small; the fields hcom reads come first.
    "tool.execute.before": async (input, output) => {
      try {
        if (disposed) return
        if (permissionPending) return
        if (!checkHcom() || !instanceName || !isBoundSession(input.sessionID)) return
        const args: Record<string, unknown> = {}
        for (const [key, value] of Object.entries(output.args ?? {})) {
          args[key] = typeof value === "string" ? extractBoundToolString(key, value, 2000) : value
        }
        // Recorded so the next idle edge is not skipped as "unchanged".
        lastReportedStatus = "active"
        if (disposed) return
        await $.nothrow()`hcom opencode-status --name ${instanceName} --status active --tool ${input.tool} --input-json ${JSON.stringify(args)}`.quiet()
      } catch (e) {
        if (!disposed) {
          log("ERROR", "plugin.tool_status_error", instanceName, { error: String(e) })
        }
      }
    },

    "experimental.chat.messages.transform": async (input, output) => {
      try {
        if (disposed) return
        if (!checkHcom()) return
        const transformSessionId = (input as Record<string, unknown>)?.sessionID as string | undefined
        if (bindingPromise) {
          await bindingPromise
          if (disposed) return
        }
        if (!instanceName && sessionId) {
          await bindIdentity(sessionId)
          if (disposed) return
        }
        if (!instanceName || !sessionId) return
        if (disposed) return
        if (transformSessionId && ignoreForeignSession("plugin.transform_ignored_foreign_session", transformSessionId)) {
          return
        }

        // OpenCode transform mutations are prompt-local, not persisted to stored
        // session history, so keep injecting the original bootstrap payload.
        const messages = output.messages ?? []
        const msgCount = messages.length
        const userMsgCount = messages.filter((m: any) => m.info.role === "user").length
        if (bootstrapText) {
          const firstUserMsg = messages.find((m: any) => m.info.role === "user")
          if (firstUserMsg) {
            firstUserMsg.parts.push({
              id: crypto.randomUUID(),
              messageID: firstUserMsg.info.id,
              sessionID: firstUserMsg.info.sessionID,
              type: "text",
              text: bootstrapText,
              synthetic: true,
            })
            log("DEBUG", "plugin.transform_bootstrap", instanceName, { msg_count: msgCount, user_msgs: userMsgCount, bootstrap_len: bootstrapText.length })
          } else {
            log("WARN", "plugin.transform_no_user_msg", instanceName, { msg_count: msgCount })
          }
        } else {
          log("DEBUG", "plugin.transform_no_bootstrap", instanceName, { msg_count: msgCount, user_msgs: userMsgCount })
        }

        // Bootstrap body fill: the PTY bootstrap injects a bodyless <hcom> tag
        // (no message text, just envelope) because the TUI input box can't safely
        // hold arbitrary message bodies (@ triggers, width overflow). The transform
        // hook fires on the same loop iteration, so we fetch the real body here,
        // replace the bodyless tag, and ack — giving the agent the full message on
        // its first turn without a wasted round-trip.
        const lastUserMsg = [...messages].reverse().find((m: any) => m.info.role === "user")
        if (lastUserMsg && lastUserMsg.parts) {
          const textPart = lastUserMsg.parts.find((p: any) =>
            p.type === "text" && !p.synthetic && typeof p.text === "string" &&
            p.text.startsWith("<hcom>") && p.text.endsWith("</hcom>") && !p.text.includes(": ")
          )
          if (textPart?.type === "text" && pendingAckId === null) {
            const msgResult = await $.nothrow()`hcom opencode-read --name ${instanceName}`.quiet()
            if (disposed) return
            if (msgResult.exitCode === 0) {
              let rawMessages: any[] = []
              try { rawMessages = JSON.parse(msgResult.text()) } catch {}
              if (Array.isArray(rawMessages) && rawMessages.length > 0) {
                const maxId = Math.max(...rawMessages.map((m: any) => m.event_id || 0))
                if (maxId > 0) {
                  if (disposed) return
                  const formatted = formatMessagesForInjection(rawMessages, instanceName)
                  textPart.text = formatted
                  pendingAckId = maxId
                  log("INFO", "plugin.transform_bootstrap_delivery", instanceName, { max_id: maxId, msg_count: rawMessages.length })
                }
              }
            }
          }
        }

        if (disposed) return

        // Deferred ack: deliverPendingToIdle called promptAsync but didn't ack.
        // Transform fires on the loop iteration processing that message — ack now.
        if (pendingAckId !== null) {
          const ackId = pendingAckId
          pendingAckId = null
          if (disposed) return
          await $.nothrow()`hcom opencode-read --name ${instanceName} --ack --up-to ${String(ackId)}`.quiet()
          if (disposed) return
          log("INFO", "plugin.deferred_ack", instanceName, { acked_to: ackId })
          if (deliveryPending && sessionId) {
            drainPendingDelivery(sessionId, "post_ack_pending_wake")
          }
        }
      } catch (e) {
        if (!disposed) {
          log("ERROR", "plugin.transform_error", instanceName, { error: String(e) })
        }
      }
    },

    "experimental.session.compacting": async (input, output) => {
      try {
        if (disposed) return
        if (!checkHcom()) return
        if (!instanceName) return
        const compactingSessionId = (input as Record<string, unknown>)?.sessionID as string | undefined
        if (compactingSessionId && ignoreForeignSession("plugin.compaction_ignored_foreign_session", compactingSessionId)) {
          return
        }

        output.context.push(
          `You are connected to hcom as "${instanceName}". ` +
          `Use --name ${instanceName} for all hcom commands.`
        )
        log("INFO", "plugin.compaction_reset", instanceName)
      } catch (e) {
        if (!disposed) {
          log("ERROR", "plugin.compaction_error", instanceName, { error: String(e) })
        }
      }
    },

    // Plugin unload, not session end: the hcom instance stays bound, since a
    // reloaded plugin rebinds the same session.
    dispose: async () => {
      disposed = true
      stopNotifyServer()
      stopReconcileTimer()
    },
  }
}

// OpenCode 2 loads plugins from the module's default export and hands `setup`
// a context with no v1 `client`/`$`. Typed structurally: the published
// @opencode-ai/plugin types do not describe the v2 session and event domains.
type V2Event = { type: string; data: Record<string, any> }
type V2TextPart = { type: string; text?: string }
type V2Draft = {
  sessionID: string
  system: V2TextPart[]
  messages?: { id?: string; role: string; content: V2TextPart[] }[]
}
type V2Registration = { dispose: () => Promise<void> }
type V2Model = { providerID: string; id: string; variant?: string }
type V2Context = {
  event: { subscribe: (options?: { signal?: AbortSignal }) => AsyncIterable<V2Event> }
  agent: { transform: (edit: (editor: { default: (id: string) => void }) => void) => Promise<V2Registration> }
  session: {
    prompt: (input: { sessionID: string; text: string; delivery?: "steer" | "queue" }) => Promise<unknown>
    switchModel: (input: { sessionID: string; model: V2Model }) => Promise<unknown>
    hook: (name: string, callback: (draft: any) => Promise<void> | void) => Promise<V2Registration>
  }
  tool: {
    hook: (
      name: "execute.before",
      callback: (event: { tool: string; sessionID: string; id: string; input: unknown }) => Promise<void> | void,
    ) => Promise<V2Registration>
  }
}

// Reshapes v2 events into the v1 events HcomPlugin handles. Execution events
// carry the busy/idle edge.
function v1Event({ type, data }: V2Event): HcomEvent | null {
  switch (type) {
    case "session.created":
      return { type, properties: { info: { ...data, id: data.sessionID } } } as any
    case "session.status":
      return { type, properties: data } as any
    case "session.idle":
      return { type: "session.status", properties: { sessionID: data.sessionID, status: { type: "idle" } } } as any
    case "session.execution.started":
      return { type: "session.status", properties: { sessionID: data.sessionID, status: { type: "busy" } } } as any
    case "session.execution.succeeded":
    case "session.execution.failed":
    case "session.execution.interrupted":
      return { type: "session.status", properties: { sessionID: data.sessionID, status: { type: "idle" } } } as any
    case "permission.asked":
      return { type, properties: { id: data.id, sessionID: data.sessionID, permission: data.action } }
    case "permission.replied":
    case "session.deleted":
      return { type, properties: data } as any
  }
  return null
}

// `provider/model[#variant]`, the form `opencode --model` took.
function parseLaunchModel(raw: string | undefined): V2Model | undefined {
  const [ref, variant] = (raw ?? "").split("#")
  const slash = ref.indexOf("/")
  if (slash <= 0 || slash === ref.length - 1) return undefined
  return { providerID: ref.slice(0, slash), id: ref.slice(slash + 1), ...(variant ? { variant } : {}) }
}

async function setupOpenCode2(ctx: V2Context) {
  const statusBySession: Record<string, { type: string }> = {}
  let markHooksReady!: () => void
  const hooksReady = new Promise<void>((resolve) => {
    markHooksReady = resolve
  })

  let disposed = false

  const client = {
    session: {
      promptAsync: ({ path, body }: any) => {
        if (disposed) return Promise.resolve()
        return ctx.session.prompt({
          sessionID: path.id,
          text: (body?.parts ?? []).map((p: V2TextPart) => p.text).join("\n"),
          delivery: "queue",
        })
      },
      status: async () => ({ data: statusBySession }),
    },
    v2Ready: hooksReady,
  }
  const hooks: any = await HcomPlugin({ client, $: Bun.$, v2Ready: hooksReady } as unknown as PluginInput)

  // The v1 transform reads user messages as { info, parts }. Sharing the part
  // objects lets its in-place text rewrite reach the request; parts it appends
  // (the bootstrap) go to the system prompt instead of the user message.
  async function transform(draft: V2Draft) {
    if (disposed) return
    const views = (draft.messages ?? []).map((m) => ({
      info: { id: m.id, role: m.role, sessionID: draft.sessionID },
      parts: [...m.content],
      added: m.content.length,
    }))
    await hooks["experimental.chat.messages.transform"]({ sessionID: draft.sessionID }, { messages: views })
    if (disposed) return
    for (const view of views) {
      for (const part of view.parts.slice(view.added)) draft.system.push({ type: "text", text: part.text })
    }
  }

  const registrations: V2Registration[] = []

  // `hcom opencode --agent/--model`, moved to env by the launcher: OpenCode 2's TUI
  // has no such flags. The agent becomes the server default. The model is switched
  // on each session's first prompt, before its turn runs: agent files override any
  // default model set here, since OpenCode applies them after this plugin's setup.
  const launchAgent = process.env.HCOM_OPENCODE_AGENT
  const launchModel = parseLaunchModel(process.env.HCOM_OPENCODE_MODEL)
  // One switch per session, shared by concurrent first prompts. A failed switch
  // cache entry is evicted so a later prompt can retry, while the current prompt
  // failure propagates (fail-closed, no silent fallback).
  const modelSwitches = new Map<string, Promise<unknown>>()
  function switchToLaunchModel(sessionID: string, model: V2Model) {
    let pending = modelSwitches.get(sessionID)
    if (!pending) {
      pending = ctx.session.switchModel({ sessionID, model })
      modelSwitches.set(sessionID, pending)
      pending.catch(() => modelSwitches.delete(sessionID))
    }
    return pending
  }
  async function onPrompt(draft: { sessionID: string }) {
    if (disposed) return
    if (launchModel) {
      await switchToLaunchModel(draft.sessionID, launchModel)
      if (disposed) return
    }
    await hooks["chat.message"]({ sessionID: draft.sessionID }, {})
  }

  const controller = new AbortController()

  try {
    if (launchAgent) registrations.push(await ctx.agent.transform((editor) => editor.default(launchAgent)))
    registrations.push(await ctx.session.hook("prompt", onPrompt))
    registrations.push(await ctx.session.hook("context", transform))
    registrations.push(await ctx.tool.hook("execute.before", (event) => {
      if (disposed) return
      return hooks["tool.execute.before"]({ tool: event.tool, sessionID: event.sessionID, callID: event.id }, { args: event.input })
    }))
    registrations.push(await ctx.session.hook("compaction", async (draft: V2Draft) => {
      if (disposed) return
      const output = { context: [] as string[] }
      await hooks["experimental.session.compacting"]({ sessionID: draft.sessionID }, output)
      if (disposed) return
      for (const text of output.context) draft.system.push({ type: "text", text })
    }))
    markHooksReady()
  } catch (e) {
    disposed = true
    markHooksReady()
    controller.abort()
    await hooks.dispose().catch(() => {})
    await Promise.allSettled(registrations.map((r) => Promise.resolve().then(() => r.dispose()))).catch(() => {})
    throw e
  }

  const consuming = (async () => {
    try {
      for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
        if (controller.signal.aborted || disposed) break
        const mapped: any = v1Event(event)
        if (!mapped) continue
        if (mapped.type === "session.status") statusBySession[mapped.properties.sessionID] = mapped.properties.status
        if (disposed) break
        await hooks.event({ event: mapped })
      }
    } catch (e) {
      if (!controller.signal.aborted && !disposed) log("ERROR", "plugin.event_subscription_failed", null, { error: String(e) })
    }
  })()

  return async () => {
    disposed = true
    controller.abort()
    await hooks.dispose()
    const results = await Promise.allSettled(registrations.map((r) => Promise.resolve().then(() => r.dispose())))
    const firstRejected = results.find((r) => r.status === "rejected") as PromiseRejectedResult | undefined
    if (firstRejected) throw firstRejected.reason
  }
}

// OpenCode 1 rejects a default export without `server`; pointing it at
// HcomPlugin keeps a single instance under both loaders.
export default {
  id: "hcom",
  server: HcomPlugin,
  setup: setupOpenCode2,
}
