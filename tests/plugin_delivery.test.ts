import { afterEach, mock, test } from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const logDir = mkdtempSync(join(tmpdir(), "hcom-plugin-delivery-"));
const previousDir = process.env.HCOM_DIR;
const previousLaunched = process.env.HCOM_LAUNCHED;
process.env.HCOM_DIR = logDir;
process.env.HCOM_LAUNCHED = "1";
let calls: string[][] = [];
let pending: any[] = [];
mock.module("node:child_process", {
	namedExports: {
		spawn(_command: string, args: string[]) {
			calls.push(args);
			const child = Object.assign(new EventEmitter(), {
				stdout: Object.assign(new EventEmitter(), { setEncoding() {} }),
				stderr: Object.assign(new EventEmitter(), { setEncoding() {} }), kill() {},
			});
			queueMicrotask(() => {
				const reply = args[0].endsWith("-start")
					? { name: "test", session_id: args[args.indexOf("--session-id") + 1] }
					: args.includes("--ack") ? { acked_to: Number(args[args.indexOf("--up-to") + 1]) }
					: args[0].endsWith("-read") ? { messages: pending, has_more: false }
					: args[0].endsWith("-approve") ? { approve: args[args.indexOf("--command") + 1].startsWith("hcom ") } : { ok: true };
				if (args.includes("--ack")) pending = [];
				child.stdout.emit("data", JSON.stringify(reply));
				child.emit("close", 0, null);
			});
			return child;
		},
	},
});
const { default: omp } = await import("../src/omp_plugin/hcom.ts");
const { default: pi } = await import("../src/pi_plugin/hcom.ts");
const runtimes: ReturnType<typeof runtime>[] = [];
function runtime(factory: any): {
	sent: any[];
	ctx: (kind?: string, session?: string, idle?: boolean) => any;
	emit: (name: string, event?: any, context?: any) => Promise<any>;
} {
	const handlers = new Map<string, any>();
	const sent: any[] = [];
	factory({ on: (name: string, fn: any) => handlers.set(name, fn), sendUserMessage: (text: string, options: any) => sent.push({ text, options }) });
	const ctx = (kind = "main", session = "parent", idle = false): any => ({
		agent: { kind, id: kind === "main" ? "Main" : "advisor", depth: 0 },
		cwd: logDir, sessionManager: { getSessionId: () => session, getSessionFile: () => undefined },
		isIdle: () => idle, hasPendingMessages: () => true,
	});
	const r = { sent, ctx, emit: async (name: string, event: any = {}, context = ctx()) => handlers.get(name)?.(event, context) };
	runtimes.push(r);
	return r;
}
async function drain() { for (let i = 0; i < 4; i++) await new Promise<void>(resolve => setImmediate(resolve)); }
afterEach(async () => {
	for (const r of runtimes.splice(0)) await r.emit("session_shutdown", { reason: "quit" });
	await drain(); calls = []; pending = [];
});

for (const [tool, factory, mode] of [["pi", pi, "steer"], ["omp", omp, "aside"]] as const) {
	test(`${tool} queues busy mail as ${mode} and ACKs only after admission`, async () => {
		const r = runtime(factory);
		await r.emit("session_start");
		pending = [{ event_id: 42, from: "sender", message: "BUSY-MAIL", intent: "inform" }];
		await r.emit("tool_result");
		assert.deepEqual(r.sent[0].options, { deliverAs: mode });
		assert.equal((calls.filter(c => c[0] === `${tool}-read` && c.includes("--ack"))).length, 0);
		const receipt = { message: { role: "user", content: r.sent[0].text } };
		if (tool === "omp") {
			await r.emit("message_end", receipt, r.ctx("sub"));
			await drain();
			assert.equal((calls.filter(c => c[0] === "omp-read" && c.includes("--ack"))).length, 0);
		}
		await r.emit("message_end", receipt);
		await drain();
		assert.equal((calls.filter(c => c[0] === `${tool}-read` && c.includes("--ack"))).length, 1);
	});
}

test("OMP depth-zero advisor and child callbacks cannot bind, stop, report, or consume parent mail", async () => {
	const parent = runtime(omp), child = runtime(omp);
	await parent.emit("session_start");
	const before = calls.length;
	pending = [{ event_id: 42, from: "sender", message: "PARENT-ONLY" }];
	for (const event of ["session_start", "session_switch", "session_branch", "agent_start", "input", "before_agent_start", "tool_call", "tool_result", "turn_end", "agent_end", "session_shutdown"]) {
		await child.emit(event, { text: "<hcom>", toolName: "bash", input: {} }, child.ctx("sub", "child"));
	}
	for (const event of ["tool_call", "tool_result"]) await parent.emit(event, { toolName: "read", input: {} }, parent.ctx("sub"));
	await drain();
	assert.equal((calls).length, before);
	assert.equal((child.sent).length, 0);
	assert.equal((parent.sent).length, 0);
	await parent.emit("tool_result");
	assert.ok((parent.sent[0].text).includes("PARENT-ONLY"));
});

test("OMP new/switch/branch rebinds and shutdown stops only the current parent", async () => {
	const r = runtime(omp);
	await r.emit("session_start");
	await r.emit("session_switch", {}, r.ctx("main", "new"));
	await r.emit("session_switch", {}, r.ctx());
	await r.emit("session_branch", {}, r.ctx("main", "branch"));
	assert.deepEqual(calls.filter(c => c[0] === "omp-start").map(c => c[c.indexOf("--session-id") + 1]), ["parent", "new", "parent", "branch"]);
	assert.equal((calls.filter(c => c[0] === "omp-stop")).length, 0);
	await r.emit("session_shutdown", {}, r.ctx("sub", "child"));
	assert.equal((calls.filter(c => c[0] === "omp-stop")).length, 0);
	await r.emit("session_shutdown");
	assert.deepEqual(calls.filter(c => c[0] === "omp-stop"), [["omp-stop", "--name", "test", "--reason", "shutdown", "--soft"]]);
});

const { HcomPlugin: opencode1, default: opencodeModule } = await import("../src/opencode_plugin/hcom.ts");
const acks = () => calls.filter(c => c[0] === "opencode-read" && c.includes("--ack")).map(c => Number(c[c.indexOf("--up-to") + 1]));
const statuses = () => calls.filter(c => c[0] === "opencode-status" && c.includes("--status")).map(c => c[c.indexOf("--status") + 1]);
async function settle(check: () => boolean) {
	for (let i = 0; i < 200 && !check(); i++) await new Promise<void>(resolve => setImmediate(resolve));
	assert.ok(check(), "condition not reached");
}
const openCodeCleanups: (() => Promise<void>)[] = [];
afterEach(async () => { for (const cleanup of openCodeCleanups.splice(0)) await cleanup(); });
async function openCode1(promptResult: any = {}) {
	const prompts: any[] = [];
	const approvals: any[] = [];
	const client = {
		session: { promptAsync: async (req: any) => (prompts.push(req), promptResult), status: async () => ({ data: {} }) },
		postSessionIdPermissionsPermissionId: async (req: any) => (approvals.push(req), {}),
	};
	const hooks: any = await opencode1({ client } as any);
	openCodeCleanups.push(() => hooks.dispose());
	const event = (type: string, properties: any) => hooks.event({ event: { type, properties } });
	const userMessage = (sessionID: string, text: string) => ({ info: { id: `m-${sessionID}`, sessionID, role: "user" }, parts: [{ id: `p-${sessionID}`, type: "text", text }] });
	await event("session.created", { info: { id: "parent" } });
	return { hooks, prompts, approvals, event, userMessage };
}

test("OpenCode acks mail only when its own session's request carries it", async () => {
	const oc = await openCode1();
	pending = [{ event_id: 5, from: "luna", message: "PARENT-MAIL" }];
	await oc.event("session.status", { sessionID: "parent", status: { type: "idle" } });
	await settle(() => oc.prompts.length === 1);
	const text = oc.prompts[0].body.parts[0].text;
	assert.ok(text.includes("PARENT-MAIL"));
	// A subagent's request neither acks the parent's mail nor gets the bootstrap.
	await oc.hooks["experimental.chat.messages.transform"]({}, { messages: [oc.userMessage("child", text)] });
	await drain();
	assert.deepEqual(acks(), []);
	await oc.hooks["experimental.chat.messages.transform"]({}, { messages: [oc.userMessage("parent", text)] });
	await settle(() => acks().length === 1);
	assert.deepEqual(acks(), [5]);
});

test("OpenCode keeps mail unread when promptAsync resolves with an error", async () => {
	const oc = await openCode1({ error: { name: "ProviderAuthError" } });
	pending = [{ event_id: 6, from: "luna", message: "REFUSED" }];
	await oc.event("session.status", { sessionID: "parent", status: { type: "idle" } });
	await settle(() => oc.prompts.length === 1);
	await drain();
	assert.deepEqual(acks(), []);
	// Released at once, so the next idle edge delivers again.
	await oc.event("session.status", { sessionID: "parent", status: { type: "idle" } });
	await settle(() => oc.prompts.length === 2);
});

test("OpenCode redelivers a dropped prompt once the grace period passes, a bounded number of times", async () => {
	mock.timers.enable({ apis: ["setInterval", "Date"], now: 1_000_000 });
	try {
		const oc = await openCode1();
		const idle = () => oc.event("session.status", { sessionID: "parent", status: { type: "idle" } });
		const advance = async (ms: number) => { for (let t = 0; t < ms; t += 5_000) { mock.timers.tick(5_000); await drain(); } };
		pending = [{ event_id: 10, from: "luna", message: "DROPPED" }];
		await idle();
		await settle(() => oc.prompts.length === 1);
		// OpenCode drops the prompt; its last idle edge lands inside the grace period.
		await idle();
		await drain();
		assert.equal(oc.prompts.length, 1);
		await advance(15_000);
		await settle(() => oc.prompts.length === 2);
		await advance(120_000);
		assert.equal(oc.prompts.length, 4);
		assert.deepEqual(acks(), []);
	} finally {
		mock.timers.reset();
	}
});

test("OpenCode stays blocked until every pending permission is answered", async () => {
	const oc = await openCode1();
	await oc.event("permission.asked", { id: "per_1", sessionID: "parent", permission: "bash" });
	await oc.event("permission.asked", { id: "per_2", sessionID: "parent", permission: "edit" });
	await oc.event("permission.replied", { sessionID: "parent", requestID: "per_1", reply: "once" });
	pending = [{ event_id: 7, from: "luna", message: "WAIT" }];
	await oc.event("permission.asked", { id: "per_3", sessionID: "other", permission: "bash" });
	await drain();
	assert.equal(statuses().at(-1), "blocked");
	assert.equal(oc.prompts.length, 0);
	await oc.event("permission.replied", { sessionID: "parent", requestID: "per_2", reply: "once" });
	await settle(() => oc.prompts.length === 1 && statuses().at(-1) === "listening");
});

test("OpenCode answers hcom's own bash requests once and leaves others to the user", async () => {
	const oc = await openCode1();
	await oc.event("permission.asked", { id: "per_h", sessionID: "parent", permission: "bash", metadata: { command: "hcom send @luna -- hi" } });
	assert.deepEqual(oc.approvals.map(a => [a.path.permissionID, a.body.response]), [["per_h", "once"]]);
	assert.equal(statuses().includes("blocked"), false);
	await oc.event("permission.asked", { id: "per_r", sessionID: "parent", permission: "bash", metadata: { command: "rm -rf build" } });
	assert.equal(oc.approvals.length, 1);
	assert.equal(statuses().at(-1), "blocked");
});

test("OpenCode fills a typed bodyless wake and acks it in the same request", async () => {
	const oc = await openCode1();
	pending = [{ event_id: 8, from: "luna", message: "BODY" }];
	const messages = [oc.userMessage("parent", "<hcom>[request #8] luna -> test</hcom>")];
	await oc.hooks["experimental.chat.messages.transform"]({}, { messages });
	assert.ok(messages[0].parts[0].text.includes("luna -> test: BODY"));
	await settle(() => acks().length === 1);
	// Request-local edits: a later request gets the same text back, not a new read.
	const again = [oc.userMessage("parent", "<hcom>[request #8] luna -> test</hcom>")];
	await oc.hooks["experimental.chat.messages.transform"]({}, { messages: again });
	assert.equal(again[0].parts[0].text, messages[0].parts[0].text);
	assert.equal(calls.filter(c => c[0] === "opencode-read" && !c.includes("--ack")).length, 1);
});

test("OpenCode 2 steers mail, acks on inbox delivery, and fills a wake in the prompt hook", async () => {
	const hooks: Record<string, any> = {};
	const prompts: any[] = [];
	const queue: any[] = [];
	let wake: (() => void) | null = null;
	const push = (type: string, data: any) => { queue.push({ type, data }); wake?.(); };
	const registration = { dispose() {} };
	const ctx: any = {
		agent: { transform: async () => registration },
		session: {
			hook: async (name: string, cb: any) => ((hooks[name] = cb), registration),
			prompt: async (input: any) => (prompts.push(input), { id: input.id }),
			switchModel: async () => {},
		},
		tool: { hook: async () => registration },
		event: {
			async *subscribe({ signal }: { signal: AbortSignal }) {
				while (!signal.aborted) {
					if (queue.length) { yield queue.shift(); continue; }
					await new Promise<void>(resolve => { wake = resolve; signal.addEventListener("abort", () => resolve(), { once: true }); });
				}
			},
		},
	};
	const cleanup = await opencodeModule.setup(ctx);
	openCodeCleanups.push(cleanup);
	push("session.created", { sessionID: "s2" });
	await settle(() => calls.some(c => c[0] === "opencode-start"));
	pending = [{ event_id: 9, from: "luna", message: "V2-MAIL" }];
	push("session.execution.succeeded", { sessionID: "s2" });
	await settle(() => prompts.length === 1);
	assert.equal(prompts[0].delivery, "steer");
	assert.ok(prompts[0].id.startsWith("msg_"));
	push("session.inbox.delivered", { sessionID: "s2", inboxID: "msg_someone_else" });
	await drain();
	assert.deepEqual(acks(), []);
	push("session.inbox.delivered", { sessionID: "s2", inboxID: prompts[0].id });
	await settle(() => acks().length === 1);
	pending = [{ event_id: 10, from: "luna", message: "WAKE-BODY" }];
	const draft = { sessionID: "s2", messageID: "msg_typed", prompt: { text: "<hcom>" } };
	await hooks.prompt(draft);
	assert.ok(draft.prompt.text.includes("WAKE-BODY"));
	// Acked once the filled prompt is delivered, not when it is filled.
	await drain();
	assert.equal(acks().length, 1);
	push("session.inbox.delivered", { sessionID: "s2", inboxID: "msg_typed" });
	await settle(() => acks().length === 2);
});

process.on("exit", () => {
	if (previousDir === undefined) delete process.env.HCOM_DIR; else process.env.HCOM_DIR = previousDir;
	if (previousLaunched === undefined) delete process.env.HCOM_LAUNCHED; else process.env.HCOM_LAUNCHED = previousLaunched;
	rmSync(logDir, { recursive: true, force: true });
});
