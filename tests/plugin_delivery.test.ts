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
					: args[0].endsWith("-read") ? { messages: pending, has_more: false } : { ok: true };
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

process.on("exit", () => {
	if (previousDir === undefined) delete process.env.HCOM_DIR; else process.env.HCOM_DIR = previousDir;
	if (previousLaunched === undefined) delete process.env.HCOM_LAUNCHED; else process.env.HCOM_LAUNCHED = previousLaunched;
	rmSync(logDir, { recursive: true, force: true });
});
