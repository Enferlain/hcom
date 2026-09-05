---
engine:
  id: hcom-antigravity
  display-name: hcom Antigravity
  description: Local Antigravity execution supervised by hcom on a dedicated self-hosted runner
  experimental: true
  provider:
    name: google
  behaviors:
    manifest:
      files:
        - AGENTS.md
      path-prefixes:
        - .agents/plugins/gh-aw/
    network:
      defaults:
        - accounts.google.com
        - oauth2.googleapis.com
        - "*.googleapis.com"
    execution:
      command-name: hcom
      step-name: Execute Antigravity through hcom
      write-timestamp: true
    mcp:
      config-path: .agents/plugins/gh-aw/mcp_config.json
      config-adapter: |
        const fs = require("fs");
        const path = require("path");

        const requireEnv = name => {
          const value = process.env[name];
          if (!value) throw new Error(`${name} is required`);
          return value;
        };

        const outputPath = requireEnv("MCP_GATEWAY_OUTPUT");
        const workspace = requireEnv("GITHUB_WORKSPACE");
        const gatewayDomain = process.env.MCP_GATEWAY_DOMAIN || "host.docker.internal";
        const gatewayPort = requireEnv("MCP_GATEWAY_PORT");
        const gatewayURL = `http://${gatewayDomain}:${gatewayPort}`;
        const output = JSON.parse(fs.readFileSync(outputPath, "utf8"));
        const source = output.mcpServers;
        const servers = source && typeof source === "object" && !Array.isArray(source) ? source : {};
        const mcpServers = {};

        for (const [name, entry] of Object.entries(servers)) {
          if (!entry || typeof entry !== "object") continue;
          const transformed = { ...entry };
          if (typeof transformed.url === "string") {
            transformed.url = transformed.url.replace(
              /^http:\/\/[^/]+\/mcp\//,
              `${gatewayURL}/mcp/`,
            );
            transformed.serverUrl = transformed.url;
            delete transformed.url;
            delete transformed.type;
          }
          delete transformed.tools;
          mcpServers[name] = transformed;
        }

        const configPath = path.join(
          workspace,
          ".agents",
          "plugins",
          "gh-aw",
          "mcp_config.json",
        );
        fs.mkdirSync(path.dirname(configPath), { recursive: true });
        fs.writeFileSync(
          path.join(path.dirname(configPath), "plugin.json"),
          JSON.stringify({ name: "gh-aw" }, null, 2),
          { mode: 0o600 },
        );
        fs.writeFileSync(configPath, JSON.stringify({ mcpServers }, null, 2), { mode: 0o600 });
        fs.chmodSync(configPath, 0o600);
    harness-script: |
      const fs = require("fs");
      const os = require("os");
      const path = require("path");
      const { spawnSync } = require("child_process");

      const log = message => process.stderr.write(`[hcom-antigravity] ${message}\n`);
      const fail = message => {
        log(message);
        process.exitCode = 1;
      };
      const run = (command, args, options = {}) => spawnSync(command, args, {
        encoding: "utf8",
        env: options.env || process.env,
        stdio: options.stdio || "pipe",
      });
      const check = (result, action) => {
        if (result.error) throw result.error;
        if (result.status !== 0) {
          const detail = (result.stderr || result.stdout || "").trim();
          throw new Error(`${action} failed with exit code ${result.status ?? "unknown"}${detail ? `: ${detail}` : ""}`);
        }
        return result;
      };

      let isolatedHcomDir;
      let caller;
      try {
        const [hcomCommand] = process.argv.slice(2);
        if (!hcomCommand) throw new Error("hcom command was not supplied by gh-aw");
        check(run(hcomCommand, ["--version"]), "hcom prerequisite check");
        check(run("agy", ["--help"]), "Antigravity prerequisite check");

        const home = process.env.HOME;
        const workspace = process.env.GITHUB_WORKSPACE;
        const promptPath = process.env.GH_AW_PROMPT;
        if (!home || !workspace || !promptPath) {
          throw new Error("HOME, GITHUB_WORKSPACE, and GH_AW_PROMPT are required");
        }
        const marker = path.join(home, ".config", "hcom", "github-runner");
        if (fs.readFileSync(marker, "utf8").trim() !== "dedicated") {
          throw new Error(`dedicated runner marker must contain 'dedicated': ${marker}`);
        }

        const sourceHcomDir = process.env.HCOM_DIR || path.join(home, ".hcom");
        const sourceScripts = path.join(sourceHcomDir, "scripts");
        if (!fs.statSync(path.join(sourceScripts, "agy.sh")).isFile()) {
          throw new Error(`required user workflow is missing: ${path.join(sourceScripts, "agy.sh")}`);
        }
        isolatedHcomDir = fs.mkdtempSync(path.join(process.env.RUNNER_TEMP || os.tmpdir(), "hcom-kuro-"));
        fs.symlinkSync(sourceScripts, path.join(isolatedHcomDir, "scripts"), "dir");
        const childEnv = { ...process.env, HCOM_DIR: isolatedHcomDir };

        const started = check(run(hcomCommand, ["start"], { env: childEnv }), "coordinator identity creation");
        const matches = [...started.stdout.matchAll(/^\[hcom:([^\]]+)\]$/gm)];
        caller = matches.at(-1)?.[1];
        if (!caller) throw new Error("hcom start did not return a coordinator identity");

        const prompt = fs.readFileSync(promptPath, "utf8");
        const result = run(hcomCommand, [
          "run", "agy",
          "--name", caller,
          "--dir", workspace,
          "--model", "gemini-3.8-flash-high",
          "--heartbeat", "15",
          "--timeout", "900",
          "--", prompt,
        ], { env: childEnv, stdio: "inherit" });
        if (result.error) throw result.error;
        if (result.status !== 0) {
          throw new Error(`hcom run agy failed with exit code ${result.status ?? "unknown"}`);
        }
      } catch (error) {
        fail(error instanceof Error ? error.message : String(error));
      } finally {
        if (isolatedHcomDir && caller) {
          run("hcom", ["stop", caller], { env: { ...process.env, HCOM_DIR: isolatedHcomDir } });
        }
        if (isolatedHcomDir) fs.rmSync(isolatedHcomDir, { recursive: true, force: true });
      }
---

<!--
Inactive provider-specific prototype retained for design reference. Do not
import it into production workflows. It intentionally has no provider API
secret: the dedicated runner's Antigravity installation owns authentication.
-->
