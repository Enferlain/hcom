// Type-only check: Kilo loads the same plugin as OpenCode 1, so the plugin must
// fit both apps' own `Plugin` types (hook shapes and the client calls it makes).
import type { Plugin as OpenCodePlugin } from "@opencode-ai/plugin"
import type { Plugin as KiloPlugin } from "@kilocode/plugin"
import { HcomPlugin } from "../src/opencode_plugin/hcom.ts"

export const openCode: OpenCodePlugin = HcomPlugin
export const kilo: KiloPlugin = HcomPlugin
