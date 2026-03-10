#!/usr/bin/env node
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const mcp_js_1 = require("@modelcontextprotocol/sdk/server/mcp.js");
const stdio_js_1 = require("@modelcontextprotocol/sdk/server/stdio.js");
const zod_1 = require("zod");
// ── Configuration ──
const API_BASE = process.env.HOMECASTR_API_URL || "https://www.homecastr.com";
const API_KEY = process.env.HOMECASTR_API_KEY || "hc_demo_public_readonly";
// ── HTTP helper ──
async function apiFetch(path, params) {
    const url = new URL(path, API_BASE);
    for (const [k, v] of Object.entries(params)) {
        if (v)
            url.searchParams.set(k, v);
    }
    const res = await fetch(url.toString(), {
        headers: { "x-api-key": API_KEY },
    });
    if (!res.ok) {
        const body = await res.text();
        throw new Error(`Homecastr API ${res.status}: ${body}`);
    }
    return res.json();
}
// ── MCP Server ──
const server = new mcp_js_1.McpServer({
    name: "homecastr",
    version: "1.0.0",
});
// ── Tool: forecast_by_address ──
server.tool("forecast_by_address", "Get probabilistic home value forecasts for any US street address. Returns current value, P10/P50/P90 forecast bands, appreciation percentage, reliability score, and fan chart data across 1-5 year horizons.", {
    address: zod_1.z.string().describe("US street address, e.g. '123 Main St Houston TX'"),
    year: zod_1.z.number().optional().describe("Target forecast year (2026-2030, default: 2030)"),
}, async ({ address, year }) => {
    const params = { address };
    if (year)
        params.year = String(year);
    const data = await apiFetch("/api/v1/forecast", params);
    return {
        content: [
            {
                type: "text",
                text: JSON.stringify(data, null, 2),
            },
        ],
    };
});
// ── Tool: forecast_by_h3 ──
server.tool("forecast_by_h3_cell", "Get neighborhood-level home value forecasts by H3 hex cell ID. For use when you already know the H3 cell index at resolution 8.", {
    h3_id: zod_1.z.string().describe("H3 cell ID at resolution 8, e.g. '882a100c65fffff'"),
    year: zod_1.z.number().optional().describe("Target forecast year (default: 2026)"),
}, async ({ h3_id, year }) => {
    const params = { h3_id };
    if (year)
        params.year = String(year);
    const data = await apiFetch("/api/v1/forecast/hex", params);
    return {
        content: [
            {
                type: "text",
                text: JSON.stringify(data, null, 2),
            },
        ],
    };
});
// ── Tool: forecast_by_parcel ──
server.tool("forecast_by_parcel", "Get lot-level home value forecasts by county tax parcel account ID. For integrations with county appraisal district data.", {
    acct: zod_1.z.string().describe("County tax account / parcel ID"),
}, async ({ acct }) => {
    const data = await apiFetch("/api/v1/forecast/lot", { acct });
    return {
        content: [
            {
                type: "text",
                text: JSON.stringify(data, null, 2),
            },
        ],
    };
});
// ── Start ──
async function main() {
    const transport = new stdio_js_1.StdioServerTransport();
    await server.connect(transport);
    console.error("Homecastr MCP server running on stdio");
}
main().catch((err) => {
    console.error("Fatal:", err);
    process.exit(1);
});
//# sourceMappingURL=index.js.map