# Homecastr MCP Server

Probabilistic home value forecasts for AI agents. Works with Claude, ChatGPT, Cursor, and any MCP-compatible client.

## Tools

| Tool | Description |
|------|-------------|
| `forecast_by_address` | Forecast any US home by street address |
| `forecast_by_h3_cell` | Forecast by H3 hex cell ID (res 8) |
| `forecast_by_parcel` | Forecast by county tax parcel ID |

## Quick Start

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "homecastr": {
      "command": "npx",
      "args": ["-y", "@homecastr/mcp-server"],
      "env": {
        "HOMECASTR_API_KEY": "hc_demo_public_readonly"
      }
    }
  }
}
```

### Local Development

```bash
cd mcp-server
npm install
npm run dev
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HOMECASTR_API_KEY` | `hc_demo_public_readonly` | API key (get one at homecastr.com/api-docs) |
| `HOMECASTR_API_URL` | `https://www.homecastr.com` | API base URL |

## Example

Ask Claude: *"What's the 5-year price forecast for 742 Evergreen Terrace, Springfield, IL?"*

Claude will call `forecast_by_address` and return:
- Current assessed value
- P10 / P50 / P90 forecast bands
- Appreciation percentage
- Model reliability score
- Fan chart data for visualization

## License

MIT
