# Homecastr

**Probabilistic home value forecasts for any US address.**

Homecastr delivers parcel-level, forward-looking price forecasts with confidence bands — not backward-looking comps. Powered by a machine learning model trained on millions of property records.

🌐 **[homecastr.com](https://www.homecastr.com)** · 📄 **[API Docs](https://www.homecastr.com/api-docs)** · 🛒 **[RapidAPI](https://rapidapi.com/dhardestylewis/api/homecastr-home-price-forecast-api)**

---

## What You Get

- **P10 / P50 / P90 forecast bands** across 1–5 year horizons
- **Current assessed value** and appreciation projections
- **Model reliability score** per address
- **Fan chart data** for visualization
- **H3 neighborhood cell** mapping

## Quick Start

### REST API

```bash
curl -H "x-api-key: hc_demo_public_readonly" \
  "https://www.homecastr.com/api/v1/forecast?address=123+Main+St+Houston+TX"
```

### MCP Server (Claude, Cursor, Windsurf)

Connect your AI agent to Homecastr — no installation required:

```json
{
  "mcpServers": {
    "homecastr": {
      "url": "https://www.homecastr.com/api/mcp/mcp"
    }
  }
}
```

Then ask: *"What's the 5-year price forecast for 742 Evergreen Terrace, Springfield, IL?"*

### RapidAPI

Subscribe at [RapidAPI Hub](https://rapidapi.com/dhardestylewis/api/homecastr-home-price-forecast-api) — free tier available.

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/v1/forecast?address=...` | Forecast by street address |
| `GET /api/v1/forecast/hex?h3_id=...` | Forecast by H3 cell (res 8) |
| `GET /api/v1/forecast/lot?acct=...` | Forecast by tax parcel ID |
| `GET /api/ping` | Health check |

## MCP Tools

| Tool | Description |
|---|---|
| `forecast_by_address` | Forecast any US home by street address |
| `forecast_by_h3_cell` | Forecast by H3 hex cell ID |
| `forecast_by_parcel` | Forecast by county tax parcel ID |

## Technology

- **Model**: Probabilistic ML with 256 Monte Carlo paths per property
- **Frontend**: Next.js, React, TypeScript
- **Deployment**: Vercel
- **Data**: Public records, census, macro indicators

## Team

**Daniel Hardesty Lewis** — Founder/CEO-CTO

## License

MIT
