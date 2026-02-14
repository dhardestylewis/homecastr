# Properlytic

**Property-Level Home Price Forecasting & Risk Intelligence**

![Properlytic Banner](https://img.shields.io/badge/Status-Active-success)
[![Deployed on Vercel](https://img.shields.io/badge/Deployed%20on-Vercel-black?style=for-the-badge&logo=vercel)](https://houston-price-prediction.vercel.app)

Properlytic is an API-first forecasting platform that brings institutional-grade risk intelligence to the residential real estate market. Instead of a single point estimate, we provide time-stamped, probabilistic forecasts with confidence bands and scenario views, empowering buyers, sellers, and homeowners to make data-driven decisions.

## 🚀 The Vision

Real estate is the largest financial decision for most, yet consumers rely on backward-looking comps and broad indices. Properlytic changes this by offering a forward-looking view that quantifies risk and makes uncertainty explicit at the property level.

Our long-term vision is to become the national decision layer for residential real estate—delivering forecasts, risk ranges, and alerts that outperform today’s portals and spreadsheets.

## 💡 Key Features

*   **Probabilistic Forecasts:** 256 Monte Carlo scenario paths per property.
*   **Property-Level Granularity:** Insights specific to the building, unit, and micro-location.
*   **Scenario Analysis:** "What if" views for rates, inventory, and policy shifts.
*   **API-First Design:** Built for integration into underwriting, portfolio monitoring, and advisory workflows.
*   **Open Evaluation:** Transparent model monitoring (coverage, drift, segment accuracy).

## 🛠️ Technology

Properlytic ingests public records, retrains frequently, and delivers fast, interpretable forecasts at parcel scale.

*   **Live Demo:** [houston-price-prediction.vercel.app](https://houston-price-prediction.vercel.app) (Houston Pilot)
*   **Wedge Market:** New York City (High heterogeneity & regime shifts)
*   **Stack:** Python, Next.js, GCP (Compute/Data)

## 👥 Team

*   **Daniel Hardesty Lewis** (Founder/CEO-CTO): Leads modeling, data engineering, and product.

## 📈 Market & Validation

We are targeting the ~$13T U.S. residential market. Our go-to-market strategy involves a dual track:
1.  **Public Distribution:** Free consumer layer to drive adoption and "standard-setting."
2.  **Institutional Pilots:** Paid API access for lenders, servicers, and investors requiring auditable, scenario-level forecasts.

## 🚀 Deployment (Vercel)

For the **Homecastr Live Agent** (Tavus) to work in production:

1. In Vercel: **Project → Settings → Environment Variables**
2. Add (for **Production** and **Preview** if you use preview URLs):
   * `TAVUS_API_KEY` — your Tavus API key
   * `TAVUS_PERSONA_ID` — optional, Tavus persona id
   * `TAVUS_REPLICA_ID` — optional, Tavus replica id
3. **Redeploy** after saving (Deployments → … → Redeploy).

If the button shows "Homecastr not configured", the toast will include a reminder to add these in Vercel. Supabase vars (`NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`) are also required for the map.

## 🔗 Links

*   **Live App:** [houston-price-prediction.vercel.app](https://houston-price-prediction.vercel.app)
*   **GitHub:** [dhardestylewis/v0-properlytic-8v](https://github.com/dhardestylewis/v0-properlytic-8v)

---
*Submitted for the Columbia Venture Competition 2026*
