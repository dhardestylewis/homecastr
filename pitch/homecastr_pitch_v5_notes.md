# Homecastr Pitch Deck V5 — Revision Notes

> **Source:** Entity backtest analysis (2026-02-22). These notes should be incorporated into the next pitch deck revision (`homecastr_pitch_v5.tex`).

---

## 1. Address the Anti-Correlation Head-On

The model is anti-correlated with entity purchases (ρ ≈ -0.1 to -0.15). Entities buy what the model ranks lowest, and those purchases outperform. A VC doing diligence WILL find this.

**Recommended framing:** The model forecasts *passive appreciation* — where values are heading without intervention. Entities create value through renovation, rezoning, and operational improvement. These are complementary signals, not competing ones. The model answers "which neighborhoods are rising?" while entities answer "which properties can I force-appreciate?"

**Suggested slide addition or revision:** A slide explicitly positioning the model as a *market intelligence layer* that complements entity deal-flow, not replaces it.

---

## 2. Fix the "1,700 Subs at $2M ARR" Claim (Slide 8)

Backtest data shows ~471 boutique+ entities (≥50 buys) in Harris County. Across 5 metros = ~2,500–3,000 entities. At 5% conversion = 125–150 subs, not 1,700.

**Options:**
- Raise price point to $499–999/mo for operators (fewer subs, same ARR)
- Include individual investors in the 1,700 count (the "8M+" from TAM slide) at $99/mo
- Be explicit: "1,700 subs = 150 operators + 1,550 individual investors"

---

## 3. Lead with the Conversational Data Moat, Not the Model

The model architecture (diffusion on public data) is replicable. The real moat is:
- **Conversational intent data** from Tavus/voice agent — nobody else has this
- **100K+ consumers asking about specific properties** reveals demand signals no API-only competitor captures
- This is the Zillow playbook: Zestimate was the traffic magnet, leads were the product

**Recommended change:** Rework the flywheel slide (slide 10) to be THE centerpiece of the pitch. Move the model/accuracy slides earlier as proof-of-concept, then land on "but the real asset is the data we collect."

**Suggested narrative arc:**
1. The gap exists (slide 2 ✅ already good)
2. We built the model to fill it (slides 3-4)
3. Free forecasts attract consumers (slide 8 free tier)
4. **Conversations reveal intent → THAT is the moat** (expanded flywheel)
5. Sell intent signals to operators/lenders (revenue model)

---

## 4. Clarify Consumer vs. Operator GTM Tension

The pitch currently tries to serve both:
- **Consumer:** Free, SEO-driven, volume play
- **Operator:** $99/mo, sales-driven, relationship play

These are very different GTM motions. MetaProp will ask "which one are you executing first?"

**Recommended answer:** Consumer-first for traffic/data, then monetize via operator tier. Make this sequencing explicit.

---

## 5. Add Entity Backtest Evidence

We now have concrete data to cite:
- **138 ICP entities** where model demonstrably outperforms (positive returns, ≥20 buys)
- **6 entities** with cross-year consistency (model wins in ≥3 of 4 origins)
- **55 Tier 1 whales** (≥100 buys) in the outperform group
- **471 boutique+ entities** in Harris County alone = addressable market proof

**Suggested slide:** "We backtested against 4,667 named entities in Harris County. The model outperforms 29% of ICP investors — including entities managing 100–1,000+ properties."

---

## 6. Portfolio Page as Initial GTM Hook

The entity backtest identifies 138 ICP entities to build personalized "what-if" portfolio pages for. These pages show:
- Their actual purchases mapped with model scores
- Neighborhood forecast overlays
- "What the model would have picked" counterfactual

Send as cold email: *"We analyzed your 303 Houston purchases. Here's what our model sees."*

This is the bridge between "we have a model" and "people are paying."

---

## 7. Key Entities for Outreach (from backtest)

### Consistent across all years (strongest proof):
- **YAMASA CO LTD** — 850 buys, +8.3pp advantage, 4/4 origins ✅
- **SHEL INVESTMENTS LLC** — 27 buys, +6.2pp, 3/3 origins ✅

### Large portfolios, model outperforms (overall):
- Bridgeland Development (1,370 buys), Zillow Homes Trust (1,151), FKH SFR Propco entities (2,197 combined), Opendoor entities (836 combined), Progress Residential (695 combined), Resicap (420 combined)

### Best cold outreach candidates:
- Open House Texas Realty (local, reachable)
- Sunrise Property Partners (104 buys, 4/4 win rate)
- Candlewood Homes (310 buys, 4/4 win rate)
- Fairport Ventures (120 buys, local fund)
