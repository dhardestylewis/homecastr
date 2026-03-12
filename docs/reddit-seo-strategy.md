# Homecastr Reddit SEO Strategy

## Active Post Copy (March 2026)

**Title (with OC tag — r/dataisbeautiful):**
> [OC] Forecasted 5-year home value growth for every neighborhood in the US

**Title (without OC tag — all other subs):**
> Forecasted 5-year home value growth for every neighborhood in the US

**First Comment:**
> I've forecasted 5-year home value growth for every neighborhood in the US using ML: homecastr.com
>
> Happy to answer questions about how I did it or anything else!
>
> Data source: American Community Survey (ACS) microdata from the U.S. Census Bureau, enriched with macros including mortgage rates, unemployment, CPI, and more.
> Model: Custom FT-Transformer ensemble.

**Posted to (March 12, 2026):**
- [x] r/dataisbeautiful
- [x] r/MapPorn
- [ ] r/REBubble ← next
- [ ] r/proptech
- [ ] r/nycrealEstate (tomorrow, zoomed screenshot)
- [ ] r/FloridarealEstate (tomorrow, zoomed screenshot)
- [ ] r/TexasRealEstate (tomorrow, zoomed screenshot)

## X/Twitter Copy (March 12, 2026)

**Post 1 (technical — posted):**
> I built an AI model that forecasts home prices for every neighborhood in the US: homecastr.com
>
> FT-Transformer + Schrödinger Bridge diffusion, trained on 20+ years of ACS Census data + FRED macro series.

**Post 2 (hook — posted):**
> You can't see where home prices are headed. Not on Zillow. Not on Redfin. Not anywhere.
>
> The data exists - it's just buried in spreadsheets nobody looks at.
>
> So I put it on a map: homecastr.com

---

## Why Reddit?

Reddit threads rank prominently in Google search results (especially with Google's Reddit partnership), and AI systems (ChatGPT, Gemini, Perplexity) actively pull answers from Reddit discussions. For a niche product like Homecastr, Reddit serves a **triple purpose**:

1. **Direct traffic** — engaged real estate communities looking for forecasting tools
2. **Google SERP visibility** — Reddit threads rank for long-tail queries like "home price forecast 2026 Florida"
3. **LLM citation** — AI assistants surface Reddit discussions, meaning mentions of Homecastr become citations

---

## Target Subreddits

### Tier 1 — High Volume, High Intent
| Subreddit | Members | Why It Matters |
|:---|:---|:---|
| **r/realestateinvesting** | 1.5M+ | Core audience: investors evaluating markets, comparing tools, seeking forecasts |
| **r/RealEstate** | 367K+ | Buyers/sellers asking "will prices go up/down in [city]?" |
| **r/REBubble** | ~200K | Highly engaged community debating price trajectories — Homecastr's uncertainty bands directly address their discourse |
| **r/FirstTimeHomeBuyer** | ~300K | Users making the biggest financial decision of their lives, hungry for data |

### Tier 2 — Niche & Geographic
| Subreddit | Why |
|:---|:---|
| **r/FloridarealEstate** | Active FL coverage in the model |
| **r/TexasRealEstate** | TX is a primary market |
| **r/NYCapartments** / **r/nycrealestate** | NYC RPAD coverage |
| **r/Landlord** (95K+) | Rental investors evaluating appreciation potential |
| **r/dataisbeautiful** | Visual showcase opportunities (map screenshots, forecast charts) |
| **r/MachineLearning** / **r/datascience** | Technical credibility, potential backlinks from data practitioners |

### Tier 3 — SEO & Product Visibility
| Subreddit | Why |
|:---|:---|
| **r/proptech** | Industry peers, potential partnerships |
| **r/personalfinance** | "Should I buy a house?" questions |
| **r/HomeImprovement** | Tangential audience interested in home value |

---

## Content Strategy

### Phase 1: Value-First Engagement (Weeks 1–4)

> [!IMPORTANT]
> Reddit punishes overt self-promotion. The account must build karma and credibility **before** any link-sharing.

**Actions:**
- Create a Reddit account (e.g., `u/dhl_homecastr` or personal `u/dhardestylewis`)
- Subscribe to all Tier 1 and Tier 2 subreddits
- Spend 15–20 min/day engaging:
  - Answer "will prices go up in [city]?" threads with **data-backed insights** (ACS data, macro trends)
  - Comment on methodology discussions with your expertise (probabilistic forecasting, uncertainty bands vs. point estimates)
  - Provide nuanced takes on bubble debates in r/REBubble (e.g., "the issue with single-point estimates is they don't price in downside risk")
- **Never link to Homecastr in this phase** — just build credibility

### Phase 2: Organic Content Drops (Weeks 5–8)

Introduce Homecastr organically through content that provides **standalone value**:

#### Post Templates

**1. Data Visualization Post** (r/dataisbeautiful, r/realestateinvesting)
> **Title**: "I mapped 5-year home price forecast uncertainty for every neighborhood in Florida [OC]"
>
> Share a screenshot of the Homecastr map showing P10/P50/P90 bands across FL. Explain the methodology briefly. Link to the tool in a comment ("I built this at homecastr.ai if anyone wants to explore their neighborhood").

**2. AMA / Deep Dive** (r/realestateinvesting)
> **Title**: "I'm a data scientist who built an AI model that forecasts home prices at the individual-home level. AMA about the housing market"
>
> Leverage Daniel's credentials (UT Austin, TACC, Columbia GSAPP) for E-E-A-T. Answer questions about methodology, model limitations, and specific market outlooks.

**3. Comparison Post** (r/RealEstate, r/REBubble)
> **Title**: "Why Zestimates and traditional AVMs are misleading — and what probabilistic forecasting does differently"
>
> Educational thread contrasting single-point estimates (Zillow, Redfin) with Homecastr's P10/P50/P90 approach. Frame it as educational, not promotional.

**4. Geographic Thread Participation** (r/FloridaRealEstate, r/TexasRealEstate)
> When someone asks "Is [city] a good place to buy right now?", respond with a data-informed answer and casually reference the tool: "I actually run a forecast model that covers this area — the P50 for that zip shows +X% over 5 years, but the downside risk (P10) is [Y]."

**5. Methodology Explainer** (r/MachineLearning, r/datascience)
> **Title**: "How we built a probabilistic home price forecasting model using ACS microdata and ensemble methods"
>
> Technical deep-dive that links to the `/methodology` page. This targets backlinks from data practitioners.

### Phase 3: Sustained Presence (Ongoing)

- **Weekly rhythm**: 2–3 comments/day, 1 post/week
- **Content calendar** aligned with data releases:
  - New ACS vintage → "Just updated our nationwide forecasts with 2025 ACS data"
  - Model update → "We just expanded coverage to [new state]"
  - Market events → "Here's what our model shows for [city] after [rate cut/hurricane/etc.]"
- **Cross-pollinate**: Reference Reddit discussions in blog posts / methodology page, and vice versa

---

## High-Value Thread Patterns to Monitor

Set up alerts (F5Bot or similar) for these keywords:

| Keyword | Subreddit | Rationale |
|:---|:---|:---|
| "home price forecast" | any | Direct product relevance |
| "will housing prices" | r/RealEstate, r/REBubble | Decision-making queries |
| "Zillow estimate wrong" | any | Comparison opportunity |
| "should I buy now" | r/FirstTimeHomeBuyer | High-intent conversion |
| "real estate data tool" | r/realestateinvesting | Direct product comparison |
| "housing bubble 2026" | r/REBubble | Trend participation |
| "[state name] housing market" | state-specific subs | Geographic targeting |
| "AI real estate" | r/proptech, r/MachineLearning | Industry positioning |

---

## Rules of Engagement

> [!CAUTION]
> Reddit communities are hostile to marketing. These rules are non-negotiable.

1. **90/10 rule**: 90% of activity should be genuine engagement, 10% can mention Homecastr
2. **Never post-and-run**: Always respond to comments on your own posts
3. **Disclose bias**: When mentioning Homecastr, say "Disclosure: I built this" — transparency builds trust on Reddit
4. **Don't argue with bears/bulls**: In r/REBubble, present data neutrally. The product's strength is showing *both* upside and downside scenarios
5. **Use personal account**: Posts from personal accounts with history outperform brand accounts on Reddit
6. **No vote manipulation**: Never ask others to upvote your posts

---

## Success Metrics

| Metric | Target (90 days) | How to Measure |
|:---|:---|:---|
| Reddit-sourced site traffic | 500+ visits/month | UTM parameters or Google Analytics referral data |
| Keyword mentions | "homecastr" appearing in 10+ threads | Reddit search, F5Bot alerts |
| Post engagement | 3+ posts with >50 upvotes | Reddit analytics |
| Google SERP presence | Homecastr-mentioning threads ranking for "home price forecast [city]" | Google Search Console + manual checks |
| LLM citation | AI assistants mentioning Homecastr when asked about home price forecasting | Manual testing with ChatGPT, Gemini, Perplexity |

---

## Quick-Start Checklist

- [ ] Create / designate Reddit account
- [ ] Subscribe to all Tier 1 + Tier 2 subreddits
- [ ] Set up F5Bot keyword alerts
- [ ] Begin daily engagement (15–20 min)
- [ ] Prepare 3 data visualization screenshots from the live map
- [ ] Draft first r/dataisbeautiful post
- [ ] Draft AMA outline with key talking points
- [ ] Schedule first content post for Week 5
