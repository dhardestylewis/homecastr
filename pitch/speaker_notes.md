# Homecastr Pitch — Speaker Notes & Q&A Prep

## Slide 1: Title
**Say:** "Homecastr is building the foundation model for residential real estate — turning property data into probabilistic forecasts that tell you where prices are going, not just where they are."

---

## Slide 2: The Problem
**Say:** "Everyone from Zillow to your local appraiser can tell you what a home is worth today. Nobody can affordably tell you where it's going. The forecasts that do exist are zip-level indexes locked behind enterprise paywalls. We're bringing lot-level, explainable forecasts to everyone."

> **Q: "HouseCanary does property-level forecasts. How is this different?"**
> A: "HouseCanary uses ML regression — point estimates with confidence intervals. We use a diffusion model that generates thousands of possible futures, producing full probability distributions. Our output is forecast *bands*, not a single number. And we're self-serve at $49/mo — HouseCanary starts at $79/mo and is primarily enterprise."

> **Q: "Why can't Zillow just add this?"**
> A: "Zillow's Zestimate is backward-looking — it estimates current value from comps. Forward-looking probabilistic forecasting is a fundamentally different model architecture. They'd need to build a new ML pipeline from scratch, and it's not their core business (advertising is)."

---

## Slide 3: Our Solution
**Say:** "We built a diffusion-based foundation model — the same class of model behind image generation — applied to property price trajectories. It generates thousands of scenarios per property, producing price bands with explainable drivers."

**Note:** Accuracy is stated as "14% median error." This is preliminary — be transparent about it.

> **Q: "14% median error — how does that compare?"**
> A: "Zillow's off-market Zestimate error is about 7.5%. We're at 14% but we're forecasting *future* prices, not estimating *current* value — a fundamentally harder problem. We expect this to improve significantly with more training data and transfer learning."

> **Q: "Why diffusion models instead of simpler ML?"**
> A: "Regression gives you one number. Diffusion gives you a *distribution* — the full range of probable outcomes. That's what investors and lenders actually need for risk assessment. A property isn't worth exactly $320K next year — it's worth $285K to $355K with different probabilities."

---

## Slide 4: Product
**Say:** "We have two products live today — a free consumer dashboard at homecastr.com and a REST API for professionals. Both are live, not mockups."

> **Q: "Can I see a demo?"**
> A: "Yes — go to homecastr.com right now. Search any Houston address. You'll see the forecast bands, neighborhood metrics, and our AI voice agent."

---

## Slide 5: Flywheel and Moat
**Say:** "The flywheel is simple: free users generate behavioral data — what they search, what they ask our AI agent, what neighborhoods they compare. That data makes our model better. Better model attracts pro users who pay."

**Key differentiator:** "The real moat is *intention data*. When someone asks our AI 'should I buy in Katy or Sugar Land?' — that reveals buyer intent that no one else collects. Every chat compounds into a dataset no one else has."

> **Q: "What stops a big player from building this?"**
> A: "Three things: (1) the intention data is proprietary — they'd need to build the consumer product first, (2) switching costs — users trust our explanations, not just numbers, (3) network effects compound over time. A year from now, our dataset is a year ahead of anyone starting today."

---

## Slide 6: Market Opportunity
**Say:** "TAM is $15B — US proptech data market. SAM is $1.2B — SFR operators and lenders who need forward-looking data. Our Year 3-5 SOM is $5-10M across our target metros."

> **Q: "How did you arrive at $1.2B SAM?"**
> A: "There are ~2M active real estate investors in the US, plus ~50K mortgage lenders and brokers. At $49/mo per investor and enterprise pricing for lenders, the addressable market for forward-looking property analytics is $1-1.5B. We used $1.2B as a conservative midpoint."

> **Q: "Is this venture scale?"**
> A: "At 1-2% SAM capture, we're at $12-24M ARR. At proptech multiples of 8-15x revenue, that's a $100-360M outcome. The SAM itself supports venture-scale returns."

---

## Slide 7: Landscape
**Say:** "Three categories of competitors: LLM wrappers like RealAI, traditional ML like HouseCanary, and investment platforms like SmartBricks. We're the only one using diffusion models with self-serve access."

> **Q: "HouseCanary has $129M in funding. What's your advantage?"**
> A: "They're enterprise-first — built for institutional investors. We're consumer-first — free dashboard, self-serve Pro, bottom-up GTM. Different ICP, different distribution. And our model architecture (diffusion) produces fundamentally different output (probability bands vs. point estimates)."

> **Q: "What about Zillow/Redfin adding forecasts?"**
> A: "Their business is advertising and lead gen, not data products. Forward-looking forecasting is a liability for them — if they predict prices wrong publicly, it damages their brand. We're purpose-built for this."

---

## Slide 8: Business Model
**Say:** "Our target customer is individual real estate investors in high-growth metros. Simple model: free dashboard converts to $49/mo Pro. Our target unit economics are strong — 7.8x LTV:CAC with a 1.5-month payback."

**Important note:** These are PROJECTIONS. Be upfront about this.

> **Q: "These unit economics are projections, right?"**
> A: "Yes. The pricing is benchmarked against competitors — we undercut HouseCanary at $79 and RealAI at $69. The CAC target of $50-100 assumes organic-first distribution through our free dashboard. We won't have real retention data until we have paying users — that's exactly what this raise funds."

> **Q: "Why 12-month retention for LTV?"**
> A: "Industry benchmark for B2C SaaS in real estate is 10-14 months. RE investors are sticky because they make investment decisions over quarters and years — they need ongoing forecast data. 12 months is conservative."

> **Q: "Why $49/mo and not $79 like HouseCanary?"**
> A: "Two reasons: (1) we're going bottom-up, self-serve — lower price = lower friction = faster adoption, and (2) our ICP is individual investors, not enterprises. $49 is an impulse buy for someone making $300K property decisions."

---

## Slide 9: Go-to-Market
**Say:** "Three phases. Now: free dashboard for RE investors in Houston. Q2-Q3: expand to Houston + Atlanta, targeting SFR operators at $200-500/mo. Q4+: Phoenix, DFW, Charlotte, plus lender pilots."

> **Q: "Why these 5 metros specifically?"**
> A: "Fastest-growing SFR investor markets in the US. Houston: we already have 1M+ properties indexed. Atlanta, Phoenix, DFW, Charlotte: highest population growth, most active investor markets, and strong data availability from county assessors."

> **Q: "Why not just use CoreLogic for all metros and go national?"**
> A: "We can acquire national CoreLogic data. The bottleneck isn't data cost — it's model validation per geography. Transfer learning accelerates this from months to weeks per metro, but each metro still needs accuracy validation before we go live. 5 metros is what we can fully quality-control. The pipeline, once proven, makes national scale a Series A execution problem."

---

## Slide 10: Where We Are (Traction)
**Say:** "Houston metro: 1M+ properties indexed. 4-year forecast horizon. 14% median error. Live dashboard and API — both shipping today."

> **Q: "14% error feels high. Can you improve it?"**
> A: "Yes. Our first ML hire's primary job is improving accuracy. With more training data from new metros and transfer learning, we expect to get below 10%. The diffusion architecture is designed to improve with more data — that's the flywheel."

> **Q: "Do you have any paying users?"**
> A: "Not yet — we're pre-revenue. The product is live and free. This raise funds the GTM hire to convert our first paying users."

---

## Slide 11: Team
**Say:** "Solo founder. DARPA-funded geospatial modeling at USC, 6 years building HPC data systems at TACC, taught ML for geosciences at UT Austin. Columbia M.S. in Urban Planning, UT Math degree. I built Homecastr end-to-end — model, API, frontend."

> **Q: "MetaProp prefers 2+ co-founders. Why solo?"**
> A: "I built the entire product — model, API, dashboard, AI agent — solo. That's unusual technical depth. My first two hires (ML/Data Engineer + GTM) effectively become co-founders in function. I'm looking for a technical co-founder who can complement my skills, but I won't bring on a co-founder just to check a box."

> **Q: "What's your unfair advantage for this problem?"**
> A: "The intersection of geospatial ML (DARPA/TACC), urban planning (Columbia), and mathematics (UT). Very few people have all three. I've published in computational geosciences and spent 6 years building high-performance data systems. This isn't a pivot — it's the logical convergence of my entire career."

---

## Slide 12: The Ask
**Say:** "Raising $1M pre-seed. Use of funds: ML/Data Engineer for model accuracy and transfer learning, GTM/Growth hire for our first 10 paying customers, and data plus compute for CoreLogic and expansion to 5 metros. 18-month milestones: 5 metros covering ~8M properties, $50K MRR, and a lender pilot or LOI."

> **Q: "Why $1M? Many pre-seeds raise $500K."**
> A: "Two NYC hires at market rate: ML/Data Eng at $200K and GTM at $130K, fully loaded, for 18 months = $594K. Plus CoreLogic data ($75K), compute ($50K), and ops. $1M gives us 22 months of runway with 19% buffer. $500K would mean hiring below-market or cutting runway to under a year."

> **Q: "$50K MRR in 18 months — how do you get there?"**
> A: "Three channels: ~300 Pro subscribers at $49/mo = $15K. API clients = $5-10K. 1-2 lender pilots at $5-10K/mo each = $10-20K. Total: $30-50K. The lender pilots are the key lever. Even without them, 500 Pro subs across 5 metros is 100 per metro — very achievable."

> **Q: "What's the valuation?"**
> A: "We're raising on a post-money SAFE at $8-10M cap. That's in line with Carta's median for pre-seed ($7.5-10M in Q2 2025) and reflects the AI premium. $1M at $10M post is 10% dilution."

> **Q: "What's the exit story?"**
> A: "Strategic acquisition by a data platform. CoreLogic was acquired for $6B because they own the real estate data layer. We're building the forecasting layer they don't have. Zillow bought Follow Up Boss for $500M. At 8-15x revenue multiples on $12-24M ARR, we're looking at a $100-360M outcome in Year 5-7."

---

## Meta: Why MetaProp
**For the application (not a slide):**

"MetaProp's 2025 priorities — AI and predictive, data-intensive categories like mortgage and asset management — align directly with what we're building. Your LP network (JLL, Cushman & Wakefield) are ideal pilot partners for our lender thesis. No portfolio company does residential property forecasting today — we fill that gap. The 22-week accelerator and Demo Day access would help us close the remaining $750K of our $1M round."
