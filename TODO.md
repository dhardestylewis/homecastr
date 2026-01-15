# Properlytic UI - TODO

> **Updated**: 2026-01-14T20:28
> **Source**: User requests (verbatim)

---

## Remaining High Priority Items

### P1 - Critical Bugs
- [x] **Wrong DB table**: h3-data-v2.ts queried empty table → Fixed to use `h3_precomputed_hex_rows`
- [x] **Reliability filter broken**: Now works (data exists in hex_rows)
- [x] **Med Years filter broken**: Hidden (no column exists in DB)
- [ ] **Zoom/cell loading issue**: Only cells on right side load at some zoom levels

### P2 - Feature Gaps
- [ ] **Fan chart missing**: Investigate if DB has fan chart columns populated
- [ ] **Predicted value alongside current value**: Add current value display
- [ ] **Natural boundary aggregation**: Consider using lot lines + organic aggregates (blocks, neighborhoods, school districts, zip codes) instead of H3 hexes - more intuitive for users

### P3 - UX Improvements
- [x] **Hide Risk Scoring section**: σ-notation metrics hidden for homeowner view
- [x] **Data Confidence tier labels**: Shows High/Medium/Low/N/A instead of %
- [ ] **Sample accuracy terminology**: Is this accuracy (higher=better) or error (lower=better)? Rename for clarity
- [ ] **Historical year colors**: Add color mode for raw dollar values
- [ ] **Auto-scaling min accounts by zoom**: Consider adjusting threshold based on H3 resolution
- [ ] **Property lot lines at max zoom**: Use parcel geometries instead of H3 hexes at innermost zoom (requires parcel data in DB)
- [ ] **Label aggregated metrics**: At outer zoom levels, clarify that metrics are aggregated across properties in hex

---

## Completed Items (Moved from above)

- [x] Investment Score format → Hidden for homeowner view
- [x] Min Med Years slider → Hidden (no data)
- [x] Highlight warnings toggle → Fixed (default off)
- [x] Competitor research → Zillow/Redfin don't use scores
- [x] Hide vs remove → Code commented out, not deleted
- [x] Create GUIDELINES.md, TODO.md, PROMPTS-LOG.md

---

## User Requests Log

### [2026-01-14 20:26]

> similar critique applies for reliability minimum - does non-None data exist for it?

**Resolution**: Data DOES exist in `h3_precomputed_hex_rows` table. Fixed query to use correct table.

### [2026-01-14 20:26]

> also what does "Mid Med Years" even mean? i don't even know much less do I think our users would

**Resolution**: Hidden filter. Column `med_years` doesn't exist in DB anyway.

### [2026-01-14 20:25]

> review your TODOs and what you have been focussing on - are you addressing the highest priority TODOs first and foremost by order of priority? everything i throw at you neednt necessarily be addressed immediately instead you should always prioritize alongside your other TODOs that is a GUIDELINE for every single prompt i provide you

**Resolution**: Added as GUIDELINE. Reprioritized to focus on critical bugs first.

### [2026-01-14 20:28]

> is "sample accuracy" intelligible to our end users? is that accuracy or error? ie is closer to 0 or closer to 100 better? use clearer terminology if keeping this metric at all

**Status**: Added to P3 TODO. Need to investigate what this value represents (appears to be error rate ~23%, not accuracy).
