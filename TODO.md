# Properlytic UI - TODO

> **Updated**: 2026-01-14T20:16
> **Source**: User requests (verbatim)

---

## User Request [2026-01-14T20:08]

> investment score is written 10.0/10 instead of 10.0/10.0 - consider our end-audience for this app homeowners would they even care? in fact which of our metrics here would they care about? shouldn't predicted value be alongside current value? what happened to the fan chart?
> at certain zoom levels only the cells on the right side load but perhaps there is some cap on how many load imposed somewhere in the pipeline or upstream that we need to workaround? 
> all of them report data confidence of 0% is that actually N/A for now or genuinely is 0% for all the ones I inspected? Min Med Years slider doesnt seem to change anything? Neither does highlight warnings? shouldnt past years still show some hex color even if not CAGR when we are on historical years? perhaps just off of raw dollar values? how to adequately convey that or the change to CAGR? or should we enable both raw predicted dollar values as one to color in and CAGR even a CAGR computed for historical years as another?

### Breakdown:

- [x] **Investment Score format**: `10.0/10` vs `10.0/10.0` → Hidden for homeowner view
- [x] **Min Med Years slider**: Does nothing → Fixed filter in map-view.tsx
- [x] **Highlight warnings toggle**: Does nothing → Fixed filter in map-view.tsx
- [ ] **Data Confidence 0%**: Display as tier label or "N/A" if unavailable
- [ ] **Predicted value alongside current value**: Add current value display
- [ ] **Fan chart missing**: Investigate if DB has `fan_p50_y1-5` columns populated
- [ ] **Zoom/cell loading issue**: Only cells on right side load at some zoom levels
- [ ] **Historical year colors**: Add color mode for raw dollar values

---

## User Request [2026-01-14T20:09]

> to what number of digits is any one of our end users gonna ultimately care about?
> 10.0/10 Risk Factor -1.73 Price Deviation -1.06σ Prediction Error -0.04σ Debt Stress -0.61σ
> for the above or any other metrics? keeping in mind for each metric you review whether our end user will care about that metrics at all or not

### Breakdown:

- [x] **Simplify precision**: Integer scores, 1 decimal % max
- [x] **Hide σ-notation metrics**: Risk Factor, Price Deviation, Prediction Error, Debt Stress → Hidden

---

## User Request [2026-01-14T20:12]

> is a homeowner gonna care about whatever "score" we come up with here and how we came up with it? or must we introduce some such score because at a minimum our potential competitors have already introduced something similar or would - search all them up for comparison
> do it all
> continue iterating through all the below and prioritize correctly into TODO so we dont lose anything

### Breakdown:

- [x] **Competitor research**: Completed - Zillow/Redfin don't use scores; Roofstock/Mashvisor do
- [x] **Hide technical metrics**: Per Zillow/Redfin model for homeowners
- [ ] **All remaining items from prior requests**

---

## User Request [2026-01-14T20:14]

> all of the above - dont fully deprecate instead just hide for now

- [x] **Hide not remove**: Code commented out, not deleted
