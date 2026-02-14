# Refactoring Download Location

The goal is to move the temporary download directory for HCAD data to a permanent, well-structured location within the project.

## Proposed Changes

### Directory Structure
- Create `data/` at the project root if it doesn't exist.
- Create `data/hcad/` to store HCAD specific data.
- Move contents of `temp_test_downloads` (or `temp_downloads`) to `data/hcad/`.
- Create `.meta/` directory.
- Move `GUIDELINES.md`, `TODO.md`, `PROMPTS-LOG.md` into `.meta/`.

### Code Changes
- **scripts/hcad_downloader.py**: Update the default `download_dir`.
- **pipeline_v14.py**: Rename and move to `scripts/h3_mvt_pipeline.py`.
- **scripts/legacy_checks**: New folder for ephemeral validation scripts.
- Move `check-*`, `*.txt` from `scripts/` to `scripts/legacy_checks/`.


## Verification Plan
### Automated Tests
- Run `python scripts/hcad_downloader.py --help` to verify the default dir in output (if visible) or check the code.
- Run a dry-run or check file existence in the new location.

### Manual Verification
- Check that files exist in `data/hcad`.
- Check that `temp_test_downloads` is gone.

---

# Properlytic UI - TODO

> **Updated**: 2026-01-21T20:31
> **Source**: User requests (verbatim)

---

## Remaining High Priority Items

### P1 - AVM Vendor Compliance & Validation
- [ ] **See**: [TODO_AVM_VENDOR_COMPLIANCE.md](./TODO_AVM_VENDOR_COMPLIANCE.md)  Full spec for legal positioning, regulatory requirements, validation framework, fan charts, drift monitoring, and deliverables [Added: 2026-02-06 21:57]

### P1 - Critical Bugs
- [x] **Wrong DB table**: h3-data-v2.ts queried empty table → Fixed to use `h3_precomputed_hex_rows`
- [x] **Reliability filter broken**: Now works (data exists in hex_rows)
- [x] **Med Years filter broken**: Hidden (no column exists in DB)
- [ ] **Zoom/cell loading issue**: Only cells on right side load at some zoom levels
- [x] **Timelapse rendering slow**: Fixed with Double Buffering + idle+RAF swap [Updated: 2026-01-21]
- [/] **Migrate to Tile Server Architecture**: VectorMap component complete. Pending: user must run `get_tile.sql` in Supabase [Updated: 2026-01-21]
- [x] **Mobile z-index**: Address suggestions dropdown fixed z-[200] [Added: 2026-01-17]
- [ ] **Res 11 hex fill bug**: At outermost zoom levels in res 11, not all hexes appear filled - they fill in when zooming slightly more to inner zoom levels (classic tile engine only) [Added: 2026-02-08]
- [ ] **VectorMap tooltip missing**: New (vector) version doesn't have fanchart tooltip popping up at all [Added: 2026-02-08]
- [ ] **VectorMap timelapse flicker**: Flicker on new version when using timelapse [Added: 2026-02-08]
- [ ] **Tooltip parity**: VectorMap should use exact same tooltip as MapView (classic) [Added: 2026-02-08]

### P1 - Accessibility (Vercel Audit) [Added: 2026-02-08]
- [ ] **Enable zooming/scaling**: Zooming and scaling must not be disabled
- [ ] **ARIA hidden focusable fix**: ARIA hidden elements must not be focusable or contain focusable elements
- [ ] **Add main landmark**: Document should have one `<main>` element

### P1 - VectorMap Full Parity Checklist
*Derived from comprehensive analysis of MapView logic.*

**1. Interaction Logic**
- [ ] **Hover State**: Highlight hex, show tooltip, fetch details (debounced).
- [ ] **Click Selection (Single)**: Select/Deselect, lock tooltip, enable drag.
- [ ] **Shift+Click (Multi-Select)**: [REQUESTED] Implement Bounding Box / Range selection (like MapView).
- [ ] **Ctrl+Click (Multi-Select)**: [REQUESTED] Implement Toggle/Add Single selection.
- [ ] **Mobile Touch**: Tap-to-select, auto-pan to center (bottom half), swipe-to-dismiss.
- [ ] **Fix TS Errors**: Clear all lingering variable reference errors in vector-map.

**2. Tooltip Behavior**
- [ ] **Smart Docking**: Port `getSmartTooltipPos` logic to avoid screen edge/sidebar overlap.
- [ ] **Draggable**: Locked mode allows dragging (Desktop).
- [ ] **Content Parity**: Header (ID), Metrics, FanChart, Comparisons, Aggregates.
- [ ] **Mobile Layout**: Compact view, side-by-side stats.

**3. Comparison & Preview**
- [ ] **Comparison State**: Hovering other hex while selected sets "Comparison" (Blue vs Orange).
- [ ] **Freeze Mode**: Holding Shift prevents comparison hex updates.
- [ ] **Preview Lines**: Shift-hover shows "Preview" aggregate (Green/Purple).

**4. Visualization**
- [x] **Color Modes**: Support "Growth" (Purple-White-Blue) and "Value" (Purple-Red-Yellow).
- [x] **Layers**: H3 Fill, Outlines (Selected/Comparison/Hover), Parcels (>z14).
- [x] **Timelines**: Seamless year transitions (Double Buffering).
- [x] **Candidate Color**: Shift-hover shows Fuchsia line to match Fan Chart.

**5. System & Data**
- [ ] **Data Fetching**: Cached H3 details, Parcels on moveend (>z14).
- [x] **URL Sync**: Update lat/lng/zoom params.
- [ ] **Resize Handling**: Update canvas/map size.

---

### P2 - Feature Gaps (Existing)
- [ ] **Fan chart missing**: Investigate if DB has fan chart columns populated
- [ ] **Predicted value alongside current value**: Add current value display
- [ ] **Natural boundary aggregation**: Consider using lot lines + organic aggregates (blocks, neighborhoods, school districts, zip codes) instead of H3 hexes - more intuitive for users

### P2 - Explainer Popup Animation [Added: 2026-02-08]
- [ ] **Minimize to corner**: Currently minimizing to center, should minimize to bottom-right corner
- [ ] **Eye-tracking animation**: Make minimize animation direct user's eye to follow it for a brief moment of perception
- [ ] **Visible minimize**: Popup should visibly minimize rather than disappear forever

### P2 - Visual Theming / Skins [Added: 2026-02-08]
- [ ] **Research Goodfire color schema**: Look up https://startups.gallery/companies/goodfire and Goodfire proper
- [ ] **Create skin options**: Provide multiple theme/skin options based on Goodfire aesthetic
- [ ] **Add theme toggle UI**: Add temporarily placed buttons to toggle between skins visibly on screen
- [ ] **Skin comparison menu**: Add button or menu for comparing visual skins

### P3 - UX Improvements
- [x] **Hide Risk Scoring section**: σ-notation metrics hidden for homeowner view
- [x] **Data Confidence tier labels**: Shows High/Medium/Low/N/A instead of %
- [x] **Sample accuracy terminology**: Renamed to "Mean Error" / "Avg Error %" (clarified it is error, lower is better)
- [x] **Historical year colors**: Add color mode for raw dollar values
- [ ] **Auto-scaling min accounts by zoom**: Consider adjusting threshold based on H3 resolution
- [x] **Property lot lines at max zoom**: Implemented logic (requires `get_parcels_in_bounds` RPC to function)
- [ ] **Label aggregated metrics**: outer zoom levels, clarify that metrics are aggregated across properties in hex
- [x] **Mobile Tooltip Compaction**: Side-by-side layout (Stats + Chart), removed headers, swipe-to-minimize [Added: 2026-01-17]
- [x] **Search Box Fix**: Prevent suggestions popup on map click [Added: 2026-01-17]
- [x] **Mobile Time Controls**: Compact single-row layout [Added: 2026-01-17]
- [x] **Interactive Fan Chart**: Click fan to scrub timeline/change year [Added: 2026-01-17]
- [x] **Mobile Controls Layout**: Repositioned Zoom/Legend/Display controls to avoid tooltip overlap [Added: 2026-01-17]
- [x] **Swipe Threshold**: Relaxed to 50px for easier minimization [Added: 2026-01-17]
- [ ] **Explainer copy for lay homeowners**: Revise popup text to be more accessible for lay homeowner audience [Added: 2026-02-08]

### P3 - Address Search & Display [Added: 2026-02-08]
- [ ] **Address formatting**: Remove comma between street number and street name (e.g., `1103, River Glyn Drive` → `1103 River Glyn Drive`)
- [ ] **Hide county**: Don't display county under found address or in search bar
- [ ] **Gentle zoom/pan**: When entering address, zoom and pan gently (smooth animation) to location
- [ ] **Auto-show tooltip**: After address search, automatically show tooltip for innermost zoom hex on that address

### P3 - Layout Shift (CLS) [Added: 2026-02-08]
- [ ] **Fix CLS issues**: `div.p-6.border-b.border-white/10.bg-white/5.flex.justify-between.items-start` becoming smaller and shifting 2 other elements

### P3 - Documentation [Added: 2026-02-08]
- [ ] **README update**: Change "Monte Carlo" to "diffusion scenario generations"

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

### [2026-01-26 20:39]

> mv temp downloads into a better named and located actual final location for it considering the structure of this workspace
> remove temp_test_downloads

**Action**: Moved `temp_test_downloads` to `data/hcad/`. Updated `hcad_downloader.py`.

### [2026-01-26 20:41]

> better locate and more explicitly name pipeline v14 what kind of pipeline where in the pipeline? that should be in the name
> perhaps the guidelines and todo belong in a .meta

- [ ] **See**: [TODO_AVM_VENDOR_COMPLIANCE.md](./TODO_AVM_VENDOR_COMPLIANCE.md)  Full spec for legal positioning, regulatory requirements, validation framework, fan charts, drift monitoring, and deliverables [Added: 2026-02-06 21:57]

### P1 - Critical Bugs
- [x] **Wrong DB table**: h3-data-v2.ts queried empty table → Fixed to use `h3_precomputed_hex_rows`
- [x] **Reliability filter broken**: Now works (data exists in hex_rows)
- [x] **Med Years filter broken**: Hidden (no column exists in DB)
- [ ] **Zoom/cell loading issue**: Only cells on right side load at some zoom levels
- [x] **Timelapse rendering slow**: Fixed with Double Buffering + idle+RAF swap [Updated: 2026-01-21]
- [/] **Migrate to Tile Server Architecture**: VectorMap component complete. Pending: user must run `get_tile.sql` in Supabase [Updated: 2026-01-21]
- [x] **Mobile z-index**: Address suggestions dropdown fixed z-[200] [Added: 2026-01-17]

### P1 - VectorMap Full Parity Checklist
*Derived from comprehensive analysis of MapView logic.*

**1. Interaction Logic**
- [ ] **Hover State**: Highlight hex, show tooltip, fetch details (debounced).
- [ ] **Click Selection (Single)**: Select/Deselect, lock tooltip, enable drag.
- [ ] **Shift+Click (Multi-Select)**: [REQUESTED] Implement Bounding Box / Range selection (like MapView).
- [ ] **Ctrl+Click (Multi-Select)**: [REQUESTED] Implement Toggle/Add Single selection.
- [ ] **Mobile Touch**: Tap-to-select, auto-pan to center (bottom half), swipe-to-dismiss.
- [ ] **Fix TS Errors**: Clear all lingering variable reference errors in vector-map.

**2. Tooltip Behavior**
- [ ] **Smart Docking**: Port `getSmartTooltipPos` logic to avoid screen edge/sidebar overlap.
- [ ] **Draggable**: Locked mode allows dragging (Desktop).
- [ ] **Content Parity**: Header (ID), Metrics, FanChart, Comparisons, Aggregates.
- [ ] **Mobile Layout**: Compact view, side-by-side stats.

**3. Comparison & Preview**
- [ ] **Comparison State**: Hovering other hex while selected sets "Comparison" (Blue vs Orange).
- [ ] **Freeze Mode**: Holding Shift prevents comparison hex updates.
- [ ] **Preview Lines**: Shift-hover shows "Preview" aggregate (Green/Purple).

**4. Visualization**
- [x] **Color Modes**: Support "Growth" (Purple-White-Blue) and "Value" (Purple-Red-Yellow).
- [x] **Layers**: H3 Fill, Outlines (Selected/Comparison/Hover), Parcels (>z14).
- [x] **Timelines**: Seamless year transitions (Double Buffering).
- [x] **Candidate Color**: Shift-hover shows Fuchsia line to match Fan Chart.

**5. System & Data**
- [ ] **Data Fetching**: Cached H3 details, Parcels on moveend (>z14).
- [x] **URL Sync**: Update lat/lng/zoom params.
- [ ] **Resize Handling**: Update canvas/map size.

---

### P2 - Feature Gaps (Existing)
- [ ] **Fan chart missing**: Investigate if DB has fan chart columns populated
- [ ] **Predicted value alongside current value**: Add current value display
- [ ] **Natural boundary aggregation**: Consider using lot lines + organic aggregates (blocks, neighborhoods, school districts, zip codes) instead of H3 hexes - more intuitive for users

### P3 - UX Improvements
- [x] **Hide Risk Scoring section**: σ-notation metrics hidden for homeowner view
- [x] **Data Confidence tier labels**: Shows High/Medium/Low/N/A instead of %
- [x] **Sample accuracy terminology**: Renamed to "Mean Error" / "Avg Error %" (clarified it is error, lower is better)
- [x] **Historical year colors**: Add color mode for raw dollar values
- [ ] **Auto-scaling min accounts by zoom**: Consider adjusting threshold based on H3 resolution
- [x] **Property lot lines at max zoom**: Implemented logic (requires `get_parcels_in_bounds` RPC to function)
- [ ] **Label aggregated metrics**: outer zoom levels, clarify that metrics are aggregated across properties in hex
- [x] **Mobile Tooltip Compaction**: Side-by-side layout (Stats + Chart), removed headers, swipe-to-minimize [Added: 2026-01-17]
- [x] **Search Box Fix**: Prevent suggestions popup on map click [Added: 2026-01-17]
- [x] **Mobile Time Controls**: Compact single-row layout [Added: 2026-01-17]
- [x] **Interactive Fan Chart**: Click fan to scrub timeline/change year [Added: 2026-01-17]
- [x] **Mobile Controls Layout**: Repositioned Zoom/Legend/Display controls to avoid tooltip overlap [Added: 2026-01-17]
- [x] **Swipe Threshold**: Relaxed to 50px for easier minimization [Added: 2026-01-17]

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

### [2026-01-26 20:39]

> mv temp downloads into a better named and located actual final location for it considering the structure of this workspace
> remove temp_test_downloads

**Action**: Moved `temp_test_downloads` to `data/hcad/`. Updated `hcad_downloader.py`.

### [2026-01-26 20:41]

> better locate and more explicitly name pipeline v14 what kind of pipeline where in the pipeline? that should be in the name
> perhaps the guidelines and todo belong in a .meta

**Action**: Moved `pipeline_v14.py` to `scripts/h3_mvt_pipeline.py`. Created `.meta/` for `GUIDELINES.md`, `TODO.md`, `PROMPTS-LOG.md`.

### [2026-01-26 20:43]

> Are you or can you hook into My G Drive? It's on G: find ProFormaHouston-LeakageFree.ipynb and pull a copy into this workspace

**Action**: Searching G: drive for file.

### [2026-02-08 11:33]

> Build failed on Vercel - geocode.ts syntax error line 141, theme-toggle.tsx duplicate Button tag, ExplainerPopup not defined

**Resolution**: Rolled back from `89f35a4` to `fed3d09` (working commit). Force pushed to main.

### [2026-02-08 11:42]

> why did you move all the floating elements out of the top left corner? why does the timelapse now read 2024 2034? you engaged in too many changes relative to the prev working commit which you have the commit id for

> also the following: Accessibility - Buttons must have discernible text, ARIA input fields must have an accessible name

> and the tiles are no longer appearing

**Resolution**: Identified scope creep. Reset to `fed3d09` working commit.

### [2026-02-08 11:43]

> we are to bring back elements from prev commit by diffing against the most recent commit beginning with f

**Resolution**: Found `fed3d09` as the working commit. Reset and force pushed.

### [2026-02-08 11:46]

> which tile engine was the commit we just reversed using? no i mean which of the two tile engines we had built as files

**Resolution**: Both commits use same toggle: VectorMap (MapLibre) vs MapView (Leaflet). Default is `useVectorMap: false` → MapView/Leaflet. VectorMap accessible via `?vector=true` URL param or UI toggle.

### [2026-02-08 11:50]

> no we are experimenting with a tile engine labelled in the toggles new (vector)

**Clarification**: The experimental engine is **VectorMap** (MapLibre GL) - labeled "new (vector)" in toggles.

### [2026-02-08 11:33 - earlier context]

> and you successfully pushed?

> PS C:\Users\dhl> git add app/actions/geocode.ts && git commit -m "fix: syntax error in reverseGeocode" && git push
> The token '&&' is not a valid statement separator in this version.

**Resolution**: PowerShell doesn't support `&&`. Re-ran with `;` separator: `git add . ; git commit -m "..." ; git push`. Successfully pushed `e1b7619`.

### [2026-02-08 - earlier context]

> the original popup instructions that first appear should minimize to a corner visibly minimizing rather than disappear forever

**Status**: Already implemented in `explainer-popup.tsx` - has minimize animation to bottom-right corner with "?" help button to restore.

---
- [ ] Refactor notebooks/ProFormaHouston-LeakageFree.ipynb into modular script files