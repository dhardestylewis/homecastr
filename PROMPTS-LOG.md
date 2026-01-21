# Properlytic UI - PROMPTS-LOG

> **Purpose**: Verbatim log of user prompts with timestamps

---

## 2026-01-14T20:08 - Homeowner UX Session

### [2026-01-14 20:08]

investment score is written 10.0/10 instead of 10.0/10.0 - consider our end-audience for this app homeowners would they even care? in fact which of our metrics here would they care about? shouldn't predicted value be alongside current value? what happened to the fan chart?
at certain zoom levels only the cells on the right side load but perhaps there is some cap on how many load imposed somewhere in the pipeline or upstream that we need to workaround? 
all of them report data confidence of 0% is that actually N/A for now or genuinely is 0% for all the ones I inspected? Min Med Years slider doesnt seem to change anything? Neither does highlight warnings? shouldnt past years still show some hex color even if not CAGR when we are on historical years? perhaps just off of raw dollar values? how to adequately convey that or the change to CAGR? or should we enable both raw predicted dollar values as one to color in and CAGR even a CAGR computed for historical years as another?

### [2026-01-14 20:09]

to what number of digits is any one of our end users gonna ultimately care about?

10.0/10
Risk Factor
-1.73
Price Deviation
-1.06σ
Prediction Error
-0.04σ
Debt Stress
-0.61σ

for the above or any other metrics? keeping in mind for each metric you review whether our end user will care about that metrics at all or not

review above and any other metric

### [2026-01-14 20:12]

is a homeowner gonna care about whatever "score" we come up with here and how we came up with it? or must we introduce some such score because at a minimum our potential competitors have already introduced something similar or would - search all them up for comparison

Risk Scoring
Investment Score
(0-10)
10.0/10
Risk Factor
-1.73
Price Deviation
-1.06σ
Prediction Error
-0.04σ
Debt Stress
-0.61σ

do it all

continue iterating through all the below and prioritize correctly into TODO so we dont lose anything

[followed by:]

investment score is written 10.0/10 instead of 10.0/10.0 - consider our end-audience for this app homeowners would they even care? in fact which of our metrics here would they care about? shouldn't predicted value be alongside current value? what happened to the fan chart?
at certain zoom levels only the cells on the right side load but perhaps there is some cap on how many load imposed somewhere in the pipeline or upstream that we need to workaround? 
all of them report data confidence of 0% is that actually N/A for now or genuinely is 0% for all the ones I inspected? Min Med Years slider doesnt seem to change anything? Neither does highlight warnings? shouldnt past years still show some hex color even if not CAGR when we are on historical years? perhaps just off of raw dollar values? how to adequately convey that or the change to CAGR? or should we enable both raw predicted dollar values as one to color in and CAGR even a CAGR computed for historical years as another?

to what number of digits is any one of our end users gonna ultimately care about?

10.0/10
Risk Factor
-1.73
Price Deviation
-1.06σ
Prediction Error
-0.04σ
Debt Stress
-0.61σ

for the above or any other metrics? keeping in mind for each metric you review whether our end user will care about that metrics at all or not

review above and any other metric

### [2026-01-14 20:14]

all of the above - dont fully deprecate instead just hide for now

### [2026-01-14 20:15]

do you have your own GUIDELINES and are you following them?

https://github.com/dhardestylewis/GUIDELINES/blob/main/GUIDELINES.md

[followed by full GUIDELINES.md content]

### [2026-01-14 20:16]

but have you placed all relevant GUIDELINES into such a file. likewise everything I've stated into a TODO as close to or exactly verbatim as possible?

### [2026-01-14 20:17]

turn off highlight warnings by default for now

### [2026-01-14 20:19]

did you pull in evere single possible GUIDELINE relevant to this project as possible?

https://github.com/dhardestylewis/GUIDELINES/blob/main/GUIDELINES.md

### [2026-01-14 20:20]

not just the full one without consideration, only everything relevant, as much verbatim as possible, while considering each and every single one in chat and whatever updates or specific alterations might be needed for this project

### [2026-01-14 20:21]

using the Mid Med Years slider now turns off everythign for any non-None value? did you check the DB to see if there any values for that in actuality?

### [2026-01-14 20:23]

should the min accounts depend on the zoom level?

## 2026-01-15T16:00 - Minimal Data Contract

### [2026-01-15 16:00]

You are to produce the minimal data contract required for the map frontend.

Goal:
Minimize database storage and upload volume by storing only the columns that the frontend truly reads.

Task:
1) Enumerate every frontend read path that touches hex data, including:
   - RPCs used (names and parameters)
   - Direct table/view reads
   - Tile generation queries (if any)
   - Any joins to grid tables for lat/lng or geometry

2) For each read path, list the exact columns referenced and whether they are required or optional.

3) Provide the minimal schema for a single canonical backend view/table that satisfies all reads.
   - Prefer computed columns in SQL rather than stored duplicates:
     - opportunity_pct = 100 * opportunity
     - trend can be derived from opportunity thresholds
   - Prefer float4 (REAL) unless float8 is required for correctness.

4) Provide the exact SQL queries that the frontend will use against that minimal schema.
   Include:
   - WHERE filters (bounds, year, resolution)
   - ORDER BY clauses
   - LIMIT usage
   - Any pagination keys

Output format:
A) Minimal column list (authoritative)
B) SQL DDL for minimal table and minimal view
C) A migration plan to switch the frontend to the minimal view/table

### [2026-01-15 16:10]

consolidate all this into a single file i can provide back to the backend eng agent

### [2026-01-15 16:12]

if truly consolidated delte other spec files

### [2026-01-15 16:13]

have you been following your GUIDELINEs/git committing

### [2026-01-15 16:14]

i still see those files in place?

### [2026-01-15 17:05]

check 2017 again

[Pipeline logs provided showing 2017 ingestion]

### [2026-01-15 17:07]

did we produce all the values you need to proceed?

[Provided python script "H3 Full Stack MVT Pipeline v14.7"]

### [2026-01-15 17:09]

reload the webserver? i am still not seeing any colors visualized even for historical years?

http://localhost:3000/?underperf=false&mode=value

## 2026-01-17T15:25 - Tooltip Lock & Comparison Mode Session

### [2026-01-17 15:25]

Next.js 16.0.10 (stale) Turbopack Console Error Server {message: ..., details: ..., hint: "", code: ...}

### [2026-01-17 15:27]

after clicking a tile a yellow dashed line should extend from the hovering tooltip to that tile and that tile should be highlighted in dashed yellow. the tooltip should then remain fixed in place rather than moving around

### [2026-01-17 15:28]

on mobile version, the list of suggested addresses is hidden behind the year element

### [2026-01-17 15:28]

within the tooltip the word "PREDICTED" is used even for historical years

### [2026-01-17 15:29]

http://localhost:3000/?id=88446c32ddfffff Internal Server Error - clicking a tile caused the above. on the mobile version similarly crashes

### [2026-01-17 15:34]

after youve clicked on a single tile it should remain fixed on that hex no matter what else you hover over, i still dont see the yellow dashed line extending from the then-fixed tooltip to the hex or a yellow dashed highlighted hex, continuing to hover over other tiles after selecting one should then display a single combined fan charts comparing the two

### [2026-01-17 15:35]

you should be able to move the tooltip out of the way or wherever you want wants it is in static mode. hitting ESC should esc out back to the original dynamic tooltip mode

### [2026-01-17 15:35]

the first time you use this the tooltip should complete a small gesture to help indicate to the user they can move it around

### [2026-01-17 15:38]

have you been committing with every prompt, and following your GUIDELINES file every prompt?

### [2026-01-17 15:40]

have you been keeping track of everything ive asked this session? is it all reflected and prioritizted in TODO and can you repeat back in priority order what remains to do in chat?

### [2026-01-17 15:42]

why is the fan chart remaining static after click but the other values changing?

### [2026-01-17 15:42]

all of these should enter comparison mode after clicking

### [2026-01-17 15:43]

[Build Error] Parsing ecmascript source code failed - Expected a semicolon

### [2026-01-17 15:47]

have you visually indicated you can hit ESC to go back? or written anywhere if necessary the mode you are in?

### [2026-01-17 15:47]

yes continue but that is not a comparison chart it is a comparison made within the same chart

### [2026-01-17 15:47]

also still not seeing the hex highlighted or pointed to

### [2026-01-17 15:49]

instead of saying "P10-P90" say that in homeowner legible terms

### [2026-01-17 15:49]

ensure the lines dont trail below the bottom of the y axis or above the top

### [2026-01-17 15:49]

### [2026-01-17 15:49]

double clicking should zoom in

### [2026-01-17 15:50]

the dashed line seems to be rendering from a different zoom/resolution level?

### [2026-01-17 15:57]

why is the hex it is pointing to different when i move the tooltip? shouldnt that remain fixed even if at the wrong zoom level?

### [2026-01-17 16:04]

why isnt the comparison mode yet activating after clicking on a hex? still see only one fan chart and likely range and one set of values

### [2026-01-17 16:07]

8a446c326a9ffff
or similar should never appear in the search bar not even once

### [2026-01-17 16:08]

why does clicking on a tile suddenly move the tooltip to a fixed location in the top middle rather than keeping it fixed wherever it was?

### [2026-01-17 16:09]

The dashed selection outline and the hover outline are being drawn in a *different canvas coordinate system* than the filled H3 tiles.
[User provided detailed DPR fix instructions]

### [2026-01-17 16:13]

the dashed line should be from the tooltip wherever it is relocated to, not from some other hex. but it should be to the selected hex

### [2026-01-17 16:15]

Comparison should initially be in dynamic mode dynamically updating the value timeline until you click again

### [2026-01-17 16:18]

why is it a dashed line from the initially selecteto the comparison selected not from the tooltip to the initially selected? with the comparison being highlighted a different color? in fact the fan chart line color and the tile highlight color should be the same for both initial and comparison,

### [2026-01-17 16:20]

the tooltip itself is not located right on the selected box, and you should not assume or rely on any relation between the two instead just go from wherever the tooltip is unrelated to the tiles to the tile

what youve got is close bu the dashed line should be from very corner of the tooltip it is currently usually a little off the diagonal from whichever corner it is

### [2026-01-17 16:21]

ensure the fan chart line color for the historical data and the range are on similar color spectra aas for

### [2026-01-17 16:21]

clicking on a second tile instead makes that the primary tile rather than for comparison

### [2026-01-17 15:50]

the dashed line seems to be rendering from a different zoom/resolution level?

## 2026-01-21T11:25 - Comparison Mode Fixes

### [2026-01-21 11:25]

candidate comparison when mouse goes off screen shouldnt appear at all, also the highlight color for candidates should match the timeline color
