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



