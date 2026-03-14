"""
generate_narratives_modal.py
=============================
Offline batch generation of AI narratives for SEO forecast pages.

Runs on Modal.  For each state/city/tract with forecast data, it:
  1. Fetches the forecast horizons, history, and comparables from Supabase
  2. Calls OpenAI's structured-outputs API (gpt-4o-mini) with the data
  3. Upserts the resulting JSON narratives into public.seo_narratives

Usage:
    # Generate for all tracts in Texas (by state FIPS)
    python -m modal run scripts/pipeline/seo/generate_narratives_modal.py \
        --state-fips 48

    # Generate for a specific city (county FIPS prefix)
    python -m modal run scripts/pipeline/seo/generate_narratives_modal.py \
        --state-fips 48 --county-fips 48201

    # Dry run (prints what would be generated without LLM calls)
    python -m modal run scripts/pipeline/seo/generate_narratives_modal.py \
        --state-fips 48 --dry-run
"""
import os

from __future__ import annotations
import modal, os, json, time

app = modal.App("seo-narrative-gen")

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install("psycopg2-binary", "openai>=1.30")
)

supabase_secret = modal.Secret.from_name(
    "supabase-creds", required_keys=["SUPABASE_DB_URL"]
)
openai_secret = modal.Secret.from_name(
    "openai-creds", required_keys=["OPENAI_API_KEY"]
)

# ---------------------------------------------------------------------------
# Schema used on the forecast tables
# ---------------------------------------------------------------------------
FORECAST_SCHEMA = os.environ.get("FORECAST_SCHEMA", "forecast_queue")
ORIGIN_YEAR = 2025
MODEL_VERSION = "gpt-4o-mini-2024-07-18"
BATCH_SIZE = 25  # tracts per LLM batch

# ---------------------------------------------------------------------------
# System prompt for the LLM
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """\
You are a senior real estate quantitative analyst writing for a public forecast page on Homecastr, an AI-powered home price forecasting platform.

You will receive structured forecast data for a specific neighborhood (census tract) including:
- Historical median home values (2015–2025)
- Forecast horizons with P10 (downside), P50 (median), P90 (upside) through 2030
- Comparable neighborhoods with their own forecast trajectories
- Metro ranking and national percentile

Your job is to produce a JSON object with four narrative fields. Each field must be:
1. **Grounded in the numbers** — cite specific dollar values, percentages, and date ranges from the data.
2. **Locally specific** — reference the neighborhood name, city, state. Never say "this area" when you can say the name.
3. **Distinct** — avoid boilerplate. Each narrative should feel like it was written by a local analyst who understands this specific market.
4. **Concise** — 2-4 sentences per field. No filler.

Output the following JSON structure ONLY (no markdown, no explanation):
{
  "market_summary": "A 2-sentence executive summary of the forecast trajectory, mentioning the neighborhood by name, the current P50, and the expected 5-year change.",
  "trend_analysis": "A paragraph explaining historical context leading into the forecast. Reference specific historical values (e.g., 'rose from $X in 2018 to $Y in 2023') before transitioning to the forecast outlook.",
  "uncertainty_interpretation": "Explanation of the gap between the P10 and P90 bands specifically for this market. Quantify the spread in dollar terms and as a percentage. Explain what drives wider or tighter bands here.",
  "comparable_narrative": "A comparison explaining WHY the provided comparable neighborhoods are behaving similarly or differently. Reference specific neighborhoods by name and compare their trajectories."
}

If no comparables are provided, write the comparable_narrative field explaining why this neighborhood has limited direct comparisons (e.g., unique price point, limited county coverage) and suggest what the reader should compare it to instead.
"""


# ---------------------------------------------------------------------------
# Fetch tract-level context from Supabase
# ---------------------------------------------------------------------------
def _fetch_tract_context(conn, tract_geoid: str) -> dict | None:
    """Pull forecast horizons, history, and comparables for a single tract."""
    cur = conn.cursor()

    # 1. Forecast horizons
    cur.execute(f"""
        SELECT horizon_m, p10, p25, p50, p75, p90, origin_year
        FROM {FORECAST_SCHEMA}.metrics_tract_forecast
        WHERE tract_geoid20 = %s
          AND origin_year = %s
          AND series_kind = 'forecast'
        ORDER BY horizon_m
    """, (tract_geoid, ORIGIN_YEAR))
    forecast_rows = cur.fetchall()
    if not forecast_rows:
        # Try fallback origin year
        cur.execute(f"""
            SELECT horizon_m, p10, p25, p50, p75, p90, origin_year
            FROM {FORECAST_SCHEMA}.metrics_tract_forecast
            WHERE tract_geoid20 = %s
              AND origin_year = %s
              AND series_kind = 'forecast'
            ORDER BY horizon_m
        """, (tract_geoid, ORIGIN_YEAR - 1))
        forecast_rows = cur.fetchall()
    if not forecast_rows:
        return None

    effective_origin = forecast_rows[0][6]
    baseline_horizon_m = (2026 - effective_origin) * 12
    baseline_p50 = None
    for r in forecast_rows:
        if r[0] == baseline_horizon_m:
            baseline_p50 = r[3]
            break
    if baseline_p50 is None:
        baseline_p50 = forecast_rows[0][3] or 1

    horizons = []
    for r in forecast_rows:
        p50_val = r[3] or 0
        horizons.append({
            "horizon_m": r[0],
            "forecast_year": effective_origin + r[0] / 12,
            "p10": r[1] or 0,
            "p25": r[2] or 0,
            "p50": p50_val,
            "p75": r[4] or 0,
            "p90": r[5] or 0,
            "spread": (r[5] or 0) - (r[1] or 0),
            "appreciation_pct": round(((p50_val / baseline_p50) - 1) * 100, 2) if baseline_p50 > 0 else 0,
        })

    # 2. History
    cur.execute(f"""
        SELECT year, COALESCE(p50, value, 0) as value
        FROM {FORECAST_SCHEMA}.metrics_tract_history
        WHERE tract_geoid20 = %s AND year BETWEEN 2015 AND 2025
        ORDER BY year
    """, (tract_geoid,))
    history = [{"year": r[0], "value": round(r[1])} for r in cur.fetchall()]

    # 3. Comparables (same county, grab top 5 by similarity)
    county_fips = tract_geoid[:5]
    cur.execute(f"""
        WITH target AS (
            SELECT p50 as target_p50
            FROM {FORECAST_SCHEMA}.metrics_tract_forecast
            WHERE tract_geoid20 = %s AND origin_year = %s AND horizon_m = 12 AND series_kind = 'forecast'
        ),
        candidates AS (
            SELECT t.tract_geoid20,
                   MAX(CASE WHEN t.horizon_m=12 THEN t.p50 END) as p50_12m,
                   MAX(CASE WHEN t.horizon_m=60 THEN t.p50 END) as p50_60m,
                   MAX(CASE WHEN t.horizon_m=60 THEN t.p10 END) as p10_60m,
                   MAX(CASE WHEN t.horizon_m=60 THEN t.p90 END) as p90_60m
            FROM {FORECAST_SCHEMA}.metrics_tract_forecast t
            WHERE t.tract_geoid20 LIKE %s
              AND t.tract_geoid20 != %s
              AND t.origin_year = %s
              AND t.series_kind = 'forecast'
              AND t.horizon_m IN (12, 60)
              AND t.p50 IS NOT NULL
            GROUP BY t.tract_geoid20
            HAVING MAX(CASE WHEN t.horizon_m=12 THEN t.p50 END) > 0
               AND MAX(CASE WHEN t.horizon_m=60 THEN t.p50 END) > 0
        )
        SELECT c.tract_geoid20, c.p50_12m, c.p50_60m,
               ROUND(((c.p50_60m / c.p50_12m) - 1) * 100, 1) as appreciation_60m,
               c.p90_60m - c.p10_60m as spread_60m
        FROM candidates c, target tg
        ORDER BY ABS(c.p50_12m - tg.target_p50)
        LIMIT 5
    """, (tract_geoid, effective_origin, f"{county_fips}%", tract_geoid, effective_origin))

    comparables = []
    for r in cur.fetchall():
        comparables.append({
            "tract_geoid": r[0],
            "name": f"Tract {r[0][5:]}",
            "p50_12m": round(r[1]),
            "p50_60m": round(r[2]),
            "appreciation_60m_pct": float(r[3]) if r[3] else 0,
            "spread_60m": round(r[4]) if r[4] else 0,
        })

    # 4. Rankings
    cur.execute(f"""
        SELECT COUNT(*) FROM {FORECAST_SCHEMA}.metrics_tract_forecast
        WHERE tract_geoid20 LIKE %s AND origin_year=%s AND horizon_m=60 AND series_kind='forecast' AND p50 IS NOT NULL
    """, (f"{county_fips}%", effective_origin))
    metro_total = cur.fetchone()[0]

    p50_60 = next((h["p50"] for h in horizons if h["horizon_m"] == 60), 0)
    cur.execute(f"""
        SELECT COUNT(*) FROM {FORECAST_SCHEMA}.metrics_tract_forecast
        WHERE tract_geoid20 LIKE %s AND origin_year=%s AND horizon_m=60 AND series_kind='forecast' AND p50 > %s
    """, (f"{county_fips}%", effective_origin, p50_60))
    metro_rank = cur.fetchone()[0] + 1

    return {
        "tract_geoid": tract_geoid,
        "origin_year": effective_origin,
        "baseline_p50": round(baseline_p50),
        "horizons": horizons,
        "history": history,
        "comparables": comparables,
        "metro_rank": metro_rank,
        "metro_total": metro_total,
    }


# ---------------------------------------------------------------------------
# Geo name resolution
# ---------------------------------------------------------------------------
def _resolve_geo_name(conn, tract_geoid: str) -> dict:
    """Try to resolve a tract geoid to neighborhood/city/state names."""
    cur = conn.cursor()
    # Try tract_names table first
    cur.execute("""
        SELECT neighborhood_name, city, state_abbr, state_name
        FROM public.tract_names
        WHERE tract_geoid20 = %s
        LIMIT 1
    """, (tract_geoid,))
    row = cur.fetchone()
    if row:
        return {
            "neighborhood": row[0] or f"Tract {tract_geoid[5:]}",
            "city": row[1] or "Unknown",
            "state_abbr": row[2] or "",
            "state_name": row[3] or "",
        }
    return {
        "neighborhood": f"Tract {tract_geoid[5:]}",
        "city": "Unknown",
        "state_abbr": "",
        "state_name": "",
    }


# ---------------------------------------------------------------------------
# Generate narrative for a single tract
# ---------------------------------------------------------------------------
def _generate_narrative(client, context: dict, geo: dict) -> dict:
    """Call OpenAI structured outputs for one tract."""
    user_msg = json.dumps({
        "neighborhood_name": geo["neighborhood"],
        "city": geo["city"],
        "state": geo["state_name"] or geo["state_abbr"],
        "current_median_value": f"${context['baseline_p50']:,}",
        "origin_year": context["origin_year"],
        "forecast_horizons": context["horizons"],
        "historical_values": context["history"],
        "comparable_neighborhoods": context["comparables"],
        "metro_rank": context["metro_rank"],
        "metro_total": context["metro_total"],
    }, indent=2)

    resp = client.chat.completions.create(
        model=MODEL_VERSION,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        response_format={"type": "json_object"},
        temperature=0.4,
        max_tokens=800,
    )

    return json.loads(resp.choices[0].message.content)


# ---------------------------------------------------------------------------
# Main Modal function
# ---------------------------------------------------------------------------
@app.function(
    image=image,
    secrets=[supabase_secret, openai_secret],
    timeout=7200,
    memory=512,
)
def generate_batch(tract_geoids: list[str], dry_run: bool = False):
    """Generate narratives for a batch of tract geoids."""
    import psycopg2
    from openai import OpenAI

    db_url = os.environ["SUPABASE_DB_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    client = OpenAI()

    results = {"generated": 0, "skipped": 0, "errors": []}

    for geoid in tract_geoids:
        try:
            ctx = _fetch_tract_context(conn, geoid)
            if not ctx:
                results["skipped"] += 1
                continue

            # Skip outliers
            h5 = next((h for h in ctx["horizons"] if h["horizon_m"] == 60), None)
            if h5:
                appr = h5["appreciation_pct"]
                bp50 = ctx["baseline_p50"]
                if appr > 100 or appr <= -95 or bp50 < 20_000 or bp50 >= 5_000_000:
                    results["skipped"] += 1
                    continue

            geo = _resolve_geo_name(conn, geoid)

            if dry_run:
                print(f"  [DRY] {geoid} → {geo['neighborhood']}, {geo['city']}")
                results["generated"] += 1
                continue

            narrative = _generate_narrative(client, ctx, geo)

            # Upsert into seo_narratives
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO public.seo_narratives (geoid, level, narrative_json, model_version)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (geoid, level) DO UPDATE
                SET narrative_json = EXCLUDED.narrative_json,
                    model_version = EXCLUDED.model_version,
                    created_at = now()
            """, (geoid, "tract", json.dumps(narrative), MODEL_VERSION))

            results["generated"] += 1
            if results["generated"] % 10 == 0:
                print(f"  Generated {results['generated']}/{len(tract_geoids)}")

        except Exception as e:
            results["errors"].append({"geoid": geoid, "error": str(e)})
            print(f"  [ERROR] {geoid}: {e}")

    conn.close()
    return results


# ---------------------------------------------------------------------------
# Also generate state/city level narratives
# ---------------------------------------------------------------------------
@app.function(
    image=image,
    secrets=[supabase_secret, openai_secret],
    timeout=3600,
    memory=512,
)
def generate_aggregate_narratives(
    state_fips: str,
    county_fips: str | None = None,
    dry_run: bool = False,
):
    """Generate state-level and city-level rollup narratives."""
    import psycopg2
    from openai import OpenAI

    db_url = os.environ["SUPABASE_DB_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    client = OpenAI()

    cur = conn.cursor()

    # Build state-level narrative
    cur.execute(f"""
        SELECT AVG(p50) as avg_p50,
               MIN(p50) as min_p50, MAX(p50) as max_p50,
               COUNT(DISTINCT tract_geoid20) as n_tracts,
               AVG(p10) as avg_p10, AVG(p90) as avg_p90
        FROM {FORECAST_SCHEMA}.metrics_tract_forecast
        WHERE tract_geoid20 LIKE %s
          AND origin_year = %s AND horizon_m = 60 AND series_kind = 'forecast'
          AND p50 IS NOT NULL
    """, (f"{state_fips}%", ORIGIN_YEAR))
    state_stats = cur.fetchone()

    if state_stats and state_stats[0]:
        state_data = {
            "avg_median_forecast_5yr": round(state_stats[0]),
            "min_p50": round(state_stats[1]),
            "max_p50": round(state_stats[2]),
            "n_tracts": state_stats[3],
            "avg_spread": round((state_stats[5] or 0) - (state_stats[4] or 0)),
        }

        # Get state name
        STATE_NAMES = {
            "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
            "06": "California", "08": "Colorado", "09": "Connecticut",
            "10": "Delaware", "11": "District of Columbia", "12": "Florida",
            "13": "Georgia", "15": "Hawaii", "16": "Idaho", "17": "Illinois",
            "18": "Indiana", "19": "Iowa", "20": "Kansas", "21": "Kentucky",
            "22": "Louisiana", "23": "Maine", "24": "Maryland",
            "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
            "28": "Mississippi", "29": "Missouri", "30": "Montana",
            "31": "Nebraska", "32": "Nevada", "33": "New Hampshire",
            "34": "New Jersey", "35": "New Mexico", "36": "New York",
            "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
            "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania",
            "44": "Rhode Island", "45": "South Carolina", "46": "South Dakota",
            "47": "Tennessee", "48": "Texas", "49": "Utah", "50": "Vermont",
            "51": "Virginia", "53": "Washington", "54": "West Virginia",
            "55": "Wisconsin", "56": "Wyoming", "72": "Puerto Rico",
        }

        state_name = STATE_NAMES.get(state_fips, f"State {state_fips}")

        if not dry_run:
            user_msg = json.dumps({
                "level": "state",
                "name": state_name,
                "stats": state_data,
            })
            resp = client.chat.completions.create(
                model=MODEL_VERSION,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT.replace(
                        "neighborhood (census tract)",
                        "state with aggregate statistics across all its neighborhoods"
                    )},
                    {"role": "user", "content": user_msg},
                ],
                response_format={"type": "json_object"},
                temperature=0.4,
                max_tokens=800,
            )
            narrative = json.loads(resp.choices[0].message.content)

            cur.execute("""
                INSERT INTO public.seo_narratives (geoid, level, narrative_json, model_version)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (geoid, level) DO UPDATE
                SET narrative_json = EXCLUDED.narrative_json,
                    model_version = EXCLUDED.model_version,
                    created_at = now()
            """, (state_fips, "state", json.dumps(narrative), MODEL_VERSION))
            print(f"  ✓ State narrative for {state_name}")
        else:
            print(f"  [DRY] State {state_name}: {state_data}")

    conn.close()
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
@app.local_entrypoint()
def main(
    state_fips: str = "48",
    county_fips: str = "",
    dry_run: bool = False,
    batch_size: int = BATCH_SIZE,
):
    """
    Fan out narrative generation across Modal containers.

    Examples:
        # All of Texas
        modal run scripts/pipeline/seo/generate_narratives_modal.py --state-fips 48

        # Just Harris County
        modal run scripts/pipeline/seo/generate_narratives_modal.py \
            --state-fips 48 --county-fips 48201
    """
    import psycopg2

    print(f"=== SEO Narrative Generation ===")
    print(f"  State FIPS: {state_fips}")
    print(f"  County FIPS: {county_fips or '(all)'}")
    print(f"  Dry run: {dry_run}")
    print(f"  Schema: {FORECAST_SCHEMA}")
    print()

    # Get list of tracts locally
    db_url = os.environ["SUPABASE_DB_URL"]
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    prefix = county_fips if county_fips else state_fips
    cur.execute(f"""
        SELECT DISTINCT tract_geoid20
        FROM {FORECAST_SCHEMA}.metrics_tract_forecast
        WHERE tract_geoid20 LIKE %s
          AND origin_year IN (%s, %s)
          AND series_kind = 'forecast'
          AND horizon_m = 60
          AND p50 IS NOT NULL
        ORDER BY tract_geoid20
    """, (f"{prefix}%", ORIGIN_YEAR, ORIGIN_YEAR - 1))
    all_tracts = [r[0] for r in cur.fetchall()]
    conn.close()

    print(f"  Found {len(all_tracts)} tracts with forecast data")

    if not all_tracts:
        print("  No tracts found. Exiting.")
        return

    # Split into batches and fan out
    batches = [all_tracts[i:i + batch_size] for i in range(0, len(all_tracts), batch_size)]
    print(f"  Dispatching {len(batches)} batches of ~{batch_size} tracts each")
    print()

    t0 = time.time()

    # Fan out tract-level generation
    results = list(generate_batch.map(
        batches,
        kwargs={"dry_run": dry_run},
    ))

    total_gen = sum(r["generated"] for r in results)
    total_skip = sum(r["skipped"] for r in results)
    total_err = sum(len(r["errors"]) for r in results)

    print(f"\n=== Complete ===")
    print(f"  Generated: {total_gen}")
    print(f"  Skipped:   {total_skip}")
    print(f"  Errors:    {total_err}")
    print(f"  Time:      {time.time() - t0:.1f}s")

    if total_err > 0:
        print("\nErrors:")
        for r in results:
            for e in r["errors"]:
                print(f"  {e['geoid']}: {e['error']}")

    # Also generate state/city aggregate narratives
    print(f"\n=== Generating aggregate narratives ===")
    generate_aggregate_narratives.remote(
        state_fips=state_fips,
        county_fips=county_fips or None,
        dry_run=dry_run,
    )
    print("  Done.")
