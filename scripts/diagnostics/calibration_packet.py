"""
Calibration Packet: Complete Diagnostic Suite
==============================================
Six metric groups to separate measurement failure from model failure.

1. Anchor integrity (forecast anchor ≈ history anchor)
2. Scaler integrity (t_scaler.scale_ per origin)  — requires checkpoint; proxied here
3. Step vs cumulative dispersion (both, side by side)
4. Within-fan vs cross-geo decomposition
5. PI coverage per horizon per origin
6. Baseline comparison (random walk)

Uses ZCTA aggregate tables for speed.
Results saved to /tmp/calibration_packet.txt
"""
import os, sys, io, warnings, math
import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")

import psycopg2
from scipy.stats import skew, kurtosis, ks_2samp

DB_URL = os.environ.get("SUPABASE_DB_URL", "")
SCHEMA = "forecast_20260220_7f31c6e4"
OUT = io.StringIO()

def p(*a, **kw):
    print(*a, **kw, file=OUT)
    print(*a, **kw)

def q(sql, params=None):
    conn = psycopg2.connect(DB_URL); conn.autocommit = True
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

# ═══════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════
p("Loading data...")

hist = q(f"""
    SELECT zcta5, year, value, p50, n
    FROM "{SCHEMA}".metrics_zcta_history
    WHERE value > 0 AND n >= 5
    ORDER BY zcta5, year
""")
p(f"  History: {len(hist):,} rows, {hist['zcta5'].nunique()} ZCTAs, "
  f"years {hist['year'].min()}-{hist['year'].max()}")

fcst = q(f"""
    SELECT zcta5, origin_year, horizon_m, forecast_year, value, p10, p25, p50, p75, p90, n
    FROM "{SCHEMA}".metrics_zcta_forecast
    WHERE value > 0 AND n >= 5
    ORDER BY zcta5, origin_year, horizon_m
""")
fcst["horizon_yr"] = (fcst["horizon_m"] / 12).astype(int)
origins = sorted(fcst["origin_year"].unique())
p(f"  Forecast: {len(fcst):,} rows, origins={origins}")

hist = hist.sort_values(["zcta5", "year"])

# ═══════════════════════════════════════════════════════════
# METRIC 1: ANCHOR INTEGRITY
# ═══════════════════════════════════════════════════════════
p(f"\n{'='*80}")
p(f"1. ANCHOR INTEGRITY")
p(f"    Does forecast anchor (value at h=0) match history value at origin_year?")
p(f"{'='*80}")

for oy in origins:
    # Get the h=1 forecast anchor: the implied starting value.
    # For value-level forecasts: anchor ≈ history.value at origin_year
    # We compare forecast.value at h=1 vs history.value at forecast_year=origin_year+1
    # But more directly: compare the ZCTA history at origin_year vs implied anchor.
    #
    # Since DB stores price levels (not log), the "anchor" is just the history value
    # at origin_year. The forecast at h=1 should be related to it.
    fh1 = fcst[(fcst["origin_year"] == oy) & (fcst["horizon_yr"] == 1)]
    h_oy = hist[hist["year"] == oy][["zcta5", "value"]].rename(
        columns={"value": "hist_anchor"})

    merged = fh1.merge(h_oy, on="zcta5", how="inner")
    if merged.empty:
        p(f"\n  origin={oy}: no matched ZCTAs")
        continue

    # Implied growth = (forecast_p50 / history_anchor) - 1
    merged["implied_g"] = (merged["p50"] - merged["hist_anchor"]) / merged["hist_anchor"]
    merged["log_ratio"] = np.log(merged["p50"].clip(1) / merged["hist_anchor"].clip(1))

    n = len(merged)
    abs_log_ratio = merged["log_ratio"].abs()
    p(f"\n  origin={oy}  n={n}")
    p(f"    |log(fcst_p50/hist)| median={abs_log_ratio.median():.4f}  "
      f"p95={abs_log_ratio.quantile(0.95):.4f}  max={abs_log_ratio.max():.4f}")
    p(f"    implied_growth mean={merged['implied_g'].mean():.4f}  "
      f"median={merged['implied_g'].median():.4f}  "
      f"std={merged['implied_g'].std():.4f}")

    # Flag: how many ZCTAs have anchor mismatch > 0.5 in log-space (i.e., >65% ratio)
    n_bad = int((abs_log_ratio > 0.5).sum())
    n_extreme = int((abs_log_ratio > 1.0).sum())
    if n_bad > 0:
        p(f"    WARNING: {n_bad}/{n} ZCTAs with |log_ratio|>0.5, "
          f"{n_extreme} with |log_ratio|>1.0")
    else:
        p(f"    OK: all anchors within 0.5 in log-space")

# ═══════════════════════════════════════════════════════════
# METRIC 2: SCALER INTEGRITY (proxy from output magnitudes)
# ═══════════════════════════════════════════════════════════
p(f"\n{'='*80}")
p(f"2. SCALER INTEGRITY (proxy: output value magnitudes per origin)")
p(f"    True test requires checkpoint; here we proxy from DB values")
p(f"{'='*80}")

p(f"\n  {'origin':>6} {'n':>6} {'p50_med':>10} {'p50_p95':>10} {'p50_max':>12} "
  f"{'p90-p10_med':>12} {'p90-p10_p95':>12}")
p(f"  {'─'*6} {'─'*6} {'─'*10} {'─'*10} {'─'*12} {'─'*12} {'─'*12}")

for oy in origins:
    f_oy = fcst[(fcst["origin_year"] == oy) & (fcst["horizon_yr"] == 1)]
    if f_oy.empty:
        continue
    fan = f_oy["p90"] - f_oy["p10"]
    p(f"  {oy:>6} {len(f_oy):>6} {f_oy['p50'].median():>10.0f} "
      f"{f_oy['p50'].quantile(0.95):>10.0f} {f_oy['p50'].max():>12.0f} "
      f"{fan.median():>12.0f} {fan.quantile(0.95):>12.0f}")

# Flag: if any origin's p50 max is >10× the median of other origins
medians = {}
for oy in origins:
    f_oy = fcst[(fcst["origin_year"] == oy) & (fcst["horizon_yr"] == 1)]
    if not f_oy.empty:
        medians[oy] = f_oy['p50'].median()

if medians:
    overall_med = np.median(list(medians.values()))
    for oy, med in medians.items():
        ratio = med / max(overall_med, 1)
        if ratio > 5 or ratio < 0.2:
            p(f"\n  ALERT: origin {oy} median p50 ({med:.0f}) is {ratio:.1f}x "
              f"the cross-origin median ({overall_med:.0f})")

# ═══════════════════════════════════════════════════════════
# METRIC 3: STEP VS CUMULATIVE DISPERSION
# ═══════════════════════════════════════════════════════════
p(f"\n{'='*80}")
p(f"3. STEP VS CUMULATIVE DISPERSION")
p(f"    Model target = step delta (y_curr - y_prev).")
p(f"    Compare std of STEP deltas vs std of CUMULATIVE deltas.")
p(f"{'='*80}")

# Historical: compute BOTH step and cumulative
for lag_type in ["step", "cumulative"]:
    p(f"\n  --- Historical ({lag_type}) ---")
    p(f"  {'h':>4} {'n':>8} {'mean':>10} {'std':>10} {'skew':>8} {'kurt':>8} {'p05':>10} {'p95':>10}")
    for h in [1, 2, 3, 4, 5]:
        if lag_type == "step":
            # Step: one-year change from h-1 to h
            if h == 1:
                shifted = hist.groupby("zcta5")["value"].shift(1)
                vals = ((hist["value"] - shifted) / shifted).dropna().values
            else:
                # year-to-year: value[t+h] vs value[t+h-1]
                # We construct this from the panel
                sdf = hist.copy()
                sdf["prev"] = sdf.groupby("zcta5")["value"].shift(1)
                vals = ((sdf["value"] - sdf["prev"]) / sdf["prev"]).dropna().values
                # Note: step deltas are the same distribution for all h (stationary assumption)
                # So we only really need h=1 step stats
                if h > 1:
                    continue
        else:
            # Cumulative: value[t+h] vs value[t]
            shifted = hist.groupby("zcta5")["value"].shift(h)
            vals = ((hist["value"] - shifted) / shifted).dropna().values

        vals = vals[np.isfinite(vals)]
        if len(vals) < 30:
            continue
        p(f"  {h:>4} {len(vals):>8,} {np.mean(vals):>10.4f} {np.std(vals):>10.4f} "
          f"{skew(vals):>8.3f} {kurtosis(vals):>8.3f} "
          f"{np.percentile(vals, 5):>10.4f} {np.percentile(vals, 95):>10.4f}")

# Forecast: step vs cumulative using quantile spread as proxy
p(f"\n  --- Forecast (cross-geo dispersion of p50, by horizon) ---")
p(f"  {'h':>4} {'n_geo':>8} {'mean_p50':>10} {'std_p50':>10} {'note'}")
for oy in origins:
    p(f"\n  origin={oy}")
    h_oy = hist[hist["year"] == oy][["zcta5", "value"]].rename(
        columns={"value": "anchor"})
    foy = fcst[fcst["origin_year"] == oy].merge(h_oy, on="zcta5", how="inner")

    for h in [1, 2, 3, 4, 5]:
        fh = foy[foy["horizon_yr"] == h]
        if fh.empty or len(fh) < 10:
            continue
        # STEP growth: forecast at h vs forecast at h-1 (or anchor if h=1)
        if h == 1:
            step_g = (fh["p50"] - fh["anchor"]) / fh["anchor"]
        else:
            fh_prev = foy[foy["horizon_yr"] == h - 1][["zcta5", "p50"]].rename(
                columns={"p50": "prev_p50"})
            fh_m = fh.merge(fh_prev, on="zcta5", how="inner")
            if fh_m.empty:
                continue
            step_g = (fh_m["p50"] - fh_m["prev_p50"]) / fh_m["prev_p50"]

        # CUMULATIVE growth: forecast at h vs anchor (anchor already in fh from foy merge)
        cum_g = (fh["p50"] - fh["anchor"]) / fh["anchor"]

        step_g = step_g.dropna()
        cum_g = cum_g.dropna()

        note = "step" if h == 1 else "step (h vs h-1)"
        if len(step_g) >= 10:
            p(f"  h={h} step:  n={len(step_g):>5} mean={step_g.mean():>+.4f} "
              f"std={step_g.std():>.4f} p05={step_g.quantile(0.05):>+.4f} "
              f"p95={step_g.quantile(0.95):>+.4f}")
        if len(cum_g) >= 10:
            p(f"  h={h} cumul: n={len(cum_g):>5} mean={cum_g.mean():>+.4f} "
              f"std={cum_g.std():>.4f} p05={cum_g.quantile(0.05):>+.4f} "
              f"p95={cum_g.quantile(0.95):>+.4f}")

# ═══════════════════════════════════════════════════════════
# METRIC 4: WITHIN-FAN VS CROSS-GEO DECOMPOSITION
# ═══════════════════════════════════════════════════════════
p(f"\n{'='*80}")
p(f"4. WITHIN-FAN VS CROSS-GEO DECOMPOSITION")
p(f"    within-fan: avg over geos of (p90-p10)/p50 per geo")
p(f"    cross-geo: std of p50 across geos")
p(f"    (Without raw scenarios, within-fan is proxied from quantiles)")
p(f"{'='*80}")

for oy in origins:
    p(f"\n  origin={oy}")
    p(f"  {'h':>4} {'within_fan_med':>15} {'within_fan_p95':>15} "
      f"{'cross_geo_std':>15} {'cross_geo_cv':>12}")

    h_oy = hist[hist["year"] == oy][["zcta5", "value"]].rename(
        columns={"value": "anchor"})

    for h in [1, 2, 3, 4, 5]:
        fh = fcst[(fcst["origin_year"] == oy) & (fcst["horizon_yr"] == h)]
        fh = fh.merge(h_oy, on="zcta5", how="inner")
        if len(fh) < 10:
            continue

        # Within-fan: (p90 - p10) / anchor per geo (proxy for scenario spread)
        within_fan = (fh["p90"] - fh["p10"]) / fh["anchor"]

        # Cross-geo: std of (p50 / anchor) across geos
        cross_geo_g = (fh["p50"] - fh["anchor"]) / fh["anchor"]

        p(f"  {h:>4} {within_fan.median():>15.4f} {within_fan.quantile(0.95):>15.4f} "
          f"{cross_geo_g.std():>15.4f} {cross_geo_g.std() / max(abs(cross_geo_g.mean()), 1e-6):>12.2f}")

# ═══════════════════════════════════════════════════════════
# METRIC 5: PI COVERAGE PER HORIZON PER ORIGIN
# ═══════════════════════════════════════════════════════════
p(f"\n{'='*80}")
p(f"5. PI COVERAGE (does actual fall within forecast interval?)")
p(f"    Requires actuals at forecast_year. Using history as actuals.")
p(f"{'='*80}")

for oy in origins:
    p(f"\n  origin={oy}")
    p(f"  {'h':>4} {'n':>6} {'cov_80(p10-p90)':>16} {'cov_50(p25-p75)':>16} {'bias (median)':>14}")

    for h in [1, 2, 3, 4, 5]:
        fy = int(oy) + h
        fh = fcst[(fcst["origin_year"] == oy) & (fcst["horizon_yr"] == h)]
        actual = hist[hist["year"] == fy][["zcta5", "value"]].rename(
            columns={"value": "actual"})

        merged = fh.merge(actual, on="zcta5", how="inner")
        if len(merged) < 10:
            continue

        in_80 = ((merged["actual"] >= merged["p10"]) &
                 (merged["actual"] <= merged["p90"])).mean()
        in_50 = ((merged["actual"] >= merged["p25"]) &
                 (merged["actual"] <= merged["p75"])).mean()
        bias = (merged["p50"] - merged["actual"]).median()
        bias_pct = bias / merged["actual"].median() * 100

        flag_80 = "LOW" if in_80 < 0.70 else "HIGH" if in_80 > 0.90 else "ok"
        flag_50 = "LOW" if in_50 < 0.40 else "HIGH" if in_50 > 0.60 else "ok"

        p(f"  {h:>4} {len(merged):>6} {in_80:>12.1%} [{flag_80:>4}] "
          f"{in_50:>12.1%} [{flag_50:>4}] {bias_pct:>+12.1f}%")

# ═══════════════════════════════════════════════════════════
# METRIC 6: BASELINE COMPARISON (random walk)
# ═══════════════════════════════════════════════════════════
p(f"\n{'='*80}")
p(f"6. BASELINE COMPARISON (random walk / persistence)")
p(f"    Persistence = forecast_value = last_known_value")
p(f"    RW = value +/- historical_std")
p(f"{'='*80}")

# Compute historical step std (one-year)
hist_step = hist.copy()
hist_step["prev_val"] = hist_step.groupby("zcta5")["value"].shift(1)
hist_step["step_g"] = (hist_step["value"] - hist_step["prev_val"]) / hist_step["prev_val"]
hist_step_std = hist_step["step_g"].dropna().std()
hist_step_mean = hist_step["step_g"].dropna().mean()
p(f"\n  Historical step growth: mean={hist_step_mean:.4f} std={hist_step_std:.4f}")

for oy in origins:
    p(f"\n  origin={oy}")
    p(f"  {'h':>4} {'n':>6} {'model_MAE':>10} {'persist_MAE':>11} {'rw_MAE':>10} {'model_wins':>11}")

    for h in [1, 2, 3, 4, 5]:
        fy = int(oy) + h
        fh = fcst[(fcst["origin_year"] == oy) & (fcst["horizon_yr"] == h)]
        # Anchor
        h_oy = hist[hist["year"] == oy][["zcta5", "value"]].rename(
            columns={"value": "anchor"})
        actual = hist[hist["year"] == fy][["zcta5", "value"]].rename(
            columns={"value": "actual"})

        merged = fh.merge(h_oy, on="zcta5").merge(actual, on="zcta5", how="inner")
        if len(merged) < 10:
            continue

        # Model error
        model_err = (merged["p50"] - merged["actual"]).abs()
        # Persistence: just use anchor
        persist_err = (merged["anchor"] - merged["actual"]).abs()
        # Random walk: anchor * (1 + mean_step_g * h) as point forecast
        rw_fcst = merged["anchor"] * (1 + hist_step_mean * h)
        rw_err = (rw_fcst - merged["actual"]).abs()

        model_wins = (model_err < persist_err).mean()

        p(f"  {h:>4} {len(merged):>6} {model_err.median():>10.0f} "
          f"{persist_err.median():>11.0f} {rw_err.median():>10.0f} "
          f"{model_wins:>10.1%}")

# ═══════════════════════════════════════════════════════════
# HORIZON SCALING: step vs cumulative side by side
# ═══════════════════════════════════════════════════════════
p(f"\n{'='*80}")
p(f"HORIZON SCALING SUMMARY")
p(f"  Step std should be FLAT across h (each is one year's delta)")
p(f"  Cumulative std should grow ~sqrt(h)")
p(f"{'='*80}")

# Forecast: compute step and cumulative std per origin
for oy in origins:
    h_oy = hist[hist["year"] == oy][["zcta5", "value"]].rename(columns={"value": "anchor"})
    foy = fcst[fcst["origin_year"] == oy].merge(h_oy, on="zcta5", how="inner")
    if len(foy) < 50:
        continue

    p(f"\n  origin={oy}")
    p(f"  {'h':>4} {'step_std':>10} {'cum_std':>10} {'cum/h1':>8} {'sqrt(h)':>8}")

    cum_std_h1 = None
    for h in [1, 2, 3, 4, 5]:
        fh = foy[foy["horizon_yr"] == h]
        if len(fh) < 10:
            continue

        # Cumulative growth
        cum_g = ((fh["p50"] - fh["anchor"]) / fh["anchor"]).dropna()

        # Step growth
        if h == 1:
            step_g = cum_g  # same for h=1
        else:
            fh_prev = foy[foy["horizon_yr"] == h - 1][["zcta5", "p50"]].rename(
                columns={"p50": "prev_p50"})
            fh_m = fh.merge(fh_prev, on="zcta5", how="inner")
            if fh_m.empty:
                continue
            step_g = ((fh_m["p50"] - fh_m["prev_p50"]) / fh_m["prev_p50"]).dropna()

        s_std = step_g.std() if len(step_g) > 0 else float('nan')
        c_std = cum_g.std() if len(cum_g) > 0 else float('nan')

        if cum_std_h1 is None and np.isfinite(c_std) and c_std > 0:
            cum_std_h1 = c_std

        ratio = c_std / cum_std_h1 if cum_std_h1 and cum_std_h1 > 0 else float('nan')

        p(f"  {h:>4} {s_std:>10.4f} {c_std:>10.4f} {ratio:>8.2f} {math.sqrt(h):>8.2f}")

p(f"\nDone.")

# Save
with open("/tmp/calibration_packet.txt", "w", encoding="utf-8") as f:
    f.write(OUT.getvalue())
print(f"\nSaved to /tmp/calibration_packet.txt")
