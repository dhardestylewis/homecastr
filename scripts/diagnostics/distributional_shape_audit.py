"""
Distributional Shape Audit: Forecast vs Historical Growth (FAST)
================================================================
Uses ZCTA-level aggregate tables for speed.
Writes results to /tmp/audit_results.txt
"""
import os, sys, io, warnings
import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")

import psycopg2
from scipy.stats import skew, kurtosis, ks_2samp, wasserstein_distance, gaussian_kde, iqr as scipy_iqr

DB_URL = os.environ.get("SUPABASE_DB_URL", "")
SCHEMA = "forecast_20260220_7f31c6e4"
OUT = io.StringIO()

def p(*args, **kwargs):
    print(*args, **kwargs, file=OUT)
    print(*args, **kwargs)

def q(sql, params=None):
    conn = psycopg2.connect(DB_URL); conn.autocommit = True
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df

# ── 1. Load data ──
p("Loading ZCTA-level data...")

hist = q(f"""
    SELECT zcta5, year, value, p50, n
    FROM "{SCHEMA}".metrics_zcta_history
    WHERE value > 0 AND n >= 5
    ORDER BY zcta5, year
""")
p(f"  History: {len(hist):,} rows, {hist['zcta5'].nunique()} ZCTAs, years {hist['year'].min()}-{hist['year'].max()}")

fcst = q(f"""
    SELECT zcta5, origin_year, horizon_m, forecast_year, value, p10, p25, p50, p75, p90, n
    FROM "{SCHEMA}".metrics_zcta_forecast
    WHERE value > 0 AND n >= 5
    ORDER BY zcta5, origin_year, horizon_m
""")
p(f"  Forecast: {len(fcst):,} rows, origins={sorted(fcst['origin_year'].unique())}")

# ── 2. Compute historical growth rates ──
hist = hist.sort_values(["zcta5", "year"])
for lag in [1, 2, 3, 4, 5]:
    shifted = hist.groupby("zcta5")["value"].shift(lag)
    hist[f"g{lag}yr"] = (hist["value"] - shifted) / shifted

# ── 3. Compute forecast implied growth ──
fcst["horizon_yr"] = (fcst["horizon_m"] / 12).astype(int)
origins = sorted(fcst["origin_year"].unique())
anchor_dfs = []
for oy in origins:
    a = hist[hist["year"] == oy][["zcta5", "value"]].rename(columns={"value": "anchor"})
    a["origin_year"] = oy
    anchor_dfs.append(a)
anchors = pd.concat(anchor_dfs, ignore_index=True)
fcst = fcst.merge(anchors, on=["zcta5", "origin_year"], how="inner")
for col in ["p10", "p25", "p50", "p75", "p90", "value"]:
    fcst[f"{col}_g"] = (fcst[col] - fcst["anchor"]) / fcst["anchor"]
p(f"  Matched {len(fcst):,} forecast rows with anchors")

# ── 4. Shape stats ──
def shape(x, label=""):
    x = x[np.isfinite(x)]
    if len(x) < 30:
        return {"label": label, "n": len(x), "note": "too few"}
    p75 = np.percentile(x, 75)
    p25 = np.percentile(x, 25)
    s = {
        "label": label, "n": len(x),
        "mean": np.mean(x), "median": np.median(x),
        "std": np.std(x), "iqr": scipy_iqr(x),
        "skew": skew(x), "kurt": kurtosis(x),
        "p01": np.percentile(x,1), "p05": np.percentile(x,5),
        "p10": np.percentile(x,10), "p90": np.percentile(x,90),
        "p95": np.percentile(x,95), "p99": np.percentile(x,99),
        "frac_gt50": np.mean(x > 0.5),
        "frac_lt30": np.mean(x < -0.3),
    }
    s["iqr_std"] = s["iqr"] / max(s["std"], 1e-10)
    s["up_tail"] = (s["p99"]-s["p90"]) / max(s["p90"]-p75, 1e-10)
    s["lo_tail"] = (s["p10"]-s["p01"]) / max(p25-s["p10"], 1e-10)
    try:
        kde = gaussian_kde(x, bw_method='silverman')
        xg = np.linspace(np.percentile(x,0.5), np.percentile(x,99.5), 500)
        yg = kde(xg)
        s["modes"] = int(np.sum(np.diff(np.sign(np.diff(yg))) < 0))
    except:
        s["modes"] = -1
    return s

# ── 5. Run audit ──
p(f"\n{'='*80}")
p(f"DISTRIBUTIONAL SHAPE AUDIT -- ZCTA-Level Value Growth")
p(f"{'='*80}")

results = []
for h in [1, 2, 3, 4, 5]:
    p(f"\n--- HORIZON = {h} year(s) ---")

    hg = hist[f"g{h}yr"].dropna().values
    hs = shape(hg, f"hist_{h}yr")
    fg = fcst[fcst["horizon_yr"] == h]["p50_g"].dropna().values
    fs = shape(fg, f"fcst_h{h}")

    if hs.get("note") or fs.get("note"):
        p(f"  Insufficient data: hist n={hs.get('n',0)}, fcst n={fs.get('n',0)}")
        continue

    ks_stat, ks_p = ks_2samp(hg[np.isfinite(hg)], fg[np.isfinite(fg)])
    wd = wasserstein_distance(hg[np.isfinite(hg)], fg[np.isfinite(fg)])
    var_ratio = fs["std"]**2 / max(hs["std"]**2, 1e-10)

    fmt = lambda v: f"{v:.4f}" if isinstance(v, float) else f"{v}"

    p(f"  {'Metric':<16} {'Hist':>10} {'Fcst':>10}  Note")
    p(f"  {'-'*16} {'-'*10} {'-'*10}  {'-'*20}")
    p(f"  {'n':<16} {hs['n']:>10,} {fs['n']:>10,}")
    p(f"  {'mean':<16} {hs['mean']:>10.4f} {fs['mean']:>10.4f}")
    p(f"  {'median':<16} {hs['median']:>10.4f} {fs['median']:>10.4f}")
    p(f"  {'std':<16} {hs['std']:>10.4f} {fs['std']:>10.4f}  VarRatio={var_ratio:.2f} {'[wide]' if var_ratio>2 else '[narrow]' if var_ratio<0.3 else '[ok]'}")
    p(f"  {'IQR':<16} {hs['iqr']:>10.4f} {fs['iqr']:>10.4f}")
    p(f"  {'IQR/std':<16} {hs['iqr_std']:>10.2f} {fs['iqr_std']:>10.2f}  (Gauss=1.35)")
    p(f"  {'skewness':<16} {hs['skew']:>10.3f} {fs['skew']:>10.3f}  {'[less skew]' if abs(fs['skew'])<abs(hs['skew']) else '[more skew]'}")
    p(f"  {'excess kurt':<16} {hs['kurt']:>10.3f} {fs['kurt']:>10.3f}  {'[thinner tail]' if fs['kurt']<hs['kurt'] else '[heavier tail]'}")
    p(f"  {'modes':<16} {hs['modes']:>10} {fs['modes']:>10}")
    p(f"  {'p01':<16} {hs['p01']:>10.4f} {fs['p01']:>10.4f}")
    p(f"  {'p05':<16} {hs['p05']:>10.4f} {fs['p05']:>10.4f}")
    p(f"  {'p95':<16} {hs['p95']:>10.4f} {fs['p95']:>10.4f}")
    p(f"  {'p99':<16} {hs['p99']:>10.4f} {fs['p99']:>10.4f}")
    p(f"  {'>50% growth':<16} {hs['frac_gt50']:>10.3%} {fs['frac_gt50']:>10.3%}")
    p(f"  {'<-30% loss':<16} {hs['frac_lt30']:>10.3%} {fs['frac_lt30']:>10.3%}")
    p(f"")
    p(f"  KS={ks_stat:.4f} (p={ks_p:.2e})  Wass={wd:.5f}  VarRatio={var_ratio:.3f}")

    results.append({"h":h, "ks":ks_stat, "wd":wd, "vr":var_ratio,
                     "sk_h":hs['skew'], "sk_f":fs['skew'],
                     "ku_h":hs['kurt'], "ku_f":fs['kurt'],
                     "sd_h":hs['std'], "sd_f":fs['std']})

# ── 6. Horizon scaling ──
if len(results) >= 2:
    p(f"\n{'='*80}")
    p(f"HORIZON SCALING (does uncertainty grow with sqrt(t)?)")
    p(f"{'='*80}")
    stds_f = [r["sd_f"] for r in results]
    stds_h = [r["sd_h"] for r in results]
    if stds_f[0] > 0 and stds_h[0] > 0:
        ratio_f = [s/stds_f[0] for s in stds_f]
        ratio_h = [s/stds_h[0] for s in stds_h]
        sqrt_t = [np.sqrt(h) for h in range(1, len(ratio_f)+1)]
        p(f"  h   sd_hist   sd_fcst  hist/h1  fcst/h1  sqrt(t)")
        for i in range(len(ratio_f)):
            p(f"  {i+1}   {stds_h[i]:.4f}   {stds_f[i]:.4f}   {ratio_h[i]:.2f}     {ratio_f[i]:.2f}     {sqrt_t[i]:.2f}")
        if ratio_f[-1] < sqrt_t[-1] * 0.7:
            p(f"\n  -> Sub-sqrt(t): model implies MEAN-REVERSION")
        elif ratio_f[-1] > sqrt_t[-1] * 1.3:
            p(f"\n  -> Super-sqrt(t): model implies MOMENTUM")
        else:
            p(f"\n  -> Approx sqrt(t) scaling (random walk)")

# ── 7. Summary table ──
p(f"\n{'='*80}")
p(f"SUMMARY")
p(f"{'='*80}")
p(f"  h    KS      Wass     VarR   skew_h  skew_f  kurt_h  kurt_f")
for r in results:
    p(f"  {r['h']}  {r['ks']:.4f}  {r['wd']:.5f}  {r['vr']:.3f}  "
      f"{r['sk_h']:+.3f}  {r['sk_f']:+.3f}  {r['ku_h']:+.3f}  {r['ku_f']:+.3f}")

# ── 8. Per-origin ──
p(f"\nPER-ORIGIN (h=1)")
for oy in sorted(fcst["origin_year"].unique()):
    fg = fcst[(fcst["origin_year"]==oy)&(fcst["horizon_yr"]==1)]["p50_g"].dropna().values
    if len(fg) < 20: continue
    p(f"  o={oy} n={len(fg):>4} mean={np.mean(fg):+.4f} std={np.std(fg):.4f} "
      f"skew={skew(fg):+.3f} kurt={kurtosis(fg):+.3f}")

p(f"\nDone.")

# Write to file
with open("/tmp/dist_audit.txt", "w", encoding="utf-8") as f:
    f.write(OUT.getvalue())
print(f"\nResults saved to /tmp/dist_audit.txt")
