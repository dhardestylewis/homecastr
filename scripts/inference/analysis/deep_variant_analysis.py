"""
Cell 2b: Deep Variant Analysis — Per-Origin Breakdown + PIT Calibration
========================================================================
Run AFTER Cell 2 (compare_checkpoint_variants.py).

Uses the in-memory `variant_raw_results` dict and `actual_vals` from Cell 2.
If those aren't available, loads from the saved pickle.

Outputs:
  1. Per-origin $1M+ metrics table (ρ consistency check)
  2. PIT histogram (calibration diagnostic)
  3. Scaler range comparison
  4. Per-bracket ρ by origin heatmap (text-based)
"""

import os
import time
import numpy as np
from scipy.stats import spearmanr

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════
ORIGINS = [2021, 2022, 2023, 2024]
VARIANTS = ["baseline", "SF200K", "SF500K", "SFstrat200K"]

VALUE_BRACKETS = [
    ("<200K",   0,        200_000),
    ("200K-500K", 200_000, 500_000),
    ("500K-1M", 500_000,  1_000_000),
    ("1M+",     1_000_000, 1e18),
]

# ═══════════════════════════════════════════════════════════════════
# LOAD DATA (from Cell 2 in-memory, or from pickle)
# ═══════════════════════════════════════════════════════════════════
_raw = globals().get("variant_raw_results")
_actuals = globals().get("actual_vals")

if _raw is None:
    print("⚠️  variant_raw_results not in memory — loading from pickle")
    import pickle
    _out_dir = globals().get("OUT_DIR", "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel")
    _results_dir = os.path.join(_out_dir, "hypothesis_results")
    # Find latest pickle
    _pkls = sorted([f for f in os.listdir(_results_dir) if f.startswith("variant_raw_") and f.endswith(".pkl")])
    if not _pkls:
        raise RuntimeError(f"No raw result pickles found in {_results_dir}")
    _pkl_path = os.path.join(_results_dir, _pkls[-1])
    print(f"   Loading: {_pkl_path}")
    with open(_pkl_path, "rb") as f:
        _raw = pickle.load(f)
    print(f"   Loaded {len(_raw)} entries")

if _actuals is None:
    raise RuntimeError("actual_vals not in memory — run Cell 2 first (or load HCAD actuals manually)")

print(f"📊 Deep Variant Analysis")
print(f"   {len(_raw)} variant×origin combos available")
print(f"   Actuals available for years: {sorted(_actuals.keys())}")


# ═══════════════════════════════════════════════════════════════════
# 1. PER-ORIGIN $1M+ BREAKDOWN (ρ consistency check)
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("🔬 PER-ORIGIN $1M+ BREAKDOWN")
print("   Checks whether ρ > 0 is consistent across origins or noise from one origin")
print("=" * 120)

# Header
print(f"\n  {'Variant':<14} {'Origin':>6} {'Hold':>4}  {'n':>5} {'MedGrw':>8} {'%Neg':>6} {'MdAE':>7} {'MAE':>8} "
      f"{'Covg':>6} {'ρ':>7}  {'ρ_p':>7}")
print(f"  {'─'*14} {'─'*6} {'─'*4}  {'─'*5} {'─'*8} {'─'*6} {'─'*7} {'─'*8} {'─'*6} {'─'*7}  {'─'*7}")

# Collect per-variant ρ arrays for consistency analysis
variant_rho_by_origin = {}

for vname in VARIANTS:
    rho_origins = []
    
    for origin in ORIGINS:
        key = (vname, origin)
        if key not in _raw:
            continue

        r = _raw[key]
        accts = r["accts"]
        base_val = r["base_val"]
        y_anchor = r["y_anchor"]
        y_levels = r["y_levels"]  # [N, S, H]
        
        base_v = _actuals.get(origin, {})
        
        # For each hold-out horizon that has actuals
        for hold in range(1, 6):
            eyr = origin + hold
            if eyr not in _actuals:
                continue
            future_v = _actuals[eyr]
            
            # Get $1M+ parcels
            bkt_idx = []
            for i in range(len(accts)):
                acct = str(accts[i]).strip()
                v = base_v.get(acct, 0)
                if v >= 1_000_000:
                    bkt_idx.append(i)
            
            if len(bkt_idx) < 5:
                continue
            
            # Median predicted growth
            h_log = np.nanmedian(y_levels[bkt_idx, :, hold - 1], axis=1)
            h_growth = np.expm1(h_log - y_anchor[bkt_idx]) * 100
            
            # Match against actuals
            pred_matched = []
            actual_matched = []
            fan_hits = 0
            fan_total = 0
            
            for i in bkt_idx:
                acct = str(accts[i]).strip()
                bv = base_v.get(acct, 0)
                av = future_v.get(acct)
                if bv > 0 and av is not None:
                    actual_g = (av - bv) / bv * 100
                    pred_g = np.expm1(np.nanmedian(y_levels[i, :, hold - 1]) - y_anchor[i]) * 100
                    pred_matched.append(pred_g)
                    actual_matched.append(actual_g)
                    
                    # Fan coverage
                    h_lev = y_levels[i, :, hold - 1]
                    p10 = np.exp(np.nanpercentile(h_lev, 10))
                    p90 = np.exp(np.nanpercentile(h_lev, 90))
                    fan_total += 1
                    if p10 <= av <= p90:
                        fan_hits += 1
            
            pred_matched = np.array(pred_matched)
            actual_matched = np.array(actual_matched)
            
            if len(pred_matched) < 10:
                continue
            
            rho, rho_p = spearmanr(pred_matched, actual_matched)
            med_grw = float(np.median(h_growth))
            pct_neg = float(np.mean(h_growth < 0) * 100)
            mdae = float(np.median(np.abs(pred_matched - actual_matched)))
            mae = float(np.mean(np.abs(pred_matched - actual_matched)))
            covg = (fan_hits / fan_total * 100) if fan_total > 0 else float('nan')
            
            rho_origins.append(rho)
            
            rho_s = f"{rho:>+6.3f}" if not np.isnan(rho) else "    —"
            rho_p_s = f"{rho_p:>6.4f}" if not np.isnan(rho_p) else "    —"
            
            print(f"  {vname:<14} {origin:>6} {hold:>4}  {len(bkt_idx):>5} {med_grw:>+7.1f}% "
                  f"{pct_neg:>5.1f}% {mdae:>6.1f}% {mae:>7.1f}% {covg:>5.1f}% "
                  f"{rho_s}  {rho_p_s}")
    
    variant_rho_by_origin[vname] = rho_origins
    if rho_origins:
        print(f"  {'':>14} {'AVG':>6} {'':>4}  {'':>5} {'':>8} {'':>6} {'':>7} {'':>8} {'':>6} "
              f"{np.mean(rho_origins):>+6.3f}  (n={len(rho_origins)} obs)")
    print()


# ═══════════════════════════════════════════════════════════════════
# 2. ρ CONSISTENCY SUMMARY
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("📈 ρ CONSISTENCY SUMMARY ($1M+ bracket, all origin×horizon combos)")
print("=" * 80)

for vname in VARIANTS:
    rhos = variant_rho_by_origin.get(vname, [])
    if not rhos:
        print(f"  {vname:<14}  no data")
        continue
    rhos = np.array(rhos)
    n_pos = np.sum(rhos > 0)
    n_neg = np.sum(rhos < 0)
    mean_rho = np.mean(rhos)
    std_rho = np.std(rhos)
    
    # One-sample t-test: is mean ρ significantly > 0?
    from scipy.stats import ttest_1samp
    if len(rhos) >= 3:
        t_stat, t_p = ttest_1samp(rhos, 0)
        sig = "✅ sig" if t_p < 0.05 and mean_rho > 0 else "❌ not sig"
    else:
        t_p = float('nan')
        sig = "too few obs"
    
    print(f"  {vname:<14}  mean={mean_rho:>+.3f}  std={std_rho:.3f}  "
          f"+:{n_pos} -:{n_neg}  t-test p={t_p:.3f}  {sig}")


# ═══════════════════════════════════════════════════════════════════
# 3. PIT HISTOGRAM (Probability Integral Transform)
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 120)
print("📊 PIT HISTOGRAM — Fan Chart Calibration Diagnostic")
print("   Perfect calibration = uniform PIT (each bin ≈ 10%)")
print("   U-shaped = fans too narrow (tails surprised)  |  ∩-shaped = fans too wide (overconfident middle)")
print("=" * 120)

PIT_BINS = 10

for vname in VARIANTS:
    pit_values = []
    
    for origin in ORIGINS:
        key = (vname, origin)
        if key not in _raw:
            continue
        
        r = _raw[key]
        accts = r["accts"]
        base_val = r["base_val"]
        y_anchor = r["y_anchor"]
        y_levels = r["y_levels"]  # [N, S, H]
        
        base_v = _actuals.get(origin, {})
        
        for hold in range(1, 6):
            eyr = origin + hold
            if eyr not in _actuals:
                continue
            future_v = _actuals[eyr]
            
            for i in range(len(accts)):
                acct = str(accts[i]).strip()
                bv = base_v.get(acct, 0)
                av = future_v.get(acct)
                
                if bv <= 0 or av is None:
                    continue
                
                # Get the fan for this parcel at this horizon
                fan = y_levels[i, :, hold - 1]  # [S] log-price levels
                fan_prices = np.exp(fan)
                
                # PIT: what percentile does the actual fall at?
                pit = np.mean(fan_prices <= av)
                pit_values.append((pit, bv))
    
    if not pit_values:
        print(f"\n  {vname}: no PIT data available")
        continue
    
    pits = np.array([p[0] for p in pit_values])
    base_vals_arr = np.array([p[1] for p in pit_values])
    
    # Overall PIT
    bin_edges = np.linspace(0, 1, PIT_BINS + 1)
    hist, _ = np.histogram(pits, bins=bin_edges)
    hist_pct = hist / len(pits) * 100
    expected_pct = 100 / PIT_BINS
    
    print(f"\n  ── {vname} (n={len(pits):,} parcel×horizon obs) ──")
    print(f"  {'Bin':>8}  {'Count':>8}  {'%':>6}  {'Expected':>8}  {'Bar'}")
    for b in range(PIT_BINS):
        bar_len = int(hist_pct[b] / expected_pct * 20)
        deviation = hist_pct[b] - expected_pct
        flag = "  ⚠️" if abs(deviation) > expected_pct * 0.5 else ""
        print(f"  {bin_edges[b]:.1f}-{bin_edges[b+1]:.1f}  {hist[b]:>8,}  {hist_pct[b]:>5.1f}%  "
              f"({expected_pct:.1f}%)  {'█' * bar_len}{flag}")
    
    # Uniformity test
    from scipy.stats import kstest
    ks_stat, ks_p = kstest(pits, 'uniform')
    
    # Diagnose shape
    left_mass = np.mean(pits < 0.1) + np.mean(pits > 0.9)
    center_mass = np.mean((pits > 0.4) & (pits < 0.6))
    
    if left_mass > 0.3:
        shape = "U-shaped → fans TOO NARROW"
    elif center_mass > 0.25:
        shape = "∩-shaped → fans TOO WIDE"
    else:
        shape = "approximately uniform → WELL CALIBRATED"
    
    print(f"  KS test: stat={ks_stat:.4f} p={ks_p:.4f}  |  Shape: {shape}")
    
    # PIT by value bracket
    print(f"\n  PIT by value bracket:")
    for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
        bkt_mask = (base_vals_arr >= bkt_lo) & (base_vals_arr < bkt_hi)
        bkt_pits = pits[bkt_mask]
        if len(bkt_pits) < 50:
            continue
        
        tail_mass = np.mean(bkt_pits < 0.1) + np.mean(bkt_pits > 0.9)
        low_tail = np.mean(bkt_pits < 0.1) * 100
        high_tail = np.mean(bkt_pits > 0.9) * 100
        median_pit = np.median(bkt_pits)
        
        # Quick mini-histogram
        h, _ = np.histogram(bkt_pits, bins=5, range=(0, 1))
        h_pct = h / len(bkt_pits) * 100
        mini = " ".join(f"{p:.0f}" for p in h_pct)
        
        cal_flag = ""
        if median_pit < 0.35:
            cal_flag = " → model OVERPREDICTS (actuals mostly below fan median)"
        elif median_pit > 0.65:
            cal_flag = " → model UNDERPREDICTS (actuals mostly above fan median)"
        
        print(f"    {bkt_label:<12}  n={len(bkt_pits):>6,}  med_pit={median_pit:.2f}  "
              f"lo_tail={low_tail:.0f}%  hi_tail={high_tail:.0f}%  [5-bin: {mini}]{cal_flag}")


# ═══════════════════════════════════════════════════════════════════
# 4. VARIANT RECOMMENDATION
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("🏆 VARIANT RECOMMENDATION")
print("=" * 80)

# Score each variant
scores = {}
for vname in VARIANTS:
    rhos = variant_rho_by_origin.get(vname, [])
    if not rhos:
        continue
    
    key_sample = None
    for origin in ORIGINS:
        k = (vname, origin)
        if k in _raw:
            key_sample = k
            break
    
    if key_sample is None:
        continue
    
    # Collect all-origins PIT data for this variant
    all_pits = []
    for origin in ORIGINS:
        key = (vname, origin)
        if key not in _raw:
            continue
        r = _raw[key]
        base_v = _actuals.get(origin, {})
        for hold in range(1, 6):
            eyr = origin + hold
            if eyr not in _actuals:
                continue
            future_v = _actuals[eyr]
            for i in range(len(r["accts"])):
                acct = str(r["accts"][i]).strip()
                bv = base_v.get(acct, 0)
                av = future_v.get(acct)
                if bv >= 1_000_000 and av is not None:
                    fan = np.exp(r["y_levels"][i, :, hold - 1])
                    pit = np.mean(fan <= av)
                    all_pits.append(pit)
    
    all_pits = np.array(all_pits) if all_pits else np.array([])
    
    mean_rho = np.mean(rhos)
    rho_consistency = np.sum(np.array(rhos) > 0) / len(rhos)
    pit_median = np.median(all_pits) if len(all_pits) > 0 else 0.5
    pit_cal_score = 1 - abs(pit_median - 0.5) * 2  # 1.0 = perfectly centered, 0 = all at one extreme
    
    # Composite score (weighted)
    score = (
        mean_rho * 40 +           # ranking ability
        rho_consistency * 20 +     # consistency
        pit_cal_score * 20 +       # calibration
        (1 if mean_rho > 0 else 0) * 20  # bonus for positive ρ
    )
    
    scores[vname] = {
        "score": score,
        "mean_rho": mean_rho,
        "rho_consistency": rho_consistency,
        "pit_median": pit_median,
        "pit_cal_score": pit_cal_score,
        "n_pit": len(all_pits),
    }

print(f"\n  {'Variant':<14} {'Score':>6} {'Avg ρ':>7} {'ρ>0 %':>7} {'PIT med':>8} {'Calibr':>7} {'Recommendation'}")
print(f"  {'─'*14} {'─'*6} {'─'*7} {'─'*7} {'─'*8} {'─'*7} {'─'*20}")

ranked = sorted(scores.items(), key=lambda x: -x[1]["score"])
for i, (vname, s) in enumerate(ranked):
    rec = "★ RECOMMENDED" if i == 0 else ""
    print(f"  {vname:<14} {s['score']:>5.1f}  {s['mean_rho']:>+.3f}  {s['rho_consistency']*100:>5.0f}%  "
          f"{s['pit_median']:>7.3f}  {s['pit_cal_score']:>6.3f}  {rec}")

winner = ranked[0][0] if ranked else "unknown"
print(f"\n  → Deploy {winner} for production inference")
print(f"    Load checkpoint: ckpt_origin_2025_{winner}.pt")
print(f"    (You'll need to train origin=2025 for this variant first)")

print("\n✅ Deep analysis complete!")
