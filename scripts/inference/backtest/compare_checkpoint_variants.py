"""
Cell 2: Compare Checkpoint Variants — Standalone $1M+ Hypothesis Test
=====================================================================
Run AFTER Cell 1 (worldmodel.py) and Cell 1.5 (retrain_sample_sweep.py).

Loads each checkpoint variant (baseline, N40K, N80K, strat20K),
runs inference on a common sample, and compares value-bracket metrics.
Outputs a side-by-side comparison table and automated hypothesis conclusion.

Borrows only the inference plumbing from the full backtest — no entity screening,
no counterfactuals, no track record, no segment analysis.

Requires Cell 1 globals:
  lf, num_use, cat_use,
  create_denoiser_v102, GlobalProjection, GeoProjection, SimpleScaler,
  Scheduler, sample_ar1_path, sample_ddim_v102_coherent_stable,
  build_inference_context_chunked_v102
"""
import os, io, time, json, glob, zipfile, random
import numpy as np
import polars as pl
from scipy.stats import spearmanr
from typing import Dict, List, Any, Optional

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════
ORIGINS = [2021, 2022, 2023, 2024]
SAMPLE_SIZE = 20000
SCENARIOS = 128
HORIZONS = 5
Z_CLIP = 50.0

# Variants to test — "" = baseline checkpoint (ckpt_origin_{yr}.pt)
# SF-only variants from the updated sweep; baseline for comparison
VARIANTS = ["", "SF200K", "SF500K", "SFstrat200K"]
VARIANT_LABELS = {"": "baseline", "SF200K": "SF200K", "SF500K": "SF500K", "SFstrat200K": "SFstrat200K"}

# Value brackets
VALUE_BRACKETS = [
    ("<200K",    0,       200_000),
    ("200K-500K", 200_000, 500_000),
    ("500K-1M",  500_000, 1_000_000),
    ("1M+",      1_000_000, float("inf")),
]

# ─── Pick up Cell 1 globals ───
_out_dir = globals().get("OUT_DIR", "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel")

_required = [
    "lf", "num_use", "cat_use",
    "create_denoiser_v102", "GlobalProjection", "GeoProjection", "SimpleScaler",
    "Scheduler", "sample_ar1_path", "sample_ddim_v102_coherent_stable",
    "build_inference_context_chunked_v102",
]
_missing = [r for r in _required if r not in dir() and r not in globals()]
if _missing:
    print(f"⚠️  Missing Cell 1 globals: {_missing}")
    print("   Make sure Cell 1 (worldmodel.py) has been executed first")
    raise SystemExit

import torch
_device = "cuda" if torch.cuda.is_available() else "cpu"

print("🔬 Checkpoint Variant Comparison — $1M+ Hypothesis Test")
print(f"   Origins: {ORIGINS}  Variants: {list(VARIANT_LABELS.values())}")
print(f"   Sample: {SAMPLE_SIZE}  Scenarios: {SCENARIOS}")

# ═══════════════════════════════════════════════════════════════════
# STEP 1: Load HCAD actuals (appraisal values by year)
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("📊 Loading HCAD actuals")
print("=" * 70)

HCAD_BASE_CANDIDATES = [
    "/content/drive/MyDrive/HCAD_Archive/property",
    "G:/My Drive/HCAD_Archive/property",
    os.path.join(os.path.expanduser("~"), "HCAD_Archive", "property"),
]

hcad_base = None
for cand in HCAD_BASE_CANDIDATES:
    if os.path.isdir(cand):
        hcad_base = cand
        break
assert hcad_base, "❌ Cannot find HCAD data"

actual_vals: Dict[int, Dict[str, float]] = {}

for yr in range(2015, 2026):
    zp = os.path.join(hcad_base, str(yr), "Real_acct_owner.zip")
    if not os.path.exists(zp):
        continue
    try:
        with zipfile.ZipFile(zp) as zf:
            names = zf.namelist()
            af = next((n for n in names if n.lower() == "real_acct.txt"), None)
            if af:
                with zf.open(af) as f:
                    raw = f.read().decode("latin-1").encode("utf-8")
                    vdf = pl.read_csv(io.BytesIO(raw), separator="\t",
                                      infer_schema_length=10000, ignore_errors=True,
                                      truncate_ragged_lines=True, quote_char=None)
                    vdf = vdf.rename({c: c.strip().lower().replace(" ","_") for c in vdf.columns})
                    ac = next((c for c in vdf.columns if c in ("acct","account","prop_id")), vdf.columns[0])
                    vc = next((c for c in vdf.columns if "tot_appr" in c or "total_appraised" in c), None)
                    if vc is None: vc = next((c for c in vdf.columns if "appr" in c), vdf.columns[1])
                    vdf = vdf.select([ac,vc]).cast({ac:pl.Utf8, vc:pl.Float64})
                    vdf = vdf.with_columns(pl.col(ac).str.strip_chars())
                    vdf = vdf.filter(pl.col(vc)>0)
                    actual_vals[yr] = dict(zip(vdf[ac].to_list(), vdf[vc].to_list()))
        print(f"  {yr}: {len(actual_vals.get(yr,{})):,} properties")
    except Exception as e:
        print(f"  {yr}: ⚠️ {e}")


def _strip_compiled_prefix(sd):
    return {k.replace("_orig_mod.", ""): v for k, v in sd.items()}


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Run bracket analysis for each variant × origin
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("🔬 Running bracket analysis across variants")
print("=" * 70)

_num_use = globals().get("num_use", [])
_cat_use = globals().get("cat_use", [])
_lf = globals().get("lf")

bracket_rows: List[Dict[str, Any]] = []

# ── Persist raw per-parcel results for post-hoc analysis ──
# Keyed by (variant_label, origin) → dict with accts, predictions, actuals
variant_raw_results: Dict[tuple, Dict[str, Any]] = {}

for variant_suffix in VARIANTS:
    variant_label = VARIANT_LABELS[variant_suffix]

    for origin in ORIGINS:
        # ── Find checkpoint ──
        if variant_suffix:
            ckpt_name = f"ckpt_origin_{origin}_{variant_suffix}.pt"
        else:
            ckpt_name = f"ckpt_origin_{origin}.pt"

        ckpt_path = os.path.join(_out_dir, ckpt_name)
        if not os.path.exists(ckpt_path):
            found = glob.glob(os.path.join(_out_dir, "**", ckpt_name), recursive=True)
            ckpt_path = found[0] if found else None
        if not ckpt_path or not os.path.exists(ckpt_path):
            print(f"\n  ⚠️ {ckpt_name} not found, skipping")
            continue

        print(f"\n  ── {variant_label} / origin={origin} ──")
        t0 = time.time()

        # ── Load checkpoint ──
        ckpt = torch.load(ckpt_path, map_location=_device, weights_only=False)
        _cfg = ckpt.get("cfg", {})
        sd = _strip_compiled_prefix(ckpt["model_state_dict"])
        sd_pg = _strip_compiled_prefix(ckpt["proj_g_state_dict"])
        sd_pgeo = _strip_compiled_prefix(ckpt["proj_geo_state_dict"])

        hist_len = sd["hist_enc.0.weight"].shape[1]
        num_dim = sd["num_enc.0.weight"].shape[1]
        n_cat = len([k for k in sd if k.startswith("cat_embs.") and k.endswith(".weight")])
        H = int(_cfg.get("H", HORIZONS))

        model = create_denoiser_v102(target_dim=H, hist_len=hist_len, num_dim=num_dim, n_cat=n_cat)
        model.load_state_dict(sd)
        model = model.to(_device).eval()

        proj_g = GlobalProjection(int(_cfg.get("GLOBAL_RANK", 8)))
        proj_g.load_state_dict(sd_pg)
        proj_g = proj_g.to(_device).eval()

        proj_geo = GeoProjection(int(_cfg.get("GEO_RANK", 4)))
        proj_geo.load_state_dict(sd_pgeo)
        proj_geo = proj_geo.to(_device).eval()

        model._y_scaler = SimpleScaler(
            mean=np.array(ckpt["y_scaler_mean"], dtype=np.float32),
            scale=np.array(ckpt["y_scaler_scale"], dtype=np.float32),
        )
        model._n_scaler = SimpleScaler(
            mean=np.array(ckpt["n_scaler_mean"], dtype=np.float32),
            scale=np.array(ckpt["n_scaler_scale"], dtype=np.float32),
        )
        model._t_scaler = SimpleScaler(
            mean=np.array(ckpt["t_scaler_mean"], dtype=np.float32),
            scale=np.array(ckpt["t_scaler_scale"], dtype=np.float32),
        )
        global_medians = ckpt.get("global_medians", {})

        # ── Sample parcels (deterministic per origin to make comparison fair) ──
        origin_accts = list(actual_vals.get(origin, {}).keys())
        if len(origin_accts) < 100:
            print(f"    ⚠️ Only {len(origin_accts)} accounts at {origin}, skipping")
            del model, proj_g, proj_geo, ckpt
            if _device == "cuda": torch.cuda.empty_cache()
            continue

        random.seed(42 + origin)
        sample_accts = random.sample(origin_accts, min(SAMPLE_SIZE, len(origin_accts)))

        # ── Build inference context ──
        ctx = build_inference_context_chunked_v102(
            lf=_lf, accts=sample_accts,
            num_use_local=_num_use,
            cat_use_local=_cat_use,
            global_medians=global_medians,
            anchor_year=int(origin),
            max_parcels=len(sample_accts),
        )

        if ctx is None or "acct" not in ctx or len(ctx["acct"]) == 0:
            print(f"    ⚠️ Context failed, skipping")
            del model, proj_g, proj_geo, ckpt
            if _device == "cuda": torch.cuda.empty_cache()
            continue

        n_valid = len(ctx["acct"])
        print(f"    Context: {n_valid:,} valid parcels")

        # ── Run inference ──
        S = SCENARIOS
        global_rank = int(_cfg.get("GLOBAL_RANK", 8))
        geo_rank = int(_cfg.get("GEO_RANK", 4))
        phi_g = float(_cfg.get("PHI_GLOBAL", 0.85))
        phi_geo = float(_cfg.get("PHI_GEO", 0.70))
        diff_steps = int(_cfg.get("DIFF_STEPS_TRAIN", 128))

        sched = Scheduler(diff_steps, device=_device)
        Zg = sample_ar1_path(H, global_rank, phi_g, batch_size=S, device=_device)
        unique_rids = np.unique(ctx["region_id"])
        Zgeo = {}
        for rid in unique_rids:
            Zgeo[int(rid)] = sample_ar1_path(H, geo_rank, phi_geo, batch_size=S, device=_device)

        orig_z_clip = globals().get("SAMPLER_Z_CLIP", 20.0)
        globals()["SAMPLER_Z_CLIP"] = Z_CLIP

        batch_size = min(int(_cfg.get("INFERENCE_BATCH_SIZE", 16384)), n_valid)
        all_deltas = []

        for b_start in range(0, n_valid, batch_size):
            b_end = min(b_start + batch_size, n_valid)
            b_deltas = sample_ddim_v102_coherent_stable(
                model=model, proj_g=proj_g, proj_geo=proj_geo,
                sched=sched,
                hist_y_b=ctx["hist_y"][b_start:b_end],
                cur_num_b=ctx["cur_num"][b_start:b_end],
                cur_cat_b=ctx["cur_cat"][b_start:b_end],
                region_id_b=ctx["region_id"][b_start:b_end],
                Zg_scenarios=Zg, Zgeo_scenarios=Zgeo,
                device=_device,
            )
            all_deltas.append(b_deltas)

        globals()["SAMPLER_Z_CLIP"] = orig_z_clip

        deltas = np.concatenate(all_deltas, axis=0)  # [N, S, H]
        accts = ctx["acct"]
        ya = ctx["y_anchor"]
        cumsum_d = np.cumsum(deltas, axis=2)
        y_levels = ya[:, None, None] + cumsum_d  # [N, S, H] log-space

        base_v = actual_vals.get(origin, {})
        dt_infer = time.time() - t0
        print(f"    Inference done in {dt_infer:.1f}s")

        # ── Persist raw results for post-hoc analysis ──
        base_vals_arr = np.array([base_v.get(str(a).strip(), 0) for a in accts])
        med_growth = np.expm1(np.nanmedian(y_levels, axis=1) - ya[:, None]) * 100  # [N, H]
        variant_raw_results[(variant_label, origin)] = {
            "accts": list(accts),
            "base_val": base_vals_arr,
            "y_anchor": ya.copy(),
            "y_levels": y_levels.copy(),  # [N, S, H] — full fan
            "med_growth": med_growth,      # [N, H] — median predicted growth %
            "deltas": deltas.copy(),
        }
        print(f"    💾 Raw results cached for {variant_label}/{origin} ({y_levels.shape})")

        # ── Bracket analysis ──
        for hold in range(1, H + 1):
            eyr = origin + hold
            if eyr not in actual_vals:
                continue
            future_v = actual_vals[eyr]

            # Median predicted growth per parcel
            h_log = np.nanmedian(y_levels[:, :, hold - 1], axis=1)
            h_growth = np.expm1(h_log - ya) * 100
            valid = np.isfinite(h_growth)

            for bkt_label, bkt_lo, bkt_hi in VALUE_BRACKETS:
                bkt_idx = []
                for i in range(len(accts)):
                    if not valid[i]:
                        continue
                    acct = str(accts[i]).strip()
                    v = base_v.get(acct, 0)
                    if bkt_lo <= v < bkt_hi:
                        bkt_idx.append(i)

                bkt_n = len(bkt_idx)
                if bkt_n < 5:
                    continue

                bkt_pred = np.array([h_growth[i] for i in bkt_idx])

                # Match against actuals
                bkt_actual = []
                bkt_pred_matched = []
                fan_hits = 0
                fan_checks = 0
                for i in bkt_idx:
                    acct = str(accts[i]).strip()
                    bv = base_v.get(acct, 0)
                    av = future_v.get(acct)
                    if bv > 0 and av is not None:
                        actual_g = (av - bv) / bv * 100
                        bkt_actual.append(actual_g)
                        bkt_pred_matched.append(h_growth[i])
                        h_lev = y_levels[i, :, hold - 1]
                        p10 = np.exp(np.nanpercentile(h_lev, 10))
                        p90 = np.exp(np.nanpercentile(h_lev, 90))
                        fan_checks += 1
                        if p10 <= av <= p90:
                            fan_hits += 1

                bkt_actual = np.array(bkt_actual)
                bkt_pred_matched = np.array(bkt_pred_matched)

                med_growth = float(np.median(bkt_pred))
                pct_neg = float(np.mean(bkt_pred < 0) * 100)
                mae = float(np.mean(np.abs(bkt_pred_matched - bkt_actual))) if len(bkt_actual) > 0 else None
                mdae = float(np.median(np.abs(bkt_pred_matched - bkt_actual))) if len(bkt_actual) > 0 else None
                coverage = (fan_hits / fan_checks * 100) if fan_checks > 0 else None
                rho = None
                if len(bkt_actual) > 10:
                    rho, _ = spearmanr(bkt_pred_matched, bkt_actual)
                    rho = float(rho)

                bracket_rows.append({
                    "variant": variant_label, "origin": origin, "horizon": hold,
                    "bracket": bkt_label, "n": bkt_n, "n_with_actual": len(bkt_actual),
                    "med_growth": med_growth, "pct_neg": pct_neg,
                    "mae": mae, "mdae": mdae, "coverage": coverage, "rho": rho,
                })

        del model, proj_g, proj_geo, ckpt
        if _device == "cuda": torch.cuda.empty_cache()


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Results — per-bracket table for each variant
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 115)
print("📊 VALUE-BRACKET RESULTS BY VARIANT")
print("=" * 115)

variants_seen = list(dict.fromkeys(r["variant"] for r in bracket_rows))

for vname in variants_seen:
    vrows = [r for r in bracket_rows if r["variant"] == vname]
    if not vrows:
        continue

    print(f"\n  ── {vname.upper()} ──")
    print(f"    {'Bracket':<12} {'n':>6}  {'Med Grw':>8} {'%Neg':>6} {'MdAE':>7} {'MAE':>9} {'Covg':>6} {'ρ':>7}")
    print(f"    {'─'*12} {'─'*6}  {'─'*8} {'─'*6} {'─'*7} {'─'*9} {'─'*6} {'─'*7}")

    for bkt_label, _, _ in VALUE_BRACKETS:
        bkt_rows = [r for r in vrows if r["bracket"] == bkt_label]
        if not bkt_rows:
            continue
        avg_n = np.mean([r["n"] for r in bkt_rows])
        avg_mg = np.mean([r["med_growth"] for r in bkt_rows])
        avg_neg = np.mean([r["pct_neg"] for r in bkt_rows])
        mae_vals = [r["mae"] for r in bkt_rows if r["mae"] is not None]
        mdae_vals = [r["mdae"] for r in bkt_rows if r["mdae"] is not None]
        cov_vals = [r["coverage"] for r in bkt_rows if r["coverage"] is not None]
        rho_vals = [r["rho"] for r in bkt_rows if r["rho"] is not None]
        avg_mae = np.mean(mae_vals) if mae_vals else float("nan")
        avg_mdae = np.mean(mdae_vals) if mdae_vals else float("nan")
        avg_cov = np.mean(cov_vals) if cov_vals else float("nan")
        avg_rho = np.mean(rho_vals) if rho_vals else float("nan")

        mdae_s = f"{avg_mdae:>6.1f}%" if not np.isnan(avg_mdae) else "    —  "
        mae_s = f"{avg_mae:>8.1f}%" if not np.isnan(avg_mae) else "      —  "
        cov_s = f"{avg_cov:>5.1f}%" if not np.isnan(avg_cov) else "   —  "
        rho_s = f"{avg_rho:>6.3f}" if not np.isnan(avg_rho) else "   —  "

        flag = ""
        if bkt_label == "1M+" and avg_neg > 55:
            flag = "  ⚠️ OOD signal"

        print(f"    {bkt_label:<12} {avg_n:>5.0f}  {avg_mg:>+7.1f}% {avg_neg:>5.1f}% {mdae_s} {mae_s} {cov_s} {rho_s}{flag}")


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Cross-variant comparison — $1M+ bracket only
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 115)
print("🔬 CROSS-VARIANT COMPARISON: $1M+ bracket")
print("   Tests whether more data or stratified sampling reduces the negative growth bias")
print("=" * 115)

print(f"\n  {'Variant':<12} {'Avg n':>7} {'Med Grw':>8} {'%Neg':>6} {'MdAE':>7} {'MAE':>9} {'Covg':>6} {'ρ':>7}")
print(f"  {'─'*12} {'─'*7} {'─'*8} {'─'*6} {'─'*7} {'─'*9} {'─'*6} {'─'*7}")

variant_neg_pcts: Dict[str, float] = {}

for vname in variants_seen:
    vrows = [r for r in bracket_rows if r["variant"] == vname and r["bracket"] == "1M+"]
    if not vrows:
        continue
    avg_n = np.mean([r["n"] for r in vrows])
    avg_mg = np.mean([r["med_growth"] for r in vrows])
    avg_neg = np.mean([r["pct_neg"] for r in vrows])
    mae_vals = [r["mae"] for r in vrows if r["mae"] is not None]
    mdae_vals = [r["mdae"] for r in vrows if r["mdae"] is not None]
    cov_vals = [r["coverage"] for r in vrows if r["coverage"] is not None]
    rho_vals = [r["rho"] for r in vrows if r["rho"] is not None]
    avg_mae = np.mean(mae_vals) if mae_vals else float("nan")
    avg_mdae = np.mean(mdae_vals) if mdae_vals else float("nan")
    avg_cov = np.mean(cov_vals) if cov_vals else float("nan")
    avg_rho = np.mean(rho_vals) if rho_vals else float("nan")

    variant_neg_pcts[vname] = avg_neg

    mdae_s = f"{avg_mdae:>6.1f}%" if not np.isnan(avg_mdae) else "    —  "
    mae_s = f"{avg_mae:>8.1f}%" if not np.isnan(avg_mae) else "      —  "
    cov_s = f"{avg_cov:>5.1f}%" if not np.isnan(avg_cov) else "   —  "
    rho_s = f"{avg_rho:>6.3f}" if not np.isnan(avg_rho) else "   —  "
    marker = "" if vname == "baseline" else (
        "  ✅" if avg_neg < variant_neg_pcts.get("baseline", 100) - 5 else "")
    print(f"  {vname:<12} {avg_n:>6.0f}  {avg_mg:>+7.1f}% {avg_neg:>5.1f}% {mdae_s} {mae_s} {cov_s} {rho_s}{marker}")


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Automated hypothesis conclusion
# ═══════════════════════════════════════════════════════════════════
base_neg = variant_neg_pcts.get("baseline", None)

if base_neg is not None:
    print(f"\n  ── HYPOTHESIS CONCLUSION ──")

    # Test 1: Does %neg drop monotonically across baseline → N40K → N80K?
    random_variants = [(v, variant_neg_pcts[v]) for v in ["baseline", "N40K", "N80K"]
                      if v in variant_neg_pcts]
    if len(random_variants) >= 2:
        negs = [neg for _, neg in random_variants]
        monotonic = all(negs[i] >= negs[i+1] for i in range(len(negs)-1))
        total_drop = negs[0] - negs[-1]

        if monotonic and total_drop > 10:
            print(f"     ✅ CONFIRMED: %neg drops monotonically {negs[0]:.0f}% → {negs[-1]:.0f}% with more data")
            print(f"        Undersampling is the root cause of the $1M+ negative growth bias.")
        elif total_drop > 5:
            print(f"     🟡 PARTIAL: %neg drops {negs[0]:.0f}% → {negs[-1]:.0f}% but not fully resolved")
        else:
            print(f"     ❌ NOT CONFIRMED: More data does not reduce the bias ({negs[0]:.0f}% → {negs[-1]:.0f}%)")
            print(f"        The negative growth may be real signal, not an OOD artifact.")

    # Test 2: Does stratification beat scaling?
    strat_neg = variant_neg_pcts.get("strat20K", None)
    n80k_neg = variant_neg_pcts.get("N80K", None)
    if strat_neg is not None and n80k_neg is not None:
        if strat_neg <= n80k_neg + 2:
            print(f"     💡 Stratified 20K ({strat_neg:.0f}%neg) matches N80K ({n80k_neg:.0f}%neg)")
            print(f"        → CHEAPEST FIX: stratified sampling, not 4× more training data")
        elif strat_neg < base_neg - 5:
            print(f"     💡 Stratified 20K helps ({strat_neg:.0f}%neg vs baseline {base_neg:.0f}%neg)")
            print(f"        but N80K is better ({n80k_neg:.0f}%neg) — scaling wins here")
        else:
            print(f"     ⚠️  Stratified 20K ({strat_neg:.0f}%neg) shows no improvement over baseline")
    elif strat_neg is not None:
        if strat_neg < base_neg - 5:
            print(f"     💡 Stratified 20K reduces bias ({strat_neg:.0f}%neg vs baseline {base_neg:.0f}%neg)")

elif not bracket_rows:
    print("\n  ⚠️  No variant checkpoints found — run Cell 1.5 first")
else:
    print("\n  ⚠️  Baseline checkpoint not found — cannot compute hypothesis comparison")


# ═══════════════════════════════════════════════════════════════════
# SAVE RESULTS TO JSON + RAW DATA
# ═══════════════════════════════════════════════════════════════════
_ts = time.strftime("%Y%m%d_%H%M%S")
_results = {
    "timestamp": _ts,
    "origins": ORIGINS,
    "sample_size": SAMPLE_SIZE,
    "variants": list(VARIANT_LABELS.values()),
    "bracket_rows": bracket_rows,
    "variant_neg_pcts": variant_neg_pcts,
}
_results_dir = os.path.join(_out_dir, "hypothesis_results")
os.makedirs(_results_dir, exist_ok=True)
_results_path = os.path.join(_results_dir, f"variant_comparison_{_ts}.json")
try:
    with open(_results_path, "w") as f:
        json.dump(_results, f, indent=2, default=str)
    print(f"\n💾 Bracket results saved: {_results_path}")
except Exception as e:
    print(f"\n⚠️  Could not save bracket results: {e}")

# Save raw per-parcel results to pickle for post-hoc analysis
import pickle
_raw_path = os.path.join(_results_dir, f"variant_raw_{_ts}.pkl")
try:
    with open(_raw_path, "wb") as f:
        pickle.dump(variant_raw_results, f)
    print(f"💾 Raw per-parcel results saved: {_raw_path}")
    _raw_mb = os.path.getsize(_raw_path) / 1e6
    print(f"   ({_raw_mb:.0f}MB — {len(variant_raw_results)} variant×origin combos)")
except Exception as e:
    print(f"⚠️  Could not save raw results: {e}")

print(f"\n📦 In-memory: variant_raw_results dict has {len(variant_raw_results)} entries")
print(f"   Keys: {list(variant_raw_results.keys())[:6]}...")
print(f"   Each entry has: accts, base_val, y_anchor, y_levels [N,S,H], med_growth [N,H], deltas [N,S,H]")
print(f"   Example: variant_raw_results[('baseline', 2021)]['y_levels'].shape")

print("\n✅ Variant comparison complete!")
