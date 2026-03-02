"""
Cell 2: Multi-Origin Model Backtest — Screening Efficiency & Entity Segmentation
=================================================================================
After worldmodel.py runs in Cell 1, this cell:
  1) Loads HCAD actuals
  2) Loops through each checkpoint origin (2021–2024)
  3) For each origin: loads checkpoint, runs inference,
     applies track-record filter, computes:
       - Screening efficiency: what quartile do entity purchases fall in?
       - Entity segmentation: which entity TYPES does the model match?
       - Counterfactual $ comparison within price bracket
  4) Prints summary table across all origins

Requires from Cell 1: create_denoiser_v102, GlobalProjection, GeoProjection,
  SimpleScaler, Scheduler, sample_ar1_path, sample_ddim_v102_coherent_stable,
  build_inference_context_chunked_v102, lf (LazyFrame)
"""
import os, sys, glob, time, io, random, zipfile, json
import numpy as np
import polars as pl
from scipy.stats import spearmanr
from collections import defaultdict

try:
    import wandb
    _HAS_WANDB = True
except ImportError:
    _HAS_WANDB = False

ENTITY_KEYWORDS = ["LLC","LP","INC","CORP","TRUST","LTD","PARTNERS","FUND",
                   "INVESTMENT","PROPERTIES","CAPITAL","HOLDINGS","VENTURES",
                   "ASSET","REALTY","MGMT","MANAGEMENT","ENTERPRISE","GROUP"]
HOMEBUILDER_KEYWORDS = ["LENNAR","PERRY","MERITAGE","D R HORTON","DR HORTON",
                        "TAYLOR MORRISON","PULTE","KB HOME","NVR ","TOLL BROTHERS"]

# Entity type classification (order matters — first match wins)
ENTITY_SEGMENTS = [
    ("Developer",    ["DEVELOPMENT","CONSTRUCTION","BUILDER","BUILDING","RENOVATI"]),
    ("Homebuilder",  HOMEBUILDER_KEYWORDS),
    ("Fund/REIT",    ["FUND","REIT","CAPITAL","INVESTMENT","ASSET","VENTURES"]),
    ("Trust",        ["TRUST","ESTATE"]),
    ("PropMgmt",     ["PROPERTIES","REALTY","MGMT","MANAGEMENT","PROPERTY"]),
    ("Holdings",     ["HOLDINGS","GROUP","ENTERPRISE"]),
    ("Other Corp",   ["LLC","LP","INC","CORP","LTD","PARTNERS"]),
]

def classify_entity(name):
    """Classify entity owner name into segments."""
    if not name:
        return None
    u = name.upper()
    for seg_name, keywords in ENTITY_SEGMENTS:
        if any(k in u for k in keywords):
            return seg_name
    return None

# Non-ICP entity keywords — these are NOT real estate investors.
# They make non-market transactions (HOA common areas, subsidized housing,
# industrial land) that inflate the model's apparent advantage.
NON_ICP_KEYWORDS = [
    # HOAs / community associations
    "HOMEOWNERS ASSOCIATION", "OWNERS ASSOCIATION", "COMMUNITY ASSOC",
    "COMM ASSOC", "PROPERTY OWNERS ASSOC", "HOA",
    # Government / affordable housing
    "COMMUNITY LAND TRUST", "COUNTY LAND TRUST", "HOUSING AUTHORITY",
    "LAND BANK", "REDEVELOPMENT",
    # Oil / gas / chemical / industrial
    "EXXON", "CHEVRON", "SHELL OIL", "CONOCOPHILLIPS", "MARATHON OIL",
    "PHILLIPS CHEMICAL", "HALLIBURTON", "SCHLUMBERGER", "BAKER HUGHES",
    # Banks / financial services (not SFR investors)
    "NATIONAL BANK", "FEDERAL CREDIT UNION", "SAVINGS BANK",
    # Utilities / infrastructure
    "ELECTRIC", "WATER AUTHORITY", "UTILITY", "PIPELINE",
    "CENTERPOINT", "ONCOR",
    # Schools / churches / nonprofits
    "SCHOOL DISTRICT", "CHURCH", "DIOCESE", "UNIVERSITY",
    "COLLEGE", "ACADEMY", "FOUNDATION",
]

def is_icp(name):
    """Return True if entity name looks like a real estate investor (ICP target)."""
    if not name:
        return False
    u = name.upper()
    return not any(k in u for k in NON_ICP_KEYWORDS)

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════
BACKTEST_ORIGINS = [2021, 2022, 2023, 2024]
BACKTEST_SAMPLE_SIZE = 20000
BACKTEST_SCENARIOS = 128
BACKTEST_HORIZONS = 5
BACKTEST_Z_CLIP = 50.0
TRACK_RECORD_MIN_HITS = 1  # only keep parcels where prior fan covered actual

# Entity sampling mode:
#   'full'   — all entity parcels + large random background (ensures ≤25% entity)
#              slower (~70K parcels) but most honest comparison
#   'capped' — up to ENTITY_CAP entity parcels (largest first) + random fill to BACKTEST_SAMPLE_SIZE
#              faster (~20K parcels), good for quick runs & named-entity leaderboard
ENTITY_SAMPLE_MODE = 'full'   # 'full' or 'capped'
ENTITY_CAP = 5000             # max entity parcels in 'capped' mode
MIN_ENTITY_PCT = 0.25         # max entity fraction of pool in 'full' mode

# ─── Pick up globals from worldmodel.py Cell 1 ───
_out_dir = globals().get("OUT_DIR", "/content/drive/MyDrive/data_backups/world_model_v10_2_fullpanel")
_max_year = globals().get("MAX_YEAR", 2025)
print(f"📦 Model output: {_out_dir}")
print(f"   Origins: {BACKTEST_ORIGINS}  Sample: {BACKTEST_SAMPLE_SIZE}")
print(f"   Track-record filter: ≥{TRACK_RECORD_MIN_HITS} hit(s) from prior origins")

HCAD_BASE_CANDIDATES = [
    "/content/drive/MyDrive/HCAD_Archive/property",
    "G:/My Drive/HCAD_Archive/property",
    os.path.join(os.path.expanduser("~"), "HCAD_Archive", "property"),
]

# ══════════════════════════════════════════════════════════════════
# STEP 1: Load HCAD actuals + owners (one-time)
# ══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("📊 STEP 1: Loading HCAD actuals")
print("=" * 70)

hcad_base = None
for cand in HCAD_BASE_CANDIDATES:
    if os.path.isdir(cand):
        hcad_base = cand
        break
assert hcad_base, "❌ Cannot find HCAD data"

actual_vals = {}
all_owners = {}

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

            of = next((n for n in names if 'owner' in n.lower() and n.endswith('.txt')), None)
            if of:
                with zf.open(of) as f:
                    raw = f.read().decode("latin-1").encode("utf-8")
                    odf = pl.read_csv(io.BytesIO(raw), separator="\t",
                                      infer_schema_length=10000, ignore_errors=True,
                                      truncate_ragged_lines=True, quote_char=None)
                    odf = odf.rename({c: c.strip().lower().replace(" ","_") for c in odf.columns})
                    ac = next((c for c in odf.columns if c in ("acct","account","prop_id")), odf.columns[0])
                    nc = [c for c in odf.columns if any(k in c for k in ['owner_nm','owner_name','ownr_nm'])]
                    if not nc: nc = [c for c in odf.columns if 'name' in c]
                    if not nc: nc = [c for c in odf.columns if 'owner' in c and c != ac]
                    nm = nc[0] if nc else odf.columns[1]
                    odf = odf.select([ac,nm]).cast({ac:pl.Utf8, nm:pl.Utf8})
                    odf = odf.with_columns(pl.col(ac).str.strip_chars())
                    all_owners[yr] = dict(zip(odf[ac].to_list(), odf[nm].to_list()))

        print(f"  {yr}: vals={len(actual_vals.get(yr,{})):,}  owners={len(all_owners.get(yr,{})):,}")
    except Exception as e:
        print(f"  {yr}: ⚠️ {e}")

# ── Detect Cell 1 class definitions ──
_has_classes = all([
    callable(globals().get("create_denoiser_v102")),
    callable(globals().get("sample_ddim_v102_coherent_stable")),
    callable(globals().get("build_inference_context_chunked_v102")),
    globals().get("lf") is not None,
])

if not _has_classes:
    print("\n⚠️ Worldmodel Cell 1 class definitions not found")
    print("   Make sure Cell 1 (worldmodel.py) has been executed first")
    raise SystemExit

import torch
_device = "cuda" if torch.cuda.is_available() else "cpu"

def _strip_compiled_prefix(sd):
    """Strip '_orig_mod.' prefix added by torch.compile."""
    return {k.replace("_orig_mod.", ""): v for k, v in sd.items()}

# ══════════════════════════════════════════════════════════════════
# Accumulators
# ══════════════════════════════════════════════════════════════════
summary_rows = []
screening_rows = []      # {origin, horizon, q1_pct, q2_pct, q3_pct, q4_pct}
segment_rows = []         # {origin, horizon, segment, n, ent_med, model_med, diff}
entity_perf_rows = []     # {origin, horizon, entity_name, segment, n_buys, ent_med, model_med, diff}
track_record = {}         # acct -> [(origin, fcast_yr, p10_dollar, p90_dollar)]

# ══════════════════════════════════════════════════════════════════
# LOOP THROUGH EACH CHECKPOINT ORIGIN
# ══════════════════════════════════════════════════════════════════
for ORIGIN in BACKTEST_ORIGINS:
    print("\n" + "█" * 70)
    print(f"█  ORIGIN = {ORIGIN}")
    print("█" * 70)

    # ── Find and load checkpoint ──
    _ckpt_path = None
    for _d in [_out_dir, globals().get("CKPT_DIR", _out_dir)]:
        _candidate = os.path.join(_d, f"ckpt_origin_{ORIGIN}.pt")
        if os.path.exists(_candidate):
            _ckpt_path = _candidate
            break
    if _ckpt_path is None:
        for _d in [_out_dir]:
            for _f in glob.glob(os.path.join(_d, "**", f"ckpt_origin_{ORIGIN}.pt"), recursive=True):
                _ckpt_path = _f
                break
            if _ckpt_path: break

    if not _ckpt_path or not os.path.exists(_ckpt_path):
        print(f"  ⚠️ No checkpoint found for origin={ORIGIN}, skipping")
        continue

    print(f"  Loading: {os.path.basename(_ckpt_path)}")
    t0 = time.time()
    _ckpt = torch.load(_ckpt_path, map_location=_device, weights_only=False)
    print(f"  Loaded in {time.time()-t0:.1f}s")

    _cfg = _ckpt.get("cfg", {})
    _sd = _strip_compiled_prefix(_ckpt["model_state_dict"])
    _sd_pg = _strip_compiled_prefix(_ckpt["proj_g_state_dict"])
    _sd_pgeo = _strip_compiled_prefix(_ckpt["proj_geo_state_dict"])

    _hist_len = _sd["hist_enc.0.weight"].shape[1]
    _num_dim = _sd["num_enc.0.weight"].shape[1]
    _n_cat = len([k for k in _sd if k.startswith("cat_embs.") and k.endswith(".weight")])
    _bt_H = int(_cfg.get("H", BACKTEST_HORIZONS))

    _bt_model = create_denoiser_v102(target_dim=_bt_H, hist_len=_hist_len, num_dim=_num_dim, n_cat=_n_cat)
    _bt_model.load_state_dict(_sd)
    _bt_model = _bt_model.to(_device).eval()

    _bt_proj_g = GlobalProjection(int(_cfg.get("GLOBAL_RANK", 8)))
    _bt_proj_g.load_state_dict(_sd_pg)
    _bt_proj_g = _bt_proj_g.to(_device).eval()

    _bt_proj_geo = GeoProjection(int(_cfg.get("GEO_RANK", 4)))
    _bt_proj_geo.load_state_dict(_sd_pgeo)
    _bt_proj_geo = _bt_proj_geo.to(_device).eval()

    _bt_model._y_scaler = SimpleScaler(
        mean=np.array(_ckpt["y_scaler_mean"], dtype=np.float32),
        scale=np.array(_ckpt["y_scaler_scale"], dtype=np.float32),
    )
    _bt_model._n_scaler = SimpleScaler(
        mean=np.array(_ckpt["n_scaler_mean"], dtype=np.float32),
        scale=np.array(_ckpt["n_scaler_scale"], dtype=np.float32),
    )
    _bt_model._t_scaler = SimpleScaler(
        mean=np.array(_ckpt["t_scaler_mean"], dtype=np.float32),
        scale=np.array(_ckpt["t_scaler_scale"], dtype=np.float32),
    )
    _bt_global_medians = _ckpt.get("global_medians", {})
    print(f"  ✅ Model reconstructed (hist={_hist_len}, num={_num_dim}, cat={_n_cat})")

    # ── Entity-biased sampling ──
    origin_accts = list(actual_vals.get(ORIGIN, {}).keys())
    if len(origin_accts) < 100:
        print(f"  ⚠️ Only {len(origin_accts)} accounts at {ORIGIN}, skipping")
        continue
    origin_accts_set = set(origin_accts)

    # Detect entity purchases from owner data (hoisted up for sampling)
    _ow_prior = all_owners.get(ORIGIN - 1, {})
    _ow_current = all_owners.get(ORIGIN, {})
    _prior_ent = {a for a, n in _ow_prior.items() if n and any(k in n.upper() for k in ENTITY_KEYWORDS)}
    _current_ent = {a for a, n in _ow_current.items() if n and any(k in n.upper() for k in ENTITY_KEYWORDS)}
    _ent_purchases_countywide = _current_ent - _prior_ent

    # Entity parcels that have actual values in the panel
    ent_in_actuals = sorted([a for a in _ent_purchases_countywide if a in origin_accts_set])
    n_entity_available = len(ent_in_actuals)
    non_ent_accts = [a for a in origin_accts if a not in _ent_purchases_countywide]
    random.seed(42)

    if ENTITY_SAMPLE_MODE == 'full':
        # Mode 1: ALL entity parcels + enough random to keep entities ≤ MIN_ENTITY_PCT
        n_entity_forced = n_entity_available
        min_random = max(BACKTEST_SAMPLE_SIZE, int(n_entity_forced / MIN_ENTITY_PCT) - n_entity_forced)
        random_fill = random.sample(non_ent_accts, min(min_random, len(non_ent_accts)))
        sample_accts = ent_in_actuals + random_fill
        print(f"  Entity-biased sample (FULL mode): {len(sample_accts):,} total")
        print(f"    Entity parcels: {n_entity_forced:,} / {len(_ent_purchases_countywide):,} county-wide")
        print(f"    Random background: {len(random_fill):,}")
        print(f"    Entity fraction: {n_entity_forced/len(sample_accts)*100:.1f}%")
    else:
        # Mode 2: CAPPED entity parcels (prioritize entities with most purchases) + random fill
        # Sort by owner name frequency to prioritize larger portfolios
        ent_name_counts = {}
        for a in ent_in_actuals:
            name = _ow_current.get(a, "")
            ent_name_counts[name] = ent_name_counts.get(name, 0) + 1
        # Sort entity parcels: largest portfolios first
        ent_in_actuals_sorted = sorted(ent_in_actuals,
            key=lambda a: -ent_name_counts.get(_ow_current.get(a, ""), 0))
        ent_selected = ent_in_actuals_sorted[:ENTITY_CAP]
        n_entity_forced = len(ent_selected)
        remaining = BACKTEST_SAMPLE_SIZE - n_entity_forced
        random_fill = random.sample(non_ent_accts, min(remaining, len(non_ent_accts)))
        sample_accts = ent_selected + random_fill
        print(f"  Entity-biased sample (CAPPED mode): {len(sample_accts):,} total")
        print(f"    Entity parcels: {n_entity_forced:,} / {n_entity_available:,} available / {len(_ent_purchases_countywide):,} county-wide")
        print(f"    Random fill: {len(random_fill):,}")
        print(f"    Entity fraction: {n_entity_forced/len(sample_accts)*100:.1f}%")

    # ── Build inference context ──
    t0 = time.time()
    ctx = build_inference_context_chunked_v102(
        lf=lf,
        accts=sample_accts,
        num_use_local=globals().get("num_use", []),
        cat_use_local=globals().get("cat_use", []),
        global_medians=_bt_global_medians,
        anchor_year=int(ORIGIN),
        max_parcels=len(sample_accts),
    )
    dt_ctx = time.time() - t0

    if ctx is None or not isinstance(ctx, dict) or "acct" not in ctx or len(ctx["acct"]) == 0:
        print(f"  ⚠️ Inference context failed, skipping")
        continue

    n_valid = len(ctx["acct"])
    print(f"  Context: {n_valid}/{len(sample_accts)} valid ({dt_ctx:.1f}s)")

    # ── Prepare latent paths ──
    _bt_S = BACKTEST_SCENARIOS
    _global_rank = int(_cfg.get("GLOBAL_RANK", 8))
    _geo_rank = int(_cfg.get("GEO_RANK", 4))
    _phi_g = float(_cfg.get("PHI_GLOBAL", 0.85))
    _phi_geo = float(_cfg.get("PHI_GEO", 0.70))
    _diff_steps = int(_cfg.get("DIFF_STEPS_TRAIN", 128))

    _bt_sched = Scheduler(_diff_steps, device=_device)
    Zg_scenarios = sample_ar1_path(_bt_H, _global_rank, _phi_g, batch_size=_bt_S, device=_device)

    _bt_unique_rids = np.unique(ctx["region_id"])
    Zgeo_scenarios = {}
    for _rid in _bt_unique_rids:
        Zgeo_scenarios[int(_rid)] = sample_ar1_path(
            _bt_H, _geo_rank, _phi_geo, batch_size=_bt_S, device=_device,
        )

    # ── Override z-clip and sample ──
    _orig_z_clip = globals().get("SAMPLER_Z_CLIP", 20.0)
    globals()["SAMPLER_Z_CLIP"] = BACKTEST_Z_CLIP

    _bt_batch_size = min(int(_cfg.get("INFERENCE_BATCH_SIZE", 16384)), n_valid)
    all_deltas = []

    t0 = time.time()
    for _b_start in range(0, n_valid, _bt_batch_size):
        _b_end = min(_b_start + _bt_batch_size, n_valid)
        _b_deltas = sample_ddim_v102_coherent_stable(
            model=_bt_model, proj_g=_bt_proj_g, proj_geo=_bt_proj_geo,
            sched=_bt_sched,
            hist_y_b=ctx["hist_y"][_b_start:_b_end],
            cur_num_b=ctx["cur_num"][_b_start:_b_end],
            cur_cat_b=ctx["cur_cat"][_b_start:_b_end],
            region_id_b=ctx["region_id"][_b_start:_b_end],
            Zg_scenarios=Zg_scenarios, Zgeo_scenarios=Zgeo_scenarios,
            device=_device,
        )
        all_deltas.append(_b_deltas)
        print(f"    batch {_b_start}:{_b_end} -> {_b_deltas.shape}")

    dt_samp = time.time() - t0
    globals()["SAMPLER_Z_CLIP"] = _orig_z_clip
    print(f"  Sampling done in {dt_samp:.1f}s")

    deltas = np.concatenate(all_deltas, axis=0)  # [N, S, H]
    bt_accts = ctx["acct"]
    ya = ctx["y_anchor"]

    # ── Compute predictions ──
    cumsum_d = np.cumsum(deltas, axis=2)           # [N, S, H]
    y_levels = ya[:, None, None] + cumsum_d        # [N, S, H] in log-space

    # ══════════════════════════════════════════════════════════════
    # Store per-parcel fan (P10/P90) for track-record
    # ══════════════════════════════════════════════════════════════
    for i in range(len(bt_accts)):
        acct = str(bt_accts[i]).strip()
        if acct not in track_record:
            track_record[acct] = []
        for h in range(_bt_H):
            fcast_yr = ORIGIN + h + 1
            h_levels = y_levels[i, :, h]
            p10_dollar = np.exp(np.nanpercentile(h_levels, 10))
            p90_dollar = np.exp(np.nanpercentile(h_levels, 90))
            track_record[acct].append((ORIGIN, fcast_yr, p10_dollar, p90_dollar))

    # ══════════════════════════════════════════════════════════════
    # TRACK-RECORD FILTER (only filter we keep)
    # ══════════════════════════════════════════════════════════════
    prior_origins = [o for o in BACKTEST_ORIGINS if o < ORIGIN]
    keep = np.ones(len(bt_accts), dtype=bool)

    if prior_origins:
        n_checked, n_hit, n_miss = 0, 0, 0
        for i in range(len(bt_accts)):
            acct = str(bt_accts[i]).strip()
            records = track_record.get(acct, [])
            if not records:
                continue
            hits, checks = 0, 0
            for (pred_origin, fcast_yr, p10_d, p90_d) in records:
                if pred_origin >= ORIGIN or fcast_yr != ORIGIN:
                    continue
                actual = actual_vals.get(ORIGIN, {}).get(acct, None)
                if actual is None:
                    continue
                checks += 1
                if p10_d <= actual <= p90_d:
                    hits += 1
            if checks > 0:
                n_checked += 1
                if hits >= min(TRACK_RECORD_MIN_HITS, checks):
                    n_hit += 1
                else:
                    n_miss += 1
                    keep[i] = False
        print(f"\n  🔍 Track-record: {n_checked:,} checked | {n_hit:,} calibrated | {n_miss:,} missed")
    else:
        print(f"\n  🔍 Track-record: first origin, no priors")

    n_kept = keep.sum()
    print(f"  ✅ {n_kept:,} / {len(bt_accts):,} kept ({n_kept/len(bt_accts)*100:.1f}%)")

    # ── Build filtered model predictions ──
    final_log = np.nanmedian(y_levels[:, :, -1], axis=1)
    g = np.expm1(final_log - ya) * 100
    valid = np.isfinite(g) & keep

    model_accts = [str(bt_accts[i]).strip() for i in range(len(bt_accts)) if valid[i]]
    model_growth = [float(g[i]) for i in range(len(bt_accts)) if valid[i]]

    # Per-horizon model growth for quartile assignment
    per_horizon_growth = {}
    for h in range(_bt_H):
        h_log = np.nanmedian(y_levels[:, :, h], axis=1)
        h_growth = np.expm1(h_log - ya) * 100
        valid_h = np.isfinite(h_growth) & keep
        per_horizon_growth[h+1] = {
            str(bt_accts[i]).strip(): float(h_growth[i])
            for i in range(len(bt_accts)) if valid_h[i]
        }
        vals = h_growth[valid_h]
        print(f"    +{h+1}yr: median={np.median(vals):.1f}%  "
              f"P10={np.percentile(vals,10):.1f}%  P90={np.percentile(vals,90):.1f}%")

    print(f"  ✅ {len(model_accts):,} valid predictions")

    # ── Reuse entity purchases from biased sampling (already computed above) ──
    ow_current = _ow_current
    ent_purchases_all = _ent_purchases_countywide

    sampled_set = set(model_accts)
    ent_purchases_in_sample = ent_purchases_all & sampled_set
    n_ent_in_sample = len(ent_purchases_in_sample)

    # Classify each entity purchase by segment
    ent_by_segment = defaultdict(set)
    for a in ent_purchases_in_sample:
        name = ow_current.get(a, "")
        seg = classify_entity(name)
        if seg:
            ent_by_segment[seg].add(a)

    print(f"  Entity purchases county-wide: {len(ent_purchases_all):,}")
    print(f"  Entity purchases in sample: {n_ent_in_sample:,} / {len(sampled_set):,}")
    for seg in sorted(ent_by_segment.keys()):
        print(f"    {seg:<15} {len(ent_by_segment[seg]):,}")

    # ── Build model ranking constrained to price bracket ──
    base_v = actual_vals.get(ORIGIN, {})
    mdf = pl.DataFrame({"acct": model_accts, "mg": model_growth}).unique(subset=["acct"])

    ent_vals = [base_v[a] for a in ent_purchases_in_sample if a in base_v and base_v[a] > 0]
    if ent_vals:
        v_lo = np.percentile(ent_vals, 5)
        v_hi = np.percentile(ent_vals, 95)
        v_med = np.median(ent_vals)
    else:
        v_lo, v_hi, v_med = 0, 1e12, 0

    mdf = mdf.with_columns(
        pl.col("acct").map_elements(lambda a: base_v.get(a, 0), return_dtype=pl.Float64).alias("val")
    )
    mdf_comparable = mdf.filter((pl.col("val") >= v_lo) & (pl.col("val") <= v_hi))
    mdf_sorted = mdf_comparable.sort("mg", descending=True)
    model_top_n = set(mdf_sorted.head(max(n_ent_in_sample, 1))["acct"].to_list())

    print(f"  Price bracket: ${v_lo/1e3:.0f}K–${v_hi/1e3:.0f}K (median ${v_med/1e3:.0f}K)")
    print(f"  Comparable pool: {len(mdf_comparable):,}  Model top-{n_ent_in_sample}")

    # ── Compare at each horizon ──
    for hold in [1, 2, 3, 4, 5]:
        eyr = ORIGIN + hold
        if eyr not in actual_vals:
            continue
        fv = actual_vals[eyr]

        def pct_return(s):
            return [(fv.get(a,0)-base_v.get(a,0))/base_v[a]*100
                    for a in s if base_v.get(a,0)>0 and a in fv]

        def dollar_return(s):
            sv, ev, n = 0.0, 0.0, 0
            for a in s:
                b = base_v.get(a, 0)
                e = fv.get(a)
                if b > 0 and e is not None:
                    sv += b; ev += e; n += 1
            return sv, ev, n

        cr = pct_return(list(base_v.keys())[:100000])
        er = pct_return(ent_purchases_in_sample)
        mr = pct_return(model_top_n)

        # Spearman ρ
        rho_m = None
        j = mdf.join(pl.DataFrame({"acct":list(base_v.keys())}), on="acct")
        p_list, a_list = j["acct"].to_list(), j["mg"].to_list()
        av = [((fv.get(x,0)-base_v.get(x,0))/base_v[x]*100) if base_v.get(x,0)>0 and x in fv else None for x in p_list]
        vv = [(x,y) for x,y in zip(a_list,av) if y is not None]
        if len(vv) > 10: rho_m, _ = spearmanr([x[0] for x in vv],[x[1] for x in vv])

        cm = np.median(cr) if cr else 0
        em = np.median(er) if er else 0
        mm_ = np.median(mr) if mr else 0

        # ══════════════════════════════════════════════════════════
        # SCREENING EFFICIENCY: which model quartile do entity buys fall in?
        # ══════════════════════════════════════════════════════════
        h_growth_dict = per_horizon_growth.get(hold, {})
        all_scores = sorted(h_growth_dict.values())
        if len(all_scores) >= 4:
            q25 = np.percentile(all_scores, 25)
            q50 = np.percentile(all_scores, 50)
            q75 = np.percentile(all_scores, 75)

            ent_in_q = [0, 0, 0, 0]  # Q1(bottom), Q2, Q3, Q4(top)
            ent_scored = 0
            for a in ent_purchases_in_sample:
                s = h_growth_dict.get(a)
                if s is None:
                    continue
                ent_scored += 1
                if s >= q75:
                    ent_in_q[3] += 1
                elif s >= q50:
                    ent_in_q[2] += 1
                elif s >= q25:
                    ent_in_q[1] += 1
                else:
                    ent_in_q[0] += 1

            if ent_scored > 0:
                q_pcts = [q/ent_scored*100 for q in ent_in_q]
                screening_rows.append({
                    "origin": ORIGIN, "horizon": hold,
                    "q1_pct": q_pcts[0], "q2_pct": q_pcts[1],
                    "q3_pct": q_pcts[2], "q4_pct": q_pcts[3],
                    "n": ent_scored,
                })

        # ══════════════════════════════════════════════════════════
        # ENTITY SEGMENTATION: compare model vs each entity type
        # ══════════════════════════════════════════════════════════
        for seg_name, seg_accts in sorted(ent_by_segment.items()):
            seg_r = pct_return(seg_accts)
            seg_n = len(seg_r)
            if seg_n < 3:
                continue
            seg_med = np.median(seg_r)

            # Model's top-N from comparable pool, where N = this segment's count
            seg_model_top = set(mdf_sorted.head(max(seg_n, 1))["acct"].to_list())
            seg_mr = pct_return(seg_model_top)
            seg_mm = np.median(seg_mr) if seg_mr else 0

            segment_rows.append({
                "origin": ORIGIN, "horizon": hold, "segment": seg_name,
                "n": seg_n, "ent_med": seg_med, "model_med": seg_mm,
                "diff": seg_mm - seg_med,
            })

        # ══════════════════════════════════════════════════════════
        # PER-NAMED-ENTITY: group purchases by owner, compare each
        # ══════════════════════════════════════════════════════════
        ent_by_name = defaultdict(set)
        for a in ent_purchases_in_sample:
            name = ow_current.get(a, "").strip()
            if name:
                ent_by_name[name].add(a)

        for ent_name, ent_accts_set in ent_by_name.items():
            e_r = pct_return(ent_accts_set)
            e_n = len(e_r)
            if e_n < 2:  # need at least 2 purchases to be meaningful
                continue
            e_med = np.median(e_r)
            seg = classify_entity(ent_name) or "Unknown"

            # Model's top-N from comparable pool, N = this entity's purchase count
            m_top = set(mdf_sorted.head(max(e_n, 1))["acct"].to_list())
            m_r = pct_return(m_top)
            m_med = np.median(m_r) if m_r else 0

            entity_perf_rows.append({
                "origin": ORIGIN, "horizon": hold,
                "entity": ent_name, "segment": seg,
                "n_buys": e_n, "ent_med": e_med,
                "model_med": m_med, "diff": m_med - e_med,
            })

        # ── Counterfactual ──
        ent_sv, ent_ev, ent_n = dollar_return(ent_purchases_in_sample)
        mod_sv, mod_ev, mod_n = dollar_return(model_top_n)

        print(f"\n  ── {hold}-YEAR ({ORIGIN}→{eyr}) ──")
        if rho_m is not None: print(f"  Model ρ={rho_m:.3f}")

        print(f"\n  {'Strategy':<34} {'Median':>8} {'vs Cty':>8} {'n':>7}")
        print(f"  {'─'*34} {'─'*8} {'─'*8} {'─'*7}")
        print(f"  {'County':<34} {cm:>+7.1f}% {'—':>8} {len(cr):>7,}")
        print(f"  {'Ent purchases (in sample)':<34} {em:>+7.1f}% {em-cm:>+7.1f}pp {len(er):>7,}")
        print(f"  {'Model top-{0} 🔬'.format(n_ent_in_sample):<34} {mm_:>+7.1f}% {mm_-cm:>+7.1f}pp {len(mr):>7,}")

        # Screening efficiency inline
        if screening_rows and screening_rows[-1]["origin"] == ORIGIN and screening_rows[-1]["horizon"] == hold:
            sr = screening_rows[-1]
            print(f"\n  📊 SCREENING: Entity buys by model quartile")
            print(f"     Q1(bottom): {sr['q1_pct']:>5.1f}%  Q2: {sr['q2_pct']:>5.1f}%  "
                  f"Q3: {sr['q3_pct']:>5.1f}%  Q4(top): {sr['q4_pct']:>5.1f}%  (n={sr['n']})")
            if sr['q4_pct'] > 25:
                print(f"     → Entity buys cluster in model's top quartile ({sr['q4_pct']:.0f}% vs 25% expected)")
            elif sr['q1_pct'] > 25:
                print(f"     ⚠️ Entity buys cluster in model's BOTTOM quartile ({sr['q1_pct']:.0f}%)")

        if ent_n > 0 and mod_n > 0:
            ent_gain = ent_ev - ent_sv
            mod_gain = mod_ev - mod_sv
            ent_ret = (ent_gain / ent_sv * 100) if ent_sv > 0 else 0
            mod_ret = (mod_gain / mod_sv * 100) if mod_sv > 0 else 0
            delta_gain = mod_gain - ent_gain

            print(f"\n  💰 COUNTERFACTUAL (same bracket, {hold}yr)")
            print(f"     Entity: ${ent_sv/1e6:>7.1f}M → ${ent_ev/1e6:>7.1f}M  gain=${ent_gain/1e6:>7.1f}M  ({ent_ret:+.1f}%)  n={ent_n:,}")
            print(f"     Model:  ${mod_sv/1e6:>7.1f}M → ${mod_ev/1e6:>7.1f}M  gain=${mod_gain/1e6:>7.1f}M  ({mod_ret:+.1f}%)  n={mod_n:,}")
            if delta_gain > 0:
                print(f"     ✅ Model: ${delta_gain/1e6:>+7.1f}M  ({mod_ret-ent_ret:+.1f}pp)")
            else:
                print(f"     ⚠️  Entity: ${-delta_gain/1e6:>+7.1f}M  ({ent_ret-mod_ret:+.1f}pp)")

        model_vs_ent = mm_ - em
        summary_rows.append({
            "origin": ORIGIN, "horizon": hold, "end_year": eyr,
            "county": cm, "entity_purchases": em, "model_q4": mm_,
            "model_vs_county": mm_ - cm, "model_vs_entity": model_vs_ent,
            "rho": rho_m,
            "n_ent_buys": ent_n, "n_model": len(mr),
            "ent_gain_M": (ent_ev-ent_sv)/1e6 if ent_n > 0 else 0,
            "mod_gain_M": (mod_ev-mod_sv)/1e6 if mod_n > 0 else 0,
            "delta_gain_M": (mod_ev-mod_sv-ent_ev+ent_sv)/1e6 if ent_n > 0 and mod_n > 0 else 0,
        })

    # Free GPU memory
    del _bt_model, _bt_proj_g, _bt_proj_geo, _ckpt
    if _device == "cuda": torch.cuda.empty_cache()

# ══════════════════════════════════════════════════════════════════
# SUMMARY TABLES
# ══════════════════════════════════════════════════════════════════
print("\n" + "=" * 95)
print("📊 SUMMARY: Model vs Entity Purchases")
print("=" * 95)

if summary_rows:
    print(f"\n  {'Origin':>6} {'Hold':>4} {'→':>4}  {'County':>8} {'EntBuy':>8} {'Model':>8}  {'M-Ent':>7} {'Δ$':>9} {'ρ':>7}")
    print(f"  {'─'*6} {'─'*4} {'─'*4}  {'─'*8} {'─'*8} {'─'*8}  {'─'*7} {'─'*9} {'─'*7}")
    for row in summary_rows:
        rho_str = f"{row['rho']:.3f}" if row['rho'] is not None else "   —  "
        win = "✅" if row["model_vs_entity"] > 0 else "  "
        print(f"  {row['origin']:>6} {row['horizon']:>4}yr {row['end_year']:>4}"
              f"  {row['county']:>+7.1f}% {row['entity_purchases']:>+7.1f}% {row['model_q4']:>+7.1f}%"
              f"  {row['model_vs_entity']:>+6.1f}pp {row['delta_gain_M']:>+8.1f}M {rho_str} {win}")

    n_wins = sum(1 for row in summary_rows if row["model_vs_entity"] > 0)
    n_total = len(summary_rows)
    avg_vs_ent = np.mean([row["model_vs_entity"] for row in summary_rows])
    total_delta = sum(row["delta_gain_M"] for row in summary_rows)

    print(f"\n  {'─'*80}")
    print(f"  Model beats entity purchases: {n_wins}/{n_total}")
    print(f"  Avg Model vs Entity median:   {avg_vs_ent:+.1f}pp")
    print(f"  Cumulative Δ$: ${total_delta:+,.1f}M")

# ══════════════════════════════════════════════════════════════════
# SCREENING EFFICIENCY TABLE
# ══════════════════════════════════════════════════════════════════
if screening_rows:
    print("\n" + "=" * 80)
    print("📊 SCREENING EFFICIENCY: Where do entity buys land in model's ranking?")
    print("   If random: each quartile ≈ 25%. Deviation → model has screening signal.")
    print("=" * 80)
    print(f"\n  {'Origin':>6} {'Hold':>4}  {'Q1(bot)':>8} {'Q2':>6} {'Q3':>6} {'Q4(top)':>8}  {'n':>5}  Signal")
    print(f"  {'─'*6} {'─'*4}  {'─'*8} {'─'*6} {'─'*6} {'─'*8}  {'─'*5}  {'─'*20}")
    for sr in screening_rows:
        signal = ""
        if sr["q4_pct"] > 30: signal = "✅ top-heavy"
        elif sr["q1_pct"] > 30: signal = "⚠️ bottom-heavy"
        else: signal = "— uniform"
        print(f"  {sr['origin']:>6} {sr['horizon']:>4}yr"
              f"  {sr['q1_pct']:>7.1f}% {sr['q2_pct']:>5.1f}% {sr['q3_pct']:>5.1f}% {sr['q4_pct']:>7.1f}%"
              f"  {sr['n']:>5}  {signal}")

    # Average screening
    avg_q4 = np.mean([s["q4_pct"] for s in screening_rows])
    avg_q1 = np.mean([s["q1_pct"] for s in screening_rows])
    print(f"\n  Average Q4(top): {avg_q4:.1f}%  Q1(bottom): {avg_q1:.1f}%")
    if avg_q4 > 30:
        print(f"  → Model concentrates entity buys in top quartile → useful screener")
    elif avg_q1 > 30:
        print(f"  ⚠️ Model pushes entity buys to BOTTOM quartile → anti-correlated")
    else:
        print(f"  → Model quartiles roughly uniform — no strong screening signal")

# ══════════════════════════════════════════════════════════════════
# ENTITY SEGMENTATION TABLE
# ══════════════════════════════════════════════════════════════════
if segment_rows:
    print("\n" + "=" * 90)
    print("📊 ENTITY SEGMENTATION: Which entity types does the model match?")
    print("   Positive diff = model outperforms that segment's purchases")
    print("=" * 90)

    # Group by segment, show avg across origins/horizons
    seg_agg = defaultdict(lambda: {"diffs": [], "n_total": 0, "wins": 0, "total": 0})
    for sr in segment_rows:
        sa = seg_agg[sr["segment"]]
        sa["diffs"].append(sr["diff"])
        sa["n_total"] += sr["n"]
        sa["total"] += 1
        if sr["diff"] > 0:
            sa["wins"] += 1

    print(f"\n  {'Segment':<15} {'Avg M-Ent':>9} {'Win Rate':>9} {'Avg n':>7}  Assessment")
    print(f"  {'─'*15} {'─'*9} {'─'*9} {'─'*7}  {'─'*30}")

    for seg in sorted(seg_agg.keys(), key=lambda s: np.mean(seg_agg[s]["diffs"]), reverse=True):
        sa = seg_agg[seg]
        avg_d = np.mean(sa["diffs"])
        win_pct = sa["wins"] / sa["total"] * 100 if sa["total"] > 0 else 0
        avg_n = sa["n_total"] / sa["total"] if sa["total"] > 0 else 0

        if avg_d > -5:
            assess = "🟢 Potential market"
        elif avg_d > -15:
            assess = "🟡 Competitive"
        else:
            assess = "🔴 Entity dominates"

        print(f"  {seg:<15} {avg_d:>+8.1f}pp {win_pct:>7.0f}%  {avg_n:>6.0f}  {assess}")

    # Detailed per-origin breakdown for top segments
    best_segs = sorted(seg_agg.keys(), key=lambda s: np.mean(seg_agg[s]["diffs"]), reverse=True)[:3]
    if best_segs:
        print(f"\n  ── Top segments detail ──")
        print(f"  {'Segment':<15} {'Origin':>6} {'Hold':>4}  {'EntMed':>8} {'ModMed':>8} {'Diff':>7} {'n':>4}")
        print(f"  {'─'*15} {'─'*6} {'─'*4}  {'─'*8} {'─'*8} {'─'*7} {'─'*4}")
        for sr in sorted(segment_rows, key=lambda x: (x["segment"], x["origin"], x["horizon"])):
            if sr["segment"] in best_segs:
                win = "✅" if sr["diff"] > 0 else "  "
                print(f"  {sr['segment']:<15} {sr['origin']:>6} {sr['horizon']:>4}yr"
                      f"  {sr['ent_med']:>+7.1f}% {sr['model_med']:>+7.1f}% {sr['diff']:>+6.1f}pp {sr['n']:>4} {win}")

# ══════════════════════════════════════════════════════════════════
# NAMED ENTITY LEADERBOARD
# ══════════════════════════════════════════════════════════════════
if entity_perf_rows:
    print("\n" + "=" * 100)
    print("🎯 TARGET CUSTOMER LEADERBOARD: Named entities model matches or outperforms")
    print("   Entities with ≥2 purchases in sample, ranked by model advantage")
    print("=" * 100)

    # Aggregate per entity across all origins and horizons
    ent_agg = defaultdict(lambda: {"diffs": [], "n_total": 0, "wins": 0, "total": 0,
                                    "segment": "", "ent_meds": [], "mod_meds": []})
    for ep in entity_perf_rows:
        ea = ent_agg[ep["entity"]]
        ea["diffs"].append(ep["diff"])
        ea["ent_meds"].append(ep["ent_med"])
        ea["mod_meds"].append(ep["model_med"])
        ea["n_total"] += ep["n_buys"]
        ea["total"] += 1
        ea["segment"] = ep["segment"]
        if ep["diff"] > 0:
            ea["wins"] += 1

    # Sort by avg diff descending (model advantage)
    ranked = sorted(ent_agg.items(), key=lambda x: np.mean(x[1]["diffs"]), reverse=True)

    # Split into ICP (real investors) and non-ICP (HOAs, government, industrial)
    ranked_icp = [(n, ea) for n, ea in ranked if is_icp(n)]
    ranked_non_icp = [(n, ea) for n, ea in ranked if not is_icp(n)]

    # ── ICP-only leaderboard ──
    outperformed = [(name, ea) for name, ea in ranked_icp if np.mean(ea["diffs"]) > 0]
    matched = [(name, ea) for name, ea in ranked_icp if -5 <= np.mean(ea["diffs"]) <= 0]
    underperformed = [(name, ea) for name, ea in ranked_icp if np.mean(ea["diffs"]) < -5]

    if outperformed:
        print(f"\n  🟢 MODEL OUTPERFORMS ({len(outperformed)} ICP entities) — strongest sales leads")
        print(f"  {'Entity':<45} {'Segment':<12} {'Avg Diff':>8} {'Wins':>6} {'Buys':>6}")
        print(f"  {'─'*45} {'─'*12} {'─'*8} {'─'*6} {'─'*6}")
        for name, ea in outperformed[:30]:
            avg_d = np.mean(ea["diffs"])
            win_str = f"{ea['wins']}/{ea['total']}"
            print(f"  {name[:44]:<45} {ea['segment']:<12} {avg_d:>+7.1f}pp {win_str:>6} {ea['n_total']:>5}")

    if matched:
        print(f"\n  🟡 MODEL COMPETITIVE ({len(matched)} ICP entities) — within 5pp")
        print(f"  {'Entity':<45} {'Segment':<12} {'Avg Diff':>8} {'Wins':>6} {'Buys':>6}")
        print(f"  {'─'*45} {'─'*12} {'─'*8} {'─'*6} {'─'*6}")
        for name, ea in matched[:20]:
            avg_d = np.mean(ea["diffs"])
            win_str = f"{ea['wins']}/{ea['total']}"
            print(f"  {name[:44]:<45} {ea['segment']:<12} {avg_d:>+7.1f}pp {win_str:>6} {ea['n_total']:>5}")

    if underperformed:
        print(f"\n  🔴 ENTITY DOMINATES ({len(underperformed)} ICP entities) — model trails by >5pp")
        print(f"  {'Entity':<45} {'Segment':<12} {'Avg Diff':>8} {'Wins':>6} {'Buys':>6}")
        print(f"  {'─'*45} {'─'*12} {'─'*8} {'─'*6} {'─'*6}")
        for name, ea in underperformed[:20]:
            avg_d = np.mean(ea["diffs"])
            win_str = f"{ea['wins']}/{ea['total']}"
            print(f"  {name[:44]:<45} {ea['segment']:<12} {avg_d:>+7.1f}pp {win_str:>6} {ea['n_total']:>5}")

    # Summary
    n_icp = len(ranked_icp)
    print(f"\n  {'─'*80}")
    print(f"  Total named entities: {len(ent_agg)}  |  ICP filter: {n_icp} kept, {len(ranked_non_icp)} excluded")
    print(f"  Model outperforms: {len(outperformed)} ({len(outperformed)/max(n_icp,1)*100:.0f}% of ICP)")
    print(f"  Model competitive: {len(matched)} ({len(matched)/max(n_icp,1)*100:.0f}% of ICP)")
    print(f"  Entity dominates:  {len(underperformed)} ({len(underperformed)/max(n_icp,1)*100:.0f}% of ICP)")

    if ranked_non_icp:
        print(f"\n  ── Excluded non-ICP entities (HOAs, govt, industrial): {len(ranked_non_icp)} ──")
        # Show top 5 excluded to confirm filter is working
        for name, ea in ranked_non_icp[:5]:
            avg_d = np.mean(ea["diffs"])
            print(f"    ✂️  {name[:50]}  ({ea['segment']}, {avg_d:+.1f}pp)")
        if len(ranked_non_icp) > 5:
            print(f"    ... and {len(ranked_non_icp)-5} more")

    # Top 10 ICP target customers with detail
    if outperformed:
        print(f"\n  ── Top 10 ICP target customer detail ──")
        for name, ea in outperformed[:10]:
            avg_ent = np.mean(ea["ent_meds"])
            avg_mod = np.mean(ea["mod_meds"])
            avg_d = np.mean(ea["diffs"])
            print(f"  📌 {name}")
            print(f"     Segment: {ea['segment']}  Total buys: {ea['n_total']}  "
                  f"Observations: {ea['total']}  Win rate: {ea['wins']}/{ea['total']}")
            print(f"     Their avg return: {avg_ent:+.1f}%  Model avg: {avg_mod:+.1f}%  "
                  f"Advantage: {avg_d:+.1f}pp")

print(f"\n  📋 Track-record: {sum(1 for a,r in track_record.items() if r):,} parcels tracked")
print("\n✅ Multi-origin backtest complete!")

# ══════════════════════════════════════════════════════════════════
# SAVE RESULTS TO JSON
# ══════════════════════════════════════════════════════════════════
_results_ts = time.strftime("%Y%m%d_%H%M%S")
_results = {
    "timestamp": _results_ts,
    "origins": BACKTEST_ORIGINS,
    "sample_size": BACKTEST_SAMPLE_SIZE,
    "entity_sample_mode": ENTITY_SAMPLE_MODE,
    "entity_cap": ENTITY_CAP if ENTITY_SAMPLE_MODE == 'capped' else 'all',
    "summary": summary_rows,
    "screening": screening_rows,
    "segments": segment_rows,
    "entity_perf": entity_perf_rows,
}
_results_dir = os.path.join(_out_dir, "backtest_results")
os.makedirs(_results_dir, exist_ok=True)
_results_path = os.path.join(_results_dir, f"backtest_{ENTITY_SAMPLE_MODE}_{_results_ts}.json")
try:
    with open(_results_path, "w") as _f:
        json.dump(_results, _f, indent=2, default=str)
    print(f"\n\U0001f4be Results saved: {_results_path}")
except Exception as _e:
    print(f"\n\u26a0\ufe0f  Could not save results: {_e}")

# W&B logging disabled — re-enable when a persistent run is available
