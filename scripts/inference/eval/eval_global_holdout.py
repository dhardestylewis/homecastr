"""
eval_global_holdout.py
======================
Evaluates each global model variant on its held-out source group.
For each variant (geo20, usda, australia, latam, canada):
  1. Load the trained checkpoints
  2. Build inference context for held-out accounts only
  3. Run inference and compute: rho, bias, MdAE, MdAPE, coverage, CRPS

Usage:
  python eval_global_holdout.py --variant usda
  python eval_global_holdout.py --variant geo20
  python eval_global_holdout.py --all
"""

import os, sys, time, argparse, hashlib
import torch
import polars as pl
import numpy as np
from scipy.stats import spearmanr

sys.stdout.reconfigure(encoding='utf-8')
ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")


def get_holdout_accts(df, variant):
    """Return the set of held-out account names for this variant."""
    if variant == "geo20":
        all_accts = df["acct"].unique().to_list()
        return {a for a in all_accts if int(hashlib.md5(a.encode("utf-8")).hexdigest()[:8], 16) % 100 < 20}
    else:
        return set(df.filter(pl.col("source_group") == variant)["acct"].unique().to_list())


def eval_variant(variant, base_dir):
    """Evaluate a single holdout variant."""
    print(f"\n{'='*60}")
    print(f"[{ts()}] EVALUATING VARIANT: {variant}")
    print(f"{'='*60}")
    
    ckpt_dir = os.path.join(base_dir, "output", f"global_v12sb_{variant}")
    data_path = os.path.join(base_dir, "_scratch", "data", "global_adapted.parquet")
    
    if not os.path.exists(ckpt_dir):
        print(f"[{ts()}] Checkpoint dir not found: {ckpt_dir}")
        return None
    
    ckpt_files = sorted([f for f in os.listdir(ckpt_dir) if f.endswith(".pt")])
    if not ckpt_files:
        print(f"[{ts()}] No checkpoints found in {ckpt_dir}")
        return None
    
    df = pl.read_parquet(data_path)
    holdout_accts = get_holdout_accts(df, variant)
    print(f"[{ts()}] Holdout accounts: {len(holdout_accts)}")
    
    # Extract origin years from checkpoint filenames
    origins = []
    for f in ckpt_files:
        try:
            origin = int(f.split("_origin_")[1].replace(".pt", ""))
            origins.append(origin)
        except:
            pass
    origins = sorted(origins)
    print(f"[{ts()}] Origins: {origins}")
    
    # Build actuals lookup
    actual_vals = {}
    for yr in range(min(origins), max(origins) + 6):
        yr_df = df.filter(
            (pl.col("yr") == yr) & pl.col("acct").is_in(list(holdout_accts))
        ).select(["acct", "tot_appr_val"])
        actual_vals[yr] = dict(zip(yr_df["acct"].to_list(), yr_df["tot_appr_val"].to_list()))
    
    # Load worldmodel
    os.environ["WM_MAX_ACCTS"] = "500000"
    os.environ["WM_SAMPLE_FRACTION"] = "1.0"
    os.environ["SKIP_WM_MAIN"] = "1"
    os.environ["INFERENCE_ONLY"] = "1"
    
    wm_v11_path = os.path.join(base_dir, "scripts", "inference", "worldmodel.py")
    wm_sb_path = os.path.join(base_dir, "scripts", "inference", "v12_sb", "worldmodel_sb.py")
    
    wm_globals = globals().copy()
    wm_globals['PANEL_PATH'] = data_path
    wm_globals['PANEL_PATH_DRIVE'] = data_path
    wm_globals['PANEL_PATH_LOCAL'] = data_path
    yr_min = int(df["yr"].min())
    yr_max = int(df["yr"].max())
    wm_globals['MIN_YEAR'] = yr_min
    wm_globals['MAX_YEAR'] = yr_max
    wm_globals['SEAM_YEAR'] = yr_max
    wm_globals['S_BLOCK'] = 16
    
    with open(wm_v11_path, "r", encoding="utf-8") as f:
        wm_v11_source = f.read()
    
    # Patch SimpleScaler for dimension mismatches
    dim_safe_patch = '''
import numpy as _np_patch
_original_SimpleScaler = SimpleScaler
class _DimSafeScaler:
    def __init__(self, mean, scale):
        self.mean = _np_patch.asarray(mean).ravel()
        self.scale = _np_patch.asarray(scale).ravel()
    def transform(self, x):
        d = x.shape[-1] if hasattr(x, 'shape') else len(x)
        m, s = self.mean, self.scale
        if len(m) < d:
            m = _np_patch.concatenate([m, _np_patch.zeros(d - len(m))])
            s = _np_patch.concatenate([s, _np_patch.ones(d - len(s))])
        elif len(m) > d:
            m = m[:d]; s = s[:d]
        return (x - m) / _np_patch.maximum(s, 1e-8)
    def inverse_transform(self, x):
        d = x.shape[-1] if hasattr(x, 'shape') else len(x)
        m, s = self.mean, self.scale
        if len(m) < d:
            m = _np_patch.concatenate([m, _np_patch.zeros(d - len(m))])
            s = _np_patch.concatenate([s, _np_patch.ones(d - len(s))])
        elif len(m) > d:
            m = m[:d]; s = s[:d]
        return x * s + m
SimpleScaler = _DimSafeScaler
'''
    import re as _re
    _match = _re.search(r'class SimpleScaler.*?(?=\nclass |\ndef [a-zA-Z]|\n[A-Z_]+\s*=)', wm_v11_source, _re.DOTALL)
    if _match:
        insert_pos = _match.end()
        wm_v11_source = wm_v11_source[:insert_pos] + "\n" + dim_safe_patch + "\n" + wm_v11_source[insert_pos:]
    else:
        wm_v11_source += "\n" + dim_safe_patch
    
    wm_v11_source = wm_v11_source.replace("if __name__ == '__main__' or globals().get('__colab__'):", "if False:")
    
    wm_globals['__file__'] = wm_v11_path
    exec(wm_v11_source, wm_globals)
    
    with open(wm_sb_path, "r", encoding="utf-8") as f:
        wm_sb_source = f.read()
    wm_globals['__file__'] = wm_sb_path
    exec(wm_sb_source, wm_globals)
    
    _device = "cuda" if torch.cuda.is_available() else "cpu"
    
    lf = wm_globals["lf"]
    create_sf2m_network = wm_globals["create_sf2m_network"]
    create_gating_network = wm_globals["create_gating_network"]
    create_token_persistence = wm_globals["create_token_persistence"]
    create_coherence_scale = wm_globals["create_coherence_scale"]
    sample_token_paths = wm_globals["sample_token_paths_learned"]
    sample_sf2m = wm_globals["sample_sf2m_v12"]
    BridgeSchedule = wm_globals["BridgeSchedule"]
    SimpleScaler = wm_globals["SimpleScaler"]
    build_inference_context = wm_globals["build_inference_context_chunked_v102"]
    
    MAX_HORIZON = 5
    scenarios = 32
    
    def _strip(d):
        return {k.replace("_orig_mod.", ""): v for k, v in d.items()}
    
    try:
        from properscoring import crps_ensemble
        HAS_CRPS = True
    except ImportError:
        HAS_CRPS = False
    
    all_results = []
    
    for origin in origins:
        ckpt_path = os.path.join(ckpt_dir, f"ckpt_v12sb_global_{variant}_origin_{origin}.pt")
        if not os.path.exists(ckpt_path):
            continue
        
        ckpt = torch.load(ckpt_path, map_location=_device, weights_only=False)
        _cfg = ckpt.get("cfg", {})
        H = int(_cfg.get("H", MAX_HORIZON))
        
        num_use_local = ckpt.get("num_use", [])
        cat_use_local = ckpt.get("cat_use", ['geo', 'agriprod'])
        
        sd = _strip(ckpt["sf2m_net_state_dict"])
        hist_len = sd["hist_enc.0.weight"].shape[1]
        num_dim = sd["num_enc.0.weight"].shape[1]
        n_cat = len([k for k in sd if k.startswith("cat_embs.") and k.endswith(".weight")])
        
        sf2m_net = create_sf2m_network(target_dim=H, hist_len=hist_len, num_dim=num_dim, n_cat=n_cat)
        sf2m_net.load_state_dict(sd)
        sf2m_net = sf2m_net.to(_device).eval()
        
        gating_sd = _strip(ckpt["gating_net_state_dict"])
        has_macro = "year_emb.weight" in gating_sd
        gating_net = create_gating_network(hist_len=hist_len, num_dim=num_dim, n_cat=n_cat, use_macro=has_macro)
        gating_net.load_state_dict(gating_sd)
        gating_net = gating_net.to(_device).eval()
        
        token_persistence = create_token_persistence()
        if "token_persistence_state_dict" in ckpt:
            token_persistence.load_state_dict(ckpt["token_persistence_state_dict"])
        token_persistence = token_persistence.to(_device).eval()
        
        coh_scale = create_coherence_scale()
        if "coh_scale_state_dict" in ckpt:
            coh_scale.load_state_dict(ckpt["coh_scale_state_dict"])
        coh_scale = coh_scale.to(_device).eval()
        
        sf2m_net._y_scaler = SimpleScaler(mean=np.array(ckpt["y_scaler_mean"]), scale=np.array(ckpt["y_scaler_scale"]))
        _n_mean = np.array(ckpt["n_scaler_mean"])
        _n_scale = np.array(ckpt["n_scaler_scale"])
        if len(_n_mean) < num_dim:
            _n_mean = np.concatenate([_n_mean, np.zeros(num_dim - len(_n_mean))])
            _n_scale = np.concatenate([_n_scale, np.ones(num_dim - len(_n_scale))])
        elif len(_n_mean) > num_dim:
            _n_mean = _n_mean[:num_dim]
            _n_scale = _n_scale[:num_dim]
        sf2m_net._n_scaler = SimpleScaler(mean=_n_mean, scale=_n_scale)
        sf2m_net._t_scaler = SimpleScaler(mean=np.array(ckpt["t_scaler_mean"]), scale=np.array(ckpt["t_scaler_scale"]))
        
        # Build context for holdout accts at this origin
        origin_accts = [a for a in actual_vals.get(origin, {}).keys() if a in holdout_accts]
        if not origin_accts:
            continue
        
        ctx = build_inference_context(
            lf=lf, accts=origin_accts, num_use_local=num_use_local, cat_use_local=cat_use_local,
            global_medians=ckpt.get("global_medians", {}), anchor_year=origin, max_parcels=len(origin_accts)
        )
        n_valid = len(ctx["acct"])
        
        # Pad/trim num dims
        actual_dim = ctx["cur_num"].shape[1]
        if actual_dim < num_dim:
            pad = np.zeros((ctx["cur_num"].shape[0], num_dim - actual_dim), dtype=ctx["cur_num"].dtype)
            ctx["cur_num"] = np.concatenate([ctx["cur_num"], pad], axis=1)
        elif actual_dim > num_dim:
            ctx["cur_num"] = ctx["cur_num"][:, :num_dim]
        
        _actual_hist = ctx["hist_y"].shape[1]
        if _actual_hist < hist_len:
            pad = np.zeros((ctx["hist_y"].shape[0], hist_len - _actual_hist), dtype=ctx["hist_y"].dtype)
            ctx["hist_y"] = np.concatenate([pad, ctx["hist_y"]], axis=1)
        elif _actual_hist > hist_len:
            ctx["hist_y"] = ctx["hist_y"][:, -hist_len:]
        
        _sweep = ckpt.get("sweep", {})
        bridge_sched = BridgeSchedule(
            sigma_max=float(_sweep.get("sigma_max", 1.0)),
            n_steps=16,
        )
        
        phi_vec = token_persistence.get_phi()
        Z_tokens = sample_token_paths(K=int(_cfg.get("K_TOKENS", 8)), H=H, phi_vec=phi_vec, S=scenarios, device=_device)
        
        # Inference
        batch_size = min(256, n_valid)
        all_deltas = []
        for b_start in range(0, n_valid, batch_size):
            b_end = min(b_start + batch_size, n_valid)
            with torch.no_grad():
                b_deltas = sample_sf2m(
                    sf2m_net=sf2m_net, gating_net=gating_net, bridge_sched=bridge_sched,
                    hist_y_b=ctx["hist_y"][b_start:b_end], cur_num_b=ctx["cur_num"][b_start:b_end],
                    cur_cat_b=ctx["cur_cat"][b_start:b_end], region_id_b=ctx["region_id"][b_start:b_end],
                    Z_tokens=Z_tokens, coh_scale=coh_scale, device=_device,
                    anchor_year=origin, mu_backbone=None,
                )
            b_deltas = np.nan_to_num(b_deltas, nan=0.0)
            b_deltas = np.clip(b_deltas, -10, 10)
            all_deltas.append(b_deltas)
        
        deltas = np.concatenate(all_deltas, axis=0)
        accts = ctx["acct"]
        ya = ctx["y_anchor"]
        y_levels = ya[:, None, None] + np.cumsum(deltas, axis=2)
        base_v = actual_vals.get(origin, {})
        
        for h in range(1, min(MAX_HORIZON + 1, yr_max - origin + 1)):
            eyr = origin + h
            if eyr not in actual_vals:
                continue
            
            future_v = actual_vals[eyr]
            preds, acts = [], []
            fan_hits, fan_checks = 0, 0
            crps_vals = []
            
            for i in range(len(accts)):
                acct = str(accts[i]).strip()
                bv = base_v.get(acct, 0)
                av = future_v.get(acct)
                if bv <= 0 or av is None:
                    continue
                
                fan = y_levels[i, :, h - 1]
                p10 = np.exp(np.nanpercentile(fan, 10))
                p90 = np.exp(np.nanpercentile(fan, 90))
                
                pred_growth = float(np.expm1(np.nanmedian(fan) - ya[i]) * 100)
                actual_growth = float((av - bv) / bv * 100)
                preds.append(pred_growth)
                acts.append(actual_growth)
                
                fan_checks += 1
                if p10 <= av <= p90:
                    fan_hits += 1
                
                if HAS_CRPS:
                    try:
                        crps_vals.append(float(crps_ensemble(np.log1p(av), fan)) * 100)
                    except Exception:
                        pass
            
            if len(preds) >= 3:
                preds_arr = np.array(preds)
                acts_arr = np.array(acts)
                mdae = float(np.median(np.abs(preds_arr - acts_arr)))
                bias = float(np.median(preds_arr - acts_arr))
                nz = acts_arr != 0
                mdape = float(np.median(np.abs(preds_arr[nz] - acts_arr[nz]) / np.abs(acts_arr[nz]))) * 100 if nz.sum() > 0 else float('nan')
                rho = spearmanr(preds, acts)[0] if len(set(preds)) > 1 and len(set(acts)) > 1 else 0.0
                covg = float(fan_hits / fan_checks * 100) if fan_checks > 0 else float('nan')
                crps_m = float(np.mean(crps_vals)) if crps_vals else float('nan')
                
                result = {
                    "variant": variant, "origin": origin, "horizon": h, "eval_yr": eyr,
                    "n": len(preds), "rho": rho, "bias": bias, "mdae": mdae,
                    "mdape": mdape, "coverage": covg, "crps": crps_m,
                }
                all_results.append(result)
                print(f"  O={origin} H{h} (Yr {eyr}): rho={rho:+.3f} | MdAE={mdae:.1f}% | MdAPE={mdape:.0f}% | Bias={bias:+.1f}% | Covg={covg:.0f}% | CRPS={crps_m:.1f}% (n={len(preds)})")
    
    # Summary across all origins
    if all_results:
        import pandas as pd
        rdf = pd.DataFrame(all_results)
        print(f"\n[{ts()}] === SUMMARY: {variant} ===")
        for h in sorted(rdf["horizon"].unique()):
            hdf = rdf[rdf["horizon"] == h]
            print(f"  H{h}: rho={hdf['rho'].mean():+.3f} | MdAE={hdf['mdae'].mean():.1f}% | MdAPE={hdf['mdape'].mean():.0f}% | Bias={hdf['bias'].mean():+.1f}% | Covg={hdf['coverage'].mean():.0f}% | CRPS={hdf['crps'].mean():.1f}% (avg n={hdf['n'].mean():.0f})")
    
    return all_results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", type=str, default=None)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    
    if args.all:
        variants = ["geo20", "usda", "australia", "latam", "canada"]
    elif args.variant:
        variants = [args.variant]
    else:
        variants = ["geo20"]
    
    all_summaries = {}
    for v in variants:
        results = eval_variant(v, base_dir)
        if results:
            all_summaries[v] = results
    
    # Cross-variant comparison
    if len(all_summaries) > 1:
        import pandas as pd
        print(f"\n{'='*60}")
        print(f"[{ts()}] CROSS-VARIANT COMPARISON")
        print(f"{'='*60}")
        
        for h in [1, 2, 3]:
            print(f"\n  --- Horizon {h} ---")
            for v, results in all_summaries.items():
                rdf = pd.DataFrame(results)
                hdf = rdf[rdf["horizon"] == h]
                if len(hdf) > 0:
                    print(f"    {v:<12}: rho={hdf['rho'].mean():+.3f} | MdAE={hdf['mdae'].mean():.1f}% | Bias={hdf['bias'].mean():+.1f}% | Covg={hdf['coverage'].mean():.0f}%")


if __name__ == "__main__":
    main()
