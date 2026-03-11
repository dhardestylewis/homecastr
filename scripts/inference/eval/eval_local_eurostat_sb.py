import os
import sys
import time
import torch
import polars as pl
import numpy as np
from scipy.stats import kstest, spearmanr

sys.stdout.reconfigure(encoding='utf-8')
ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    data_path = os.path.join(base_dir, "_scratch", "data", "eurostat_adapted.parquet")
    ckpt_dir = os.path.join(base_dir, "output", "eurostat_v12sb")
    
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Missing {data_path}")
        
    print(f"[{ts()}] Loading Eurostat adapted data from {data_path}")
    df_actuals = pl.read_parquet(data_path)
    
    origins = [2019, 2020, 2021, 2022, 2023, 2024]
    # Load holdout accts
    holdout_path = os.path.join(base_dir, "_scratch", "data", "eurostat_holdout_accts.txt")
    holdout_accts = set()
    if os.path.exists(holdout_path):
        with open(holdout_path, "r", encoding="utf-8") as f:
            holdout_accts = {line.strip() for line in f if line.strip()}
        print(f"[{ts()}] Loaded {len(holdout_accts)} geoholdout accounts")
    else:
        print(f"[{ts()}] ⚠️ No holdout file found, evaluating on ALL regions")

    # Pre-extract actuals for evaluation
    actual_vals = {}
    for origin in origins:
        for yr in [origin] + [origin + h for h in range(1, 6)]:
            if yr not in actual_vals:
                yr_df = df_actuals.filter(pl.col("yr") == yr).select(["acct", "tot_appr_val"])
                # Only keep ground truth for the holdout set if it exists
                if holdout_accts:
                    yr_df = yr_df.filter(pl.col("acct").is_in(list(holdout_accts)))
                actual_vals[yr] = dict(zip(yr_df["acct"].to_list(), yr_df["tot_appr_val"].to_list()))

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
    yr_min = int(df_actuals["yr"].min())
    yr_max = int(df_actuals["yr"].max())
    wm_globals['MIN_YEAR'] = yr_min
    wm_globals['MAX_YEAR'] = yr_max
    wm_globals['SEAM_YEAR'] = yr_max
    wm_globals['S_BLOCK'] = 16
    
    with open(wm_v11_path, "r", encoding="utf-8") as f:
        wm_v11_source = f.read()
        
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
            m = m[:d]
            s = s[:d]
        return (x - m) / _np_patch.maximum(s, 1e-8)
    def inverse_transform(self, x):
        d = x.shape[-1] if hasattr(x, 'shape') else len(x)
        m, s = self.mean, self.scale
        if len(m) < d:
            m = _np_patch.concatenate([m, _np_patch.zeros(d - len(m))])
            s = _np_patch.concatenate([s, _np_patch.ones(d - len(s))])
        elif len(m) > d:
            m = m[:d]
            s = s[:d]
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
    print(f"[{ts()}] Context loaded (v12_sb). Device: {_device}")
    
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
    # Speed up inference: N_STEPS=16 instead of 32, S=32 instead of 128
    scenarios = 32
    
    def _strip(d): return {k.replace("_orig_mod.", ""): v for k, v in d.items()}
    
    # Try importing properscoring for CRPS
    try:
        from properscoring import crps_ensemble
        HAS_CRPS = True
    except ImportError:
        print(f"[{ts()}] properscoring not installed, skipping CRPS metrics")
        HAS_CRPS = False
    
    for origin in origins:    
        ckpt_path = os.path.join(ckpt_dir, f"ckpt_v12sb_eurostat_origin_{origin}.pt")
        if not os.path.exists(ckpt_path):
            print(f"[{ts()}] ⚠️ Checkpoint not found for origin {origin}, skipping")
            continue
            
        print(f"\n==============================================")
        print(f"[{ts()}] ── Evaluating Origin {origin} ──")
        print(f"==============================================")
        
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
            n_pad = num_dim - len(_n_mean)
            _n_mean = np.concatenate([_n_mean, np.zeros(n_pad)])
            _n_scale = np.concatenate([_n_scale, np.ones(n_pad)])
        elif len(_n_mean) > num_dim:
            _n_mean = _n_mean[:num_dim]
            _n_scale = _n_scale[:num_dim]
        sf2m_net._n_scaler = SimpleScaler(mean=_n_mean, scale=_n_scale)
        sf2m_net._t_scaler = SimpleScaler(mean=np.array(ckpt["t_scaler_mean"]), scale=np.array(ckpt["t_scaler_scale"]))
        
        origin_accts = list(actual_vals.get(origin, {}).keys())
        if len(origin_accts) == 0:
            print(f"[{ts()}] ⚠️ No ground truth at origin {origin}, skipping")
            continue
            
        ctx = build_inference_context(
            lf=lf, accts=origin_accts, num_use_local=num_use_local, cat_use_local=cat_use_local,
            global_medians=ckpt.get("global_medians", {}), anchor_year=origin, max_parcels=min(500000, len(origin_accts))
        )
        
        actual_dim = ctx["cur_num"].shape[1]
        if actual_dim < num_dim:
            n_pad = num_dim - actual_dim
            pad = np.zeros((ctx["cur_num"].shape[0], n_pad), dtype=ctx["cur_num"].dtype)
            ctx["cur_num"] = np.concatenate([ctx["cur_num"], pad], axis=1)
        elif actual_dim > num_dim:
            ctx["cur_num"] = ctx["cur_num"][:, :num_dim]
            
        _actual_hist = ctx["hist_y"].shape[1]
        if _actual_hist < hist_len:
            pad = np.zeros((ctx["hist_y"].shape[0], hist_len - _actual_hist), dtype=ctx["hist_y"].dtype)
            ctx["hist_y"] = np.concatenate([pad, ctx["hist_y"]], axis=1)
        elif _actual_hist > hist_len:
            ctx["hist_y"] = ctx["hist_y"][:, -hist_len:]
            
        n_valid = len(ctx["acct"])
        print(f"[{ts()}] Built context for {n_valid:,} NUTS regions/crops")
        
        _sweep = ckpt.get("sweep", {})
        bridge_sched = BridgeSchedule(
            sigma_max=float(_sweep.get("sigma_max", 1.0)),
            n_steps=16, # Faster inference
        )
        
        phi_vec = token_persistence.get_phi()
        Z_tokens = sample_token_paths(K=int(_cfg.get("K_TOKENS", 8)), H=H, phi_vec=phi_vec, S=scenarios, device=_device)
        
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
            if np.any(np.isnan(b_deltas)):
                b_deltas = np.nan_to_num(b_deltas, nan=0.0)
            if np.any(np.isinf(b_deltas)):
                b_deltas = np.clip(b_deltas, -10, 10)
            all_deltas.append(b_deltas)
            
        deltas = np.concatenate(all_deltas, axis=0)
        
        accts = ctx["acct"]
        ya = ctx["y_anchor"]
        y_levels = ya[:, None, None] + np.cumsum(deltas, axis=2)  # [N, S, H] log-space
        
        base_v = actual_vals.get(origin, {})
        
        print(f"\n[{ts()}] ── Metrics for Origin {origin} ──")
        for h in range(1, MAX_HORIZON + 1):
            h_idx = h - 1
            eyr = origin + h
            if eyr not in actual_vals:
                continue
                
            future_v = actual_vals[eyr]
            
            preds = []
            acts = []
            fan_hits = 0
            fan_checks = 0
            crps_log_vals = []
            
            for i in range(len(accts)):
                acct = str(accts[i]).strip()
                bv = base_v.get(acct, 0)
                av = future_v.get(acct)
                
                if bv <= 0 or av is None:
                    continue
                    
                fan = y_levels[i, :, h_idx]
                p10_dollar = np.exp(np.nanpercentile(fan, 10))
                p90_dollar = np.exp(np.nanpercentile(fan, 90))
                
                pred_growth = float(np.expm1(np.nanmedian(fan) - ya[i]) * 100)
                actual_growth = float((av - bv) / bv * 100)
                preds.append(pred_growth)
                acts.append(actual_growth)
                
                fan_checks += 1
                if p10_dollar <= av <= p90_dollar:
                    fan_hits += 1
                
                if HAS_CRPS:
                    try:
                        _crps_log = crps_ensemble(np.log1p(av), fan)
                        crps_log_vals.append(float(_crps_log) * 100)
                    except Exception:
                        pass
                        
            preds_arr = np.array(preds)
            acts_arr = np.array(acts)
            
            if len(preds) > 10:
                abs_err = np.abs(preds_arr - acts_arr)
                mdae = float(np.median(abs_err))
                bias = float(np.median(preds_arr - acts_arr))  # Median Error
                
                non_zero_acts = acts_arr[acts_arr != 0]
                non_zero_preds = preds_arr[acts_arr != 0]
                if len(non_zero_acts) > 0:
                    mape = float(np.mean(np.abs(non_zero_preds - non_zero_acts) / np.abs(non_zero_acts))) * 100
                    mdape = float(np.median(np.abs(non_zero_preds - non_zero_acts) / np.abs(non_zero_acts))) * 100
                else:
                    mape = float('nan')
                    mdape = float('nan')
                
                if len(set(preds)) > 1 and len(set(acts)) > 1:
                    rho, _ = spearmanr(preds, acts)
                else:
                    rho = 0.0
                    
                coverage = float(fan_hits / fan_checks * 100) if fan_checks > 0 else float('nan')
                crps_log_mean = float(np.mean(crps_log_vals)) if HAS_CRPS and crps_log_vals else float('nan')
                
                print(f"  Horizon {h} (Year {eyr}): ρ: {rho:+.3f} | MdAE: {mdae:.1f}% | MdAPE: {mdape:.1f}% | Bias: {bias:+.1f}% | Covg: {coverage:.1f}% | CRPS: {crps_log_mean:.3f}% (n={len(preds)})")

if __name__ == "__main__":
    main()
