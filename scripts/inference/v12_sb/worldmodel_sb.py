# =============================================================================
# World Model v12_sb — Training code (SF2M)
# Simulation-Free Schrödinger Bridge via Score and Flow Matching
# =============================================================================
# This file is exec'd by train_modal_sb.py (same pattern as v11).
# It loads all model/data definitions from worldmodel_inference_sb.py,
# then adds the train_sf2m_v12 training loop.

import os, sys, time, math

# ─── Load all shared code from inference module ───
_sb_dir = os.path.dirname(os.path.abspath(__file__))
_inf_path = os.path.join(_sb_dir, "worldmodel_inference_sb.py")
with open(_inf_path, "r") as _f:
    exec(_f.read(), globals())

# At this point, all classes (SF2MNetwork, BridgeSchedule, OTCoupler,
# FTTransformerBackbone, GatingNetwork, etc.) and utility functions
# are in the global namespace.

import numpy as np
import torch
import torch.nn as nn
rng = np.random.default_rng(SEED)

# =============================================================================
# v12_sb SF2M: Unified flow+score training loop
# =============================================================================
def train_sf2m_v12(
    shard_paths: list,
    origin: int,
    epochs: int,
    sf2m_net: nn.Module,        # SF2MNetwork: shared trunk + flow_head + score_head
    gating_net: nn.Module,
    token_persistence,          # TokenPersistence
    coh_scale,                  # CoherenceScale
    device: str,
    num_dim: int,
    n_cat: int,
    work_dirs: dict,
    mu_backbone=None,           # AB1: FT-Transformer mu backbone
    ot_coupler=None,            # OTCoupler: condition-aware minibatch OT
    resume_state: dict = None,  # Resume checkpoint dict (epoch, model states, optimizer, etc.)
    checkpoint_callback=None,   # callable(epoch, state_dict) -> None, saves resume ckpt
    checkpoint_every: int = 5,  # Save resume checkpoint every N epochs
):
    """
    SF2M training loop: simulation-free joint flow+score matching on SB path.

    Key design choices (per user feedback):
    1. UNIFIED NETWORK: Single SF2MNetwork with shared trunk, single forward pass.
    2. SCALED SCORE: Score head outputs sigma^2*nabla_log_p_t (more stable).
    3. CONDITION-AWARE OT: OTCoupler uses conditioning features in cost matrix.
    4. OT MICROBATCHING: OT runs on small chunks (1024), not full 64k batch.
    5. FROZEN AB1: mu_backbone is frozen by default (FREEZE_AB1=1).
    6. CLOSED-FORM TARGETS: BB conditional flow and scaled score, no simulation.

    Training objective:
        L = L_flow + SCORE_LOSS_WEIGHT * L_score

    where:
        L_flow  = E[ ||v_theta(t,x_t,c) - u_t(x_t|x_0,x_1)||^2 ]
        L_score = E[ ||scaled_s_theta(t,x_t,c) - scaled_score_target||^2 ]

    Note: scaled_score_target = -(x_t - mu_t) = sigma(t)^2 * nabla log p_t.
    This avoids division by the variance term, improving numerical stability.
    """
    if not shard_paths:
        raise ValueError(f"No shards for origin {origin}")

    phi_init = token_persistence.get_phi_list()
    sigma_init = coh_scale.get_sigma()
    _ab1_active = (mu_backbone is not None) and AB1_ENABLED
    _ot_active = (ot_coupler is not None) and _HAS_POT
    _freeze_ab1 = _ab1_active and FREEZE_AB1

    print(f"[{ts()}] train_sf2m_v12 origin={origin} shards={len(shard_paths)} epochs={epochs}")
    print(f"[{ts()}] phi_k = {[f'{p:.3f}' for p in phi_init]}  sigma_u={sigma_init:.3f}")
    print(f"[{ts()}] AB1={_ab1_active} FREEZE_AB1={_freeze_ab1} OT={_ot_active}")
    print(f"[{ts()}] SF2M: sigma_max={BRIDGE_SIGMA_MAX} score_weight={SCORE_LOSS_WEIGHT} "
          f"OT_micro={OT_MICROBATCH} OT_cond_w={OT_COND_WEIGHT}")

    y_floor = max(float(SCALE_FLOOR_Y), 0.10)
    y_scaler, n_scaler, t_scaler = fit_scalers_from_shards_v102_robust_y(
        shard_paths=shard_paths, num_dim=int(num_dim),
        scale_floor_y=float(y_floor), scale_floor_num=float(SCALE_FLOOR_NUM),
        scale_floor_tgt=float(SCALE_FLOOR_TGT), max_y_rows=500_000)

    assert_y_scaler_contract(y_scaler=y_scaler, shard_paths=shard_paths,
        z_clip=float(SAMPLER_Z_CLIP) if SAMPLER_Z_CLIP is not None else 20.0,
        max_check_rows=200_000, max_sat_frac=0.025)

    # Attach scalers to unified network
    sf2m_net._y_scaler = y_scaler
    sf2m_net._n_scaler = n_scaler
    sf2m_net._t_scaler = t_scaler

    scaled_dir = os.path.join(work_dirs["SCALED_SHARD_ROOT"], f"origin_{int(origin)}")
    scaled_paths = write_scaled_shards_v102(
        shard_paths_raw=shard_paths, out_dir_scaled=scaled_dir,
        y_scaler=y_scaler, n_scaler=n_scaler, t_scaler=t_scaler,
        num_dim=int(num_dim), keep_acct=False,
        use_float16=bool(SCALED_SHARDS_FLOAT16))
    print(f"[{ts()}] Scaled shards ready: {len(scaled_paths)}")
    sys.stdout.flush()

    # Bridge schedule
    bridge_sched = BridgeSchedule(n_steps=N_BRIDGE_STEPS, sigma_max=BRIDGE_SIGMA_MAX,
                                   sigma_min=BRIDGE_SIGMA_MIN, device=device)

    # Freeze AB1 if configured (train SB residual model only)
    if _freeze_ab1:
        for p in mu_backbone.parameters():
            p.requires_grad = False
        print(f"[{ts()}] AB1 mu_backbone FROZEN ({sum(1 for _ in mu_backbone.parameters())} params)")

    # Optimizer param groups — single unified network
    param_groups = [
        {"params": list(sf2m_net.parameters()), "lr": DRIFT_LR},
        {"params": list(gating_net.parameters()), "lr": DRIFT_LR},
        {"params": list(token_persistence.parameters()), "lr": DRIFT_LR * 10},
        {"params": list(coh_scale.parameters()), "lr": DRIFT_LR * 5},
    ]
    if _ab1_active and not _freeze_ab1:
        param_groups.append({"params": list(mu_backbone.parameters()), "lr": DRIFT_LR})

    n_params = sum(len(pg["params"]) for pg in param_groups)
    print(f"[{ts()}] Optimizer: {n_params} params, {len(param_groups)} groups")
    try:
        opt = torch.optim.AdamW(param_groups, weight_decay=1e-4, fused=True)
    except TypeError:
        opt = torch.optim.AdamW(param_groups, weight_decay=1e-4)

    lr_sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=int(epochs), eta_min=DRIFT_LR * 0.1)

    # ── Resume from checkpoint if provided ──
    start_epoch = 0
    _total_batches = 0
    losses, losses_flow, losses_score = [], [], []
    if resume_state is not None:
        start_epoch = resume_state.get("epoch", 0)
        _total_batches = resume_state.get("total_batches", 0)
        losses = resume_state.get("losses", [])
        sf2m_net.load_state_dict(resume_state["sf2m_net_state_dict"])
        gating_net.load_state_dict(resume_state["gating_net_state_dict"])
        token_persistence.load_state_dict(resume_state["token_persistence_state_dict"])
        coh_scale.load_state_dict(resume_state["coh_scale_state_dict"])
        if _ab1_active and resume_state.get("mu_backbone_state_dict") is not None:
            mu_backbone.load_state_dict(resume_state["mu_backbone_state_dict"])
        if "optimizer_state_dict" in resume_state:
            opt.load_state_dict(resume_state["optimizer_state_dict"])
        if "lr_scheduler_state_dict" in resume_state:
            lr_sched.load_state_dict(resume_state["lr_scheduler_state_dict"])
        print(f"[{ts()}] ♻️ Resumed from epoch {start_epoch}, {_total_batches} batches, best_loss={min(losses) if losses else 'N/A'}")

    sf2m_net.train(); gating_net.train()
    token_persistence.train(); coh_scale.train()
    if _ab1_active:
        if _freeze_ab1:
            mu_backbone.eval()  # frozen: keep in eval mode
        else:
            mu_backbone.train()

    autocast_ctx = get_autocast_ctx(device)

    # Diagnostics
    _diag_alpha_sum = None
    _diag_alpha_sq_sum = None
    _diag_alpha_entropy_sum = 0.0
    _diag_alpha_count = 0
    scaled_paths_local = list(scaled_paths)
    print(f"[{ts()}] Starting SF2M training: epochs {start_epoch+1}-{int(epochs)}")
    sys.stdout.flush()

    for ep in range(start_epoch, int(epochs)):
        t_ep0 = time.time()
        rng.shuffle(scaled_paths_local)
        ep_losses, ep_losses_flow, ep_losses_score = [], [], []
        _diag_alpha_sum = torch.zeros(K_TOKENS, device=device)
        _diag_alpha_sq_sum = torch.zeros(K_TOKENS, device=device)
        _diag_alpha_entropy_sum = 0.0
        _diag_alpha_count = 0

        for _shard_idx, (_, z) in enumerate(_iter_shards_npz(scaled_paths_local)):
            hist_y_s = z["hist_y_s"]
            cur_num_s = z["cur_num_s"]
            x0_s = z["x0_s"]    # scaled target deltas [N, H]
            mask = z["mask"]
            cur_cat = z["cur_cat"].astype(np.int64, copy=False)
            region_id = z["region_id"].astype(np.int64, copy=False)

            n = int(x0_s.shape[0])
            if n == 0: continue
            perm = rng.permutation(n)

            for start in range(0, n, int(DIFF_BATCH)):
                b = perm[start:start + int(DIFF_BATCH)]
                B = int(b.size)
                if B == 0: continue

                hy = torch.from_numpy(hist_y_s[b]).to(device, non_blocking=True)
                xn = torch.from_numpy(cur_num_s[b]).to(device, non_blocking=True) if int(num_dim) > 0 \
                     else torch.zeros((B, 0), device=device, dtype=torch.float32)
                x_target = torch.from_numpy(x0_s[b]).to(device, non_blocking=True).float()
                m = torch.from_numpy(mask[b]).to(device, non_blocking=True).float()
                xc = torch.from_numpy(cur_cat[b]).to(device, non_blocking=True)
                rid = torch.from_numpy(region_id[b]).to(device, non_blocking=True)

                # Sample token paths per batch
                Z_k = sample_token_paths_learned(
                    K_TOKENS, H, token_persistence.get_phi(), 1, device)
                Z_k = Z_k.expand(B, -1, -1).clone()

                try:
                    with autocast_ctx:
                        oy = torch.full((B,), int(origin), dtype=torch.long, device=device)
                        alpha = gating_net(hy.float(), xn.float(), xc, rid, origin_year=oy)

                        with torch.no_grad():
                            _diag_alpha_sum += alpha.sum(dim=0)
                            _diag_alpha_sq_sum += (alpha ** 2).sum(dim=0)
                            _diag_alpha_entropy_sum += float(-(alpha * torch.log(alpha + 1e-8)).sum().item())
                            _diag_alpha_count += B

                        sigma_u = coh_scale()
                        u_i = compute_shared_driver(alpha, Z_k)
                        u_i_scaled = sigma_u * u_i
                        h_scale = torch.sqrt(torch.arange(1, H + 1, device=device, dtype=torch.float32)).unsqueeze(0)
                        u_i_scaled = u_i_scaled * h_scale

                        # ── AB1: compute mu_hat and residual ──
                        if _ab1_active:
                            with torch.no_grad() if _freeze_ab1 else contextlib.nullcontext():
                                mu_hat, ctx_emb = _chunked_mu_forward(
                                    mu_backbone, hy.float(), xn.float(), xc, rid, origin_year=oy)
                            x_1 = x_target - mu_hat   # residual target
                        else:
                            x_1 = x_target
                            ctx_emb = None

                        # ── SF2M: Source sampling x_0 ~ N(0, I_H) ──
                        x_0 = torch.randn_like(x_1)

                        # ── SF2M: Condition-aware OT coupling ──
                        if _ot_active and ot_coupler is not None:
                            # Rich conditioning: history + region embedding + AB1 context
                            with torch.no_grad():
                                r_emb = sf2m_net.region_emb(rid.clamp(0, GEO_BUCKETS-1).long())
                                cond_parts = [hy.float(), r_emb]
                                if ctx_emb is not None:
                                    cond_parts.append(ctx_emb.detach())
                                cond_feats = torch.cat(cond_parts, dim=1)
                            x_0, x_1 = ot_coupler.couple(x_0, x_1, cond_feats, cond_feats)

                        # ── SF2M: Sample bridge time t ~ U(eps, 1-eps) ──
                        t = torch.rand(B, device=device)
                        t = t * (1.0 - 2e-4) + 1e-4

                        # ── SF2M: Interpolant x_t ──
                        eps = torch.randn_like(x_1)
                        x_t = bridge_sched.interpolate(x_0, x_1, t, eps)

                        # ── SF2M: Closed-form targets ──
                        u_target = bridge_sched.compute_conditional_flow(x_0, x_1, t, x_t)
                        scaled_s_target = bridge_sched.compute_scaled_score_target(x_0, x_1, t, x_t)

                        # ── SF2M: Single forward pass for both outputs ──
                        v_hat, scaled_s_hat = sf2m_net(
                            x_t, t, hy.float(), xn.float(), xc, rid,
                            u_i_scaled, ctx_emb=ctx_emb)

                        # ── SF2M: Flow matching loss ──
                        _flow_sq = (v_hat - u_target) ** 2 * m
                        _per_h_count = m.sum(dim=0).clamp(min=1.0)
                        _per_h_flow = _flow_sq.sum(dim=0) / _per_h_count
                        _n_active_h = (m.sum(dim=0) > 0).sum().clamp(min=1)
                        loss_flow = _per_h_flow.sum() / _n_active_h

                        # ── SF2M: Scaled score matching loss ──
                        # Target: -(x_t - mu_t). No lambda weighting needed for
                        # scaled parameterization (stability is built into the target).
                        _score_sq = (scaled_s_hat - scaled_s_target) ** 2 * m
                        _per_h_score = _score_sq.sum(dim=0) / _per_h_count
                        loss_score = _per_h_score.sum() / _n_active_h

                        # Combined loss
                        loss_total = loss_flow + float(SCORE_LOSS_WEIGHT) * loss_score

                        # AB1 mu-backbone loss (only if not frozen)
                        if _ab1_active and not _freeze_ab1:
                            _mu_sub = min(2048, B)
                            mu_hat_grad, _ = mu_backbone(
                                hy[:_mu_sub].float(), xn[:_mu_sub].float(),
                                xc[:_mu_sub], rid[:_mu_sub], origin_year=oy[:_mu_sub])
                            _mu_err = torch.nn.functional.huber_loss(
                                mu_hat_grad, x_target[:_mu_sub], reduction='none') * m[:_mu_sub]
                            _mu_count = m[:_mu_sub].sum(dim=0).clamp(min=1.0)
                            loss_mu = (_mu_err.sum(dim=0) / _mu_count).sum() / _n_active_h
                            loss_total = float(LAMBDA_MU) * loss_mu + float(LAMBDA_DRIFT) * loss_total

                    opt.zero_grad(set_to_none=True)
                    loss_total.backward()
                    all_params = [p for pg in param_groups for p in pg["params"]]
                    torch.nn.utils.clip_grad_norm_(all_params, 1.0)
                    opt.step()

                    ep_losses.append(float(loss_total.item()))
                    ep_losses_flow.append(float(loss_flow.item()))
                    ep_losses_score.append(float(loss_score.item()))
                    _total_batches += 1

                    if _total_batches <= 3 or _total_batches % 10 == 0:
                        print(f"  batch {_total_batches} ep={ep} L={float(loss_total.item()):.5f} "
                              f"Lf={float(loss_flow.item()):.5f} Ls={float(loss_score.item()):.5f} B={B}", flush=True)

                except Exception as _batch_err:
                    print(f"\n❌ TRAINING ERROR ep={ep} batch={_total_batches} B={B}: "
                          f"{type(_batch_err).__name__}: {_batch_err}", flush=True)
                    import traceback; traceback.print_exc(); sys.stdout.flush()
                    raise

        lr_sched.step()
        mean_loss = float(np.mean(ep_losses)) if ep_losses else float("nan")
        mean_flow = float(np.mean(ep_losses_flow)) if ep_losses_flow else float("nan")
        mean_score = float(np.mean(ep_losses_score)) if ep_losses_score else float("nan")
        losses.append(mean_loss)

        # Early stopping
        EARLY_STOP_PATIENCE = 5
        if len(losses) > EARLY_STOP_PATIENCE:
            recent_best = min(losses[-EARLY_STOP_PATIENCE:])
            overall_best = min(losses[:-EARLY_STOP_PATIENCE])
            if recent_best > overall_best * 1.001:
                print(f"[{ts()}] Early stop at ep {ep+1}: {mean_loss:.6f} (best={min(losses):.6f})")
                break

        dt_ep = time.time() - t_ep0
        phi_vals = token_persistence.get_phi_list()
        sigma_u_val = coh_scale.get_sigma()
        alpha_mean = (_diag_alpha_sum / max(1, _diag_alpha_count)).cpu().tolist() if _diag_alpha_count > 0 else [0.0] * K_TOKENS
        alpha_sq_mean = (_diag_alpha_sq_sum / max(1, _diag_alpha_count)).cpu().tolist() if _diag_alpha_count > 0 else [1.0] * K_TOKENS
        eff_k = 1.0 / max(sum(alpha_sq_mean), 1e-8)
        mean_entropy = _diag_alpha_entropy_sum / max(1, _diag_alpha_count)

        # OT transport diagnostic
        ot_diag = OTCoupler.get_transport_diagnostic()
        OTCoupler.reset_diagnostic()
        ot_off_diag = ot_diag['off_diagonal_frac']

        if (ep == 0) or ((ep + 1) % max(1, int(epochs) // 5) == 0):
            print(f"[{ts()}] o={origin} ep={ep+1}/{epochs} L={mean_loss:.6f} Lf={mean_flow:.6f} Ls={mean_score:.6f} {dt_ep:.1f}s")
            print(f"[{ts()}]   phi={[f'{p:.3f}' for p in phi_vals]}  sigma_u={sigma_u_val:.3f}  eff_k={eff_k:.2f}  OT_offdiag={ot_off_diag:.3f}")
            sys.stdout.flush()
        else:
            eta = (int(epochs) - (ep + 1)) * dt_ep / 60.0
            print(f"  ep {ep+1}/{epochs} L={mean_loss:.6f} Lf={mean_flow:.6f} Ls={mean_score:.6f} {dt_ep:.0f}s ETA={eta:.1f}m", flush=True)

        # W&B logging
        log_data = {
            f"train/loss_origin_{origin}": mean_loss,
            "train/loss": mean_loss,
            "train/loss_flow": mean_flow,
            "train/loss_score": mean_score,
            "train/epoch": ep + 1,
            "train/origin": origin,
            "train/lr": float(lr_sched.get_last_lr()[0]),
            "tokens/sigma_u": sigma_u_val,
            "tokens/effective_k": eff_k,
            "tokens/alpha_entropy": mean_entropy,
            "ot/off_diagonal_frac": ot_off_diag,
            "ot/total_pairs": ot_diag['total_pairs'],
        }
        for k_idx, phi_k in enumerate(phi_vals):
            log_data[f"tokens/phi_{k_idx}"] = phi_k
        for k_idx, a_k in enumerate(alpha_mean):
            log_data[f"tokens/alpha_mean_{k_idx}"] = a_k
        wb_log(log_data)

        # ── Periodic checkpoint for resume ──
        if checkpoint_callback is not None and ((ep + 1) % checkpoint_every == 0 or ep + 1 == int(epochs)):
            _resume_state = {
                "epoch": ep + 1,
                "total_batches": _total_batches,
                "losses": list(losses),
                "sf2m_net_state_dict": sf2m_net.state_dict(),
                "gating_net_state_dict": gating_net.state_dict(),
                "token_persistence_state_dict": token_persistence.state_dict(),
                "coh_scale_state_dict": coh_scale.state_dict(),
                "mu_backbone_state_dict": mu_backbone.state_dict() if _ab1_active else None,
                "optimizer_state_dict": opt.state_dict(),
                "lr_scheduler_state_dict": lr_sched.state_dict(),
            }
            try:
                checkpoint_callback(ep + 1, _resume_state)
                print(f"[{ts()}] 💾 Resume checkpoint saved at epoch {ep + 1}")
            except Exception as _ckpt_err:
                print(f"[{ts()}] ⚠️ Checkpoint save failed (non-fatal): {_ckpt_err}")
            sys.stdout.flush()

    print(f"[{ts()}] FINAL phi_k = {[f'{p:.4f}' for p in token_persistence.get_phi_list()]}")
    print(f"[{ts()}] FINAL sigma_u = {coh_scale.get_sigma():.4f}")

    return y_scaler, n_scaler, t_scaler, losses, scaled_paths

# Backward compat alias
train_bridge_v12 = train_sf2m_v12
