# World Model v12_sb — Conditional Residual Path-Space Schrödinger Bridge

## Overview

v12_sb replaces the DDPM/DDIM diffusion core from v11 with a **conditional residual
path-space Schrödinger Bridge** around the AB1 (FT-Transformer) mean path.

### What changed from v11

| Component | v11 | v12_sb |
|---|---|---|
| Generative core | DDPM epsilon-prediction + DDIM reverse | Path-space SB with Euler-Maruyama forward |
| Training loss | ε-MSE with min-SNR weighting | Conditional bridge matching (flow matching) |
| Scheduler | β-schedule noise ladder | Bridge-time s ∈ [0,1] discretization |
| Network | Conv1dDenoiserV11 (noise predictor) | PathDriftNetwork (drift/velocity predictor) |
| Target object | Future delta vector [B, H] | Cumulative residual path R_path [B, H] |
| Sampler | DDIM reverse denoising loop | Euler-Maruyama forward simulation |

### What is kept from v11

- All data plumbing (`build_master_training_shards_v102_local`, etc.)
- AB1 `FTTransformerBackbone` deterministic mean predictor
- `GatingNetwork`, `TokenPersistence`, `CoherenceScale` (cross-parcel coherence)
- Scalers (`SimpleScaler`, `RunningMeanVar`)
- W&B instrumentation, checkpointing
- `build_inference_context_chunked_v102`

## Architecture

```
c_i  ──→  AB1 FTTransformer  ──→  (μ_i, h_c_i)
                                        │
                                        ▼
target deltas  ──→  cumsum  ──→  Y_path  ──→  R_path = Y_path − μ_i
                                                         │
                                                         ▼
                                          PathDriftNetwork learns:
                                          u_θ(s, X_s, h_c) for s ∈ [0,1]
                                                         │
                              Inference:                 │
                              Euler-Maruyama from X_0=0  ──→  X_1 = R̂_path
                                                                    │
                              Reconstruction:  Ŷ_path = μ_i + R̂_path
                              Convert back:    deltas = diff(Ŷ_path)
```

## Files

- `worldmodel_sb.py` — Full training code (replaces `worldmodel.py`)
- `worldmodel_inference_sb.py` — Inference-only subset (replaces `worldmodel_inference.py`)

## Training

```bash
modal run scripts/pipeline/training/train_modal_sb.py --jurisdiction hcad_houston --origin 2019
```

## Checkpoint naming

Checkpoints use suffix `_sb`: `ckpt_origin_2019_SF500K_sb.pt`

## Version conventions

Future model architectures should follow this versioned directory pattern:
```
scripts/inference/v12_sb/    ← this version
scripts/inference/v13_xxx/   ← next version
scripts/inference/v14_yyy/   ← etc.
```
