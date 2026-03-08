---
description: How to train the world model for a jurisdiction
---

# Training a Jurisdiction

## Prerequisites
- Modal CLI authenticated to the **homecastr** workspace: `modal profile activate homecastr`
- GCS credentials secret: `gcs-creds` (contains `GOOGLE_APPLICATION_CREDENTIALS_JSON`)
- WandB secret: `wandb-creds` (contains `WANDB_API_KEY`)
- Fixed code uploaded to GCS: `gs://properlytic-raw-data/code/worldmodel.py`

## Single jurisdiction + origin
// turbo
```bash
modal run scripts/pipeline/train_modal.py --jurisdiction <name> --origin <year>
```

Available jurisdictions (must match `panel/jurisdiction=<name>/part.parquet` on GCS, or have `PANEL_GCS_OVERRIDES` entry):
- `hcad_houston`, `acs_nationwide`, `sf_ca`, `cook_county_il`, `nyc`, `philly`, `seattle_wa`, `france_dvf`, `txgio_texas`, `uk_ppd`, `vancouver_bc`, `florida_dor`

## All jurisdictions (parallel)
// turbo
```bash
python scripts/pipeline/launch/launch_all_training.py
```

This launches every (jurisdiction × origin) as a separate Modal A100 container in parallel.

## Key notes
- **Cost**: ~$3-6 per jurisdiction-origin pair (~1-2 hrs on A100-40GB)
- **Checkpoints** are saved to both GCS (`checkpoints/<jurisdiction>/`) and Modal volume
- **Code path**: Always use `scripts/pipeline/train_modal.py` (NOT `scripts/train_modal.py` which was a stale copy and has been deleted)
- **ACS**: Uses `median_home_value` as target; Census suppression values are auto-cleaned
- **Upload code first**: Any local fixes to `worldmodel.py` must be uploaded to GCS before training picks them up
