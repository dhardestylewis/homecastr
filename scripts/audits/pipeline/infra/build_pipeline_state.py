"""
Master Pipeline State Builder
Queries GCS, Supabase, W&B, and local logs to build a single verified YAML.
"""
import os
import json, yaml, os, re, psycopg2
from datetime import datetime
from google.cloud import storage
import wandb

DB_URL = os.environ["SUPABASE_DB_URL"]
SCHEMA = "forecast_20260220_7f31c6e4"
GCS_BUCKET = "properlytic-raw-data"
SCRIPTS = os.path.dirname(os.path.abspath(__file__))  # scripts/audits/
ROOT = os.path.dirname(os.path.dirname(SCRIPTS))       # project root
LOG_DIR = os.path.join(os.path.dirname(SCRIPTS), "logs")
TRAIN_DIR = os.path.join(LOG_DIR, "train")

# Canonical jurisdiction IDs (as used across systems)
JURISDICTIONS = [
    "hcad_houston", "sf_ca", "nyc", "philly",
    "france_dvf", "seattle_wa", "uk_ppd",
    "cook_county_il", "denver_co", "maricopa_az",
    "vancouver_bc", "massgis",
]

# GCS prefix → jurisdiction mapping
GCS_PREFIX_MAP = {
    "hcad": "hcad_houston",
    "sf": "sf_ca",
    "sf_sales": "sf_ca",
    "nyc": "nyc",
    "nyc_pluto": "nyc",
    "ny_state": "nyc",
    "philly": "philly",
    "france_dvf": "france_dvf",
    "seattle_wa": "seattle_wa",
    "uk_ppd": "uk_ppd",
    "cook_county_il": "cook_county_il",
    "denver_co": "denver_co",
    "maricopa_az": "maricopa_az",
    "vancouver_bc": "vancouver_bc",
    "massgis": "massgis",
    "txgio": "txgio",
}

# W&B run name → jurisdiction mapping
def parse_wandb_name(name):
    """Extract jurisdiction and origin from W&B run name."""
    # Training: v11-{jur}-SF500K-o{year}
    m = re.match(r"v11-(.+?)-SF500K-o(\d+)", name)
    if m:
        return m.group(1), int(m.group(2)), "train"
    # Eval: eval_v11_{jur}_{year}
    m = re.match(r"eval_v11_(.+?)_(\d{4})$", name)
    if m:
        return m.group(1), int(m.group(2)), "eval"
    # Eval without year: eval_v11_{jur}
    m = re.match(r"eval_v11_(.+?)$", name)
    if m:
        return m.group(1), None, "eval"
    return None, None, None

# Supabase jurisdiction name mapping
SUPABASE_JUR_MAP = {
    "hcad": "hcad_houston",
    "france_dvf": "france_dvf",
    "seattle_wa": "seattle_wa",
}


def build_state():
    state = {
        "generated_at": datetime.now().isoformat(),
        "schema_version": "1.0",
        "pipeline_stages": [
            "raw_data",       # Raw source data in GCS
            "panel",          # Built master panel in GCS
            "training",       # Model training on Modal
            "checkpoint",     # Trained model checkpoints in GCS
            "eval",           # Evaluation runs via W&B
            "inference",      # Forecasts generated
            "supabase",       # Data loaded to Supabase
            "map_visible",    # Visible on the web map
        ],
        "shared_resources": {},
        "jurisdictions": {},
    }

    # Initialize jurisdictions
    for jur in JURISDICTIONS:
        state["jurisdictions"][jur] = {
            "raw_data": {"gcs_files": 0, "gcs_mb": 0, "prefixes": [], "detail": []},
            "panel": {"exists": False, "file": None, "size_mb": 0},
            "training": {"runs": [], "total": 0, "ok": 0, "failed": 0},
            "checkpoint": {"origins": [], "total": 0, "files": []},
            "eval": {"runs": [], "total": 0, "with_metrics": 0, "crashed": 0},
            "inference": {"status": "unknown"},
            "supabase": {"parcel_forecast": 0, "tract_forecast": 0, "zcta_forecast": 0, "ladder": 0},
            "map_visible": False,
        }

    # ══════════════════════════════════════════════
    # 1. GCS INVENTORY
    # ══════════════════════════════════════════════
    print("1/5 Scanning GCS...")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    shared_prefixes = {"census", "climate", "code", "diagnostics", "epa", "fema",
                       "fred", "geo", "lehd", "macro", "boe", "panel", "checkpoints"}

    for blob in bucket.list_blobs():
        prefix = blob.name.split("/")[0]

        # Shared resources
        if prefix in shared_prefixes:
            if prefix not in state["shared_resources"]:
                state["shared_resources"][prefix] = {"files": 0, "total_mb": 0}
            state["shared_resources"][prefix]["files"] += 1
            state["shared_resources"][prefix]["total_mb"] = round(
                state["shared_resources"][prefix]["total_mb"] + blob.size / 1e6, 1
            )
            # Checkpoints go to jurisdictions too
            if prefix == "checkpoints" and blob.name.endswith(".pt"):
                parts = blob.name.split("/")
                if len(parts) >= 3:
                    ckpt_jur = parts[1]
                    if ckpt_jur in state["jurisdictions"]:
                        # Extract origin year
                        m = re.search(r"origin_(\d+)", blob.name)
                        origin = int(m.group(1)) if m else None
                        state["jurisdictions"][ckpt_jur]["checkpoint"]["files"].append(
                            {"file": blob.name, "size_mb": round(blob.size / 1e6, 1), "origin": origin}
                        )
                        if origin and origin not in state["jurisdictions"][ckpt_jur]["checkpoint"]["origins"]:
                            state["jurisdictions"][ckpt_jur]["checkpoint"]["origins"].append(origin)
                            state["jurisdictions"][ckpt_jur]["checkpoint"]["total"] += 1
            # Panels
            if prefix == "panel" and blob.name.endswith(".parquet"):
                # Try to map to jurisdiction from filename
                for jur in JURISDICTIONS:
                    if jur.split("_")[0] in blob.name.lower():
                        state["jurisdictions"][jur]["panel"]["exists"] = True
                        state["jurisdictions"][jur]["panel"]["file"] = blob.name
                        state["jurisdictions"][jur]["panel"]["size_mb"] = round(blob.size / 1e6, 1)
            continue

        # Jurisdiction data
        jur = GCS_PREFIX_MAP.get(prefix)
        if jur and jur in state["jurisdictions"]:
            rd = state["jurisdictions"][jur]["raw_data"]
            rd["gcs_files"] += 1
            rd["gcs_mb"] = round(rd["gcs_mb"] + blob.size / 1e6, 1)
            if prefix not in rd["prefixes"]:
                rd["prefixes"].append(prefix)
            if rd["gcs_files"] <= 5:  # only store first 5 as detail
                rd["detail"].append(f"{blob.name} ({blob.size/1e6:.1f}MB)")

    # Sort checkpoint origins
    for jur in state["jurisdictions"].values():
        jur["checkpoint"]["origins"] = sorted(jur["checkpoint"]["origins"])

    # ══════════════════════════════════════════════
    # 2. SUPABASE
    # ══════════════════════════════════════════════
    print("2/5 Querying Supabase...")
    try:
        conn = psycopg2.connect(DB_URL)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"SET search_path TO {SCHEMA}, public")

        for table in ["metrics_parcel_forecast", "metrics_tract_forecast", "metrics_zcta_forecast"]:
            try:
                cur.execute(f"SELECT jurisdiction, count(*) FROM {table} GROUP BY jurisdiction")
                for jur_name, cnt in cur.fetchall():
                    mapped = SUPABASE_JUR_MAP.get(jur_name, jur_name)
                    if mapped in state["jurisdictions"]:
                        key = table.replace("metrics_", "").replace("_forecast", "_forecast")
                        state["jurisdictions"][mapped]["supabase"][key] = cnt
            except Exception as e:
                conn.rollback()

        try:
            cur.execute("SELECT jurisdiction, count(*) FROM parcel_ladder_v1 GROUP BY jurisdiction")
            for jur_name, cnt in cur.fetchall():
                mapped = SUPABASE_JUR_MAP.get(jur_name, jur_name)
                if mapped in state["jurisdictions"]:
                    state["jurisdictions"][mapped]["supabase"]["ladder"] = cnt
        except:
            conn.rollback()

        conn.close()
    except Exception as e:
        state["supabase_error"] = str(e)

    # ══════════════════════════════════════════════
    # 3. W&B RUNS
    # ══════════════════════════════════════════════
    print("3/5 Pulling W&B...")
    try:
        api = wandb.Api()
        runs = list(api.runs("dhardestylewis-columbia-university/homecastr", per_page=200))

        for r in runs:
            jur, origin, run_type = parse_wandb_name(r.name)
            if not jur or jur not in state["jurisdictions"]:
                continue

            cfg = dict(r.config) if r.config else {}
            summ = {}
            if r.summary:
                for k, v in dict(r.summary).items():
                    if not k.startswith("_") and not isinstance(v, (dict, list)):
                        try:
                            json.dumps(v)
                            summ[k] = v
                        except:
                            pass

            # Round floats to keep file manageable
            for k in list(summ.keys()):
                if isinstance(summ[k], float):
                    summ[k] = round(summ[k], 6)

            run_info = {
                "name": r.name,
                "id": r.id,
                "state": r.state,
                "origin": origin or cfg.get("origin"),
                "n_metrics": len(summ),
                "created": str(r.created_at),
                "config": cfg,
                "metrics": summ,
            }

            if run_type == "train":
                state["jurisdictions"][jur]["training"]["runs"].append(run_info)
                state["jurisdictions"][jur]["training"]["total"] += 1
                if r.state == "finished":
                    state["jurisdictions"][jur]["training"]["ok"] += 1
                else:
                    state["jurisdictions"][jur]["training"]["failed"] += 1
            elif run_type == "eval":
                state["jurisdictions"][jur]["eval"]["runs"].append(run_info)
                state["jurisdictions"][jur]["eval"]["total"] += 1
                if r.state == "crashed":
                    state["jurisdictions"][jur]["eval"]["crashed"] += 1
                elif len(summ) > 0:
                    state["jurisdictions"][jur]["eval"]["with_metrics"] += 1

    except Exception as e:
        state["wandb_error"] = str(e)

    # ══════════════════════════════════════════════
    # 4. LOCAL TRAINING LOGS
    # ══════════════════════════════════════════════
    print("4/5 Scanning local logs...")
    if os.path.exists(TRAIN_DIR):
        for f in sorted(os.listdir(TRAIN_DIR)):
            if f.endswith("_err.log"):
                continue
            path = os.path.join(TRAIN_DIR, f)
            size = os.path.getsize(path)
            with open(path, "r", errors="replace") as fh:
                content = fh.read()
            has_err = "Error" in content or "Traceback" in content
            last_line = content.strip().split("\n")[-1][:120] if content.strip() else "(empty)"

            # Map log filename to jurisdiction
            for jur in JURISDICTIONS:
                jur_short = jur.replace("_houston", "").replace("_ca", "")
                if f.startswith(jur_short) or f.startswith(jur):
                    if "local_logs" not in state["jurisdictions"][jur]:
                        state["jurisdictions"][jur]["local_logs"] = []
                    state["jurisdictions"][jur]["local_logs"].append({
                        "file": f,
                        "size": size,
                        "status": "ERROR" if has_err else "OK",
                        "last_line": last_line,
                    })
                    break

    # ══════════════════════════════════════════════
    # 5. DERIVE PIPELINE STAGE
    # ══════════════════════════════════════════════
    print("5/5 Computing pipeline stages...")
    for jur_name, jur in state["jurisdictions"].items():
        stages_complete = []
        if jur["raw_data"]["gcs_files"] > 0:
            stages_complete.append("raw_data")
        if jur["panel"]["exists"]:
            stages_complete.append("panel")
        if jur["training"]["ok"] > 0:
            stages_complete.append("training")
        if jur["checkpoint"]["total"] > 0:
            stages_complete.append("checkpoint")
        if jur["eval"]["with_metrics"] > 0:
            stages_complete.append("eval")
        sb = jur["supabase"]
        if sb.get("parcel_forecast", 0) > 0 or sb.get("ladder", 0) > 0:
            stages_complete.append("supabase")
            jur["inference"]["status"] = "complete"
        # map_visible requires manual check — set hcad only
        if jur_name == "hcad_houston":
            jur["map_visible"] = True

        jur["pipeline_progress"] = stages_complete
        jur["furthest_stage"] = stages_complete[-1] if stages_complete else "none"
        total_stages = len(state["pipeline_stages"])
        jur["completion_pct"] = round(len(stages_complete) / total_stages * 100)

    # ══════════════════════════════════════════════
    # WRITE
    # ══════════════════════════════════════════════
    out_yaml = os.path.join(LOG_DIR, "pipeline_state.yaml")
    out_json = os.path.join(LOG_DIR, "pipeline_state.json")

    with open(out_yaml, "w") as f:
        yaml.dump(state, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    with open(out_json, "w") as f:
        json.dump(state, f, indent=2, default=str)

    print(f"\n✅ Written to {out_yaml}")
    print(f"✅ Written to {out_json}")

    # Print summary
    print("\n=== PIPELINE SUMMARY ===")
    for jur_name, jur in state["jurisdictions"].items():
        stages = jur.get("pipeline_progress", [])
        pct = jur.get("completion_pct", 0)
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        ckpts = jur["checkpoint"]["total"]
        sb_total = sum(v for k, v in jur["supabase"].items() if isinstance(v, int))
        print(f"  {jur_name:20s} {bar} {pct:>3}%  ckpts={ckpts}  supabase={sb_total:>10,}  stage={jur['furthest_stage']}")


if __name__ == "__main__":
    build_state()
