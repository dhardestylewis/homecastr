import modal
import os
import pickle
import re

app = modal.App("consolidate-calibrators-merged")
eval_volume = modal.Volume.from_name("properlytic-checkpoints")

# Need scikit-learn to load/save the IsotonicRegression objects properly
image = modal.Image.debian_slim(python_version="3.11").pip_install("scikit-learn")

@app.function(image=image, volumes={"/output": eval_volume})
def consolidate_and_merge():
    # Refresh volume
    eval_volume.reload()
    
    print("Consolidating AND merging calibrators in /output...")
    # Find all per-origin calibrators
    # Group by jurisdiction
    jurisdictions = {}
    
    for root, dirs, files in os.walk("/output"):
        for file in files:
            if file.startswith("calibrators_") and file.endswith(".pkl") and "_o" in file:
                # E.g. calibrators_v11_hcad_houston_o2023.pkl
                m = re.match(r"calibrators_(v\d+)_(.+)_o(\d+)\.pkl", file)
                if m:
                    version = m.group(1)
                    jur = m.group(2)
                    origin = int(m.group(3))
                    
                    key = (version, jur)
                    full_path = os.path.join(root, file)
                    if key not in jurisdictions:
                        jurisdictions[key] = []
                    jurisdictions[key].append((origin, full_path, root))

    # For each jurisdiction, load origins in chronological order and merge
    for (version, jur), org_list in jurisdictions.items():
        org_list.sort(key=lambda x: x[0])  # Sort by origin ascending
        
        merged_models = {}
        target_dir = org_list[0][2] # Take root dir of the first one
        
        for origin, src_path, _ in org_list:
            print(f"[{jur} {version}] Loading origin {origin} from {os.path.basename(src_path)}...")
            try:
                with open(src_path, "rb") as f:
                    data = pickle.load(f)
                    # Update master dict with newer models (overwrites older ones)
                    # This preserves long-horizon models from older origins while putting
                    # the newest information in for short-horizon models.
                    merged_models.update(data)
            except Exception as e:
                print(f"  Error loading {src_path}: {e}")
        
        if merged_models:
            # Report what horizons are in the final merge
            horizons = sorted(list(set(k[0] for k in merged_models.keys())))
            print(f"[{jur} {version}] Final merged calibrator has {len(merged_models)} models for horizons: {horizons}")
            
            # Save the consolidated merge
            dst_name = f"calibrators_{version}_{jur}.pkl"
            dst_path = os.path.join(target_dir, dst_name)
            print(f"[{jur} {version}] Saving merged calibrator to {dst_name}")
            try:
                with open(dst_path, "wb") as f:
                    pickle.dump(merged_models, f)
            except Exception as e:
                print(f"  Error saving {dst_path}: {e}")
            
            # Update the latest origin file too, so future sweeps that load it
            # get the full historical context
            latest_origin = org_list[-1][0]
            latest_path = org_list[-1][1]
            try:
                with open(latest_path, "wb") as f:
                    pickle.dump(merged_models, f)
                print(f"[{jur} {version}] Also updated latest origin {os.path.basename(latest_path)} with merged data")
            except Exception as e:
                print(f"  Error updating {latest_path}: {e}")
                
    eval_volume.commit()
    print("Done! Volume committed.")
    
@app.local_entrypoint()
def main():
    consolidate_and_merge.remote()
