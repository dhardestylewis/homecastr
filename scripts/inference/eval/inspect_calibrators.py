import modal
import os
import pickle

app = modal.App("inspect-calibrators")
eval_volume = modal.Volume.from_name("properlytic-checkpoints")

# Need scikit-learn because the pickle file contains IsotonicRegression instances
image = modal.Image.debian_slim(python_version="3.11").pip_install("scikit-learn")

@app.function(image=image, volumes={"/output": eval_volume})
def inspect():
    # Refresh volume
    eval_volume.reload()
    
    files = []
    for root, _, fs in os.walk("/output"):
        for f in fs:
            if "calibrators_v11" in f and f.endswith(".pkl"):
                files.append(os.path.join(root, f))
    
    files.sort()
    for fp in files:
        try:
            with open(fp, "rb") as f:
                data = pickle.load(f)
                keys = list(data.keys())
                horizons = sorted(list(set(k[0] for k in keys)))
                print(f"{os.path.basename(fp)}: {len(keys)} models, Horizons: {horizons}")
        except Exception as e:
            print(f"{os.path.basename(fp)}: Error reading - {e}")

@app.local_entrypoint()
def main():
    inspect.remote()
