import modal

app = modal.App("test-image-env")

image = (
    modal.Image.from_registry(
        "pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime",
        add_python="3.11",
    )
    .apt_install("libgdal-dev", "libspatialindex-dev")
    .pip_install(
        "google-cloud-storage>=2.10",
        "pandas>=2.0",
        "geopandas>=0.14",
        "numpy>=1.24",
        "pyarrow>=12.0",
        "requests>=2.28",
        "shapely>=2.0",
        "h3>=3.7",
        "fastapi[standard]",
        "wandb",
        "polars",
        "torch",
    )
)

@app.function(image=image)
def check_env():
    import sys
    import subprocess
    print("Python execution:", sys.executable)
    print("Python version:", sys.version)
    pip_freeze = subprocess.check_output([sys.executable, "-m", "pip", "freeze"]).decode("utf-8")
    print("PIP FREEZE:", pip_freeze)
    try:
        import polars
        print("Polars found!", polars.__version__)
    except ImportError as e:
        print("Polars error:", e)
    return sys.executable, sys.version, pip_freeze

@app.local_entrypoint()
def main():
    exec_path, version, freeze = check_env.remote()
    print("FINISHED")
    print(exec_path, version, freeze[:500])
