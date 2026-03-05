"""
Quick Modal script to list contents of the inference-outputs volume.
Usage: python -m modal run scripts/inference/upload/list_vol_contents.py
"""
import modal
import os

app = modal.App("list-vol-contents")
image = modal.Image.debian_slim(python_version="3.11")
output_vol = modal.Volume.from_name("inference-outputs", create_if_missing=False)

@app.function(image=image, volumes={"/output": output_vol}, timeout=120, memory=512)
def list_vol() -> list:
    """List all files and directories at the top level of /output/."""
    results = []
    for entry in sorted(os.listdir("/output")):
        full = os.path.join("/output", entry)
        if os.path.isdir(full):
            # Count files recursively
            count = sum(len(files) for _, _, files in os.walk(full))
            results.append(f"DIR  {entry}/ ({count} files)")
        else:
            sz = os.path.getsize(full) / 1e6
            results.append(f"FILE {entry} ({sz:.1f} MB)")
    return results

@app.local_entrypoint()
def main():
    items = list_vol.remote()
    print(f"\n{'='*60}", flush=True)
    print(f"Contents of Modal volume 'inference-outputs' (/output/):", flush=True)
    print(f"{'='*60}", flush=True)
    for item in items:
        print(f"  {item}", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"Total entries: {len(items)}", flush=True)
