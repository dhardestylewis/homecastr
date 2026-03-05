"""Download HCAD codebook and inspect acct format."""
import urllib.request, zipfile, io

# Download codebook
print("=== Downloading Code_description_real.zip ===")
data = urllib.request.urlopen("https://download.hcad.org/data/CAMA/2025/Code_description_real.zip").read()
print(f"  Downloaded {len(data)} bytes")

zf = zipfile.ZipFile(io.BytesIO(data))
print(f"  Files: {zf.namelist()}")

for name in zf.namelist():
    content = zf.read(name).decode("latin-1")
    print(f"\n{'='*60}")
    print(f"--- {name} ({len(content)} chars) ---")
    print(content[:5000])

# Also check PP_files
print(f"\n\n{'='*60}")
print("=== Downloading PP_files.zip ===")
data2 = urllib.request.urlopen("https://download.hcad.org/data/CAMA/2025/PP_files.zip").read()
print(f"  Downloaded {len(data2)} bytes")
zf2 = zipfile.ZipFile(io.BytesIO(data2))
print(f"  Files: {zf2.namelist()}")

for name in zf2.namelist():
    content = zf2.read(name).decode("latin-1")
    print(f"\n--- {name} (first 2000 chars) ---")
    print(content[:2000])
