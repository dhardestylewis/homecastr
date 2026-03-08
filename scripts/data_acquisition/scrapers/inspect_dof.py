"""Read DSF record layout and inspect a sample DOF assessment file."""
import pandas as pd
import zipfile, io, requests, json

# 1. Read the record layout XLSX
print("=== DOF Record Layout ===")
df = pd.read_excel('scripts/data_acquisition/download/layout-pts-property-master.xlsx')
print(f"Shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print()
# Print first 80 rows
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 40)
print(df.head(80).to_string())
print()
print("...")
print()
# Print last 20 rows
print(df.tail(20).to_string())

# 2. Download and inspect a sample DOF file
print("\n\n=== Sample DOF File (FY26 TC1) ===")
url = "https://www.nyc.gov/assets/finance/downloads/tar/fy26_tc1.zip"
r = requests.get(url, timeout=60)
z = zipfile.ZipFile(io.BytesIO(r.content))
print(f"ZIP contents: {z.namelist()}")

for name in z.namelist():
    with z.open(name) as f:
        raw = f.read(10000)
        text = raw.decode('utf-8', errors='replace')
        lines = text.split('\n')
        print(f"\nFile: {name}")
        print(f"Line count in sample: {len(lines)}")
        print(f"Line 0 length: {len(lines[0])}")
        print(f"Line 1 length: {len(lines[1])}" if len(lines) > 1 else "")
        print(f"\nFirst 3 lines (first 200 chars each):")
        for i, line in enumerate(lines[:3]):
            print(f"  [{i}] {line[:200]}")
        
        # Count total lines in full file
        f.seek(0)
        total = sum(1 for _ in f)
        print(f"\nTotal lines in file: {total}")
    break  # Just first file
