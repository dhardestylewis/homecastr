"""Quick test: list TxGIO S3 FGDB files for 2024."""
import urllib.request
import xml.etree.ElementTree as ET

bucket = "tnris-data-warehouse"
prefix = "LCD/collection/stratmap-2024-land-parcels/fgdb/"
url = f"https://{bucket}.s3.us-east-1.amazonaws.com/?list-type=2&prefix={prefix}&max-keys=20"

print(f"Fetching: {url}")
with urllib.request.urlopen(urllib.request.Request(url), timeout=30) as resp:
    data = resp.read()

root = ET.fromstring(data)
ns = "http://s3.amazonaws.com/doc/2006-03-01/"
contents = root.findall(f".//{{{ns}}}Contents")
print(f"Found {len(contents)} items in FGDB folder")

total = 0
for c in contents:
    k = c.find(f"{{{ns}}}Key").text
    s = int(c.find(f"{{{ns}}}Size").text)
    total += s
    fname = k.replace(prefix, "")
    print(f"  {fname} ({s/1024/1024:.1f} MB)")

print(f"\nTotal: {total/1024/1024:.0f} MB ({total/1024/1024/1024:.1f} GB)")

# Check if truncated
trunc = root.find(f"{{{ns}}}IsTruncated")
if trunc is not None and trunc.text == "true":
    print("  (TRUNCATED - more files available)")

# Also count total objects
kc = root.find(f"{{{ns}}}KeyCount")
if kc is not None:
    print(f"  KeyCount in this page: {kc.text}")
