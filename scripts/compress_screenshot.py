import os
from PIL import Image

def find_file():
    target = "Screenshot 2026-03-12 155728.png"
    search_dirs = [
        r"C:\Users\dhl\Downloads",
        r"C:\Users\dhl\Documents",
        r"C:\Users\dhl\Desktop",
        r"C:\Users\dhl\OneDrive\Pictures\Screenshots",
        r"C:\Users\dhl\OneDrive\Desktop",
        r"C:\Users\dhl\Pictures\Screenshots",
        r"C:\Users\dhl\data\Projects",
        r"C:\Users\dhl\data"
    ]
    for d in search_dirs:
        if not os.path.exists(d): continue
        for root, dirs, files in os.walk(d):
            if target in files:
                return os.path.join(root, target)
    return None

def compress(source, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    size_mb = os.path.getsize(source) / (1024*1024)
    print(f"Found at {source} ({size_mb:.2f} MB)")
    
    img = Image.open(source)
    if img.mode in ("RGBA", "P"): img = img.convert("RGB")
    
    # Compress until < 4.8MB
    q = 95
    while True:
        img.save(dest, format="JPEG", quality=q, optimize=True)
        if os.path.getsize(dest) < 4.8 * 1024 * 1024 or q <= 10:
            break
        q -= 5
    print(f"Saved to {dest} ({os.path.getsize(dest)/(1024*1024):.2f} MB)")

source = find_file()
if source:
    dest = r"c:\Users\dhl\data\Projects\Properlytic_UI\v0-properlytic-8v\public\Screenshot_2026-03-12_155728.jpg"
    compress(source, dest)
else:
    print("File Not found in standard directories.")
