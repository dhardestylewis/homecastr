import os
import time
import random
import requests
import concurrent.futures
import sys
from threading import Lock, Event

# --- 1. CONFIGURATION ---
BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "hcad")

# PARANOID SETTINGS
MAX_WORKERS = 2          # Low concurrency (looks like 2 tabs open)
MAX_RETRIES = 5          # Be patient with connection drops
MIN_SLEEP = 3.0          # Minimum wait between files
MAX_SLEEP = 8.0          # Maximum wait (very human pace)
BREAK_INTERVAL = 15      # Take a "coffee break" every 15 files
BREAK_DURATION = 30      # Break lasts 30 seconds

# BROWSER PERSONAS (Identity Rotation)
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Safari on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    # Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

# --- GLOBAL STATE ---
print_lock = Lock()
stop_event = Event()
files_downloaded_count = 0

def get_headers():
    """Returns a random browser identity."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://hcad.org/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }

def safe_log(msg):
    with print_lock:
        print(msg)

def download_task(task):
    global files_downloaded_count
    
    # 0. Check Kill Switch
    if stop_event.is_set(): return None

    url, final_path = task
    filename = os.path.basename(final_path)
    temp_path = final_path + ".part"

    # 1. Resume Check
    if os.path.exists(final_path):
        if os.path.getsize(final_path) > 1024:
            return None # Silent skip
        os.remove(final_path)

    # 2. "Coffee Break" Logic
    # Every N files, sleep longer to break patterns
    with print_lock:
        files_downloaded_count += 1
        current_count = files_downloaded_count
    
    if current_count % BREAK_INTERVAL == 0:
        safe_log(f"☕ Taking a 'coffee break' for {BREAK_DURATION}s to cool down...")
        time.sleep(BREAK_DURATION)

    # 3. Download Loop
    for attempt in range(1, MAX_RETRIES + 1):
        if stop_event.is_set(): return None

        try:
            # Human Jitter (Random wait)
            sleep_time = random.uniform(MIN_SLEEP, MAX_SLEEP)
            time.sleep(sleep_time)

            # Request
            with requests.get(url, headers=get_headers(), stream=True, timeout=45) as r:
                # Handle "File Not Found" (Normal for older years)
                if r.status_code == 404:
                    return None
                
                # Handle "Forbidden" (THE DANGER ZONE)
                if r.status_code == 403:
                    safe_log(f"🚨 403 FORBIDDEN DETECTED on {filename}")
                    safe_log(f"🛑 EMERGENCY STOP triggered to protect IP.")
                    stop_event.set() # Kill all other threads
                    return None

                r.raise_for_status()

                # Atomic Write
                os.makedirs(os.path.dirname(temp_path), exist_ok=True)
                with open(temp_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024*1024):
                        if chunk: f.write(chunk)
            
            # Finalize
            os.rename(temp_path, final_path)
            size_mb = os.path.getsize(final_path) / (1024*1024)
            return f"✅ Got: {filename} ({size_mb:.1f} MB)"

        except Exception as e:
            if attempt < MAX_RETRIES:
                safe_log(f"⚠️  Retry {attempt}/{MAX_RETRIES} ({filename}): {e}")
                time.sleep(10 * attempt) # Exponential backoff
            else:
                return f"❌ FAILED: {filename}"

def main():
    print(f"🕵️  HCAD STEALTH DOWNLOADER (Paranoid Mode)")
    print(f"📂 Saving to: {BASE_DIR}")
    print(f"🐌 Pace: {MIN_SLEEP}-{MAX_SLEEP}s delay | ☕ Break every {BREAK_INTERVAL} files")
    print("---------------------------------------------------------------")

    # 1. Build Task List
    tasks = []
    
    # Historical Data (2005-2025)
    for year in range(2025, 2004, -1):
        year_dir = os.path.join(BASE_DIR, str(year))
        
        # Attributes
        files = [
            "Real_acct_owner.zip", "Real_building_land.zip", "Real_jur_exempt.zip", 
            "Code_description_real.zip", "Real_acct_ownership_history.zip",
            "Real_structural_elem.zip", "Real_neighborhood.zip", "Real_extra.zip",
            "PP_files.zip", "Code_description_pp.zip", "PP_marine.zip",
            "Hearing_files.zip"
        ]
        for f in files:
            tasks.append((f"https://download.hcad.org/data/CAMA/{year}/{f}", os.path.join(year_dir, f)))

        # GIS Parcels
        if year == 2025:
             tasks.append(("https://download.hcad.org/data/GIS/Parcels.zip", os.path.join(year_dir, "Parcels_2025.zip")))
        else:
             tasks.append((f"https://download.hcad.org/data/GIS/Parcels_{year}_Oct.zip", os.path.join(year_dir, f"Parcels_{year}_Oct.zip")))
             tasks.append((f"https://download.hcad.org/data/GIS/Parcels_{year}.zip", os.path.join(year_dir, f"Parcels_{year}.zip")))

    # Static GIS Layers
    gis_layers = [
        "Abstract.zip", "Blk_num.zip", "City.zip", "College.zip", "County.zip", 
        "Easement.zip", "Emergency.zip", "Fire.zip", "Hwy.zip", "ROW_line.zip", 
        "School.zip", "Sub_poly.zip", "TIRZ.zip", "Utility.zip", "Water_district.zip", 
        "Flood_plain.zip"
    ]
    for layer in gis_layers:
        tasks.append((f"https://download.hcad.org/data/GIS/{layer}", os.path.join(BASE_DIR, "GIS_Context", layer)))

    print(f"📝 Job Queue: {len(tasks)} files.")

    # 2. Execute with Stealth
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_task, t) for t in tasks]
        
        for future in concurrent.futures.as_completed(futures):
            # If kill switch is pulled, cancel pending
            if stop_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                print("\n🛑 STOPPING: User IP Protection Triggered.")
                break
                
            res = future.result()
            if res: safe_log(res)

    print("\n✅ Run Complete. Check output folder.")

if __name__ == "__main__":
    main()