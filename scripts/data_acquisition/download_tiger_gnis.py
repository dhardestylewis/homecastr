import os
import requests
import zipfile
import shutil

TIGER_YEAR = "2022"
BASE_DIR = r"C:\tmp\geo_layers"
os.makedirs(BASE_DIR, exist_ok=True)

ALL_STATES = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17", 
    "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", 
    "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", 
    "47", "48", "49", "50", "51", "53", "54", "55", "56"
]

def download_and_extract(url, out_dir):
    filename = url.split("/")[-1]
    zip_path = os.path.join(BASE_DIR, filename)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
    }
    
    if not os.path.exists(out_dir):
        if not os.path.exists(zip_path):
            print(f"Downloading {url}...")
            try:
                with requests.get(url, stream=True, headers=headers, timeout=60) as r:
                    r.raise_for_status()
                    with open(zip_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
            except Exception as e:
                print(f"Failed to download {url}: {e}")
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                return
        
        print(f"Extracting {filename}...")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(out_dir)
            os.remove(zip_path) # Clean up zip after extract
        except zipfile.BadZipFile:
            print(f"Bad zip file: {zip_path}")
            os.remove(zip_path)

def download_tiger(state_fips):
    # Tracts
    tract_url = f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/TRACT/tl_{TIGER_YEAR}_{state_fips}_tract.zip"
    download_and_extract(tract_url, os.path.join(BASE_DIR, "tracts", state_fips))
    
    # Places
    place_url = f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/PLACE/tl_{TIGER_YEAR}_{state_fips}_place.zip"
    download_and_extract(place_url, os.path.join(BASE_DIR, "places", state_fips))
    
    # Cousubs
    cousub_url = f"https://www2.census.gov/geo/tiger/TIGER{TIGER_YEAR}/COUSUB/tl_{TIGER_YEAR}_{state_fips}_cousub.zip"
    download_and_extract(cousub_url, os.path.join(BASE_DIR, "cousubs", state_fips))

def download_gnis():
    # USGS GNIS Populated Places
    gnis_url = "https://prd-tnm.s3.amazonaws.com/StagedProducts/GeographicNames/DomesticNames/DomesticNames_National_Text.zip"
    gnis_dir = os.path.join(BASE_DIR, "gnis")
    if not os.path.exists(gnis_dir):
        download_and_extract(gnis_url, gnis_dir)

if __name__ == "__main__":
    print(f"Setting up geographic layers in {BASE_DIR}")
    for state in ALL_STATES:
        download_tiger(state)
        
    download_gnis()
    print("Done downloading data layers.")
