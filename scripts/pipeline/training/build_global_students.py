"""
build_global_students.py
========================
Phase 2: Global Geography Extractor & WDI KNN Matcher

1. Downloads Natural Earth Admin 1 (States/Provinces) boundaries.
2. Fetches the same 6 WDI indicators (GDP per capita, Urban %, etc.) for all global countries for the anchor year (default 2024).
3. Evaluates structural similarity (KNN) against the 27 Eurostat Teacher nations.
4. Outputs `_scratch/data/global_student_matches.parquet` with the assigned Eurostat NUTS neighbor and GDP-scaled baseline anchors.
"""

import os
import sys
import json
import urllib.request
import time
import zipfile
import io
import pandas as pd
import polars as pl
import geopandas as gpd
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
ts = lambda: time.strftime("%Y-%m-%d %H:%M:%S")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DATA_DIR = os.path.join(BASE_DIR, "_scratch", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Shared WDI indicators from the enrichment script
WDI_INDICATORS = {
    "NY.GDP.PCAP.CD":    "wdi_gdp_per_capita",
    "FR.INR.RINR":       "wdi_real_interest_rate",
    "SP.URB.TOTL.IN.ZS": "wdi_urban_pct",
    "EN.POP.DNST":       "wdi_pop_density",
    "AG.LND.AGRI.ZS":    "wdi_ag_land_pct",
    "FP.CPI.TOTL.ZG":    "wdi_cpi_inflation",
}

def download_natural_earth_admin1():
    """Download 1:10m Admin 1 States & Provinces from Natural Earth."""
    print(f"[{ts()}] Downloading Natural Earth Admin 1 polygons...")
    url = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip"
    
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req) as resp:
        zip_data = resp.read()
        
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        shp_file = [f for f in z.namelist() if f.endswith('.shp')][0]
        gdf = gpd.read_file(io.BytesIO(zip_data), vfs=f"zip://{shp_file}")
        
    print(f"[{ts()}] Loaded {len(gdf):,} Admin 1 regions")
    
    # We need iso2, region name, and geometry centroid
    # In Natural Earth 10m admin 1, iso2 is usually in 'iso_a2'
    if 'iso_a2' in gdf.columns:
        gdf = gdf.rename(columns={'iso_a2': 'iso2', 'name': 'region_name'})
    else:
        raise ValueError("Could not find ISO2 column in Natural Earth shapefile")
        
    gdf['centroid_lon'] = gdf.geometry.centroid.x
    gdf['centroid_lat'] = gdf.geometry.centroid.y
    
    # Keep essential columns
    keep_cols = ['iso2', 'region_name', 'centroid_lon', 'centroid_lat', 'geometry']
    gdf = gdf[[c for c in keep_cols if c in gdf.columns]]
    
    # Drop rows without ISO2 code
    gdf = gdf[gdf['iso2'].notna() & (gdf['iso2'] != '') & (gdf['iso2'] != '-99')]
    
    return gdf

def fetch_wdi_latest(iso2_list, target_year=2023):
    """
    Fetch the most recent WDI indicator reading for countries.
    Uses target_year (typically year-1 to ensure data availability).
    """
    print(f"[{ts()}] Fetching WDI backfill for {len(iso2_list)} countries (Target Year: {target_year})")
    all_frames = []
    
    # World Bank API takes max 300-400 chars of country codes, chunk them
    chunk_size = 50
    for i in range(0, len(iso2_list), chunk_size):
        chunk = iso2_list[i:i+chunk_size]
        country_str = ";".join(sorted(set(chunk)))
        
        chunk_data = []
        for indicator_code, col_name in WDI_INDICATORS.items():
            url = (
                f"https://api.worldbank.org/v2/country/{country_str}/indicator/{indicator_code}"
                f"?date={target_year-5}:{target_year}&format=json&per_page=5000"
            )
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    
                if not isinstance(data, list) or len(data) < 2:
                    continue
                    
                # Store the most recent non-null value per country in the 5-year window
                country_max_yr = {}
                for entry in data[1]:
                    val = entry.get("value")
                    if val is None: continue
                    
                    yr = int(entry.get("date"))
                    iso2 = entry.get("country", {}).get("id", "")
                    
                    if iso2 not in country_max_yr or yr > country_max_yr[iso2]['yr']:
                        country_max_yr[iso2] = {
                            "iso2": iso2,
                            "wdi_col": col_name,
                            "val": float(val),
                            "yr": yr
                        }
                        
                for rec in country_max_yr.values():
                    chunk_data.append(rec)
                    
            except Exception as e:
                print(f"    WARNING chunk error for {indicator_code}: {e}")
                
        if chunk_data:
            all_frames.extend(chunk_data)
            time.sleep(1) # Be nice to WB API

    if not all_frames:
        raise RuntimeError("Failed to fetch WDI backfill data")
        
    df_long = pd.DataFrame(all_frames)
    
    # Pivot to wide format: iso2 -> wdi_gdp_per_capita, wdi_urban_pct, etc.
    df_wide = df_long.pivot(index='iso2', columns='wdi_col', values='val').reset_index()
    return pl.from_pandas(df_wide)

def calculate_nuts_baselines(adapted_path, origin_year=2024):
    """Calculate the median base price per NUTS country to act as Anchors."""
    print(f"[{ts()}] Calculating Teacher NUTS Base Prices (origin={origin_year})")
    
    # Only keep accounts that were physically in the 80% training set
    # OR dynamically infer the NUTS baseline if it's the whole panel
    df = pl.read_parquet(adapted_path)
    
    # Filter to most recent available year (origin_year or earlier fallback)
    base_prices = df.filter(pl.col("yr") <= origin_year) \
                    .sort(["acct", "yr"]) \
                    .group_by("acct") \
                    .last()
                    
    # The teacher acct is "geo_agriprod". We want country-level baselines (first 2 chars of geo).
    # Since we need to assign a NUTS neighbor id, we'll actually aggregate at the country level.
    base_prices = base_prices.with_columns(
        (pl.col("geo").str.slice(0, 2)).alias("iso2")
    )
    
    teacher_country_base = base_prices.group_by("iso2").agg(
        pl.col("price_eur_per_hectare").median().alias("teacher_base_eur_ha")
    )
    
    return teacher_country_base

def main():
    # 1. Download World Student Regions
    gdf_world = download_natural_earth_admin1()
    world_iso2s = gdf_world['iso2'].unique().tolist()
    print(f"[{ts()}] Target Student Countries: {len(world_iso2s)}")
    
    # 2. Extract Teacher WDI profile (from enriched panel)
    enriched_path = os.path.join(DATA_DIR, "eurostat_enriched.parquet")
    if not os.path.exists(enriched_path):
        raise FileNotFoundError(f"Missing {enriched_path}. Please run enrich_eurostat_globals.py first.")
        
    df_teacher = pl.read_parquet(enriched_path)
    
    # The teacher iso2 mapping (from first 2 chars of geo)
    # We will grab the 2023 (or latest) WDI vector for each Teacher country
    wdi_cols = list(WDI_INDICATORS.values())
    
    # Re-extract ISO2 using the exact map from the enrichment script
    NUTS_TO_ISO2 = {
        "AT": "AT", "BE": "BE", "BG": "BG", "CY": "CY", "CZ": "CZ",
        "DE": "DE", "DK": "DK", "EE": "EE", "EL": "GR", "ES": "ES",
        "FI": "FI", "FR": "FR", "HR": "HR", "HU": "HU", "IE": "IE",
        "IT": "IT", "LT": "LT", "LU": "LU", "LV": "LV", "MT": "MT",
        "NL": "NL", "PL": "PL", "PT": "PT", "RO": "RO", "SE": "SE",
        "SI": "SI", "SK": "SK", "UK": "GB", "NO": "NO", "IS": "IS",
        "CH": "CH", "RS": "RS", "ME": "ME", "MK": "MK", "AL": "AL",
        "BA": "BA", "TR": "TR", "XK": "XK",
    }
    
    df_t = df_teacher.with_columns(
        pl.col("acct").str.split("_").list.first().str.slice(0, 2).replace(
            NUTS_TO_ISO2, default=pl.col("acct").str.split("_").list.first().str.slice(0, 2)
        ).alias("iso2")
    )
    
    # Group by ISO2 and get the median WDI vector over the last 5 years to be stable
    teacher_wdi = df_t.filter(pl.col("yr") >= 2019).group_by("iso2").agg([
        pl.col(c).median() for c in wdi_cols
    ]).drop_nulls()
    
    print(f"[{ts()}] Extracted {len(teacher_wdi)} Teacher WDI Profiles")
    
    # 3. Fetch Student WDI profile
    # Only fetch for countries we don't already have in the teacher set
    teacher_iso2s = teacher_wdi["iso2"].to_list()
    student_fetch_list = [iso for iso in world_iso2s if iso not in teacher_iso2s]
    
    df_student_wdi = fetch_wdi_latest(student_fetch_list, target_year=2023)
    print(f"[{ts()}] Fetched WDI for {len(df_student_wdi)} Student Countries")
    
    # Impute missing WDI with global medians just so KNN doesn't drop countries
    for c in wdi_cols:
        if c in df_student_wdi.columns:
            global_med = df_student_wdi[c].median()
            df_student_wdi = df_student_wdi.with_columns(pl.col(c).fill_null(global_med))
            
    # 4. KNN Matching
    # Combine Teacher and Student to normalize properly
    all_wdi = pl.concat([teacher_wdi, df_student_wdi], how="diagonal_relaxed")
    
    # We want to match ON: log(GDP), urban%, pop density, inflation, interest rate, ag_pct
    pandas_wdi = all_wdi.to_pandas()
    # Log transform strictly positive heavy-tail features
    if "wdi_gdp_per_capita" in pandas_wdi.columns:
        pandas_wdi["wdi_gdp_per_capita_log"] = np.log1p(pandas_wdi["wdi_gdp_per_capita"].clip(lower=0))
    if "wdi_pop_density" in pandas_wdi.columns:
        pandas_wdi["wdi_pop_density_log"] = np.log1p(pandas_wdi["wdi_pop_density"].clip(lower=0))
        
    # Scale Features
    match_features = ["wdi_gdp_per_capita_log", "wdi_pop_density_log", "wdi_urban_pct", "wdi_ag_land_pct"]
    for c in match_features:
        if c not in pandas_wdi.columns:
            pandas_wdi[c] = 0.0
            
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(pandas_wdi[match_features].fillna(0))
    pandas_wdi["X_vec"] = list(X_scaled)
    
    # Fit KNN strictly on the Teacher set
    teacher_mask = pandas_wdi["iso2"].isin(teacher_iso2s)
    df_t_pd = pandas_wdi[teacher_mask].copy()
    X_teacher = np.stack(df_t_pd["X_vec"].values)
    
    knn = NearestNeighbors(n_neighbors=1, metric="euclidean")
    knn.fit(X_teacher)
    
    # Find nearest teacher for every student
    distances, indices = knn.kneighbors(np.stack(pandas_wdi["X_vec"].values))
    pandas_wdi["matched_teacher_iso2"] = df_t_pd["iso2"].iloc[indices[:, 0]].values
    pandas_wdi["match_distance"] = distances[:, 0]
    
    # 5. Output Assignment logic
    # Load Teacher base prices
    adapted_path = os.path.join(DATA_DIR, "eurostat_adapted.parquet")
    teacher_bases = calculate_nuts_baselines(adapted_path, origin_year=2024).to_pandas()
    
    # Merge matches with prices
    res_df = pd.merge(pandas_wdi, teacher_bases, left_on="matched_teacher_iso2", right_on="iso2", suffixes=("", "_t"))
    
    # ── Option B: GDP-Scaled Anchors ──
    # Synthesize the student baseline anchor by scaling the teacher baseline linearly via GDP per capita
    # student_y0 = teacher_y0 * (student_gdp / teacher_gdp)
    
    # Get teacher's own GDP to scale against
    teacher_gdp_map = df_t_pd.set_index("iso2")["wdi_gdp_per_capita"].to_dict()
    res_df["teacher_gdp_capita"] = res_df["matched_teacher_iso2"].map(teacher_gdp_map)
    
    # Apply Option B logic (clamped between 0.05x and 2.0x to avoid extreme outliers)
    res_df["gdp_ratio"] = res_df["wdi_gdp_per_capita"] / res_df["teacher_gdp_capita"].replace(0, np.nan)
    res_df["gdp_ratio_clamped"] = res_df["gdp_ratio"].clip(lower=0.05, upper=2.0)
    
    res_df["student_base_eur_ha"] = res_df["teacher_base_eur_ha"] * res_df["gdp_ratio_clamped"]
    
    # 6. Apply back to original GADM geometries
    print(f"[{ts()}] Joining structural anchors back to {len(gdf_world)} spatial boundaries...")
    final_gdf = gdf_world.merge(res_df[['iso2', 'matched_teacher_iso2', 'match_distance', 
                                        'student_base_eur_ha', 'wdi_gdp_per_capita', 'wdi_urban_pct']], 
                                on='iso2', how='inner')
                                
    out_file = os.path.join(DATA_DIR, "global_student_matches.parquet")
    # Convert to standard Pandas for Parquet save (drop geometry if needed, or save as geoparquet)
    # We will save as standard parquet with lat/lon centroids for easy loading in Torch
    output_df = pd.DataFrame(final_gdf.drop(columns='geometry'))
    output_pl = pl.from_pandas(output_df)
    
    output_pl.write_parquet(out_file)
    print(f"[{ts()}] Success: Saved {len(output_pl)} student boundaries to {out_file}")
    
    # Print a few fun examples of cross-hemisphere assignments
    print("\nSample Mappings (Student -> Nearest Structural Euro Teacher):")
    sample = output_pl.filter(pl.col("iso2").is_in(["IN", "BR", "ZA", "ID", "CA"])).to_pandas()
    for _, row in sample.drop_duplicates(subset=["iso2"]).iterrows():
        print(f"  {row['region_name']} ({row['iso2']})  ->  {row['matched_teacher_iso2']} ")
        print(f"    Student Base: €{row['student_base_eur_ha']:.0f}/ha  (Ratio: {row['wdi_gdp_per_capita']:.0f} vs {teacher_gdp_map.get(row['matched_teacher_iso2'], 0):.0f})")

if __name__ == "__main__":
    main()
