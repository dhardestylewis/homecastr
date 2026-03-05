# Verified Master Schema — Actual Column Headers from GCS
## `gs://properlytic-raw-data` | Project: `properlytic-data`

Last verified: 2026-02-26T12:20Z

---

## Verified Sources (data confirmed in GCS)

### 1. Cook County IL — 19 columns
**GCS:** `cook_county_il/assessed_values_chunk_*.csv` (~27 chunks)
```
pin, year, class, township_code, township_name, nbhd,
mailed_bldg, mailed_land, mailed_tot, mailed_hie,
certified_bldg, certified_land, certified_tot, certified_hie,
board_bldg, board_land, board_tot, board_hie,
row_id
```

| Source Column | → Canonical | Notes |
|---|---|---|
| `pin` | `parcel_id` | 14-digit PIN |
| `year` | (tax year) | Assessment year |
| `class` | `dwelling_type` | Property class code |
| `township_code` | (region) | Township identifier |
| `certified_bldg` | `improvement_value` | Board-certified building value |
| `certified_land` | `land_value` | Board-certified land value |
| `certified_tot` | `assessed_value` | Board-certified total |
| `mailed_bldg/land/tot` | (initial assessment) | Pre-appeal values |
| `board_bldg/land/tot` | (final assessment) | Post-appeal values |

**⚠️ Missing:** `sale_price`, `sale_date`, `sqft`, `year_built`, `bedrooms`, `bathrooms`, `address`, `lat_lon`
**→ Need:** Cook County Sales dataset (separate Socrata endpoint)

---

### 2. San Francisco — 45 columns
**GCS:** `sf/secured_roll/chunk_*.csv`
```
closed_roll_year, property_location, parcel_number, block, lot,
volume_number, use_code, use_definition, property_class_code,
property_class_code_definition, year_property_built,
number_of_bathrooms, number_of_bedrooms, number_of_rooms,
number_of_stories, number_of_units, zoning_code, construction_type,
lot_depth, lot_frontage, property_area, basement_area, lot_area,
lot_code, tax_rate_area_code, percent_of_ownership, exemption_code,
exemption_code_definition, status_code, misc_exemption_value,
homeowner_exemption_value, current_sales_date,
assessed_fixtures_value, assessed_improvement_value,
assessed_land_value, assessed_personal_property_value,
assessor_neighborhood_district, assessor_neighborhood_code,
assessor_neighborhood, supervisor_district, supervisor_district_2012,
analysis_neighborhood, the_geom, row_id, data_as_of, data_loaded_at
```

| Source Column | → Canonical | Notes |
|---|---|---|
| `parcel_number` / `block` + `lot` | `parcel_id` | Block-Lot format |
| `closed_roll_year` | (tax year) | Roll year |
| `property_class_code` | `dwelling_type` | With definition in `_definition` |
| `assessed_improvement_value` | `improvement_value` | |
| `assessed_land_value` | `land_value` | |
| `assessed_improvement_value` + `assessed_land_value` | `assessed_value` | Sum |
| `property_area` | `sqft` | Building area |
| `lot_area` | `land_area` | |
| `year_property_built` | `year_built` | |
| `number_of_bedrooms` | `bedrooms` | |
| `number_of_bathrooms` | `bathrooms` | |
| `number_of_stories` | `stories` | |
| `property_location` | `address` | |
| `the_geom` | `lat_lon` | Point geometry |
| `current_sales_date` | `sale_date` | Last sale only |

**✅ Richest verified source — 13/15 canonical fields present**

---

### 3. France DVF — (gzipped, schema from documentation)
**GCS:** `france_dvf/20{19-24}_full.csv.gz` (6 files)
```
id_mutation, date_mutation, nature_mutation, valeur_fonciere,
adresse_numero, adresse_suffixe, adresse_nom_voie, adresse_code_voie,
code_postal, code_commune, nom_commune, code_departement,
ancien_code_commune, ancien_nom_commune, id_parcelle,
ancien_id_parcelle, numero_volume, lot1_numero, lot1_surface_carrez,
lot2_numero, lot2_surface_carrez, lot3_numero, lot3_surface_carrez,
lot4_numero, lot4_surface_carrez, lot5_numero, lot5_surface_carrez,
nombre_lots, code_type_local, type_local, surface_reelle_bati,
nombre_pieces_principales, code_nature_culture,
nature_culture, code_nature_culture_speciale,
nature_culture_speciale, surface_terrain, longitude, latitude
```

| Source Column | → Canonical |
|---|---|
| `valeur_fonciere` | `sale_price` |
| `date_mutation` | `sale_date` |
| `type_local` | `dwelling_type` |
| `surface_reelle_bati` | `sqft` |
| `surface_terrain` | `land_area` |
| `nombre_pieces_principales` | `bedrooms` (approx) |
| `adresse_*` | `address` |
| `longitude` / `latitude` | `lat_lon` |
| `id_parcelle` | `parcel_id` |

---

### 4. MassGIS — (ZIP/GDB, schema from documentation)
**GCS:** `massgis/MassGIS_L3_Parcels_gdb.zip` (1 file, ~1.4GB)
**Format:** ESRI File Geodatabase — requires `geopandas` / `fiona` to read
**Known fields (from MassGIS docs):**
```
LOC_ID, POLY_TYPE, MAP_NO, SOURCE, PLAN_ID, MAPSHEET,
SITE_ADDR, ADDR_NUM, FULL_STR, CITY, ZIP, OWNER1,
LS_PRICE, LS_DATE, BLDG_VAL, LAND_VAL, OTHER_VAL, TOTAL_VAL,
FY, LOT_SIZE, YEAR_BUILT, BLD_AREA, UNITS, RES_AREA,
STYLE, STORIES, NUM_ROOMS, BDRMS, FULL_BTH, HLF_BTH,
PROP_TYPE, USE_CODE, SITE, ZONE, TOWN_ID
```

---

## ❌ Broken Sources (need URL fixes)

| Source | Problem | Fix |
|---|---|---|
| **Maricopa AZ** | Download URL returns HTML page, not CSV | Need updated bulk download URL from mcassessor.maricopa.gov |
| **UK PPD** | Cloud Function returned 503 | HTTPS URL may need different request path |
| **King County WA** | Cloud Function returned error | aqua.kingcounty.gov may block non-browser requests |
| **NY State** | No chunks appeared in GCS | Socrata endpoint may need different dataset ID |
| **TXGIO** | TNRIS API didn't return download links | Need Texas Comptroller Socrata dataset ID |
| **LA County** | Cloud Function returned error | Socrata endpoint may need verification |

---

## 🎯 Canonical Target Schema

All sources map to this unified schema for training:

```python
# Canonical fields (15 total)
schema = {
    "parcel_id":         str,   # Unique parcel identifier
    "jurisdiction":      str,   # Source jurisdiction code (e.g. "cook_il", "sf_ca", "uk")
    "year":              int,   # Assessment / transaction year
    "sale_price":        float, # Transaction price (null if assessment-only)
    "sale_date":         str,   # Transaction date ISO-8601
    "assessed_value":    float, # Total assessed value
    "land_value":        float, # Land-only component
    "improvement_value": float, # Building/improvement component
    "dwelling_type":     str,   # Standardized: SF, CONDO, MULTI, COMMERCIAL, VACANT
    "sqft":              float, # Living/building area
    "land_area":         float, # Lot area
    "year_built":        int,   # Construction year
    "bedrooms":          int,   # Bedroom count
    "bathrooms":         float, # Bathroom count
    "stories":           float, # Number of stories
    "address":           str,   # Full address
    "lat":               float, # Latitude
    "lon":               float, # Longitude
}
```
