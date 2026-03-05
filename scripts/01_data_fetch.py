"""
01_data_fetch.py
─────────────────────────────────────────────────────────────────────────────
Downloads and caches all raw input data:
  1. CVLZ sign locations  (Seattle Open Data / Socrata)
  2. Seattle parcel polygons (Seattle Open Data)
  3. King County Assessor building data (King County GIS)
  4. PSRC Regional Center boundaries

Outputs (data/raw/):
  cvlz_signs_raw.gpkg
  seattle_parcels_raw.gpkg
  kc_assessor_raw.csv
  regional_centers_raw.gpkg

Usage:
  python scripts/01_data_fetch.py

Then inspect outputs/tables/rc_screening.csv to select your 3–5 centers.
"""

import sys
import json
import requests
import warnings
import pandas as pd
import geopandas as gpd
from pathlib import Path
from tqdm import tqdm

# ── local config ──────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    DATA_RAW, DATA_PROCESSED, OUT_TABLES,
    CRS_GEO, CRS_PROJ,
    SOCRATA_DOMAIN, CVLZ_DATASET, CVLZ_FEATURE_SERVICE, CVLZ_CATEGORY_CODE,
    PARCEL_DATASET, PSRC_CENTERS_URL,
    MIN_CVLZ_COUNT, MIN_DEMAND_PARCELS, DATA_COMPLETENESS,
)

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# 1  CVLZ locations
# ─────────────────────────────────────────────────────────────────────────────

def fetch_cvlz() -> gpd.GeoDataFrame:
    """
    Pull SDOT Street Signs filtered to CVLZ category.
    Returns a GeoDataFrame in WGS-84.
    """
    print("[01] Fetching CVLZ sign locations from Seattle Open Data …")
    out = DATA_RAW / "cvlz_signs_raw.gpkg"

    # ArcGIS feature service (public) — returns GeoJSON in WGS-84
    params = {
        "where": f"CATEGORY='{CVLZ_CATEGORY_CODE}'",
        "outFields": "*",
        "f": "geojson",
        "resultRecordCount": 5000,
    }
    resp = requests.get(CVLZ_FEATURE_SERVICE + "/query", params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", [])
    if not features:
        raise ValueError("No CVLZ records returned from feature service.")

    gdf = gpd.GeoDataFrame.from_features(features, crs=CRS_GEO)
    print(f"  → {len(gdf):,} CVLZ signs fetched")
    gdf.to_file(out, driver="GPKG", layer="cvlz")
    print(f"  Saved → {out.relative_to(DATA_RAW.parent.parent)}")
    return gdf


# ─────────────────────────────────────────────────────────────────────────────
# 2  Seattle parcels
# ─────────────────────────────────────────────────────────────────────────────

def fetch_parcels() -> gpd.GeoDataFrame:
    """
    Download Seattle parcel polygons.
    Source: King County OpenData ArcGIS service (parcel_area layer).
    We bound the query to the Seattle extent in WGS-84 to avoid pulling
    the entire county.
    """
    print("[01] Fetching Seattle parcel polygons …")
    out = DATA_RAW / "seattle_parcels_raw.gpkg"
    base_url = (
        "https://gisdata.kingcounty.gov/arcgis/rest/services/"
        "OpenDataPortal/property__parcel_area/MapServer/439/query"
    )
    seattle_bbox = {"xmin": -122.45, "ymin": 47.48, "xmax": -122.20, "ymax": 47.75}

    features = []
    offset = 0
    page_size = 2000

    while True:
        params = {
            "where": "1=1",
            "geometry": json.dumps(seattle_bbox),
            "geometryType": "esriGeometryEnvelope",
            "inSR": 4326,
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "MAJOR,MINOR,PIN",
            "outSR": 4326,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }
        resp = requests.get(base_url, params=params, timeout=120)
        resp.raise_for_status()
        page = resp.json().get("features", [])
        if not page:
            break
        features.extend(page)
        offset += len(page)
        print(f"  … {offset:,} parcels")
        if len(page) < page_size:
            break

    if not features:
        raise ValueError("No parcel features returned from King County service.")

    gdf = gpd.GeoDataFrame.from_features(features, crs=CRS_GEO)
    print(f"  → {len(gdf):,} parcels")
    gdf.to_file(out, driver="GPKG", layer="parcels")
    print(f"  Saved → {out.relative_to(DATA_RAW.parent.parent)}")
    return gdf


# ─────────────────────────────────────────────────────────────────────────────
# 3  King County Assessor – building / unit data
# ─────────────────────────────────────────────────────────────────────────────

def fetch_assessor() -> pd.DataFrame:
    """
    Download KC Assessor parcel records (PRESENTUSE, SqFtTotLiving, Units).
    Source: https://info.kingcounty.gov/assessor/DataDownload/default.aspx
    We pull the 'parcel' CSV (ResBldg for residential, CommBldg for commercial).
    """
    print("[01] Fetching King County Assessor data …")
    out = DATA_RAW / "kc_assessor_raw.csv"

    # Parcel attribute table (smallest relevant download)
    urls = {
        "parcel": "https://aqua.kingcounty.gov/Assessor/DataDownload/Parcel.zip",
        "resbldg": "https://aqua.kingcounty.gov/Assessor/DataDownload/ResBldg.zip",
    }

    dfs = {}
    for name, url in urls.items():
        print(f"  Downloading {name} …")
        resp = requests.get(url, stream=True, timeout=300)
        if resp.status_code != 200:
            print(f"  ⚠  Could not reach {url} (status {resp.status_code}).")
            print("     Download manually from https://info.kingcounty.gov/assessor/DataDownload/")
            continue
        import zipfile, io
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
        dfs[name] = pd.read_csv(z.open(csv_name), dtype=str)

    if "parcel" in dfs and "resbldg" in dfs:
        parcel  = dfs["parcel"][["Major","Minor","PresentUse","SqFtLot","ZoningCode"]]
        resbldg = dfs["resbldg"][["Major","Minor","SqFtTotLiving","NbrLivingUnits"]]
        combined = parcel.merge(resbldg, on=["Major","Minor"], how="left")
        combined.to_csv(out, index=False)
        print(f"  → {len(combined):,} records saved → {out.relative_to(DATA_RAW.parent.parent)}")
        return combined
    else:
        print("  ⚠  Assessor download incomplete. Create data/raw/kc_assessor_raw.csv manually.")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# 4  PSRC Regional Center boundaries
# ─────────────────────────────────────────────────────────────────────────────

def fetch_regional_centers() -> gpd.GeoDataFrame:
    """
    Download PSRC Regional and Metropolitan Center boundaries.
    Filters to Seattle city limits.
    """
    print("[01] Fetching PSRC Regional Center boundaries …")
    out = DATA_RAW / "regional_centers_raw.gpkg"

    try:
        resp = requests.get(PSRC_CENTERS_URL, timeout=60)
        resp.raise_for_status()
        gdf = gpd.read_file(resp.text)
    except Exception as e:
        print(f"  ⚠  PSRC AGOL endpoint failed ({e}).")
        print("  Trying shinyapps monitoring export …")
        # Fallback: the monitoring tool's static GeoJSON
        fallback = (
            "https://raw.githubusercontent.com/psrc/centers-monitoring/"
            "main/data/regional_centers.geojson"
        )
        try:
            gdf = gpd.read_file(fallback)
        except Exception as e2:
            print(f"  ⚠  Fallback also failed ({e2}).")
            print("  Please download manually from https://psrcwa.shinyapps.io/centers-monitoring/")
            return gpd.GeoDataFrame()

    # Keep only Seattle-area centers (filter by jurisdiction field if present)
    seattle_filter = [
        "Capitol Hill", "South Lake Union", "University District",
        "Northgate", "Columbia City", "Rainier Beach", "Ballard",
        "First Hill", "12th Ave", "Crown Hill", "Bitter Lake",
    ]
    name_col = next(
        (c for c in gdf.columns if "name" in c.lower() or "center" in c.lower()),
        gdf.columns[0]
    )
    gdf = gdf[gdf[name_col].str.contains("|".join(seattle_filter), case=False, na=False)]
    gdf = gdf.to_crs(CRS_GEO)

    print(f"  → {len(gdf)} Seattle Regional Centers retained")
    gdf.to_file(out, driver="GPKG", layer="regional_centers")
    print(f"  Saved → {out.relative_to(DATA_RAW.parent.parent)}")
    return gdf


# ─────────────────────────────────────────────────────────────────────────────
# 5  Regional Center screening table
# ─────────────────────────────────────────────────────────────────────────────

def screen_centers(
    centers: gpd.GeoDataFrame,
    cvlz: gpd.GeoDataFrame,
    parcels: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """
    Compute per-center screening metrics and flag which centers pass all criteria.
    Outputs outputs/tables/rc_screening.csv.
    """
    print("[01] Screening Regional Centers …")
    if centers.empty or cvlz.empty or parcels.empty:
        print("  ⚠  Skipping screening – one or more input layers is empty.")
        return pd.DataFrame()

    # Project everything to WA State Plane for area/distance calculations
    centers_p = centers.to_crs(CRS_PROJ)
    cvlz_p    = cvlz.to_crs(CRS_PROJ)
    parcels_p = parcels.to_crs(CRS_PROJ)

    name_col = next(
        (c for c in centers.columns if "name" in c.lower()),
        centers.columns[0]
    )

    rows = []
    for _, rc in centers_p.iterrows():
        rc_geom = rc.geometry
        name    = rc[name_col]

        # CVLZs within boundary
        cvlz_in = cvlz_p[cvlz_p.within(rc_geom)]
        n_cvlz  = len(cvlz_in)

        # Parcels within boundary
        parc_in = parcels_p[parcels_p.centroid.within(rc_geom)]
        n_parc  = len(parc_in)

        # Data completeness (proxy: non-null geometry count)
        completeness = 1.0  # will be refined in script 02 after assessor join

        rows.append({
            "center_name":    name,
            "n_cvlz":         n_cvlz,
            "n_parcels":      n_parc,
            "completeness":   round(completeness, 2),
            "pass_cvlz":      n_cvlz  >= MIN_CVLZ_COUNT,
            "pass_parcels":   n_parc  >= MIN_DEMAND_PARCELS,
            "pass_complete":  completeness >= DATA_COMPLETENESS,
            "SELECTED":       (n_cvlz >= MIN_CVLZ_COUNT and n_parc >= MIN_DEMAND_PARCELS),
        })

    df = pd.DataFrame(rows).sort_values("n_cvlz", ascending=False)
    out = OUT_TABLES / "rc_screening.csv"
    df.to_csv(out, index=False)
    print(f"  Screening table → {out.relative_to(DATA_RAW.parent.parent.parent)}")
    print(df[["center_name","n_cvlz","n_parcels","SELECTED"]].to_string(index=False))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cvlz    = fetch_cvlz()
    parcels = fetch_parcels()
    _       = fetch_assessor()
    centers = fetch_regional_centers()
    _       = screen_centers(centers, cvlz, parcels)
    print("\n[01] Done. Review outputs/tables/rc_screening.csv and update SELECTED_CENTERS in config.py.")
