"""
02_demand_layer.py
─────────────────────────────────────────────────────────────────────────────
Builds the parcel-level Delivery Demand Index (DDI) for each selected
Regional Center.

Inputs (data/raw/):
  seattle_parcels_raw.gpkg
  kc_assessor_raw.csv
  regional_centers_raw.gpkg

Outputs (data/processed/):
  parcels_demand.gpkg   — parcel polygons + DDI + land_use_cat + centroid_x/y

Outputs (outputs/tables/):
  demand_summary.csv    — DDI statistics per center per land-use category

Algorithm
─────────────────────────────────────────────────────────────────────────────
  1. Join Seattle parcels with KC Assessor to get GFA and unit counts.
  2. Map PRESENTUSE codes → demand category (config.KC_USE_MAP).
  3. Compute DDI_i = W_k × (S_i / norm_k).
  4. Clip to selected Regional Centers.
  5. Flag parcels missing both GFA and units; apply FAR-0.5 fallback for those.
  6. Export.
"""

import sys
import warnings
import pandas as pd
import numpy as np
import geopandas as gpd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    DATA_RAW, DATA_PROCESSED, OUT_TABLES,
    CRS_GEO, CRS_PROJ,
    DEMAND_WEIGHTS, KC_USE_MAP,
)

warnings.filterwarnings("ignore")

# ── User-defined: edit after reviewing rc_screening.csv ──────────────────────
SELECTED_CENTERS = [
    "Capitol Hill",
    "South Lake Union",
    "University District",
    "Northgate",
    "Columbia City",
]

FAR_FALLBACK = 0.5   # assumed floor-area ratio when no GFA available


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

def _clean_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def map_demand_category(use_code) -> str:
    """Map KC PRESENTUSE integer to a DEMAND_WEIGHTS key."""
    try:
        code = int(use_code)
    except (TypeError, ValueError):
        return "OTHER"
    return KC_USE_MAP.get(code, "OTHER")


def compute_ddi(row: pd.Series) -> float:
    """
    Compute DDI for a single parcel row.

    DDI_i = W_k × (S_i / norm_k)

    Where:
        k         = demand category
        W_k       = base weight
        S_i       = size metric value (GFA or units)
        norm_k    = normalizer (1 or 1000)
    """
    cat   = row["demand_cat"]
    if cat not in DEMAND_WEIGHTS:
        return 0.0

    weight, size_metric, normalizer, _ = DEMAND_WEIGHTS[cat]
    size_val = row[size_metric] if size_metric in row.index else 0.0
    if pd.isna(size_val) or size_val <= 0:
        return 0.0
    return weight * (size_val / normalizer)


# ─────────────────────────────────────────────────────────────────────────────
# main pipeline
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── 1  Load raw layers ────────────────────────────────────────────────────
    print("[02] Loading parcel geometries …")
    parcels = gpd.read_file(DATA_RAW / "seattle_parcels_raw.gpkg", layer="parcels")
    parcels = parcels.to_crs(CRS_PROJ)

    print("[02] Loading KC Assessor data …")
    assessor_path = DATA_RAW / "kc_assessor_raw.csv"
    if not assessor_path.exists():
        raise FileNotFoundError(
            "kc_assessor_raw.csv not found. Run 01_data_fetch.py first "
            "or manually place the file in data/raw/."
        )
    assessor = pd.read_csv(assessor_path, dtype=str)

    print("[02] Loading Regional Center boundaries …")
    centers = gpd.read_file(DATA_RAW / "regional_centers_raw.gpkg", layer="regional_centers")
    centers = centers.to_crs(CRS_PROJ)

    # Filter to selected centers
    name_col = next(c for c in centers.columns if "name" in c.lower())
    centers  = centers[centers[name_col].isin(SELECTED_CENTERS)].copy()
    if centers.empty:
        raise ValueError(
            f"None of SELECTED_CENTERS found in regional_centers_raw.gpkg.\n"
            f"Available names: {list(centers[name_col])}"
        )
    print(f"  → Analysing {len(centers)} Regional Centers: {SELECTED_CENTERS}")

    # ── 2  Clean assessor data ────────────────────────────────────────────────
    assessor = assessor.copy()
    # Create a 12-char parcel number matching Seattle format (Major+Minor padded)
    assessor["parcel_key"] = (
        assessor["Major"].str.zfill(6) + assessor["Minor"].str.zfill(4)
    )
    assessor["gfa_sqft"]   = _clean_numeric(assessor.get("SqFtTotLiving", pd.Series(dtype=float)))
    assessor["res_units"]  = _clean_numeric(assessor.get("NbrLivingUnits", pd.Series(dtype=float)))
    assessor["lot_sqft"]   = _clean_numeric(assessor.get("SqFtLot", pd.Series(dtype=float)))
    assessor["present_use"]= assessor.get("PresentUse", pd.Series(dtype=str))

    assessor_slim = assessor[["parcel_key","gfa_sqft","res_units","lot_sqft","present_use"]].drop_duplicates("parcel_key")

    # ── 3  Join parcels + assessor ────────────────────────────────────────────
    print("[02] Joining parcels with assessor records …")
    # Normalise parcel PIN to the same 10-char key
    if "pin" in parcels.columns:
        parcels["parcel_key"] = parcels["pin"].str.replace("-","").str.zfill(10)
    else:
        # Fallback: create dummy key so the merge still runs
        parcels["parcel_key"] = np.arange(len(parcels)).astype(str)

    merged = parcels.merge(assessor_slim, on="parcel_key", how="left")
    merged["gfa_sqft"]  = _clean_numeric(merged["gfa_sqft"])
    merged["res_units"] = _clean_numeric(merged["res_units"])
    merged["lot_sqft"]  = _clean_numeric(merged["lot_sqft"])

    # ── 4  FAR fallback for missing GFA ───────────────────────────────────────
    missing_gfa = (merged["gfa_sqft"] <= 0) & (merged["res_units"] <= 0)
    merged.loc[missing_gfa, "gfa_sqft"] = merged.loc[missing_gfa, "lot_sqft"] * FAR_FALLBACK
    merged["gfa_estimated"] = missing_gfa  # flag for methods disclosure

    pct_estimated = missing_gfa.mean() * 100
    print(f"  GFA estimated via FAR fallback for {pct_estimated:.1f}% of parcels")

    # ── 5  Map demand category + compute DDI ─────────────────────────────────
    print("[02] Computing Delivery Demand Index …")
    merged["demand_cat"] = merged["present_use"].apply(
        lambda x: map_demand_category(x) if pd.notna(x) else "OTHER"
    )

    # Rename size columns to match DEMAND_WEIGHTS expectations
    # (DEMAND_WEIGHTS keys reference "gfa_sqft" and "res_units" directly)
    merged["DDI"] = merged.apply(compute_ddi, axis=1)

    # Drop parcels with zero demand (non-delivery uses)
    demand_parcels = merged[merged["DDI"] > 0].copy()
    print(f"  → {len(demand_parcels):,} demand-generating parcels (DDI > 0)")

    # ── 6  Add centroid coordinates (for spatial join in script 04) ───────────
    demand_parcels["centroid_x"] = demand_parcels.geometry.centroid.x
    demand_parcels["centroid_y"] = demand_parcels.geometry.centroid.y

    # ── 7  Clip to selected Regional Centers, add center label ───────────────
    print("[02] Clipping to selected Regional Centers …")
    all_centers_union = centers.union_all()
    demand_parcels = demand_parcels[demand_parcels.centroid.within(all_centers_union)].copy()

    # Spatial join to label each parcel with its center
    demand_parcels = gpd.sjoin(
        demand_parcels,
        centers[[name_col, "geometry"]].rename(columns={name_col: "center_name"}),
        how="left",
        predicate="within",
    ).drop(columns=["index_right"], errors="ignore")

    print(f"  → {len(demand_parcels):,} demand parcels within selected centers")

    # ── 8  Export spatial layer ───────────────────────────────────────────────
    out_gpkg = DATA_PROCESSED / "parcels_demand.gpkg"
    cols_keep = [
        "parcel_key", "center_name", "demand_cat", "DDI",
        "gfa_sqft", "res_units", "gfa_estimated",
        "centroid_x", "centroid_y", "geometry",
    ]
    cols_keep = [c for c in cols_keep if c in demand_parcels.columns]
    demand_parcels[cols_keep].to_file(out_gpkg, driver="GPKG", layer="demand_parcels")
    print(f"  Saved → {out_gpkg.relative_to(DATA_RAW.parent.parent)}")

    # ── 9  Summary table ──────────────────────────────────────────────────────
    summary = (
        demand_parcels
        .groupby(["center_name", "demand_cat"])
        .agg(
            n_parcels=("DDI", "count"),
            total_DDI=("DDI", "sum"),
            mean_DDI=("DDI", "mean"),
            max_DDI=("DDI", "max"),
        )
        .reset_index()
        .round(2)
    )
    out_csv = OUT_TABLES / "demand_summary.csv"
    summary.to_csv(out_csv, index=False)
    print(f"  Summary → {out_csv.relative_to(DATA_RAW.parent.parent.parent)}")
    print("\n" + summary.to_string(index=False))

    print("\n[02] Done.")


if __name__ == "__main__":
    main()
