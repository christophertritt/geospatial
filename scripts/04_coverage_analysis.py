"""
04_coverage_analysis.py
─────────────────────────────────────────────────────────────────────────────
Computes coverage and demand-capacity indicators:

  Per parcel:
    - covered_{threshold}: bool – centroid within service area union
    - gap_{threshold}: bool    – NOT covered at threshold

  Per Regional Center × threshold:
    - WCR  = Weighted Coverage Rate   = Σ DDI_covered / Σ DDI_total
    - PCR  = Point Coverage Rate      = N_covered / N_total
    - total_DDI, n_parcels

  Per 100 m grid cell:
    - DDI_sum
    - n_cvlz      : CVLZs whose service area (100 m) intersects cell
    - DCR         : Demand-to-Capacity Ratio = DDI_sum / max(n_cvlz, 1)
    - covered     : cell intersects supply union at 100 m
    - stress_tier : "high" / "medium" / "low" based on DCR percentile

Inputs (data/processed/):
  parcels_demand.gpkg
  cvlz_service_areas.gpkg
  cvlz_union.gpkg

Inputs (data/raw/):
  regional_centers_raw.gpkg
  cvlz_signs_raw.gpkg

Outputs (data/processed/):
  parcels_demand_covered.gpkg   — parcels with coverage flags
  grid_indicators.gpkg          — 100 m grid with DDI_sum, n_cvlz, DCR

Outputs (outputs/tables/):
  coverage_summary.csv          — WCR, PCR per center per threshold
  center_overview.csv           — totals per center
"""

import sys
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import box

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    DATA_RAW, DATA_PROCESSED, OUT_TABLES, OUT_SPATIAL,
    CRS_GEO, CRS_PROJ,
    WALK_THRESHOLDS, PRIMARY_THRESHOLD, GRID_CELL_M,
)

warnings.filterwarnings("ignore")

SELECTED_CENTERS = [
    "Capitol Hill",
    "South Lake Union",
    "University District",
    "Northgate",
    "Columbia City",
]


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────

def make_grid(gdf_proj: gpd.GeoDataFrame, cell_m: int) -> gpd.GeoDataFrame:
    """Create a regular grid of cell_m × cell_m squares covering the bbox of gdf."""
    xmin, ymin, xmax, ymax = gdf_proj.total_bounds
    cols = np.arange(xmin, xmax + cell_m, cell_m)
    rows = np.arange(ymin, ymax + cell_m, cell_m)
    cells = [
        box(x, y, x + cell_m, y + cell_m)
        for x in cols[:-1]
        for y in rows[:-1]
    ]
    return gpd.GeoDataFrame({"geometry": cells}, crs=gdf_proj.crs)


# ─────────────────────────────────────────────────────────────────────────────
# main pipeline
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── Load processed layers ─────────────────────────────────────────────────
    print("[04] Loading demand parcels …")
    demand = gpd.read_file(DATA_PROCESSED / "parcels_demand.gpkg", layer="demand_parcels")
    demand = demand.to_crs(CRS_PROJ)

    print("[04] Loading CVLZ union service areas …")
    union_path = DATA_PROCESSED / "cvlz_union.gpkg"
    if not union_path.exists():
        raise FileNotFoundError("Run 03_supply_layer.py first.")
    union_all = gpd.read_file(union_path, layer="union")
    union_all = union_all.to_crs(CRS_PROJ)

    print("[04] Loading individual service areas …")
    sa_all = gpd.read_file(DATA_PROCESSED / "cvlz_service_areas.gpkg", layer="service_areas")
    sa_all = sa_all.to_crs(CRS_PROJ)

    print("[04] Loading Regional Center boundaries …")
    centers = gpd.read_file(DATA_RAW / "regional_centers_raw.gpkg", layer="regional_centers")
    centers = centers.to_crs(CRS_PROJ)
    name_col = next(c for c in centers.columns if "name" in c.lower())
    centers  = centers[centers[name_col].isin(SELECTED_CENTERS)].copy()
    centers  = centers.rename(columns={name_col: "center_name"})

    print("[04] Loading raw CVLZ points …")
    cvlz = gpd.read_file(DATA_RAW / "cvlz_signs_raw.gpkg", layer="cvlz")
    cvlz = cvlz.to_crs(CRS_PROJ)

    # ── Coverage flags on parcel centroids ───────────────────────────────────
    print("[04] Computing parcel-level coverage flags …")
    demand["centroid_geom"] = demand.geometry.centroid
    centroids_gdf = demand.set_geometry("centroid_geom")

    for thresh_name in WALK_THRESHOLDS:
        union_thresh = union_all[union_all["threshold"] == thresh_name].copy()
        if union_thresh.empty:
            demand[f"covered_{thresh_name}"] = False
            continue

        # One union polygon per center
        covered_flags = gpd.sjoin(
            centroids_gdf[["parcel_key","center_name","centroid_geom"]],
            union_thresh[["center_name","geometry"]],
            how="left",
            predicate="within",
        )
        covered_flags["_covered"] = covered_flags["center_name_right"].notna()
        # Deduplicate (a parcel shouldn't match two centers)
        covered_flags = covered_flags.groupby("parcel_key")["_covered"].max().reset_index()

        demand = demand.merge(covered_flags.rename(columns={"_covered": f"covered_{thresh_name}"}),
                              on="parcel_key", how="left")
        demand[f"covered_{thresh_name}"] = demand[f"covered_{thresh_name}"].fillna(False)
        demand[f"gap_{thresh_name}"]     = ~demand[f"covered_{thresh_name}"]

    # ── Save enriched parcel layer ────────────────────────────────────────────
    out_parcels = DATA_PROCESSED / "parcels_demand_covered.gpkg"
    demand.drop(columns=["centroid_geom"], errors="ignore")\
          .to_file(out_parcels, driver="GPKG", layer="demand_parcels")
    print(f"  Covered parcels → {out_parcels.relative_to(DATA_RAW.parent.parent)}")

    # ── Coverage summary table ────────────────────────────────────────────────
    print("[04] Computing WCR / PCR per center per threshold …")
    rows = []
    for center in SELECTED_CENTERS:
        sub = demand[demand["center_name"] == center]
        if sub.empty:
            continue
        total_ddi = sub["DDI"].sum()
        n_total   = len(sub)
        for thresh_name, dist_m in WALK_THRESHOLDS.items():
            cov_col = f"covered_{thresh_name}"
            if cov_col not in sub.columns:
                continue
            covered = sub[sub[cov_col]]
            wcr = covered["DDI"].sum() / total_ddi if total_ddi > 0 else 0.0
            pcr = len(covered) / n_total if n_total > 0 else 0.0
            rows.append({
                "center_name":   center,
                "threshold":     thresh_name,
                "dist_m":        dist_m,
                "n_parcels":     n_total,
                "n_covered":     len(covered),
                "n_uncovered":   n_total - len(covered),
                "total_DDI":     round(total_ddi, 1),
                "DDI_covered":   round(covered["DDI"].sum(), 1),
                "WCR":           round(wcr, 3),
                "PCR":           round(pcr, 3),
            })

    coverage_df = pd.DataFrame(rows)
    out_cov = OUT_TABLES / "coverage_summary.csv"
    coverage_df.to_csv(out_cov, index=False)
    print(f"  Coverage summary → {out_cov.relative_to(DATA_RAW.parent.parent.parent)}")
    print("\n" + coverage_df[["center_name","threshold","WCR","PCR","n_uncovered"]].to_string(index=False))

    # ── Center overview table ─────────────────────────────────────────────────
    n_cvlz_per_center = (
        gpd.sjoin(cvlz, centers[["center_name","geometry"]], how="inner", predicate="within")
        .groupby("center_name")
        .size()
        .rename("n_cvlz")
        .reset_index()
    )
    area_km2 = centers.set_index("center_name")["geometry"].area / 1e6

    overview_rows = []
    for center in SELECTED_CENTERS:
        sub = demand[demand["center_name"] == center]
        n_cv = n_cvlz_per_center.set_index("center_name")["n_cvlz"].get(center, 0)
        a    = area_km2.get(center, np.nan)
        overview_rows.append({
            "center_name":    center,
            "n_parcels":      len(sub),
            "total_DDI":      round(sub["DDI"].sum(), 1),
            "n_cvlz":         int(n_cv),
            "area_km2":       round(a, 3) if not np.isnan(a) else None,
            "cvlz_density":   round(n_cv / a, 2) if (not np.isnan(a) and a > 0) else None,
        })
    overview_df = pd.DataFrame(overview_rows)
    out_ov = OUT_TABLES / "center_overview.csv"
    overview_df.to_csv(out_ov, index=False)
    print(f"\n  Center overview → {out_ov.relative_to(DATA_RAW.parent.parent.parent)}")

    # ── 100 m grid layer ──────────────────────────────────────────────────────
    print("[04] Building 100 m grid indicators …")
    grid_list = []

    for _, rc in centers.iterrows():
        center_name = rc["center_name"]
        rc_geom     = rc.geometry

        # Build grid covering this RC
        rc_gdf  = gpd.GeoDataFrame({"geometry": [rc_geom]}, crs=CRS_PROJ)
        grid    = make_grid(rc_gdf, GRID_CELL_M)
        grid    = grid[grid.intersects(rc_geom)].copy()
        grid["center_name"] = center_name

        # Demand parcels for this center
        sub_demand = demand[demand["center_name"] == center_name].copy()
        sub_demand = sub_demand.set_geometry("centroid_geom") if "centroid_geom" in sub_demand.columns else sub_demand

        # Aggregate DDI per cell
        ddi_join = gpd.sjoin(
            sub_demand[["DDI","geometry"]],
            grid[["geometry"]].reset_index().rename(columns={"index":"cell_idx"}),
            how="right",
            predicate="within",
        )
        ddi_agg = ddi_join.groupby("cell_idx")["DDI"].sum().rename("DDI_sum")
        grid    = grid.reset_index().rename(columns={"index":"cell_idx"})
        grid    = grid.merge(ddi_agg, on="cell_idx", how="left")
        grid["DDI_sum"] = grid["DDI_sum"].fillna(0.0)

        # Count CVLZs whose 100 m service area overlaps each cell
        sa_center = sa_all[
            (sa_all["center_name"] == center_name) &
            (sa_all["threshold"] == PRIMARY_THRESHOLD)
        ]
        if not sa_center.empty:
            cvlz_count_join = gpd.sjoin(
                sa_center[["geometry"]].reset_index().rename(columns={"index":"sa_idx"}),
                grid[["cell_idx","geometry"]],
                how="left",
                predicate="intersects",
            )
            cvlz_per_cell = cvlz_count_join.groupby("cell_idx")["sa_idx"].count().rename("n_cvlz")
            grid = grid.merge(cvlz_per_cell, on="cell_idx", how="left")
            grid["n_cvlz"] = grid["n_cvlz"].fillna(0).astype(int)
        else:
            grid["n_cvlz"] = 0

        # DCR = DDI_sum / max(n_cvlz, 1)
        grid["DCR"] = grid["DDI_sum"] / grid["n_cvlz"].clip(lower=1)

        grid_list.append(grid)

    if grid_list:
        full_grid = pd.concat(grid_list, ignore_index=True)
        full_grid = gpd.GeoDataFrame(full_grid, crs=CRS_PROJ)

        # Stress tier: percentile-based within each center
        def _tier(s):
            q75 = s.quantile(0.75)
            q50 = s.quantile(0.50)
            return s.apply(lambda x: "high" if x >= q75 else ("medium" if x >= q50 else "low"))

        full_grid["stress_tier"] = full_grid.groupby("center_name")["DCR"].transform(_tier)

        out_grid = OUT_SPATIAL / "grid_indicators.gpkg"
        full_grid.to_file(out_grid, driver="GPKG", layer="grid")
        print(f"  Grid indicators → {out_grid.relative_to(DATA_RAW.parent.parent.parent)}")

    print("\n[04] Done.")


if __name__ == "__main__":
    main()
