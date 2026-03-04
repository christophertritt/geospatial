"""
05_gap_analysis.py
─────────────────────────────────────────────────────────────────────────────
Identifies spatial gaps in CVLZ coverage, clusters them into actionable
"gap areas," and produces the final comparison summary across centers.

Inputs (data/processed/):
  parcels_demand_covered.gpkg

Outputs (outputs/spatial/):
  gap_parcels.gpkg       — uncovered parcels with Gap Score (GS)
  gap_clusters.gpkg      — DBSCAN-clustered gap areas (aggregate GS, centroid)

Outputs (outputs/tables/):
  gap_summary.csv        — top gap clusters per center ranked by aggregate GS
  comparative_summary.csv — all key indicators side-by-side across centers

Outputs (outputs/figures/):
  map_{center_name}.png  — one overview map per Regional Center

Gap Score (GS)
─────────────────────────────────────────────────────────────────────────────
  GS_i = DDI_i   if parcel i is NOT covered at PRIMARY_THRESHOLD
          0       otherwise

DBSCAN clustering
─────────────────────────────────────────────────────────────────────────────
  eps        = GAP_CLUSTER_EPS_M  (100 m, in projected CRS units)
  min_samples = GAP_CLUSTER_MIN_SAMPLES  (3 parcels)
  noise points (label = -1) are retained as single-parcel "micro-gaps"
"""

import sys
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path
from sklearn.cluster import DBSCAN
from shapely.geometry import MultiPoint
from shapely.ops import unary_union

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    DATA_RAW, DATA_PROCESSED, OUT_TABLES, OUT_SPATIAL, OUT_FIGURES,
    CRS_GEO, CRS_PROJ,
    PRIMARY_THRESHOLD, GAP_CLUSTER_EPS_M, GAP_CLUSTER_MIN_SAMPLES,
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

def cluster_gaps(gap_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Run DBSCAN on gap parcel centroids and return a GeoDataFrame of clusters
    with aggregate statistics.
    """
    if gap_gdf.empty:
        return gpd.GeoDataFrame()

    coords = np.column_stack([gap_gdf.geometry.centroid.x,
                               gap_gdf.geometry.centroid.y])
    labels = DBSCAN(
        eps=GAP_CLUSTER_EPS_M,
        min_samples=GAP_CLUSTER_MIN_SAMPLES,
        algorithm="ball_tree",
        metric="euclidean",
    ).fit_predict(coords)

    gap_gdf = gap_gdf.copy()
    gap_gdf["cluster_id"] = labels

    clusters = []
    for cid in sorted(gap_gdf["cluster_id"].unique()):
        sub = gap_gdf[gap_gdf["cluster_id"] == cid]
        centroids = MultiPoint(list(zip(sub.geometry.centroid.x,
                                        sub.geometry.centroid.y)))
        clusters.append({
            "center_name":   sub["center_name"].iloc[0],
            "cluster_id":    int(cid),
            "n_parcels":     len(sub),
            "total_GS":      round(sub["GS"].sum(), 2),
            "mean_DDI":      round(sub["DDI"].mean(), 2),
            "max_DDI":       round(sub["DDI"].max(), 2),
            "top_use":       sub.groupby("demand_cat")["GS"].sum().idxmax(),
            "is_noise":      (cid == -1),
            "geometry":      centroids.convex_hull if len(sub) >= 3 else centroids.centroid,
        })

    return gpd.GeoDataFrame(clusters, crs=CRS_PROJ)


def plot_center(center_name: str,
                demand: gpd.GeoDataFrame,
                union: gpd.GeoDataFrame,
                gap_parcels: gpd.GeoDataFrame,
                gap_clusters: gpd.GeoDataFrame,
                cvlz: gpd.GeoDataFrame,
                rc_boundary: gpd.GeoDataFrame) -> None:
    """Produce a 2-panel map for a single Regional Center."""
    try:
        import contextily as ctx
        USE_BASEMAP = True
    except ImportError:
        USE_BASEMAP = False

    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    fig.suptitle(f"{center_name} — CVLZ Coverage & Gap Analysis",
                 fontsize=14, fontweight="bold")

    # ── Left panel: demand + coverage ──────────────────────────────────────
    ax = axes[0]
    ax.set_title("Demand Parcels & CVLZ Service Area (100 m)")

    rc_boundary.boundary.plot(ax=ax, color="black", linewidth=1.5, zorder=3)

    # Service area footprint
    if not union.empty:
        union.plot(ax=ax, color="steelblue", alpha=0.25, zorder=1,
                   label="CVLZ service area (100 m)")

    # Demand parcels coloured by DDI
    if not demand.empty:
        demand.plot(ax=ax, column="DDI", cmap="YlOrRd", alpha=0.7,
                    legend=True,
                    legend_kwds={"label": "DDI", "shrink": 0.5},
                    zorder=2)

    # CVLZ points
    if not cvlz.empty:
        cvlz.plot(ax=ax, color="navy", markersize=15, marker="^", zorder=4,
                  label="CVLZ")

    if USE_BASEMAP:
        try:
            ctx.add_basemap(ax, crs=demand.crs.to_string(), source=ctx.providers.CartoDB.Positron)
        except Exception:
            pass

    ax.legend(fontsize=8)
    ax.set_axis_off()

    # ── Right panel: gap areas + DCR heatmap ───────────────────────────────
    ax = axes[1]
    ax.set_title("Gap Parcels & Gap Clusters")

    rc_boundary.boundary.plot(ax=ax, color="black", linewidth=1.5, zorder=4)

    if not union.empty:
        union.plot(ax=ax, color="steelblue", alpha=0.15, zorder=1)

    if not gap_parcels.empty:
        gap_parcels.plot(ax=ax, color="firebrick", alpha=0.6, markersize=4,
                         marker="o", zorder=2, label="Gap parcel")

    if not gap_clusters.empty:
        gap_clusters[~gap_clusters["is_noise"]].plot(
            ax=ax, facecolor="none", edgecolor="red", linewidth=2,
            zorder=3, label="Gap cluster")

    if not cvlz.empty:
        cvlz.plot(ax=ax, color="navy", markersize=15, marker="^", zorder=5)

    if USE_BASEMAP:
        try:
            ctx.add_basemap(ax, crs=demand.crs.to_string(), source=ctx.providers.CartoDB.Positron)
        except Exception:
            pass

    legend_elements = [
        mpatches.Patch(facecolor="steelblue", alpha=0.3, label="CVLZ service area"),
        mpatches.Patch(facecolor="firebrick", alpha=0.7, label="Gap parcel"),
        mpatches.Patch(facecolor="none", edgecolor="red", linewidth=2, label="Gap cluster"),
    ]
    ax.legend(handles=legend_elements, fontsize=8)
    ax.set_axis_off()

    plt.tight_layout()
    fname = OUT_FIGURES / f"map_{center_name.lower().replace(' ','_')}.png"
    plt.savefig(fname, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Map saved → {fname.relative_to(DATA_RAW.parent.parent.parent)}")


# ─────────────────────────────────────────────────────────────────────────────
# main pipeline
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── Load data ─────────────────────────────────────────────────────────────
    print("[05] Loading covered parcel layer …")
    demand = gpd.read_file(DATA_PROCESSED / "parcels_demand_covered.gpkg",
                           layer="demand_parcels")
    demand = demand.to_crs(CRS_PROJ)

    gap_col = f"gap_{PRIMARY_THRESHOLD}"
    if gap_col not in demand.columns:
        raise ValueError(f"Column '{gap_col}' not found. Run 04_coverage_analysis.py first.")

    print("[05] Loading CVLZ union service areas …")
    union_all = gpd.read_file(DATA_PROCESSED / "cvlz_union.gpkg", layer="union")
    union_all = union_all.to_crs(CRS_PROJ)

    print("[05] Loading Regional Centers …")
    centers = gpd.read_file(DATA_RAW / "regional_centers_raw.gpkg", layer="regional_centers")
    centers = centers.to_crs(CRS_PROJ)
    name_col = next(c for c in centers.columns if "name" in c.lower())
    centers  = centers[centers[name_col].isin(SELECTED_CENTERS)].copy()
    centers  = centers.rename(columns={name_col: "center_name"})

    print("[05] Loading CVLZ points …")
    cvlz = gpd.read_file(DATA_RAW / "cvlz_signs_raw.gpkg", layer="cvlz")
    cvlz = cvlz.to_crs(CRS_PROJ)

    # ── Compute Gap Score ─────────────────────────────────────────────────────
    demand["GS"] = demand.apply(
        lambda r: r["DDI"] if r[gap_col] else 0.0, axis=1
    )

    gap_parcels = demand[demand["GS"] > 0].copy()
    print(f"  {len(gap_parcels):,} gap parcels (GS > 0) at {PRIMARY_THRESHOLD} threshold")

    # Convert parcel polygons to centroids for plotting
    gap_centroids = gap_parcels.copy()
    gap_centroids["geometry"] = gap_centroids.geometry.centroid

    # ── DBSCAN clustering per center ──────────────────────────────────────────
    print("[05] Clustering gap parcels …")
    all_clusters = []
    all_gap_parcels = []

    for center in SELECTED_CENTERS:
        sub_gaps = gap_centroids[gap_centroids["center_name"] == center].copy()
        print(f"  {center}: {len(sub_gaps)} gap parcels")
        if sub_gaps.empty:
            continue

        clusters = cluster_gaps(sub_gaps)
        clusters["center_name"] = center
        all_clusters.append(clusters)
        all_gap_parcels.append(sub_gaps)

        # ── Maps ──────────────────────────────────────────────────────────────
        rc_boundary = centers[centers["center_name"] == center]
        union_center = union_all[
            (union_all["center_name"] == center) &
            (union_all["threshold"] == PRIMARY_THRESHOLD)
        ]
        demand_center = demand[demand["center_name"] == center]
        cvlz_center   = gpd.sjoin(cvlz, rc_boundary[["geometry"]], how="inner",
                                   predicate="within").drop(columns=["index_right"], errors="ignore")
        plot_center(
            center_name  = center,
            demand       = demand_center,
            union        = union_center,
            gap_parcels  = sub_gaps,
            gap_clusters = clusters if not clusters.empty else gpd.GeoDataFrame(),
            cvlz         = cvlz_center,
            rc_boundary  = rc_boundary,
        )

    # ── Save gap parcel and cluster layers ────────────────────────────────────
    if all_gap_parcels:
        gdf_gaps = pd.concat(all_gap_parcels, ignore_index=True)
        gdf_gaps = gpd.GeoDataFrame(gdf_gaps, crs=CRS_PROJ)
        out_gp = OUT_SPATIAL / "gap_parcels.gpkg"
        gdf_gaps.to_file(out_gp, driver="GPKG", layer="gap_parcels")
        print(f"\n  Gap parcels → {out_gp.relative_to(DATA_RAW.parent.parent.parent)}")

    if all_clusters:
        gdf_clusters = pd.concat(all_clusters, ignore_index=True)
        gdf_clusters = gpd.GeoDataFrame(gdf_clusters, crs=CRS_PROJ)
        out_gc = OUT_SPATIAL / "gap_clusters.gpkg"
        gdf_clusters.to_file(out_gc, driver="GPKG", layer="gap_clusters")
        print(f"  Gap clusters → {out_gc.relative_to(DATA_RAW.parent.parent.parent)}")

        # ── Gap summary table ─────────────────────────────────────────────────
        top_n = (
            gdf_clusters[~gdf_clusters["is_noise"]]
            .sort_values(["center_name","total_GS"], ascending=[True, False])
            .groupby("center_name")
            .head(5)
        )
        out_gs = OUT_TABLES / "gap_summary.csv"
        top_n.drop(columns=["geometry"]).to_csv(out_gs, index=False)
        print(f"  Gap summary  → {out_gs.relative_to(DATA_RAW.parent.parent.parent)}")

    # ── Comparative summary ───────────────────────────────────────────────────
    print("\n[05] Building comparative summary …")
    cov_df  = pd.read_csv(OUT_TABLES / "coverage_summary.csv")
    over_df = pd.read_csv(OUT_TABLES / "center_overview.csv")

    primary = cov_df[cov_df["threshold"] == PRIMARY_THRESHOLD][
        ["center_name","WCR","PCR","n_uncovered"]
    ]
    if all_clusters:
        gap_agg = (
            gdf_clusters[~gdf_clusters["is_noise"]]
            .groupby("center_name")
            .agg(n_gap_clusters=("cluster_id","count"), total_gap_score=("total_GS","sum"))
            .reset_index()
        )
        comp = over_df.merge(primary, on="center_name", how="left")\
                      .merge(gap_agg, on="center_name", how="left")
    else:
        comp = over_df.merge(primary, on="center_name", how="left")

    comp = comp.sort_values("total_DDI", ascending=False)
    out_comp = OUT_TABLES / "comparative_summary.csv"
    comp.to_csv(out_comp, index=False)
    print(f"\n  Comparative summary → {out_comp.relative_to(DATA_RAW.parent.parent.parent)}")
    print("\n" + comp.to_string(index=False))

    # ── Plain-language bullets ────────────────────────────────────────────────
    print("\n" + "─"*70)
    print("POLICY BULLETS (draft — update with actual numbers)")
    print("─"*70)
    for _, row in comp.iterrows():
        wcr = row.get("WCR", np.nan)
        dcr_flag = "HIGH STRESS" if not np.isnan(wcr) and wcr < 0.6 else "moderate stress"
        nc  = int(row.get("n_gap_clusters", 0) or 0)
        print(f"\n{row['center_name']}:")
        print(f"  • {round(wcr*100,1) if not np.isnan(wcr) else '?'}% of weighted demand is within "
              f"100 m of a CVLZ ({dcr_flag}).")
        if nc:
            print(f"  • {nc} spatial gap cluster(s) identified; adding a CVLZ in the top cluster "
                  f"would reduce unmet demand by ~{round(row.get('total_gap_score',0)/max(nc,1),1)} DDI units.")
        print(f"  • CVLZ density is {row.get('cvlz_density','?')} loading zones per km²; "
              f"citywide target guidance recommends ≥ 4–6 per km² in mixed-use centers.")

    print("\n[05] Done.")


if __name__ == "__main__":
    main()
