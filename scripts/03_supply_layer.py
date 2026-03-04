"""
03_supply_layer.py
─────────────────────────────────────────────────────────────────────────────
Builds pedestrian network-based service areas around each CVLZ for all three
walking distance thresholds (50 m, 100 m, 150 m).

Inputs:
  data/raw/cvlz_signs_raw.gpkg
  data/raw/regional_centers_raw.gpkg

Outputs (data/processed/):
  cvlz_service_areas.gpkg  — polygon footprints per CVLZ, per threshold, per center
  cvlz_union.gpkg          — unioned supply footprint per center per threshold

Algorithm
─────────────────────────────────────────────────────────────────────────────
  For each selected Regional Center:
    1. Download the OSMnx walk network (center boundary + OSM_BUFFER_M buffer).
    2. Clip CVLZ points to center boundary.
    3. Snap each CVLZ to nearest network node (within SNAP_TOLERANCE_M).
    4. For each threshold d:
         a. Run ego_graph(G, node, radius=d, distance='length') to get subgraph.
         b. Extract node coordinates → convex hull (service area polygon).
       (For very small graphs, alpha-shape falls back to convex hull.)
    5. Union individual service areas per threshold.
    6. Save all layers.
"""

import sys
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import MultiPoint
from shapely.ops import unary_union

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    DATA_RAW, DATA_PROCESSED,
    CRS_GEO, CRS_PROJ,
    WALK_THRESHOLDS, SNAP_TOLERANCE_M, OSM_BUFFER_M, NETWORK_TYPE,
)

warnings.filterwarnings("ignore")

try:
    import osmnx as ox
    import networkx as nx
    ox.settings.log_console = False
    ox.settings.use_cache   = True
    ox.settings.cache_folder = str(DATA_RAW / "osm_cache")
except ImportError:
    raise ImportError("osmnx is required. Install with: conda install -c conda-forge osmnx")

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

def _nodes_to_polygon(G_sub, G_full, default_radius_m: float):
    """
    Convert the nodes of a network subgraph to a service-area polygon.
    Uses convex hull of node coordinates; falls back to a circle if < 3 nodes.
    """
    nodes = list(G_sub.nodes)
    if len(nodes) < 3:
        # Too few nodes – use a buffer circle around the origin node
        n_data = G_full.nodes[nodes[0]]
        pt = gpd.GeoSeries(
            [gpd.points_from_xy([n_data["x"]], [n_data["y"]])[0]],
            crs=CRS_GEO
        ).to_crs(CRS_PROJ).iloc[0]
        return pt.buffer(default_radius_m)

    coords = [(G_full.nodes[n]["x"], G_full.nodes[n]["y"]) for n in nodes]
    mp = MultiPoint(coords)
    hull = mp.convex_hull
    # Convert from lon/lat to projected CRS
    hull_gdf = gpd.GeoSeries([hull], crs=CRS_GEO).to_crs(CRS_PROJ)
    return hull_gdf.iloc[0]


def snap_to_network(G, lon: float, lat: float, tolerance_m: float):
    """
    Find the nearest network node to (lon, lat).
    Returns node_id or None if the nearest node is farther than tolerance_m.
    """
    node_id, dist = ox.distance.nearest_nodes(G, lon, lat, return_dist=True)
    if dist > tolerance_m:
        return None
    return node_id


# ─────────────────────────────────────────────────────────────────────────────
# main pipeline
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── Load inputs ───────────────────────────────────────────────────────────
    print("[03] Loading CVLZ signs …")
    cvlz = gpd.read_file(DATA_RAW / "cvlz_signs_raw.gpkg", layer="cvlz")
    cvlz = cvlz.to_crs(CRS_GEO)

    print("[03] Loading Regional Center boundaries …")
    centers = gpd.read_file(DATA_RAW / "regional_centers_raw.gpkg", layer="regional_centers")
    centers = centers.to_crs(CRS_GEO)
    name_col = next(c for c in centers.columns if "name" in c.lower())
    centers  = centers[centers[name_col].isin(SELECTED_CENTERS)].copy()

    all_service_areas = []  # list of dicts → GeoDataFrame
    all_unions        = []  # list of dicts → GeoDataFrame

    for _, rc in centers.iterrows():
        center_name = rc[name_col]
        rc_geom_geo = rc.geometry
        rc_geom_prj = gpd.GeoSeries([rc_geom_geo], crs=CRS_GEO).to_crs(CRS_PROJ).iloc[0]

        print(f"\n[03] ── {center_name} ──")

        # ── 1  Download OSM walking network ────────────────────────────────
        print(f"  Downloading OSM walk network (buffer={OSM_BUFFER_M} m) …")
        rc_buffered = gpd.GeoSeries([rc_geom_prj], crs=CRS_PROJ)\
                        .buffer(OSM_BUFFER_M).to_crs(CRS_GEO).iloc[0]
        try:
            G = ox.graph_from_polygon(rc_buffered, network_type=NETWORK_TYPE)
            G = ox.add_edge_lengths(G)
            print(f"  Graph: {len(G.nodes):,} nodes, {len(G.edges):,} edges")
        except Exception as e:
            print(f"  ⚠  OSMnx failed for {center_name}: {e}. Skipping.")
            continue

        # ── 2  Clip CVLZs to center ─────────────────────────────────────────
        cvlz_rc = cvlz[cvlz.within(rc_geom_geo)].copy()
        print(f"  {len(cvlz_rc)} CVLZs in boundary")
        if cvlz_rc.empty:
            print(f"  ⚠  No CVLZs found in {center_name}. Skipping.")
            continue

        # ── 3  Snap CVLZs → network nodes ───────────────────────────────────
        cvlz_rc["node_id"] = cvlz_rc.apply(
            lambda r: snap_to_network(G, r.geometry.x, r.geometry.y, SNAP_TOLERANCE_M),
            axis=1,
        )
        snapped = cvlz_rc.dropna(subset=["node_id"])
        n_failed = len(cvlz_rc) - len(snapped)
        if n_failed:
            print(f"  ⚠  {n_failed} CVLZ(s) could not snap within {SNAP_TOLERANCE_M} m – excluded")

        # ── 4  Build service areas per threshold ────────────────────────────
        for thresh_name, dist_m in WALK_THRESHOLDS.items():
            print(f"  Building service areas [{thresh_name}: {dist_m} m] …", end=" ")
            polygons_this_thresh = []

            for _, cvlz_row in snapped.iterrows():
                node = int(cvlz_row["node_id"])
                try:
                    subgraph = nx.ego_graph(G, node, radius=dist_m, distance="length")
                    poly = _nodes_to_polygon(subgraph, G, dist_m)
                    all_service_areas.append({
                        "center_name":  center_name,
                        "threshold":    thresh_name,
                        "dist_m":       dist_m,
                        "cvlz_idx":     cvlz_row.name,
                        "geometry":     poly,
                    })
                    polygons_this_thresh.append(poly)
                except Exception:
                    pass

            # ── 5  Union per threshold ───────────────────────────────────────
            if polygons_this_thresh:
                union_poly = unary_union(polygons_this_thresh)
                all_unions.append({
                    "center_name": center_name,
                    "threshold":   thresh_name,
                    "dist_m":      dist_m,
                    "n_cvlz":      len(polygons_this_thresh),
                    "geometry":    union_poly,
                })
                print(f"{len(polygons_this_thresh)} areas → unioned ✓")
            else:
                print("no valid areas")

    # ── 6  Save outputs ────────────────────────────────────────────────────────
    out_sa  = DATA_PROCESSED / "cvlz_service_areas.gpkg"
    out_uni = DATA_PROCESSED / "cvlz_union.gpkg"

    if all_service_areas:
        sa_gdf = gpd.GeoDataFrame(all_service_areas, crs=CRS_PROJ)
        sa_gdf.to_file(out_sa, driver="GPKG", layer="service_areas")
        print(f"\n  Individual service areas → {out_sa.relative_to(DATA_RAW.parent.parent)}")
    else:
        print("\n  ⚠  No service areas generated.")

    if all_unions:
        un_gdf = gpd.GeoDataFrame(all_unions, crs=CRS_PROJ)
        un_gdf.to_file(out_uni, driver="GPKG", layer="union")
        print(f"  Unioned footprints       → {out_uni.relative_to(DATA_RAW.parent.parent)}")

    print("\n[03] Done.")


if __name__ == "__main__":
    main()
