# CVLZ Supply–Demand Analysis — Python Pipeline

**Project:** URBAN 522 — Sobaih, Tritt, Levin (2026)
**Purpose:** Measure how well Commercial Vehicle Loading Zone (CVLZ) supply matches last-mile delivery demand in Seattle's Regional Centers.

---

## Quick Start

```bash
# 1. Create the conda environment
conda env create -f scripts/00_environment.yml
conda activate cvlz_analysis

# 2. Fetch all raw data (takes 5–15 min depending on network)
python scripts/01_data_fetch.py

# 3. Review outputs/tables/rc_screening.csv and update SELECTED_CENTERS in each script

# 4. Build the demand layer
python scripts/02_demand_layer.py

# 5. Build CVLZ service areas (OSMnx; takes 10–30 min)
python scripts/03_supply_layer.py

# 6. Compute coverage metrics
python scripts/04_coverage_analysis.py

# 7. Run gap analysis and generate maps
python scripts/05_gap_analysis.py
```

---

## Folder Structure

```
cvlz_analysis/
├── scripts/
│   ├── config.py               ← all tunable constants (edit here)
│   ├── 00_environment.yml      ← conda environment
│   ├── 01_data_fetch.py        ← download raw data
│   ├── 02_demand_layer.py      ← build parcel DDI
│   ├── 03_supply_layer.py      ← build CVLZ service areas
│   ├── 04_coverage_analysis.py ← WCR, PCR, DCR, grid
│   └── 05_gap_analysis.py      ← gap parcels, clusters, maps
├── data/
│   ├── raw/                    ← unmodified source downloads
│   └── processed/              ← cleaned, merged spatial layers
└── outputs/
    ├── tables/                 ← CSV summary tables
    ├── spatial/                ← GeoPackage / GeoJSON exports
    └── figures/                ← PNG maps per center
```

---

## Key Outputs

| File | Description |
|------|-------------|
| `outputs/tables/rc_screening.csv` | CVLZ & parcel counts per center → use to select final study areas |
| `outputs/tables/coverage_summary.csv` | WCR, PCR at 50/100/150 m per center |
| `outputs/tables/center_overview.csv` | N parcels, N CVLZ, total DDI, CVLZ density |
| `outputs/tables/gap_summary.csv` | Top 5 gap clusters per center ranked by aggregate Gap Score |
| `outputs/tables/comparative_summary.csv` | All key indicators side-by-side |
| `outputs/spatial/parcels_demand.gpkg` | Parcel polygons with DDI |
| `outputs/spatial/cvlz_service_areas.gpkg` | Individual service area polygons |
| `outputs/spatial/grid_indicators.gpkg` | 100 m grid with DDI, n_CVLZ, DCR |
| `outputs/spatial/gap_clusters.gpkg` | Clustered gap areas (CVLZ siting candidates) |
| `outputs/figures/map_*.png` | Two-panel overview maps per center |

---

## Indicator Formulas

| Indicator | Formula |
|-----------|---------|
| DDI | `W_k × (S_i / norm_k)` |
| WCR | `Σ DDI_covered / Σ DDI_total` |
| PCR | `N_covered / N_total` |
| DCR | `DDI_sum_cell / max(n_CVLZ_cell, 1)` |
| Gap Score | `DDI_i × 1{uncovered at 100 m}` |

See `config.py` for all weight values and threshold definitions.

---

## Data Sources

| Dataset | Source |
|---------|--------|
| CVLZ locations | Seattle Open Data (SDOT Street Signs, `tdmb-n22r`) |
| Parcel polygons | Seattle Open Data (`5bgi-ypbi`) |
| Floor area / units | King County Assessor Data Download |
| Regional Center boundaries | PSRC Growth Centers |
| Walking network | OpenStreetMap via OSMnx |

---

## Tuning Parameters

All tunable parameters live in `config.py`:

- `WALK_THRESHOLDS` — distance thresholds in metres
- `DEMAND_WEIGHTS` — per-category weight, size metric, normalizer
- `KC_USE_MAP` — King County PRESENTUSE code → demand category mapping
- `GRID_CELL_M` — grid cell size for aggregation
- `GAP_CLUSTER_EPS_M` / `GAP_CLUSTER_MIN_SAMPLES` — DBSCAN parameters
- `SELECTED_CENTERS` — update after reviewing `rc_screening.csv`
