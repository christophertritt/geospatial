"""
config.py
─────────────────────────────────────────────────────────────────────────────
Centralised constants for the CVLZ supply–demand analysis.
Edit this file to change thresholds, weights, or paths without touching
any other script.
"""
from pathlib import Path

# ── Project root (parent of scripts/) ────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent

# ── Directory layout ──────────────────────────────────────────────────────────
DATA_RAW       = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
OUT_TABLES     = ROOT / "outputs" / "tables"
OUT_SPATIAL    = ROOT / "outputs" / "spatial"
OUT_FIGURES    = ROOT / "outputs" / "figures"

for d in [DATA_RAW, DATA_PROCESSED, OUT_TABLES, OUT_SPATIAL, OUT_FIGURES]:
    d.mkdir(parents=True, exist_ok=True)

# ── Coordinate reference systems ──────────────────────────────────────────────
CRS_GEO   = "EPSG:4326"   # WGS-84 – used for OSM / API downloads
CRS_PROJ  = "EPSG:2926"   # WA State Plane South (feet) – used for distance ops

# ── Walking distance thresholds (metres) ─────────────────────────────────────
WALK_THRESHOLDS = {
    "strict":   50,
    "moderate": 100,
    "lenient":  150,
}
PRIMARY_THRESHOLD = "moderate"  # 100 m – used for gap analysis and DCR

# ── CVLZ network snap tolerance (metres) ─────────────────────────────────────
SNAP_TOLERANCE_M = 20

# ── Grid cell size for aggregation (metres) ──────────────────────────────────
GRID_CELL_M = 100

# ── OSM network type ─────────────────────────────────────────────────────────
NETWORK_TYPE = "walk"
OSM_BUFFER_M = 200  # buffer around each RC boundary for network download

# ── Demand weights (weekly deliveries per unit / per 1,000 sq ft) ─────────────
# Based on Holguín-Veras et al. (2012) freight trip attraction differentials.
#
# Structure: { seattle_use_code_prefix: (weight, size_metric, normalizer) }
#   size_metric: "gfa_sqft" or "res_units"
#   normalizer : divisor applied to the size metric before multiplying by weight
#
DEMAND_WEIGHTS = {
    # ---------- key: (weight, size_metric, normalizer, display_label) ----------
    "GROCERY":   (5.0, "gfa_sqft",   1000, "Grocery / Supermarket"),
    "RESTAURANT":(3.5, "gfa_sqft",   1000, "Restaurant / Cafe"),
    "RETAIL":    (2.0, "gfa_sqft",   1000, "Retail (General)"),
    "SERVICES":  (1.5, "gfa_sqft",   1000, "Personal Services"),
    "MULTIFAM":  (0.8, "res_units",     1, "Mixed-Use / Multi-Family"),
    "OFFICE":    (0.4, "gfa_sqft",   1000, "Office"),
    "OTHER":     (0.1, "gfa_sqft",   1000, "Other / Institutional"),
}

# ── Seattle land-use code → DEMAND_WEIGHTS key mapping ───────────────────────
# Derived from Seattle LBCS and King County Assessor PRESENTUSE codes.
LAND_USE_MAP = {
    # Grocery / supermarket
    "GROC": "GROCERY", "FOOD": "GROCERY", "SUPER": "GROCERY",
    # Restaurant / cafe
    "REST": "RESTAURANT", "CAFE": "RESTAURANT", "BAR": "RESTAURANT",
    # Retail
    "RETL": "RETAIL", "SHOP": "RETAIL", "COMM": "RETAIL",
    # Personal services
    "SERV": "SERVICES", "HAIR": "SERVICES", "HLTH": "SERVICES",
    # Multi-family / mixed-use residential
    "APRT": "MULTIFAM", "CONDO": "MULTIFAM", "MIXD": "MULTIFAM",
    "MFRES": "MULTIFAM",
    # Office
    "OFFC": "OFFICE", "PROF": "OFFICE",
    # Catch-all
    "INST": "OTHER", "RELIG": "OTHER", "PARK": "OTHER",
}

# ── King County Assessor PRESENTUSE → demand key ─────────────────────────────
# https://info.kingcounty.gov/assessor/esales/Glossary.aspx?type=r
KC_USE_MAP = {
    2:   "MULTIFAM",   # Condominium (residential)
    3:   "MULTIFAM",   # Mobile home park
    4:   "MULTIFAM",   # Multi-family (general)
    6:   "RETAIL",     # Retail store
    8:   "GROCERY",    # Supermarket / grocery
    10:  "RESTAURANT", # Restaurant
    11:  "RESTAURANT", # Fast food
    14:  "RETAIL",     # Shopping center
    16:  "OFFICE",     # Office building
    17:  "OFFICE",     # Medical / dental office
    18:  "SERVICES",   # Service shop
    19:  "RETAIL",     # Department store
    29:  "SERVICES",   # Personal services
    30:  "MULTIFAM",   # Apartment (4+ units)
    34:  "MULTIFAM",   # Condominium complex
    53:  "OTHER",      # Church / religious
    66:  "OTHER",      # Government
}

# ── Gap clustering parameters (DBSCAN) ───────────────────────────────────────
GAP_CLUSTER_EPS_M = 100   # metres
GAP_CLUSTER_MIN_SAMPLES = 3

# ── Regional Center selection thresholds ─────────────────────────────────────
MIN_CVLZ_COUNT      = 3
MIN_DEMAND_PARCELS  = 50
MIN_CATEGORIES      = 2      # number of distinct demand categories
DATA_COMPLETENESS   = 0.90   # share of parcels with GFA or units

# ── Seattle Open Data / Socrata endpoints ────────────────────────────────────
SOCRATA_DOMAIN = "data.seattle.gov"
CVLZ_DATASET   = "tdmb-n22r"   # legacy Socrata ID (deprecated)
# Current SDOT Street Signs feature service (ArcGIS Online)
CVLZ_FEATURE_SERVICE = (
    "https://services.arcgis.com/ZOyb2t4B0UYuYNYH/arcgis/rest/services/"
    "SDOT_Street_Signs/FeatureServer/1"
)
CVLZ_CATEGORY_CODE = "PCVL"  # commercial vehicle load zones
PARCEL_DATASET = "https://data.seattle.gov/resource/5bgi-ypbi.geojson"

# ── PSRC centers GeoJSON ──────────────────────────────────────────────────────
PSRC_CENTERS_URL = (
    "https://services6.arcgis.com/GWxg6t7KXELn1thE/arcgis/rest/services/"
    "Regional_Growth_Centers/FeatureServer/0/query"
    "?where=1%3D1&outFields=*&f=geojson"
)
