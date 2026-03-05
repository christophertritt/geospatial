# Seattle CVLZ (Commercial Vehicle Loading Zone) Effectiveness Analysis

## Overview
This analysis evaluates the effectiveness of Commercial Vehicle Loading Zones across Seattle's PSRC Urban Centers using a comprehensive 4-component scoring model. The framework is designed to support SDOT decision-making and is ready for integration with live CVLZ data.

**Data Status:** Modeled/Synthetic CVLZ locations — framework ready for SDOT live data integration

## Key Findings

### Tier Distribution
- **High Effectiveness (≥75 pts):** 110 zones (70.5%)
- **Medium Effectiveness (55-74 pts):** 44 zones (28.2%)
- **Low Effectiveness (<55 pts):** 2 zones (1.3%)

### Score Statistics
- **Mean Score:** 79.99
- **Median Score:** 81.20
- **Std Deviation:** 10.78
- **Range:** 48.5 - 98.6

### Street Type Distribution
- **Arterial Streets:** 82 zones (52.6%)
- **Collector Streets:** 47 zones (30.1%)
- **Local Streets:** 27 zones (17.3%)

### Correlation Analysis
- **Demand vs Effectiveness Correlation:** r = 0.696 (p-value: 0.000000)
- Interpretation: Moderate positive relationship between commercial demand and CVLZ effectiveness

## Urban Centers Analyzed
1. **Downtown Seattle** - Core business district
2. **South Lake Union** - Tech hub and mixed-use development
3. **Capitol Hill / First Hill** - Vibrant commercial corridor
4. **University District** - Education and retail nexus
5. **Uptown / Queen Anne** - Mixed residential-commercial
6. **Northgate** - Regional urban center

Note: Wallingford and Rainier Beach are PSRC Urban Villages (not Urban Centers) and were removed from this analysis.

## Scoring Methodology

### Component 1: Commercial Demand Score (0-40 points)
- Counts businesses within 100m buffer of CVLZ location
- Weighted by business type:
  - Food service: 1.5x multiplier
  - Retail: 1.2x multiplier
  - Office: 1.0x multiplier
  - Mixed: 0.95x multiplier
- Reflects commercial intensity and freight demand

### Component 2: Network Node Importance (0-25 points)
- Arterial street: 20 base points
- Collector street: 12 base points
- Local street: 5 base points
- Intersection proximity bonus: +0-5 points
- Reflects network significance for freight movement

### Component 3: Turnover Efficiency Score (0-20 points)
**[RENAMED FROM "Accessibility Score" - Fixed Bug 1]**

Measures CVLZ effectiveness based on time limit adequacy, following NACTO Curb Appeal and FHWA freight research standards:

- **≤20 minutes (optimal turnover):** 18-20 points
  - Ideal for high-frequency deliveries
  - Maximizes vehicle-miles served per zone
- **21-30 minutes (good turnover):** 15-19 points
  - Adequate for most food service
- **31-45 minutes (acceptable):** 9-13 points
  - Supports mid-range delivery types
- **60+ minutes (poor turnover):** 4-8 points
  - Low effectiveness for on-street CVLZs
  - Creates congestion without proportional benefits

**Key Principle:** Shorter time limits = higher turnover = more effective. This reflects urban freight efficiency literature showing that CVLZ effectiveness is inversely correlated with time limit for on-street zones.

### Component 4: Temporal Coverage (0-15 points)
- 24-hour operation: 15 points
- Early morning (6-7am start): +3 points
- Peak delivery window alignment: +2 points
- Reflects availability during primary delivery periods

### Effectiveness Tier Classification
- **High (≥75 pts):** Highly effective zones - prioritize for expansion/protection
- **Medium (55-74 pts):** Moderately effective zones - standard management
- **Low (<55 pts):** Underperforming zones - evaluate for redesign

## Data Sources
- **Urban Centers:** PSRC (Puget Sound Regional Council) Vision 2050 official designation
- **Business Licenses:** Seattle Open Data Portal
- **Street Networks:** OpenStreetMap / SDOT reference data
- **CVLZ Locations:** Synthetic (modeled from real street patterns)

## Validation & Corrections Applied

### Bug 1: Accessibility Score Was Backwards
**Fixed:** Renamed to "Turnover Efficiency Score" and reversed scoring logic.
- Original implementation scored long time limits (60 min) at maximum (20 pts)
- Per NACTO and FHWA standards, this was backwards
- Corrected: short limits now score high, long limits score low
- Reflects turnover adequacy for on-street commercial loading effectiveness

### Bug 2: Low Tier Was Structurally Unreachable
**Fixed:** Introduced street type variation and adjusted thresholds.
- Original: all streets classified as arterial → minimum score ~50 → Low tier (<40) impossible
- Corrected: 60% arterial, 30% collector, 10% local streets
- New thresholds: High ≥75, Medium 55-74, Low <55
- Now all three tiers are genuinely reachable

### Bug 3: Business Type Weighting Not Implemented
**Fixed:** Added business_mix attribute and implemented multipliers.
- Original: methodology stated weighting but code used random fallback
- Corrected: each CVLZ assigned business_mix (food_heavy, retail_heavy, office_heavy, mixed)
- Multipliers applied: food_heavy 1.5x, retail_heavy 1.2x, office_heavy 1.0x, mixed 0.95x

### Bug 4: Two Invalid Urban Centers
**Fixed:** Removed Wallingford and Rainier Beach from Urban Centers list.
- These are officially PSRC Urban Villages, not Urban Centers
- Corrected list: 6 official PSRC Urban Centers only
- Wallingford and Rainier Beach can be analyzed separately as villages if needed

### Bug 5: Duplicate Street Coordinates
**Fixed:** Corrected "University St" to "Roosevelt Way NE" with proper coordinates.
- Original: "45th St NE" and "University St" had identical coordinates
- Result: overlapping spatial features
- Corrected: "Roosevelt Way NE" now at (-122.3200, 47.6550) to (-122.3200, 47.6750)

### Bug 6: All Streets Classified as Arterial
**Fixed:** Implemented mixed street type distribution.
- Original: every street tagged as arterial → network_score had only 4.9-point spread
- Corrected: arterial, collector, local classification with realistic proportions
- Result: network_score now spans full 0-25 range with meaningful variation

### Bug 7: Correlation Coefficient Mismatch
**Fixed:** Updated correlation value in documentation.
- Original README stated r=0.82
- Actual computed correlation: r=0.696
- Now reflects true demand-effectiveness relationship

### Bug 8: Spatial Duplicates Within 10m
**Fixed:** Implemented deduplication algorithm.
- Original: CVLZ-0076 & CVLZ-0080 within 10m on Northgate Ave N
- Original: CVLZ-0022 & CVLZ-0024 within 10m on Pine St
- Corrected: removed 3 spatially redundant CVLZs
- Threshold: any pair within 15m deduplicated by removing higher-indexed CVLZ

## Outputs Generated
1. **GeoJSON:** `cvlz_scored.geojson` - Full CVLZ dataset with scores and tier assignments
2. **Excel:** `CVLZ_Analysis.xlsx` - Multi-sheet workbook with summary and methodology
3. **Maps:** 
   - Overview map (all CVLZs by effectiveness)
   - Downtown detail map
   - South Lake Union detail map
4. **Charts:**
   - Score distribution histogram
   - Component scores by urban center
   - Tier distribution pie chart
   - Time limit vs turnover efficiency scatter plot
   - Street type analysis
5. **Dashboard:** `CVLZ_Dashboard.html` - Interactive summary with all statistics

## Usage Notes
- All visualizations include annotation: "Modeled/Synthetic CVLZ locations — framework ready for SDOT live data integration"
- Reproduce with `np.random.seed(42)` for deterministic synthetic data
- Street type variation ensures realistic variance in effectiveness scores
- Business type weighting reflects actual delivery demand patterns
- Correlation r=0.696 indicates moderate commercial demand signal in effectiveness

## Recommendations
1. **Expand High-Tier Zones:** Prioritize High effectiveness zones (110 zones) for protection and capacity increases
2. **Optimize Time Limits:** Focus on zones with 60+ minute limits for reduction to 20-30 minutes where feasible
3. **Street Type Strategy:** Collector and local streets show underutilization potential - evaluate for targeted CVLZs
4. **Urban Center Focus:** Downtown Seattle shows highest average effectiveness (85.5 pts)

## Document Control
- **Version:** FIXED (all 8 bugs addressed)
- **Analysis Date:** 2026-03-04
- **Last Updated:** 2026-03-04
- **Status:** Ready for SDOT live data integration

---

**For questions or integration requests, contact SDOT Analysis Team**
