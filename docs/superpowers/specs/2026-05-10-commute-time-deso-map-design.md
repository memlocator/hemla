# Commute-Time DeSO Map Design

## Context

Hemla helps compare Stockholm-area places to live using price, commute, safety, and other signals. The current map already supports arbitrary destinations and computes commute minutes per DeSO area, but the visual presentation is cluttered: many markers and a blurred point heat layer make it hard to read travel-time geography.

The goal is to make transit time from a selected destination the primary map reading, so a user can quickly understand where living is practical for a commute.

## Decision

Use DeSO polygons as the primary commute-time visualization.

Each DeSO polygon is colored by the computed transit commute time from that DeSO area to the selected destination. The map should not draw circular buffers around the destination. Transit accessibility is network-shaped, so the geometry should come from per-area commute results joined to real DeSO boundaries.

## User Experience

The default map mode is `Commute time`.

The main map layer is a choropleth of DeSO polygons using commute-time buckets:

- `0-20 min`
- `20-35 min`
- `35-50 min`
- `50-70 min`
- `70+ min`
- `N/A`

The selected max-commute setting remains a key control. Areas within the threshold should be visually emphasized; areas outside the threshold should remain visible but muted enough that reachable places are easy to scan.

Markers become secondary. The default view should avoid showing every area marker at once. It may show only the destination marker, the selected area, and optionally top-ranked areas at higher zoom levels. Polygon click and hover replace marker hover as the main area interaction.

Clicking a polygon selects that area and reuses the existing selected-area panel. Hovering a polygon shows a compact tooltip:

- Area name
- Municipality
- Commute time to destination
- Price per sqm
- Fit score

The map layer switcher should stay small. Initial modes:

- `Commute time`
- `Fit`
- `Price`

Other metrics can stay out of the primary map control until they have a clear use in the home-search workflow.

## Data Model

The existing DeSO GeoJSON at `backend/data/raw/deso_2025_stockholm.geojson` contains the polygon boundaries. Its feature key is `properties.desokod`, for example `0186C1020`.

The existing API and CSV use area IDs in the form `deso_0186c1020`.

The frontend join key should normalize GeoJSON features as:

```text
deso_ + lower(properties.desokod)
```

That normalized key should match `area.id` from `/api/areas`.

Commute time should come from `item.breakdown.commute_minutes`, because that value already reflects the selected destination. If unavailable, fall back to `area.metrics.sl_commute_to_tcentralen_min` only for legacy/default T-Centralen views where that fallback is explicitly acceptable. Otherwise show the polygon as `N/A`.

## Architecture

The first implementation can serve the static GeoJSON from the frontend or backend as long as it avoids loading the larger GeoPackage in the browser. The existing 4.4 MB Stockholm GeoJSON is acceptable for a first pass, but it should be loaded once and reused across map renders.

Frontend responsibilities:

- Load DeSO GeoJSON once.
- Build a map from normalized DeSO ID to scored area result.
- Render a Leaflet GeoJSON layer for polygons.
- Style polygons based on the active map metric.
- Handle polygon hover, click, selected state, and threshold emphasis.
- Remove or demote the existing blurred point heat layer for commute mode.

Backend responsibilities:

- Continue returning commute minutes for the selected destination through `/api/areas`.
- Optionally expose the DeSO GeoJSON through a stable endpoint if direct static serving is not convenient.

## Visual Rules

The commute map should prioritize clarity over density.

Use a restrained sequential palette where shorter commute is more prominent and longer commute is quieter. Keep missing data visually distinct but low-emphasis.

Polygon boundaries should be thin and subtle. The selected polygon can use a stronger outline. Hover should raise outline contrast without changing layout or adding large UI elements.

The destination marker should be visually distinct from residential areas. It should remain visible even when area markers are hidden.

Avoid decorative heat blobs, circular travel rings, and dense always-on marker icons in commute mode.

## Error Handling

If the GeoJSON fails to load, the app should still show the existing area list and a non-blocking map error state.

If a polygon has no matching area result, render it as unavailable or omit it from the active layer.

If an area result has no polygon, it can remain visible in the list but should not break map rendering.

If the selected destination has no transit result for many areas, show `N/A` polygons clearly and keep the selected destination visible so the user understands the query context.

## Testing

Manual verification should cover:

- Default load renders DeSO polygons.
- Changing destination updates commute colors.
- Changing max commute changes in-range emphasis.
- Clicking a polygon selects the matching area.
- Hover tooltip shows commute, price, and fit.
- Toggling map mode between commute, fit, and price changes polygon styling.
- Mobile layout remains readable without marker clutter.

Automated tests should cover the DeSO ID normalization and bucket assignment logic if those helpers are extracted.

## Deferred Work

Do not split DeSO polygons in the first version.

A later version may add a smoothed commute surface as an optional secondary layer, but it must be clearly treated as interpolation rather than exact source geometry.

Do not add route-line visualization in this design. The first need is area search by commute-time geography, not trip itinerary inspection.
