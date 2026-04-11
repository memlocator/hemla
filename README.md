# Hemla

App to identify good-value areas to live in around Stockholm using:
- Lantmäteriet context
- SL transit metrics
- Additional free/public indicators with optional live enrichment

Stack:
- Frontend: Svelte + Tailwind + Leaflet (with heatmap)
- Backend: FastAPI
- Runtime: Docker Compose (hot reload in dev)

## Startup

Start the full stack with Docker Compose:

```bash
docker compose up --build
```

Run in the background:

```bash
docker compose up --build -d
```

Stop the stack:

```bash
docker compose down
```

Services:
- Frontend: `http://127.0.0.1:5173`
- Backend API docs: `http://127.0.0.1:8000/docs`
- Backend health check: `http://127.0.0.1:8000/health`

Hot reload behavior:
- Backend reloads automatically on changes in `backend/app`
- Frontend reloads automatically on changes in `frontend/src`

## Map Features

- Interactive OpenStreetMap base map
- Clickable area markers
- Heatmap overlay with selectable factor:
  - Price/sqm
  - Value score
  - Commute time
  - PM2.5
  - Flood risk
  - Livability

## API Endpoints

- `GET /health`
- `GET /api/data_mode`
- `GET /api/sources`
- `GET /api/areas?budget_sek_per_sqm=70000&max_commute_min=35&municipality=Solna&live=true`
- `GET /api/network?budget_sek_per_sqm=70000&max_commute_min=35&live=true`
- `GET /api/drilldown/{area_id}?live=true`
- `GET /api/areas/{area_id}?live=true`
- `POST /api/refresh` (clears live enrichment cache)

## Free Data Sources

- Lantmäteriet Open Geodata
- SL Trafiklab APIs
- SCB Open API
- Open-Meteo (live PM2.5 + precipitation)
- BRÅ statistics
- PTS broadband data
- OpenStreetMap / Overpass (live parks, schools, healthcare, bike infra, major roads)

Note: SL/SCB/BRÅ/PTS values are currently seeded baseline values in the demo dataset. Open-Meteo and Overpass are queried live when `live=true`.

## Use DeSO + Stockholm Data (Non-Synthetic)

Backend now prioritizes a real CSV dataset if present:

- Default path: `backend/data/deso_stockholm_areas.csv`
- Optional override: env var `HEMLA_AREAS_CSV=/abs/path/to/file.csv`

Required schema template is provided at:

- `backend/data/deso_stockholm_areas.template.csv`

When file is loaded successfully:

- `/health` and `/api/data_mode` show `areas_source=csv:...`
- Seed fallback is not used.

## Live Data Flow

1. Turn on `Live enrichment` in the UI.
2. Choose a map `Heatmap factor`.
3. Click `Update` to fetch `/api/areas?live=true`.
4. Click `Refresh live` to clear cache and force re-enrichment.

Live enrichment updates these data points:
- `pm25_ugm3` (Open-Meteo)
- `flood_risk_score` (Open-Meteo precipitation proxy)
- `green_access_score` (Overpass park density)
- `schools_score` (Overpass education amenities)
- `healthcare_access_score` (Overpass healthcare amenities)
- `bikeability_score` (Overpass cycle infra density)
- `noise_score` (Overpass major-road proxy)

## Real-Data Interpolation Plan

Current strict mode avoids fabricated values. If a metric is unavailable in source data, the API/UI shows `N/A`.

Planned next steps for better coverage:
- Commute: cluster DeSO centroids (1-2 km, municipality-bounded), query travel APIs per cluster, interpolate to member DeSO zones.
- Pricing: ingest real price sources (municipality/listing granularity), normalize to SEK/sqm by size/type/time window, interpolate to DeSO with confidence flags.
- Crime: ingest real crime sources (e.g. BRÅ/open municipal stats), normalize to per-1,000 rates by category/time window, interpolate to DeSO with confidence flags.
- Data quality flags in API/UI for each metric:
  - `real`
  - `interpolated`
  - `missing`
