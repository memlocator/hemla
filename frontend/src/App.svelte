<script>
  import { onMount } from 'svelte';
  import L from 'leaflet';
  import 'leaflet/dist/leaflet.css';
  import 'leaflet.heat/dist/leaflet-heat.js';
  import { MAP_METRIC_OPTIONS, commuteBucket, constrainedGoodness, metricValue, normalizeDesoId } from './mapMetrics.js';

  let municipality = '';
  let municipalityOptions = [''];
  let budgetCapSek = 3850000;
  let budgetCapPerSqm = 70000;
  let maxCommute = 35;
  // Each destination: { label, lat, lon, weight (1-100), query, suggestions, loading, error }
  let destinations = [
    { label: 'T-Centralen, Stockholm', lat: 59.3303, lon: 18.0586, weight: 100, query: 'T-Centralen, Stockholm', suggestions: [], loading: false, error: '' }
  ];
  let mapSearchQuery = '';
  let mapSearchSuggestions = [];
  let mapSearchLoading = false;
  let mapSearchError = '';

  let priorityPrice = 34;
  let priorityCommute = 33;
  let priorityCrime = 33;

  let showHeatmap = true;
  let showMarkers = false;
  let heatMetric = 'weighted';
  let minTransitType = '';
  let referencePresetId = '2r-55';

  function readFromUrl() {
    const p = new URLSearchParams(window.location.search);
    if (p.has('municipality')) municipality = p.get('municipality');
    if (p.has('budget')) { budgetCapPerSqm = Number(p.get('budget')); budgetCapSek = Math.round(budgetCapPerSqm * activeReferencePreset.sqm); }
    if (p.has('commute')) maxCommute = Number(p.get('commute'));
    const dlats = p.getAll('dlat').map(Number);
    const dlons = p.getAll('dlon').map(Number);
    const dweights = p.getAll('dw').map(Number);
    const dlabels = p.getAll('dlabel');
    if (dlats.length && dlons.length) {
      destinations = dlats.map((lat, i) => ({
        label: dlabels[i] || `Destination ${i + 1}`,
        lat, lon: dlons[i], weight: dweights[i] ?? 100,
        query: dlabels[i] || '', suggestions: [], loading: false, error: ''
      }));
    }
    if (p.has('pp')) priorityPrice = Number(p.get('pp'));
    if (p.has('pc')) priorityCommute = Number(p.get('pc'));
    if (p.has('ps')) priorityCrime = Number(p.get('ps'));
    if (p.has('transit')) minTransitType = p.get('transit');
    if (p.has('ref')) referencePresetId = p.get('ref');
    if (p.has('heat')) heatMetric = p.get('heat');
  }

  function pushToUrl() {
    const p = new URLSearchParams();
    if (municipality) p.set('municipality', municipality);
    if (budgetCapPerSqm !== 70000) p.set('budget', budgetCapPerSqm);
    if (maxCommute !== 35) p.set('commute', maxCommute);
    const defaultDest = destinations.length === 1 && destinations[0].lat === 59.3303 && destinations[0].lon === 18.0586;
    if (!defaultDest) {
      for (const d of destinations) {
        p.append('dlat', d.lat); p.append('dlon', d.lon);
        p.append('dw', d.weight); p.append('dlabel', d.label);
      }
    }
    if (priorityPrice !== 34) p.set('pp', priorityPrice);
    if (priorityCommute !== 33) p.set('pc', priorityCommute);
    if (priorityCrime !== 33) p.set('ps', priorityCrime);
    if (minTransitType) p.set('transit', minTransitType);
    if (referencePresetId !== '2r-55') p.set('ref', referencePresetId);
    if (heatMetric !== 'weighted') p.set('heat', heatMetric);
    const qs = p.toString();
    const newUrl = qs ? `${window.location.pathname}?${qs}` : window.location.pathname;
    if (newUrl !== window.location.href.replace(window.location.origin, '')) {
      history.replaceState(null, '', newUrl);
    }
  }

  let loading = false;
  let _mounted = false;
  let error = '';
  let areas = [];
  let selectedId = '';

  let mapContainer;
  let map;
  let markerLayer;
  let heatLayer;
  let polygonLayer;
  let baseTileLayer;
  let searchResultLayer;
  let destinationLayer;
  let desoGeojson = null;
  let geojsonError = '';
  let lastAreaKey = '';
  let autoRefreshTimer;
  let isDark = false;
  let sidebarWidth = 360;
  let suppressMarkerClicksUntil = 0;
  let hoverTooltipTimer;
  let detailCardWidth = 320;
  let detailCardHeight = 360;

  let showSales = false;
  let salesData = null;
  let salesLoading = false;

  let showSalesLayer = false;
  let salesLayer = null;
  let salesLayerLoading = false;
  let allSalesData = null;
  let saleMin = 0;
  let saleMax = 1;

  const heatOptions = MAP_METRIC_OPTIONS;
  const referencePresets = [
    { id: '1r-30', label: '1 rok · 30 sqm', rooms: 1, sqm: 30 },
    { id: '1r-35', label: '1 rok · 35 sqm', rooms: 1, sqm: 35 },
    { id: '1r-40', label: '1 rok · 40 sqm', rooms: 1, sqm: 40 },
    { id: '2r-45', label: '2 rok · 45 sqm', rooms: 2, sqm: 45 },
    { id: '2r-55', label: '2 rok · 55 sqm', rooms: 2, sqm: 55 },
    { id: '2r-65', label: '2 rok · 65 sqm', rooms: 2, sqm: 65 },
    { id: '3r-70', label: '3 rok · 70 sqm', rooms: 3, sqm: 70 },
    { id: '3r-85', label: '3 rok · 85 sqm', rooms: 3, sqm: 85 },
    { id: '3r-100', label: '3 rok · 100 sqm', rooms: 3, sqm: 100 }
  ];
  const roomRanges = [
    { label: '1 rok', minSqm: 30, maxSqm: 40 },
    { label: '2 rok', minSqm: 45, maxSqm: 65 },
    { label: '3 rok', minSqm: 70, maxSqm: 100 }
  ];
  const transitMeta = {
    subway: { label: 'Subway' },
    commuter_rail: { label: 'Commuter rail' },
    tram: { label: 'Tram' },
    bus: { label: 'Bus' },
    ferry: { label: 'Ferry' }
  };
  const transitFilterOptions = [
    { value: '', label: 'Any' },
    { value: 'tram', label: 'Tram+' },
    { value: 'commuter_rail', label: 'Rail+' },
    { value: 'subway', label: 'Subway only' }
  ];

  const transitPriority = { subway: 3, commuter_rail: 2, tram: 1, bus: 0, ferry: 0 };
  $: filteredAreas = minTransitType
    ? areas.filter((x) => (transitPriority[x.area.metrics.transit_type] ?? -1) >= (transitPriority[minTransitType] ?? 0))
    : areas;

  $: selected = filteredAreas.find((x) => x.area.id === selectedId) ?? null;
  $: if (selectedId) { showSales = false; salesData = null; }
  $: activeReferencePreset = referencePresets.find((x) => x.id === referencePresetId) ?? referencePresets[4];
  $: scoreBreaks = (() => {
    const values = filteredAreas.map((x) => x.value_score).filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
    if (values.length < 4) return { q1: 45, q2: 50, q3: 55 };
    const at = (p) => values[Math.min(values.length - 1, Math.floor((values.length - 1) * p))];
    return { q1: at(0.25), q2: at(0.5), q3: at(0.75) };
  })();

  function scoreColor(score) {
    if (score >= scoreBreaks.q3) return '#0f766e';
    if (score >= scoreBreaks.q2) return '#0ea5e9';
    if (score >= scoreBreaks.q1) return '#eab308';
    return '#ef4444';
  }

  function estimatePrice(pricePerSqm, sqm) {
    return Math.round((pricePerSqm * sqm) / 10000) * 10000;
  }

  function formatSek(value) {
    return `${Math.round(value).toLocaleString('sv-SE')} SEK`;
  }

  function formatMsek(value) {
    return `${(value / 1_000_000).toFixed(1)} MSEK`;
  }

  function referencePrice(item) {
    const pricePerSqm = item.area.metrics.avg_price_sek_per_sqm;
    if (pricePerSqm == null) return null;
    return estimatePrice(pricePerSqm, activeReferencePreset.sqm);
  }

  function roomRangeLabel(item, minSqm, maxSqm) {
    const pricePerSqm = item.area.metrics.avg_price_sek_per_sqm;
    if (pricePerSqm == null) return 'N/A';
    const minPrice = estimatePrice(pricePerSqm, minSqm);
    const maxPrice = estimatePrice(pricePerSqm, maxSqm);
    return `${formatMsek(minPrice)} - ${formatMsek(maxPrice)}`;
  }

  function priceLabel(metrics) {
    if (metrics.avg_price_sek_per_sqm == null) return 'N/A';
    return `${metrics.avg_price_sek_per_sqm.toLocaleString('sv-SE')} SEK/sqm`;
  }

  function crimeLabel(metrics) {
    if (metrics.crime_rate_per_1000 == null) return 'N/A';
    return `${metrics.crime_rate_per_1000.toFixed(1)} / 1000`;
  }

  function commuteLabel(item) {
    const commute = item.breakdown.commute_minutes ?? item.area.metrics.sl_commute_to_tcentralen_min;
    if (commute == null) return 'N/A';
    return `${Math.round(commute)} min`;
  }

  function commuteDestLines(item) {
    const validDests = destinations.filter((d) => Number.isFinite(d.lat));
    if (validDests.length <= 1) return null;
    return validDests.map((d, i) => {
      const mins = item.breakdown[`dest_commute_${i}`];
      return { label: d.label, mins: mins != null ? Math.round(mins) : null };
    });
  }

  function commuteMinutes(item) {
    const commute = item?.breakdown?.commute_minutes ?? item?.area?.metrics?.sl_commute_to_tcentralen_min;
    return Number.isFinite(commute) ? Number(commute) : null;
  }

  function transitIconSvg(type, { size = 14, color = 'currentColor' } = {}) {
    const common = `width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"`;
    const styles = `stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"`;
    const icons = {
      subway: `<svg ${common}><rect x="6" y="3.5" width="12" height="13" rx="3" ${styles}/><path d="M9 18.5 7.5 21M15 18.5 16.5 21M8 13h8M9.25 8.75h.01M14.75 8.75h.01M5.5 21h13" ${styles}/></svg>`,
      commuter_rail: `<svg ${common}><path d="M7 4.5h10a2 2 0 0 1 2 2v7.5a3.5 3.5 0 0 1-3.5 3.5h-7A3.5 3.5 0 0 1 5 14V6.5a2 2 0 0 1 2-2Z" ${styles}/><path d="M8.5 8.5h7M8.5 12h7M9 18l-1.5 3M15 18l1.5 3M6 21h12" ${styles}/></svg>`,
      tram: `<svg ${common}><path d="M8 6.5h8a3 3 0 0 1 3 3v4.5a3 3 0 0 1-3 3H8a3 3 0 0 1-3-3V9.5a3 3 0 0 1 3-3Z" ${styles}/><path d="M9.5 17.5 8 21M14.5 17.5 16 21M6 21h12M9 4l3-2 3 2M8.5 11h7" ${styles}/></svg>`,
      bus: `<svg ${common}><rect x="5" y="4.5" width="14" height="12" rx="2.5" ${styles}/><path d="M8 16.5 7 20M16 16.5 17 20M5 9.5h14M8.5 12.5h.01M15.5 12.5h.01M6 20h12" ${styles}/></svg>`,
      ferry: `<svg ${common}><path d="M4 14.5h16l-2 4.5H6l-2-4.5ZM8 10.5V7.5h8v3M12 4v3.5M4.5 20c1.5 1 3 1.5 4.5 1.5S12 21 13.5 20c1.5 1 3 1.5 4.5 1.5S21 21 21 21" ${styles}/></svg>`
    };
    return icons[type] ?? '';
  }

  function themeIconSvg(mode, { size = 15, color = 'currentColor' } = {}) {
    const common = `width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true"`;
    const styles = `stroke="${color}" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"`;
    if (mode === 'sun') {
      return `<svg ${common}><circle cx="12" cy="12" r="4" ${styles}/><path d="M12 2.5v2.25M12 19.25v2.25M4.75 12H2.5M21.5 12h-2.25M5.9 5.9 4.3 4.3M19.7 19.7l-1.6-1.6M18.1 5.9l1.6-1.6M4.3 19.7l1.6-1.6" ${styles}/></svg>`;
    }
    return `<svg ${common}><path d="M18.5 14.5A7.5 7.5 0 0 1 9.5 5.5a8 8 0 1 0 9 9Z" ${styles}/></svg>`;
  }

  function transitLabel(metrics, { iconColor = 'currentColor', iconSize = 14 } = {}) {
    const t = metrics.transit_type;
    if (!t || t === 'none') return null;
    const label = transitMeta[t]?.label ?? t;
    const walk = metrics.nearest_station_walk_min;
    const station = metrics.nearest_station_name;
    const parts = [label];
    if (station) parts.push(`(${station}`);
    if (walk) parts[parts.length - 1] += (station ? `, ${walk} min walk)` : '');
    else if (station) parts[parts.length - 1] += ')';
    const icon = transitIconSvg(t, { size: iconSize, color: iconColor });
    return `<span style="display:inline-flex;align-items:center;gap:0.35rem;vertical-align:middle">${icon}<span>${parts.join(' ')}</span></span>`;
  }

  function queryParams() {
    const params = new URLSearchParams({
      budget_sek_per_sqm: String(budgetCapPerSqm),
      max_commute_min: String(maxCommute),
      priority_price: String(priorityPrice),
      priority_commute: String(priorityCommute),
      priority_crime: String(priorityCrime),
      detail_level: 'base',
      live: 'false'
    });

    if (municipality) params.set('municipality', municipality);
    for (const d of destinations) {
      if (Number.isFinite(d.lat) && Number.isFinite(d.lon)) {
        params.append('dest_lat', String(d.lat));
        params.append('dest_lon', String(d.lon));
        params.append('dest_weight', String(d.weight));
      }
    }
    return params;
  }

  function syncBudgetFromTotal() {
    budgetCapSek = Number(budgetCapSek);
    budgetCapPerSqm = Math.round(budgetCapSek / activeReferencePreset.sqm);
  }

  function syncBudgetFromPerSqm() {
    budgetCapPerSqm = Number(budgetCapPerSqm);
    budgetCapSek = Math.round((budgetCapPerSqm * activeReferencePreset.sqm) / 10000) * 10000;
  }

  function onBudgetTotalChange() {
    syncBudgetFromTotal();
  }

  function onBudgetPerSqmChange() {
    syncBudgetFromPerSqm();
  }

  function onReferencePresetChange() {
    syncBudgetFromTotal();
  }

  async function searchDestination(i) {
    const d = destinations[i];
    const q = d.query.trim();
    if (q.length < 2) { destinations[i].error = 'Type at least 2 characters.'; destinations = destinations; return; }
    destinations[i].loading = true; destinations[i].error = ''; destinations = destinations;
    try {
      const res = await fetch(`/api/geocode?${new URLSearchParams({ q, limit: '6' })}`);
      if (!res.ok) throw new Error(`Geocode API returned ${res.status}`);
      destinations[i].suggestions = await res.json();
      if (!destinations[i].suggestions.length) destinations[i].error = 'No matches found.';
    } catch (e) {
      destinations[i].suggestions = [];
      destinations[i].error = e instanceof Error ? e.message : 'Could not search';
    } finally {
      destinations[i].loading = false; destinations = destinations;
    }
  }

  function pickDestination(i, suggestion) {
    destinations[i] = { ...destinations[i], label: suggestion.label, lat: suggestion.lat, lon: suggestion.lon, query: suggestion.label, suggestions: [], error: '' };
    destinations = destinations;
    renderDestinationMarker();
    loadAreas();
  }

  function addDestination() {
    destinations = [...destinations, { label: '', lat: NaN, lon: NaN, weight: 50, query: '', suggestions: [], loading: false, error: '' }];
  }

  function removeDestination(i) {
    destinations = destinations.filter((_, j) => j !== i);
    renderDestinationMarker();
    loadAreas();
  }

  function distanceInKm(aLat, aLon, bLat, bLon) {
    const toRad = (deg) => (deg * Math.PI) / 180;
    const dLat = toRad(bLat - aLat);
    const dLon = toRad(bLon - aLon);
    const lat1 = toRad(aLat);
    const lat2 = toRad(bLat);
    const h =
      Math.sin(dLat / 2) ** 2 +
      Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
    return 6371 * 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
  }

  function nearestAreaTo(lat, lon) {
    let best = null;
    let bestDistance = Infinity;
    for (const item of filteredAreas) {
      const d = distanceInKm(lat, lon, item.area.coordinates.lat, item.area.coordinates.lon);
      if (d < bestDistance) {
        bestDistance = d;
        best = item;
      }
    }
    return best && bestDistance <= 2.5 ? best : null;
  }

  function renderSearchResult(lat, lon) {
    if (!map || !searchResultLayer) return;
    searchResultLayer.clearLayers();
    L.circleMarker([lat, lon], {
      radius: 10,
      color: '#0f172a',
      weight: 2,
      fillColor: '#ffffff',
      fillOpacity: 0.95
    }).addTo(searchResultLayer);
    L.circleMarker([lat, lon], {
      radius: 20,
      color: '#0ea5e9',
      weight: 2,
      fillOpacity: 0.18
    }).addTo(searchResultLayer);
  }

  async function searchMapLocation() {
    const q = mapSearchQuery.trim();
    if (q.length < 2) {
      mapSearchSuggestions = [];
      mapSearchError = 'Type at least 2 characters.';
      return;
    }

    mapSearchLoading = true;
    mapSearchError = '';
    try {
      const params = new URLSearchParams({ q, limit: '5' });
      const res = await fetch(`/api/geocode?${params.toString()}`);
      if (!res.ok) throw new Error(`Geocode API returned ${res.status}`);
      mapSearchSuggestions = await res.json();
      if (!mapSearchSuggestions.length) mapSearchError = 'No places found.';
    } catch (e) {
      mapSearchSuggestions = [];
      mapSearchError = e instanceof Error ? e.message : 'Could not search map';
    } finally {
      mapSearchLoading = false;
    }
  }

  function pickMapSearchResult(suggestion) {
    mapSearchQuery = suggestion.label;
    mapSearchSuggestions = [];
    mapSearchError = '';
    if (!map) return;

    renderSearchResult(suggestion.lat, suggestion.lon);
    map.flyTo([suggestion.lat, suggestion.lon], Math.max(map.getZoom(), 13), {
      duration: 0.5
    });

    const nearest = nearestAreaTo(suggestion.lat, suggestion.lon);
    if (nearest) {
      selectedId = nearest.area.id;
      renderMap();
    }
  }

  function salePriceColor(ppm, min, max) {
    const t = max > min ? Math.max(0, Math.min(1, (ppm - min) / (max - min))) : 0.5;
    const r = Math.round(t < 0.5 ? t * 2 * 255 : 255);
    const g = Math.round(t < 0.5 ? 255 : (1 - t) * 2 * 255);
    return `rgb(${r},${g},50)`;
  }

  function saleListingScore(s, desoItem) {
    // Price score for this specific listing vs user budget
    const norm = (v, lo, hi, rev) => { const t = Math.max(0, Math.min(1, (v - lo) / (hi - lo))); return rev ? 1 - t : t; };
    const affordability = norm(s.p, 25000, 130000, true);
    const budgetFit = s.p <= budgetCapPerSqm ? 1 : Math.max(0, 1 - (s.p - budgetCapPerSqm) / 40000);
    const priceScore = affordability * 0.6 + budgetFit * 0.4;

    const commuteScore = desoItem ? (desoItem.breakdown.commute ?? 0) / 100 : 0.0;
    const crimeScore   = desoItem ? (desoItem.breakdown.crime   ?? 50) / 100 : 0.5;

    const total = Math.max(1, priorityPrice + priorityCommute + priorityCrime);
    const blended = (priceScore * priorityPrice + commuteScore * priorityCommute + crimeScore * priorityCrime) / total;
    return Math.round(blended * 100);
  }

  function renderSalesInView() {
    if (!map || !salesLayer || !allSalesData || !showSalesLayer) return;
    salesLayer.clearLayers();
    const bounds = map.getBounds().pad(0.15);
    const zoom = map.getZoom();
    const desoById = new Map(filteredAreas.map((item) => [item.area.id, item]));
    const gridSize = zoom >= 14 ? 0 : zoom >= 12 ? 0.003 : zoom >= 10 ? 0.01 : 0.03;
    const seen = new Set();
    for (const s of allSalesData) {
      if (!bounds.contains([s.la, s.lo])) continue;
      if (gridSize > 0) {
        const key = `${Math.round(s.la / gridSize)},${Math.round(s.lo / gridSize)}`;
        if (seen.has(key)) continue;
        seen.add(key);
      }
      const desoItem = s.d ? desoById.get(s.d) : null;
      const color = scoreColor(saleListingScore(s, desoItem));
      const date = s.t ? new Date(s.t * 1000).toLocaleDateString('sv-SE') : '';
      L.circleMarker([s.la, s.lo], {
        radius: zoom >= 14 ? 5 : zoom >= 12 ? 4 : 3,
        color,
        fillColor: color,
        fillOpacity: 0.8,
        weight: 0,
        pane: 'markerPane',
      }).bindPopup(
        `<b>${s.a}</b><br>${s.r} · ${s.s} m²<br>${s.p.toLocaleString('sv-SE')} SEK/m²<br>${s.f.toLocaleString('sv-SE')} SEK total<br>${date}`,
        { maxWidth: 200 }
      ).addTo(salesLayer);
    }
  }

  async function toggleSalesLayer() {
    if (!map || !salesLayer) return;
    if (showSalesLayer) {
      salesLayer.clearLayers();
      showSalesLayer = false;
      map.off('moveend', renderSalesInView);
      return;
    }
    salesLayerLoading = true;
    try {
      if (!allSalesData) {
        const res = await fetch('/api/sold_listings_all');
        const data = await res.json();
        allSalesData = data.listings;
        const prices = allSalesData.map((s) => s.p).filter(Boolean);
        saleMin = Math.min(...prices);
        saleMax = Math.max(...prices);
      }
      showSalesLayer = true;
      renderSalesInView();
      map.on('moveend', renderSalesInView);
    } catch (e) { console.error(e); }
    salesLayerLoading = false;
  }

  async function toggleSales() {
    if (showSales) { showSales = false; return; }
    if (!selected) return;
    salesLoading = true;
    try {
      const res = await fetch(`/api/areas/${selected.area.id}/sold_listings?limit=100`);
      salesData = await res.json();
      showSales = true;
    } catch { salesData = null; }
    salesLoading = false;
  }

  function formatDate(ts) {
    if (!ts) return '';
    return new Date(ts * 1000).toLocaleDateString('sv-SE');
  }

  async function loadMunicipalities() {
    try {
      const res = await fetch('/api/municipalities');
      if (!res.ok) throw new Error('Municipality API failed');
      const names = await res.json();
      municipalityOptions = ['', ...(names || [])];
    } catch {
      municipalityOptions = [''];
    }
  }

  async function loadDesoGeojson() {
    geojsonError = '';
    try {
      const res = await fetch('/api/deso_geojson');
      if (!res.ok) throw new Error(`DeSO GeoJSON returned ${res.status}`);
      desoGeojson = await res.json();
      renderMap();
    } catch (e) {
      geojsonError = e instanceof Error ? e.message : 'Could not load DeSO polygons';
      desoGeojson = null;
      renderMap();
    }
  }

  async function loadAreas() {
    loading = true;
    error = '';
    try {
      const res = await fetch(`/api/areas?${queryParams().toString()}`);
      if (!res.ok) throw new Error(`Areas API returned ${res.status}`);
      const data = await res.json();
      areas = data.items || [];
      if (!areas.some((x) => x.area.id === selectedId)) {
        selectedId = areas[0]?.area.id ?? '';
      }
      renderMap();
    } catch (e) {
      error = e instanceof Error ? e.message : 'Could not load areas';
      areas = [];
      selectedId = '';
      renderMap();
    } finally {
      loading = false;
    }
  }

  function scheduleAutoRefresh() {
    if (autoRefreshTimer) clearTimeout(autoRefreshTimer);
    autoRefreshTimer = setTimeout(() => {
      loadAreas();
    }, 500);
  }

  function initMap() {
    if (map || !mapContainer) return;

    map = L.map(mapContainer, {
      zoomControl: false,
      scrollWheelZoom: true,
      attributionControl: true
    }).setView([59.336, 18.065], 10);

    L.control.zoom({ position: 'bottomright' }).addTo(map);

    updateMapTiles();

    map.on('dragstart', () => {
      suppressMarkerClicksUntil = Date.now() + 250;
    });

    map.on('dragend', () => {
      suppressMarkerClicksUntil = Date.now() + 350;
    });

    map.createPane('polygonPane').style.zIndex = '330';
    map.createPane('heatPane').style.zIndex = '350';
    polygonLayer = L.layerGroup().addTo(map);
    markerLayer = L.layerGroup().addTo(map);
    salesLayer = L.layerGroup().addTo(map);
    destinationLayer = L.layerGroup().addTo(map);
    searchResultLayer = L.layerGroup().addTo(map);

    requestAnimationFrame(() => map && map.invalidateSize());
  }

  function updateMapTiles() {
    if (!map) return;
    if (baseTileLayer) {
      map.removeLayer(baseTileLayer);
      baseTileLayer = undefined;
    }

    if (isDark) {
      baseTileLayer = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        maxZoom: 19,
        attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
      });
    } else {
      baseTileLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19,
        attribution: '&copy; OpenStreetMap contributors'
      });
    }

    baseTileLayer.addTo(map);
  }

  function toggleTheme() {
    isDark = !isDark;
    localStorage.setItem('hemla-theme', isDark ? 'dark' : 'light');
    updateMapTiles();
  }

  function removeHeat() {
    if (map && heatLayer) {
      map.removeLayer(heatLayer);
      heatLayer = undefined;
    }
    const pane = map?.getPane('heatPane');
    if (pane) pane.style.filter = '';
  }

  function metricRaw(item) {
    return metricValue(item, heatMetric);
  }

  function metricIsLowerBetter() {
    return heatMetric === 'commute' || heatMetric === 'price';
  }

  function metricColor(item, min, max) {
    if (heatMetric === 'commute') {
      return commuteBucket(commuteMinutes(item)).color;
    }

    const raw = metricRaw(item);
    if (!Number.isFinite(raw)) return '#94a3b8';
    const base = max === min ? 0.5 : (raw - min) / (max - min);
    const good = constrainedGoodness(
      item,
      heatMetric,
      metricIsLowerBetter() ? 1 - base : base,
      { budgetCapPerSqm, maxCommute },
    );
    if (good >= 0.66) return '#0f766e';
    if (good >= 0.33) return '#0ea5e9';
    return '#f97316';
  }

  function polygonStyle(item, { hovered = false } = {}) {
    const values = filteredAreas.map((area) => metricRaw(area)).filter((value) => Number.isFinite(value));
    const min = values.length ? Math.min(...values) : 0;
    const max = values.length ? Math.max(...values) : 1;
    const commute = commuteMinutes(item);
    const inRange = commute == null || commute <= maxCommute;
    const active = item?.area?.id === selectedId;
    const unavailable = !item || (heatMetric === 'commute' && commute == null);

    return {
      pane: 'polygonPane',
      color: active ? '#0f172a' : '#334155',
      weight: active ? 2.5 : hovered ? 1.5 : 0,
      opacity: unavailable ? 0.45 : 0.85,
      fillColor: metricColor(item, min, max),
      fillOpacity: unavailable ? 0.08 : inRange ? 0.38 : 0.14,
      interactive: Boolean(item),
    };
  }

  function renderDestinationMarker() {
    if (!map || !destinationLayer) return;
    destinationLayer.clearLayers();
    for (const d of destinations) {
      if (!Number.isFinite(d.lat) || !Number.isFinite(d.lon)) continue;
      L.circleMarker([d.lat, d.lon], {
        radius: 7, color: '#ffffff', weight: 3, fillColor: '#0f172a', fillOpacity: 1,
      }).bindTooltip(d.label, { permanent: false }).addTo(destinationLayer);
      L.circleMarker([d.lat, d.lon], {
        radius: 16, color: '#0f172a', weight: 2, fillOpacity: 0.08,
      }).addTo(destinationLayer);
    }
  }

  function renderPolygons(areaById) {
    if (!map || !polygonLayer) return;
    polygonLayer.clearLayers();
    if (!showHeatmap || !desoGeojson) return;

    const layer = L.geoJSON(desoGeojson, {
      pane: 'polygonPane',
      filter: (feature) => areaById.has(normalizeDesoId(feature?.properties?.desokod)),
      style: (feature) => {
        const id = normalizeDesoId(feature?.properties?.desokod);
        return polygonStyle(areaById.get(id));
      },
      onEachFeature: (feature, layer) => {
        const id = normalizeDesoId(feature?.properties?.desokod);
        const item = areaById.get(id);
        if (!item) return;

        layer.on('mouseover', () => layer.setStyle(polygonStyle(item, { hovered: true })));
        layer.on('mouseout', () => layer.setStyle(polygonStyle(item)));
        layer.on('click', () => {
          selectedId = item.area.id;
          renderMap();
        });
      },
    });
    layer.addTo(polygonLayer);
  }


  function renderMap() {
    if (!map || !markerLayer || !polygonLayer) return;

    if (hoverTooltipTimer) {
      clearTimeout(hoverTooltipTimer);
      hoverTooltipTimer = undefined;
    }

    markerLayer.clearLayers();
    polygonLayer.clearLayers();
    removeHeat();
    renderDestinationMarker();

    if (!filteredAreas.length) return;

    const bounds = [];
    const areaById = new Map(filteredAreas.map((item) => [item.area.id, item]));
    renderPolygons(areaById);

    for (const item of filteredAreas) {
      const latLng = [item.area.coordinates.lat, item.area.coordinates.lon];
      bounds.push(latLng);
      const active = item.area.id === selectedId;
      if (!showMarkers && !active) continue;
      const markerSize = active ? 22 : 16;
      const withinRange = item.area.metrics.nearest_station_walk_min != null && item.area.metrics.nearest_station_walk_min <= 9;
      const transitIconSize = Math.round(markerSize * 0.72);
      const transitIcon = withinRange ? transitIconSvg(item.area.metrics.transit_type, { size: transitIconSize, color: '#ffffff' }) : '';
      const marker = L.marker(latLng, {
        icon: L.divIcon({
          className: '',
          iconSize: [markerSize, markerSize],
          iconAnchor: [markerSize / 2, markerSize / 2],
          html: `<div style="width:${markerSize}px;height:${markerSize}px;border-radius:50%;background:${scoreColor(item.value_score)};border:${active ? '2px solid #0f172a' : '1px solid #ffffff'};display:flex;align-items:center;justify-content:center;line-height:1">${transitIcon}</div>`
        })
      }).addTo(markerLayer);

      const commute = commuteLabel(item);
      const price = priceLabel(item.area.metrics);
      const crime = crimeLabel(item.area.metrics);
      const refPrice = referencePrice(item);
      const refPriceLabel = refPrice == null ? 'N/A' : formatSek(refPrice);
      const transit = transitLabel(item.area.metrics, { iconColor: '#0f172a', iconSize: 13 });
      const tooltip = L.tooltip({
        permanent: false,
        sticky: true,
        interactive: false
      }).setContent(
        `<div style=\"min-width:200px\">
          <div style=\"font-weight:700\">${item.area.name}</div>
          <div style=\"font-size:12px;color:#475569\">${item.area.municipality}</div>
          <div style=\"margin-top:6px;font-size:12px\">Fit: <b>${item.value_score}</b></div>
          <div style=\"font-size:12px\">Price: <b>${price}</b></div>
          <div style=\"font-size:12px\">Commute: <b>${commute}</b> to destination</div>
          ${transit ? `<div style=\"font-size:12px\">Transit: <b>${transit}</b></div>` : ''}
          <div style=\"font-size:12px\">Crime: <b>${crime}</b></div>
          <div style=\"font-size:12px\">Ref (${activeReferencePreset.rooms} rok, ${activeReferencePreset.sqm} sqm): <b>${refPriceLabel}</b></div>
        </div>`
      );
      marker.on('mouseover', () => {
        if (hoverTooltipTimer) clearTimeout(hoverTooltipTimer);
        hoverTooltipTimer = setTimeout(() => {
          tooltip.setLatLng(latLng);
          tooltip.addTo(map);
          hoverTooltipTimer = undefined;
        }, 500);
      });
      marker.on('mouseout', () => {
        if (hoverTooltipTimer) {
          clearTimeout(hoverTooltipTimer);
          hoverTooltipTimer = undefined;
        }
        if (map?.hasLayer(tooltip)) map.removeLayer(tooltip);
      });
      marker.on('click', () => {
        if (hoverTooltipTimer) {
          clearTimeout(hoverTooltipTimer);
          hoverTooltipTimer = undefined;
        }
        if (map?.hasLayer(tooltip)) map.removeLayer(tooltip);
        if (Date.now() < suppressMarkerClicksUntil) return;
        selectedId = item.area.id;
        renderMap();
      });
    }

    const currentKey = `${filteredAreas.map((a) => a.area.id).sort().join('|')}|${Boolean(desoGeojson)}`;
    if (currentKey !== lastAreaKey) {
      lastAreaKey = currentKey;
      if (bounds.length === 1) {
        map.setView(bounds[0], 12);
      } else {
        map.fitBounds(bounds, { padding: [40, 40], maxZoom: 12 });
      }
    }
  }

  function focusAreaOnMap(areaId) {
    if (!map) return;
    const item = areas.find((x) => x.area.id === areaId);
    if (!item) return;

    const targetZoom = Math.max(map.getZoom(), 12);
    map.flyTo([item.area.coordinates.lat, item.area.coordinates.lon], targetZoom, {
      duration: 0.5
    });
  }

  function applyPreset(type) {
    if (type === 'balanced') {
      priorityPrice = 34;
      priorityCommute = 33;
      priorityCrime = 33;
    } else if (type === 'commute') {
      priorityPrice = 20;
      priorityCommute = 60;
      priorityCrime = 20;
    } else if (type === 'safety') {
      priorityPrice = 20;
      priorityCommute = 20;
      priorityCrime = 60;
    } else if (type === 'price') {
      priorityPrice = 60;
      priorityCommute = 20;
      priorityCrime = 20;
    }

  }

  function startDetailCardResize(event) {
    event.preventDefault();
    const card = event.currentTarget.closest('.resizable-detail-card');
    const shell = card?.closest('.map-shell');
    if (!card || !shell) return;

    const startX = event.clientX;
    const startY = event.clientY;
    const startWidth = card.getBoundingClientRect().width;
    const startHeight = card.getBoundingClientRect().height;
    const shellRect = shell.getBoundingClientRect();
    const maxWidth = Math.max(260, shellRect.width - 24);
    const maxHeight = Math.max(220, shellRect.height - 24);

    const clamp = (value, min, max) => Math.min(max, Math.max(min, value));

    function onMove(moveEvent) {
      detailCardWidth = clamp(startWidth + moveEvent.clientX - startX, 260, maxWidth);
      detailCardHeight = clamp(startHeight + startY - moveEvent.clientY, 220, maxHeight);
    }

    function onUp() {
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', onUp);
    }

    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp, { once: true });
  }

  const SIDEBAR_MIN = 200;
  const SIDEBAR_DEFAULT = 360;
  const SIDEBAR_COLLAPSE_THRESHOLD = 120;

  function startSidebarResize(event) {
    event.preventDefault();
    const startX = event.clientX;
    const startWidth = sidebarWidth;

    function onMove(e) {
      const newWidth = startWidth - (e.clientX - startX);
      sidebarWidth = Math.max(0, newWidth);
    }

    function onUp() {
      window.removeEventListener('pointermove', onMove);
      if (sidebarWidth < SIDEBAR_COLLAPSE_THRESHOLD) sidebarWidth = 0;
      else if (sidebarWidth < SIDEBAR_MIN) sidebarWidth = SIDEBAR_MIN;
    }

    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp, { once: true });
  }

  $: if (map && sidebarWidth !== undefined) requestAnimationFrame(() => map && map.invalidateSize());
  $: if (filteredAreas) renderSalesInView();

  $: if (map) {
    filteredAreas;
    selectedId;
    showHeatmap;
    showMarkers;
    heatMetric;
    referencePresetId;
    maxCommute;
    desoGeojson;
    renderMap();
  }

  $: if (map) {
    isDark;
    updateMapTiles();
  }

  $: {
    municipality;
    maxCommute;
    budgetCapSek;
    budgetCapPerSqm;
    priorityPrice;
    priorityCommute;
    priorityCrime;
    destinations;
    referencePresetId;
    minTransitType;
    heatMetric;
    if (_mounted) pushToUrl();
    if (map) scheduleAutoRefresh();
  }

  onMount(async () => {
    try {
      const stored = localStorage.getItem('hemla-theme');
      if (stored === 'dark') {
        isDark = true;
      } else if (stored === 'light') {
        isDark = false;
      } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
        isDark = true;
      }
    } catch {
      isDark = false;
    }

    readFromUrl();
    _mounted = true;

    // Fall back to localStorage for destination only if still at default
    if (destinations.length === 1 && destinations[0].lat === 59.3303) {
      try {
        const dest = localStorage.getItem('hemla-destination');
        if (dest) {
          const { query, lat, lon } = JSON.parse(dest);
          destinations = [{ ...destinations[0], label: query, query, lat, lon }];
        }
      } catch { /* ignore */ }
    }

    initMap();
    await Promise.all([loadMunicipalities(), loadDesoGeojson(), loadAreas()]);

    return () => {
      if (autoRefreshTimer) clearTimeout(autoRefreshTimer);
      if (map) {
        map.remove();
        map = undefined;
        markerLayer = undefined;
        heatLayer = undefined;
        polygonLayer = undefined;
        salesLayer = undefined;
        showSalesLayer = false;
        baseTileLayer = undefined;
        searchResultLayer = undefined;
        destinationLayer = undefined;
      }
    };
  });
</script>

<div class={`relative grid w-full transition-colors lg:h-[100dvh] lg:max-h-[100dvh] lg:grid-rows-[auto,minmax(0,1fr)] lg:overflow-hidden ${isDark ? 'theme-dark' : 'theme-light'}`}>
  <header class={`flex items-center justify-between gap-4 px-6 py-3 ${isDark ? 'border-b border-white/10 bg-[#080f1e]' : 'border-b border-slate-200 bg-white'}`}>
    <div class="flex items-center gap-3">
      <span class={`font-brand text-lg font-extrabold leading-none tracking-[-0.03em] ${isDark ? 'text-white' : 'text-slate-900'}`}>hemla.</span>
      <span class={`hidden h-4 w-px sm:block ${isDark ? 'bg-white/10' : 'bg-slate-200'}`}></span>
      <span class={`hidden text-[12px] font-medium tracking-[0.01em] md:block ${isDark ? 'text-white/40' : 'text-slate-500'}`}></span>
    </div>
    <div class="flex items-center gap-2">
      <div class={`hidden items-center gap-1 rounded-md px-3 py-1.5 text-xs sm:flex ${isDark ? 'bg-white/5 text-white/50' : 'bg-slate-100 text-slate-500'}`}>
        <span class={`font-medium ${isDark ? 'text-white/80' : 'text-slate-700'}`}>{filteredAreas.length}</span> areas
        {#if municipality}<span class="opacity-30">·</span><span class={`font-medium ${isDark ? 'text-white/80' : 'text-slate-700'}`}>{municipality}</span>{/if}
      </div>
      <div class={`flex items-center gap-1 rounded-md px-2 py-1.5 text-xs ${isDark ? 'bg-white/5 text-white/50' : 'bg-slate-100 text-slate-500'}`}>
        <span class="opacity-60">apt</span>
        <select class={`bg-transparent text-xs font-medium outline-none ${isDark ? 'text-white/80' : 'text-slate-700'}`} bind:value={referencePresetId} on:change={onReferencePresetChange}>
          {#each referencePresets as preset}
            <option value={preset.id}>{preset.label}</option>
          {/each}
        </select>
      </div>
      <button class={`flex items-center justify-center rounded-md px-3 py-1.5 text-xs font-medium transition ${isDark ? 'bg-white/10 text-white/70 hover:bg-white/15' : 'bg-slate-100 text-slate-600 hover:bg-slate-200'}`} on:click={toggleTheme} aria-label={isDark ? 'Switch to light mode' : 'Switch to dark mode'}>
        {@html themeIconSvg(isDark ? 'sun' : 'moon', { color: isDark ? '#fbbf24' : '#0f172a' })}
      </button>
    </div>
  </header>

  <section class="flex min-h-0 gap-4 p-4 lg:h-full lg:px-6 lg:py-4">
    <div class="panel h-full min-h-0 flex-1 p-3 md:p-4">
      <div class="map-shell border border-slate-300/80 bg-slate-100">
        <div bind:this={mapContainer} class="map-container" style={isDark ? 'background:#0b1222' : ''}></div>

        <div class="pointer-events-none absolute inset-x-3 top-3 z-[1000] flex justify-center md:pr-28">
          <div class="pointer-events-auto relative w-full max-w-md">
            <div class={`flex items-center gap-2 rounded-full border px-2 py-2 shadow-lg backdrop-blur ${isDark ? 'border-white/10 bg-[#080f1ee6]' : 'border-slate-300/70 bg-white/85'}`}>
              <input
                class={`w-full rounded-full border-none bg-transparent px-3 text-sm outline-none ring-0 focus:outline-none focus:ring-0 ${isDark ? 'text-white placeholder:text-white/35' : 'text-slate-900 placeholder:text-slate-400'}`}
                type="text"
                bind:value={mapSearchQuery}
                on:keydown={(e) => {
                  if (e.key === 'Enter') {
                    e.preventDefault();
                    if (mapSearchSuggestions.length) pickMapSearchResult(mapSearchSuggestions[0]);
                    else searchMapLocation();
                  }
                }}
                placeholder="Search a place on the map"
              />
              <button
                class={`rounded-full border px-3 py-1.5 text-xs font-medium shadow-sm transition ${isDark ? 'border-teal-500/45 bg-teal-500/20 text-teal-100 hover:bg-teal-500/30' : 'border-teal-600 bg-teal-600 text-white hover:bg-teal-700'}`}
                on:click={searchMapLocation}
              >
                Search
              </button>
            </div>
            {#if mapSearchLoading}
              <div class={`mt-2 rounded-2xl px-3 py-2 text-xs shadow-lg ${isDark ? 'bg-[#080f1ee6] text-white/60' : 'bg-white/95 text-slate-500'}`}>Searching...</div>
            {:else if mapSearchError}
              <div class={`mt-2 rounded-2xl px-3 py-2 text-xs shadow-lg ${isDark ? 'bg-[#080f1ee6] text-rose-300' : 'bg-white/95 text-red-700'}`}>{mapSearchError}</div>
            {:else if mapSearchSuggestions.length}
              <div class={`mt-2 overflow-hidden rounded-2xl border shadow-lg ${isDark ? 'border-white/10 bg-[#080f1ef2]' : 'border-slate-200 bg-white/95'}`}>
                {#each mapSearchSuggestions as suggestion}
                  <button
                    type="button"
                    class={`block w-full px-3 py-2 text-left text-sm transition ${isDark ? 'text-white/80 hover:bg-white/5' : 'text-slate-700 hover:bg-slate-50'}`}
                    on:click={() => pickMapSearchResult(suggestion)}
                  >
                    {suggestion.label}
                  </button>
                {/each}
              </div>
            {/if}
          </div>
        </div>

        <div class="absolute right-3 top-3 flex flex-col gap-1" style="z-index:1000">
          <button
            class={`pointer-events-auto rounded-lg border px-2 py-1 text-[11px] font-medium shadow-sm transition ${showMarkers ? 'border-teal-600 bg-teal-600 text-white hover:bg-teal-700' : 'border-slate-300 bg-white/80 text-slate-500 hover:bg-white'}`}
            on:click={() => { showMarkers = !showMarkers; }}
            title="Toggle markers"
          >● Markers</button>
          <button
            class={`pointer-events-auto rounded-lg border px-2 py-1 text-[11px] font-medium shadow-sm transition ${showHeatmap ? 'border-teal-600 bg-teal-600 text-white hover:bg-teal-700' : 'border-slate-300 bg-white/80 text-slate-500 hover:bg-white'}`}
            on:click={() => { showHeatmap = !showHeatmap; }}
            title="Toggle area layer"
          >▦ Areas</button>
          <button
            class={`pointer-events-auto rounded-lg border px-2 py-1 text-[11px] font-medium shadow-sm transition ${showSalesLayer ? 'border-teal-600 bg-teal-600 text-white hover:bg-teal-700' : 'border-slate-300 bg-white/80 text-slate-500 hover:bg-white'}`}
            on:click={toggleSalesLayer}
            title="Toggle real sales layer"
            disabled={salesLayerLoading}
          >{#if salesLayerLoading}…{:else}<svg xmlns="http://www.w3.org/2000/svg" width="11" height="11" viewBox="0 0 16 16" fill="currentColor" style="display:inline;vertical-align:-1px;margin-right:3px"><path d="M2 2h5.5l6.5 6.5a1.5 1.5 0 0 1 0 2.1l-3.4 3.4a1.5 1.5 0 0 1-2.1 0L2 7.5V2zm2 1v3.9l5.8 5.8.7-.7L4.6 6.3 4 3H4zm2 2a1 1 0 1 0 0-2 1 1 0 0 0 0 2z"/></svg>Sales{/if}</button>
          {#if showHeatmap}
            <select
              class="pointer-events-auto rounded-lg border border-slate-300 bg-white/90 px-2 py-1 text-[11px] font-medium shadow-sm"
              bind:value={heatMetric}
            >
              {#each heatOptions as option}
                <option value={option.id}>{option.label}</option>
              {/each}
            </select>
          {/if}
          {#if geojsonError}
            <div class={`pointer-events-auto max-w-40 rounded-lg border px-2 py-1 text-[11px] shadow-sm ${isDark ? 'border-rose-400/30 bg-[#080f1ee6] text-rose-200' : 'border-red-200 bg-white/90 text-red-700'}`}>{geojsonError}</div>
          {/if}
        </div>

        {#if selected}
          <div
            class={`resizable-detail-card pointer-events-auto absolute bottom-3 left-3 z-[1000] hidden rounded-2xl border p-[0.8rem] shadow-[0_20px_40px_-26px_rgba(15,23,42,0.55)] backdrop-blur md:block xl:p-[0.9rem] ${isDark ? 'border-slate-400/15 bg-[#080f1ee0] text-slate-200' : 'border-slate-400/30 bg-white/92 text-slate-900'}`}
            style={`width:${detailCardWidth}px;height:${detailCardHeight}px`}
          >
            <button
              class={`detail-card-resize-handle ${isDark ? 'border-white/15 bg-white/10' : 'border-slate-300 bg-white/90'}`}
              type="button"
              aria-label="Resize selected area panel"
              title="Resize"
              on:pointerdown={startDetailCardResize}
            ></button>
            <p class={`m-0 text-[11px] font-semibold uppercase tracking-[0.12em] ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>Selected area</p>
            <div class="mt-2 flex items-start justify-between gap-3">
              <div>
                <p class="m-0 text-base font-bold">{selected.area.name}</p>
                <p class={`m-0 text-xs ${isDark ? 'text-slate-300' : 'text-slate-600'}`}>{selected.area.municipality}</p>
              </div>
              <p class="m-0 text-2xl font-bold leading-none" style={`color:${scoreColor(selected.value_score)}`}>{selected.value_score}</p>
            </div>
            <div class="mt-3 grid gap-1.5 text-sm">
              <p class="m-0">Price: <span class="font-semibold">{priceLabel(selected.area.metrics)}</span>
                {#if selected.area.metrics.price_source === 'real'}
                  <span class={`ml-1 text-xs ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>based on {selected.area.metrics.price_n_listings} sales</span>
                {:else if selected.area.metrics.price_source === 'municipality_median'}
                  <span class={`ml-1 text-xs ${isDark ? 'text-amber-400' : 'text-amber-600'}`}>municipality avg</span>
                {/if}
              </p>
              {#if commuteDestLines(selected)}
                {#each commuteDestLines(selected) as row, i}
                  <p class="m-0 flex gap-1 min-w-0">
                    <span class="shrink-0">Commute {i + 1}:</span>
                    <span class="font-semibold shrink-0">{row.mins != null ? `${row.mins} min` : 'N/A'}</span>
                    <span class={`truncate text-xs self-end ${isDark ? 'text-slate-400' : 'text-slate-500'}`} title={row.label}>{row.label}</span>
                  </p>
                {/each}
              {:else}
                <p class="m-0">Commute: <span class="font-semibold">{commuteLabel(selected)}</span></p>
              {/if}
              {#if transitLabel(selected.area.metrics)}<p class="m-0">Transit: <span class="font-semibold">{@html transitLabel(selected.area.metrics, { iconColor: isDark ? '#e2e8f0' : '#0f172a' })}</span></p>{/if}
              <p class="m-0">Crime: <span class="font-semibold">{crimeLabel(selected.area.metrics)}</span></p>
              <p class="m-0">Ref ({activeReferencePreset.rooms} rok, {activeReferencePreset.sqm} sqm): <span class="font-semibold">{referencePrice(selected) == null ? 'N/A' : formatSek(referencePrice(selected))}</span></p>
            </div>
            <button
              on:click={toggleSales}
              class={`mt-3 w-full rounded px-3 py-1.5 text-xs font-medium transition-colors ${showSales ? (isDark ? 'bg-cyan-700 text-white' : 'bg-cyan-600 text-white') : (isDark ? 'bg-white/10 text-slate-200 hover:bg-white/20' : 'bg-slate-100 text-slate-700 hover:bg-slate-200')}`}
            >
              {salesLoading ? 'Loading…' : showSales ? 'Hide sales' : `Show recent sales${selected.area.metrics.price_n_listings ? ` (${selected.area.metrics.price_n_listings})` : ''}`}
            </button>
            {#if showSales && salesData}
              <div class={`mt-2 max-h-60 overflow-y-auto rounded text-xs ${isDark ? 'bg-white/5' : 'bg-slate-50'}`}>
                {#if salesData.listings.length === 0}
                  <p class={`px-3 py-2 ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>No sales data for this zone.</p>
                {:else}
                  <div class={`sticky top-0 grid grid-cols-4 gap-1 px-3 py-1.5 font-semibold ${isDark ? 'bg-slate-800 text-slate-300' : 'bg-slate-200 text-slate-600'}`}>
                    <span class="col-span-2">Address</span>
                    <span class="text-right">Price/sqm</span>
                    <span class="text-right">Date</span>
                  </div>
                  {#each salesData.listings as s}
                    <div class={`grid grid-cols-4 gap-1 border-t px-3 py-1 ${isDark ? 'border-white/5 text-slate-300' : 'border-slate-100 text-slate-700'}`}>
                      <span class="col-span-2 truncate" title={s.street_address}>{s.street_address} · {s.rooms} · {s.sqm} m²</span>
                      <span class="text-right">{s.price_per_sqm_sek.toLocaleString('sv-SE')}</span>
                      <span class="text-right">{formatDate(s.sold_at)}</span>
                    </div>
                  {/each}
                  {#if salesData.total > salesData.listings.length}
                    <p class={`px-3 py-1.5 ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>Showing {salesData.listings.length} of {salesData.total} sales</p>
                  {/if}
                {/if}
              </div>
            {/if}
            <div class={`mt-3 hidden rounded-lg px-3 py-2 text-xs sm:block ${isDark ? 'bg-white/5 text-white/70' : 'bg-slate-100 text-slate-700'}`}>
              {#each roomRanges as rr}
                <p class="m-0">{rr.label} ({rr.minSqm}-{rr.maxSqm} sqm): <span class="font-semibold">{roomRangeLabel(selected, rr.minSqm, rr.maxSqm)}</span></p>
              {/each}
            </div>
            <div class="mt-3 hidden gap-2 text-xs sm:grid">
              <div>
                <div class="mb-1 flex justify-between"><span>Price fit</span><span>{selected.breakdown.price ?? selected.breakdown.affordability}</span></div>
                <div class="h-1.5 rounded bg-slate-200/80"><div class="h-1.5 rounded bg-cyan-600" style={`width:${selected.breakdown.price ?? selected.breakdown.affordability}%`}></div></div>
              </div>
              <div>
                <div class="mb-1 flex justify-between"><span>Commute fit</span><span>{selected.breakdown.commute ?? selected.breakdown.mobility}</span></div>
                <div class="h-1.5 rounded bg-slate-200/80"><div class="h-1.5 rounded bg-cyan-600" style={`width:${selected.breakdown.commute ?? selected.breakdown.mobility}%`}></div></div>
              </div>
              <div>
                <div class="mb-1 flex justify-between"><span>Crime safety fit</span><span>{selected.breakdown.crime ?? selected.breakdown.safety}</span></div>
                <div class="h-1.5 rounded bg-slate-200/80"><div class="h-1.5 rounded bg-cyan-600" style={`width:${selected.breakdown.crime ?? selected.breakdown.safety}%`}></div></div>
              </div>
            </div>
          </div>
        {/if}
      </div>

      {#if selected}
        <details class={`mt-3 rounded-2xl border md:hidden ${isDark ? 'border-white/10 bg-[#080f1e] text-slate-200' : 'border-slate-300 bg-white text-slate-900'}`} open>
          <summary class="flex cursor-pointer list-none items-center justify-between gap-3 px-4 py-3">
            <div class="min-w-0">
              <p class={`m-0 text-[11px] font-semibold uppercase tracking-[0.12em] ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>Selected area</p>
              <p class="m-0 mt-1 truncate text-sm font-semibold">{selected.area.name}</p>
              <p class={`m-0 text-xs ${isDark ? 'text-slate-300' : 'text-slate-600'}`}>{selected.area.municipality}</p>
            </div>
            <div class="flex items-center gap-3">
              <p class="m-0 text-xl font-bold leading-none" style={`color:${scoreColor(selected.value_score)}`}>{selected.value_score}</p>
              <span class={`text-xs ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>Expand</span>
            </div>
          </summary>
          <div class={`border-t px-4 pb-4 pt-3 ${isDark ? 'border-white/10' : 'border-slate-200'}`}>
            <div class="grid gap-1.5 text-sm">
              <p class="m-0">Price: <span class="font-semibold">{priceLabel(selected.area.metrics)}</span>
                {#if selected.area.metrics.price_source === 'real'}
                  <span class={`ml-1 text-xs ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>based on {selected.area.metrics.price_n_listings} sales</span>
                {:else if selected.area.metrics.price_source === 'municipality_median'}
                  <span class={`ml-1 text-xs ${isDark ? 'text-amber-400' : 'text-amber-600'}`}>municipality avg</span>
                {/if}
              </p>
              {#if commuteDestLines(selected)}
                {#each commuteDestLines(selected) as row, i}
                  <p class="m-0 flex gap-1 min-w-0">
                    <span class="shrink-0">Commute {i + 1}:</span>
                    <span class="font-semibold shrink-0">{row.mins != null ? `${row.mins} min` : 'N/A'}</span>
                    <span class={`truncate text-xs self-end ${isDark ? 'text-slate-400' : 'text-slate-500'}`} title={row.label}>{row.label}</span>
                  </p>
                {/each}
              {:else}
                <p class="m-0">Commute: <span class="font-semibold">{commuteLabel(selected)}</span></p>
              {/if}
              {#if transitLabel(selected.area.metrics)}<p class="m-0">Transit: <span class="font-semibold">{@html transitLabel(selected.area.metrics, { iconColor: isDark ? '#e2e8f0' : '#0f172a' })}</span></p>{/if}
              <p class="m-0">Crime: <span class="font-semibold">{crimeLabel(selected.area.metrics)}</span></p>
              <p class="m-0">Ref ({activeReferencePreset.rooms} rok, {activeReferencePreset.sqm} sqm): <span class="font-semibold">{referencePrice(selected) == null ? 'N/A' : formatSek(referencePrice(selected))}</span></p>
            </div>
            <button
              on:click={toggleSales}
              class={`mt-3 w-full rounded px-3 py-1.5 text-xs font-medium transition-colors ${showSales ? (isDark ? 'bg-cyan-700 text-white' : 'bg-cyan-600 text-white') : (isDark ? 'bg-white/10 text-slate-200 hover:bg-white/20' : 'bg-slate-100 text-slate-700 hover:bg-slate-200')}`}
            >
              {salesLoading ? 'Loading…' : showSales ? 'Hide sales' : `Show recent sales${selected.area.metrics.price_n_listings ? ` (${selected.area.metrics.price_n_listings})` : ''}`}
            </button>
            {#if showSales && salesData}
              <div class={`mt-2 max-h-60 overflow-y-auto rounded text-xs ${isDark ? 'bg-white/5' : 'bg-slate-50'}`}>
                {#if salesData.listings.length === 0}
                  <p class={`px-3 py-2 ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>No sales data for this zone.</p>
                {:else}
                  <div class={`sticky top-0 grid grid-cols-4 gap-1 px-3 py-1.5 font-semibold ${isDark ? 'bg-slate-800 text-slate-300' : 'bg-slate-200 text-slate-600'}`}>
                    <span class="col-span-2">Address</span>
                    <span class="text-right">Price/sqm</span>
                    <span class="text-right">Date</span>
                  </div>
                  {#each salesData.listings as s}
                    <div class={`grid grid-cols-4 gap-1 border-t px-3 py-1 ${isDark ? 'border-white/5 text-slate-300' : 'border-slate-100 text-slate-700'}`}>
                      <span class="col-span-2 truncate" title={s.street_address}>{s.street_address} · {s.rooms} · {s.sqm} m²</span>
                      <span class="text-right">{s.price_per_sqm_sek.toLocaleString('sv-SE')}</span>
                      <span class="text-right">{formatDate(s.sold_at)}</span>
                    </div>
                  {/each}
                  {#if salesData.total > salesData.listings.length}
                    <p class={`px-3 py-1.5 ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>Showing {salesData.listings.length} of {salesData.total} sales</p>
                  {/if}
                {/if}
              </div>
            {/if}
          </div>
        </details>
      {/if}
    </div>

    {#if sidebarWidth === 0}
      <button
        class={`self-center rounded-l-lg border border-r-0 px-1.5 py-3 text-xs shadow-md transition flex-shrink-0 ${isDark ? 'border-white/10 bg-[#080f1e] text-white/60 hover:text-white/90' : 'border-slate-200 bg-white text-slate-400 hover:text-slate-700'}`}
        on:click={() => { sidebarWidth = SIDEBAR_DEFAULT; }}
        title="Show panel"
      >◀</button>
    {:else}
    <aside class="panel relative flex h-full flex-shrink-0 flex-col overflow-hidden" style="width:{sidebarWidth}px">
      <button
        class={`absolute left-0 top-0 z-10 h-full w-1.5 cursor-col-resize opacity-0 hover:opacity-100 transition-opacity ${isDark ? 'bg-white/20' : 'bg-slate-400/40'}`}
        on:pointerdown={startSidebarResize}
        title="Drag to resize"
      ></button>
      <div class="min-w-0 overflow-x-hidden overflow-y-auto pl-2">
      <div class="grid min-w-0 gap-3">
        <div class="rounded-xl border border-slate-300 bg-white p-3">
          <p class="m-0 mb-2 text-sm font-semibold">Constraints</p>
          <div class="grid gap-2">
            <p class={`m-0 text-sm font-semibold ${isDark ? 'text-slate-200' : 'text-slate-800'}`}>Destinations</p>
            {#each destinations as d, i}
              <div class={`min-w-0 rounded-xl border p-2 ${isDark ? 'border-white/10 bg-white/5' : 'border-slate-200 bg-slate-50'}`}>
                <div class="flex gap-1">
                  <input
                    class={`w-full rounded-lg border px-2 py-1.5 text-xs ${isDark ? 'border-white/10 bg-white/10 text-white placeholder:text-white/30' : 'border-slate-300 bg-white text-slate-900'}`}
                    type="text"
                    bind:value={d.query}
                    on:keydown={(e) => { if (e.key === 'Enter') { e.preventDefault(); searchDestination(i); } }}
                    placeholder="e.g. KTH, Stockholm"
                  />
                  <button class={`whitespace-nowrap rounded-lg border px-2 py-1 text-xs font-semibold transition ${isDark ? 'border-white/10 bg-white/10 text-white/70 hover:bg-white/20' : 'border-slate-300 bg-white text-slate-700 hover:bg-slate-50'}`} on:click={() => searchDestination(i)}>Go</button>
                  {#if destinations.length > 1}
                    <button class={`rounded-lg border px-2 py-1 text-xs transition hover:text-red-500 ${isDark ? 'border-white/10 text-white/30' : 'border-slate-200 text-slate-400'}`} on:click={() => removeDestination(i)}>✕</button>
                  {/if}
                </div>
                {#if d.label && d.label !== d.query && Number.isFinite(d.lat)}
                  <p class={`m-0 mt-1 truncate text-xs ${isDark ? 'text-slate-400' : 'text-slate-500'}`} title={d.label}>{d.label}</p>
                {/if}
                {#if d.loading}<p class={`m-0 mt-1 text-xs ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>Searching…</p>{/if}
                {#if d.error}<p class="m-0 mt-1 text-xs text-red-400">{d.error}</p>{/if}
                {#if d.suggestions.length}
                  <div class={`mt-1 grid gap-0.5 rounded-lg border p-1 ${isDark ? 'border-white/10 bg-white/5' : 'border-slate-200 bg-white'}`}>
                    {#each d.suggestions as s}
                      <button type="button" class={`rounded px-2 py-1.5 text-left text-xs transition ${isDark ? 'text-white/70 hover:bg-white/10' : 'hover:bg-slate-100'}`} on:click={() => pickDestination(i, s)}>{s.label}</button>
                    {/each}
                  </div>
                {/if}
                {#if destinations.length > 1}
                  <div class="mt-2 flex items-center gap-2">
                    <span class={`w-12 text-xs ${isDark ? 'text-slate-400' : 'text-slate-500'}`}>Weight</span>
                    <input type="range" min="0" max="100" step="5" bind:value={d.weight} on:input={() => { destinations = destinations; scheduleAutoRefresh(); }} class="flex-1" />
                    <span class={`w-6 text-right text-xs font-medium ${isDark ? 'text-slate-300' : 'text-slate-700'}`}>{d.weight}</span>
                  </div>
                {/if}
              </div>
            {/each}
            <button class={`rounded-xl border border-dashed py-1.5 text-xs transition hover:border-teal-400 hover:text-teal-500 ${isDark ? 'border-white/20 text-white/40' : 'border-slate-300 text-slate-500'}`} on:click={addDestination}>+ Add destination</button>
          </div>
          <label class="grid gap-1 text-sm font-semibold text-slate-800">Municipality
            <select class="rounded-xl border border-slate-300 bg-white px-3 py-2" bind:value={municipality} on:change={scheduleAutoRefresh}>
              {#each municipalityOptions as option}
                <option value={option}>{option || 'All'}</option>
              {/each}
            </select>
          </label>
          <div class="mt-2 grid gap-1">
            <p class="m-0 text-sm font-semibold text-slate-800">Min transit type</p>
            <div class="flex flex-wrap gap-2">
              {#each transitFilterOptions as option}
                <button
                  type="button"
                  class={`rounded-lg border px-2 py-1 text-xs transition ${minTransitType === option.value ? 'border-teal-600 bg-teal-50 font-semibold text-teal-800' : 'border-slate-300 bg-white hover:border-teal-300'}`}
                  on:click={() => { minTransitType = option.value; }}
                >
                  <span class="inline-flex items-center gap-1.5 align-middle">
                    {#if option.value}
                      {@html transitIconSvg(option.value, { size: 12, color: minTransitType === option.value ? '#115e59' : '#475569' })}
                    {/if}
                    <span>{option.label}</span>
                  </span>
                </button>
              {/each}
            </div>
          </div>
          <label class="mt-2 grid gap-1 text-sm font-semibold text-slate-800">Budget cap (max total price)
            <input class="rounded-xl border border-slate-300 bg-white px-3 py-2" type="number" bind:value={budgetCapSek} min="1000000" max="15000000" step="50000" on:input={onBudgetTotalChange} />
            <input class="w-full" type="range" bind:value={budgetCapSek} min="1000000" max="15000000" step="50000" on:input={onBudgetTotalChange} />
            <span class="text-xs font-normal text-slate-500">Applied as max price for {activeReferencePreset.rooms} rok · {activeReferencePreset.sqm} sqm.</span>
          </label>
          <label class="mt-2 grid gap-1 text-sm font-semibold text-slate-800">Budget cap (SEK/sqm)
            <input class="rounded-xl border border-slate-300 bg-white px-3 py-2" type="number" bind:value={budgetCapPerSqm} min="30000" max="150000" step="500" on:input={onBudgetPerSqmChange} />
            <input class="w-full" type="range" bind:value={budgetCapPerSqm} min="30000" max="150000" step="500" on:input={onBudgetPerSqmChange} />
            <span class="text-xs font-normal text-slate-500">Linked to total cap via selected reference size.</span>
          </label>
          <label class="mt-2 grid gap-1 text-sm font-semibold text-slate-800">Max commute to destination (min)
            <input class="rounded-xl border border-slate-300 bg-white px-3 py-2" type="number" bind:value={maxCommute} min="10" max="120" step="1" on:input={scheduleAutoRefresh} />
            <input class="w-full" type="range" bind:value={maxCommute} min="10" max="120" step="1" on:input={scheduleAutoRefresh} />
            <span class="text-xs font-normal text-slate-500">Rush hour (Tue 08:15).</span>
          </label>
        </div>

        <div class="rounded-xl border border-slate-300 bg-white p-3">
          <p class="m-0 mb-2 text-sm font-semibold">What You Value</p>
          <label class="grid gap-1 text-sm font-semibold text-slate-800">Price: {priorityPrice}
            <input type="range" min="0" max="100" step="1" bind:value={priorityPrice} on:input={scheduleAutoRefresh} />
          </label>
          <label class="mt-2 grid gap-1 text-sm font-semibold text-slate-800">Commute: {priorityCommute}
            <input type="range" min="0" max="100" step="1" bind:value={priorityCommute} on:input={scheduleAutoRefresh} />
          </label>
          <label class="mt-2 grid gap-1 text-sm font-semibold text-slate-800">Crime safety: {priorityCrime}
            <input type="range" min="0" max="100" step="1" bind:value={priorityCrime} on:input={scheduleAutoRefresh} />
          </label>
          <div class="mt-3 grid grid-cols-2 gap-2">
            <button class="rounded-lg border border-slate-300 bg-white px-2 py-2 text-xs" on:click={() => applyPreset('balanced')}>Balanced</button>
            <button class="rounded-lg border border-slate-300 bg-white px-2 py-2 text-xs" on:click={() => applyPreset('commute')}>Commute-first</button>
            <button class="rounded-lg border border-slate-300 bg-white px-2 py-2 text-xs" on:click={() => applyPreset('safety')}>Safety-first</button>
            <button class="rounded-lg border border-slate-300 bg-white px-2 py-2 text-xs" on:click={() => applyPreset('price')}>Price-first</button>
          </div>
        </div>

        <div class="rounded-xl border border-slate-300 bg-white p-3">
        </div>

        {#if loading}
          <p class="m-0 text-sm text-slate-600">Loading areas...</p>
        {:else if error}
          <p class="m-0 text-sm font-semibold text-red-700">{error}</p>
        {/if}

        <div class="rounded-xl border border-slate-300 bg-white p-3">
          <p class="m-0 mb-2 text-sm font-semibold">Top matches</p>
          <div class="grid max-h-[34dvh] gap-2 overflow-auto pr-1">
            {#each filteredAreas.slice(0, 20) as item, idx}
              <button
                type="button"
                class={`min-w-0 overflow-hidden rounded-xl border p-3 text-left text-sm transition ${item.area.id === selectedId ? 'border-teal-600 bg-teal-50' : 'border-slate-300 hover:border-teal-300'}`}
                on:click={() => {
                  selectedId = item.area.id;
                  focusAreaOnMap(item.area.id);
                  renderMap();
                }}
              >
                <p class="m-0 text-xs text-slate-500">#{idx + 1} fit</p>
                <p class="m-0 font-semibold">{item.area.name}</p>
                <p class="m-0 text-xs text-slate-600">{item.area.municipality}</p>
                <p class="m-0 mt-1 text-sm font-semibold" style={`color:${scoreColor(item.value_score)}`}>{item.value_score}</p>
                {#if commuteDestLines(item)}
                  {#each commuteDestLines(item) as row}
                    <p class="m-0 mt-0.5 flex gap-1 min-w-0 text-xs text-slate-600">
                      <span class="shrink-0 font-semibold">{row.mins != null ? `${row.mins} min` : 'N/A'}</span>
                      <span class="truncate" title={row.label}>to {row.label}</span>
                    </p>
                  {/each}
                  <p class="m-0 mt-0.5 text-xs text-slate-600">{priceLabel(item.area.metrics)} • {crimeLabel(item.area.metrics)} crime</p>
                {:else}
                  <p class="m-0 mt-1 text-xs text-slate-600">{commuteLabel(item)} commute • {priceLabel(item.area.metrics)} • {crimeLabel(item.area.metrics)} crime</p>
                {/if}
                <p class="m-0 mt-1 text-[11px] text-slate-600">
                  Ref ({activeReferencePreset.rooms} rok, {activeReferencePreset.sqm} sqm): {referencePrice(item) == null ? 'N/A' : formatSek(referencePrice(item))}
                </p>
                <p class="m-0 mt-1 text-[11px] text-slate-500">
                  Price fit: {Math.round(item.breakdown.price ?? item.breakdown.affordability ?? 0)} • Commute fit: {Math.round(item.breakdown.commute ?? item.breakdown.mobility ?? 0)} • Safety fit: {Math.round(item.breakdown.crime ?? item.breakdown.safety ?? 0)}
                </p>
              </button>
            {/each}
          </div>
        </div>

      </div>
      </div>
    </aside>
    {/if}
  </section>
</div>
