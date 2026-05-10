export const COMMUTE_BUCKETS = [
  { id: '0-20', label: '0-20 min', color: '#0f766e', max: 20 },
  { id: '20-35', label: '20-35 min', color: '#0ea5e9', max: 35 },
  { id: '35-50', label: '35-50 min', color: '#eab308', max: 50 },
  { id: '50-70', label: '50-70 min', color: '#f97316', max: 70 },
  { id: '70+', label: '70+ min', color: '#dc2626', max: Infinity },
];

export const NA_BUCKET = { id: 'na', label: 'N/A', color: '#94a3b8' };

export const MAP_METRIC_OPTIONS = [
  { id: 'commute', label: 'Commute time' },
  { id: 'weighted', label: 'Hybrid' },
  { id: 'price', label: 'Price/sqm' },
  { id: 'safety', label: 'Safety' },
];

export function normalizeDesoId(value) {
  if (!value) return null;
  const raw = String(value).trim().toLowerCase();
  return raw.startsWith('deso_') ? raw : `deso_${raw}`;
}

export function commuteBucket(minutes) {
  if (!Number.isFinite(minutes)) return NA_BUCKET;
  const bucket = COMMUTE_BUCKETS.find((candidate) => minutes <= candidate.max);
  if (!bucket) return NA_BUCKET;
  return { id: bucket.id, label: bucket.label, color: bucket.color };
}

export function metricValue(item, metric) {
  if (!item) return null;
  const metrics = item.area?.metrics ?? {};
  if (metric === 'commute') return item.breakdown?.commute_minutes ?? null;
  if (metric === 'weighted') return item.value_score ?? null;
  if (metric === 'fit') return item.value_score ?? null;
  if (metric === 'price') return metrics.avg_price_sek_per_sqm ?? null;
  if (metric === 'safety') return item.breakdown?.crime ?? item.breakdown?.safety ?? null;
  if (metric === 'income') return metrics.median_income_sek ?? null;
  return item.value_score ?? null;
}

export function constrainedGoodness(item, metric, goodness, { budgetCapPerSqm, maxCommute }) {
  if (metric !== 'weighted' || !item) return goodness;

  const price = item.area?.metrics?.avg_price_sek_per_sqm;
  const commute = item.breakdown?.commute_minutes ?? item.area?.metrics?.sl_commute_to_tcentralen_min;
  const overBudget = Number.isFinite(price) && Number.isFinite(budgetCapPerSqm) && price > budgetCapPerSqm;
  const overCommute = Number.isFinite(commute) && Number.isFinite(maxCommute) && commute > maxCommute;

  return overBudget || overCommute ? Math.min(goodness, 0.32) : goodness;
}
