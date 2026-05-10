import assert from 'node:assert/strict';

import {
  MAP_METRIC_OPTIONS,
  commuteBucket,
  constrainedGoodness,
  metricValue,
  normalizeDesoId,
} from './mapMetrics.js';

assert.equal(normalizeDesoId('0186C1020'), 'deso_0186c1020');
assert.equal(normalizeDesoId('deso_0186c1020'), 'deso_0186c1020');
assert.equal(normalizeDesoId(null), null);

assert.deepEqual(commuteBucket(18), { id: '0-20', label: '0-20 min', color: '#0f766e' });
assert.deepEqual(commuteBucket(35), { id: '20-35', label: '20-35 min', color: '#0ea5e9' });
assert.deepEqual(commuteBucket(51), { id: '50-70', label: '50-70 min', color: '#f97316' });
assert.deepEqual(commuteBucket(null), { id: 'na', label: 'N/A', color: '#94a3b8' });

const areaResult = {
  value_score: 72.5,
  breakdown: { commute_minutes: 31 },
  area: {
    metrics: {
      avg_price_sek_per_sqm: 64000,
      median_income_sek: 420000,
    },
  },
};

assert.equal(metricValue(areaResult, 'commute'), 31);
assert.equal(metricValue(areaResult, 'weighted'), 72.5);
assert.equal(metricValue(areaResult, 'fit'), 72.5);
assert.equal(metricValue(areaResult, 'price'), 64000);
assert.equal(metricValue(areaResult, 'income'), 420000);
assert.equal(metricValue(null, 'commute'), null);
assert.deepEqual(
  MAP_METRIC_OPTIONS.map((option) => [option.id, option.label]),
  [
    ['commute', 'Commute time'],
    ['weighted', 'Hybrid'],
    ['price', 'Price/sqm'],
    ['safety', 'Safety'],
  ],
);

assert.equal(metricValue({ ...areaResult, breakdown: { ...areaResult.breakdown, crime: 81 } }, 'safety'), 81);

assert.equal(
  constrainedGoodness(areaResult, 'weighted', 0.82, { budgetCapPerSqm: 60000, maxCommute: 35 }),
  0.32,
);
assert.equal(
  constrainedGoodness(areaResult, 'weighted', 0.82, { budgetCapPerSqm: 70000, maxCommute: 30 }),
  0.32,
);
assert.equal(
  constrainedGoodness(areaResult, 'weighted', 0.82, { budgetCapPerSqm: 70000, maxCommute: 35 }),
  0.82,
);
