"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");

global.HTMLElement = class {};
global.window = {};
const registered = new Map();
global.customElements = {
  define(name, constructor) {
    registered.set(name, constructor);
  },
};

require(path.resolve(
  __dirname,
  "../custom_components/smart_energy_planner/frontend/smart-energy-planner-card.js",
));

const Card = registered.get("smart-energy-planner-card");
assert.ok(Card, "card custom element should be registered");

const card = new Card();
const horizonStart = new Date("2026-08-07T12:00:00+02:00");
const horizonEnd = new Date("2026-08-07T14:00:00+02:00");
const points = card.extractSolarPoints(
  {
    attributes: {
      estimated_hourly_solar_forecast: [
        {
          start: "2026-08-07T12:00:00+02:00",
          end: "2026-08-07T12:30:00+02:00",
          estimated_kwh: 1.0,
          estimated_kw: 2.0,
        },
        {
          start: "2026-08-07T12:30:00+02:00",
          end: "2026-08-07T13:00:00+02:00",
          estimated_kwh: 2.0,
          estimated_kw: 4.0,
        },
      ],
    },
  },
  horizonStart,
  horizonEnd,
);

const sourcePoints = points.filter((point) => !point.synthetic);
const gapPoints = points.filter((point) => point.synthetic);
assert.deepEqual(sourcePoints.map((point) => point.value), [2.0, 4.0]);
assert.deepEqual(gapPoints.map((point) => point.value), [0, 0]);
assert.equal(gapPoints[0].start.toISOString(), new Date("2026-08-07T13:00:00+02:00").toISOString());

const line = card.linePath(
  points,
  (date) => date.getTime() / 60000,
  (value) => 100 - value,
);
sourcePoints.forEach((point) => {
  const coordinate = `${(point.time.getTime() / 60000).toFixed(2)} ${(100 - point.value).toFixed(2)}`;
  assert.ok(line.includes(coordinate), `curve should pass through ${coordinate}`);
});

console.log("frontend card solar forecast checks passed");
