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
assert.equal(sourcePoints[0].time.toISOString(), new Date("2026-08-07T12:00:00+02:00").toISOString());
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

const runningDayPoints = card.extractSolarPoints(
  {
    attributes: {
      estimated_hourly_solar_forecast: [{
        start: "2026-08-07T12:00:00+02:00",
        end: "2026-08-07T12:30:00+02:00",
        estimated_kw: 3.2,
      }],
    },
  },
  new Date("2026-08-07T11:30:00+02:00"),
  new Date("2026-08-07T13:00:00+02:00"),
);
assert.equal(
  runningDayPoints.some((point) => point.synthetic && point.time < sourcePoints[0].time),
  false,
  "a short leading gap must not pull the running forecast curve down to zero",
);

const previewState = {
  state: 'accu_uit',
  attributes: {
    planned_battery_mode_schedule: [
      {at: '2026-08-07T12:00:00+02:00', mode: 'ontladen'},
      {at: '2026-08-07T13:00:00+02:00', mode: 'accu_uit'},
    ],
    estimated_battery_mode_windows: [
      {start: '2026-08-07T13:00:00+02:00', end: '2026-08-07T14:00:00+02:00', mode: 'laden_met_zonne_energie'},
    ],
  },
};
const previewSchedule = card.extractModeSchedule(previewState, horizonStart, horizonEnd);
assert.equal(card.isPreviewTime(previewState, new Date('2026-08-07T12:30:00+02:00')), false);
assert.equal(card.isPreviewTime(previewState, new Date('2026-08-07T13:30:00+02:00')), true);
assert.equal(previewSchedule[0].mode, 'ontladen');
assert.equal(previewSchedule[1].mode, 'laden_met_zonne_energie');
assert.equal(previewState.attributes.planned_battery_mode_schedule[1].mode, 'accu_uit', 'preview must not mutate live schedule');
delete previewState.attributes.estimated_battery_mode_windows;
assert.equal(card.extractModeSchedule(previewState, horizonStart, horizonEnd)[1].mode, 'accu_uit');
console.log("frontend card solar forecast and estimated schedule checks passed");
