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
const Mini = registered.get('smart-energy-planner-mini-card');
assert.ok(Mini);
const mini = new Mini();
mini.setConfig({planner_entity:'sensor.planner', consumption_entity:'sensor.house', solar_entity:'sensor.sun'});
mini._hass = {states:{'sensor.house':{attributes:{unit_of_measurement:'W'}}}};
mini._history = {'sensor.house':[
  {state:'1000', last_changed:'2026-08-07T10:00:00Z', attributes:{unit_of_measurement:'W'}},
  {state:'3000', last_changed:'2026-08-07T10:15:00Z', attributes:{unit_of_measurement:'W'}},
  {state:'unavailable', last_changed:'2026-08-07T10:30:00Z'}
]};
assert.equal(mini.recordedPower('sensor.house',new Date('2026-08-07T10:00:00Z'),new Date('2026-08-07T10:30:00Z')),2);
assert.equal(mini.recordedPower('sensor.house',new Date('2026-08-07T10:30:00Z'),new Date('2026-08-07T11:00:00Z')),undefined);
assert.equal(mini.forecastPower([{start:'2026-08-07T10:00:00Z',end:'2026-08-07T10:30:00Z',estimated_kwh:1}],new Date('2026-08-07T10:00:00Z'),new Date('2026-08-07T10:30:00Z')),2);
const splitNow = new Date('2026-08-07T12:07:00');
mini._history = {};
const miniDay = mini.miniSlots({attributes:{}},splitNow);
assert.equal(miniDay.start.getHours(),0);
assert.equal(miniDay.end.getDate(),miniDay.start.getDate()+1);
assert.ok(miniDay.slots.some(s => +s.end === +splitNow && !s.future));
assert.ok(miniDay.slots.some(s => +s.start === +splitNow && s.future));
assert.ok(miniDay.slots.every(s => s.demand === undefined), 'missing measurements and forecasts must remain missing');
assert.ok(registered.get('smart-energy-planner-mini-card-editor'));
mini._hass.states['sensor.energy'] = {attributes:{unit_of_measurement:'kWh',state_class:'total_increasing'}};
mini._history['sensor.energy'] = [
  {state:'100',last_changed:'2026-08-07T10:00:00Z'},
  {state:'101',last_changed:'2026-08-07T10:30:00Z'},
  {state:'0.5',last_changed:'2026-08-07T11:00:00Z'},
];
assert.equal(mini.recordedPower('sensor.energy',new Date('2026-08-07T10:00:00Z'),new Date('2026-08-07T10:30:00Z')),2);
assert.equal(mini.recordedPower('sensor.energy',new Date('2026-08-07T10:30:00Z'),new Date('2026-08-07T11:00:00Z')),1, 'counter reset accounts only for the new reading');
assert.equal(mini.recordedPower('sensor.energy',new Date('2026-08-07T11:00:00Z'),new Date('2026-08-07T11:15:00Z')),undefined, 'energy counter must not extrapolate after its last reading');
mini._hass.states['sensor.total'] = {attributes:{unit_of_measurement:'kWh',state_class:'total'}};
mini._history['sensor.total'] = [
  {state:'1000',last_changed:'2026-08-07T10:00:00Z'},
  {state:'1003',last_changed:'2026-08-07T10:05:00Z'},
  {state:'1000',last_changed:'2026-08-07T10:05:02Z'},
  {state:'1000.5',last_changed:'2026-08-07T10:15:00Z'},
];
assert.ok(Math.abs(mini.recordedPower('sensor.total',new Date('2026-08-07T10:00:00Z'),new Date('2026-08-07T10:15:00Z'))-2) < 1e-8, 'total corrections must cancel: 0.5 kWh over 15 min is 2 kW, not an invented spike');
assert.equal(mini.recordedPower('sensor.total',new Date('2026-08-07T10:05:00Z'),new Date('2026-08-07T10:05:02Z')),-5400, 'retain signed total corrections until period aggregation');
const actualConsumption = require('./fixtures/nspanel_consumption_history_2026_10_02.json');
mini._hass.states[actualConsumption.entity_id] = {attributes:actualConsumption.attributes};
mini._history[actualConsumption.entity_id] = actualConsumption.rows.map(([last_changed,state]) => ({last_changed,state}));
const actualStart = +new Date('2026-10-02T00:00:00+02:00'), actualEnd = +new Date(actualConsumption.now);
const actualQuarters = [];
for (let t=actualStart;t<actualEnd;t+=900000) {
  const end = Math.min(t+900000,actualEnd);
  actualQuarters.push({power:mini.recordedPower(actualConsumption.entity_id,new Date(t),new Date(end)),duration:end-t});
}
assert.ok(Math.abs(Math.max(...actualQuarters.map(q=>q.power))-1.720704404669761)<1e-8, 'actual CSV must no longer reproduce the false 49.74 kW peak');
const uniqueReadings = [...new Map(actualConsumption.rows.map(([time,state])=>[+new Date(time),Number(state)])).entries()].sort((a,b)=>a[0]-b[0]);
const counterAt = time => {
  const i = uniqueReadings.findIndex(([t]) => t>=time);
  if (uniqueReadings[i][0]===time) return uniqueReadings[i][1];
  const [a,va] = uniqueReadings[i-1], [b,vb] = uniqueReadings[i];
  return va+(vb-va)*(time-a)/(b-a);
};
const calculatedEnergy = actualQuarters.reduce((sum,q)=>sum+q.power*q.duration/3600000,0);
assert.ok(Math.abs(calculatedEnergy-(counterAt(actualEnd)-counterAt(actualStart)))<1e-8, 'displayed energy must equal the independently interpolated start/end counter difference');
mini.config.consumption_entity = actualConsumption.entity_id;
const hourlyActual = mini.miniHourlyPower({attributes:{}},new Date(actualEnd),new Date(actualStart),new Date(actualStart+86400000));
assert.equal(hourlyActual.length,25, 'current hour must retain separate measured and forecast parts');
assert.ok(hourlyActual.every(p => p.future ? p.demand === undefined : Number.isFinite(p.demand)), 'missing forecasts must not substitute for recorded values');
const hourlyEnergy = hourlyActual.filter(p => !p.future).reduce((sum,p)=>sum+p.demand*(+p.end-+p.start)/3600000,0);
assert.ok(Math.abs(hourlyEnergy-calculatedEnergy)<1e-8, 'hourly aggregation must conserve the actual CSV energy');
const duplicateStart = +new Date('2026-10-01T16:30:00Z'), duplicateEnd = duplicateStart+900000;
const duplicatePower = mini.recordedPower(actualConsumption.entity_id,new Date(duplicateStart),new Date(duplicateEnd));
assert.ok(Math.abs(duplicatePower/4-(counterAt(duplicateEnd)-counterAt(duplicateStart)))<1e-8, 'same-timestamp corrections in the actual CSV must not create thousands of kWh');
const nordpoolFixture = require('./fixtures/nspanel_nordpool_2026_10_02.json');
mini.config.price_entity = nordpoolFixture.entity_id;
mini._hass.states[nordpoolFixture.entity_id] = {attributes:{raw_today:nordpoolFixture.raw_today}};
const priceDay = mini.miniSlots({attributes:{upcoming_energy_price_windows:[]}},new Date('2026-10-02T14:07:00+02:00'));
assert.equal(priceDay.slots.find(s => +s.start === +new Date('2026-10-02T00:00:00+02:00')).price,.356);
assert.equal(mini.weightedAveragePrice(new Date('2026-10-02T00:00:00+02:00'),new Date('2026-10-02T01:00:00+02:00'),mini.extractAllPriceWindows(undefined,mini._hass.states[nordpoolFixture.entity_id])),(.356+.348+.338+.335)/4);
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
