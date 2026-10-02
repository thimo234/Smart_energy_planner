// Replay exported counter history through the actual card calculation.
const path = require('node:path');
global.HTMLElement = class {};
global.window = {};
const registered = new Map();
global.customElements = {define:(name,type) => registered.set(name,type)};
require('../custom_components/smart_energy_planner/frontend/smart-energy-planner-card.js');
const fixture = require(path.resolve(process.argv[2] || 'tests/fixtures/nspanel_consumption_history_2026_10_02.json'));
const card = new (registered.get('smart-energy-planner-mini-card'))();
card.setConfig({planner_entity:'sensor.planner',consumption_entity:fixture.entity_id});
const rows = fixture.rows.map(([last_changed,state]) => ({last_changed,state}));
card._history = {[fixture.entity_id]:rows};
card._hass = {states:{[fixture.entity_id]:{attributes:fixture.attributes}}};
const now = new Date(fixture.now), start = new Date(now); start.setHours(0,0,0,0);
const quarters = [];
for (let t=+start;t<+now;t+=900000) {
  const a = new Date(t), b = new Date(Math.min(t+900000,+now));
  const corrected = card.recordedPower(fixture.entity_id,a,b);
  let energy=0,covered=0;
  rows.forEach((row,i) => {
    const next = rows[i+1]; if (!next) return;
    const x=+new Date(row.last_changed), y=+new Date(next.last_changed);
    const delta=Number(next.state)-Number(row.state), overlap=Math.max(0,Math.min(+b,y)-Math.max(+a,x));
    if(y>x && delta>=0 && overlap) { energy+=delta*overlap/(y-x); covered+=overlap; }
  });
  quarters.push({start:a.toISOString(),end:b.toISOString(),old:covered?energy*3600000/covered:null,corrected});
}
console.log(JSON.stringify({quarters,oldMax:Math.max(...quarters.map(q=>q.old || 0)),correctedMax:Math.max(...quarters.map(q=>q.corrected || 0)),correctedMin:Math.min(...quarters.map(q=>q.corrected || 0))},null,2));
