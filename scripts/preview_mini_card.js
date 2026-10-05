// Synthetic UI preview; no live Home Assistant data or planner calculation.
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
(async () => {
  const output = path.resolve(process.argv[2] || 'mini-card-preview');
  fs.mkdirSync(output, {recursive:true});
  const browser = await chromium.launch({headless:true, executablePath:process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe'});
  const page = await browser.newPage({viewport:{width:480,height:210}});
  await page.setContent('<body style="margin:0;background:#fafafa;font-family:Arial"><div style="padding:8px;font-size:11px">Nord Pool-prijzen · verbruik/zon synthetisch · nu 14:07</div><smart-energy-planner-mini-card></smart-energy-planner-mini-card></body>');
  await page.addScriptTag({path:path.resolve(__dirname,'../custom_components/smart_energy_planner/frontend/smart-energy-planner-card.js')});
  const priceFixture = JSON.parse(fs.readFileSync(path.resolve(__dirname,'../tests/fixtures/nspanel_nordpool_2026_10_02.json'),'utf8'));
  await page.evaluate((priceFixture) => {
    const NativeDate = Date;
    window.Date = class extends NativeDate {
      constructor(...args) { super(...(args.length ? args : ['2026-10-02T14:07:00+02:00'])); }
      static now() { return +new NativeDate('2026-10-02T14:07:00+02:00'); }
    };
    const start = new Date(); start.setHours(0,0,0,0);
    const prices=[], demand=[], solar=[], house=[], sun=[], modes=[];
    for(let i=0;i<96;i++) {
      const a=new Date(+start+i*900000), b=new Date(+a+900000), h=i/4;
      const pv=Math.max(0,3.5*Math.sin((h-6)/12*Math.PI));
      const load=.35+(h>17&&h<21 ? 1.5 : .2)+.15*Math.sin(i);
      prices.push({start:a.toISOString(),end:b.toISOString(),price:.18+.10*Math.cos((h-19)/12*Math.PI)});
      demand.push({start:a.toISOString(),end:b.toISOString(),estimated_kwh:load/4});
      solar.push({start:a.toISOString(),end:b.toISOString(),estimated_kw:pv});
      if(a<new Date()) {
        house.push({entity_id:'sensor.house',state:String(load*1100),last_changed:a.toISOString(),attributes:{unit_of_measurement:'W'}});
        sun.push({entity_id:'sensor.sun',state:String(pv*.95),last_changed:a.toISOString(),attributes:{unit_of_measurement:'kW'}});
        modes.push({entity_id:'sensor.planner',state:h<8?'ontladen':h<11?'accu_uit':'laden_met_zonne_energie',last_changed:a.toISOString()});
      }
    }
    const card=document.querySelector('smart-energy-planner-mini-card');
    card.setConfig({planner_entity:'sensor.planner',consumption_entity:'sensor.house',solar_entity:'sensor.sun',soc_entity:'sensor.battery_soc'});
    card._hass={states:{'sensor.planner':{attributes:{upcoming_energy_price_windows:prices,estimated_hourly_home_demand:demand,estimated_hourly_solar_forecast:solar,planned_battery_mode_schedule:[{at:start.toISOString(),mode:'accu_uit'},{at:new Date(+start+11*3600000).toISOString(),mode:'laden_met_zonne_energie'},{at:new Date(+start+16*3600000).toISOString(),mode:'accu_uit'},{at:new Date(+start+18*3600000).toISOString(),mode:'ontladen'}]}}}};
    card._history={'sensor.house':house,'sensor.sun':sun,'sensor.planner':modes};
    card._hass.states['sensor.battery_soc'] = {state:'52',attributes:{unit_of_measurement:'%'}};
    card._history['sensor.battery_soc'] = [{last_changed:start.toISOString(),state:'63'},{last_changed:new Date(+start+12*3600000).toISOString(),state:'52'}];
    card.config.price_entity = priceFixture.entity_id;
    card._hass.states[priceFixture.entity_id] = {attributes:{raw_today:priceFixture.raw_today}};
    card.render();
  },priceFixture);
  if (process.argv.includes('--real-history')) {
    const actual = JSON.parse(fs.readFileSync(path.resolve(__dirname,'../tests/fixtures/nspanel_consumption_history_2026_10_02.json'),'utf8'));
    await page.evaluate(actual => {
      const BaseDate = Date;
      window.Date = class extends BaseDate {
        constructor(...args) { super(...(args.length ? args : [actual.now])); }
        static now() { return +new BaseDate(actual.now); }
      };
      document.querySelector('body > div').textContent = 'Verbruik uit CSV · Nord Pool-prijzen · zon/voorspelling synthetisch';
      const card = document.querySelector('smart-energy-planner-mini-card');
      card._hass.states['sensor.house'] = {attributes:actual.attributes};
      card._history['sensor.house'] = actual.rows.map(([last_changed,state]) => ({last_changed,state}));
      card.render();
    },actual);
  }
  if (process.argv.includes('--blend-mismatch')) {
    await page.evaluate(() => {
      const card = document.querySelector('smart-energy-planner-mini-card');
      card._history['sensor.sun'] = card._history['sensor.sun'].map(row => ({...row,state:String(Number(row.state)*1.6)}));
      card.render();
    });
  }
  if (process.argv.includes('--total-corrections') || process.argv.includes('--real-history')) {
    if (!process.argv.includes('--real-history')) {
    await page.evaluate(() => {
      const card = document.querySelector('smart-energy-planner-mini-card');
      const start = new Date(); start.setHours(0,0,0,0);
      const rows = [];
      for (let minute=0;minute<=14*60+7;minute++) {
        const time = new Date(+start+minute*60000);
        const correction = minute === 10*60+5 ? 3 : minute === 11*60+35 ? 4 : 0;
        rows.push({entity_id:'sensor.house',state:String(14000+minute*.6/60+correction),last_changed:time.toISOString()});
      }
      card._hass.states['sensor.house'] = {attributes:{unit_of_measurement:'kWh',state_class:'total'}};
      card._history['sensor.house'] = rows;
      card.render();
    });
    }
    await page.screenshot({path:path.join(output,'mini-total-corrected.png')});
    await page.evaluate(() => {
      const card = document.querySelector('smart-energy-planner-mini-card');
      card._correctedRecordedPower = card.recordedPower;
      card.recordedPower = function(entity,start,end) {
        const corrected = this._correctedRecordedPower(entity,start,end);
        if (entity !== 'sensor.house') return corrected;
        const rows = this._history[entity]; let energy = 0, duration = 0;
        for (let i=0;i<rows.length-1;i++) {
          const a = +new Date(rows[i].last_changed), b = +new Date(rows[i+1].last_changed);
          const overlap = Math.max(0,Math.min(+end,b)-Math.max(+start,a));
          const delta = Number(rows[i+1].state)-Number(rows[i].state);
          if (overlap && delta >= 0) { energy += delta*overlap/(b-a); duration += overlap; }
        }
        return duration ? energy*3600000/duration : undefined;
      };
      card.render();
    });
    await page.screenshot({path:path.join(output,'mini-total-before.png')});
    await page.evaluate(() => {
      const card = document.querySelector('smart-energy-planner-mini-card');
      card.recordedPower = card._correctedRecordedPower; card.render();
    });
  }
  const svg=page.locator('svg');
  if((await svg.boundingBox()).height>150) throw Error('Chart exceeds 150 px');
  const anchored = await page.evaluate(() => {
    const card = document.querySelector('smart-energy-planner-mini-card'), now = new Date();
    const {start,end} = card.miniSlots(card._hass.states[card.config.planner_entity],now);
    const points = card.miniHourlyPower(card._hass.states[card.config.planner_entity],now,start,end);
    const last = points.filter(p => !p.future).at(-1);
    const max = Math.max(1,...points.flatMap(p => [p.demand,p.solar]).filter(Number.isFinite));
    const x = 8 + (+now-+start)/(+end-+start)*462;
    for (const [key,color] of [['solar','#ffda37'],['demand','#c25bc9']]) {
      if (!Number.isFinite(last?.[key])) continue;
      const path = card.querySelector(`path[stroke="${color}"]`);
      let a=0,b=path.getTotalLength();
      for(let i=0;i<40;i++) { const middle=(a+b)/2; if(path.getPointAtLength(middle).x < x) a=middle; else b=middle; }
      const actual = path.getPointAtLength((a+b)/2).y;
      if (Math.abs(actual-(126-last[key]/max*114))>.02) return false;
    }
    return true;
  });
  if (!anchored) throw Error('History/forecast seam must anchor to the last measured hourly value');
  await page.locator('[data-slot="6"]').dispatchEvent('pointerdown');
  if (!(await page.locator('[data-tooltip]').textContent()).includes('Accu om 06:00: 63%')) throw Error('Historical details must show the recorded SOC, not planner score');
  await page.screenshot({path:path.join(output,'mini-left.png')});
  await page.evaluate(() => { document.body.style.background='#111'; document.body.style.setProperty('--primary-text-color','#eee'); document.body.style.setProperty('--card-background-color','#111'); document.body.style.color='#eee'; });
  await page.screenshot({path:path.join(output,'mini-left-dark.png')});
  await page.evaluate(() => { document.body.style.background='#fafafa'; document.body.style.removeProperty('--primary-text-color'); document.body.style.removeProperty('--card-background-color'); document.body.style.removeProperty('color'); });
  const firstX=await page.locator('[data-tooltip] rect').getAttribute('x');
  await page.locator('[data-slot="19"]').dispatchEvent('pointerdown');
  if(await page.locator('[data-tooltip] rect').count()) throw Error('Any column tap must dismiss open details');
  await page.locator('[data-slot="19"]').dispatchEvent('pointerdown');
  await page.screenshot({path:path.join(output,'mini-right.png')});
  const secondX=await page.locator('[data-tooltip] rect').getAttribute('x');
  if(+firstX<=123 || +secondX>=373) throw Error('Tooltip side selection failed');
  if(await page.locator('[data-slot]').count() !== 24) throw Error('Expected hourly columns');
  if(!((await page.locator('[data-tooltip]').textContent()).includes('19:00–20:00'))) throw Error('Expected hourly selection');
  await page.locator('[data-slot="19"]').dispatchEvent('pointerdown');
  if(await page.locator('[data-tooltip] rect').count()) throw Error('Column tap must dismiss values');
  await page.locator('[data-slot="19"]').dispatchEvent('pointerdown');
  await page.locator('[data-tooltip] rect').click();
  if(await page.locator('[data-tooltip] rect').count()) throw Error('Popup tap must hide values');
  await page.evaluate(() => document.querySelector('smart-energy-planner-mini-card').render());
  if(await page.locator('[data-tooltip] rect').count()) throw Error('Update must preserve hidden selection');
  await page.screenshot({path:path.join(output,'mini-hidden.png')});
  await page.locator('[data-slot="19"]').dispatchEvent('pointerdown');
  if(!await page.locator('[data-tooltip] rect').count()) throw Error('Column tap must show values again');
  await page.locator('svg').click({position:{x:10,y:10}});
  if(await page.locator('[data-tooltip] rect').count()) throw Error('Axis tap must dismiss values');
  await page.locator('[data-slot="19"]').dispatchEvent('pointerdown');
  if ((await page.locator('ha-card').evaluate(el => getComputedStyle(el).borderTopWidth)) !== '0px') throw Error('Card border must be absent');
  if(await page.locator('[data-mode-band]:not([rx="3"])').count()) throw Error('Mode backgrounds must have small rounded corners');
  if(!((await page.locator('path[stroke="#ffda37"]').first().getAttribute('d')).includes('C '))) throw Error('Power curves must use cubic interpolation');
  const colors = await page.locator('svg').evaluate(svg => {
    const past = [...svg.querySelectorAll('[clip-path]')].filter(el => el.getAttribute('clip-path').includes('-past'));
    const future = [...svg.querySelectorAll('[clip-path]')].filter(el => el.getAttribute('clip-path').includes('-future'));
    return past.every((el,i) => el.getAttribute('fill') === future[i].getAttribute('fill') && el.getAttribute('stroke') === future[i].getAttribute('stroke') && (+el.getAttribute('opacity') > +future[i].getAttribute('opacity') || future[i].getAttribute('mask')?.includes('-blend')));
  });
  if(!colors) throw Error('Forecast must preserve colors with lower opacity');
  const bases = await page.locator('[data-price-base]').evaluateAll(elements => elements.every(el => getComputedStyle(el).fill !== 'none' && getComputedStyle(el).opacity === '1'));
  if (!bases || await page.locator('[data-price-base]').count() !== 24) throw Error('Price columns require opaque neutral bases to prevent mode tinting');
  const continuous = await page.locator('path[stroke="#c25bc9"]').first().getAttribute('d');
  if ((continuous.match(/M /g) || []).length !== 1) throw Error('Available history/forecast should join as one smooth curve');
  const html=await page.content();
  fs.writeFileSync(path.join(output,'preview.html'),html);
  await page.setViewportSize({width:320,height:210});
  await page.screenshot({path:path.join(output,'mini-320.png')});
  const layout = await page.locator('svg').evaluate(svg => ![...svg.querySelectorAll('text')].some(el => ['kW','€/kWh'].includes(el.textContent)));
  if (!layout) throw Error('Left scales must be absent');
  const editorCheck = await page.evaluate(() => {
    // A focused picker stub reproduces the editor lifecycle during HA updates.
    // Actual HA picker behavior still needs installation verification.
    customElements.define('ha-entity-picker', class extends HTMLElement {
      connectedCallback() {
        if (!this.shadowRoot) this.attachShadow({mode:'open'}).innerHTML = '<input aria-label="Zoeken">';
      }
    });
    const editor = document.createElement('smart-energy-planner-mini-card-editor');
    document.body.append(editor);
    const config = {planner_entity:'sensor.planner'};
    editor.setConfig(config); editor.hass = {states:{}};
    const picker = editor.querySelector('[data-key="consumption_entity"]');
    const input = picker.shadowRoot.querySelector('input');
    input.focus(); input.value = 'huis';
    for (let i=0;i<5;i++) { editor.hass = {states:{}}; editor.setConfig({...config}); }
    const stable = editor.querySelector('[data-key="consumption_entity"]') === picker && picker.shadowRoot.activeElement === input && input.value === 'huis';
    let events = 0;
    editor.addEventListener('config-changed', () => { events++; editor.setConfig({...editor.config}); });
    picker.value = 'sensor.house';
    picker.dispatchEvent(new CustomEvent('value-changed',{bubbles:true,detail:{value:'sensor.house'}}));
    const solar = editor.querySelector('[data-key="solar_entity"]');
    solar.value = 'sensor.sun';
    solar.dispatchEvent(new CustomEvent('value-changed',{bubbles:true,detail:{value:'sensor.sun'}}));
    const result = stable && events === 2 && editor.config.consumption_entity === 'sensor.house' && editor.config.solar_entity === 'sensor.sun';
    editor.remove(); return result;
  });
  if (!editorCheck) throw Error('Editor failed focus/search/selection persistence');
  const refreshCheck = await page.evaluate(async () => {
    const originalNow = Date.now;
    let clock = originalNow(), requests = 0, renders = 0;
    Date.now = () => clock;
    const card = document.createElement('smart-energy-planner-mini-card');
    card.setConfig({planner_entity:'sensor.planner'});
    card.render = () => { renders++; };
    const hass = {states:{},callApi:async (_method,url) => {
      if (!url.includes('minimal_response')) throw Error('History payload is not compact');
      requests++; return [];
    }};
    card._hass = hass; document.body.append(card);
    await new Promise(resolve => setTimeout(resolve,0));
    for (let i=0;i<500;i++) card.hass = {...hass};
    await card.refreshMini();
    const quiet = requests === 1 && renders === 1;
    clock += 299999; await card.refreshMini();
    const throttled = requests === 1 && renders === 1;
    clock++; await card.refreshMini();
    const periodic = requests === 2 && renders === 2;
    card.remove(); clock += 300000; await card.refreshMini();
    const stopped = requests === 2 && renders === 2;
    Date.now = originalNow;
    return quiet && throttled && periodic && stopped;
  });
  if (!refreshCheck) throw Error('Mini refresh throttling failed');
  console.log('Preview, selection, scale spacing and persistent editor checks passed');
  await browser.close();
})().catch(error=>{console.error(error);process.exitCode=1;});
