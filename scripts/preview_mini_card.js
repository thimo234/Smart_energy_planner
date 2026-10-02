// Synthetic UI preview; no live Home Assistant data or planner calculation.
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
(async () => {
  const output = path.resolve(process.argv[2] || 'mini-card-preview');
  fs.mkdirSync(output, {recursive:true});
  const browser = await chromium.launch({headless:true, executablePath:process.env.CHROME_PATH || 'C:/Program Files/Google/Chrome/Application/chrome.exe'});
  const page = await browser.newPage({viewport:{width:480,height:210}});
  await page.setContent('<body style="margin:0;background:#fafafa;font-family:Arial"><div style="padding:8px;font-size:12px">NSPanel Pro · synthetisch voorbeeld · nu 14:07</div><smart-energy-planner-mini-card></smart-energy-planner-mini-card></body>');
  await page.addScriptTag({path:path.resolve(__dirname,'../custom_components/smart_energy_planner/frontend/smart-energy-planner-card.js')});
  await page.evaluate(() => {
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
    card.setConfig({planner_entity:'sensor.planner',consumption_entity:'sensor.house',solar_entity:'sensor.sun'});
    card._hass={states:{'sensor.planner':{attributes:{upcoming_energy_price_windows:prices,estimated_hourly_home_demand:demand,estimated_hourly_solar_forecast:solar,planned_battery_mode_schedule:[{at:start.toISOString(),mode:'accu_uit'},{at:new Date(+start+11*3600000).toISOString(),mode:'laden_met_zonne_energie'},{at:new Date(+start+16*3600000).toISOString(),mode:'accu_uit'},{at:new Date(+start+18*3600000).toISOString(),mode:'ontladen'}]}}}};
    card._history={'sensor.house':house,'sensor.sun':sun,'sensor.planner':modes};
    card.render();
  });
  const svg=page.locator('svg');
  if((await svg.boundingBox()).height>150) throw Error('Chart exceeds 150 px');
  await page.locator('[data-slot="6"]').dispatchEvent('pointerdown');
  await page.screenshot({path:path.join(output,'mini-left.png')});
  const firstX=await page.locator('[data-tooltip] rect').getAttribute('x');
  await page.locator('[data-slot="19"]').dispatchEvent('pointerdown');
  await page.screenshot({path:path.join(output,'mini-right.png')});
  const secondX=await page.locator('[data-tooltip] rect').getAttribute('x');
  if(+firstX<=167 || +secondX>=385) throw Error('Tooltip side selection failed');
  if(await page.locator('[data-slot]').count() !== 24) throw Error('Expected hourly columns');
  if(!((await page.locator('[data-tooltip]').textContent()).includes('19:00–20:00'))) throw Error('Expected hourly selection');
  await page.locator('[data-slot="19"]').dispatchEvent('pointerdown');
  if(!await page.locator('[data-tooltip] rect').count()) throw Error('Column tap must keep values visible');
  await page.locator('[data-tooltip] rect').click();
  if(await page.locator('[data-tooltip] rect').count()) throw Error('Popup tap must hide values');
  await page.evaluate(() => document.querySelector('smart-energy-planner-mini-card').render());
  if(await page.locator('[data-tooltip] rect').count()) throw Error('Update must preserve hidden selection');
  await page.screenshot({path:path.join(output,'mini-hidden.png')});
  await page.locator('[data-slot="19"]').dispatchEvent('pointerdown');
  if(!await page.locator('[data-tooltip] rect').count()) throw Error('Column tap must show values again');
  const colors = await page.locator('svg').evaluate(svg => {
    const past = [...svg.querySelectorAll('[clip-path]')].filter(el => el.getAttribute('clip-path').includes('-past'));
    const future = [...svg.querySelectorAll('[clip-path]')].filter(el => el.getAttribute('clip-path').includes('-future'));
    return past.every((el,i) => el.getAttribute('fill') === future[i].getAttribute('fill') && el.getAttribute('stroke') === future[i].getAttribute('stroke') && +el.getAttribute('opacity') > +future[i].getAttribute('opacity'));
  });
  if(!colors) throw Error('Forecast must preserve colors with lower opacity');
  const html=await page.content();
  fs.writeFileSync(path.join(output,'preview.html'),html);
  await page.setViewportSize({width:320,height:210});
  await page.screenshot({path:path.join(output,'mini-320.png')});
  console.log('Preview and left/right selection passed at 480 and 320 px');
  await browser.close();
})().catch(error=>{console.error(error);process.exitCode=1;});
