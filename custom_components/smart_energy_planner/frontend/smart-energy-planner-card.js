class SmartEnergyPlannerCard extends HTMLElement {
  static getConfigElement() {
    return document.createElement("smart-energy-planner-card-editor");
  }

  static getStubConfig(hass) {
    const states = Object.keys(hass?.states || {});
    const priceEntity = states.find((entityId) => {
      const attributes = hass.states[entityId]?.attributes || {};
      return Array.isArray(attributes.raw_today) || Array.isArray(attributes.raw_tomorrow);
    });
    const demandEntity = states.find((entityId) => {
      const attributes = hass.states[entityId]?.attributes || {};
      return Array.isArray(attributes.estimated_hourly_home_demand);
    });
    const plannerEntity = states.find((entityId) => {
      const attributes = hass.states[entityId]?.attributes || {};
      return Array.isArray(attributes.planned_battery_mode_schedule);
    });

    return {
      price_entity: priceEntity || "sensor.energy_price",
      demand_entity: demandEntity || "sensor.smart_energy_planner_estimated_home_demand_today",
      planner_entity: plannerEntity || "sensor.smart_energy_planner_battery_strategy",
      hours_to_show: 24,
    };
  }

  setConfig(config) {
    if (!config.price_entity) {
      throw new Error("price_entity is required");
    }
    this.config = {
      title: "Energieprijs planning",
      hours_to_show: 24,
      show_legend: true,
      ...config,
    };
  }

  set hass(hass) {
    this._hass = hass;
    this.render();
  }

  getCardSize() {
    return 4;
  }

  render() {
    if (!this.config || !this._hass) {
      return;
    }

    const priceState = this._hass.states[this.config.price_entity];
    const demandState = this.config.demand_entity
      ? this._hass.states[this.config.demand_entity]
      : undefined;
    const plannerState = this.config.planner_entity
      ? this._hass.states[this.config.planner_entity]
      : undefined;

    if (!priceState) {
      this.innerHTML = this.renderError(`Price entity not found: ${this.config.price_entity}`);
      return;
    }

    const now = new Date();
    const horizonEnd = new Date(now.getTime() + Number(this.config.hours_to_show) * 60 * 60 * 1000);
    const priceWindows = this.extractPriceWindows(priceState, now, horizonEnd);
    const demandPoints = this.extractDemandPoints(demandState, now, horizonEnd);
    const modeSchedule = this.extractModeSchedule(plannerState, now, horizonEnd);

    if (!priceWindows.length) {
      this.innerHTML = this.renderError("No price windows found on raw_today/raw_tomorrow");
      return;
    }

    this.innerHTML = `
      <ha-card>
        <div class="card">
          <div class="header">
            <div>
              <div class="title">${this.escape(this.config.title)}</div>
              <div class="subtitle">${this.formatRange(now, horizonEnd)}</div>
            </div>
            <div class="badge">${this.escape(String(plannerState?.state || ""))}</div>
          </div>
          ${this.renderChart(priceWindows, demandPoints, modeSchedule, now, horizonEnd)}
          ${this.config.show_legend ? this.renderLegend() : ""}
        </div>
      </ha-card>
      ${this.renderStyles()}
    `;
  }

  renderChart(priceWindows, demandPoints, modeSchedule, horizonStart, horizonEnd) {
    const width = 960;
    const height = 320;
    const pad = { top: 20, right: 38, bottom: 42, left: 54 };
    const plotWidth = width - pad.left - pad.right;
    const plotHeight = height - pad.top - pad.bottom;
    const priceValues = priceWindows.flatMap((window) => [window.price]);
    const demandValues = demandPoints.map((point) => point.value);
    const minPrice = Math.min(...priceValues, 0);
    const maxPrice = Math.max(...priceValues, 0.01);
    const maxDemand = Math.max(...demandValues, 0.1);
    const timeSpan = horizonEnd.getTime() - horizonStart.getTime();

    const x = (date) => {
      const ratio = (date.getTime() - horizonStart.getTime()) / timeSpan;
      return pad.left + Math.max(0, Math.min(1, ratio)) * plotWidth;
    };
    const yPrice = (value) => {
      const ratio = (value - minPrice) / Math.max(maxPrice - minPrice, 0.0001);
      return pad.top + (1 - ratio) * plotHeight;
    };
    const yDemand = (value) => {
      const ratio = value / Math.max(maxDemand, 0.0001);
      return pad.top + (1 - ratio) * plotHeight;
    };

    const pricePath = this.stepPath(
      priceWindows.map((window) => ({
        start: window.start,
        end: window.end,
        value: window.price,
      })),
      x,
      yPrice,
    );
    const demandPath = this.linePath(demandPoints, x, yDemand);
    const modeBands = this.modeBands(modeSchedule, horizonStart, horizonEnd);
    const ticks = this.timeTicks(horizonStart, horizonEnd, Number(this.config.hours_to_show));
    const priceTicks = this.valueTicks(minPrice, maxPrice, 4);

    return `
      <svg class="chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Energy price planning chart">
        <rect x="0" y="0" width="${width}" height="${height}" class="chart-bg"></rect>
        ${modeBands.map((band) => `
          <rect
            x="${x(band.start)}"
            y="${pad.top}"
            width="${Math.max(1, x(band.end) - x(band.start))}"
            height="${plotHeight}"
            class="mode-band mode-${this.modeClass(band.mode)}"
          ></rect>
        `).join("")}
        ${priceTicks.map((tick) => `
          <line x1="${pad.left}" y1="${yPrice(tick)}" x2="${width - pad.right}" y2="${yPrice(tick)}" class="grid"></line>
          <text x="${pad.left - 10}" y="${yPrice(tick) + 4}" class="axis label-right">${this.formatNumber(tick)}</text>
        `).join("")}
        ${ticks.map((tick) => `
          <line x1="${x(tick)}" y1="${pad.top}" x2="${x(tick)}" y2="${height - pad.bottom}" class="grid vertical"></line>
          <text x="${x(tick)}" y="${height - 16}" class="axis label-center">${this.formatTime(tick)}</text>
        `).join("")}
        <text x="${pad.left}" y="${height - 6}" class="axis">Prijs</text>
        <text x="${width - pad.right}" y="${height - 6}" class="axis label-right">Verbruik</text>
        <path d="${pricePath}" class="price-line"></path>
        ${demandPath ? `<path d="${demandPath}" class="demand-line"></path>` : ""}
      </svg>
    `;
  }

  renderLegend() {
    const modes = [
      ["accu_uit", "Uit"],
      ["laden_met_zonne_energie", "Laden zon"],
      ["laden_van_net", "Laden net"],
      ["ontladen", "Ontladen"],
      ["ontladen_naar_net", "Naar net"],
    ];

    return `
      <div class="legend">
        <span class="legend-line price"></span><span>Prijs</span>
        <span class="legend-line demand"></span><span>Verbruik</span>
        ${modes.map(([mode, label]) => `
          <span class="swatch mode-${this.modeClass(mode)}"></span><span>${label}</span>
        `).join("")}
      </div>
    `;
  }

  extractPriceWindows(priceState, horizonStart, horizonEnd) {
    const attributes = priceState.attributes || {};
    const rawToday = Array.isArray(attributes.raw_today) ? attributes.raw_today : [];
    const rawTomorrow = Array.isArray(attributes.raw_tomorrow) ? attributes.raw_tomorrow : [];
    const raw = [...rawToday, ...rawTomorrow];
    const windows = [];
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    raw.forEach((entry, index) => {
      const parsed = this.parsePriceEntry(entry, index, todayStart);
      if (!parsed || parsed.end <= horizonStart || parsed.start >= horizonEnd) {
        return;
      }
      windows.push({
        start: new Date(Math.max(parsed.start.getTime(), horizonStart.getTime())),
        end: new Date(Math.min(parsed.end.getTime(), horizonEnd.getTime())),
        price: parsed.price,
      });
    });

    windows.sort((a, b) => a.start - b.start);
    return windows;
  }

  parsePriceEntry(entry, index, todayStart) {
    if (typeof entry === "number") {
      const start = new Date(todayStart.getTime() + index * 60 * 60 * 1000);
      return { start, end: new Date(start.getTime() + 60 * 60 * 1000), price: entry };
    }
    if (!entry || typeof entry !== "object") {
      return undefined;
    }

    const start = this.parseDate(
      entry.start || entry.start_time || entry.from || entry.datetime || entry.time || entry.date,
    );
    let end = this.parseDate(entry.end || entry.end_time || entry.to);
    const price = this.parseNumber(
      entry.value ?? entry.price ?? entry.total ?? entry.marketprice ?? entry.market_price,
    );

    if (!start || price === undefined) {
      return undefined;
    }
    if (!end) {
      end = new Date(start.getTime() + 60 * 60 * 1000);
    }
    return { start, end, price };
  }

  extractDemandPoints(demandState, horizonStart, horizonEnd) {
    const raw = demandState?.attributes?.estimated_hourly_home_demand;
    if (!Array.isArray(raw)) {
      return [];
    }

    return raw
      .map((slot) => {
        const start = this.parseDate(slot.start);
        const end = this.parseDate(slot.end);
        const value = this.parseNumber(slot.estimated_kwh);
        if (!start || !end || value === undefined) {
          return undefined;
        }
        const midpoint = new Date((start.getTime() + end.getTime()) / 2);
        return { time: midpoint, value };
      })
      .filter((point) => point && point.time >= horizonStart && point.time <= horizonEnd)
      .sort((a, b) => a.time - b.time);
  }

  extractModeSchedule(plannerState, horizonStart, horizonEnd) {
    const raw = plannerState?.attributes?.planned_battery_mode_schedule;
    if (!Array.isArray(raw)) {
      return [{ at: horizonStart, mode: String(plannerState?.state || "accu_uit") }];
    }

    const schedule = raw
      .map((item) => {
        const at = this.parseDate(item.at);
        const mode = String(item.mode || "accu_uit");
        return at ? { at, mode } : undefined;
      })
      .filter(Boolean)
      .sort((a, b) => a.at - b.at);

    const activeBeforeStart = [...schedule].reverse().find((item) => item.at <= horizonStart);
    const future = schedule.filter((item) => item.at > horizonStart && item.at < horizonEnd);
    return [
      { at: horizonStart, mode: activeBeforeStart?.mode || String(plannerState?.state || "accu_uit") },
      ...future,
      { at: horizonEnd, mode: future.at(-1)?.mode || activeBeforeStart?.mode || String(plannerState?.state || "accu_uit") },
    ];
  }

  modeBands(schedule, horizonStart, horizonEnd) {
    const bands = [];
    for (let index = 0; index < schedule.length - 1; index += 1) {
      const current = schedule[index];
      const next = schedule[index + 1];
      const start = new Date(Math.max(current.at.getTime(), horizonStart.getTime()));
      const end = new Date(Math.min(next.at.getTime(), horizonEnd.getTime()));
      if (end > start) {
        bands.push({ start, end, mode: current.mode });
      }
    }
    return bands;
  }

  stepPath(windows, x, y) {
    if (!windows.length) {
      return "";
    }

    const parts = [];
    windows.forEach((window, index) => {
      const xStart = x(window.start);
      const xEnd = x(window.end);
      const yValue = y(window.value);
      if (index === 0) {
        parts.push(`M ${xStart.toFixed(2)} ${yValue.toFixed(2)}`);
      } else {
        parts.push(`L ${xStart.toFixed(2)} ${yValue.toFixed(2)}`);
      }
      parts.push(`L ${xEnd.toFixed(2)} ${yValue.toFixed(2)}`);
    });
    return parts.join(" ");
  }

  linePath(points, x, y) {
    if (!points.length) {
      return "";
    }
    return points
      .map((point, index) => {
        const command = index === 0 ? "M" : "L";
        return `${command} ${x(point.time).toFixed(2)} ${y(point.value).toFixed(2)}`;
      })
      .join(" ");
  }

  timeTicks(start, end, hoursToShow) {
    const tickHours = hoursToShow <= 12 ? 2 : hoursToShow <= 30 ? 4 : 6;
    const ticks = [];
    const tick = new Date(start);
    tick.setMinutes(0, 0, 0);
    if (tick < start) {
      tick.setHours(tick.getHours() + 1);
    }
    while (tick <= end) {
      if (tick.getHours() % tickHours === 0) {
        ticks.push(new Date(tick));
      }
      tick.setHours(tick.getHours() + 1);
    }
    return ticks;
  }

  valueTicks(min, max, count) {
    if (count <= 1) {
      return [min, max];
    }
    const ticks = [];
    const step = (max - min) / (count - 1);
    for (let index = 0; index < count; index += 1) {
      ticks.push(min + step * index);
    }
    return ticks;
  }

  parseDate(value) {
    if (!value) {
      return undefined;
    }
    const parsed = value instanceof Date ? value : new Date(value);
    return Number.isNaN(parsed.getTime()) ? undefined : parsed;
  }

  parseNumber(value) {
    if (value === undefined || value === null || value === "") {
      return undefined;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  modeClass(mode) {
    return String(mode || "accu_uit").replaceAll("_", "-");
  }

  formatRange(start, end) {
    return `${this.formatDateTime(start)} - ${this.formatDateTime(end)}`;
  }

  formatDateTime(date) {
    return new Intl.DateTimeFormat(undefined, {
      weekday: "short",
      hour: "2-digit",
      minute: "2-digit",
    }).format(date);
  }

  formatTime(date) {
    return new Intl.DateTimeFormat(undefined, {
      hour: "2-digit",
      minute: "2-digit",
    }).format(date);
  }

  formatNumber(value) {
    return Number(value).toFixed(2);
  }

  escape(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  renderError(message) {
    return `
      <ha-card>
        <div class="card error">${this.escape(message)}</div>
      </ha-card>
      ${this.renderStyles()}
    `;
  }

  renderStyles() {
    return `
      <style>
        :host {
          display: block;
        }
        .card {
          padding: 16px;
        }
        .header {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 12px;
          margin-bottom: 12px;
        }
        .title {
          color: var(--primary-text-color);
          font-size: 18px;
          font-weight: 600;
          line-height: 1.25;
        }
        .subtitle {
          color: var(--secondary-text-color);
          font-size: 12px;
          margin-top: 3px;
        }
        .badge {
          color: var(--primary-text-color);
          background: var(--secondary-background-color);
          border-radius: 6px;
          font-size: 12px;
          line-height: 1.2;
          max-width: 180px;
          overflow-wrap: anywhere;
          padding: 6px 8px;
          text-align: right;
        }
        .chart {
          display: block;
          height: auto;
          overflow: visible;
          width: 100%;
        }
        .chart-bg {
          fill: var(--card-background-color);
        }
        .grid {
          stroke: var(--divider-color);
          stroke-width: 1;
        }
        .grid.vertical {
          opacity: 0.55;
        }
        .axis {
          fill: var(--secondary-text-color);
          font-size: 12px;
        }
        .label-right {
          text-anchor: end;
        }
        .label-center {
          text-anchor: middle;
        }
        .price-line {
          fill: none;
          stroke: var(--primary-color);
          stroke-linecap: round;
          stroke-linejoin: round;
          stroke-width: 3;
        }
        .demand-line {
          fill: none;
          stroke: var(--warning-color, #f39c12);
          stroke-dasharray: 7 7;
          stroke-linecap: round;
          stroke-linejoin: round;
          stroke-width: 3;
        }
        .mode-band {
          opacity: 0.2;
        }
        .mode-accu-uit {
          fill: #9aa0a6;
        }
        .mode-laden-met-zonne-energie {
          fill: #34a853;
        }
        .mode-laden-van-net {
          fill: #4285f4;
        }
        .mode-ontladen {
          fill: #fbbc04;
        }
        .mode-ontladen-naar-net {
          fill: #ea4335;
        }
        .legend {
          align-items: center;
          color: var(--secondary-text-color);
          display: flex;
          flex-wrap: wrap;
          font-size: 12px;
          gap: 8px;
          margin-top: 8px;
        }
        .swatch {
          border-radius: 3px;
          display: inline-block;
          height: 10px;
          width: 18px;
        }
        .legend-line {
          display: inline-block;
          height: 0;
          width: 24px;
        }
        .legend-line.price {
          border-top: 3px solid var(--primary-color);
        }
        .legend-line.demand {
          border-top: 3px dashed var(--warning-color, #f39c12);
        }
        .error {
          color: var(--error-color);
        }
      </style>
    `;
  }
}

customElements.define("smart-energy-planner-card", SmartEnergyPlannerCard);

window.customCards = window.customCards || [];
window.customCards.push({
  type: "smart-energy-planner-card",
  name: "Smart Energy Planner Card",
  description: "Shows upcoming energy prices, expected demand, and planned battery modes.",
});
