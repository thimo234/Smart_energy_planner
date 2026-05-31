class SmartEnergyPlannerCard extends HTMLElement {
  static getConfigElement() {
    return document.createElement("smart-energy-planner-card-editor");
  }

  static getStubConfig(hass) {
    const states = Object.keys(hass?.states || {});
    const plannerEntity = states.find((entityId) => {
      const attributes = hass.states[entityId]?.attributes || {};
      return (
        Array.isArray(attributes.planned_battery_mode_schedule)
        && Array.isArray(attributes.upcoming_energy_price_windows)
      );
    });

    return {
      planner_entity: plannerEntity || "sensor.smart_energy_planner_battery_strategy",
    };
  }

  setConfig(config) {
    if (!config.planner_entity) {
      throw new Error("planner_entity is required");
    }
    this.config = {
      title: "Energieprijs planning",
      show_title: true,
      show_legend: true,
      ...config,
    };
    this._lastHtml = "";
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

    const plannerState = this._hass.states[this.config.planner_entity];
    const priceState = this.config.price_entity
      ? this._hass.states[this.config.price_entity]
      : undefined;
    const demandState = this.config.demand_entity
      ? this._hass.states[this.config.demand_entity]
      : undefined;

    if (!plannerState) {
      this.updateHtml(this.renderError(`Planner entity not found: ${this.config.planner_entity}`));
      return;
    }

    const now = new Date();
    now.setSeconds(0, 0);
    const horizonStart = this.getHorizonStart(plannerState, now);
    const allPriceWindows = this.extractAllPriceWindows(plannerState, priceState);
    const horizonEnd = this.getHorizonEnd(allPriceWindows, horizonStart);
    const priceWindows = this.clipPriceWindows(allPriceWindows, horizonStart, horizonEnd);
    const demandPoints = this.extractDemandPoints(plannerState, demandState, horizonStart, horizonEnd);
    const solarPoints = this.extractSolarPoints(plannerState, horizonStart, horizonEnd);
    const modeSchedule = this.extractModeSchedule(plannerState, horizonStart, horizonEnd);
    const modeBands = this.modeBands(modeSchedule, horizonStart, horizonEnd);
    const chartWidth = this.chartWidth(priceWindows, horizonStart, horizonEnd);

    if (!priceWindows.length) {
      this.updateHtml(this.renderError("No price windows found on the selected planner"));
      return;
    }

    this.updateHtml(`
      <ha-card>
        <div class="card">
          ${this.config.show_title === false ? "" : this.renderHeader(horizonStart, horizonEnd)}
          ${this.renderSummary(priceWindows, demandPoints, solarPoints, now)}
          ${this.renderChart(priceWindows, demandPoints, solarPoints, modeBands, horizonStart, horizonEnd, now, chartWidth)}
          ${this.renderModeTimeline(modeBands, plannerState, horizonStart, horizonEnd, now, chartWidth)}
          ${this.config.show_legend ? this.renderLegend() : ""}
        </div>
      </ha-card>
      ${this.renderStyles()}
    `);
  }

  renderHeader(horizonStart, horizonEnd) {
    return `
      <div class="header">
        <div>
          <div class="title">${this.escape(this.config.title || "")}</div>
          <div class="subtitle">${this.formatRange(horizonStart, horizonEnd)}</div>
        </div>
      </div>
    `;
  }

  getHorizonStart(plannerState, now) {
    const start = new Date(now);
    const resolution = String(plannerState?.attributes?.price_resolution || "");
    if (resolution === "quarter_hourly") {
      start.setMinutes(Math.floor(start.getMinutes() / 15) * 15, 0, 0);
      start.setHours(start.getHours() - 1);
      return start;
    }
    start.setMinutes(0, 0, 0);
    start.setHours(start.getHours() - 1);
    return start;
  }

  getHorizonEnd(priceWindows, horizonStart) {
    const configuredHours = this.parseNumber(this.config.max_hours_to_show);
    if (configuredHours !== undefined && configuredHours > 0) {
      return new Date(horizonStart.getTime() + configuredHours * 60 * 60 * 1000);
    }

    const lastKnownEnd = priceWindows
      .filter((window) => window.end > horizonStart)
      .reduce((latest, window) => Math.max(latest, window.end.getTime()), 0);

    if (lastKnownEnd > horizonStart.getTime()) {
      return new Date(lastKnownEnd);
    }
    return new Date(horizonStart.getTime() + 24 * 60 * 60 * 1000);
  }

  chartWidth(priceWindows, horizonStart, horizonEnd) {
    const horizonHours = Math.max(1, (horizonEnd.getTime() - horizonStart.getTime()) / (60 * 60 * 1000));
    const chartPadding = 88 + 10;
    return Math.max(430, Math.ceil(chartPadding + (horizonHours * 28)));
  }

  renderSummary(priceWindows, demandPoints, solarPoints, now) {
    const currentValues = this.selectionValues(now, priceWindows, demandPoints, solarPoints);

    return `
      <div class="summary">
        <div class="metric">
          <span>Tijd</span>
          <strong data-selected-time>Nu</strong>
        </div>
        <div class="metric">
          <span>Prijs</span>
          <strong data-selected-price>${this.formatSelectedValue(currentValues.price)}</strong>
        </div>
        <div class="metric">
          <span>Verbruik</span>
          <strong data-selected-demand>${this.formatSelectedValue(currentValues.demand)}</strong>
        </div>
        <div class="metric">
          <span>Zon</span>
          <strong data-selected-solar>${this.formatSelectedValue(currentValues.solar, "0.00")}</strong>
        </div>
      </div>
    `;
  }

  renderChart(priceWindows, demandPoints, solarPoints, modeBands, horizonStart, horizonEnd, now, chartWidth) {
    const width = chartWidth;
    const height = 270;
    const pad = { top: 26, right: 10, bottom: 68, left: 88 };
    const plotWidth = width - pad.left - pad.right;
    const plotHeight = height - pad.top - pad.bottom;
    const priceValues = priceWindows.flatMap((window) => [window.price]);
    const energyValues = [...demandPoints, ...solarPoints].map((point) => point.value);
    const minPrice = Math.min(...priceValues, 0);
    const maxPrice = Math.max(...priceValues, 0.01);
    const maxEnergy = Math.max(...energyValues, 0.1);
    const timeSpan = horizonEnd.getTime() - horizonStart.getTime();

    const x = (date) => {
      const ratio = (date.getTime() - horizonStart.getTime()) / timeSpan;
      return pad.left + Math.max(0, Math.min(1, ratio)) * plotWidth;
    };
    const yPrice = (value) => {
      const ratio = (value - minPrice) / Math.max(maxPrice - minPrice, 0.0001);
      const baseline = pad.top + plotHeight;
      const barScale = plotHeight * 0.78;
      return baseline - (ratio * barScale);
    };
    const yDemand = (value) => {
      const ratio = value / Math.max(maxEnergy, 0.0001);
      return pad.top + (1 - ratio) * plotHeight;
    };

    const demandPath = this.linePath(demandPoints, x, yDemand);
    const solarPath = this.linePath(solarPoints, x, yDemand);
    const hoursToShow = (horizonEnd.getTime() - horizonStart.getTime()) / (60 * 60 * 1000);
    const ticks = this.timeTicks(horizonStart, horizonEnd, hoursToShow);
    const priceTicks = this.valueTicks(minPrice, maxPrice, 4);
    const energyTicks = this.valueTicks(0, maxEnergy, 4);
    const lowThreshold = minPrice + ((maxPrice - minPrice) * 0.33);
    const highThreshold = minPrice + ((maxPrice - minPrice) * 0.66);
    const nowInRange = now >= horizonStart && now <= horizonEnd;
    const nowValues = this.selectionValues(now, priceWindows, demandPoints, solarPoints);
    const nowX = nowInRange ? x(now) : x(priceWindows[0]?.start || horizonStart);

    return `
      <div class="chart-wrap">
        <div class="chart-scroll">
          <svg
            class="chart"
            style="width:${width}px;min-width:430px;"
            viewBox="0 0 ${width} ${height}"
            role="img"
            aria-label="Energy price planning chart"
            data-now-time="Nu"
            data-now-x="${nowX.toFixed(2)}"
            data-now-price="${this.formatSelectedValue(nowValues.price)}"
            data-now-demand="${this.formatSelectedValue(nowValues.demand)}"
            data-now-solar="${this.formatSelectedValue(nowValues.solar, "0.00")}"
          >
            <rect x="0" y="0" width="${width}" height="${height}" class="chart-bg"></rect>
            ${priceTicks.map((tick) => `
              <line x1="${pad.left}" y1="${yPrice(tick)}" x2="${width - pad.right}" y2="${yPrice(tick)}" class="grid"></line>
              <text x="${pad.left - 48}" y="${yPrice(tick) + 4}" class="axis label-right">${this.formatNumber(tick)}</text>
            `).join("")}
            ${energyTicks.map((tick) => `
              <text x="${pad.left - 40}" y="${yDemand(tick) + 4}" class="axis axis-kwh">${this.formatNumber(tick)}</text>
            `).join("")}
            ${ticks.map((tick) => `
              <line x1="${x(tick)}" y1="${pad.top}" x2="${x(tick)}" y2="${height - pad.bottom}" class="grid vertical"></line>
              <text x="${x(tick)}" y="${height - 45}" class="axis label-center">${this.formatTime(tick)}</text>
            `).join("")}
            <text x="${pad.left - 48}" y="${height - 20}" class="axis axis-muted label-right">&euro;</text>
            <text x="${pad.left - 40}" y="${height - 20}" class="axis axis-muted">kWh</text>
            ${priceWindows.map((window) => {
              const xStart = x(window.start);
              const xEnd = x(window.end);
              const barWidth = Math.max(7, xEnd - xStart - 4);
              const yValue = yPrice(window.price);
              const barHeight = Math.max(2, (pad.top + plotHeight) - yValue);
              const selectTime = new Date((window.start.getTime() + window.end.getTime()) / 2);
              const values = this.selectionValues(selectTime, priceWindows, demandPoints, solarPoints);
              const isCurrentWindow = window.start <= now && window.end > now;
              return `
                <rect
                  x="${(xStart + 2).toFixed(2)}"
                  y="${yValue.toFixed(2)}"
                  width="${barWidth.toFixed(2)}"
                  height="${barHeight.toFixed(2)}"
                  rx="7"
                  class="price-bar ${this.priceClass(window.price, lowThreshold, highThreshold)}${isCurrentWindow ? " selected" : ""}"
                  data-selection-time="${this.escape(this.formatTime(selectTime))}"
                  data-selection-x="${((xStart + xEnd) / 2).toFixed(2)}"
                  data-selected-price="${this.formatSelectedValue(values.price)}"
                  data-selected-demand="${this.formatSelectedValue(values.demand)}"
                  data-selected-solar="${this.formatSelectedValue(values.solar, "0.00")}"
                  ${isCurrentWindow ? "data-now-selection" : ""}
                ></rect>
              `;
            }).join("")}
            ${demandPath ? `<path d="${demandPath}" class="demand-line"></path>` : ""}
            ${solarPath ? `<path d="${solarPath}" class="solar-line"></path>` : ""}
            ${nowInRange ? `
              <line x1="${x(now).toFixed(2)}" y1="${pad.top}" x2="${x(now).toFixed(2)}" y2="${height - pad.bottom}" class="now-line"></line>
              <text x="${(x(now) + 8).toFixed(2)}" y="${pad.top + 16}" class="now-label">Nu</text>
            ` : ""}
          </svg>
        </div>
      </div>
    `;
  }

  renderModeTimeline(modeBands, plannerState, horizonStart, horizonEnd, now, chartWidth) {
    const horizonMs = horizonEnd.getTime() - horizonStart.getTime();
    const currentMode = this.currentMode(plannerState, modeBands, now);
    const nowInRange = now >= horizonStart && now <= horizonEnd;
    const nowLeft = nowInRange ? ((now.getTime() - horizonStart.getTime()) / horizonMs) * 100 : 0;
    return `
      <div class="mode-panel">
        <div class="mode-panel-header">
          <span>Huidige plannermodus</span>
          <strong class="mode-pill mode-${this.modeClass(currentMode)}">${this.modeLabel(currentMode)}</strong>
        </div>
        <div class="mode-scroll">
          <div class="mode-track" style="width:${chartWidth}px;min-width:430px;">
            ${modeBands.map((band) => {
              const left = ((band.start.getTime() - horizonStart.getTime()) / horizonMs) * 100;
              const width = ((band.end.getTime() - band.start.getTime()) / horizonMs) * 100;
              return `
                <div
                  class="mode-box mode-${this.modeClass(band.mode)}"
                  style="left:${Math.max(0, left).toFixed(3)}%;width:${Math.max(0.5, width).toFixed(3)}%;"
                  title="${this.escape(`${this.formatTime(band.start)} - ${this.formatTime(band.end)} ${this.modeLabel(band.mode)}`)}"
                >
                </div>
              `;
            }).join("")}
            ${nowInRange ? `<div class="mode-now-line" style="left:${nowLeft.toFixed(3)}%;"></div>` : ""}
          </div>
        </div>
      </div>
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
        ${modes.map(([mode, label]) => `
          <span class="swatch mode-${this.modeClass(mode)}"></span><span>${label}</span>
        `).join("")}
      </div>
    `;
  }

  extractAllPriceWindows(plannerState, priceState) {
    const plannerWindows = plannerState?.attributes?.upcoming_energy_price_windows;
    if (Array.isArray(plannerWindows)) {
      return plannerWindows
        .map((entry) => {
          const start = this.parseDate(entry.start);
          const end = this.parseDate(entry.end);
          const price = this.parseNumber(entry.price);
          if (!start || !end || price === undefined) {
            return undefined;
          }
          return { start, end, price };
        })
        .filter(Boolean)
        .sort((a, b) => a.start - b.start);
    }

    const attributes = priceState?.attributes || {};
    const rawToday = Array.isArray(attributes.raw_today) ? attributes.raw_today : [];
    const rawTomorrow = Array.isArray(attributes.raw_tomorrow) ? attributes.raw_tomorrow : [];
    const raw = [...rawToday, ...rawTomorrow];
    const windows = [];
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    raw.forEach((entry, index) => {
      const parsed = this.parsePriceEntry(entry, index, todayStart);
      if (parsed) {
        windows.push(parsed);
      }
    });

    windows.sort((a, b) => a.start - b.start);
    return windows;
  }

  clipPriceWindows(priceWindows, horizonStart, horizonEnd) {
    return priceWindows
      .map((window) => {
        if (window.end <= horizonStart || window.start >= horizonEnd) {
          return undefined;
        }
        return {
          start: new Date(Math.max(window.start.getTime(), horizonStart.getTime())),
          end: new Date(Math.min(window.end.getTime(), horizonEnd.getTime())),
          price: window.price,
        };
      })
      .filter(Boolean)
      .sort((a, b) => a.start - b.start);
  }

  extractPriceWindows(plannerState, priceState, horizonStart, horizonEnd) {
    return this.clipPriceWindows(
      this.extractAllPriceWindows(plannerState, priceState),
      horizonStart,
      horizonEnd,
    );
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

  extractDemandPoints(plannerState, demandState, horizonStart, horizonEnd) {
    const raw = plannerState?.attributes?.estimated_hourly_home_demand
      || demandState?.attributes?.estimated_hourly_home_demand;
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

  extractSolarPoints(plannerState, horizonStart, horizonEnd) {
    const raw = plannerState?.attributes?.estimated_hourly_solar_forecast;
    if (!Array.isArray(raw)) {
      return [];
    }

    return raw
      .map((slot) => {
        const start = this.parseDate(slot.start);
        const end = this.parseDate(slot.end);
        const value = this.parseNumber(slot.estimated_kwh ?? slot.forecast_kwh ?? slot.pv_estimate);
        if (!start || !end || value === undefined || value <= 0.01) {
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
    const fallbackMode = this.currentMode(plannerState, [], horizonStart);
    return [
      { at: horizonStart, mode: activeBeforeStart?.mode || fallbackMode },
      ...future,
      { at: horizonEnd, mode: future.at(-1)?.mode || activeBeforeStart?.mode || fallbackMode },
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
    if (points.length === 1) {
      return `M ${x(points[0].time).toFixed(2)} ${y(points[0].value).toFixed(2)}`;
    }

    const coords = points.map((point) => ({
      x: x(point.time),
      y: y(point.value),
    }));
    const parts = [`M ${coords[0].x.toFixed(2)} ${coords[0].y.toFixed(2)}`];

    for (let index = 0; index < coords.length - 1; index += 1) {
      const p1 = coords[index];
      const p2 = coords[index + 1];
      const p0 = coords[Math.max(0, index - 1)];
      const p3 = coords[Math.min(coords.length - 1, index + 2)];
      const dx = p2.x - p1.x;
      const cp1x = p1.x + (dx * 0.35);
      const cp2x = p2.x - (dx * 0.35);
      const slope1 = (p2.y - p0.y) / Math.max(1, p2.x - p0.x);
      const slope2 = (p3.y - p1.y) / Math.max(1, p3.x - p1.x);
      const cp1y = p1.y + (slope1 * (cp1x - p1.x));
      const cp2y = p2.y - (slope2 * (p2.x - cp2x));
      parts.push(
        `C ${cp1x.toFixed(2)} ${cp1y.toFixed(2)}, ${cp2x.toFixed(2)} ${cp2y.toFixed(2)}, ${p2.x.toFixed(2)} ${p2.y.toFixed(2)}`,
      );
    }

    return parts.join(" ");
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

  modeLabel(mode) {
    const labels = {
      accu_uit: "Uit",
      laden_met_zonne_energie: "Laden zon",
      laden_van_net: "Laden net",
      ontladen: "Ontladen",
      ontladen_naar_net: "Naar net",
      not_applicable: "N.v.t.",
    };
    return labels[String(mode)] || String(mode || "Onbekend").replaceAll("_", " ");
  }

  currentMode(plannerState, modeBands, now) {
    const activeBand = modeBands.find((band) => band.start <= now && band.end > now);
    if (activeBand?.mode) {
      return activeBand.mode;
    }

    const attributes = plannerState?.attributes || {};
    return String(
      attributes.current_relevant_battery_window_mode
        || attributes.battery_strategy
        || "accu_uit",
    );
  }

  selectionValues(time, priceWindows, demandPoints, solarPoints) {
    const priceWindow = priceWindows.find((window) => window.start <= time && window.end > time);
    return {
      price: priceWindow?.price,
      demand: this.nearestPoint(demandPoints, time)?.value,
      solar: this.nearestPoint(solarPoints, time)?.value,
    };
  }

  nearestPoint(points, time) {
    const maxDistance = 45 * 60 * 1000;
    let nearest;
    let nearestDistance = Number.POSITIVE_INFINITY;

    points.forEach((point) => {
      const distance = Math.abs(point.time.getTime() - time.getTime());
      if (distance < nearestDistance) {
        nearest = point;
        nearestDistance = distance;
      }
    });

    return nearestDistance <= maxDistance ? nearest : undefined;
  }

  priceClass(price, lowThreshold, highThreshold) {
    if (price <= lowThreshold) {
      return "price-low";
    }
    if (price >= highThreshold) {
      return "price-high";
    }
    return "price-mid";
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

  formatSelectedValue(value, fallback = "-") {
    if (value === undefined || value === null || Number.isNaN(Number(value))) {
      return fallback;
    }
    return this.formatNumber(value);
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

  updateHtml(html) {
    if (html === this._lastHtml) {
      return;
    }
    this._lastHtml = html;
    this.innerHTML = html;
    this.attachSelectionHandlers();
    this.attachScrollSync();
  }

  attachScrollSync() {
    const chartScroll = this.querySelector(".chart-scroll");
    const modeScroll = this.querySelector(".mode-scroll");
    if (!chartScroll || !modeScroll) {
      return;
    }

    let syncing = false;
    const sync = (source, target) => {
      if (syncing) {
        return;
      }
      syncing = true;
      target.scrollLeft = source.scrollLeft;
      requestAnimationFrame(() => {
        syncing = false;
      });
    };

    chartScroll.addEventListener("scroll", () => sync(chartScroll, modeScroll), { passive: true });
    modeScroll.addEventListener("scroll", () => sync(modeScroll, chartScroll), { passive: true });
  }

  attachSelectionHandlers() {
    const selectedTime = this.querySelector("[data-selected-time]");
    const selectedPrice = this.querySelector("[data-selected-price]");
    const selectedDemand = this.querySelector("[data-selected-demand]");
    const selectedSolar = this.querySelector("[data-selected-solar]");

    if (!selectedTime || !selectedPrice || !selectedDemand || !selectedSolar) {
      return;
    }

    const applySelection = (dataset, element) => {
      selectedTime.textContent = dataset.selectionTime || dataset.nowTime || "Nu";
      selectedPrice.textContent = dataset.selectedPrice || dataset.nowPrice || "-";
      selectedDemand.textContent = dataset.selectedDemand || dataset.nowDemand || "-";
      selectedSolar.textContent = dataset.selectedSolar || dataset.nowSolar || "0.00";

      if (element) {
        this.querySelectorAll(".selected").forEach((selected) => selected.classList.remove("selected"));
        element.classList.add("selected");
      }
    };

    this.querySelectorAll("[data-selection-time]").forEach((element) => {
      element.addEventListener("click", () => applySelection(element.dataset, element));
    });
  }

  renderStyles() {
    return `
      <style>
        :host {
          display: block;
        }
        ha-card {
          background: transparent;
          border: 0;
          box-shadow: none;
        }
        .card {
          min-height: 260px;
          padding: 7px;
        }
        .header {
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 8px;
          margin-bottom: 5px;
        }
        .title {
          color: var(--primary-text-color);
          font-size: 14px;
          font-weight: 600;
          line-height: 1.25;
        }
        .subtitle {
          color: var(--secondary-text-color);
          font-size: 10px;
          margin-top: 2px;
        }
        .summary {
          display: grid;
          gap: 4px;
          grid-template-columns: repeat(4, minmax(0, 1fr));
          margin: 0 0 2px;
        }
        .metric {
          background: rgba(0, 0, 0, 0.16);
          border-radius: 6px;
          min-width: 0;
          padding: 4px 5px;
        }
        .metric span {
          color: var(--secondary-text-color);
          display: block;
          font-size: 10px;
          line-height: 1.2;
        }
        .metric strong {
          color: var(--primary-text-color);
          display: block;
          font-size: 14px;
          line-height: 1.25;
          margin-top: 1px;
          overflow-wrap: anywhere;
        }
        .mode-current strong {
          font-size: 16px;
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
        .chart-wrap {
          position: relative;
        }
        .chart-scroll,
        .mode-scroll {
          overflow-x: auto;
          overflow-y: hidden;
          -webkit-overflow-scrolling: touch;
        }
        .chart-scroll {
          scrollbar-color: rgba(255, 255, 255, 0.55) rgba(255, 255, 255, 0.16);
          scrollbar-width: thin;
        }
        .chart-scroll::-webkit-scrollbar {
          height: 10px;
        }
        .chart-scroll::-webkit-scrollbar-track {
          background: rgba(255, 255, 255, 0.16);
          border-radius: 999px;
        }
        .chart-scroll::-webkit-scrollbar-thumb {
          background: rgba(255, 255, 255, 0.55);
          border-radius: 999px;
        }
        .mode-scroll {
          -ms-overflow-style: none;
          scrollbar-width: none;
        }
        .mode-scroll::-webkit-scrollbar {
          display: none;
        }
        .chart-scroll {
          padding-right: 0;
        }
        .chart {
          display: block;
          height: auto;
          overflow: visible;
        }
        .chart-bg {
          fill: transparent;
        }
        .grid {
          stroke: var(--divider-color);
          stroke-dasharray: 4 5;
          stroke-width: 1.2;
        }
        .grid.vertical {
          opacity: 0.55;
        }
        .axis {
          fill: var(--primary-text-color);
          font-size: 12px;
          font-weight: 600;
        }
        .axis-muted {
          fill: var(--secondary-text-color);
          font-size: 11px;
          font-weight: 500;
        }
        .axis-kwh {
          fill: var(--secondary-text-color);
        }
        .label-right {
          text-anchor: end;
        }
        .label-center {
          text-anchor: middle;
        }
        .price-bar {
          cursor: pointer;
          opacity: 0.58;
        }
        .demand-line {
          fill: none;
          stroke: #ab47bc;
          stroke-dasharray: 6 6;
          stroke-linecap: round;
          stroke-linejoin: round;
          stroke-width: 5;
        }
        .solar-line {
          fill: none;
          stroke: #fdd835;
          stroke-linecap: round;
          stroke-linejoin: round;
          stroke-width: 5;
        }
        .selected {
          filter: brightness(1.25);
          opacity: 0.95;
          stroke: var(--primary-text-color);
          stroke-width: 3;
        }
        .now-line {
          stroke: var(--primary-text-color);
          stroke-dasharray: 3 3;
          stroke-linecap: round;
          stroke-width: 3;
        }
        .now-label {
          fill: var(--primary-text-color);
          font-size: 12px;
          font-weight: 700;
        }
        .mode-band {
          opacity: 0.2;
        }
        .price-low {
          fill: #43a047;
          background: #43a047;
        }
        .price-mid {
          fill: #f9ab00;
          background: #f9ab00;
        }
        .price-high {
          fill: #d93025;
          background: #d93025;
        }
        .mode-accu-uit {
          fill: #9aa0a6;
          background: #9aa0a6;
        }
        .mode-laden-met-zonne-energie {
          fill: #34a853;
          background: #34a853;
        }
        .mode-laden-van-net {
          fill: #4285f4;
          background: #4285f4;
        }
        .mode-ontladen {
          fill: #fbbc04;
          background: #fbbc04;
        }
        .mode-ontladen-naar-net {
          fill: #ea4335;
          background: #ea4335;
        }
        .mode-not-applicable {
          fill: #9aa0a6;
          background: #9aa0a6;
        }
        .mode-panel {
          margin-top: 2px;
        }
        .mode-panel-header {
          align-items: center;
          color: var(--secondary-text-color);
          display: flex;
          font-size: 12px;
          justify-content: space-between;
          gap: 8px;
          margin-bottom: 4px;
        }
        .mode-pill {
          border-radius: 999px;
          color: #fff;
          font-size: 12px;
          line-height: 1.2;
          padding: 5px 9px;
          text-shadow: 0 1px 1px rgba(0, 0, 0, 0.25);
          white-space: nowrap;
        }
        .mode-track {
          background: var(--secondary-background-color);
          border-radius: 8px;
          height: 30px;
          overflow: hidden;
          position: relative;
        }
        .mode-now-line {
          background: var(--primary-text-color);
          bottom: 0;
          box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.25);
          position: absolute;
          top: 0;
          transform: translateX(-50%);
          width: 3px;
          z-index: 2;
        }
        .mode-box {
          border-right: 1px solid rgba(255, 255, 255, 0.28);
          bottom: 0;
          left: 0;
          min-width: 18px;
          overflow: hidden;
          position: absolute;
          top: 0;
        }
        .legend {
          align-items: center;
          color: var(--secondary-text-color);
          display: flex;
          flex-wrap: wrap;
          font-size: 12px;
          gap: 6px;
          margin-top: 5px;
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
          border-top: 3px dashed #ab47bc;
        }
        .legend-line.now {
          border-top: 3px dashed var(--primary-text-color);
        }
        .error {
          color: var(--error-color);
        }
        @media (max-width: 520px) {
          .summary {
            grid-template-columns: repeat(2, minmax(0, 1fr));
          }
          .chart-scroll {
            -ms-overflow-style: none;
            scrollbar-width: none;
          }
          .chart-scroll::-webkit-scrollbar {
            display: none;
          }
          .mode-box {
            font-size: 10px;
          }
        }
      </style>
    `;
  }
}

class SmartEnergyPlannerCardEditor extends HTMLElement {
  setConfig(config) {
    this.config = { ...config };
    this.render();
  }

  set hass(hass) {
    this._hass = hass;
    this.render();
  }

  render() {
    if (!this._hass || !this.config) {
      return;
    }

    this.innerHTML = `
      <div class="editor">
        <ha-entity-picker
          label="Planner"
          domain-filter="sensor"
          allow-custom-entity
        ></ha-entity-picker>
        <ha-textfield
          label="Titel"
          .value="${this.escape(this.config.title ?? "")}"
        ></ha-textfield>
        <ha-formfield label="Titel tonen">
          <ha-switch></ha-switch>
        </ha-formfield>
      </div>
      <style>
        .editor {
          display: grid;
          gap: 16px;
        }
      </style>
    `;

    const picker = this.querySelector("ha-entity-picker");
    picker.hass = this._hass;
    picker.value = this.config.planner_entity || "";
    picker.addEventListener("value-changed", (event) => {
      this.updateConfig({ planner_entity: event.detail.value });
    });

    const titleField = this.querySelector("ha-textfield");
    titleField.value = this.config.title ?? "";
    titleField.addEventListener("input", (event) => {
      this.updateConfig({ title: event.target.value });
    });

    const titleSwitch = this.querySelector("ha-switch");
    titleSwitch.checked = this.config.show_title !== false;
    titleSwitch.addEventListener("change", (event) => {
      this.updateConfig({ show_title: event.target.checked });
    });
  }

  updateConfig(changedConfig) {
    this.config = {
      ...this.config,
      ...changedConfig,
    };
    delete this.config.price_entity;
    delete this.config.demand_entity;
    delete this.config.hours_to_show;
    this.dispatchEvent(
      new CustomEvent("config-changed", {
        bubbles: true,
        composed: true,
        detail: { config: this.config },
      }),
    );
  }
}

customElements.define("smart-energy-planner-card", SmartEnergyPlannerCard);
customElements.define("smart-energy-planner-card-editor", SmartEnergyPlannerCardEditor);

window.customCards = window.customCards || [];
window.customCards.push({
  type: "smart-energy-planner-card",
  name: "Smart Energy Planner Card",
  description: "Shows upcoming energy prices, expected demand, and planned battery modes.",
});
