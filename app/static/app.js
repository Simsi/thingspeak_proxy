(() => {
  const app = window.APP_BOOTSTRAP || {};
  if (!app.measurements_url) return;

  const csrfToken = app.csrf_token;
  const refreshIntervalMs = Number(app.refresh_interval_ms || 15000);

  const refs = {
    messageBox: document.getElementById('message-box'),
    deviceSelect: document.getElementById('device-select'),
    deviceMeta: document.getElementById('device-meta'),
    devicesTable: document.getElementById('devices-table'),
    destinationsTable: document.getElementById('destinations-table'),
    themeButtons: Array.from(document.querySelectorAll('[data-theme-value]')),
  };

  const chartConfigs = [
    { fieldName: 'warm_stream', canvasId: 'chart-warm-stream', label: 'Тепловой поток', unit: 'Вт/м²', colorVar: '--chart-warm' },
    { fieldName: 'surface_temp', canvasId: 'chart-surface-temp', label: 'Температура поверхности', unit: '°C', colorVar: '--chart-surface' },
    { fieldName: 'air_temp', canvasId: 'chart-air-temp', label: 'Температура воздуха', unit: '°C', colorVar: '--chart-air-temp' },
    { fieldName: 'air_hum', canvasId: 'chart-air-hum', label: 'Влажность воздуха', unit: '%', colorVar: '--chart-air-hum' },
  ];

  const state = {
    devices: Array.isArray(app.devices) ? app.devices.slice() : [],
    destinations: Array.isArray(app.destinations) ? app.destinations.slice() : [],
    selectedDeviceHash: null,
    measurements: [],
    lastLoadedDeviceHash: null,
    autoRefreshHandle: null,
    theme: readInitialTheme(),
  };

  const formatAxisDate = new Intl.DateTimeFormat('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });

  const formatTooltipDate = new Intl.DateTimeFormat('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });

  function readInitialTheme() {
    try {
      return localStorage.getItem('ts-monitor-theme') || document.documentElement.getAttribute('data-theme') || 'dark';
    } catch (error) {
      return document.documentElement.getAttribute('data-theme') || 'dark';
    }
  }

  function applyTheme(theme) {
    state.theme = theme;
    document.documentElement.setAttribute('data-theme', theme);
    try {
      localStorage.setItem('ts-monitor-theme', theme);
    } catch (error) {
      // ignore
    }
    refs.themeButtons.forEach((button) => {
      button.setAttribute('aria-pressed', String(button.dataset.themeValue === theme));
    });
    Object.values(charts).forEach((chart) => chart.draw());
  }

  function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  class InteractiveChart {
    constructor(config) {
      this.fieldName = config.fieldName;
      this.label = config.label;
      this.unit = config.unit;
      this.colorVar = config.colorVar;
      this.canvas = document.getElementById(config.canvasId);
      this.card = this.canvas.closest('.chart-card');
      this.stage = this.canvas.closest('.chart-stage');
      this.lastEl = document.querySelector(`[data-field-last="${this.fieldName}"]`);
      this.rangeEl = document.querySelector(`[data-field-range="${this.fieldName}"]`);
      this.ctx = this.canvas.getContext('2d');
      this.data = [];
      this.filteredData = [];
      this.fullXRange = null;
      this.viewX = null;
      this.manualY = null;
      this.hoveredPoint = null;
      this.drag = null;
      this.lastRect = null;

      this.canvas.addEventListener('wheel', (event) => this.onWheel(event), { passive: false });
      this.canvas.addEventListener('pointerdown', (event) => this.onPointerDown(event));
      this.canvas.addEventListener('pointermove', (event) => this.onPointerMove(event));
      this.canvas.addEventListener('pointerleave', () => this.onPointerLeave());
      this.canvas.addEventListener('dblclick', () => this.resetView());
      window.addEventListener('pointerup', () => this.onPointerUp());
      window.addEventListener('pointermove', (event) => this.onWindowPointerMove(event));

      this.resizeObserver = new ResizeObserver(() => this.resize());
      this.resizeObserver.observe(this.stage);
      this.resize();
    }

    get color() {
      return cssVar(this.colorVar) || '#7bbfff';
    }

    setData(items) {
      const fallbackBase = Date.now();
      const normalized = [];
      items.forEach((item, index) => {
        const value = toFiniteNumber(item[this.fieldName]);
        if (value === null) return;
        const ts = parseTimestamp(item.source_created_at) ?? parseTimestamp(item.inserted_at) ?? (fallbackBase + index * 1000);
        normalized.push({
          x: ts,
          y: value,
          eventId: item.event_id,
          rawTimestamp: item.source_created_at || item.inserted_at || null,
        });
      });

      normalized.sort((a, b) => {
        if (a.x !== b.x) return a.x - b.x;
        return (a.eventId || 0) - (b.eventId || 0);
      });

      this.data = normalized;
      this.fullXRange = normalized.length
        ? { min: normalized[0].x, max: normalized[normalized.length - 1].x }
        : null;

      const shouldReset = !this.viewX || state.lastLoadedDeviceHash !== state.selectedDeviceHash;
      if (shouldReset) this.resetView(false);
      else this.viewX = clampXView(this.viewX, this.fullXRange);

      this.updateSummary();
      this.draw();
    }

    resetView(drawNow = true) {
      this.hoveredPoint = null;
      this.manualY = null;
      if (!this.fullXRange) {
        this.viewX = null;
        if (drawNow) this.draw();
        return;
      }
      const min = this.fullXRange.min;
      const max = this.fullXRange.max;
      this.viewX = min === max ? { min: min - 60_000, max: max + 60_000 } : { min, max };
      if (drawNow) this.draw();
    }

    resize() {
      const rect = this.stage.getBoundingClientRect();
      if (!rect.width || !rect.height) return;
      const dpr = window.devicePixelRatio || 1;
      this.canvas.width = Math.max(1, Math.floor(rect.width * dpr));
      this.canvas.height = Math.max(1, Math.floor(rect.height * dpr));
      this.canvas.style.width = `${rect.width}px`;
      this.canvas.style.height = `${rect.height}px`;
      this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      this.lastRect = rect;
      this.draw();
    }

    getPlotRect() {
      const width = this.lastRect ? this.lastRect.width : this.stage.clientWidth;
      const height = this.lastRect ? this.lastRect.height : this.stage.clientHeight;
      return {
        x: 52,
        y: 10,
        width: Math.max(10, width - 64),
        height: Math.max(10, height - 34),
      };
    }

    getVisibleData() {
      if (!this.data.length || !this.viewX) return [];
      const min = this.viewX.min;
      const max = this.viewX.max;
      return this.data.filter((point) => point.x >= min && point.x <= max);
    }

    getYRange(visibleData) {
      const source = visibleData.length ? visibleData : this.data;
      if (!source.length) return null;
      if (this.manualY) return { min: this.manualY.min, max: this.manualY.max };

      let min = source[0].y;
      let max = source[0].y;
      source.forEach((point) => {
        if (point.y < min) min = point.y;
        if (point.y > max) max = point.y;
      });

      if (min === max) {
        const pad = Math.max(Math.abs(min) * 0.05, 0.5);
        return { min: min - pad, max: max + pad };
      }
      const pad = (max - min) * 0.08;
      return { min: min - pad, max: max + pad };
    }

    draw() {
      const ctx = this.ctx;
      const width = this.lastRect ? this.lastRect.width : this.stage.clientWidth;
      const height = this.lastRect ? this.lastRect.height : this.stage.clientHeight;
      ctx.clearRect(0, 0, width, height);

      const plot = this.getPlotRect();
      this.filteredData = this.getVisibleData();
      const yRange = this.getYRange(this.filteredData);
      this.drawChrome(plot, yRange);

      if (!this.data.length || !this.viewX || !this.filteredData.length || !yRange) {
        this.drawEmptyState(plot);
        return;
      }

      const xScale = (value) => plot.x + ((value - this.viewX.min) / (this.viewX.max - this.viewX.min)) * plot.width;
      const yScale = (value) => plot.y + plot.height - ((value - yRange.min) / (yRange.max - yRange.min)) * plot.height;

      ctx.save();
      ctx.beginPath();
      ctx.rect(plot.x, plot.y, plot.width, plot.height);
      ctx.clip();

      const fillGradient = ctx.createLinearGradient(0, plot.y, 0, plot.y + plot.height);
      fillGradient.addColorStop(0, hexToRgba(this.color, 0.14));
      fillGradient.addColorStop(1, hexToRgba(this.color, 0));

      ctx.beginPath();
      this.filteredData.forEach((point, index) => {
        const x = xScale(point.x);
        const y = yScale(point.y);
        if (index === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.lineWidth = 2.1;
      ctx.strokeStyle = this.color;
      ctx.stroke();

      if (this.filteredData.length > 1) {
        const first = this.filteredData[0];
        const last = this.filteredData[this.filteredData.length - 1];
        ctx.lineTo(xScale(last.x), plot.y + plot.height);
        ctx.lineTo(xScale(first.x), plot.y + plot.height);
        ctx.closePath();
        ctx.fillStyle = fillGradient;
        ctx.fill();
      }

      if (this.filteredData.length <= 220) {
        ctx.fillStyle = this.color;
        this.filteredData.forEach((point) => {
          ctx.beginPath();
          ctx.arc(xScale(point.x), yScale(point.y), 1.7, 0, Math.PI * 2);
          ctx.fill();
        });
      }

      if (this.hoveredPoint && this.hoveredPoint.point) {
        const p = this.hoveredPoint.point;
        const x = xScale(p.x);
        const y = yScale(p.y);
        ctx.strokeStyle = cssVar('--chart-hover');
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, plot.y);
        ctx.lineTo(x, plot.y + plot.height);
        ctx.stroke();
        ctx.beginPath();
        ctx.arc(x, y, 4, 0, Math.PI * 2);
        ctx.fillStyle = this.color;
        ctx.fill();
      }

      ctx.restore();
    }

    drawChrome(plot, yRange) {
      const ctx = this.ctx;
      const width = this.lastRect ? this.lastRect.width : this.stage.clientWidth;
      const height = this.lastRect ? this.lastRect.height : this.stage.clientHeight;
      const background = ctx.createLinearGradient(0, 0, 0, height);
      background.addColorStop(0, cssVar('--chart-bg-top'));
      background.addColorStop(1, cssVar('--chart-bg-bottom'));
      ctx.fillStyle = background;
      ctx.fillRect(0, 0, width, height);
      if (!plot) return;

      ctx.save();
      ctx.strokeStyle = cssVar('--chart-grid');
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = plot.y + (plot.height / 4) * i;
        ctx.beginPath();
        ctx.moveTo(plot.x, y);
        ctx.lineTo(plot.x + plot.width, y);
        ctx.stroke();
      }

      ctx.strokeStyle = cssVar('--chart-axis');
      ctx.beginPath();
      ctx.moveTo(plot.x, plot.y + plot.height);
      ctx.lineTo(plot.x + plot.width, plot.y + plot.height);
      ctx.stroke();

      ctx.fillStyle = cssVar('--chart-label');
      ctx.font = '12px Inter, sans-serif';
      ctx.textBaseline = 'middle';
      ctx.textAlign = 'right';
      if (yRange) {
        const yTicks = buildYTicks(yRange, 4);
        yTicks.forEach((value, index) => {
          const y = plot.y + (plot.height / (yTicks.length - 1 || 1)) * index;
          ctx.fillText(formatNumber(value), plot.x - 8, y);
        });
      }

      if (this.viewX && this.viewX.max > this.viewX.min) {
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        buildXTicks(this.viewX, 3).forEach((tick) => {
          const x = plot.x + ((tick - this.viewX.min) / (this.viewX.max - this.viewX.min)) * plot.width;
          ctx.fillText(formatAxisDate.format(new Date(tick)), x, plot.y + plot.height + 7);
        });
      }
      ctx.restore();
    }

    drawEmptyState(plot) {
      const ctx = this.ctx;
      if (!plot) return;
      ctx.save();
      ctx.fillStyle = cssVar('--text-muted');
      ctx.font = '14px Inter, sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText('Нет данных', plot.x + plot.width / 2, plot.y + plot.height / 2);
      ctx.restore();
    }

    updateSummary() {
      if (!this.lastEl || !this.rangeEl) return;
      if (!this.data.length) {
        this.lastEl.textContent = 'Последнее: —';
        this.rangeEl.textContent = 'Диапазон: —';
        return;
      }
      const lastPoint = this.data[this.data.length - 1];
      let min = this.data[0].y;
      let max = this.data[0].y;
      this.data.forEach((point) => {
        if (point.y < min) min = point.y;
        if (point.y > max) max = point.y;
      });
      this.lastEl.textContent = `Последнее: ${formatNumber(lastPoint.y)} ${this.unit}`;
      this.rangeEl.textContent = `Диапазон: ${formatNumber(min)} … ${formatNumber(max)}`;
    }

    screenToData(x, y) {
      if (!this.viewX) return null;
      const plot = this.getPlotRect();
      if (x < plot.x || x > plot.x + plot.width || y < plot.y || y > plot.y + plot.height) return null;
      const yRange = this.getYRange(this.filteredData);
      if (!yRange) return null;
      return {
        x: this.viewX.min + ((x - plot.x) / plot.width) * (this.viewX.max - this.viewX.min),
        y: yRange.max - ((y - plot.y) / plot.height) * (yRange.max - yRange.min),
      };
    }

    findClosestPoint(clientX, clientY) {
      if (!this.filteredData.length || !this.viewX) return null;
      const rect = this.canvas.getBoundingClientRect();
      const plot = this.getPlotRect();
      const localX = clientX - rect.left;
      const localY = clientY - rect.top;
      if (localX < plot.x || localX > plot.x + plot.width || localY < plot.y || localY > plot.y + plot.height) return null;
      const dataPoint = this.screenToData(localX, localY);
      if (!dataPoint) return null;
      let closest = this.filteredData[0];
      let minDistance = Math.abs(closest.x - dataPoint.x);
      for (let i = 1; i < this.filteredData.length; i += 1) {
        const point = this.filteredData[i];
        const distance = Math.abs(point.x - dataPoint.x);
        if (distance < minDistance) {
          minDistance = distance;
          closest = point;
        }
      }
      return { point: closest, localX, localY };
    }

    onPointerDown(event) {
      if (!this.viewX || event.button !== 0) return;
      this.canvas.setPointerCapture(event.pointerId);
      this.drag = {
        startClientX: event.clientX,
        startClientY: event.clientY,
        startViewX: { ...this.viewX },
        startManualY: this.manualY ? { ...this.manualY } : this.getYRange(this.filteredData),
      };
    }

    onPointerMove(event) {
      if (this.drag) return;
      this.hoveredPoint = this.findClosestPoint(event.clientX, event.clientY);
      this.draw();
    }

    onPointerLeave() {
      if (this.drag) return;
      this.hoveredPoint = null;
      this.draw();
    }

    onWindowPointerMove(event) {
      if (!this.drag || !this.viewX) return;
      const plot = this.getPlotRect();
      const dx = event.clientX - this.drag.startClientX;
      const rangeX = this.drag.startViewX.max - this.drag.startViewX.min;
      const deltaX = (dx / plot.width) * rangeX;
      this.viewX = clampXView({
        min: this.drag.startViewX.min - deltaX,
        max: this.drag.startViewX.max - deltaX,
      }, this.fullXRange);

      if (event.shiftKey && this.drag.startManualY) {
        const dy = event.clientY - this.drag.startClientY;
        const rangeY = this.drag.startManualY.max - this.drag.startManualY.min;
        const deltaY = (dy / plot.height) * rangeY;
        this.manualY = {
          min: this.drag.startManualY.min + deltaY,
          max: this.drag.startManualY.max + deltaY,
        };
      }
      this.draw();
    }

    onPointerUp() {
      this.drag = null;
    }

    onWheel(event) {
      if (!this.viewX || !this.fullXRange) return;
      event.preventDefault();
      const rect = this.canvas.getBoundingClientRect();
      const localX = event.clientX - rect.left;
      const localY = event.clientY - rect.top;
      const plot = this.getPlotRect();
      if (localX < plot.x || localX > plot.x + plot.width || localY < plot.y || localY > plot.y + plot.height) return;
      const zoomFactor = Math.exp(event.deltaY * 0.0014);

      if (event.shiftKey) {
        const yRange = this.manualY || this.getYRange(this.filteredData);
        if (!yRange) return;
        const yValue = yRange.max - ((localY - plot.y) / plot.height) * (yRange.max - yRange.min);
        const next = {
          min: yValue - (yValue - yRange.min) * zoomFactor,
          max: yValue + (yRange.max - yValue) * zoomFactor,
        };
        if (next.max - next.min > 1e-9) this.manualY = next;
      } else {
        const xValue = this.viewX.min + ((localX - plot.x) / plot.width) * (this.viewX.max - this.viewX.min);
        const next = {
          min: xValue - (xValue - this.viewX.min) * zoomFactor,
          max: xValue + (this.viewX.max - xValue) * zoomFactor,
        };
        if (next.max - next.min > 1000) this.viewX = clampXView(next, this.fullXRange);
      }
      this.draw();
    }
  }

  const charts = Object.fromEntries(chartConfigs.map((config) => [config.fieldName, new InteractiveChart(config)]));

  function clampXView(view, fullRange) {
    if (!view || !fullRange) return view;
    const fullWidth = Math.max(1000, fullRange.max - fullRange.min);
    let width = Math.max(1000, view.max - view.min);
    width = Math.min(width, fullWidth);
    let min = view.min;
    let max = min + width;
    if (min < fullRange.min) {
      min = fullRange.min;
      max = min + width;
    }
    if (max > fullRange.max) {
      max = fullRange.max;
      min = max - width;
    }
    if (width >= fullWidth) {
      min = fullRange.min;
      max = fullRange.max;
    }
    return { min, max };
  }

  function buildXTicks(viewX, segments) {
    if (!viewX) return [];
    const ticks = [];
    const step = (viewX.max - viewX.min) / segments;
    for (let i = 0; i <= segments; i += 1) ticks.push(viewX.min + step * i);
    return ticks;
  }

  function buildYTicks(range, segments) {
    const ticks = [];
    for (let i = 0; i <= segments; i += 1) {
      ticks.push(range.max - ((range.max - range.min) / segments) * i);
    }
    return ticks;
  }

  function formatNumber(value) {
    if (!Number.isFinite(value)) return '—';
    const abs = Math.abs(value);
    const digits = abs >= 100 ? 0 : 2;
    return value.toFixed(digits);
  }

  function toFiniteNumber(value) {
    if (value === null || value === undefined || value === '') return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function parseTimestamp(value) {
    if (!value) return null;
    const ts = Date.parse(value);
    return Number.isFinite(ts) ? ts : null;
  }

  function hexToRgba(hex, alpha) {
    const sanitized = hex.replace('#', '');
    if (sanitized.length !== 6) return `rgba(123, 191, 255, ${alpha})`;
    const r = Number.parseInt(sanitized.slice(0, 2), 16);
    const g = Number.parseInt(sanitized.slice(2, 4), 16);
    const b = Number.parseInt(sanitized.slice(4, 6), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }

  function showMessage(text, type = 'success') {
    if (!refs.messageBox) return;
    const div = document.createElement('div');
    div.className = `alert ${type === 'error' ? 'alert-error' : 'alert-success'}`;
    div.textContent = text;
    refs.messageBox.innerHTML = '';
    refs.messageBox.appendChild(div);
    window.setTimeout(() => {
      if (div.parentNode === refs.messageBox) refs.messageBox.removeChild(div);
    }, 3800);
  }

  async function readJsonResponse(response) {
    const raw = await response.text();
    let payload = {};
    if (raw) {
      try {
        payload = JSON.parse(raw);
      } catch (error) {
        throw new Error(`Сервер вернул невалидный JSON: ${raw.slice(0, 180)}`);
      }
    }
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.error || `HTTP ${response.status}`);
    }
    return payload;
  }

  function requestJson(url, options = {}) {
    const headers = {
      'Content-Type': 'application/json',
      'X-CSRF-Token': csrfToken,
      ...(options.headers || {}),
    };
    return fetch(url, {
      cache: 'no-store',
      credentials: 'same-origin',
      ...options,
      headers,
    }).then(readJsonResponse);
  }

  function findDeviceByHash(deviceHash) {
    return state.devices.find((item) => item.device_hash === deviceHash) || null;
  }

  function refreshDeviceMeta() {
    const device = findDeviceByHash(state.selectedDeviceHash);
    const text = device ? `${device.name} · ${device.thingspeak_url}` : 'Устройство не выбрано';
    if (refs.deviceMeta) refs.deviceMeta.textContent = text;
    if (refs.deviceSelect) refs.deviceSelect.title = text;
  }

  function refreshDeviceSelect() {
    const currentValue = state.selectedDeviceHash;
    refs.deviceSelect.innerHTML = '';
    if (!state.devices.length) {
      const option = document.createElement('option');
      option.textContent = 'Нет устройств';
      option.value = '';
      refs.deviceSelect.appendChild(option);
      refs.deviceSelect.disabled = true;
      state.selectedDeviceHash = null;
      refreshDeviceMeta();
      return;
    }

    refs.deviceSelect.disabled = false;
    state.devices.forEach((device) => {
      const option = document.createElement('option');
      option.value = device.device_hash;
      option.textContent = `${device.name} · ${device.device_hash}`;
      refs.deviceSelect.appendChild(option);
    });

    const selectedExists = state.devices.some((device) => device.device_hash === currentValue);
    state.selectedDeviceHash = selectedExists ? currentValue : state.devices[0].device_hash;
    refs.deviceSelect.value = state.selectedDeviceHash;
    refreshDeviceMeta();
  }

  function buildInputCell(field, value = '', readOnly = false) {
    const td = document.createElement('td');
    const input = document.createElement('input');
    input.dataset.field = field;
    input.value = value;
    input.readOnly = readOnly;
    td.appendChild(input);
    return td;
  }

  function buildDeleteCell() {
    const td = document.createElement('td');
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'danger-inline';
    button.dataset.action = 'delete-row';
    button.textContent = '×';
    td.appendChild(button);
    return td;
  }

  function addDeviceRow(item = {}) {
    const tbody = refs.devicesTable.querySelector('tbody');
    const tr = document.createElement('tr');
    tr.appendChild(buildInputCell('name', item.name || ''));
    tr.appendChild(buildInputCell('thingspeak_url', item.thingspeak_url || ''));
    tr.appendChild(buildInputCell('device_hash', item.device_hash || '', true));
    tr.appendChild(buildDeleteCell());
    tbody.appendChild(tr);
  }

  function addDestinationRow(item = {}) {
    const tbody = refs.destinationsTable.querySelector('tbody');
    const tr = document.createElement('tr');
    tr.appendChild(buildInputCell('name', item.name || ''));
    tr.appendChild(buildInputCell('host', item.host || ''));
    tr.appendChild(buildInputCell('port', item.port || ''));
    tr.appendChild(buildInputCell('path', item.path || ''));
    tr.appendChild(buildDeleteCell());
    tbody.appendChild(tr);
  }

  function collectTableRows(table) {
    return Array.from(table.querySelectorAll('tbody tr'))
      .map((row) => {
        const result = {};
        row.querySelectorAll('input').forEach((input) => {
          result[input.dataset.field] = input.value.trim();
        });
        return result;
      })
      .filter((item) => Object.values(item).some(Boolean));
  }

  function repopulateTable(table, items, kind) {
    const tbody = table.querySelector('tbody');
    tbody.innerHTML = '';
    items.forEach((item) => {
      if (kind === 'device') addDeviceRow(item);
      else addDestinationRow(item);
    });
  }

  async function loadBootstrap() {
    const payload = await requestJson(app.bootstrap_url, { method: 'GET', headers: {} });
    state.devices = Array.isArray(payload.devices) ? payload.devices : [];
    state.destinations = Array.isArray(payload.destinations) ? payload.destinations : [];
    refreshDeviceSelect();
    repopulateTable(refs.devicesTable, state.devices, 'device');
    repopulateTable(refs.destinationsTable, state.destinations, 'destination');
  }

  async function loadMeasurements({ silent = false } = {}) {
    if (!state.selectedDeviceHash) {
      Object.values(charts).forEach((chart) => chart.setData([]));
      return;
    }

    const params = new URLSearchParams({
      device_hash: state.selectedDeviceHash,
      limit: '5000',
      _ts: String(Date.now()),
    });

    try {
      const payload = await requestJson(`${app.measurements_url}?${params.toString()}`, { method: 'GET', headers: {} });
      const items = Array.isArray(payload.items) ? payload.items : [];
      state.measurements = items;
      Object.values(charts).forEach((chart) => chart.setData(items));
      state.lastLoadedDeviceHash = state.selectedDeviceHash;
      if (!silent) {
        const device = findDeviceByHash(state.selectedDeviceHash);
        showMessage(`Загружено ${items.length} точек${device ? ` · ${device.name}` : ''}`);
      }
    } catch (error) {
      showMessage(error.message || 'Не удалось загрузить данные', 'error');
    }
  }

  async function saveDevices() {
    const devices = collectTableRows(refs.devicesTable);
    const payload = await requestJson(app.replace_devices_url, {
      method: 'POST',
      body: JSON.stringify({ devices }),
    });
    state.devices = Array.isArray(payload.devices) ? payload.devices : [];
    refreshDeviceSelect();
    repopulateTable(refs.devicesTable, state.devices, 'device');
    await loadMeasurements({ silent: true });
    showMessage('Устройства сохранены');
  }

  async function saveDestinations() {
    const destinations = collectTableRows(refs.destinationsTable);
    const payload = await requestJson(app.replace_destinations_url, {
      method: 'POST',
      body: JSON.stringify({ destinations }),
    });
    state.destinations = Array.isArray(payload.destinations) ? payload.destinations : [];
    repopulateTable(refs.destinationsTable, state.destinations, 'destination');
    showMessage('Серверы сохранены');
  }

  function wireTableDelete(table) {
    table.addEventListener('click', (event) => {
      const button = event.target.closest('[data-action="delete-row"]');
      if (!button) return;
      const row = button.closest('tr');
      if (row) row.remove();
    });
  }

  async function refreshLoop() {
    try {
      const bootstrap = await requestJson(app.bootstrap_url, { method: 'GET', headers: {} });
      const incomingDevices = Array.isArray(bootstrap.devices) ? bootstrap.devices : [];
      const incomingDestinations = Array.isArray(bootstrap.destinations) ? bootstrap.destinations : [];
      if (JSON.stringify(incomingDevices) !== JSON.stringify(state.devices)) {
        state.devices = incomingDevices;
        refreshDeviceSelect();
        repopulateTable(refs.devicesTable, state.devices, 'device');
      }
      if (JSON.stringify(incomingDestinations) !== JSON.stringify(state.destinations)) {
        state.destinations = incomingDestinations;
        repopulateTable(refs.destinationsTable, state.destinations, 'destination');
      }
      await loadMeasurements({ silent: true });
    } catch (error) {
      showMessage(error.message || 'Ошибка фонового обновления', 'error');
    }
  }

  async function init() {
    refs.themeButtons.forEach((button) => {
      button.addEventListener('click', () => applyTheme(button.dataset.themeValue));
    });
    applyTheme(state.theme);

    refreshDeviceSelect();
    refreshDeviceMeta();
    wireTableDelete(refs.devicesTable);
    wireTableDelete(refs.destinationsTable);

    document.getElementById('add-device').addEventListener('click', () => addDeviceRow());
    document.getElementById('add-destination').addEventListener('click', () => addDestinationRow());
    document.getElementById('save-devices').addEventListener('click', async () => {
      try {
        await saveDevices();
      } catch (error) {
        showMessage(error.message || 'Не удалось сохранить устройства', 'error');
      }
    });
    document.getElementById('save-destinations').addEventListener('click', async () => {
      try {
        await saveDestinations();
      } catch (error) {
        showMessage(error.message || 'Не удалось сохранить конечные серверы', 'error');
      }
    });

    refs.deviceSelect.addEventListener('change', async () => {
      state.selectedDeviceHash = refs.deviceSelect.value || null;
      refreshDeviceMeta();
      await loadMeasurements({ silent: true });
    });

    await loadBootstrap();
    await loadMeasurements({ silent: true });
    state.autoRefreshHandle = window.setInterval(refreshLoop, refreshIntervalMs);
  }

  init().catch((error) => {
    showMessage(error.message || 'Ошибка инициализации интерфейса', 'error');
  });
})();
