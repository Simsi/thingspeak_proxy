(() => {
  const app = window.APP_BOOTSTRAP || {};
  const csrfToken = app.csrf_token;
  const messageBox = document.getElementById('message-box');
  const deviceSelect = document.getElementById('device-select');
  const deviceMeta = document.getElementById('device-meta');
  const devicesTable = document.getElementById('devices-table');
  const destinationsTable = document.getElementById('destinations-table');
  const chartConfigs = [
    { svgId: 'chart-warm-stream', fieldName: 'warm_stream', label: 'Тепловой поток', unit: 'Вт/м²' },
    { svgId: 'chart-surface-temp', fieldName: 'surface_temp', label: 'Температура поверхности', unit: '°C' },
    { svgId: 'chart-air-temp', fieldName: 'air_temp', label: 'Температура воздуха', unit: '°C' },
    { svgId: 'chart-air-hum', fieldName: 'air_hum', label: 'Влажность воздуха', unit: '%' },
  ];

  function showMessage(text, type = 'success') {
    if (!messageBox) return;
    const div = document.createElement('div');
    div.className = `alert ${type === 'error' ? 'alert-error' : 'alert-success'}`;
    div.textContent = text;
    messageBox.innerHTML = '';
    messageBox.appendChild(div);
    window.setTimeout(() => {
      if (div.parentNode === messageBox) {
        messageBox.removeChild(div);
      }
    }, 5000);
  }

  async function readJsonResponse(response) {
    const rawText = await response.text();
    let payload = {};
    if (rawText) {
      try {
        payload = JSON.parse(rawText);
      } catch (error) {
        const preview = rawText.slice(0, 220).replace(/\s+/g, ' ').trim();
        throw new Error(`Сервер вернул невалидный JSON: ${preview || 'пустой ответ'}`);
      }
    }
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.error || `HTTP ${response.status}`);
    }
    return payload;
  }

  function requestJson(url, options = {}) {
    const headers = Object.assign({}, options.headers || {}, {
      'Content-Type': 'application/json',
      'X-CSRF-Token': csrfToken,
    });
    return fetch(url, Object.assign({}, options, {
      headers,
      cache: 'no-store',
      credentials: options.credentials || 'same-origin',
    })).then(readJsonResponse);
  }

  function toFiniteNumber(value) {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  function parseTimestamp(value) {
    if (!value) return null;
    const ts = Date.parse(value);
    return Number.isFinite(ts) ? ts : null;
  }

  function buildInputCell(field, value = '', readonly = false) {
    const td = document.createElement('td');
    const input = document.createElement('input');
    input.dataset.field = field;
    input.value = value;
    if (readonly) input.readOnly = true;
    td.appendChild(input);
    return td;
  }

  function buildDeleteCell() {
    const td = document.createElement('td');
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'danger-inline';
    button.dataset.action = 'delete-row';
    button.textContent = 'Удалить';
    td.appendChild(button);
    return td;
  }

  function addDeviceRow(item = {}) {
    const tbody = devicesTable.querySelector('tbody');
    const tr = document.createElement('tr');
    tr.appendChild(buildInputCell('name', item.name || ''));
    tr.appendChild(buildInputCell('thingspeak_url', item.thingspeak_url || ''));
    tr.appendChild(buildInputCell('device_hash', item.device_hash || '', true));
    tr.appendChild(buildDeleteCell());
    tbody.appendChild(tr);
  }

  function addDestinationRow(item = {}) {
    const tbody = destinationsTable.querySelector('tbody');
    const tr = document.createElement('tr');
    tr.appendChild(buildInputCell('name', item.name || ''));
    tr.appendChild(buildInputCell('host', item.host || ''));
    tr.appendChild(buildInputCell('port', item.port || ''));
    tr.appendChild(buildInputCell('path', item.path || ''));
    tr.appendChild(buildDeleteCell());
    tbody.appendChild(tr);
  }

  function collectTableRows(tableId) {
    const table = document.getElementById(tableId);
    const rows = Array.from(table.querySelectorAll('tbody tr'));
    return rows.map((tr) => {
      const item = {};
      tr.querySelectorAll('input').forEach((input) => {
        item[input.dataset.field] = input.value.trim();
      });
      return item;
    }).filter((item) => Object.values(item).some(Boolean));
  }

  function findDeviceByHash(deviceHash) {
    return (app.devices || []).find((device) => device.device_hash === deviceHash) || null;
  }

  function updateDeviceMeta(device) {
    if (!deviceMeta) return;
    if (!device) {
      deviceMeta.textContent = 'Устройство не выбрано';
      return;
    }
    deviceMeta.textContent = `Устройство: ${device.name} · Канал: ${device.device_hash}`;
  }

  function refreshDeviceSelect(devices, preserveValue) {
    const selected = preserveValue || deviceSelect.value;
    deviceSelect.innerHTML = '';
    devices.forEach((device) => {
      const option = document.createElement('option');
      option.value = device.device_hash;
      option.textContent = `${device.name} (${device.device_hash})`;
      option.dataset.name = device.name;
      option.dataset.url = device.thingspeak_url || '';
      deviceSelect.appendChild(option);
    });
    if (devices.length === 0) {
      const option = document.createElement('option');
      option.value = '';
      option.textContent = 'Нет устройств';
      deviceSelect.appendChild(option);
      deviceSelect.disabled = true;
      updateDeviceMeta(null);
    } else {
      deviceSelect.disabled = false;
      const exists = devices.some((device) => device.device_hash === selected);
      deviceSelect.value = exists ? selected : devices[0].device_hash;
      updateDeviceMeta(findDeviceByHash(deviceSelect.value));
    }
  }

  function formatValue(value) {
    return value == null || Number.isNaN(value) ? '—' : Number(value).toFixed(2);
  }

  function formatAxisTime(timestamp) {
    if (!timestamp) return '';
    return new Date(timestamp).toLocaleString('ru-RU', {
      day: '2-digit',
      month: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
  }

  function escapeXml(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function normalizeItems(items) {
    return (items || []).map((item) => ({
      event_id: Number(item.event_id || 0),
      device_name: item.device_name || '',
      device_hash: item.device_hash || '',
      source_created_at: item.source_created_at || null,
      source_ts: parseTimestamp(item.source_created_at),
      warm_stream: toFiniteNumber(item.warm_stream),
      surface_temp: toFiniteNumber(item.surface_temp),
      air_temp: toFiniteNumber(item.air_temp),
      air_hum: toFiniteNumber(item.air_hum),
    })).sort((a, b) => {
      const ta = a.source_ts;
      const tb = b.source_ts;
      if (ta !== null && tb !== null && ta !== tb) return ta - tb;
      if (ta !== null && tb === null) return -1;
      if (ta === null && tb !== null) return 1;
      return a.event_id - b.event_id;
    });
  }

  function renderPlaceholder(svg, text) {
    svg.innerHTML = `
      <rect x="0" y="0" width="560" height="260" fill="#fafafa" rx="12" ry="12"></rect>
      <text x="280" y="130" text-anchor="middle" class="placeholder-text">${escapeXml(text)}</text>
    `;
  }

  function renderChart(svgId, items, fieldName, label, unit) {
    const svg = document.getElementById(svgId);
    if (!svg) return;

    const width = 560;
    const height = 260;
    const padLeft = 52;
    const padRight = 18;
    const padTop = 18;
    const padBottom = 42;
    const plotWidth = width - padLeft - padRight;
    const plotHeight = height - padTop - padBottom;

    const withValues = items.filter((item) => item[fieldName] !== null);
    if (!withValues.length) {
      renderPlaceholder(svg, `Нет данных для «${label}»`);
      return;
    }

    let min = Math.min(...withValues.map((item) => item[fieldName]));
    let max = Math.max(...withValues.map((item) => item[fieldName]));
    if (min === max) {
      min -= 1;
      max += 1;
    }
    const range = max - min;

    const xValue = (item) => item.source_ts ?? item.event_id;
    const minX = Math.min(...withValues.map(xValue));
    const maxX = Math.max(...withValues.map(xValue));
    const xRange = Math.max(maxX - minX, 1);

    const points = withValues.map((item) => {
      const x = padLeft + ((xValue(item) - minX) / xRange) * plotWidth;
      const y = padTop + ((max - item[fieldName]) / range) * plotHeight;
      return { x, y };
    });

    const polyline = points.map((p) => `${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(' ');
    const grid = [];
    for (let i = 0; i < 5; i += 1) {
      const y = padTop + (plotHeight * i) / 4;
      const value = (max - (range * i) / 4).toFixed(2);
      grid.push(`<line class="grid-line" x1="${padLeft}" y1="${y}" x2="${width - padRight}" y2="${y}" />`);
      grid.push(`<text class="axis-text" x="8" y="${y + 4}">${value}</text>`);
    }

    const firstItem = withValues[0];
    const midItem = withValues[Math.floor(withValues.length / 2)];
    const lastItem = withValues[withValues.length - 1];
    const circles = points.length <= 400
      ? points.map((p) => `<circle class="chart-dot" cx="${p.x.toFixed(2)}" cy="${p.y.toFixed(2)}" r="2.5"></circle>`).join('')
      : '';

    const xLabelLeft = firstItem.source_ts ? formatAxisTime(firstItem.source_ts) : `event ${firstItem.event_id}`;
    const xLabelMid = midItem.source_ts ? formatAxisTime(midItem.source_ts) : `event ${midItem.event_id}`;
    const xLabelRight = lastItem.source_ts ? formatAxisTime(lastItem.source_ts) : `event ${lastItem.event_id}`;

    svg.innerHTML = `
      <rect x="0" y="0" width="${width}" height="${height}" fill="#fafafa" rx="12" ry="12"></rect>
      ${grid.join('')}
      <line class="axis-line" x1="${padLeft}" y1="${height - padBottom}" x2="${width - padRight}" y2="${height - padBottom}" />
      <line class="axis-line" x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${height - padBottom}" />
      <polyline class="chart-line" points="${polyline}"></polyline>
      ${circles}
      <text class="axis-text" x="${padLeft}" y="${height - 12}">${escapeXml(xLabelLeft)}</text>
      <text class="axis-text" x="${padLeft + plotWidth / 2 - 35}" y="${height - 12}">${escapeXml(xLabelMid)}</text>
      <text class="axis-text" x="${width - padRight - 92}" y="${height - 12}">${escapeXml(xLabelRight)}</text>
      <text class="axis-text" x="${width - 190}" y="18">последнее ${formatValue(lastItem[fieldName])} ${escapeXml(unit)}</text>
    `;
  }

  function renderAllCharts(items) {
    chartConfigs.forEach((config) => {
      renderChart(config.svgId, items, config.fieldName, config.label, config.unit);
    });
  }

  async function loadMeasurements(deviceHash) {
    updateDeviceMeta(findDeviceByHash(deviceHash));
    if (!deviceHash) {
      renderAllCharts([]);
      return;
    }
    try {
      const url = new URL(app.measurements_url, window.location.origin);
      url.searchParams.set('device_hash', deviceHash);
      url.searchParams.set('limit', '5000');
      url.searchParams.set('_ts', Date.now().toString());
      const payload = await fetch(url.toString(), { credentials: 'same-origin', cache: 'no-store' }).then(readJsonResponse);
      renderAllCharts(normalizeItems(payload.items || []));
    } catch (error) {
      renderAllCharts([]);
      showMessage(error.message, 'error');
    }
  }

  async function saveDevices() {
    try {
      const devices = collectTableRows('devices-table');
      const payload = await requestJson(app.replace_devices_url, {
        method: 'POST',
        credentials: 'same-origin',
        body: JSON.stringify({ devices }),
      });
      app.devices = payload.devices || [];
      const currentValue = deviceSelect.value;
      devicesTable.querySelector('tbody').innerHTML = '';
      app.devices.forEach(addDeviceRow);
      refreshDeviceSelect(app.devices, currentValue);
      await loadMeasurements(deviceSelect.value);
      showMessage('Список устройств сохранён');
    } catch (error) {
      showMessage(error.message, 'error');
    }
  }

  async function saveDestinations() {
    try {
      const destinations = collectTableRows('destinations-table');
      const payload = await requestJson(app.replace_destinations_url, {
        method: 'POST',
        credentials: 'same-origin',
        body: JSON.stringify({ destinations }),
      });
      app.destinations = payload.destinations || [];
      destinationsTable.querySelector('tbody').innerHTML = '';
      app.destinations.forEach(addDestinationRow);
      showMessage('Список конечных серверов сохранён');
    } catch (error) {
      showMessage(error.message, 'error');
    }
  }

  document.addEventListener('click', (event) => {
    const button = event.target.closest('button');
    if (!button) return;
    if (button.dataset.action === 'delete-row') {
      const row = button.closest('tr');
      if (row) row.remove();
    }
  });

  document.getElementById('add-device')?.addEventListener('click', () => addDeviceRow());
  document.getElementById('add-destination')?.addEventListener('click', () => addDestinationRow());
  document.getElementById('save-devices')?.addEventListener('click', saveDevices);
  document.getElementById('save-destinations')?.addEventListener('click', saveDestinations);
  deviceSelect?.addEventListener('change', () => loadMeasurements(deviceSelect.value));

  refreshDeviceSelect(app.devices || [], app.devices?.[0]?.device_hash || '');
  loadMeasurements(deviceSelect.value);

  const refreshIntervalMs = Number(app.refresh_interval_ms || 15000);
  if (Number.isFinite(refreshIntervalMs) && refreshIntervalMs >= 3000) {
    window.setInterval(() => {
      if (deviceSelect && deviceSelect.value) {
        loadMeasurements(deviceSelect.value);
      }
    }, refreshIntervalMs);
  }
})();
