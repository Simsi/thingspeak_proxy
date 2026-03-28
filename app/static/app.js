(() => {
  const app = window.APP_BOOTSTRAP || {};
  const csrfToken = app.csrf_token;
  const messageBox = document.getElementById('message-box');
  const deviceSelect = document.getElementById('device-select');
  const devicesTable = document.getElementById('devices-table');
  const destinationsTable = document.getElementById('destinations-table');
  const selectedDeviceMeta = document.getElementById('selected-device-meta');

  function showMessage(text, type = 'success') {
    if (!messageBox) return;
    const div = document.createElement('div');
    div.className = `alert ${type === 'error' ? 'alert-error' : 'alert-success'}`;
    div.textContent = text;
    messageBox.innerHTML = '';
    messageBox.appendChild(div);
    setTimeout(() => {
      if (div.parentNode === messageBox) {
        messageBox.removeChild(div);
      }
    }, 4500);
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

  function getSelectedDevice() {
    if (!deviceSelect || !deviceSelect.selectedOptions || deviceSelect.selectedOptions.length === 0) {
      return null;
    }
    const option = deviceSelect.selectedOptions[0];
    return {
      deviceHash: option.value || '',
      deviceName: option.dataset.deviceName || '',
    };
  }

  function updateSelectedDeviceMeta() {
    if (!selectedDeviceMeta) return;
    const selected = getSelectedDevice();
    if (!selected || !selected.deviceHash) {
      selectedDeviceMeta.textContent = 'Устройство не выбрано';
      return;
    }
    selectedDeviceMeta.textContent = `Устройство: ${selected.deviceName} · канал ${selected.deviceHash}`;
  }

  function refreshDeviceSelect(devices, preserveHash) {
    const previousHash = preserveHash || getSelectedDevice()?.deviceHash || '';
    deviceSelect.innerHTML = '';
    devices.forEach((device) => {
      const option = document.createElement('option');
      option.value = device.device_hash;
      option.dataset.deviceName = device.name;
      option.textContent = `${device.name} (${device.device_hash})`;
      deviceSelect.appendChild(option);
    });
    if (devices.length === 0) {
      const option = document.createElement('option');
      option.value = '';
      option.textContent = 'Нет устройств';
      deviceSelect.appendChild(option);
      deviceSelect.disabled = true;
    } else {
      deviceSelect.disabled = false;
      const exists = devices.some((device) => device.device_hash === previousHash);
      deviceSelect.value = exists ? previousHash : devices[0].device_hash;
    }
    updateSelectedDeviceMeta();
  }

  function formatValue(value) {
    return Number.isFinite(value) ? value.toFixed(2) : '—';
  }

  function formatShortTimestamp(value) {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return date.toLocaleString('ru-RU', {
      day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit'
    });
  }

  function normalizeChartItems(items, fieldName) {
    return items
      .map((item) => {
        const numericValue = Number(item[fieldName]);
        return {
          ...item,
          numericValue: Number.isFinite(numericValue) ? numericValue : null,
          xLabel: formatShortTimestamp(item.source_created_at || item.inserted_at),
        };
      })
      .filter((item) => item.numericValue !== null);
  }

  function renderChart(svgId, items, fieldName, label) {
    const svg = document.getElementById(svgId);
    if (!svg) return;
    const width = 500;
    const height = 240;
    const padLeft = 50;
    const padRight = 20;
    const padTop = 16;
    const padBottom = 38;
    const plotWidth = width - padLeft - padRight;
    const plotHeight = height - padTop - padBottom;

    const validItems = normalizeChartItems(items, fieldName);
    if (validItems.length === 0) {
      svg.innerHTML = `<rect x="0" y="0" width="${width}" height="${height}" fill="#fafafa" />
        <text x="250" y="120" text-anchor="middle" class="placeholder-text">Нет данных для ${label}</text>`;
      return;
    }

    const values = validItems.map((item) => item.numericValue);
    let min = Math.min(...values);
    let max = Math.max(...values);
    if (min === max) {
      min -= 1;
      max += 1;
    }

    const points = validItems.map((item, idx) => {
      const x = padLeft + (idx * plotWidth) / Math.max(validItems.length - 1, 1);
      const y = padTop + ((max - item.numericValue) * plotHeight) / (max - min);
      return `${x},${y}`;
    }).join(' ');

    const circles = validItems.map((item, idx) => {
      const x = padLeft + (idx * plotWidth) / Math.max(validItems.length - 1, 1);
      const y = padTop + ((max - item.numericValue) * plotHeight) / (max - min);
      return `<circle cx="${x}" cy="${y}" r="2.8" class="chart-point"></circle>`;
    }).join('');

    const gridLines = [];
    for (let i = 0; i < 5; i += 1) {
      const y = padTop + (plotHeight * i) / 4;
      const value = (max - ((max - min) * i) / 4).toFixed(2);
      gridLines.push(`<line class="grid-line" x1="${padLeft}" y1="${y}" x2="${width - padRight}" y2="${y}" />`);
      gridLines.push(`<text class="axis-text" x="6" y="${y + 4}">${value}</text>`);
    }

    const firstItem = validItems[0];
    const lastItem = validItems[validItems.length - 1];
    const lastValue = lastItem.numericValue;

    svg.innerHTML = `
      <rect x="0" y="0" width="${width}" height="${height}" fill="#fafafa" rx="10" ry="10"></rect>
      ${gridLines.join('')}
      <line class="axis-line" x1="${padLeft}" y1="${height - padBottom}" x2="${width - padRight}" y2="${height - padBottom}"></line>
      <line class="axis-line" x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${height - padBottom}"></line>
      <polyline class="chart-line" points="${points}"></polyline>
      ${circles}
      <text class="axis-text" x="${padLeft}" y="${height - 10}">${firstItem.xLabel || `event ${firstItem.event_id}`}</text>
      <text class="axis-text" x="${Math.max(padLeft + 90, width - 150)}" y="${height - 10}">${lastItem.xLabel || `event ${lastItem.event_id}`}</text>
      <text class="axis-text" x="${width - 165}" y="18">последнее ${formatValue(lastValue)}</text>
    `;
  }

  async function loadMeasurementsBySelection() {
    const selected = getSelectedDevice();
    if (!selected || !selected.deviceHash) {
      renderChart('chart-warm-stream', [], 'warm_stream', 'теплового потока');
      renderChart('chart-surface-temp', [], 'surface_temp', 'температуры поверхности');
      renderChart('chart-air-temp', [], 'air_temp', 'температуры воздуха');
      renderChart('chart-air-hum', [], 'air_hum', 'влажности воздуха');
      updateSelectedDeviceMeta();
      return;
    }

    try {
      updateSelectedDeviceMeta();
      const params = new URLSearchParams({
        device_hash: selected.deviceHash,
        _ts: String(Date.now()),
      });
      const url = `${app.measurements_url}?${params.toString()}`;
      const payload = await fetch(url, { credentials: 'same-origin', cache: 'no-store' }).then(readJsonResponse);
      const items = Array.isArray(payload.items) ? payload.items : [];
      renderChart('chart-warm-stream', items, 'warm_stream', 'теплового потока');
      renderChart('chart-surface-temp', items, 'surface_temp', 'температуры поверхности');
      renderChart('chart-air-temp', items, 'air_temp', 'температуры воздуха');
      renderChart('chart-air-hum', items, 'air_hum', 'влажности воздуха');
      if (items.length === 0) {
        showMessage(`Для устройства ${selected.deviceName} пока нет данных в БД`, 'error');
      }
    } catch (error) {
      showMessage(error.message, 'error');
    }
  }

  async function saveDevices() {
    try {
      const devices = collectTableRows('devices-table');
      const currentHash = getSelectedDevice()?.deviceHash || '';
      const payload = await requestJson(app.replace_devices_url, {
        method: 'POST',
        credentials: 'same-origin',
        body: JSON.stringify({ devices }),
      });
      devicesTable.querySelector('tbody').innerHTML = '';
      payload.devices.forEach(addDeviceRow);
      refreshDeviceSelect(payload.devices, currentHash);
      await loadMeasurementsBySelection();
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
      destinationsTable.querySelector('tbody').innerHTML = '';
      payload.destinations.forEach(addDestinationRow);
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
  deviceSelect?.addEventListener('change', loadMeasurementsBySelection);

  refreshDeviceSelect(app.devices || [], app.devices?.[0]?.device_hash || '');
  loadMeasurementsBySelection();

  const refreshIntervalMs = Number(app.refresh_interval_ms || 15000);
  if (Number.isFinite(refreshIntervalMs) && refreshIntervalMs >= 3000) {
    window.setInterval(() => {
      if (deviceSelect && deviceSelect.value) {
        loadMeasurementsBySelection();
      }
    }, refreshIntervalMs);
  }
})();
