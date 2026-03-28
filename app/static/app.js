(() => {
  const app = window.APP_BOOTSTRAP || {};
  const csrfToken = app.csrf_token;
  const messageBox = document.getElementById('message-box');
  const deviceSelect = document.getElementById('device-select');
  const devicesTable = document.getElementById('devices-table');
  const destinationsTable = document.getElementById('destinations-table');

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

  function requestJson(url, options = {}) {
    const headers = Object.assign({}, options.headers || {}, {
      'Content-Type': 'application/json',
      'X-CSRF-Token': csrfToken,
    });
    return fetch(url, Object.assign({}, options, { headers })).then(async (response) => {
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.error || `HTTP ${response.status}`);
      }
      return payload;
    });
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

  function refreshDeviceSelect(devices, preserveValue) {
    const selected = preserveValue || deviceSelect.value;
    deviceSelect.innerHTML = '';
    devices.forEach((device) => {
      const option = document.createElement('option');
      option.value = device.name;
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
      const exists = devices.some((device) => device.name === selected);
      deviceSelect.value = exists ? selected : devices[0].name;
    }
  }

  function formatValue(value) {
    return value == null || Number.isNaN(value) ? '—' : Number(value).toFixed(2);
  }

  function renderChart(svgId, items, fieldName, label) {
    const svg = document.getElementById(svgId);
    if (!svg) return;
    const width = 500;
    const height = 240;
    const padLeft = 40;
    const padRight = 16;
    const padTop = 16;
    const padBottom = 32;
    const plotWidth = width - padLeft - padRight;
    const plotHeight = height - padTop - padBottom;

    const values = items.map((item) => item[fieldName]).filter((x) => x !== null && x !== undefined);
    if (values.length === 0) {
      svg.innerHTML = `<rect x="0" y="0" width="${width}" height="${height}" fill="#fafafa" />
        <text x="250" y="120" text-anchor="middle" class="placeholder-text">Нет данных для ${label}</text>`;
      return;
    }

    let min = Math.min(...values);
    let max = Math.max(...values);
    if (min === max) {
      min -= 1;
      max += 1;
    }

    const validItems = items.filter((item) => item[fieldName] !== null && item[fieldName] !== undefined);
    const points = validItems.map((item, idx) => {
      const x = padLeft + (idx * plotWidth) / Math.max(validItems.length - 1, 1);
      const y = padTop + ((max - item[fieldName]) * plotHeight) / (max - min);
      return `${x},${y}`;
    }).join(' ');

    const gridLines = [];
    for (let i = 0; i < 5; i += 1) {
      const y = padTop + (plotHeight * i) / 4;
      const value = (max - ((max - min) * i) / 4).toFixed(2);
      gridLines.push(`<line class="grid-line" x1="${padLeft}" y1="${y}" x2="${width - padRight}" y2="${y}" />`);
      gridLines.push(`<text class="axis-text" x="6" y="${y + 4}">${value}</text>`);
    }

    const firstEvent = validItems[0]?.event_id ?? '';
    const lastEvent = validItems[validItems.length - 1]?.event_id ?? '';

    svg.innerHTML = `
      <rect x="0" y="0" width="${width}" height="${height}" fill="#fafafa" rx="10" ry="10"></rect>
      ${gridLines.join('')}
      <polyline class="chart-line" points="${points}"></polyline>
      <text class="axis-text" x="${padLeft}" y="${height - 10}">event ${firstEvent}</text>
      <text class="axis-text" x="${width - padRight - 70}" y="${height - 10}">event ${lastEvent}</text>
      <text class="axis-text" x="${width - 130}" y="18">min ${formatValue(min)} / max ${formatValue(max)}</text>
    `;
  }

  async function loadMeasurements(deviceName) {
    if (!deviceName) {
      renderChart('chart-warm-stream', [], 'warm_stream', 'теплового потока');
      renderChart('chart-surface-temp', [], 'surface_temp', 'температуры поверхности');
      renderChart('chart-air-temp', [], 'air_temp', 'температуры воздуха');
      renderChart('chart-air-hum', [], 'air_hum', 'влажности воздуха');
      return;
    }
    try {
      const url = `${app.measurements_url}?device_name=${encodeURIComponent(deviceName)}`;
      const payload = await fetch(url, { credentials: 'same-origin' }).then((response) => response.json());
      if (!payload.ok) {
        throw new Error(payload.error || 'Не удалось загрузить измерения');
      }
      const items = payload.items || [];
      renderChart('chart-warm-stream', items, 'warm_stream', 'теплового потока');
      renderChart('chart-surface-temp', items, 'surface_temp', 'температуры поверхности');
      renderChart('chart-air-temp', items, 'air_temp', 'температуры воздуха');
      renderChart('chart-air-hum', items, 'air_hum', 'влажности воздуха');
    } catch (error) {
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
      const currentValue = deviceSelect.value;
      devicesTable.querySelector('tbody').innerHTML = '';
      payload.devices.forEach(addDeviceRow);
      refreshDeviceSelect(payload.devices, currentValue);
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
  deviceSelect?.addEventListener('change', () => loadMeasurements(deviceSelect.value));

  refreshDeviceSelect(app.devices || [], app.devices?.[0]?.name || '');
  loadMeasurements(deviceSelect.value);
})();
