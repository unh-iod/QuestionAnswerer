/* ============================================================
   ACS Disability Statistics Explorer — app.js
   ============================================================ */

'use strict';

// ── State ────────────────────────────────────────────────────────────────────
const S = {
  schema:      null,
  geoLevel:    'state',      // 'state' | 'county'
  disability:  null,
  measure:     null,
  geos:        [],           // selected geography names
  yearMode:    'single',     // 'single' | 'all'
  year:        null,
  filterI:     null,
  tableData:   null,         // { columns, rows, geo_col }
  sortCol:     null,
  sortDir:     'asc',
  viewMode:    'full',       // 'full' | 'summary'
  summaryMode: 'max',        // 'max' | 'min' | 'geo'
  summaryGeo:  null,
};

// ── DOM refs ─────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);

// ── Init ─────────────────────────────────────────────────────────────────────
async function init() {
  const res = await fetch('/api/schema');
  S.schema = await res.json();
  buildDisabilitySelect();
  buildMeasureGrid();
  buildYearSelect();
  wireControls();
  updatePills();
}

// ── Step 1 builders ───────────────────────────────────────────────────────────
function buildDisabilitySelect() {
  const sel = $('selectDisability');
  sel.innerHTML = S.schema.disability_types
    .map(d => `<option value="${d.value}">${d.label}</option>`)
    .join('');
  S.disability = S.schema.disability_types[0].value;
}

function buildMeasureGrid() {
  const measures = S.geoLevel === 'state'
    ? S.schema.state_measures
    : S.schema.county_measures;

  const groups = {};
  measures.forEach(m => {
    if (!groups[m.group]) groups[m.group] = [];
    groups[m.group].push(m);
  });

  let html = '';
  Object.entries(groups).forEach(([grp, items]) => {
    html += `<div class="measure-group-label">${grp}</div>`;
    items.forEach(m => {
      html += `<button class="measure-btn${S.measure === m.value ? ' selected' : ''}"
                       data-value="${m.value}">${m.label}</button>`;
    });
  });
  $('measureGroups').innerHTML = html;

  document.querySelectorAll('.measure-btn').forEach(btn => {
    btn.addEventListener('click', () => selectMeasure(btn.dataset.value));
  });
}

function selectMeasure(val) {
  S.measure = val;
  document.querySelectorAll('.measure-btn').forEach(b => {
    b.classList.toggle('selected', b.dataset.value === val);
  });
  onMeasureOrGeoLevelChange();
}

// ── Step 2: geography list ────────────────────────────────────────────────────
function buildGeoList(filter = '') {
  const list = S.geoLevel === 'state'
    ? S.schema.us_states
    : [];                          // county list populated dynamically if needed

  const lower = filter.toLowerCase();
  const filtered = list.filter(g => g.toLowerCase().includes(lower));

  $('geoList').innerHTML = filtered.map(g => {
    const checked = S.geos.includes(g);
    return `<label class="geo-item${checked ? ' checked' : ''}">
      <input type="checkbox" value="${g}" ${checked ? 'checked' : ''} />
      ${g}
    </label>`;
  }).join('');

  $('geoList').querySelectorAll('input[type=checkbox]').forEach(cb => {
    cb.addEventListener('change', () => {
      if (cb.checked) {
        if (!S.geos.includes(cb.value)) S.geos.push(cb.value);
      } else {
        S.geos = S.geos.filter(g => g !== cb.value);
      }
      cb.closest('.geo-item').classList.toggle('checked', cb.checked);
      onGeoSelectionChange();
    });
  });
}

// ── Step 3: year select ───────────────────────────────────────────────────────
function buildYearSelect() {
  const years = S.geoLevel === 'state'
    ? S.schema.state_years
    : S.schema.county_years;

  const sel = $('selectYear');
  sel.innerHTML = [...years].reverse()
    .map(y => `<option value="${y}">${y}</option>`)
    .join('');

  S.year = years[years.length - 1]; // default to most recent
  sel.value = S.year;
}

// ── Step 4: filter select ─────────────────────────────────────────────────────
async function buildFilterSelect() {
  let filters;
  if (S.geoLevel === 'state') {
    filters = S.schema.state_filters;
  } else {
    const res = await fetch(`/api/county_filters?measure=${S.measure}`);
    const data = await res.json();
    filters = data.filters;
  }

  $('selectFilter').innerHTML = filters
    .map(f => `<option value="${f.i}">${f.label}</option>`)
    .join('');

  S.filterI = filters[0].i;
  $('selectFilter').value = S.filterI;
}

// ── Wiring ────────────────────────────────────────────────────────────────────
function wireControls() {
  // Geo level segmented control
  document.querySelectorAll('#geoLevelControl .seg-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#geoLevelControl .seg-btn')
        .forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      S.geoLevel = btn.dataset.value;
      S.measure  = null;
      S.geos     = [];
      onGeoLevelChange();
    });
  });

  // Disability select
  $('selectDisability').addEventListener('change', e => {
    S.disability = e.target.value;
    updatePills();
    checkReadiness();
  });

  // Year mode
  document.querySelectorAll('#yearModeControl .seg-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#yearModeControl .seg-btn')
        .forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      S.yearMode = btn.dataset.value;
      $('yearPickerWrap').classList.toggle('hidden', S.yearMode === 'all');
      onYearChange();
    });
  });

  $('selectYear').addEventListener('change', e => {
    S.year = parseInt(e.target.value);
    onYearChange();
  });

  // Filter select
  $('selectFilter').addEventListener('change', e => {
    S.filterI = parseInt(e.target.value);
    updatePills();
    checkReadiness();
  });

  // Geo search
  $('geoSearch').addEventListener('input', e => buildGeoList(e.target.value));

  // Select all / clear
  $('btnSelectAll').addEventListener('click', () => {
    const geos = S.geoLevel === 'state' ? S.schema.us_states : [];
    S.geos = [...geos];
    buildGeoList($('geoSearch').value);
    onGeoSelectionChange();
  });
  $('btnClearAll').addEventListener('click', () => {
    S.geos = [];
    buildGeoList($('geoSearch').value);
    onGeoSelectionChange();
  });

  // Show data
  $('btnShowData').addEventListener('click', fetchData);

  // View toggle
  document.querySelectorAll('#viewToggle .vt-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#viewToggle .vt-btn')
        .forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      S.viewMode = btn.dataset.view;
      $('summaryBar').classList.toggle('hidden', S.viewMode === 'full');
      renderTable();
    });
  });

  // Summary mode
  document.querySelectorAll('#summaryModeControl .seg-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#summaryModeControl .seg-btn')
        .forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      S.summaryMode = btn.dataset.value;
      $('geoPickerWrap').classList.toggle('hidden', S.summaryMode !== 'geo');
      renderTable();
    });
  });

  $('selectSummaryGeo').addEventListener('change', e => {
    S.summaryGeo = e.target.value;
    renderTable();
  });

  // Downloads
  $('btnDlCSV').addEventListener('click',  () => downloadTable('csv'));
  $('btnDlXLSX').addEventListener('click', () => downloadTable('xlsx'));
}

// ── State change handlers ─────────────────────────────────────────────────────
function onGeoLevelChange() {
  S.measure = null;
  S.geos    = [];
  buildMeasureGrid();
  buildYearSelect();
  buildGeoList();
  resetFromStep(2);
  updatePills();
  checkReadiness();
}

function onMeasureOrGeoLevelChange() {
  resetFromStep(2);
  buildGeoList();
  unlockStep(2);
  updatePills();
  checkReadiness();
}

function onGeoSelectionChange() {
  if (S.geos.length > 0) {
    unlockStep(3);
    buildFilterSelect().then(() => unlockStep(4));
  } else {
    lockStep(3);
    lockStep(4);
  }
  updatePills();
  checkReadiness();
}

function onYearChange() {
  updatePills();
  checkReadiness();
}

// ── Step lock/unlock ──────────────────────────────────────────────────────────
function unlockStep(n) {
  const el = $(`step${n}`);
  el.classList.remove('locked');
  // Trigger re-animation each time step is revealed
  el.classList.remove('unlocked');
  void el.offsetWidth; // reflow
  el.classList.add('unlocked');
}

function lockStep(n) {
  const el = $(`step${n}`);
  el.classList.add('locked');
  el.classList.remove('unlocked');
}

function resetFromStep(n) {
  for (let i = n; i <= 4; i++) lockStep(i);
  if (n <= 3) { S.geos = []; buildGeoList(); }
  if (n <= 4) { S.filterI = null; $('selectFilter').innerHTML = ''; }
}

// ── Pills ─────────────────────────────────────────────────────────────────────
function updatePills() {
  const measures = S.geoLevel === 'state'
    ? S.schema?.state_measures
    : S.schema?.county_measures;

  const measureLabel = measures
    ? (measures.find(m => m.value === S.measure)?.label ?? '—')
    : '—';

  const disabilityLabel = S.schema?.disability_types
    .find(d => d.value === S.disability)?.label ?? '—';

  const geoLabel = S.geos.length
    ? (S.geos.length === 1 ? S.geos[0] : `${S.geos.length} geographies`)
    : '—';

  const yearLabel = S.yearMode === 'all'
    ? 'All years'
    : (S.year ? String(S.year) : '—');

  const pills = [
    { label: S.geoLevel === 'state' ? 'US/State' : 'County', active: true },
    { label: disabilityLabel,  active: !!S.disability },
    { label: measureLabel,     active: !!S.measure },
    { label: geoLabel,         active: S.geos.length > 0 },
    { label: yearLabel,        active: !!S.year || S.yearMode === 'all' },
  ];

  $('selectionPills').innerHTML = pills
    .map(p => `<span class="pill${p.active ? ' active' : ''}">${p.label}</span>`)
    .join('');
}

function checkReadiness() {
  const ready = S.disability && S.measure && S.geos.length > 0 &&
    (S.yearMode === 'all' || S.year) && S.filterI != null;
  $('btnShowData').disabled = !ready;
}

// ── Fetch data ────────────────────────────────────────────────────────────────
async function fetchData() {
  const years = S.yearMode === 'all'
    ? (S.geoLevel === 'state' ? S.schema.state_years : S.schema.county_years)
    : [S.year];

  showLoading(`Loading 0 of ${years.length} file${years.length > 1 ? 's' : ''}…`);

  try {
    const res = await fetch('/api/data', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        geo_level:   S.geoLevel,
        geographies: S.geos,
        measure:     S.measure,
        disability:  S.disability,
        years:       years,
        i:           S.filterI,
      }),
    });

    if (!res.ok) {
      const err = await res.json();
      showError(err.error || 'Failed to load data.');
      return;
    }

    // Safely parse — R can emit bare NaN tokens which are invalid JSON.
    // We scrub them from the raw text before parsing.
    const rawText = await res.text();
    const cleanText = rawText
      .replace(/:NaN/g, ':null')
      .replace(/:Inf/g, ':null')
      .replace(/:-Inf/g, ':null');
    let data;
    try {
      data = JSON.parse(cleanText);
    } catch (parseErr) {
      showError('Could not parse server response: ' + parseErr.message);
      return;
    }
    showLoading(`Loaded ${data.loaded_files} of ${data.total_files} files…`);

    // Small delay so user sees the final count
    await new Promise(r => setTimeout(r, 400));

    S.tableData = data;
    S.sortCol   = null;

    // Build geo picker for summary mode
    buildSummaryGeoPicker(data);
    showResults(data);

  } catch (e) {
    showError('Network error: ' + e.message);
  }
}

// ── Results rendering ─────────────────────────────────────────────────────────
function buildSummaryGeoPicker(data) {
  const geoCol = data.geo_col;
  if (!geoCol) return;

  const uniqueGeos = [...new Set(data.rows.map(r => r[geoCol]).filter(Boolean))].sort();
  $('selectSummaryGeo').innerHTML = uniqueGeos
    .map(g => `<option value="${g}">${g}</option>`)
    .join('');
  S.summaryGeo = uniqueGeos[0] ?? null;
}

function showResults(data) {
  $('emptyState').classList.add('hidden');
  $('loadingState').classList.add('hidden');
  $('errorState').classList.add('hidden');
  $('results').classList.remove('hidden');

  // Meta line
  const measures = S.geoLevel === 'state' ? S.schema.state_measures : S.schema.county_measures;
  const mLabel   = measures.find(m => m.value === S.measure)?.label ?? S.measure;
  const dLabel   = S.schema.disability_types.find(d => d.value === S.disability)?.label ?? S.disability;

  $('resultsMeta').innerHTML =
    `<strong>${mLabel}</strong> — ${dLabel} &nbsp;·&nbsp; ` +
    `${S.geos.length} geography${S.geos.length > 1 ? 'ies' : ''} &nbsp;·&nbsp; ` +
    `${S.yearMode === 'all' ? 'All years' : S.year} &nbsp;·&nbsp; ` +
    `${data.rows.length} rows loaded`;

  $('summaryBar').classList.toggle('hidden', S.viewMode !== 'summary');
  renderTable();
}

function getDisplayRows() {
  if (!S.tableData) return [];
  let rows = [...S.tableData.rows];

  if (S.viewMode === 'summary' && S.tableData.geo_col) {
    const geoCol = S.tableData.geo_col;
    const numericCols = S.tableData.columns.filter(c => {
      const vals = rows.map(r => r[c]).filter(v => v != null);
      return vals.length > 0 && vals.every(v => !isNaN(Number(v)));
    });

    if (S.summaryMode === 'max' && numericCols.length > 0) {
      const pivot = numericCols[0];
      const maxVal = Math.max(...rows.map(r => Number(r[pivot]) || -Infinity));
      rows = rows.filter(r => Number(r[pivot]) === maxVal);
    } else if (S.summaryMode === 'min' && numericCols.length > 0) {
      const pivot = numericCols[0];
      const minVal = Math.min(...rows.map(r => Number(r[pivot]) || Infinity));
      rows = rows.filter(r => Number(r[pivot]) === minVal);
    } else if (S.summaryMode === 'geo' && S.summaryGeo) {
      rows = rows.filter(r => r[geoCol] === S.summaryGeo);
    }
  }

  // Sort
  if (S.sortCol != null) {
    const col = S.tableData.columns[S.sortCol];
    rows.sort((a, b) => {
      const av = a[col], bv = b[col];
      const an = Number(av), bn = Number(bv);
      const useNum = !isNaN(an) && !isNaN(bn);
      const cmp = useNum ? an - bn : String(av ?? '').localeCompare(String(bv ?? ''));
      return S.sortDir === 'asc' ? cmp : -cmp;
    });
  }

  return rows;
}

function isNumericCol(colName) {
  if (!S.tableData) return false;
  const vals = S.tableData.rows.map(r => r[colName]).filter(v => v != null);
  return vals.length > 0 && vals.every(v => !isNaN(Number(v)));
}

function renderTable() {
  if (!S.tableData) return;

  const cols = S.tableData.columns;
  const rows = getDisplayRows();

  // Header
  $('tableHead').innerHTML = `<tr>${cols.map((c, i) => {
    let cls = '';
    if (S.sortCol === i) cls = ` class="sort-${S.sortDir}"`;
    return `<th${cls} data-col="${i}">${formatColName(c)}</th>`;
  }).join('')}</tr>`;

  // Sort click
  $('tableHead').querySelectorAll('th').forEach(th => {
    th.addEventListener('click', () => {
      const idx = parseInt(th.dataset.col);
      if (S.sortCol === idx) {
        S.sortDir = S.sortDir === 'asc' ? 'desc' : 'asc';
      } else {
        S.sortCol = idx;
        S.sortDir = 'asc';
      }
      renderTable();
    });
  });

  // Body
  $('tableBody').innerHTML = rows.map(row => {
    return `<tr>${cols.map(c => {
      const v = row[c];
      const num = isNumericCol(c);
      const display = v == null ? '—' : (num ? fmtNum(v, c) : v);
      return `<td class="${num ? 'numeric' : ''}">${display}</td>`;
    }).join('')}</tr>`;
  }).join('');

  if (rows.length === 0) {
    $('tableBody').innerHTML = `<tr><td colspan="${cols.length}" style="text-align:center;color:var(--text3);padding:24px;">No rows match the current filter.</td></tr>`;
  }
}

function formatColName(col) {
  return col.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
}

function fmtNum(v, col) {
  const n = Number(v);
  if (isNaN(n)) return v;
  const colLower = col.toLowerCase();
  // Percentages
  if (colLower.includes('pct') || colLower.includes('percent') ||
      colLower.includes('rate') || colLower.includes('ratio') ||
      colLower.includes('prev')) {
    return n.toLocaleString(undefined, { maximumFractionDigits: 1 }) + '%';
  }
  // Large numbers
  if (Math.abs(n) >= 1000) {
    return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

// ── UI state helpers ──────────────────────────────────────────────────────────
function showLoading(msg) {
  $('emptyState').classList.add('hidden');
  $('results').classList.add('hidden');
  $('errorState').classList.add('hidden');
  $('loadingState').classList.remove('hidden');
  $('loadingMsg').textContent = msg;
}

function showError(msg) {
  $('emptyState').classList.add('hidden');
  $('results').classList.add('hidden');
  $('loadingState').classList.add('hidden');
  $('errorState').classList.remove('hidden');
  $('errorMsg').textContent = msg;
}

// ── Downloads ─────────────────────────────────────────────────────────────────
function downloadTable(format) {
  if (!S.tableData) return;

  const cols = S.tableData.columns;
  const rows = getDisplayRows();

  if (format === 'csv') {
    const lines = [
      cols.map(c => `"${c}"`).join(','),
      ...rows.map(row => cols.map(c => {
        const v = row[c];
        return v == null ? '' : `"${String(v).replace(/"/g, '""')}"`;
      }).join(','))
    ];
    triggerDownload(lines.join('\n'), 'data.csv', 'text/csv');

  } else if (format === 'xlsx') {
    // Build a simple XLSX using only browser APIs (XML-based)
    const xlsxContent = buildXLSX(cols, rows);
    triggerDownload(xlsxContent, 'data.xlsx',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', true);
  }
}

function triggerDownload(content, filename, mimeType, isBinary = false) {
  let blob;
  if (isBinary) {
    // content is a Uint8Array
    blob = new Blob([content], { type: mimeType });
  } else {
    blob = new Blob([content], { type: mimeType });
  }
  const url = URL.createObjectURL(blob);
  const a   = document.createElement('a');
  a.href     = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function buildXLSX(cols, rows) {
  // Minimal XLSX via SheetJS-compatible approach using a data URI trick
  // We'll use the XML SpreadsheetML format (xls-compatible xlsx)
  const escape = v => String(v ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

  let xml = `<?xml version="1.0"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
<Worksheet ss:Name="Data"><Table>
<Row>${cols.map(c => `<Cell><Data ss:Type="String">${escape(c)}</Data></Cell>`).join('')}</Row>
${rows.map(row =>
  `<Row>${cols.map(c => {
    const v = row[c];
    const n = Number(v);
    const type = (v != null && !isNaN(n)) ? 'Number' : 'String';
    return `<Cell><Data ss:Type="${type}">${escape(v ?? '')}</Data></Cell>`;
  }).join('')}</Row>`
).join('\n')}
</Table></Worksheet></Workbook>`;

  return xml;
}

// ── Boot ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', init);
