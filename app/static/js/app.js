/* ============================================================
   ACS Disability Statistics Explorer — app.js
   ============================================================ */

'use strict';

// ── State ────────────────────────────────────────────────────────────────────
const S = {
  schema:      null,
  geoLevel:    'state',
  disability:  null,
  measure:     null,
  geos:        [],
  yearMode:    'single',
  year:        null,
  // State-level filters (3 separate)
  gender:      'All',
  race:        'All',
  age:         'All',
  // County-level legacy filter
  filterI:     null,
  // Derived
  iIndex:      null,   // computed from gender/race/age for state; = filterI for county
  filterValid: true,   // false when Any/Any/Any
  tableData:   null,
  sortCol:     null,
  sortDir:     'asc',
  viewMode:    'full',
  summaryMode: 'max',
  summaryGeo:  null,
};

// i-index lookup matching server: (genderAny, raceAny, ageAny) -> i
// Row 8 (true/true/true) is excluded
const I_LOOKUP = {
  'false|false|false': 1,
  'false|false|true':  2,
  'false|true|false':  3,
  'false|true|true':   4,
  'true|false|false':  5,
  'true|false|true':   6,
  'true|true|false':   7,
  // true|true|true -> undefined (invalid)
};

function computeI(gender, race, age) {
  const key = `${gender !== 'All'}|${race !== 'All'}|${age !== 'All'}`;
  return I_LOOKUP[key] ?? null;  // null = invalid (Any/Any/Any)
}

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

// ── Step 1 ────────────────────────────────────────────────────────────────────
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
      const sel = S.measure === m.value ? ' selected' : '';
      html += `<button class="measure-btn${sel}" data-value="${m.value}">${m.label}</button>`;
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

// ── Step 2 ────────────────────────────────────────────────────────────────────
function buildGeoList(filter = '') {
  const list = S.geoLevel === 'state' ? S.schema.us_states : [];
  const lower = filter.toLowerCase();
  const filtered = list.filter(g => g.toLowerCase().includes(lower));

  $('geoList').innerHTML = filtered.map(g => {
    const checked = S.geos.includes(g);
    return `<label class="geo-item${checked ? ' checked' : ''}">
      <input type="checkbox" value="${g}" ${checked ? 'checked' : ''} />${g}
    </label>`;
  }).join('');

  $('geoList').querySelectorAll('input[type=checkbox]').forEach(cb => {
    cb.addEventListener('change', () => {
      if (cb.checked) { if (!S.geos.includes(cb.value)) S.geos.push(cb.value); }
      else             { S.geos = S.geos.filter(g => g !== cb.value); }
      cb.closest('.geo-item').classList.toggle('checked', cb.checked);
      onGeoSelectionChange();
    });
  });
}

// ── Step 3 ────────────────────────────────────────────────────────────────────
function buildYearSelect() {
  const years = S.geoLevel === 'state' ? S.schema.state_years : S.schema.county_years;
  const sel = $('selectYear');
  sel.innerHTML = [...years].reverse().map(y => `<option value="${y}">${y}</option>`).join('');
  S.year = years[years.length - 1];
  sel.value = S.year;
}

// ── Step 4 ────────────────────────────────────────────────────────────────────
function buildStateFilters() {
  // Gender
  $('selectGender').innerHTML = S.schema.gender_options
    .map(o => `<option value="${o.value}">${o.label}</option>`).join('');
  S.gender = 'All';

  // Race
  $('selectRace').innerHTML = S.schema.race_options
    .map(o => `<option value="${o.value}">${o.label}</option>`).join('');
  S.race = 'All';

  // Age — options depend on measure
  buildAgeSelect();

  updateIIndex();
}

function buildAgeSelect() {
  const ageGroupKey = S.schema.measure_age_group[S.measure] ?? 'population';
  const options     = S.schema.age_groups[ageGroupKey] ?? S.schema.age_groups['population'];

  $('selectAge').innerHTML = options
    .map(o => `<option value="${o.value}">${o.label}</option>`).join('');

  // Reset age if current value not in new options
  const vals = options.map(o => o.value);
  if (!vals.includes(S.age)) S.age = 'All';
  $('selectAge').value = S.age;
}

function updateIIndex() {
  S.iIndex = computeI(S.gender, S.race, S.age);
  S.filterValid = S.iIndex !== null;
  $('filterWarning').classList.toggle('hidden', S.filterValid);
  updatePills();
  checkReadiness();
}

async function buildCountyFilterSelect() {
  const res  = await fetch(`/api/county_filters?measure=${S.measure}`);
  const data = await res.json();
  $('selectFilter').innerHTML = data.filters
    .map(f => `<option value="${f.i}">${f.label}</option>`).join('');
  S.filterI = data.filters[0].i;
  $('selectFilter').value = S.filterI;
}

function showStateFilters() {
  $('stateFilterWrap').classList.remove('hidden');
  $('countyFilterWrap').classList.add('hidden');
}

function showCountyFilters() {
  $('countyFilterWrap').classList.remove('hidden');
  $('stateFilterWrap').classList.add('hidden');
}

// ── Wiring ────────────────────────────────────────────────────────────────────
function wireControls() {
  // Geo level
  document.querySelectorAll('#geoLevelControl .seg-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#geoLevelControl .seg-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      S.geoLevel = btn.dataset.value;
      S.measure  = null;
      S.geos     = [];
      onGeoLevelChange();
    });
  });

  // Disability
  $('selectDisability').addEventListener('change', e => {
    S.disability = e.target.value;
    updatePills();
    checkReadiness();
  });

  // Year mode
  document.querySelectorAll('#yearModeControl .seg-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#yearModeControl .seg-btn').forEach(b => b.classList.remove('active'));
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

  // State filters
  $('selectGender').addEventListener('change', e => {
    S.gender = e.target.value;
    updateIIndex();
  });
  $('selectRace').addEventListener('change', e => {
    S.race = e.target.value;
    updateIIndex();
  });
  $('selectAge').addEventListener('change', e => {
    S.age = e.target.value;
    updateIIndex();
  });

  // County legacy filter
  $('selectFilter').addEventListener('change', e => {
    S.filterI = parseInt(e.target.value);
    updatePills();
    checkReadiness();
  });

  // Geo search
  $('geoSearch').addEventListener('input', e => buildGeoList(e.target.value));
  $('btnSelectAll').addEventListener('click', () => {
    S.geos = S.geoLevel === 'state' ? [...S.schema.us_states] : [];
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
      document.querySelectorAll('#viewToggle .vt-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      S.viewMode = btn.dataset.view;
      $('summaryBar').classList.toggle('hidden', S.viewMode === 'full');
      renderTable();
    });
  });

  // Summary mode
  document.querySelectorAll('#summaryModeControl .seg-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('#summaryModeControl .seg-btn').forEach(b => b.classList.remove('active'));
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
  // If state level, rebuild age options when measure changes
  if (S.geoLevel === 'state') buildAgeSelect();
  updatePills();
  checkReadiness();
}

function onGeoSelectionChange() {
  if (S.geos.length > 0) {
    unlockStep(3);
    unlockStep(4);
    if (S.geoLevel === 'state') {
      showStateFilters();
      buildStateFilters();
    } else {
      showCountyFilters();
      buildCountyFilterSelect();
    }
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
  el.classList.remove('unlocked');
  void el.offsetWidth;
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
  if (n <= 4) {
    S.filterI    = null;
    S.gender     = 'All';
    S.race       = 'All';
    S.age        = 'All';
    S.iIndex     = null;
    S.filterValid = true;
    $('filterWarning').classList.add('hidden');
    $('selectFilter').innerHTML = '';
  }
}

// ── Pills ─────────────────────────────────────────────────────────────────────
function updatePills() {
  const measures   = S.geoLevel === 'state' ? S.schema?.state_measures : S.schema?.county_measures;
  const mLabel     = measures?.find(m => m.value === S.measure)?.label ?? '—';
  const dLabel     = S.schema?.disability_types.find(d => d.value === S.disability)?.label ?? '—';
  const geoLabel   = S.geos.length ? (S.geos.length === 1 ? S.geos[0] : `${S.geos.length} geographies`) : '—';
  const yearLabel  = S.yearMode === 'all' ? 'All years' : (S.year ? String(S.year) : '—');

  let filterLabel = '—';
  if (S.geoLevel === 'state' && S.iIndex !== null) {
    const parts = [];
    if (S.gender !== 'All') parts.push(S.gender);
    if (S.race   !== 'All') parts.push(S.schema.race_options.find(r => r.value === S.race)?.label ?? S.race);
    if (S.age    !== 'All') parts.push(S.schema.age_groups[S.schema.measure_age_group[S.measure]]?.find(a => a.value === S.age)?.label ?? S.age);
    filterLabel = parts.length ? parts.join(' · ') : 'All';
  } else if (S.geoLevel === 'county' && S.filterI) {
    filterLabel = `i=${S.filterI}`;
  }

  const pills = [
    { label: S.geoLevel === 'state' ? 'US/State' : 'County', active: true },
    { label: dLabel,       active: !!S.disability },
    { label: mLabel,       active: !!S.measure },
    { label: geoLabel,     active: S.geos.length > 0 },
    { label: yearLabel,    active: !!S.year || S.yearMode === 'all' },
    { label: filterLabel,  active: S.geoLevel === 'county' ? !!S.filterI : S.iIndex !== null },
  ];

  $('selectionPills').innerHTML = pills
    .map(p => `<span class="pill${p.active ? ' active' : ''}">${p.label}</span>`)
    .join('');
}

function checkReadiness() {
  let ready = S.disability && S.measure && S.geos.length > 0 &&
              (S.yearMode === 'all' || S.year);

  if (S.geoLevel === 'state') {
    ready = ready && S.iIndex !== null && S.filterValid;
  } else {
    ready = ready && S.filterI != null;
  }

  $('btnShowData').disabled = !ready;
}

// ── Fetch data ────────────────────────────────────────────────────────────────
async function fetchData() {
  const years = S.yearMode === 'all'
    ? (S.geoLevel === 'state' ? S.schema.state_years : S.schema.county_years)
    : [S.year];

  showLoading(`Loading 0 of ${years.length} file${years.length > 1 ? 's' : ''}…`);

  try {
    const body = {
      geo_level:   S.geoLevel,
      geographies: S.geos,
      measure:     S.measure,
      disability:  S.disability,
      years:       years,
    };

    if (S.geoLevel === 'state') {
      body.gender = S.gender;
      body.race   = S.race;
      body.age    = S.age;
    } else {
      body.i = S.filterI;
    }

    const res = await fetch('/api/data', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.json();
      showError(err.error || 'Failed to load data.');
      return;
    }

    const rawText   = await res.text();
    const cleanText = rawText.replace(/:NaN/g, ':null').replace(/:Inf/g, ':null').replace(/:-Inf/g, ':null');
    let data;
    try {
      data = JSON.parse(cleanText);
    } catch (parseErr) {
      showError('Could not parse server response: ' + parseErr.message);
      return;
    }

    showLoading(`Loaded ${data.loaded_files} of ${data.total_files} files…`);
    await new Promise(r => setTimeout(r, 400));

    S.tableData = data;
    S.sortCol   = null;
    buildSummaryGeoPicker(data);
    showResults(data);

  } catch (e) {
    showError('Network error: ' + e.message);
  }
}

// ── Results ───────────────────────────────────────────────────────────────────
function buildSummaryGeoPicker(data) {
  const geoCol = data.geo_col;
  if (!geoCol) return;
  const uniqueGeos = [...new Set(data.rows.map(r => r[geoCol]).filter(Boolean))].sort();
  $('selectSummaryGeo').innerHTML = uniqueGeos.map(g => `<option value="${g}">${g}</option>`).join('');
  S.summaryGeo = uniqueGeos[0] ?? null;
}

function showResults(data) {
  $('emptyState').classList.add('hidden');
  $('loadingState').classList.add('hidden');
  $('errorState').classList.add('hidden');
  $('results').classList.remove('hidden');

  const measures = S.geoLevel === 'state' ? S.schema.state_measures : S.schema.county_measures;
  const mLabel   = measures.find(m => m.value === S.measure)?.label ?? S.measure;
  const dLabel   = S.schema.disability_types.find(d => d.value === S.disability)?.label ?? S.disability;

  $('resultsMeta').innerHTML =
    `<strong>${mLabel}</strong> — ${dLabel} &nbsp;·&nbsp; ` +
    `${S.geos.length} geography${S.geos.length !== 1 ? 'ies' : ''} &nbsp;·&nbsp; ` +
    `${S.yearMode === 'all' ? 'All years' : S.year} &nbsp;·&nbsp; ` +
    `${data.rows.length} rows loaded`;

  $('summaryBar').classList.toggle('hidden', S.viewMode !== 'summary');
  renderTable();
}

function getDisplayRows() {
  if (!S.tableData) return [];
  let rows = [...S.tableData.rows];

  if (S.viewMode === 'summary' && S.tableData.geo_col) {
    const geoCol     = S.tableData.geo_col;
    const numericCols = S.tableData.columns.filter(c => {
      const vals = rows.map(r => r[c]).filter(v => v != null);
      return vals.length > 0 && vals.every(v => !isNaN(Number(v)));
    });

    if (S.summaryMode === 'max' && numericCols.length > 0) {
      const pivot  = numericCols[0];
      const maxVal = Math.max(...rows.map(r => Number(r[pivot]) || -Infinity));
      rows = rows.filter(r => Number(r[pivot]) === maxVal);
    } else if (S.summaryMode === 'min' && numericCols.length > 0) {
      const pivot  = numericCols[0];
      const minVal = Math.min(...rows.map(r => Number(r[pivot]) || Infinity));
      rows = rows.filter(r => Number(r[pivot]) === minVal);
    } else if (S.summaryMode === 'geo' && S.summaryGeo) {
      rows = rows.filter(r => r[geoCol] === S.summaryGeo);
    }
  }

  if (S.sortCol != null) {
    const col = S.tableData.columns[S.sortCol];
    rows.sort((a, b) => {
      const av = a[col], bv = b[col];
      const an = Number(av), bn = Number(bv);
      const useNum = !isNaN(an) && !isNaN(bn);
      const cmp    = useNum ? an - bn : String(av ?? '').localeCompare(String(bv ?? ''));
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

  $('tableHead').innerHTML = `<tr>${cols.map((c, i) => {
    const cls = S.sortCol === i ? ` class="sort-${S.sortDir}"` : '';
    return `<th${cls} data-col="${i}">${formatColName(c)}</th>`;
  }).join('')}</tr>`;

  $('tableHead').querySelectorAll('th').forEach(th => {
    th.addEventListener('click', () => {
      const idx = parseInt(th.dataset.col);
      S.sortDir = S.sortCol === idx ? (S.sortDir === 'asc' ? 'desc' : 'asc') : 'asc';
      S.sortCol = idx;
      renderTable();
    });
  });

  $('tableBody').innerHTML = rows.length
    ? rows.map(row => `<tr>${cols.map(c => {
        const v   = row[c];
        const num = isNumericCol(c);
        return `<td class="${num ? 'numeric' : ''}">${v == null ? '—' : (num ? fmtNum(v, c) : v)}</td>`;
      }).join('')}</tr>`).join('')
    : `<tr><td colspan="${cols.length}" style="text-align:center;color:var(--text3);padding:24px;">No rows match the current filter.</td></tr>`;
}

function formatColName(col) {
  return col.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
}

function fmtNum(v, col) {
  const n = Number(v);
  if (isNaN(n)) return v;
  const c = col.toLowerCase();
  if (c.includes('pct') || c.includes('percent') || c.includes('rate') || c.includes('ratio') || c.includes('prev'))
    return n.toLocaleString(undefined, { maximumFractionDigits: 1 }) + '%';
  if (Math.abs(n) >= 1000)
    return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

// ── UI helpers ────────────────────────────────────────────────────────────────
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
    triggerDownload(buildXLSX(cols, rows), 'data.xlsx',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  }
}

function triggerDownload(content, filename, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

function buildXLSX(cols, rows) {
  const esc = v => String(v ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  return `<?xml version="1.0"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet" xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
<Worksheet ss:Name="Data"><Table>
<Row>${cols.map(c => `<Cell><Data ss:Type="String">${esc(c)}</Data></Cell>`).join('')}</Row>
${rows.map(row => `<Row>${cols.map(c => {
  const v = row[c], n = Number(v);
  return `<Cell><Data ss:Type="${v != null && !isNaN(n) ? 'Number' : 'String'}">${esc(v ?? '')}</Data></Cell>`;
}).join('')}</Row>`).join('\n')}
</Table></Worksheet></Workbook>`;
}

// ── Boot ──────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', init);
