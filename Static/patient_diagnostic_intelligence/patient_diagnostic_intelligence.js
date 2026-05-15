(() => {
  const state = {
    page: 1,
    pageSize: 25,
    totalRows: 0,
    hasMore: false,
    tests: [],
    selectedTests: new Map(),
    parameterSearch: '',
    visitSearch: '',
    selectedTab: 'history',
    suggestionToken: 0,
    selectedPatientId: '',
    selectedPatient: null,
    lastPatientSearch: '',
  };

  const $ = (id) => document.getElementById(id);
  const shell = document.querySelector('.pdi-shell');
  let toastSeq = 0;

  function toast(message, type = 'ok', duration = 3600) {
    const node = document.createElement('div');
    node.dataset.toastId = `pdi-toast-${++toastSeq}`;
    node.className = `toast ${type}`;
    node.innerHTML = toastMarkup(message, type);
    $('toastHost').appendChild(node);
    requestAnimationFrame(() => node.classList.add('show'));
    if (duration) setTimeout(() => removeToast(node), duration);
    return node;
  }

  function toastMarkup(message, type) {
    const spinner = type === 'loading' ? '<span class="toast-spinner"></span>' : '';
    return `${spinner}<span>${esc(message)}</span>`;
  }

  function removeToast(node) {
    if (!node) return;
    node.classList.remove('show');
    node.classList.add('hiding');
    setTimeout(() => node.remove(), 220);
  }

  function updateToast(node, message, type = 'ok', duration = 3200) {
    if (!node) return toast(message, type, duration);
    node.className = `toast ${type} show`;
    node.innerHTML = toastMarkup(message, type);
    if (duration) setTimeout(() => removeToast(node), duration);
    return node;
  }

  function processingToast(message) {
    return toast(message, 'loading', 0);
  }

  function markToastError(err, node) {
    updateToast(node, err.message || 'Request failed', 'error', 4200);
    err.pdiToastShown = true;
    throw err;
  }

  function showError(err) {
    if (!err?.pdiToastShown) toast(err.message || 'Request failed', 'error');
  }

  function selectedValues(el) {
    return Array.from(el.selectedOptions || []).map(o => o.value).filter(Boolean);
  }

  function selectedOptionLabels(el) {
    return Array.from(el.selectedOptions || []).map(o => o.textContent.trim()).filter(Boolean);
  }

  function selectedTestIds() {
    return Array.from(state.selectedTests.keys());
  }

  function clearSelectedPatient() {
    state.selectedPatientId = '';
    state.selectedPatient = null;
  }

  function patientSearchValue() {
    return ($('patientSearch')?.value || '').trim();
  }

  function rememberTest(value, label) {
    if (!value) return;
    state.selectedTests.set(String(value), String(label || value).trim());
  }

  function todayIso() {
    return new Date().toISOString().slice(0, 10);
  }

  function setDefaultDates() {
    const today = new Date();
    const from = new Date(today);
    from.setDate(today.getDate() - 29);
    $('fromDate').value = from.toISOString().slice(0, 10);
    $('toDate').value = todayIso();
  }

  function payload() {
    return {
      unit: $('unitSelect').value,
      time_preset: 'custom',
      from_date: $('fromDate').value,
      to_date: $('toDate').value,
      test_ids: selectedTestIds().map(Number),
      parameter_ids: selectedValues($('parameterSelect')).map(Number),
      result_status: $('resultStatus').value,
      auth_status: $('authStatus').value,
      visit_types: selectedValues($('visitType')),
      patient_id: state.selectedPatientId ? Number(state.selectedPatientId) : null,
      patient_search: patientSearchValue(),
      followup_gap_days: Number($('followupGap').value || 90),
      report_type: $('reportType').value,
      match_mode: $('matchMode').value,
    };
  }

  function hasSelectedTests() {
    return selectedTestIds().length > 0;
  }

  function testRequiredMessage() {
    return 'Select at least one pathology test before loading diagnostic data.';
  }

  function markTestSelectorAttention() {
    const trigger = $('testComboTrigger');
    if (!trigger) return;
    trigger.classList.add('field-attention');
    setTimeout(() => trigger.classList.remove('field-attention'), 2600);
  }

  function showTestRequired() {
    markTestSelectorAttention();
    toast(testRequiredMessage(), 'warn');
    setHistoryPrompt(testRequiredMessage());
    setComparisonVisible(false);
  }

  function setHistoryPrompt(message) {
    updateKpis({});
    $('historyTable').innerHTML = `<tbody><tr><td>${esc(message)}</td></tr></tbody>`;
    $('historyMeta').textContent = '0 records on this page';
    $('pageInfo').textContent = 'Page 1';
    state.totalRows = 0;
    state.hasMore = false;
    renderIntelligence([]);
  }

  async function api(url, options = {}) {
    const res = await fetch(url, {
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      ...options,
    });
    const text = await res.text();
    let data = {};
    try { data = text ? JSON.parse(text) : {}; } catch { data = { status: 'error', message: text || 'Request failed' }; }
    if (!res.ok || data.status === 'error') throw new Error(data.message || 'Request failed');
    return data;
  }

  async function download(url, body, message) {
    if (state.selectedTab === 'comparison') {
      toast('Parameter comparison is available for grid view only and is not included in exports.', 'warn');
      return;
    }
    if (!hasSelectedTests()) {
      showTestRequired();
      return;
    }
    const notice = processingToast(message || 'Preparing export. Please wait...');
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        let msg = await res.text();
        try { msg = JSON.parse(msg).message || msg; } catch {}
        throw new Error(msg || 'Export failed');
      }
      const blob = await res.blob();
      const disposition = res.headers.get('content-disposition') || '';
      const match = disposition.match(/filename\*=UTF-8''([^;]+)|filename="?([^"]+)"?/i);
      const filename = decodeURIComponent((match && (match[1] || match[2])) || 'Patient_Diagnostic_Intelligence_Report');
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(a.href), 1500);
      updateToast(notice, 'Export generated. Download started.', 'ok');
    } catch (err) {
      markToastError(err, notice);
    }
  }

  async function loadOptions() {
    const data = await api('/api/patient-diagnostic/reports/options');
    $('reportType').innerHTML = (data.report_types || []).map(r => `<option value="${esc(r.key)}">${esc(r.label)}</option>`).join('');
  }

  async function loadTests() {
    const q = $('testSearch').value.trim();
    const unit = $('unitSelect').value;
    const data = await api(`/api/patient-diagnostic/test-master?unit=${encodeURIComponent(unit)}&q=${encodeURIComponent(q)}&limit=500`);
    state.tests = data.data || [];
    renderTests(state.tests);
  }

  function renderTests(tests) {
    $('testSelect').innerHTML = tests.map(t => {
      const id = String(t.test_id);
      const label = `${t.service_name || t.test_name || id}${t.test_name && t.service_name !== t.test_name ? ' - ' + t.test_name : ''}`;
      return `<option value="${esc(id)}" ${state.selectedTests.has(id) ? 'selected' : ''}>${esc(label)}</option>`;
    }).join('');
    renderCombo('test');
  }

  async function loadParameters() {
    const unit = $('unitSelect').value;
    const ids = selectedTestIds();
    const query = ids.map(id => `test_ids=${encodeURIComponent(id)}`).join('&');
    const data = await api(`/api/patient-diagnostic/parameter-master?unit=${encodeURIComponent(unit)}&${query}`);
    $('parameterSelect').innerHTML = (data.data || []).map(p => `<option value="${esc(p.parameter_id)}">${esc(p.test_name || '')} - ${esc(p.parameter_name || p.parameter_id)}</option>`).join('');
    renderCombo('parameter');
  }

  async function loadPatientTestSuggestions() {
    const host = $('patientTestSuggestions');
    const search = patientSearchValue();
    if (!host || search.length < 2) {
      renderPatientTestSuggestions([]);
      renderPatientSummary([]);
      clearSelectedPatient();
      state.lastPatientSearch = search;
      return;
    }
    state.lastPatientSearch = search;
    const token = ++state.suggestionToken;
    host.classList.add('show');
    host.innerHTML = '<div class="suggestion-note">Finding tests for this patient...</div>';
    renderPatientSummary([], 0, 'loading');
    try {
      const data = await api('/api/patient-diagnostic/patient-tests', { method: 'POST', body: JSON.stringify(payload()) });
      if (token !== state.suggestionToken) return;
      const patients = data.patients || [];
      if (state.selectedPatientId && patients.length === 1) state.selectedPatient = patients[0];
      renderPatientSummary(patients, data.patient_count || 0, data.message || '');
      renderPatientTestSuggestions(data.data || [], data.message || '', data.patient_count || 0);
    } catch (err) {
      if (token !== state.suggestionToken) return;
      renderPatientSummary([], 0, 'error');
      host.classList.add('show');
      host.innerHTML = `<div class="suggestion-note">${esc(err.message || 'Unable to load patient tests.')}</div>`;
    }
  }

  function renderPatientSummary(patients = [], patientCount = 0, stateText = '') {
    const host = $('patientSummary');
    if (!host) return;
    host.classList.remove('patient-summary-selecting');
    if (stateText === 'loading') {
      host.innerHTML = `
        <div class="panel-kicker">Comparison helper</div>
        <div class="panel-title">Looking up patient</div>
        <div class="patient-summary-empty">Searching matching patient details and conducted tests.</div>
      `;
      return;
    }
    if (!patients.length) {
      const title = stateText === 'error' ? 'Patient lookup failed' : 'Find a patient';
      const copy = stateText === 'error'
        ? 'Could not load patient details right now.'
        : 'Search by registration no, patient ID, name, mobile or visit no to view conducted tests here.';
      host.innerHTML = `
        <div class="panel-kicker">Comparison helper</div>
        <div class="panel-title">${esc(title)}</div>
        <div class="patient-summary-empty">${esc(copy)}</div>
      `;
      return;
    }
    if (patientCount > 1) {
      host.classList.add('patient-summary-selecting');
      const items = patients.slice(0, 8).map(patient => {
        const label = [patient.registration_no, patient.patient_age ? `${patient.patient_age} yrs` : '', patient.mobile].filter(Boolean).join(' | ');
        return `
          <button type="button" class="patient-choice" data-patient-choice-id="${esc(patient.patient_id || '')}">
            <span>${esc(patient.patient_name || 'Patient')}</span>
            <small>${esc(label || 'Patient record')}</small>
          </button>
        `;
      }).join('');
      host.innerHTML = `
        <div class="panel-kicker">Comparison helper</div>
        <div class="panel-title">Select patient</div>
        <div class="patient-summary-empty">Multiple matches found. Choose one patient to continue.</div>
        <div class="patient-choice-list">${items}</div>
      `;
      return;
    }
    const patient = patients[0] || {};
    const registration = String(patient.registration_no || '');
    const patientId = String(patient.patient_id || '');
    const showPatientId = shouldShowPatientId(patientId, registration);
    host.innerHTML = `
      <div class="panel-kicker">Patient matched</div>
      <div class="panel-title">${esc(patient.patient_name || 'Patient')}</div>
      <div class="patient-detail-grid">
        <span>Registration</span><strong>${esc(patient.registration_no || '-')}</strong>
        ${showPatientId ? `<span>Patient ID</span><strong>${esc(patientId)}</strong>` : ''}
        <span>Age</span><strong>${esc(patient.patient_age || '-')}</strong>
        <span>Mobile</span><strong>${esc(patient.mobile || '-')}</strong>
      </div>
      <button type="button" class="change-patient-btn" id="changePatientBtn">Change patient</button>
    `;
  }

  function shouldShowPatientId(patientId, registration) {
    const pid = String(patientId || '').trim();
    const reg = String(registration || '').trim();
    if (!pid || !reg) return !!pid;
    if (pid.toLowerCase() === reg.toLowerCase()) return false;
    const regSuffix = (reg.split(/[-/]/).pop() || reg).trim();
    if (pid.toLowerCase() === regSuffix.toLowerCase()) return false;
    const pidDigits = pid.replace(/\D/g, '').replace(/^0+/, '');
    const suffixDigits = regSuffix.replace(/\D/g, '').replace(/^0+/, '');
    return !(pidDigits && suffixDigits && pidDigits === suffixDigits);
  }

  function renderPatientTestSuggestions(rows, message = '', patientCount = 0) {
    const host = $('patientTestSuggestions');
    if (!host) return;
    host.classList.remove('has-history');
    if (patientCount > 1 && !state.selectedPatientId) {
      host.classList.remove('show');
      host.innerHTML = '';
      return;
    }
    if (!rows.length) {
      if (message) {
        host.classList.add('show');
        host.innerHTML = `<div class="suggestion-note">${esc(message)}</div>`;
      } else {
        host.classList.remove('show');
        host.innerHTML = '';
      }
      return;
    }
    host.classList.add('show', 'has-history');
    const note = '<div class="suggestion-note suggestion-note-head"><span>Patient test history</span><strong>Select a test</strong></div>';
    host.innerHTML = note + rows.slice(0, 12).map(row => {
      const id = String(row.test_id || '');
      const label = row.test_name || row.service_name || id;
      const iterations = Number(row.iteration_count || 0);
      const selected = state.selectedTests.has(id) ? ' is-selected' : '';
      return `
        <button type="button" class="test-suggestion${selected}" data-suggested-test-id="${esc(id)}" data-suggested-test-label="${esc(label)}">
          <span class="suggestion-title">${esc(label)}</span>
          <span class="suggestion-pill">${iterations}x</span>
          <span class="suggestion-meta">${esc(iterations === 1 ? '1 iteration' : `${iterations} iterations`)}</span>
          <span class="suggestion-date">${esc(row.last_test_date ? `Last ${row.last_test_date}` : 'Last date unavailable')}</span>
        </button>
      `;
    }).join('');
  }

  function selectSingleTest(testId, label) {
    if (!testId) return;
    state.selectedTests.clear();
    Array.from($('testSelect').options || []).forEach(o => { o.selected = false; });
    let opt = Array.from($('testSelect').options || []).find(o => String(o.value) === String(testId));
    if (!opt) {
      opt = new Option(label || testId, testId, true, true);
      $('testSelect').appendChild(opt);
    }
    opt.selected = true;
    rememberTest(testId, label || opt.textContent || testId);
    renderCombo('test');
    loadParameters().catch(err => toast(err.message, 'error'));
    updateSuggestedTestActive();
    toast('Test selected for comparison', 'ok');
  }

  function updateSuggestedTestActive() {
    document.querySelectorAll('[data-suggested-test-id]').forEach(btn => {
      btn.classList.toggle('is-selected', state.selectedTests.has(String(btn.dataset.suggestedTestId || '')));
    });
  }

  function selectPatient(patientId) {
    if (!patientId) return;
    state.selectedPatientId = String(patientId);
    const patientButton = Array.from(document.querySelectorAll('[data-patient-choice-id]')).find(btn => btn.dataset.patientChoiceId === String(patientId));
    const patientName = patientButton?.querySelector('span')?.textContent || 'Selected patient';
    state.selectedPatient = { patient_id: patientId, patient_name: patientName };
    toast('Patient selected. Loading conducted tests...', 'ok');
    loadPatientTestSuggestions();
  }

  function unlockPatientSelection() {
    clearSelectedPatient();
    setComparisonVisible(false);
    loadPatientTestSuggestions();
  }

  async function loadHistory() {
    if (!hasSelectedTests()) {
      showTestRequired();
      return;
    }
    const notice = processingToast('Loading diagnostic history. Please wait...');
    try {
      const data = await api(`/api/patient-diagnostic/patient-history?page=${state.page}&page_size=${state.pageSize}`, { method: 'POST', body: JSON.stringify(payload()) });
      state.totalRows = data.total_rows || 0;
      state.hasMore = !!data.has_more;
      updateKpis(data.summary || {});
      const rows = data.rows || [];
      renderTable('historyTable', rows, historyColumns(), { group: true });
      $('pageInfo').textContent = state.hasMore ? `Page ${state.page} - more records available` : `Page ${state.page}`;
      $('historyMeta').textContent = `${rows.length} records on this page${state.hasMore ? ' - more available' : ''}`;
      updateToast(notice, data.message || (state.totalRows ? 'Diagnostic history loaded' : 'No records found'), state.totalRows ? 'ok' : 'warn');
      renderIntelligence(rows);
      refreshComparison().catch(() => {
        setComparisonVisible(false);
        $('comparisonMeta').textContent = 'Parameter comparison is not available for the current filters.';
      });
    } catch (err) {
      markToastError(err, notice);
    }
  }

  async function loadAbnormal() {
    if (!hasSelectedTests()) {
      showTestRequired();
      return;
    }
    const notice = processingToast('Loading abnormal result worklist. Please wait...');
    try {
      const data = await api('/api/patient-diagnostic/abnormal-results', { method: 'POST', body: JSON.stringify(payload()) });
      updateKpis(data.summary || {});
      renderTable('abnormalTable', data.rows || [], abnormalColumns(), { group: true });
      updateToast(notice, data.rows?.length ? 'Abnormal result worklist loaded' : 'No abnormal records found', data.rows?.length ? 'ok' : 'warn');
    } catch (err) {
      markToastError(err, notice);
    }
  }

  function updateKpis(summary) {
    document.querySelectorAll('[data-kpi]').forEach(el => {
      const key = el.getAttribute('data-kpi');
      el.textContent = Number(summary[key] || 0).toLocaleString('en-IN');
    });
  }

  function renderTable(id, rows, columns, options = {}) {
    const table = $(id);
    if (!rows.length) {
      table.innerHTML = '<tbody><tr><td>No records found for the selected filters.</td></tr></tbody>';
      return;
    }
    const groupCounts = {};
    if (options.group) {
      rows.forEach(row => {
        const key = patientGroupKey(row);
        groupCounts[key] = (groupCounts[key] || 0) + 1;
      });
    }
    let lastGroup = '';
    let groupIndex = -1;
    const body = rows.map(row => {
      const groupKey = patientGroupKey(row);
      let cls = statusRowClass(row);
      let groupHeader = '';
      if (options.group) {
        if (groupKey !== lastGroup) {
          groupIndex += 1;
          cls += ' patient-group-start';
          lastGroup = groupKey;
          groupHeader = `<tr class="patient-group-header ${groupIndex % 2 === 0 ? 'patient-group-even' : 'patient-group-odd'}"><td colspan="${columns.length}">${groupLabel(row, groupCounts[groupKey])}</td></tr>`;
        }
        cls += groupIndex % 2 === 0 ? ' patient-group-even' : ' patient-group-odd';
      }
      return `${groupHeader}<tr class="${cls.trim()}">${columns.map(c => `<td class="${cellClass(row[c.key], c.key, options)}">${formatCell(row[c.key], c.key, options)}</td>`).join('')}</tr>`;
    }).join('');
    table.innerHTML = `
      <thead><tr>${columns.map(c => `<th>${esc(c.label)}</th>`).join('')}</tr></thead>
      <tbody>${body}</tbody>
    `;
  }

  function groupLabel(row, count) {
    const parts = [
      ['Patient', row.patient_name || 'Unknown patient'],
      ['Reg', row.registration_no || ''],
      ['Visit', row.visit_no || ''],
      ['Date', row.visit_date || row.last_visit_date || ''],
      ['Test', row.test_name || row.test || 'Diagnostic test'],
    ].filter(([, value]) => value);
    const suffix = count > 1 ? `${count} parameters` : '1 parameter';
    return `
      <div class="group-label-main">${parts.map(([label, value]) => `<span><b>${esc(label)}:</b> ${esc(value)}</span>`).join('')}</div>
      <div class="group-label-sub">${esc(suffix)}</div>
    `;
  }

  function patientGroupKey(row) {
    return [
      row.patient_id || row.registration_no || row.patient_name || '',
      row.visit_no || row.visit_id || row.last_visit_date || row.visit_date || '',
      row.test_id || row.test_name || '',
    ].join('|');
  }

  function statusRowClass(row) {
    const status = String(row.result_status || row.last_result_status || '').toLowerCase().replace(/\s+/g, '-');
    return status ? `status-row-${status}` : '';
  }

  function renderIntelligence(rows) {
    const grouped = {};
    rows.forEach(r => {
      const key = r.test_name || 'Unknown Test';
      grouped[key] ||= { test_name: key, patient_count: new Set(), abnormal_count: 0, total: 0, last_performed_date: '', common_abnormal_parameters: {} };
      grouped[key].patient_count.add(r.patient_id || r.registration_no || r.patient_name);
      grouped[key].total += 1;
      if (['Abnormal', 'High', 'Low', 'Critical'].includes(r.result_status)) {
        grouped[key].abnormal_count += 1;
        grouped[key].common_abnormal_parameters[r.parameter_name || 'Unknown'] = (grouped[key].common_abnormal_parameters[r.parameter_name || 'Unknown'] || 0) + 1;
      }
      if (String(r.result_date || '') > String(grouped[key].last_performed_date || '')) grouped[key].last_performed_date = r.result_date;
    });
    const out = Object.values(grouped).map(g => ({
      test_name: g.test_name,
      patient_count: g.patient_count.size,
      abnormal_percentage: g.total ? ((g.abnormal_count / g.total) * 100).toFixed(2) + '%' : '0%',
      last_performed_date: g.last_performed_date || '',
      most_common_abnormal_parameters: Object.entries(g.common_abnormal_parameters).sort((a,b) => b[1] - a[1]).slice(0, 3).map(x => x[0]).join(', '),
    }));
    renderTable('intelligenceTable', out, inferColumns(out));
  }

  async function refreshComparison() {
    if (!comparisonEligible()) {
      setComparisonVisible(false);
      $('comparisonTable').innerHTML = '<tbody><tr><td>Select exactly one test and search one patient to compare repeated test iterations.</td></tr></tbody>';
      $('comparisonMeta').textContent = 'Available for one selected test and one patient.';
      return;
    }
    const data = await api('/api/patient-diagnostic/test-comparison', { method: 'POST', body: JSON.stringify(payload()) });
    if (!data.available) {
      setComparisonVisible(false);
      $('comparisonTable').innerHTML = `<tbody><tr><td>${esc(data.message || 'Comparison not available for the selected filters.')}</td></tr></tbody>`;
      $('comparisonMeta').textContent = data.message || 'Comparison not available for the selected filters.';
      return;
    }
    setComparisonVisible(true);
    $('comparisonMeta').textContent = `${data.patient?.name || 'Patient'} | ${data.patient?.registration_no || ''} | ${data.test_name || 'Selected test'} | ${data.message || ''}`;
    renderTable('comparisonTable', data.rows || [], data.columns || inferColumns(data.rows || []), { comparison: true });
  }

  function comparisonEligible() {
    return selectedTestIds().length === 1 && Boolean(($('patientSearch').value || '').trim());
  }

  function setComparisonVisible(visible) {
    const tab = $('comparisonTab');
    const fabLink = $('comparisonFabLink');
    if (tab) tab.classList.toggle('tab-hidden', !visible);
    if (fabLink) fabLink.classList.toggle('tab-hidden', !visible);
    if (!visible && state.selectedTab === 'comparison') {
      document.querySelector('[data-tab="history"]')?.click();
    }
  }

  function historyColumns() {
    return [
      ['patient_name', 'Patient name'], ['registration_no', 'Registration no'], ['mobile', 'Mobile'],
      ['patient_age', 'Age'], ['visit_no', 'Visit no'], ['visit_date', 'Visit date'], ['test_name', 'Test name'],
      ['parameter_name', 'Parameter'], ['result', 'Result'], ['unit', 'Unit'], ['normal_range', 'Normal range'],
      ['result_status', 'Result status'], ['doctor_name', 'Doctor'],
    ].map(([key, label]) => ({ key, label }));
  }
  function abnormalColumns() {
    return [
      ['patient_name', 'Patient name'], ['registration_no', 'Registration no'], ['mobile', 'Mobile'],
      ['patient_age', 'Age'], ['visit_date', 'Visit date'], ['test_name', 'Test name'], ['parameter_name', 'Parameter'],
      ['result', 'Result'], ['normal_range', 'Normal range'], ['result_status', 'Status'],
      ['doctor_name', 'Doctor'], ['suggested_action', 'Suggested action'],
    ].map(([key, label]) => ({ key, label }));
  }
  function inferColumns(rows) {
    const hidden = new Set(['authorization_status', 'result_auth_flag', 'result_auth_by', 'result_auth_datetime', 'rn', 'total_rows']);
    const keys = rows[0] ? Object.keys(rows[0]).filter(k => !hidden.has(k)).slice(0, 16) : [];
    return keys.map(k => ({ key: k, label: k.replace(/_/g, ' ').replace(/\b\w/g, s => s.toUpperCase()) }));
  }
  function cellClass(value, key, options = {}) {
    if (options.comparison && /^i\d+$/.test(String(key))) {
      const status = comparisonCellStatus(value);
      return status ? `comparison-cell comparison-${status}` : 'comparison-cell';
    }
    return '';
  }

  function comparisonCellStatus(value) {
    const match = String(value || '').match(/\((Normal|High|Low|Abnormal|Critical|Unclassified)\)\s*$/i);
    return match ? match[1].toLowerCase() : '';
  }

  function formatCell(value, key, options = {}) {
    if (value === null || value === undefined || value === '') return '';
    if (options.comparison && /^i\d+$/.test(String(key))) {
      const match = String(value).match(/^(.*)\s+\((Normal|High|Low|Abnormal|Critical|Unclassified)\)\s*$/i);
      if (match) {
        const status = match[2];
        return `<span class="comparison-value"><strong>${esc(match[1])}</strong><span class="mini-status ${status.toLowerCase()}">${esc(status)}</span></span>`;
      }
    }
    if (key.includes('status') || key === 'priority') {
      return `<span class="badge ${String(value).toLowerCase().replace(/\s+/g, '-')}">${esc(value)}</span>`;
    }
    return esc(String(value));
  }
  function esc(value) {
    return String(value ?? '').replace(/[&<>"']/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
  }

  const comboConfig = {
    test: {
      selectId: 'testSelect',
      triggerId: 'testComboTrigger',
      listId: 'testComboList',
      searchId: 'testSearch',
      placeholder: 'Search and select tests',
      allLabel: 'Search and select tests',
    },
    parameter: {
      selectId: 'parameterSelect',
      triggerId: 'parameterComboTrigger',
      listId: 'parameterComboList',
      searchId: 'parameterSearch',
      placeholder: 'Search parameters',
      allLabel: 'All parameters',
    },
    visit: {
      selectId: 'visitType',
      triggerId: 'visitComboTrigger',
      listId: 'visitComboList',
      searchId: 'visitSearch',
      placeholder: 'Search visit type',
      allLabel: 'All visit types',
    },
  };

  function renderCombo(name) {
    const cfg = comboConfig[name];
    const select = $(cfg.selectId);
    const list = $(cfg.listId);
    const search = ($(cfg.searchId)?.value || '').trim().toLowerCase();
    const options = Array.from(select.options || []);
    const visible = options.filter(opt => opt.textContent.toLowerCase().includes(search));
    if (!visible.length) {
      list.innerHTML = `<div class="combo-empty">No matches found</div>`;
    } else if (name === 'visit') {
      list.innerHTML = renderVisitComboOptions(visible, search);
    } else {
      list.innerHTML = visible.map(opt => `
        <div class="combo-option ${opt.selected ? 'is-selected' : ''}" data-value="${esc(opt.value)}" data-combo-option="${name}">
          <span class="combo-check"></span>
          <span>${esc(opt.textContent)}</span>
        </div>
      `).join('');
    }
    updateComboLabel(name);
  }

  function renderVisitComboOptions(options, search) {
    const groups = [
      { label: 'OutPatient', values: ['OPD', 'DPV'] },
      { label: 'In-Patient', values: ['IPD'] },
      { label: 'HealthCheckup', values: ['HCV'] },
    ];
    const byValue = Object.fromEntries(options.map(opt => [opt.value, opt]));
    const html = groups.map(group => {
      const items = group.values
        .map(value => byValue[value])
        .filter(Boolean)
        .filter(opt => !search || `${group.label} ${opt.textContent}`.toLowerCase().includes(search));
      if (!items.length) return '';
      return `
        <div class="combo-group-label">${esc(group.label)}</div>
        ${items.map(opt => `
          <div class="combo-option ${opt.selected ? 'is-selected' : ''}" data-value="${esc(opt.value)}" data-combo-option="visit">
            <span class="combo-check"></span>
            <span>${esc(opt.textContent)}</span>
          </div>
        `).join('')}
      `;
    }).join('');
    return html || `<div class="combo-empty">No matches found</div>`;
  }

  function updateComboLabel(name) {
    const cfg = comboConfig[name];
    const select = $(cfg.selectId);
    const trigger = $(cfg.triggerId);
    const values = name === 'test' ? Array.from(state.selectedTests.values()) : selectedOptionLabels(select);
    if (!values.length) {
      trigger.textContent = cfg.allLabel;
      trigger.title = cfg.allLabel;
    } else if (values.length === 1) {
      trigger.textContent = compactLabel(values[0]);
      trigger.title = values[0];
    } else {
      trigger.textContent = `${values.length} selected`;
      trigger.title = values.join(', ');
    }
  }

  function compactLabel(value) {
    const text = String(value || '').trim();
    return text.length > 30 ? `${text.slice(0, 27)}...` : text;
  }

  function setOptionSelected(select, value, selected) {
    const opt = Array.from(select.options || []).find(item => item.value === value);
    if (opt) opt.selected = selected;
  }

  function toggleCombo(name) {
    const combo = document.querySelector(`.search-combo[data-combo="${name}"]`);
    const isOpen = combo.classList.contains('open');
    closeCombos();
    if (!isOpen) {
      combo.classList.add('open');
      renderCombo(name);
      const search = $(comboConfig[name].searchId);
      if (search) setTimeout(() => search.focus(), 0);
    }
  }

  function closeCombos() {
    document.querySelectorAll('.search-combo.open').forEach(el => el.classList.remove('open'));
  }

  function closeFabMenu() {
    const menu = $('pdiFabMenu');
    const toggle = $('pdiFabToggle');
    if (menu) menu.classList.remove('open');
    if (toggle) toggle.setAttribute('aria-expanded', 'false');
  }

  function openFabTab(name) {
    const tab = document.querySelector(`.pdi-tabs [data-tab="${name}"]`);
    const panel = $(`tab-${name}`);
    if (!tab || !panel) return;
    tab.click();
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  document.addEventListener('click', async (event) => {
    const fabToggle = event.target.closest('#pdiFabToggle');
    if (fabToggle) {
      const menu = $('pdiFabMenu');
      const willOpen = !menu.classList.contains('open');
      menu.classList.toggle('open', willOpen);
      fabToggle.setAttribute('aria-expanded', willOpen ? 'true' : 'false');
      return;
    }

    const fabTab = event.target.closest('[data-fab-tab]');
    if (fabTab) {
      openFabTab(fabTab.dataset.fabTab);
      closeFabMenu();
      return;
    }

    if (!event.target.closest('.pdi-fab-user')) closeFabMenu();

    if (event.target.closest('#changePatientBtn')) {
      unlockPatientSelection();
      return;
    }

    const patientChoice = event.target.closest('[data-patient-choice-id]');
    if (patientChoice) {
      selectPatient(patientChoice.dataset.patientChoiceId);
      return;
    }

    const suggestedTest = event.target.closest('[data-suggested-test-id]');
    if (suggestedTest) {
      selectSingleTest(suggestedTest.dataset.suggestedTestId, suggestedTest.dataset.suggestedTestLabel);
      return;
    }

    const trigger = event.target.closest('.combo-trigger');
    if (trigger) {
      const combo = trigger.closest('.search-combo')?.dataset.combo;
      if (combo) toggleCombo(combo);
      return;
    }
    const option = event.target.closest('[data-combo-option]');
    if (option) {
      const name = option.dataset.comboOption;
      const cfg = comboConfig[name];
      const select = $(cfg.selectId);
      const value = option.dataset.value;
      const current = Array.from(select.options || []).find(item => item.value === value)?.selected;
      const opt = Array.from(select.options || []).find(item => item.value === value);
      if (name === 'test') {
        if (current) {
          state.selectedTests.delete(String(value));
        } else {
          rememberTest(value, opt?.textContent || value);
        }
      }
      setOptionSelected(select, value, !current);
      renderCombo(name);
      if (name === 'test') {
        loadParameters().catch(err => toast(err.message, 'error'));
      }
      return;
    }
    if (!event.target.closest('.search-combo')) closeCombos();

    const tab = event.target.closest('[data-tab]');
    if (tab) {
      document.querySelectorAll('[data-tab]').forEach(b => b.classList.remove('active'));
      tab.classList.add('active');
      document.querySelectorAll('.pdi-tab-panel').forEach(p => p.classList.remove('active'));
      $(`tab-${tab.dataset.tab}`).classList.add('active');
      state.selectedTab = tab.dataset.tab;
    }
    const load = event.target.closest('[data-load]');
    if (load) {
      try {
        if (load.dataset.load === 'abnormal') await loadAbnormal();
      } catch (err) { showError(err); }
    }
  });

  $('testSearch').addEventListener('input', debounce(() => loadTests().catch(err => toast(err.message, 'error')), 350));
  $('patientSearch').addEventListener('input', () => {
    if (state.selectedPatientId && patientSearchValue() !== state.lastPatientSearch) {
      clearSelectedPatient();
      setComparisonVisible(false);
    }
  });
  $('patientSearch').addEventListener('input', debounce(() => {
    loadPatientTestSuggestions();
  }, 450));
  $('fromDate').addEventListener('change', () => loadPatientTestSuggestions());
  $('toDate').addEventListener('change', () => loadPatientTestSuggestions());
  $('parameterSearch').addEventListener('input', () => renderCombo('parameter'));
  $('visitSearch').addEventListener('input', () => renderCombo('visit'));
  $('testSelect').addEventListener('change', () => {
    Array.from($('testSelect').options || []).forEach((opt) => {
      if (opt.selected) rememberTest(opt.value, opt.textContent);
      else state.selectedTests.delete(String(opt.value));
    });
    renderCombo('test');
    loadParameters().catch(err => toast(err.message, 'error'));
  });
  $('unitSelect').addEventListener('change', () => {
    state.selectedTests.clear();
    clearSelectedPatient();
    $('parameterSelect').innerHTML = '';
    setComparisonVisible(false);
    renderPatientTestSuggestions([]);
    renderPatientSummary([]);
    loadTests().then(loadParameters).catch(err => toast(err.message, 'error'));
  });
  $('selectVisibleTests').addEventListener('click', () => {
    const visibleValues = Array.from(document.querySelectorAll('#testComboList [data-value]')).map(el => el.dataset.value);
    Array.from($('testSelect').options).forEach(o => {
      if (visibleValues.includes(o.value)) {
        o.selected = true;
        rememberTest(o.value, o.textContent);
      }
    });
    renderCombo('test');
    loadParameters().catch(err => toast(err.message, 'error'));
  });
  $('clearTests').addEventListener('click', () => {
    state.selectedTests.clear();
    Array.from($('testSelect').options).forEach(o => o.selected = false);
    $('parameterSelect').innerHTML = '';
    renderCombo('test');
    renderCombo('parameter');
    setComparisonVisible(false);
  });
  $('applyFilters').addEventListener('click', () => { closeCombos(); state.page = 1; loadHistory().catch(showError); });
  $('resetFilters').addEventListener('click', () => {
    setDefaultDates();
    $('patientSearch').value = '';
    clearSelectedPatient();
    $('resultStatus').value = 'all';
    $('authStatus').value = 'all';
    $('followupGap').value = 90;
    state.selectedTests.clear();
    Array.from($('testSelect').options).forEach(o => o.selected = false);
    Array.from($('parameterSelect').options).forEach(o => o.selected = false);
    Array.from($('visitType').options).forEach(o => o.selected = false);
    $('testSearch').value = '';
    $('parameterSearch').value = '';
    $('visitSearch').value = '';
    renderCombo('test');
    renderCombo('parameter');
    renderCombo('visit');
    setComparisonVisible(false);
    renderPatientTestSuggestions([]);
    renderPatientSummary([]);
    toast('Filters reset');
  });
  $('prevPage').addEventListener('click', () => { if (state.page > 1) { state.page -= 1; loadHistory().catch(showError); } });
  $('nextPage').addEventListener('click', () => { if (state.hasMore) { state.page += 1; loadHistory().catch(showError); } });
  $('exportExcel').addEventListener('click', () => download('/api/patient-diagnostic/reports/export-excel', payload(), 'Preparing Excel export. Please wait...').catch(showError));
  $('exportPdf').addEventListener('click', () => download('/api/patient-diagnostic/reports/export-pdf', payload(), 'Preparing PDF export. Please wait...').catch(showError));

  function debounce(fn, delay) {
    let timer = null;
    return (...args) => {
      clearTimeout(timer);
      timer = setTimeout(() => fn(...args), delay);
    };
  }

  async function init() {
    setDefaultDates();
    await loadOptions();
    await loadTests();
    renderCombo('visit');
    setHistoryPrompt('Select at least one pathology test, then apply filters.');
  }
  init().catch(showError);
})();
