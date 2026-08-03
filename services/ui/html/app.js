// The UI only talks to its own origin. nginx reverse-proxies these prefixes to
// the producer and serving Kubernetes Services (see nginx.conf) -> no CORS issues.
const API_PRODUCER = "/api/producer";
const API_SERVING = "/api/serving";
const FALLBACK_REFRESH_MS = 3000;
const FLAGGED_BUFFER_LIMIT = 100;
const TABLE_ROW_LIMIT = 15;

let latestSummary = { processed: 0, flagged: 0, fraud_rate: 0 };
let latestFlaggedItems = [];
let activeRules = { highAmount: 800, riskThreshold: 0.7 };

const MERCHANTS = [
  ["M0001", "QuickCash ATM (high risk)", 0.85],
  ["M0002", "Global Electronics", 0.35],
  ["M0003", "City Supermarket", 0.05],
  ["M0004", "LuxWatches Online (high risk)", 0.70],
  ["M0006", "CryptoExchange X (high risk)", 0.90],
  ["M0009", "Betsy Casino (high risk)", 0.88],
  ["M0010", "Corner Bakery", 0.03],
  ["M0011", "Overseas Wire Ltd (high risk)", 0.92],
  ["M0013", "Pharmacy Central", 0.08],
  ["M0015", "RideShare Go", 0.20],
];

function populateMerchants() {
  const sel = document.getElementById("merchant");
  for (const [id, name, risk] of MERCHANTS) {
    const opt = document.createElement("option");
    opt.value = id;
    opt.dataset.risk = String(risk);
    opt.textContent = `${id} — ${name}`;
    sel.appendChild(opt);
  }
  sel.value = "M0002";
}

async function readApiResponse(response) {
  const body = await response.text();
  let data = null;
  try {
    data = body ? JSON.parse(body) : {};
  } catch {
    const message = response.ok
      ? "Server returned an invalid response"
      : `HTTP ${response.status} ${response.statusText}`;
    throw new Error(message);
  }
  if (!response.ok) {
    throw new Error(data.detail || `HTTP ${response.status} ${response.statusText}`);
  }
  return data;
}

async function loadRules() {
  try {
    const response = await fetch(`${API_PRODUCER}/rules`);
    const data = await readApiResponse(response);
    activeRules = {
      highAmount: Number(data.high_amount),
      riskThreshold: Number(data.risk_threshold),
    };
  } catch {
    // Defaults match the chart and keep the preview usable during startup.
  }
  updateManualRulePreview();
}

function updateManualRulePreview() {
  const form = document.getElementById("tx-form");
  const amountFlag = Number(form.amount.value) > activeRules.highAmount;
  const selected = form.merchant_id.selectedOptions[0];
  const merchantFlag =
    Number(selected?.dataset.risk ?? 0) >= activeRules.riskThreshold;
  const preview = document.getElementById("manual-rule-preview");
  const reason = fraudReason({
    amount_flag: Number(amountFlag),
    merchant_flag: Number(merchantFlag),
  });
  const flagged = amountFlag || merchantFlag;
  preview.textContent = flagged ? `Expected: ${reason.label}` : "Expected: Not flagged";
  preview.className = `manual-preview reason-badge ${
    flagged ? reason.className : "reason-normal"
  }`;
}

async function submitTransaction(evt) {
  evt.preventDefault();
  const form = evt.target;
  const payload = {
    card_id: form.card_id.value,
    user_id: form.user_id.value,
    merchant_id: form.merchant_id.value,
    amount: parseFloat(form.amount.value),
    country: form.country.value,
  };
  const out = document.getElementById("tx-result");
  try {
    const res = await fetch(`${API_PRODUCER}/transactions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await readApiResponse(res);
    out.textContent = `Accepted: ${data.transaction_id}`;
    out.className = "result ok";
  } catch (e) {
    out.textContent = `Error: ${e.message}`;
    out.className = "result err";
  }
}

async function simulate() {
  const count = parseInt(document.getElementById("sim-count").value, 10);
  const fraud_ratio = parseFloat(document.getElementById("sim-ratio").value) / 100;
  const out = document.getElementById("sim-result");
  out.textContent = "Scheduling stream…";
  out.className = "result";
  try {
    const res = await fetch(`${API_PRODUCER}/simulate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count, fraud_ratio, burst_card: true }),
    });
    const data = await readApiResponse(res);
    const plan = data.planned;
    out.textContent =
      `Scheduled ${data.scheduled.toLocaleString()} events (~${data.estimated_seconds}s): ` +
      `${plan.high_amount} amount, ${plan.high_risk_merchant} merchant, ` +
      `${plan.both} both, +${plan.velocity_burst} velocity-only`;
    out.className = "result ok";
  } catch (e) {
    out.textContent = `Error: ${e.message}`;
    out.className = "result err";
  }
}

function fmtTime(v) {
  if (!v) return "";
  const d = new Date(v);
  if (isNaN(d)) return String(v);
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  const ms = String(d.getMilliseconds()).padStart(3, "0");
  return `${hh}:${mm}:${ss}.${ms}`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(
    /[&<>'"]/g,
    (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[char]
  );
}

function formatAmount(value) {
  const amount = Number(value);
  return Number.isFinite(amount)
    ? amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : "";
}

function fraudReason(row) {
  const amount = Number(row.amount_flag) === 1;
  const merchant = Number(row.merchant_flag) === 1;
  if (amount && merchant) {
    return { key: "both", label: "Amount + merchant", className: "reason-both" };
  }
  if (amount) {
    return { key: "amount", label: "High amount", className: "reason-amount" };
  }
  if (merchant) {
    return { key: "merchant", label: "High-risk merchant", className: "reason-merchant" };
  }
  return { key: "rule", label: "Rule match", className: "reason-rule" };
}

function fillTable(id, rows, mapper, emptyColumns, emptyMessage = "No data yet") {
  const tbody = document.querySelector(`#${id} tbody`);
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = mapper(r);
    tbody.appendChild(tr);
  }
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${emptyColumns}" class="empty">${escapeHtml(emptyMessage)}</td></tr>`;
  }
}

function renderFlaggedTable() {
  const query = document.getElementById("flagged-search").value.trim().toLowerCase();
  const selectedReason = document.getElementById("flagged-reason").value;
  const filtered = latestFlaggedItems.filter((row) => {
    const searchable = [row.card_id, row.merchant_id, row.merchant_category]
      .map((value) => String(value ?? "").toLowerCase())
      .join(" ");
    const matchesQuery = !query || searchable.includes(query);
    const matchesReason = !selectedReason || fraudReason(row).key === selectedReason;
    return matchesQuery && matchesReason;
  });
  const visible = filtered.slice(0, TABLE_ROW_LIMIT);
  const total = Number(latestSummary.flagged ?? 0);
  const hasFilters = Boolean(query || selectedReason);

  fillTable(
    "flagged-table",
    visible,
    (row) => {
      const reason = fraudReason(row);
      return (
        `<td>${escapeHtml(fmtTime(row.event_time))}</td>` +
        `<td class="mono">${escapeHtml(row.card_id)}</td>` +
        `<td><span class="merchant-id">${escapeHtml(row.merchant_id)}</span>` +
        `<span class="merchant-category">${escapeHtml(row.merchant_category)}</span></td>` +
        `<td class="amount">€${escapeHtml(formatAmount(row.amount))}</td>` +
        `<td class="score">${escapeHtml(Number(row.fraud_score ?? 0).toFixed(3))}</td>` +
        `<td><span class="reason-badge ${reason.className}">${escapeHtml(reason.label)}</span></td>`
      );
    },
    6,
    hasFilters ? "No flagged transactions match these filters" : "No flagged transactions yet"
  );

  const count = document.getElementById("flagged-count");
  if (total === 0) {
    count.textContent = "No flagged transactions yet";
  } else if (hasFilters) {
    count.textContent = `Showing ${visible.length} · ${filtered.length} matches in latest ${latestFlaggedItems.length} · ${total} total flagged`;
  } else {
    count.textContent = `Showing latest ${visible.length} of ${total} flagged`;
  }
}

async function refresh() {
  const status = document.getElementById("dashboard-status");
  try {
    const [summary, flagged, velocity] = await Promise.all([
      fetch(`${API_SERVING}/summary`).then((r) => r.json()),
      fetch(`${API_SERVING}/flagged?limit=${FLAGGED_BUFFER_LIMIT}`).then((r) => r.json()),
      fetch(`${API_SERVING}/velocity?limit=15`).then((r) => r.json()),
    ]);

    renderDashboard(summary, flagged, velocity);
    status.textContent = "";
    status.className = "result";
  } catch (e) {
    status.textContent = "Pipeline data is temporarily unavailable.";
    status.className = "result err";
  }
}

function renderDashboard(summary, flagged, velocity) {
  latestSummary = summary || latestSummary;
  latestFlaggedItems = flagged?.items || [];
  document.getElementById("m-processed").textContent = summary?.processed ?? 0;
  document.getElementById("m-flagged").textContent = summary?.flagged ?? 0;
  document.getElementById("m-rate").textContent =
    Math.round((summary?.fraud_rate ?? 0) * 100) + "%";

  renderFlaggedTable();

  fillTable(
    "velocity-table",
    velocity?.items || [],
    (row) =>
      `<td>${escapeHtml(fmtTime(row.window_end))}</td><td class="mono">${escapeHtml(row.card_id)}</td>` +
      `<td>${escapeHtml(row.tx_count)}</td><td class="amount">€${escapeHtml(formatAmount(row.amount_sum))}</td>`,
    4
  );
}

let sse = null;
let fallbackTimer = null;

function startFallbackPolling() {
  stopFallbackPolling();
  refresh();
  fallbackTimer = setInterval(refresh, FALLBACK_REFRESH_MS);
}

function stopFallbackPolling() {
  if (fallbackTimer) {
    clearInterval(fallbackTimer);
    fallbackTimer = null;
  }
}

function connectStream() {
  if (!window.EventSource) {
    startFallbackPolling();
    return;
  }

  if (sse) {
    sse.close();
  }

  sse = new EventSource(`${API_SERVING}/stream?limit=${FLAGGED_BUFFER_LIMIT}`);

  sse.addEventListener("dashboard", (evt) => {
    try {
      const data = JSON.parse(evt.data);
      renderDashboard(data.summary, data.flagged, data.velocity);
      stopFallbackPolling();
    } catch (e) {
      // ignore malformed event
    }
  });

  sse.onopen = () => {
    stopFallbackPolling();
    const status = document.getElementById("dashboard-status");
    status.textContent = "";
    status.className = "result";
  };

  sse.onerror = () => {
    const status = document.getElementById("dashboard-status");
    status.textContent = "Live stream reconnecting; fallback polling is active.";
    status.className = "result err";
    startFallbackPolling();
    setTimeout(connectStream, 2000);
  };
}

document.getElementById("tx-form").addEventListener("submit", submitTransaction);
document.querySelector('#tx-form input[name="amount"]').addEventListener("input", updateManualRulePreview);
document.getElementById("merchant").addEventListener("change", updateManualRulePreview);
document.getElementById("simulate").addEventListener("click", simulate);
document.getElementById("flagged-search").addEventListener("input", renderFlaggedTable);
document.getElementById("flagged-reason").addEventListener("change", renderFlaggedTable);
populateMerchants();
updateManualRulePreview();
loadRules();
connectStream();
