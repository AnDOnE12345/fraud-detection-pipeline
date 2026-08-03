// The UI only talks to its own origin. nginx reverse-proxies these prefixes to
// the producer and serving Kubernetes Services (see nginx.conf) -> no CORS issues.
const API_PRODUCER = "/api/producer";
const API_SERVING = "/api/serving";

const MERCHANTS = [
  ["M0001", "QuickCash ATM (high risk)"],
  ["M0002", "Global Electronics"],
  ["M0003", "City Supermarket"],
  ["M0004", "LuxWatches Online"],
  ["M0006", "CryptoExchange X (high risk)"],
  ["M0009", "Betsy Casino (high risk)"],
  ["M0010", "Corner Bakery"],
  ["M0011", "Overseas Wire Ltd (high risk)"],
  ["M0013", "Pharmacy Central"],
  ["M0015", "RideShare Go"],
];

function populateMerchants() {
  const sel = document.getElementById("merchant");
  for (const [id, name] of MERCHANTS) {
    const opt = document.createElement("option");
    opt.value = id;
    opt.textContent = `${id} — ${name}`;
    sel.appendChild(opt);
  }
  sel.value = "M0006";
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
    const data = await res.json();
    out.textContent = res.ok
      ? `Accepted: ${data.transaction_id}`
      : `Error: ${data.detail || res.status}`;
    out.className = res.ok ? "result ok" : "result err";
  } catch (e) {
    out.textContent = `Network error: ${e}`;
    out.className = "result err";
  }
}

async function simulate() {
  const count = parseInt(document.getElementById("sim-count").value, 10);
  const fraud_ratio = parseFloat(document.getElementById("sim-ratio").value);
  const out = document.getElementById("sim-result");
  out.textContent = "Producing…";
  out.className = "result";
  try {
    const res = await fetch(`${API_PRODUCER}/simulate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ count, fraud_ratio, burst_card: true }),
    });
    const data = await res.json();
    out.textContent = res.ok ? `Produced ${data.produced} events` : `Error ${res.status}`;
    out.className = res.ok ? "result ok" : "result err";
  } catch (e) {
    out.textContent = `Network error: ${e}`;
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

function fillTable(id, rows, mapper) {
  const tbody = document.querySelector(`#${id} tbody`);
  tbody.innerHTML = "";
  for (const r of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = mapper(r);
    tbody.appendChild(tr);
  }
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="5" class="empty">No data yet</td></tr>`;
  }
}

async function refresh() {
  try {
    const [summary, flagged, velocity] = await Promise.all([
      fetch(`${API_SERVING}/summary`).then((r) => r.json()),
      fetch(`${API_SERVING}/flagged?limit=15`).then((r) => r.json()),
      fetch(`${API_SERVING}/velocity?limit=15`).then((r) => r.json()),
    ]);

    document.getElementById("m-processed").textContent = summary.processed ?? 0;
    document.getElementById("m-flagged").textContent = summary.flagged ?? 0;
    document.getElementById("m-rate").textContent =
      Math.round((summary.fraud_rate ?? 0) * 100) + "%";

    fillTable(
      "flagged-table",
      flagged.items || [],
      (r) =>
        `<td>${fmtTime(r.event_time)}</td><td>${r.card_id ?? ""}</td>` +
        `<td>${r.merchant_id ?? ""}</td><td>${(r.amount ?? 0).toFixed?.(2) ?? r.amount}</td>` +
        `<td>${r.fraud_score ?? ""}</td>`
    );

    fillTable(
      "velocity-table",
      velocity.items || [],
      (r) =>
        `<td>${fmtTime(r.window_end)}</td><td>${r.card_id ?? ""}</td>` +
        `<td>${r.tx_count ?? ""}</td><td>${r.amount_sum ?? ""}</td>`
    );
  } catch (e) {
    // Serving may not be reachable yet; ignore during startup.
  }
}

document.getElementById("tx-form").addEventListener("submit", submitTransaction);
document.getElementById("simulate").addEventListener("click", simulate);
populateMerchants();
refresh();
setInterval(refresh, 5000);
