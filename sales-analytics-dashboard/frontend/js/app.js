/* Sales Analytics Dashboard — front-end (vanilla JS + Chart.js). No build step needed. */
"use strict";

const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const C = { indigo: "#6366f1", cyan: "#22d3ee", green: "#34d399", amber: "#fbbf24", rose: "#fb7185", violet: "#a78bfa", sky: "#38bdf8", slate: "#64748b" };
const PAL = [C.indigo, C.cyan, C.green, C.amber, C.rose, C.violet, C.sky];
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

const state = { from: "", to: "", region: "", category: "", channel: "", grain: "month", metric: "revenue", tab: "overview" };
const stale = new Set(["overview", "customers", "predict", "cloud"]);
let META = null, queryCount = 0, sampleSeed = 1;
const charts = {};

/* ---------------- formatting ---------------- */
const inr = (n) => {                       // Indian compact notation: K, L (lakh), Cr (crore)
  if (n == null || isNaN(n)) return "—";
  const a = Math.abs(n), s = n < 0 ? "-" : "";
  if (a >= 1e7) return `${s}₹${(a / 1e7).toFixed(a >= 1e8 ? 0 : 2)} Cr`;
  if (a >= 1e5) return `${s}₹${(a / 1e5).toFixed(2)} L`;
  if (a >= 1e3) return `${s}₹${(a / 1e3).toFixed(1)}K`;
  return `${s}₹${a.toFixed(0)}`;
};
const compact = (n) => n == null ? "—" : n >= 1e7 ? (n / 1e7).toFixed(2) + " Cr" : n >= 1e5 ? (n / 1e5).toFixed(2) + " L" : n >= 1e3 ? (n / 1e3).toFixed(1) + "K" : Math.round(n).toString();
const int = (n) => Number(n).toLocaleString("en-IN", { maximumFractionDigits: 0 });
const pct = (x, d = 1) => x == null ? "—" : (x * 100).toFixed(d) + "%";
const bytes = (b) => b >= 1e9 ? (b / 1e9).toFixed(2) + " GB" : b >= 1e6 ? (b / 1e6).toFixed(1) + " MB" : b >= 1e3 ? (b / 1e3).toFixed(1) + " KB" : b + " B";
const mlabel = (iso) => { const [y, m] = iso.split("-"); return `${MONTHS[+m - 1]} ${y.slice(2)}`; };
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const alpha = (hex, a) => { const n = parseInt(hex.slice(1), 16); return `rgba(${n >> 16},${(n >> 8) & 255},${n & 255},${a})`; };

/* ---------------- chart defaults ---------------- */
Chart.defaults.color = "#8e99ba";
Chart.defaults.font.family = getComputedStyle(document.body).fontFamily;
Chart.defaults.font.size = 12;
Chart.defaults.borderColor = "rgba(255,255,255,.06)";
Chart.defaults.maintainAspectRatio = false;
Chart.defaults.plugins.legend.labels.usePointStyle = true;
Chart.defaults.plugins.legend.labels.boxWidth = 8;
Object.assign(Chart.defaults.plugins.tooltip, { backgroundColor: "#0a0f1f", borderColor: "#243052", borderWidth: 1, padding: 10, titleColor: "#e8ecf8", bodyColor: "#c5cde6", cornerRadius: 10 });
Chart.defaults.interaction = { mode: "index", intersect: false };

function chart(id, cfg) {
  if (charts[id]) charts[id].destroy();
  charts[id] = new Chart(document.getElementById(id), cfg);
  return charts[id];
}
const axisMoney = { ticks: { callback: (v) => inr(v) }, grid: { color: "rgba(255,255,255,.06)" } };

/* ---------------- API ---------------- */
async function api(path, extra = {}) {
  const p = new URLSearchParams();
  for (const k of ["from", "to", "region", "category", "channel"]) if (state[k]) p.set(k, state[k]);
  for (const [k, v] of Object.entries(extra)) p.set(k, v);
  const t = performance.now();
  const r = await fetch(`/api/${path}?${p}`);
  const j = await r.json();
  if (!r.ok) { const e = new Error(j.error || r.statusText); e.code = j.code; throw e; }
  queryCount++;
  const ms = j.meta ? j.meta.query_ms : 0;
  $("#pill-perf span").textContent = `${j.meta.cached ? "cache hit · " : ""}${ms} ms engine · ${Math.round(performance.now() - t)} ms round-trip`;
  $("#foot-stats").textContent = `${queryCount} analytics queries served this session`;
  return j.data;
}
const getJSON = async (u) => (await fetch(u)).json();

function toast(msg, err = false) {
  const t = $("#toast"); t.textContent = msg; t.className = "toast" + (err ? " err" : ""); t.hidden = false;
  clearTimeout(toast._t); toast._t = setTimeout(() => (t.hidden = true), 5000);
}

/* ---------------- bootstrap ---------------- */
async function init() {
  try {
    META = await api("meta");
  } catch (e) {
    const b = $("#banner"); b.hidden = false;
    b.innerHTML = `<b>No data in the cloud data lake yet.</b> Run <code>python -m etl.generate_data</code> then <code>python -m etl.pipeline</code> and refresh this page.`;
    return;
  }
  const st = META.storage;
  $("#pill-storage span").innerHTML = `${esc(st.backend)} · <code>${esc(st.bucket)}</code>`;
  $("#pill-rows span").textContent = `${(META.rows / 1e6).toFixed(2)}M lines · ${int(META.customers)} customers · ${META.products} products`;
  fill("#f-region", META.regions); fill("#f-category", META.categories); fill("#f-channel", META.channels);
  state.from = META.min_date; state.to = META.max_date;
  buildPresets(); syncInputs(); buildExports(); buildStaticTables();

  $$(".tab-btn").forEach((b) => b.addEventListener("click", () => switchTab(b.dataset.tab)));
  $("#f-from").addEventListener("change", (e) => { state.from = e.target.value; changed(); });
  $("#f-to").addEventListener("change", (e) => { state.to = e.target.value; changed(); });
  for (const k of ["region", "category", "channel"]) $("#f-" + k).addEventListener("change", (e) => { state[k] = e.target.value; changed(); });
  $("#f-reset").addEventListener("click", () => { Object.assign(state, { from: META.min_date, to: META.max_date, region: "", category: "", channel: "" }); syncInputs(); changed(); });
  $$("#metric-seg button").forEach((b) => b.addEventListener("click", () => { state.metric = b.dataset.v; toggle("#metric-seg", b); loadTrend(); }));
  $$("#grain-seg button").forEach((b) => b.addEventListener("click", () => { state.grain = b.dataset.v; toggle("#grain-seg", b); loadTrend(); }));
  setupIngest();
  loadTab("overview");
}
const fill = (sel, vals) => vals.forEach((v) => $(sel).insertAdjacentHTML("beforeend", `<option>${esc(v)}</option>`));
const toggle = (sel, btn) => { $$(sel + " button").forEach((b) => b.classList.toggle("on", b === btn)); };

function buildPresets() {
  const end = new Date(META.max_date + "T00:00:00"), iso = (d) => d.toISOString().slice(0, 10);
  const back = (m) => { const d = new Date(end); d.setMonth(d.getMonth() - m); d.setDate(d.getDate() + 1); return iso(d); };
  const y = end.getFullYear();
  const P = [["All time", META.min_date, META.max_date], ["Last 12M", back(12), META.max_date], ["Last 6M", back(6), META.max_date],
             ["Last 3M", back(3), META.max_date], [`${y}`, `${y}-01-01`, META.max_date], [`${y - 1}`, `${y - 1}-01-01`, `${y - 1}-12-31`]];
  const box = $("#presets"); box.innerHTML = "";
  P.forEach(([l, f, t]) => { const b = document.createElement("button"); b.textContent = l; b.dataset.f = f; b.dataset.t = t;
    b.onclick = () => { state.from = f; state.to = t; syncInputs(); changed(); }; box.appendChild(b); });
}
function syncInputs() {
  $("#f-from").value = state.from; $("#f-to").value = state.to;
  $("#f-from").min = $("#f-to").min = META.min_date; $("#f-from").max = $("#f-to").max = META.max_date;
  for (const k of ["region", "category", "channel"]) $("#f-" + k).value = state[k];
  $$("#presets button").forEach((b) => b.classList.toggle("on", b.dataset.f === state.from && b.dataset.t === state.to));
}
function qsFilters() {
  const p = new URLSearchParams(); for (const k of ["from", "to", "region", "category", "channel"]) if (state[k]) p.set(k, state[k]); return p.toString();
}
function buildExports() {
  const items = [["monthly", "Monthly trend"], ["category", "By category"], ["region", "By region"], ["state", "By state"], ["top-products", "Top products"], ["rfm", "RFM segments"]];
  const box = $("#export-list"); box.innerHTML = "";
  items.forEach(([k, l]) => { const a = document.createElement("a"); a.textContent = l; a.href = "#"; a.onclick = (e) => { e.preventDefault(); location.href = `/api/export/${k}.csv?${qsFilters()}`; $(".menu").open = false; }; box.appendChild(a); });
}

function changed() {
  syncInputs(); buildExports();
  ["overview", "customers", "predict"].forEach((t) => stale.add(t));
  loadTab(state.tab);
}
function switchTab(t) {
  state.tab = t;
  $$(".tab-btn").forEach((b) => b.classList.toggle("active", b.dataset.tab === t));
  $$(".tabpane").forEach((p) => p.classList.toggle("active", p.id === "pane-" + t));
  $("#filters").style.display = t === "cloud" ? "none" : "";
  loadTab(t);
}
async function loadTab(t) {
  if (!stale.has(t)) return;
  stale.delete(t);
  const pane = $("#pane-" + t); pane.classList.add("loading");
  try {
    await ({ overview: loadOverview, customers: loadCustomers, predict: loadPredict, cloud: loadCloud }[t])();
  } catch (e) { toast("Error: " + e.message, true); console.error(e); }
  pane.classList.remove("loading");
}

/* =================================================================== OVERVIEW */
async function loadOverview() {
  await Promise.all([loadKpis(), loadTrend(), loadCategory(), loadMatrix(), loadStates(), loadProducts(), loadChannel(), loadHeat(), loadPayment()]);
}
const KPI = [
  { k: "revenue", l: "Revenue", f: inr }, { k: "profit", l: "Net profit", f: inr }, { k: "orders", l: "Orders", f: compact },
  { k: "aov", l: "Avg order value", f: inr }, { k: "margin", l: "Profit margin", f: (v) => pct(v), pp: true }, { k: "customers", l: "Active customers", f: compact },
];
function spark(vals, color) {
  if (!vals || vals.length < 2) return "";
  const w = 96, h = 34, mn = Math.min(...vals), mx = Math.max(...vals), rng = mx - mn || 1;
  const pts = vals.map((v, i) => `${(i / (vals.length - 1) * w).toFixed(1)},${(h - 3 - ((v - mn) / rng) * (h - 8)).toFixed(1)}`);
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}"><polyline points="${pts.join(" ")}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
}
async function loadKpis() {
  const d = await api("kpis");
  $("#kpis").innerHTML = KPI.map((m, i) => {
    const g = d.growth[m.k], up = g != null && g > 0.0005, dn = g != null && g < -0.0005;
    const txt = g == null ? "n/a" : m.pp ? `${g >= 0 ? "+" : ""}${(g * 100).toFixed(1)} pp` : `${g >= 0 ? "+" : ""}${(g * 100).toFixed(1)}%`;
    return `<div class="kpi"><div class="lbl">${m.l}</div><div class="val">${m.f(d.current[m.k])}</div>
      <span class="delta ${up ? "up" : dn ? "down" : "flat"}">${up ? "▲" : dn ? "▼" : "•"} ${txt}</span><span class="vs">vs prior year</span>
      ${spark(d.spark[m.k], PAL[i % PAL.length])}</div>`;
  }).join("");
}
async function loadTrend() {
  const d = await api("trend", { grain: state.grain });
  const m = state.metric, lbl = { revenue: "Revenue", profit: "Profit", orders: "Orders" }[m];
  const fmt = m === "orders" ? compact : inr;
  const labels = d.series.map((r) => r.period);
  const show = (p) => state.grain === "month" ? mlabel(p) : p.slice(5) + "/" + p.slice(2, 4);
  const ma = (() => { const w = d.moving_avg_window, v = d.series.map((r) => r[m]); return v.map((_, i) => { const s = v.slice(Math.max(0, i - w + 1), i + 1); return s.reduce((a, b) => a + b, 0) / s.length; }); })();
  const ds = [];
  if (state.grain === "month") ds.push({ type: "bar", label: lbl, data: d.series.map((r) => r[m]), backgroundColor: alpha(C.indigo, .75), borderRadius: 5, order: 3 });
  else ds.push({ type: "line", label: lbl, data: d.series.map((r) => r[m]), borderColor: C.indigo, backgroundColor: alpha(C.indigo, .18), fill: true, pointRadius: 0, borderWidth: 1.6, tension: .25, order: 3 });
  ds.push({ type: "line", label: `${d.moving_avg_window}-period moving avg`, data: ma, borderColor: C.cyan, borderWidth: 2.4, pointRadius: 0, tension: .35, order: 1 });
  if (m === "revenue" && d.previous_year.length) {
    const map = Object.fromEntries(d.previous_year.map((r) => [r.period, r.revenue]));
    ds.push({ type: "line", label: "Same period last year", data: labels.map((p) => map[p] ?? null), borderColor: C.amber, borderDash: [6, 5], borderWidth: 1.8, pointRadius: 0, tension: .3, order: 2 });
  }
  chart("ch-trend", { data: { labels: labels.map(show), datasets: ds },
    options: { scales: { y: { ...axisMoney, ticks: { callback: (v) => fmt(v) } }, x: { grid: { display: false }, ticks: { maxTicksLimit: 14, autoSkip: true } } },
      plugins: { tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${fmt(c.parsed.y)}` } } } } });
}
async function loadCategory() {
  const d = await api("breakdown/category");
  chart("ch-category", { type: "doughnut", data: { labels: d.map((r) => `${r.name} · ${pct(r.share, 0)}`), datasets: [{ data: d.map((r) => r.revenue), backgroundColor: PAL, borderColor: "#121a30", borderWidth: 3, hoverOffset: 8 }] },
    options: { cutout: "62%", interaction: { mode: "nearest" }, plugins: { legend: { position: "bottom", labels: { padding: 12 } }, tooltip: { callbacks: { label: (c) => ` ${inr(c.parsed)} · margin ${pct(d[c.dataIndex].margin)}` } } } } });
}
async function loadMatrix() {
  const rows = await api("matrix");
  const regions = [...new Set(rows.map((r) => r.region))], cats = [...new Set(rows.map((r) => r.category))];
  chart("ch-matrix", { type: "bar", data: { labels: regions, datasets: cats.map((c, i) => ({ label: c, backgroundColor: PAL[i % PAL.length], borderRadius: 3,
      data: regions.map((rg) => (rows.find((r) => r.region === rg && r.category === c) || {}).revenue || 0) })) },
    options: { scales: { x: { stacked: true, grid: { display: false } }, y: { stacked: true, ...axisMoney } }, plugins: { legend: { position: "bottom", labels: { boxWidth: 8, padding: 10, font: { size: 11 } } }, tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${inr(c.parsed.y)}` } } } } });
}
async function loadStates() {
  const d = await api("breakdown/state", { limit: 10 });
  chart("ch-state", { type: "bar", data: { labels: d.map((r) => r.name), datasets: [{ label: "Revenue", data: d.map((r) => r.revenue), borderRadius: 6, backgroundColor: (c) => { const g = c.chart.ctx.createLinearGradient(0, 0, c.chart.width, 0); g.addColorStop(0, C.indigo); g.addColorStop(1, C.cyan); return g; } }] },
    options: { indexAxis: "y", scales: { x: axisMoney, y: { grid: { display: false } } }, plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => [`Revenue ${inr(c.parsed.x)}`, `Margin ${pct(d[c.dataIndex].margin)} · ${compact(d[c.dataIndex].orders)} orders`] } } } } });
}
async function loadChannel() {
  const d = await api("breakdown/channel");
  chart("ch-channel", { type: "doughnut", data: { labels: d.map((r) => `${r.name} · ${pct(r.share, 0)}`), datasets: [{ data: d.map((r) => r.revenue), backgroundColor: [C.cyan, C.indigo, C.amber, C.green], borderColor: "#121a30", borderWidth: 3 }] },
    options: { cutout: "60%", interaction: { mode: "nearest" }, plugins: { legend: { position: "right", labels: { padding: 12 } }, tooltip: { callbacks: { label: (c) => ` ${inr(c.parsed)}` } } } } });
}
async function loadPayment() {
  const d = await api("breakdown/payment_method");
  chart("ch-payment", { type: "bar", data: { labels: d.map((r) => r.name), datasets: [{ data: d.map((r) => r.share * 100), backgroundColor: PAL, borderRadius: 6 }] },
    options: { indexAxis: "y", scales: { x: { ticks: { callback: (v) => v + "%" } }, y: { grid: { display: false } } }, plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => ` ${c.parsed.x.toFixed(1)}% · ${inr(d[c.dataIndex].revenue)}` } } } } });
}
async function loadProducts() {
  const d = await api("top-products", { n: 10 }), mx = Math.max(...d.map((r) => r.revenue));
  $("#tbl-products").innerHTML = `<thead><tr><th>#</th><th>Product</th><th>Revenue</th><th class="num">Units</th><th class="num">Margin</th><th class="num">Avg disc.</th></tr></thead><tbody>` +
    d.map((r, i) => `<tr><td>${i + 1}</td><td><b>${esc(r.name)}</b><br><span class="chip">${esc(r.category)}</span></td>
      <td style="min-width:130px">${inr(r.revenue)}<div class="bar"><i style="width:${r.revenue / mx * 100}%"></i></div></td>
      <td class="num">${int(r.units)}</td><td class="num"><span class="chip ${r.margin > .3 ? "g" : r.margin > .18 ? "b" : "a"}">${pct(r.margin)}</span></td><td class="num">${pct(r.avg_discount)}</td></tr>`).join("") + "</tbody>";
}
async function loadHeat() {
  const d = await api("heatmap"), all = d.grid.flat(), mx = Math.max(...all), mn = Math.min(...all.filter((v) => v > 0), mx);
  const col = (v) => { const t = mx === mn ? 0 : Math.sqrt((v - mn) / (mx - mn)); const a = [26, 35, 63], b = [34, 211, 238], c = [99, 102, 241]; const m = t < .5 ? a.map((x, i) => x + (c[i] - x) * t * 2) : c.map((x, i) => x + (b[i] - x) * (t - .5) * 2); return `rgb(${m.map(Math.round)})`; };
  let h = `<div></div>` + d.months.map((m) => `<div class="h">${m}</div>`).join("");
  d.grid.forEach((row, i) => { h += `<div class="rl">${d.days[i]}</div>` + row.map((v, j) => `<div class="cell" style="background:${col(v)}" title="${d.days[i]} · ${d.months[j]}: ${inr(v)}">${(v / 1e7).toFixed(0)}</div>`).join(""); });
  $("#heatmap").innerHTML = h + `<div></div><div style="grid-column:span 12" class="note">Cell values are revenue in ₹ crore. Brighter = higher sales — note the festive Oct–Nov peak and weekend lift.</div>`;
}

/* =================================================================== CUSTOMERS */
const SEG = {
  "Champions": ["g", "Reward them, ask for reviews, early access to launches"], "Loyal Customers": ["g", "Upsell premium products, loyalty programme"],
  "Potential Loyalists": ["b", "Offer membership, recommend related products"], "New Customers": ["b", "Welcome journey, onboarding offers"],
  "Needs Attention": ["a", "Limited-time offers, personalised reminders"], "Cant Lose Them": ["r", "Win back with personal outreach & special deals"],
  "At Risk": ["r", "Send reactivation campaigns, ask for feedback"], "Hibernating": ["a", "Re-engage with discounts on favourite categories"], "Lost": ["", "Low-cost revival campaign, otherwise deprioritise"],
};
const SEGCOL = { "Champions": C.green, "Loyal Customers": "#10b981", "Potential Loyalists": C.cyan, "New Customers": C.sky, "Needs Attention": C.amber, "Cant Lose Them": C.rose, "At Risk": "#f97316", "Hibernating": C.violet, "Lost": C.slate };
async function loadCustomers() {
  const [rfm, coh, bk] = await Promise.all([api("rfm"), api("cohorts"), api("basket")]);
  const segs = rfm.segments, name = (s) => s === "Cant Lose Them" ? "Can't Lose Them" : s;
  chart("ch-rfm", { type: "bar", data: { labels: segs.map((s) => name(s.segment)), datasets: [
      { label: "% of customers", data: segs.map((s) => s.customer_share * 100), backgroundColor: alpha(C.slate, .8), borderRadius: 4 },
      { label: "% of revenue", data: segs.map((s) => s.revenue_share * 100), backgroundColor: segs.map((s) => SEGCOL[s.segment]), borderRadius: 4 }] },
    options: { indexAxis: "y", scales: { x: { ticks: { callback: (v) => v + "%" } }, y: { grid: { display: false } } }, plugins: { tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.parsed.x.toFixed(1)}%` } } } } });
  $("#rfm-sub").textContent = `${int(rfm.total_customers)} customers scored as of ${rfm.as_of}`;
  $("#tbl-rfm").innerHTML = `<thead><tr><th>Segment</th><th class="num">Customers</th><th class="num">Revenue</th><th class="num">Avg spend</th><th class="num">Orders</th><th class="num">Last buy</th><th>Recommended action</th></tr></thead><tbody>` +
    segs.map((s) => `<tr><td><span class="chip ${SEG[s.segment][0]}">${name(s.segment)}</span></td><td class="num">${int(s.customers)}</td><td class="num">${inr(s.revenue)}</td><td class="num">${inr(s.avg_spend)}</td>
      <td class="num">${s.avg_orders.toFixed(1)}</td><td class="num">${Math.round(s.avg_recency_days)}d ago</td><td style="color:var(--muted);font-size:12px">${SEG[s.segment][1]}</td></tr>`).join("") + "</tbody>";

  const cell = (v) => v == null ? `<td></td>` : `<td class="c" style="background:rgba(99,102,241,${(0.12 + v * 0.9).toFixed(2)})">${(v * 100).toFixed(0)}%</td>`;
  $("#tbl-cohort").innerHTML = `<thead><tr><th>Cohort</th><th>Size</th>${[...Array(12).keys()].map((k) => `<th>M${k}</th>`).join("")}</tr></thead><tbody>` +
    coh.cohorts.map((c) => `<tr><td><b>${mlabel(c.cohort)}</b></td><td>${int(c.size)}</td>${c.retention.map(cell).join("")}</tr>`).join("") + "</tbody>";

  $("#tbl-basket").innerHTML = `<thead><tr><th>Product A</th><th>Product B</th><th class="num">Bought together</th><th class="num">Support</th><th class="num">P(B|A)</th><th class="num">P(A|B)</th><th style="min-width:150px">Lift</th></tr></thead><tbody>` +
    (bk.rules.length ? bk.rules.map((r) => `<tr><td><b>${esc(r.a)}</b></td><td><b>${esc(r.b)}</b></td><td class="num">${int(r.together)}</td><td class="num">${pct(r.support, 2)}</td>
      <td class="num">${pct(r.confidence_a_to_b, 0)}</td><td class="num">${pct(r.confidence_b_to_a, 0)}</td><td><span class="chip g">${r.lift.toFixed(1)}×</span><div class="bar"><i style="width:${Math.min(100, r.lift / bk.rules[0].lift * 100)}%"></i></div></td></tr>`).join("")
      : `<tr><td colspan="7" style="color:var(--muted)">Not enough multi-item orders in this selection.</td></tr>`) + "</tbody>";
}

/* =================================================================== PREDICT */
async function loadPredict() {
  const [fc, an] = await Promise.allSettled([api("forecast"), api("anomalies")]);
  if (fc.status === "fulfilled") drawForecast(fc.value);
  else { toast(fc.reason.message, true); if (charts["ch-forecast"]) charts["ch-forecast"].destroy(); $("#fc-stats").innerHTML = `<div class="note">${esc(fc.reason.message)}</div>`; }
  if (an.status === "fulfilled") drawAnomalies(an.value);
}
function drawForecast(f) {
  const H = f.history, F = f.forecast, labels = [...H, ...F].map((r) => mlabel(r.period));
  const nH = H.length, pad = (a, n, front) => front ? [...Array(n).fill(null), ...a] : [...a, ...Array(n).fill(null)];
  const lastActual = H[nH - 1].actual;
  chart("ch-forecast", { type: "line", data: { labels, datasets: [
      { label: "Upper 95%", data: pad([lastActual, ...F.map((r) => r.upper)], nH - 1, true), borderColor: "transparent", backgroundColor: alpha(C.cyan, .16), pointRadius: 0, fill: "+1", order: 5 },
      { label: "Lower 95%", data: pad([lastActual, ...F.map((r) => r.lower)], nH - 1, true), borderColor: "transparent", pointRadius: 0, fill: false, order: 5 },
      { label: "Actual revenue", data: pad(H.map((r) => r.actual), F.length), borderColor: C.indigo, backgroundColor: C.indigo, borderWidth: 2.6, pointRadius: 2, tension: .25, order: 1 },
      { label: "Model fit", data: pad(H.map((r) => r.fitted), F.length), borderColor: C.slate, borderDash: [4, 4], borderWidth: 1.5, pointRadius: 0, tension: .25, order: 3 },
      { label: "Forecast", data: pad([lastActual, ...F.map((r) => r.value)], nH - 1, true), borderColor: C.cyan, borderWidth: 2.8, borderDash: [7, 5], pointRadius: 3, tension: .25, order: 2 }] },
    options: { scales: { y: axisMoney, x: { grid: { display: false }, ticks: { maxTicksLimit: 14 } } }, plugins: { legend: { labels: { filter: (i) => !i.text.includes("Lower") } }, tooltip: { callbacks: { label: (c) => c.parsed.y == null ? null : `${c.dataset.label}: ${inr(c.parsed.y)}` } } } } });
  $("#fc-model").textContent = f.model;
  const s = f.stats, peak = f.seasonality.reduce((a, b) => (b.index > a.index ? b : a));
  $("#fc-stats").innerHTML = [["Yearly growth", pct(s.annual_growth_pct), "trend component"], ["Accuracy", pct(1 - s.mape), `MAPE ${pct(s.mape)} in-sample`],
    ["Next 6 months", inr(s.horizon_total), "expected revenue"], ["Peak month", MONTHS[peak.month - 1], `${peak.index.toFixed(2)}× average`]]
    .map(([t, v, d]) => `<div class="tile"><div class="t">${t}</div><div class="v">${v}</div><div class="d">${d}</div></div>`).join("");
  chart("ch-season", { type: "bar", data: { labels: MONTHS, datasets: [{ data: f.seasonality.map((x) => x.index), backgroundColor: f.seasonality.map((x) => (x.index >= 1 ? C.cyan : alpha(C.slate, .9))), borderRadius: 4 }] },
    options: { scales: { y: { min: 0.6, ticks: { callback: (v) => v.toFixed(1) + "×" } }, x: { grid: { display: false } } }, plugins: { legend: { display: false }, title: { display: true, text: "Seasonal index (1.0 = average month)", color: "#8e99ba", font: { size: 11, weight: "normal" } }, tooltip: { callbacks: { label: (c) => `${c.parsed.y.toFixed(2)}× average` } } } } });
}
function drawAnomalies(a) {
  const S = a.series;
  $("#an-count").className = "chip " + (a.stats.flagged ? "a" : "g");
  $("#an-count").textContent = `${a.stats.flagged || 0} unusual days · ${a.stats.spikes || 0} spikes · ${a.stats.drops || 0} drops`;
  $("#an-method").textContent = a.method || "";
  chart("ch-anom", { type: "line", data: { labels: S.map((r) => r.date), datasets: [
      { label: "Daily revenue", data: S.map((r) => r.revenue), borderColor: C.indigo, backgroundColor: alpha(C.indigo, .12), fill: true, borderWidth: 1.3, pointRadius: 0, tension: .1, order: 3 },
      { label: "Expected (rolling median)", data: S.map((r) => r.baseline), borderColor: C.slate, borderDash: [5, 4], borderWidth: 1.4, pointRadius: 0, order: 2 },
      { label: "Spike", data: S.map((r) => (r.anomaly && r.z > 0 ? r.revenue : null)), showLine: false, pointRadius: 5, pointBackgroundColor: C.amber, borderColor: C.amber, order: 1 },
      { label: "Drop", data: S.map((r) => (r.anomaly && r.z < 0 ? r.revenue : null)), showLine: false, pointRadius: 6, pointBackgroundColor: C.rose, borderColor: C.rose, order: 1 }] },
    options: { scales: { y: axisMoney, x: { grid: { display: false }, ticks: { maxTicksLimit: 12, callback(v) { const d = this.getLabelForValue(v); return mlabel(d.slice(0, 7)); } } } }, plugins: { tooltip: { callbacks: { label: (c) => c.parsed.y == null ? null : `${c.dataset.label}: ${inr(c.parsed.y)}` } } } } });
  $("#tbl-anom").innerHTML = `<thead><tr><th>Date</th><th>Type</th><th class="num">Revenue</th><th class="num">Expected</th><th class="num">Deviation</th><th class="num">z-score</th><th>Next step</th></tr></thead><tbody>` +
    a.anomalies.map((r) => `<tr><td><b>${r.date}</b></td><td><span class="chip ${r.type === "spike" ? "a" : "r"}">${r.type}</span></td><td class="num">${inr(r.revenue)}</td><td class="num">${inr(r.baseline)}</td>
      <td class="num">${r.deviation_pct >= 0 ? "+" : ""}${(r.deviation_pct * 100).toFixed(0)}%</td><td class="num">${r.z.toFixed(1)}</td>
      <td style="color:var(--muted);font-size:12px">${r.type === "spike" ? "Check campaign / festival effect" : "Check outage, stock-out or payment failure"}</td></tr>`).join("") + "</tbody>";
}

/* =================================================================== CLOUD */
const ZONE_INFO = { "raw/": ["Bronze", "CSV exactly as received", "b"], "curated/": ["Silver", "Clean typed Parquet, partitioned", "g"], "quarantine/": ["Rejected", "Bad rows + reason (audit trail)", "r"], "_meta/": ["Metadata", "Run manifests & checksums", ""] };
async function loadCloud() {
  const d = await getJSON("/api/lake");
  const mxb = Math.max(...d.zones.map((z) => z.bytes));
  $("#lake-sub").textContent = `${d.storage.backend} · bucket ${d.storage.bucket} · ${int(d.total_objects)} objects · ${bytes(d.total_bytes)} · ${d.partitions} partitions`;
  $("#zones").innerHTML = ["raw/", "curated/", "quarantine/", "_meta/"].map((k) => d.zones.find((z) => z.zone === k)).filter(Boolean).map((z) => {
    const [n, desc, cls] = ZONE_INFO[z.zone];
    return `<div class="zone"><div class="zh"><b>${z.zone}</b><span class="chip ${cls}">${n}</span></div><div class="zs">${desc}</div>
      <div class="bar"><i style="width:${Math.max(2, z.bytes / mxb * 100)}%"></i></div><div class="zs">${int(z.objects)} objects · ${bytes(z.bytes)}</div></div>`; }).join("");
  const r = d.latest_run;
  if (r) {
    $("#etl-sub").textContent = `${r.mode} · ${r.files} files · ${r.workers} parallel workers · ${new Date(r.finished_at).toLocaleString()}`;
    $("#etl-tiles").innerHTML = [["Rows in", int(r.rows_in), "raw lines"], ["Clean rows", int(r.rows_out), "to Parquet"], ["Rejected", pct(r.reject_rate, 2), `${int(r.rows_rejected)} rows`], ["Compression", (r.compression_ratio || 0) + "×", `${bytes(r.raw_bytes)} → ${bytes(r.parquet_bytes)}`],
      ["Duration", r.seconds + " s", "wall clock"], ["Throughput", int(r.rows_per_second), "rows / second"], ["Workers", r.workers, "parallel map tasks"], ["Errors", r.errors.length, "failed files"]]
      .map(([t, v, s]) => `<div class="tile"><div class="t">${t}</div><div class="v">${v}</div><div class="d">${s}</div></div>`).join("");
    const reasons = Object.entries(r.reasons);
    chart("ch-reasons", { type: "bar", data: { labels: reasons.map(([k]) => k.replace(/_/g, " ")), datasets: [{ data: reasons.map(([, v]) => v), backgroundColor: C.rose, borderRadius: 5 }] },
      options: { indexAxis: "y", scales: { x: { grid: { color: "rgba(255,255,255,.06)" } }, y: { grid: { display: false } } }, plugins: { legend: { display: false }, title: { display: true, text: "Why rows were rejected (data-quality report)", color: "#8e99ba", font: { size: 11, weight: "normal" } } } } });
  }
  $("#tbl-objects").innerHTML = `<thead><tr><th>Object key</th><th class="num">Size</th><th>Modified</th></tr></thead><tbody>` +
    d.objects.map((o) => `<tr><td>${esc(o.key)}</td><td class="num">${bytes(o.size)}</td><td style="color:var(--muted)">${new Date(o.last_modified).toLocaleString()}</td></tr>`).join("") + "</tbody>";
  $("#tbl-runs").innerHTML = `<thead><tr><th>Run</th><th>Trigger</th><th>Mode</th><th class="num">Files</th><th class="num">Rows in</th><th class="num">Clean</th><th class="num">Rejected</th><th class="num">Compression</th><th class="num">Seconds</th></tr></thead><tbody>` +
    d.history.filter(Boolean).map((h) => `<tr><td><code>${h.run_id}</code></td><td>${esc(h.trigger)}</td><td><span class="chip ${h.mode === "incremental" ? "b" : ""}">${h.mode}</span></td><td class="num">${h.files}</td><td class="num">${int(h.rows_in)}</td><td class="num">${int(h.rows_out)}</td><td class="num">${int(h.rows_rejected)}</td><td class="num">${h.compression_ratio || "—"}×</td><td class="num">${h.seconds}</td></tr>`).join("") + "</tbody>";
}
function buildStaticTables() {
  const nodes = [["Data sources", "POS, website, mobile app and marketplace orders exported as CSV", ["CSV", "REST upload"]],
    ["Cloud object storage", "Durable, scalable bucket organised as a data lake (raw → curated → quarantine)", ["S3 API", "MinIO / AWS S3", "Hive partitions"], true],
    ["ETL pipeline", "Validate, clean, enrich; parallel map tasks; bad rows quarantined", ["Python", "pandas", "multiprocessing"]],
    ["Columnar format", "Snappy-fast, zstd-compressed Parquet partitioned by year / month", ["Parquet", "zstd"]],
    ["Analytics engine", "Vectorised SQL OLAP: window functions, joins, rolling stats, forecasts", ["DuckDB", "SQL", "NumPy"]],
    ["REST API + cache", "Parameterised endpoints, LRU result cache, upload & ingestion", ["Flask", "JSON"]],
    ["Dashboard", "Interactive filters, drill-downs, heat-maps, forecasts, live ingestion", ["Chart.js", "JS"]]];
  $("#arch").innerHTML = nodes.map((n, i) => `<div class="node ${n[3] ? "cloud" : ""}"><div class="n">${i + 1}</div><h4>${n[0]}</h4><p>${n[1]}</p><div class="tech">${n[2].map((t) => `<span class="chip">${t}</span>`).join("")}</div></div>${i < nodes.length - 1 ? '<div class="arrow">→</div>' : ""}`).join("");
  const rows = [["Volume", "2 million+ order lines generated and processed (scales to billions by swapping DuckDB for Spark/Athena — same Parquet files)", "etl/generate_data.py"],
    ["Variety", "Structured CSV in, semi-structured JSON manifests, columnar Parquet out", "storage/, etl/"],
    ["Velocity", "Live ingestion: a new CSV is stored, cleaned and queryable within seconds", "POST /api/ingest"],
    ["Veracity (data quality)", "Rule-based validation, quarantine zone, rejection report, checksum-based idempotency", "etl/pipeline.py"],
    ["Data lake / medallion zones", "raw (bronze) → curated (silver) with Hive partitioning year= / month=", "Cloud Data Lake tab"],
    ["MapReduce paradigm", "Map: one worker per file cleans it in parallel. Reduce: statistics merged into one run report", "run_pipeline()"],
    ["Columnar storage & compression", "Parquet + zstd gives ~3× smaller than CSV; queries read only the columns they need", "ETL report"],
    ["Partition pruning", "Queries on a date range open only the matching year=/month= folders", "analytics/engine.py"],
    ["Cloud object storage (IaaS)", "S3-compatible API; local emulation, MinIO container or real AWS S3 by changing env vars", "storage/object_store.py"],
    ["Scalability & elasticity", "Stateless API containers behind a load balancer; storage and compute scale independently", "Dockerfile, docker-compose.yml"],
    ["Descriptive analytics", "KPIs, trends, drill-downs, cohort retention", "Overview tab"],
    ["Diagnostic analytics", "Region × category pivot, heat-maps, channel mix", "Overview tab"],
    ["Predictive analytics", "Seasonal growth forecast with prediction interval", "Forecast tab"],
    ["Prescriptive analytics", "RFM playbook, market-basket rules → bundles and campaigns", "Customers tab"],
    ["Anomaly detection", "Robust statistics (median/MAD) on weekday-adjusted daily sales", "Forecast tab"],
    ["Caching & performance", "LRU API cache + sub-second multi-threaded columnar scans", "app.py"],
    ["Security basics", "Parameterised SQL, whitelist validation, upload size / type limit, path-traversal guard", "app.py, storage/"]];
  $("#tbl-concepts").innerHTML = `<thead><tr><th>Concept</th><th>How this project demonstrates it</th><th>Where</th></tr></thead><tbody>` +
    rows.map(([a, b, c]) => `<tr><td><b>${a}</b></td><td style="color:var(--muted)">${b}</td><td><code>${c}</code></td></tr>`).join("") + "</tbody>";
}

/* ---------------- ingestion ---------------- */
function setupIngest() {
  const file = $("#file"), drop = $("#drop"), btn = $("#btn-ingest");
  const pick = (f) => { if (!f) return; file._f = f; $("#drop-name").textContent = `${f.name} · ${bytes(f.size)}`; btn.disabled = false; };
  file.onchange = () => pick(file.files[0]);
  ["dragover", "dragenter"].forEach((e) => drop.addEventListener(e, (ev) => { ev.preventDefault(); drop.classList.add("over"); }));
  ["dragleave", "drop"].forEach((e) => drop.addEventListener(e, (ev) => { ev.preventDefault(); drop.classList.remove("over"); }));
  drop.addEventListener("drop", (ev) => pick(ev.dataTransfer.files[0]));
  btn.onclick = async () => {
    const steps = $("#ingest-steps"); btn.disabled = true;
    const set = (i, cls, txt) => { const li = steps.children[i] || steps.appendChild(document.createElement("li")); li.className = cls; li.textContent = txt; };
    steps.innerHTML = ""; set(0, "run", "Uploading file to cloud storage (raw/uploads/) …");
    const fd = new FormData(); fd.append("file", file._f);
    try {
      const r = await fetch("/api/ingest", { method: "POST", body: fd }), j = await r.json();
      if (!r.ok) throw new Error(j.error || "Upload failed");
      const rep = j.report;
      set(0, "ok", `Stored in bucket as ${j.object_key}`);
      set(1, "ok", `ETL validated ${int(rep.rows_in)} rows → ${int(rep.rows_out)} clean, ${int(rep.rows_rejected)} quarantined (${Object.entries(rep.reasons).map(([k, v]) => `${k.replace(/_/g, " ")}: ${v}`).join(", ") || "none"})`);
      set(2, "ok", `Written as partitioned Parquet · compression ${rep.compression_ratio}×`);
      set(3, "ok", `Query engine refreshed — lake now holds ${int(j.rows_in_lake)} lines, latest order date ${j.max_date}`);
      set(4, "ok", `Done in ${(j.total_ms / 1000).toFixed(1)} s`);
      sampleSeed++; $("#sample-link").href = `/api/sample.csv?seed=${sampleSeed}`;
      META = await api("meta"); state.to = META.max_date; buildPresets(); syncInputs();
      $("#pill-rows span").textContent = `${(META.rows / 1e6).toFixed(2)}M lines · ${int(META.customers)} customers · ${META.products} products`;
      ["overview", "customers", "predict", "cloud"].forEach((t) => stale.add(t)); await loadTab("cloud");
      toast("Ingestion complete — go to Overview to see the new orders in the charts.");
    } catch (e) { set(steps.children.length ? steps.children.length - 1 : 0, "err", e.message); toast(e.message, true); }
    file.value = ""; file._f = null; $("#drop-name").textContent = "No file selected";
  };
}

init();
