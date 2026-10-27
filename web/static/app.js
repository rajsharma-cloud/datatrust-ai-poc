// DataTrust AI web page. All data comes from /api/* (PostgreSQL gold + audit), nothing is hard-coded.
const state = { page: "observe", version: null, overview: null, incidents: null, charts: {}, history: [] };
const $ = (id) => document.getElementById(id);

// ------------------------------------------------------------------ helpers
async function api(path, options) {
  const r = await fetch(path, options);
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || body.detail || r.statusText);
  return body;
}
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const num = (v) => (v == null ? "–" : Number(v).toLocaleString("en-IN"));
const inr = (v) => (v == null ? "–" : "₹" + Number(v).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
const inrShort = (v) => {
  const n = Number(v || 0);
  if (Math.abs(n) >= 1e7) return "₹" + (n / 1e7).toFixed(2) + " Cr";
  if (Math.abs(n) >= 1e5) return "₹" + (n / 1e5).toFixed(2) + " L";
  return inr(n);
};
const tone = (score) => (score >= 90 ? "ok" : score >= 75 ? "warn" : "bad");
const color = { ok: "#0f7b6c", warn: "#b26b12", bad: "#c3312b" };

function showError(msg) {
  const box = $("errorBox");
  if (!msg) return box.classList.add("hidden");
  box.textContent = "Could not load data: " + msg + "  (is PostgreSQL running and has the pipeline been run?)";
  box.classList.remove("hidden");
}
function toast(msg) {
  const t = $("toast");
  t.textContent = msg;
  t.classList.remove("hidden");
  clearTimeout(t._h);
  t._h = setTimeout(() => t.classList.add("hidden"), 4000);
}

// simple markdown: **bold**, _italic_, *italic*, "- " bullets, blank line = paragraph
function md(text) {
  const lines = esc(text).split("\n");
  let html = "", inList = false;
  for (const raw of lines) {
    let l = raw.replace(/\*\*(.+?)\*\*/g, "<b>$1</b>").replace(/(^|\s)_(.+?)_(?=\s|$|[.,)])/g, "$1<i>$2</i>")
      .replace(/(^|\s)\*(\S.+?)\*(?=\s|$|[.,)])/g, "$1<i>$2</i>");
    if (/^\s*- /.test(l)) {
      if (!inList) { html += "<ul>"; inList = true; }
      html += "<li>" + l.replace(/^\s*- /, "") + "</li>";
      continue;
    }
    if (inList) { html += "</ul>"; inList = false; }
    html += l.trim() ? "<p>" + l + "</p>" : "";
  }
  return html + (inList ? "</ul>" : "");
}

const MONEY = /amount|revenue|gross|net|refund|price|difference|value/i;
function tableHtml(t, maxRows = 500) {
  if (!t || !t.rows || !t.rows.length) return "";
  const money = t.columns.map((c) => MONEY.test(c) && !/payments|count|rows/i.test(c));
  const head = t.columns.map((c) => `<th>${esc(c)}</th>`).join("");
  const body = t.rows.slice(0, maxRows).map((r) => "<tr>" + r.map((v, i) => {
    if (money[i] && v !== null && v !== "" && !isNaN(Number(v))) v = Number(v);
    const isNum = typeof v === "number";
    const shown = v === null ? "" : typeof v === "boolean" ? (v ? "yes" : "no") : isNum ? (money[i] ? inr(v) : num(v)) : esc(v);
    return `<td class="${isNum ? "num" : ""}">${shown}</td>`;
  }).join("") + "</tr>").join("");
  return `<div class="tablewrap"><table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}
const objTable = (rows, cols) => tableHtml(rows && rows.length ? { columns: cols || Object.keys(rows[0]), rows: rows.map((r) => (cols || Object.keys(rows[0])).map((c) => r[c])) } : null);

function gauge(el, score, label) {
  const t = tone(score), r = 70, c = 2 * Math.PI * r, filled = (c * score) / 100;
  $(el).innerHTML = `<svg width="190" height="190" viewBox="0 0 190 190" role="img" aria-label="Trust score ${score}">
    <circle cx="95" cy="95" r="${r}" fill="none" stroke="#e1e5ea" stroke-width="18"/>
    <circle cx="95" cy="95" r="${r}" fill="none" stroke="${color[t]}" stroke-width="18" stroke-linecap="round"
      stroke-dasharray="${filled} ${c}" transform="rotate(-90 95 95)"/>
    <text x="95" y="102" text-anchor="middle" font-size="46" font-weight="800" fill="#18212d">${score}</text>
    <text x="95" y="128" text-anchor="middle" font-size="13" fill="#66707c">/ 100</text></svg>`;
}

function drawChart(id, type, labels, values, label, opts = {}) {
  if (!window.Chart) return;
  if (state.charts[id]) state.charts[id].destroy();
  const ctx = typeof id === "string" ? $(id) : id;
  if (!ctx) return;
  const pointColors = opts.pointColors || "#2f5d99";
  state.charts[id] = new Chart(ctx, {
    type,
    data: { labels, datasets: [{ label, data: values, borderColor: "#2f5d99", backgroundColor: type === "bar" ? "#2f5d99" : "rgba(47,93,153,.08)",
      pointBackgroundColor: pointColors, pointBorderColor: pointColors, pointRadius: type === "line" ? 4 : 0, tension: 0.15, fill: type === "line" }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false },
      tooltip: { callbacks: { label: (c) => (opts.money ? inr(c.parsed.y) : num(c.parsed.y)) } } },
      scales: { y: { min: opts.min, max: opts.max, ticks: { callback: (v) => (opts.money ? inrShort(v) : v) } } } },
  });
}

// ------------------------------------------------------------------ navigation
document.querySelectorAll("#tabs button").forEach((b) => b.addEventListener("click", () => go(b.dataset.page)));
function go(page, arg) {
  state.page = page;
  document.querySelectorAll("#tabs button").forEach((b) => b.classList.toggle("active", b.dataset.page === page));
  document.querySelectorAll(".page").forEach((p) => p.classList.add("hidden"));
  $("page-" + page).classList.remove("hidden");
  render(page, arg).catch((e) => showError(e.message));
  if (page === "ask") setTimeout(() => $("askInput").focus(), 50);
}

async function render(page, arg) {
  if (page === "observe") return renderObserve();
  if (page === "detect") return renderDetect();
  if (page === "investigate") return renderInvestigate(arg);
  if (page === "lineage") return renderLineage();
  if (page === "impact") return renderImpact(arg);
  if (page === "verify") return renderVerify();
  if (page === "pipeline") return renderPipeline();
  if (page === "ask") return renderAsk();
}

// ------------------------------------------------------------------ data loading + auto refresh
async function loadCore() {
  const [ov, inc] = await Promise.all([api("/api/overview"), api("/api/incidents")]);
  state.overview = ov;
  state.incidents = inc;
  const t = ov.trust;
  $("topScore").textContent = `Trust score ${t.score} · ${t.label}`;
  showError(null);
}

async function checkVersion() {
  try {
    const v = await api("/api/version");
    const first = state.version === null;
    if (v.signature !== state.version) {
      state.version = v.signature;
      await loadCore();
      if (!first) toast("New data detected: page refreshed");
      if (state.page !== "ask") await render(state.page);
    }
    $("refreshInfo").textContent = "last pipeline run " + (v.last_run || "–").slice(0, 16) + " · checked " + new Date().toLocaleTimeString();
  } catch (e) {
    showError(e.message);
  }
}

// ------------------------------------------------------------------ 1. observe
function renderObserve() {
  const { overview: o, trust: t, pipeline: p } = state.overview;
  const cov = o.coverage || {};
  $("obsTitle").textContent = "1. Observe: " + ({ Healthy: "the estate is healthy", "Needs attention": "the estate needs attention", "At risk": "the estate is at risk" }[t.label] || t.label);
  $("obsCoverage").textContent = cov.first_date ? `Data from ${cov.first_date} to ${cov.last_date} (${cov.days} business dates).` : "";
  gauge("gauge", t.score);
  $("gaugeLabel").innerHTML = `<span class="${tone(t.score)}">${t.label}</span>`;
  drawChart("trendChart", "line", t.trend.map((x) => x.business_date), t.trend.map((x) => x.score), "Trust score",
    { min: 50, max: 100, pointColors: t.trend.map((x) => color[tone(x.score)]) });
  const assets = o.assets.reduce((s, a) => s + a.n, 0);
  const kpis = [
    ["Data assets", assets, ""], ["Gross revenue", inrShort(o.totals.gross), ""], ["Open incidents", o.incidents.open, o.incidents.high ? "bad" : o.incidents.open ? "warn" : ""],
    ["Revenue gaps", o.incidents.money, o.incidents.money ? "bad" : ""], ["Quality issues", o.incidents.quality, o.incidents.quality ? "warn" : ""],
    ["Last run", (p.layers.every((l) => l.status === "SUCCESS") && p.layers.length) ? "SUCCESS" : "CHECK", p.layers.every((l) => l.status === "SUCCESS") ? "" : "bad"],
  ];
  $("obsKpis").innerHTML = kpis.map(([l, v, c]) => `<div class="kpi ${c}"><div class="l">${l}</div><div class="v">${typeof v === "number" ? num(v) : esc(v)}</div></div>`).join("");
  const d = t.dimensions;
  const bars = [["Completeness (source → silver)", d.completeness], ["Validity (passed rules)", d.validity],
    ["Uniqueness (no duplicates)", d.uniqueness], ["Reconciliation (settlement match)", d.reconciliation]];
  $("dims").innerHTML = bars.map(([l, v]) => `<div class="bar"><div class="t"><span>${l}</span><b>${v ?? "–"}%</b></div>
    <div class="track"><div class="fill" style="width:${v || 0}%;background:${color[tone(v)]}"></div></div></div>`).join("") +
    `<div class="small muted">Last successful run: ${d.freshness_hours ?? "–"} h ago</div>`;
  $("reasons").innerHTML = t.reasons.length ? "<p><b>Why not 100:</b> " + t.reasons.map((r) => `−${r.points} ${esc(r.reason)}`).join(" · ") + "</p>" : "";
  drawChart("revChart", "bar", o.daily.map((x) => x.business_date), o.daily.map((x) => x.gross_revenue), "Gross revenue", { money: true });
  const sch = Object.fromEntries(o.assets.map((a) => [a.schema, a.n]));
  const st = (s) => (s === "SUCCESS" ? "ok" : s ? "bad" : "grey");
  const lay = Object.fromEntries(p.layers.map((l) => [l.layer, l.status]));
  $("estate").innerHTML = [
    ["Source files", "local / ADLS", "ok", "Connected"],
    ["Bronze", `${sch.bronze || 0} tables`, st(lay.BRONZE), lay.BRONZE || "no run"],
    ["Silver", `${sch.silver || 0} tables`, st(lay.SILVER), lay.SILVER || "no run"],
    ["Gold", `${sch.gold || 0} views`, st(lay.GOLD), lay.GOLD || "no run"],
    ["AI agent", state.overview.llm === "none" ? "built-in (offline)" : state.overview.llm, "ok", "Ready"],
  ].map(([n, d2, c, s]) => `<div class="item"><div>${n} <span class="small muted">${d2}</span></div><span class="chip ${c}">${esc(s)}</span></div>`).join("");
}

// ------------------------------------------------------------------ 2. detect
function incidentRow(i) {
  const appr = i.approval ? `<span class="chip warn">remediation approved by ${esc(i.approval.approved_by)}</span>` : "";
  return `<div class="incident"><span class="sev ${i.severity}">${i.severity}</span>
    <div class="body"><div class="t">${esc(i.title)} ${appr}</div><div class="small muted">${esc(i.summary)}</div></div>
    <button class="btn light" onclick="go('investigate','${i.id}')">Investigate</button></div>`;
}
function renderDetect() {
  const t = state.overview.trust, open = state.incidents.open;
  const top = open[0];
  $("detectBanner").innerHTML = top
    ? `<div class="banner bad"><h2>${top.severity} RISK DETECTED: ${esc(top.title)}</h2><div>${esc(top.summary)}</div></div>`
    : `<div class="banner ok"><h2>NO OPEN INCIDENTS</h2><div>Every check passed on the latest data.</div></div>`;
  gauge("gauge2", t.score);
  $("gaugeLabel2").innerHTML = `<span class="${tone(t.score)}">${t.label}</span>`;
  drawChart("trendChart2", "line", t.trend.map((x) => x.business_date), t.trend.map((x) => x.score), "Trust score",
    { min: 50, max: 100, pointColors: t.trend.map((x) => color[tone(x.score)]) });
  $("incidentList").innerHTML = open.length ? open.map(incidentRow).join("") : "<p class='muted'>Nothing open.</p>";
}

// ------------------------------------------------------------------ 3. investigate
function fillSelect(sel, chosen) {
  const open = state.incidents.open;
  $(sel).innerHTML = open.map((i) => `<option value="${i.id}" ${i.id === chosen ? "selected" : ""}>${i.severity} · ${esc(i.title)}</option>`).join("");
  return chosen && open.some((i) => i.id === chosen) ? chosen : open[0] && open[0].id;
}
$("invSelect").addEventListener("change", (e) => renderInvestigate(e.target.value));
async function renderInvestigate(id) {
  id = fillSelect("invSelect", id || state.selected);
  state.selected = id;
  if (!id) { $("invBody").innerHTML = "<div class='banner ok'><h2>Nothing to investigate</h2>No open incidents.</div>"; return; }
  $("invBody").innerHTML = "<p class='muted'>Running checks…</p>";
  const inv = await api("/api/investigate/" + encodeURIComponent(id));
  if (!inv.found) { $("invBody").innerHTML = "<p>That incident is no longer open.</p>"; return; }
  $("invBody").innerHTML = `
    <div class="grid g-2">
      <div class="card"><h3>What happened?</h3><div class="big">${esc(inv.incident.title)}</div><div class="muted">${esc(inv.incident.summary)}</div></div>
      <div class="card"><h3>Root cause</h3><div class="big">${esc(inv.root_cause)}</div><div class="muted">Confidence ${inv.confidence}% (all evidence checks agree = high)</div></div>
    </div>
    <div class="grid g-2">
      <div class="card"><h3>Why does DataTrust believe this?</h3>${inv.checks.map((c) => `<div class="check"><span class="${c.ok ? "ok" : "bad"}">${c.ok ? "✓" : "✗"}</span> ${esc(c.text)}<div class="src">${esc(c.source)}</div></div>`).join("")}</div>
      <div class="card"><h3>Evidence chain</h3><ul class="chain">${inv.chain.map((c) => `<li>${esc(c)}</li>`).join("")}</ul></div>
    </div>
    ${inv.sample.length ? `<div class="card"><h3>Affected records (${inv.sample.length} shown)</h3>${objTable(inv.sample)}</div>` : ""}
    <button class="btn" onclick="go('impact','${id}')">See impact and decide →</button>`;
}

// ------------------------------------------------------------------ 4. lineage
async function renderLineage() {
  const lin = await api("/api/lineage");
  $("linSub").textContent = `Latest batch ${lin.batch_id} (${lin.business_date}). Gold views come from the PostgreSQL catalog (information_schema.view_table_usage).`;
  const node = (name, count, sub, cls) => `<div class="node ${cls}"><div class="n">${esc(name)}</div><div class="c">${count}</div><div class="s">${sub}</div></div>`;
  const arrow = (txt) => `<div class="arrow">${txt || ""}</div>`;
  const rows = lin.entities.map((e) => {
    const srcCls = e.source_to_bronze_gap ? "bad" : "";
    const silCls = e.unexplained_loss ? "bad" : e.rejected || e.duplicates ? "warn" : "";
    const held = [e.rejected ? `${e.rejected} rejected` : "", e.duplicates ? `${e.duplicates} duplicates` : "", e.scd2_unchanged ? `${e.scd2_unchanged} unchanged (SCD2)` : ""].filter(Boolean).join(", ");
    return `<div class="card"><h3>${esc(e.entity)} <span class="chip ${e.status === "OK" ? "ok" : e.status === "MISSING" ? "bad" : "warn"}">${e.status.replace("_", " ")}</span></h3>
      <div class="flow">
        ${node(e.source_file, e.source_rows == null ? "n/a" : num(e.source_rows) + " rows", "source", srcCls)}
        ${arrow(e.source_to_bronze_gap ? `−${e.source_to_bronze_gap}` : "")}
        ${node(e.bronze_table, num(e.bronze_rows) + " rows", "landed as delivered", srcCls)}
        ${arrow(e.rejected || e.duplicates ? "held back" : "")}
        ${node(e.silver_table, num(e.silver_loaded) + " loaded", held || "clean", silCls)}
        ${arrow()}
        ${node("gold", e.gold_views.length + " view(s)", e.gold_views.join(", ") || "–", silCls)}
      </div></div>`;
  }).join("");
  const recon = lin.settlement;
  const bad = recon.filter((r) => r.recon_status === "MISMATCH");
  $("linBody").innerHTML = `
    <div class="banner ${bad.length ? "bad" : "ok"}"><h2>External target: payment gateway settlement</h2>
      ${bad.length ? bad.map((r) => `${r.business_date}: gateway settled ${num(r.settled_payments)} payments, silver has ${num(r.silver_payments)} → ${r.payment_difference} payments / ${inr(r.amount_difference)} short`).join("<br>") : "All dates match."}</div>
    ${rows}
    <div class="card"><h3>Settlement reconciliation by date</h3>${objTable(recon)}</div>`;
}

// ------------------------------------------------------------------ 5. impact
$("impSelect").addEventListener("change", (e) => renderImpact(e.target.value));
async function renderImpact(id) {
  id = fillSelect("impSelect", id || state.selected);
  state.selected = id;
  if (!id) { $("impBody").innerHTML = "<div class='banner ok'><h2>Nothing to decide</h2>No open incidents.</div>"; return; }
  const inv = await api("/api/investigate/" + encodeURIComponent(id));
  if (!inv.found) { $("impBody").innerHTML = "<p>That incident is no longer open.</p>"; return; }
  const im = inv.impact, inc = inv.incident;
  const tiles = [["Gold views affected", im.gold_views.length], ["Business date", im.business_date || "–"],
    ["Rows affected", inc.metrics.payment_difference ?? inc.metrics.n ?? inc.metrics.duplicates ?? "–"],
    ["Revenue at risk", im.amount_at_risk != null ? inr(im.amount_at_risk) : "–"]];
  $("impBody").innerHTML = `
    <div class="kpis" style="grid-template-columns:repeat(4,1fr)">${tiles.map(([l, v], i) => `<div class="kpi ${i === 3 && im.amount_at_risk ? "bad" : ""}"><div class="l">${l}</div><div class="v">${esc(typeof v === "number" ? num(v) : v)}</div></div>`).join("")}</div>
    <div class="grid g-2">
      <div class="card"><h3>What it touches</h3>
        <p><b>${esc(inc.title)}</b></p>
        <p>Gold views reading <code>${esc(im.entity || "")}</code>: ${im.gold_views.map((v) => `<span class="chip warn">gold.${esc(v)}</span>`).join(" ") || "–"}</p>
        <p class="small muted">Revenue at risk is the exact amount of the gap in the data, not an estimate. Reports built on the views above show the reduced numbers until the fix is loaded.</p></div>
      <div class="card"><h3>Recommended remediation</h3>
        <div class="actions">${im.actions.map((a) => `<label><input type="checkbox" checked value="${esc(a)}"> ${esc(a)}</label>`).join("")}</div>
        <div class="field"><input id="approver" placeholder="Approved by (your name)"></div>
        ${inc.approval ? `<p class="small">Already approved by <b>${esc(inc.approval.approved_by)}</b> at ${esc(inc.approval.approved_at)}. Waiting for the next pipeline run to verify.</p>` : ""}
        <button class="btn" id="approveBtn" style="width:100%">Approve remediation</button></div>
    </div>`;
  $("approveBtn").onclick = async () => {
    const actions = [...document.querySelectorAll(".actions input:checked")].map((x) => x.value);
    const by = $("approver").value.trim() || "web user";
    $("approveBtn").disabled = true;
    try {
      await api("/api/approve", { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ incident_id: id, title: inc.title, actions, approved_by: by }) });
      toast("Approved and logged in audit.remediation_log");
      await loadCore();
      go("verify");
    } catch (e) { showError(e.message); $("approveBtn").disabled = false; }
  };
}

// ------------------------------------------------------------------ 6. verify
async function renderVerify() {
  const { open, resolved } = state.incidents;
  const t = state.overview.trust, p = state.overview.pipeline;
  const waiting = open.filter((i) => i.approval);
  $("verifyBody").innerHTML = `
    ${resolved.length ? `<div class="banner ok"><h2>INCIDENT(S) RESOLVED</h2>${resolved.map((r) => `✓ ${esc(r.incident_title)} (approved by ${esc(r.approved_by)}): no longer present in the latest data.`).join("<br>")}</div>` : ""}
    <div class="grid g-2">
      <div class="card"><h3>Remediation log</h3>
        ${waiting.map((i) => `<div class="check"><span class="warn">⏳</span> <b>${esc(i.title)}</b><div class="small muted">Approved by ${esc(i.approval.approved_by)} at ${esc(i.approval.approved_at)}. Still present in the data: fix the source and re-run the pipeline.</div></div>`).join("")}
        ${resolved.map((r) => `<div class="check"><span class="ok">✓</span> <b>${esc(r.incident_title)}</b><div class="small muted">${esc(r.actions)}</div></div>`).join("")}
        ${!waiting.length && !resolved.length ? "<p class='muted'>No remediation approved yet. Approve one on page 5.</p>" : ""}
      </div>
      <div class="card center"><div id="gauge3"></div><div class="gauge-label"><span class="${tone(t.score)}">${t.label}</span></div>
        <div class="small muted">Open incidents ${open.length} · Latest run ${esc(p.batch.batch_id || "–")}: ${p.layers.map((l) => l.layer + " " + l.status).join(", ")}</div></div>
    </div>`;
  gauge("gauge3", t.score);
}

// ------------------------------------------------------------------ pipeline log
async function renderPipeline() {
  const p = await api("/api/pipeline");
  $("pipeBody").innerHTML = `
    <div class="card"><h3>Latest batch ${esc(p.batch.batch_id || "–")}: every step</h3>${objTable(p.steps, ["layer", "pipeline_id", "status", "rows_in", "rows_out", "rows_rejected", "duration_sec", "message"]) || "<p class='muted'>No step log yet (run --setup and the pipeline after the step-log update).</p>"}</div>
    <div class="card"><h3>Recent batches</h3>${objTable(p.history)}</div>
    <div class="card"><h3>Error log (failures and warnings)</h3>${objTable(p.errors, ["logged_at", "batch_id", "layer", "pipeline_id", "status", "error_type", "error_message"]) || "<p class='muted'>Empty.</p>"}</div>`;
}

// ------------------------------------------------------------------ ask
const EXAMPLES = ["What is the total revenue?", "Revenue last 7 days by day", "Revenue this month by payment method",
  "Revenue last month", "Revenue last quarter", "Revenue by region", "Is any data missing from source to target?",
  "Why is 2026-10-08 a mismatch?", "Which rows were rejected?", "What is the trust score?",
  "Did the last pipeline run succeed?", "Top 5 customers this month", "Show history of customer C00781"];
function renderAsk() {
  const mode = state.overview ? state.overview.llm : "none";
  $("askMode").textContent = mode === "none" ? "Mode: built-in agent (free, offline)." : `Mode: ${mode} model with DataTrust tools.`;
  $("examples").innerHTML = EXAMPLES.map((q) => `<button>${esc(q)}</button>`).join("");
  $("examples").querySelectorAll("button").forEach((b) => (b.onclick = () => askQuestion(b.textContent)));
  if (!$("chatLog").children.length) addBot({ answer: "Hi! Ask me anything about your revenue, data quality, missing data or pipeline runs. The answers come from the gold layer and refresh after every pipeline run.", suggestions: EXAMPLES.slice(0, 4) });
}
$("askForm").addEventListener("submit", (e) => { e.preventDefault(); const q = $("askInput").value.trim(); if (q) askQuestion(q); });

function addUser(q) {
  $("chatLog").insertAdjacentHTML("beforeend", `<div class="msg user"><div class="bubble">${esc(q)}</div></div>`);
}
let chartSeq = 0;
function addBot(r) {
  const id = "askChart" + ++chartSeq;
  const sugg = (r.suggestions || []).map((s) => `<button>${esc(s)}</button>`).join("");
  const html = `<div class="msg bot"><div class="bubble">${md(r.answer || "")}
      ${r.chart ? `<div class="chartbox"><canvas id="${id}"></canvas></div>` : ""}
      ${tableHtml(r.table, 200)}
      ${r.sources && r.sources.length ? `<div class="meta">Source: ${r.sources.map(esc).join(", ")}${r.mode && r.mode !== "rules" ? " · answered by " + esc(r.mode) : ""}</div>` : ""}
      ${sugg ? `<div class="sugg">${sugg}</div>` : ""}</div></div>`;
  $("chatLog").insertAdjacentHTML("beforeend", html);
  const last = $("chatLog").lastElementChild;
  last.querySelectorAll(".sugg button").forEach((b) => (b.onclick = () => askQuestion(b.textContent)));
  if (r.chart) {
    const money = /revenue/i.test(r.chart.label);
    drawChart(id, r.chart.type, r.chart.labels, r.chart.values, r.chart.label,
      r.chart.label === "Trust score" ? { min: 50, max: 100 } : { money });
  }
  $("chatLog").scrollTop = $("chatLog").scrollHeight;
}
async function askQuestion(q) {
  if (state.page !== "ask") go("ask");
  $("askInput").value = "";
  addUser(q);
  $("chatLog").insertAdjacentHTML("beforeend", `<div class="msg bot typing" id="typing">checking the data…</div>`);
  $("chatLog").scrollTop = $("chatLog").scrollHeight;
  try {
    const r = await api("/api/ask", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question: q, history: state.history }) });
    $("typing").remove();
    addBot(r);
    state.history.push({ role: "user", content: q }, { role: "assistant", content: r.answer });
  } catch (e) {
    $("typing").remove();
    addBot({ answer: "Sorry, that failed: " + e.message });
  }
}

// ------------------------------------------------------------------ start
(async function start() {
  await checkVersion();                      // loads the data and draws page 1
  setInterval(checkVersion, 15000);          // auto refresh when the pipeline writes new data
})();
