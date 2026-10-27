"""
DataTrust AI agent: answers questions about the data from the gold views and audit tables.

Two modes (set LLM_PROVIDER in .env):
  none       (default, free, offline): a rule-based agent understands the common questions
             (revenue for any period / by day, method, region, segment; missing data source -> target;
             rejected rows; duplicates; settlement reconciliation; pipeline status; trust score; customers).
  anthropic  Claude via the Anthropic API (ANTHROPIC_API_KEY), with the same tools.
  ollama     a free local model via Ollama (OLLAMA_MODEL, e.g. qwen2.5:7b), with the same tools.

In LLM mode the model never sees the database directly: it can only call the tools below
(fixed read-only queries + a SELECT-only, gold-only query tool). Every answer names its source.
"""
import calendar
import json
import os
import re
from datetime import date, timedelta

import data_service as ds

MONTHS = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
MONTHS.update({m.lower(): i for i, m in enumerate(calendar.month_abbr) if m})

EXAMPLES = ["What is the total revenue?", "Revenue last 7 days by day", "Revenue this month by payment method",
            "Revenue on 2026-10-05", "Revenue last month", "Revenue last quarter", "Revenue by region",
            "Is any data missing from source to target?", "Show settlement reconciliation",
            "Why is 2026-10-08 a mismatch?", "Which rows were rejected?", "Were there duplicates?",
            "What is the trust score?", "Did the last pipeline run succeed?", "Top 5 customers this month",
            "Show history of customer C00781"]


# ============================================================================ periods
def _month_range(year: int, month: int):
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def _quarter_range(year: int, q: int):
    start = date(year, 3 * (q - 1) + 1, 1)
    return start, _month_range(year, 3 * q)[1]


def _parse_day(text: str, default_year: int):
    """'2026-10-05', '05-10-2026', '5 oct', 'oct 5', '5 october 2026' -> date"""
    m = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", text)
    if m:
        return date(int(m[1]), int(m[2]), int(m[3]))
    m = re.search(r"\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b", text)
    if m:
        return date(int(m[3]), int(m[2]), int(m[1]))                      # dd-mm-yyyy (India)
    names = "|".join(sorted(MONTHS, key=len, reverse=True))
    m = re.search(rf"\b(\d{{1,2}})(?:st|nd|rd|th)?\s+({names})\b\.?(?:\s+(\d{{4}}))?", text)
    if m:
        return date(int(m[3] or default_year), MONTHS[m[2]], int(m[1]))
    m = re.search(rf"\b({names})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?\b(?:,?\s+(\d{{4}}))?", text)
    if m and int(m[2]) <= 31:
        return date(int(m[3] or default_year), MONTHS[m[1]], int(m[2]))
    return None


def parse_period(question: str):
    """-> (start, end, label). Relative periods count back from ds.as_of_date()."""
    q = question.lower()
    today = ds.as_of_date()
    cov = ds.data_coverage()

    m = re.search(r"(?:between|from)\s+(.+?)\s+(?:and|to|till|until)\s+(.+)", q)
    if m:
        a, b = _parse_day(m[1], today.year), _parse_day(m[2], today.year)
        if a and b:
            return min(a, b), max(a, b), f"{min(a, b)} to {max(a, b)}"
    if "today" in q:
        return today, today, f"today ({today})"
    if "yesterday" in q:
        d = today - timedelta(days=1)
        return d, d, f"yesterday ({d})"
    m = re.search(r"(?:last|past|previous)\s+(\d+)\s+days?", q)
    if m:
        n = int(m[1])
        return today - timedelta(days=n - 1), today, f"last {n} days"
    if re.search(r"\b(this|current)\s+week\b|\bwtd\b", q):
        s = today - timedelta(days=today.weekday())
        return s, today, "this week"
    if re.search(r"\b(last|previous)\s+week\b", q):
        s = today - timedelta(days=today.weekday() + 7)
        return s, s + timedelta(days=6), "last week"
    if re.search(r"\b(this|current)\s+month\b|\bmtd\b|month to date", q):
        return today.replace(day=1), today, f"this month ({calendar.month_name[today.month]} {today.year})"
    if re.search(r"\b(last|previous|prior)\s+month\b", q):
        prev = today.replace(day=1) - timedelta(days=1)
        s, e = _month_range(prev.year, prev.month)
        return s, e, f"last month ({calendar.month_name[prev.month]} {prev.year})"
    qn = (today.month - 1) // 3 + 1
    if re.search(r"\b(this|current)\s+quarter\b|\bqtd\b", q):
        s, _ = _quarter_range(today.year, qn)
        return s, today, f"this quarter (Q{qn} {today.year})"
    if re.search(r"\b(last|previous|prior)\s+quarter\b", q):
        y, pq = (today.year, qn - 1) if qn > 1 else (today.year - 1, 4)
        s, e = _quarter_range(y, pq)
        return s, e, f"last quarter (Q{pq} {y})"
    m = re.search(r"\bq([1-4])(?:\s*(?:fy)?\s*(\d{4}))?\b", q)
    if m:
        y = int(m[2] or today.year)
        s, e = _quarter_range(y, int(m[1]))
        return s, e, f"Q{m[1]} {y}"
    if re.search(r"\b(this|current)\s+year\b|\bytd\b", q):
        return date(today.year, 1, 1), today, f"this year ({today.year})"
    if re.search(r"\b(last|previous)\s+year\b", q):
        return date(today.year - 1, 1, 1), date(today.year - 1, 12, 31), f"last year ({today.year - 1})"
    d = _parse_day(q, today.year)
    if d:
        return d, d, str(d)
    names = "|".join(sorted((k for k in MONTHS if len(k) > 3 or k in ("may",)), key=len, reverse=True))
    m = re.search(rf"\b({names})\b(?:\s+(\d{{4}}))?", q)
    if m:
        y = int(m[2] or today.year)
        s, e = _month_range(y, MONTHS[m[1]])
        return s, e, f"{calendar.month_name[MONTHS[m[1]]]} {y}"
    if cov.get("first_date"):
        return date.fromisoformat(cov["first_date"]), date.fromisoformat(cov["last_date"]), "all loaded dates"
    return today, today, "today"


# ============================================================================ answer helpers
def reply(answer, table=None, chart=None, sources=None, suggestions=None, mode="rules"):
    return {"answer": answer, "table": table, "chart": chart, "sources": sources or [],
            "suggestions": suggestions or [], "mode": mode}


def as_table(data: list, columns: list = None):
    if not data:
        return None
    columns = columns or list(data[0].keys())
    return {"columns": columns, "rows": [[r.get(c) for c in columns] for r in data]}


def coverage_note(start: date, end: date) -> str:
    cov = ds.data_coverage()
    if not cov.get("first_date"):
        return "No data is loaded in gold yet. Run the pipeline first."
    first, last = date.fromisoformat(cov["first_date"]), date.fromisoformat(cov["last_date"])
    if end < first or start > last:
        return (f"No data is loaded for this period. Gold currently has {cov['days']} business date(s): "
                f"{cov['first_date']} to {cov['last_date']}. The answer will fill in automatically once the "
                f"pipeline loads those dates.")
    if start < first or end > last:
        return f"Note: gold only has data from {cov['first_date']} to {cov['last_date']}, so this is a partial period."
    return ""


# ============================================================================ rule-based intents
def answer_revenue(q: str):
    start, end, label = parse_period(q)
    group = ("method" if re.search(r"method|upi|card|wallet|netbanking|mode", q) else
             "region" if "region" in q else
             "segment" if "segment" in q else
             "day" if re.search(r"by day|daily|per day|day wise|day-wise|trend|each day", q) else None)
    if group is None and (end - start).days >= 1:
        group = "day"                                        # a multi-day period always gets the daily trend
    r = ds.revenue(start, end, group)
    t = r["total"]
    note = coverage_note(start, end)
    if not t.get("days"):
        return reply(f"**Revenue for {label}** ({start} to {end}): no data.\n\n{note}",
                     sources=["gold.daily_revenue"], suggestions=["What is the total revenue?", "Revenue by day"])
    text = (f"**Revenue for {label}** ({start} to {end}, {t['days']} business day(s) with data)\n\n"
            f"- Gross revenue (captured payments): **{ds.inr(t['gross'])}** from {int(t['payments']):,} payments\n"
            f"- Refunds: {ds.inr(t['refunds'])}\n"
            f"- Net revenue: **{ds.inr(t['net'])}**\n"
            f"- Orders: {int(t['orders']):,}")
    if note:
        text += f"\n\n{note}"
    mism = [x for x in ds.settlement_recon() if x["recon_status"] == "MISMATCH" and start.isoformat() <= x["business_date"] <= end.isoformat()]
    if mism:
        text += ("\n\n⚠ Trust warning: " + "; ".join(
            f"{x['business_date']} is short by {ds.inr(x['amount_difference'])} vs the gateway settlement" for x in mism)
            + ". Ask *why is " + mism[0]["business_date"] + " a mismatch?* for the root cause.")
    chart = table = None
    if r["breakdown"]:
        table = as_table(r["breakdown"], ["label", "amount", "payments"])
        table["columns"] = [group, "revenue", "payments"]
        chart = {"type": "bar", "labels": [b["label"] for b in r["breakdown"]],
                 "values": [b["amount"] for b in r["breakdown"]], "label": f"Revenue by {group}"}
    return reply(text, table, chart, [r["source"]], ["Revenue by payment method", "Revenue by region"])


def answer_top_customers(q: str):
    start, end, label = parse_period(q)
    m = re.search(r"top\s+(\d+)", q)
    data = ds.top_customers(start, end, int(m[1]) if m else 10)
    if not data:
        return reply(f"No captured payments for {label}. {coverage_note(start, end)}", sources=["silver.payments"])
    return reply(f"**Top {len(data)} customers by captured revenue, {label}** ({start} to {end})",
                 as_table(data), {"type": "bar", "labels": [d["customer_id"] for d in data],
                                  "values": [d["amount"] for d in data], "label": "Revenue"},
                 ["silver.payments", "gold.customer_current"])


def answer_missing(q: str):
    lin = ds.lineage()
    ents = lin["entities"]
    lost = [e for e in ents if e["source_to_bronze_gap"] or e["unexplained_loss"]]
    held = [e for e in ents if e["rejected"] or e["duplicates"]]
    recon = lin["settlement"]
    mism = [r for r in recon if r["recon_status"] == "MISMATCH"]
    lines = [f"**Source → target check for the latest batch {lin['batch_id']}** ({lin['business_date']})\n"]
    if lost:
        lines.append("❌ **Rows missing without explanation:** " + "; ".join(
            f"{e['entity']} (source {e['source_rows']}, bronze {e['bronze_rows']}, silver {e['silver_loaded']})" for e in lost))
    else:
        lines.append("✅ **No rows were lost.** Every source row reached bronze, and every bronze row is either "
                     "in silver or explained (rejected by a rule, or a duplicate).")
    if held:
        lines.append("\n**Held back on purpose:**")
        for e in held:
            parts = []
            if e["rejected"]:
                parts.append(f"{e['rejected']} rejected by quality rules")
            if e["duplicates"]:
                parts.append(f"{e['duplicates']} duplicates removed")
            lines.append(f"- {e['entity']}: {', '.join(parts)}")
    if mism:
        lines.append("\n**Against the external target (payment gateway settlement):**")
        for r in mism:
            lines.append(f"- {r['business_date']}: gateway settled {r['settled_payments']:,} payments, silver has "
                         f"{r['silver_payments']:,} → **{r['payment_difference']} payments / {ds.inr(r['amount_difference'])} "
                         f"missing in revenue** ({r['rejected_payments']} payments rejected in silver that day)")
    else:
        lines.append("\n✅ Settlement reconciliation: every date matches.")
    table = as_table(ents, ["entity", "source_rows", "bronze_rows", "silver_loaded", "rejected", "duplicates",
                            "unexplained_loss", "status"])
    return reply("\n".join(lines), table, None,
                 ["audit.step_log", "bronze.*", "silver.rejected_records", "gold.settlement_reconciliation"],
                 ["Why is " + mism[0]["business_date"] + " a mismatch?" if mism else "Show settlement reconciliation",
                  "Which rows were rejected?"])


def answer_recon(q: str):
    d = _parse_day(q, ds.as_of_date().year)
    data = ds.settlement_recon(d.isoformat() if d else None)
    if re.search(r"\bwhy\b|root cause|reason|explain|investigat", q):
        mism = [r for r in data if r["recon_status"] == "MISMATCH"] or \
               [r for r in ds.settlement_recon() if r["recon_status"] == "MISMATCH"]
        if mism:
            return answer_investigate(f"RECON-{mism[0]['business_date']}")
    bad = [r for r in data if r["recon_status"] == "MISMATCH"]
    text = (f"**Settlement reconciliation** (gateway settlement vs silver captured payments): "
            f"{len(data) - len(bad)} of {len(data)} date(s) MATCH.")
    for r in bad:
        text += (f"\n\n❌ {r['business_date']}: settled {r['settled_payments']:,} / {ds.inr(r['settled_amount'])}, "
                 f"silver {r['silver_payments']:,} / {ds.inr(r['silver_amount'])} → difference "
                 f"{r['payment_difference']} payments, {ds.inr(r['amount_difference'])}")
    return reply(text, as_table(data), None, ["gold.settlement_reconciliation"],
                 [f"Why is {bad[0]['business_date']} a mismatch?"] if bad else [])


def answer_investigate(incident_id: str):
    inv = ds.investigate(incident_id)
    if not inv["found"]:
        return reply("That incident is no longer open: the latest data does not show it.")
    lines = [f"**{inv['incident']['title']}**", "", f"**Root cause** (confidence {inv['confidence']}%): {inv['root_cause']}",
             "", "**Evidence:**"]
    lines += [f"{'✅' if c['ok'] else '⚠'} {c['text']}  _(source: {c['source']})_" for c in inv["checks"]]
    imp = inv["impact"]
    if imp["gold_views"]:
        lines += ["", f"**Impact:** gold views {', '.join(imp['gold_views'])}"
                  + (f"; revenue at risk {ds.inr(imp['amount_at_risk'])}" if imp["amount_at_risk"] else "")]
    if imp["actions"]:
        lines += ["", "**Recommended:** " + " → ".join(imp["actions"])]
    return reply("\n".join(lines), as_table(inv["sample"]), None,
                 ["gold.settlement_reconciliation", "silver.rejected_records", "information_schema.view_table_usage"])


def answer_rejected(q: str):
    data = ds.rows("""SELECT entity, rule_name, business_date::text AS business_date, COUNT(*) AS rows_rejected,
                             string_agg(record_key, ', ' ORDER BY record_key) AS record_keys
                      FROM silver.rejected_records GROUP BY 1, 2, 3 ORDER BY 1, 3""")
    if not data:
        return reply("✅ No rows are rejected: every row passed the silver quality rules.",
                     sources=["silver.rejected_records"])
    total = sum(r["rows_rejected"] for r in data)
    text = f"**{total} row(s) rejected by silver quality rules** (kept in silver.rejected_records, not in gold):\n"
    text += "\n".join(f"- {r['rows_rejected']} {r['entity']} on {r['business_date']}: rule **{r['rule_name']}**" for r in data)
    return reply(text, as_table(data), None, ["silver.rejected_records"], ["Were there duplicates?"])


def answer_duplicates(q: str):
    ents = [e for e in ds.lineage()["entities"] if e["duplicates"]]
    if not ents:
        return reply("✅ No duplicates in the latest batch.", sources=["audit.step_log"])
    text = "**Duplicates removed in silver (latest batch):**\n" + "\n".join(
        f"- {e['entity']}: {e['duplicates']} duplicate row(s) in {e['source_file']}; silver kept the latest copy, "
        f"so revenue is not double counted" for e in ents)
    return reply(text, as_table(ents, ["entity", "source_file", "bronze_rows", "duplicates", "silver_loaded"]),
                 None, ["audit.step_log", "bronze.*"])


def answer_trust(q: str):
    t = ds.trust_score()
    d = t["dimensions"]
    text = (f"**Trust score: {t['score']} / 100 — {t['label']}**\n\n"
            f"- Completeness {d['completeness']}% · Validity {d['validity']}% · Uniqueness {d['uniqueness']}% · "
            f"Reconciliation {d['reconciliation']}% · Last successful run {d['freshness_hours']} h ago\n")
    if t["reasons"]:
        text += "\n**Why not 100:**\n" + "\n".join(f"- −{r['points']}: {r['reason']}" for r in t["reasons"])
    return reply(text, as_table(t["trend"]), {"type": "line", "labels": [x["business_date"] for x in t["trend"]],
                                              "values": [x["score"] for x in t["trend"]], "label": "Trust score"},
                 ["gold.settlement_reconciliation", "silver.rejected_records", "audit.step_log"])


def answer_pipeline(q: str):
    s = ds.pipeline_status()
    b = s["batch"]
    if not b:
        return reply("No pipeline run found yet.")
    layers = ", ".join(f"{l['layer']} {l['status']}" for l in s["layers"])
    failed = [x for x in s["steps"] if x["status"] in ("FAILED", "CANCELLED")]
    text = f"**Latest batch {b['batch_id']}** ({b['business_date'] or 'ALL DATES'}, {b['load_mode']}) finished {b['ended_at']}: {layers}."
    if failed:
        text += "\n\n❌ **Failed step:** " + "; ".join(f"{x['pipeline_id']}: {x['message']}" for x in failed)
    elif s["steps"]:
        text += f"\n\n✅ All {len(s['steps'])} steps succeeded or were skipped by gating."
    errs = [e for e in s["errors"] if e["status"] in ("FAILED", "CANCELLED")][:5]
    if re.search(r"error|fail", q) and errs:
        text += "\n\n**Latest errors:**\n" + "\n".join(f"- {e['logged_at']} {e['pipeline_id']}: {e['error_message'][:150]}" for e in errs)
    return reply(text, as_table(s["steps"], ["layer", "pipeline_id", "status", "rows_in", "rows_out", "rows_rejected",
                                             "duration_sec", "message"]) if s["steps"] else as_table(s["history"]),
                 None, ["audit.batch_control", "audit.step_log", "audit.error_log"])


def answer_incidents(q: str):
    inc = ds.incidents()
    if not inc:
        return reply("✅ No open incidents. The data is healthy.")
    text = f"**{len(inc)} open issue(s):**\n" + "\n".join(f"- **{i['severity']}** {i['title']}" for i in inc)
    return reply(text, as_table([{"severity": i["severity"], "id": i["id"], "title": i["title"]} for i in inc]),
                 None, ["DataTrust incident detection"])


def answer_orders(q: str):
    start, end, label = parse_period(q)
    data = ds.rows("""SELECT business_date::text AS business_date, total_orders, completed_orders, cancelled_orders,
                             captured_payments, failed_payments
                      FROM gold.daily_revenue WHERE business_date BETWEEN %(s)s AND %(e)s ORDER BY 1""",
                   {"s": start, "e": end})
    if not data:
        return reply(f"No orders for {label}. {coverage_note(start, end)}", sources=["gold.daily_revenue"])
    tot = {k: sum(r[k] for r in data) for k in ("total_orders", "completed_orders", "cancelled_orders",
                                                "captured_payments", "failed_payments")}
    rate = 100.0 * tot["cancelled_orders"] / max(tot["total_orders"], 1)
    text = (f"**Orders, {label}** ({start} to {end}): {tot['total_orders']:,} orders, {tot['completed_orders']:,} "
            f"completed, {tot['cancelled_orders']:,} cancelled ({rate:.1f}%). Payments: {tot['captured_payments']:,} "
            f"captured, {tot['failed_payments']:,} failed.")
    return reply(text, as_table(data), {"type": "bar", "labels": [r["business_date"] for r in data],
                                        "values": [r["total_orders"] for r in data], "label": "Orders"},
                 ["gold.daily_revenue"])


def answer_customer(customer_id: str):
    data = ds.customer_history(customer_id)
    if not data:
        return reply(f"Customer {customer_id.upper()} is not in silver.customers.")
    cur = next((r for r in data if r["is_current"]), data[-1])
    text = (f"**{cur['customer_name']} ({cur['customer_id']})**: {len(data)} version(s) in the SCD2 history. "
            f"Now: {cur['city']}, {cur['state']} ({cur['region']}), segment {cur['segment']}.")
    return reply(text, as_table(data), None, ["silver.customers (SCD2)"])


def answer_products(q: str):
    data = ds.rows("""SELECT product_id, title, price, category_name, price_since FROM gold.product_catalog
                      ORDER BY price DESC LIMIT 15""")
    return reply(f"**Product catalog** (current versions, most expensive first): {len(data)} shown.",
                 as_table(data), None, ["gold.product_catalog"])


def rules_agent(question: str) -> dict:
    q = question.lower().strip()
    m = re.search(r"\bc\d{5}\b", q)
    if m:
        return answer_customer(m[0])
    if re.fullmatch(r"(hi|hello|hey|help|\?|what can you do\??)", q) or "example" in q:
        return reply("I answer questions from your gold data. Try one of these:", suggestions=EXAMPLES)
    if re.search(r"trust|health|score", q):
        return answer_trust(q)
    if re.search(r"why|root cause|investigat|explain", q) and re.search(r"mismatch|gap|recon|settle|missing|short|\d{4}-\d{2}-\d{2}", q):
        return answer_recon(q + " why")
    if re.search(r"missing|source to target|source vs target|source-to-target|lost|gap|complete|lineage|drop", q):
        return answer_missing(q)
    if re.search(r"recon|settle|mismatch|match", q):
        return answer_recon(q)
    if re.search(r"duplicat|dupe", q):
        return answer_duplicates(q)
    if re.search(r"reject|quality|invalid|bad row|dq\b|rule", q):
        return answer_rejected(q)
    if re.search(r"pipeline|batch|last run|job|step|error|fail|load status|ran\b|succeed", q):
        return answer_pipeline(q)
    if re.search(r"incident|issue|problem|risk|alert|anomal", q):
        return answer_incidents(q)
    if re.search(r"top\s*\d*\s*customer|best customer|biggest customer", q):
        return answer_top_customers(q)
    if re.search(r"product|catalog|price", q):
        return answer_products(q)
    if re.search(r"order|cancel", q) and not re.search(r"revenue|sales", q):
        return answer_orders(q)
    if re.search(r"revenue|sales|gmv|income|earn|turnover|net|refund|payment|amount|how much|collect|business", q):
        return answer_revenue(q)
    return reply("I could not match that question to the data I know. Try one of these "
                 "(or set LLM_PROVIDER in .env for free-form questions):", suggestions=EXAMPLES)


# ============================================================================ LLM agent (optional)
TOOLS = [
    ("get_revenue", "Revenue (gross, refunds, net, payments, orders) for a date range, optionally grouped.",
     {"start_date": {"type": "string", "description": "YYYY-MM-DD"}, "end_date": {"type": "string"},
      "group_by": {"type": "string", "enum": ["day", "method", "region", "segment"]}}, ["start_date", "end_date"]),
    ("get_source_to_target", "Rows at every hop source file -> bronze -> silver (rejected, duplicates, loss) for "
     "the latest batch, plus settlement reconciliation. Use for missing data questions.", {}, []),
    ("get_settlement_reconciliation", "Payment gateway settlement vs silver per business date.",
     {"business_date": {"type": "string"}}, []),
    ("get_incidents", "Open data trust incidents (mismatches, rejected rows, duplicates, failed steps).", {}, []),
    ("investigate_incident", "Root cause, evidence and impact for one incident id from get_incidents.",
     {"incident_id": {"type": "string"}}, ["incident_id"]),
    ("get_trust_score", "Trust score 0-100 with the reasons and daily trend.", {}, []),
    ("get_pipeline_status", "Latest batch, layer status, every step, latest errors.", {}, []),
    ("get_top_customers", "Top customers by captured revenue in a date range.",
     {"start_date": {"type": "string"}, "end_date": {"type": "string"}, "limit": {"type": "integer"}},
     ["start_date", "end_date"]),
    ("get_customer_history", "SCD2 history of one customer.", {"customer_id": {"type": "string"}}, ["customer_id"]),
    ("query_gold", "Run ONE read-only SELECT on gold views only: gold.daily_revenue, gold.payment_method_summary, "
     "gold.settlement_reconciliation, gold.customer_current, gold.revenue_by_segment, gold.product_catalog, "
     "gold.pipeline_status, gold.pipeline_errors, gold.pipeline_steps.", {"sql": {"type": "string"}}, ["sql"]),
]


def run_tool(name: str, args: dict):
    d = lambda k: date.fromisoformat(args[k])                                  # noqa: E731
    if name == "get_revenue":
        return ds.revenue(d("start_date"), d("end_date"), args.get("group_by"))
    if name == "get_source_to_target":
        return ds.lineage()
    if name == "get_settlement_reconciliation":
        return ds.settlement_recon(args.get("business_date"))
    if name == "get_incidents":
        return [{k: i[k] for k in ("id", "severity", "type", "title", "summary")} for i in ds.incidents()]
    if name == "investigate_incident":
        return ds.investigate(args["incident_id"])
    if name == "get_trust_score":
        return ds.trust_score()
    if name == "get_pipeline_status":
        s = ds.pipeline_status()
        return {"batch": s["batch"], "layers": s["layers"], "steps": s["steps"], "errors": s["errors"][:10]}
    if name == "get_top_customers":
        return ds.top_customers(d("start_date"), d("end_date"), int(args.get("limit", 10)))
    if name == "get_customer_history":
        return ds.customer_history(args["customer_id"])
    if name == "query_gold":
        return ds.query_gold(args["sql"])
    raise ValueError(f"unknown tool {name}")


def _system_prompt():
    cov = ds.data_coverage()
    return (f"You are DataTrust AI, a data trust assistant for a payments business (currency INR). "
            f"Answer ONLY from tool results, never invent numbers. Data in gold covers {cov.get('first_date')} to "
            f"{cov.get('last_date')}. Relative periods (last month, last quarter, last 7 days) count back from "
            f"{ds.as_of_date()}. If a period has no data, say so and state the available range. Format money in "
            f"Indian style (₹18,32,333.70). Be short: a bold headline number, then 2-5 bullets. Name the source view. "
            f"If revenue is asked for a date with a settlement MISMATCH, warn about it.")


def _table_from(result):
    if isinstance(result, list) and result and isinstance(result[0], dict):
        return as_table(result)
    if isinstance(result, dict):
        for key in ("breakdown", "entities", "rows", "steps", "sample"):
            v = result.get(key)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return as_table(v)
    return None


def llm_agent(question: str, history: list) -> dict:
    import requests
    provider = os.getenv("LLM_PROVIDER", "none").lower()
    used, last_table = [], None
    messages = [m for m in history[-6:] if m.get("role") in ("user", "assistant")] + [{"role": "user", "content": question}]

    for _ in range(6):                                           # tool loop
        if provider == "anthropic":
            resp = requests.post("https://api.anthropic.com/v1/messages", timeout=60, headers={
                "x-api-key": os.environ["ANTHROPIC_API_KEY"], "anthropic-version": "2023-06-01",
                "content-type": "application/json"}, json={
                "model": os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5-5"), "max_tokens": 1200,
                "system": _system_prompt(), "messages": messages,
                "tools": [{"name": n, "description": desc,
                           "input_schema": {"type": "object", "properties": p, "required": r}} for n, desc, p, r in TOOLS]})
            resp.raise_for_status()
            body = resp.json()
            calls = [b for b in body["content"] if b["type"] == "tool_use"]
            if not calls:
                text = "".join(b.get("text", "") for b in body["content"] if b["type"] == "text")
                return reply(text, last_table, None, used, mode="anthropic")
            messages.append({"role": "assistant", "content": body["content"]})
            results = []
            for c in calls:
                out = _safe_tool(c["name"], c["input"])
                used.append(c["name"])
                last_table = _table_from(out) or last_table
                results.append({"type": "tool_result", "tool_use_id": c["id"],
                                "content": json.dumps(out, default=str)[:20000]})
            messages.append({"role": "user", "content": results})
        else:                                                    # ollama (free, local)
            resp = requests.post(os.getenv("OLLAMA_URL", "http://localhost:11434") + "/api/chat", timeout=180, json={
                "model": os.getenv("OLLAMA_MODEL", "qwen2.5:7b"), "stream": False,
                "messages": [{"role": "system", "content": _system_prompt()}] + messages,
                "tools": [{"type": "function", "function": {"name": n, "description": desc, "parameters":
                           {"type": "object", "properties": p, "required": r}}} for n, desc, p, r in TOOLS]})
            resp.raise_for_status()
            msg = resp.json()["message"]
            calls = msg.get("tool_calls") or []
            if not calls:
                return reply(msg.get("content", ""), last_table, None, used, mode="ollama")
            messages.append(msg)
            for c in calls:
                fn = c["function"]
                args = fn["arguments"] if isinstance(fn["arguments"], dict) else json.loads(fn["arguments"] or "{}")
                out = _safe_tool(fn["name"], args)
                used.append(fn["name"])
                last_table = _table_from(out) or last_table
                messages.append({"role": "tool", "content": json.dumps(out, default=str)[:20000]})
    return reply("I needed too many steps for that question. Please ask it more specifically.", last_table, None, used)


def _safe_tool(name, args):
    try:
        return run_tool(name, args or {})
    except Exception as e:                                        # the model sees the error and can retry
        return {"error": f"{type(e).__name__}: {e}"}


# ============================================================================ entry point
def ask(question: str, history: list = None) -> dict:
    if os.getenv("LLM_PROVIDER", "none").lower() in ("anthropic", "ollama"):
        try:
            return llm_agent(question, history or [])
        except Exception as e:
            result = rules_agent(question)
            result["answer"] += f"\n\n_(AI model unavailable: {type(e).__name__}; answered by the built-in agent.)_"
            return result
    return rules_agent(question)
