# DataTrust AI web app

A web page on top of your PostgreSQL gold views and audit tables. Every number is a live query;
when the pipeline runs again, the page detects it (checks every 15 s) and refreshes itself.

## Run
```powershell
.venv\Scripts\activate
pip install -r requirements.txt          # adds fastapi, uvicorn, requests
python web\server.py                     # open http://localhost:8000
```
Keep it running in one terminal; run the pipeline in another (`python run_pipeline.py --full` or a date).

## Pages
| Page | What it shows | Where the data comes from |
|---|---|---|
| 1. Observe | Trust score, trend per business date, KPIs, quality bars, revenue chart, layer status | gold.*, audit.batch_control, audit.step_log |
| Ask DataTrust AI | Chat: revenue for any period / by day, method, region, segment; missing data; rejects; duplicates; reconciliation; pipeline; trust score; customers | agent.py tools |
| 2. Detect | Open incidents, most severe first | gold.settlement_reconciliation, silver.rejected_records, audit.step_log |
| 3. Investigate | Root cause, evidence checks (each names its source), evidence chain, affected rows | same, plus bronze / source file counts |
| 4. Explain | Source file → bronze → silver → gold for every entity, with rows held back or lost | source CSV, bronze, audit.step_log, information_schema.view_table_usage |
| 5. Impact & decide | Gold views affected, revenue at risk (exact gap), actions; Approve | audit.remediation_log (created automatically) |
| 6. Verify | Approved incidents become RESOLVED only when the next run no longer shows them | audit.remediation_log + live detection |
| Pipeline log | Steps, batches, errors and warnings | audit.* |

## Trust score (explainable, shown on page 1)
Starts at 100: −15 per business date where settlement ≠ silver (max −45), −up to 10 for rows lost between
source and silver, −up to 10 for rejected rows, −up to 5 for duplicates, −30 if the latest run has a
failed/skipped layer, −10 if no successful run in 24 h. ≥ 90 Healthy, ≥ 75 Needs attention, below At risk.
The rules are constants at the top of data_service.py.

## AI agent modes (.env LLM_PROVIDER)
* `none` (default): built-in agent, free and offline. Understands the questions listed on the Ask page.
* `ollama`: free local model. Install Ollama, `ollama pull qwen2.5:7b`, set LLM_PROVIDER=ollama.
* `anthropic`: Claude via API key (paid). Set LLM_PROVIDER=anthropic and ANTHROPIC_API_KEY.

In the model modes the model can only call fixed read-only tools plus a SELECT-only query on gold.* views
(read-only transaction, 10 s timeout, 200 rows). If the model is unavailable, the built-in agent answers.

## Dates
'Last month', 'last 7 days', 'yesterday' count back from the latest business date in gold
(DATATRUST_AS_OF=data). With the sample data (2026-10-01 to 10-08), 'last month' = September → the agent
says no data is loaded and shows the available range; it fills in automatically once those dates are loaded.
