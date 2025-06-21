## Core Rules

You have two modes of operation:

1. **Plan mode** – collect and refine a plan with the user; **no code changes**.
2. **Act mode** – implement the approved plan and modify the codebase.

* Start in Plan mode; only switch to Act mode when the user types **ACT**.
* Print `# Mode: PLAN` or `# Mode: ACT` at the top of every reply.
* After each Act response, return to Plan mode unless the user says otherwise or types **PLAN**.
* If asked to change code in Plan mode, remind the user to approve the plan first.
* In Plan mode always output the **full, updated plan** each time.

## Agent Logs

All agent actions must be appended to **`agent-logs.md`** so future sessions share full context.

---

description: EOZ Procurement‑Risk Dataset Builder – Project‑wide rules
alwaysApply: true
-----------------

# 🛰️  Project Purpose

Automate data collection from Kazakhstan’s EOZ portal to train an AI that detects procurement‑corruption patterns (price inflation, single‑bid awards, repeated winners, contract splitting).

# 🔌  EOZ API Endpoints (cheatsheet)

| Endpoint                           | Request Body                                                                                        | Notes                                                         |
| ---------------------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| **POST /api/uicommand/get/page**   | `{ "page":0, "entity":"Plan", "length":1000, "filter":{…} }`                                        | Returns list of Plan records                                  |
|                                    | Same body but `entity:"_Lot"` or `"OrderDetail"`                                                    | Lists lots / contracts                                        |
| **Tender endpoints unavailable**   | —                                                                                                   | `/get/page` & `/get/object` for Tender return empty/403; skip |
| **POST /api/uicommand/get/object** | `{ "entity":"_Lot", "uuid":"<externalId>" }` or `{ "entity":"OrderDetail", "uuid":"<externalId>" }` | Returns full object JSON                                      |

Minimal list filters:

```jsonc
// Plan
{ "page":0, "entity":"Plan", "length":1000, "filter":{ "includeMyTru":0 } }
// _Lot
{ "page":0, "entity":"_Lot", "length":1000, "filter":{ "tru":null, "includeMyTru":0 } }
// OrderDetail
{ "page":0, "entity":"OrderDetail", "length":1000, "filter":{} }
```

Key response attributes to keep:

| Entity      | Field → Column                                                                                                                                                     | Notes |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----- |
| Plan        | `externalId → plan_id`, `sum → plan_price`                                                                                                                         |       |
| \_Lot       | `externalId → lot_id`, `externalPlanId → plan_id`, `customerbin`, `amount → lot_amount`, `titleRu`, `methodTrade.id → lot_method_id`, `startDate → lot_start_date` |       |
| OrderDetail | `externalId (lot_id)`, `providerBin`, `customerBin`, `sum → contract_sum`, `acceptDate`, `paidSum`, `methodTrade.id → order_method_id`                             |       |

# 🗄️  Data‑flow

1. **Async download** `/get/page` lists → queue `/get/object` for each `uuid`  (≤ 50 concurrent, retry 1‑3‑10 s).
2. **Raw archive** every JSON to `raw/pages/<entity>/` & `raw/objects/<entity>/`.
3. **Normalise** with `pandas.json_normalize(sep='.')`; write Parquet chunks in `bronze/`.
4. **Join** in DuckDB → `dataset/procurements.parquet` using `plan_id` & `lot_id`  (Tender skipped).
5. **Feature engineering** in `features.py`:

   * `price_z` – MAD z‑score per `title_ru`
   * `single_flag` – `lot_method_id == 6 or order_method_id == 6`
   * `repeat_flag` – win‑rate > 0.6 & n ≥ 5
   * `split_flag` – ≥ 3 lots ≤ 100 000 ₸ within 30 days
   * `underpaid_flag` – `paid_sum < 0.9*contract_sum`
6. **Risk score** = `2*price_flag + 1.5*single_flag + 1.5*repeat_flag + split_flag + underpaid_flag`.
7. **Visualise** with Streamlit (`dashboard.py`): traffic‑light table, filters, CSV export.

# 🧰  Coding Standards

* Python 3.11, PEP‑8, Black, type hints.
* Libraries: `httpx`, `asyncio`, `tqdm.asyncio`, `pandas`/`polars`, `duckdb`, `pyarrow`, `scikit‑learn`, `streamlit`, `structlog`.

# 🗂️  Suggested Module Layout

```
src/
 ├─ eoz_downloader.py      # fetch_page()/fetch_object()
 ├─ normalizer.py          # flatten & select columns
 ├─ joiner.py              # DuckDB merge
 ├─ features.py            # flag calculations
 ├─ risk_model.py          # Isolation Forest
 ├─ dashboard.py           # UI
 └─ __main__.py            # CLI
```

# 🤖  Agent Behaviour Shortcuts

* **“Generate dataset builder”** → scaffold full `src/` tree + `poetry` files.
* **“Update risk rules”** → modify `features.py` & `risk_model.py`.
* **“Explain endpoint X”** → quote payloads/paths, avoid extra requests.

# ✨  Reusable Snippets

```python
def build_payload(entity: str, page: int = 0, length: int = 1000, *, flt: dict | None = None) -> dict:
    return {"page": page, "entity": entity, "length": length, "filter": flt or {}}

BACKOFF = [1, 3, 10]
```
