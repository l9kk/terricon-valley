# Intelligent Procurement‑Risk Dataset Builder

**EOZ Kazakhstan – 48‑hour Hackathon Playbook (v1.0 / 2025‑06‑21)**

---

## 1 ▪ Why We’re Doing This

Kazakhstan’s public‑procurement portal **EOZ** exposes structured JSON for every purchase **plan → tender → lot → contract**. Our hackathon goal is to build an AI that flags corruption red‑flags (price inflation, single‑bid awards, contract splitting, buyer–supplier collusion). A clean relational dataset is the critical first step.

---

## 2 ▪ EOZ API Quick Facts

| Item           | Value                                                |
| -------------- | ---------------------------------------------------- |
| Base URL       | `https://www.eoz.kz/api/uicommand/`                  |
| Authentication | **None** for read‑only requests                      |
| Transport      | HTTPS, JSON (UTF‑8)                                  |
| Throttle       | ≈ 5 req/s per IP; retry with 1 s back‑off on 502/504 |
| Pagination     | Body fields `page` (0‑based) & `length` (max 1000)   |

---

## 3 ▪ Endpoint Reference

### 3.1 `/get/page` – list endpoint

```jsonc
{
  "page":0,
  "entity":"<EntityName>",
  "length":1000,
  "filter":{ … }
}
```

Response → keys `content[]`, `totalPages`, `totalElements`.

| Entity (`entity`) | Purpose                                                                                                            | Minimal filter                     |   |                      |                                    |
| ----------------- | ------------------------------------------------------------------------------------------------------------------ | ---------------------------------- | - | -------------------- | ---------------------------------- |
| `Plan`            | Planned purchase lines                                                                                             | `{ "includeMyTru":0 }`             |   |                      |                                    |
| `Tender`          | **Not accessible** – both `/get/page` and `/get/object` return empty/403. Skip scraping; rely on `_Lot` data only. | —                                  |   |                      |                                    |
| `_Lot`            | Lots inside a tender                                                                                               | `{ "tru":null, "includeMyTru":0 }` |   | Lots inside a tender | `{ "tru":null, "includeMyTru":0 }` |
| `OrderDetail`     | Signed contracts                                                                                                   | `{}`                               |   |                      |                                    |

### 3.2 `/get/object` – full object endpoint

```jsonc
{ "entity":"<EntityName>", "uuid":"<externalId>" }
```

`uuid` values are collected from `/get/page` responses.

### 3.3 Cookie header requirement (Plan only)
The /get/page endpoint for entity:"Plan" silently caps results at 1 000 rows unless you include the session cookies that EOZ sets in the browser.
Always add the following header when fetching Plan pages:

Cookie: _ym_uid=1750414057249358088; _ym_d=1750414057; _ym_isad=2;
        _ga=GA1.1.558229306.1750414057; _fbp=fb.1.1750414057029.260535807123590267;
        _ga_FEYYKCHQ9W=GS2.1.s1750476259$o8$g1$t1750476260$j59$l0$h0;
        _ym_visorc=w; JSESSIONID=0FF60479F5CD569AF74A273E0E6997B6

---

## 4 ▪ Data Model

```
Plan (plan_id) 1───n Lot (lot_id) 1───1 OrderDetail  
*(Tender object skipped – not retrievable)*
                       │
                       n
                       │
                Tender (tender_id)
```

Join keys

```
Plan.externalId     → Lot.externalPlanId
Tender.externalId   → Lot.externalTenderId
Lot.externalId      → OrderDetail.externalId   # If null ⇒ fallback composite key
```

Fallback key =

```text
sha256(lot_number + customer_bin + round(contract_sum,0))
```

---

## 5 ▪ Column Set (after page+object merge)

| Entity          | Column                          | JSON path                             | Type     |      |          |              |     |
| --------------- | ------------------------------- | ------------------------------------- | -------- | ---- | -------- | ------------ | --- |
| **Plan**        | `plan_id`                       | `externalId`                          | str      |      |          |              |     |
|                 | `plan_price`                    | `sum`                                 | float    |      |          |              |     |
|                 | `plan_method_id`                | `methodTrade.id`                      | int      |      |          |              |     |
|                 | `plan_customer_bin`             | `customerBin.biniin`                  | str      |      |          |              |     |
| **Lot**         | `lot_id`                        | `externalId`                          | str      | \*\* | `lot_id` | `externalId` | str |
|                 | `plan_id` / `tender_id`         | `externalPlanId` / `externalTenderId` | str      |      |          |              |     |
|                 | `customer_bin`                  | `customerBin.biniin`                  | str      |      |          |              |     |
|                 | `lot_amount`                    | `amount`                              | float    |      |          |              |     |
|                 | `title_ru`                      | `titleRu`                             | str      |      |          |              |     |
|                 | `lot_method_id`                 | `methodTrade.id`                      | int      |      |          |              |     |
|                 | `lot_start_date`                | `startDate`                           | datetime |      |          |              |     |
| **OrderDetail** | `lot_id`                        | `externalId`                          | str      |      |          |              |     |
|                 | `provider_bin` / `customer_bin` | `providerBin` / `customerBin`         | str      |      |          |              |     |
|                 | `contract_sum`                  | `sum`                                 | float    |      |          |              |     |
|                 | `paid_sum`                      | `paidSum`                             | float    |      |          |              |     |
|                 | `accept_date`                   | `acceptDate`                          | datetime |      |          |              |     |
|                 | `order_method_id`               | `methodTrade.id`                      | int      |      |          |              |     |

---

## 6 ▪ End‑to‑End Workflow (2018‑01‑01 → 2025‑06‑21)

| Phase                                            | Tasks                                                                                                                                                                                                                                                                            | Deliverables             | Tech                       |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------ | -------------------------- |
| **0 Prereqs**                                    |  • Confirm period & disk (≈ 5 GB raw) • Set concurrency = 50 • `poetry install`                                                                                                                                                                                                  | `pyproject.toml`, `.env` | —                          |
| **1 Skeleton**                                   |  Create dirs `raw/pages`, `raw/objects`, `bronze`, `dataset`, `logs`, `src`                                                                                                                                                                                                      | Empty tree               | shell                      |
| \*\*2 Downloader \*\***`src/eoz_downloader.py`** |  Async cycle: 1. `fetch_page` → save `raw/pages/<entity>/<page>.json` 2. For each `uuid` queue `fetch_object` → save `raw/objects/<entity>/<uuid>.json` 3. Merge page+object dict; push to queue 4. Retry ×3 on 502/504 (back‑off 1‑3‑10 s) 5. `Semaphore(50)`; show `tqdm` bars | Full raw archive         | `asyncio`, `httpx`, `tqdm` |
| \*\*3 Normalizer \*\***`src/normalizer.py`**     |  `pandas.json_normalize(sep='.')` → prune cols → append Parquet chunks (`bronze/<entity>.parquet`, ZSTD, row‑group 50 k)                                                                                                                                                         | 4 Bronze Parquets        | `pandas / polars`          |
| \*\*4 Joiner \*\***`src/joiner.py`**             |  DuckDB SQL joins (no Tender object):                                                                                                                                                                                                                                            |                          |                            |

```
SELECT o.*, l.*, p.*
FROM parquet_scan('bronze/OrderDetail.parquet') o
JOIN parquet_scan('bronze/_Lot.parquet') l ON o.lot_id = l.lot_id
JOIN parquet_scan('bronze/Plan.parquet')  p ON l.plan_id = p.plan_id;
```

Persist ➜ `dataset/procurements.parquet` | Fact table               | `duckdb`                   |
\| \*\*5 Features \*\***`src/features.py`**         |  Derive flags & `risk_score`; fit **IsolationForest(contamination = 0.05)** → `model.pkl`                                                                                                                                                                                        | Enriched Parquet + model | `scikit‑learn`             |
\| \*\*6 Dashboard \*\***`src/dashboard.py`**       |  Streamlit table with traffic‑light styling + filters + CSV export                                                                                                                                                                                                               | Live demo                | `streamlit`                |
\| **7 Logs & Docs**                            |  Log each action → `agent-logs.md`; keep this doc & README current                                                                                                                                                                                                              | Full history             | —                          |

---

## 7 ▪ Risk‑Flag Rules

| Flag                | Condition                                                   | Output               |
| ------------------- | ----------------------------------------------------------- | -------------------- |
| **Single bidder**   | any `*_method_id == 6` **or** `bidders == 1`                | `single_flag = 1`    |
| **Over‑pricing**    | `price_z > 3` (MAD z‑score)                                 | `price_flag = 1`     |
| **Repeated winner** | `win_rate(customer,provider) > 0.60` & `n ≥ 5`              | `repeat_flag = 1`    |
| **Split purchase**  | ≥ 3 lots, same title+buyer, each ≤ 100 000 ₸ within 30 days | `split_flag = 1`     |
| **Under‑paid**      | `paid_sum < 0.9 × contract_sum`                             | `underpaid_flag = 1` |

```python
risk_score = (
    2*price_flag + 1.5*single_flag + 1.5*repeat_flag + split_flag + underpaid_flag
)
```

---

## 8 ▪ Sample Prompt for an AI Code‑Generator

```text
"Generate a complete Python 3.11 project that:
 • Scrapes EOZ (Plan/Tender/_Lot/OrderDetail) 2018‑01‑01→2025‑06‑21,
 • Saves raw pages & objects, normalises to Parquet, joins with DuckDB,
 • Computes price_z, single/repeat/split/underpaid flags, risk_score,
 • Fits Isolation Forest, stores model.pkl,
 • Provides Streamlit dashboard with filters & traffic‑light table.
Include retries, Semaphore(50), tqdm progress, and pyproject.toml."
```

---

## 9 ▪ Quick‑Start Snippet (minimal)

```python
import asyncio, httpx, pandas as pd, duckdb, json
from pathlib import Path
API = "https://www.eoz.kz/api/uicommand"

def build_payload(entity, page, length=500):
    return {"page": page, "entity": entity, "length": length, "filter": {}}

async def post(client, path, payload):
    r = await client.post(f"{API}{path}", json=payload); r.raise_for_status()
    return r.json()

async def fetch_entity(entity):
    Path(f"raw/pages/{entity}").mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(timeout=30) as c:
        page = 0
        while True:
            data = await post(c, "/get/page", build_payload(entity, page))
            if not data["content"]: break
            Path(f"raw/pages/{entity}/{page}.json").write_text(json.dumps(data))
            df = pd.json_normalize(data["content"], sep=".")
            df.to_parquet(f"bronze/{entity}.parquet", append=True)
            page += 1

asyncio.run(fetch_entity("Plan"))
```

---

## 10 ▪ Streamlit Demo Preview

```python
import streamlit as st, duckdb
st.title("EOZ Procurement‑Risk Monitor")
qry = "SELECT *,
       CASE WHEN risk_score>=3 THEN 'red' WHEN risk_score>=1 THEN 'yellow' ELSE 'green' END AS color
       FROM parquet_scan('dataset/procurements.parquet')"
df = duckdb.query(qry).df()
st.dataframe(df, use_container_width=True)
```

Run → `streamlit run src/dashboard.py`

---