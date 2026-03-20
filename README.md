# SQL-Driven Analytics Dashboard

<div align="center">

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-DirectQuery-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![asyncpg](https://img.shields.io/badge/asyncpg-0.29-5C6BC0?style=for-the-badge)
![pytest](https://img.shields.io/badge/pytest-16_tests-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)

**End-to-end analytics pipeline: PostgreSQL stored procedures → FastAPI → live web dashboard + Power BI**

</div>

---

## Dashboard Preview

![Dashboard Preview](docs/images/dashboard_preview.svg)

> **4 pages:** Executive Overview · Product Performance · Customer LTV · Campaign ROI  
> Dark-mode, Chart.js 4, single HTML file — no build step required. Falls back to mock data when API is offline.

---

## Architecture

![Architecture Diagram](docs/images/architecture.svg)

---

## Database Schema (ERD)

![Entity Relationship Diagram](docs/images/erd.svg)

> **3NF normalised** — `order_items` snapshots `unit_price` + `cost_price` at purchase time so historical revenue is immutable even when product prices change.

---

## Query Execution Flow

![Query Flow](docs/images/query_flow.svg)

---

## Repository Structure

```
sql-analytics-dashboard/
│
├── sql/
│   ├── 01_schema.sql          # Tables, indexes, views, stored functions
│   ├── 02_indexes.sql         # Covering & partial indexes
│   └── 03_maintenance.sql     # Monitoring views, pg_cron scheduling
│
├── api/
│   ├── main.py                # FastAPI app — all 10 endpoints
│   ├── database.py            # asyncpg connection pool
│   └── schemas.py             # Pydantic v2 response models
│
├── frontend/
│   └── dashboard.html         # Single-file dark-mode analytics UI (Chart.js 4)
│
├── scripts/
│   ├── seed_data.py           # Faker en_IN data generator (500 customers / 3k orders)
│   └── setup.sh               # One-command bootstrap script
│
├── tests/
│   └── test_api.py            # pytest async test suite (16 tests)
│
├── docs/
│   ├── powerbi_guide.md       # 20+ DAX measures + DirectQuery setup
│   └── images/
│       ├── architecture.svg
│       ├── dashboard_preview.svg
│       ├── erd.svg
│       └── query_flow.svg
│
├── .env.example
├── .gitignore
├── pytest.ini
├── requirements.txt
└── README.md
```

---

## Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL 14+
- _(Optional)_ Power BI Desktop for the BI layer

### One-command setup
```bash
git clone https://github.com/your-username/sql-analytics-dashboard
cd sql-analytics-dashboard
bash scripts/setup.sh
```

### Manual setup
```bash
# 1. Database
psql -U postgres -c "CREATE DATABASE analytics;"
psql -U postgres -d analytics -f sql/01_schema.sql
psql -U postgres -d analytics -f sql/02_indexes.sql
psql -U postgres -d analytics -f sql/03_maintenance.sql

# 2. Python environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Seed data (500 customers, 3 000 orders, 26 products, 15 campaigns)
python scripts/seed_data.py --dsn "postgresql://postgres:postgres@localhost:5432/analytics"

# 4. Start API
uvicorn api.main:app --reload --port 8000
# Swagger UI → http://localhost:8000/docs

# 5. Open dashboard
open frontend/dashboard.html
```

---

## Database Design

### Normalisation — 3NF

| Table | Key design decision |
|-------|---------------------|
| `raw.order_items` | Snapshots `unit_price` + `cost_price` at purchase — historical revenue is immutable |
| `raw.categories` | Self-referencing `parent_id` supports sub-category hierarchies |
| `raw.order_attribution` | Many-to-many bridge between orders and campaigns |
| `raw.regions` | Extracted from customers to avoid repeating country strings |

### Indexing Strategy

| Index | Type | Reason |
|-------|------|--------|
| `idx_orders_date DESC` | B-tree | Time-range scans are the #1 query pattern |
| `idx_orders_active` | **Partial** B-tree | Excludes cancelled/refunded — ~20% smaller index |
| `idx_items_order` + `idx_items_product` | B-tree | JOIN hotpaths for revenue aggregation |
| `idx_customers_region_tier` | Composite | Covers both common filter dimensions in one index |
| `idx_products_category WHERE is_active` | **Partial** | Only live products in the index |
| `idx_order_items_order_product INCLUDE (...)` | **Covering** | Index-only scan for revenue sums — zero heap access |

### Views & Materialised Views

| Object | Type | Description |
|--------|------|-------------|
| `mart.v_daily_revenue` | View | Time-series revenue + profit |
| `mart.v_product_performance` | View | Per-product units, revenue, margin % |
| `mart.v_customer_ltv` | View | LTV, recency, tier, region |
| `mart.v_campaign_roi` | View | Attributed revenue, ROI % |
| `mart.v_channel_mix` | View | Orders + revenue by channel/month |
| `mart.mv_monthly_kpis` | **Materialised** | Pre-aggregated monthly KPIs, refreshed hourly |

### Stored Functions (`api` schema)

```sql
-- Period-over-period KPI comparison
SELECT * FROM api.get_kpi_summary(30);   -- last 30 days vs prior 30

-- Top N products by revenue
SELECT * FROM api.get_top_products(10, 30);

-- Refresh materialised view (called by FastAPI + pg_cron)
CALL api.refresh_marts();
```

---

## API Reference

| Method | Endpoint | Key params | Backed by |
|--------|----------|-----------|-----------|
| `GET` | `/api/v1/kpis` | `days=30` | `api.get_kpi_summary()` |
| `GET` | `/api/v1/revenue/daily` | `days=90` | `mart.v_daily_revenue` |
| `GET` | `/api/v1/revenue/monthly` | `months=12` | `mart.mv_monthly_kpis` |
| `GET` | `/api/v1/products/top` | `limit=10`, `days=30` | `api.get_top_products()` |
| `GET` | `/api/v1/products` | `category`, `min_margin` | `mart.v_product_performance` |
| `GET` | `/api/v1/customers/ltv` | `tier`, `region`, `limit` | `mart.v_customer_ltv` |
| `GET` | `/api/v1/campaigns/roi` | — | `mart.v_campaign_roi` |
| `GET` | `/api/v1/channels` | `months=6` | `mart.v_channel_mix` |
| `POST` | `/api/v1/explain` | `{"query":"SELECT …"}` | `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` |
| `POST` | `/api/v1/admin/refresh-marts` | — | `CALL api.refresh_marts()` |

Interactive Swagger UI: **`http://localhost:8000/docs`**

---

## Power BI Integration

See [`docs/powerbi_guide.md`](docs/powerbi_guide.md) for the full guide. Highlights:

**Connection:** DirectQuery on `mart.*` views — data stays live with no import lag.

**DAX Measures (20+):**
```dax
Revenue MoM %   = DIVIDE([Total Revenue] - [Revenue LM], [Revenue LM], BLANK())
Revenue YTD     = CALCULATE([Total Revenue], DATESYTD(DateTable[Date]))
YoY Growth %    = DIVIDE([Revenue YTD] - [Revenue PY YTD], [Revenue PY YTD], BLANK())
ROAS            = DIVIDE(SUM(attributed_revenue), SUM(spend), 0)
Retention Rate  = DIVIDE(COUNTROWS(INTERSECT(thisMonth, lastMonth)), COUNTROWS(lastMonth), 0)
```

**4 Report Pages:** Executive Overview · Product Deep-Dive · Customer Segmentation · Campaign ROI

---

## Running Tests

```bash
pytest tests/ -v
```

```
tests/test_api.py::test_root                        PASSED
tests/test_api.py::test_health                      PASSED
tests/test_api.py::test_kpis_default                PASSED
tests/test_api.py::test_kpis_custom_window          PASSED
tests/test_api.py::test_kpis_invalid_days           PASSED
tests/test_api.py::test_daily_revenue_structure     PASSED
tests/test_api.py::test_monthly_kpis                PASSED
tests/test_api.py::test_top_products_default        PASSED
tests/test_api.py::test_top_products_limit          PASSED
tests/test_api.py::test_products_category_filter    PASSED
tests/test_api.py::test_customer_ltv_structure      PASSED
tests/test_api.py::test_customer_ltv_tier_filter    PASSED
tests/test_api.py::test_campaign_roi                PASSED
tests/test_api.py::test_channel_mix                 PASSED
tests/test_api.py::test_explain_valid_query         PASSED
tests/test_api.py::test_explain_rejects_non_select  PASSED

16 passed in 2.34s
```

---

## Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Database | PostgreSQL | 16 |
| DB Driver | asyncpg | 0.29 |
| API | FastAPI + uvicorn | 0.111 |
| Validation | Pydantic | v2 |
| Seed data | Faker (`en_IN` locale) | 24.x |
| Frontend | Vanilla JS + Chart.js | 4.4 |
| BI | Power BI Desktop | DirectQuery |
| Tests | pytest + httpx (async) | 8.x |

---

## Resume Bullet Points

> **What this project demonstrates for a CV/portfolio:**

- **SQL**: Normalised 3NF schema design, covering/partial indexes reducing index bloat by ~20%, materialised views with concurrent refresh, stored functions with planner-friendly parameterisation, `EXPLAIN (ANALYZE, BUFFERS)` query-plan debugging endpoint
- **Python**: asyncpg connection pooling, FastAPI dependency injection pattern, Pydantic v2 data validation, async httpx test client
- **Power BI**: DAX measures for period-over-period comparisons (MoM%, YTD, YoY), DirectQuery live connection, custom Date Table with week/quarter columns
- **Engineering decisions**: Separation of `raw` / `mart` / `api` schemas, snapshot pattern for immutable historical pricing, partial-index strategy, security guardrail on EXPLAIN endpoint (SELECT-only, rolled-back transaction)

![alt text](architecture.svg) ![alt text](dashboard_preview.svg) ![alt text](erd.svg) ![alt text](query_flow.svg)