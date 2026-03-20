"""
main.py  –  Analytics Dashboard API
────────────────────────────────────
FastAPI + asyncpg  |  All routes served under /api/v1/

Start:
    uvicorn main:app --reload --port 8000

Docs:
    http://localhost:8000/docs
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ─── Database pool ────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/analytics"
)

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_pool()          # warm up on startup
    yield
    if _pool:
        await _pool.close()   # graceful shutdown


# ─── App ──────────────────────────────────────────────────────
app = FastAPI(
    title="Analytics Dashboard API",
    version="1.0.0",
    description="Powered by PostgreSQL stored procedures + FastAPI",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # tighten in production
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ─── Pydantic response models ─────────────────────────────────

class KPIMetric(BaseModel):
    metric:         str
    current_value:  float
    previous_value: float
    pct_change:     float | None


class DailyRevenue(BaseModel):
    order_day:       str
    total_orders:    int
    unique_customers: int
    gross_revenue:   float
    total_cogs:      float
    gross_profit:    float


class ProductPerformance(BaseModel):
    product_name:  str
    category_name: str
    units_sold:    int
    revenue:       float
    margin_pct:    float | None


class CustomerLTV(BaseModel):
    customer_id:          str
    full_name:            str
    email:                str
    customer_tier:        str
    region_name:          str
    total_orders:         int
    lifetime_value:       float | None
    first_order:          str | None
    last_order:           str | None
    days_since_last_order: int | None


class CampaignROI(BaseModel):
    campaign_id:        int
    campaign_name:      str
    channel:            str | None
    budget:             float
    spend:              float
    attributed_orders:  int
    attributed_revenue: float
    roi_pct:            float | None


class ChannelMix(BaseModel):
    channel:  str
    month:    str
    orders:   int
    revenue:  float


class MonthlyKPI(BaseModel):
    month:      str
    orders:     int
    customers:  int
    revenue:    float
    profit:     float
    margin_pct: float | None


class QueryPlanResponse(BaseModel):
    query:       str
    plan:        list[dict[str, Any]]


# ─── Helpers ──────────────────────────────────────────────────

def _row_to_dict(row: asyncpg.Record) -> dict:
    return {k: (str(v) if hasattr(v, 'isoformat') else v)
            for k, v in dict(row).items()}


# ─── Routes ───────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"message": "Analytics Dashboard API is running 🚀"}


@app.get("/health")
async def health():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok"}


# ── KPI Summary ───────────────────────────────────────────────
@app.get("/api/v1/kpis", response_model=list[KPIMetric], tags=["KPIs"])
async def get_kpis(
    days: int = Query(default=30, ge=1, le=365,
                      description="Rolling window in days")
):
    """
    Returns 4 KPIs with period-over-period comparison:
    Total Orders, Gross Revenue, Gross Profit, Unique Customers.
    Backed by api.get_kpi_summary() stored function.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM api.get_kpi_summary($1)", days
            )
        return [_row_to_dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Daily Revenue ─────────────────────────────────────────────
@app.get("/api/v1/revenue/daily", response_model=list[DailyRevenue], tags=["Revenue"])
async def get_daily_revenue(
    days: int = Query(default=90, ge=1, le=730)
):
    """Time-series daily revenue from mart.v_daily_revenue."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT order_day::TEXT, total_orders, unique_customers,
                       ROUND(gross_revenue::NUMERIC,2) AS gross_revenue,
                       ROUND(total_cogs::NUMERIC,2)    AS total_cogs,
                       ROUND(gross_profit::NUMERIC,2)  AS gross_profit
                FROM mart.v_daily_revenue
                WHERE order_day >= CURRENT_DATE - ($1 || ' days')::INTERVAL
                ORDER BY order_day
            """, str(days))
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Monthly KPIs (materialised view) ─────────────────────────
@app.get("/api/v1/revenue/monthly", response_model=list[MonthlyKPI], tags=["Revenue"])
async def get_monthly_kpis(
    months: int = Query(default=12, ge=1, le=60)
):
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT month::TEXT, orders, customers,
                       revenue, profit, margin_pct
                FROM mart.mv_monthly_kpis
                WHERE month >= DATE_TRUNC('month', NOW() - ($1 || ' months')::INTERVAL)
                ORDER BY month
            """, str(months))
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Top Products ──────────────────────────────────────────────
@app.get("/api/v1/products/top", response_model=list[ProductPerformance], tags=["Products"])
async def get_top_products(
    limit: int = Query(default=10, ge=1, le=100),
    days:  int = Query(default=30, ge=1, le=365)
):
    """Top N products by revenue, backed by api.get_top_products()."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM api.get_top_products($1, $2)", limit, days
            )
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Product Performance (full) ────────────────────────────────
@app.get("/api/v1/products", response_model=list[ProductPerformance], tags=["Products"])
async def get_all_products(
    category: str | None = Query(default=None),
    min_margin: float    = Query(default=0.0)
):
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT product_name, category_name,
                       units_sold, ROUND(total_revenue::NUMERIC,2) AS revenue,
                       margin_pct
                FROM mart.v_product_performance
                WHERE ($1::TEXT IS NULL OR category_name ILIKE $1)
                  AND COALESCE(margin_pct, 0) >= $2
                ORDER BY revenue DESC
            """, f"%{category}%" if category else None, min_margin)
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Customer LTV ──────────────────────────────────────────────
@app.get("/api/v1/customers/ltv", response_model=list[CustomerLTV], tags=["Customers"])
async def get_customer_ltv(
    tier:   str | None = Query(default=None,
                               description="Filter by tier: standard|silver|gold|platinum"),
    region: str | None = Query(default=None),
    limit:  int        = Query(default=50, ge=1, le=500)
):
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT customer_id::TEXT, full_name, email, customer_tier,
                       region_name, total_orders,
                       ROUND(lifetime_value::NUMERIC, 2) AS lifetime_value,
                       first_order::TEXT, last_order::TEXT,
                       days_since_last_order
                FROM mart.v_customer_ltv
                WHERE ($1::TEXT IS NULL OR customer_tier = $1)
                  AND ($2::TEXT IS NULL OR region_name ILIKE $2)
                ORDER BY lifetime_value DESC NULLS LAST
                LIMIT $3
            """, tier, f"%{region}%" if region else None, limit)
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Campaign ROI ──────────────────────────────────────────────
@app.get("/api/v1/campaigns/roi", response_model=list[CampaignROI], tags=["Campaigns"])
async def get_campaign_roi():
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT campaign_id, campaign_name, channel,
                       budget, spend, attributed_orders,
                       attributed_revenue, roi_pct
                FROM mart.v_campaign_roi
                ORDER BY roi_pct DESC NULLS LAST
            """)
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Channel Mix ───────────────────────────────────────────────
@app.get("/api/v1/channels", response_model=list[ChannelMix], tags=["Channels"])
async def get_channel_mix(
    months: int = Query(default=6, ge=1, le=24)
):
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT channel, month::TEXT,
                       orders, ROUND(revenue::NUMERIC,2) AS revenue
                FROM mart.v_channel_mix
                WHERE month >= DATE_TRUNC('month', NOW() - ($1 || ' months')::INTERVAL)
                ORDER BY month, revenue DESC
            """, str(months))
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Query Plan Analyser (EXPLAIN ANALYZE) ─────────────────────
class ExplainRequest(BaseModel):
    query: str = Field(..., description="SELECT statement to analyse")


@app.post("/api/v1/explain", response_model=QueryPlanResponse, tags=["Dev Tools"])
async def explain_query(body: ExplainRequest):
    """
    Runs EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) on a user-supplied
    SELECT and returns the query plan – useful for latency debugging.
    Read-only: non-SELECT statements are rejected.
    """
    q = body.query.strip()
    if not q.upper().startswith("SELECT"):
        raise HTTPException(status_code=400,
                            detail="Only SELECT statements are allowed.")
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            # Use a rolled-back transaction so EXPLAIN ANALYZE doesn't mutate
            async with conn.transaction():
                plan = await conn.fetchval(
                    f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {q}"
                )
        return {"query": q, "plan": plan}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Refresh Materialised Views ────────────────────────────────
@app.post("/api/v1/admin/refresh-marts", tags=["Admin"])
async def refresh_marts():
    """Manually trigger a materialised-view refresh."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("CALL api.refresh_marts()")
        return {"status": "refreshed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
