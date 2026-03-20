"""
tests/test_api.py
─────────────────
pytest + httpx async test suite for the Analytics Dashboard API.

Run:
    pytest tests/ -v --asyncio-mode=auto
"""

import pytest
from httpx import AsyncClient, ASGITransport

# Import the FastAPI app
import sys
sys.path.insert(0, "..")
from main import app


# ─── Fixtures ─────────────────────────────────────────────────

@pytest.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as c:
        yield c


# ─── Health & Root ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_root(client):
    r = await client.get("/")
    assert r.status_code == 200
    assert "running" in r.json()["message"].lower()


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


# ─── KPIs ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_kpis_default(client):
    r = await client.get("/api/v1/kpis")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 4
    metrics = {item["metric"] for item in data}
    assert "Total Orders"     in metrics
    assert "Gross Revenue"    in metrics
    assert "Gross Profit"     in metrics
    assert "Unique Customers" in metrics


@pytest.mark.asyncio
async def test_kpis_custom_window(client):
    r = await client.get("/api/v1/kpis?days=7")
    assert r.status_code == 200
    assert len(r.json()) == 4


@pytest.mark.asyncio
async def test_kpis_invalid_days(client):
    r = await client.get("/api/v1/kpis?days=0")
    assert r.status_code == 422    # Pydantic validation error


# ─── Revenue ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_daily_revenue_structure(client):
    r = await client.get("/api/v1/revenue/daily?days=30")
    assert r.status_code == 200
    if r.json():
        day = r.json()[0]
        assert "order_day"       in day
        assert "gross_revenue"   in day
        assert "gross_profit"    in day
        assert "total_orders"    in day


@pytest.mark.asyncio
async def test_monthly_kpis(client):
    r = await client.get("/api/v1/revenue/monthly?months=6")
    assert r.status_code == 200
    data = r.json()
    if data:
        assert "month"    in data[0]
        assert "revenue"  in data[0]
        assert "margin_pct" in data[0]


# ─── Products ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_top_products_default(client):
    r = await client.get("/api/v1/products/top")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) <= 10


@pytest.mark.asyncio
async def test_top_products_limit(client):
    r = await client.get("/api/v1/products/top?limit=5&days=60")
    assert r.status_code == 200
    assert len(r.json()) <= 5


@pytest.mark.asyncio
async def test_products_category_filter(client):
    r = await client.get("/api/v1/products?category=Electronics")
    assert r.status_code == 200
    for p in r.json():
        assert "Electronics" in p["category_name"]


# ─── Customers ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_customer_ltv_structure(client):
    r = await client.get("/api/v1/customers/ltv?limit=10")
    assert r.status_code == 200
    data = r.json()
    assert len(data) <= 10
    if data:
        c = data[0]
        assert "customer_id"    in c
        assert "customer_tier"  in c
        assert "lifetime_value" in c


@pytest.mark.asyncio
async def test_customer_ltv_tier_filter(client):
    r = await client.get("/api/v1/customers/ltv?tier=gold&limit=20")
    assert r.status_code == 200
    for c in r.json():
        assert c["customer_tier"] == "gold"


# ─── Campaigns ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_campaign_roi(client):
    r = await client.get("/api/v1/campaigns/roi")
    assert r.status_code == 200
    data = r.json()
    if data:
        camp = data[0]
        assert "campaign_name"       in camp
        assert "attributed_revenue"  in camp
        assert "roi_pct"             in camp


# ─── Channels ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_channel_mix(client):
    r = await client.get("/api/v1/channels?months=3")
    assert r.status_code == 200
    for row in r.json():
        assert row["channel"] in ("web","mobile","store","partner")


# ─── EXPLAIN endpoint ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_explain_valid_query(client):
    r = await client.post(
        "/api/v1/explain",
        json={"query": "SELECT * FROM mart.v_daily_revenue LIMIT 5"}
    )
    assert r.status_code == 200
    assert "plan" in r.json()


@pytest.mark.asyncio
async def test_explain_rejects_non_select(client):
    r = await client.post(
        "/api/v1/explain",
        json={"query": "DROP TABLE raw.orders"}
    )
    assert r.status_code == 400
    assert "SELECT" in r.json()["detail"]
