"""
api/schemas.py
──────────────
Pydantic v2 response models for all API endpoints.
"""

from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field


class KPIMetric(BaseModel):
    metric:         str
    current_value:  float
    previous_value: float
    pct_change:     float | None


class DailyRevenue(BaseModel):
    order_day:        str
    total_orders:     int
    unique_customers: int
    gross_revenue:    float
    total_cogs:       float
    gross_profit:     float


class MonthlyKPI(BaseModel):
    month:      str
    orders:     int
    customers:  int
    revenue:    float
    profit:     float
    margin_pct: float | None


class ProductPerformance(BaseModel):
    product_name:  str
    category_name: str
    units_sold:    int
    revenue:       float
    margin_pct:    float | None


class CustomerLTV(BaseModel):
    customer_id:           str
    full_name:             str
    email:                 str
    customer_tier:         str
    region_name:           str
    total_orders:          int
    lifetime_value:        float | None
    first_order:           str | None
    last_order:            str | None
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
    channel: str
    month:   str
    orders:  int
    revenue: float


class QueryPlanResponse(BaseModel):
    query: str
    plan:  list[dict[str, Any]]


class ExplainRequest(BaseModel):
    query: str = Field(..., description="SELECT statement to analyse")
