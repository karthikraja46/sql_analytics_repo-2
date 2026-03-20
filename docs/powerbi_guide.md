# Power BI Integration Guide
## Analytics Dashboard — Direct Query + DAX Reference

---

## 1. Connect Power BI to PostgreSQL

1. Open Power BI Desktop → **Get Data** → **PostgreSQL database**
2. Enter:
   - **Server**: `localhost` (or your host)
   - **Database**: `analytics`
3. Choose **DirectQuery** (keeps data live; no import lag)
4. Select these views/tables under the `mart` schema:

| Object | Type | Purpose |
|--------|------|---------|
| `mart.v_daily_revenue` | View | Time-series revenue |
| `mart.v_product_performance` | View | Product KPIs |
| `mart.v_customer_ltv` | View | Customer segmentation |
| `mart.v_campaign_roi` | View | Marketing ROI |
| `mart.v_channel_mix` | View | Channel breakdown |
| `mart.mv_monthly_kpis` | Mat. View | Pre-aggregated monthly KPIs |
| `raw.orders` | Table | Raw order header |
| `raw.customers` | Table | Customer master |

---

## 2. Data Model (Relationships)

Set these relationships in the **Model** view:

```
raw.orders[customer_id]  →  raw.customers[customer_id]   (Many-to-One)
mart.v_daily_revenue     ←  Date Table[Date]              (Many-to-One)
mart.mv_monthly_kpis     ←  Date Table[Month]             (Many-to-One)
```

Create a **Date Table** (mark as Date Table):
```dax
DateTable = 
ADDCOLUMNS(
    CALENDAR(DATE(2024,1,1), DATE(2026,12,31)),
    "Year",        YEAR([Date]),
    "MonthNum",    MONTH([Date]),
    "MonthName",   FORMAT([Date], "MMM YYYY"),
    "Quarter",     "Q" & QUARTER([Date]),
    "WeekNum",     WEEKNUM([Date]),
    "DayOfWeek",   FORMAT([Date], "dddd"),
    "IsWeekend",   WEEKDAY([Date], 2) >= 6
)
```

---

## 3. DAX Measures

### 3.1 Core Revenue Measures

```dax
// ── Total Revenue ────────────────────────────────────────────
Total Revenue = 
SUMX(
    raw.order_items,
    raw.order_items[quantity] * raw.order_items[unit_price]
        * (1 - RELATED(raw.orders[discount_pct]) / 100)
)

// ── Gross Profit ─────────────────────────────────────────────
Gross Profit = 
SUMX(
    raw.order_items,
    raw.order_items[quantity]
        * (raw.order_items[unit_price] - raw.order_items[cost_price])
)

// ── Gross Margin % ───────────────────────────────────────────
Gross Margin % = 
DIVIDE([Gross Profit], [Total Revenue], 0)

// ── Average Order Value ──────────────────────────────────────
AOV = 
DIVIDE([Total Revenue], DISTINCTCOUNT(raw.orders[order_id]), 0)
```

### 3.2 Period-over-Period Comparisons

```dax
// ── Revenue LM (Last Month) ──────────────────────────────────
Revenue LM = 
CALCULATE(
    [Total Revenue],
    DATEADD(DateTable[Date], -1, MONTH)
)

// ── Revenue MoM % ────────────────────────────────────────────
Revenue MoM % = 
DIVIDE([Total Revenue] - [Revenue LM], [Revenue LM], BLANK())

// ── Revenue YTD ───────────────────────────────────────────────
Revenue YTD = 
CALCULATE([Total Revenue], DATESYTD(DateTable[Date]))

// ── Revenue PY YTD ────────────────────────────────────────────
Revenue PY YTD = 
CALCULATE([Revenue YTD], SAMEPERIODLASTYEAR(DateTable[Date]))

// ── YoY Growth % ─────────────────────────────────────────────
YoY Growth % = 
DIVIDE([Revenue YTD] - [Revenue PY YTD], [Revenue PY YTD], BLANK())
```

### 3.3 Customer Analytics

```dax
// ── Active Customers (placed order in last 90 days) ──────────
Active Customers = 
CALCULATE(
    DISTINCTCOUNT(raw.orders[customer_id]),
    DATESINPERIOD(DateTable[Date], MAX(DateTable[Date]), -90, DAY)
)

// ── Customer Retention Rate ───────────────────────────────────
Retention Rate = 
VAR CustomersThisMonth =
    CALCULATETABLE(
        VALUES(raw.orders[customer_id]),
        DATESMTD(DateTable[Date])
    )
VAR CustomersLastMonth =
    CALCULATETABLE(
        VALUES(raw.orders[customer_id]),
        DATEADD(DATESMTD(DateTable[Date]), -1, MONTH)
    )
RETURN
    DIVIDE(
        COUNTROWS(INTERSECT(CustomersThisMonth, CustomersLastMonth)),
        COUNTROWS(CustomersLastMonth),
        0
    )

// ── Average LTV ───────────────────────────────────────────────
Avg LTV = 
AVERAGEX(
    SUMMARIZE(
        raw.orders,
        raw.orders[customer_id],
        "cltv", [Total Revenue]
    ),
    [cltv]
)
```

### 3.4 Campaign ROI

```dax
// ── ROAS (Return on Ad Spend) ─────────────────────────────────
ROAS = 
DIVIDE(
    SUMX(mart.v_campaign_roi, mart.v_campaign_roi[attributed_revenue]),
    SUMX(mart.v_campaign_roi, mart.v_campaign_roi[spend]),
    0
)

// ── Campaign ROI % ────────────────────────────────────────────
Campaign ROI % = 
DIVIDE(
    SUMX(mart.v_campaign_roi,
         mart.v_campaign_roi[attributed_revenue] - mart.v_campaign_roi[spend]),
    SUMX(mart.v_campaign_roi, mart.v_campaign_roi[spend]),
    0
)
```

### 3.5 Product Performance

```dax
// ── Top Category by Revenue ───────────────────────────────────
Top Category = 
TOPN(1,
    SUMMARIZE(
        mart.v_product_performance,
        mart.v_product_performance[category_name],
        "rev", SUM(mart.v_product_performance[total_revenue])
    ),
    [rev], DESC
)

// ── % of Total Revenue (for treemap / waterfall) ──────────────
Revenue Share % = 
DIVIDE(
    [Total Revenue],
    CALCULATE([Total Revenue], ALL(raw.products)),
    0
)
```

---

## 4. Recommended Visuals & Pages

### Page 1 – Executive Overview
| Visual | Fields |
|--------|--------|
| Card (4x) | Total Revenue, Gross Margin %, Active Customers, AOV |
| Line Chart | DateTable[Month] × Total Revenue + Revenue LM |
| KPI Card | YoY Growth %, Revenue MoM % |
| Donut | Channel mix |

### Page 2 – Product Deep-Dive
| Visual | Fields |
|--------|--------|
| Bar Chart | product_name × revenue (Top 10 filter) |
| Matrix | category_name × units_sold, revenue, margin_pct |
| Scatter | units_sold (X) × margin_pct (Y), size = revenue |

### Page 3 – Customer Segmentation
| Visual | Fields |
|--------|--------|
| Treemap | customer_tier × lifetime_value |
| Map | region_name → bubble size = revenue |
| Table | Top 20 by LTV with tier badge |

### Page 4 – Campaign ROI
| Visual | Fields |
|--------|--------|
| Clustered Bar | campaign_name × spend vs attributed_revenue |
| KPI Card | ROAS, Campaign ROI % |
| Scatter | budget (X) × roi_pct (Y) |

---

## 5. Scheduled Refresh (Power BI Service)

After publishing to Power BI Service:
1. Go to **Dataset Settings** → **Gateway connection** → link your on-prem gateway (if local DB)
2. Set **Scheduled refresh** → every 1 hour
3. Or call `POST /api/v1/admin/refresh-marts` via Power Automate before each refresh to ensure
   materialised views are up-to-date before Power BI queries them.
