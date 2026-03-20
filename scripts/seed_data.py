"""
seed_data.py
────────────
Populates the analytics database with realistic synthetic data.
Run once after applying 01_schema.sql.

Usage:
    python seed_data.py --dsn "postgresql://user:pass@localhost:5432/analytics"
"""

import argparse
import random
from datetime import datetime, timedelta
from uuid import uuid4

from faker import Faker
import psycopg2
from psycopg2.extras import execute_values

fake = Faker("en_IN")
random.seed(42)
Faker.seed(42)

# ── Config ────────────────────────────────────────────────────
N_CUSTOMERS    = 500
N_ORDERS       = 3000
N_CAMPAIGNS    = 15
START_DATE     = datetime(2024, 1, 1)
END_DATE       = datetime(2025, 3, 1)

REGIONS = ["Bengaluru", "Mumbai", "Delhi", "Chennai", "Hyderabad",
           "Pune", "Kolkata", "Ahmedabad"]

CATEGORIES = {
    "Electronics":    ["Laptop", "Smartphone", "Tablet", "Headphones",
                       "Smart Watch", "USB Hub", "Webcam"],
    "Home & Kitchen": ["Air Fryer", "Coffee Maker", "Blender",
                       "Instant Pot", "Toaster"],
    "Books":          ["Python Cookbook", "Clean Code", "Designing Data-Intensive Apps",
                       "The Pragmatic Programmer", "SQL Antipatterns"],
    "Fashion":        ["Running Shoes", "Backpack", "Sunglasses", "Polo T-Shirt"],
    "Fitness":        ["Yoga Mat", "Resistance Bands", "Dumbbell Set",
                       "Protein Powder", "Jump Rope"],
}

TIERS    = ["standard", "silver", "gold", "platinum"]
STATUSES = ["delivered", "delivered", "delivered", "shipped",
            "processing", "cancelled", "refunded"]
CHANNELS = ["web", "web", "mobile", "mobile", "store", "partner"]

# ── Helpers ───────────────────────────────────────────────────

def rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def insert_regions(cur) -> list[int]:
    rows = [(r, "India") for r in REGIONS]
    execute_values(cur,
        "INSERT INTO raw.regions (region_name, country) VALUES %s "
        "ON CONFLICT (region_name) DO NOTHING RETURNING region_id",
        rows
    )
    cur.execute("SELECT region_id FROM raw.regions ORDER BY region_id")
    return [row[0] for row in cur.fetchall()]


def insert_categories(cur) -> dict[str, int]:
    cat_ids = {}
    for cat in CATEGORIES:
        cur.execute(
            "INSERT INTO raw.categories (category_name) VALUES (%s) "
            "ON CONFLICT (category_name) DO NOTHING RETURNING category_id",
            (cat,)
        )
        row = cur.fetchone()
        if row:
            cat_ids[cat] = row[0]
    if not cat_ids:
        cur.execute("SELECT category_id, category_name FROM raw.categories")
        cat_ids = {r[1]: r[0] for r in cur.fetchall()}
    return cat_ids


def insert_products(cur, cat_ids: dict[str, int]) -> list[int]:
    rows = []
    sku_set = set()
    for cat_name, products in CATEGORIES.items():
        cat_id = cat_ids[cat_name]
        for prod in products:
            base_price = round(random.uniform(199, 49999), 2)
            cost       = round(base_price * random.uniform(0.45, 0.72), 2)
            sku        = f"SKU-{cat_name[:3].upper()}-{random.randint(1000,9999)}"
            while sku in sku_set:
                sku = f"SKU-{cat_name[:3].upper()}-{random.randint(1000,9999)}"
            sku_set.add(sku)
            rows.append((sku, prod, cat_id, base_price, cost, True))

    execute_values(cur,
        "INSERT INTO raw.products "
        "(sku, product_name, category_id, unit_price, cost_price, is_active) "
        "VALUES %s ON CONFLICT (sku) DO NOTHING",
        rows
    )
    cur.execute("SELECT product_id, unit_price, cost_price FROM raw.products")
    return cur.fetchall()   # list of (product_id, unit_price, cost_price)


def insert_customers(cur, region_ids: list[int]) -> list[str]:
    rows = []
    emails = set()
    for _ in range(N_CUSTOMERS):
        email = fake.unique.email()
        while email in emails:
            email = fake.unique.email()
        emails.add(email)
        rows.append((
            str(uuid4()),
            fake.name(),
            email,
            random.choice(region_ids),
            rand_date(START_DATE, END_DATE).date(),
            random.choices(TIERS, weights=[60, 20, 15, 5])[0],
        ))
    execute_values(cur,
        "INSERT INTO raw.customers "
        "(customer_id, full_name, email, region_id, signup_date, customer_tier) "
        "VALUES %s ON CONFLICT (email) DO NOTHING",
        rows
    )
    cur.execute("SELECT customer_id FROM raw.customers")
    return [str(r[0]) for r in cur.fetchall()]


def insert_campaigns(cur) -> list[int]:
    rows = []
    channels = ["email", "social", "search", "display", "affiliate"]
    for i in range(1, N_CAMPAIGNS + 1):
        start = rand_date(START_DATE, END_DATE - timedelta(days=30)).date()
        end   = start + timedelta(days=random.randint(14, 90))
        budget = round(random.uniform(10_000, 500_000), 2)
        spend  = round(budget * random.uniform(0.5, 1.05), 2)
        rows.append((
            f"Campaign {i:02d} – {fake.catch_phrase()[:40]}",
            random.choice(channels),
            start, end,
            budget, spend,
        ))
    execute_values(cur,
        "INSERT INTO raw.campaigns "
        "(campaign_name, channel, start_date, end_date, budget, spend) "
        "VALUES %s",
        rows
    )
    cur.execute("SELECT campaign_id FROM raw.campaigns")
    return [r[0] for r in cur.fetchall()]


def insert_orders(cur, customer_ids, products, campaign_ids):
    order_rows = []
    item_rows  = []
    attribution_rows = []

    for _ in range(N_ORDERS):
        oid     = str(uuid4())
        cust    = random.choice(customer_ids)
        odate   = rand_date(START_DATE, END_DATE)
        status  = random.choices(STATUSES, weights=[50,10,8,5,5,3,3])[0]
        channel = random.choice(CHANNELS)
        disc    = random.choices([0, 5, 10, 15, 20], weights=[50,20,15,10,5])[0]

        order_rows.append((oid, cust, odate, status, channel, disc))

        # 1–5 line items
        n_items = random.randint(1, 5)
        chosen  = random.sample(products, min(n_items, len(products)))
        for prod_id, unit_price, cost_price in chosen:
            qty = random.randint(1, 4)
            item_rows.append((oid, prod_id, qty,
                              float(unit_price), float(cost_price)))

        # ~40% of orders attributed to a campaign
        if random.random() < 0.4:
            camp = random.choice(campaign_ids)
            attribution_rows.append((oid, camp))

    execute_values(cur,
        "INSERT INTO raw.orders "
        "(order_id, customer_id, order_date, status, channel, discount_pct) "
        "VALUES %s",
        order_rows
    )
    execute_values(cur,
        "INSERT INTO raw.order_items "
        "(order_id, product_id, quantity, unit_price, cost_price) "
        "VALUES %s",
        item_rows
    )
    if attribution_rows:
        execute_values(cur,
            "INSERT INTO raw.order_attribution (order_id, campaign_id) "
            "VALUES %s ON CONFLICT DO NOTHING",
            attribution_rows
        )

    print(f"  ✓  {len(order_rows)} orders  |  {len(item_rows)} line items  "
          f"|  {len(attribution_rows)} attributions")


# ── Main ──────────────────────────────────────────────────────

def seed(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur  = conn.cursor()

    try:
        print("Seeding regions …")
        region_ids = insert_regions(cur)

        print("Seeding categories …")
        cat_ids = insert_categories(cur)

        print("Seeding products …")
        products = insert_products(cur, cat_ids)

        print("Seeding customers …")
        customer_ids = insert_customers(cur, region_ids)

        print("Seeding campaigns …")
        campaign_ids = insert_campaigns(cur)

        print("Seeding orders & items …")
        insert_orders(cur, customer_ids, products, campaign_ids)

        print("Refreshing materialised views …")
        cur.execute("CALL api.refresh_marts()")

        conn.commit()
        print("\n✅  Seed complete!")

    except Exception as exc:
        conn.rollback()
        print(f"❌  Seed failed: {exc}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dsn",
        default="postgresql://postgres:postgres@localhost:5432/analytics",
        help="PostgreSQL connection string"
    )
    args = parser.parse_args()
    seed(args.dsn)
