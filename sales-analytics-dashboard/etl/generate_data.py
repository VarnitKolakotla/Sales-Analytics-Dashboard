"""Synthetic Indian retail / e-commerce sales generator (big-data scale).

Creates millions of order lines with realistic behaviour so the analytics have
something real to discover:
  * yearly growth, weekend effect, Diwali / festive-season ramps, sale-day spikes
  * outage days (sales collapse) -> anomaly detection demo
  * customers who sign up over time -> cohort retention demo
  * products that are bought together (phone + case)    -> market-basket demo
  * ~1.5% deliberately dirty rows (duplicates, nulls, bad dates ...) -> ETL cleaning demo

Usage:  python -m etl.generate_data --rows 2000000
"""
from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd

import config
from etl.schema import RAW_COLUMNS
from storage import get_store

START = pd.Timestamp("2023-01-01")
END = pd.Timestamp("2026-09-30")

# category -> (margin range, [(product, list price INR), ...])
CATALOG = {
    "Electronics": ((0.12, 0.22), [("Smartphone", 24999), ("Laptop", 54999), ("Wireless Earbuds", 2999), ("Smart Watch", 6999),
        ("Bluetooth Speaker", 2499), ("Tablet", 18999), ("Power Bank", 1499), ("Phone Case", 499), ("Laptop Bag", 1299),
        ("LED Monitor", 11999), ("Wireless Mouse", 699), ("Mechanical Keyboard", 3499), ("Smart TV 43in", 28999),
        ("Webcam HD", 1999), ("USB-C Charger", 999)]),
    "Fashion": ((0.35, 0.50), [("Men's Jeans", 1999), ("Cotton T-Shirt", 599), ("Kurta Set", 1799), ("Running Shoes", 3499),
        ("Sports Socks", 249), ("Women's Saree", 3499), ("Leather Wallet", 899), ("Sunglasses", 1499), ("Formal Shirt", 1499),
        ("Sneakers", 2799), ("Backpack", 1699), ("Winter Jacket", 3299), ("Denim Jacket", 2599), ("Handbag", 2299),
        ("Ethnic Dress", 2199)]),
    "Home & Kitchen": ((0.28, 0.40), [("Air Fryer", 5999), ("Mixer Grinder", 3299), ("Non-stick Cookware Set", 2499),
        ("Bedsheet Set", 1499), ("Water Purifier", 12999), ("Vacuum Cleaner", 7499), ("Pressure Cooker", 1799),
        ("LED Bulb Pack", 499), ("Dinner Set", 2999), ("Wall Clock", 799), ("Storage Containers", 699),
        ("Electric Kettle", 1199), ("Curtains Pair", 1299), ("Study Table", 4499), ("Office Chair", 6999)]),
    "Grocery": ((0.12, 0.20), [("Basmati Rice 5kg", 649), ("Cold Pressed Oil 1L", 399), ("Almonds 500g", 549),
        ("Tea Powder 1kg", 449), ("Coffee Beans 500g", 699), ("Protein Muesli", 349), ("Honey 500g", 329), ("Ghee 1L", 699),
        ("Dry Fruits Box", 1299), ("Instant Noodles Pack", 199), ("Whole Wheat Atta 10kg", 499), ("Masala Combo", 299),
        ("Organic Jaggery", 199), ("Green Tea Box", 249), ("Olive Oil 1L", 899)]),
    "Beauty & Personal Care": ((0.40, 0.55), [("Face Serum", 899), ("Sunscreen SPF50", 499), ("Hair Dryer", 1499),
        ("Perfume 100ml", 1799), ("Shampoo 650ml", 449), ("Electric Trimmer", 1399), ("Moisturizer", 549),
        ("Lipstick Set", 999), ("Face Wash", 299), ("Beard Oil", 399), ("Hair Oil 500ml", 349), ("Body Lotion", 499),
        ("Makeup Kit", 2499), ("Facial Kit", 799), ("Deodorant Pack", 399)]),
    "Sports & Fitness": ((0.30, 0.45), [("Yoga Mat", 799), ("Dumbbell Set 20kg", 2999), ("Cricket Bat", 2499),
        ("Badminton Racket", 1299), ("Football", 899), ("Resistance Bands", 599), ("Treadmill", 32999),
        ("Fitness Tracker", 2499), ("Skipping Rope", 249), ("Protein Powder 1kg", 2299), ("Cycling Helmet", 1599),
        ("Gym Gloves", 499), ("Shaker Bottle", 349), ("Camping Tent", 3999), ("Foam Roller", 899)]),
    "Books & Stationery": ((0.30, 0.40), [("Notebook Pack of 6", 399), ("Fountain Pen", 699), ("Engineering Textbook", 899),
        ("Competitive Exam Guide", 749), ("Fiction Bestseller", 349), ("Desk Organizer", 599), ("Sketch Pad Set", 449),
        ("Planner 2026", 499), ("Highlighter Set", 199), ("Calculator Scientific", 899), ("Art Supplies Kit", 1299),
        ("Children Storybook Set", 599), ("Geometry Box", 249), ("Whiteboard + Markers", 999),
        ("Self-help Bestseller", 399)]),
}
CATEGORIES = list(CATALOG)
CAT_BASE_P = np.array([0.17, 0.22, 0.15, 0.20, 0.10, 0.08, 0.08])
CAT_SEASON = np.array([   # Jan..Dec multiplier per category
    [1.0, .95, .95, .95, 1.0, .95, .9, 1.05, 1.1, 1.6, 1.5, 1.2],
    [1.1, .95, 1.0, 1.05, 1.0, 1.1, .95, 1.0, 1.1, 1.5, 1.4, 1.3],
    [1.0, .95, 1.0, 1.0, 1.0, 1.0, .95, 1.0, 1.05, 1.35, 1.3, 1.1],
    [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.05, 1.15, 1.15, 1.05],
    [1.0, 1.05, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.05, 1.25, 1.2, 1.15],
    [1.2, 1.1, 1.0, 1.0, .95, .9, .9, .95, 1.0, 1.0, 1.0, 1.05],
    [1.1, 1.0, 1.0, 1.15, 1.0, 1.2, 1.15, 1.0, 1.0, 1.0, 1.0, 1.0],
])
COMPANIONS = [("Smartphone", "Phone Case"), ("Smartphone", "Power Bank"), ("Smartphone", "USB-C Charger"),
              ("Laptop", "Laptop Bag"), ("Laptop", "Wireless Mouse"), ("Running Shoes", "Sports Socks"),
              ("Treadmill", "Fitness Tracker"), ("Yoga Mat", "Resistance Bands"), ("Face Wash", "Moisturizer"),
              ("Shampoo 650ml", "Hair Oil 500ml"), ("Tea Powder 1kg", "Honey 500g"),
              ("Air Fryer", "Non-stick Cookware Set"), ("Fountain Pen", "Notebook Pack of 6"),
              ("Smart TV 43in", "Bluetooth Speaker"), ("Sunscreen SPF50", "Face Serum"),
              ("Protein Powder 1kg", "Shaker Bottle"), ("Sneakers", "Sports Socks"), ("Tablet", "Wireless Earbuds")]

STATES = [("Maharashtra", "West", ["Mumbai", "Pune", "Nagpur", "Nashik"], .14),
          ("Karnataka", "South", ["Bengaluru", "Mysuru", "Mangaluru"], .11),
          ("Delhi", "North", ["New Delhi"], .10),
          ("Tamil Nadu", "South", ["Chennai", "Coimbatore", "Madurai"], .09),
          ("Telangana", "South", ["Hyderabad", "Warangal"], .08),
          ("Uttar Pradesh", "North", ["Lucknow", "Noida", "Kanpur", "Varanasi"], .08),
          ("Gujarat", "West", ["Ahmedabad", "Surat", "Vadodara"], .08),
          ("West Bengal", "East", ["Kolkata", "Siliguri"], .06),
          ("Rajasthan", "North", ["Jaipur", "Jodhpur"], .04),
          ("Madhya Pradesh", "Central", ["Indore", "Bhopal"], .04),
          ("Punjab", "North", ["Ludhiana", "Amritsar"], .03),
          ("Kerala", "South", ["Kochi", "Thiruvananthapuram"], .04),
          ("Odisha", "East", ["Bhubaneswar"], .025),
          ("Bihar", "East", ["Patna"], .02),
          ("Chhattisgarh", "Central", ["Raipur"], .015),
          ("Assam", "East", ["Guwahati"], .015)]

CHANNELS = ["Website", "Mobile App", "Retail Store", "Marketplace"]
CH_START = np.array([.30, .28, .24, .18])
CH_END = np.array([.20, .40, .14, .26])        # shift towards app / marketplace over time
PAYMENTS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery", "Wallet"]
PAY_P = np.array([.46, .20, .12, .05, .12, .05])
DISCOUNTS = np.array([0, .05, .10, .15, .20, .30])
DISC_NORMAL = np.array([.45, .20, .15, .10, .07, .03])
DISC_FESTIVE = np.array([.15, .15, .20, .20, .18, .12])
DOW_FACTOR = np.array([.95, .93, .95, .98, 1.05, 1.17, 1.15])          # Mon..Sun
MONTH_FACTOR = np.array([1.05, .95, 1.0, .97, 1.0, .95, .9, 1.0, 1.05, 1.35, 1.4, 1.2])

# one-off business events: multiplier on that day's order volume
EVENTS = {"2023-08-15": 2.0, "2024-01-26": 1.8, "2024-06-14": 0.05, "2025-03-08": 2.4,
          "2025-09-22": 2.2, "2026-01-26": 1.9, "2026-05-02": 0.08}
DIWALI = ["2023-11-12", "2024-11-01", "2025-10-20", "2026-11-08"]
DIWALI_RAMP = {-6: 1.2, -5: 1.3, -4: 1.5, -3: 1.8, -2: 2.1, -1: 2.4, 0: 1.7, 1: 0.8}
LINES_P = np.array([.55, .28, .12, .05])        # 1..4 lines per order -> mean 1.67
LINES_MEAN = float((np.arange(1, 5) * LINES_P).sum())


def day_weights(days: pd.DatetimeIndex) -> np.ndarray:
    years = (days - START).days.values / 365.25
    w = (1 + 0.30 * years) * DOW_FACTOR[days.dayofweek.values] * MONTH_FACTOR[days.month.values - 1]
    w = w.astype(float)
    for d, m in EVENTS.items():
        w[days == pd.Timestamp(d)] *= m
    for d in DIWALI:
        for off, m in DIWALI_RAMP.items():
            w[days == pd.Timestamp(d) + pd.Timedelta(days=off)] *= m
    oct_sale = (days.month == 10) & (days.day <= 7)
    w[oct_sale] *= 1.25
    return w


class SalesGenerator:
    """Deterministic world (customers + products) so every run / upload is consistent."""

    def __init__(self, seed: int = 42, n_customers: int = 150_000):
        self.seed = seed
        r = np.random.default_rng(seed)
        # ---- products
        names, cats, price, cost_ratio, pop = [], [], [], [], []
        self.cat_products = []                       # per category: indices of product rows
        for ci, c in enumerate(CATEGORIES):
            (mlo, mhi), items = CATALOG[c]
            idx = []
            ranks = r.permutation(len(items))
            for (n, p), rk in zip(items, ranks):
                idx.append(len(names))
                names.append(n); cats.append(ci); price.append(p)
                cost_ratio.append(1 - r.uniform(mlo, mhi)); pop.append(1 / (rk + 1) ** 0.7)
            self.cat_products.append(np.array(idx))
        self.p_name = np.array(names, dtype=object)
        self.p_cat = np.array(cats)
        self.p_price = np.array(price, dtype=float)
        self.p_cost_ratio = np.array(cost_ratio)
        pop = np.array(pop)
        self.cat_pop = [pop[ix] / pop[ix].sum() for ix in self.cat_products]
        name_to_ix = {n: i for i, n in enumerate(names)}
        comp = {i: [] for i in range(len(names))}
        for a, b in COMPANIONS:
            comp[name_to_ix[a]].append(name_to_ix[b]); comp[name_to_ix[b]].append(name_to_ix[a])
        mx = max(len(v) for v in comp.values())
        self.comp = -np.ones((len(names), mx), dtype=int)
        self.comp_n = np.zeros(len(names), dtype=int)
        for i, v in comp.items():
            self.comp_n[i] = len(v)
            self.comp[i, :len(v)] = v
        # ---- customers (sorted by sign-up day so eligibility is a prefix)
        self.n_customers = n_customers
        H = (END - START).days + 1
        self.signup_day = np.sort((H * r.random(n_customers) ** 0.85).astype(int))
        self.activity = r.lognormal(0, 1.0, n_customers)
        sw = np.array([s[3] for s in STATES]); sw = sw / sw.sum()
        self.c_state = r.choice(len(STATES), n_customers, p=sw)
        self.c_city = np.array([r.integers(len(STATES[s][2])) for s in range(len(STATES))])  # placeholder
        self.c_city = (r.random(n_customers) * np.array([len(STATES[s][2]) for s in self.c_state])).astype(int)

    # ------------------------------------------------------------------
    def generate(self, n_rows: int, start: pd.Timestamp = START, end: pd.Timestamp = END,
                 dirty: bool = True, id_start: int = 1, seed_offset: int = 0, id_prefix: str = "ORD"):
        """Yield (YYYY-MM, DataFrame) for every month in [start, end]."""
        r = np.random.default_rng(self.seed + 1 + seed_offset)
        days = pd.date_range(start, end)
        n_orders = max(1, int(n_rows / LINES_MEAN))
        w = day_weights(days)
        day_ix = np.sort(r.choice(len(days), n_orders, p=w / w.sum()))
        order_dates = days[day_ix]
        lines = r.choice(np.arange(1, 5), n_orders, p=LINES_P)
        order_of_row = np.repeat(np.arange(n_orders), lines)
        n = len(order_of_row)
        row_start = np.cumsum(lines) - lines
        line_no = np.arange(n) - row_start[order_of_row] + 1
        row_dates = order_dates[order_of_row]
        moy = row_dates.month.values
        ym_codes = row_dates.year.values * 100 + row_dates.month.values

        # ---- customer per order (eligible = signed-up before the month ends)
        cust = np.empty(n_orders, dtype=int)
        o_ym = order_dates.year.values * 100 + order_dates.month.values
        for ym in np.unique(o_ym):
            m = o_ym == ym
            month_end = pd.Timestamp(year=ym // 100, month=ym % 100, day=1) + pd.offsets.MonthEnd(0)
            k = max(50, int(np.searchsorted(self.signup_day, (month_end - START).days, side="right")))
            k = min(k, self.n_customers)
            cdf = np.cumsum(self.activity[:k])
            cust[m] = np.searchsorted(cdf, r.random(m.sum()) * cdf[-1])
        t_frac = np.clip((order_dates - START).days.values / ((END - START).days), 0, 1)
        # ---- channel (mix drifts over time)
        probs = CH_START[None, :] + (CH_END - CH_START)[None, :] * t_frac[:, None]
        channel = np.minimum((r.random(n_orders)[:, None] > np.cumsum(probs, axis=1)).sum(axis=1), 3)
        pay = r.choice(len(PAYMENTS), n_orders, p=PAY_P)
        festive_o = np.isin(order_dates.month.values, [10, 11]) | np.isin(
            order_dates.strftime("%Y-%m-%d"), list(EVENTS))

        # ---- products
        cat = np.empty(n, dtype=int)
        for m_ in range(1, 13):
            mk = moy == m_
            if mk.any():
                p = CAT_BASE_P * CAT_SEASON[:, m_ - 1]
                cat[mk] = r.choice(len(CATEGORIES), mk.sum(), p=p / p.sum())
        prod = np.empty(n, dtype=int)
        for ci in range(len(CATEGORIES)):
            mk = cat == ci
            if mk.any():
                prod[mk] = r.choice(self.cat_products[ci], mk.sum(), p=self.cat_pop[ci])
        for k in (2, 3, 4):                           # "bought together" behaviour
            ix = np.where(line_no == k)[0]
            prev = prod[ix - 1]
            take = (r.random(len(ix)) < 0.38) & (self.comp_n[prev] > 0)
            pick = (r.random(len(ix)) * np.maximum(self.comp_n[prev], 1)).astype(int)
            prod[ix[take]] = self.comp[prev[take], pick[take]]
        cat = self.p_cat[prod]

        # ---- money
        years = (row_dates - START).days.values / 365.25
        infl = 1 + 0.05 * years
        unit_price = np.round(self.p_price[prod] * infl * r.lognormal(0, 0.02, n), 2)
        unit_cost = np.round(self.p_price[prod] * self.p_cost_ratio[prod] * infl, 2)
        fest_row = festive_o[order_of_row]
        disc = np.where(fest_row, r.choice(DISCOUNTS, n, p=DISC_FESTIVE), r.choice(DISCOUNTS, n, p=DISC_NORMAL))
        qty = np.ones(n, dtype=int)
        for ci, c in enumerate(CATEGORIES):
            mk = cat == ci
            pq = {"Electronics": [.90, .07, .02, .005, .005], "Grocery": [.40, .30, .15, .10, .05],
                  "Books & Stationery": [.40, .30, .15, .10, .05], "Beauty & Personal Care": [.50, .28, .12, .06, .04]
                  }.get(c, [.62, .22, .10, .04, .02])
            qty[mk] = r.choice(np.arange(1, 6), mk.sum(), p=pq)

        oid = id_start + order_of_row
        c_of_row = cust[order_of_row]
        state_ix = self.c_state[c_of_row]
        city_ix = self.c_city[c_of_row]
        st_names = np.array([s[0] for s in STATES], dtype=object)
        st_region = np.array([s[1] for s in STATES], dtype=object)
        cities = [s[2] for s in STATES]
        city_arr = np.array([cities[s][c] for s, c in zip(state_ix, city_ix)], dtype=object) \
            if n < 400_000 else self._cities_fast(state_ix, city_ix, cities)

        ym_unique = np.unique(ym_codes)
        for ym in ym_unique:
            m = ym_codes == ym
            df = pd.DataFrame({
                "order_id": np.char.add(f"{id_prefix}-", np.char.zfill(oid[m].astype(str), 8)),
                "line_no": line_no[m],
                "order_date": row_dates[m].strftime("%Y-%m-%d"),
                "customer_id": np.char.add("CUST-", np.char.zfill((c_of_row[m] + 1).astype(str), 6)),
                "product_id": np.char.add("P", np.char.zfill((prod[m] + 1).astype(str), 3)),
                "product_name": self.p_name[prod[m]],
                "category": np.array(CATEGORIES, dtype=object)[cat[m]],
                "region": st_region[state_ix[m]],
                "state": st_names[state_ix[m]],
                "city": city_arr[m],
                "channel": np.array(CHANNELS, dtype=object)[channel[order_of_row][m]],
                "payment_method": np.array(PAYMENTS, dtype=object)[pay[order_of_row][m]],
                "quantity": qty[m], "unit_price": unit_price[m], "unit_cost": unit_cost[m],
                "discount_pct": disc[m],
            })[RAW_COLUMNS]
            if dirty:
                df = inject_dirt(df, r)
            yield f"{ym // 100}-{ym % 100:02d}", df

    @staticmethod
    def _cities_fast(state_ix, city_ix, cities):
        out = np.empty(len(state_ix), dtype=object)
        for s in range(len(cities)):
            m = state_ix == s
            out[m] = np.array(cities[s], dtype=object)[city_ix[m]]
        return out


def inject_dirt(df: pd.DataFrame, r: np.random.Generator) -> pd.DataFrame:
    """Make ~1.5% of rows realistically bad so the ETL has real work to do."""
    n = len(df)
    df = df.copy()
    def pick(p): return r.random(n) < p
    df.loc[pick(0.003), "customer_id"] = None                                   # missing customer
    df.loc[pick(0.002), "quantity"] = r.choice([0, -1], 1)[0]                   # impossible quantity
    df.loc[pick(0.001), "order_date"] = r.choice(["N/A", "2025-02-30", "32/13/2025"])  # corrupt date
    df.loc[pick(0.001), "unit_price"] = -1.0                                    # negative price
    df.loc[pick(0.010), "discount_pct"] = np.nan                                # missing discount (fill 0)
    mk = pick(0.020)                                                            # messy casing/spaces
    half = r.random(n) < 0.5
    df.loc[mk & half, "region"] = df.loc[mk & half, "region"].str.lower()
    df.loc[mk & ~half, "region"] = " " + df.loc[mk & ~half, "region"] + " "
    mk = pick(0.015)
    df.loc[mk, "payment_method"] = df.loc[mk, "payment_method"].str.upper() + " "
    dup = df.sample(frac=0.005, random_state=int(r.integers(1_000_000)))        # duplicates
    return pd.concat([df, dup], ignore_index=True)


def main():
    ap = argparse.ArgumentParser(description="Generate raw sales data and upload it to cloud storage")
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--customers", type=int, default=150_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--reset", action="store_true", help="delete raw/ zone first")
    a = ap.parse_args()

    store = get_store()
    if a.reset:
        print(f"Removed {store.delete_prefix(config.ZONE_RAW)} old raw objects")
    t0 = time.time()
    gen = SalesGenerator(a.seed, a.customers)
    tmp = Path(tempfile.mkdtemp(prefix="gen_"))
    total_rows = total_bytes = 0
    for ym, df in gen.generate(a.rows):
        f = tmp / f"orders_{ym}.csv"
        df.to_csv(f, index=False)
        store.put_file(f, f"{config.ZONE_RAW}ingest_month={ym}/orders_{ym}.csv")
        total_rows += len(df); total_bytes += f.stat().st_size
        f.unlink()
        print(f"  uploaded raw/ingest_month={ym}  rows={len(df):>8,}", flush=True)
    print(f"\nDone: {total_rows:,} raw rows, {total_bytes / 1e6:,.1f} MB -> {store.describe()['backend']} "
          f"[{store.describe()['bucket']}] in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
