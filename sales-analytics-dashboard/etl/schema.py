"""Data contract between the raw zone and the curated zone."""

RAW_COLUMNS = [
    "order_id", "line_no", "order_date", "customer_id", "product_id", "product_name",
    "category", "region", "state", "city", "channel", "payment_method",
    "quantity", "unit_price", "unit_cost", "discount_pct",
]

# Canonical spellings; the cleaner maps messy variants (" south ", "UPI ", "upi") onto these.
CANONICAL = {
    "region": ["North", "South", "East", "West", "Central"],
    "channel": ["Website", "Mobile App", "Retail Store", "Marketplace"],
    "payment_method": ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery", "Wallet"],
}

CURATED_COLUMNS = [
    "order_id", "line_no", "order_date", "customer_id", "product_id", "product_name",
    "category", "region", "state", "city", "channel", "payment_method",
    "quantity", "unit_price", "unit_cost", "discount_pct",
    "gross_amount", "discount_amount", "revenue", "cost", "profit",
    "dow", "is_weekend", "quarter",
]
