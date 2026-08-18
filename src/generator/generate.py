from pathlib import Path

import numpy as np
import pandas as pd
import yaml


# ============================================================
# CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE = ROOT / "config" / "data_scale.yaml"

PROFILE = "small"
OUTPUT = ROOT / "data" / PROFILE

with open(CONFIG_FILE, encoding="utf-8") as f:
    cfg = yaml.safe_load(f)[PROFILE]

OUTPUT.mkdir(parents=True, exist_ok=True)

rng = np.random.default_rng(42)


# ============================================================
# CUSTOMERS
# ============================================================

customers = pd.DataFrame({
    "customer_id": np.arange(1, cfg["customers"] + 1),
    "country": rng.choice(
        ["FR", "DE", "ES", "IT", "BE"],
        cfg["customers"]
    )
})


# ============================================================
# PRODUCTS
# ============================================================

products = pd.DataFrame({
    "product_id": np.arange(1, cfg["products"] + 1),
    "category": rng.choice(
        ["tech", "home", "sport", "fashion"],
        cfg["products"]
    ),
    "price": rng.uniform(
        5,
        500,
        cfg["products"]
    ).round(2)
})


# ============================================================
# ORDERS
# ============================================================

orders = pd.DataFrame({
    "order_id": np.arange(1, cfg["orders"] + 1),
    "customer_id": rng.integers(
        1,
        cfg["customers"] + 1,
        cfg["orders"]
    ),
    "order_date": (
        pd.Timestamp("2026-01-01")
        + pd.to_timedelta(
            rng.integers(0, 90, cfg["orders"]),
            unit="D"
        )
    ),
    "status": rng.choice(
        ["created", "paid", "shipped", "cancelled"],
        cfg["orders"]
    )
})


# ============================================================
# ORDER ITEMS
# ============================================================

order_items = pd.DataFrame({
    "order_item_id": np.arange(1, cfg["order_items"] + 1),
    "order_id": rng.integers(
        1,
        cfg["orders"] + 1,
        cfg["order_items"]
    ),
    "product_id": rng.integers(
        1,
        cfg["products"] + 1,
        cfg["order_items"]
    ),
    "quantity": rng.integers(
        1,
        6,
        cfg["order_items"]
    )
})


# ============================================================
# PAYMENTS
# ============================================================

payments = pd.DataFrame({
    "payment_id": np.arange(1, cfg["payments"] + 1),
    "order_id": rng.integers(
        1,
        cfg["orders"] + 1,
        cfg["payments"]
    ),
    "amount": rng.uniform(
        10,
        500,
        cfg["payments"]
    ).round(2),
    "status": rng.choice(
        ["accepted", "refused", "refunded"],
        cfg["payments"]
    )
})


# ============================================================
# TRANSACTIONS
# ============================================================

transactions = pd.DataFrame({
    "transaction_id": np.arange(1, cfg["transactions"] + 1),
    "payment_id": rng.integers(
        1,
        cfg["payments"] + 1,
        cfg["transactions"]
    ),
    "amount": rng.uniform(
        -500,
        500,
        cfg["transactions"]
    ).round(2)
})


# ============================================================
# EVENTS
# ============================================================

event_orders = rng.integers(
    1,
    cfg["orders"] + 1,
    cfg["events"]
)

events = pd.DataFrame({
    "event_id": np.arange(1, cfg["events"] + 1),
    "order_id": event_orders,
    "customer_id": (
        orders
        .set_index("order_id")
        .loc[event_orders, "customer_id"]
        .to_numpy()
    ),
    "event_type": rng.choice(
        ["view", "checkout", "payment", "shipment"],
        cfg["events"]
    ),
    "event_time": (
        pd.Timestamp("2026-01-01")
        + pd.to_timedelta(
            rng.integers(0, 90 * 24 * 60, cfg["events"]),
            unit="m"
        )
    )
})


# ============================================================
# CONTROLLED ANOMALIES
# ============================================================

anomalies = cfg["anomalies"]


# Data skew:
# une partie importante des commandes appartient au customer_id = 1
skew_n = int(len(orders) * anomalies["skew_customer_rate"])

skew_idx = rng.choice(
    orders.index,
    skew_n,
    replace=False
)

orders.loc[skew_idx, "customer_id"] = 1


# Réaligner customer_id des events avec les orders
events["customer_id"] = (
    orders
    .set_index("order_id")
    .loc[events["order_id"], "customer_id"]
    .to_numpy()
)


# NULL sur payment.amount
null_n = int(len(payments) * anomalies["null_rate"])

null_idx = rng.choice(
    payments.index,
    null_n,
    replace=False
)

payments.loc[null_idx, "amount"] = np.nan


# Valeurs invalides sur payment.status
invalid_n = int(len(payments) * anomalies["invalid_rate"])

invalid_idx = rng.choice(
    payments.index,
    invalid_n,
    replace=False
)

payments.loc[invalid_idx, "status"] = "INVALID"


# Doublons dans events
# On remplace certaines lignes par des lignes existantes
# afin de conserver exactement le volume configuré.
duplicate_n = int(
    len(events) * anomalies["duplicate_rate"]
)

target_idx = rng.choice(
    events.index,
    duplicate_n,
    replace=False
)

source_idx = rng.choice(
    events.index,
    duplicate_n,
    replace=True
)

events.loc[target_idx] = events.loc[source_idx].to_numpy()


# Late events
late_n = int(
    len(events) * anomalies["late_event_rate"]
)

late_idx = rng.choice(
    events.index,
    late_n,
    replace=False
)

events.loc[late_idx, "event_time"] -= pd.Timedelta(days=14)


# ============================================================
# WRITE RAW FILES
# ============================================================

tables = {
    "customers": customers,
    "products": products,
    "orders": orders,
    "order_items": order_items,
    "payments": payments,
    "transactions": transactions,
    "events": events,
}

print("\n=== GENERATED DATA ===")

for name, df in tables.items():
    path = OUTPUT / f"{name}.csv"
    df.to_csv(path, index=False)

    print(f"{name:15} → {len(df):,}")


# ============================================================
# QUALITY / DISTRIBUTION CHECK
# ============================================================

print("\n=== CONTROLLED ANOMALIES ===")

print(
    f"Orders customer_id=1 : "
    f"{(orders['customer_id'] == 1).sum():,}"
)

print(
    f"Payments NULL amount : "
    f"{payments['amount'].isna().sum():,}"
)

print(
    f"Payments INVALID     : "
    f"{(payments['status'] == 'INVALID').sum():,}"
)

print(
    f"Duplicate event_id   : "
    f"{events['event_id'].duplicated().sum():,}"
)

print(
    f"Oldest event         : "
    f"{events['event_time'].min()}"
)