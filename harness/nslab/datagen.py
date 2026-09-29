"""Deterministic synthetic data generator (seeded) producing JSON documents cached as NDJSON.

The base fields are generated with the same seed, order and distributions as rd-databases/harness/rdlab/datagen.py,
so `customers`, `products`, `orders`, `events` ... contain exactly the same values as the relational lab. The extra
NoSQL-only fields (`customers.location`, `products.embedding`, `orders.items`, `inventory.version`) are derived from
per-row RNGs seeded by the row id so they do not disturb the base stream.
"""
from __future__ import annotations

import datetime as dt
import decimal
import json
import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .schema import COLLECTION_BY_NAME, LOAD_ORDER
from .util import DATA

SEED = 20260912
T_START = dt.datetime(2024, 1, 1)
T_END = dt.datetime(2025, 12, 31, 23, 59, 59)
SPAN_S = int((T_END - T_START).total_seconds())

COUNTRIES = [("US", 30), ("DE", 12), ("GB", 10), ("TR", 10), ("FR", 8), ("NL", 5), ("ES", 5), ("IT", 5),
             ("BR", 4), ("IN", 4), ("JP", 3), ("CA", 2), ("AU", 2)]
COUNTRY_CENTROID = {"US": (39.8, -98.6), "DE": (51.2, 10.4), "GB": (54.0, -2.5), "TR": (39.0, 35.2), "FR": (46.6, 2.2),
                    "NL": (52.1, 5.3), "ES": (40.4, -3.7), "IT": (42.5, 12.5), "BR": (-14.2, -51.9), "IN": (20.6, 78.9),
                    "JP": (36.2, 138.3), "CA": (56.1, -106.3), "AU": (-25.3, 133.8)}
TIERS = [("free", 70), ("plus", 20), ("pro", 8), ("vip", 2)]
STATUSES = [("delivered", 70), ("shipped", 10), ("pending", 8), ("cancelled", 7), ("returned", 5)]
EVENT_TYPES = [("page_view", 60), ("add_to_cart", 15), ("login", 10), ("checkout", 8), ("review", 4),
               ("support", 2), ("churn", 1)]
COLORS = ["red", "blue", "green", "black", "white", "silver", "gold"]
MATERIALS = ["steel", "plastic", "wood", "glass", "carbon", "cotton"]
ADJ = ["wireless", "portable", "ergonomic", "compact", "premium", "rugged", "smart", "eco", "ultra", "classic"]
NOUN = ["charger", "headphones", "keyboard", "lamp", "backpack", "bottle", "camera", "speaker", "monitor", "router",
        "tripod", "blender", "kettle", "jacket", "sneakers", "notebook", "drone", "watch", "mouse", "cable"]
FIRST = ["Ada", "Berk", "Cem", "Deniz", "Ece", "Fatih", "Gul", "Hakan", "Ipek", "Jale", "Kaan", "Leyla", "Mert", "Nil",
         "Omer", "Pinar", "Rana", "Selin", "Tolga", "Umut", "Vera", "Yusuf", "Zeynep", "Liam", "Olivia", "Noah", "Emma",
         "Mia", "Lucas", "Sofia"]
LAST = ["Karatay", "Yilmaz", "Kaya", "Demir", "Sahin", "Celik", "Yildiz", "Aydin", "Ozturk", "Arslan", "Smith", "Muller",
        "Garcia", "Rossi", "Silva", "Tanaka", "Dubois", "Novak", "Brown", "Jansen"]
WORDS = ("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna "
         "aliqua quality battery fast light durable design value warranty bluetooth usb charging stereo noise cancelling "
         "waterproof travel office gaming kitchen outdoor fitness wireless charger compact premium rugged smart eco ultra "
         "classic aluminium steel leather fabric mesh foam rubber glass ceramic bamboo recycled solar magnetic foldable "
         "adjustable ergonomic lightweight heavy sturdy slim wide narrow tall short round square modern vintage minimal "
         "bright dim warm cool silent loud powerful efficient reliable affordable luxury handmade imported local seasonal "
         "limited edition bundle refurbished certified tested approved rated popular trending new improved upgraded "
         "professional beginner advanced family kids adult pet garden bathroom bedroom living dining balcony garage "
         "workshop studio classroom hospital hotel restaurant camping hiking cycling running swimming yoga winter summer "
         "spring autumn morning evening daily weekly monthly annual gift holiday birthday anniversary wedding").split()

VECTOR_DIM = 8


def _weighted(rng: random.Random, pairs: list[tuple[str, int]]) -> str:
    vals, weights = zip(*pairs)
    return rng.choices(vals, weights=weights, k=1)[0]


def _ts(rng: random.Random, start: dt.datetime = T_START, span: int = SPAN_S) -> dt.datetime:
    return (start + dt.timedelta(seconds=rng.randrange(span))).replace(microsecond=0)


@dataclass(frozen=True)
class Sizes:
    customers: int
    categories: int
    products: int
    orders: int
    events: int
    warehouses: int = 4

    @classmethod
    def for_scale(cls, scale: float) -> "Sizes":
        return cls(customers=max(1000, int(20_000 * scale)), categories=200, products=max(500, int(5_000 * scale)),
                   orders=max(5000, int(200_000 * scale)), events=max(20_000, int(1_000_000 * scale)))


# --- derived NoSQL-only fields (per-row RNG so the base stream stays identical to rd-databases) --------------------
def location_for(customer_id: int, country: str) -> dict[str, float]:
    r = random.Random(customer_id * 104729 + 7)
    lat, lon = COUNTRY_CENTROID[country]
    return {"lat": round(lat + r.uniform(-4.0, 4.0), 5), "lon": round(lon + r.uniform(-6.0, 6.0), 5)}


def embedding_for(product_id: int, category_id: int) -> list[float]:
    """8-dim unit vector: a category-dependent direction plus product noise (so kNN by category is meaningful)."""
    base = random.Random(category_id * 7919)
    noise = random.Random(product_id * 7919 + 13)
    v = [base.gauss(0, 1) + 0.6 * noise.gauss(0, 1) for _ in range(VECTOR_DIM)]
    n = math.sqrt(sum(x * x for x in v)) or 1.0
    return [round(x / n, 5) for x in v]


def query_vector(seed: int, category_id: int) -> list[float]:
    return embedding_for(10_000_000 + seed, category_id)


# --- generators (base fields identical to rd-databases) --------------------------------------------------------------
def gen_customers(rng: random.Random, n: int) -> Iterator[dict]:
    for i in range(1, n + 1):
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        attrs = {"newsletter": rng.random() < 0.4, "age": rng.randint(18, 80), "lang": rng.choice(["en", "tr", "de", "fr"])}
        country = _weighted(rng, COUNTRIES)
        tier = _weighted(rng, TIERS)
        created = _ts(rng)
        has_attrs = rng.random() < 0.9
        yield {"id": i, "name": name, "email": f"user{i}@example.com", "country_code": country, "tier": tier,
               "created_at": created, "attrs": attrs if has_attrs else None, "location": location_for(i, country)}


def gen_categories(rng: random.Random, n: int) -> Iterator[dict]:
    roots = 10
    rows: list[tuple] = []
    for i in range(1, roots + 1):
        rows.append((i, None, f"Category {i}", 0))
    for i in range(roots + 1, n + 1):
        parent = rng.randint(1, i - 1)
        depth = rows[parent - 1][3] + 1
        if depth > 3:
            parent = rng.randint(1, roots)
            depth = 1
        rows.append((i, parent, f"Category {i}", depth))
    for i, p, name, depth in rows:
        yield {"id": i, "parent_id": p, "name": name, "depth": depth}


def gen_products(rng: random.Random, n: int, n_categories: int) -> Iterator[dict]:
    for i in range(1, n + 1):
        name = f"{rng.choice(ADJ).title()} {rng.choice(NOUN).title()} {rng.choice(['X', 'Pro', 'Mini', 'Max', 'Lite'])}{rng.randint(1, 99)}"
        desc = " ".join(rng.choice(WORDS) for _ in range(rng.randint(20, 60)))
        attrs = {"color": rng.choice(COLORS), "weight": round(rng.uniform(0.1, 30.0), 2), "material": rng.choice(MATERIALS),
                 "tags": rng.sample(ADJ, 2)}
        price = decimal.Decimal(rng.randint(199, 99999)) / 100
        cat = rng.randint(1, n_categories)
        active = rng.random() < 0.85
        yield {"id": i, "category_id": cat, "sku": f"SKU-{i:07d}", "name": name, "description": desc, "price": float(price),
               "attrs": attrs, "active": active, "embedding": embedding_for(i, cat)}


def gen_orders_and_items(rng: random.Random, n_orders: int, n_customers: int, n_products: int,
                         product_prices: list[float]) -> Iterator[dict]:
    """Yields order documents with `items` embedded (the flat order_items file is derived from them)."""
    item_id = 0
    for oid in range(1, n_orders + 1):
        cid = rng.randint(1, n_customers)
        n_items = rng.choices([1, 2, 3, 4, 5, 6], weights=[30, 30, 20, 10, 6, 4], k=1)[0]
        total = decimal.Decimal(0)
        items = []
        for _ in range(n_items):
            item_id += 1
            pid = rng.randint(1, n_products)
            qty = rng.choices([1, 2, 3, 4, 5], weights=[60, 20, 10, 6, 4], k=1)[0]
            unit = decimal.Decimal(str(product_prices[pid - 1]))
            items.append({"id": item_id, "product_id": pid, "qty": qty, "unit_price": float(unit)})
            total += unit * qty
        yield {"id": oid, "customer_id": cid, "status": _weighted(rng, STATUSES), "ordered_at": _ts(rng),
               "total_amount": float(total), "shipping_country": _weighted(rng, COUNTRIES), "items": items}


def gen_events(rng: random.Random, n: int, n_customers: int) -> Iterator[dict]:
    for i in range(1, n + 1):
        cid = rng.randint(1, n_customers)
        et = _weighted(rng, EVENT_TYPES)
        payload = {"page": f"/p/{rng.randint(1, 5000)}", "ref": rng.choice(["direct", "search", "email", "social"]),
                   "device": rng.choice(["mobile", "desktop", "tablet"])}
        occurred = _ts(rng)
        has_payload = rng.random() < 0.8
        value = decimal.Decimal(rng.randint(0, 100000)) / 10000
        yield {"id": i, "customer_id": cid, "event_type": et, "occurred_at": occurred,
               "payload": payload if has_payload else None, "value_num": float(value)}


def gen_inventory(rng: random.Random, n_products: int, warehouses: int) -> Iterator[dict]:
    for pid in range(1, n_products + 1):
        for w in range(1, warehouses + 1):
            yield {"product_id": pid, "warehouse_id": w, "qty": rng.randint(0, 500), "updated_at": _ts(rng), "version": 1}


# --- NDJSON cache ----------------------------------------------------------------------------------------------------
def _enc(o: Any) -> Any:
    if isinstance(o, dt.datetime):
        return o.strftime("%Y-%m-%dT%H:%M:%SZ")
    raise TypeError(type(o))


def csv_dir(scale: float) -> Path:
    return DATA / f"scale-{scale:g}"


def ensure_docs(scale: float, force: bool = False) -> dict[str, Path]:
    """Generate (once) deterministic NDJSON files for the given scale; return collection -> path."""
    d = csv_dir(scale)
    marker = d / "_complete"
    paths = {c: d / f"{c}.ndjson" for c in LOAD_ORDER}
    if marker.exists() and not force:
        return paths
    d.mkdir(parents=True, exist_ok=True)
    sizes = Sizes.for_scale(scale)
    rng = random.Random(SEED)

    def write(coll: str, docs) -> int:
        n = 0
        with open(paths[coll], "w", encoding="utf-8") as f:
            for doc in docs:
                f.write(json.dumps(doc, default=_enc, separators=(",", ":")))
                f.write("\n")
                n += 1
        return n

    counts: dict[str, int] = {}
    counts["customers"] = write("customers", gen_customers(rng, sizes.customers))
    counts["categories"] = write("categories", gen_categories(rng, sizes.categories))
    products = list(gen_products(rng, sizes.products, sizes.categories))
    counts["products"] = write("products", products)
    prices = [p["price"] for p in products]
    del products
    n_items = 0
    with open(paths["order_items"], "w", encoding="utf-8") as fi:
        def orders_and_items():
            nonlocal n_items
            for o in gen_orders_and_items(rng, sizes.orders, sizes.customers, sizes.products, prices):
                for it in o["items"]:
                    fi.write(json.dumps({"id": it["id"], "order_id": o["id"], "product_id": it["product_id"], "qty": it["qty"],
                                         "unit_price": it["unit_price"]}, separators=(",", ":")))
                    fi.write("\n")
                    n_items += 1
                yield o
        counts["orders"] = write("orders", orders_and_items())
    counts["order_items"] = n_items
    counts["events"] = write("events", gen_events(rng, sizes.events, sizes.customers))
    counts["inventory"] = write("inventory", gen_inventory(rng, sizes.products, sizes.warehouses))
    (d / "counts.json").write_text(json.dumps(counts, indent=2))
    marker.write_text("ok")
    return paths


def parse_ts(s: str) -> dt.datetime:
    return dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def read_docs(path: Path, collection: str, *, parse_dates: bool = True) -> Iterator[dict]:
    """Yield documents from a cached NDJSON file (timestamps restored to naive-UTC datetime when parse_dates)."""
    c = COLLECTION_BY_NAME[collection]
    ts = c.ts_fields if parse_dates else ()
    with open(path, encoding="utf-8") as f:
        for line in f:
            doc = json.loads(line)
            for k in ts:
                if doc.get(k):
                    doc[k] = parse_ts(doc[k])
            yield doc


def counts_for(scale: float) -> dict[str, int]:
    p = csv_dir(scale) / "counts.json"
    return json.loads(p.read_text()) if p.exists() else {}
